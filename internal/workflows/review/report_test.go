package review

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
)

const reportSpecial = "原始中文 | <script> & [連結](https://example.invalid) \"引號\" \\ 路徑\n````````\n<!-- pwc-review-data -->\n# 不可變成標題"

type reportFixture struct {
	attempt   *contract.Attempt
	script    string
	request   contract.Request
	candidate map[string]any
	prepared  Prepared
	reviewed  map[string]Reviewed
}

func reportTestArtifact(t *testing.T, prepared Prepared, validation any) []byte {
	t.Helper()
	body := resourceTestJSON(t, map[string]any{"prepared": prepared, "validation": validation})
	return []byte("# Review report\n\n<!-- pwc-review-data -->\n```json\n" + string(body) + "\n```\n")
}

func reportTempDir(t *testing.T) string {
	t.Helper()
	// macOS's system temporary directory may itself use the /var alias.
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func newReportFixture(t *testing.T) *reportFixture {
	t.Helper()
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	store, err := contract.NewStore(registry, contract.Options{BaseDir: reportTempDir(t), Prompt: "report renderer"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	paths, err := ExtractSkills(reportTempDir(t))
	if err != nil {
		t.Fatal(err)
	}
	publish := func(schema string, data any) contract.Ref {
		attempt, _ := resourceTestCandidate(t, store, schema, data)
		staged, err := attempt.Stage(context.Background(), contract.Spec{SchemaID: schema})
		if err != nil {
			t.Fatal(err)
		}
		ref, err := attempt.Publish(context.Background(), staged)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	p := resourceTestData()[PrepareSchema].(Prepared)
	p.OpenQuestions = []string{"來源尚待確認：" + reportSpecial}
	p.Sources[0].Note = reportSpecial
	p.Requirements = []Requirement{}
	for _, kind := range []string{"requirement", "constraint", "decision"} {
		p.Requirements = append(p.Requirements, Requirement{ID: "req-" + kind, Kind: kind, Statement: kind + reportSpecial, SourceIDs: []string{"src-001"}})
	}
	contextRef := publish(PrepareSchema, p)
	inputs := []contract.Ref{contextRef}
	v := resourceTestData()[ValidationSchema].(Validated)
	v.Context, v.ReportFile = contextRef, "review-report"
	v.Reviewers, v.Requirements = []ReviewerResult{}, []Assessment{}
	v.Completeness, v.Limitations = "limited", []string{"保留限制 " + reportSpecial}
	v.Findings[0].Title = "完整 finding " + reportSpecial
	v.Findings[0].Impact = reportSpecial
	v.Dispositions[0].Reason = "確認理由 " + reportSpecial
	for _, action := range []string{"merged", "excluded", "unconfirmed"} {
		target := ""
		if action == "merged" {
			target = v.Findings[0].ID
		}
		v.Dispositions = append(v.Dispositions, Disposition{FindingID: "scale-" + action, Action: action, TargetID: target, Reason: action + reportSpecial})
	}
	reviewed := map[string]Reviewed{}
	for _, role := range []string{"code", "scale", "simplicity"} {
		rv := resourceTestData()[ReviewerSchema].(Reviewed)
		rv.Role, rv.Context = role, contextRef
		rv.Limitations = []string{role + " 原始限制 " + reportSpecial}
		rv.Findings, rv.Requirements = []Finding{}, []Assessment{}
		if role == "code" {
			for _, requirement := range p.Requirements {
				rv.Requirements = append(rv.Requirements, Assessment{RequirementID: requirement.ID, Status: "unconfirmed", Evidence: []Evidence{}, Reason: "原判定 " + reportSpecial})
				v.Requirements = append(v.Requirements, Assessment{RequirementID: requirement.ID, Status: "satisfied", Evidence: v.Findings[0].Evidence, Reason: "獨立驗證 " + reportSpecial})
			}
		}
		ref := publish(ReviewerSchema, rv)
		inputs = append(inputs, ref)
		v.Reviewers = append(v.Reviewers, ReviewerResult{Role: role, Status: "succeeded", Ref: &ref, Detail: role + " published " + reportSpecial})
		reviewed[role] = rv
	}
	attempt, raw := resourceTestCandidate(t, store, ValidationSchema, v)
	f := &reportFixture{attempt: attempt, script: filepath.Join(filepath.Dir(paths["validate"]), "scripts", "render_report.py"), prepared: p, reviewed: reviewed}
	if err := json.Unmarshal(raw, &f.candidate); err != nil {
		t.Fatal(err)
	}
	requestRaw, err := os.ReadFile(attempt.RequestPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(requestRaw, &f.request); err != nil {
		t.Fatal(err)
	}
	f.request.Inputs = inputs
	f.save(t)
	return f
}

func (f *reportFixture) save(t *testing.T) {
	t.Helper()
	fixtureWrite(t, f.attempt.RequestPath(), resourceTestJSON(t, f.request))
	fixtureWrite(t, f.attempt.CandidatePath(), resourceTestJSON(t, f.candidate))
}

func runReport(t *testing.T, script, request, candidate string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, "python3", script, request, candidate)
	cmd.Dir = t.TempDir()
	cmd.WaitDelay = time.Second
	output, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("renderer exceeded subprocess deadline: %v\n%s", ctx.Err(), output)
	}
	return output, err
}

func reportRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func reportBlocks(t *testing.T, raw string) []any {
	t.Helper()
	lines := strings.Split(raw, "\n")
	var blocks []any
	for i := 0; i < len(lines); i++ {
		if !strings.HasSuffix(lines[i], "json") || !strings.HasPrefix(lines[i], "```") {
			continue
		}
		fence := strings.TrimSuffix(lines[i], "json")
		if strings.Trim(fence, "`") != "" {
			t.Fatal("non-backtick fence")
		}
		start := i + 1
		for i = start; i < len(lines) && lines[i] != fence; i++ {
		}
		if i == len(lines) {
			t.Fatal("unterminated JSON fence")
		}
		body := strings.Join(lines[start:i], "\n")
		for _, run := range regexp.MustCompile("`+").FindAllString(body, -1) {
			if len(run) >= len(fence) {
				t.Fatal("external value can escape fence")
			}
		}
		var value any
		if err := json.Unmarshal([]byte(body), &value); err != nil {
			t.Fatal(err)
		}
		blocks = append(blocks, value)
	}
	return blocks
}

func reportHasValue(t *testing.T, blocks []any, want any) {
	t.Helper()
	var normalized any
	if err := json.Unmarshal(resourceTestJSON(t, want), &normalized); err != nil {
		t.Fatal(err)
	}
	for _, block := range blocks {
		if reflect.DeepEqual(block, normalized) {
			return
		}
	}
	t.Fatalf("readable section omitted complete value: %#v", want)
}

func TestRenderReportEmbeddedDeterministicAndComplete(t *testing.T) {
	t.Chdir(t.TempDir())
	f := newReportFixture(t)
	original := reportRead(t, f.attempt.CandidatePath())
	requestBefore := reportRead(t, f.attempt.RequestPath())
	preparedBefore := reportRead(t, f.request.Inputs[0].Path)
	filesBefore := map[string][]byte{}
	for _, entry := range f.candidate["files"].([]any) {
		path := entry.(map[string]any)["path"].(string)
		filesBefore[path] = reportRead(t, filepath.Join(f.attempt.Dir(), path))
	}
	if output, err := runReport(t, f.script, f.attempt.RequestPath(), f.attempt.CandidatePath()); err != nil {
		t.Fatalf("renderer: %v\n%s", err, output)
	}
	updated := reportRead(t, f.attempt.CandidatePath())
	var candidate map[string]any
	if err := json.Unmarshal(updated, &candidate); err != nil {
		t.Fatal(err)
	}
	want := resourceTestObject(t, f.candidate)
	want["data"].(map[string]any)["report_file"] = "review-report"
	want["files"] = append(want["files"].([]any), map[string]any{"id": "review-report", "kind": "artifact", "path": "artifacts/review-report.md"})
	if !reflect.DeepEqual(candidate, want) {
		t.Fatal("candidate changed beyond report_file and appended file entry")
	}
	for path, before := range filesBefore {
		if got := reportRead(t, filepath.Join(f.attempt.Dir(), path)); !bytes.Equal(got, before) {
			t.Fatal("existing evidence/artifact changed")
		}
	}
	if !bytes.Equal(requestBefore, reportRead(t, f.attempt.RequestPath())) || !bytes.Equal(preparedBefore, reportRead(t, f.request.Inputs[0].Path)) {
		t.Fatal("renderer changed an input")
	}
	reportPath := filepath.Join(f.attempt.Dir(), "artifacts", "review-report.md")
	report := reportRead(t, reportPath)
	parts := strings.Split(string(report), "<!-- pwc-review-data -->\n")
	if len(parts) != 2 {
		t.Fatal("appendix delimiter is missing or external text escaped JSON")
	}
	appendix := reportBlocks(t, parts[1])
	if len(appendix) != 1 {
		t.Fatal("expected one final appendix JSON block")
	}
	reportHasValue(t, appendix, map[string]any{"prepared": f.prepared, "validation": candidate["data"]})
	blocks := reportBlocks(t, parts[0])
	reportHasValue(t, blocks, candidate["meta"])
	reportHasValue(t, blocks, map[string]any{"prepared": f.prepared.Pin, "validation": f.prepared.Pin})
	reportHasValue(t, blocks, f.request.Inputs[0])
	data := candidate["data"].(map[string]any)
	for _, key := range []string{"reviewers", "findings", "dispositions"} {
		for _, value := range data[key].([]any) {
			reportHasValue(t, blocks, value)
		}
	}
	for _, source := range f.prepared.Sources {
		reportHasValue(t, blocks, source)
	}
	for i, requirement := range f.prepared.Requirements {
		reportHasValue(t, blocks, requirement)
		reportHasValue(t, blocks, []any{data["requirements"].([]any)[i]})
		reportHasValue(t, blocks, []Assessment{f.reviewed["code"].Requirements[i]})
	}
	for _, reviewer := range f.reviewed {
		reportHasValue(t, blocks, reviewer.Limitations)
	}
	reportHasValue(t, blocks, f.prepared.OpenQuestions)
	reportHasValue(t, blocks, data["limitations"])
	reportHasValue(t, blocks, map[string]any{"completeness": data["completeness"], "conclusion": data["conclusion"], "report_file": "review-report"})
	for _, text := range []string{"原始中文", "### 需求 1", "### 需求 2", "### 需求 3", "### Finding 1", "tests/build/compile/deploy", "OCR", "local correctness coverage", "沒有外部 write"} {
		if !strings.Contains(string(report), text) {
			t.Fatalf("missing report section/declaration: %q", text)
		}
	}
	if _, err := f.attempt.Stage(context.Background(), contract.Spec{SchemaID: ValidationSchema}); err != nil {
		t.Fatalf("renderer produced non-schemaful candidate: %v", err)
	}
	if output, err := runReport(t, f.script, f.attempt.RequestPath(), f.attempt.CandidatePath()); err == nil {
		t.Fatalf("second invocation overwrote report: %s", output)
	}
	if !bytes.Equal(updated, reportRead(t, f.attempt.CandidatePath())) || !bytes.Equal(report, reportRead(t, reportPath)) {
		t.Fatal("rejected invocation changed outputs")
	}
	fixtureWrite(t, f.attempt.CandidatePath(), original)
	if err := os.Remove(reportPath); err != nil {
		t.Fatal(err)
	}
	if output, err := runReport(t, f.script, f.attempt.RequestPath(), f.attempt.CandidatePath()); err != nil {
		t.Fatalf("determinism replay: %v\n%s", err, output)
	}
	if !bytes.Equal(updated, reportRead(t, f.attempt.CandidatePath())) || !bytes.Equal(report, reportRead(t, reportPath)) {
		t.Fatal("identical inputs did not produce identical bytes")
	}
}

func TestRenderReportPreservesIncompleteUnknown(t *testing.T) {
	f := newReportFixture(t)
	data := f.candidate["data"].(map[string]any)
	rows := data["reviewers"].([]any)
	for i, status := range []string{"failed", "missing"} {
		row := rows[i+1].(map[string]any)
		row["status"], row["ref"], row["detail"] = status, nil, "未取得結果 "+reportSpecial
	}
	data["findings"], data["dispositions"] = []any{}, []any{}
	data["completeness"], data["conclusion"] = "incomplete", "undetermined"
	for _, item := range data["requirements"].([]any) {
		assessment := item.(map[string]any)
		assessment["status"], assessment["evidence"] = "unconfirmed", []any{}
	}
	f.request.Inputs = f.request.Inputs[:2]
	f.save(t)
	if output, err := runReport(t, f.script, f.attempt.RequestPath(), f.attempt.CandidatePath()); err != nil {
		t.Fatalf("renderer: %v\n%s", err, output)
	}
	report := string(reportRead(t, filepath.Join(f.attempt.Dir(), "artifacts", "review-report.md")))
	blocks := reportBlocks(t, strings.Split(report, "<!-- pwc-review-data -->\n")[0])
	for _, row := range rows {
		reportHasValue(t, blocks, row)
	}
	for _, item := range data["requirements"].([]any) {
		reportHasValue(t, blocks, []any{item})
	}
	reportHasValue(t, blocks, map[string]any{"completeness": "incomplete", "conclusion": "undetermined", "report_file": "review-report"})
}

func TestRenderReportRejectsInvalidInputsWithoutWrites(t *testing.T) {
	for _, name := range []string{
		"wrong context", "wrong manifest", "meta run", "meta invocation", "meta attempt", "meta token", "meta version", "meta schema",
		"input digest", "input identity", "input schema", "wrong pin", "duplicate reviewer input", "unlisted reviewer",
		"file ID collision", "file path collision", "entry traversal", "duplicate file ID", "duplicate file path",
		"candidate symlink", "candidate hardlink", "candidate FIFO", "request symlink", "prepare symlink", "ancestor symlink",
		"artifacts symlink", "artifacts regular", "report symlink", "report regular", "report directory", "report FIFO",
		"different attempt", "traversal argument", "relative argument",
	} {
		t.Run(name, func(t *testing.T) {
			f := newReportFixture(t)
			data := f.candidate["data"].(map[string]any)
			meta := f.candidate["meta"].(map[string]any)
			files := f.candidate["files"].([]any)
			requestPath, candidatePath := f.attempt.RequestPath(), f.attempt.CandidatePath()
			outside := filepath.Join(reportTempDir(t), "keep")
			fixtureWrite(t, outside, []byte("untouched"))
			link := func(target, path string) {
				t.Helper()
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			remove := func(path string) {
				t.Helper()
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			fifo := func(path string) {
				t.Helper()
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch name {
			case "wrong context":
				data["context"].(map[string]any)["path"] = outside
			case "wrong manifest":
				data["context"].(map[string]any)["manifest_sha256"] = strings.Repeat("f", 64)
			case "meta run", "meta invocation", "meta attempt", "meta token", "meta version", "meta schema":
				key := map[string]string{"meta run": "run_id", "meta invocation": "invocation_id", "meta attempt": "attempt_id", "meta token": "dispatch_token", "meta version": "version", "meta schema": "schema_id"}[name]
				meta[key] = "wrong"
			case "input digest":
				fixtureWrite(t, f.request.Inputs[0].Path, []byte("{}"))
			case "input identity":
				f.request.Inputs[0].AttemptID = "other"
				data["context"] = resourceTestObject(t, f.request.Inputs[0])
			case "input schema":
				f.request.Inputs[0].SchemaID = ReviewerSchema
				data["context"] = resourceTestObject(t, f.request.Inputs[0])
			case "wrong pin":
				data["pin"].(map[string]any)["context_id"] = "other"
			case "duplicate reviewer input":
				f.request.Inputs[1] = f.request.Inputs[2]
			case "unlisted reviewer":
				f.request.Inputs = f.request.Inputs[:1]
			case "file ID collision":
				files[0].(map[string]any)["id"] = "review-report"
			case "file path collision":
				files[0].(map[string]any)["path"] = "artifacts/review-report.md"
			case "entry traversal":
				files[0].(map[string]any)["path"] = "artifacts/../artifacts/review-report.md"
			case "duplicate file ID":
				files[1].(map[string]any)["id"] = files[0].(map[string]any)["id"]
			case "duplicate file path":
				files[1].(map[string]any)["path"] = files[0].(map[string]any)["path"]
			}
			f.save(t)
			artifacts := filepath.Join(f.attempt.Dir(), "artifacts")
			reportPath := filepath.Join(artifacts, "review-report.md")
			switch name {
			case "candidate symlink", "request symlink", "prepare symlink":
				path := candidatePath
				switch name {
				case "request symlink":
					path = requestPath
				case "prepare symlink":
					path = f.request.Inputs[0].Path
				}
				remove(path)
				link(outside, path)
			case "candidate hardlink":
				remove(candidatePath)
				if err := os.Link(outside, candidatePath); err != nil {
					t.Fatal(err)
				}
			case "candidate FIFO":
				remove(candidatePath)
				fifo(candidatePath)
			case "ancestor symlink":
				alias := filepath.Join(reportTempDir(t), "attempt")
				link(f.attempt.Dir(), alias)
				requestPath, candidatePath = filepath.Join(alias, "request.json"), filepath.Join(alias, "candidate.json")
			case "artifacts symlink":
				remove(artifacts)
				link(filepath.Dir(outside), artifacts)
			case "artifacts regular":
				remove(artifacts)
				fixtureWrite(t, artifacts, []byte("keep"))
			case "report symlink":
				link(outside, reportPath)
			case "report regular":
				fixtureWrite(t, reportPath, []byte("keep"))
			case "report directory":
				if err := os.Mkdir(reportPath, 0700); err != nil {
					t.Fatal(err)
				}
			case "report FIFO":
				fifo(reportPath)
			case "different attempt":
				candidatePath = filepath.Join(reportTempDir(t), "candidate.json")
				fixtureWrite(t, candidatePath, reportRead(t, f.attempt.CandidatePath()))
			case "traversal argument":
				candidatePath = f.attempt.Dir() + "/artifacts/../candidate.json"
			case "relative argument":
				candidatePath = "candidate.json"
			}
			var before []byte
			if name != "candidate FIFO" {
				before = reportRead(t, f.attempt.CandidatePath())
			}
			if output, err := runReport(t, f.script, requestPath, candidatePath); err == nil {
				t.Fatalf("invalid input accepted: %s", output)
			}
			if name != "candidate FIFO" && !bytes.Equal(before, reportRead(t, f.attempt.CandidatePath())) {
				t.Fatal("rejected input changed candidate")
			}
			if string(reportRead(t, outside)) != "untouched" {
				t.Fatal("renderer overwrote another file")
			}
			if name == "report regular" && string(reportRead(t, reportPath)) != "keep" {
				t.Fatal("renderer overwrote an existing report")
			}
			if name != "artifacts regular" && !strings.HasPrefix(name, "report ") {
				if _, err := os.Lstat(reportPath); !os.IsNotExist(err) {
					t.Fatalf("rejected input created report: %v", err)
				}
			}
		})
	}
}
