package review

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

func checkFixture(t *testing.T) (*Checkout, Prepared, contract.Ref, map[string]Reviewed, []ReviewerResult, Validated) {
	t.Helper()
	p := resourceTestData()[PrepareSchema].(Prepared)
	c := &Checkout{URL: p.Pin.URL, Repository: p.Pin.Repository, Number: p.Pin.Number, BaseSHA: p.Pin.BaseSHA, HeadSHA: p.Pin.HeadSHA, MergeBase: p.Pin.MergeBase, DiffRange: p.Pin.DiffRange, ContextID: p.Pin.ContextID, Worktree: t.TempDir(), Snapshots: map[string]string{}}
	fixtureWrite(t, filepath.Join(c.Worktree, "service.go"), []byte("one\ntwo\nthree\nfour"))
	p.Sources = []Source{}
	for _, key := range []string{"metadata", "diff", "changed-files", "issues", "inline", "reviews"} {
		path := filepath.Join(t.TempDir(), key)
		fixtureWrite(t, path, []byte("snapshot "+key))
		c.Snapshots[key] = path
		p.Sources = append(p.Sources, Source{ID: key, Kind: key, URL: "file://" + path, Status: "available", FileID: key})
	}
	p.Requirements[0].SourceIDs = []string{"metadata"}
	ref := resourceTestData()[ReviewerSchema].(Reviewed).Context
	reviewed := map[string]Reviewed{}
	rows := []ReviewerResult{}
	for _, role := range []string{"code", "scale", "simplicity"} {
		rv := resourceTestData()[ReviewerSchema].(Reviewed)
		rv.Role = role
		rv.Findings = []Finding{}
		if role != "code" {
			rv.Requirements = []Assessment{}
		}
		reviewed[role] = rv
		rr := ref
		rr.SchemaID = ReviewerSchema
		rr.AttemptID = role
		rows = append(rows, ReviewerResult{Role: role, Status: "succeeded", Ref: &rr, Detail: "published"})
	}
	v := Validated{Pin: pin(c), Context: ref, Reviewers: rows, Findings: []Finding{}, Dispositions: []Disposition{}, Requirements: reviewed["code"].Requirements, Completeness: "complete", Conclusion: "no_confirmed_findings", Limitations: []string{}, ReportFile: "report"}
	return c, p, ref, reviewed, rows, v
}

func TestCheckPreparedSemantics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Checkout, *Prepared, map[string]checkFile, string)
		want   string
	}{
		{"valid", func(*Checkout, *Prepared, map[string]checkFile, string) {}, ""},
		{"pin", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) { p.Pin.ContextID += "other" }, "pin mismatch"},
		{"duplicate source", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			p.Sources = append(p.Sources, p.Sources[0])
		}, "source ID"},
		{"unknown file", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) { p.Sources[0].FileID = "absent" }, "unknown file"},
		{"omitted snapshot", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) { p.Sources = p.Sources[1:] }, "absent from sources"},
		{"wrong snapshot", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) { p.Sources[0].FileID = "diff" }, "bytes differ"},
		{"controller mandatory missing", func(c *Checkout, _ *Prepared, _ map[string]checkFile, _ string) { delete(c.Snapshots, "diff") }, "controller snapshot"},
		{"arbitrary file ID", func(_ *Checkout, p *Prepared, f map[string]checkFile, _ string) {
			f["arbitrary"] = f["metadata"]
			p.Sources[0].FileID = "arbitrary"
		}, ""},
		{"URL source identity", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			p.Sources[0].ID = "src-metadata"
			p.Requirements[0].SourceIDs = []string{"src-metadata"}
		}, ""},
		{"missing is not available", func(c *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			delete(c.Snapshots, "issues")
			c.Missing = []string{"issues"}
		}, "unavailable source"},
		{"missing without note", func(c *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			delete(c.Snapshots, "issues")
			c.Missing = []string{"issues"}
			p.Sources[3].Status = "missing"
			p.Sources[3].FileID = ""
		}, "uncertainty note"},
		{"honest missing", func(c *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			delete(c.Snapshots, "issues")
			c.Missing = []string{"issues"}
			p.Sources[3].Status = "missing"
			p.Sources[3].FileID = ""
			p.Sources[3].Note = "API unavailable"
		}, ""},
		{"conflicting external source", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			p.Sources = append(p.Sources, Source{ID: "design", Kind: "design", URL: "https://example.invalid/design", Status: "conflict", FileID: "metadata", Note: "Linked design conflicts with PR requirements"})
		}, ""},
		{"conflict without note", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			p.Sources = append(p.Sources, Source{ID: "design", Kind: "design", URL: "https://example.invalid/design", Status: "conflict", FileID: "metadata"})
		}, "uncertainty note"},
		{"duplicate requirement", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			p.Requirements = append(p.Requirements, p.Requirements[0])
		}, "requirement ID"},
		{"source ID duplicate", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			p.Requirements[0].SourceIDs = []string{"metadata", "metadata"}
		}, "unknown/duplicate source"},
		{"source ID unknown", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			p.Requirements[0].SourceIDs = []string{"unknown"}
		}, "unknown/duplicate source"},
		{"no source", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) { p.Requirements[0].SourceIDs = nil }, "statement and sources"},
		{"empty baseline", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) { p.Requirements = nil }, "open question"},
		{"honest empty baseline", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) {
			p.Requirements = nil
			p.OpenQuestions = []string{"No authoritative requirements available"}
		}, ""},
		{"blank question", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) { p.OpenQuestions = []string{" "} }, "blank text"},
		{"context not artifact", func(_ *Checkout, p *Prepared, _ map[string]checkFile, _ string) { p.ContextFile = "metadata" }, "not an artifact"},
		{"empty context", func(_ *Checkout, _ *Prepared, _ map[string]checkFile, dir string) {
			fixtureWrite(t, filepath.Join(dir, "context"), nil)
		}, "nonempty UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, p, _, _, _, _ := checkFixture(t)
			dir := t.TempDir()
			ref := contract.Ref{Path: filepath.Join(dir, "contract.json")}
			files := map[string]checkFile{}
			for key, path := range c.Snapshots {
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				fixtureWrite(t, filepath.Join(dir, key), raw)
				files[key] = checkFile{ID: key, Kind: "evidence", Path: key}
			}
			fixtureWrite(t, filepath.Join(dir, "context"), []byte("Context"))
			files["context"] = checkFile{ID: "context", Kind: "artifact", Path: "context"}
			tc.change(c, &p, files, dir)
			checkError(t, preparedSemantics(context.Background(), ref, c, p, files), tc.want)
		})
	}
}
func checkError(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("got %v, want error containing %q", err, want)
	}
}

func TestCheckReviewedSemantics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Reviewed)
		want   string
	}{
		{"valid", func(*Reviewed) {}, ""},
		{"pin", func(v *Reviewed) { v.Pin.HeadSHA = strings.ToUpper(v.Pin.HeadSHA) }, "pin mismatch"},
		{"context digest", func(v *Reviewed) { v.Context.ManifestSHA256 = strings.Repeat("c", 64) }, "context Ref mismatch"},
		{"wrong role", func(v *Reviewed) { v.Role = "scale" }, "role mismatch"},
		{"no coverage", func(v *Reviewed) { v.Coverage = nil }, "coverage is empty"},
		{"blank coverage", func(v *Reviewed) { v.Coverage = []string{" "} }, "blank text"},
		{"blank limitation", func(v *Reviewed) { v.Limitations = []string{"\n"} }, "blank text"},
		{"wrong finding role", func(v *Reviewed) { v.Findings[0].ID = "scale-001" }, "finding ID"},
		{"duplicate finding", func(v *Reviewed) { v.Findings = append(v.Findings, v.Findings[0]) }, "finding ID"},
		{"empty impact", func(v *Reviewed) { v.Findings[0].Impact = " " }, "impact/evidence"},
		{"empty evidence", func(v *Reviewed) { v.Findings[0].Evidence = nil }, "impact/evidence"},
		{"location beyond head", func(v *Reviewed) { v.Findings[0].Location.EndLine = 5 }, "exceeds 4"},
		{"empty reason", func(v *Reviewed) { v.Requirements[0].Reason = " " }, "needs reason"},
		{"no code assessment", func(v *Reviewed) { v.Requirements = nil }, "exactly once"},
		{"unknown requirement", func(v *Reviewed) { v.Requirements[0].RequirementID = "unknown" }, "unknown/duplicate"},
		{"duplicate assessment", func(v *Reviewed) { v.Requirements = append(v.Requirements, v.Requirements[0]) }, "unknown/duplicate"},
		{"satisfied without head", func(v *Reviewed) { v.Requirements[0].Status = "satisfied"; v.Requirements[0].Evidence = nil }, "needs head evidence"},
		{"unconfirmed without head", func(v *Reviewed) { v.Requirements[0].Status = "unconfirmed"; v.Requirements[0].Evidence = nil }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, p, ref, _, _, _ := checkFixture(t)
			v := resourceTestData()[ReviewerSchema].(Reviewed)
			tc.change(&v)
			checkError(t, reviewedSemantics(context.Background(), c, p, ref, "code", v), tc.want)
		})
	}
	for _, role := range []string{"scale", "simplicity"} {
		t.Run(role+" subset", func(t *testing.T) {
			c, p, ref, rv, _, _ := checkFixture(t)
			checkError(t, reviewedSemantics(context.Background(), c, p, ref, role, rv[role]), "")
		})
	}
}

func TestCheckEvidenceRootAndLines(t *testing.T) {
	for _, tc := range []struct {
		name, path, text string
		line, end        int
		symlink          bool
		want             string
	}{
		{"newline", "head", "one\ntwo\n", 1, 2, false, ""},
		{"unterminated", "head", "one\ntwo", 2, 2, false, ""},
		{"empty", "head", "", 1, 1, false, "exceeds 0"},
		{"no phantom trailing line", "head", "one\n", 1, 2, false, "exceeds 1"},
		{"reverse range", "head", "one\ntwo", 2, 1, false, "range"},
		{"zero", "head", "one", 0, 1, false, "range"},
		{"traversal", "../head", "one", 1, 1, false, "relative and rooted"},
		{"absolute", "/etc/passwd", "one", 1, 1, false, "relative and rooted"},
		{"outside symlink", "head", "one", 1, 1, true, "head evidence"},
		{"directory", ".", "one", 1, 1, false, "regular file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Checkout{Worktree: t.TempDir()}
			if tc.symlink {
				outside := filepath.Join(t.TempDir(), "head")
				fixtureWrite(t, outside, []byte(tc.text))
				if err := os.Symlink(outside, filepath.Join(c.Worktree, "head")); err != nil {
					t.Fatal(err)
				}
			} else {
				fixtureWrite(t, filepath.Join(c.Worktree, "head"), []byte(tc.text))
			}
			checkError(t, checkEvidence(context.Background(), c, Evidence{Path: tc.path, Line: tc.line, EndLine: tc.end, Detail: "head verification"}), tc.want)
		})
	}
}

func TestCheckValidatedSemantics(t *testing.T) {
	type input struct {
		c    *Checkout
		p    Prepared
		ref  contract.Ref
		rv   map[string]Reviewed
		rows []ReviewerResult
		v    Validated
	}
	limited := func(i *input) {
		i.v.Completeness = "limited"
		i.v.Conclusion = "undetermined"
		i.v.Limitations = []string{"Review evidence is limited"}
	}
	finding := func(i *input) {
		f := resourceTestData()[ReviewerSchema].(Reviewed).Findings[0]
		r := i.rv["code"]
		r.Findings = []Finding{f}
		i.rv["code"] = r
		i.v.Findings = []Finding{f}
		i.v.Dispositions = []Disposition{{FindingID: f.ID, Action: "confirmed", TargetID: f.ID, Reason: "verified head"}}
		i.v.Conclusion = "findings"
	}
	merged := func(i *input) {
		finding(i)
		f := i.v.Findings[0]
		f.ID = "scale-001"
		r := i.rv["scale"]
		r.Findings = []Finding{f}
		i.rv["scale"] = r
		i.v.Dispositions = append(i.v.Dispositions, Disposition{FindingID: f.ID, Action: "merged", TargetID: i.v.Findings[0].ID, Reason: "same verified head defect"})
	}
	for _, tc := range []struct {
		name   string
		change func(*input)
		want   string
	}{
		{"complete", func(*input) {}, ""},
		{"conservative limited", limited, ""},
		{"limited lacks reason", func(i *input) { limited(i); i.v.Limitations = nil }, "explicit limitations"},
		{"incomplete without failure", func(i *input) { i.v.Completeness = "incomplete" }, "requires missing/failed"},
		{"wrong reviewer detail", func(i *input) { i.v.Reviewers[0].Detail = "changed" }, "result mismatch"},
		{"wrong reviewer ref", func(i *input) { ref := *i.v.Reviewers[0].Ref; ref.SHA256 = "changed"; i.v.Reviewers[0].Ref = &ref }, "result mismatch"},
		{"duplicate reviewer", func(i *input) { i.v.Reviewers[1] = i.v.Reviewers[0] }, "result mismatch"},
		{"unknown role", func(i *input) { i.v.Reviewers[0].Role = "other" }, "result mismatch"},
		{"missing reviewer lied success", func(i *input) { i.rows[1].Status = "missing"; i.rows[1].Ref = nil; delete(i.rv, "scale") }, "result mismatch"},
		{"failed means incomplete", func(i *input) {
			i.rows[1].Status = "failed"
			i.rows[1].Ref = nil
			delete(i.rv, "scale")
			i.v.Reviewers[1] = i.rows[1]
			limited(i)
		}, "requires incomplete"},
		{"honest incomplete", func(i *input) {
			i.rows[1].Status = "failed"
			i.rows[1].Ref = nil
			delete(i.rv, "scale")
			i.v.Reviewers[1] = i.rows[1]
			limited(i)
			i.v.Completeness = "incomplete"
		}, ""},
		{"missing source PR body still available", func(i *input) {
			i.p.Sources = append(i.p.Sources, Source{ID: "design", Status: "missing", Note: "unavailable"})
		}, "overstates"},
		{"conflict", func(i *input) { i.p.Sources[0].Status = "conflict" }, "overstates"},
		{"open question", func(i *input) { i.p.OpenQuestions = []string{"Which requirement wins?"} }, "overstates"},
		{"reviewer limitation", func(i *input) {
			r := i.rv["scale"]
			r.Limitations = []string{"No capacity evidence"}
			i.rv["scale"] = r
		}, "overstates"},
		{"reviewer unconfirmed requirement", func(i *input) {
			r := i.rv["code"]
			r.Requirements = append([]Assessment(nil), r.Requirements...)
			r.Requirements[0].Status = "unconfirmed"
			i.rv["code"] = r
		}, "overstates"},
		{"validation limitation", func(i *input) { i.v.Limitations = []string{"No external source"} }, "overstates"},
		{"validation unconfirmed requirement", func(i *input) { i.v.Requirements[0].Status = "unconfirmed" }, "overstates"},
		{"no baseline", func(i *input) {
			i.p.Requirements = nil
			i.v.Requirements = nil
			r := i.rv["code"]
			r.Requirements = nil
			i.rv["code"] = r
		}, "overstates"},
		{"missing baseline row", func(i *input) { i.v.Requirements = nil }, "exactly once"},
		{"unknown baseline row", func(i *input) { i.v.Requirements[0].RequirementID = "new" }, "unknown/duplicate"},
		{"confirmed", finding, ""},
		{"confirmed and merged", merged, ""},
		{"excluded final ID with merged contributor", func(i *input) {
			merged(i)
			i.v.Dispositions[0].Action = "excluded"
			i.v.Dispositions[0].TargetID = ""
		}, "no self-confirmed source"},
		{"confirmed targets another final ID", func(i *input) {
			merged(i)
			i.v.Findings = append(i.v.Findings, i.rv["scale"].Findings[0])
			i.v.Dispositions[0].TargetID = "scale-001"
			i.v.Dispositions[1].Action = "confirmed"
			i.v.Dispositions[1].TargetID = "scale-001"
		}, "confirmed disposition must retain its own ID"},
		{"self merge", func(i *input) {
			finding(i)
			i.v.Dispositions[0].Action = "merged"
		}, "merged disposition must target another finding"},
		{"submodule content cannot claim complete", func(i *input) {
			i.c.Missing = []string{"submodule-content"}
		}, "overstates"},
		{"submodule content honestly limited", func(i *input) {
			i.c.Missing = []string{"submodule-content"}
			limited(i)
			i.v.Limitations = []string{"Submodule gitlinks inspected; external content was not acquired"}
		}, ""},
		{"duplicate confirmed", func(i *input) { finding(i); i.v.Findings = append(i.v.Findings, i.v.Findings[0]) }, "finding ID"},
		{"new confirmed ID", func(i *input) { finding(i); i.v.Findings[0].ID = "code-new" }, "has no source"},
		{"no dispositions", func(i *input) { finding(i); i.v.Dispositions = nil }, "not disposed exactly once"},
		{"duplicate disposition", func(i *input) { finding(i); i.v.Dispositions = append(i.v.Dispositions, i.v.Dispositions[0]) }, "invalid/duplicate disposition"},
		{"unknown disposition", func(i *input) { finding(i); i.v.Dispositions[0].FindingID = "scale-new" }, "invalid/duplicate disposition"},
		{"unknown target", func(i *input) { finding(i); i.v.Dispositions[0].TargetID = "scale-new" }, "confirmed disposition must retain its own ID"},
		{"merged unknown target", func(i *input) { merged(i); i.v.Dispositions[1].TargetID = "scale-new" }, "not confirmed"},
		{"orphan target", func(i *input) { finding(i); i.v.Dispositions[0].Action = "excluded"; i.v.Dispositions[0].TargetID = "" }, "no self-confirmed source"},
		{"excluded with target", func(i *input) { finding(i); i.v.Dispositions[0].Action = "excluded" }, "has target"},
		{"excluded", func(i *input) {
			finding(i)
			i.v.Findings = nil
			i.v.Conclusion = "no_confirmed_findings"
			i.v.Dispositions[0].Action = "excluded"
			i.v.Dispositions[0].TargetID = ""
		}, ""},
		{"unconfirmed cannot complete", func(i *input) {
			finding(i)
			i.v.Findings = nil
			i.v.Dispositions[0].Action = "unconfirmed"
			i.v.Dispositions[0].TargetID = ""
		}, "overstates"},
		{"unconfirmed limited", func(i *input) {
			finding(i)
			i.v.Findings = nil
			i.v.Dispositions[0].Action = "unconfirmed"
			i.v.Dispositions[0].TargetID = ""
			limited(i)
		}, ""},
		{"wrong conclusion", func(i *input) { i.v.Conclusion = "findings" }, "conclusion must be"},
		{"findings despite limited", func(i *input) { finding(i); limited(i); i.v.Conclusion = "findings" }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, p, ref, rv, rows, v := checkFixture(t)
			v.Reviewers = append([]ReviewerResult(nil), rows...)
			v.Requirements = append([]Assessment(nil), v.Requirements...)
			i := input{c, p, ref, rv, rows, v}
			tc.change(&i)
			checkError(t, validatedSemantics(context.Background(), i.c, i.p, i.ref, i.rows, i.rv, i.v), tc.want)
		})
	}
}

func TestCheckReportAppendix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fence  string
		change func(map[string]any)
		raw    func(string) string
		want   string
	}{
		{name: "exact values"},
		{name: "four backticks with backtick data", fence: "````"},
		{name: "nine backticks with backtick data", fence: "`````````"},
		{name: "wrongPrepared.statement", change: func(a map[string]any) {
			a["prepared"].(map[string]any)["requirements"].([]any)[0].(map[string]any)["statement"] = "Different accepted requirement"
		}, want: "differs from accepted"},
		{name: "wrongRef", change: func(a map[string]any) {
			a["validation"].(map[string]any)["context"].(map[string]any)["manifest_sha256"] = strings.Repeat("f", 64)
		}, want: "differs from accepted"},
		{name: "wrong reviewer Ref", change: func(a map[string]any) {
			a["validation"].(map[string]any)["reviewers"].([]any)[0].(map[string]any)["ref"].(map[string]any)["sha256"] = strings.Repeat("f", 64)
		}, want: "differs from accepted"},
		{name: "wrongValidationstatus", change: func(a map[string]any) {
			a["validation"].(map[string]any)["completeness"] = "limited"
		}, want: "differs from accepted"},
		{name: "wrong reviewer status", change: func(a map[string]any) {
			a["validation"].(map[string]any)["reviewers"].([]any)[0].(map[string]any)["status"] = "failed"
		}, want: "differs from accepted"},
		{name: "missingappendix", raw: func(string) string { return "# Report without traceability" }, want: "lacks the structured traceability appendix"},
		{name: "missing prepared", change: func(a map[string]any) { delete(a, "prepared") }, want: "differs from accepted"},
		{name: "missing validation", change: func(a map[string]any) { delete(a, "validation") }, want: "differs from accepted"},
		{name: "short fence", fence: "``", want: "invalid report appendix fence"},
		{name: "non-backtick fence", fence: "~~~", want: "invalid report appendix fence"},
		{name: "wrong language", raw: func(s string) string { return strings.Replace(s, "```json\n", "```text\n", 1) }, want: "invalid report appendix fence"},
		{name: "unclosed fence", raw: func(s string) string { return strings.TrimSuffix(s, "\n```\n") }, want: "not closed"},
		{name: "four-backtick mismatched close with backtick data", fence: "````", raw: func(s string) string { return strings.TrimSuffix(s, "\n````\n") + "\n```\n" }, want: "not closed"},
		{name: "long fence wrong prepared with backtick data", fence: "`````````", change: func(a map[string]any) {
			a["prepared"].(map[string]any)["requirements"].([]any)[0].(map[string]any)["statement"] = reportSpecial + "different"
		}, want: "differs from accepted"},
		{name: "invalid JSON", raw: func(s string) string { return strings.Replace(s, "\n{", "\n!{", 1) }, want: "invalid report appendix"},
		{name: "trailing text", raw: func(s string) string { return s + "not part of appendix" }, want: "not closed"},
		{name: "empty", raw: func(string) string { return "" }, want: "nonempty UTF-8"},
		{name: "invalid UTF-8", raw: func(string) string { return string([]byte{0xff}) }, want: "nonempty UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, p, _, _, _, v := checkFixture(t)
			fence := tc.fence
			if fence == "" {
				fence = "```"
			}
			if len(fence) >= 4 {
				p.Requirements[0].Statement += " embedded " + strings.Repeat("`", len(fence)-1)
			}
			appendix := resourceTestObject(t, map[string]any{"prepared": p, "validation": v})
			if tc.change != nil {
				tc.change(appendix)
			}
			raw := "# Report\n\n<!-- pwc-review-data -->\n" + fence + "json\n" + string(resourceTestJSON(t, appendix)) + "\n" + fence + "\n"
			if tc.raw != nil {
				raw = tc.raw(raw)
			}
			dir := t.TempDir()
			fixtureWrite(t, filepath.Join(dir, "report.md"), []byte(raw))
			ref := contract.Ref{Path: filepath.Join(dir, "contract.json")}
			files := map[string]checkFile{"report": {ID: "report", Kind: "artifact", Path: "report.md"}}
			checkError(t, checkReport(context.Background(), ref, files, p, v), tc.want)
		})
	}
}

func TestCheckProtocolSubprocess(t *testing.T) {
	if os.Getenv("PWC_CHECK_PROTOCOL") != "1" {
		return
	}
	for _, arg := range os.Args {
		if arg == "--version" {
			fmt.Println("0.84.3")
			os.Exit(0)
		}
	}
	if err := protocol.Serve(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

// The child speaks real Pi RPC. Only its candidate output is supplied at the
// external prompt barrier; Step, publication, and committed resolution are real.
func TestCheckCommittedResolverIntegration(t *testing.T) {
	c, prepared, _, _, _, _ := checkFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	bridge := filepath.Join(dir, "bridge")
	if err := os.Mkdir(bridge, 0700); err != nil {
		t.Fatal(err)
	}
	host, err := protocol.NewHost(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	var run *engine.Run
	var joined chan struct{}
	t.Cleanup(func() {
		if run != nil {
			run.Cancel(engine.OriginControllerUser)
		}
		if err := host.Close(); err != nil {
			t.Error(err)
		}
		if joined != nil {
			select {
			case <-joined:
			case <-time.After(10 * time.Second):
				t.Error("engine cleanup did not join")
			}
		}
	})
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	policy := engine.DefaultRunPolicy()
	policy.RunTimeout = 25 * time.Second
	policy.AttemptTimeout = 10 * time.Second
	policy.Runtime.HealthInterval = time.Hour
	pi, err := runtime.New(runtime.Options{Executable: executable, Args: []string{"-test.run=^TestCheckProtocolSubprocess$", "--"}, Env: []string{"PWC_CHECK_PROTOCOL=1", "PWC_ENGINE_CONTROL=" + host.Addr().String(), "GORACE=atexit_sleep_ms=0", "PI_CODING_AGENT_DIR=" + filepath.Join(dir, "agent")}, BridgeDir: bridge, Policy: policy.Runtime, Observe: func(ctx context.Context, o runtime.Observation) error { return run.Observe(ctx, o) }})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	type candidate struct {
		data  any
		files map[string][]byte
		kind  map[string]string
	}
	payloads := make(chan candidate, 1)
	definition := engine.Definition{Name: "check-integration", Version: "v1", Policy: policy, Execute: func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
		session, err := r.OpenSession(ctx, engine.RoleSpec{Name: "worker", Model: runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}})
		if err != nil {
			return engine.Result{}, err
		}
		step := func(key, schema string, data any, files map[string][]byte, kinds map[string]string) (contract.Ref, error) {
			payloads <- candidate{data, files, kinds}
			result, err := r.Root().Step(ctx, engine.StepSpec{Key: key, Session: session, Prompt: key, Output: contract.Spec{SchemaID: schema}})
			return result.Output, err
		}
		files := map[string][]byte{"context": []byte("Pinned context")}
		kinds := map[string]string{"context": "artifact"}
		for key, path := range c.Snapshots {
			raw, err := os.ReadFile(path)
			if err != nil {
				return engine.Result{}, err
			}
			files[key] = raw
			kinds[key] = "evidence"
		}
		contextRef, err := step("prepare", PrepareSchema, prepared, files, kinds)
		if err != nil {
			return engine.Result{}, err
		}
		p, err := checkPrepared(ctx, r, contextRef, c)
		if err != nil {
			return engine.Result{}, err
		}
		forged := contextRef
		forged.ManifestSHA256 = strings.Repeat("c", 64)
		if _, err := checkPrepared(ctx, r, forged, c); err == nil {
			return engine.Result{}, fmt.Errorf("forged Ref accepted")
		}
		if _, err := checkReviewed(ctx, r, contextRef, c, p, contextRef, "code"); err == nil {
			return engine.Result{}, fmt.Errorf("wrong schema accepted")
		}
		results := map[string]Reviewed{}
		rows := []ReviewerResult{}
		for _, role := range []string{"code", "scale", "simplicity"} {
			v := resourceTestData()[ReviewerSchema].(Reviewed)
			v.Role = role
			v.Context = contextRef
			v.Findings = []Finding{}
			if role != "code" {
				v.Requirements = []Assessment{}
			}
			ref, err := step(role, ReviewerSchema, v, nil, nil)
			if err != nil {
				return engine.Result{}, err
			}
			checked, err := checkReviewed(ctx, r, ref, c, p, contextRef, role)
			if err != nil {
				return engine.Result{}, err
			}
			results[role] = checked
			rows = append(rows, ReviewerResult{Role: role, Status: "succeeded", Ref: &ref, Detail: "published"})
		}
		v := Validated{Pin: pin(c), Context: contextRef, Reviewers: rows, Findings: []Finding{}, Dispositions: []Disposition{}, Requirements: results["code"].Requirements, Completeness: "complete", Conclusion: "no_confirmed_findings", Limitations: []string{}, ReportFile: "report"}
		final, err := step("validation", ValidationSchema, v, map[string][]byte{"report": reportTestArtifact(t, p, v)}, map[string]string{"report": "artifact"})
		if err != nil {
			return engine.Result{}, err
		}
		got, err := checkValidated(ctx, r, final, c, p, contextRef, rows, results)
		if err != nil {
			return engine.Result{}, err
		}
		if !reflect.DeepEqual(got, v) {
			return engine.Result{}, fmt.Errorf("validation round trip changed")
		}
		// A valid published envelope cannot turn an unreadable/invalid report into success.
		for _, bad := range []struct {
			name string
			raw  []byte
		}{{"empty", nil}, {"invalid-utf8", []byte{0xff}}} {
			ref, err := step(bad.name, ValidationSchema, v, map[string][]byte{"report": bad.raw}, map[string]string{"report": "artifact"})
			if err != nil {
				return engine.Result{}, err
			}
			if _, err := checkValidated(ctx, r, ref, c, p, contextRef, rows, results); err == nil || !strings.Contains(err.Error(), "nonempty UTF-8") {
				return engine.Result{}, fmt.Errorf("invalid report accepted: %v", err)
			}
		}
		tampered, err := step("tampered", PrepareSchema, prepared, files, kinds)
		if err != nil {
			return engine.Result{}, err
		}
		if err := os.WriteFile(filepath.Join(filepath.Dir(tampered.Path), "evidence", "metadata"), []byte("tampered snapshot"), 0600); err != nil {
			return engine.Result{}, err
		}
		if _, err := checkPrepared(ctx, r, tampered, c); err == nil || !strings.Contains(err.Error(), "digest/size mismatch") {
			return engine.Result{}, fmt.Errorf("published file tampering not rejected by resolver: %v", err)
		}
		return engine.Result{Outputs: map[string]contract.Ref{"final": final}}, nil
	}}
	run, err = engine.New(ctx, definition, engine.Input{Prompt: "verify", LaunchCWD: dir}, engine.Options{BaseDir: dir, Schemas: registry, Runtime: pi, PiVersion: "0.84.3"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan engine.Report, 1)
	joined = make(chan struct{})
	go func() { defer close(joined); done <- run.Execute() }()
	next := func() protocol.Event {
		t.Helper()
		select {
		case event, ok := <-host.Events():
			if !ok {
				t.Fatal("control host closed")
			}
			if event.Err != nil {
				t.Fatal(event.Err)
			}
			return event
		case report := <-done:
			t.Fatalf("engine ended before control: %+v", report)
		case <-ctx.Done():
			t.Fatalf("waiting for control: %v", context.Cause(ctx))
		}
		return protocol.Event{}
	}
	hello := next().Message
	if hello.Type != "hello" {
		t.Fatalf("hello: %+v", hello)
	}
	for n := 0; n < 8; n++ {
		event := next()
		message := event.Message
		if message.Type != "prompt" {
			t.Fatalf("unexpected control %+v", message)
		}
		payload := <-payloads
		var request contract.Request
		if err := protocol.ReadJSON(message.RequestPath, &request); err != nil {
			t.Fatal(err)
		}
		entries := []contract.FileEntry{}
		for id, raw := range payload.files {
			kind := payload.kind[id]
			prefix := "evidence"
			if kind == "artifact" {
				prefix = "artifacts"
			}
			path := filepath.Join(prefix, id)
			fixtureWrite(t, filepath.Join(filepath.Dir(message.CandidatePath), path), raw)
			entries = append(entries, contract.FileEntry{ID: id, Kind: kind, Path: path})
		}
		if err := protocol.WriteEnvelope(message.CandidatePath, request, payload.data, entries); err != nil {
			t.Fatal(err)
		}
		if err := event.Reply(protocol.Control{Type: "settle"}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case report := <-done:
		if report.Outcome != engine.Succeeded || len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 {
			t.Fatalf("integration: %+v", report)
		}
	case <-ctx.Done():
		t.Fatal("engine did not join")
	}
}
