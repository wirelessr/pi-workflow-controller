package review

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"pi-workflow-controller/internal/contract"
)

func resourceTestStore(t *testing.T) *contract.Store {
	t.Helper()
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	store, err := contract.NewStore(registry, contract.Options{BaseDir: t.TempDir(), Prompt: "review resource verification"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func resourceTestData() map[string]any {
	pin := Pin{URL: "https://github.com/owner/repo/pull/1", Repository: "owner/repo", Number: 1,
		BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), MergeBase: strings.Repeat("a", 40),
		DiffRange: strings.Repeat("a", 40) + ".." + strings.Repeat("b", 40), ContextID: "context-1"}
	ref := contract.Ref{RunID: strings.Repeat("a", 32), AttemptID: strings.Repeat("b", 32),
		Path: "/run/attempt/published/contract.json", SchemaID: PrepareSchema,
		SHA256: strings.Repeat("a", 64), ManifestSHA256: strings.Repeat("b", 64)}
	evidence := Evidence{Path: "service.go", Line: 2, EndLine: 4, Detail: "caller 傳遞錯誤的設定"}
	finding := Finding{ID: "code-001", Severity: "high", Title: "設定未傳遞", Location: evidence, Evidence: []Evidence{evidence}, Impact: "caller 行為違反需求"}
	assessments := []Assessment{{RequirementID: "req-001", Status: "not_satisfied", Evidence: []Evidence{evidence}, Reason: "設定沒有傳至 consumer"}}
	return map[string]any{
		PrepareSchema: Prepared{Pin: pin,
			Sources:       []Source{{ID: "src-001", Kind: "design", URL: "file:///snapshots/design.md", Status: "available", FileID: "source", Note: "保留原始 snapshot"}},
			Requirements:  []Requirement{{ID: "req-001", Kind: "requirement", Statement: "傳遞設定", SourceIDs: []string{"src-001"}}},
			OpenQuestions: []string{}, ContextFile: "context"},
		ReviewerSchema: Reviewed{Pin: pin, Role: "code", Context: ref, Coverage: []string{"追蹤 service.go 的 caller 與設定流"},
			Limitations: []string{}, Findings: []Finding{finding}, Requirements: assessments},
		ValidationSchema: Validated{Pin: pin, Context: ref,
			Reviewers: []ReviewerResult{{Role: "code", Status: "succeeded", Ref: &ref, Detail: "published"},
				{Role: "scale", Status: "failed", Ref: nil, Detail: "timeout"}, {Role: "simplicity", Status: "missing", Ref: nil, Detail: "not dispatched"}},
			Findings: []Finding{finding}, Dispositions: []Disposition{{FindingID: "code-001", Action: "confirmed", TargetID: "code-001", Reason: "head caller 證實"}},
			Requirements: assessments, Completeness: "incomplete", Conclusion: "findings", Limitations: []string{"scale failed; simplicity missing"}, ReportFile: "report"},
	}
}

func resourceTestJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func resourceTestObject(t *testing.T, value any) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal(resourceTestJSON(t, value), &object); err != nil {
		t.Fatal(err)
	}
	return object
}

func resourceTestCandidate(t *testing.T, store *contract.Store, schema string, data any) (*contract.Attempt, []byte) {
	t.Helper()
	id := contract.Identity{RunID: store.RunID(), InvocationID: contract.NewID(), AttemptID: contract.NewID(), DispatchToken: contract.NewID()}
	attempt, err := store.BeginAttempt(id, contract.Request{Identity: id, Prompt: "verify pinned review", Output: contract.OutputSpec{SchemaID: schema}})
	if err != nil {
		t.Fatal(err)
	}
	files := []map[string]string{
		{"id": "source", "kind": "evidence", "path": "evidence/source.md"},
		{"id": "context", "kind": "artifact", "path": "artifacts/context.md"},
		{"id": "report", "kind": "artifact", "path": "artifacts/report.md"},
	}
	for _, entry := range files {
		raw := []byte("固定證據 " + entry["id"])
		if schema == ValidationSchema && entry["id"] == "report" {
			raw = reportTestArtifact(t, resourceTestData()[PrepareSchema].(Prepared), data)
		}
		fixtureWrite(t, filepath.Join(attempt.Dir(), entry["path"]), raw)
	}
	raw := resourceTestJSON(t, map[string]any{
		"meta": map[string]any{"version": 1, "run_id": id.RunID, "invocation_id": id.InvocationID, "attempt_id": id.AttemptID, "dispatch_token": id.DispatchToken, "schema_id": schema},
		"data": data, "files": files,
	})
	fixtureWrite(t, attempt.CandidatePath(), raw)
	return attempt, raw
}

func TestReviewResourcesStoreRoundTrip(t *testing.T) {
	store := resourceTestStore(t)
	for schema, data := range resourceTestData() {
		t.Run(schema, func(t *testing.T) {
			attempt, raw := resourceTestCandidate(t, store, schema, data)
			var request contract.Request
			requestBytes, err := os.ReadFile(attempt.RequestPath())
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(requestBytes, &request); err != nil {
				t.Fatal(err)
			}
			for _, resource := range Resources() {
				location, ok := request.Output.Resources[resource.URI]
				if !ok || !filepath.IsAbs(location.Path) {
					t.Fatalf("resource not published: %s", resource.URI)
				}
				got, err := os.ReadFile(location.Path)
				sum := sha256.Sum256(resource.JSON)
				if err != nil || !bytes.Equal(got, resource.JSON) || location.SHA256 != hex.EncodeToString(sum[:]) {
					t.Fatalf("resource bytes/digest differ: %s: %v", resource.URI, err)
				}
			}
			for _, definition := range Schemas() {
				if definition.ID == schema && request.Output.Schema != request.Output.Resources[definition.URI] {
					t.Fatal("wrong output schema selection")
				}
			}
			staged, err := attempt.Stage(context.Background(), contract.Spec{SchemaID: schema})
			if err != nil {
				t.Fatal(err)
			}
			ref, err := attempt.Publish(context.Background(), staged)
			if err != nil {
				t.Fatal(err)
			}
			got, err := store.Read(context.Background(), ref)
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatalf("published round trip: %v", err)
			}
			copied, err := os.ReadFile(filepath.Join(filepath.Dir(ref.Path), "evidence/source.md"))
			if err != nil || string(copied) != "固定證據 source" {
				t.Fatalf("snapshot not published: %q: %v", copied, err)
			}
		})
	}
}

func TestReviewSchemasValidateBusinessValues(t *testing.T) {
	store := resourceTestStore(t)
	cases := []struct {
		name, schema string
		change       func(map[string]any)
		valid        bool
	}{
		{"available source", PrepareSchema, func(map[string]any) {}, true},
		{"fetched source rejected", PrepareSchema, func(v map[string]any) { v["sources"].([]any)[0].(map[string]any)["status"] = "fetched" }, false},
		{"conflict source", PrepareSchema, func(v map[string]any) { v["sources"].([]any)[0].(map[string]any)["status"] = "conflict" }, true},
		{"conflict source without URL", PrepareSchema, func(v map[string]any) {
			s := v["sources"].([]any)[0].(map[string]any)
			s["status"], s["url"] = "conflict", ""
		}, false},
		{"conflict source without snapshot ID", PrepareSchema, func(v map[string]any) {
			s := v["sources"].([]any)[0].(map[string]any)
			s["status"], s["file_id"] = "conflict", ""
		}, false},
		{"missing source without URL", PrepareSchema, func(v map[string]any) {
			s := v["sources"].([]any)[0].(map[string]any)
			s["status"], s["file_id"], s["url"], s["note"] = "missing", "", "", "沒有 linked design"
		}, true},
		{"available source without URL", PrepareSchema, func(v map[string]any) { v["sources"].([]any)[0].(map[string]any)["url"] = "" }, false},
		{"available source without snapshot ID", PrepareSchema, func(v map[string]any) { v["sources"].([]any)[0].(map[string]any)["file_id"] = "" }, false},
		{"non-URL source identifier", PrepareSchema, func(v map[string]any) { v["sources"].([]any)[0].(map[string]any)["url"] = "TASK-123 design reference" }, true},
		{"constraint", PrepareSchema, func(v map[string]any) { v["requirements"].([]any)[0].(map[string]any)["kind"] = "constraint" }, true},
		{"decision", PrepareSchema, func(v map[string]any) { v["requirements"].([]any)[0].(map[string]any)["kind"] = "decision" }, true},
		{"wrong requirement kind", PrepareSchema, func(v map[string]any) { v["requirements"].([]any)[0].(map[string]any)["kind"] = "claim" }, false},
		{"missing requirement ID", PrepareSchema, func(v map[string]any) { v["requirements"].([]any)[0].(map[string]any)["id"] = "" }, false},
		{"oversize source ID", PrepareSchema, func(v map[string]any) { v["sources"].([]any)[0].(map[string]any)["id"] = strings.Repeat("x", 129) }, false},
		{"invalid head SHA", PrepareSchema, func(v map[string]any) { v["pin"].(map[string]any)["head_sha"] = strings.Repeat("z", 40) }, false},
		{"short base SHA", PrepareSchema, func(v map[string]any) { v["pin"].(map[string]any)["base_sha"] = "abc123" }, false},
		{"mutable diff range", PrepareSchema, func(v map[string]any) { v["pin"].(map[string]any)["diff_range"] = "main..feature" }, false},
		{"zero PR", PrepareSchema, func(v map[string]any) { v["pin"].(map[string]any)["number"] = 0 }, false},
		{"empty coverage", ReviewerSchema, func(v map[string]any) { v["coverage"] = []any{} }, false},
		{"fourth role", ReviewerSchema, func(v map[string]any) { v["role"] = "local" }, false},
		{"scale", ReviewerSchema, func(v map[string]any) {
			v["role"] = "scale"
			v["findings"].([]any)[0].(map[string]any)["id"] = "scale-001"
		}, true},
		{"simplicity", ReviewerSchema, func(v map[string]any) {
			v["role"] = "simplicity"
			v["findings"].([]any)[0].(map[string]any)["id"] = "simplicity-001"
		}, true},
		{"unprefixed finding", ReviewerSchema, func(v map[string]any) { v["findings"].([]any)[0].(map[string]any)["id"] = "001" }, false},
		{"empty head evidence", ReviewerSchema, func(v map[string]any) { v["findings"].([]any)[0].(map[string]any)["evidence"] = []any{} }, false},
		{"zero line", ReviewerSchema, func(v map[string]any) {
			v["findings"].([]any)[0].(map[string]any)["location"].(map[string]any)["line"] = 0
		}, false},
		{"bad Ref digest", ReviewerSchema, func(v map[string]any) { v["context"].(map[string]any)["manifest_sha256"] = "wrong" }, false},
		{"null context", ReviewerSchema, func(v map[string]any) { v["context"] = nil }, false},
		{"satisfied", ReviewerSchema, func(v map[string]any) { v["requirements"].([]any)[0].(map[string]any)["status"] = "satisfied" }, true},
		{"unconfirmed", ReviewerSchema, func(v map[string]any) {
			a := v["requirements"].([]any)[0].(map[string]any)
			a["status"], a["evidence"] = "unconfirmed", []any{}
		}, true},
		{"confirmed assessment without evidence", ReviewerSchema, func(v map[string]any) { v["requirements"].([]any)[0].(map[string]any)["evidence"] = []any{} }, false},
		{"unknown assessment spelling", ReviewerSchema, func(v map[string]any) { v["requirements"].([]any)[0].(map[string]any)["status"] = "unknown" }, false},
		{"too few reviewers", ValidationSchema, func(v map[string]any) { v["reviewers"] = v["reviewers"].([]any)[:2] }, false},
		{"too many reviewers", ValidationSchema, func(v map[string]any) { a := v["reviewers"].([]any); v["reviewers"] = append(a, a[0]) }, false},
		{"wrong reviewer status", ValidationSchema, func(v map[string]any) { v["reviewers"].([]any)[0].(map[string]any)["status"] = "success" }, false},
		{"non-null malformed reviewer Ref", ValidationSchema, func(v map[string]any) { v["reviewers"].([]any)[1].(map[string]any)["ref"] = map[string]any{} }, false},
		{"merged", ValidationSchema, func(v map[string]any) { v["dispositions"].([]any)[0].(map[string]any)["action"] = "merged" }, true},
		{"excluded", ValidationSchema, func(v map[string]any) {
			d := v["dispositions"].([]any)[0].(map[string]any)
			d["action"], d["target_id"] = "excluded", ""
		}, true},
		{"unconfirmed disposition", ValidationSchema, func(v map[string]any) {
			d := v["dispositions"].([]any)[0].(map[string]any)
			d["action"], d["target_id"] = "unconfirmed", ""
		}, true},
		{"excluded with target", ValidationSchema, func(v map[string]any) { v["dispositions"].([]any)[0].(map[string]any)["action"] = "excluded" }, false},
		{"confirmed without target", ValidationSchema, func(v map[string]any) { v["dispositions"].([]any)[0].(map[string]any)["target_id"] = "" }, false},
		{"wrong disposition", ValidationSchema, func(v map[string]any) { v["dispositions"].([]any)[0].(map[string]any)["action"] = "dropped" }, false},
		{"limited undetermined", ValidationSchema, func(v map[string]any) { v["completeness"], v["conclusion"] = "limited", "undetermined" }, true},
		{"complete no findings", ValidationSchema, func(v map[string]any) { v["completeness"], v["conclusion"] = "complete", "no_confirmed_findings" }, true},
		{"unknown completeness", ValidationSchema, func(v map[string]any) { v["completeness"] = "partial" }, false},
		{"approval is not conclusion", ValidationSchema, func(v map[string]any) { v["conclusion"] = "approved" }, false},
		{"too many findings", ReviewerSchema, func(v map[string]any) {
			item := v["findings"].([]any)[0]
			items := make([]any, 33)
			for i := range items {
				items[i] = item
			}
			v["findings"] = items
		}, false},
		{"oversize text", ReviewerSchema, func(v map[string]any) { v["coverage"] = []string{strings.Repeat("x", 2049)} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := resourceTestObject(t, resourceTestData()[tc.schema])
			tc.change(data)
			attempt, _ := resourceTestCandidate(t, store, tc.schema, data)
			staged, err := attempt.Stage(context.Background(), contract.Spec{SchemaID: tc.schema})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v: %v", tc.valid, err)
			}
			if tc.valid {
				if err := staged.Discard(); err != nil {
					t.Fatal(err)
				}
			} else {
				var typed *contract.Error
				if !errors.As(err, &typed) || typed.Code != contract.ContractInvalid || typed.Phase != "schema" {
					t.Fatalf("not a schema rejection: %v", err)
				}
			}
		})
	}
}

func TestReviewSchemasRequireEveryTypedFieldAndRejectExtras(t *testing.T) {
	store := resourceTestStore(t)
	// Recursively visit real typed fixtures, including nested Evidence and Ref,
	// so schema omissions cannot be hidden by testing only the root constants.
	for schema, fixture := range resourceTestData() {
		root := resourceTestObject(t, fixture)
		var visit func(any, string)
		visit = func(value any, path string) {
			switch value := value.(type) {
			case map[string]any:
				check := func(name string) {
					t.Run(schema+path+name, func(t *testing.T) {
						attempt, _ := resourceTestCandidate(t, store, schema, root)
						_, err := attempt.Stage(context.Background(), contract.Spec{SchemaID: schema})
						var typed *contract.Error
						if !errors.As(err, &typed) || typed.Code != contract.ContractInvalid || typed.Phase != "schema" {
							t.Fatalf("invalid object escaped schema: %v", err)
						}
					})
				}
				value["unexpected"] = true
				check("/extra")
				delete(value, "unexpected")
				for key, child := range value {
					delete(value, key)
					check("/missing-" + key)
					value[key] = child
					visit(child, path+"/"+key)
				}
			case []any:
				for index, child := range value {
					visit(child, fmt.Sprintf("%s/%d", path, index))
				}
			}
		}
		visit(root, "")
	}
}

func TestReviewCandidateAggregateByteLimit(t *testing.T) {
	store := resourceTestStore(t)
	attempt, raw := resourceTestCandidate(t, store, PrepareSchema, resourceTestData()[PrepareSchema])
	raw = append(raw, bytes.Repeat([]byte(" "), (1<<20)-len(raw)+1)...)
	fixtureWrite(t, attempt.CandidatePath(), raw)
	staged, err := attempt.Stage(context.Background(), contract.Spec{SchemaID: PrepareSchema})
	var typed *contract.Error
	if staged != nil || !errors.As(err, &typed) || typed.Code != contract.LimitExceeded {
		t.Fatalf("aggregate byte limit not enforced: %v", err)
	}
}

func TestExtractReviewSkillsAwayFromSource(t *testing.T) {
	// Reexecute the test binary without the source cwd or global skill settings.
	// All content and references must therefore be available from go:embed.
	if os.Getenv("PWC_REVIEW_RESOURCE_HELPER") == "1" {
		resourceTestExtractedTree(t)
		return
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestExtractReviewSkillsAwayFromSource$", "-test.v")
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "PWC_REVIEW_RESOURCE_HELPER=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("standalone extraction: %v\n%s", err, output)
	}
}

func resourceTestExtractedTree(t *testing.T) {
	t.Helper()
	if err := os.Mkdir("run", 0700); err != nil {
		t.Fatal(err)
	}
	paths, err := ExtractSkills("run")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 5 {
		t.Fatalf("unexpected skill mapping: %v", paths)
	}
	root, err := filepath.Abs(filepath.Join("run", "review-skills"))
	if err != nil {
		t.Fatal(err)
	}
	// Change cwd again before resolving references, not just before extraction.
	t.Chdir(t.TempDir())
	link := regexp.MustCompile(`\]\(((?:references/|\.\./common/)[^)]+)\)`)
	for _, role := range []string{"prepare", "code", "scale", "simplicity", "validate"} {
		path := paths[role]
		if !filepath.IsAbs(path) || path != filepath.Join(root, "pwc-review-"+role, "SKILL.md") {
			t.Fatalf("wrong skill path for %s: %s", role, path)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(raw, []byte("---\nname: pwc-review-"+role+"\ndescription: ")) {
			t.Fatalf("invalid skill frontmatter: %s", path)
		}
		links := link.FindAllSubmatch(raw, -1)
		if len(links) == 0 {
			t.Fatalf("skill has no relative references: %s", path)
		}
		for _, match := range links {
			reference, err := os.ReadFile(filepath.Join(filepath.Dir(path), string(match[1])))
			if err != nil || len(reference) == 0 {
				t.Fatalf("relative reference unreadable from another cwd: %s: %v", match[1], err)
			}
		}
	}
	var expectedFiles int
	err = fs.WalkDir(reviewResources, "skills", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := strings.TrimPrefix(path, "skills")
		copied := filepath.Join(root, rel)
		info, err := os.Lstat(copied)
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if !info.IsDir() || info.Mode().Perm() != 0700 {
				t.Fatalf("directory mode: %s: %v", copied, info.Mode())
			}
			return nil
		}
		expectedFiles++
		want, err := reviewResources.ReadFile(path)
		if err != nil {
			return err
		}
		got, err := os.ReadFile(copied)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || !bytes.Equal(got, want) {
			t.Fatalf("copied bytes/mode: %s", copied)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	actualFiles := 0
	if err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			actualFiles++
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if actualFiles != expectedFiles {
		t.Fatalf("unexpected extracted files: got %d, want %d", actualFiles, expectedFiles)
	}
	if got, err := ExtractSkills(filepath.Dir(root)); err == nil || got != nil {
		t.Fatalf("second extraction overwrote files: %v %v", got, err)
	}
}

func TestExtractReviewSkillsNeverOverwrites(t *testing.T) {
	for _, kind := range []string{"file", "directory", "symlink", "populated"} {
		t.Run(kind, func(t *testing.T) {
			run := t.TempDir()
			root := filepath.Join(run, "review-skills")
			sentinel := root
			switch kind {
			case "file":
				fixtureWrite(t, root, []byte("keep"))
			case "directory", "populated":
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
				if kind == "populated" {
					sentinel = filepath.Join(root, "pwc-review-code", "SKILL.md")
					if err := os.Mkdir(filepath.Dir(sentinel), 0700); err != nil {
						t.Fatal(err)
					}
					fixtureWrite(t, sentinel, []byte("keep"))
				}
			case "symlink":
				outside := t.TempDir()
				sentinel = filepath.Join(outside, "sentinel")
				fixtureWrite(t, sentinel, []byte("keep"))
				if err := os.Symlink(outside, root); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(root)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ExtractSkills(run)
			if !errors.Is(err, os.ErrExist) || got != nil {
				t.Fatalf("existing content accepted: %v %v", got, err)
			}
			after, err := os.Lstat(root)
			if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatalf("existing inode/mode changed: %v", err)
			}
			if kind == "directory" {
				entries, err := os.ReadDir(root)
				if err != nil || len(entries) != 0 {
					t.Fatalf("existing directory populated: %v %v", entries, err)
				}
			} else {
				raw, err := os.ReadFile(sentinel)
				if err != nil || string(raw) != "keep" {
					t.Fatalf("existing file changed: %q %v", raw, err)
				}
			}
		})
	}
	for _, path := range []string{"", filepath.Join(t.TempDir(), "absent")} {
		if got, err := ExtractSkills(path); err == nil || got != nil {
			t.Fatalf("invalid run directory accepted: %q: %v %v", path, got, err)
		}
	}
}

func TestReviewResourceCallsOwnTheirBytes(t *testing.T) {
	first := Resources()
	original := Resources()
	for i := range first {
		for j := range first[i].JSON {
			first[i].JSON[j] = ' '
		}
		first[i].URI = "changed"
	}
	if !reflect.DeepEqual(Resources(), original) {
		t.Fatal("caller mutation changed embedded schema resources")
	}
	if _, err := contract.NewRegistry(Resources(), Schemas()); err != nil {
		t.Fatal(err)
	}
}
