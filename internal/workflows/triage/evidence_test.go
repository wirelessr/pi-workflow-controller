package triage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/testutil/protocol"
)

func TestCheckGapIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		gaps []Gap
		want string
	}{
		{"empty", nil, ""},
		{"unique", []Gap{{"a", "x"}, {"b", "x"}}, ""},
		{"duplicate id with different text", []Gap{{"a", "x"}, {"b", "y"}, {"a", "z"}}, `duplicate gap id "a" at gaps[0] and gaps[2]; ids must be unique within this contract`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckGapIDs("gaps", tc.gaps)
			if got := errText(err); got != tc.want {
				t.Fatalf("CheckGapIDs = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCheckEvidenceDiagnostics(t *testing.T) {
	ref := func(attempt string) contract.Ref {
		return contract.Ref{RunID: "run", AttemptID: attempt, Path: "/runs/" + attempt + "/contract.json", SchemaID: "triage.example.v1", SHA256: strings.Repeat("a", 64), ManifestSHA256: strings.Repeat("b", 64)}
	}
	ticket := ref("ticket")
	audit := ref("audit")
	audit.SchemaID = AuditSchema
	shared := []contract.FileEntry{{ID: "shared", Kind: "evidence", Path: "evidence/shared.txt"}}
	inputs := Inputs{
		Citable: map[contract.Ref][]contract.FileEntry{ticket: {{ID: "issue", Kind: "evidence", Path: "evidence/issue.json"}, {ID: "summary", Kind: "artifact", Path: "artifacts/summary.md"}},
			audit: {{ID: "findings", Kind: "evidence", Path: "evidence/findings.json"}}, ref("second"): shared, ref("first"): shared},
	}
	own := []contract.FileEntry{{ID: "identity-receipt", Kind: "evidence", Path: "evidence/receipt.json"}, {ID: "report", Kind: "artifact", Path: "artifacts/report.md"}}
	tampered := ticket
	tampered.SHA256 = strings.Repeat("c", 64)
	moved := ticket
	moved.RunID, moved.Path = "other-run", "/elsewhere"
	unknown := ref("other")
	for _, tc := range []struct {
		name string
		e    Evidence
		want string
	}{
		{"own file", Evidence{FileID: "identity-receipt"}, ""},
		{"citable input file", Evidence{Ref: &ticket, FileID: "issue"}, ""},
		{"own file with extension", Evidence{FileID: "identity-receipt.json"},
			`facts[0].evidence[1].file_id: got "identity-receipt.json", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id) (files[] declares "identity-receipt" at evidence/receipt.json: cite that bare id, never a path or an added extension)`},
		{"own file by its path", Evidence{FileID: "evidence/receipt.json"},
			`facts[0].evidence[1].file_id: got "evidence/receipt.json", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id) (files[] declares "identity-receipt" at evidence/receipt.json: cite that bare id, never a path or an added extension)`},
		{"no hint toward a non-evidence file", Evidence{FileID: "report.md"},
			`facts[0].evidence[1].file_id: got "report.md", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id)`},
		{"own file of wrong kind", Evidence{FileID: "report"},
			`facts[0].evidence[1].file_id: got "report", which this contract declares as kind=artifact; want a kind=evidence file`},
		{"null ref naming an input's file", Evidence{FileID: "issue"},
			`facts[0].evidence[1].file_id: got "issue", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id) (input attempt ticket (triage.example.v1) declares "issue": set ref to that input's ref copied byte-exact from the request)`},
		{"null ref naming a file of two inputs", Evidence{FileID: "shared"},
			`facts[0].evidence[1].file_id: got "shared", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id) (inputs attempt first (triage.example.v1), attempt second (triage.example.v1) declare "shared": set ref to the ref of the input you read, copied byte-exact from the request)`},
		{"no hint toward a judgment input", Evidence{FileID: "findings"},
			`facts[0].evidence[1].file_id: got "findings", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id)`},
		{"null ref naming an input's non-evidence file", Evidence{FileID: "summary"},
			`facts[0].evidence[1].file_id: got "summary", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id)`},
		{"input file missing", Evidence{Ref: &ticket, FileID: "comments"},
			`facts[0].evidence[1].file_id: got "comments", which input attempt ticket (triage.example.v1) does not declare in files[]; want a kind=evidence id from that input's files[]`},
		{"input file of wrong kind", Evidence{Ref: &ticket, FileID: "summary"},
			`facts[0].evidence[1].file_id: got "summary", which input attempt ticket (triage.example.v1) declares as kind=artifact; want a kind=evidence file`},
		{"mistyped citable ref", Evidence{Ref: &tampered, FileID: "issue"},
			"facts[0].evidence[1].ref: got attempt ticket with sha256 \"" + strings.Repeat("c", 64) + "\" (committed \"" + strings.Repeat("a", 64) + "\"); want the ref copied byte-exact from the request"},
		{"ref with several mistyped fields", Evidence{Ref: &moved, FileID: "issue"},
			`facts[0].evidence[1].ref: got attempt ticket with run_id "other-run" (committed "run"), path "/elsewhere" (committed "/runs/ticket/contract.json"); want the ref copied byte-exact from the request`},
		{"unknown ref", Evidence{Ref: &unknown, FileID: "issue"},
			`facts[0].evidence[1].ref: got attempt other (triage.example.v1), which is not an input of this Step; want one of this Step's citable inputs copied byte-exact from the request`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := errText(inputs.CheckEvidence("facts[0].evidence[1]", tc.e, own)); got != tc.want {
				t.Fatalf("CheckEvidence =\n%s\nwant\n%s", got, tc.want)
			}
		})
	}
}

// The common definitions are only reachable through a contract schema, so
// the test registers one that uses each of them and publishes through the
// real Store.
func TestCommonSchemaDefinitions(t *testing.T) {
	const schemaID = "triage.common-fixture.v1"
	fixture := contract.Resource{URI: "https://fixture.local/common-fixture.json", JSON: json.RawMessage(`{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type": "object", "additionalProperties": false,
		"required": ["gaps", "evidence"],
		"properties": {
			"gaps": {"type": "array", "items": {"$ref": "` + schemaURI + `common.v1.json#/$defs/gap"}},
			"evidence": {"type": "array", "items": {"$ref": "` + schemaURI + `common.v1.json#/$defs/evidence"}}
		}}`)}
	registry, err := contract.NewRegistry(append(Resources(), fixture), []contract.SchemaDefinition{{ID: schemaID, URI: fixture.URI}})
	if err != nil {
		t.Fatal(err)
	}
	store, err := contract.NewStore(registry, contract.Options{BaseDir: t.TempDir(), Prompt: "anonymous common schema validation"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	ref := map[string]any{"run_id": "r", "attempt_id": "a", "path": "/p", "schema_id": "s", "sha256": strings.Repeat("a", 64), "manifest_sha256": strings.Repeat("b", 64)}
	valid := func() map[string]any {
		return map[string]any{
			"gaps":     []any{map[string]any{"id": "attachment-1.download_v2", "text": "attachment download failed"}},
			"evidence": []any{map[string]any{"ref": ref, "file_id": "f"}, map[string]any{"ref": nil, "file_id": "g"}},
		}
	}
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
		ok     bool
	}{
		{"valid", func(map[string]any) {}, true},
		{"gap id at length limit", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["id"] = strings.Repeat("a", 128) }, true},
		{"gap id over length limit", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["id"] = strings.Repeat("a", 129) }, false},
		{"gap id with leading hyphen", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["id"] = "-a" }, false},
		{"gap id with space", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["id"] = "a b" }, false},
		{"gap without text", func(v map[string]any) { delete(v["gaps"].([]any)[0].(map[string]any), "text") }, false},
		{"gap with extra field", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["status"] = "open" }, false},
		{"evidence with extra field", func(v map[string]any) { v["evidence"].([]any)[0].(map[string]any)["path"] = "evidence/f" }, false},
		{"evidence without ref", func(v map[string]any) { delete(v["evidence"].([]any)[0].(map[string]any), "ref") }, false},
		{"locator with a pointer", func(v map[string]any) {
			v["evidence"].([]any)[0].(map[string]any)["locator"] = map[string]any{"pointer": "/a"}
		}, true},
		{"locator with a byte range", func(v map[string]any) {
			v["evidence"].([]any)[0].(map[string]any)["locator"] = map[string]any{"offset": 0, "length": 3}
		}, true},
		{"empty locator", func(v map[string]any) { v["evidence"].([]any)[0].(map[string]any)["locator"] = map[string]any{} }, false},
		{"locator with both forms", func(v map[string]any) {
			v["evidence"].([]any)[0].(map[string]any)["locator"] = map[string]any{"pointer": "", "offset": 0}
		}, false},
		{"locator with an offset only", func(v map[string]any) {
			v["evidence"].([]any)[0].(map[string]any)["locator"] = map[string]any{"offset": 0}
		}, false},
		{"locator with a zero length", func(v map[string]any) {
			v["evidence"].([]any)[0].(map[string]any)["locator"] = map[string]any{"offset": 0, "length": 0}
		}, false},
		{"gap id empty", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["id"] = "" }, false},
		{"gap text blank", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["text"] = " \n" }, false},
		{"gap without id", func(v map[string]any) { delete(v["gaps"].([]any)[0].(map[string]any), "id") }, false},
		{"evidence with empty file id", func(v map[string]any) { v["evidence"].([]any)[0].(map[string]any)["file_id"] = "" }, false},
		{"truncated ref digest", func(v map[string]any) {
			short := map[string]any{}
			for k, x := range ref {
				short[k] = x
			}
			short["sha256"] = "abc"
			v["evidence"].([]any)[0].(map[string]any)["ref"] = short
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := valid()
			tc.change(data)
			err := publishFixture(t, store, schemaID, data)
			var rejected *contract.Error
			if tc.ok && err != nil || !tc.ok && (!errors.As(err, &rejected) || rejected.Code != contract.ContractInvalid || rejected.Phase != "schema") {
				t.Fatalf("publish error = %v, want ok=%t or a data schema rejection", err, tc.ok)
			}
		})
	}
}

func publishFixture(t *testing.T, store *contract.Store, schema string, data any) error {
	t.Helper()
	id := contract.Identity{RunID: store.RunID(), InvocationID: contract.NewID(), AttemptID: contract.NewID(), DispatchToken: contract.NewID()}
	req := contract.Request{Identity: id, Prompt: "anonymous contract validation", Output: contract.OutputSpec{SchemaID: schema}}
	attempt, err := store.BeginAttempt(id, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteEnvelope(attempt.CandidatePath(), req, data, []contract.FileEntry{}); err != nil {
		t.Fatal(err)
	}
	staged, err := attempt.Stage(t.Context(), contract.Spec{SchemaID: schema})
	if err != nil {
		return err
	}
	defer func() {
		if err := staged.Discard(); err != nil {
			t.Error(err)
		}
	}()
	_, err = attempt.Publish(t.Context(), staged)
	return err
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestViolations(t *testing.T) {
	if violations(nil) != nil {
		t.Fatal("no violations must be nil")
	}
	var errs []error
	for i := range maxViolations + 3 {
		errs = append(errs, fmt.Errorf("v%d", i))
	}
	got := errText(violations(errs))
	if lines := strings.Split(got, "\n"); len(lines) != maxViolations+1 || lines[0] != "v0" || lines[maxViolations] != "and 3 more violations not listed" {
		t.Fatalf("violations = %q", got)
	}
	if !errors.Is(violations(append(errs[:1:1], context.Canceled)), context.Canceled) {
		t.Fatal("a joined cause must stay matchable")
	}
}
