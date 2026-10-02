package triagev2

import (
	"encoding/json"
	"errors"
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
	ticket, history := ref("ticket"), ref("history")
	inputs := Inputs{
		Citable:    map[contract.Ref]Input{ticket: {Label: "intake", Files: []contract.FileEntry{{ID: "issue", Kind: "evidence"}, {ID: "summary", Kind: "artifact"}}}},
		Background: map[contract.Ref]Input{history: {Label: "earlier round analysis", Files: []contract.FileEntry{{ID: "notes", Kind: "evidence"}}}},
	}
	own := []contract.FileEntry{{ID: "identity-receipt", Kind: "evidence"}, {ID: "report", Kind: "artifact"}}
	tampered := ticket
	tampered.SHA256 = strings.Repeat("c", 64)
	unknown := ref("other")
	for _, tc := range []struct {
		name string
		e    Evidence
		want string
	}{
		{"own file", Evidence{FileID: "identity-receipt"}, ""},
		{"citable input file", Evidence{Ref: &ticket, FileID: "issue"}, ""},
		{"own file with extension", Evidence{FileID: "identity-receipt.json"},
			`facts[0].evidence[1].file_id: got "identity-receipt.json", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id) (files[] declares "identity-receipt": cite the bare id, without a path or extension)`},
		{"own file of wrong kind", Evidence{FileID: "report"},
			`facts[0].evidence[1].file_id: got "report", which this contract declares as kind=artifact; want a kind=evidence file`},
		{"null ref naming an input's file", Evidence{FileID: "issue"},
			`facts[0].evidence[1].file_id: got "issue", which this contract does not declare in files[]; want a kind=evidence id from this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id)`},
		{"input file missing", Evidence{Ref: &ticket, FileID: "comments"},
			`facts[0].evidence[1].file_id: got "comments", which input attempt ticket (triage.example.v1) (intake) does not declare in files[]; want a kind=evidence id from that input's files[]`},
		{"input file of wrong kind", Evidence{Ref: &ticket, FileID: "summary"},
			`facts[0].evidence[1].file_id: got "summary", which input attempt ticket (triage.example.v1) (intake) declares as kind=artifact; want a kind=evidence file`},
		{"background input", Evidence{Ref: &history, FileID: "notes"},
			`facts[0].evidence[1].ref: got attempt history (triage.example.v1) (earlier round analysis), a background input; want an evidence owner from this Step's citable inputs: background inputs may be read but not cited`},
		{"mistyped citable ref", Evidence{Ref: &tampered, FileID: "issue"},
			`facts[0].evidence[1].ref: got attempt ticket with fields sha256 differing from the committed input (intake); want the ref copied byte-exact from the request`},
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
		"required": ["gaps", "dispositions", "evidence"],
		"properties": {
			"gaps": {"type": "array", "items": {"$ref": "` + schemaURI + `common.v1.json#/$defs/gap"}},
			"dispositions": {"type": "array", "items": {"$ref": "` + schemaURI + `common.v1.json#/$defs/gap_disposition"}},
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
			"gaps":         []any{map[string]any{"id": "attachment-1.download", "text": "attachment download failed"}},
			"dispositions": []any{map[string]any{"gap": map[string]any{"ref": ref, "gap_id": "G1"}, "disposition": "resolved", "reason": "fetched later", "basis": []any{map[string]any{"ref": nil, "file_id": "f"}}}},
			"evidence":     []any{map[string]any{"ref": ref, "file_id": "f"}},
		}
	}
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
		ok     bool
	}{
		{"valid", func(map[string]any) {}, true},
		{"gap id with space", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["id"] = "a b" }, false},
		{"gap id empty", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["id"] = "" }, false},
		{"gap text blank", func(v map[string]any) { v["gaps"].([]any)[0].(map[string]any)["text"] = " \n" }, false},
		{"gap without id", func(v map[string]any) { delete(v["gaps"].([]any)[0].(map[string]any), "id") }, false},
		{"unknown disposition", func(v map[string]any) {
			v["dispositions"].([]any)[0].(map[string]any)["disposition"] = "closed"
		}, false},
		{"disposition without basis", func(v map[string]any) {
			v["dispositions"].([]any)[0].(map[string]any)["basis"] = []any{}
		}, false},
		{"disposition of a gap without its ref", func(v map[string]any) {
			v["dispositions"].([]any)[0].(map[string]any)["gap"] = map[string]any{"ref": nil, "gap_id": "G1"}
		}, false},
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
			_, err := publishFixture(t, store, schemaID, data)
			var rejected *contract.Error
			if tc.ok && err != nil || !tc.ok && (!errors.As(err, &rejected) || rejected.Code != contract.ContractInvalid || rejected.Phase != "schema") {
				t.Fatalf("publish error = %v, want ok=%t or a data schema rejection", err, tc.ok)
			}
		})
	}
}

func publishFixture(t *testing.T, store *contract.Store, schema string, data any) (contract.Ref, error) {
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
		return contract.Ref{}, err
	}
	defer func() {
		if err := staged.Discard(); err != nil {
			t.Error(err)
		}
	}()
	return attempt.Publish(t.Context(), staged)
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
