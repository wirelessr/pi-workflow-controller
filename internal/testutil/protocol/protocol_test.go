package protocol_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/testutil/protocol"
)

func TestParseDispatch(t *testing.T) {
	dir := t.TempDir()
	requestPath := filepath.Join(dir, "request JSON.json")
	candidatePath := filepath.Join(dir, "candidate JSON.json")
	request := contract.Request{
		Identity: contract.Identity{DispatchToken: "dispatch-token"},
		Prompt:   `繁體 "quoted" $(touch forbidden)`,
		// Parsing must not read schema resources or create the candidate.
		Output: contract.OutputSpec{Schema: contract.SchemaResource{Path: filepath.Join(dir, "absent-schema.json")}},
	}
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(requestPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	invalidPath := filepath.Join(dir, "invalid.json")
	if err := os.WriteFile(invalidPath, []byte(`{`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, requestPath, candidatePath, token, wantError string
	}{
		{"valid", requestPath, candidatePath, request.Identity.DispatchToken, ""},
		{"missing-request-path", "", candidatePath, request.Identity.DispatchToken, "missing absolute request/candidate paths"},
		{"relative-request-path", "request.json", candidatePath, request.Identity.DispatchToken, "missing absolute request/candidate paths"},
		{"missing-candidate-path", requestPath, "", request.Identity.DispatchToken, "missing absolute request/candidate paths"},
		{"relative-candidate-path", requestPath, "candidate.json", request.Identity.DispatchToken, "missing absolute request/candidate paths"},
		{"missing-request-file", filepath.Join(dir, "missing.json"), candidatePath, request.Identity.DispatchToken, "missing.json"},
		{"invalid-request-json", invalidPath, candidatePath, request.Identity.DispatchToken, "unexpected end of JSON input"},
		{"token-mismatch", requestPath, candidatePath, "another-token", "dispatch token differs from request identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			message := fmt.Sprintf("Controller dispatch %s\nRead request JSON: %s\nAfter the task finishes, read output.schema.path. Write the complete envelope with exactly request.identity and output.schema_id to: %s\n", tc.token, tc.requestPath, tc.candidatePath)
			gotRequestPath, gotCandidatePath, gotRequest, err := protocol.ParseDispatch(message)
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("ParseDispatch error = %v, want %q", err, tc.wantError)
				}
			} else if err != nil || gotRequestPath != requestPath || gotCandidatePath != candidatePath || !reflect.DeepEqual(gotRequest, request) {
				t.Fatalf("ParseDispatch = %q, %q, %+v, %v", gotRequestPath, gotCandidatePath, gotRequest, err)
			}
			if _, err := os.Stat(candidatePath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("parsing created candidate: %v", err)
			}
		})
	}
}
