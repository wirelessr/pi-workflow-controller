package triagev2

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/testutil/protocol"
)

func intakeStore(t *testing.T) *contract.Store {
	t.Helper()
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	store, err := contract.NewStore(registry, contract.Options{BaseDir: t.TempDir(), Prompt: "anonymous intake validation"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func publishWithFiles(t *testing.T, store *contract.Store, schema string, data any, files map[string][]byte) (contract.Ref, contract.Publication[Intake], error) {
	t.Helper()
	id := contract.Identity{RunID: store.RunID(), InvocationID: contract.NewID(), AttemptID: contract.NewID(), DispatchToken: contract.NewID()}
	req := contract.Request{Identity: id, Prompt: "anonymous", Output: contract.OutputSpec{SchemaID: schema}}
	attempt, err := store.BeginAttempt(id, req)
	if err != nil {
		t.Fatal(err)
	}
	call := agentCall{Request: req, Candidate: attempt.CandidatePath()}
	if err := protocol.WriteEnvelope(call.Candidate, req, data, call.writeFiles(t, files)); err != nil {
		t.Fatal(err)
	}
	staged, err := attempt.Stage(context.Background(), contract.Spec{SchemaID: schema})
	if err != nil {
		return contract.Ref{}, contract.Publication[Intake]{}, err
	}
	ref, err := attempt.Publish(context.Background(), staged)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := store.Read(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	p, err := contract.DecodePublication[Intake](raw)
	if err != nil {
		t.Fatal(err)
	}
	return ref, p, nil
}

func TestCheckIntakePublication(t *testing.T) {
	setIssue := func(files map[string][]byte, change func(fields map[string]any)) {
		var issue map[string]any
		_ = json.Unmarshal(files["issue"], &issue)
		change(issue["fields"].(map[string]any))
		files["issue"], _ = json.Marshal(issue)
	}
	for _, tc := range []struct {
		name   string
		change func(v *Intake, files map[string][]byte)
		want   string
	}{
		{"complete", func(*Intake, map[string][]byte) {}, ""},
		{"incomplete with a gap", func(v *Intake, files map[string][]byte) {
			v.Fields = Source{Status: "missing", Reason: "field metadata access denied"}
			v.Complete, v.Gaps = false, []Gap{{ID: "fields", Text: "field metadata access denied"}}
		}, ""},
		{"zero comments is complete", func(v *Intake, files map[string][]byte) {
			setIssue(files, func(f map[string]any) { f["comment"] = map[string]any{"total": 0, "comments": []any{}} })
			files["page-0"] = []byte(`{"startAt":0,"total":0,"comments":[]}`)
			delete(files, "page-1")
			v.Comments = v.Comments[:1]
		}, ""},
		{"wrong ticket", func(v *Intake, _ map[string][]byte) { v.Ticket = "CASE-99" }, "ticket/url"},
		{"api url", func(v *Intake, _ map[string][]byte) { v.URL = "https://jira.example.invalid/rest/api/2/issue/CASE-17" }, ""},
		{"fetched_at not utc", func(v *Intake, _ map[string][]byte) { v.FetchedAt = "2025-01-03T00:00:00+01:00" }, ""},
		{"available source without its file", func(v *Intake, _ map[string][]byte) { v.Issue.FileID = "absent" }, "issue.file_id"},
		{"unavailable source without a reason", func(v *Intake, _ map[string][]byte) {
			v.Fields = Source{Status: "missing"}
			v.Complete, v.Gaps = false, []Gap{{ID: "fields", Text: "x"}}
		}, "fields.reason"},
		{"raw issue of another ticket", func(_ *Intake, files map[string][]byte) { files["issue"] = []byte(`{"key":"CASE-99","fields":{}}`) }, "raw issue key"},
		{"raw issue without all fields", func(_ *Intake, files map[string][]byte) {
			setIssue(files, func(f map[string]any) { delete(f, "attachment") })
		}, "fields.attachment"},
		{"empty field metadata", func(_ *Intake, files map[string][]byte) { files["fields"] = []byte(`[]`) }, "empty field metadata"},
		{"field metadata in a wrapper object", func(_ *Intake, files map[string][]byte) { files["fields"] = []byte(`{"fields":[]}`) }, "not a JSON array"},
		{"duplicate comment", func(_ *Intake, files map[string][]byte) {
			files["page-1"] = []byte(`{"startAt":1,"total":2,"comments":[{"id":"c1","body":"again"}]}`)
		}, "duplicate or has no body"},
		{"null comment body", func(_ *Intake, files map[string][]byte) {
			files["page-1"] = []byte(`{"startAt":1,"total":2,"comments":[{"id":"c2","body":null}]}`)
		}, "duplicate or has no body"},
		{"page start differs from the raw page", func(_ *Intake, files map[string][]byte) {
			files["page-1"] = []byte(`{"startAt":0,"total":2,"comments":[{"id":"c2","body":"x"}]}`)
		}, "comments[1]"},
		{"missing comment page claimed complete", func(v *Intake, files map[string][]byte) {
			delete(files, "page-1")
			v.Comments = v.Comments[:1]
		}, "complete/gaps"},
		{"complete with a gap", func(v *Intake, _ map[string][]byte) { v.Gaps = []Gap{{ID: "x", Text: "nothing missing"}} }, "complete/gaps"},
		{"duplicate gap ids", func(v *Intake, _ map[string][]byte) {
			v.Fields = Source{Status: "missing", Reason: "denied"}
			v.Complete, v.Gaps = false, []Gap{{ID: "x", Text: "a"}, {ID: "x", Text: "b"}}
		}, "duplicate gap id"},
		{"missing linked issue", func(v *Intake, _ map[string][]byte) { v.Linked = []LinkedIssue{} }, "formal links"},
		{"unexpected linked issue", func(v *Intake, _ map[string][]byte) { v.Linked[0].Key = "CASE-19" }, "linked[0].key"},
		{"linked snapshot of another issue", func(_ *Intake, files map[string][]byte) {
			files["linked"] = []byte(`{"key":"CASE-19","fields":{"a":1}}`)
		}, "snapshot is of"},
		{"missing attachment", func(v *Intake, _ map[string][]byte) { v.Attachments = []Attachment{} }, "attachments: got 0"},
		{"truncated attachment", func(_ *Intake, files map[string][]byte) { files["bundle"] = []byte("cut") }, "bytes; the raw issue says"},
		{"analysis without content", func(v *Intake, _ map[string][]byte) {
			v.Attachments[0].Content = Source{Status: "missing", Reason: "download failed"}
			v.Complete, v.Gaps = false, []Gap{{ID: "a1", Text: "download failed"}}
		}, "needs available content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, files := intakeFiles()
			tc.change(&v, files)
			ref, p, err := publishWithFiles(t, intakeStore(t), IntakeSchema, v, files)
			if err == nil {
				_, err = checkIntakePublication(context.Background(), ref, p, "CASE-17")
			}
			if tc.want == "" && tc.name != "api url" && tc.name != "fetched_at not utc" {
				if err != nil {
					t.Fatalf("valid intake rejected: %v", err)
				}
				return
			}
			if err == nil || tc.want != "" && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCheckAnchor(t *testing.T) {
	ev := Evidence{FileID: "e"}
	paired := Evidence{FileID: "p"}
	millis := int64(1735770600000)
	ok := func(string, Evidence) error { return nil }
	for _, tc := range []struct {
		name string
		a    TimeAnchor
		want string
	}{
		{"rfc3339 with offset", TimeAnchor{Original: "2025-01-02T00:30:00+02:00", Format: "rfc3339", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 7200, Evidence: ev}, ""},
		{"rfc3339 offset mismatch", TimeAnchor{Original: "2025-01-02T00:30:00+02:00", Format: "rfc3339", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 0, Evidence: ev}, "offset_seconds"},
		{"wrong utc", TimeAnchor{Original: "2025-01-02T00:30:00+02:00", Format: "rfc3339", SourceTZ: "+02:00", UTC: "2025-01-02T00:30:00Z", OffsetSeconds: 7200, Evidence: ev}, "converts to 2025-01-01T22:30:00Z"},
		{"utc without Z", TimeAnchor{Original: "2025-01-02T00:30:00+02:00", Format: "rfc3339", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00+00:00", OffsetSeconds: 7200, Evidence: ev}, "explicit UTC"},
		{"epoch millis", TimeAnchor{Original: "1735770600000", Format: "epoch-millis", SourceTZ: "UTC", UTC: "2025-01-01T22:30:00Z", Evidence: ev}, ""},
		{"epoch with a zone", TimeAnchor{Original: "1735770600", Format: "epoch-seconds", SourceTZ: "Asia/Taipei", UTC: "2025-01-01T22:30:00Z", Evidence: ev}, "epoch value is UTC"},
		{"local paired", TimeAnchor{Original: "2025-01-02T00:30:00", Format: "local-paired", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 7200, Evidence: ev, PairedEpochMillis: &millis, PairedEvidence: &paired}, ""},
		{"local without a pair", TimeAnchor{Original: "2025-01-02T00:30:00", Format: "local-paired", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 7200, Evidence: ev}, "same event"},
		{"local paired with itself", TimeAnchor{Original: "2025-01-02T00:30:00", Format: "local-paired", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 7200, Evidence: ev, PairedEpochMillis: &millis, PairedEvidence: &ev}, "its own absolute evidence"},
		{"local with a wrong offset", TimeAnchor{Original: "2025-01-02T00:30:00", Format: "local-paired", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 3600, Evidence: ev, PairedEpochMillis: &millis, PairedEvidence: &paired}, "is not the paired epoch"},
		{"pair on a non-local anchor", TimeAnchor{Original: "1735770600000", Format: "epoch-millis", SourceTZ: "UTC", UTC: "2025-01-01T22:30:00Z", Evidence: ev, PairedEpochMillis: &millis}, "must be null"},
		{"unparseable original", TimeAnchor{Original: "yesterday", Format: "rfc3339", SourceTZ: "UTC", UTC: "2025-01-01T22:30:00Z", Evidence: ev}, "does not parse"},
		{"unknown format", TimeAnchor{Original: "x", Format: "iso-week", SourceTZ: "UTC", UTC: "2025-01-01T22:30:00Z", Evidence: ev}, "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkAnchor("time_anchors[0]", tc.a, ok)
			if got := errText(err); tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("checkAnchor = %q, want %q", got, tc.want)
			}
		})
	}
	stop := errors.New("bad citation")
	if err := checkAnchor("a", TimeAnchor{Format: "rfc3339", Evidence: ev}, func(string, Evidence) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("citation failure not reported first: %v", err)
	}
}

func TestRawFileBoundaries(t *testing.T) {
	dir := t.TempDir()
	for name, data := range map[string][]byte{"data": []byte("body"), "empty": nil} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "dir"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(dir, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, path, want, err string }{
		{"regular", "data", "body", ""},
		{"empty", "empty", "", ""},
		{"outside symlink", "escape", "", "escapes"},
		{"not local", "../data", "", "unknown/local"},
		{"directory", "dir", "", "invalid evidence file"},
		{"fifo", "fifo", "", "invalid evidence file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := rawFile(context.Background(), contract.Ref{Path: filepath.Join(dir, "contract.json")}, []contract.FileEntry{{ID: "f", Kind: "evidence", Path: tc.path}}, "f")
			if got := errText(err); tc.err == "" && got != "" || tc.err != "" && !strings.Contains(got, tc.err) {
				t.Fatalf("error = %q, want %q", got, tc.err)
			}
			if err == nil && (raw == nil || string(raw) != tc.want) {
				t.Fatalf("body = %q (nil %t), want %q", raw, raw == nil, tc.want)
			}
		})
	}
}
