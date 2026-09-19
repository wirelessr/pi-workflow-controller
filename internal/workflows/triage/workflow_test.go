package triage

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

func TestRawFileReaderBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, path, want, message string
		cancel                    bool
		cause                     error
	}{
		{name: "regular", path: "data", want: "body"},
		{name: "empty", path: "empty"},
		{name: "in-root-symlink", path: "link", want: "body"},
		{name: "outside-symlink", path: "escape", message: "escapes from parent"},
		{name: "nonlocal", path: "../data", message: "unknown/local file required"},
		{name: "unknown", message: "unknown/local file required"},
		{name: "missing", path: "missing", cause: os.ErrNotExist},
		{name: "directory", path: "dir", message: "invalid evidence file"},
		{name: "fifo", path: "fifo", message: "invalid evidence file"},
		{name: "oversized", path: "large", message: "invalid evidence file"},
		{name: "cancelled", path: "data", cancel: true, cause: context.Canceled},
		{name: "missing-before-cancel", path: "missing", cancel: true, cause: os.ErrNotExist},
		{name: "stat-before-cancel", path: "dir", cancel: true, message: "invalid evidence file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "data"), []byte("body"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "empty"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(dir, "dir"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("data", filepath.Join(dir, "link")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(dir, "escape")); err != nil {
				t.Fatal(err)
			}
			if err := syscall.Mkfifo(filepath.Join(dir, "fifo"), 0600); err != nil {
				t.Fatal(err)
			}
			f, err := os.Create(filepath.Join(dir, "large"))
			if err != nil {
				t.Fatal(err)
			}
			err = f.Truncate((64 << 20) + 1)
			closeErr := f.Close()
			if err != nil || closeErr != nil {
				t.Fatal(errors.Join(err, closeErr))
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancel {
				cancel()
			}
			raw, err := rawFile(ctx, contract.Ref{Path: filepath.Join(dir, "contract.json")}, []file{{ID: "f", Kind: "evidence", Path: tc.path}}, "f")
			if tc.cause != nil {
				if !errors.Is(err, tc.cause) {
					t.Fatalf("error = %v, want %v", err, tc.cause)
				}
			} else if tc.message != "" {
				if err == nil || !strings.Contains(err.Error(), tc.message) {
					t.Fatalf("error = %v, want %q", err, tc.message)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err == nil && raw == nil {
				t.Fatal("successful raw read must preserve non-nil empty bytes")
			}
			if string(raw) != tc.want {
				t.Fatalf("body = %q, want %q", raw, tc.want)
			}
		})
	}
}

func TestTriageProtocolSubprocess(t *testing.T) {
	if os.Getenv("PWC_TRIAGE_PROTOCOL") != "1" {
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

// Only the external runtime execution context is shortened; Step and its
// cancellation/commit/cleanup machinery remain real.
type deadlineRuntime struct{ runtime.Runtime }
type deadlineSession struct{ runtime.Session }

func (r deadlineRuntime) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	s, err := r.Runtime.Start(ctx, spec)
	if err != nil {
		return nil, err
	}
	return deadlineSession{s}, nil
}
func (s deadlineSession) Execute(ctx context.Context, d runtime.Dispatch) (runtime.Execution, error) {
	timed, cancel := context.WithTimeoutCause(ctx, time.Second, &runtime.Failure{Code: runtime.TimedOut, Origin: runtime.AttemptDeadline, Message: "fixture step deadline"})
	defer cancel()
	return s.Session.Execute(timed, d)
}

func testJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func available(id string) Source       { return Source{Status: "available", FileID: id} }
func unavailable(reason string) Source { return Source{Status: "missing", Reason: reason} }
func testScope() Scope {
	return Scope{Ticket: "CASE-17", Stack: "test-stack", Pop: "test-pop", Binding: "local:test-cluster", TenantIDs: []string{"17"}}
}

func intakeFixture(mode string) (Intake, map[string][]byte) {
	files := map[string][]byte{
		"fields":    []byte(`[{"id":"customfield_1","name":"Ambiguous"},{"id":"customfield_2","name":"Ambiguous"},{"id":"description","name":"Description"}]`),
		"page-0":    []byte(`{"startAt":0,"total":2,"comments":[{"id":"c1","body":{"type":"doc","content":[{"text":"Local time missing timezone; inspect attachment"}]}}]}`),
		"page-1":    []byte(`{"startAt":1,"total":2,"comments":[{"id":"c2","body":{"type":"doc","content":[{"text":"Second page contains event correlation"}]}}]}`),
		"linked":    []byte(`{"key":"CASE-18","fields":{"summary":"Same incident reference"}}`),
		"bundle":    []byte("event=sample at 2025-01-02T00:30:00+02:00\n"),
		"extracted": []byte("processInfo/epoch correlation; observed event sample, not ticket activity\n"),
	}
	if strings.HasPrefix(mode, "support-") {
		files["bundle"] = []byte("event=sample at 2025-01-02T00:30:00; timezone unknown\n")
		files["page-1"] = []byte(`{"startAt":1,"total":2,"comments":[{"id":"c2","body":"Server receipt for tenant 17 at 2025-01-01T22:30:00Z; later receipt at 2025-01-01T22:50:00Z"}]}`)
	}
	files["issue"] = testJSON(map[string]any{"key": "CASE-17", "fields": map[string]any{
		"description":   map[string]any{"type": "doc", "content": []any{map[string]any{"text": strings.Repeat("unabridged ", 80)}}},
		"customfield_1": "not an identity", "customfield_2": 17,
		"comment":    map[string]any{"total": 2, "comments": []any{}},
		"issuelinks": []any{map[string]any{"outwardIssue": map[string]any{"key": "CASE-18"}}},
		"attachment": []any{map[string]any{"id": "a1", "size": len(files["bundle"]), "filename": "bundle.txt"}},
	}})
	v := Intake{Ticket: "CASE-17", URL: "https://jira.example.invalid/browse/CASE-17", FetchedAt: "2025-01-03T00:00:00Z", Issue: available("issue"), Fields: available("fields"), Comments: []CommentPage{{0, available("page-0")}, {1, available("page-1")}}, Linked: []LinkedIssue{{"CASE-18", available("linked")}}, Attachments: []Attachment{{"a1", available("bundle"), available("extracted")}}, Complete: true, Gaps: []string{}}
	switch mode {
	case "duplicate-attachment", "conflicting-attachment", "duplicate-empty-attachment":
		var issue map[string]any
		_ = json.Unmarshal(files["issue"], &issue)
		size := len(files["bundle"])
		if mode == "duplicate-empty-attachment" {
			files["bundle"] = []byte{}
			size = 0
		}
		otherSize := size
		if mode == "conflicting-attachment" {
			otherSize++
		}
		issue["fields"].(map[string]any)["attachment"] = []any{map[string]any{"id": "a1", "size": otherSize}, map[string]any{"id": "a1", "size": size}}
		files["issue"] = testJSON(issue)
	case "missing-attachment-size":
		var issue map[string]any
		_ = json.Unmarshal(files["issue"], &issue)
		issue["fields"].(map[string]any)["attachment"] = []any{map[string]any{"id": "a1"}}
		files["issue"] = testJSON(issue)
		files["bundle"] = []byte{}
	case "null-comment":
		files["page-1"] = []byte(`{"startAt":1,"total":2,"comments":[{"id":"c2","body":null}]}`)
	case "missing-page", "false-complete", "dropped-gap", "false-ready":
		delete(files, "page-1")
		v.Comments = v.Comments[:1]
		if mode != "false-complete" {
			v.Complete = false
			v.Gaps = []string{"comment page 1 unavailable; retry required"}
		}
	case "duplicate-comment":
		files["page-1"] = []byte(`{"startAt":1,"total":2,"comments":[{"id":"c1","body":"duplicate"}]}`)
	case "wrong-page-offset":
		files["page-1"] = []byte(`{"startAt":0,"total":2,"comments":[{"id":"c2","body":"second"}]}`)
	case "changed-total":
		files["page-1"] = []byte(`{"startAt":1,"total":3,"comments":[{"id":"c2","body":"second"}]}`)
		v.Complete = false
		v.Gaps = []string{"comment total changed during retrieval"}
	case "missing-linked":
		v.Linked = []LinkedIssue{}
	case "missing-attachment":
		v.Attachments = []Attachment{}
	case "truncated-attachment":
		files["bundle"] = []byte("truncated")
	case "unsafe-attachment", "oversized-attachment", "vision-pending":
		status := "unsafe"
		if mode == "oversized-attachment" {
			status = "too-large"
		}
		if mode != "vision-pending" {
			v.Attachments[0].Content = Source{Status: status, Reason: mode}
		}
		v.Attachments[0].Analysis = unavailable(mode)
		v.Complete = false
		v.Gaps = []string{mode}
	case "raw-key":
		files["issue"] = []byte(`{"key":"CASE-99","fields":{}}`)
	case "missing-fields":
		v.Fields = unavailable("field metadata access denied")
		v.Complete = false
		v.Gaps = []string{"field metadata access denied"}
	case "analysis-without-content":
		v.Attachments[0].Content = unavailable("download failed")
		v.Complete, v.Gaps = false, []string{"download failed"}
	case "initial-update":
		v.Update = true
	case "unknown-file":
		v.Issue.FileID = "absent"
	case "file-escape":
		v.Issue.FileID = "escape"
		files["escape"] = []byte("outside")
	}
	return v, files
}

func wikiFixture(mode string, intake contract.Ref) (WikiSearch, map[string][]byte) {
	v := WikiSearch{Intake: intake, Queries: []string{"sample symptom component"}, Scope: "wiki-only", Status: "completed-no-matches", Pages: []Source{}, Search: available("search"), Gaps: []string{}}
	files := map[string][]byte{"search": []byte(`{"query":"sample symptom component","complete":true,"matches":[]}`)}
	switch mode {
	case "wiki-history-identity", "wiki-history-conflict", "wiki-history-environment", "wiki-history-release":
		v.Status = "completed-with-matches"
		v.Pages = []Source{available("historical-identity")}
		lookup := IdentityLookup{Stack: "test-stack", Pop: "test-pop", Binding: "local:test-cluster", Release: "release-example", Matches: []TenantIdentity{{TenantID: "17", OrgKey: "org-example"}}}
		switch mode {
		case "wiki-history-conflict":
			lookup.Matches = append(lookup.Matches, TenantIdentity{TenantID: "18", OrgKey: "other-org"})
		case "wiki-history-environment":
			lookup.Pop = "other-pop"
		case "wiki-history-release":
			lookup.Release = "other-release"
		}
		files["historical-identity"] = testJSON(lookup)
	case "wiki-matches":
		v.Status = "completed-with-matches"
		v.Pages = []Source{available("wiki-page")}
		files["wiki-page"] = []byte("Prior pattern only, not runtime proof")
	case "wiki-partial", "wiki-unavailable":
		v.Status = "partial"
		if mode == "wiki-unavailable" {
			v.Status = "unavailable"
		}
		v.Search = Source{Status: "partial", FileID: "search", Reason: "read failed"}
		v.Gaps = []string{"wiki search must be retried"}
	case "wiki-not-run":
		v.Status = "not-run"
		v.Search = unavailable("search not attempted")
		v.Gaps = []string{"required wiki search pending"}
	case "wiki-false-empty":
		v.Search = unavailable("search never completed")
	case "wiki-ref":
		v.Intake.SHA256 = strings.Repeat("0", 64)
	}
	return v, files
}

func contextFixture(mode string, scope Scope, inputs []contract.Ref, intake Intake, wiki WikiSearch) (Context, map[string][]byte) {
	e := Evidence{FileID: "resolution"}
	fact := func(value string) Fact { return Fact{Value: value, Evidence: []Evidence{e}} }
	timestamp := TimeAnchor{Event: "sample", Original: "2025-01-02T00:30:00+02:00", Format: "rfc3339", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 7200, Evidence: Evidence{Ref: &inputs[0], FileID: "bundle"}}
	v := Context{Intake: inputs[0], Wiki: inputs[1], Scope: scope, Problem: "Synthetic event needs investigation", Identity: Identity{Lookup: &e, Status: "resolved", Stack: fact(scope.Stack), Pop: fact(scope.Pop), Binding: fact(scope.Binding), TenantID: fact("17"), OrgKey: fact("org-example"), UserKey: Fact{Evidence: []Evidence{}}, Release: fact("release-example")}, Time: TimeResolution{Status: "resolved", From: timestamp.UTC, To: timestamp.UTC, Anchors: []TimeAnchor{timestamp}}, Observations: []Fact{{Value: "Last comment preserved", Evidence: []Evidence{{Ref: &inputs[0], FileID: "page-1"}}}}, Attempts: []ResolutionAttempt{{Kind: "identity", Source: "authorized target lookup", Outcome: "one matching tenant/orgkey", Evidence: []Evidence{e}}, {Kind: "time", Source: "full ticket and bundle correlation", Outcome: "explicit offset resolves incident UTC", Evidence: []Evidence{{Ref: &inputs[0], FileID: "bundle"}}}}, AttachmentComplete: true, WikiStatus: wiki.Status, Gaps: append(append([]string{}, intake.Gaps...), wiki.Gaps...), Readiness: "ready"}
	files := map[string][]byte{"resolution": testJSON(IdentityLookup{Stack: "test-stack", Pop: "test-pop", Binding: "local:test-cluster", Release: "release-example", Matches: []TenantIdentity{{TenantID: "17", OrgKey: "org-example"}}})}
	if strings.HasPrefix(mode, "blank-") || mode == "whitespace-target" {
		files["resolution"] = testJSON(IdentityLookup{Stack: scope.Stack, Pop: scope.Pop, Binding: scope.Binding, Release: "release-example", Matches: []TenantIdentity{{TenantID: "17", OrgKey: "org-example"}}})
	}
	for _, a := range intake.Attachments {
		v.AttachmentComplete = v.AttachmentComplete && a.Content.Status == "available" && a.Analysis.Status == "available"
	}
	if !intake.Complete || !wikiComplete(wiki) || mode == "ticket-only" {
		v.Identity.Lookup = nil
		v.Readiness = "needs-resolution"
		v.Identity.Status = "unresolved"
		v.Identity.TenantID = Fact{Evidence: []Evidence{}}
		v.Attempts[0].Source = "committed local evidence"
		v.Attempts[0].Outcome = "prerequisites incomplete; production lookup deferred"
	}
	if len(intake.Comments) < 2 {
		v.Observations[0].Evidence[0].FileID = "page-0"
	}
	switch mode {
	case "ticket-only":
		v.Gaps = []string{"production target not yet authorized; resolve from local sources"}
	case "false-ready":
		v.Readiness = "ready"
	case "dropped-gap":
		v.Gaps = []string{"different unresolved gap"}
	case "lookup-conflict":
		files["resolution"] = testJSON(IdentityLookup{Stack: "test-stack", Pop: "test-pop", Binding: "local:test-cluster", Release: "release-example", Matches: []TenantIdentity{{TenantID: "17", OrgKey: "org-example"}, {TenantID: "18", OrgKey: "other-org"}}})
	case "lookup-environment":
		files["resolution"] = testJSON(IdentityLookup{Stack: "test-stack", Pop: "other-pop", Binding: "local:test-cluster", Release: "release-example", Matches: []TenantIdentity{{TenantID: "17", OrgKey: "org-example"}}})
	case "lookup-release":
		files["resolution"] = testJSON(IdentityLookup{Stack: "test-stack", Pop: "test-pop", Binding: "local:test-cluster", Release: "other-release", Matches: []TenantIdentity{{TenantID: "17", OrgKey: "org-example"}}})
	case "wiki-history-identity", "wiki-history-conflict", "wiki-history-environment", "wiki-history-release":
		v.Identity.Lookup = &Evidence{Ref: &inputs[1], FileID: "historical-identity"}
	case "conflicting-foreign-lookup":
		v.Identity.Status = "conflicting"
		v.Readiness = "needs-resolution"
		v.Gaps = []string{"identity conflict"}
		bad := inputs[0]
		bad.RunID = "foreign"
		v.Identity.Lookup = &Evidence{Ref: &bad, FileID: "issue"}
	case "missing-lookup":
		v.Identity.Lookup = nil
	case "self-paired":
		a := &v.Time.Anchors[0]
		n := int64(1735770600000)
		a.Format = "local-paired"
		a.Original = "2025-01-02T00:30:00"
		a.PairedEpochMillis = &n
		a.PairedEvidence = &a.Evidence
	case "wrong-scope":
		v.Scope.Pop = "other-pop"
	case "wrong-environment":
		v.Identity.Pop = fact("other-pop")
	case "wrong-tenant":
		v.Identity.TenantID = fact("99")
	case "no-release":
		v.Identity.Release = Fact{Evidence: []Evidence{}}
	case "identity-conflict":
		v.Identity.Status = "conflicting"
		v.Readiness = "needs-resolution"
		v.Gaps = []string{"tenant/orgkey lookup conflicts"}
	case "time-unresolved":
		v.Time = TimeResolution{Status: "unresolved", Anchors: []TimeAnchor{}}
		v.Readiness = "needs-resolution"
		v.Gaps = []string{"no trustworthy absolute anchor after inspecting available sources"}
	case "time-conflict":
		v.Time.Status = "conflicting"
		v.Time.From = ""
		v.Time.To = ""
		v.Readiness = "needs-resolution"
		v.Gaps = []string{"same-event anchors disagree"}
	case "guessed-zone":
		v.Time.Anchors[0].Original = "2025-01-02T00:30:00"
	case "wrong-utc":
		v.Time.Anchors[0].UTC = "2025-01-02T00:30:00Z"
	case "wrong-window":
		v.Time.From = "2025-01-01T00:00:00Z"
	case "foreign-ref":
		bad := inputs[0]
		bad.RunID = "foreign"
		v.Observations[0].Evidence[0].Ref = &bad
	case "uncommitted-ref":
		bad := inputs[0]
		bad.Path = filepath.Join(filepath.Dir(bad.Path), "candidate.json")
		v.Observations[0].Evidence[0].Ref = &bad
	case "missing-resolution":
		v.Attempts = v.Attempts[:1]
	case "no-evidence":
		v.Identity.TenantID.Evidence = []Evidence{}
	case "epoch-millis":
		v.Time.Anchors[0].Original = "1735770600000"
		v.Time.Anchors[0].Format = "epoch-millis"
		v.Time.Anchors[0].SourceTZ = "UTC"
		v.Time.Anchors[0].OffsetSeconds = 0
	case "local-paired", "local-no-pair", "wrong-offset":
		n := int64(1735770600000)
		a := &v.Time.Anchors[0]
		a.Original = "2025-01-02T00:30:00"
		a.Format = "local-paired"
		a.SourceTZ = "same-event local/epoch correlation"
		a.PairedEpochMillis = &n
		a.PairedEvidence = &e
		if mode == "local-no-pair" {
			a.PairedEvidence = nil
		}
		if mode == "wrong-offset" {
			a.OffsetSeconds = 3600
		}
	case "dst-offsets":
		a := &v.Time.Anchors[0]
		a.Original = "2025-11-02T01:30:00-04:00"
		a.SourceTZ = "-04:00"
		a.UTC = "2025-11-02T05:30:00Z"
		a.OffsetSeconds = -14400
		b := *a
		b.Original = "2025-11-02T01:30:00-05:00"
		b.SourceTZ = "-05:00"
		b.UTC = "2025-11-02T06:30:00Z"
		b.OffsetSeconds = -18000
		v.Time.From = a.UTC
		v.Time.To = b.UTC
		v.Time.Anchors = append(v.Time.Anchors, b)
	}
	return v, files
}

func writeCandidate(t *testing.T, m protocol.Control, req contract.Request, data any, files map[string][]byte, escape bool) {
	t.Helper()
	entries := []file{}
	keys := make([]string, 0, len(files))
	for id := range files {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		path := filepath.Join("evidence", id)
		if err := os.WriteFile(filepath.Join(filepath.Dir(m.CandidatePath), path), files[id], 0600); err != nil {
			t.Fatal(err)
		}
		if escape && id == "escape" {
			path = "../evidence/escape"
		}
		entries = append(entries, file{ID: id, Kind: "evidence", Path: path})
	}
	writeEnvelope(t, m, req, data, entries)
}

func writeEnvelope(t *testing.T, m protocol.Control, req contract.Request, data any, entries []file) {
	t.Helper()
	meta := struct {
		contract.Identity
		Version  int    `json:"version"`
		SchemaID string `json:"schema_id"`
	}{req.Identity, 1, req.Output.SchemaID}
	if err := os.WriteFile(m.CandidatePath, testJSON(map[string]any{"meta": meta, "data": data, "files": entries}), 0600); err != nil {
		t.Fatal(err)
	}
}

func resolutionFixture(name, mode string, scope Scope, inputs []contract.Ref, intake Intake, wiki WikiSearch, prior Context) (Context, map[string][]byte) {
	var tree any
	_ = json.Unmarshal(testJSON(prior), &tree)
	var qualify func(any)
	qualify = func(node any) {
		switch node := node.(type) {
		case map[string]any:
			if _, ok := node["ref"]; ok && node["ref"] == nil {
				node["ref"] = inputs[2]
			}
			for _, value := range node {
				qualify(value)
			}
		case []any:
			for _, value := range node {
				qualify(value)
			}
		}
	}
	qualify(tree)
	var old Context
	_ = json.Unmarshal(testJSON(tree), &old)
	fixtureMode := "complete"
	if mode == "ticket-only" {
		fixtureMode = mode
	}
	v, files := contextFixture(fixtureMode, scope, inputs, intake, wiki)
	v.Previous = &inputs[2]
	v.Problem = old.Problem
	if old.Identity.Status == "resolved" {
		v.Identity = old.Identity
	}
	if old.Time.Status == "resolved" {
		v.Time = old.Time
	}
	v.Observations = old.Observations
	v.Attempts = append(old.Attempts, v.Attempts...)
	for _, gap := range old.Gaps {
		if !strings.Contains(strings.Join(v.Gaps, "\n"), gap) {
			v.ResolvedGaps = append(v.ResolvedGaps, Fact{Value: gap, Evidence: []Evidence{{FileID: "remediation"}}})
		}
	}
	files["remediation"] = testJSON(map[string]any{"wiki_status": wiki.Status, "identity_status": v.Identity.Status, "time_status": v.Time.Status, "outcome": "anonymous boundary resolution evidence"})
	switch name {
	case "resolve-drop-gap":
		v.ResolvedGaps = nil
	case "resolve-foreign-evidence":
		bad := inputs[2]
		bad.RunID = "foreign"
		v.ResolvedGaps[0].Evidence[0] = Evidence{Ref: &bad, FileID: "resolution"}
	case "resolve-replace-valid-time":
		v.Time.Anchors[0].Event = "different event"
	case "resolve-wrong-previous":
		v.Previous = &inputs[0]
	case "resolve-old-evidence":
		v.ResolvedGaps[0].Evidence[0] = Evidence{Ref: &inputs[0], FileID: "issue"}
	case "resolve-dropped-history":
		v.Attempts = v.Attempts[len(old.Attempts):]
	}
	return v, files
}

func assertRetainedInputOwners(t *testing.T, inputs []contract.Ref) {
	t.Helper()
	var visit func(any)
	visit = func(value any) {
		switch node := value.(type) {
		case []any:
			for _, child := range node {
				visit(child)
			}
		case map[string]any:
			if owner, ok := node["ref"]; ok && owner != nil {
				var ref contract.Ref
				if err := json.Unmarshal(testJSON(owner), &ref); err != nil {
					t.Fatal(err)
				}
				if !slices.Contains(inputs, ref) {
					t.Fatalf("retained owner absent from explicit inputs: %+v", ref)
				}
				var p publication[json.RawMessage]
				if err := protocol.ReadJSON(ref.Path, &p); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, f := range p.Files {
					if f.ID == node["file_id"] {
						if _, err := os.ReadFile(filepath.Join(filepath.Dir(ref.Path), f.Path)); err != nil {
							t.Fatal(err)
						}
						found = true
					}
				}
				if !found {
					t.Fatal("retained file absent from owner")
				}
			}
			for _, child := range node {
				visit(child)
			}
		}
	}
	for _, ref := range inputs {
		var p publication[any]
		if err := protocol.ReadJSON(ref.Path, &p); err != nil {
			t.Fatal(err)
		}
		visit(p.Data)
	}
}

// The update API responses change inventory without any per-source dispatch.
func updateHTTPFixture(t *testing.T, name string, observe func(*http.Request)) *httptest.Server {
	t.Helper()
	_, raw := intakeFixture("complete")
	bundle := []byte("new attachment evidence\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observe(r)
		if r.Header.Get("Authorization") != "" {
			t.Error("HTTP fixture received Authorization")
		}
		switch r.URL.Path {
		case "/rest/api/3/issue/CASE-17":
			if r.URL.Query().Get("fields") != "*all" {
				t.Error("update omitted raw fields")
			}
			if strings.HasPrefix(name, "update-issue-failure") {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("issue unavailable"))
				return
			}
			var issue map[string]any
			_ = json.Unmarshal(raw["issue"], &issue)
			fields := issue["fields"].(map[string]any)
			switch name {
			case "update-remove", "update-retain-gap", "update-drop-gap", "update-resolve-gap":
				fields["attachment"], fields["issuelinks"] = []any{}, []any{}
			case "update-replace", "update-repeat":
				fields["attachment"] = []any{map[string]any{"id": "a2", "size": len(bundle)}}
				fields["issuelinks"] = []any{map[string]any{"outwardIssue": map[string]any{"key": "CASE-19"}}}
			case "update-content", "update-retain-content", "update-truncated":
				fields["attachment"] = []any{map[string]any{"id": "a1", "size": len(bundle)}}
			case "update-comments-repage":
				fields["comment"] = map[string]any{"total": 1, "comments": []any{}}
			case "update-comments-missing", "update-comments-false-complete":
				fields["comment"] = map[string]any{"total": 3, "comments": []any{}}
			case "update-then-refresh", "update-removed-still-in-raw", "update-page", "update-wrong-task":
			default:
				fields["attachment"] = append(fields["attachment"].([]any), map[string]any{"id": "a2", "size": len(bundle)})
				fields["issuelinks"] = append(fields["issuelinks"].([]any), map[string]any{"outwardIssue": map[string]any{"key": "CASE-19"}})
			}
			_, _ = w.Write(testJSON(issue))
		case "/rest/api/3/issue/CASE-19":
			_, _ = w.Write([]byte(`{"key":"CASE-19","fields":{"summary":"New incident reference"}}`))
		case "/attachment":
			if name == "update-partial" {
				w.WriteHeader(http.StatusForbidden)
			}
			body := bundle
			if name == "update-truncated" {
				body = body[:len(body)-1]
			}
			_, _ = w.Write(body)
		case "/rest/api/3/issue/CASE-17/comment":
			start := r.URL.Query().Get("startAt")
			if strings.HasPrefix(name, "update-comments-") {
				total := 3
				if name == "update-comments-repage" {
					total = 1
				}
				n := 0
				if start == "1" {
					n = 1
				}
				if start == "2" {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte("comment page unavailable"))
					return
				}
				_, _ = w.Write(testJSON(map[string]any{"startAt": n, "total": total, "comments": []any{map[string]any{"id": fmt.Sprintf("c%d", n+1), "body": "updated comment"}}}))
			} else {
				_, _ = w.Write(raw["page-"+start])
			}
		default:
			t.Errorf("update unexpectedly reacquired %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// This is an external provider fixture, not a replacement Step/validator or a
// product acquisition entry. Only anonymous localhost sources are read.
func refreshIntakeFixture(t *testing.T, ctx context.Context, name string, count int, inputs []contract.Ref, task stageTask, base string) (Intake, map[string][]byte, int32) {
	t.Helper()
	var prior publication[Intake]
	if err := protocol.ReadJSON(inputs[0].Path, &prior); err != nil {
		t.Fatal(err)
	}
	v := prior.Data
	v.Previous, v.Work = &inputs[0], task.SourceWork
	v.Update = task.Stage == "intake-update"
	v.FetchedAt = "2025-01-04T00:00:00Z"
	qualify := func(s *Source) {
		if s.FileID != "" && s.Ref == nil {
			s.Ref = &inputs[0]
		}
	}
	qualify(&v.Issue)
	qualify(&v.Fields)
	for i := range v.Comments {
		qualify(&v.Comments[i].Source)
	}
	for i := range v.Linked {
		qualify(&v.Linked[i].Source)
	}
	for i := range v.Attachments {
		qualify(&v.Attachments[i].Content)
		qualify(&v.Attachments[i].Analysis)
	}
	files := map[string][]byte{}
	var receipts []map[string]any
	fetch := func(id, endpoint string) Source {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatalf("fixture response: %v %v", readErr, closeErr)
		}
		files[id] = raw
		receipts = append(receipts, map[string]any{"file_id": id, "http_status": resp.StatusCode, "source": endpoint})
		if resp.StatusCode != http.StatusOK {
			return Source{Status: "partial", FileID: id, Reason: "HTTP source unavailable"}
		}
		return available(id)
	}
	if v.Update {
		v.Issue = fetch("new-issue", "/rest/api/3/issue/CASE-17?fields=*all")
		v.Work = []intakeWork{{Source: "issue", Reason: "update ticket snapshot"}}
		record := func(selector string) {
			v.Work = append(v.Work, intakeWork{Source: selector, Reason: "agent handled inventory update"})
		}
		if name == "update-page" {
			v.Comments[1].Source = fetch("new-page-1", "/rest/api/3/issue/CASE-17/comment?startAt=1")
			record("comment:1")
		}
		if strings.HasPrefix(name, "update-comments-") {
			v.Comments[0].Source = fetch("new-page-0", "/rest/api/3/issue/CASE-17/comment?startAt=0")
			record("comment:0")
			record("comment:1")
			if name == "update-comments-repage" {
				v.Comments = v.Comments[:1]
			} else {
				v.Comments[1].Source = fetch("new-page-1", "/rest/api/3/issue/CASE-17/comment?startAt=1")
				v.Comments = append(v.Comments, CommentPage{Start: 2, Source: fetch("new-page-2", "/rest/api/3/issue/CASE-17/comment?startAt=2")})
				record("comment:2")
			}
		}
		if v.Issue.Status == "available" {
			var issue acquisitionIssue
			if err := json.Unmarshal(files["new-issue"], &issue); err != nil {
				t.Fatal(err)
			}
			var links []acquisitionLink
			var attachments []acquisitionAttachment
			_ = json.Unmarshal(issue.Fields["issuelinks"], &links)
			_ = json.Unmarshal(issue.Fields["attachment"], &attachments)
			oldLinks, oldAttachments := v.Linked, v.Attachments
			v.Linked, v.Attachments = []LinkedIssue{}, []Attachment{}
			for _, link := range links {
				key := link.Out.Key
				n := slices.IndexFunc(oldLinks, func(l LinkedIssue) bool { return l.Key == key })
				if n >= 0 {
					v.Linked = append(v.Linked, oldLinks[n])
				} else {
					source := fetch("new-linked", "/rest/api/3/issue/"+key+"?fields=*all")
					v.Linked = append(v.Linked, LinkedIssue{Key: key, Source: source})
					record("linked:" + key)
				}
			}
			for _, old := range oldLinks {
				if !slices.ContainsFunc(v.Linked, func(l LinkedIssue) bool { return l.Key == old.Key }) {
					record("linked:" + old.Key)
				}
			}
			for _, attachment := range attachments {
				n := slices.IndexFunc(oldAttachments, func(a Attachment) bool { return a.ID == attachment.ID })
				if n >= 0 {
					a := oldAttachments[n]
					if name == "update-content" || name == "update-truncated" {
						a.Content = fetch("new-bundle", "/attachment")
						record("attachment-content:" + a.ID)
					}
					v.Attachments = append(v.Attachments, a)
				} else {
					content := fetch("new-bundle", "/attachment")
					analysis := available("new-bundle")
					if content.Status != "available" {
						analysis = unavailable("content incomplete")
					}
					v.Attachments = append(v.Attachments, Attachment{ID: attachment.ID, Content: content, Analysis: analysis})
					record("attachment-content:" + attachment.ID)
					record("attachment-analysis:" + attachment.ID)
				}
			}
			for _, old := range oldAttachments {
				if !slices.ContainsFunc(v.Attachments, func(a Attachment) bool { return a.ID == old.ID }) {
					record("attachment-content:" + old.ID)
					record("attachment-analysis:" + old.ID)
				}
			}
		}
	} else {
		for _, item := range task.SourceWork {
			switch item.Source {
			case "issue":
				fetch("new-issue", "/rest/api/3/issue/CASE-17?fields=*all")
				v.Issue = Source{Status: "partial", FileID: "new-issue", Reason: "issue response incomplete"}
				if name == "refresh-issue-missing" {
					v.Issue.Status = "missing"
				}
			case "comment:1":
				fetch("new-page", "/rest/api/3/issue/CASE-17/comment?startAt=1")
				if len(v.Comments) == 1 {
					v.Comments = append(v.Comments, CommentPage{Start: 1})
				}
				v.Comments[1].Source = available("new-page")
			case "attachment-content:a1":
				v.Attachments[0].Content = fetch("new-bundle", "/attachment")
				if strings.HasPrefix(name, "refresh-content-") {
					v.Attachments[0].Content.Status = "partial"
					v.Attachments[0].Content.Reason = "replacement content incomplete"
				}
			case "attachment-analysis:a1":
				v.Attachments[0].Analysis = available("new-bundle")
			default:
				t.Fatal("unexpected fixture work")
			}
		}
	}
	v.Complete, v.Gaps = true, []string{}
	if v.Update || strings.HasPrefix(name, "refresh-content-") {
		sources := []Source{v.Issue, v.Fields}
		for _, p := range v.Comments {
			sources = append(sources, p.Source)
		}
		for _, l := range v.Linked {
			sources = append(sources, l.Source)
		}
		for _, a := range v.Attachments {
			sources = append(sources, a.Content, a.Analysis)
		}
		for _, source := range sources {
			if source.Status != "available" {
				v.Complete = false
				v.Gaps = append(v.Gaps, source.Reason)
			}
		}
	}
	if strings.HasPrefix(name, "refresh-issue-") {
		v.Complete, v.Gaps = false, append(append([]string{}, prior.Data.Gaps...), "issue response incomplete")
	}
	if (name == "refresh-repeat" && count == 4) || name == "refresh-false-complete" {
		v.Comments[1].Source.Status, v.Comments[1].Source.Reason = "partial", "source still incomplete"
		if name != "refresh-false-complete" {
			v.Complete, v.Gaps = false, []string{"source still incomplete"}
		}
	}
	files["revision-metadata"] = testJSON(map[string]any{"records": receipts, "gaps": v.Gaps})
	v.Acquisition = &Source{Status: "available", FileID: "revision-metadata"}
	switch name {
	case "refresh-unselected-copy", "update-unrecorded-change":
		files["copied-fields"] = []byte(`[{"id":"description","name":"Description"}]`)
		v.Fields = available("copied-fields")
	case "refresh-foreign-ref", "update-foreign-ref":
		bad := inputs[0]
		bad.RunID = "foreign"
		v.Fields.Ref = &bad
	case "refresh-wrong-previous", "update-wrong-previous":
		v.Previous = &inputs[1]
	case "refresh-work-changed":
		v.Work = append(append([]intakeWork{}, v.Work...), intakeWork{Source: "fields", Reason: "unrequested expansion"})
	case "refresh-no-metadata", "update-no-metadata":
		v.Acquisition = nil
	case "refresh-dropped-source":
		v.Linked = []LinkedIssue{}
	case "refresh-escalated-task":
		v.Update = true
		v.Work = append(v.Work, intakeWork{Source: "issue", Reason: "unrequested full update"})
		files["new-issue"] = testJSON(map[string]any{})
		v.Issue = Source{Status: "partial", FileID: "new-issue", Reason: "issue incomplete"}
		v.Complete, v.Gaps = false, []string{"issue incomplete"}
	case "update-comments-false-complete":
		v.Complete, v.Gaps = true, []string{}
	case "update-wrong-task":
		v.Update = false
	case "update-no-issue":
		v.Work = v.Work[1:]
		v.Issue = prior.Data.Issue
		qualify(&v.Issue)
	case "update-duplicate-work":
		v.Work = append(v.Work, v.Work[0])
	case "update-blank-reason":
		v.Work[0].Reason = " "
	case "update-unrecorded-add":
		v.Work = v.Work[:1]
	case "update-omitted-slot":
		v.Attachments = v.Attachments[:1]
		v.Work = slices.DeleteFunc(v.Work, func(w intakeWork) bool { return strings.HasSuffix(w.Source, ":a2") })
	case "update-new-ref":
		v.Attachments[1].Content = v.Attachments[0].Content
	case "update-removed-still-in-raw", "update-issue-failure-remove":
		v.Linked, v.Attachments = []LinkedIssue{}, []Attachment{}
		v.Work = append(v.Work, intakeWork{Source: "linked:CASE-18", Reason: "removed"}, intakeWork{Source: "attachment-content:a1", Reason: "removed"}, intakeWork{Source: "attachment-analysis:a1", Reason: "removed"})
	case "update-history-alias":
		v.Attachments[0].Analysis.Ref = &inputs[0]
		v.Attachments[0].Analysis.FileID = "fields"
	}
	return v, files, int32(len(receipts))
}

// Only the external API/provider boundary is simulated. The fixture performs
// real HTTP acquisition in one context task; it is not a product tool wrapper.
func supportingHTTPFixture(t *testing.T, name string, requests *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/identity":
			if name == "support-malformed-lookup" {
				_, _ = w.Write([]byte(`{"rows":`))
				return
			}
			target := IdentityLookup{Stack: "test-stack", Pop: "test-pop", Binding: "local:test-cluster", Release: "release-example", Matches: []TenantIdentity{{TenantID: "17", OrgKey: "org-example"}}}
			if name == "support-lookup-conflict" {
				target.Matches = append(target.Matches, TenantIdentity{TenantID: "18", OrgKey: "other-org"})
			}
			if name == "support-lookup-environment" {
				target.Pop = "other-pop"
			}
			if name == "support-lookup-release" {
				target.Release = "other-release"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"target": target, "query": "read-only tenant lookup for 17"})
		case "/events":
			if r.URL.Query().Get("from") == "" || r.URL.Query().Get("to") == "" {
				t.Error("supporting request lacks bounded time")
			}
			if r.URL.Query().Get("filter") == "tenant:17" {
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte(`{"events":[{"trace":"sample"}],"partial":true}`))
				return
			}
			if r.URL.Query().Get("filter") != "trace:sample" {
				t.Error("fixture did not narrow with observed trace")
			}
			mode := r.URL.Query().Get("mode")
			if mode == "unavailable" {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"source unavailable"}`))
			} else if mode == "partial" {
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write([]byte(`{"events":[],"partial":true}`))
			} else if mode == "empty" {
				_, _ = w.Write([]byte(`{"events":[],"partial":false}`))
			} else {
				epoch := int64(1735770600000)
				if name == "support-wrong-epoch" {
					epoch += 1000
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"events": []any{map[string]any{"trace": "sample", "epoch_millis": epoch}}, "partial": false})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func supportingContextFixture(t *testing.T, ctx context.Context, base, name string, cycle int, task stageTask, inputs []contract.Ref, v Context, files map[string][]byte) (Context, map[string][]byte) {
	t.Helper()
	// A trustworthy server receipt is available, but the client timestamp is
	// still unresolved when supporting acquisition begins.
	if cycle == 0 {
		v.Time = TimeResolution{Status: "unresolved", Anchors: []TimeAnchor{}}
		v.Attempts[1].Outcome = "local client timestamp has no timezone; seek same-event server evidence"
	}
	if !task.RuntimeResolutionAllowed {
		return v, files
	}
	fetch := func(id, endpoint string) int {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		files[id] = raw
		files[id+"-metadata"] = testJSON(map[string]any{"request": endpoint, "http_status": resp.StatusCode, "fetched_at": "2025-01-03T00:00:00Z"})
		return resp.StatusCode
	}
	if cycle == 0 {
		fetch("identity-raw", "/identity")
		var raw struct {
			Target IdentityLookup `json:"target"`
		}
		if err := json.Unmarshal(files["identity-raw"], &raw); err != nil {
			delete(files, "resolution")
			v.Identity.Lookup = nil
			v.Identity.Status = "unresolved"
			v.Gaps = append(v.Gaps, "identity response malformed")
			v.Attempts[0].Outcome = "raw lookup response could not be parsed"
		} else {
			files["resolution"] = testJSON(raw.Target)
			if len(raw.Target.Matches) != 1 {
				v.Identity.Status = "conflicting"
				v.Gaps = append(v.Gaps, "identity response has conflicting rows")
				v.Attempts[0].Outcome = "raw lookup returned conflicting rows"
			}
		}
		v.Attempts[0].Evidence = []Evidence{{FileID: "identity-raw"}, {FileID: "identity-raw-metadata"}}
		for _, f := range []*Fact{&v.Identity.Stack, &v.Identity.Pop, &v.Identity.Binding, &v.Identity.TenantID, &v.Identity.OrgKey, &v.Identity.Release} {
			f.Evidence = []Evidence{{FileID: "identity-raw"}, {FileID: "identity-raw-metadata"}}
		}
	}
	mode := "complete"
	switch name {
	case "support-partial":
		mode = "partial"
	case "support-unavailable":
		mode = "unavailable"
	case "support-empty":
		mode = "empty"
	}
	if strings.HasPrefix(name, "support-resolve-") && (cycle == 0 || (name == "support-resolve-repeat" && cycle == 1)) {
		mode = "partial"
	}
	attempt := ResolutionAttempt{Kind: "time", Source: "authorized same-event supporting search", Outcome: "preserve both broad partial and narrowed result", Evidence: []Evidence{{FileID: "events-narrow"}, {FileID: "events-narrow-metadata"}}}
	for n, spec := range []struct{ id, from, to, filter string }{
		{"events-broad", "2025-01-01T22:29:00Z", "2025-01-01T22:31:00Z", "tenant:17"},
		{"events-narrow", "2025-01-01T22:29:59Z", "2025-01-01T22:30:01Z", "trace:sample"},
	} {
		filter := spec.filter
		if n == 1 {
			var prior struct {
				Events []struct {
					Trace string `json:"trace"`
				} `json:"events"`
			}
			if err := json.Unmarshal(files["events-broad"], &prior); err != nil || len(prior.Events) != 1 {
				t.Fatal("missing narrowing evidence")
			}
			filter = "trace:" + prior.Events[0].Trace
		}
		status := fetch(spec.id, "/events?from="+spec.from+"&to="+spec.to+"&filter="+filter+"&mode="+mode)
		q := SupportingQuery{Source: "anonymous event source", Filter: filter, From: spec.from, To: spec.to, Basis: []Evidence{{Ref: &inputs[0], FileID: "page-1"}}, Status: "complete", Outcome: "small window around server receipt; narrow with observed trace after volume limit; not full-incident coverage", Evidence: []Evidence{{FileID: spec.id}, {FileID: spec.id + "-metadata"}}}
		if n == 1 {
			q.Basis = append(q.Basis, Evidence{FileID: "events-broad"})
		}
		if status == http.StatusPartialContent {
			q.Status = "partial"
		} else if status != http.StatusOK {
			q.Status = "unavailable"
		}
		attempt.Queries = append(attempt.Queries, q)
	}
	var events struct {
		Events []struct {
			Epoch int64 `json:"epoch_millis"`
		} `json:"events"`
	}
	if err := json.Unmarshal(files["events-narrow"], &events); err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != 0 {
		epoch := events.Events[0].Epoch
		a := TimeAnchor{Event: "sample", Original: "2025-01-02T00:30:00", Format: "local-paired", SourceTZ: "same-event server epoch", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 7200, Evidence: Evidence{Ref: &inputs[0], FileID: "bundle"}, PairedEpochMillis: &epoch, PairedEvidence: &Evidence{FileID: "events-narrow"}}
		v.Time = TimeResolution{Status: "resolved", From: a.UTC, To: a.UTC, Anchors: []TimeAnchor{a}}
		for n := range v.ResolvedGaps {
			v.ResolvedGaps[n].Evidence = []Evidence{{FileID: "events-narrow"}, {FileID: "events-narrow-metadata"}}
		}
		if name == "support-subwindow" {
			v.Time.Anchors = append(v.Time.Anchors, TimeAnchor{Event: "later receipt", Original: "2025-01-01T22:50:00Z", Format: "rfc3339", SourceTZ: "UTC", UTC: "2025-01-01T22:50:00Z", Evidence: Evidence{Ref: &inputs[0], FileID: "page-1"}})
			v.Time.To = "2025-01-01T22:50:00Z"
		}
	} else {
		v.Time = TimeResolution{Status: "unresolved", Anchors: []TimeAnchor{}}
		v.Gaps = append(v.Gaps, "supporting time evidence pending")
		v.ResolvedGaps = nil
	}
	q := &attempt.Queries[1]
	switch name {
	case "support-zero-window":
		q.To = q.From
	case "support-reversed-window":
		q.From, q.To = q.To, q.From
	case "support-nonutc":
		q.From = "2025-01-01T22:29:59"
	case "support-no-basis":
		q.Basis = []Evidence{}
	case "support-missing-file":
		q.Basis[0].FileID = "absent-seed"
	case "support-no-result":
		q.Evidence = []Evidence{}
	case "support-blank-filter":
		q.Filter = " "
	case "support-no-outcome":
		q.Outcome = ""
	case "support-invalid-status":
		q.Status = "ready"
	case "support-foreign-basis":
		bad := inputs[0]
		bad.RunID = "foreign"
		q.Basis[0].Ref = &bad
	case "support-uncommitted-result":
		bad := inputs[0]
		bad.Path = filepath.Join(filepath.Dir(bad.Path), "candidate.json")
		q.Evidence[0].Ref = &bad
	}
	v.Attempts = append(v.Attempts, attempt)
	if cycle > 0 {
		switch name {
		case "support-resolve-dropped-history":
			v.Attempts = append(v.Attempts[:2], v.Attempts[3:]...)
		case "support-resolve-altered-query":
			v.Attempts[2].Queries[0].Status = "complete"
		case "support-resolve-retag-basis":
			v.Attempts[2].Queries[1].Basis[1].Ref = nil
		}
	}
	v.Readiness = "ready"
	if len(v.Gaps) > 0 {
		v.Readiness = "needs-resolution"
	}
	return v, files
}

func plannerFixture(t *testing.T, mode string, req contract.Request, task stageTask, step int, initialIntake contract.Ref) PlannerState {
	t.Helper()
	var supporting publication[Context]
	if len(req.Inputs) < 3 || req.Inputs[0].SchemaID != ContextSchema {
		t.Fatal("planner lost supporting context")
	}
	if err := protocol.ReadJSON(req.Inputs[0].Path, &supporting); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(task.Gaps, supporting.Data.Gaps) {
		t.Fatal("planner lost supporting gaps")
	}
	basis := supporting.Data.Observations[0].Evidence[0]
	if basis.Ref == nil {
		basis.Ref = &req.Inputs[0]
	}
	if mode == "planner-history" {
		if supporting.Data.Intake == initialIntake || !slices.Contains(req.Inputs, initialIntake) {
			t.Fatal("planner lost the original intake owner after revision")
		}
		basis = Evidence{Ref: &initialIntake, FileID: "issue"}
	}
	v := PlannerState{Context: req.Inputs[0], Hypotheses: []PlannerHypothesis{{ID: "h1", Statement: "A request may have stalled", Assessment: "Unverified; compare runtime evidence", Evidence: []Evidence{basis}}}, Pending: []PlannerQuestion{{Question: "Where did the request stop?", Requirements: []string{"Correlate the same request across authorized sources"}, Basis: []Evidence{basis}}}, Gaps: slices.Clone(supporting.Data.Gaps), Rationale: "Preserve prerequisites before choosing evidence work"}
	if step == 1 {
		if task.Previous != nil {
			t.Fatal("initial planner invented prior state")
		}
	} else {
		if task.Previous == nil || len(req.Inputs) < 4 || *task.Previous != req.Inputs[1] {
			t.Fatal("planner continuation lost exact prior state")
		}
		var prior publication[PlannerState]
		if err := protocol.ReadJSON(req.Inputs[1].Path, &prior); err != nil {
			t.Fatal(err)
		}
		v = prior.Data
		v.Previous = &req.Inputs[1]
		v.Rationale += "; continued from committed snapshot"
		v.Pending[0].Requirements = append(v.Pending[0].Requirements, "Preserve the prior question when handing off")
	}
	switch mode {
	case "planner-empty":
		v.Hypotheses, v.Pending = []PlannerHypothesis{}, []PlannerQuestion{}
	case "planner-no-evidence":
		v.Hypotheses[0].Evidence, v.Pending[0].Basis = []Evidence{}, []Evidence{}
	case "planner-wrong-context":
		v.Context = supporting.Data.Intake
	case "planner-wrong-previous":
		if step == 2 {
			v.Previous = nil
		}
	case "planner-invented-previous":
		v.Previous = &req.Inputs[0]
	case "planner-drop-gap":
		v.Gaps = []string{}
	case "planner-foreign-evidence", "planner-uncommitted-evidence":
		bad := *basis.Ref
		if mode == "planner-foreign-evidence" {
			bad.RunID = "foreign"
		} else {
			bad.Path = filepath.Join(filepath.Dir(bad.Path), "candidate.json")
		}
		v.Hypotheses[0].Evidence[0].Ref = &bad
	case "planner-local-evidence":
		v.Hypotheses[0].Evidence[0].Ref = nil
	case "planner-missing-file":
		v.Pending[0].Basis[0].FileID = "not-an-input-file"
	case "planner-duplicate-id":
		v.Hypotheses = append(v.Hypotheses, v.Hypotheses[0])
	case "planner-blank-assessment":
		v.Hypotheses[0].Assessment = " "
	case "planner-blank-rationale":
		v.Rationale = " "
	case "planner-empty-requirements":
		v.Pending[0].Requirements = []string{}
	}
	return v
}

// Only the provider chooses the work kind; the Controller consumes the emitted
// contract, including evidence retained across inventory changes.
func plannerWorkFixture(t *testing.T, name, kind string, req contract.Request, task stageTask, step int, initial contract.Ref) PlannerState {
	t.Helper()
	var current publication[Context]
	if err := protocol.ReadJSON(req.Inputs[0].Path, &current); err != nil {
		t.Fatal(err)
	}
	v := plannerFixture(t, "", req, task, step, initial)
	v.Context, v.Gaps = req.Inputs[0], slices.Clone(current.Data.Gaps)
	v.SupportingWork = nil
	if step == 1 || (name == "work-repeat" && step == 2) {
		v.SupportingWork = &SupportingWork{Kind: kind, Reason: "Recheck supporting inputs before further investigation", Basis: []Evidence{{Ref: &initial, FileID: "issue"}}, Sources: []intakeWork{}}
		if kind == "refresh" {
			v.SupportingWork.Sources = []intakeWork{{Source: "comment:1", Reason: "missing comment page"}}
		}
	}
	if step > 1 {
		v.Hypotheses[0].Evidence = []Evidence{{Ref: &initial, FileID: "issue"}}
		v.Rationale = "Reassessed new supporting context; historical evidence retains its original owner"
		if name == "work-wrong-result-context" {
			var prior publication[PlannerState]
			if err := protocol.ReadJSON(req.Inputs[1].Path, &prior); err != nil {
				t.Fatal(err)
			}
			v.Context = prior.Data.Context
		}
		if name == "work-drop-gap" {
			v.Gaps = []string{}
		}
		return v
	}
	w := v.SupportingWork
	switch name {
	case "work-no-proposal", "work-unproposed-transition":
		v.SupportingWork = nil
	case "work-bad-kind":
		w.Kind = "logs"
	case "work-blank-reason":
		w.Reason = " "
	case "work-no-basis":
		w.Basis = []Evidence{}
	case "work-local-basis":
		w.Basis[0].Ref = nil
	case "work-foreign-basis", "work-uncommitted-basis":
		bad := initial
		if name == "work-foreign-basis" {
			bad.RunID = "foreign"
		} else {
			bad.Path = filepath.Join(filepath.Dir(bad.Path), "candidate.json")
		}
		w.Basis[0].Ref = &bad
	case "work-missing-file":
		w.Basis[0].FileID = "absent"
	case "work-refresh-empty":
		w.Sources = []intakeWork{}
	case "work-unknown-source":
		w.Sources[0].Source = "linked:CASE-99"
	case "work-duplicate-source":
		w.Sources = append(w.Sources, w.Sources[0])
	case "work-blank-source-reason":
		w.Sources[0].Reason = " "
	case "work-extra-sources":
		w.Sources = []intakeWork{{Source: "issue", Reason: "not an update approval list"}}
	}
	return v
}

func assertPlannerOutcome(report engine.Report, name string, failed bool, ref, first contract.Ref, expected PlannerState) error {
	if !failed {
		var p publication[PlannerState]
		if err := protocol.ReadJSON(ref.Path, &p); err != nil {
			return err
		}
		if !reflect.DeepEqual(p.Data, expected) {
			return fmt.Errorf("planner snapshot lost decision state")
		}
		if ref != first {
			firstAttempt, lastAttempt := report.Snapshot.Attempts[first.AttemptID], report.Snapshot.Attempts[ref.AttemptID]
			sameHandle := firstAttempt.HandleID == lastAttempt.HandleID
			wantRequirements := 2
			if name == "planner-reuse-handoff" {
				wantRequirements = 3
			}
			if sameHandle != (name == "planner-reuse") || p.Data.Previous == nil || (name != "planner-reuse-handoff" && *p.Data.Previous != first) || len(p.Data.Pending[0].Requirements) != wantRequirements {
				return fmt.Errorf("planner reuse/fresh reconstruction mismatch")
			}
		}
	} else if first.AttemptID != "" {
		attempt := report.Snapshot.Attempts[first.AttemptID]
		if ref != first || attempt.Output == nil || *attempt.Output != first || attempt.State != engine.Succeeded {
			return fmt.Errorf("failed planner replaced or lost last accepted state")
		}
	}
	wantError := map[string]string{
		"planner-wrong-context":        "planner context/previous mismatch",
		"planner-wrong-previous":       "planner context/previous mismatch",
		"planner-invented-previous":    "planner context/previous mismatch",
		"planner-drop-gap":             "planning cannot remove supporting context gaps",
		"planner-foreign-evidence":     "planner evidence must name an exact supporting input owner/file",
		"planner-uncommitted-evidence": "planner evidence must name an exact supporting input owner/file",
		"planner-local-evidence":       "planner evidence must name an exact supporting input owner/file",
		"planner-missing-file":         "planner evidence must name an exact supporting input owner/file",
		"planner-duplicate-id":         "planner hypotheses require unique IDs, statements and assessments",
		"planner-blank-assessment":     "planner hypotheses require unique IDs, statements and assessments",
		"planner-blank-rationale":      "planner requires rationale and nonblank gaps",
		"planner-empty-requirements":   "planner questions require concrete evidence requirements",
		"planner-uncommitted-input":    "ReferenceInvalid",
		"planner-wrong-scope":          "context source/scope mismatch",
		"planner-missing-model":        "explicit role/model/thinking and absolute cwd required",
		"planner-attempt-cap":          "LimitExceeded",
		"planner-session-cap":          "LimitExceeded",
	}
	if want := wantError[name]; want != "" && !strings.Contains(fmt.Sprint(report.Failure), want) {
		return fmt.Errorf("planner failure missing %q: %v", want, report.Failure)
	}
	for _, session := range report.Snapshot.Sessions {
		if session.Role.Name == "triage-planner" && session.Role.Model != (runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}) {
			return fmt.Errorf("planner inherited analysis model")
		}
	}
	if name == "planner-provider-failure" || name == "planner-timeout" {
		var failure *engine.Failure
		if !errors.As(report.Failure, &failure) || (name == "planner-provider-failure" && failure.Code != engine.ProviderFailed) || (name == "planner-timeout" && (failure.Code != engine.TimedOut || failure.Origin != engine.OriginAttemptDeadline)) {
			return fmt.Errorf("planner lost typed failure: %v", report.Failure)
		}
		for _, attempt := range report.Snapshot.Attempts {
			if attempt.State != engine.Succeeded && attempt.Output != nil {
				return fmt.Errorf("failed planner candidate became committed")
			}
		}
	}
	return nil
}

func corruptInputEvidence(ref contract.Ref) error {
	var p publication[json.RawMessage]
	if err := protocol.ReadJSON(ref.Path, &p); err != nil {
		return err
	}
	if len(p.Files) == 0 {
		return fmt.Errorf("fixture needs input evidence to corrupt")
	}
	return os.WriteFile(filepath.Join(filepath.Dir(ref.Path), p.Files[0].Path), []byte("changed after acceptance"), 0600)
}

type triageCase struct {
	name    string
	stages  int
	failure bool
	ready   bool
}

var triageCases = []triageCase{
	{"work-tamper-during-worker", 7, true, false},
	{"work-tamper-evidence-before-support", 4, true, false}, {"work-tamper-evidence-after-support", 7, true, false}, {"work-tamper-evidence-handoff", 8, true, false},
	{"work-read-isolation", 8, false, true}, {"work-read-cancellation", 8, false, true}, {"work-read-exact-ref", 8, false, true},
	{"work-unproposed-transition", 7, true, false}, {"work-skipped-context", 10, true, false},
	{"work-wrong-task", 7, true, false}, {"work-resolve-changed-intake", 7, true, false}, {"work-refresh-work-mismatch", 7, true, false},
	{"work-reuse", 9, false, true},
	{"work-resolve", 7, false, true}, {"work-time", 6, false, true}, {"work-refresh", 8, false, true}, {"work-update", 8, false, true},
	{"work-update-incomplete", 8, false, true}, {"work-update-partial", 8, false, false}, {"work-ticket-only", 6, false, false},
	{"work-handoff", 9, false, true}, {"work-repeat", 13, false, true},
	{"work-no-proposal", 4, true, false}, {"work-bad-kind", 4, true, false}, {"work-blank-reason", 4, true, false}, {"work-no-basis", 4, true, false},
	{"work-local-basis", 4, true, false}, {"work-foreign-basis", 4, true, false}, {"work-uncommitted-basis", 4, true, false}, {"work-missing-file", 4, true, false},
	{"work-resolve-ready", 4, true, false}, {"work-refresh-ready", 4, true, false}, {"work-refresh-empty", 4, true, false}, {"work-unknown-source", 4, true, false},
	{"work-duplicate-source", 4, true, false}, {"work-blank-source-reason", 4, true, false}, {"work-extra-sources", 4, true, false}, {"work-extra-control", 4, true, false},
	{"work-wrong-result-context", 8, true, false}, {"work-drop-gap", 8, true, false},
	{"work-worker-provider-failure", 5, true, false}, {"work-worker-timeout", 5, true, false}, {"work-worker-cancel", 5, true, false},
	{"work-planner-provider-failure", 8, true, false}, {"work-planner-timeout", 8, true, false},
	{"work-cleanup-failure", 4, true, false}, {"work-worker-cleanup-failure", 5, true, false},
	{"work-attempt-cap", 4, true, false}, {"work-final-cap", 7, true, false}, {"work-session-cap", 4, true, false},
	{"work-tamper-proposal", 4, true, false}, {"work-tamper-history", 8, true, false},
	{"planner-ready", 4, false, true}, {"planner-incomplete", 4, false, false}, {"planner-ticket-only", 4, false, false}, {"planner-empty", 4, false, true}, {"planner-no-evidence", 4, false, true}, {"planner-reuse-handoff", 6, false, true},
	{"planner-reuse", 5, false, true}, {"planner-handoff", 5, false, true}, {"planner-history", 8, false, true}, {"planner-support", 5, false, true},
	{"planner-wrong-context", 4, true, false}, {"planner-wrong-previous", 5, true, false}, {"planner-invented-previous", 4, true, false}, {"planner-drop-gap", 4, true, false},
	{"planner-foreign-evidence", 4, true, false}, {"planner-uncommitted-evidence", 4, true, false}, {"planner-local-evidence", 4, true, false}, {"planner-missing-file", 4, true, false},
	{"planner-duplicate-id", 4, true, false}, {"planner-blank-assessment", 4, true, false}, {"planner-blank-rationale", 4, true, false}, {"planner-empty-requirements", 4, true, false}, {"planner-extra-control", 4, true, false},
	{"planner-uncommitted-input", 3, true, false}, {"planner-wrong-scope", 3, true, false}, {"planner-missing-model", 3, true, false},
	{"planner-provider-failure", 5, true, false}, {"planner-cancel", 5, true, false}, {"planner-timeout", 5, true, false}, {"planner-cleanup-failure", 4, true, false}, {"planner-attempt-cap", 4, true, false}, {"planner-session-cap", 4, true, false}, {"planner-tampered-handoff", 4, true, false},
	{"support-ticket-only", 3, false, false}, {"support-wiki-partial", 3, false, false}, {"support-incomplete", 3, false, false}, {"support-wrong-epoch", 3, true, false}, {"support-missing-file", 3, true, false},
	{"support-complete", 3, false, true}, {"support-subwindow", 3, false, true}, {"support-partial", 3, false, false}, {"support-unavailable", 3, false, false}, {"support-empty", 3, false, false},
	{"support-lookup-conflict", 3, false, false}, {"support-malformed-lookup", 3, false, false}, {"support-lookup-environment", 3, true, false}, {"support-lookup-release", 3, true, false},
	{"support-zero-window", 3, true, false}, {"support-reversed-window", 3, true, false}, {"support-nonutc", 3, true, false}, {"support-no-basis", 3, true, false}, {"support-no-result", 3, true, false}, {"support-blank-filter", 3, true, false}, {"support-no-outcome", 3, true, false}, {"support-invalid-status", 3, true, false}, {"support-foreign-basis", 3, true, false}, {"support-uncommitted-result", 3, true, false},
	{"support-resolve-altered-query", 4, true, false}, {"support-resolve-retag-basis", 4, true, false},
	{"support-resolve-empty-prior", 4, false, true}, {"support-resolve-empty-next", 4, false, true}, {"support-resolve-empty-both", 4, false, true},
	{"support-resolve-success", 4, false, true}, {"support-resolve-repeat", 5, false, true}, {"support-resolve-dropped-history", 4, true, false},
	{"support-resolve-provider-failure", 4, true, false}, {"support-resolve-cancel", 4, true, false}, {"support-resolve-timeout", 4, true, false}, {"support-resolve-cleanup-failure", 4, true, false}, {"support-resolve-attempt-cap", 3, true, false},
	{"update-page", 6, false, true}, {"update-comments-repage", 6, false, true}, {"update-comments-missing", 6, false, false}, {"update-comments-false-complete", 4, true, false},
	{"update-add", 6, false, true}, {"update-remove", 6, false, true}, {"update-replace", 6, false, true}, {"update-repeat", 9, false, true},
	{"update-content", 6, false, true}, {"update-retain-content", 6, false, true}, {"update-partial", 6, false, false}, {"update-issue-failure", 6, false, false},
	{"update-retain-gap", 6, false, false}, {"update-resolve-gap", 6, false, true}, {"update-drop-gap", 6, true, false},
	{"update-history-lookup", 6, false, true}, {"update-then-resolve", 8, false, true}, {"update-then-refresh", 9, false, true},
	{"update-truncated", 4, true, false}, {"update-unrecorded-change", 4, true, false}, {"update-foreign-ref", 4, true, false}, {"update-wrong-previous", 4, true, false}, {"update-no-metadata", 4, true, false},
	{"update-wrong-task", 4, true, false}, {"update-no-issue", 4, true, false}, {"update-duplicate-work", 4, true, false}, {"update-blank-reason", 4, true, false}, {"update-unrecorded-add", 4, true, false},
	{"update-omitted-slot", 4, true, false}, {"update-new-ref", 4, true, false}, {"update-removed-still-in-raw", 4, true, false}, {"update-issue-failure-remove", 4, true, false}, {"update-history-alias", 4, true, false},
	{"update-old-wiki-binding", 5, true, false}, {"update-provider-failure", 4, true, false}, {"update-cancel", 4, true, false}, {"update-timeout", 4, true, false}, {"update-cleanup-failure", 4, true, false}, {"update-attempt-cap", 3, true, false}, {"update-uncommitted-input", 3, true, false},
	{"refresh-content-failure", 6, false, false}, {"refresh-content-failure-repeat", 9, false, false}, {"refresh-content-new-analysis", 4, true, false}, {"refresh-escalated-task", 4, true, false},
	{"analysis-without-content", 1, true, false}, {"initial-update", 1, true, false},
	{"refresh-issue-partial", 6, false, false}, {"refresh-issue-missing", 6, false, false}, {"refresh-issue-repeat", 9, false, false},
	{"refresh-page", 6, false, true}, {"refresh-attachment", 6, false, true}, {"refresh-invalidated", 6, false, true},
	{"refresh-historical-wiki", 6, false, true}, {"refresh-wiki-partial", 6, false, false}, {"refresh-repeat", 9, false, true}, {"refresh-then-resolve", 8, false, true},
	{"refresh-unselected-copy", 4, true, false}, {"refresh-foreign-ref", 4, true, false}, {"refresh-wrong-previous", 4, true, false}, {"refresh-work-changed", 4, true, false}, {"refresh-no-metadata", 4, true, false}, {"refresh-dropped-source", 4, true, false}, {"refresh-false-complete", 4, true, false},
	{"refresh-old-wiki-binding", 5, true, false}, {"refresh-false-no-matches", 5, true, false}, {"refresh-drop-gap", 6, true, false}, {"refresh-dropped-history", 6, true, false}, {"refresh-context-foreign-ref", 6, true, false},
	{"refresh-provider-failure", 4, true, false}, {"refresh-cancel", 4, true, false}, {"refresh-timeout", 4, true, false}, {"refresh-cleanup-failure", 4, true, false}, {"refresh-attempt-cap", 3, true, false}, {"refresh-uncommitted-input", 3, true, false},
	{"missing-attachment-size", 1, true, false},
	{"blank-stack", 3, true, false}, {"blank-pop", 3, true, false}, {"blank-binding", 3, true, false}, {"blank-target", 3, true, false}, {"whitespace-target", 3, true, false},
	{"duplicate-attachment", 1, true, false}, {"conflicting-attachment", 1, true, false}, {"duplicate-empty-attachment", 1, true, false}, {"null-comment", 1, true, false}, {"http-page-null", 3, false, false},
	{"http-complete", 3, false, true}, {"http-page-failure", 3, false, false}, {"http-page-total", 3, false, false}, {"http-page-empty", 3, false, false}, {"http-page-offset", 3, false, false}, {"http-page-duplicate", 3, false, false}, {"http-page-short", 3, false, false}, {"http-page-partial", 3, false, false}, {"http-malformed-issue", 3, false, false}, {"http-malformed-fields", 3, false, false}, {"http-linked-failure", 3, false, false}, {"http-attachment-partial", 3, false, false}, {"http-unsafe-zip", 3, false, false}, {"http-oversized", 3, false, false}, {"http-metadata-limit", 3, false, false},
	{"resolve-ready", 3, false, true}, {"resolve-acquisition-gap", 5, false, false},
	{"resolve-wiki", 5, false, true}, {"resolve-wiki-unavailable", 5, false, true}, {"resolve-wiki-not-run", 5, false, true}, {"resolve-wiki-partial-again", 5, false, false}, {"resolve-repeat", 7, false, true}, {"resolve-time", 4, false, true}, {"resolve-identity", 4, false, true}, {"resolve-ticket-only", 4, false, false},
	{"resolve-drop-gap", 5, true, false}, {"resolve-foreign-evidence", 5, true, false}, {"resolve-replace-valid-time", 5, true, false}, {"resolve-wrong-previous", 5, true, false}, {"resolve-old-evidence", 5, true, false}, {"resolve-dropped-history", 5, true, false},
	{"resolve-provider-failure", 4, true, false}, {"resolve-cancel", 4, true, false}, {"resolve-timeout", 4, true, false}, {"resolve-cleanup-failure", 4, true, false}, {"resolve-attempt-cap", 3, true, false}, {"resolve-uncommitted-input", 3, true, false},
	{"wiki-history-identity", 3, false, true}, {"wiki-history-conflict", 3, true, false}, {"wiki-history-environment", 3, true, false}, {"wiki-history-release", 3, true, false}, {"conflicting-foreign-lookup", 3, true, false},
	{"ticket-only", 3, false, false}, {"wiki-not-run", 3, false, false},
	{"false-ready", 3, true, false}, {"dropped-gap", 3, true, false}, {"self-paired", 3, true, false}, {"lookup-conflict", 3, true, false}, {"lookup-environment", 3, true, false}, {"lookup-release", 3, true, false}, {"missing-lookup", 3, true, false},
	{"complete", 3, false, true}, {"wiki-matches", 3, false, true}, {"epoch-millis", 3, false, true}, {"local-paired", 3, false, true}, {"dst-offsets", 3, false, true},
	{"missing-page", 3, false, false}, {"changed-total", 3, false, false}, {"missing-fields", 3, false, false}, {"unsafe-attachment", 3, false, false}, {"oversized-attachment", 3, false, false}, {"vision-pending", 3, false, false}, {"wiki-partial", 3, false, false}, {"wiki-unavailable", 3, false, false}, {"identity-conflict", 3, false, false}, {"time-unresolved", 3, false, false}, {"time-conflict", 3, false, false},
	{"false-complete", 1, true, false}, {"duplicate-comment", 1, true, false}, {"wrong-page-offset", 1, true, false}, {"missing-linked", 1, true, false}, {"missing-attachment", 1, true, false}, {"truncated-attachment", 1, true, false}, {"raw-key", 1, true, false}, {"unknown-file", 1, true, false}, {"file-escape", 1, true, false},
	{"wiki-false-empty", 2, true, false}, {"wiki-ref", 2, true, false},
	{"wrong-scope", 3, true, false}, {"wrong-environment", 3, true, false}, {"wrong-tenant", 3, true, false}, {"no-release", 3, true, false}, {"guessed-zone", 3, true, false}, {"wrong-utc", 3, true, false}, {"wrong-window", 3, true, false}, {"foreign-ref", 3, true, false}, {"uncommitted-ref", 3, true, false}, {"missing-resolution", 3, true, false}, {"no-evidence", 3, true, false}, {"local-no-pair", 3, true, false}, {"wrong-offset", 3, true, false},
	{"attempt-timeout", 1, true, false}, {"provider-failure", 1, true, false}, {"cancel", 1, true, false}, {"cleanup-failure", 1, true, false}, {"attempt-cap", 1, true, false},
}

// Data/contract scenarios use the real Store and publication validators below.
// Dispatch, cross-Step ownership, session and failure scenarios stay on RPC.
// Truncated intake, blank proposal reason and history-alias negatives also stay
// there to detect a production loader accidentally bypassing its validator.
func validationStage(name string) string {
	switch name {
	case "duplicate-attachment", "conflicting-attachment", "duplicate-empty-attachment", "missing-attachment-size", "null-comment",
		"analysis-without-content", "missing-page", "changed-total", "missing-fields", "unsafe-attachment", "oversized-attachment", "vision-pending",
		"false-complete", "duplicate-comment", "wrong-page-offset", "missing-linked", "missing-attachment", "raw-key", "unknown-file", "file-escape":
		return "intake"
	case "wiki-partial", "wiki-unavailable", "wiki-not-run", "wiki-false-empty", "wiki-ref":
		return "wiki"
	case "blank-stack", "blank-pop", "blank-binding", "blank-target", "whitespace-target", "false-ready", "dropped-gap", "self-paired",
		"lookup-conflict", "lookup-environment", "lookup-release", "missing-lookup", "conflicting-foreign-lookup",
		"wiki-history-identity", "wiki-history-conflict", "wiki-history-environment", "wiki-history-release",
		"epoch-millis", "local-paired", "dst-offsets", "identity-conflict", "time-unresolved", "time-conflict",
		"wrong-scope", "wrong-environment", "wrong-tenant", "no-release", "guessed-zone", "wrong-utc", "wrong-window", "foreign-ref", "uncommitted-ref", "missing-resolution", "no-evidence", "local-no-pair", "wrong-offset":
		return "context"
	case "support-wrong-epoch", "support-missing-file", "support-subwindow", "support-unavailable", "support-empty",
		"support-lookup-conflict", "support-malformed-lookup", "support-lookup-environment", "support-lookup-release",
		"support-zero-window", "support-reversed-window", "support-nonutc", "support-no-basis", "support-no-result", "support-blank-filter", "support-no-outcome", "support-invalid-status", "support-foreign-basis", "support-uncommitted-result":
		return "support"
	case "planner-empty", "planner-no-evidence", "planner-drop-gap", "planner-foreign-evidence", "planner-uncommitted-evidence", "planner-local-evidence", "planner-missing-file", "planner-duplicate-id", "planner-blank-assessment", "planner-blank-rationale", "planner-empty-requirements", "planner-extra-control":
		return "planner"
	case "work-bad-kind", "work-no-basis", "work-local-basis", "work-foreign-basis", "work-uncommitted-basis", "work-missing-file", "work-resolve-ready", "work-refresh-ready", "work-refresh-empty", "work-unknown-source", "work-duplicate-source", "work-blank-source-reason", "work-extra-sources", "work-extra-control":
		return "proposal"
	case "refresh-unselected-copy", "refresh-foreign-ref", "refresh-wrong-previous", "refresh-no-metadata", "refresh-dropped-source", "refresh-false-complete", "refresh-content-new-analysis", "refresh-content-failure", "refresh-issue-partial", "refresh-issue-missing",
		"update-comments-repage", "update-comments-missing", "update-comments-false-complete", "update-content", "update-retain-content", "update-truncated", "update-unrecorded-change", "update-foreign-ref", "update-wrong-previous", "update-no-metadata", "update-no-issue", "update-duplicate-work", "update-blank-reason", "update-unrecorded-add", "update-omitted-slot", "update-new-ref", "update-removed-still-in-raw", "update-issue-failure-remove", "update-issue-failure":
		return "intake-revision"
	}
	return ""
}

// Store publications here are not engine-committed dispatch inputs. These tests
// exercise schema/filesystem and publication rules, not session orchestration.
func storeFixture(t *testing.T, store *contract.Store, schema string, data any, files map[string][]byte, escape bool) (contract.Ref, error) {
	t.Helper()
	id := contract.Identity{RunID: store.RunID(), InvocationID: contract.NewID(), AttemptID: contract.NewID(), DispatchToken: contract.NewID()}
	req := contract.Request{Identity: id, Prompt: "anonymous contract validation", Output: contract.OutputSpec{SchemaID: schema}}
	attempt, err := store.BeginAttempt(id, req)
	if err != nil {
		t.Fatal(err)
	}
	writeCandidate(t, protocol.Control{CandidatePath: attempt.CandidatePath()}, req, data, files, escape)
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

func storedPublication[T any](t *testing.T, store *contract.Store, ref contract.Ref, expected ...T) publication[T] {
	t.Helper()
	raw, err := store.Read(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	var p publication[T]
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if len(expected) != 0 && !reflect.DeepEqual(p.Data, expected[0]) {
		t.Fatal("Store publication changed fixture data")
	}
	return p
}

type validationInputs struct {
	intakeRef contract.Ref
	wikiRef   contract.Ref
	intake    publication[Intake]
	wiki      publication[WikiSearch]
}

func testIntakePublicationRevision(t *testing.T, store *contract.Store, name string, base validationInputs) (bool, error) {
	t.Helper()
	updating := strings.HasPrefix(name, "update-")
	task := stageTask{Stage: "intake-revision", SourceWork: []intakeWork{{Source: "comment:1", Reason: "retry missing comment page"}}}
	if updating {
		task.Stage, task.SourceWork = "intake-update", nil
	} else if strings.HasPrefix(name, "refresh-issue-") {
		task.SourceWork = []intakeWork{{Source: "issue", Reason: "caller declared issue snapshot stale"}}
	} else if strings.HasPrefix(name, "refresh-content-") {
		task.SourceWork = []intakeWork{{Source: "attachment-content:a1", Reason: "update content without automatically repeating analysis"}}
		if name == "refresh-content-new-analysis" {
			task.SourceWork = append(task.SourceWork, intakeWork{Source: "attachment-analysis:a1", Reason: "new analysis result"})
		}
	}
	var requests atomic.Int32
	observe := func(*http.Request) { requests.Add(1) }
	var server *httptest.Server
	if updating {
		server = updateHTTPFixture(t, name, observe)
	} else {
		mode := "complete"
		if strings.HasPrefix(name, "refresh-issue-") {
			mode = "malformed-issue"
		}
		if name == "refresh-issue-missing" {
			mode = "issue-failure"
		}
		_, files := intakeFixture("complete")
		server = acquireFixture(t, mode, files["bundle"], "text/plain", observe)
	}
	v, files, fetched := refreshIntakeFixture(t, t.Context(), name, 4, []contract.Ref{base.intakeRef, base.wikiRef}, task, server.URL)
	if requests.Load() != fetched || fetched == 0 {
		t.Fatal("revision fixture did not preserve actual acquisition")
	}
	ref, err := storeFixture(t, store, IntakeSchema, v, files, false)
	if err != nil {
		return false, err
	}
	p := storedPublication[Intake](t, store, ref, v)
	if err := checkIntakeRevision(p.Data, base.intake.Data, base.intakeRef); err != nil {
		return false, err
	}
	inventory := base.intake.Data.Issue
	inventory.Ref = &base.intakeRef
	v, err = checkIntakePublication(t.Context(), ref, p, testScope().Ticket, map[contract.Ref][]file{base.intakeRef: base.intake.Files}, &inventory)
	if err != nil {
		return false, err
	}
	if v.Acquisition == nil || v.Acquisition.Ref != nil || !hasFile(p.Files, v.Acquisition.FileID) {
		t.Fatal("revision lost its own acquisition diagnostics")
	}
	switch name {
	case "update-content", "update-retain-content", "refresh-content-failure":
		oldAnalysis := base.intake.Data.Attachments[0].Analysis
		oldAnalysis.Ref = &base.intakeRef
		if !reflect.DeepEqual(v.Attachments[0].Analysis, oldAnalysis) {
			t.Fatal("revision relabeled historical analysis")
		}
		if name == "update-retain-content" {
			oldContent := base.intake.Data.Attachments[0].Content
			oldContent.Ref = &base.intakeRef
			if !reflect.DeepEqual(v.Attachments[0].Content, oldContent) {
				t.Fatal("revision relabeled historical content")
			}
		} else if v.Attachments[0].Content.Ref != nil || !hasFile(p.Files, v.Attachments[0].Content.FileID) {
			t.Fatal("revision lost new content/partial bytes")
		}
	case "update-comments-repage":
		if len(v.Comments) != 1 || v.Comments[0].Source.Ref != nil || len(base.intake.Data.Comments) != 2 {
			t.Fatal("repagination changed history or kept the obsolete page")
		}
	case "update-comments-missing":
		page := v.Comments[2].Source
		if v.Complete || page.Status != "partial" || page.Ref != nil || !hasFile(p.Files, page.FileID) {
			t.Fatal("partial page lost its status or raw evidence")
		}
	}
	return v.Complete, nil
}

func validationError(name string) string {
	switch name {
	case "duplicate-attachment", "conflicting-attachment", "duplicate-empty-attachment", "missing-attachment-size":
		return "invalid raw attachment"
	case "null-comment", "duplicate-comment":
		return "duplicate/incomplete comment"
	case "analysis-without-content", "refresh-content-new-analysis":
		return "analysis without attachment content"
	case "false-complete", "refresh-false-complete", "update-comments-false-complete":
		return "intake completeness/gaps mismatch"
	case "wrong-page-offset":
		return "invalid comment page metadata"
	case "missing-linked", "update-removed-still-in-raw", "update-issue-failure-remove":
		return "linked issue inventory omitted entries"
	case "missing-attachment", "update-omitted-slot":
		return "attachment inventory omitted entries"
	case "truncated-attachment", "update-truncated":
		return "truncated attachment"
	case "raw-key":
		return "raw issue key mismatch"
	case "unknown-file":
		return "missing evidence file"
	case "file-escape":
		return "invalid or duplicate file reference"
	case "support-invalid-status":
		return "/attempts/2/queries/1/status"
	case "planner-extra-control":
		return "additional properties 'model' not allowed"
	case "work-extra-control":
		return "/supporting_work"
	case "work-bad-kind":
		return "/supporting_work/kind"
	case "wiki-false-empty":
		return "wiki completion inconsistent"
	case "wiki-ref":
		return "wiki search provenance missing"
	case "blank-stack", "blank-pop", "blank-binding", "blank-target", "whitespace-target":
		return "resolved identity requires complete target facts"
	case "false-ready":
		return "context readiness/gaps mismatch"
	case "dropped-gap":
		return "context dropped upstream gap"
	case "self-paired":
		return "local timestamp cannot be its own absolute evidence"
	case "lookup-conflict", "lookup-environment", "lookup-release", "wiki-history-conflict", "wiki-history-environment", "wiki-history-release", "wrong-environment", "wrong-tenant", "no-release", "support-lookup-environment", "support-lookup-release":
		return "identity conflicts with target/DB resolution receipt"
	case "missing-lookup":
		return "resolved identity requires target/DB resolution receipt"
	case "conflicting-foreign-lookup", "foreign-ref", "uncommitted-ref", "support-foreign-basis", "support-uncommitted-result":
		return "evidence is not an exact committed input"
	case "wrong-scope":
		return "context source/scope mismatch"
	case "guessed-zone", "wrong-utc":
		return "UTC conversion mismatch"
	case "wrong-window":
		return "UTC window must match observed anchors"
	case "missing-resolution":
		return "active identity and time resolution required"
	case "no-evidence":
		return "fact lacks evidence"
	case "local-no-pair":
		return "local timestamp needs same-event absolute evidence"
	case "wrong-offset", "support-wrong-epoch":
		return "calculated local/epoch offset mismatch"
	case "support-missing-file":
		return "unknown evidence file"
	case "support-zero-window", "support-reversed-window", "support-nonutc":
		return "supporting query requires a nonzero UTC window"
	case "support-no-basis", "support-no-result", "support-blank-filter", "support-no-outcome":
		return "supporting query lacks conditions, basis or result evidence"
	case "planner-drop-gap":
		return "planning cannot remove supporting context gaps"
	case "planner-foreign-evidence", "planner-uncommitted-evidence", "planner-local-evidence", "planner-missing-file":
		return "planner evidence must name an exact supporting input owner/file"
	case "planner-duplicate-id", "planner-blank-assessment":
		return "planner hypotheses require unique IDs, statements and assessments"
	case "planner-blank-rationale":
		return "planner requires rationale and nonblank gaps"
	case "planner-empty-requirements":
		return "planner questions require concrete evidence requirements"
	case "work-blank-reason", "work-no-basis":
		return "supporting work requires reason and basis"
	case "work-local-basis", "work-foreign-basis", "work-uncommitted-basis", "work-missing-file":
		return "supporting work basis must name an exact supporting input owner/file"
	case "work-resolve-ready":
		return "supporting resolve requires needs-resolution context"
	case "work-refresh-ready", "work-refresh-empty":
		return "local intake revision requires incomplete intake and explicit work"
	case "work-unknown-source":
		return "unknown source work selector"
	case "work-duplicate-source", "work-blank-source-reason", "update-duplicate-work", "update-blank-reason":
		return "source work requires unique selectors and reasons"
	case "work-extra-sources":
		return "only refresh accepts source selectors"
	case "refresh-unselected-copy", "refresh-foreign-ref", "update-unrecorded-change", "update-foreign-ref", "update-history-alias":
		return "unselected source must retain exact prior provenance"
	case "refresh-wrong-previous", "update-wrong-previous":
		return "intake revision must bind exact previous ticket/source"
	case "refresh-no-metadata", "update-no-metadata":
		return "intake revision requires own acquisition metadata/diagnostics"
	case "refresh-dropped-source":
		return "intake revision dropped unrecorded source"
	case "update-no-issue":
		return "intake update requires a new issue result"
	case "update-unrecorded-add":
		return "unrequested source added"
	case "update-new-ref":
		return "source work needs a new local result"
	}
	return ""
}

func validationRejection(name string, err error) error {
	if err == nil {
		return fmt.Errorf("expected validation rejection")
	}
	phase := ""
	switch name {
	case "file-escape":
		phase = "files"
	case "support-invalid-status", "planner-extra-control", "work-extra-control", "work-bad-kind":
		phase = "schema"
	}
	var execution *engine.Failure
	expectedCode := engine.WorkflowFailed
	if phase != "" {
		expectedCode = engine.ContractInvalid
	}
	if errors.As(err, &execution) && execution.Code != expectedCode {
		return fmt.Errorf("expected %s classification, not execution failure: %w", expectedCode, err)
	}
	var failure *contract.Error
	var pathError *os.PathError
	if phase != "" {
		if !errors.As(err, &failure) || failure.Code != contract.ContractInvalid || failure.Phase != phase {
			return fmt.Errorf("expected ContractInvalid at %s, not execution failure: %w", phase, err)
		}
	} else if errors.As(err, &failure) || errors.As(err, &pathError) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("expected semantic rejection, not execution/Store failure: %w", err)
	}
	want := validationError(name)
	if want == "" || !strings.Contains(err.Error(), want) {
		return fmt.Errorf("expected rejection containing %q: %w", want, err)
	}
	if name == "work-extra-control" && !strings.Contains(err.Error(), "additional properties 'model' not allowed") {
		return fmt.Errorf("expected extra model control rejection: %w", err)
	}
	return nil
}

func newValidationStore(t *testing.T) *contract.Store {
	t.Helper()
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	store, err := contract.NewStore(registry, contract.Options{BaseDir: t.TempDir(), Prompt: "anonymous validation cases", Limits: contract.DefaultLimits()})
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

func TestValidationRejectionFailures(t *testing.T) {
	store := newValidationStore(t)
	for _, mode := range []string{"report-storage", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			id := contract.Identity{RunID: store.RunID(), InvocationID: contract.NewID(), AttemptID: contract.NewID(), DispatchToken: contract.NewID()}
			req := contract.Request{Identity: id, Prompt: "anonymous rejection diagnostics", Output: contract.OutputSpec{SchemaID: PlannerSchema}}
			attempt, err := store.BeginAttempt(id, req)
			if err != nil {
				t.Fatal(err)
			}
			v := PlannerState{Hypotheses: []PlannerHypothesis{}, Pending: []PlannerQuestion{}, Gaps: []string{}, SupportingWork: &SupportingWork{Kind: "logs", Basis: []Evidence{}, Sources: []intakeWork{}}}
			writeCandidate(t, protocol.Control{CandidatePath: attempt.CandidatePath()}, req, v, nil, false)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "report-storage" {
				if err := os.Mkdir(filepath.Join(attempt.Dir(), "validation.json"), 0700); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			_, err = attempt.Stage(ctx, contract.Spec{SchemaID: PlannerSchema})
			if mode == "report-storage" {
				var failure *contract.Error
				if !errors.As(err, &failure) || failure.Code != contract.StorageFailed || !strings.Contains(err.Error(), "/supporting_work/kind") {
					t.Fatalf("fixture did not retain storage failure around the intended schema rejection: %v", err)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("fixture did not cancel Stage: %v", err)
			}
			if validationRejection("work-bad-kind", err) == nil {
				t.Fatal("execution failure counted as expected schema rejection")
			}
		})
	}
}

func assertSupportingData(t *testing.T, p publication[Context], wantQueries int) {
	t.Helper()
	count := 0
	for _, attempt := range p.Data.Attempts {
		for _, q := range attempt.Queries {
			count++
			if q.From == p.Data.Time.From && q.To == p.Data.Time.To {
				t.Fatal("query window was conflated with observed interval")
			}
			if q.Filter == "tenant:17" && q.Status != "partial" {
				t.Fatal("successful narrow query erased earlier partial status")
			}
		}
	}
	if count != wantQueries {
		t.Fatalf("query history count=%d, want=%d", count, wantQueries)
	}
	for _, id := range []string{"events-broad", "events-narrow", "events-broad-metadata", "events-narrow-metadata"} {
		if !hasFile(p.Files, id) {
			t.Fatalf("missing raw query evidence %s", id)
		}
	}
}

func TestTriageValidation(t *testing.T) {
	store := newValidationStore(t)
	// Shared inputs are immutable Store publications, never a reused Agent or
	// engine Run. Each scenario produces its own candidate and local evidence.
	bases := map[string]validationInputs{}
	inputs := func(t *testing.T, key string) validationInputs {
		t.Helper()
		if base, ok := bases[key]; ok {
			return base
		}
		intakeMode, wikiMode := "complete", "complete"
		switch key {
		case "missing-page", "revision-incomplete":
			intakeMode = "missing-page"
		case "support":
			intakeMode = "support-complete"
		case "wiki-partial":
			wikiMode = key
		default:
			if strings.HasPrefix(key, "wiki-history-") {
				wikiMode = key
			}
		}
		iv, files := intakeFixture(intakeMode)
		if key == "revision-incomplete" {
			iv.Comments = append(iv.Comments, CommentPage{Start: 1, Source: unavailable(iv.Gaps[0])})
		}
		ir, err := storeFixture(t, store, IntakeSchema, iv, files, false)
		if err != nil {
			t.Fatal(err)
		}
		ip := storedPublication[Intake](t, store, ir)
		if _, err := checkIntakePublication(t.Context(), ir, ip, testScope().Ticket, nil, nil); err != nil {
			t.Fatal("invalid test prerequisite: ", err)
		}
		wv, files := wikiFixture(wikiMode, ir)
		wr, err := storeFixture(t, store, WikiSchema, wv, files, false)
		if err != nil {
			t.Fatal(err)
		}
		wp := storedPublication[WikiSearch](t, store, wr)
		if _, err := checkWikiPublication(wp, ir); err != nil {
			t.Fatal("invalid test prerequisite: ", err)
		}
		base := validationInputs{ir, wr, ip, wp}
		bases[key] = base
		return base
	}
	for _, tc := range triageCases {
		stage := validationStage(tc.name)
		if stage == "" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			key := "complete"
			switch {
			case stage == "support":
				key = "support"
			case tc.name == "false-ready" || tc.name == "dropped-gap":
				key = "missing-page"
			case tc.name == "planner-drop-gap":
				key = "wiki-partial"
			case strings.HasPrefix(tc.name, "wiki-history-"):
				key = tc.name
			case stage == "intake-revision":
				key = "revision-complete"
				if strings.HasPrefix(tc.name, "refresh-") {
					key = "revision-incomplete"
				}
			case stage == "proposal" && (strings.HasPrefix(tc.name, "work-refresh-") || tc.name == "work-unknown-source" || tc.name == "work-duplicate-source" || tc.name == "work-blank-source-reason") && tc.name != "work-refresh-ready":
				key = "missing-page"
			}
			base := inputs(t, key)
			scope := testScope()
			switch tc.name {
			case "blank-stack":
				scope.Stack = ""
			case "blank-pop":
				scope.Pop = ""
			case "blank-binding":
				scope.Binding = ""
			case "blank-target":
				scope.Stack, scope.Pop, scope.Binding = "", "", ""
			case "whitespace-target":
				scope.Stack, scope.Pop, scope.Binding = " ", " ", " "
			}
			var err error
			var ready bool
			sources := map[contract.Ref][]file{base.intakeRef: base.intake.Files, base.wikiRef: base.wiki.Files}
			switch stage {
			case "intake":
				v, files := intakeFixture(tc.name)
				var ref contract.Ref
				ref, err = storeFixture(t, store, IntakeSchema, v, files, tc.name == "file-escape")
				if err == nil {
					v, err = checkIntakePublication(t.Context(), ref, storedPublication[Intake](t, store, ref, v), scope.Ticket, nil, nil)
					ready = v.Complete
				}
			case "wiki":
				v, files := wikiFixture(tc.name, base.intakeRef)
				var ref contract.Ref
				ref, err = storeFixture(t, store, WikiSchema, v, files, false)
				if err == nil {
					v, err = checkWikiPublication(storedPublication[WikiSearch](t, store, ref, v), base.intakeRef)
					ready = wikiComplete(v)
				}
			case "context", "support":
				refs := []contract.Ref{base.intakeRef, base.wikiRef}
				v, files := contextFixture(tc.name, scope, refs, base.intake.Data, base.wiki.Data)
				if stage == "support" {
					var requests atomic.Int32
					server := supportingHTTPFixture(t, tc.name, &requests)
					v, files = supportingContextFixture(t, t.Context(), server.URL, tc.name, 0, stageTask{RuntimeResolutionAllowed: true}, refs, v, files)
					if requests.Load() != 3 {
						t.Fatal("supporting fixture did not preserve actual acquisition")
					}
				}
				var ref contract.Ref
				ref, err = storeFixture(t, store, ContextSchema, v, files, false)
				if err == nil {
					p := storedPublication[Context](t, store, ref, v)
					v, err = checkContextPublication(t.Context(), ref, p, scope, base.intakeRef, base.wikiRef, base.intake.Data, base.wiki.Data, sources)
					ready = v.Readiness == "ready"
					if err == nil && !tc.failure && stage == "support" {
						assertSupportingData(t, p, 2)
					}
				}
			case "planner", "proposal":
				cv, files := contextFixture("complete", scope, []contract.Ref{base.intakeRef, base.wikiRef}, base.intake.Data, base.wiki.Data)
				cr, e := storeFixture(t, store, ContextSchema, cv, files, false)
				if e != nil {
					t.Fatal(e)
				}
				cp := storedPublication[Context](t, store, cr)
				if _, e := checkContextPublication(t.Context(), cr, cp, scope, base.intakeRef, base.wikiRef, base.intake.Data, base.wiki.Data, sources); e != nil {
					t.Fatal("invalid test prerequisite: ", e)
				}
				sources[cr] = cp.Files
				h := contextHistory{ref: cr, value: cp.Data, sources: sources}
				req := contract.Request{Inputs: []contract.Ref{cr, base.intakeRef, base.wikiRef}}
				task := stageTask{Gaps: cp.Data.Gaps}
				var v PlannerState
				if stage == "proposal" {
					kind := "update"
					if tc.name == "work-resolve-ready" {
						kind = "resolve"
					} else if strings.HasPrefix(tc.name, "work-refresh-") || tc.name == "work-unknown-source" || tc.name == "work-duplicate-source" || tc.name == "work-blank-source-reason" {
						kind = "refresh"
					}
					v = plannerWorkFixture(t, tc.name, kind, req, task, 1, base.intakeRef)
				} else {
					v = plannerFixture(t, tc.name, req, task, 1, base.intakeRef)
				}
				var data any = v
				if tc.name == "planner-extra-control" || tc.name == "work-extra-control" {
					var extra map[string]any
					if err := json.Unmarshal(testJSON(v), &extra); err != nil {
						t.Fatal(err)
					}
					if stage == "proposal" {
						extra["supporting_work"].(map[string]any)["model"] = "agent-selected-model"
					} else {
						extra["model"] = "agent-selected-model"
					}
					data = extra
				}
				var ref contract.Ref
				ref, err = storeFixture(t, store, PlannerSchema, data, nil, false)
				if err == nil {
					v = storedPublication[PlannerState](t, store, ref, v).Data
					err = checkPlannerSnapshot(v, h)
					if err == nil {
						err = checkSupportingProposal(h, v.SupportingWork)
					}
					if err == nil && v.SupportingWork != nil && v.SupportingWork.Kind == "refresh" {
						err = checkIntakeWork(base.intake.Data, v.SupportingWork.Sources)
					}
					ready = cp.Data.Readiness == "ready"
				}
			case "intake-revision":
				ready, err = testIntakePublicationRevision(t, store, tc.name, base)
			default:
				t.Fatal("unknown validation stage")
			}
			if (err != nil) != tc.failure || (!tc.failure && ready != tc.ready) {
				t.Fatalf("stage=%s ready=%t error=%v", stage, ready, err)
			}
			if tc.failure {
				if err := validationRejection(tc.name, err); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestIntakeToContext(t *testing.T) {
	for _, tc := range triageCases {
		if validationStage(tc.name) != "" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			working := strings.HasPrefix(tc.name, "work-")
			transitionProbe := slices.Contains([]string{"work-unproposed-transition", "work-skipped-context", "work-wrong-task", "work-resolve-changed-intake", "work-refresh-work-mismatch"}, tc.name)
			workKind, workFixture := "update", "update-replace"
			switch tc.name {
			case "work-resolve", "work-time", "work-ticket-only", "work-resolve-ready", "work-resolve-changed-intake":
				workKind = "resolve"
			case "work-refresh", "work-refresh-ready", "work-refresh-empty", "work-unknown-source", "work-duplicate-source", "work-blank-source-reason", "work-refresh-work-mismatch":
				workKind, workFixture = "refresh", "refresh-page"
			case "work-wrong-task":
				workKind, workFixture = "refresh", "update-page"
			case "work-update-partial", "work-drop-gap":
				workFixture = "update-partial"
			case "work-update-incomplete":
				workFixture = "update-page"
			}
			supporting := strings.HasPrefix(tc.name, "support-")
			resolving := strings.HasPrefix(tc.name, "resolve-") || strings.HasPrefix(tc.name, "support-resolve-") || (working && workKind == "resolve")
			refreshing := strings.HasPrefix(tc.name, "refresh-") || (working && workKind == "refresh")
			planning := strings.HasPrefix(tc.name, "planner-") || working
			updating := strings.HasPrefix(tc.name, "update-") || tc.name == "planner-history" || (working && workKind == "update") || tc.name == "work-wrong-task" || tc.name == "work-resolve-changed-intake"
			revising := refreshing || updating
			acquiring := (resolving && !supporting) || revising || strings.HasPrefix(tc.name, "http-")
			mode := tc.name
			if revising {
				mode = "wiki-matches"
			}
			if tc.name == "update-history-lookup" {
				mode = "wiki-history-identity"
			}
			if resolving {
				mode = "wiki-partial"
				switch tc.name {
				case "resolve-ready":
					mode = "complete"
				case "resolve-time":
					mode = "time-unresolved"
				case "resolve-identity":
					mode = "identity-conflict"
				case "resolve-ticket-only":
					mode = "ticket-only"
				case "resolve-wiki-unavailable":
					mode = "wiki-unavailable"
				case "resolve-wiki-not-run":
					mode = "wiki-not-run"
				}
			}
			if supporting {
				mode = tc.name
				switch tc.name {
				case "support-ticket-only":
					mode = "ticket-only"
				case "support-wiki-partial":
					mode = "wiki-partial"
				case "support-incomplete":
					mode = "missing-page"
				}
			}
			if planning && !revising {
				mode = "complete"
				if tc.name == "planner-incomplete" || tc.name == "planner-drop-gap" {
					mode = "wiki-partial"
				}
				if tc.name == "planner-ticket-only" {
					mode = "ticket-only"
				}
			}
			if working {
				mode = "complete"
				switch tc.name {
				case "work-resolve", "work-resolve-changed-intake":
					mode = "wiki-partial"
				case "work-time":
					mode = "time-unresolved"
				case "work-ticket-only":
					mode = "ticket-only"
				}
			}
			scope := testScope()
			if mode == "ticket-only" {
				scope = Scope{Ticket: "CASE-17", TenantIDs: []string{}}
			}
			switch mode {
			case "blank-stack":
				scope.Stack = ""
			case "blank-pop":
				scope.Pop = ""
			case "blank-binding":
				scope.Binding = ""
			case "blank-target":
				scope.Stack, scope.Pop, scope.Binding = "", "", ""
			case "whitespace-target":
				scope.Stack, scope.Pop, scope.Binding = " ", " ", " "
			}
			targetAuthorized := strings.TrimSpace(scope.Stack) != "" && strings.TrimSpace(scope.Pop) != "" && strings.TrimSpace(scope.Binding) != "" && len(scope.TenantIDs) > 0
			var supportingRequests atomic.Int32
			var supportingURL string
			if supporting || tc.name == "planner-support" {
				supportingURL = supportingHTTPFixture(t, tc.name, &supportingRequests).URL
			}
			var requests, newRequests atomic.Int32
			var revisionURL string
			var acquisition acquisitionOptions
			if acquiring {
				_, raw := intakeFixture("complete")
				bundle, mime := raw["bundle"], "text/plain"
				if tc.name == "http-unsafe-zip" {
					var archive bytes.Buffer
					writer := zip.NewWriter(&archive)
					entry, err := writer.Create("../escape")
					if err != nil {
						t.Fatal(err)
					}
					if _, err := entry.Write([]byte("unsafe")); err != nil {
						t.Fatal(err)
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
					bundle = archive.Bytes()
					mime = "application/zip"
				}
				if tc.name == "http-oversized" {
					bundle = []byte(strings.Repeat("x", 8193))
				}
				httpMode := strings.TrimPrefix(tc.name, "http-")
				if tc.name == "resolve-acquisition-gap" || refreshing || tc.name == "update-then-refresh" || tc.name == "update-page" || tc.name == "update-wrong-task" {
					httpMode = "page-failure"
				}
				if tc.name == "refresh-attachment" || tc.name == "update-retain-gap" || tc.name == "update-drop-gap" || tc.name == "update-resolve-gap" {
					httpMode = "attachment-partial"
				}
				if working && (tc.name == "work-update-incomplete" || (workKind == "refresh" && tc.name != "work-refresh-ready")) {
					httpMode = "page-failure"
				}
				if tc.name == "work-refresh-ready" {
					httpMode = "complete"
				}
				server := acquireFixture(t, httpMode, bundle, mime, func(*http.Request) { requests.Add(1) })
				acquisition = acquisitionOptions{BaseURL: server.URL}
				if refreshing {
					revisionMode := "complete"
					if strings.HasPrefix(tc.name, "refresh-issue-") {
						revisionMode = "malformed-issue"
					}
					if tc.name == "refresh-issue-missing" {
						revisionMode = "issue-failure"
					}
					revisionServer := acquireFixture(t, revisionMode, bundle, mime, func(req *http.Request) {
						newRequests.Add(1)
						if strings.HasPrefix(tc.name, "refresh-issue-") {
							if req.URL.Path != "/rest/api/3/issue/CASE-17" {
								t.Error("revision fetched an unrequested source")
							}
							return
						}
						if strings.HasPrefix(tc.name, "refresh-content-") {
							if req.URL.Path != "/attachment" {
								t.Error("content refresh fetched another source")
							}
							return
						}
						if req.URL.Path != "/attachment" && (req.URL.Path != "/rest/api/3/issue/CASE-17/comment" || req.URL.Query().Get("startAt") != "1") {
							t.Error("revision fetched an unrequested source")
						}
					})
					revisionURL = revisionServer.URL
				}
				if updating {
					name := tc.name
					if working {
						name = workFixture
					}
					revisionURL = updateHTTPFixture(t, name, func(*http.Request) { newRequests.Add(1) }).URL
				}
				if tc.name == "http-oversized" {
					acquisition.MaxBytes = 8192
				}
				if tc.name == "http-metadata-limit" {
					acquisition.MaxBytes = 2048
				}
			}
			dir := t.TempDir()
			bridge := filepath.Join(dir, "bridge")
			if err := os.Mkdir(bridge, 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			type event struct {
				conn net.Conn
				m    protocol.Control
			}
			events := make(chan event, 16)
			var readers sync.WaitGroup
			var mu sync.Mutex
			var conns []net.Conn
			acceptDone := make(chan struct{})
			go func() {
				defer close(acceptDone)
				for {
					c, err := listener.Accept()
					if err != nil {
						return
					}
					mu.Lock()
					conns = append(conns, c)
					mu.Unlock()
					readers.Add(1)
					go func() {
						defer readers.Done()
						dec := json.NewDecoder(c)
						for {
							var m protocol.Control
							if err := dec.Decode(&m); err != nil {
								return
							}
							select {
							case events <- event{c, m}:
							case <-ctx.Done():
								return
							}
						}
					}()
				}
			}()
			var r *engine.Run
			done := make(chan struct{})
			var report engine.Report
			var result ContextResult
			var beforeResolution ContextResult
			var plannerRef, firstPlannerRef contract.Ref
			var plannerSteps int
			var expectedPlanner PlannerState
			t.Cleanup(func() {
				cancel()
				_ = listener.Close()
				<-acceptDone
				mu.Lock()
				for _, c := range conns {
					_ = c.Close()
				}
				mu.Unlock()
				if r != nil {
					r.Cancel(engine.OriginControllerUser)
					select {
					case <-done:
					case <-time.After(8 * time.Second):
						t.Error("fixture run did not join")
					}
				}
				readers.Wait()
			})
			policy := slicePolicy()
			policy.RunTimeout = time.Nanosecond
			policy.Runtime.StartupTimeout = 5 * time.Second
			policy.Runtime.CleanupTimeout = 3 * time.Second
			policy.Runtime.AbortGrace = 50 * time.Millisecond
			policy.Runtime.HealthInterval = time.Hour
			if tc.name == "attempt-cap" {
				policy.MaxTotalAttempts = 1
			}
			if tc.name == "resolve-attempt-cap" || tc.name == "refresh-attempt-cap" || tc.name == "update-attempt-cap" || tc.name == "support-resolve-attempt-cap" {
				policy.MaxTotalAttempts = 3
			}
			if tc.name == "planner-attempt-cap" || tc.name == "work-attempt-cap" {
				policy.MaxTotalAttempts = 4
			}
			if tc.name == "work-final-cap" {
				policy.MaxTotalAttempts = 7
			}
			if tc.name == "planner-session-cap" || tc.name == "work-session-cap" {
				policy.MaxTotalSessions = 4
				policy.MaxLiveSessions = 4
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			pi, err := runtime.New(runtime.Options{Executable: exe, Args: []string{"-test.run=^TestTriageProtocolSubprocess$", "--"}, Env: []string{"PWC_TRIAGE_PROTOCOL=1", "PWC_ENGINE_MANUAL_CANDIDATE=1", "PWC_ENGINE_CONTROL=" + listener.Addr().String(), "GORACE=atexit_sleep_ms=0"}, BridgeDir: bridge, Policy: policy.Runtime, Observe: func(ctx context.Context, o runtime.Observation) error { return r.Observe(ctx, o) }})
			if err != nil {
				t.Fatal(err)
			}
			registry, err := contract.NewRegistry(Resources(), Schemas())
			if err != nil {
				t.Fatal(err)
			}
			def := engine.Definition{Name: "anonymous-triage-slice", Version: "1", Policy: policy, Execute: func(ctx context.Context, run *engine.Run, _ engine.Input) (engine.Result, error) {
				var err error
				models := sliceModels{FetchThinking: "high", Analysis: runtime.ModelSpec{Provider: "fixture", ID: "analysis", Thinking: "high"}}
				result, err = executeSlice(ctx, run, scope, models)
				if err == nil && resolving && !working {
					beforeResolution = result
					if tc.name == "resolve-uncommitted-input" {
						result.Context.Path = filepath.Join(filepath.Dir(result.Context.Path), "candidate.json")
					}
					result, err = resolveSlice(ctx, run, scope, models, result)
					if err == nil && (tc.name == "resolve-repeat" || tc.name == "support-resolve-repeat") {
						result, err = resolveSlice(ctx, run, scope, models, result)
					}
				}
				if err == nil && revising && !working {
					beforeResolution = result
					if tc.name == "refresh-uncommitted-input" || tc.name == "update-uncommitted-input" {
						result.Context.Path = filepath.Join(filepath.Dir(result.Context.Path), "candidate.json")
					}
					work := []intakeWork{{Source: "comment:1", Reason: "retry missing comment page"}}
					if tc.name == "refresh-attachment" {
						work = []intakeWork{{Source: "attachment-content:a1", Reason: "retry truncated attachment"}, {Source: "attachment-analysis:a1", Reason: "extract replacement content"}}
					}
					if tc.name == "refresh-invalidated" {
						work = append(work, intakeWork{Source: "attachment-content:a1", Reason: "caller declared prior attachment stale"}, intakeWork{Source: "attachment-analysis:a1", Reason: "extract replacement content"})
					}
					if strings.HasPrefix(tc.name, "refresh-issue-") {
						work = []intakeWork{{Source: "issue", Reason: "caller declared issue snapshot stale"}}
					}
					if strings.HasPrefix(tc.name, "refresh-content-") {
						work = []intakeWork{{Source: "attachment-content:a1", Reason: "update content without automatically repeating analysis"}}
						if tc.name == "refresh-content-new-analysis" {
							work = append(work, intakeWork{Source: "attachment-analysis:a1", Reason: "new analysis result"})
						}
					}
					if updating {
						result, err = updateSlice(ctx, run, scope, models, result)
					} else {
						result, err = refreshSlice(ctx, run, scope, models, result, work)
					}
					if err == nil && tc.name == "update-repeat" {
						result, err = updateSlice(ctx, run, scope, models, result)
					}
					if err == nil && (tc.name == "refresh-repeat" || tc.name == "refresh-issue-repeat" || tc.name == "refresh-content-failure-repeat" || tc.name == "update-then-refresh") {
						result, err = refreshSlice(ctx, run, scope, models, result, work)
					}
					if err == nil && (tc.name == "refresh-then-resolve" || tc.name == "update-then-resolve") {
						result, err = resolveSlice(ctx, run, scope, models, result)
					}
				}
				if err == nil && planning {
					model := runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}
					if tc.name == "planner-missing-model" {
						model = runtime.ModelSpec{}
					}
					input, plannerScope := result.Context, scope
					if tc.name == "planner-uncommitted-input" {
						input.Path = filepath.Join(filepath.Dir(input.Path), "candidate.json")
					}
					if tc.name == "planner-wrong-scope" {
						plannerScope.Pop = "other-pop"
					}
					if working {
						beforeResolution = result
					}
					var planner *plannerCaller
					planner, err = startPlanner(ctx, run, plannerScope, model, input)
					if err == nil {
						plannerRef, err = planner.step(ctx)
						firstPlannerRef = plannerRef
					}
					if err == nil && strings.HasPrefix(tc.name, "work-read-") {
						readCtx, cancelRead := context.WithCancel(ctx)
						a := newAcceptance(readCtx, run)
						original, readErr := readAccepted[PlannerState](a, plannerRef, PlannerSchema)
						if readErr != nil {
							err = readErr
						} else {
							switch tc.name {
							case "work-read-isolation":
								want := original.Data.Pending[0].Requirements[0]
								original.Data.Pending[0].Requirements[0] = "mutated by this caller"
								again, e := readAccepted[PlannerState](a, plannerRef, PlannerSchema)
								if e != nil || again.Data.Pending[0].Requirements[0] != want {
									t.Error("acceptance read shared mutable decoded state")
								}
								owner, e := readAccepted[Intake](a, beforeResolution.Intake, IntakeSchema)
								if e != nil || len(owner.Files) == 0 {
									t.Errorf("fixture intake lacks readable files: %v", e)
									break
								}
								wantFile := owner.Files[0]
								owner.Files[0].ID = "mutated file mapping"
								owner, e = readAccepted[Intake](a, beforeResolution.Intake, IntakeSchema)
								if e != nil || len(owner.Files) == 0 || owner.Files[0] != wantFile {
									t.Error("acceptance read shared mutable file mappings")
								}
							case "work-read-cancellation":
								cancelRead()
								if _, e := readAccepted[PlannerState](a, plannerRef, PlannerSchema); !errors.Is(e, context.Canceled) {
									t.Errorf("repeated read swallowed cancellation: %v", e)
								}
							case "work-read-exact-ref":
								for _, field := range []string{"run", "attempt", "path", "schema", "digest", "manifest"} {
									bad, schema := plannerRef, PlannerSchema
									switch field {
									case "run":
										bad.RunID = "foreign"
									case "attempt":
										bad.AttemptID = "unknown"
									case "path":
										bad.Path = filepath.Join(filepath.Dir(bad.Path), "candidate.json")
									case "schema":
										bad.SchemaID, schema = ContextSchema, ContextSchema
									case "digest":
										bad.SHA256 = strings.Repeat("0", 64)
									case "manifest":
										bad.ManifestSHA256 = strings.Repeat("0", 64)
									}
									if _, e := readAccepted[PlannerState](a, bad, schema); e == nil || !strings.Contains(e.Error(), "ReferenceInvalid") {
										t.Errorf("repeated read ignored exact %s binding: %v", field, e)
									}
								}
								if _, e := readAccepted[PlannerState](a, plannerRef, ContextSchema); e == nil || !strings.Contains(e.Error(), "expected "+ContextSchema) {
									t.Errorf("repeated read ignored required schema: %v", e)
								}
							}
						}
						cancelRead()
					}
					if err == nil && transitionProbe {
						err = planner.close(ctx)
						other := result
						if err == nil && tc.name == "work-refresh-work-mismatch" {
							other, err = refreshSlice(ctx, run, scope, models, other, []intakeWork{{Source: "comment:1", Reason: "missing comment page"}, {Source: "attachment-content:a1", Reason: "separate caller task"}, {Source: "attachment-analysis:a1", Reason: "mechanical extraction"}})
						} else if err == nil {
							other, err = updateSlice(ctx, run, scope, models, other)
						}
						if err == nil && tc.name == "work-skipped-context" {
							other, err = updateSlice(ctx, run, scope, models, other)
						}
						if err == nil {
							sessions := len(run.Snapshot().Sessions)
							_, err = openPlanner(ctx, run, scope, model, other.Context, &plannerRef)
							if err == nil || len(run.Snapshot().Sessions) != sessions {
								t.Error("unapproved context transition opened a Planner session")
							}
						}
					}
					if err == nil && working {
						if tc.name == "work-tamper-evidence-before-support" {
							err = corruptInputEvidence(beforeResolution.Intake)
						}
						if tc.name == "work-tamper-proposal" {
							err = os.WriteFile(plannerRef.Path, []byte("{}"), 0600)
						}
						cycles := 1
						if tc.name == "work-repeat" {
							cycles = 2
						}
						for cycle := 0; cycle < cycles && err == nil; cycle++ {
							var next *plannerCaller
							next, err = planner.support(ctx, models)
							if tc.name == "work-tamper-during-worker" && (err == nil || !strings.Contains(err.Error(), "supporting result acceptance: ")) {
								t.Errorf("worker mutation was not rejected by post-worker acceptance: %v", err)
							}
							if err == nil {
								if _, reused := planner.support(ctx, models); reused == nil {
									t.Error("consumed supporting proposal was dispatched twice")
								}
								planner = next
								if tc.name == "work-tamper-evidence-after-support" {
									err = corruptInputEvidence(beforeResolution.Intake)
									if err != nil {
										break
									}
								}
								var accepted contract.Ref
								accepted, err = planner.step(ctx)
								if err == nil {
									plannerRef = accepted
								}
							}
						}
						if err == nil && tc.name == "work-reuse" {
							plannerRef, err = planner.step(ctx)
						}
						if err == nil && tc.name == "work-tamper-history" {
							err = os.WriteFile(firstPlannerRef.Path, []byte("{}"), 0600)
						}
						if err == nil && tc.name == "work-tamper-evidence-handoff" {
							err = corruptInputEvidence(beforeResolution.Intake)
						}
						if err == nil && (tc.name == "work-handoff" || tc.name == "work-repeat" || tc.name == "work-tamper-history" || tc.name == "work-tamper-evidence-handoff") {
							var next *plannerCaller
							next, err = planner.handoff(ctx)
							if err == nil {
								planner = next
								plannerRef, err = planner.step(ctx)
							}
						}
						if err == nil {
							h := planner.history
							result = ContextResult{Intake: h.value.Intake, Wiki: h.value.Wiki, Context: h.ref, Ready: h.value.Readiness == "ready"}
						}
					}
					if err == nil && tc.name == "planner-tampered-handoff" {
						// Filesystem boundary corruption must be detected before launch.
						err = os.WriteFile(plannerRef.Path, []byte("{}"), 0600)
					}
					if err == nil && tc.name == "planner-reuse-handoff" {
						plannerRef, err = planner.step(ctx)
					}
					if err == nil && (tc.name == "planner-reuse" || tc.name == "planner-reuse-handoff" || tc.name == "planner-handoff" || tc.name == "planner-history" || tc.name == "planner-support" || tc.name == "planner-wrong-previous" || strings.HasSuffix(tc.name, "-failure") || strings.HasSuffix(tc.name, "-cancel") || strings.HasSuffix(tc.name, "-timeout") || strings.HasSuffix(tc.name, "-cap") || tc.name == "planner-tampered-handoff") {
						if tc.name != "planner-reuse" {
							var next *plannerCaller
							next, err = planner.handoff(ctx)
							if err == nil {
								planner = next
							}
						}
						if err == nil {
							var next contract.Ref
							next, err = planner.step(ctx)
							if err == nil {
								plannerRef = next
							}
						}
					}
					if err == nil {
						err = planner.close(ctx)
					}
					if err != nil && planner != nil {
						if (plannerRef.AttemptID == "" && planner.last != nil) || (plannerRef.AttemptID != "" && (planner.last == nil || *planner.last != plannerRef)) {
							t.Error("failed Planner caller changed its last accepted Ref")
						}
						// A failed caller cannot hide the error by reusing or replacing
						// its session. Preserve the original error returned to the run.
						if _, stopped := planner.step(ctx); stopped == nil || !strings.Contains(stopped.Error(), "planner session is stopped") {
							t.Error("failed Planner caller allowed another Step")
						}
						if _, stopped := planner.support(ctx, models); stopped == nil || !strings.Contains(stopped.Error(), "supporting dispatch requires") {
							t.Error("failed Planner caller allowed supporting work")
						}
						if _, stopped := planner.handoff(ctx); stopped == nil || !strings.Contains(stopped.Error(), "planner handoff requires") {
							t.Error("failed Planner caller allowed handoff")
						}
					}
				}
				if err != nil && strings.Contains(tc.name, "cleanup-failure") {
					var failure *engine.Failure
					if !errors.As(err, &failure) || failure.Code != engine.CleanupFailed {
						t.Errorf("cleanup failure lost its runtime classification: %v", err)
					}
				}
				outputs := map[string]contract.Ref{}
				// No final selection: these are supporting refs, not a report.
				if err == nil {
					outputs = map[string]contract.Ref{"intake": result.Intake, "wiki": result.Wiki, "context": result.Context}
					if planning {
						outputs["planner"] = plannerRef
					}
				}
				return engine.Result{Outputs: outputs}, err
			}}
			var transport runtime.Runtime = pi
			if tc.name == "attempt-timeout" || tc.name == "resolve-timeout" || tc.name == "refresh-timeout" || tc.name == "update-timeout" || tc.name == "support-resolve-timeout" || tc.name == "planner-timeout" || tc.name == "work-worker-timeout" || tc.name == "work-planner-timeout" {
				transport = deadlineRuntime{pi}
			}
			r, err = engine.New(ctx, def, engine.Input{Prompt: "CASE-17", LaunchCWD: dir}, engine.Options{BaseDir: dir, Schemas: registry, Runtime: transport})
			if err != nil {
				t.Fatal(err)
			}
			go func() { report = r.Execute(); close(done) }()
			count := 0
			hellos := map[string]protocol.Control{}
			var intake Intake
			var wiki WikiSearch
			var expectedContext Context
			var initialIntake contract.Ref
			var acquiredFiles []file
			var acquiredRequests int32
			var refreshRequests int32
			var revisionIntake contract.Ref
			observedSupportingRefs := map[contract.Ref]bool{}
		loop:
			for {
				select {
				case <-done:
					break loop
				case <-ctx.Done():
					t.Fatal("slice exceeded test deadline")
				case e := <-events:
					if e.m.Type == "hello" {
						if _, ok := hellos[e.m.SessionID]; ok {
							t.Fatal("session reused")
						}
						for _, s := range r.Snapshot().Sessions {
							if s.Identity.SessionID != "" && s.Identity.SessionID != e.m.SessionID && s.State != "Closed" {
								t.Fatalf("new stage before old cleanup: %+v", s)
							}
						}
						hellos[e.m.SessionID] = e.m
						continue
					}
					if (tc.name == "cancel" || tc.name == "attempt-timeout" || strings.HasSuffix(tc.name, "-cancel") || strings.HasSuffix(tc.name, "-timeout")) && e.m.Type == "held" {
						continue
					}
					if e.m.Type != "prompt" {
						t.Fatalf("unexpected control %s", e.m.Type)
					}
					count++
					var req contract.Request
					if err := protocol.ReadJSON(e.m.RequestPath, &req); err != nil {
						t.Fatal(err)
					}
					var task stageTask
					if err := json.Unmarshal([]byte(req.Prompt), &task); err != nil {
						t.Fatal(err)
					}
					if req.Output.SchemaID == ContextSchema && !strings.Contains(task.Requirements, supportingResolutionRequirements) {
						t.Fatalf("supporting requirements missing from %s task", task.Stage)
					}
					if !reflect.DeepEqual(task.Scope, scope) {
						t.Fatal("scope not in prompt")
					}
					if working && !transitionProbe && count > 4 && task.Stage != "planner" {
						if task.SupportingProposal == nil || !slices.Contains(req.Inputs, *task.SupportingProposal) {
							t.Fatal("supporting task lost exact proposal input")
						}
						var latest contract.Ref
						var seq uint64
						for _, attempt := range r.Snapshot().Attempts {
							if attempt.Output != nil && attempt.Output.SchemaID == PlannerSchema && attempt.State == engine.Succeeded && attempt.LastSeq > seq {
								latest, seq = *attempt.Output, attempt.LastSeq
							}
						}
						if *task.SupportingProposal != latest {
							t.Fatal("supporting task received a stale proposal instead of the latest accepted Planner Ref")
						}
						var proposal publication[PlannerState]
						if err := protocol.ReadJSON(task.SupportingProposal.Path, &proposal); err != nil {
							t.Fatal(err)
						}
						if proposal.Data.SupportingWork == nil || proposal.Data.SupportingWork.Kind != workKind {
							t.Fatal("supporting dispatch differs from accepted proposal")
						}
					}
					if task.Stage != "planner" {
						for _, ref := range req.Inputs {
							if ref.SchemaID != PlannerSchema {
								observedSupportingRefs[ref] = true
							}
						}
					}
					var data any
					var files map[string][]byte
					switch count {
					case 1:
						if task.Stage != "intake" || len(req.Inputs) != 0 {
							t.Fatal("intake inputs")
						}
						if acquiring {
							intake, acquiredFiles, err = acquireIntake(ctx, filepath.Dir(e.m.CandidatePath), scope, acquisition)
							if err != nil {
								t.Fatal(err)
							}
							acquiredRequests = requests.Load()
						} else {
							intake, files = intakeFixture(mode)
						}
						data = intake
					case 2:
						if task.Stage != "wiki" || len(req.Inputs) != 1 {
							t.Fatal("wiki inputs")
						}
						initialIntake = req.Inputs[0]
						wiki, files = wikiFixture(mode, req.Inputs[0])
						data = wiki
					case 3:
						if task.Stage != "context" || len(req.Inputs) != 2 || req.Inputs[0] != wiki.Intake {
							t.Fatal("context inputs")
						}
						if task.RuntimeResolutionAllowed != (intake.Complete && wikiComplete(wiki) && targetAuthorized) {
							t.Fatal("prerequisite gate mismatch")
						}
						expectedContext, files = contextFixture(mode, scope, req.Inputs, intake, wiki)
						if acquiring {
							if !hasFile(acquiredFiles, "page-1") {
								expectedContext.Observations[0].Evidence[0].FileID = "issue"
							}
							if !hasFile(acquiredFiles, "bundle") || len(intake.Attachments) == 0 || intake.Attachments[0].Content.Status != "available" || intake.Attachments[0].Analysis.Status != "available" {
								expectedContext.Time = TimeResolution{Status: "unresolved", Anchors: []TimeAnchor{}}
								expectedContext.Attempts[1].Evidence[0].FileID = "issue"
								expectedContext.Attempts[1].Outcome = "attachment unavailable or incomplete; trustworthy incident anchor pending"
							}
						}
						if supporting || tc.name == "planner-support" {
							expectedContext, files = supportingContextFixture(t, ctx, supportingURL, tc.name, 0, task, req.Inputs, expectedContext, files)
						}
						data = expectedContext
					default:
						if task.Stage == "planner" {
							plannerSteps++
							if !planning || req.Output.SchemaID != PlannerSchema || task.Requirements != plannerRequirements || task.RuntimeResolutionAllowed {
								t.Fatal("planner task/model contract mismatch")
							}
							for ref := range observedSupportingRefs {
								if !slices.Contains(req.Inputs, ref) {
									t.Fatalf("planner did not receive historical supporting owner %s/%s", ref.SchemaID, ref.AttemptID)
								}
							}
							if working {
								expectedPlanner = plannerWorkFixture(t, tc.name, workKind, req, task, plannerSteps, initialIntake)
							} else {
								expectedPlanner = plannerFixture(t, tc.name, req, task, plannerSteps, initialIntake)
							}
							data = expectedPlanner
							if tc.name == "planner-extra-control" || tc.name == "work-extra-control" {
								var extra map[string]any
								if err := json.Unmarshal(testJSON(data), &extra); err != nil {
									t.Fatal(err)
								}
								if working {
									extra["supporting_work"].(map[string]any)["model"] = "agent-selected-model"
								} else {
									extra["model"] = "agent-selected-model"
								}
								data = extra
							}
							break
						}
						if revising && task.Stage != "wiki-resolution" && task.Stage != "context-resolution" {
							switch task.Stage {
							case "intake-revision", "intake-update":
								if task.Stage == "intake-update" && (len(task.SourceWork) != 0 || !updating) {
									t.Fatal("update turned into per-source dispatch")
								}
								var fetched int32
								name := tc.name
								if working {
									name = workFixture
								}
								intake, files, fetched = refreshIntakeFixture(t, ctx, name, count, req.Inputs, task, revisionURL)
								refreshRequests += fetched
								if task.Stage == "intake-update" && count == 4 && (tc.name == "update-add" || tc.name == "update-replace") && fetched != 3 {
									t.Fatal("new issue/link/attachment were not acquired in one Step")
								}
								data = intake
							case "wiki-revision":
								revisionIntake = req.Inputs[0]
								wikiMode := "wiki-matches"
								if tc.name == "refresh-wiki-partial" || tc.name == "refresh-then-resolve" || tc.name == "update-then-resolve" {
									wikiMode = "wiki-partial"
								}
								if tc.name == "refresh-false-no-matches" {
									wikiMode = "wiki-false-empty"
								}
								wiki, files = wikiFixture(wikiMode, req.Inputs[0])
								if tc.name == "refresh-old-wiki-binding" || tc.name == "update-old-wiki-binding" {
									wiki.Intake = initialIntake
								}
								data = wiki
							case "context-revision":
								var prior publication[Context]
								if err := protocol.ReadJSON(req.Inputs[2].Path, &prior); err != nil {
									t.Fatal(err)
								}
								expectedContext, files = resolutionFixture("", mode, scope, req.Inputs, intake, wiki, prior.Data)
								// Provider fixture qualifies retained source ownership independently.
								for n := len(prior.Data.Attempts); n < len(expectedContext.Attempts); n++ {
									if expectedContext.Attempts[n].Kind == "time" {
										expectedContext.Attempts[n].Evidence = []Evidence{{FileID: "remediation"}}
									}
								}
								if prior.Data.Time.Status != "resolved" {
									for n := range expectedContext.Time.Anchors {
										expectedContext.Time.Anchors[n].Evidence = Evidence{Ref: &req.Inputs[0], FileID: "new-bundle"}
									}
								}
								if tc.name == "refresh-invalidated" {
									expectedContext.Time.Anchors[0].Event = "reassessed event"
									expectedContext.Time.Anchors[0].Evidence = Evidence{Ref: &req.Inputs[0], FileID: "new-bundle"}
								}
								if tc.name == "refresh-historical-wiki" {
									expectedContext.Observations = append(expectedContext.Observations, Fact{Value: "Agent retained a prior wiki pattern", Evidence: []Evidence{{Ref: &prior.Data.Wiki, FileID: "wiki-page"}}})
								}
								if updating {
									for n := range expectedContext.Time.Anchors {
										if prior.Data.Time.Status != "resolved" {
											expectedContext.Time.Anchors[n].Evidence = Evidence{FileID: "remediation"}
										}
									}
									if tc.name == "update-retain-gap" {
										expectedContext.Gaps = append(expectedContext.Gaps, prior.Data.Gaps...)
										expectedContext.ResolvedGaps = nil
										expectedContext.Readiness = "needs-resolution"
									}
								}
								if tc.name == "refresh-drop-gap" || tc.name == "update-drop-gap" {
									expectedContext.ResolvedGaps = nil
								}
								if tc.name == "refresh-dropped-history" {
									expectedContext.Attempts = expectedContext.Attempts[len(prior.Data.Attempts):]
								}
								if tc.name == "refresh-context-foreign-ref" {
									bad := prior.Data.Wiki
									bad.RunID = "foreign"
									expectedContext.Observations[0].Evidence[0].Ref = &bad
								}
								data = expectedContext
							default:
								t.Fatal("unexpected revision stage")
							}
							if requests.Load() != acquiredRequests || newRequests.Load() != refreshRequests {
								t.Fatal("revision repeated original acquisition or missed designated work")
							}
							break
						}
						expectedIntake := initialIntake
						if revising {
							expectedIntake = revisionIntake
						}
						if (!resolving && !revising) || req.Inputs[0] != expectedIntake || task.Previous == nil || requests.Load() != acquiredRequests {
							t.Fatal("resolution reacquired intake or lost committed input")
						}
						if task.Stage == "wiki-resolution" {
							if len(req.Inputs) < 3 || *task.Previous != req.Inputs[2] || req.Inputs[1].SchemaID != WikiSchema {
								t.Fatal("wiki remediation inputs")
							}
							wikiMode := "wiki-matches"
							if tc.name == "resolve-wiki-partial-again" || (tc.name == "resolve-repeat" && count == 4) {
								wikiMode = "wiki-partial"
							}
							wiki, files = wikiFixture(wikiMode, req.Inputs[0])
							data = wiki
						} else if task.Stage == "context-resolution" {
							if len(req.Inputs) < 3 || *task.Previous != req.Inputs[2] {
								t.Fatal("context remediation inputs")
							}
							var prior publication[Context]
							if err := protocol.ReadJSON(req.Inputs[2].Path, &prior); err != nil {
								t.Fatal(err)
							}
							kinds := []string{}
							if prior.Data.Identity.Status != "resolved" {
								kinds = append(kinds, "identity")
							}
							if prior.Data.Time.Status != "resolved" {
								kinds = append(kinds, "time")
							}
							if strings.Join(task.ResolutionKinds, ",") != strings.Join(kinds, ",") || !reflect.DeepEqual(task.Gaps, prior.Data.Gaps) {
								t.Fatal("Controller did not scope remediation to committed gaps")
							}
							if task.RuntimeResolutionAllowed != (intake.Complete && wikiComplete(wiki) && targetAuthorized) {
								t.Fatal("resolution prerequisite gate mismatch")
							}
							expectedContext, files = resolutionFixture(tc.name, mode, scope, req.Inputs, intake, wiki, prior.Data)
							if supporting {
								expectedContext, files = supportingContextFixture(t, ctx, supportingURL, tc.name, count-3, task, req.Inputs, expectedContext, files)
							}
							if revising {
								for n := len(prior.Data.Attempts); n < len(expectedContext.Attempts); n++ {
									if expectedContext.Attempts[n].Kind == "time" {
										expectedContext.Attempts[n].Evidence = []Evidence{{FileID: "remediation"}}
									}
								}
							}
							data = expectedContext
						} else {
							t.Fatal("unexpected extra step")
						}
					}
					if count > 3 {
						assertRetainedInputOwners(t, req.Inputs)
					}
					for _, ref := range req.Inputs {
						a := r.Snapshot().Attempts[ref.AttemptID]
						if a.Output == nil || *a.Output != ref || a.State != engine.Succeeded {
							t.Fatal("noncommitted input")
						}
					}
					if strings.HasPrefix(tc.name, "support-resolve-empty-") && ((count == 3 && tc.name != "support-resolve-empty-next") || (count == 4 && tc.name != "support-resolve-empty-prior")) {
						// Bypass only Go's omitempty encoder, not the real schema or
						// parser: an agent may explicitly emit a legal empty array.
						var raw map[string]any
						if err := json.Unmarshal(testJSON(data), &raw); err != nil {
							t.Fatal(err)
						}
						raw["attempts"].([]any)[0].(map[string]any)["queries"] = []any{}
						data = raw
						expectedContext.Attempts[0].Queries = []SupportingQuery{}
					}
					if tc.name == "work-tamper-during-worker" && task.Stage == "context-revision" {
						// Corrupt the prior Planner after this Step's input resolution.
						// Only the post-worker transition consumes that proposal again.
						if err := os.WriteFile(task.SupportingProposal.Path, []byte("{}"), 0600); err != nil {
							t.Fatal(err)
						}
					}
					if acquiring && count == 1 {
						writeEnvelope(t, e.m, req, data, acquiredFiles)
					} else {
						writeCandidate(t, e.m, req, data, files, tc.name == "file-escape")
					}
					ack := "settle"
					if task.Stage == "planner" && plannerSteps == 2 {
						switch tc.name {
						case "planner-provider-failure":
							ack = "provider-error"
						case "planner-timeout":
							ack = "hold"
						case "planner-cancel":
							ack = "hold"
							r.Cancel(engine.OriginControllerUser)
						}
					}
					if !working && (tc.name == "provider-failure" || ((resolving || revising) && strings.HasSuffix(tc.name, "-provider-failure") && count == 4)) {
						ack = "provider-error"
					}
					if !working && (tc.name == "attempt-timeout" || ((resolving || revising) && strings.HasSuffix(tc.name, "-timeout") && count == 4)) {
						ack = "hold"
					}
					if !working && (tc.name == "cancel" || ((resolving || revising) && strings.HasSuffix(tc.name, "-cancel") && count == 4)) {
						ack = "hold"
						r.Cancel(engine.OriginControllerUser)
					}
					workFailureAt := 5
					if strings.HasPrefix(tc.name, "work-planner-") {
						workFailureAt = 8
					}
					if working && count == workFailureAt {
						switch {
						case strings.HasSuffix(tc.name, "-provider-failure"):
							ack = "provider-error"
						case strings.HasSuffix(tc.name, "-timeout"):
							ack = "hold"
						case strings.HasSuffix(tc.name, "-cancel"):
							ack = "hold"
							r.Cancel(engine.OriginControllerUser)
						}
					}
					cleanupAt := 4
					if tc.name == "work-worker-cleanup-failure" {
						cleanupAt = 5
					}
					if tc.name == "cleanup-failure" || ((resolving || revising || planning) && strings.HasSuffix(tc.name, "-cleanup-failure") && count == cleanupAt) {
						for sid := range hellos {
							path := filepath.Join(bridge, sid+".json")
							if _, err := os.Stat(path); os.IsNotExist(err) {
								continue
							}
							if err := os.Rename(path, path+".recovering"); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := json.NewEncoder(e.conn).Encode(protocol.Control{Type: ack}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if count != tc.stages || (report.ExitCode != 0) != tc.failure {
				t.Fatalf("stages=%d outcome=%s failure=%v", count, report.Outcome, report.Failure)
			}
			if tc.name == "truncated-attachment" || tc.name == "update-history-alias" || tc.name == "work-blank-reason" {
				if err := validationRejection(tc.name, report.Failure); err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "update-wrong-task" && !strings.Contains(fmt.Sprint(report.Failure), "intake revision changed dispatched task/work/previous") {
				t.Fatalf("wrong task was not rejected at dispatch binding: %v", report.Failure)
			}
			if report.Final != nil {
				t.Fatal("slice invented final report")
			}
			supportingAllowed := targetAuthorized && intake.Complete && wikiComplete(wiki)
			if supporting {
				wantRequests := int32(0)
				if supportingAllowed {
					wantRequests = int32(3 + 2*(count-3))
				}
				if supportingRequests.Load() != wantRequests {
					t.Fatalf("supporting acquisition requests=%d, want=%d", supportingRequests.Load(), wantRequests)
				}
			}
			if (tc.name == "support-no-basis" || tc.name == "support-no-result") && !strings.Contains(fmt.Sprint(report.Failure), "supporting query lacks conditions, basis or result evidence") {
				t.Fatalf("empty array was not rejected by semantic acceptance: %v", report.Failure)
			}
			if (tc.name == "support-resolve-dropped-history" || tc.name == "support-resolve-altered-query" || tc.name == "support-resolve-retag-basis") && !strings.Contains(fmt.Sprint(report.Failure), "context revision dropped resolution history") {
				t.Fatalf("wrong rejection for dropped query history: %v", report.Failure)
			}
			if tc.name == "work-tamper-during-worker" {
				raw, err := os.ReadFile(filepath.Join(r.Dir(), "events.jsonl"))
				if err != nil {
					t.Fatal(err)
				}
				for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
					var event engine.Event
					if err := json.Unmarshal(line, &event); err != nil {
						t.Fatal(err)
					}
					if event.Kind == "Decision" && event.Details.(map[string]any)["name"] == "supporting-"+firstPlannerRef.AttemptID+"-recorded" {
						t.Fatal("post-worker acceptance reused the corrupted pre-worker proposal")
					}
				}
			}
			if working {
				wantError := map[string]string{
					"work-tamper-during-worker":           "ReferenceInvalid",
					"work-tamper-evidence-before-support": "ReferenceInvalid",
					"work-tamper-evidence-after-support":  "ReferenceInvalid",
					"work-tamper-evidence-handoff":        "ReferenceInvalid",
					"work-unproposed-transition":          "planner context change requires prior supporting proposal and direct context successor",
					"work-skipped-context":                "planner context change requires prior supporting proposal and direct context successor",
					"work-wrong-task":                     "supporting result differs from proposed intake task",
					"work-refresh-work-mismatch":          "supporting result differs from proposed intake task",
					"work-resolve-changed-intake":         "supporting resolve result changed intake or wiki task binding",
					"work-no-proposal":                    "structured supporting_work",
					"work-bad-kind":                       "ContractInvalid",
					"work-blank-reason":                   "supporting work requires reason and basis",
					"work-no-basis":                       "supporting work requires reason and basis",
					"work-local-basis":                    "supporting work basis must name an exact supporting input owner/file",
					"work-foreign-basis":                  "supporting work basis must name an exact supporting input owner/file",
					"work-uncommitted-basis":              "supporting work basis must name an exact supporting input owner/file",
					"work-missing-file":                   "supporting work basis must name an exact supporting input owner/file",
					"work-resolve-ready":                  "supporting resolve requires needs-resolution context",
					"work-refresh-ready":                  "local intake revision requires incomplete intake and explicit work",
					"work-refresh-empty":                  "local intake revision requires incomplete intake and explicit work",
					"work-unknown-source":                 "unknown source work selector",
					"work-duplicate-source":               "source work requires unique selectors and reasons",
					"work-blank-source-reason":            "source work requires unique selectors and reasons",
					"work-extra-sources":                  "only refresh accepts source selectors",
					"work-extra-control":                  "ContractInvalid",
					"work-wrong-result-context":           "planner context/previous mismatch",
					"work-drop-gap":                       "planning cannot remove supporting context gaps",
					"work-attempt-cap":                    "LimitExceeded", "work-final-cap": "LimitExceeded", "work-session-cap": "LimitExceeded",
					"work-tamper-proposal": "ReferenceInvalid", "work-tamper-history": "ReferenceInvalid",
				}[tc.name]
				if wantError != "" && !strings.Contains(fmt.Sprint(report.Failure), wantError) {
					t.Fatalf("supporting failure missing %q: %v", wantError, report.Failure)
				}
				if !tc.failure {
					var state, first publication[PlannerState]
					if err := protocol.ReadJSON(plannerRef.Path, &state); err != nil {
						t.Fatal(err)
					}
					if err := protocol.ReadJSON(firstPlannerRef.Path, &first); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(state.Data, expectedPlanner) || state.Data.Context != result.Context || first.Data.Context != beforeResolution.Context || state.Data.Context == first.Data.Context || state.Data.SupportingWork != nil {
						t.Fatal("supporting work lost planning state/context binding or replayed consumed proposal")
					}
					if state.Data.Hypotheses[0].Evidence[0].Ref == nil || *state.Data.Hypotheses[0].Evidence[0].Ref != initialIntake || !slices.Contains(state.Data.Pending[0].Requirements, "Preserve the prior question when handing off") {
						t.Fatal("supporting work lost historical owners or planning questions")
					}
					for _, session := range report.Snapshot.Sessions {
						if session.Role.Name == "triage-planner" && session.Role.Model != (runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}) {
							t.Fatal("supporting handoff changed planner model")
						}
					}
				}
				if plannerRef.AttemptID != "" {
					accepted := report.Snapshot.Attempts[plannerRef.AttemptID]
					if accepted.State != engine.Succeeded || accepted.Output == nil || *accepted.Output != plannerRef {
						t.Fatal("supporting failure lost last accepted Planner state")
					}
				}
			}
			if planning && !working {
				if err := assertPlannerOutcome(report, tc.name, tc.failure, plannerRef, firstPlannerRef, expectedPlanner); err != nil {
					t.Fatal(err)
				}
				if tc.name == "planner-support" && supportingRequests.Load() != 3 {
					t.Fatal("planner reacquired supporting sources")
				}
			}
			if !tc.failure {
				sessions := tc.stages
				if tc.name == "planner-reuse" || tc.name == "planner-reuse-handoff" || tc.name == "work-reuse" {
					sessions--
				}
				if result.Ready != tc.ready || result.Context.RunID != result.Intake.RunID || len(report.Snapshot.Attempts) != tc.stages || len(report.Snapshot.Sessions) != sessions {
					t.Fatalf("context/accounting: %+v", result)
				}
				var published publication[Context]
				if err := protocol.ReadJSON(result.Context.Path, &published); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(published.Data, expectedContext) {
					t.Fatal("committed context lost provenance/state")
				}
				if supporting && supportingAllowed {
					assertSupportingData(t, published, 2*(count-2))
					for _, attempt := range published.Data.Attempts {
						for _, q := range attempt.Queries {
							for _, evidence := range append(slices.Clone(q.Basis), q.Evidence...) {
								owner, ownerFiles := result.Context, published.Files
								if evidence.Ref != nil {
									owner = *evidence.Ref
									var raw publication[json.RawMessage]
									if err := protocol.ReadJSON(owner.Path, &raw); err != nil {
										t.Fatal(err)
									}
									ownerFiles = raw.Files
								}
								a := report.Snapshot.Attempts[owner.AttemptID]
								if a.Output == nil || *a.Output != owner || !hasFile(ownerFiles, evidence.FileID) {
									t.Fatal("query lost exact committed owner")
								}
							}
						}
					}
					if strings.HasPrefix(tc.name, "support-resolve-") {
						if hasFile(published.Files, "identity-raw") {
							t.Fatal("resolved identity was reacquired or copied")
						}
						first := published.Data.Attempts[2].Queries[1]
						if first.Basis[1].Ref == nil || *first.Basis[1].Ref != beforeResolution.Context || first.Evidence[0].Ref == nil || *first.Evidence[0].Ref != beforeResolution.Context {
							t.Fatal("historical query evidence was relabeled")
						}
					}
				}
				if revising {
					var revised, initial publication[Intake]
					if err := protocol.ReadJSON(result.Intake.Path, &revised); err != nil {
						t.Fatal(err)
					}
					if err := protocol.ReadJSON(initialIntake.Path, &initial); err != nil {
						t.Fatal(err)
					}
					if revised.Data.Update != (updating && tc.name != "update-then-refresh") {
						t.Fatal("intake lost task binding")
					}
					if revised.Data.Acquisition == nil || revised.Data.Acquisition.Ref != nil || !hasFile(revised.Files, revised.Data.Acquisition.FileID) {
						t.Fatal("new acquisition diagnostics not owned by revision")
					}
					switch tc.name {
					case "update-page":
						oldPage := initial.Data.Comments[0].Source
						oldPage.Ref = &initialIntake
						if !reflect.DeepEqual(revised.Data.Comments[0].Source, oldPage) || revised.Data.Comments[1].Source.Ref != nil || !hasFile(revised.Files, revised.Data.Comments[1].Source.FileID) {
							t.Fatal("update did not retain page owner and acquire missing page in one Step")
						}
					case "update-comments-repage":
						if len(revised.Data.Comments) != 1 || revised.Data.Comments[0].Source.Ref != nil || len(initial.Data.Comments) != 2 {
							t.Fatal("repagination changed history or retained obsolete active page")
						}
					case "update-comments-missing":
						page := revised.Data.Comments[2].Source
						if revised.Data.Complete || page.Status != "partial" || !hasFile(revised.Files, page.FileID) {
							t.Fatal("partial comment page lost status or raw evidence")
						}
					case "update-content", "update-retain-content", "refresh-content-failure", "refresh-content-failure-repeat":
						oldAnalysis := initial.Data.Attachments[0].Analysis
						oldAnalysis.Ref = &initialIntake
						if !reflect.DeepEqual(revised.Data.Attachments[0].Analysis, oldAnalysis) {
							t.Fatal("source update rewrote historical analysis ownership")
						}
						if tc.name == "update-retain-content" {
							oldContent := initial.Data.Attachments[0].Content
							oldContent.Ref = &initialIntake
							if !reflect.DeepEqual(revised.Data.Attachments[0].Content, oldContent) {
								t.Fatal("historical content was relabeled as a new download")
							}
						} else if !hasFile(revised.Files, revised.Data.Attachments[0].Content.FileID) || revised.Data.Attachments[0].Content.Ref != nil {
							t.Fatal("new content/partial bytes not preserved")
						}
					case "update-remove", "update-retain-gap", "update-resolve-gap":
						if len(revised.Data.Attachments) != 0 || len(revised.Data.Linked) != 0 {
							t.Fatal("removed sources remain in active inventory")
						}
						if len(initial.Data.Attachments) != 1 || len(initial.Data.Linked) != 1 {
							t.Fatal("removal changed committed history")
						}
					case "update-repeat":
						if revised.Data.Attachments[0].Content.Ref == nil || revised.Data.Previous == nil || *revised.Data.Attachments[0].Content.Ref != *revised.Data.Previous {
							t.Fatal("second update did not retain the first update's true owner")
						}
						if published.Data.Time.Anchors[0].Evidence.Ref == nil || *published.Data.Time.Anchors[0].Evidence.Ref != initialIntake {
							t.Fatal("removed inventory evidence lost historical binding")
						}
					}
				}
			}
			if !strings.HasSuffix(tc.name, "cleanup-failure") {
				for _, c := range report.Cleanup {
					if !c.WaitCompleted || !c.ProcessExited || len(c.Unconfirmed) > 0 {
						t.Fatalf("cleanup incomplete: %+v", c)
					}
				}
			}
			if strings.HasSuffix(tc.name, "cleanup-failure") && len(report.CleanupErrors) == 0 {
				t.Fatal("cleanup failure not retained")
			}
			if tc.name == "attempt-timeout" || tc.name == "provider-failure" {
				var failure *engine.Failure
				if !errors.As(report.Failure, &failure) {
					t.Fatalf("lost typed failure: %v", report.Failure)
				}
				if tc.name == "attempt-timeout" && (failure.Code != engine.TimedOut || failure.Origin != engine.OriginAttemptDeadline) {
					t.Fatalf("timeout reclassified: %+v", failure)
				}
				if tc.name == "provider-failure" && failure.Code != engine.ProviderFailed {
					t.Fatalf("provider failure reclassified: %+v", failure)
				}
				// Vary only the external error text; keep the real RPC failure code.
				collision := *failure
				collision.Message = validationError("truncated-attachment")
				if validationRejection("truncated-attachment", &collision) == nil {
					t.Fatal("execution failure text counted as semantic rejection")
				}
				for _, a := range report.Snapshot.Attempts {
					if a.Output != nil {
						t.Fatal("failed candidate became committed output")
					}
				}
			}
			if (resolving || revising) && !strings.HasSuffix(tc.name, "-uncommitted-input") {
				if requests.Load() != acquiredRequests || (!supporting && acquiredRequests == 0) {
					t.Fatal("resolution repeated HTTP acquisition")
				}
				if tc.failure && !reflect.DeepEqual(result, beforeResolution) {
					t.Fatal("failed resolution replaced the last accepted state")
				}
				for _, ref := range []contract.Ref{beforeResolution.Intake, beforeResolution.Wiki, beforeResolution.Context} {
					a := report.Snapshot.Attempts[ref.AttemptID]
					if a.Output == nil || *a.Output != ref || a.State != engine.Succeeded {
						t.Fatal("resolution lost prior committed history")
					}
				}
			}
			if (resolving || revising) && (strings.HasSuffix(tc.name, "-provider-failure") || strings.HasSuffix(tc.name, "-timeout")) {
				var failure *engine.Failure
				if !errors.As(report.Failure, &failure) {
					t.Fatal("resolution lost typed execution failure")
				}
				if strings.HasSuffix(tc.name, "-provider-failure") && failure.Code != engine.ProviderFailed {
					t.Fatalf("resolution provider failure reclassified: %+v", failure)
				}
				if strings.HasSuffix(tc.name, "-timeout") && (failure.Code != engine.TimedOut || failure.Origin != engine.OriginAttemptDeadline) {
					t.Fatalf("resolution timeout reclassified: %+v", failure)
				}
				for _, a := range report.Snapshot.Attempts {
					if a.State != engine.Succeeded && a.Output != nil {
						t.Fatal("failed resolution candidate became committed")
					}
				}
			}
			for _, s := range report.Snapshot.Sessions {
				if (s.Role.Name == "triage-intake" || s.Role.Name == "triage-intake-revision" || s.Role.Name == "triage-intake-update") && (s.Role.Model.Provider != "fireworks" || s.Role.Model.ID != "accounts/fireworks/models/deepseek-v4p1-flash") {
					t.Fatal("mechanical model binding changed")
				}
			}
			if (tc.name == "cancel" || strings.HasSuffix(tc.name, "-cancel")) && report.Outcome != engine.CancelledState {
				t.Fatal("cancellation swallowed")
			}
			if strings.HasSuffix(tc.name, "attempt-cap") && !strings.Contains(fmt.Sprint(report.Failure), "LimitExceeded") {
				t.Fatal("hard cap swallowed")
			}
		})
	}
}
