package triage

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
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
type deadlineRuntime struct {
	runtime.Runtime
	taskTimeouts map[string]time.Duration
}
type deadlineSession struct {
	runtime.Session
	taskTimeouts map[string]time.Duration
}

func (r deadlineRuntime) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	s, err := r.Runtime.Start(ctx, spec)
	if err != nil {
		return nil, err
	}
	return deadlineSession{s, r.taskTimeouts}, nil
}
func (s deadlineSession) Execute(ctx context.Context, d runtime.Dispatch) (runtime.Execution, error) {
	timeout := time.Second
	if len(s.taskTimeouts) != 0 {
		_, _, req, err := protocol.ParseDispatch(d.Message)
		if err != nil {
			return runtime.Execution{}, err
		}
		var task struct {
			Task WorkerTask `json:"task"`
		}
		if err := json.Unmarshal([]byte(req.Prompt), &task); err != nil {
			return runtime.Execution{}, err
		}
		if override, ok := s.taskTimeouts[task.Task.ID]; ok {
			timeout = override
		}
	}
	timed, cancel := context.WithTimeoutCause(ctx, timeout, &runtime.Failure{Code: runtime.TimedOut, Origin: runtime.AttemptDeadline, Message: "fixture step deadline"})
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
	if err := protocol.WriteEnvelope(m.CandidatePath, req, data, entries); err != nil {
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

func workerPlannerFixture(t *testing.T, name string, req contract.Request, v PlannerState, step int) PlannerState {
	t.Helper()
	basis := v.Hypotheses[0].Evidence[0]
	if step == 1 {
		v.SupportingWork = nil
		task := WorkerTask{ID: "w1", SourceKind: "code", Responsibility: "evidence-only", Question: "Inspect the supplied anonymous trace", Requirements: []string{"Deliver evidence and limitations"}, Basis: []Evidence{basis}, DependsOn: []string{}}
		if name == "m1-analysis" {
			task.Responsibility = "analysis"
		}
		v.WorkerTasks = []WorkerTask{task}
		switch name {
		case "m1-pending":
			v.WorkerTasks = nil
		case "m1-unsatisfied", "m1-dependencies", "m1-dependencies-incomplete":
			task.ID, task.DependsOn = "w2", []string{"w1"}
			v.WorkerTasks = append(v.WorkerTasks, task)
		case "m1-task-duplicate":
			v.WorkerTasks = append(v.WorkerTasks, task)
		case "m1-task-owner":
			v.WorkerTasks[0].Basis[0].Ref = nil
		case "m1-logs", "m1-query-utc":
			v.WorkerTasks[0].SourceKind = "logs"
			v.WorkerTasks[0].Search = &WorkerSearch{Source: "anonymous", Filter: "trace=fixture", From: "2025-01-01T00:00:00Z", To: "2025-01-01T00:01:00Z", Basis: []Evidence{basis}}
		case "m1-task-utc":
			v.WorkerTasks[0].SourceKind = "logs"
			v.WorkerTasks[0].Search = &WorkerSearch{Source: "anonymous", Filter: "trace=fixture", From: "2025-01-01T00:00:00Z", To: "2025-01-01T00:00:00Z", Basis: []Evidence{basis}}
		}
		return v
	}
	var task struct {
		WorkerResults []contract.Ref `json:"worker_results"`
	}
	if err := json.Unmarshal([]byte(req.Prompt), &task); err != nil {
		t.Fatal(err)
	}
	if len(task.WorkerResults) == 0 {
		t.Fatal("Planner not explicitly given worker results")
	}
	v.WorkerResults, v.WorkerTasks = task.WorkerResults, nil
	for _, ref := range task.WorkerResults {
		if !slices.Contains(req.Inputs, ref) {
			t.Fatal("Planner worker result absent from Inputs")
		}
		var result publication[WorkerResult]
		if err := protocol.ReadJSON(ref.Path, &result); err != nil {
			t.Fatal(err)
		}
		for _, owner := range []contract.Ref{result.Data.Proposal, result.Data.Context} {
			if !slices.Contains(req.Inputs, owner) {
				t.Fatal("Planner lost true historical worker owner")
			}
		}
		if name == "m1-support" && step == 3 && result.Data.Context == v.Context {
			t.Fatal("support update rebound historical worker result to new context")
		}
		if name == "m1-support" {
			v.Hypotheses[0].Evidence = append(v.Hypotheses[0].Evidence, Evidence{Ref: &ref, FileID: "worker-raw"})
		} else {
			v.Hypotheses[0].Evidence = []Evidence{{Ref: &ref, FileID: "worker-raw"}}
		}
	}
	if name == "m1-support" && step == 2 {
		v.SupportingWork = &SupportingWork{Kind: "update", Reason: "Reassess refreshed source inventory", Basis: []Evidence{basis}, Sources: []intakeWork{}}
	}
	switch name {
	case "m1-planner-drop":
		v.WorkerResults = nil
		v.Hypotheses[0].Evidence = []Evidence{basis}
	case "m1-planner-invent":
		bad := task.WorkerResults[0]
		bad.Path = filepath.Join(filepath.Dir(bad.Path), "candidate.json")
		v.WorkerResults = []contract.Ref{bad}
	case "m1-planner-invent-committed":
		v.WorkerResults = []contract.Ref{req.Inputs[0]}
	case "m1-reuse-id":
		v.WorkerTasks = []WorkerTask{{ID: "w1", SourceKind: "code", Responsibility: "evidence-only", Question: "Repeat", Requirements: []string{"Deliver evidence"}, Basis: []Evidence{basis}, DependsOn: []string{}}}
	}
	return v
}

func workerFixture(name string, request workerRequest, inputs []contract.Ref) (WorkerResult, map[string][]byte) {
	v := WorkerResult{Proposal: request.Proposal, Context: request.Context, TaskID: request.Task.ID, Work: "Inspected an anonymous fixture", Status: "complete", Inputs: slices.Clone(inputs), Evidence: []Evidence{{FileID: "worker-raw"}}, Queries: []SupportingQuery{}, Analysis: []Fact{}, Gaps: []string{}, Next: []PlannerQuestion{}}
	files := map[string][]byte{"worker-raw": []byte("anonymous evidence\n")}
	v.Evidence = append(v.Evidence, request.Task.Basis...)
	if (strings.HasPrefix(name, "m4-reframe-") || name == "m5-supplement-reframe-inspection-resume") && strings.HasPrefix(request.Task.ID, "inspection") {
		v.Work = "Read anonymous-wiki-job status without creating, restarting or resubmitting work"
		files["worker-raw"] = []byte("job=anonymous-wiki-job status=completed; existing partial results available for read-only retrieval; no remote resubmission; hypothesis applicability unverified\n")
	}
	switch name {
	case "m1-incomplete", "m1-dependencies-incomplete":
		v.Status, v.Gaps = "incomplete", []string{"Upstream source supplied only partial evidence"}
	case "m1-incomplete-no-gap":
		v.Status = "incomplete"
	case "m1-logs", "m1-query-utc":
		v.Queries = []SupportingQuery{{Source: "anonymous", Filter: "trace=fixture AND narrowed", From: "2025-01-01T00:00:10Z", To: "2025-01-01T00:00:20Z", Basis: slices.Clone(request.Task.Basis), Status: "complete", Outcome: "No records in this subwindow; not incident-wide disproof", Evidence: slices.Clone(v.Evidence)}}
		if name == "m1-query-utc" {
			v.Queries[0].To = v.Queries[0].From
		}
	case "m1-analysis", "m1-evidence-analysis":
		v.Analysis = []Fact{{Value: "An unverified interpretation", Evidence: slices.Clone(v.Evidence)}}
	case "m1-task-id":
		v.TaskID = "unproposed"
	case "m1-proposal":
		v.Proposal = request.Context
	case "m1-context":
		v.Context = request.Proposal
	case "m1-inputs":
		v.Inputs = []contract.Ref{request.Context}
	case "m1-owner":
		bad := request.Context
		bad.RunID = "foreign"
		v.Evidence[0].Ref = &bad
	case "m1-file":
		v.Evidence[0].FileID = "undeclared"
	case "m1-schema":
		v.Status = "root-cause-confirmed"
	}
	return v, files
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

func m2PlannerFixture(t *testing.T, name string, req contract.Request, task stageTask, step int) PlannerState {
	t.Helper()
	if strings.HasPrefix(name, "r5-") {
		if name == "r5-incomplete" {
			return plannerFixture(t, "complete", req, task, step, contract.Ref{})
		}
		if strings.HasPrefix(name, "r5-version") && step <= 3 {
			return m2PlannerFixture(t, "m5-new-claim-all-fresh", req, task, step)
		}
		if step <= 2 {
			return m2PlannerFixture(t, "m5-three-fresh-yield", req, task, step)
		}
		var prior publication[PlannerState]
		if err := protocol.ReadJSON(task.Previous.Path, &prior); err != nil {
			t.Fatal(err)
		}
		v := prior.Data
		v.Previous, v.VerificationReview = task.Previous, nil
		v.Ledger.ConsumedBatch, v.Ledger.Changes = []contract.Ref{}, []HypothesisChange{}
		return v
	}
	if strings.HasPrefix(name, "m5-supplement-reframe-") {
		var v PlannerState
		if step <= 3 {
			v = m2PlannerFixture(t, "m5-new-claim-all-fresh", req, task, step)
		} else {
			v = plannerFixture(t, "complete", req, task, step, contract.Ref{})
			var delivered struct {
				Verification  *PlannerVerification `json:"verification"`
				Recovery      *PlannerRecovery     `json:"recovery"`
				WorkerResults []contract.Ref       `json:"worker_results"`
				WikiResults   []contract.Ref       `json:"wiki_results"`
			}
			if err := json.Unmarshal([]byte(req.Prompt), &delivered); err != nil {
				t.Fatal(err)
			}
			v.Verification, v.Recovery = delivered.Verification, delivered.Recovery
			v.WorkerResults, v.WikiResults = append([]contract.Ref{}, delivered.WorkerResults...), append([]contract.Ref{}, delivered.WikiResults...)
			v.WorkerTasks, v.WikiTask, v.VerificationRequest, v.VerificationReview = []WorkerTask{}, nil, nil, nil
			v.RecoveryChoices = nil
			v.Ledger = &InvestigationLedger{Action: "yield", Reason: "Hand off the remaining runtime gap", Round: 2, NoProgress: 2, ConsumedBatch: []contract.Ref{}, Changes: []HypothesisChange{}}
			for _, ref := range v.WikiResults {
				var wiki publication[WikiSearch]
				if err := protocol.ReadJSON(ref.Path, &wiki); err != nil {
					t.Fatal(err)
				}
				for _, gap := range wiki.Data.Gaps {
					if !slices.Contains(v.Gaps, gap) {
						v.Gaps = append(v.Gaps, gap)
					}
				}
			}
		}
		basis := slices.Clone(v.Hypotheses[0].Evidence)
		if step == 3 {
			var context publication[Context]
			if err := protocol.ReadJSON(v.Context.Path, &context); err != nil {
				t.Fatal(err)
			}
			var wiki publication[WikiSearch]
			if err := protocol.ReadJSON(context.Data.Wiki.Path, &wiki); err != nil {
				t.Fatal(err)
			}
			v.Ledger.Action = "reframe"
			v.Ledger.Reframe = &InvestigationReframe{Change: "Compare a healthy control", Reason: "Two verification deliveries without Agent-reported progress", Basis: basis}
			v.WikiTask = &InvestigationWikiTask{ID: "verification-reframe", Terms: []string{"anonymous healthy control"}, PreviousTerms: slices.Clone(wiki.Data.Queries), Reason: "Test a different premise", Basis: basis}
			v.VerificationReview.NextAction = "reframe"
		}
		if step >= 4 {
			if name == "m5-supplement-reframe-wiki" {
				v.Ledger.ReframeRound, v.Ledger.ReframeStreak = 2, 2
			} else {
				d := v.Recovery.Deliveries[0]
				if d.Kind != "wiki" || len(d.Results) != 0 || len(d.Failures) != 1 || d.Failures[0].Code != engine.TimedOut || d.Failures[0].Cleanup == nil || !d.Failures[0].Cleanup.ConfirmsLocalClose(d.Failures[0].Identity.SessionID) {
					t.Fatal("M5 reframe lost the real failed wiki and strict cleanup")
				}
				switch step {
				case 4:
					v.Ledger.Action = "workers"
					v.WorkerTasks = []WorkerTask{{ID: "inspection", SourceKind: "code", Responsibility: "evidence-only", Question: "Read anonymous-wiki-job status without resubmitting work", Requirements: []string{"Retain remote-job status evidence"}, Basis: basis, DependsOn: []string{}}}
					v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "inspect", Reason: "Inspect the uncertain reframe job read-only", Basis: basis}}
				case 5:
					var proposal publication[PlannerState]
					if err := protocol.ReadJSON(d.Proposal.Path, &proposal); err != nil {
						t.Fatal(err)
					}
					v.Ledger.Action, v.Ledger.Reframe = "reframe", proposal.Data.Ledger.Reframe
					v.WikiTask = proposal.Data.WikiTask
					v.Ledger.ConsumedBatch = slices.Clone(v.WorkerResults)
					v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "resume", Reason: "Inspection confirms completion; read the existing job result without resubmission", Basis: []Evidence{{Ref: &v.WorkerResults[0], FileID: "worker-raw"}}}}
				case 6:
					v.Ledger.ReframeRound, v.Ledger.ReframeStreak = 3, 3
				}
				if step >= 5 {
					v.Ledger.Round, v.Ledger.NoProgress = 3, 3
				}
			}
		}
		t.Logf("M5 reframe step=%d action=%s round=%d no_progress=%d boundary=%d/%d cycle=%d", step, v.Ledger.Action, v.Ledger.Round, v.Ledger.NoProgress, v.Ledger.ReframeRound, v.Ledger.ReframeStreak, v.Recovery.DispatchCycle)
		return v
	}
	if strings.HasPrefix(name, "m5-") {
		if name == "m5-planner-feedback-timeout" && step > 1 {
			step = 2
		}
		v := plannerFixture(t, "complete", req, task, step, contract.Ref{})
		var delivered struct {
			Verification  *PlannerVerification `json:"verification"`
			Recovery      *PlannerRecovery     `json:"recovery"`
			WorkerResults []contract.Ref       `json:"worker_results"`
		}
		if err := json.Unmarshal([]byte(req.Prompt), &delivered); err != nil {
			t.Fatal(err)
		}
		if delivered.Verification == nil || delivered.Recovery == nil || !strings.Contains(task.Requirements, verificationPlannerRequirements) {
			t.Fatal("M5 missing explicit policy/requirements")
		}
		v.Verification, v.Recovery = delivered.Verification, delivered.Recovery
		v.WorkerTasks, v.WorkerResults, v.WikiResults = []WorkerTask{}, append([]contract.Ref{}, delivered.WorkerResults...), []contract.Ref{}
		v.Ledger = &InvestigationLedger{Action: "verify", Reason: "Test the supplied candidate independently", ConsumedBatch: []contract.Ref{}, Changes: []HypothesisChange{}}
		basis := slices.Clone(v.Hypotheses[0].Evidence)
		v.VerificationRequest = &VerificationRequest{Candidate: &ClaimCandidate{ID: "candidate-1", Statement: "The request may have stalled", Premises: []string{"The observation concerns the same request"}, AllowedEvidence: basis}, Reason: "Independent scrutiny before choosing further work"}
		if name == "m5-feedback-worker-new-claim" && step == 3 {
			if len(v.WorkerResults) != 1 || !slices.Contains(req.Inputs, v.WorkerResults[0]) {
				t.Fatal("Planner did not receive exact worker evidence")
			}
			basis = []Evidence{{Ref: &v.WorkerResults[0], FileID: "worker-raw"}}
			v.Ledger.Round, v.Ledger.NoProgress = 2, 0
			v.Ledger.ConsumedBatch = slices.Clone(v.WorkerResults)
			v.Ledger.Changes = []HypothesisChange{{HypothesisID: "h1", Change: "Agent refines the candidate after new evidence", Reason: "Worker supplied the counterexample inspection", Basis: basis}}
			v.Hypotheses[0].Evidence = basis
			v.VerificationRequest.Candidate = &ClaimCandidate{ID: "candidate-2", Statement: "A refined candidate after inspecting the counterexample", Premises: []string{"New evidence is applicable"}, AllowedEvidence: basis}
			v.VerificationReview = nil
			return v
		}
		if step > 1 {
			index := step - 2
			if name == "m5-feedback-worker-new-claim" && step == 4 {
				index = 1
			}
			if len(v.Verification.Deliveries) != index+1 {
				t.Fatal("M5 feedback was not delivered exactly once per Planner round")
			}
			d := v.Verification.Deliveries[index]
			v.Ledger.Action, v.Ledger.Round, v.Ledger.NoProgress = "yield", step-1, step-1
			v.VerificationRequest = nil
			v.VerificationReview = &PlannerVerificationReview{DeliveryID: d.ID, Claim: d.Claim, NextAction: "yield", Disputes: []VerificationIssue{}, Assessment: VerificationAssessment{Support: "incomplete", Reason: "Agreement does not establish causation", Basis: basis, RuntimeBasis: []Evidence{}, Measurement: "unavailable", Window: "unavailable", Filter: "unavailable", Environment: "unavailable", Release: "unavailable", Counterexamples: []VerificationIssue{}, Gaps: []string{"Runtime observation remains missing"}}}
			v.Gaps = append(v.Gaps, "Runtime observation remains missing; no further authorized path in this fixture")
			if step == 2 {
				switch name {
				case "m5-same-version-missing-only", "m5-feedback-repeat-completed":
					v.Ledger.Action = "verify"
					v.VerificationRequest = &VerificationRequest{Claim: &d.Claim, Reason: "Complete unavailable roles without repeating accepted work"}
				case "m5-new-claim-all-fresh", "m5-new-evidence-all-fresh", "m5-feedback-history-prefix", "m5-new-version-cross-reject":
					v.Ledger.Action = "verify"
					candidate := ClaimCandidate{ID: "candidate-2", Statement: "A revised candidate about the stalled request", Premises: []string{"A different interpretation requires independent verification"}, AllowedEvidence: basis}
					if name == "m5-new-evidence-all-fresh" {
						var previousClaim publication[PureClaim]
						if err := protocol.ReadJSON(d.Claim.Path, &previousClaim); err != nil {
							t.Fatal(err)
						}
						candidate = previousClaim.Data.Candidate
						candidate.AllowedEvidence = slices.Clone(candidate.AllowedEvidence)
						var owner publication[json.RawMessage]
						if err := protocol.ReadJSON(basis[0].Ref.Path, &owner); err != nil {
							t.Fatal(err)
						}
						for _, f := range owner.Files {
							if f.ID != basis[0].FileID {
								candidate.AllowedEvidence = append(candidate.AllowedEvidence, Evidence{Ref: basis[0].Ref, FileID: f.ID})
								break
							}
						}
						if len(candidate.AllowedEvidence) != 2 {
							t.Fatal("fixture needs a second authorized evidence file")
						}
					}
					v.VerificationRequest = &VerificationRequest{Candidate: &candidate, Reason: "New candidate/evidence version requires all three fresh roles"}
				case "m5-feedback-worker-new-claim":
					v.Ledger.Action = "workers"
					v.WorkerTasks = []WorkerTask{{ID: "counterexample-inspection", SourceKind: "code", Responsibility: "evidence-only", Question: "Inspect the falsifiable alternative raised in verification", Requirements: []string{"Report actual supplied evidence and missing runtime observations"}, Basis: basis, DependsOn: []string{}}}
					issue := VerificationIssue{Statement: "A healthy control may show the same observation without a stall", Disposition: "needs inspection", Reason: "Agreement has not ruled out this counterexample", Basis: basis}
					v.VerificationReview.Disputes = []VerificationIssue{issue}
					v.VerificationReview.Assessment.Counterexamples = []VerificationIssue{issue}
				case "m5-agent-changes-reset":
					v.Ledger.Changes = []HypothesisChange{{HypothesisID: "h1", Change: "Agent changed the interpretation", Reason: "Counterexample requires revising the premise", Basis: basis}}
					v.Ledger.NoProgress = 0
				case "m5-agent-inference":
					v.VerificationReview.Assessment.Support = "inference"
				case "m5-agent-runtime-declared":
					v.VerificationReview.Assessment.Support = "runtime-supported under the Agent's declared assumptions"
					v.VerificationReview.Assessment.RuntimeBasis = slices.Clone(basis)
					v.VerificationReview.Assessment.Reason = "Agent assesses the supplied observation as applicable runtime evidence; Controller does not infer that from its source schema"
				}
			}
			if name == "m5-feedback-worker-new-claim" && step == 4 {
				v.Ledger.NoProgress = 1
			}
			if name == "m5-feedback-history-prefix" && step == 3 {
				v.Verification.Deliveries[0].ID = "changed-history"
			}
			v.VerificationReview.NextAction = v.Ledger.Action
			switch name {
			case "m5-feedback-missing":
				v.VerificationReview = nil
			case "m5-feedback-wrong-claim":
				v.VerificationReview.Claim = v.Context
			case "m5-feedback-wrong-action":
				v.VerificationReview.NextAction = "plan"
			case "m5-feedback-model-echo":
				v.Verification.Policy.Con.Model.ID = "analysis"
			}
		}
		return v
	}
	fixtureStep := step
	m4 := strings.HasPrefix(name, "m4-") && name != "m4-nil-recovery-timeout"
	if m4 && task.Previous == nil {
		fixtureStep = 1
	}
	v := plannerFixture(t, "complete", req, task, fixtureStep, contract.Ref{})
	var delivered struct {
		WorkerResults []contract.Ref     `json:"worker_results"`
		WikiResults   []contract.Ref     `json:"wiki_results"`
		AdaptiveNote  string             `json:"adaptive_note"`
		Checkpoint    *PlannerCheckpoint `json:"checkpoint"`
		Recovery      *PlannerRecovery   `json:"recovery"`
	}
	if err := json.Unmarshal([]byte(req.Prompt), &delivered); err != nil {
		t.Fatal(err)
	}
	m3 := strings.HasPrefix(name, "m3-")
	if m4 {
		if delivered.Recovery == nil || !strings.Contains(task.Requirements, recoveryRequirements) {
			t.Fatal("M4 omitted recovery metadata or requirements")
		}
		if (delivered.Checkpoint != nil) != (name == "m4-capacity-fresh-worker-timeout" || name == "m4-allfail-reframe-checkpoint" || strings.HasPrefix(name, "m4-reframe-")) {
			t.Fatal("M4 recovery unexpectedly changed capacity policy")
		}
	} else if m3 && name != "m3-illegal-optin" {
		if delivered.Checkpoint == nil || !strings.HasPrefix(task.Requirements, plannerRequirements+"\n\n"+adaptiveRequirements+"\n\nCopy the supplied checkpoint object exactly") {
			t.Fatal("M3 entry omitted checkpoint or requirements")
		}
	} else if task.Requirements != plannerRequirements+"\n\n"+adaptiveRequirements || delivered.Checkpoint != nil {
		t.Fatal("adaptive Planner lost its requirements or nil policy gained checkpoint requirements")
	}
	previousWorkers, previousWikis := len(v.WorkerResults), len(v.WikiResults)
	l := &InvestigationLedger{Action: "yield", Reason: "Hand accepted investigation state to later work; no verified conclusion", ConsumedBatch: []contract.Ref{}, Changes: []HypothesisChange{}}
	if v.Ledger != nil {
		l.Round, l.NoProgress = v.Ledger.Round, v.Ledger.NoProgress
		l.ReframeRound, l.ReframeStreak = v.Ledger.ReframeRound, v.Ledger.ReframeStreak
		if len(delivered.WikiResults) > previousWikis && v.Ledger.Action == "reframe" {
			l.ReframeRound, l.ReframeStreak = l.Round, l.NoProgress
		}
	}
	v.Context = req.Inputs[0]
	v.WorkerTasks, v.SupportingWork, v.WikiTask = []WorkerTask{}, nil, nil
	v.WorkerResults = append([]contract.Ref{}, delivered.WorkerResults...)
	v.WikiResults = append([]contract.Ref{}, delivered.WikiResults...)
	basis := slices.Clone(v.Hypotheses[0].Evidence)
	if len(v.WorkerResults) > previousWorkers {
		l.ConsumedBatch = slices.Clone(v.WorkerResults[previousWorkers:])
		l.Round++
		l.NoProgress++
		if name == "m2-progress-explicit" && l.Round == 2 {
			l.Changes = []HypothesisChange{{HypothesisID: "h1", Change: "Agent reports a changed interpretation without rewriting assessment", Reason: "The supplied comparison changes the hypothesis", Basis: []Evidence{{Ref: &v.WorkerResults[len(v.WorkerResults)-1], FileID: "worker-raw"}}}}
			l.NoProgress, l.ReframeStreak = 0, 0
		}
	}
	for _, ref := range v.WorkerResults {
		var p publication[WorkerResult]
		if err := protocol.ReadJSON(ref.Path, &p); err != nil {
			t.Fatal(err)
		}
		for _, owner := range append([]contract.Ref{ref, p.Data.Proposal, p.Data.Context}, p.Data.Inputs...) {
			if !slices.Contains(req.Inputs, owner) {
				t.Fatal("adaptive Planner lost exact worker evidence owner")
			}
		}
	}
	for _, ref := range v.WikiResults {
		var p publication[WikiSearch]
		if err := protocol.ReadJSON(ref.Path, &p); err != nil || p.Data.Task == nil {
			t.Fatal("missing investigation wiki binding", err)
		}
		for _, owner := range append([]contract.Ref{ref, p.Data.Task.Proposal, p.Data.Task.Context}, p.Data.Task.Inputs...) {
			if !slices.Contains(req.Inputs, owner) {
				t.Fatal("adaptive Planner lost exact wiki evidence owner")
			}
		}
		for _, gap := range p.Data.Gaps {
			if !slices.Contains(v.Gaps, gap) {
				v.Gaps = append(v.Gaps, gap)
			}
		}
	}
	workers := func(ids ...string) {
		l.Action = "workers"
		for _, id := range ids {
			v.WorkerTasks = append(v.WorkerTasks, WorkerTask{ID: id, SourceKind: "code", Responsibility: "evidence-only", Question: "Inspect anonymous evidence", Requirements: []string{"Keep actual limitations"}, Basis: slices.Clone(basis), DependsOn: []string{}})
		}
	}
	wiki := func(reframe bool) {
		l.Action = "wiki"
		previous := contract.Ref{}
		var c publication[Context]
		if err := protocol.ReadJSON(v.Context.Path, &c); err != nil {
			t.Fatal(err)
		}
		previous = c.Data.Wiki
		if len(v.WikiResults) != 0 {
			previous = v.WikiResults[len(v.WikiResults)-1]
		}
		var old publication[WikiSearch]
		if err := protocol.ReadJSON(previous.Path, &old); err != nil {
			t.Fatal(err)
		}
		v.WikiTask = &InvestigationWikiTask{ID: fmt.Sprintf("wiki-%d", step), Terms: []string{fmt.Sprintf("anonymous healthy control %d", step)}, PreviousTerms: slices.Clone(old.Data.Queries), Reason: "Agent chooses a healthy-control comparison", Basis: slices.Clone(basis)}
		if reframe {
			l.Action = "reframe"
			l.Reframe = &InvestigationReframe{Change: "Compare healthy controls instead of repeating the failing trace", Reason: "Agent reports two rounds without hypothesis progress", Basis: slices.Clone(basis)}
		}
	}
	switch {
	case slices.Contains([]string{"m2-parallel-batches-yield", "m2-batch-consumed-order", "m2-batch-results-order", "m2-batch-foreign-ref"}, name) || strings.HasPrefix(name, "m2-branch-"):
		if step == 1 {
			workers("w1", "w2", "w3", "w4", "w5")
			v.WorkerTasks[3].DependsOn = []string{"w1"}
		} else if step == 2 {
			workers("w4", "w5")
			v.WorkerTasks[0].DependsOn = []string{"w1"}
		}
	case name == "m2-no-ready-feedback":
		if step == 1 {
			workers("waiting")
			v.WorkerTasks[0].DependsOn = []string{"not-accepted"}
		} else if step == 2 {
			if !strings.Contains(delivered.AdaptiveNote, "No declared task has all dependencies in accepted worker results") || l.Round != 0 || l.NoProgress != 0 {
				t.Fatal("no-ready dispatch did not return mechanical feedback without progress")
			}
			workers("w1")
		}
	case strings.HasPrefix(name, "m2-reframe-"):
		if step < 3 {
			workers(fmt.Sprintf("w%d", step))
		} else if step == 3 || (name == "m2-reframe-repeat" && step == 6) {
			wiki(true)
		} else if name == "m2-reframe-repeat" && (step == 4 || step == 5) {
			workers(fmt.Sprintf("w%d", step-1))
		}
	case name == "m2-wiki-support":
		switch step {
		case 1:
			workers("w1")
		case 2:
			wiki(false)
		case 3:
			l.Action = "support"
			v.SupportingWork = &SupportingWork{Kind: "resolve", Reason: "Resolve the existing time gap", Basis: slices.Clone(basis), Sources: []intakeWork{}}
		}
	case strings.HasPrefix(name, "m2-wiki-"):
		if step == 1 {
			wiki(false)
		} else if step == 2 {
			workers("w1")
			v.WorkerTasks[0].Basis = []Evidence{{Ref: &v.WikiResults[0], FileID: "search"}}
			if name == "m2-wiki-partial-runtime" || name == "m2-wiki-runtime" {
				v.WorkerTasks[0].SourceKind = "logs"
				v.WorkerTasks[0].Search = &WorkerSearch{Source: "anonymous", Filter: "trace=fixture", From: "2025-01-01T00:00:00Z", To: "2025-01-01T00:01:00Z", Basis: slices.Clone(basis)}
			}
		}
	default:
		if step == 1 {
			workers("w1")
		} else if step == 2 && name == "m2-progress-explicit" {
			workers("w2")
		} else if step == 2 && name == "m2-session-not-progress" {
			l.Action = "plan"
		}
	}
	if m4 {
		priorDeliveries := 0
		if v.Recovery != nil {
			priorDeliveries = len(v.Recovery.Deliveries)
		}
		v.Recovery, v.Checkpoint = delivered.Recovery, delivered.Checkpoint
		v.RecoveryChoices = []RecoveryChoice{}
		v.WorkerTasks, v.SupportingWork, v.WikiTask = []WorkerTask{}, nil, nil
		l.Action = "yield"
		if len(v.Recovery.Deliveries) > priorDeliveries {
			d := v.Recovery.Deliveries[priorDeliveries]
			if d.Kind == "workers" && len(d.Results) == 0 {
				l.Round++
				l.NoProgress++
			}
		}
		if len(v.Recovery.Deliveries) == 0 && (name == "m4-success-batch" || name == "m4-worker-timeout" || name == "m4-capacity-fresh-worker-timeout" || strings.HasPrefix(name, "m4-mixed-") || strings.HasPrefix(name, "m4-delivery-")) {
			workers("w1")
			if name == "m4-success-batch" || strings.HasPrefix(name, "m4-mixed-") {
				workers("w2", "w3")
			}
		}
		if strings.HasPrefix(name, "m4-allfail-") || strings.HasPrefix(name, "m4-reframe-") {
			if len(v.Recovery.Deliveries) < 2 {
				workers(fmt.Sprintf("failed-%d", len(v.Recovery.Deliveries)))
			} else if (name == "m4-allfail-reframe-checkpoint" || strings.HasPrefix(name, "m4-reframe-")) && len(v.Recovery.Deliveries) == 2 {
				wiki(true)
			}
			if len(v.Recovery.Deliveries) > 0 && len(v.Recovery.Deliveries) <= 2 {
				d := v.Recovery.Deliveries[len(v.Recovery.Deliveries)-1]
				v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "redirect", Reason: "Use a different offline source; do not resubmit the uncertain operation", Basis: slices.Clone(basis)}}
			}
			if name == "m4-allfail-agent-progress" && len(v.Recovery.Deliveries) == 2 {
				l.Changes = []HypothesisChange{{HypothesisID: "h1", Change: "Agent reinterprets the existing evidence despite execution failure", Reason: "Alternative interpretation, not timeout as incident disproof", Basis: slices.Clone(basis)}}
				l.NoProgress, l.ReframeStreak = 0, 0
			}
		}
		if strings.HasPrefix(name, "m4-meta-") && len(v.Recovery.PlannerFailures) > 0 {
			f := &v.Recovery.PlannerFailures[0]
			switch name {
			case "m4-meta-identity":
				f.Identity.SessionID = "not-the-dispatched-session"
			case "m4-meta-attempt":
				f.AttemptID = req.Identity.AttemptID
			case "m4-meta-cleanup":
				f.Cleanup.Identity.HandleID = "not-the-owned-handle"
			case "m4-meta-diagnostic":
				f.Diagnostic = "invented diagnostic"
			case "m4-meta-prefix":
				if task.Previous == nil {
					l.Action = "plan"
				} else {
					f.Diagnostic = "rewritten historical diagnostic"
				}
			}
		}
		if strings.HasPrefix(name, "m4-reframe-") {
			deliveries := len(v.Recovery.Deliveries)
			wantRound := []int{0, 1, 2, 2, 3, 3}
			wantStreak := wantRound
			wantBoundary, wantChanges := 0, 0
			if deliveries >= 5 {
				wantBoundary = 3
			}
			if name == "m4-reframe-stale-inspection" {
				wantRound, wantStreak = []int{0, 1, 2, 2, 3, 4, 5}, []int{0, 1, 2, 2, 0, 1, 2}
				wantBoundary = 0
				if deliveries == 4 {
					l.Changes = []HypothesisChange{{HypothesisID: "h1", Change: "Agent revises the hypothesis using inspection evidence", Reason: "A changed interpretation, not automatic inspection progress", Basis: []Evidence{{Ref: &v.WorkerResults[0], FileID: "worker-raw"}}}}
					l.NoProgress, l.ReframeStreak, wantChanges = 0, 0, 1
				}
			}
			if name == "m4-reframe-completed-inspection" {
				wantRound = []int{0, 1, 2, 2, 3, 3, 4, 5}
				wantStreak = wantRound
			}
			if deliveries >= len(wantRound) || l.Round != wantRound[deliveries] || l.NoProgress != wantStreak[deliveries] || l.ReframeRound != wantBoundary || l.ReframeStreak != wantBoundary || len(l.Changes) != wantChanges || v.Checkpoint.DispatchCycle != deliveries || v.Checkpoint.CheckpointCycle != deliveries/3*3 {
				t.Fatalf("reframe recovery changed round/no-progress/boundary/checkpoint: deliveries=%d ledger=%+v checkpoint=%+v", deliveries, l, v.Checkpoint)
			}
			if deliveries >= 3 {
				d := v.Recovery.Deliveries[2]
				if d.Kind != "wiki" || len(d.Results) != 0 || len(d.Failures) != 1 || d.Failures[0].Code != engine.TimedOut || d.Failures[0].Origin != engine.OriginAttemptDeadline || d.Failures[0].Cleanup == nil || !d.Failures[0].Cleanup.ConfirmsLocalClose(d.Failures[0].Identity.SessionID) {
					t.Fatal("reframe timeout lost actual failed delivery or confirmed cleanup")
				}
				if len(v.Checkpoint.ControllerFeedback) != 1 || v.Checkpoint.ControllerFeedback[0].After != d.Proposal {
					t.Fatal("fresh Planner lost committed reframe handoff feedback")
				}
				switch deliveries {
				case 3:
					if !v.Checkpoint.FullCheckpoint || len(v.WorkerResults) != 0 || len(v.WikiResults) != 0 {
						t.Fatal("failed reframe invented results or lost the three-delivery checkpoint")
					}
					workers("inspection")
					v.WorkerTasks[0].Question = "Read anonymous-wiki-job status without creating or resubmitting remote work"
					v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "inspect", Reason: "Inspect unknown reframe wiki job read-only before continuation", Basis: slices.Clone(basis)}}
					if name == "m4-reframe-no-inspection" {
						v.WorkerTasks[0].Question = "Investigate a different offline source"
						v.RecoveryChoices[0].Action = "redirect"
						v.RecoveryChoices[0].Reason = "Use offline evidence without repeating the uncertain wiki operation"
					}
				case 4:
					if len(v.WorkerResults) != 1 || len(l.ConsumedBatch) != 1 {
						t.Fatal("safe reframe resume lacks the committed inspection round")
					}
					var original publication[PlannerState]
					if err := protocol.ReadJSON(d.Proposal.Path, &original); err != nil {
						t.Fatal(err)
					}
					v.WikiTask, l.Action, l.Reframe = original.Data.WikiTask, "reframe", original.Data.Ledger.Reframe
					v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "resume", Reason: "Inspection confirms anonymous-wiki-job completed; read its result without resubmitting the search", Basis: []Evidence{{Ref: &v.WorkerResults[0], FileID: "worker-raw"}}}}
				}
				if name == "m4-reframe-stale-inspection" && deliveries >= 4 {
					v.WikiTask, l.Reframe = nil, nil
					workers(fmt.Sprintf("inspection-%d", deliveries))
					v.WorkerTasks[0].Question = "Read remaining anonymous job status without resubmitting work"
					v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "inspect", Reason: "Continue read-only inspection of the earlier unresolved wiki operation", Basis: slices.Clone(basis)}}
				}
				if name == "m4-reframe-completed-inspection" && deliveries >= 5 {
					workers(fmt.Sprintf("later-%d", deliveries))
					if deliveries == 7 {
						v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "inspect", Reason: "Inspect the earlier wiki operation again", Basis: slices.Clone(basis)}}
					}
				}
			}
			t.Logf("reframe recovery: deliveries=%d action=%s round=%d no_progress=%d reframe=%d/%d", deliveries, l.Action, l.Round, l.NoProgress, l.ReframeRound, l.ReframeStreak)
		}
		if name == "m4-wiki-timeout-partial-resume" {
			switch len(v.Recovery.Deliveries) {
			case 0:
				wiki(false)
			case 1:
				workers("inspection")
				v.RecoveryChoices = []RecoveryChoice{{DeliveryID: v.Recovery.Deliveries[0].ID, Action: "inspect", Reason: "Inspect unknown wiki job before continuing", Basis: slices.Clone(basis)}}
			case 2:
				d := v.Recovery.Deliveries[0]
				var original publication[PlannerState]
				if err := protocol.ReadJSON(d.Proposal.Path, &original); err != nil {
					t.Fatal(err)
				}
				v.WikiTask, l.Action = original.Data.WikiTask, "wiki"
				v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "resume", Reason: "Inspection confirms wiki job completed; read the result without repeating remote work", Basis: []Evidence{{Ref: &v.WorkerResults[0], FileID: "worker-raw"}}}}
			}
		}
		if strings.HasPrefix(name, "m4-support-") {
			switch len(v.Recovery.Deliveries) {
			case 0:
				l.Action = "support"
				v.SupportingWork = &SupportingWork{Kind: "resolve", Reason: "Complete supporting wiki prerequisites", Basis: slices.Clone(basis), Sources: []intakeWork{}}
				if strings.HasPrefix(name, "m4-support-update-") {
					v.SupportingWork.Kind = "update"
				}
			case 1:
				d := v.Recovery.Deliveries[0]
				if d.Support == nil || len(d.Failures) != 1 || len(d.Results) != 0 || (d.Support.Intake != nil) != strings.HasPrefix(name, "m4-support-update-") || d.Support.Proposal != d.Proposal || d.Support.Context != v.Context {
					t.Fatal("support failure lost original proposal/phase or reacquired intake")
				}
				workers("inspection")
				v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "inspect", Reason: "Remote status is unknown; inspect read-only evidence before continuation", Basis: slices.Clone(basis)}}
			case 2:
				d := v.Recovery.Deliveries[0]
				if len(v.WorkerResults) != 1 {
					t.Fatal("resume lacks a committed inspection result")
				}
				l.Action = "support"
				work := d.Support.Work
				v.SupportingWork = &work
				v.RecoveryChoices = []RecoveryChoice{{DeliveryID: d.ID, Action: "resume", Reason: "Inspection confirms the remote read completed and continuation will not resubmit it", Basis: []Evidence{{Ref: &v.WorkerResults[0], FileID: "worker-raw"}}}}
			}
		}
		if strings.HasPrefix(name, "m4-support-unsafe-") && len(v.Recovery.Deliveries) == 2 {
			switch name {
			case "m4-support-unsafe-no-basis":
				v.RecoveryChoices[0].Basis = []Evidence{}
			case "m4-support-unsafe-no-reason":
				v.RecoveryChoices[0].Reason = " "
			case "m4-support-unsafe-owner":
				bad := *v.RecoveryChoices[0].Basis[0].Ref
				bad.SHA256 = strings.Repeat("0", 64)
				v.RecoveryChoices[0].Basis[0].Ref = &bad
			}
		}
		if strings.HasPrefix(name, "m4-delivery-") && len(v.Recovery.Deliveries) == 1 {
			d := &v.Recovery.Deliveries[0]
			switch name {
			case "m4-delivery-id":
				d.ID = "invented-delivery-id"
			case "m4-delivery-proposal":
				d.Proposal = d.Context
			case "m4-delivery-context":
				d.Context = d.Proposal
			case "m4-delivery-results":
				d.Results = []contract.Ref{}
			}
		}
		if (name == "m4-allfail-blind-new-id" || name == "m4-allfail-caller-bool") && len(v.Recovery.Deliveries) == 1 {
			v.RecoveryChoices = []RecoveryChoice{}
		}
		switch name {
		case "m4-policy-echo":
			v.Recovery.Policy.PlannerRetries++
		case "m4-drop-recovery":
			v.Recovery = nil
		case "m4-cycle-echo":
			v.Recovery.DispatchCycle++
		}
	}
	if m3 {
		v.WorkerTasks, v.SupportingWork, v.WikiTask = []WorkerTask{}, nil, nil
		l.Action = "yield"
		v.Checkpoint = delivered.Checkpoint
		if len(l.ConsumedBatch) > 0 {
			l.Changes = []HypothesisChange{{HypothesisID: "h1", Change: "Agent reports an anonymous comparison", Reason: "Continue the authorized investigation", Basis: basis}}
			l.NoProgress, l.ReframeStreak = 0, 0
		}
		support := func() {
			l.Action = "support"
			v.SupportingWork = &SupportingWork{Kind: "resolve", Reason: "Complete the missing supporting wiki search", Basis: slices.Clone(basis), Sources: []intakeWork{}}
		}
		switch name {
		case "m3-cycle-six":
			switch step {
			case 1:
				workers("w1", "w2", "w3")
			case 2:
				support()
			case 3, 5:
				wiki(false)
			case 4, 6:
				workers(fmt.Sprintf("w%d", step))
			}
		case "m3-support-no-sample", "m3-support-pending":
			if step == 1 {
				support()
			}
		case "m3-yield-no-sample":
		case "m3-plan-zero", "m3-capacity-at-plan", "m3-fresh-reconstruct":
			if step == 1 {
				l.Action = "plan"
			}
			if step == 2 {
				workers("w1")
			}
		case "m3-no-ready-zero", "m3-feedback-reorder":
			if step == 1 {
				workers("waiting")
				v.WorkerTasks[0].DependsOn = []string{"not-accepted"}
			} else if step == 2 && name == "m3-no-ready-zero" {
				if !strings.Contains(delivered.AdaptiveNote, "No declared task") {
					t.Fatal("M3 lost no-ready feedback")
				}
				workers("w1")
			}
		case "m3-fresh-wiki-before-step":
			if step == 1 {
				wiki(false)
			}
		default:
			if step == 1 {
				workers("w1")
			}
		}
		if step == 1 && name == "m3-checkpoint-missing" {
			v.Checkpoint = nil
		}
		if step == 1 && name == "m3-policy-initial-echo" {
			v.Checkpoint.Policy.HandoffPercent = 81
		}
		if strings.HasPrefix(name, "m3-retained-feedback-") {
			if step == 2 {
				l.Action = "plan"
			}
			if step == 3 {
				switch name {
				case "m3-retained-feedback-drop":
					v.Checkpoint.ControllerFeedback = v.Checkpoint.ControllerFeedback[1:]
				case "m3-retained-feedback-change":
					v.Checkpoint.ControllerFeedback[0].Note = "rewritten committed feedback"
				case "m3-retained-feedback-reorder":
					slices.Reverse(v.Checkpoint.ControllerFeedback)
				}
			}
		}
		if step == 1 && name == "m3-illegal-optin" {
			v.Checkpoint = &PlannerCheckpoint{Policy: PlannerCapacityPolicy{HandoffPercent: 80}, ControllerFeedback: []PlannerFeedback{}}
		}
		if step == 2 {
			c := v.Checkpoint
			switch name {
			case "m3-checkpoint-drop":
				v.Checkpoint = nil
			case "m3-policy-change":
				c.Policy.HandoffPercent = 81
			case "m3-cycle-backward":
				c.DispatchCycle = 0
			case "m3-cycle-skip":
				c.DispatchCycle++
			case "m3-checkpoint-forged":
				c.CheckpointCycle = 3
			case "m3-full-forged":
				c.FullCheckpoint = true
			case "m3-feedback-drop":
				c.ControllerFeedback = []PlannerFeedback{}
			case "m3-feedback-change":
				c.ControllerFeedback[0].Note = "Agent rewrote controller diagnostic"
			case "m3-feedback-reorder":
				slices.Reverse(c.ControllerFeedback)
			case "m3-feedback-owner":
				c.ControllerFeedback[0].After = v.Context
			case "m3-feedback-blank":
				c.ControllerFeedback[0].Note = " "
			case "m3-feedback-invent":
				c.ControllerFeedback = append(c.ControllerFeedback, PlannerFeedback{After: *v.Previous, Note: "Invented diagnostic"})
			case "m3-note-change":
				c.AdaptiveNote = "invented controller note"
			}
		}
	}
	if name == "m2-parallel-batches-yield" && step == 3 {
		v.Gaps = append(v.Gaps, "Agent reports no further authorized comparison; later phase must retain uncertainty")
	}
	v.Ledger = l
	if name == "m2-assessment-not-progress" && step == 2 {
		v.Hypotheses[0].Assessment = "Entirely rewritten wording, with no reported hypothesis change"
	}
	if name == "m2-query-not-progress" || name == "m2-query-utc" {
		for i := range v.WorkerTasks {
			v.WorkerTasks[i].SourceKind = "logs"
			v.WorkerTasks[i].Search = &WorkerSearch{Source: "anonymous", Filter: "trace=fixture", From: "2025-01-01T00:00:00Z", To: "2025-01-01T00:01:00Z", Basis: slices.Clone(basis)}
		}
	}
	if step == 1 {
		switch name {
		case "m2-ledger-missing":
			v.Ledger = nil
		case "m2-action-mismatch":
			l.Action = "yield"
		case "m2-progress-without-batch":
			l.Changes = []HypothesisChange{{HypothesisID: "h1", Change: "new", Reason: "not a worker round", Basis: basis}}
		case "m2-round-initial":
			l.Round = 1
		case "m2-wiki-previous-terms":
			v.WikiTask.PreviousTerms = []string{"invented previous query"}
		case "m2-wiki-basis-owner":
			v.WikiTask.Basis[0].Ref = nil
		}
	}
	if step == 2 {
		switch name {
		case "m2-batch-consumed-order":
			l.ConsumedBatch[0], l.ConsumedBatch[1] = l.ConsumedBatch[1], l.ConsumedBatch[0]
		case "m2-batch-results-order":
			v.WorkerResults[0], v.WorkerResults[1] = v.WorkerResults[1], v.WorkerResults[0]
		case "m2-batch-foreign-ref":
			v.WorkerResults[0].SHA256 = strings.Repeat("0", 64)
		case "m2-consumed-drop":
			l.ConsumedBatch = []contract.Ref{}
		case "m2-consumed-foreign":
			l.ConsumedBatch[0].SHA256 = strings.Repeat("0", 64)
		case "m2-round-echo":
			l.Round++
		case "m2-streak-echo", "m2-session-not-progress":
			if name == "m2-streak-echo" {
				l.NoProgress = 0
			}
		case "m2-reframe-counter-echo":
			l.ReframeRound++
		case "m2-ledger-drop":
			v.Ledger = nil
		case "m2-progress-unknown-id", "m2-progress-owner", "m2-progress-duplicate":
			l.Changes = []HypothesisChange{{HypothesisID: "h1", Change: "Agent-declared change", Reason: "comparison", Basis: basis}}
			l.NoProgress = 0
			if name == "m2-progress-unknown-id" {
				l.Changes[0].HypothesisID = "unknown"
			}
			if name == "m2-progress-owner" {
				l.Changes[0].Basis[0].Ref = nil
			}
			if name == "m2-progress-duplicate" {
				l.Changes = append(l.Changes, l.Changes[0])
			}
		case "m2-wiki-drop-ref":
			v.WikiResults = nil
			v.WorkerTasks[0].Basis = slices.Clone(basis)
		case "m2-wiki-partial-drop-gap":
			v.Gaps = []string{}
		}
	}
	if step == 3 {
		switch name {
		case "m2-reframe-required":
			l.Action, l.Reframe, v.WikiTask = "plan", nil, nil
		case "m2-reframe-same-terms":
			v.WikiTask.Terms = slices.Clone(v.WikiTask.PreviousTerms)
		case "m2-reframe-yield-gap":
			l.Action, l.Reframe, v.WikiTask = "yield", nil, nil
			v.Gaps = []string{"No further authorized evidence path; next phase must retain this gap"}
		}
	}
	if step == 4 && name == "m2-reframe-wiki-reset" {
		l.NoProgress = 0
	}
	return v
}

func m2PlannerData(t *testing.T, v PlannerState) map[string]any {
	t.Helper()
	var data map[string]any
	if err := json.Unmarshal(testJSON(v), &data); err != nil {
		t.Fatal(err)
	}
	// Agent JSON must explicitly retain empty deliveries; the compatibility
	// Go type omits them for old non-adaptive callers.
	data["worker_tasks"] = append([]WorkerTask{}, v.WorkerTasks...)
	data["worker_results"] = append([]contract.Ref{}, v.WorkerResults...)
	data["wiki_results"] = append([]contract.Ref{}, v.WikiResults...)
	return data
}

func m2WikiFixture(t *testing.T, name string, req contract.Request) (WikiSearch, map[string][]byte) {
	t.Helper()
	var task struct {
		Intake       contract.Ref          `json:"intake"`
		Task         InvestigationWikiTask `json:"task"`
		Binding      WikiTaskBinding       `json:"binding"`
		Requirements string                `json:"requirements"`
	}
	if err := json.Unmarshal([]byte(req.Prompt), &task); err != nil {
		t.Fatal(err)
	}
	requirements := investigationWikiRequirements
	if strings.HasPrefix(name, "m4-") || strings.HasPrefix(name, "m5-supplement-reframe-") {
		requirements += "\nRead the exact proposal recovery metadata and choices. Do not repeat a failed search or submit remote work unless the Agent has supplied an evidence-backed safe resume or nonoverlapping redirect. Preserve the original failed proposal/task and diagnostic binding in recovery metadata; this attempt has its own binding."
	}
	if !slices.Equal(task.Binding.Inputs, req.Inputs) || !slices.Contains(req.Inputs, task.Binding.Proposal) || !slices.Contains(req.Inputs, task.Binding.Context) || task.Requirements != requirements {
		t.Fatal("wiki task lost exact binding/requirements")
	}
	mode := "complete"
	if strings.Contains(name, "partial") || strings.HasPrefix(name, "m4-reframe-") {
		mode = "wiki-partial"
	}
	v, files := wikiFixture(mode, task.Intake)
	v.Task, v.Queries = &task.Binding, slices.Clone(task.Task.Terms)
	files["search"] = testJSON(map[string]any{"queries": v.Queries, "status": v.Status, "gaps": v.Gaps})
	switch name {
	case "m2-wiki-binding-proposal":
		v.Task.Proposal = task.Binding.Context
	case "m2-wiki-binding-context":
		v.Task.Context = task.Binding.Proposal
	case "m2-wiki-binding-task":
		v.Task.TaskID = "unproposed"
	case "m2-wiki-binding-inputs":
		v.Task.Inputs = []contract.Ref{}
	case "m2-wiki-binding-intake":
		v.Intake = task.Binding.Context
	case "m2-wiki-omitted-term":
		v.Queries = []string{"not the dispatched term"}
	}
	return v, files
}

// All barriers use the existing Host events and the real engine snapshot. Only
// the event-loop goroutine owns these maps; the workflow never mutates them.
type m2Barrier struct {
	held     map[string]protocol.Event
	attempts map[string]string
	order    []string
	waiting  string
	proved   bool
	faulted  bool
	stats    []contract.Ref
}

func (b *m2Barrier) release(t *testing.T, r *engine.Run, name, bridge string) {
	t.Helper()
	if strings.HasPrefix(name, "m5-supplement-exhausted-") {
		if len(b.held) != 1 || b.faulted {
			return
		}
		exhausted, fatal := "pro", "cross"
		if strings.HasSuffix(name, "fatal-pro") {
			exhausted, fatal = "cross", "pro"
		}
		for _, retry := range r.Snapshot().Retries {
			if strings.HasSuffix(retry.Scope, "-"+exhausted+"-recovery") && !retry.Active {
				if retry.RetryCount != 1 || retry.MaxRetries != 1 {
					t.Fatal("fatal permutation did not exhaust the real role retry")
				}
				b.proved, b.faulted = true, true
				b.order = []string{exhausted, fatal}
				if err := b.held[fatal].Reply(protocol.Control{Type: "provider-error"}); err != nil {
					t.Fatal(err)
				}
				return
			}
		}
		return
	}
	if strings.HasPrefix(name, "m5-") {
		if len(b.held) != 3 || b.faulted {
			return
		}
		snapshot := r.Snapshot()
		if !b.proved {
			live, planners, verifiers := 0, 0, 0
			for _, session := range snapshot.Sessions {
				if session.State == "Closed" {
					continue
				}
				live++
				if session.Role.Name == "triage-planner" {
					planners++
				}
				if strings.HasPrefix(session.Role.Name, "triage-verify-") {
					verifiers++
				}
			}
			if live != 4 || planners != 1 || verifiers != 3 {
				t.Fatalf("M5 parallel barrier live=%d Planner=%d verifiers=%d", live, planners, verifiers)
			}
			for _, id := range []string{"pro", "con", "cross"} {
				if snapshot.Attempts[b.attempts[id]].Output != nil {
					t.Fatal("verifier completed before all three started")
				}
			}
			b.proved = true
		}
		if b.waiting != "" {
			a := snapshot.Attempts[b.attempts[b.waiting]]
			if a.State != engine.Succeeded || a.Output == nil || snapshot.Sessions[a.HandleID].State != "Closed" {
				return
			}
			finished := false
			for _, retry := range snapshot.Retries {
				if strings.HasSuffix(retry.Scope, "-"+b.waiting+"-recovery") && !retry.Active {
					finished = true
				}
			}
			if !finished {
				return
			}
			b.waiting = ""
		}
		if len(b.order) == 3 {
			return
		}
		order := []string{"cross", "con", "pro"}
		if strings.HasPrefix(name, "m5-partial-") {
			order = []string{"pro", "con", "cross"}
		}
		id, ack := order[len(b.order)], "settle"
		if id == "cross" && strings.HasPrefix(name, "m5-partial-") {
			switch name {
			case "m5-partial-provider-fatal":
				ack = "provider-error"
			case "m5-partial-user-cancel":
				ack = "hold"
			case "m5-partial-cleanup-fatal":
				sid := snapshot.Sessions[snapshot.Attempts[b.attempts[id]].HandleID].Identity.SessionID
				path := filepath.Join(bridge, sid+".json")
				if err := os.Rename(path, path+".recovering"); err != nil {
					t.Fatal(err)
				}
			case "m5-partial-storage-fatal", "m5-partial-journal-fatal":
				file := "run.json"
				if name == "m5-partial-journal-fatal" {
					file = "events.jsonl"
				}
				path := filepath.Join(r.Dir(), file)
				if err := os.Rename(path, path+".recovering"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			b.faulted = true
		}
		if err := b.held[id].Reply(protocol.Control{Type: ack}); err != nil {
			t.Fatal(err)
		}
		b.order, b.waiting = append(b.order, id), id
		if name == "m5-partial-user-cancel" && id == "cross" {
			r.Cancel(engine.OriginControllerUser)
		}
		return
	}
	if len(b.held) != 3 || b.faulted {
		return
	}
	snapshot := r.Snapshot()
	if !b.proved {
		live, planners, workers := 0, 0, 0
		for _, s := range snapshot.Sessions {
			if s.State == "Closed" {
				continue
			}
			live++
			if s.Role.Name == "triage-planner" {
				planners++
			}
			if strings.HasPrefix(s.Role.Name, "triage-worker-") {
				workers++
			}
		}
		if live != 4 || planners != 1 || workers != 3 {
			t.Fatalf("M2 barrier live=%d planners=%d workers=%d", live, planners, workers)
		}
		for _, id := range []string{"w1", "w2", "w3"} {
			if snapshot.Attempts[b.attempts[id]].Output != nil {
				t.Fatal("worker committed before all three prompts reached barrier")
			}
		}
		b.proved = true
	}
	if name == "m4-mixed-three-failed" {
		for _, id := range []string{"w1", "w3", "w2"} {
			ack := "hold"
			if id == "w2" {
				ack = "compaction-error"
			}
			if err := b.held[id].Reply(protocol.Control{Type: ack}); err != nil {
				t.Fatal(err)
			}
		}
		b.faulted, b.order = true, []string{"w2"}
		return
	}
	if b.waiting != "" {
		a := snapshot.Attempts[b.attempts[b.waiting]]
		if a.State != engine.Succeeded || a.Output == nil || snapshot.Sessions[a.HandleID].State != "Closed" && !strings.HasPrefix(name, "m4-mixed-committed-close-") {
			return
		}
		if name == "m4-mixed-committed-close-cleanup-fatal" {
			sid := snapshot.Sessions[a.HandleID].Identity.SessionID
			path := filepath.Join(bridge, sid+".json")
			if err := os.Rename(path, path+".recovering"); err != nil {
				t.Fatal(err)
			}
		}
		b.waiting = ""
	}
	if len(b.order) == 3 {
		return
	}
	id := []string{"w3", "w2", "w1"}[len(b.order)]
	ack := "settle"
	if id == "w2" {
		if (strings.HasPrefix(name, "m2-branch-") && name != "m2-branch-binding") || strings.HasPrefix(name, "m4-mixed-") {
			b.faulted = true
			if name != "m2-branch-abort-unacknowledged" {
				// A streaming sibling must leave the fixture's prompt-ack
				// barrier so the real RPC abort can be acknowledged.
				control := "hold"
				if name == "m4-mixed-storage-fatal" || name == "m4-mixed-journal-fatal" || name == "m4-mixed-user-cancel" {
					control = "hold-abort"
				}
				if name == "m4-mixed-wait-fatal" {
					control = "hold-abort-exit"
				}
				if err := b.held["w1"].Reply(protocol.Control{Type: control}); err != nil {
					t.Fatal(err)
				}
			}
		}
		switch name {
		case "m4-mixed-compaction", "m4-mixed-committed-close-cancel", "m4-mixed-committed-close-cleanup-fatal", "m4-mixed-committed-close-storage-fatal", "m4-mixed-committed-close-journal-fatal", "m4-mixed-binding-fatal", "m4-mixed-storage-fatal", "m4-mixed-journal-fatal", "m4-mixed-user-cancel", "m4-mixed-wait-fatal":
			ack = "compaction-error"
		case "m2-branch-provider-failure", "m2-branch-abort-unacknowledged", "m4-mixed-provider-fatal":
			ack = "provider-error"
		case "m2-branch-cancel", "m2-branch-timeout", "m4-mixed-timeout":
			ack = "hold"
		case "m2-branch-cleanup-failure", "m4-mixed-cleanup-fatal":
			sid := snapshot.Sessions[snapshot.Attempts[b.attempts[id]].HandleID].Identity.SessionID
			path := filepath.Join(bridge, sid+".json")
			if err := os.Rename(path, path+".recovering"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := b.held[id].Reply(protocol.Control{Type: ack}); err != nil {
		t.Fatal(err)
	}
	b.order = append(b.order, id)
	b.waiting = id
	if name == "m2-branch-cancel" && id == "w2" {
		r.Cancel(engine.OriginControllerUser)
	}
}

func r5Run(t *testing.T, ctx context.Context, r *engine.Run, scope Scope, input contract.Ref, name string) (contract.Ref, contract.Ref, error) {
	t.Helper()
	renderer, err := ExtractReport(r.Dir())
	if err != nil {
		return contract.Ref{}, contract.Ref{}, err
	}
	p, err := startPlanner(ctx, r, scope, runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}, input)
	if err != nil {
		return contract.Ref{}, contract.Ref{}, err
	}
	if name != "r5-incomplete" {
		p.adaptive = true
		p.recovery = &PlannerRecovery{Policy: RecoveryPolicy{PlannerRetries: 0}, Deliveries: []RecoveryDelivery{}, PlannerFailures: []RecoveryFailure{}}
		p.verification = &PlannerVerification{Policy: VerificationPolicy{
			Pro:   VerifierPolicy{Model: runtime.ModelSpec{Provider: "fixture", ID: "pro", Thinking: "high"}},
			Con:   VerifierPolicy{Model: runtime.ModelSpec{Provider: "fixture", ID: "con", Thinking: "medium"}},
			Cross: VerifierPolicy{Model: runtime.ModelSpec{Provider: "fixture", ID: "cross", Thinking: "low"}},
		}, Claims: []contract.Ref{}, Deliveries: []VerificationDelivery{}}
		p.identity, err = r.SessionIdentity(ctx, p.handle)
		if err != nil {
			return contract.Ref{}, contract.Ref{}, err
		}
	}
	if _, err = p.planningStep(ctx); err != nil {
		return contract.Ref{}, contract.Ref{}, err
	}
	if name != "r5-incomplete" {
		if err = p.verify(ctx); err != nil {
			return *p.last, contract.Ref{}, err
		}
		if _, err = p.planningStep(ctx); err != nil {
			return *p.last, contract.Ref{}, err
		}
		if strings.HasPrefix(name, "r5-version") {
			if err = p.verify(ctx); err != nil {
				return *p.last, contract.Ref{}, err
			}
			if _, err = p.planningStep(ctx); err != nil {
				return *p.last, contract.Ref{}, err
			}
		}
		if _, err = p.planningStep(ctx); err != nil {
			return *p.last, contract.Ref{}, err
		}
	}
	state := *p.last
	if name == "r5-producer-forgery" {
		task, inputs, inputErr := p.reportInputs(newAcceptance(ctx, r))
		if inputErr != nil {
			return state, contract.Ref{}, inputErr
		}
		if err := p.close(ctx); err != nil {
			return state, contract.Ref{}, err
		}
		foreign, openErr := startPlanner(ctx, r, scope, p.model, input)
		if openErr != nil {
			return state, contract.Ref{}, openErr
		}
		task.Renderer = renderer
		out, stepErr := r.Root().Step(ctx, engine.StepSpec{Key: "report-" + state.AttemptID, Session: foreign.handle, Prompt: string(testJSON(task)), Inputs: inputs, Output: contract.Spec{SchemaID: ReportSchema}, Timeout: time.Minute})
		if stepErr != nil {
			return state, contract.Ref{}, stepErr
		}
		return state, contract.Ref{}, p.checkReport(ctx, out.Output, task, inputs)
	}
	report, err := p.report(ctx, renderer)
	if *p.last != state {
		t.Error("report replaced the accepted investigation state")
	}
	if err != nil {
		return state, contract.Ref{}, err
	}
	if name == "r5-uncommitted" {
		task, inputs, inputErr := p.reportInputs(newAcceptance(ctx, r))
		if inputErr != nil {
			return state, contract.Ref{}, inputErr
		}
		report.Path = filepath.Join(filepath.Dir(report.Path), "candidate.json")
		return state, contract.Ref{}, p.checkReport(ctx, report, task, inputs)
	}
	if name == "r5-historical" {
		later, handoffErr := p.handoff(ctx)
		if handoffErr != nil {
			return state, contract.Ref{}, handoffErr
		}
		if _, err = later.planningStep(ctx); err != nil {
			return state, contract.Ref{}, err
		}
		if err = later.close(ctx); err != nil {
			return state, contract.Ref{}, err
		}
	} else if err = p.close(ctx); err != nil {
		return state, contract.Ref{}, err
	}
	return state, report, nil
}

func r5RenderCandidate(t *testing.T, ctx context.Context, r *engine.Run, name string, m protocol.Control, req contract.Request) bool {
	t.Helper()
	var task reportTask
	if err := json.Unmarshal([]byte(req.Prompt), &task); err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(filepath.Dir(m.CandidatePath), "artifacts", "triage-report.md")
	if name == "r5-renderer-collision" {
		if err := os.WriteFile(reportPath, []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.CommandContext(ctx, "python3", "-B", task.Renderer, filepath.Join(filepath.Dir(m.CandidatePath), "request.json"), m.CandidatePath)
	cmd.WaitDelay = time.Second
	output, err := cmd.CombinedOutput()
	if name == "r5-renderer-collision" {
		if err == nil {
			t.Fatal("renderer overwrote an existing report")
		}
		got, readErr := os.ReadFile(reportPath)
		if readErr != nil || string(got) != "keep" {
			t.Fatalf("collision changed prior content: %v", readErr)
		}
		return false
	}
	if err != nil {
		t.Fatalf("renderer: %v: %s", err, output)
	}
	var envelope struct {
		Data  InvestigationReport `json:"data"`
		Files []file              `json:"files"`
	}
	if err := protocol.ReadJSON(m.CandidatePath, &envelope); err != nil {
		t.Fatal(err)
	}
	switch name {
	case "r5-assessment-owner":
		envelope.Data.Claims[0].AssessmentOwner = task.State
	case "r5-claim-version":
		envelope.Data.Claims[0].Claim = task.Context
	case "r5-state-binding":
		envelope.Data.State = task.Context
	case "r5-context-binding":
		envelope.Data.Context = task.State
	case "r5-artifact-kind":
		envelope.Files[0].Kind = "evidence"
	case "r5-file-path":
		envelope.Files[0].Path = "artifacts/missing.md"
	case "r5-body-tamper":
		if err := os.WriteFile(reportPath, []byte("# Forged report\n"), 0600); err != nil {
			t.Fatal(err)
		}
	case "r5-version-binding":
		envelope.Data.Claims[0].Claim = envelope.Data.Claims[1].Claim
	case "r5-evidence-tamper":
		if err := os.WriteFile(task.Context.Path, []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
	case "r5-cancel":
		r.Cancel(engine.OriginControllerUser)
	}
	writeEnvelope(t, m, req, envelope.Data, envelope.Files)
	return true
}

func r5AssertOutcome(t *testing.T, tc triageCase, report engine.Report, stateRef, reportRef contract.Ref, count int) {
	t.Helper()
	if (report.Failure != nil) != tc.failure {
		t.Fatalf("failure=%v, want %v", report.Failure, tc.failure)
	}
	if count != tc.stages {
		t.Fatalf("report stages=%d, want %d", count, tc.stages)
	}
	if tc.failure {
		want := map[string]string{"r5-assessment-owner": "exact distinct claim/delivery/assessment owner", "r5-claim-version": "exact distinct claim/delivery/assessment owner", "r5-state-binding": "state/context/file binding mismatch", "r5-context-binding": "state/context/file binding mismatch", "r5-body-tamper": "deterministic accepted projection"}[tc.name]
		if want != "" && !strings.Contains(fmt.Sprint(report.Failure), want) {
			t.Fatalf("wrong report rejection: %v, want %s", report.Failure, want)
		}
		if tc.name == "r5-cleanup-failure" && len(report.CleanupErrors) == 0 {
			t.Fatal("cleanup failure was not retained")
		}
		if tc.name == "r5-producer-forgery" && !strings.Contains(fmt.Sprint(report.Failure), "exact Planner producer") {
			t.Fatalf("wrong forged producer rejection: %v", report.Failure)
		}
		if tc.name == "r5-version-binding" && !strings.Contains(fmt.Sprint(report.Failure), "exact distinct claim/delivery/assessment owner") {
			t.Fatalf("wrong version rejection: %v", report.Failure)
		}
		t.Logf("report rejection: %v", report.Failure)
		if report.Final != nil || reportRef.AttemptID != "" {
			t.Fatal("invalid report gained final selection")
		}
		return
	}
	if report.Final == nil || report.Final.Ref != reportRef || report.Final.Output != "report" || report.Final.Step != "report-"+stateRef.AttemptID {
		t.Fatalf("wrong report final: %+v", report.Final)
	}
	attempt := report.Snapshot.Attempts[reportRef.AttemptID]
	if attempt.Output == nil || *attempt.Output != reportRef || attempt.State != engine.Succeeded || report.Final.HandleID != attempt.HandleID || report.Final.Scope != attempt.Scope {
		t.Fatal("final lost exact committed producer")
	}
	if tc.name == "r5-historical" {
		later := false
		for _, other := range report.Snapshot.Attempts {
			later = later || other.HandleID != attempt.HandleID && other.LastSeq > attempt.LastSeq
		}
		if !later {
			t.Fatal("fixture did not exercise final selection of an earlier producer")
		}
	}
	if report.Snapshot.Sessions[attempt.HandleID].State != "Closed" {
		t.Fatal("report Planner was not closed")
	}
	var accepted publication[InvestigationReport]
	if err := protocol.ReadJSON(reportRef.Path, &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Data.State != stateRef || len(accepted.Files) != 1 || report.Final.ArtifactPath != filepath.Join(filepath.Dir(reportRef.Path), accepted.Files[0].Path) {
		t.Fatal("final artifact/state binding changed")
	}
	wantClaims := 1
	if tc.name == "r5-versioned" {
		wantClaims = 2
	}
	if tc.name != "r5-incomplete" && (len(accepted.Data.Claims) != wantClaims || accepted.Data.Claims[0].AssessmentOwner == stateRef) {
		t.Fatal("historical assessment was rebound to yield state")
	}
	body, err := os.ReadFile(report.Final.ArtifactPath)
	if err != nil || !bytes.Contains(body, []byte("Missing runtime evidence")) || !bytes.Contains(body, []byte(stateRef.SHA256)) || bytes.Contains(body, []byte("\"handle_id\"")) {
		t.Fatalf("report projection missing: %v", err)
	}
}

func m2Run(t *testing.T, ctx context.Context, r *engine.Run, scope Scope, models sliceModels, input contract.Ref, name string) (contract.Ref, error) {
	t.Helper()
	plannerModel := runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}
	if strings.HasPrefix(name, "m5-") {
		verification := &VerificationPolicy{
			Pro:   VerifierPolicy{Model: runtime.ModelSpec{Provider: "fixture", ID: "pro", Thinking: "high"}, Retries: 1},
			Con:   VerifierPolicy{Model: runtime.ModelSpec{Provider: "fixture", ID: "con", Thinking: "medium"}, Retries: 1},
			Cross: VerifierPolicy{Model: runtime.ModelSpec{Provider: "fixture", ID: "cross", Thinking: "low"}, Retries: 1},
		}
		recovery := &RecoveryPolicy{PlannerRetries: 1}
		for role, policy := range map[string]*VerifierPolicy{"pro": &verification.Pro, "con": &verification.Con, "cross": &verification.Cross} {
			if name == "m5-policy-"+role+"-model" {
				policy.Model = runtime.ModelSpec{}
			}
			if name == "m5-policy-"+role+"-retries" {
				policy.Retries = -1
			}
		}
		if name == "m5-policy-no-recovery" {
			recovery = nil
		}
		if name == "m5-pending-claim-fresh-handoff" || name == "m5-delivery-fresh-handoff" || strings.Contains(name, "-owner-") || name == "m5-supplement-claim-parent-binding" {
			p, err := startPlanner(ctx, r, scope, plannerModel, input)
			if err != nil {
				return contract.Ref{}, err
			}
			p.adaptive = true
			p.recovery = &PlannerRecovery{Policy: *recovery, Deliveries: []RecoveryDelivery{}, PlannerFailures: []RecoveryFailure{}}
			p.verification = &PlannerVerification{Policy: *verification, Claims: []contract.Ref{}, Deliveries: []VerificationDelivery{}}
			p.identity, err = r.SessionIdentity(ctx, p.handle)
			if err != nil {
				return contract.Ref{}, err
			}
			if _, err = p.planningStep(ctx); err != nil {
				return contract.Ref{}, err
			}
			if strings.HasPrefix(name, "m5-supplement-") {
				validClaim, err := p.claimStep(ctx, r.Root())
				if err != nil {
					return contract.Ref{}, err
				}
				claim, err := newAcceptance(ctx, r).loadClaim(scope, validClaim)
				if err != nil {
					return contract.Ref{}, err
				}
				model, role, key := plannerModel, "triage-planner", "claim-"+p.last.AttemptID
				schema, inputs, data := ClaimSchema, []contract.Ref{*p.last, input}, any(claim)
				if strings.Contains(name, "verifier-owner") {
					model, role, key = verification.Pro.Model, "triage-verify-pro", "verify-"+validClaim.AttemptID+"-pro"
					inputs = claimInputs(validClaim, claim)
					task := map[string]any{"stage": "verify-pro", "role": "pro", "claim": validClaim, "allowed_evidence": claim.Candidate.AllowedEvidence, "requirements": verifierRequirements}
					valid, err := taskStepRecovery(ctx, r, r.Root(), model, "verify-pro", key, task, VerificationSchema, inputs, true)
					if err != nil {
						return contract.Ref{}, err
					}
					if err := newAcceptance(ctx, r).checkVerificationResult(valid, validClaim, claim, "pro", verification.Pro); err != nil {
						return contract.Ref{}, err
					}
					publication, err := readAccepted[VerificationResult](newAcceptance(ctx, r), valid, VerificationSchema)
					if err != nil {
						return contract.Ref{}, err
					}
					schema, data = VerificationSchema, publication.Data
				}
				if name == "m5-supplement-claim-parent-binding" {
					parent, err := readAccepted[PlannerState](newAcceptance(ctx, r), *p.last, PlannerSchema)
					if err != nil {
						return contract.Ref{}, err
					}
					parent.Data.VerificationRequest.Candidate.Statement = "A different unaccepted proposal"
					prompt := string(testJSON(map[string]any{"stage": "m5-supplement-publication", "data": m2PlannerData(t, parent.Data)}))
					out, err := r.Root().Step(ctx, engine.StepSpec{Key: "unaccepted-proposal", Session: p.handle, Prompt: prompt, Inputs: inputs, Output: contract.Spec{SchemaID: PlannerSchema}, Timeout: time.Minute})
					if err != nil {
						return contract.Ref{}, err
					}
					claim.ParentState, claim.Candidate = out.Output, *parent.Data.VerificationRequest.Candidate
					key, data, inputs = "claim-"+out.AttemptID, claim, []contract.Ref{out.Output, input}
				}
				switch {
				case strings.HasSuffix(name, "-role"):
					role += "-unapproved"
				case strings.HasSuffix(name, "-model"):
					model.ID += "-unapproved"
				case strings.HasSuffix(name, "-key"):
					key += "-unapproved"
				}
				if err := r.CloseSession(ctx, p.handle); err != nil {
					return contract.Ref{}, err
				}
				attackScope, err := r.Root().Child("producer-attack")
				if err != nil {
					return contract.Ref{}, err
				}
				handle, err := r.OpenSession(ctx, engine.RoleSpec{Name: role, Model: model})
				if err != nil {
					return contract.Ref{}, err
				}
				prompt := string(testJSON(map[string]any{"stage": "m5-supplement-publication", "data": data}))
				out, err := attackScope.Step(ctx, engine.StepSpec{Key: key, Session: handle, Prompt: prompt, Inputs: inputs, Output: contract.Spec{SchemaID: schema}, Timeout: time.Minute})
				if err != nil {
					return contract.Ref{}, err
				}
				if err := r.CloseSession(ctx, handle); err != nil {
					return contract.Ref{}, err
				}
				attempt := r.Snapshot().Attempts[out.AttemptID]
				owner := r.Snapshot().Sessions[attempt.HandleID]
				if attempt.State != engine.Succeeded || attempt.Output == nil || *attempt.Output != out.Output || attempt.Key != key || owner.Role.Name != role || owner.Role.Model != model || owner.State != "Closed" {
					t.Fatal("producer attack did not reach genuine committed Step ownership")
				}
				if _, err := engine.ReadContract(ctx, r, out.Output); err != nil {
					return contract.Ref{}, err
				}
				switch {
				case name == "m5-supplement-claim-parent-binding":
					p.verification.Claims = []contract.Ref{out.Output}
					err = p.checkPendingClaims(ctx)
				case schema == ClaimSchema:
					_, err = newAcceptance(ctx, r).loadClaim(scope, out.Output)
				default:
					err = newAcceptance(ctx, r).checkVerificationResult(out.Output, validClaim, claim, "pro", verification.Pro)
				}
				if err == nil {
					t.Error("committed producer/binding attack was accepted")
				}
				return contract.Ref{}, err
			}
			if name == "m5-pending-claim-fresh-handoff" {
				_, err = p.claimStep(ctx, r.Root())
			} else {
				err = p.verify(ctx)
			}
			if err != nil {
				return contract.Ref{}, err
			}
			old, attempts := p, len(r.Snapshot().Attempts)
			p, err = p.handoff(ctx)
			if err != nil {
				return contract.Ref{}, err
			}
			if !old.stopped || p.identity.SessionID == old.identity.SessionID || p.last == nil || *p.last != *old.last || !reflect.DeepEqual(p.verification, old.verification) || attempts != len(r.Snapshot().Attempts) {
				t.Error("fresh handoff lost reference-linked pending metadata or created a copying Step")
			}
			if name == "m5-pending-claim-fresh-handoff" {
				if err = p.verify(ctx); err != nil {
					return contract.Ref{}, err
				}
			}
			return p.adapt(ctx, models)
		}
		ref, err := executeInvestigation(ctx, r, scope, plannerModel, models, input, nil, recovery, verification)
		if strings.HasPrefix(name, "m5-supplement-exhausted-") {
			var pending *PendingVerificationError
			var failure *engine.Failure
			if !errors.As(err, &pending) || !errors.As(err, &failure) || failure.Code != engine.ProviderFailed || pending.Claim == nil || pending.History == nil || len(pending.History.Deliveries) != 0 || len(pending.Roles) != 2 {
				t.Errorf("unavailable masked fatal or lost pending feedback: %v", err)
			} else {
				unavailable, accepted := 0, 0
				for _, role := range pending.Roles {
					if role.Unavailable && role.Result == nil && role.Exhausted == string(engine.RetryExhausted) && len(role.Failures) == 2 {
						unavailable++
					}
					if role.Result != nil && !role.Unavailable {
						accepted++
					}
				}
				if unavailable != 1 || accepted != 1 {
					t.Error("fatal lost actual exhausted/successful siblings")
				}
			}
		}
		if strings.HasPrefix(name, "m5-partial-") || slices.Contains([]string{"m5-verifier-role", "m5-verifier-claim", "m5-verifier-evidence", "m5-verifier-unauthorized-basis", "m5-verifier-schema"}, name) {
			var pending *PendingVerificationError
			if !errors.As(err, &pending) || pending.Claim == nil || pending.History == nil || len(pending.History.Claims) != 1 || pending.History.Claims[0] != *pending.Claim || len(pending.History.Deliveries) != 0 || len(pending.Roles) != 2 || pending.Unwrap() == nil {
				t.Errorf("lost accepted claim/partial feedback or manufactured checkpoint: pending=%+v error=%v", pending, err)
			} else {
				var claim publication[PureClaim]
				if e := protocol.ReadJSON(pending.Claim.Path, &claim); e != nil || claim.Data.ParentState != pending.State {
					t.Errorf("pending claim lost reference-linked state: %v", e)
				}
				for _, role := range pending.Roles {
					if role.Result == nil || role.Unavailable || r.Snapshot().Attempts[role.Result.AttemptID].State != engine.Succeeded {
						t.Error("pending role is not an accepted result")
					}
				}
			}
		}
		return ref, err
	}
	if strings.HasPrefix(name, "m4-mixed-committed-close-") {
		p, err := startPlanner(ctx, r, scope, plannerModel, input)
		if err != nil {
			return contract.Ref{}, err
		}
		p.adaptive = true
		p.recovery = &PlannerRecovery{Policy: RecoveryPolicy{PlannerRetries: 1}, Deliveries: []RecoveryDelivery{}, PlannerFailures: []RecoveryFailure{}}
		p.identity, err = r.SessionIdentity(ctx, p.handle)
		if err != nil {
			return contract.Ref{}, err
		}
		if _, err = p.planningStep(ctx); err != nil {
			return contract.Ref{}, err
		}
		prepared := make([]preparedWorker, 3)
		branches := make([]engine.Branch, 3)
		for i, id := range []string{"w1", "w2", "w3"} {
			prepared[i], err = prepareWorker(ctx, r, scope, input, *p.last, nil, id)
			if err != nil {
				return contract.Ref{}, err
			}
			work := prepared[i]
			work.recovery = true
			branches[i] = engine.Branch{Name: id, Do: func(ctx context.Context, s *engine.Scope) (engine.Result, error) {
				if work.request.Task.ID != "w3" {
					ref, err := runWorker(ctx, r, s, models, work)
					return engine.Result{Outputs: map[string]contract.Ref{"worker": ref}}, err
				}
				// Exercise the real commit -> cancelled close boundary without a
				// production hook. This is primitive/consumer integration, not a
				// deterministic reproduction inside taskStepRecovery itself.
				model := runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: models.FetchThinking}
				h, err := r.OpenSession(ctx, engine.RoleSpec{Name: "triage-" + work.request.Stage, CWD: filepath.Join(r.Dir(), "triage-work"), Model: model})
				if err != nil {
					return engine.Result{}, err
				}
				identity, err := r.SessionIdentity(ctx, h)
				if err != nil {
					return engine.Result{}, err
				}
				out, err := s.Step(ctx, engine.StepSpec{Key: work.key, Session: h, Prompt: string(testJSON(work.request)), Inputs: work.inputs, Output: contract.Spec{SchemaID: WorkerSchema}})
				if err != nil {
					return engine.Result{}, err
				}
				<-ctx.Done()
				var cause *engine.Failure
				if !errors.As(context.Cause(ctx), &cause) || cause.Origin != engine.OriginFailFastSibling || r.Snapshot().Sessions[identity.HandleID].State != "Idle" {
					return engine.Result{}, fmt.Errorf("fixture missed committed-before-close sibling cancellation")
				}
				ref, err := closeTaskStep(ctx, r, h, identity, work.request.Stage, out, true)
				if err == nil || ref != out.Output || r.Snapshot().Sessions[identity.HandleID].State != "Idle" {
					return engine.Result{}, fmt.Errorf("cancelled close changed committed output or ran cleanup")
				}
				return engine.Result{Outputs: map[string]contract.Ref{"worker": ref}}, err
			}}
		}
		joined, groupErr := r.Root().Parallel(ctx, "committed-close-boundary", engine.FailFast, branches)
		ref := joined[2].Result.Outputs["worker"]
		attempt := r.Snapshot().Attempts[ref.AttemptID]
		if ref == (contract.Ref{}) || attempt.State != engine.Succeeded || attempt.Output == nil || *attempt.Output != ref {
			return contract.Ref{}, fmt.Errorf("fixture lost the real succeeded sibling: %w", groupErr)
		}
		faultPath := ""
		switch name {
		case "m4-mixed-committed-close-storage-fatal":
			faultPath = filepath.Join(r.Dir(), "run.json")
		case "m4-mixed-committed-close-journal-fatal":
			faultPath = filepath.Join(r.Dir(), "events.jsonl")
		}
		if faultPath != "" {
			if err := os.Rename(faultPath, faultPath+".recovering"); err != nil {
				return contract.Ref{}, err
			}
			if err := os.Mkdir(faultPath, 0700); err != nil {
				return contract.Ref{}, err
			}
		}
		if _, err := p.receiveWorkers(ctx, prepared, joined, groupErr); err != nil {
			if len(p.workerResults) != 0 || len(p.recovery.Deliveries) != 0 {
				t.Error("fatal committed sibling was delivered to Planner")
			}
			return contract.Ref{}, err
		}
		if strings.HasSuffix(name, "-fatal") {
			return contract.Ref{}, fmt.Errorf("committed sibling swallowed cleanup persistence failure")
		}
		if r.Snapshot().Sessions[attempt.HandleID].State != "Closed" {
			return contract.Ref{}, fmt.Errorf("committed sibling accepted before parent-context strict cleanup")
		}
		return p.adapt(ctx, models)
	}
	if strings.HasPrefix(name, "m4-") {
		if name == "m4-nil-recovery-timeout" {
			return executeInvestigation(ctx, r, scope, plannerModel, models, input, nil, nil, nil)
		}
		var capacity *PlannerCapacityPolicy
		if name == "m4-capacity-fresh-worker-timeout" || name == "m4-allfail-reframe-checkpoint" || strings.HasPrefix(name, "m4-reframe-") {
			capacity = &PlannerCapacityPolicy{HandoffPercent: 80}
		}
		return executeInvestigation(ctx, r, scope, plannerModel, models, input, capacity, &RecoveryPolicy{PlannerRetries: 1}, nil)
	}
	m3 := strings.HasPrefix(name, "m3-")
	capacity := &PlannerCapacityPolicy{HandoffPercent: 80}
	if strings.HasPrefix(name, "m3-policy-invalid-") {
		switch strings.TrimPrefix(name, "m3-policy-invalid-") {
		case "zero":
			capacity.HandoffPercent = 0
		case "negative":
			capacity.HandoffPercent = -1
		case "above":
			capacity.HandoffPercent = 101
		case "nan":
			capacity.HandoffPercent = math.NaN()
		case "infinite":
			capacity.HandoffPercent = math.Inf(1)
		}
		return executeInvestigation(ctx, r, scope, plannerModel, models, input, capacity, nil, nil)
	}
	if slices.Contains([]string{"m3-cycle-six", "m3-capacity-below", "m3-capacity-zero", "m3-capacity-at", "m3-capacity-at-plan", "m3-capacity-above", "m3-unknown", "m3-unknown-null", "m3-unknown-missing", "m3-unknown-tokens", "m3-plan-zero", "m3-no-ready-zero", "m3-support-no-sample", "m3-yield-no-sample"}, name) {
		return executeInvestigation(ctx, r, scope, plannerModel, models, input, capacity, nil, nil)
	}
	if name == "m2-parallel-batches-yield" || name == "m2-no-ready-feedback" || name == "m2-wiki-support" {
		return executeInvestigation(ctx, r, scope, plannerModel, models, input, nil, nil, nil)
	}
	p, err := startPlanner(ctx, r, scope, plannerModel, input)
	if err != nil {
		return contract.Ref{}, err
	}
	p.adaptive = true
	if m3 && name != "m3-illegal-optin" {
		p.capacity = capacity
	}
	if strings.HasPrefix(name, "m3-fresh-") || name == "m3-support-pending" {
		_, err = p.step(ctx)
		if err == nil && name != "m3-fresh-reconstruct" {
			p, err = p.capacityHandoff(ctx)
		}
		if err == nil {
			switch name {
			case "m3-fresh-worker-before-step":
				_, err = p.workReady(ctx, models)
			case "m3-fresh-wiki-before-step":
				_, err = p.searchWiki(ctx, models)
			}
		}
		if err == nil {
			old, before := p, r.Snapshot()
			switch name {
			case "m3-support-pending":
				p, err = p.support(ctx, models)
			case "m3-fresh-reconstruct":
				err = p.close(ctx)
				if err == nil {
					p, err = openPlannerWithEvidence(ctx, r, scope, plannerModel, input, old.last, old.workerResults, old.wikiResults)
				}
			default:
				p, err = p.handoff(ctx)
			}
			if err == nil {
				if !old.stopped || p.last == nil || *p.last != *old.last || p.model != old.model || p.capacity == nil || *p.capacity != *old.capacity || !slices.Equal(p.workerResults, old.workerResults) || !slices.Equal(p.wikiResults, old.wikiResults) || !slices.Equal(p.pendingFeedback, old.pendingFeedback) || p.adaptiveNote != old.adaptiveNote {
					t.Error("M3 fresh continuation lost exact state, policy, pending feedback or independent Model")
				}
				if name != "m3-support-pending" && len(r.Snapshot().Attempts) != len(before.Attempts) {
					t.Error("M3 handoff created a copying Step or reset accounting")
				}
				if name == "m3-support-pending" && p.history.ref == old.history.ref {
					t.Error("M3 supporting handoff lost new context")
				}
				if p.capacity == old.capacity {
					t.Error("M3 fresh capacity policy aliases prior caller")
				}
			}
		}
	}
	if name == "m2-session-not-progress" || name == "m2-wiki-handoff" {
		_, err = p.step(ctx)
		if err == nil {
			if name == "m2-wiki-handoff" {
				_, err = p.searchWiki(ctx, models)
			} else {
				_, err = p.workReady(ctx, models)
				if err == nil {
					_, err = p.step(ctx)
				}
			}
		}
		if err == nil {
			old, before := p, r.Snapshot()
			p, err = p.handoff(ctx)
			if err == nil && (!old.stopped || !slices.Equal(p.workerResults, old.workerResults) || !slices.Equal(p.wikiResults, old.wikiResults) || p.last == nil || *p.last != *old.last || len(r.Snapshot().Attempts) != len(before.Attempts)) {
				t.Error("fresh session lost accepted state/deliveries or reset attempt accounting")
			}
		}
	}
	if err == nil {
		var ref contract.Ref
		ref, err = p.adapt(ctx, models)
		if err == nil {
			return ref, nil
		}
	}
	if p == nil {
		return contract.Ref{}, err
	}
	last := p.last
	ordinal := 1
	if name == "m3-checkpoint-missing" || name == "m3-illegal-optin" || name == "m3-policy-initial-echo" {
		ordinal = 0
	}
	if strings.HasPrefix(name, "m3-retained-feedback-") {
		ordinal = 2
	}
	switch name {
	case "m2-ledger-missing", "m2-action-mismatch", "m2-progress-without-batch", "m2-round-initial", "m2-wiki-previous-terms", "m2-wiki-basis-owner":
		ordinal = 0
	case "m2-reframe-required", "m2-reframe-same-terms", "m2-wiki-partial-runtime":
		ordinal = 2
	case "m2-reframe-wiki-reset":
		ordinal = 3
	}
	var committed []engine.AttemptState
	for _, attempt := range r.Snapshot().Attempts {
		if attempt.Output != nil && attempt.Output.SchemaID == PlannerSchema {
			committed = append(committed, attempt)
		}
	}
	slices.SortFunc(committed, func(a, b engine.AttemptState) int {
		if a.LastSeq < b.LastSeq {
			return -1
		}
		if a.LastSeq > b.LastSeq {
			return 1
		}
		return 0
	})
	if ordinal == 0 {
		if last != nil {
			t.Error("invalid first Planner publication became accepted state")
		}
	} else if len(committed) < ordinal || last == nil || *last != *committed[ordinal-1].Output {
		t.Error("failed adaptive call replaced its exact last accepted Planner Ref")
	}
	workers, wikis := slices.Clone(p.workerResults), slices.Clone(p.wikiResults)
	if !p.stopped {
		t.Error("failed adaptive caller was not stopped")
	}
	if strings.HasPrefix(name, "m2-branch-") && (len(workers) != 0 || last == nil) {
		t.Error("partial batch became a Planner checkpoint/delivery")
	}
	if last != nil {
		var state publication[PlannerState]
		if e := protocol.ReadJSON(last.Path, &state); e != nil {
			t.Error(e)
		} else if strings.HasPrefix(name, "m2-branch-") && (state.Data.Previous != nil || state.Data.Ledger.Round != 0) {
			t.Error("batch failure replaced last accepted Planner state")
		}
	}
	beforeRejectedRetries := r.Snapshot()
	if _, e := p.workReady(ctx, models); e == nil {
		t.Error("failed caller allowed another batch")
	}
	if _, e := p.step(ctx); e == nil {
		t.Error("failed caller allowed another Planner Step")
	}
	if _, e := p.searchWiki(ctx, models); e == nil {
		t.Error("failed caller allowed another wiki search")
	}
	if _, e := p.handoff(ctx); e == nil {
		t.Error("failed caller allowed fresh handoff")
	}
	if _, e := p.support(ctx, models); e == nil {
		t.Error("failed caller allowed supporting work")
	}
	if m3 && (len(r.Snapshot().Attempts) != len(beforeRejectedRetries.Attempts) || len(r.Snapshot().Sessions) != len(beforeRejectedRetries.Sessions)) {
		t.Error("stopped M3 retries changed session/attempt accounting")
	}
	if p.last != last || !slices.Equal(workers, p.workerResults) || !slices.Equal(wikis, p.wikiResults) {
		t.Error("rejected retries changed last/deliveries")
	}
	return contract.Ref{}, err
}

func m2AssertWikiOwners(t *testing.T, inputs []contract.Ref) {
	t.Helper()
	found := false
	for _, ref := range inputs {
		if ref.SchemaID != WikiSchema {
			continue
		}
		var p publication[WikiSearch]
		if err := protocol.ReadJSON(ref.Path, &p); err != nil {
			t.Fatal(err)
		}
		if p.Data.Task == nil {
			continue
		}
		found = true
		for _, owner := range append([]contract.Ref{p.Data.Task.Proposal, p.Data.Task.Context}, p.Data.Task.Inputs...) {
			if !slices.Contains(inputs, owner) {
				t.Fatal("new wiki lost original exact evidence owner")
			}
		}
	}
	if !found {
		t.Fatal("new investigation wiki absent from consumer Inputs")
	}
}

func m2AssertWorkerInputs(t *testing.T, r *engine.Run, req contract.Request, request workerRequest, original ContextResult, name string) {
	t.Helper()
	requirements := workerRequirements
	if strings.HasPrefix(name, "m4-support-") || name == "m4-wiki-timeout-partial-resume" || (strings.HasPrefix(name, "m4-reframe-") || name == "m5-supplement-reframe-inspection-resume") && strings.HasPrefix(request.Task.ID, "inspection") {
		requirements += "\nRecovery inspection only: inspect authorized read-only status/evidence for the uncertain remote work in the exact proposal recovery metadata. Do not create, resubmit or restart that work. Record job identity, actual status, limitations and evidence-backed safety assessment."
	}
	if request.Requirements != requirements || (!strings.HasPrefix(name, "m3-") && request.Context != original.Context) || !slices.Contains(req.Inputs, request.Proposal) || !slices.Contains(req.Inputs, request.Context) {
		t.Fatal("M2 worker changed original context or lost explicit proposal/requirements")
	}
	var state publication[PlannerState]
	if err := protocol.ReadJSON(request.Proposal.Path, &state); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(state.Data.WorkerTasks, func(task WorkerTask) bool { return reflect.DeepEqual(task, request.Task) }) {
		t.Fatal("worker differs from declared task")
	}
	if len(state.Data.WikiResults) > 0 {
		m2AssertWikiOwners(t, req.Inputs)
	}
	for _, dep := range request.Dependencies {
		attempt := r.Snapshot().Attempts[dep.AttemptID]
		if !slices.Contains(req.Inputs, dep) || attempt.Output == nil || *attempt.Output != dep || attempt.State != engine.Succeeded || r.Snapshot().Sessions[attempt.HandleID].State != "Closed" {
			t.Fatal("dependent work dispatched before committed, closed dependency")
		}
	}
	if name == "m2-parallel-batches-yield" && request.Task.ID == "w4" {
		if len(request.Dependencies) != 1 || len(state.Data.WorkerResults) != 3 {
			t.Fatal("dependent task leaked into the first batch")
		}
		var dep publication[WorkerResult]
		if err := protocol.ReadJSON(request.Dependencies[0].Path, &dep); err != nil || dep.Data.TaskID != "w1" {
			t.Fatal("dependency ID lost exact result binding", err)
		}
	}
	model := r.Snapshot().Sessions[r.Snapshot().Attempts[req.Identity.AttemptID].HandleID].Role.Model
	if model != (runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: "high"}) {
		t.Fatal("M2 evidence-only worker changed model binding")
	}
}

func m2StoreSchema(t *testing.T, store *contract.Store, base validationInputs, name string) {
	t.Helper()
	if strings.HasPrefix(name, "m5-store-") {
		claim := PureClaim{ParentState: base.intakeRef, Context: base.intakeRef, Candidate: ClaimCandidate{ID: "candidate", Statement: "Anonymous candidate", Premises: []string{}, AllowedEvidence: []Evidence{{Ref: &base.intakeRef, FileID: "issue"}}}}
		schema, field := ClaimSchema, "ledger"
		var raw map[string]any
		if err := json.Unmarshal(testJSON(claim), &raw); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(name, "m5-store-verifier-") {
			raw = nil
			schema = VerificationSchema
			v := VerificationResult{Claim: base.intakeRef, Role: "pro", AllowedEvidence: claim.Candidate.AllowedEvidence, Assessment: VerificationAssessment{Support: "incomplete", Reason: "Runtime missing", Basis: []Evidence{}, RuntimeBasis: []Evidence{}, Measurement: "unavailable", Window: "unavailable", Filter: "unavailable", Environment: "unavailable", Release: "unavailable", Counterexamples: []VerificationIssue{}, Gaps: []string{}}}
			if err := json.Unmarshal(testJSON(v), &raw); err != nil {
				t.Fatal(err)
			}
		}
		// Establish the valid schema shape before injecting one agent-output fault.
		if _, err := storeFixture(t, store, schema, raw, nil, false); err != nil {
			t.Fatal("M5 schema prerequisite", err)
		}
		switch name {
		case "m5-store-claim-ledger":
			raw["ledger"] = map[string]any{}
		case "m5-store-claim-verdict":
			field = "verdict"
			raw[field] = "confirmed"
		case "m5-store-claim-missing-parent":
			field = "parent_state"
			delete(raw, field)
		case "m5-store-verifier-model":
			field = "model"
			raw[field] = "agent-selected"
		case "m5-store-verifier-role":
			field = "role"
			raw[field] = "planner"
		case "m5-store-verifier-missing-assessment":
			field = "assessment"
			delete(raw, field)
		}
		_, err := storeFixture(t, store, schema, raw, nil, false)
		var failure *contract.Error
		if !errors.As(err, &failure) || failure.Code != contract.ContractInvalid || failure.Phase != "schema" || !strings.Contains(err.Error(), field) {
			t.Fatalf("M5 schema rejection for %s: %v", field, err)
		}
		return
	}
	v := PlannerState{Context: base.intakeRef, Hypotheses: []PlannerHypothesis{}, Pending: []PlannerQuestion{}, Gaps: []string{}, Rationale: "Anonymous schema boundary", Ledger: &InvestigationLedger{Action: "yield", Reason: "Accepted investigation only", ConsumedBatch: []contract.Ref{}, Changes: []HypothesisChange{}}}
	var data any = m2PlannerData(t, v)
	if strings.HasPrefix(name, "m3-store-") {
		if name == "m3-store-absent" {
			ref, err := storeFixture(t, store, PlannerSchema, data, nil, false)
			if err != nil {
				t.Fatal("nil policy schema regression", err)
			}
			if storedPublication[PlannerState](t, store, ref).Data.Checkpoint != nil {
				t.Fatal("nil policy invented checkpoint")
			}
			return
		}
		v.Checkpoint = &PlannerCheckpoint{Policy: PlannerCapacityPolicy{HandoffPercent: 80}, ControllerFeedback: []PlannerFeedback{}}
		v.WorkerTasks, v.WorkerResults, v.WikiResults = []WorkerTask{}, []contract.Ref{}, []contract.Ref{}
		ref, err := storeFixture(t, store, PlannerSchema, m2PlannerData(t, v), nil, false)
		if err != nil {
			t.Fatal("invalid M3 schema prerequisite", err)
		}
		storedPublication[PlannerState](t, store, ref, v)
		raw := m2PlannerData(t, v)
		c := raw["checkpoint"].(map[string]any)
		field := strings.TrimPrefix(name, "m3-store-missing-")
		if strings.HasPrefix(name, "m3-store-missing-") {
			delete(c, field)
		} else {
			switch name {
			case "m3-store-negative-cycle":
				c["dispatch_cycle"], field = -1, "dispatch_cycle"
			case "m3-store-fractional-cycle":
				c["dispatch_cycle"], field = 1.5, "dispatch_cycle"
			case "m3-store-negative-checkpoint":
				c["checkpoint_cycle"], field = -1, "checkpoint_cycle"
			case "m3-store-full-type":
				c["full_checkpoint"], field = "true", "full_checkpoint"
			case "m3-store-policy-zero":
				c["policy"].(map[string]any)["handoff_percent"], field = 0, "handoff_percent"
			case "m3-store-policy-above":
				c["policy"].(map[string]any)["handoff_percent"], field = 101, "handoff_percent"
			case "m3-store-extra-control":
				c["model"], field = "agent-selected", "model"
			case "m3-store-policy-extra":
				c["policy"].(map[string]any)["model"], field = "agent-selected", "model"
			case "m3-store-feedback-null":
				c["controller_feedback"], field = nil, "controller_feedback"
			case "m3-store-null":
				raw["checkpoint"], field = nil, "checkpoint"
			default:
				t.Fatal("unmapped M3 schema case")
			}
		}
		_, err = storeFixture(t, store, PlannerSchema, raw, nil, false)
		var failure *contract.Error
		if !errors.As(err, &failure) || failure.Code != contract.ContractInvalid || failure.Phase != "schema" || !strings.Contains(err.Error(), field) {
			t.Fatalf("expected M3 %s schema rejection: %v", field, err)
		}
		return
	}
	if _, err := storeFixture(t, store, PlannerSchema, data, nil, false); err != nil {
		t.Fatal("invalid M2 schema prerequisite", err)
	}
	schema, field := PlannerSchema, "action"
	switch name {
	case "m2-store-action":
		v.Ledger.Action = "final-report"
		data = m2PlannerData(t, v)
	case "m2-store-counter":
		v.Ledger.Round = -1
		data = m2PlannerData(t, v)
		field = "round"
	case "m2-store-extra-control":
		raw := m2PlannerData(t, v)
		raw["ledger"].(map[string]any)["model"] = "agent-selected"
		data, field = raw, "model"
	case "m2-store-wiki-binding":
		wiki, files := wikiFixture("complete", base.intakeRef)
		wiki.Task = &WikiTaskBinding{Proposal: base.intakeRef, Context: base.intakeRef, TaskID: "", Inputs: []contract.Ref{base.intakeRef}}
		_, err := storeFixture(t, store, WikiSchema, wiki, files, false)
		var failure *contract.Error
		if !errors.As(err, &failure) || failure.Code != contract.ContractInvalid || failure.Phase != "schema" || !strings.Contains(err.Error(), "task_id") {
			t.Fatalf("expected wiki binding schema rejection: %v", err)
		}
		return
	}
	_, err := storeFixture(t, store, schema, data, nil, false)
	var failure *contract.Error
	if !errors.As(err, &failure) || failure.Code != contract.ContractInvalid || failure.Phase != "schema" || !strings.Contains(err.Error(), field) {
		t.Fatalf("expected %s schema rejection: %v", field, err)
	}
}

func m2AssertOutcome(t *testing.T, tc triageCase, report engine.Report, ref contract.Ref, original ContextResult, expected PlannerState, barrier m2Barrier, prompts, hellos int) {
	t.Helper()
	if strings.HasPrefix(tc.name, "m5-") {
		if tc.failure {
			if report.Failure == nil || ref != (contract.Ref{}) || len(report.Result.Outputs) != 0 || report.Final != nil {
				t.Fatal("M5 failure became accepted output")
			}
			want := ""
			switch {
			case strings.HasPrefix(tc.name, "m5-supplement-claim-owner-"):
				want = "proposing Planner role/model"
			case strings.HasPrefix(tc.name, "m5-supplement-verifier-owner-"):
				want = "exact fresh role/model/version owner"
			case tc.name == "m5-supplement-claim-parent-binding":
				want = "exact parent continuation"
			case strings.HasPrefix(tc.name, "m5-supplement-exhausted-"):
				want = "ProviderFailed"
				if !barrier.proved || !barrier.faulted || len(barrier.order) != 2 {
					t.Fatal("fatal was not injected after sibling retry exhaustion")
				}
			case strings.HasPrefix(tc.name, "m5-policy-"):
				want = "requires an explicit"
			case tc.name == "m5-claim-projection", tc.name == "m5-claim-context":
				want = "candidate projection"
			case tc.name == "m5-claim-parent":
				want = "claim"
			case tc.name == "m5-claim-ledger", tc.name == "m5-verifier-schema":
				want = "ContractInvalid"
			case tc.name == "m5-verifier-role", tc.name == "m5-verifier-claim", tc.name == "m5-verifier-evidence", tc.name == "m5-new-version-cross-reject":
				want = "version mismatch"
			case tc.name == "m5-verifier-unauthorized-basis":
				want = "exact allowed evidence"
			case tc.name == "m5-feedback-missing", tc.name == "m5-feedback-wrong-claim", tc.name == "m5-feedback-wrong-action":
				want = "Planner must assess"
			case tc.name == "m5-feedback-model-echo", tc.name == "m5-feedback-history-prefix":
				want = "history prefix changed"
			case tc.name == "m5-feedback-repeat-completed":
				want = "only complete unavailable roles"
			case tc.name == "m5-claim-retry-exhausted":
				want = "RetryExhausted"
			case tc.name == "m5-verifier-attempt-limit", tc.name == "m5-verifier-live-limit":
				want = "LimitExceeded"
			case tc.name == "m5-partial-provider-fatal":
				want = "ProviderFailed"
			case tc.name == "m5-partial-cleanup-fatal":
				want = "CleanupFailed"
			case tc.name == "m5-partial-storage-fatal":
				want = "StorageFailed"
			case tc.name == "m5-partial-journal-fatal":
				want = "JournalFailed"
			case tc.name == "m5-partial-user-cancel":
				want = "Cancel"
			}
			if want == "" || !strings.Contains(fmt.Sprint(report.Failure), want) {
				t.Fatalf("M5 wrong failure, want %s: %v", want, report.Failure)
			}
			if tc.name == "m5-verifier-attempt-limit" || tc.name == "m5-verifier-live-limit" {
				// Limits can lock the run before already allocated siblings reach
				// the external provider; prompts are not attempt/session accounting.
				if prompts < tc.stages || prompts > 7 || prompts > len(report.Snapshot.Attempts) || hellos > len(report.Snapshot.Sessions) || len(report.Cleanup) != len(report.Snapshot.Sessions) {
					t.Fatal("M5 pre-dispatch limit lost allocated accounting or cleanup")
				}
				if tc.name == "m5-verifier-attempt-limit" && len(report.Snapshot.Attempts) != 7 {
					t.Fatal("attempt cap did not retain all allocated attempts")
				}
				if tc.name == "m5-verifier-live-limit" && len(report.Snapshot.Sessions) != 6 {
					t.Fatal("live cap ignored the persistent Planner slot")
				}
				claims := 0
				for _, attempt := range report.Snapshot.Attempts {
					if attempt.Output != nil && attempt.Output.SchemaID == ClaimSchema {
						claims++
					}
					if attempt.Output != nil && attempt.Output.SchemaID == PlannerSchema {
						var state publication[PlannerState]
						if err := protocol.ReadJSON(attempt.Output.Path, &state); err != nil {
							t.Fatal(err)
						}
						if len(state.Data.Verification.Deliveries) != 0 || state.Data.Ledger.Round != 0 {
							t.Fatal("limit failure became verification-unavailable or a completed round")
						}
					}
				}
				if claims != 1 {
					t.Fatal("limit failure discarded committed claim")
				}
				for _, session := range report.Snapshot.Sessions {
					if session.State != "Closed" {
						t.Fatal("limit failure left an owned session open")
					}
				}
				return
			}
			if prompts != tc.stages || len(report.Snapshot.Attempts) != prompts || len(report.Snapshot.Sessions) != hellos || len(report.Cleanup) != hellos {
				t.Fatalf("M5 failure accounting: prompts=%d attempts=%d sessions=%d hellos=%d cleanup=%d", prompts, len(report.Snapshot.Attempts), len(report.Snapshot.Sessions), hellos, len(report.Cleanup))
			}
			for _, attempt := range report.Snapshot.Attempts {
				if attempt.Output == nil || attempt.Output.SchemaID != PlannerSchema {
					continue
				}
				var state publication[PlannerState]
				if err := protocol.ReadJSON(attempt.Output.Path, &state); err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(tc.name, "m5-partial-") && (len(state.Data.Verification.Claims) != 0 || len(state.Data.Verification.Deliveries) != 0 || state.Data.Ledger.Round != 0 || state.Data.Recovery.DispatchCycle != 0) {
					t.Fatal("partial verification became a complete checkpoint/cycle")
				}
			}
			if strings.HasPrefix(tc.name, "m5-partial-") && (!barrier.proved || !slices.Equal(barrier.order, []string{"pro", "con", "cross"})) {
				t.Fatal("M5 partial fault did not inspect all three branches")
			}
			return
		}
		if strings.HasPrefix(tc.name, "m5-supplement-reframe-") {
			if report.Failure != nil || report.Final != nil || prompts != tc.stages || len(report.Snapshot.Attempts) != prompts || len(report.Snapshot.Sessions) != hellos || len(report.Cleanup) != hellos {
				t.Fatalf("M5 reframe accounting/failure: prompts=%d attempts=%d failure=%v", prompts, len(report.Snapshot.Attempts), report.Failure)
			}
			var final publication[PlannerState]
			if err := protocol.ReadJSON(ref.Path, &final); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(final.Data, expected) {
				got, want := reflect.ValueOf(final.Data), reflect.ValueOf(expected)
				for i := 0; i < got.NumField(); i++ {
					if !reflect.DeepEqual(got.Field(i).Interface(), want.Field(i).Interface()) {
						t.Logf("M5 fixture field %s: got=%#v want=%#v", got.Type().Field(i).Name, got.Field(i).Interface(), want.Field(i).Interface())
					}
				}
			}
			if !reflect.DeepEqual(final.Data, expected) || len(final.Data.Verification.Claims) != 2 || len(final.Data.Verification.Deliveries) != 2 || len(final.Data.WikiResults) != 1 || len(final.Data.Ledger.ConsumedBatch) != 0 {
				t.Fatal("reframe lost exact feedback, wiki, or Planner declaration")
			}
			actions, rounds, boundaries := []string{"verify", "verify", "reframe", "yield"}, []int{0, 1, 2, 2}, []int{0, 0, 0, 2}
			if tc.name == "m5-supplement-reframe-inspection-resume" {
				actions, rounds, boundaries = []string{"verify", "verify", "reframe", "workers", "reframe", "yield"}, []int{0, 1, 2, 2, 3, 3}, []int{0, 0, 0, 0, 0, 3}
				if len(final.Data.WorkerResults) != 1 || len(final.Data.Recovery.Deliveries) != 3 {
					t.Fatal("inspection/resume was not delivered")
				}
			}
			cursor := &ref
			for i := len(actions) - 1; i >= 0; i-- {
				if cursor == nil {
					t.Fatal("missing committed Planner continuation")
				}
				var state publication[PlannerState]
				if err := protocol.ReadJSON(cursor.Path, &state); err != nil {
					t.Fatal(err)
				}
				l := state.Data.Ledger
				if l.Action != actions[i] || l.Round != rounds[i] || l.NoProgress != rounds[i] || l.ReframeRound != boundaries[i] || l.ReframeStreak != boundaries[i] || state.Data.Recovery.DispatchCycle != i {
					t.Fatalf("M5 reframe transition %d: ledger=%+v cycle=%d", i, l, state.Data.Recovery.DispatchCycle)
				}
				cursor = state.Data.Previous
			}
			if cursor != nil {
				t.Fatal("unexpected extra Planner round")
			}
			retries, verifierAttempts := 0, 0
			seen := map[string]bool{}
			for _, retry := range report.Snapshot.Retries {
				retries += retry.RetryCount
			}
			for _, attempt := range report.Snapshot.Attempts {
				if strings.HasPrefix(attempt.Key, "verify-") {
					if seen[attempt.HandleID] || report.Snapshot.Sessions[attempt.HandleID].State != "Closed" {
						t.Fatal("verifier was not fresh/closed")
					}
					seen[attempt.HandleID] = true
					verifierAttempts++
				}
			}
			if retries != 1 || verifierAttempts != 7 {
				t.Fatal("fresh/role retry was lost or counted as another batch")
			}
			return
		}
		if report.Failure != nil {
			t.Fatalf("M5 investigation failed: %v", report.Failure)
		}
		if prompts != tc.stages || len(report.Snapshot.Attempts) != prompts || len(report.Snapshot.Sessions) != hellos || len(report.Cleanup) != hellos || report.Final != nil {
			t.Fatal("M5 accounting/final output mismatch")
		}
		var state publication[PlannerState]
		if err := protocol.ReadJSON(ref.Path, &state); err != nil {
			t.Fatal(err)
		}
		batches, claims, noProgress, retries := 1, 1, 1, 0
		if tc.name == "m5-same-version-missing-only" {
			batches, noProgress = 2, 2
		}
		if tc.name == "m5-new-claim-all-fresh" || tc.name == "m5-new-evidence-all-fresh" {
			batches, claims, noProgress = 2, 2, 2
		}
		if tc.name == "m5-feedback-worker-new-claim" {
			batches, claims, noProgress = 2, 2, 1
		}
		if tc.name == "m5-agent-changes-reset" {
			noProgress = 0
		}
		if slices.Contains([]string{"m5-verifier-timeout-retry", "m5-verifier-compaction-retry", "m5-verifier-unavailable", "m5-same-version-missing-only", "m5-claim-timeout-retry", "m5-planner-feedback-timeout"}, tc.name) {
			retries = 1
		}
		rounds := batches
		if tc.name == "m5-feedback-worker-new-claim" {
			rounds++
		}
		if !reflect.DeepEqual(state.Data, expected) || state.Data.Ledger.Round != rounds || state.Data.Ledger.NoProgress != noProgress || state.Data.Recovery.DispatchCycle != rounds || state.Data.Checkpoint != nil || len(state.Data.Verification.Claims) != claims || len(state.Data.Verification.Deliveries) != batches || len(state.Data.Ledger.ConsumedBatch) != 0 {
			t.Fatal("M5 feedback/counters or Agent declaration changed")
		}
		wantSupport := "incomplete"
		if tc.name == "m5-agent-inference" {
			wantSupport = "inference"
		}
		if tc.name == "m5-agent-runtime-declared" {
			wantSupport = "runtime-supported under the Agent's declared assumptions"
		}
		if state.Data.VerificationReview.Assessment.Support != wantSupport {
			t.Fatal("Controller promoted the roster rather than retaining Agent support declaration")
		}
		for _, claimRef := range state.Data.Verification.Claims {
			var claim publication[map[string]json.RawMessage]
			if err := protocol.ReadJSON(claimRef.Path, &claim); err != nil {
				t.Fatal(err)
			}
			if len(claim.Files) != 0 || len(claim.Data) != 3 || claim.Data["parent_state"] == nil || claim.Data["context"] == nil || claim.Data["candidate"] == nil {
				t.Fatal("pure claim leaked ledger/verdict/narrative")
			}
		}
		if tc.name == "m5-new-evidence-all-fresh" {
			var before, after publication[PureClaim]
			if err := protocol.ReadJSON(state.Data.Verification.Claims[0].Path, &before); err != nil {
				t.Fatal(err)
			}
			if err := protocol.ReadJSON(state.Data.Verification.Claims[1].Path, &after); err != nil {
				t.Fatal(err)
			}
			if before.Data.Candidate.ID != after.Data.Candidate.ID || before.Data.Candidate.Statement != after.Data.Candidate.Statement || !slices.Equal(before.Data.Candidate.Premises, after.Data.Candidate.Premises) || len(after.Data.Candidate.AllowedEvidence) != len(before.Data.Candidate.AllowedEvidence)+1 || !reflect.DeepEqual(before.Data.Candidate.AllowedEvidence, after.Data.Candidate.AllowedEvidence[:len(before.Data.Candidate.AllowedEvidence)]) {
				t.Fatal("evidence-only version fixture also changed the candidate claim")
			}
		}
		for batch, delivery := range state.Data.Verification.Deliveries {
			if delivery.Claim != state.Data.Verification.Claims[min(batch, claims-1)] {
				t.Fatal("verification rebound to a different claim version")
			}
			for i, role := range []string{"pro", "con", "cross"} {
				got := delivery.Roles[i]
				unavailable := role == "con" && batch == 0 && (tc.name == "m5-verifier-unavailable" || tc.name == "m5-same-version-missing-only")
				failures := 0
				if unavailable {
					failures = 2
				} else if role == "con" && (tc.name == "m5-verifier-timeout-retry" || tc.name == "m5-verifier-compaction-retry") {
					failures = 1
				}
				if got.Role != role || got.Unavailable != unavailable || (got.Result == nil) != unavailable || len(got.Failures) != failures {
					t.Fatalf("invalid ordered delivery: %+v", got)
				}
				for _, failed := range got.Failures {
					attempt := report.Snapshot.Attempts[failed.AttemptID]
					owner := report.Snapshot.Sessions[attempt.HandleID]
					if failed.Stage != "verify-"+role || failed.RunID != attempt.Identity.RunID || failed.AttemptID != attempt.Identity.AttemptID || attempt.Failure == nil || failed.Code != attempt.Failure.Code || failed.Cleanup == nil || !failed.Cleanup.ConfirmsLocalClose(owner.Identity.SessionID) || !sameIdentity(failed.Identity, owner.Identity) || owner.Role.Name != "triage-verify-"+role || attempt.Output != nil || failed.Diagnostic == "" {
						t.Fatal("verifier retry lost actual typed failure/owner/strict cleanup accounting")
					}
				}
				if unavailable && got.Exhausted != string(engine.RetryExhausted) {
					t.Fatal("unavailable omitted exhausted retry")
				}
				if got.Result == nil {
					continue
				}
				if batch > 0 && role != "con" && tc.name == "m5-same-version-missing-only" && !reflect.DeepEqual(got, state.Data.Verification.Deliveries[0].Roles[i]) {
					t.Fatal("same-version accepted role was rerun or rebound")
				}
				if batch > 0 && claims == 2 && *got.Result == *state.Data.Verification.Deliveries[0].Roles[i].Result {
					t.Fatal("new version reused an old role result")
				}
				attempt := report.Snapshot.Attempts[got.Result.AttemptID]
				owner := report.Snapshot.Sessions[attempt.HandleID]
				thinking := map[string]string{"pro": "high", "con": "medium", "cross": "low"}[role]
				if owner.Role.Name != "triage-verify-"+role || owner.Role.Model != (runtime.ModelSpec{Provider: "fixture", ID: role, Thinking: thinking}) || owner.State != "Closed" {
					t.Fatal("verifier model/role/fresh cleanup mismatch")
				}
				for id, other := range report.Snapshot.Attempts {
					if id != got.Result.AttemptID && other.HandleID == attempt.HandleID {
						t.Fatal("verifier reused a session")
					}
				}
			}
		}
		roleAttempts := map[string]int{}
		for _, attempt := range report.Snapshot.Attempts {
			owner := report.Snapshot.Sessions[attempt.HandleID]
			if strings.HasPrefix(owner.Role.Name, "triage-verify-") {
				roleAttempts[strings.TrimPrefix(owner.Role.Name, "triage-verify-")]++
			}
		}
		for _, role := range []string{"pro", "con", "cross"} {
			want := claims
			if role == "con" && slices.Contains([]string{"m5-verifier-timeout-retry", "m5-verifier-compaction-retry", "m5-verifier-unavailable"}, tc.name) {
				want = 2
			}
			if role == "con" && tc.name == "m5-same-version-missing-only" {
				want = 3
			}
			if roleAttempts[role] != want {
				t.Fatalf("M5 role %s attempts=%d want=%d", role, roleAttempts[role], want)
			}
		}
		actualRetries := 0
		for _, retry := range report.Snapshot.Retries {
			if retry.Active || retry.MaxRetries != 1 {
				t.Fatal("M5 retry exceeded caller policy or remained active")
			}
			actualRetries += retry.RetryCount
		}
		if actualRetries != retries {
			t.Fatalf("M5 retries=%d want=%d", actualRetries, retries)
		}
		if tc.name == "m5-planner-feedback-timeout" {
			if len(state.Data.Recovery.PlannerFailures) != 1 || len(state.Data.Verification.Claims) != 1 || len(state.Data.Verification.Deliveries) != 1 {
				t.Fatal("post-claim Planner retry lost committed claim/delivery or duplicated the round")
			}
		}
		if tc.name == "m5-reverse-completion" && (!barrier.proved || !slices.Equal(barrier.order, []string{"cross", "con", "pro"})) {
			t.Fatal("M5 did not prove parallel three-angle execution with Planner slot")
		}
		for _, cleanup := range report.Cleanup {
			if !cleanup.ConfirmsLocalClose(cleanup.Identity.SessionID) {
				t.Fatal("M5 unconfirmed cleanup")
			}
		}
		return
	}
	if strings.HasPrefix(tc.name, "m4-") {
		if report.Final != nil || len(report.Snapshot.Attempts) != prompts || len(report.Snapshot.Sessions) != hellos || len(report.Cleanup) != hellos {
			t.Fatal("M4 lost actual attempt/session accounting or invented a final report")
		}
		persistenceFault := tc.name == "m4-planner-storage-fatal" || tc.name == "m4-planner-journal-fatal" || tc.name == "m4-mixed-storage-fatal" || tc.name == "m4-mixed-journal-fatal" || tc.name == "m4-mixed-committed-close-storage-fatal" || tc.name == "m4-mixed-committed-close-journal-fatal"
		plannerSessions, failedAttempts, retries := 0, 0, 0
		for _, s := range report.Snapshot.Sessions {
			if s.State != "Closed" && !persistenceFault || s.Identity.SessionID == "" {
				t.Fatal("M4 returned before owned session cleanup")
			}
			if s.Role.Name == "triage-planner" {
				plannerSessions++
				if s.Role.Model != (runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}) {
					t.Fatal("M4 changed Planner model")
				}
			}
		}
		for _, c := range report.Cleanup {
			owner, ok := report.Snapshot.Sessions[c.Identity.HandleID]
			if !ok || !sameIdentity(c.Identity, owner.Identity) || !c.WaitCompleted || !c.ProcessExited || !c.ConfirmsLocalClose(owner.Identity.SessionID) && tc.name != "m4-mixed-cleanup-fatal" && tc.name != "m4-mixed-wait-fatal" && tc.name != "m4-mixed-committed-close-cleanup-fatal" {
				t.Fatalf("M4 cleanup differs from independently recorded owner: %+v", c)
			}
		}
		for _, a := range report.Snapshot.Attempts {
			if a.Failure != nil {
				failedAttempts++
				if a.Output != nil {
					t.Fatal("M4 promoted a failed candidate to committed output")
				}
			}
		}
		for _, retry := range report.Snapshot.Retries {
			if retry.Active && !persistenceFault || retry.MaxRetries != 1 {
				t.Fatal("M4 retry activation escaped finite policy or remained active")
			}
			retries += retry.RetryCount
		}
		wantPlanners, wantRetries := 1, 0
		if tc.name == "m4-planner-first-timeout" || tc.name == "m4-planner-retry-exhausted" || tc.name == "m4-planner-first-compaction" || strings.HasPrefix(tc.name, "m4-meta-") || strings.HasPrefix(tc.name, "m4-planner-later-") || tc.name == "m4-planner-parent-deadline" || tc.name == "m4-planner-run-limit" || persistenceFault {
			wantPlanners, wantRetries = 2, 1
		}
		if strings.HasPrefix(tc.name, "m4-mixed-") {
			wantPlanners, wantRetries = 1, 0
		}
		if tc.name == "m4-capacity-fresh-worker-timeout" || strings.HasPrefix(tc.name, "m4-reframe-") {
			wantPlanners = 2
		}
		if strings.HasPrefix(tc.name, "m4-support-") {
			wantPlanners = 3
			if tc.failure {
				wantPlanners = 2
			}
		}
		if plannerSessions != wantPlanners || retries != wantRetries {
			t.Fatalf("M4 Planner sessions/retries=%d/%d, want %d/%d", plannerSessions, retries, wantPlanners, wantRetries)
		}
		if tc.failure {
			if ref != (contract.Ref{}) || report.Failure == nil {
				t.Fatal("M4 failure returned a successful planning state")
			}
			var failure *engine.Failure
			if !errors.As(report.Failure, &failure) {
				t.Fatalf("M4 lost typed root failure: %v", report.Failure)
			}
			deadlineAttempts := map[string]bool{}
			for id, a := range report.Snapshot.Attempts {
				if a.Failure != nil && a.Failure.Code == engine.TimedOut && a.Failure.Origin == engine.OriginAttemptDeadline {
					deadlineAttempts[id] = true
				}
			}
			if (tc.name == "m4-planner-later-user-cancel" || tc.name == "m4-planner-parent-deadline" || tc.name == "m4-planner-run-limit" || persistenceFault && strings.HasPrefix(tc.name, "m4-planner-")) && len(deadlineAttempts) != 1 {
				t.Fatal("locked root outcome discarded the original failed attempt/accounting")
			}
			switch tc.name {
			case "m4-mixed-committed-close-cleanup-fatal", "m4-mixed-committed-close-storage-fatal", "m4-mixed-committed-close-journal-fatal":
				success := report.Snapshot.Attempts[barrier.attempts["w3"]]
				primary := report.Snapshot.Attempts[barrier.attempts["w2"]]
				want := engine.CleanupFailed
				if strings.Contains(tc.name, "storage") {
					want = engine.StorageFailed
				}
				if strings.Contains(tc.name, "journal") {
					want = engine.JournalFailed
				}
				if failure.Code != want || success.State != engine.Succeeded || success.Output == nil || success.Failure != nil || primary.Failure == nil || primary.Failure.Code != engine.CompactionFailed || !barrier.proved {
					t.Fatalf("committed sibling failure lost fatal classification or actual success/primary: %v", report.Failure)
				}
			case "m4-mixed-storage-fatal", "m4-mixed-journal-fatal", "m4-mixed-user-cancel", "m4-mixed-wait-fatal":
				primary := report.Snapshot.Attempts[barrier.attempts["w2"]]
				if primary.Failure == nil || primary.Failure.Code != engine.CompactionFailed || !barrier.proved {
					t.Fatal("mixed fatal lost recoverable primary")
				}
				want := engine.StorageFailed
				if tc.name == "m4-mixed-journal-fatal" {
					want = engine.JournalFailed
				}
				if tc.name == "m4-mixed-user-cancel" {
					want = engine.Cancelled
				}
				if tc.name != "m4-mixed-wait-fatal" && failure.Code != want {
					t.Fatalf("mixed fatal classified as %s, want %s: %v", failure.Code, want, report.Failure)
				}
				if tc.name == "m4-mixed-user-cancel" && failure.Origin != engine.OriginControllerUser {
					t.Fatal("sibling user cancellation changed to recoverable FailFastSibling")
				}
				if tc.name == "m4-mixed-wait-fatal" {
					sibling := report.Snapshot.Attempts[barrier.attempts["w1"]]
					found := false
					for _, c := range report.Cleanup {
						if c.Identity.HandleID == sibling.HandleID {
							found = c.WaitError == "exit status 3" && c.WaitCompleted && c.ProcessExited && !c.ConfirmsLocalClose(c.Identity.SessionID)
						}
					}
					failureHasWait := false
					var walk func(error)
					walk = func(err error) {
						if f, ok := err.(*engine.Failure); ok && f.Cleanup != nil && f.Cleanup.Identity.HandleID == sibling.HandleID && f.Cleanup.WaitError == "exit status 3" {
							failureHasWait = true
						}
						switch e := err.(type) {
						case interface{ Unwrap() []error }:
							for _, child := range e.Unwrap() {
								walk(child)
							}
						case interface{ Unwrap() error }:
							walk(e.Unwrap())
						}
					}
					walk(report.Failure)
					if !found || !failureHasWait {
						t.Fatalf("nonzero sibling exit lost strict-close failure: %v; report=%v chain=%v", report.Failure, found, failureHasWait)
					}
				}
			case "m4-reframe-no-inspection", "m4-reframe-stale-inspection", "m4-reframe-completed-inspection":
				want := "two rounds without reported hypothesis progress require reframe"
				if tc.name == "m4-reframe-completed-inspection" {
					want = "recovery choice requires unresolved delivery, reason and evidence basis"
				}
				if failure.Code != engine.WorkflowFailed || len(deadlineAttempts) != 3 || !strings.Contains(report.Failure.Error(), want) {
					t.Fatalf("old reframe or ordinary workers bypassed mandatory reframe: %v", report.Failure)
				}
			case "m4-allfail-blind-new-id":
				if !strings.Contains(report.Failure.Error(), "unresolved remote work requires explicit safe continuation or inspection") {
					t.Fatalf("new task ID bypassed unknown remote safety: %v", report.Failure)
				}
			case "m4-allfail-caller-bool":
				if failure.Code != engine.ContractInvalid {
					t.Fatalf("caller boolean bypassed safety contract: %v", report.Failure)
				}
			case "m4-delivery-id":
				if !strings.Contains(report.Failure.Error(), "planner recovery differs from supplied delivery metadata") {
					t.Fatalf("invented delivery ID accepted: %v", report.Failure)
				}
			case "m4-delivery-proposal", "m4-delivery-context":
				if !strings.Contains(report.Failure.Error(), "delivery proposal/context mismatch") {
					t.Fatalf("delivery owner mismatch accepted: %v", report.Failure)
				}
			case "m4-delivery-results":
				if !strings.Contains(report.Failure.Error(), "worker delivery differs from accepted batch") {
					t.Fatalf("dropped delivery results accepted: %v", report.Failure)
				}
			case "m4-nil-recovery-timeout":
				if failure.Code != engine.TimedOut || failure.Origin != engine.OriginAttemptDeadline || len(report.Snapshot.Retries) != 0 {
					t.Fatalf("nil policy changed original timeout classification: %v", report.Failure)
				}
			case "m4-planner-later-user-cancel":
				if failure.Code != engine.Cancelled || failure.Origin != engine.OriginControllerUser {
					t.Fatalf("old timeout downgraded later user cancellation: %v", report.Failure)
				}
			case "m4-planner-parent-deadline":
				if failure.Code != engine.TimedOut || failure.Origin != engine.OriginRunDeadline {
					t.Fatalf("old attempt timeout downgraded parent deadline: %v", report.Failure)
				}
			case "m4-planner-run-limit":
				if failure.Code != engine.LimitExceeded || failure.LimitScope != "run" {
					t.Fatalf("recovery swallowed run cap or original timeout: %v", report.Failure)
				}
			case "m4-planner-storage-fatal", "m4-planner-journal-fatal":
				code := engine.StorageFailed
				if tc.name == "m4-planner-journal-fatal" {
					code = engine.JournalFailed
				}
				if failure.Code != code || report.Snapshot.StatePersisted {
					t.Fatalf("recovery swallowed fatal persistence boundary or original cause: %v", report.Failure)
				}
			case "m4-mixed-cleanup-fatal":
				if len(report.CleanupErrors) == 0 || !strings.Contains(report.Failure.Error(), "cleanup") {
					t.Fatalf("M4 swallowed branch cleanup failure: %v", report.Failure)
				}
			case "m4-meta-identity", "m4-meta-attempt", "m4-meta-cleanup":
				if !strings.Contains(report.Failure.Error(), "recovery failure differs from owned failed attempt/cleanup") {
					t.Fatalf("wrong identity rejection: %v", report.Failure)
				}
			case "m4-meta-prefix":
				if !strings.Contains(report.Failure.Error(), "recovery history prefix changed") {
					t.Fatalf("wrong history rejection: %v", report.Failure)
				}
			case "m4-meta-diagnostic":
				if !strings.Contains(report.Failure.Error(), "planner recovery differs from supplied delivery metadata") {
					t.Fatalf("wrong metadata echo rejection: %v", report.Failure)
				}
			case "m4-support-unsafe-no-basis":
				if failure.Code != engine.ContractInvalid {
					t.Fatalf("empty safety basis bypassed schema minItems: %v", report.Failure)
				}
			case "m4-support-unsafe-no-reason":
				if !strings.Contains(report.Failure.Error(), "recovery choice requires unresolved delivery, reason and evidence basis") {
					t.Fatalf("wrong clearance rejection: %v", report.Failure)
				}
			case "m4-support-unsafe-owner":
				if !strings.Contains(report.Failure.Error(), "investigation basis must name an exact input owner/file") {
					t.Fatalf("wrong clearance owner rejection: %v", report.Failure)
				}
			case "m4-mixed-binding-fatal":
				if failure.Code != engine.WorkflowFailed || !strings.Contains(report.Failure.Error(), "worker result acceptance") || !strings.Contains(report.Failure.Error(), "fixture compaction failed") {
					t.Fatalf("M4 skipped sibling acceptance or lost original compaction chain: %v", report.Failure)
				}
			case "m4-planner-retry-exhausted":
				causes := map[string]bool{}
				var visit func(error)
				visit = func(err error) {
					if f, ok := err.(*engine.Failure); ok && f.Code == engine.TimedOut && f.Origin == engine.OriginAttemptDeadline && f.AttemptID != "" {
						causes[f.AttemptID] = true
					}
					switch e := err.(type) {
					case interface{ Unwrap() []error }:
						for _, child := range e.Unwrap() {
							visit(child)
						}
					case interface{ Unwrap() error }:
						visit(e.Unwrap())
					}
				}
				visit(report.Failure)
				if failure.Code != engine.RetryExhausted || failedAttempts != 2 || !reflect.DeepEqual(causes, deadlineAttempts) || len(causes) != 2 {
					t.Fatalf("M4 exhausted retry lost original deadline chain/accounting: causes=%v attempts=%v error=%v", causes, deadlineAttempts, report.Failure)
				}
			case "m4-planner-unknown-provider", "m4-mixed-provider-fatal":
				if failure.Code != engine.ProviderFailed || failure.Origin != engine.OriginProvider || failedAttempts < 1 {
					t.Fatalf("M4 invented recoverable overflow: %v", report.Failure)
				}
			case "m4-planner-user-cancel":
				if failure.Code != engine.Cancelled || failure.Origin != engine.OriginControllerUser {
					t.Fatalf("M4 swallowed user cancellation: %v", report.Failure)
				}
			case "m4-policy-echo", "m4-drop-recovery":
				if !strings.Contains(report.Failure.Error(), "planner recovery differs from supplied delivery metadata") {
					t.Fatalf("wrong recovery echo rejection: %v", report.Failure)
				}
			case "m4-cycle-echo":
				if !strings.Contains(report.Failure.Error(), "recovery dispatch cycle differs from delivered history") {
					t.Fatalf("wrong recovery cycle rejection: %v", report.Failure)
				}
			}
			return
		}
		var accepted publication[PlannerState]
		if err := protocol.ReadJSON(ref.Path, &accepted); err != nil || !reflect.DeepEqual(accepted.Data.Recovery, expected.Recovery) {
			t.Fatal("M4 final state lost exact recovery delivery", err)
		}
		v := accepted.Data
		if strings.HasPrefix(tc.name, "m4-allfail-") {
			if v.Context != original.Context || len(v.WorkerResults) != 0 || v.Recovery == nil || len(v.Recovery.PlannerFailures) != 0 || len(v.Recovery.Deliveries) < 2 || v.Ledger.Round != 2 || failedAttempts != 2 {
				t.Fatal("all-failed rounds fabricated results or changed context/accounting")
			}
			for _, d := range v.Recovery.Deliveries[:2] {
				if d.Kind != "workers" || len(d.Results) != 0 || len(d.Failures) != 1 || d.Failures[0].Code != engine.TimedOut || d.Failures[0].Origin != engine.OriginAttemptDeadline {
					t.Fatal("all-failed round lost typed delivery")
				}
			}
			if tc.name == "m4-allfail-reframe-checkpoint" {
				if len(v.Recovery.Deliveries) != 3 || v.Recovery.DispatchCycle != 3 || v.Recovery.Deliveries[2].Kind != "wiki" || len(v.WikiResults) != 1 || v.Ledger.NoProgress != 2 || v.Ledger.ReframeRound != 2 || v.Ledger.ReframeStreak != 2 || v.Checkpoint == nil || !v.Checkpoint.FullCheckpoint || v.Checkpoint.DispatchCycle != 3 || v.Checkpoint.CheckpointCycle != 3 || len(barrier.stats) != 3 {
					t.Fatal("two explicit no-progress failed rounds did not reframe/checkpoint exactly once")
				}
				var proposal publication[PlannerState]
				if err := protocol.ReadJSON(v.Recovery.Deliveries[2].Proposal.Path, &proposal); err != nil || proposal.Data.Ledger.Action != "reframe" || proposal.Data.Ledger.Reframe == nil || len(proposal.Data.Ledger.Changes) != 0 {
					t.Fatal("reframe did not consume Agent's explicit no-progress declaration", err)
				}
			} else if len(v.Recovery.Deliveries) != 2 || v.Ledger.NoProgress != 0 || len(v.Ledger.Changes) != 1 || v.Hypotheses[0].Assessment != "Unverified; compare runtime evidence" {
				t.Fatal("Controller substituted content comparison for Agent-reported progress")
			}
			return
		}
		if tc.name == "m4-reframe-timeout-inspection-resume" {
			if v.Context != original.Context || v.Recovery == nil || len(v.Recovery.Deliveries) != 5 || v.Recovery.DispatchCycle != 5 || len(v.Recovery.PlannerFailures) != 0 || len(v.WorkerResults) != 1 || len(v.WikiResults) != 1 || failedAttempts != 3 || v.Ledger.Round != 3 || v.Ledger.NoProgress != 3 || v.Ledger.ReframeRound != 3 || v.Ledger.ReframeStreak != 3 || len(v.Ledger.Changes) != 0 || len(v.Ledger.ConsumedBatch) != 0 || v.Ledger.Action != "yield" {
				t.Fatal("reframe timeout/inspection/resume lost actual rounds, boundary or failure accounting")
			}
			if v.Checkpoint == nil || v.Checkpoint.DispatchCycle != 5 || v.Checkpoint.CheckpointCycle != 3 || v.Checkpoint.FullCheckpoint || len(barrier.stats) != 5 {
				t.Fatal("fresh reframe recovery reset checkpoint/dispatch accounting or repeated capacity sampling")
			}
			for _, d := range v.Recovery.Deliveries[:2] {
				if d.Kind != "workers" || len(d.Results) != 0 || len(d.Failures) != 1 || d.Failures[0].Code != engine.TimedOut || d.Failures[0].Origin != engine.OriginAttemptDeadline {
					t.Fatal("reframe did not follow two actual all-failed worker rounds")
				}
			}
			failed, inspected, resumed := v.Recovery.Deliveries[2], v.Recovery.Deliveries[3], v.Recovery.Deliveries[4]
			if inspected.Kind != "workers" || len(inspected.Results) != 1 || inspected.Results[0] != v.WorkerResults[0] || len(inspected.Failures) != 0 || resumed.Kind != "wiki" || len(resumed.Results) != 1 || resumed.Results[0] != v.WikiResults[0] || len(resumed.Failures) != 0 {
				t.Fatal("read-only inspection and safe resume lost their exact delivered results")
			}
			var before, inspection, after publication[PlannerState]
			if err := protocol.ReadJSON(failed.Proposal.Path, &before); err != nil {
				t.Fatal(err)
			}
			if err := protocol.ReadJSON(inspected.Proposal.Path, &inspection); err != nil {
				t.Fatal(err)
			}
			if err := protocol.ReadJSON(resumed.Proposal.Path, &after); err != nil {
				t.Fatal(err)
			}
			if before.Data.Ledger.Action != "reframe" || before.Data.Ledger.Round != 2 || before.Data.Ledger.NoProgress != 2 || before.Data.Ledger.ReframeRound != 0 || before.Data.Ledger.ReframeStreak != 0 || !reflect.DeepEqual(before.Data.WikiTask, after.Data.WikiTask) || !reflect.DeepEqual(before.Data.Ledger.Reframe, after.Data.Ledger.Reframe) || after.Data.Ledger.Action != "reframe" {
				t.Fatal("safe resume replaced the failed reframe task, terms, rationale or original ledger")
			}
			if len(inspection.Data.RecoveryChoices) != 1 || inspection.Data.RecoveryChoices[0].DeliveryID != failed.ID || inspection.Data.RecoveryChoices[0].Action != "inspect" || len(after.Data.RecoveryChoices) != 1 || after.Data.RecoveryChoices[0].DeliveryID != failed.ID || after.Data.RecoveryChoices[0].Action != "resume" || len(after.Data.RecoveryChoices[0].Basis) != 1 || after.Data.RecoveryChoices[0].Basis[0].Ref == nil || *after.Data.RecoveryChoices[0].Basis[0].Ref != v.WorkerResults[0] || after.Data.RecoveryChoices[0].Basis[0].FileID != "worker-raw" {
				t.Fatal("safe resume lacks exact inspection evidence and failed-delivery authorization")
			}
			if report.Snapshot.Attempts[failed.Proposal.AttemptID].HandleID == report.Snapshot.Attempts[inspected.Proposal.AttemptID].HandleID {
				t.Fatal("inspection Planner did not reconstruct the failed reframe across fresh handoff")
			}
			var wiki publication[WikiSearch]
			if err := protocol.ReadJSON(v.WikiResults[0].Path, &wiki); err != nil || wiki.Data.Status != "partial" || wiki.Data.Task == nil || wiki.Data.Task.Proposal != resumed.Proposal || wiki.Data.Task.TaskID != before.Data.WikiTask.ID || wiki.Data.Intake != original.Intake || len(wiki.Data.Gaps) == 0 {
				t.Fatal("resumed reframe lost partial status, actual owner or retained intake", err)
			}
			for _, gap := range wiki.Data.Gaps {
				if !slices.Contains(v.Gaps, gap) {
					t.Fatal("resumed reframe hid incomplete search gaps")
				}
			}
			return
		}
		if tc.name == "m4-wiki-timeout-partial-resume" {
			if v.Context != original.Context || len(v.Recovery.Deliveries) != 3 || v.Recovery.DispatchCycle != 3 || len(v.WikiResults) != 1 || len(v.WorkerResults) != 1 || v.Ledger.Round != 1 || failedAttempts != 1 {
				t.Fatal("wiki safe resume changed context or invented a round/result")
			}
			failed, resumed := v.Recovery.Deliveries[0], v.Recovery.Deliveries[2]
			if failed.Kind != "wiki" || len(failed.Results) != 0 || len(failed.Failures) != 1 || failed.Failures[0].Code != engine.TimedOut || resumed.Kind != "wiki" || len(resumed.Failures) != 0 || len(resumed.Results) != 1 || resumed.Results[0] != v.WikiResults[0] {
				t.Fatal("partial wiki search was confused with execution failure")
			}
			var before, after publication[PlannerState]
			var wiki publication[WikiSearch]
			if err := protocol.ReadJSON(failed.Proposal.Path, &before); err != nil {
				t.Fatal(err)
			}
			if err := protocol.ReadJSON(resumed.Proposal.Path, &after); err != nil || !reflect.DeepEqual(before.Data.WikiTask, after.Data.WikiTask) {
				t.Fatal("wiki resume changed original task/terms/history", err)
			}
			if err := protocol.ReadJSON(v.WikiResults[0].Path, &wiki); err != nil || wiki.Data.Status != "partial" || wiki.Data.Task.Proposal != resumed.Proposal || wiki.Data.Intake != original.Intake || len(wiki.Data.Gaps) == 0 {
				t.Fatal("wiki partial lost its actual owner and gap", err)
			}
			for _, gap := range wiki.Data.Gaps {
				if !slices.Contains(v.Gaps, gap) {
					t.Fatal("Planner dropped partial wiki gap")
				}
			}
			return
		}
		if strings.HasPrefix(tc.name, "m4-support-") {
			if v.Recovery == nil || len(v.Recovery.Deliveries) != 3 || v.Recovery.DispatchCycle != 3 || v.Ledger.Round != 1 || v.Ledger.NoProgress != 1 || failedAttempts != 1 || len(v.WorkerResults) != 1 {
				t.Fatal("support failure/inspection/resume lost real dispatch/round accounting")
			}
			failed, inspected, resumed := v.Recovery.Deliveries[0], v.Recovery.Deliveries[1], v.Recovery.Deliveries[2]
			if failed.Kind != "support" || failed.Support == nil || len(failed.Failures) != 1 || len(failed.Results) != 0 || (failed.Support.Intake != nil) != strings.HasPrefix(tc.name, "m4-support-update-") || failed.Context != original.Context || failed.Support.Proposal != failed.Proposal || failed.Support.Context != original.Context {
				t.Fatal("partial support replaced context or lost old intake/original proposal")
			}
			phase := "wiki-resolution"
			if strings.Contains(tc.name, "context-timeout") {
				phase = "context-resolution"
			}
			if strings.HasPrefix(tc.name, "m4-support-update-") {
				phase = strings.Replace(phase, "resolution", "revision", 1)
			}
			f := failed.Failures[0]
			a := report.Snapshot.Attempts[f.AttemptID]
			owner := report.Snapshot.Sessions[a.HandleID]
			if failed.Support.FailedPhase != phase || f.Stage != phase || f.Code != engine.TimedOut || f.Origin != engine.OriginAttemptDeadline || a.Output != nil || a.Failure == nil || !sameIdentity(f.Identity, owner.Identity) || f.Cleanup == nil || !f.Cleanup.ConfirmsLocalClose(owner.Identity.SessionID) {
				t.Fatal("support phase failure differs from actual timeout/cleanup")
			}
			if (failed.Support.Wiki != nil) != (strings.HasPrefix(phase, "context-")) {
				t.Fatal("support lost or invented an accepted wiki phase")
			}
			if inspected.Kind != "workers" || len(inspected.Results) != 1 || inspected.Results[0] != v.WorkerResults[0] || len(inspected.Failures) != 0 || resumed.Kind != "support" || len(resumed.Results) != 1 || resumed.Results[0] != v.Context || len(resumed.Failures) != 0 || resumed.Support == nil || resumed.Support.Proposal != failed.Proposal || resumed.Support.Authorization == nil || *resumed.Support.Authorization != resumed.Proposal {
				t.Fatal("safe support resume did not retain original task plus accepted authorization")
			}
			if strings.HasPrefix(phase, "context-") && (resumed.Support.Wiki == nil || *resumed.Support.Wiki != *failed.Support.Wiki) {
				t.Fatal("resume repeated the already accepted wiki phase")
			}
			var updated publication[Context]
			wantIntake := original.Intake
			if strings.HasPrefix(tc.name, "m4-support-update-") {
				if failed.Support.Intake == nil || resumed.Support.Intake == nil || *failed.Support.Intake != *resumed.Support.Intake || *failed.Support.Intake == original.Intake {
					t.Fatal("update resume lost accepted intake or reacquired it")
				}
				wantIntake = *failed.Support.Intake
			}
			if err := protocol.ReadJSON(v.Context.Path, &updated); err != nil || v.Context == original.Context || updated.Data.Intake != wantIntake || updated.Data.Previous == nil || *updated.Data.Previous != original.Context {
				t.Fatal("completed continuation lost original context/intake binding", err)
			}
			for _, d := range []RecoveryDelivery{inspected, resumed} {
				var proposal publication[PlannerState]
				if err := protocol.ReadJSON(d.Proposal.Path, &proposal); err != nil || len(proposal.Data.RecoveryChoices) != 1 {
					t.Fatal("inspection/resume did not consume a committed choice", err)
				}
				choice := proposal.Data.RecoveryChoices[0]
				want := "inspect"
				if d.Kind == "support" {
					want = "resume"
				}
				if choice.DeliveryID != failed.ID || choice.Action != want || choice.Reason == "" || len(choice.Basis) == 0 {
					t.Fatal("continuation lost evidence-backed safety choice")
				}
			}
			return
		}
		if v.Context != original.Context || v.Recovery == nil || v.Recovery.DispatchCycle != len(v.Recovery.Deliveries) {
			t.Fatal("M4 changed context binding or dispatch accounting")
		}
		if tc.name == "m4-planner-first-timeout" || tc.name == "m4-planner-first-compaction" {
			if len(v.Recovery.PlannerFailures) != 1 || len(v.Recovery.Deliveries) != 0 || v.Ledger.Round != 0 || failedAttempts != 1 {
				t.Fatal("Planner retry invented dispatch/round or lost typed failure")
			}
		} else {
			if len(v.Recovery.Deliveries) != 1 || v.Ledger.Round != 1 || v.Ledger.NoProgress != 1 {
				t.Fatal("M4 batch was not counted exactly once with Agent-reported no progress")
			}
			d := v.Recovery.Deliveries[0]
			if d.Kind != "workers" || d.Context != original.Context || v.Previous == nil || d.Proposal != *v.Previous || !slices.Equal(d.Results, v.Ledger.ConsumedBatch) {
				t.Fatal("M4 delivery lost exact proposal/results binding")
			}
			if tc.name == "m4-success-batch" {
				if len(d.Results) != 3 || len(d.Failures) != 0 || failedAttempts != 0 {
					t.Fatal("M4 successful batch changed three-worker delivery")
				}
			} else if tc.name == "m4-mixed-three-failed" {
				if len(d.Results) != 0 || len(d.Failures) != 3 || failedAttempts != 3 || len(v.WorkerResults) != 0 || len(v.Ledger.ConsumedBatch) != 0 || !barrier.proved || !slices.Equal(barrier.order, []string{"w2"}) {
					t.Fatal("three failed workers invented results or changed single-delivery accounting")
				}
			} else if strings.HasPrefix(tc.name, "m4-mixed-") {
				if len(d.Results) != 1 || len(d.Failures) != 2 || failedAttempts != 2 || !barrier.proved || !slices.Equal(barrier.order, []string{"w3", "w2"}) {
					t.Fatal("mixed batch lost committed sibling or failure delivery")
				}
				var worker publication[WorkerResult]
				if err := protocol.ReadJSON(d.Results[0].Path, &worker); err != nil || worker.Data.TaskID != "w3" || report.Snapshot.Attempts[d.Results[0].AttemptID].Output == nil {
					t.Fatal("mixed batch promoted an uncommitted candidate or lost successful sibling", err)
				}
			} else if len(d.Results) != 0 || len(d.Failures) != 1 || failedAttempts != 1 || len(v.WorkerResults) != 0 {
				t.Fatal("M4 all-failed batch invented worker output or discarded typed failure")
			}
		}
		failures := slices.Clone(v.Recovery.PlannerFailures)
		for _, d := range v.Recovery.Deliveries {
			failures = append(failures, d.Failures...)
		}
		for _, f := range failures {
			a, ok := report.Snapshot.Attempts[f.AttemptID]
			owner := report.Snapshot.Sessions[f.Identity.HandleID]
			wantCode, wantOrigin := engine.TimedOut, engine.OriginAttemptDeadline
			if tc.name == "m4-planner-first-compaction" || (tc.name == "m4-mixed-compaction" || tc.name == "m4-mixed-committed-close-cancel" || tc.name == "m4-mixed-three-failed") && f.TaskID == "w2" {
				wantCode, wantOrigin = engine.CompactionFailed, engine.OriginCompaction
			}
			if strings.HasPrefix(tc.name, "m4-mixed-") && (f.TaskID == "w1" || tc.name == "m4-mixed-three-failed" && f.TaskID == "w3") {
				wantCode, wantOrigin = engine.Cancelled, engine.OriginFailFastSibling
			}
			if !ok || a.Failure == nil || a.Output != nil || f.Code != wantCode || f.Origin != wantOrigin || f.RunID != report.Snapshot.RunID || f.StepID != a.Identity.InvocationID || a.HandleID != f.Identity.HandleID || !sameIdentity(f.Identity, owner.Identity) || f.Cleanup == nil || !sameIdentity(f.Cleanup.Identity, owner.Identity) || !f.Cleanup.ConfirmsLocalClose(owner.Identity.SessionID) {
				t.Fatalf("M4 recovery identity/failure differs from actual failed attempt: %+v", f)
			}
		}
		if tc.name == "m4-capacity-fresh-worker-timeout" {
			if len(barrier.stats) != 1 || v.Checkpoint == nil || v.Checkpoint.DispatchCycle != 1 || v.Checkpoint.FullCheckpoint {
				t.Fatal("M4 capacity fresh introduced a second dispatch/checkpoint")
			}
			for _, s := range report.Snapshot.Sessions {
				if s.Role.Name != "triage-planner" || s.Identity.HandleID == report.Snapshot.Attempts[v.Recovery.Deliveries[0].Proposal.AttemptID].HandleID {
					continue
				}
				for _, a := range report.Snapshot.Attempts {
					if a.HandleID == s.Identity.HandleID && a.LastSeq < report.Snapshot.Attempts[failures[0].AttemptID].LastSeq {
						t.Fatal("fresh Planner unexpectedly ran a Step before worker failure")
					}
				}
			}
		} else if len(barrier.stats) != 0 || v.Checkpoint != nil {
			t.Fatal("M4 nil capacity performed stats or acquired checkpoint policy")
		}
		return
	}
	if strings.HasPrefix(tc.name, "m3-") {
		if report.Final != nil || len(report.Snapshot.Retries) != 0 || report.Snapshot.Policy.MaxLiveSessions != 4 || len(report.Snapshot.Attempts) != prompts || len(report.Snapshot.Sessions) != hellos || len(report.Cleanup) != hellos {
			t.Fatal("M3 lost run accounting or invented retries/final")
		}
		planners := 0
		for _, s := range report.Snapshot.Sessions {
			if s.State != "Closed" {
				t.Fatal("M3 returned before session cleanup")
			}
			if s.Role.Name == "triage-planner" {
				planners++
				if s.Role.Model != (runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}) {
					t.Fatal("M3 changed the independent Planner Model")
				}
			}
		}
		for _, c := range report.Cleanup {
			if !c.WaitCompleted || !c.ProcessExited {
				t.Fatal("M3 did not wait for local process exit")
			}
			if tc.name != "m3-stats-cleanup-failure" && tc.name != "m3-stats-fatal" && !c.ConfirmsLocalClose(c.Identity.SessionID) {
				t.Fatalf("M3 strict cleanup failed: %+v", c)
			}
		}
		if (len(report.CleanupErrors) != 0) != (tc.name == "m3-stats-cleanup-failure" || tc.name == "m3-stats-fatal") {
			t.Fatal("M3 swallowed or invented cleanup errors")
		}
		if tc.name == "m3-stats-fatal" && !slices.ContainsFunc(report.Cleanup, func(c runtime.CleanupReport) bool {
			return slices.Contains(c.Unconfirmed, "abort not acknowledged") && slices.Contains(c.Unconfirmed, "abort_bash not acknowledged")
		}) {
			t.Fatal("fatal reader shutdown lost unacknowledged cleanup diagnostics")
		}
		var states []engine.AttemptState
		workers := map[string]bool{}
		for _, a := range report.Snapshot.Attempts {
			if a.State != engine.Succeeded || a.Output == nil {
				t.Fatal("M3 boundary failure rewrote a committed Step")
			}
			if a.Output.SchemaID == PlannerSchema {
				states = append(states, a)
			}
			if a.Output.SchemaID == WorkerSchema {
				var worker publication[WorkerResult]
				if err := protocol.ReadJSON(a.Output.Path, &worker); err != nil {
					t.Fatal(err)
				}
				key := worker.Data.Proposal.AttemptID + "/" + worker.Data.TaskID
				if workers[key] {
					t.Fatal("M3 replayed a task from the same accepted proposal")
				}
				workers[key] = true
			}
		}
		slices.SortFunc(states, func(a, b engine.AttemptState) int {
			if a.LastSeq < b.LastSeq {
				return -1
			}
			if a.LastSeq > b.LastSeq {
				return 1
			}
			return 0
		})
		if tc.failure {
			if ref.AttemptID != "" || len(report.Result.Outputs) != 0 {
				t.Fatal("M3 failure exposed successful outputs")
			}
			if strings.HasPrefix(tc.name, "m3-policy-invalid-") {
				if planners != 0 || len(states) != 0 || len(barrier.stats) != 0 || !strings.Contains(fmt.Sprint(report.Failure), "explicit finite handoff percentage") {
					t.Fatal("invalid policy dispatched a Planner or was not rejected at entry")
				}
				return
			}
			if planners != 1 {
				t.Fatal("M3 error was hidden by a fresh session")
			}
			if strings.HasPrefix(tc.name, "m3-stats-") {
				want := engine.ProtocolFailed
				switch tc.name {
				case "m3-stats-identity", "m3-stats-history":
					want = engine.SessionChanged
				case "m3-stats-timeout":
					want = engine.RPCUnresponsive
				case "m3-stats-cancel":
					want = engine.Cancelled
				case "m3-stats-cleanup-failure":
					want = engine.CleanupFailed
				}
				var failure *engine.Failure
				if !errors.As(report.Failure, &failure) || failure.Code != want || len(states) != 1 || len(barrier.stats) != 1 || len(workers) != 0 {
					t.Fatalf("M3 stats failure lost classification/accepted state: want %s, got %v", want, report.Failure)
				}
				if tc.name == "m3-stats-cancel" && failure.Origin != engine.OriginControllerUser {
					t.Fatal("stats cancellation lost its origin")
				}
				return
			}
			message := "checkpoint differs from supplied continuation metadata"
			switch tc.name {
			case "m3-checkpoint-drop":
				message = "retain checkpoint metadata"
			case "m3-policy-change":
				message = "cannot change checkpoint capacity policy"
			case "m3-cycle-backward", "m3-cycle-skip", "m3-checkpoint-forged", "m3-full-forged":
				message = "counter echo mismatch"
			case "m3-retained-feedback-drop", "m3-retained-feedback-change", "m3-retained-feedback-reorder":
				message = "cannot drop or change controller feedback"
			case "m3-feedback-owner", "m3-feedback-blank":
				message = "feedback must bind the preceding accepted snapshot"
			}
			var failure *engine.Failure
			var schemaErr *contract.Error
			if errors.As(report.Failure, &failure) || errors.As(report.Failure, &schemaErr) || !strings.Contains(fmt.Sprint(report.Failure), message) {
				t.Fatalf("M3 semantic publication rejection, want %q, got %v", message, report.Failure)
			}
			return
		}
		var final publication[PlannerState]
		if err := protocol.ReadJSON(ref.Path, &final); err != nil || !reflect.DeepEqual(final.Data, expected) || report.Result.Outputs["planner"] != ref || final.Data.Ledger.Action != "yield" {
			t.Fatal("M3 lost exact full final snapshot", err)
		}
		cycles, wantPlanners, wantStats := []int{0, 1}, 1, 1
		switch tc.name {
		case "m3-cycle-six":
			cycles, wantPlanners, wantStats = []int{0, 1, 2, 3, 4, 5, 6}, 2, 5
		case "m3-plan-zero", "m3-no-ready-zero":
			cycles, wantStats = []int{0, 0, 1}, 2
		case "m3-capacity-at-plan":
			cycles, wantPlanners, wantStats = []int{0, 0, 1}, 3, 2
		case "m3-fresh-reconstruct":
			cycles, wantPlanners = []int{0, 0, 1}, 2
		case "m3-capacity-at", "m3-capacity-above", "m3-fresh-worker-before-step", "m3-fresh-wiki-before-step", "m3-support-pending":
			wantPlanners = 2
		case "m3-support-no-sample":
			wantPlanners, wantStats = 2, 0
		case "m3-yield-no-sample":
			cycles, wantStats = []int{0}, 0
		}
		if len(states) != len(cycles) || planners != wantPlanners || len(barrier.stats) != wantStats {
			t.Fatalf("M3 copying Step, fresh loop or extra stats: snapshots=%d planners=%d stats=%d", len(states), planners, len(barrier.stats))
		}
		for i, a := range states {
			var p publication[PlannerState]
			if err := protocol.ReadJSON(a.Output.Path, &p); err != nil {
				t.Fatal(err)
			}
			c := p.Data.Checkpoint
			full := i > 0 && cycles[i] != cycles[i-1] && cycles[i]%3 == 0
			if c == nil || c.Policy.HandoffPercent != 80 || c.DispatchCycle != cycles[i] || c.CheckpointCycle != cycles[i]/3*3 || c.FullCheckpoint != full {
				t.Fatalf("M3 snapshot %d counters/full/policy differ: %+v", i+1, c)
			}
			if i == 0 && p.Data.Previous != nil || i > 0 && (p.Data.Previous == nil || *p.Data.Previous != *states[i-1].Output) {
				t.Fatal("M3 snapshot lost exact committed previous")
			}
			if len(p.Data.Hypotheses) != 1 || len(p.Data.Pending) != 1 {
				t.Fatal("M3 checkpoint was a delta instead of full domain state")
			}
		}
		c := final.Data.Checkpoint
		feedbackSamples := 0
		for _, f := range c.ControllerFeedback {
			if strings.Contains(f.Note, "No declared task") {
				continue
			}
			_, raw, ok := strings.Cut(f.Note, "On-demand sample: ")
			var usage runtime.ContextUsage
			if !ok || json.Unmarshal([]byte(raw), &usage) != nil || usage.Identity.SessionID == "" || usage.SampledAt.IsZero() || usage.Seq == 0 || !slices.Contains(barrier.stats, f.After) {
				t.Fatal("M3 dropped actual sample diagnostics or exact snapshot feedback binding")
			}
			if tc.name == "m3-capacity-at" || tc.name == "m3-capacity-above" || tc.name == "m3-capacity-at-plan" {
				if usage.Percent == nil || *usage.Percent < 80 || !strings.Contains(f.Note, "strict close") {
					t.Fatal("known high usage was not retained")
				}
			} else if usage.Percent != nil || !strings.Contains(f.Note, "unknown") || !strings.Contains(f.Note, "without treating it as zero") {
				t.Fatal("unknown became zero or a business conclusion")
			}
			if tc.name == "m3-unknown-tokens" && (usage.Tokens == nil || *usage.Tokens != 99000) {
				t.Fatal("unknown percent lost known tokens")
			}
			feedbackSamples++
		}
		wantSamples := wantStats
		if tc.name == "m3-cycle-six" || tc.name == "m3-capacity-below" || tc.name == "m3-capacity-zero" {
			wantSamples = 0
		}
		if feedbackSamples != wantSamples {
			t.Fatalf("M3 pending feedback lost across fresh/support: got %d want %d", feedbackSamples, wantSamples)
		}
		if tc.name == "m3-cycle-six" {
			if len(final.Data.WorkerResults) != 5 || len(final.Data.WikiResults) != 2 || final.Data.Context == original.Context || final.Data.Ledger.Round != 3 {
				t.Fatal("M3 batch/support/wiki cycles were conflated with Steps or worker rounds")
			}
			for _, workerRef := range final.Data.WorkerResults[:3] {
				var w publication[WorkerResult]
				if err := protocol.ReadJSON(workerRef.Path, &w); err != nil || w.Data.Context != original.Context {
					t.Fatal("M3 rebound historical worker to new context", err)
				}
			}
		}
		return
	}
	if report.Final != nil || len(report.Snapshot.Retries) != 0 {
		t.Fatal("investigation invented final delivery/retry policy")
	}
	if report.Snapshot.Policy.MaxLiveSessions != 4 || len(report.Snapshot.Attempts) != prompts || len(report.Snapshot.Sessions) != hellos {
		t.Fatal("M2 run limits/accounting changed")
	}
	planners := 0
	for _, s := range report.Snapshot.Sessions {
		if s.State != "Closed" {
			t.Fatal("M2 run returned before joining/closing every session")
		}
		if s.Role.Name == "triage-planner" {
			planners++
			if s.Role.Model != (runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}) {
				t.Fatalf("M2 Planner model changed: %+v", s.Role.Model)
			}
		}
	}
	if (tc.name == "m2-wiki-support" || tc.name == "m2-wiki-handoff" || tc.name == "m2-session-not-progress") && planners != 2 {
		t.Fatalf("M2 support/handoff must retain the Planner model in both sessions, got %d sessions", planners)
	}
	if len(report.Cleanup) != len(report.Snapshot.Sessions) {
		t.Fatal("M2 cleanup accounting lost a session report")
	}
	for _, c := range report.Cleanup {
		if !c.WaitCompleted || !c.ProcessExited {
			t.Fatal("M2 cleanup did not confirm local Wait/process exit")
		}
		if tc.name != "m2-branch-cleanup-failure" && tc.name != "m2-branch-abort-unacknowledged" && !c.ConfirmsLocalClose(c.Identity.SessionID) {
			t.Fatalf("M2 cleanup not strict: WaitError=%q KillError=%q DiscoveryError=%q Unconfirmed=%v StderrTail=%.300q", c.WaitError, c.KillError, c.DiscoveryError, c.Unconfirmed, c.StderrTail)
		}
	}
	if tc.name == "m2-branch-cleanup-failure" || tc.name == "m2-branch-abort-unacknowledged" {
		if len(report.CleanupErrors) == 0 {
			t.Fatal("M2 cleanup failure was swallowed")
		}
	} else if len(report.CleanupErrors) != 0 {
		t.Fatal("M2 has unexpected cleanup failures")
	}
	if tc.name == "m2-branch-abort-unacknowledged" {
		if !slices.ContainsFunc(report.Cleanup, func(c runtime.CleanupReport) bool { return slices.Contains(c.Unconfirmed, "abort not acknowledged") }) {
			t.Fatal("frozen sibling lost abort/cleanup diagnostics")
		}
	}
	for _, a := range report.Snapshot.Attempts {
		if !slices.Contains([]engine.State{engine.Succeeded, engine.Failed, engine.CancelledState, engine.TimedOutState}, a.State) {
			t.Fatal("M2 returned with an unjoined attempt")
		}
	}
	if tc.failure {
		if ref.AttemptID != "" || len(report.Result.Outputs) != 0 {
			t.Fatal("failed investigation exposed checkpoint outputs")
		}
		want := engine.WorkflowFailed
		switch tc.name {
		case "m2-branch-provider-failure", "m2-branch-abort-unacknowledged", "m2-planner-provider-failure":
			want = engine.ProviderFailed
		case "m2-branch-cancel":
			want = engine.Cancelled
		case "m2-branch-timeout":
			want = engine.TimedOut
		case "m2-branch-cleanup-failure":
			want = engine.CleanupFailed
		case "m2-branch-schema":
			want = engine.ContractInvalid
		case "m2-attempt-cap":
			want = engine.LimitExceeded
		}
		var failure *engine.Failure
		if want == engine.WorkflowFailed {
			var contractErr *contract.Error
			if errors.As(report.Failure, &failure) || errors.As(report.Failure, &contractErr) || errors.Is(report.Failure, context.Canceled) || errors.Is(report.Failure, context.DeadlineExceeded) {
				t.Fatalf("M2 semantic rejection became execution/contract failure: %v", report.Failure)
			}
		} else if !errors.As(report.Failure, &failure) || failure.Code != want {
			t.Fatalf("M2 expected %s, got %v", want, report.Failure)
		}
		if tc.name == "m2-branch-timeout" && failure.Origin != engine.OriginAttemptDeadline {
			t.Fatal("M2 timeout lost attempt deadline origin")
		}
		messages := map[string]string{
			"m2-batch-consumed-order": "consumed batch mismatch", "m2-batch-results-order": "consumed batch mismatch", "m2-batch-foreign-ref": "distinct workflow-accepted refs",
			"m2-ledger-missing": "adaptive ledger", "m2-ledger-drop": "retained ledger", "m2-action-mismatch": "action and proposed work differ",
			"m2-progress-without-batch": "progress requires a consumed worker round", "m2-round-initial": "round/streak/reframe echo mismatch",
			"m2-consumed-drop": "consumed batch mismatch", "m2-consumed-foreign": "consumed batch mismatch", "m2-round-echo": "round/streak/reframe echo mismatch", "m2-streak-echo": "round/streak/reframe echo mismatch", "m2-reframe-counter-echo": "round/streak/reframe echo mismatch",
			"m2-progress-unknown-id": "distinct declared ID", "m2-progress-duplicate": "distinct declared ID", "m2-progress-owner": "exact input owner/file",
			"m2-reframe-required": "two rounds without reported hypothesis progress require reframe", "m2-reframe-same-terms": "changed wiki terms", "m2-reframe-wiki-reset": "round/streak/reframe echo mismatch",
			"m2-wiki-partial-runtime": "completed investigation wiki prerequisite", "m2-wiki-partial-drop-gap": "cannot hide investigation wiki gaps", "m2-wiki-drop-ref": "exact wiki deliveries",
			"m2-wiki-previous-terms": "previous_terms differ", "m2-wiki-basis-owner": "exact input owner/file",
			"m2-wiki-binding-proposal": "expected triage.planner.v1", "m2-wiki-binding-context": "proposal/task/context/history mismatch", "m2-wiki-binding-task": "proposal/task/context/history mismatch", "m2-wiki-binding-inputs": "wiki inputs mismatch", "m2-wiki-binding-intake": "wiki search provenance missing", "m2-wiki-omitted-term": "omitted a dispatched search term",
			"m2-branch-binding": "proposal/context/task/inputs mismatch", "m2-query-utc": "UTC", "m2-incomplete-no-gap": "complete/incomplete delivery with gaps",
		}
		if message, ok := messages[tc.name]; ok && !strings.Contains(report.Failure.Error(), message) {
			t.Fatalf("wrong M2 rejection, want %q: %v", message, report.Failure)
		}
		if tc.name == "m2-wiki-partial-runtime" {
			var originalWiki publication[WikiSearch]
			if err := protocol.ReadJSON(original.Wiki.Path, &originalWiki); err != nil || !wikiComplete(originalWiki.Data) {
				t.Fatal("runtime gate fixture did not start with completed historical wiki", err)
			}
			partial := false
			for _, a := range report.Snapshot.Attempts {
				if a.Output == nil {
					continue
				}
				if a.Output.SchemaID == WorkerSchema {
					t.Fatal("partial investigation wiki permitted runtime worker dispatch")
				}
				if a.Output.SchemaID == WikiSchema {
					var wiki publication[WikiSearch]
					if err := protocol.ReadJSON(a.Output.Path, &wiki); err != nil {
						t.Fatal(err)
					}
					partial = partial || (wiki.Data.Task != nil && wiki.Data.Status == "partial" && len(wiki.Data.Gaps) > 0)
				}
			}
			if !partial {
				t.Fatal("runtime prerequisite failure lacked actual partial investigation wiki")
			}
		}
		if strings.HasPrefix(tc.name, "m2-branch-") {
			if !barrier.proved {
				t.Fatal("branch failure did not exercise three live workers")
			}
			committed := 0
			planners := 0
			for _, a := range report.Snapshot.Attempts {
				if a.Output != nil && a.Output.SchemaID == WorkerSchema {
					committed++
				}
				if a.Output != nil && a.Output.SchemaID == PlannerSchema {
					planners++
				}
			}
			if committed < 1 || planners != 1 {
				t.Fatal("failure did not preserve partial publication without a new Planner checkpoint")
			}
		}
		return
	}
	var accepted publication[PlannerState]
	if err := protocol.ReadJSON(ref.Path, &accepted); err != nil || !reflect.DeepEqual(accepted.Data, expected) {
		t.Fatal("M2 final accepted state differs from the submitted snapshot", err)
	}
	state := accepted.Data
	if state.Ledger == nil || state.Ledger.Action != "yield" || report.Result.Outputs["planner"] != ref {
		t.Fatal("yield is not the exact accepted investigation state")
	}
	attempt := report.Snapshot.Attempts[ref.AttemptID]
	if attempt.State != engine.Succeeded || attempt.Output == nil || *attempt.Output != ref {
		t.Fatal("yield was not engine committed")
	}
	wantRound, wantStreak, wantReframe, wantBoundary := 1, 1, 0, 0
	switch tc.name {
	case "m2-progress-explicit":
		wantRound, wantStreak = 2, 0
	case "m2-parallel-batches-yield", "m2-reframe-yield-gap":
		wantRound, wantStreak = 2, 2
	case "m2-reframe-complete", "m2-reframe-partial":
		wantRound, wantStreak, wantReframe, wantBoundary = 2, 2, 2, 2
	case "m2-reframe-repeat":
		wantRound, wantStreak, wantReframe, wantBoundary = 4, 4, 4, 4
	}
	l := state.Ledger
	if l.Round != wantRound || l.NoProgress != wantStreak || l.ReframeRound != wantReframe || l.ReframeStreak != wantBoundary {
		t.Fatalf("ledger counters round=%d streak=%d reframe=%d boundary=%d", l.Round, l.NoProgress, l.ReframeRound, l.ReframeStreak)
	}
	if tc.name == "m2-progress-explicit" && (len(l.Changes) != 1 || state.Hypotheses[0].Assessment != "Unverified; compare runtime evidence") {
		t.Fatal("Agent-declared progress was coupled to assessment text")
	}
	if tc.name == "m2-parallel-batches-yield" {
		if !barrier.proved || !slices.Equal(barrier.order, []string{"w3", "w2", "w1"}) {
			t.Fatal("reverse completion barrier was not exercised")
		}
		if len(state.WorkerResults) != 5 {
			t.Fatal("multiple batches lost results")
		}
		for i, ref := range state.WorkerResults {
			var worker publication[WorkerResult]
			if err := protocol.ReadJSON(ref.Path, &worker); err != nil || worker.Data.TaskID != fmt.Sprintf("w%d", i+1) {
				t.Fatal("joined refs not in declared order", err)
			}
		}
		if report.Snapshot.Attempts[barrier.attempts["w3"]].LastSeq >= report.Snapshot.Attempts[barrier.attempts["w2"]].LastSeq || report.Snapshot.Attempts[barrier.attempts["w2"]].LastSeq >= report.Snapshot.Attempts[barrier.attempts["w1"]].LastSeq {
			t.Fatal("actual committed order did not reverse declaration order")
		}
	}
	if len(state.WikiResults) > 0 {
		var old publication[Context]
		if err := protocol.ReadJSON(original.Context.Path, &old); err != nil || old.Data.Wiki != original.Wiki || old.Data.Intake != original.Intake {
			t.Fatal("investigation rebound original context/wiki/intake", err)
		}
		for _, wikiRef := range state.WikiResults {
			var wiki publication[WikiSearch]
			if err := protocol.ReadJSON(wikiRef.Path, &wiki); err != nil || wiki.Data.Task == nil || wiki.Data.Task.Context != original.Context || wiki.Data.Intake != original.Intake || wikiRef == original.Wiki {
				t.Fatal("new wiki lost its own exact historical binding", err)
			}
			for _, gap := range wiki.Data.Gaps {
				if !slices.Contains(state.Gaps, gap) {
					t.Fatal("partial wiki gap was hidden by old complete context")
				}
			}
		}
	}
	if tc.name == "m2-wiki-support" {
		if state.Context == original.Context {
			t.Fatal("support action did not produce a revised context")
		}
		var next publication[Context]
		if err := protocol.ReadJSON(state.Context.Path, &next); err != nil || next.Data.Previous == nil || *next.Data.Previous != original.Context || next.Data.Intake != original.Intake || next.Data.Wiki != original.Wiki || next.Data.Readiness != "ready" {
			t.Fatal("support revision lost original provenance/readiness", err)
		}
	}
}

type triageCase struct {
	name    string
	stages  int
	failure bool
	ready   bool
}

var triageCases = []triageCase{
	{"m5-three-fresh-yield", 9, false, true},
	{"m5-reverse-completion", 9, false, true},
	{"m5-pending-claim-fresh-handoff", 9, false, true},
	{"m5-delivery-fresh-handoff", 9, false, true},
	{"m5-feedback-worker-new-claim", 16, false, true},
	{"m5-feedback-history-prefix", 14, true, false},
	{"m5-new-version-cross-reject", 13, true, false},
	{"m5-verifier-timeout-retry", 10, false, true},
	{"m5-verifier-compaction-retry", 10, false, true},
	{"m5-verifier-unavailable", 10, false, true},
	{"m5-same-version-missing-only", 12, false, true},
	{"m5-new-claim-all-fresh", 14, false, true},
	{"m5-new-evidence-all-fresh", 14, false, true},
	{"m5-agent-changes-reset", 9, false, true},
	{"m5-agent-inference", 9, false, true},
	{"m5-agent-runtime-declared", 9, false, true},
	{"m5-claim-timeout-retry", 10, false, true},
	{"m5-planner-feedback-timeout", 10, false, true},
	{"m5-claim-retry-exhausted", 6, true, false},
	{"m5-verifier-attempt-limit", 5, true, false},
	{"m5-verifier-live-limit", 5, true, false},
	{"m5-policy-pro-model", 3, true, false},
	{"m5-policy-con-model", 3, true, false},
	{"m5-policy-cross-model", 3, true, false},
	{"m5-policy-pro-retries", 3, true, false},
	{"m5-policy-con-retries", 3, true, false},
	{"m5-policy-cross-retries", 3, true, false},
	{"m5-policy-no-recovery", 3, true, false},
	{"m5-claim-projection", 5, true, false},
	{"m5-claim-parent", 5, true, false},
	{"m5-claim-context", 5, true, false},
	{"m5-claim-ledger", 5, true, false},
	{"m5-verifier-role", 8, true, false},
	{"m5-verifier-claim", 8, true, false},
	{"m5-verifier-evidence", 8, true, false},
	{"m5-verifier-unauthorized-basis", 8, true, false},
	{"m5-verifier-schema", 8, true, false},
	{"m5-feedback-missing", 9, true, false},
	{"m5-feedback-wrong-claim", 9, true, false},
	{"m5-feedback-wrong-action", 9, true, false},
	{"m5-feedback-model-echo", 9, true, false},
	{"m5-feedback-repeat-completed", 9, true, false},
	{"m5-partial-provider-fatal", 8, true, false},
	{"m5-partial-cleanup-fatal", 8, true, false},
	{"m5-partial-storage-fatal", 8, true, false},
	{"m5-partial-journal-fatal", 8, true, false},
	{"m5-partial-user-cancel", 8, true, false},
	{"m5-supplement-claim-owner-role", 6, true, false},
	{"m5-supplement-claim-owner-model", 6, true, false},
	{"m5-supplement-claim-owner-key", 6, true, false},
	{"m5-supplement-verifier-owner-role", 7, true, false},
	{"m5-supplement-verifier-owner-model", 7, true, false},
	{"m5-supplement-verifier-owner-key", 7, true, false},
	{"m5-supplement-claim-parent-binding", 7, true, false},
	{"m5-supplement-reframe-wiki", 17, false, true},
	{"m5-supplement-reframe-inspection-resume", 21, false, true},
	{"m5-supplement-exhausted-pro-fatal-cross", 9, true, false},
	{"m5-supplement-exhausted-cross-fatal-pro", 9, true, false},
	{"m5-store-claim-ledger", 0, true, false},
	{"m5-store-claim-verdict", 0, true, false},
	{"m5-store-claim-missing-parent", 0, true, false},
	{"m5-store-verifier-model", 0, true, false},
	{"m5-store-verifier-role", 0, true, false},
	{"m5-store-verifier-missing-assessment", 0, true, false},
	{"m4-delivery-id", 6, true, false},
	{"m4-delivery-proposal", 6, true, false},
	{"m4-delivery-context", 6, true, false},
	{"m4-delivery-results", 6, true, false},
	{"m4-allfail-blind-new-id", 6, true, false},
	{"m4-allfail-caller-bool", 6, true, false},
	{"m4-nil-recovery-timeout", 4, true, false},
	{"m4-planner-later-user-cancel", 5, true, false},
	{"m4-planner-parent-deadline", 5, true, false},
	{"m4-planner-run-limit", 4, true, false},
	{"m4-planner-storage-fatal", 5, true, false},
	{"m4-planner-journal-fatal", 5, true, false},
	{"m4-mixed-cleanup-fatal", 7, true, false},
	{"m4-mixed-storage-fatal", 7, true, false},
	{"m4-mixed-journal-fatal", 7, true, false},
	{"m4-mixed-journal-fatal-late-abort", 7, true, false},
	{"m4-mixed-user-cancel", 7, true, false},
	{"m4-mixed-wait-fatal", 7, true, false},
	{"m4-mixed-three-failed", 8, false, true},
	{"m4-support-update-wiki-timeout", 12, false, true},
	{"m4-support-update-context-timeout", 12, false, true},
	{"m4-allfail-reframe-checkpoint", 10, false, true},
	{"m4-reframe-timeout-inspection-resume", 14, false, true},
	{"m4-reframe-no-inspection", 10, true, false},
	{"m4-reframe-stale-inspection", 16, true, false},
	{"m4-reframe-completed-inspection", 18, true, false},
	{"m4-allfail-agent-progress", 8, false, true},
	{"m4-wiki-timeout-partial-resume", 10, false, true},
	{"m4-mixed-binding-fatal", 7, true, false},
	{"m4-support-unsafe-no-basis", 8, true, false},
	{"m4-support-unsafe-no-reason", 8, true, false},
	{"m4-support-unsafe-owner", 8, true, false},
	{"m4-meta-identity", 5, true, false},
	{"m4-meta-attempt", 5, true, false},
	{"m4-meta-cleanup", 5, true, false},
	{"m4-meta-diagnostic", 5, true, false},
	{"m4-meta-prefix", 6, true, false},
	{"m4-planner-first-compaction", 5, false, true},
	{"m4-mixed-timeout", 8, false, true},
	{"m4-mixed-compaction", 8, false, true},
	{"m4-mixed-committed-close-cancel", 8, false, true},
	{"m4-mixed-committed-close-cleanup-fatal", 7, true, false},
	{"m4-mixed-committed-close-storage-fatal", 7, true, false},
	{"m4-mixed-committed-close-journal-fatal", 7, true, false},
	{"m4-mixed-provider-fatal", 7, true, false},
	{"m4-support-resolve-wiki-timeout", 11, false, true},
	{"m4-support-resolve-context-timeout", 11, false, true},
	{"m4-planner-first-timeout", 5, false, true},
	{"m4-planner-retry-exhausted", 5, true, false},
	{"m4-planner-unknown-provider", 4, true, false},
	{"m4-planner-user-cancel", 4, true, false},
	{"m4-worker-timeout", 6, false, true},
	{"m4-capacity-fresh-worker-timeout", 6, false, true},
	{"m4-success-batch", 8, false, true},
	{"m4-policy-echo", 4, true, false},
	{"m4-drop-recovery", 4, true, false},
	{"m4-cycle-echo", 4, true, false},
	{"m3-cycle-six", 19, false, true},
	{"m3-capacity-zero", 6, false, true}, {"m3-capacity-at-plan", 7, false, true}, {"m3-policy-initial-echo", 4, true, false},
	{"m3-retained-feedback-drop", 7, true, false}, {"m3-retained-feedback-change", 7, true, false}, {"m3-retained-feedback-reorder", 7, true, false},
	{"m3-stats-window-zero", 4, true, false}, {"m3-stats-percent-type", 4, true, false}, {"m3-stats-tokens-type", 4, true, false}, {"m3-stats-missing-percent", 4, true, false}, {"m3-stats-missing-identity", 4, true, false},
	{"m3-capacity-below", 6, false, true}, {"m3-capacity-at", 6, false, true}, {"m3-capacity-above", 6, false, true},
	{"m3-unknown", 6, false, true}, {"m3-unknown-null", 6, false, true}, {"m3-unknown-missing", 6, false, true}, {"m3-unknown-tokens", 6, false, true},
	{"m3-plan-zero", 7, false, true}, {"m3-no-ready-zero", 7, false, true},
	{"m3-support-no-sample", 7, false, true}, {"m3-yield-no-sample", 4, false, true},
	{"m3-fresh-worker-before-step", 6, false, true}, {"m3-fresh-wiki-before-step", 6, false, true}, {"m3-fresh-reconstruct", 7, false, true}, {"m3-support-pending", 7, false, true},
	{"m3-checkpoint-missing", 4, true, false}, {"m3-illegal-optin", 4, true, false},
	{"m3-checkpoint-drop", 6, true, false}, {"m3-policy-change", 6, true, false}, {"m3-cycle-backward", 6, true, false}, {"m3-cycle-skip", 6, true, false}, {"m3-checkpoint-forged", 6, true, false}, {"m3-full-forged", 6, true, false},
	{"m3-feedback-drop", 6, true, false}, {"m3-feedback-change", 6, true, false}, {"m3-feedback-reorder", 5, true, false}, {"m3-feedback-owner", 6, true, false}, {"m3-feedback-blank", 6, true, false}, {"m3-feedback-invent", 6, true, false}, {"m3-note-change", 6, true, false},
	{"m3-stats-reject", 4, true, false}, {"m3-stats-identity", 4, true, false}, {"m3-stats-history", 4, true, false}, {"m3-stats-format", 4, true, false}, {"m3-stats-timeout", 4, true, false}, {"m3-stats-cancel", 4, true, false}, {"m3-stats-fatal", 4, true, false}, {"m3-stats-cleanup-failure", 4, true, false},
	{"m3-policy-invalid-zero", 3, true, false}, {"m3-policy-invalid-negative", 3, true, false}, {"m3-policy-invalid-above", 3, true, false}, {"m3-policy-invalid-nan", 3, true, false}, {"m3-policy-invalid-infinite", 3, true, false},
	{"m3-store-absent", 0, false, false}, {"m3-store-null", 0, true, false},
	{"m3-store-missing-policy", 0, true, false}, {"m3-store-missing-dispatch_cycle", 0, true, false}, {"m3-store-missing-checkpoint_cycle", 0, true, false}, {"m3-store-missing-full_checkpoint", 0, true, false}, {"m3-store-missing-adaptive_note", 0, true, false}, {"m3-store-missing-controller_feedback", 0, true, false},
	{"m3-store-negative-cycle", 0, true, false}, {"m3-store-fractional-cycle", 0, true, false}, {"m3-store-negative-checkpoint", 0, true, false}, {"m3-store-full-type", 0, true, false}, {"m3-store-policy-zero", 0, true, false}, {"m3-store-policy-above", 0, true, false}, {"m3-store-extra-control", 0, true, false}, {"m3-store-policy-extra", 0, true, false}, {"m3-store-feedback-null", 0, true, false},
	{"m2-batch-consumed-order", 8, true, false}, {"m2-batch-results-order", 8, true, false}, {"m2-batch-foreign-ref", 8, true, false},
	{"m2-parallel-batches-yield", 11, false, true}, {"m2-no-ready-feedback", 7, false, true},
	{"m2-progress-explicit", 8, false, true}, {"m2-assessment-not-progress", 6, false, true},
	{"m2-files-not-progress", 6, false, true}, {"m2-query-not-progress", 6, false, true}, {"m2-session-not-progress", 7, false, true},
	{"m2-ledger-missing", 4, true, false}, {"m2-action-mismatch", 4, true, false}, {"m2-progress-without-batch", 4, true, false}, {"m2-round-initial", 4, true, false},
	{"m2-consumed-drop", 6, true, false}, {"m2-consumed-foreign", 6, true, false}, {"m2-round-echo", 6, true, false}, {"m2-streak-echo", 6, true, false},
	{"m2-reframe-counter-echo", 6, true, false}, {"m2-ledger-drop", 6, true, false}, {"m2-progress-unknown-id", 6, true, false}, {"m2-progress-owner", 6, true, false}, {"m2-progress-duplicate", 6, true, false},
	{"m2-reframe-complete", 10, false, true}, {"m2-reframe-partial", 10, false, true}, {"m2-reframe-repeat", 16, false, true}, {"m2-reframe-yield-gap", 8, false, true},
	{"m2-reframe-required", 8, true, false}, {"m2-reframe-same-terms", 8, true, false}, {"m2-reframe-wiki-reset", 10, true, false},
	{"m2-wiki-worker", 8, false, true}, {"m2-wiki-runtime", 8, false, true}, {"m2-wiki-handoff", 8, false, true}, {"m2-wiki-support", 10, false, true}, {"m2-wiki-partial-worker", 8, false, true},
	{"m2-wiki-partial-runtime", 6, true, false}, {"m2-wiki-partial-drop-gap", 6, true, false}, {"m2-wiki-drop-ref", 6, true, false},
	{"m2-wiki-previous-terms", 4, true, false}, {"m2-wiki-basis-owner", 4, true, false},
	{"m2-wiki-binding-proposal", 5, true, false}, {"m2-wiki-binding-context", 5, true, false}, {"m2-wiki-binding-task", 5, true, false}, {"m2-wiki-binding-inputs", 5, true, false}, {"m2-wiki-binding-intake", 5, true, false}, {"m2-wiki-omitted-term", 5, true, false},
	{"m2-branch-abort-unacknowledged", 7, true, false}, {"m2-branch-provider-failure", 7, true, false}, {"m2-branch-cancel", 7, true, false}, {"m2-branch-timeout", 7, true, false}, {"m2-branch-cleanup-failure", 7, true, false}, {"m2-branch-binding", 7, true, false}, {"m2-branch-schema", 7, true, false},
	{"m2-query-utc", 5, true, false}, {"m2-incomplete-no-gap", 5, true, false}, {"m2-planner-provider-failure", 6, true, false}, {"m2-attempt-cap", 5, true, false},
	{"m2-store-action", 0, true, false}, {"m2-store-counter", 0, true, false}, {"m2-store-extra-control", 0, true, false}, {"m2-store-wiki-binding", 0, true, false},
	{"m1-dependencies-incomplete", 7, false, true},
	{"m1-support", 10, false, true}, {"m1-logs", 6, false, true},
	{"m1-worker-timeout", 5, true, false}, {"m1-worker-cancel", 5, true, false}, {"m1-planner-provider-failure", 6, true, false},
	{"m1-planner-invent-committed", 6, true, false}, {"m1-incomplete-no-gap", 5, true, false}, {"m1-query-utc", 5, true, false},
	{"m1-complete", 6, false, true}, {"m1-incomplete", 6, false, true}, {"m1-analysis", 6, false, true},
	{"m1-handoff-before", 6, false, true}, {"m1-handoff-after", 7, false, true}, {"m1-dependencies", 7, false, true},
	{"m1-pending", 4, true, false}, {"m1-unsatisfied", 4, true, false}, {"m1-redispatch", 5, true, false},
	{"m1-reuse-id", 6, true, false}, {"m1-planner-drop", 6, true, false}, {"m1-planner-invent", 6, true, false},
	{"m1-task-id", 5, true, false}, {"m1-proposal", 5, true, false}, {"m1-context", 5, true, false}, {"m1-inputs", 5, true, false},
	{"m1-owner", 5, true, false}, {"m1-file", 5, true, false}, {"m1-schema", 5, true, false}, {"m1-evidence-analysis", 5, true, false},
	{"m1-task-utc", 4, true, false}, {"m1-task-owner", 4, true, false}, {"m1-task-duplicate", 4, true, false},
	{"m1-worker-provider-failure", 5, true, false}, {"m1-worker-cleanup-failure", 5, true, false},
	{"m1-store-schema", 0, true, false}, {"m1-store-file", 0, true, false},
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
	{"r5-historical", 12, false, true},
	{"r5-incomplete", 5, false, true},
	{"r5-assessment-owner", 11, true, true},
	{"r5-claim-version", 11, true, true},
	{"r5-state-binding", 11, true, true},
	{"r5-context-binding", 11, true, true},
	{"r5-artifact-kind", 11, true, true},
	{"r5-file-path", 11, true, true},
	{"r5-body-tamper", 11, true, true},
	{"r5-renderer-collision", 11, true, true},
	{"r5-cancel", 11, true, true},
	{"r5-versioned", 16, false, true},
	{"r5-version-binding", 16, true, true},
	{"r5-producer-forgery", 11, true, true},
	{"r5-uncommitted", 11, true, true},
	{"r5-evidence-tamper", 11, true, true},
	{"r5-cleanup-failure", 11, true, true},
}

// Data/contract scenarios use the real Store and publication validators below.
// Dispatch, cross-Step ownership, session and failure scenarios stay on RPC.
// Truncated intake, blank proposal reason and history-alias negatives also stay
// there to detect a production loader accidentally bypassing its validator.
func validationStage(name string) string {
	if strings.HasPrefix(name, "m5-store-") {
		return "m2-schema"
	}
	if strings.HasPrefix(name, "m3-store-") {
		return "m2-schema"
	}
	if strings.HasPrefix(name, "m2-store-") {
		return "m2-schema"
	}
	if strings.HasPrefix(name, "m1-store-") {
		return "worker"
	}
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
	if name == "m1-store-file" {
		return "unknown worker evidence file"
	}
	if name == "m1-store-schema" {
		return "schema"
	}
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
	case "support-invalid-status", "planner-extra-control", "work-extra-control", "work-bad-kind", "m1-store-schema":
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
			if stage == "m2-schema" {
				m2StoreSchema(t, store, base, tc.name)
				return
			}
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
			case "worker":
				request := workerRequest{Proposal: base.wikiRef, Context: base.intakeRef, Task: WorkerTask{ID: "w1", Responsibility: "evidence-only"}}
				refs := []contract.Ref{base.intakeRef, base.wikiRef}
				v, files := workerFixture("m1-"+strings.TrimPrefix(tc.name, "m1-store-"), request, refs)
				ref, e := storeFixture(t, store, WorkerSchema, v, files, false)
				err = e
				if err == nil {
					err = checkWorkerResult(storedPublication[WorkerResult](t, store, ref, v), request, refs, sources)
				}
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
			lateAbort := tc.name == "m4-mixed-journal-fatal-late-abort"
			if lateAbort {
				// Keep the original fault path and all of its outcome assertions.
				tc.name = "m4-mixed-journal-fatal"
			}
			m1 := strings.HasPrefix(tc.name, "m1-")
			m3 := strings.HasPrefix(tc.name, "m3-")
			m4 := strings.HasPrefix(tc.name, "m4-")
			r5 := strings.HasPrefix(tc.name, "r5-")
			m5 := strings.HasPrefix(tc.name, "m5-") || r5
			m2 := strings.HasPrefix(tc.name, "m2-") || m3 || m4 || m5
			working := strings.HasPrefix(tc.name, "work-") || tc.name == "m1-support"
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
			planning := strings.HasPrefix(tc.name, "planner-") || working || m1
			updating := strings.HasPrefix(tc.name, "update-") || tc.name == "planner-history" || (working && workKind == "update") || tc.name == "work-wrong-task" || tc.name == "work-resolve-changed-intake"
			revising := refreshing || updating
			acquiring := (resolving && !supporting) || revising || strings.HasPrefix(tc.name, "http-") || strings.HasPrefix(tc.name, "m4-support-")
			mode := tc.name
			if m2 {
				mode = "complete"
				if tc.name == "m2-wiki-support" {
					mode = "time-unresolved"
				}
				if tc.name == "m3-cycle-six" || tc.name == "m3-support-no-sample" || tc.name == "m3-support-pending" || strings.HasPrefix(tc.name, "m4-support-") {
					mode = "wiki-partial"
				}
			}
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
			if strings.HasPrefix(tc.name, "m4-support-update-") {
				revisionURL = updateHTTPFixture(t, "update-replace", func(*http.Request) { newRequests.Add(1) }).URL
			}
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
			fixtureTimeout := 20 * time.Second
			if m2 {
				fixtureTimeout = 90 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), fixtureTimeout)
			defer cancel()
			host, err := protocol.NewHost(ctx, 16)
			if err != nil {
				t.Fatal(err)
			}
			var r *engine.Run
			done := make(chan struct{})
			var report engine.Report
			var result ContextResult
			var beforeResolution ContextResult
			var plannerRef, firstPlannerRef, reportRef contract.Ref
			var plannerSteps int
			var expectedPlanner PlannerState
			t.Cleanup(func() {
				cancel()
				if r != nil {
					r.Cancel(engine.OriginControllerUser)
				}
				if err := host.Close(); err != nil {
					t.Errorf("fixture host close: %v", err)
				}
				if r != nil {
					select {
					case <-done:
					case <-time.After(8 * time.Second):
						t.Error("fixture run did not join")
					}
				}
			})
			policy := slicePolicy()
			policy.RunTimeout = time.Nanosecond
			policy.Runtime.StartupTimeout = 5 * time.Second
			policy.Runtime.CleanupTimeout = 3 * time.Second
			policy.Runtime.AbortGrace = 50 * time.Millisecond
			policy.Runtime.HealthInterval = time.Hour
			if m2 {
				policy.MaxLiveSessions = 4 // Three ready workers plus the persistent Planner.
				if tc.name == "m2-attempt-cap" {
					policy.MaxTotalAttempts = 5
				}
			}
			if tc.name == "m5-verifier-attempt-limit" {
				policy.MaxTotalAttempts = 7
			}
			if tc.name == "m5-verifier-live-limit" {
				policy.MaxLiveSessions = 3
			}
			if tc.name == "m4-planner-run-limit" {
				policy.MaxTotalAttempts = 4
			}
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
			statsControl := ""
			if m3 || tc.name == "m4-capacity-fresh-worker-timeout" || tc.name == "m4-allfail-reframe-checkpoint" || strings.HasPrefix(tc.name, "m4-reframe-") {
				statsControl = "1"
			}
			if tc.name == "m3-stats-timeout" {
				policy.Runtime.RPCTimeout = time.Second
			}
			pi, err := runtime.New(runtime.Options{Executable: exe, Args: []string{"-test.run=^TestTriageProtocolSubprocess$", "--"}, Env: []string{"PWC_TRIAGE_PROTOCOL=1", "PWC_ENGINE_MANUAL_CANDIDATE=1", "PWC_ENGINE_CONTROL_STATS=" + statsControl, "PWC_ENGINE_CONTROL=" + host.Addr().String(), "GORACE=atexit_sleep_ms=0"}, BridgeDir: bridge, Policy: policy.Runtime, Observe: func(ctx context.Context, o runtime.Observation) error { return r.Observe(ctx, o) }})
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
				if err == nil && r5 {
					plannerRef, reportRef, err = r5Run(t, ctx, run, scope, result.Context, tc.name)
				}
				if err == nil && m2 && !r5 {
					plannerRef, err = m2Run(t, ctx, run, scope, models, result.Context, tc.name)
				}
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
					if err == nil && m1 {
						proposal := plannerRef
						id := "w1"
						if tc.name == "m1-unsatisfied" {
							id = "w2"
						}
						var delivered contract.Ref
						delivered, err = planner.work(ctx, models, id)
						if planner.last == nil || *planner.last != proposal {
							t.Error("worker changed the last Planner snapshot")
						}
						if err != nil && len(planner.workerResults) != 0 {
							t.Error("failed worker became workflow accepted")
						}
						if err == nil {
							if !slices.Equal(planner.workerResults, []contract.Ref{delivered}) || planner.stopped {
								t.Error("worker success lost accepted result or stopped Planner")
							}
							a := run.Snapshot().Attempts[delivered.AttemptID]
							if run.Snapshot().Sessions[a.HandleID].State != "Closed" {
								t.Error("worker accepted before its session closed")
							}
							if a.State != engine.Succeeded || a.Output == nil || *a.Output != delivered {
								t.Error("worker delivery was not engine committed")
							}
							if tc.name == "m1-redispatch" {
								_, err = planner.work(ctx, models, id)
							}
							if tc.name == "m1-dependencies" || tc.name == "m1-dependencies-incomplete" {
								var second contract.Ref
								second, err = planner.work(ctx, models, "w2")
								if err == nil && !slices.Equal(planner.workerResults, []contract.Ref{delivered, second}) {
									t.Error("dependency results lost delivery order")
								}
							}
						}
						if err == nil && tc.name == "m1-handoff-before" {
							var next *plannerCaller
							next, err = planner.handoff(ctx)
							if err == nil {
								planner = next
							}
						}
						if err == nil {
							var next contract.Ref
							retained := slices.Clone(planner.workerResults)
							next, err = planner.step(ctx)
							if !slices.Equal(planner.workerResults, retained) {
								t.Error("Planner rejection or snapshot changed accepted deliveries")
							}
							if err == nil {
								plannerRef = next
							}
						}
						if err == nil && tc.name == "m1-handoff-after" {
							var next *plannerCaller
							next, err = planner.handoff(ctx)
							if err == nil {
								planner = next
								plannerRef, err = planner.step(ctx)
							}
						}
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
					if err == nil && !m1 && (tc.name == "planner-reuse" || tc.name == "planner-reuse-handoff" || tc.name == "planner-handoff" || tc.name == "planner-history" || tc.name == "planner-support" || tc.name == "planner-wrong-previous" || strings.HasSuffix(tc.name, "-failure") || strings.HasSuffix(tc.name, "-cancel") || strings.HasSuffix(tc.name, "-timeout") || strings.HasSuffix(tc.name, "-cap") || tc.name == "planner-tampered-handoff") {
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
						if m1 {
							if _, stopped := planner.work(ctx, models, "w1"); stopped == nil || !strings.Contains(stopped.Error(), "usable planner") {
								t.Error("failed caller allowed another worker dispatch")
							}
						}
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
					if planning || m2 {
						outputs["planner"] = plannerRef
					}
				}
				if r5 && err == nil {
					outputs["report"] = reportRef
					return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: "report", FileID: ReportFileID}}, nil
				}
				return engine.Result{Outputs: outputs}, err
			}}
			var transport runtime.Runtime = pi
			if tc.name == "attempt-timeout" || tc.name == "resolve-timeout" || tc.name == "refresh-timeout" || tc.name == "update-timeout" || tc.name == "support-resolve-timeout" || tc.name == "planner-timeout" || tc.name == "work-worker-timeout" || tc.name == "work-planner-timeout" || tc.name == "m1-worker-timeout" {
				transport = deadlineRuntime{Runtime: pi}
			}
			if tc.name == "m2-branch-timeout" || m4 || m5 {
				transport = deadlineRuntime{Runtime: pi}
				if tc.name == "m4-mixed-timeout" || tc.name == "m4-mixed-storage-fatal" || tc.name == "m4-mixed-journal-fatal" || tc.name == "m4-mixed-user-cancel" || tc.name == "m4-mixed-wait-fatal" || tc.name == "m4-mixed-three-failed" {
					transport = deadlineRuntime{Runtime: pi, taskTimeouts: map[string]time.Duration{"w1": time.Minute}}
				}
			}
			if strings.HasPrefix(tc.name, "m5-supplement-exhausted-") {
				transport = deadlineRuntime{Runtime: pi, taskTimeouts: map[string]time.Duration{"": time.Minute}}
			}
			runCtx := ctx
			var cancelParent context.CancelCauseFunc
			if tc.name == "m4-planner-parent-deadline" {
				runCtx, cancelParent = context.WithCancelCause(ctx)
				defer cancelParent(nil)
			}
			r, err = engine.New(runCtx, def, engine.Input{Prompt: "CASE-17", LaunchCWD: dir}, engine.Options{BaseDir: dir, Schemas: registry, Runtime: transport})
			if err != nil {
				t.Fatal(err)
			}
			go func() { report = r.Execute(); close(done) }()
			count := 0
			m4Phases := map[string]int{}
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
			barrier := m2Barrier{held: map[string]protocol.Event{}, attempts: map[string]string{}}
			var abortEvent protocol.Event
			var pendingLateAborts []struct {
				event protocol.Event
				err   error
			}
			acceptLateAbort := func(e protocol.Event, err error) bool {
				if tc.name != "m4-mixed-journal-fatal" || e.Message.Type != "abort" || !errors.Is(err, net.ErrClosed) {
					return false
				}
				// EOF can close this control peer before its queued abort is handled.
				// A socket error alone is not proof that the owned run closed cleanly.
				select {
				case <-done:
				default:
					return false
				}
				if ctx.Err() != nil {
					return false
				}
				var failure *engine.Failure
				if !errors.As(report.Failure, &failure) || failure.Code != engine.JournalFailed {
					return false
				}
				owner := report.Snapshot.Sessions[report.Snapshot.Attempts[barrier.attempts["w1"]].HandleID]
				if owner.Identity.SessionID == "" || owner.Identity.SessionID != e.Message.SessionID {
					return false
				}
				for _, c := range report.Cleanup {
					if sameIdentity(c.Identity, owner.Identity) && c.ConfirmsLocalClose(owner.Identity.SessionID) {
						t.Logf("late abort reply after JournalFailed run and owned peer cleanup: %v", err)
						return true
					}
				}
				return false
			}
			var barrierTick <-chan time.Time
			if m2 {
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				barrierTick = ticker.C
			}
		loop:
			for {
				select {
				case <-done:
					break loop
				case <-barrierTick:
					barrier.release(t, r, tc.name, bridge)
				case <-ctx.Done():
					t.Fatal("slice exceeded test deadline")
				case e, ok := <-host.Events():
					if !ok {
						t.Fatal("fixture host events closed")
					}
					if e.Err != nil {
						t.Fatalf("fixture host event: %v", e.Err)
					}
					if e.Message.Type == "hello" {
						if _, ok := hellos[e.Message.SessionID]; ok {
							t.Fatal("session reused")
						}
						workerOpening := false
						if m1 {
							for _, session := range r.Snapshot().Sessions {
								if session.State != "Closed" && (session.Identity.SessionID == e.Message.SessionID || session.Identity.SessionID == "") && strings.HasPrefix(session.Role.Name, "triage-worker-") {
									workerOpening = true
								}
							}
						}
						if m2 {
							snapshot := r.Snapshot()
							openingPlanner := false
							live := 0
							for _, s := range snapshot.Sessions {
								if s.State != "Closed" {
									live++
									if s.Role.Name == "triage-planner" && (s.Identity.SessionID == e.Message.SessionID || s.Identity.SessionID == "") {
										openingPlanner = true
									}
								}
							}
							if openingPlanner {
								for _, s := range snapshot.Sessions {
									if s.State != "Closed" && s.Identity.SessionID != "" && s.Identity.SessionID != e.Message.SessionID {
										t.Fatal("M2 fresh Planner opened before prior sessions closed")
									}
								}
							}
							if live > 4 {
								t.Fatalf("M2 exceeded three workers plus live Planner: %d", live)
							}
							hellos[e.Message.SessionID] = e.Message
							continue
						}
						for _, s := range r.Snapshot().Sessions {
							if s.Identity.SessionID != "" && s.Identity.SessionID != e.Message.SessionID && s.State != "Closed" && !(m1 && workerOpening && s.Role.Name == "triage-planner") {
								t.Fatalf("new stage before old cleanup: %+v", s)
							}
						}
						hellos[e.Message.SessionID] = e.Message
						continue
					}
					if (m3 || m4) && e.Message.Type == "stats" {
						snapshot := r.Snapshot()
						var latest *engine.AttemptState
						for _, a := range snapshot.Attempts {
							if a.Output != nil && a.Output.SchemaID == PlannerSchema && (latest == nil || a.LastSeq > latest.LastSeq) {
								copy := a
								latest = &copy
							}
						}
						if latest == nil || latest.State != engine.Succeeded || snapshot.Sessions[latest.HandleID].Identity.SessionID != e.Message.SessionID {
							t.Fatal("stats did not sample the live owner of an accepted Planner snapshot")
						}
						if slices.Contains(barrier.stats, *latest.Output) {
							t.Fatal("capacity polled twice for the same snapshot")
						}
						barrier.stats = append(barrier.stats, *latest.Output)
						var percent any
						var tokens any
						switch tc.name {
						case "m3-capacity-below", "m3-cycle-six", "m4-allfail-reframe-checkpoint":
							percent, tokens = 79.9, 79900
						case "m4-reframe-timeout-inspection-resume", "m4-reframe-no-inspection", "m4-reframe-stale-inspection", "m4-reframe-completed-inspection":
							percent, tokens = 79.9, 79900
							if len(barrier.stats) == 3 {
								percent, tokens = 80, 80000
							}
						case "m3-capacity-zero":
							percent, tokens = 0, 0
						case "m3-capacity-at", "m3-capacity-at-plan", "m3-stats-cleanup-failure", "m4-capacity-fresh-worker-timeout":
							percent, tokens = 80, 80000
						case "m3-capacity-above":
							percent, tokens = 95, 95000
						case "m3-unknown-tokens":
							tokens = 99000
						}
						body := map[string]any{"sessionId": e.Message.SessionID, "sessionFile": e.Message.History, "contextUsage": map[string]any{"tokens": tokens, "contextWindow": 100000, "percent": percent}}
						ack := protocol.Control{Type: "stats"}
						switch tc.name {
						case "m3-unknown-null":
							body["contextUsage"] = nil
						case "m3-unknown-missing":
							delete(body, "contextUsage")
						case "m3-stats-identity":
							body["sessionId"] = "foreign-session"
						case "m3-stats-history":
							body["sessionFile"] = "foreign-history"
						case "m3-stats-format":
							body["contextUsage"] = map[string]any{"tokens": -1, "percent": nil, "contextWindow": 100000}
						case "m3-stats-window-zero":
							body["contextUsage"].(map[string]any)["contextWindow"] = 0
						case "m3-stats-percent-type":
							body["contextUsage"].(map[string]any)["percent"] = "80"
						case "m3-stats-tokens-type":
							body["contextUsage"].(map[string]any)["tokens"] = "80000"
						case "m3-stats-missing-percent":
							delete(body["contextUsage"].(map[string]any), "percent")
						case "m3-stats-missing-identity":
							delete(body, "sessionId")
						case "m3-stats-reject":
							ack.Type = "stats-reject"
						case "m3-stats-fatal":
							ack.Type = "stats-fatal"
						case "m3-stats-timeout", "m3-stats-cancel":
							ack.Type = "stats-hold"
						case "m3-stats-cleanup-failure":
							path := filepath.Join(bridge, e.Message.SessionID+".json")
							if err := os.Rename(path, path+".recovering"); err != nil {
								t.Fatal(err)
							}
						}
						ack.Data = testJSON(body)
						if err := e.Reply(ack); err != nil {
							t.Fatal(err)
						}
						if tc.name == "m3-stats-cancel" {
							r.Cancel(engine.OriginControllerUser)
						}
						continue
					}
					if m4 && e.Message.Type == "abort" {
						snapshot := r.Snapshot()
						primary := snapshot.Attempts[barrier.attempts["w2"]]
						sibling := snapshot.Sessions[snapshot.Attempts[barrier.attempts["w1"]].HandleID]
						if primary.Failure == nil || primary.Failure.Code != engine.CompactionFailed || sibling.Identity.SessionID != e.Message.SessionID {
							t.Fatal("fault did not occur in sibling after recoverable primary")
						}
						ack := "release-abort"
						if tc.name == "m4-mixed-storage-fatal" || tc.name == "m4-mixed-journal-fatal" {
							path := filepath.Join(r.Dir(), "run.json")
							if tc.name == "m4-mixed-journal-fatal" {
								path = filepath.Join(r.Dir(), "events.jsonl")
							}
							if err := os.Rename(path, path+".before-fault"); err != nil {
								t.Fatal(err)
							}
							if err := os.Mkdir(path, 0700); err != nil {
								t.Fatal(err)
							}
						}
						if tc.name == "m4-mixed-user-cancel" {
							r.Cancel(engine.OriginControllerUser)
						}
						abortEvent = e
						if tc.name == "m4-mixed-wait-fatal" {
							continue
						}
						if err := e.Reply(protocol.Control{Type: ack}); err != nil {
							if tc.name != "m4-mixed-journal-fatal" || !errors.Is(err, net.ErrClosed) {
								t.Fatal(err)
							}
							// Other peers may still need replies before Execute can finish.
							pendingLateAborts = append(pendingLateAborts, struct {
								event protocol.Event
								err   error
							}{e, err})
						}
						continue
					}
					if m2 && e.Message.Type == "held" {
						continue
					}
					if (tc.name == "cancel" || tc.name == "attempt-timeout" || strings.HasSuffix(tc.name, "-cancel") || strings.HasSuffix(tc.name, "-timeout")) && e.Message.Type == "held" {
						continue
					}
					if m5 && e.Message.Type == "held" {
						continue
					}
					if e.Message.Type != "prompt" {
						t.Fatalf("unexpected control %s", e.Message.Type)
					}
					count++
					var req contract.Request
					if err := protocol.ReadJSON(e.Message.RequestPath, &req); err != nil {
						t.Fatal(err)
					}
					var task stageTask
					if err := json.Unmarshal([]byte(req.Prompt), &task); err != nil {
						t.Fatal(err)
					}
					if m4 {
						model := runtime.ModelSpec{Provider: "fixture", ID: "analysis", Thinking: "high"}
						if task.Stage == "intake" || task.Stage == "intake-update" || task.Stage == "intake-revision" {
							model = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: "high"}
						}
						if task.Stage == "planner" {
							model.ID = "planner"
						}
						if req.Output.SchemaID == WorkerSchema {
							var request workerRequest
							if err := json.Unmarshal([]byte(req.Prompt), &request); err != nil {
								t.Fatal(err)
							}
							if request.Task.Responsibility != "analysis" {
								model = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: "high"}
							}
						}
						m4Phases[task.Stage]++
						snapshot := r.Snapshot()
						session := snapshot.Sessions[snapshot.Attempts[req.Identity.AttemptID].HandleID]
						if session.Role.Name != "triage-"+task.Stage || session.Role.Model != model {
							t.Fatalf("M4 %s invocation %d model/role=%+v, want %+v", task.Stage, m4Phases[task.Stage], session.Role, model)
						}
						if strings.HasPrefix(tc.name, "m4-support-") && task.Stage != "intake" {
							if requests.Load() == 0 || requests.Load() != acquiredRequests {
								t.Fatal("support continuation reacquired Jira or skipped initial HTTP acquisition")
							}
						}
					}
					if req.Output.SchemaID == ContextSchema && !strings.Contains(task.Requirements, supportingResolutionRequirements) {
						t.Fatalf("supporting requirements missing from %s task", task.Stage)
					}
					if m5 {
						m4Phases[task.Stage]++
					}
					pureVerification := m5 && (req.Output.SchemaID == ReportSchema || req.Output.SchemaID == ClaimSchema || req.Output.SchemaID == VerificationSchema || task.Stage == "m5-supplement-publication")
					if !pureVerification && !reflect.DeepEqual(task.Scope, scope) {
						t.Fatal("scope not in prompt")
					}
					if working && !transitionProbe && count > 4 && task.Stage != "planner" && req.Output.SchemaID != WorkerSchema {
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
					if m2 && count > 3 {
						if task.Stage == "m5-supplement-publication" {
							var payload struct {
								Data json.RawMessage `json:"data"`
							}
							if err := json.Unmarshal([]byte(req.Prompt), &payload); err != nil {
								t.Fatal(err)
							}
							data = payload.Data
						} else {
							switch req.Output.SchemaID {
							case ReportSchema:
								var reportTask reportTask
								if err := json.Unmarshal([]byte(req.Prompt), &reportTask); err != nil {
									t.Fatal(err)
								}
								var state publication[PlannerState]
								if err := protocol.ReadJSON(reportTask.State.Path, &state); err != nil {
									t.Fatal(err)
								}
								selected := append([]ReportClaim{}, reportTask.Assessments...)
								data = InvestigationReport{State: reportTask.State, Context: reportTask.Context, Claims: selected, Completeness: "incomplete", Closure: "Missing runtime evidence, not disproof", Gaps: append([]string{}, state.Data.Gaps...), NextSteps: []string{"Obtain authorized runtime measurements"}, ReportFile: ReportFileID}
							case ClaimSchema:
								var claimTask struct {
									Projection PureClaim `json:"projection"`
								}
								if err := json.Unmarshal([]byte(req.Prompt), &claimTask); err != nil {
									t.Fatal(err)
								}
								projection := claimTask.Projection
								snapshot := r.Snapshot()
								owner := snapshot.Sessions[snapshot.Attempts[req.Identity.AttemptID].HandleID]
								parent := snapshot.Sessions[snapshot.Attempts[projection.ParentState.AttemptID].HandleID]
								if owner.Identity != parent.Identity && tc.name != "m5-claim-timeout-retry" && tc.name != "m5-claim-retry-exhausted" || owner.Role.Model != (runtime.ModelSpec{Provider: "fixture", ID: "planner", Thinking: "high"}) || owner.Role.Name != "triage-planner" || !slices.Contains(req.Inputs, projection.ParentState) || !slices.Contains(req.Inputs, projection.Context) {
									t.Fatal("claim lost proposing Planner/session/exact inputs")
								}
								switch tc.name {
								case "m5-claim-projection":
									projection.Candidate.Statement = "A different candidate"
								case "m5-claim-parent":
									projection.ParentState = projection.Context
								case "m5-claim-context":
									projection.Context = projection.ParentState
								}
								data = projection
								if tc.name == "m5-claim-ledger" {
									var raw map[string]any
									if err := json.Unmarshal(testJSON(data), &raw); err != nil {
										t.Fatal(err)
									}
									raw["ledger"] = map[string]any{}
									data = raw
								}
							case VerificationSchema:
								var verificationTask struct {
									Role            string       `json:"role"`
									Claim           contract.Ref `json:"claim"`
									AllowedEvidence []Evidence   `json:"allowed_evidence"`
								}
								if err := json.Unmarshal([]byte(req.Prompt), &verificationTask); err != nil {
									t.Fatal(err)
								}
								var claim publication[PureClaim]
								if err := protocol.ReadJSON(verificationTask.Claim.Path, &claim); err != nil {
									t.Fatal(err)
								}
								wantInputs := []contract.Ref{verificationTask.Claim}
								for _, evidence := range claim.Data.Candidate.AllowedEvidence {
									if !slices.Contains(wantInputs, *evidence.Ref) {
										wantInputs = append(wantInputs, *evidence.Ref)
									}
								}
								if !slices.Equal(req.Inputs, wantInputs) || !reflect.DeepEqual(verificationTask.AllowedEvidence, claim.Data.Candidate.AllowedEvidence) {
									t.Fatal("fresh verifier received other Planner inputs or wrong evidence version")
								}
								var prompt map[string]json.RawMessage
								if err := json.Unmarshal([]byte(req.Prompt), &prompt); err != nil {
									t.Fatal(err)
								}
								if len(prompt) != 5 || req.Feedback != nil {
									t.Fatal("verifier prompt contains extra Planner state or retry feedback")
								}
								for _, key := range []string{"stage", "role", "claim", "allowed_evidence", "requirements"} {
									if _, ok := prompt[key]; !ok {
										t.Fatalf("missing verifier input %s", key)
									}
								}
								data = VerificationResult{Claim: verificationTask.Claim, Role: verificationTask.Role, AllowedEvidence: verificationTask.AllowedEvidence, Assessment: VerificationAssessment{Support: map[string]string{"pro": "supports inference", "con": "counterexample unresolved", "cross": "measurement incomplete"}[verificationTask.Role], Reason: "Candidate fits supplied observation but runtime is missing", Basis: verificationTask.AllowedEvidence, RuntimeBasis: []Evidence{}, Measurement: "unavailable", Window: "unavailable", Filter: "unavailable", Environment: "unavailable", Release: "unavailable", Counterexamples: []VerificationIssue{}, Gaps: []string{"Runtime observation remains missing"}}}
								if verificationTask.Role == "con" {
									v := data.(VerificationResult)
									switch tc.name {
									case "m5-verifier-role":
										v.Role = "pro"
									case "m5-verifier-claim":
										v.Claim = claim.Data.ParentState
									case "m5-verifier-evidence":
										v.AllowedEvidence = []Evidence{}
									case "m5-verifier-unauthorized-basis":
										v.Assessment.Basis = []Evidence{{Ref: &claim.Data.ParentState, FileID: "issue"}}
									case "m5-verifier-schema":
										v.Role = "planner"
									}
									data = v
								}
								if tc.name == "m5-new-version-cross-reject" && verificationTask.Role == "cross" && m4Phases[task.Stage] == 2 {
									var parent publication[PlannerState]
									if err := protocol.ReadJSON(claim.Data.ParentState.Path, &parent); err != nil {
										t.Fatal(err)
									}
									v := data.(VerificationResult)
									v.Claim = parent.Data.Verification.Claims[0]
									data = v
								}
								if strings.HasPrefix(tc.name, "m5-supplement-exhausted-") {
									fatal := "cross"
									if strings.HasSuffix(tc.name, "fatal-pro") {
										fatal = "pro"
									}
									if verificationTask.Role == fatal {
										barrier.held[fatal], barrier.attempts[fatal] = e, req.Identity.AttemptID
									}
								}
								if tc.name == "m5-reverse-completion" || strings.HasPrefix(tc.name, "m5-partial-") {
									barrier.held[verificationTask.Role], barrier.attempts[verificationTask.Role] = e, req.Identity.AttemptID
								}
							case IntakeSchema:
								if !strings.HasPrefix(tc.name, "m4-support-update-") || task.Stage != "intake-update" || m4Phases[task.Stage] != 1 {
									t.Fatal("unexpected or replayed M4 intake update")
								}
								var fetched int32
								intake, files, fetched = refreshIntakeFixture(t, ctx, "update-replace", count, req.Inputs, task, revisionURL)
								if fetched != 3 || newRequests.Load() != fetched {
									t.Fatal("update did not fetch issue/link/attachment exactly once")
								}
								data = intake
							case PlannerSchema:
								plannerSteps++
								expectedPlanner = m2PlannerFixture(t, tc.name, req, task, plannerSteps)
								data = m2PlannerData(t, expectedPlanner)
								if tc.name == "m4-allfail-caller-bool" && plannerSteps == 2 {
									data.(map[string]any)["remote_job_safe"] = true
								}
							case WikiSchema:
								if (m3 || m4) && task.SupportingProposal != nil {
									wiki, files = wikiFixture("complete", req.Inputs[0])
									data = wiki
								} else {
									data, files = m2WikiFixture(t, tc.name, req)
								}
							case WorkerSchema:
								var request workerRequest
								if err := json.Unmarshal([]byte(req.Prompt), &request); err != nil {
									t.Fatal(err)
								}
								m2AssertWorkerInputs(t, r, req, request, result, tc.name)
								fixtureName := tc.name
								if request.Task.SourceKind == "logs" {
									fixtureName = "m1-logs"
								}
								if tc.name == "m2-query-utc" {
									fixtureName = "m1-query-utc"
								}
								if tc.name == "m2-incomplete-no-gap" {
									fixtureName = "m1-incomplete-no-gap"
								}
								v, raw := workerFixture(fixtureName, request, req.Inputs)
								if m4 && request.Task.ID == "inspection" {
									raw["worker-raw"] = testJSON(map[string]any{"job_id": "anonymous-read", "status": "completed", "resubmitted": false, "safe_next_phase": "read the already available result"})
								}
								if tc.name == "m4-mixed-binding-fatal" && request.Task.ID == "w3" {
									v.TaskID = "not-dispatched"
								}
								if tc.name == "m2-files-not-progress" {
									raw["additional-raw"] = []byte("another anonymous document")
									v.Evidence = append(v.Evidence, Evidence{FileID: "additional-raw"})
								}
								if request.Task.ID == "w2" {
									if tc.name == "m2-branch-binding" {
										v.TaskID = "not-dispatched"
									}
									if tc.name == "m2-branch-schema" {
										v.Status = "invented-status"
									}
								}
								data, files = v, raw
								if (slices.Contains([]string{"m2-parallel-batches-yield", "m2-batch-consumed-order", "m2-batch-results-order", "m2-batch-foreign-ref"}, tc.name) || strings.HasPrefix(tc.name, "m2-branch-") || strings.HasPrefix(tc.name, "m4-mixed-")) && slices.Contains([]string{"w1", "w2", "w3"}, request.Task.ID) {
									barrier.held[request.Task.ID], barrier.attempts[request.Task.ID] = e, req.Identity.AttemptID
								}
							case ContextSchema:
								if (!m3 && !m4 && tc.name != "m2-wiki-support") || task.SupportingProposal == nil {
									t.Fatal("unexpected M2 supporting task")
								}
								if !m3 && !m4 {
									m2AssertWikiOwners(t, req.Inputs)
								}
								var prior publication[Context]
								if err := protocol.ReadJSON(req.Inputs[2].Path, &prior); err != nil {
									t.Fatal(err)
								}
								resolved, raw := resolutionFixture("resolve-time", "time-unresolved", scope, req.Inputs, intake, wiki, prior.Data)
								if strings.HasPrefix(tc.name, "m4-support-update-") {
									for n := len(prior.Data.Attempts); n < len(resolved.Attempts); n++ {
										if resolved.Attempts[n].Kind == "time" {
											resolved.Attempts[n].Evidence = []Evidence{{FileID: "remediation"}}
										}
									}
								}
								data, files = resolved, raw
							default:
								t.Fatal("unexpected M2 schema")
							}
						}
					} else if req.Output.SchemaID == WorkerSchema {
						var request workerRequest
						if err := json.Unmarshal([]byte(req.Prompt), &request); err != nil {
							t.Fatal(err)
						}
						if !slices.Contains(req.Inputs, request.Proposal) || !slices.Contains(req.Inputs, request.Context) {
							t.Fatal("worker lost proposal/context Inputs")
						}
						if request.Task.ID == "w2" {
							if len(request.Dependencies) != 1 || !slices.Contains(req.Inputs, request.Dependencies[0]) {
								t.Fatal("task IDs not resolved to exact dependency Inputs")
							}
							var dependency publication[WorkerResult]
							if err := protocol.ReadJSON(request.Dependencies[0].Path, &dependency); err != nil || dependency.Data.TaskID != "w1" {
								t.Fatal("wrong dependency result", err)
							}
						}
						model := r.Snapshot().Sessions[r.Snapshot().Attempts[req.Identity.AttemptID].HandleID].Role.Model
						wantModel := runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: "high"}
						if request.Task.Responsibility == "analysis" {
							wantModel = runtime.ModelSpec{Provider: "fixture", ID: "analysis", Thinking: "high"}
						}
						if model != wantModel {
							t.Fatal("worker responsibility/model binding changed")
						}
						if request.Requirements != workerRequirements {
							t.Fatal("worker lost task requirements")
						}
						data, files = workerFixture(tc.name, request, req.Inputs)
					} else {
						switch count {
						case 1:
							if task.Stage != "intake" || len(req.Inputs) != 0 {
								t.Fatal("intake inputs")
							}
							if acquiring {
								intake, acquiredFiles, err = acquireIntake(ctx, filepath.Dir(e.Message.CandidatePath), scope, acquisition)
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
								if m1 {
									expectedPlanner = workerPlannerFixture(t, tc.name, req, expectedPlanner, plannerSteps)
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
					}
					if count > 3 && !pureVerification {
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
						writeEnvelope(t, e.Message, req, data, acquiredFiles)
					} else {
						writeCandidate(t, e.Message, req, data, files, tc.name == "file-escape")
					}
					if r5 && req.Output.SchemaID == ReportSchema {
						if !r5RenderCandidate(t, ctx, r, tc.name, e.Message, req) {
							if err := e.Reply(protocol.Control{Type: "compaction-error"}); err != nil {
								t.Fatal(err)
							}
							continue loop
						}
					}
					if m2 && (req.Output.SchemaID == WorkerSchema || m5 && req.Output.SchemaID == VerificationSchema) {
						for _, attemptID := range barrier.attempts {
							if attemptID == req.Identity.AttemptID {
								barrier.release(t, r, tc.name, bridge)
								continue loop
							}
						}
					}
					ack := "settle"
					if m5 {
						if strings.HasPrefix(tc.name, "m5-supplement-exhausted-") {
							exhausted := "pro"
							if strings.HasSuffix(tc.name, "fatal-pro") {
								exhausted = "cross"
							}
							if task.Stage == "verify-"+exhausted {
								ack = "compaction-error"
							}
						}
						if strings.HasPrefix(tc.name, "m5-supplement-reframe-") && task.Stage == "verify-con" && m4Phases[task.Stage] == 1 {
							ack = "compaction-error"
						}
						if tc.name == "m5-supplement-reframe-inspection-resume" && task.Stage == "wiki-investigation" && m4Phases[task.Stage] == 1 {
							ack = "hold"
						}
						if task.Stage == "verify-con" {
							n := m4Phases[task.Stage]
							if tc.name == "m5-verifier-timeout-retry" && n == 1 || (tc.name == "m5-verifier-unavailable" || tc.name == "m5-same-version-missing-only") && n <= 2 {
								ack = "hold"
							}
							if tc.name == "m5-verifier-compaction-retry" && n == 1 {
								ack = "compaction-error"
							}
						}
						if (tc.name == "m5-claim-timeout-retry" && m4Phases[task.Stage] == 1 || tc.name == "m5-claim-retry-exhausted") && task.Stage == "planner-claim" {
							ack = "hold"
						}
						if tc.name == "m5-planner-feedback-timeout" && task.Stage == "planner" && m4Phases[task.Stage] == 2 {
							ack = "hold"
						}
					}
					if m4 {
						if strings.HasPrefix(tc.name, "m4-meta-") && task.Stage == "planner" && plannerSteps == 1 || strings.HasPrefix(tc.name, "m4-allfail-") && req.Output.SchemaID == WorkerSchema || (strings.HasPrefix(tc.name, "m4-support-unsafe-") && task.Stage == "wiki-resolution" || tc.name == "m4-wiki-timeout-partial-resume" && task.Stage == "wiki-investigation") && m4Phases[task.Stage] == 1 {
							ack = "hold"
						}
						switch tc.name {
						case "m4-reframe-timeout-inspection-resume", "m4-reframe-no-inspection", "m4-reframe-stale-inspection", "m4-reframe-completed-inspection":
							if req.Output.SchemaID == WorkerSchema && m4Phases[task.Stage] <= 2 || task.Stage == "wiki-investigation" && m4Phases[task.Stage] == 1 {
								ack = "hold"
							}
						case "m4-support-update-wiki-timeout":
							if task.Stage == "wiki-revision" && m4Phases[task.Stage] == 1 {
								ack = "hold"
							}
						case "m4-support-update-context-timeout":
							if task.Stage == "context-revision" && m4Phases[task.Stage] == 1 {
								ack = "hold"
							}
						case "m4-support-resolve-wiki-timeout":
							if task.Stage == "wiki-resolution" && m4Phases[task.Stage] == 1 {
								ack = "hold"
							}
						case "m4-support-resolve-context-timeout":
							if task.Stage == "context-resolution" && m4Phases[task.Stage] == 1 {
								ack = "hold"
							}
						case "m4-nil-recovery-timeout", "m4-planner-later-user-cancel", "m4-planner-parent-deadline", "m4-planner-run-limit", "m4-planner-storage-fatal", "m4-planner-journal-fatal":
							if task.Stage == "planner" && plannerSteps == 1 {
								ack = "hold"
							} else if task.Stage == "planner" && plannerSteps == 2 {
								switch tc.name {
								case "m4-planner-later-user-cancel":
									ack = "hold"
									r.Cancel(engine.OriginControllerUser)
								case "m4-planner-parent-deadline":
									ack = "hold"
									cancelParent(context.DeadlineExceeded)
								case "m4-planner-storage-fatal", "m4-planner-journal-fatal":
									path := filepath.Join(r.Dir(), "run.json")
									if tc.name == "m4-planner-journal-fatal" {
										path = filepath.Join(r.Dir(), "events.jsonl")
									}
									if err := os.Rename(path, path+".before-fault"); err != nil {
										t.Fatal(err)
									}
									if err := os.Mkdir(path, 0700); err != nil {
										t.Fatal(err)
									}
								}
							}
						case "m4-planner-first-compaction":
							if task.Stage == "planner" && plannerSteps == 1 {
								ack = "compaction-error"
							}
						case "m4-planner-first-timeout":
							if task.Stage == "planner" && plannerSteps == 1 {
								ack = "hold"
							}
						case "m4-planner-retry-exhausted":
							if task.Stage == "planner" {
								ack = "hold"
							}
						case "m4-planner-unknown-provider":
							if task.Stage == "planner" {
								ack = "provider-error"
							}
						case "m4-planner-user-cancel":
							if task.Stage == "planner" {
								ack = "hold"
								r.Cancel(engine.OriginControllerUser)
							}
						case "m4-worker-timeout", "m4-capacity-fresh-worker-timeout":
							if req.Output.SchemaID == WorkerSchema {
								ack = "hold"
							}
						}
					}
					if tc.name == "m2-planner-provider-failure" && plannerSteps == 2 {
						ack = "provider-error"
					}
					if m1 && req.Output.SchemaID == WorkerSchema && tc.name == "m1-worker-provider-failure" {
						ack = "provider-error"
					}
					if m1 && req.Output.SchemaID == WorkerSchema {
						switch tc.name {
						case "m1-worker-timeout":
							ack = "hold"
						case "m1-worker-cancel":
							ack = "hold"
							r.Cancel(engine.OriginControllerUser)
						}
					}
					if m1 && task.Stage == "planner" && plannerSteps == 2 && tc.name == "m1-planner-provider-failure" {
						ack = "provider-error"
					}
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
					if tc.name == "work-worker-cleanup-failure" || tc.name == "m1-worker-cleanup-failure" {
						cleanupAt = 5
					}
					if tc.name == "cleanup-failure" || r5 && tc.name == "r5-cleanup-failure" && req.Output.SchemaID == ReportSchema || ((resolving || revising || planning) && strings.HasSuffix(tc.name, "-cleanup-failure") && count == cleanupAt) {
						for sid := range hellos {
							if (m1 || r5) && sid != r.Snapshot().Sessions[r.Snapshot().Attempts[req.Identity.AttemptID].HandleID].Identity.SessionID {
								continue
							}
							path := filepath.Join(bridge, sid+".json")
							if _, err := os.Stat(path); os.IsNotExist(err) {
								continue
							}
							if err := os.Rename(path, path+".recovering"); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := e.Reply(protocol.Control{Type: ack}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if lateAbort {
				if abortEvent.Message.Type != "abort" {
					t.Fatal("journal fault did not reach the original abort barrier")
				}
				// Join the real Host after Execute so the late write is deterministic.
				if err := host.Close(); err != nil {
					t.Fatal(err)
				}
				err := abortEvent.Reply(protocol.Control{Type: "release-abort"})
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("completed journal-fatal run did not accept its closed-peer late abort: %v", err)
				}
				pendingLateAborts = append(pendingLateAborts, struct {
					event protocol.Event
					err   error
				}{abortEvent, err})
			}
			for _, pending := range pendingLateAborts {
				if !acceptLateAbort(pending.event, pending.err) {
					t.Fatalf("completed journal-fatal run did not accept its closed-peer late abort: %v", pending.err)
				}
			}
			if tc.name == "m4-reframe-timeout-inspection-resume" && report.ExitCode == 0 {
				if m4Phases["intake"] != 1 || m4Phases["wiki"] != 1 || m4Phases["context"] != 1 || m4Phases["wiki-investigation"] != 2 || plannerSteps != 6 {
					t.Fatalf("reframe recovery repeated acquisition or skipped continuation: phases=%v", m4Phases)
				}
			}
			limitBeforePrompt := tc.name == "m5-verifier-attempt-limit" || tc.name == "m5-verifier-live-limit"
			if count != tc.stages && !limitBeforePrompt || (report.ExitCode != 0) != tc.failure {
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
			if report.Final != nil && !r5 {
				t.Fatal("slice invented final report")
			}
			if strings.HasPrefix(tc.name, "m4-support-") && !tc.failure {
				wantWiki, wantContext := 2, 1
				if strings.Contains(tc.name, "context-timeout") {
					wantWiki, wantContext = 1, 2
				}
				wikiPhase, contextPhase := "wiki-resolution", "context-resolution"
				if strings.HasPrefix(tc.name, "m4-support-update-") {
					wikiPhase, contextPhase = "wiki-revision", "context-revision"
					if m4Phases["intake-update"] != 1 || newRequests.Load() != 3 {
						t.Fatal("support update resume repeated Jira acquisition")
					}
				}
				if m4Phases["intake"] != 1 || m4Phases[wikiPhase] != wantWiki || m4Phases[contextPhase] != wantContext || requests.Load() == 0 || requests.Load() != acquiredRequests {
					t.Fatalf("support repeated acquisition/completed phases: phases=%v HTTP=%d initial=%d", m4Phases, requests.Load(), acquiredRequests)
				}
			}
			if m4 && !tc.failure {
				journal, err := os.Open(filepath.Join(r.Dir(), "events.jsonl"))
				if err != nil {
					t.Fatal(err)
				}
				decoder := json.NewDecoder(journal)
				decisions := map[contract.Ref]uint64{}
				for {
					var event struct {
						Kind    string
						Seq     uint64
						Details struct {
							Reason string
							Refs   []contract.Ref
						}
					}
					if err := decoder.Decode(&event); err != nil {
						if err != io.EOF {
							t.Error(err)
						}
						break
					}
					if event.Kind == "Decision" && strings.HasPrefix(event.Details.Reason, "Worker delivery accepted for Planner interpretation") {
						for _, ref := range event.Details.Refs {
							if ref.SchemaID == WorkerSchema {
								decisions[ref] = event.Seq
							}
						}
					}
				}
				if err := journal.Close(); err != nil {
					t.Fatal(err)
				}
				for _, ref := range expectedPlanner.WorkerResults {
					attempt := report.Snapshot.Attempts[ref.AttemptID]
					if attempt.State != engine.Succeeded || attempt.Output == nil || *attempt.Output != ref || decisions[ref] <= attempt.LastSeq || decisions[ref] >= report.Snapshot.Attempts[plannerRef.AttemptID].LastSeq {
						t.Fatalf("worker %s was delivered without committed output then acceptance Decision before Planner", ref.AttemptID)
					}
				}
			}
			if r5 {
				r5AssertOutcome(t, tc, report, plannerRef, reportRef, count)
				return
			}
			if m2 {
				m2AssertOutcome(t, tc, report, plannerRef, result, expectedPlanner, barrier, count, len(hellos))
				return
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
					if tc.name == "m1-support" && !slices.ContainsFunc(state.Data.Hypotheses[0].Evidence, func(e Evidence) bool {
						return e.Ref != nil && slices.Contains(state.Data.WorkerResults, *e.Ref) && e.FileID == "worker-raw"
					}) {
						t.Fatal("supporting handoff lost worker evidence alongside historical intake evidence")
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
			if m1 {
				wantSessions := tc.stages - (plannerSteps - 1)
				if tc.name == "m1-handoff-before" || tc.name == "m1-handoff-after" || tc.name == "m1-support" {
					wantSessions++
				}
				if len(report.Snapshot.Sessions) != wantSessions {
					t.Fatal("M1 failed/successful session accounting changed")
				}
				if len(report.Snapshot.Attempts) != tc.stages {
					t.Fatal("M1 attempt accounting changed")
				}
				if tc.failure {
					var failure *engine.Failure
					want := engine.WorkflowFailed
					if tc.name == "m1-schema" {
						want = engine.ContractInvalid
					}
					if tc.name == "m1-worker-provider-failure" || tc.name == "m1-planner-provider-failure" {
						want = engine.ProviderFailed
					}
					if tc.name == "m1-worker-timeout" {
						want = engine.TimedOut
					}
					if tc.name == "m1-worker-cancel" {
						want = engine.Cancelled
					}
					if tc.name == "m1-worker-cleanup-failure" {
						want = engine.CleanupFailed
					}
					if want == engine.WorkflowFailed {
						wantText := map[string]string{
							"m1-incomplete-no-gap": "complete/incomplete delivery with gaps", "m1-planner-invent-committed": "distinct workflow-accepted refs", "m1-query-utc": "UTC",
							"m1-pending": "explicit proposed task ID", "m1-unsatisfied": "has no accepted result", "m1-redispatch": "task ID already completed",
							"m1-reuse-id": "unique, uncompleted IDs", "m1-planner-drop": "worker_results differ from accepted deliveries", "m1-planner-invent": "distinct workflow-accepted refs",
							"m1-task-id": "proposal/context/task/inputs mismatch", "m1-proposal": "proposal/context/task/inputs mismatch", "m1-context": "proposal/context/task/inputs mismatch", "m1-inputs": "proposal/context/task/inputs mismatch",
							"m1-owner": "not an exact committed input", "m1-file": "unknown worker evidence file", "m1-evidence-analysis": "evidence-only worker cannot supply analysis",
							"m1-task-utc": "nonzero UTC window", "m1-task-owner": "exact input owner/file", "m1-task-duplicate": "unique, uncompleted IDs",
						}[tc.name]
						var contractErr *contract.Error
						if wantText == "" || !strings.Contains(report.Failure.Error(), wantText) || errors.As(report.Failure, &failure) || errors.As(report.Failure, &contractErr) {
							t.Fatalf("M1 semantic rejection changed: %v", report.Failure)
						}
					} else if !errors.As(report.Failure, &failure) || failure.Code != want {
						t.Fatalf("M1 failure classification: want %s got %v", want, report.Failure)
					}
					if tc.name == "m1-worker-timeout" && failure != nil && failure.Origin != engine.OriginAttemptDeadline {
						t.Fatal("worker timeout lost deadline origin")
					}
					if tc.name == "m1-worker-provider-failure" || tc.name == "m1-worker-timeout" || tc.name == "m1-worker-cancel" || tc.name == "m1-schema" || tc.name == "m1-planner-provider-failure" {
						failed := 0
						for _, attempt := range report.Snapshot.Attempts {
							if attempt.State != engine.Succeeded {
								failed++
								if attempt.Output != nil {
									t.Fatal("failed Step candidate became committed")
								}
							}
						}
						if failed != 1 {
							t.Fatal("execution failure was not retained as one failed attempt")
						}
					}
				} else {
					var accepted publication[PlannerState]
					if err := protocol.ReadJSON(plannerRef.Path, &accepted); err != nil || !reflect.DeepEqual(accepted.Data, expectedPlanner) {
						t.Fatal("M1 Planner did not retain result state", err)
					}
				}
			}
			if planning && !working && !m1 {
				if err := assertPlannerOutcome(report, tc.name, tc.failure, plannerRef, firstPlannerRef, expectedPlanner); err != nil {
					t.Fatal(err)
				}
				if tc.name == "planner-support" && supportingRequests.Load() != 3 {
					t.Fatal("planner reacquired supporting sources")
				}
			}
			if !tc.failure {
				sessions := tc.stages
				if m1 && tc.name != "m1-handoff-before" {
					sessions--
				}
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
