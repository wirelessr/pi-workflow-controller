package triage

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

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
	case "wiki-history-identity":
		v.Status = "completed-with-matches"
		v.Pages = []Source{available("historical-identity")}
		files["historical-identity"] = testJSON(IdentityLookup{Stack: "test-stack", Pop: "test-pop", Binding: "local:test-cluster", Release: "release-example", Matches: []TenantIdentity{{TenantID: "17", OrgKey: "org-example"}}})
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
	case "wiki-history-identity":
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

func TestIntakeToContext(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stages  int
		failure bool
		ready   bool
	}{
		{"missing-attachment-size", 1, true, false},
		{"blank-stack", 3, true, false}, {"blank-pop", 3, true, false}, {"blank-binding", 3, true, false}, {"blank-target", 3, true, false}, {"whitespace-target", 3, true, false},
		{"duplicate-attachment", 1, true, false}, {"conflicting-attachment", 1, true, false}, {"duplicate-empty-attachment", 1, true, false}, {"null-comment", 1, true, false}, {"http-page-null", 3, false, false},
		{"http-complete", 3, false, true}, {"http-page-failure", 3, false, false}, {"http-page-total", 3, false, false}, {"http-page-empty", 3, false, false}, {"http-page-offset", 3, false, false}, {"http-page-duplicate", 3, false, false}, {"http-page-short", 3, false, false}, {"http-page-partial", 3, false, false}, {"http-malformed-issue", 3, false, false}, {"http-malformed-fields", 3, false, false}, {"http-linked-failure", 3, false, false}, {"http-attachment-partial", 3, false, false}, {"http-unsafe-zip", 3, false, false}, {"http-oversized", 3, false, false}, {"http-metadata-limit", 3, false, false},
		{"resolve-ready", 3, false, true}, {"resolve-acquisition-gap", 5, false, false},
		{"resolve-wiki", 5, false, true}, {"resolve-wiki-unavailable", 5, false, true}, {"resolve-wiki-not-run", 5, false, true}, {"resolve-wiki-partial-again", 5, false, false}, {"resolve-repeat", 7, false, true}, {"resolve-time", 4, false, true}, {"resolve-identity", 4, false, true}, {"resolve-ticket-only", 4, false, false},
		{"resolve-drop-gap", 5, true, false}, {"resolve-foreign-evidence", 5, true, false}, {"resolve-replace-valid-time", 5, true, false}, {"resolve-wrong-previous", 5, true, false}, {"resolve-old-evidence", 5, true, false}, {"resolve-dropped-history", 5, true, false},
		{"resolve-provider-failure", 4, true, false}, {"resolve-cancel", 4, true, false}, {"resolve-timeout", 4, true, false}, {"resolve-cleanup-failure", 4, true, false}, {"resolve-attempt-cap", 3, true, false}, {"resolve-uncommitted-input", 3, true, false},
		{"wiki-history-identity", 3, true, false}, {"conflicting-foreign-lookup", 3, true, false},
		{"ticket-only", 3, false, false}, {"wiki-not-run", 3, false, false},
		{"false-ready", 3, true, false}, {"dropped-gap", 3, true, false}, {"self-paired", 3, true, false}, {"lookup-conflict", 3, true, false}, {"lookup-environment", 3, true, false}, {"lookup-release", 3, true, false}, {"missing-lookup", 3, true, false},
		{"complete", 3, false, true}, {"wiki-matches", 3, false, true}, {"epoch-millis", 3, false, true}, {"local-paired", 3, false, true}, {"dst-offsets", 3, false, true},
		{"missing-page", 3, false, false}, {"changed-total", 3, false, false}, {"missing-fields", 3, false, false}, {"unsafe-attachment", 3, false, false}, {"oversized-attachment", 3, false, false}, {"vision-pending", 3, false, false}, {"wiki-partial", 3, false, false}, {"wiki-unavailable", 3, false, false}, {"identity-conflict", 3, false, false}, {"time-unresolved", 3, false, false}, {"time-conflict", 3, false, false},
		{"false-complete", 1, true, false}, {"duplicate-comment", 1, true, false}, {"wrong-page-offset", 1, true, false}, {"missing-linked", 1, true, false}, {"missing-attachment", 1, true, false}, {"truncated-attachment", 1, true, false}, {"raw-key", 1, true, false}, {"unknown-file", 1, true, false}, {"file-escape", 1, true, false},
		{"wiki-false-empty", 2, true, false}, {"wiki-ref", 2, true, false},
		{"wrong-scope", 3, true, false}, {"wrong-environment", 3, true, false}, {"wrong-tenant", 3, true, false}, {"no-release", 3, true, false}, {"guessed-zone", 3, true, false}, {"wrong-utc", 3, true, false}, {"wrong-window", 3, true, false}, {"foreign-ref", 3, true, false}, {"uncommitted-ref", 3, true, false}, {"missing-resolution", 3, true, false}, {"no-evidence", 3, true, false}, {"local-no-pair", 3, true, false}, {"wrong-offset", 3, true, false},
		{"attempt-timeout", 1, true, false}, {"provider-failure", 1, true, false}, {"cancel", 1, true, false}, {"cleanup-failure", 1, true, false}, {"attempt-cap", 1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolving := strings.HasPrefix(tc.name, "resolve-")
			acquiring := resolving || strings.HasPrefix(tc.name, "http-")
			mode := tc.name
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
			var requests atomic.Int32
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
				if tc.name == "resolve-acquisition-gap" {
					httpMode = "page-failure"
				}
				server := acquireFixture(t, httpMode, bundle, mime, func(*http.Request) { requests.Add(1) })
				acquisition = acquisitionOptions{BaseURL: server.URL}
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
			if tc.name == "resolve-attempt-cap" {
				policy.MaxTotalAttempts = 3
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
				if err == nil && resolving {
					beforeResolution = result
					if tc.name == "resolve-uncommitted-input" {
						result.Context.Path = filepath.Join(filepath.Dir(result.Context.Path), "candidate.json")
					}
					result, err = resolveSlice(ctx, run, scope, models, result)
					if err == nil && tc.name == "resolve-repeat" {
						result, err = resolveSlice(ctx, run, scope, models, result)
					}
				}
				outputs := map[string]contract.Ref{}
				// No final selection: these are supporting refs, not a report.
				if err == nil {
					outputs = map[string]contract.Ref{"intake": result.Intake, "wiki": result.Wiki, "context": result.Context}
				}
				return engine.Result{Outputs: outputs}, err
			}}
			var transport runtime.Runtime = pi
			if tc.name == "attempt-timeout" || tc.name == "resolve-timeout" {
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
					if (tc.name == "cancel" || tc.name == "attempt-timeout" || tc.name == "resolve-cancel" || tc.name == "resolve-timeout") && e.m.Type == "held" {
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
					if !reflect.DeepEqual(task.Scope, scope) {
						t.Fatal("scope not in prompt")
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
						data = expectedContext
					default:
						if !resolving || req.Inputs[0] != initialIntake || task.Previous == nil || requests.Load() != acquiredRequests {
							t.Fatal("resolution reacquired intake or lost committed input")
						}
						if task.Stage == "wiki-resolution" {
							if len(req.Inputs) != 3 || *task.Previous != req.Inputs[2] || req.Inputs[1].SchemaID != WikiSchema {
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
							data = expectedContext
						} else {
							t.Fatal("unexpected extra step")
						}
					}
					for _, ref := range req.Inputs {
						a := r.Snapshot().Attempts[ref.AttemptID]
						if a.Output == nil || *a.Output != ref || a.State != engine.Succeeded {
							t.Fatal("noncommitted input")
						}
					}
					if acquiring && count == 1 {
						writeEnvelope(t, e.m, req, data, acquiredFiles)
					} else {
						writeCandidate(t, e.m, req, data, files, tc.name == "file-escape")
					}
					ack := "settle"
					if tc.name == "provider-failure" || (tc.name == "resolve-provider-failure" && count == 4) {
						ack = "provider-error"
					}
					if tc.name == "attempt-timeout" || (tc.name == "resolve-timeout" && count == 4) {
						ack = "hold"
					}
					if tc.name == "cancel" || (tc.name == "resolve-cancel" && count == 4) {
						ack = "hold"
						r.Cancel(engine.OriginControllerUser)
					}
					if tc.name == "cleanup-failure" || (tc.name == "resolve-cleanup-failure" && count == 4) {
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
			if report.Final != nil {
				t.Fatal("slice invented final report")
			}
			if !tc.failure {
				if result.Ready != tc.ready || result.Context.RunID != result.Intake.RunID || len(report.Snapshot.Attempts) != tc.stages || len(report.Snapshot.Sessions) != tc.stages {
					t.Fatalf("context/accounting: %+v", result)
				}
				var published publication[Context]
				if err := protocol.ReadJSON(result.Context.Path, &published); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(published.Data, expectedContext) {
					t.Fatal("committed context lost provenance/state")
				}
			}
			if tc.name != "cleanup-failure" && tc.name != "resolve-cleanup-failure" {
				for _, c := range report.Cleanup {
					if !c.WaitCompleted || !c.ProcessExited || len(c.Unconfirmed) > 0 {
						t.Fatalf("cleanup incomplete: %+v", c)
					}
				}
			}
			if (tc.name == "cleanup-failure" || tc.name == "resolve-cleanup-failure") && len(report.CleanupErrors) == 0 {
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
				for _, a := range report.Snapshot.Attempts {
					if a.Output != nil {
						t.Fatal("failed candidate became committed output")
					}
				}
			}
			if resolving && tc.name != "resolve-uncommitted-input" {
				if requests.Load() != acquiredRequests || acquiredRequests == 0 {
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
			if tc.name == "resolve-provider-failure" || tc.name == "resolve-timeout" {
				var failure *engine.Failure
				if !errors.As(report.Failure, &failure) {
					t.Fatal("resolution lost typed execution failure")
				}
				if tc.name == "resolve-provider-failure" && failure.Code != engine.ProviderFailed {
					t.Fatalf("resolution provider failure reclassified: %+v", failure)
				}
				if tc.name == "resolve-timeout" && (failure.Code != engine.TimedOut || failure.Origin != engine.OriginAttemptDeadline) {
					t.Fatalf("resolution timeout reclassified: %+v", failure)
				}
				for _, a := range report.Snapshot.Attempts {
					if a.State != engine.Succeeded && a.Output != nil {
						t.Fatal("failed resolution candidate became committed")
					}
				}
			}
			for _, s := range report.Snapshot.Sessions {
				if s.Role.Name == "triage-intake" && (s.Role.Model.Provider != "fireworks" || s.Role.Model.ID != "accounts/fireworks/models/deepseek-v4p1-flash") {
					t.Fatal("mechanical model binding changed")
				}
			}
			if (tc.name == "cancel" || tc.name == "resolve-cancel") && report.Outcome != engine.CancelledState {
				t.Fatal("cancellation swallowed")
			}
			if (tc.name == "attempt-cap" || tc.name == "resolve-attempt-cap") && !strings.Contains(fmt.Sprint(report.Failure), "LimitExceeded") {
				t.Fatal("hard cap swallowed")
			}
		})
	}
}
