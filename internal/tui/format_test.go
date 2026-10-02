package tui

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/x/ansi"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

func displaySnapshot(now time.Time) engine.Snapshot {
	return engine.Snapshot{
		RunID: "run-1", Workflow: "test", State: engine.Running, StatePersisted: true, CreatedAt: now.Add(-10 * time.Second),
		Sessions: map[string]engine.SessionStatus{
			"a": {Health: "Online", State: "Busy", Role: engine.RoleSpec{Name: "worker", Model: runtime.ModelSpec{Provider: "provider", ID: "model-a"}}},
			"b": {Health: "Offline", State: "Closed", Role: engine.RoleSpec{Name: "reviewer", Model: runtime.ModelSpec{Provider: "provider", ID: "model-b"}}},
			"c": {Health: "Unresponsive", State: "Busy"},
		},
		Invocations: map[string]engine.InvocationState{
			"inv-b": {Scope: "root/outer/inner/parallel/right", Key: "review", Epoch: "epoch-b", LastAttemptID: "b2", State: engine.AwaitingScope, Provisional: engine.Failed, RetryActivationIDs: []string{"outer", "old"}},
			"inv-a": {Scope: "root/outer/inner/parallel/left", Key: "produce", Epoch: "epoch-a", LastAttemptID: "a3", State: engine.Running, RetryActivationIDs: []string{"outer", "new"}},
		},
		Attempts: map[string]engine.AttemptState{
			"a1": {Number: 1, State: engine.Failed, HandleID: "a"},
			"a3": {Number: 3, State: engine.Running, HandleID: "a", StartedAt: now.Add(-2300 * time.Millisecond)},
			"b2": {Number: 2, State: engine.Failed, HandleID: "b", StartedAt: now.Add(-4 * time.Second), FinishedAt: now.Add(-time.Second), Failure: &engine.FailureInfo{Code: engine.ContractInvalid, Message: "schema invalid", Cause: "missing field"}},
		},
		Retries: map[string]engine.RetryStatus{
			"outer": {Scope: "root/outer", RetryState: engine.RetryState{ActivationID: "outer", RetryCount: 1, MaxRetries: 3}, Active: true},
			"old":   {Scope: "root/outer/inner", RetryState: engine.RetryState{ActivationID: "old", RetryCount: 2, MaxRetries: 2, Feedback: &engine.Feedback{Message: "old feedback"}}},
			"new":   {Scope: "root/outer/inner", RetryState: engine.RetryState{ActivationID: "new", RetryCount: 0, MaxRetries: 2, Feedback: &engine.Feedback{Message: "new feedback"}}, Active: true},
		},
	}
}

func requireText(t *testing.T, got string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(got, value) {
			t.Errorf("missing %q in:\n%s", value, got)
		}
	}
}

func TestFormatSnapshotParallelAndRetryActivations(t *testing.T) {
	now := time.Now()
	s := displaySnapshot(now)
	got := FormatSnapshot(s, now)
	assertPlain(t, got)
	requireText(t, got, "Elapsed: 10s", "health=Online", "health=Offline", "health=Unresponsive", "provider/model-a", "provider/model-b",
		"root/outer/inner/parallel/left/produce", "root/outer/inner/parallel/right/review", "epoch=epoch-a", "epoch=epoch-b",
		"latest attempt=a3 (#3)  state=Running  elapsed=2.3s", "latest attempt=b2 (#2)  state=Failed  elapsed=3s", "state=AwaitingScope  provisional=Failed", "Cause: missing field",
		"activation=outer  retry=1/3  active=true", "activation=old  retry=2/2  active=false", "activation=new  retry=0/2  active=true", "Latest feedback: new feedback", "Latest feedback: old feedback", "Retry activations (scope budgets)")
	if strings.Contains(got, "latest attempt=a1") {
		t.Fatal("historical attempt shown as latest")
	}
	for range 10 {
		if FormatSnapshot(s, now) != got {
			t.Fatal("unstable map order")
		}
	}
	m := New(nil, nil)
	m.receive(s, now)
	view := m.View().Content
	requireText(t, view, "\x1b[", "health=Online", "health=Offline", "health=Unresponsive")
	requireText(t, ansi.Strip(view), "health=Online", "health=Offline", "health=Unresponsive", "Latest feedback: new feedback")
}

func TestFormatSnapshotExactNestedRepeatedActivationMembership(t *testing.T) {
	now := time.Now()
	s := displaySnapshot(now)
	for _, tc := range []struct {
		name         string
		ids          []string
		want, absent []string
	}{
		{"first inner activation", []string{"outer", "old"}, []string{"activation=outer  retry=1/3", "activation=old  retry=2/2", "Latest feedback: old feedback"}, []string{"activation=new", "new feedback"}},
		{"repeated inner activation", []string{"outer", "new"}, []string{"activation=outer  retry=1/3", "activation=new  retry=0/2", "Latest feedback: new feedback"}, []string{"activation=old", "old feedback"}},
		{"no ancestry metadata", nil, nil, []string{"activation=", "Latest feedback:"}},
		{"missing activation", []string{"missing"}, []string{"activation=missing (metadata unavailable)"}, []string{"activation=outer", "activation=old", "activation=new", "Latest feedback:"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := s.Invocations["inv-a"]
			v.RetryActivationIDs = tc.ids
			s.Invocations["inv-a"] = v
			got := FormatSnapshot(s, now)
			left, rest, ok := strings.Cut(got, "  root/outer/inner/parallel/right/review")
			if !ok {
				t.Fatal("missing parallel invocation")
			}
			right, _, _ := strings.Cut(rest, "Retry activations (scope budgets):")
			requireText(t, left, tc.want...)
			for _, absent := range tc.absent {
				if strings.Contains(left, absent) {
					t.Errorf("wrong activation on inv-a: %q in %s", absent, left)
				}
			}
			requireText(t, right, "activation=outer  retry=1/3", "activation=old  retry=2/2", "Latest feedback: old feedback")
			if strings.Contains(right, "activation=new") || strings.Contains(right, "new feedback") {
				t.Fatalf("repeated activation contaminated untouched invocation: %s", right)
			}
		})
	}
}

func TestFormatSnapshotRetryWithoutStep(t *testing.T) {
	for _, active := range []bool{true, false} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			s := engine.Snapshot{Retries: map[string]engine.RetryStatus{
				"no-step": {Scope: "root/decision-retry", Active: active, RetryState: engine.RetryState{ActivationID: "no-step", RetryCount: 1, MaxRetries: 2, Feedback: &engine.Feedback{Message: "retry decision", SourceCode: "Rejected"}}},
			}}
			got := FormatSnapshot(s, time.Now())
			requireText(t, got, "Retry activations (scope budgets):", fmt.Sprintf("root/decision-retry  activation=no-step  retry=1/2  active=%t", active), "Latest feedback: retry decision", "code=Rejected")
			assertPlain(t, got)
		})
	}
}

func TestFormatSnapshotFeedbackSummary(t *testing.T) {
	const suffix = "… [full text in request/events]"
	for _, tc := range []struct{ name, message, want string }{
		{"short", "repair output", "repair output"},
		{"at rune limit", strings.Repeat("界", 240), strings.Repeat("界", 240)},
		{"over rune limit", strings.Repeat("界", 241), strings.Repeat("界", 240-utf8.RuneCountInString(suffix)) + suffix},
		{"sanitize before counting", "\x1b[31m" + strings.Repeat("界", 240) + "\x1b[0m", strings.Repeat("界", 240)},
		{"osc across boundary", strings.Repeat("界", 239) + "\x1b]52;c;" + strings.Repeat("SECRET", 60) + "\a界", strings.Repeat("界", 240)},
		{"unterminated osc", strings.Repeat("界", 240) + "\x1b]52;c;SECRET", strings.Repeat("界", 240)},
		{"multibyte c1 continuation", strings.Repeat("界", 239) + "ĝtail", strings.Repeat("界", 240-utf8.RuneCountInString(suffix)) + suffix},
		{"unsafe long message", "\x1b]52;c;SECRET\a" + strings.Repeat("界", 241) + "\x1b[2J", strings.Repeat("界", 240-utf8.RuneCountInString(suffix)) + suffix},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			s := displaySnapshot(now)
			a := s.Attempts["a3"]
			a.Feedback = &engine.Feedback{Message: tc.message, SourceAttemptID: "a1", SourceCode: "Direct"}
			s.Attempts["a3"] = a
			s.Retries["new"].Feedback.Message = tc.message
			m := New(nil, nil)
			m.receive(s, now)
			for _, output := range []string{FormatSnapshot(s, now), ansi.Strip(m.View().Content)} {
				assertPlain(t, output)
				count := 0
				for _, line := range strings.Split(output, "\n") {
					_, rest, ok := strings.Cut(line, "Latest feedback: ")
					if !ok || strings.Contains(rest, "old feedback") {
						continue
					}
					summary, _, _ := strings.Cut(rest, "  source attempt=")
					if summary != tc.want || utf8.RuneCountInString(summary) > 240 {
						t.Errorf("summary=%q, want %q", summary, tc.want)
					}
					count++
				}
				if count != 3 {
					t.Errorf("got %d summaries, want attempt, attached retry and retry section", count)
				}
			}
			if a.Feedback.Message != tc.message || s.Retries["new"].Feedback.Message != tc.message {
				t.Fatal("rendering mutated raw feedback")
			}
		})
	}
}

func TestFormatSnapshotFeedbackBelongsToLatestAttempt(t *testing.T) {
	now := time.Now()
	for _, latest := range []string{"a3", "a4", "missing"} {
		t.Run(latest, func(t *testing.T) {
			s := displaySnapshot(now)
			for id, message := range map[string]string{"a1": "historical only", "a3": "direct step feedback", "b2": "other step feedback"} {
				a := s.Attempts[id]
				a.Feedback = &engine.Feedback{Message: message, SourceAttemptID: "a1", SourceCode: "Rejected"}
				s.Attempts[id] = a
			}
			s.Attempts["a4"] = engine.AttemptState{Number: 4, State: engine.Preparing}
			inv := s.Invocations["inv-a"]
			inv.LastAttemptID = latest
			inv.RetryActivationIDs = nil
			s.Invocations["inv-a"] = inv
			m := New(nil, nil)
			m.receive(s, now)
			for _, got := range []string{FormatSnapshot(s, now), ansi.Strip(m.View().Content)} {
				left, right, ok := strings.Cut(got, "root/outer/inner/parallel/right/review")
				if !ok {
					t.Fatal("missing parallel invocation")
				}
				requireText(t, left, "latest attempt="+latest)
				if strings.Contains(left, "direct step feedback") != (latest == "a3") || strings.Contains(left, "other step feedback") || strings.Contains(got, "historical only") {
					t.Fatalf("feedback assigned to wrong attempt: %s", got)
				}
				if latest == "a3" {
					requireText(t, left, "Latest feedback: direct step feedback  source attempt=a1  code=Rejected")
				} else if strings.Contains(left, "Latest feedback:") {
					t.Fatalf("stale feedback retained for %s: %s", latest, left)
				}
				requireText(t, right, "Latest feedback: other step feedback", "Retry activations (scope budgets):", "Latest feedback: new feedback")
			}
		})
	}
}

func TestFormattersSanitizeEachField(t *testing.T) {
	attack := "visible\x1b]52;c;SECRET"
	now := time.Now()
	s := displaySnapshot(now)
	s.Workflow = attack
	v := s.Sessions["a"]
	v.Role.Model.ID = attack
	s.Sessions["a"] = v
	s.Retries["new"].Feedback.Message = attack
	inv := s.Invocations["inv-a"]
	inv.Scope, inv.Key, inv.Epoch = attack, attack, attack
	inv.RetryActivationIDs = []string{attack, "missing" + attack}
	s.Invocations["inv-a"] = inv
	s.Retries[attack] = engine.RetryStatus{Scope: attack, RetryState: engine.RetryState{ActivationID: attack, Feedback: &engine.Feedback{Message: attack, SourceAttemptID: attack, SourceCode: attack}}}
	s.Failure = &engine.FailureInfo{Code: engine.Code(attack), Message: attack, Cause: attack}
	m := New(nil, nil)
	m.receive(s, now)
	for _, output := range []string{
		ansi.Strip(m.View().Content),
		FormatSnapshot(s, now),
		FormatReport(engine.Report{Snapshot: s, Outcome: engine.State(attack), Failure: errors.New(attack), Result: engine.Result{Outputs: map[string]contract.Ref{attack: {Path: attack}}}, Cleanup: []runtime.CleanupReport{{WaitError: attack, Unconfirmed: []string{attack}, StderrTail: attack}}}, attack),
	} {
		assertPlain(t, output)
		if strings.Contains(output, "SECRET") {
			t.Fatalf("control payload leaked: %q", output)
		}
		requireText(t, output, "visible", "Run:")
	}
	requireText(t, FormatSnapshot(s, now), "thinking=", "source attempt=", "phase=", "activation=visible  retry=", "activation=missingvisible (metadata unavailable)")
}

func TestFormatReportAllCleanupAndFinalizationDiagnostics(t *testing.T) {
	longCause := "root cause " + strings.Repeat("界", 300)
	cause := &engine.Failure{Code: engine.Cancelled, Origin: engine.OriginSignalTERM, Message: "cancelled", Cause: errors.New(longCause)}
	r := engine.Report{
		Outcome: engine.CancelledState, ExitCode: 143,
		Failure:  fmt.Errorf("outer: %w", cause),
		Result:   engine.Result{Outputs: map[string]contract.Ref{"diagnostic": {Path: "/nonexistent/published/contract.json", RunID: "run", AttemptID: "attempt", SchemaID: "schema"}}},
		Snapshot: engine.Snapshot{RunID: "run", StatePersisted: false},
		Cleanup: []runtime.CleanupReport{
			{Identity: runtime.Identity{HandleID: "first"}, WaitError: "wait error", KillError: "kill error", DiscoveryError: "discovery error", Unconfirmed: []string{"bash unknown", "observer unknown"}, StderrTruncated: true, StderrTail: "stderr content"},
			{Identity: runtime.Identity{HandleID: "second"}, ProcessExited: true, WaitCompleted: true, DiscoveryRemoved: []string{"/removed/discovery.json"}},
		},
		CleanupErrors:      []error{&engine.Failure{Code: engine.CleanupFailed, Message: "cleanup error", Cause: errors.New("cleanup cause")}},
		FinalizationErrors: []error{errors.Join(&engine.Failure{Code: engine.FinalizationFailed, Phase: "RunFinished", Message: "cannot persist", Cause: errors.New("disk full")}, errors.New("close failed"))},
	}
	got := FormatReport(r, "/actual/run/path")
	assertPlain(t, got)
	requireText(t, got, "Outcome: Cancelled", "Exit code: 143", "Run path: /actual/run/path", "EMERGENCY: state_persisted=false", "metadata only; not a commit assertion", "/nonexistent/published/contract.json",
		"origin=SignalTERM", "Cause: "+longCause, "handle=first", "handle=second", "Abort acknowledged=false", "AbortBash acknowledged=false", "SIGKILL=false", "Wait completed=false", "Process exited=false",
		"Wait warning: wait error", "Kill warning: kill error", "Discovery warning: discovery error", "Unconfirmed: bash unknown", "Unconfirmed: observer unknown", "Warning: stderr truncated", "Stderr tail: stderr content", "/removed/discovery.json",
		"cleanup cause", "FinalizationFailed", "phase=RunFinished", "Cause: disk full", "Cause: close failed")
}

func TestFormatReportFinalDelivery(t *testing.T) {
	for _, name := range []string{"success", "failed-incomplete", "no-final", "missing-session", "controller-artifact", "exit-unconfirmed", "wait-unconfirmed", "wrong-cleanup-handle"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			ref := contract.Ref{Path: filepath.Join(dir, "published", "contract.json")}
			r := engine.Report{
				Outcome: engine.Succeeded,
				Final:   &engine.FinalDelivery{Output: "report", Ref: ref, ArtifactPath: filepath.Join(dir, "published", "artifacts", "readable.txt"), HandleID: "producer", Scope: "root/review", Step: "validate"},
				Result:  engine.Result{Outputs: map[string]contract.Ref{"report": ref}},
				Snapshot: engine.Snapshot{StatePersisted: true, Sessions: map[string]engine.SessionStatus{
					"producer": {Role: engine.RoleSpec{Name: "validator"}, State: "Closed", Identity: runtime.Identity{HandleID: "producer", SessionID: "snapshot-session-id", SessionFile: filepath.Join(dir, "history.jsonl")}},
					"later":    {Role: engine.RoleSpec{Name: "last-role"}, State: "Closed", Identity: runtime.Identity{SessionID: "last-session-id", SessionFile: "/last-session-file"}},
				}},
				Cleanup: []runtime.CleanupReport{
					{Identity: runtime.Identity{HandleID: "producer", SessionID: "cleanup-not-display-id"}, ProcessExited: true, WaitCompleted: true},
					{Identity: runtime.Identity{HandleID: "later"}, ProcessExited: true, WaitCompleted: true},
				},
				CleanupErrors:      []error{errors.New("cleanup diagnostic")},
				FinalizationErrors: []error{errors.New("finalization diagnostic")},
			}
			switch name {
			case "failed-incomplete":
				r.Outcome, r.ExitCode, r.Failure = engine.Failed, 1, errors.New("required reviewer failed; incomplete report")
			case "no-final":
				r.Final = nil
			case "missing-session":
				delete(r.Snapshot.Sessions, "producer")
			case "controller-artifact":
				r.Final.HandleID, r.Final.Ref.AttemptID = "", "attached"
				r.Snapshot.Attempts = map[string]engine.AttemptState{"attached": {Controller: true, State: engine.Succeeded}}
			case "exit-unconfirmed":
				r.Cleanup[0].ProcessExited = false
			case "wait-unconfirmed":
				r.Cleanup[0].WaitCompleted = false
			case "wrong-cleanup-handle":
				r.Cleanup[0].Identity.HandleID = "not-producer"
			}
			if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte(`{"sessionId":"disk-id-not-snapshot"}`), 0600); err != nil {
				t.Fatal(err)
			}
			got := FormatReport(r, dir)
			if err := os.RemoveAll(dir); err != nil {
				t.Fatal(err)
			}
			if after := FormatReport(r, dir); after != got {
				t.Fatalf("display depends on disk/Pi data after Store close: before=%s after=%s", got, after)
			}
			assertPlain(t, got)
			requireText(t, got, "Result index path: "+filepath.Join(dir, "result.json"), "Cleanup warning: cleanup diagnostic", "Finalization error: finalization diagnostic")
			if name == "no-final" {
				requireText(t, got, "Final output/node: unavailable", "no session inferred", ref.Path)
				for _, forbidden := range []string{"Final node:", "  Session ID:", "  Session file:", "  Role:", "  Contract:", "Readable artifact:"} {
					if strings.Contains(got, forbidden) {
						t.Errorf("unselected Ref inferred %q: %s", forbidden, got)
					}
				}
				return
			}
			requireText(t, got, "Final output (verified before cleanup): report", "Contract: "+ref.Path, "Readable artifact: "+r.Final.ArtifactPath, "Final node: root/review/validate")
			switch name {
			case "controller-artifact":
				requireText(t, got, "Producer: controller (no session to resume)")
				if strings.Contains(got, "  Session ID:") || strings.Contains(got, "Session metadata unavailable") {
					t.Fatal("controller artifact shown with session metadata")
				}
			case "missing-session":
				requireText(t, got, "Session metadata unavailable")
				if strings.Contains(got, "  Session ID:") || strings.Contains(got, "Session state: Closed") {
					t.Fatal("missing producer inferred another session")
				}
			default:
				requireText(t, got, "Role: validator", "Session ID: snapshot-session-id", "Session file: "+filepath.Join(dir, "history.jsonl"))
				closed := name == "success" || name == "failed-incomplete"
				if strings.Contains(got, "Session state: Closed (process exit confirmed;") != closed {
					t.Fatalf("wrong exit confidence: %s", got)
				}
				if !closed {
					requireText(t, got, "process exit unconfirmed; do not resume until cleanup is verified")
				}
			}
			if name == "failed-incomplete" {
				requireText(t, got, "Outcome: Failed  Exit code: 1", "required reviewer failed; incomplete report")
			}
			for _, forbidden := range []string{"Session ID: cleanup-not-display-id", "disk-id-not-snapshot", "last-session-id", "last-session-file", "Role: last-role"} {
				if strings.Contains(got, forbidden) {
					t.Errorf("wrong metadata source %q: %s", forbidden, got)
				}
			}
		})
	}
}

func TestFormatReportFinalFieldsSanitizedIndependently(t *testing.T) {
	for _, attack := range []string{"visible\x1b[31m\x1b[0m", "visible\x1b]52;c;SECRET\a", "visible\x1b]52;c;SECRET"} {
		for _, field := range []string{"output", "contract", "artifact", "scope", "step", "handle", "role", "session-id", "session-file", "session-state", "result-index"} {
			t.Run(fmt.Sprintf("%q/%s", attack, field), func(t *testing.T) {
				dir := "/run"
				final := &engine.FinalDelivery{Output: "report", Ref: contract.Ref{Path: "/contract"}, ArtifactPath: "/artifact", HandleID: "handle", Scope: "root", Step: "validate"}
				session := engine.SessionStatus{Role: engine.RoleSpec{Name: "validator"}, State: "Unresponsive", Identity: runtime.Identity{SessionID: "session-id", SessionFile: "/session-file"}}
				switch field {
				case "output":
					final.Output = attack
				case "contract":
					final.Ref.Path = attack
				case "artifact":
					final.ArtifactPath = attack
				case "scope":
					final.Scope = attack
				case "step":
					final.Step = attack
				case "handle":
					final.HandleID = attack
				case "role":
					session.Role.Name = attack
				case "session-id":
					session.Identity.SessionID = attack
				case "session-file":
					session.Identity.SessionFile = attack
				case "session-state":
					session.State = attack
				case "result-index":
					dir = attack
				}
				r := engine.Report{Final: final, Snapshot: engine.Snapshot{Sessions: map[string]engine.SessionStatus{final.HandleID: session}}, Cleanup: []runtime.CleanupReport{{Identity: runtime.Identity{HandleID: final.HandleID}}}, CleanupErrors: []error{errors.New("old cleanup error")}, FinalizationErrors: []error{errors.New("old finalization error")}}
				got := FormatReport(r, dir)
				assertPlain(t, got)
				if strings.Contains(got, "SECRET") {
					t.Fatalf("control payload leaked: %q", got)
				}
				requireText(t, got, "visible", "Result index path:", "Final output (verified before cleanup):", "Contract:", "Readable artifact:", "Final node:", "Role:", "Session ID:", "Session file:", "Session state:", "process exit unconfirmed", "Output Ref paths", "Cleanup report 1:", "Cleanup warning: old cleanup error", "Finalization error: old finalization error")
				if field == "scope" {
					requireText(t, got, "Final node: visible/validate")
				}
			})
		}
	}
}

func TestElapsedBoundaries(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name       string
		start, end time.Time
		want       string
	}{
		{"unknown", time.Time{}, time.Time{}, "0s"},
		{"future", now.Add(time.Hour), time.Time{}, "0s"},
		{"running", now.Add(-1250 * time.Millisecond), time.Time{}, "1.2s"},
		{"finished", now.Add(-time.Hour), now.Add(-time.Hour + 2500*time.Millisecond), "2.5s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := duration(elapsed(tc.start, tc.end, now)); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatSnapshotControllerAttempt(t *testing.T) {
	now := time.Now()
	s := displaySnapshot(now)
	s.Invocations["inv-c"] = engine.InvocationState{Scope: "root", Key: "caller-prompt", Epoch: "epoch-c", LastAttemptID: "c1", State: engine.Succeeded}
	s.Attempts["c1"] = engine.AttemptState{Number: 1, State: engine.Succeeded, Controller: true, StartedAt: now.Add(-time.Second), FinishedAt: now}
	got := FormatSnapshot(s, now)
	requireText(t, got, "root/caller-prompt", "latest attempt=c1 (#1)  state=Succeeded  elapsed=1s  producer=controller")
	if strings.Contains(got, "latest attempt=c1 (#1)  state=Succeeded  elapsed=1s  session=") {
		t.Fatal("controller attempt shown with a session")
	}
}
