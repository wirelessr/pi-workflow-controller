package triagev2

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// TestRetryInputsRecovery drives the recovery seam over the real engine,
// runtime and RPC protocol: an attempt that times out is confirmed closed
// and retried in a fresh session; anything else is not retried.
func TestRetryInputsRecovery(t *testing.T) {
	stop := errors.New("caller refused the retry")
	for _, tc := range []struct {
		name      string
		answers   []string
		retries   int
		recovered error
		sessions  int
		failures  int
		code      engine.Code
		contains  string
	}{
		{name: "timeout then success", answers: []string{"hold", ""}, retries: 1, sessions: 2, failures: 1},
		{name: "timeouts exhaust the retries", answers: []string{"hold", "hold"}, retries: 1, sessions: 2, failures: 2, code: engine.RetryExhausted},
		{name: "an invalid contract is repaired once in the same session, then not retried", answers: []string{"invalid", "invalid"}, retries: 1, sessions: 1, code: engine.ContractInvalid},
		{name: "a failing recovered callback stops the retry", answers: []string{"hold"}, retries: 1, recovered: stop, sessions: 1, failures: 1, contains: stop.Error()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ref contract.Ref
			var failures []RecoveryFailure
			var retryErr error
			var seen []RecoveryFailure
			model := runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}
			calls := 0
			res := runHarness(t, "CASE-17", func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
				ref, failures, retryErr = RetryInputs(ctx, r, r.Root(), "seam", "out", tc.retries, func(ctx context.Context, s *engine.Scope, fb *engine.Feedback) (contract.Ref, error) {
					tk := newTask(r, "probe", "CASE-17", nil)
					return RunTaskStep(ctx, r, TaskStep{Scope: s, Model: model, Stage: "probe", Key: "probe", Task: tk, Schema: PromptSchema, Feedback: fb, Recovery: true, Timeout: time.Second})
				}, func(_ context.Context, f RecoveryFailure, _ error, _ bool) error {
					seen = append(seen, f)
					return tc.recovered
				})
				return engine.Result{}, nil
			}, func(t *testing.T, call agentCall) string {
				answer := tc.answers[calls]
				calls++
				switch answer {
				case "hold":
					return "hold"
				case "invalid":
					call.reply(t, map[string]any{"ticket": "not a key"}, nil)
				default:
					call.reply(t, CallerPrompt{Ticket: "CASE-17"}, nil)
				}
				return ""
			})
			if calls != len(tc.answers) || len(res.Report.Snapshot.Sessions) != tc.sessions {
				t.Fatalf("dispatches = %d sessions = %d, want %d and %d", calls, len(res.Report.Snapshot.Sessions), len(tc.answers), tc.sessions)
			}
			for _, s := range res.Report.Snapshot.Sessions {
				if s.State != "Closed" {
					t.Fatalf("session %s left %s", s.ID, s.State)
				}
			}
			if len(failures) != tc.failures {
				t.Fatalf("recovery failures = %+v, want %d", failures, tc.failures)
			}
			for _, f := range failures {
				if f.Code != engine.TimedOut || f.Origin != engine.OriginAttemptDeadline || f.Cleanup == nil || !f.Cleanup.ConfirmsLocalClose(f.Identity.SessionID) || f.Stage != "probe" || f.AttemptID == "" {
					t.Fatalf("recovery failure = %+v, want a confirmed-closed attempt timeout", f)
				}
			}
			if tc.recovered != nil && len(seen) != 1 {
				t.Fatalf("recovered callback saw %d failures", len(seen))
			}
			switch {
			case tc.code == "" && tc.contains == "":
				if retryErr != nil || ref == (contract.Ref{}) || !strings.HasPrefix(ref.SchemaID, "triage.prompt") {
					t.Fatalf("ref = %+v err = %v", ref, retryErr)
				}
			case tc.code != "":
				var f *engine.Failure
				if !errors.As(retryErr, &f) || f.Code != tc.code || ref != (contract.Ref{}) {
					t.Fatalf("error = %v, want %s", retryErr, tc.code)
				}
			default:
				if retryErr == nil || !strings.Contains(retryErr.Error(), tc.contains) || ref != (contract.Ref{}) {
					t.Fatalf("error = %v, want it to keep %q", retryErr, tc.contains)
				}
			}
		})
	}
}
