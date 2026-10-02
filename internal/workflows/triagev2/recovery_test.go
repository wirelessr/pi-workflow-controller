package triagev2

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

func TestRecoverableClassification(t *testing.T) {
	timeout := func(cause error) *engine.Failure {
		return &engine.Failure{Code: engine.TimedOut, Origin: engine.OriginAttemptDeadline, Cause: cause}
	}
	sibling := &engine.Failure{Code: engine.Cancelled, Origin: engine.OriginFailFastSibling}
	for _, tc := range []struct {
		name    string
		err     error
		sibling bool
		want    bool
	}{
		{"attempt timeout", timeout(nil), false, true},
		{"compaction failure", &engine.Failure{Code: engine.CompactionFailed, Origin: engine.OriginCompaction}, false, true},
		{"wrapped attempt timeout", fmt.Errorf("step: %w", timeout(nil)), false, true},
		{"run deadline timeout", &engine.Failure{Code: engine.TimedOut, Origin: engine.OriginRunDeadline}, false, false},
		{"run-scoped limit", &engine.Failure{Code: engine.TimedOut, Origin: engine.OriginAttemptDeadline, LimitScope: "run"}, false, false},
		{"contract error", &contract.Error{Code: "ContractInvalid"}, false, false},
		{"timeout caused by storage failure", timeout(&engine.Failure{Code: engine.StorageFailed}), false, false},
		{"timeout caused by journal failure", timeout(&engine.Failure{Code: engine.JournalFailed}), false, false},
		{"timeout caused by cleanup failure", timeout(&engine.Failure{Code: engine.CleanupFailed}), false, false},
		{"timeout caused by contract error", timeout(&contract.Error{}), false, false},
		{"timeout caused by user cancel", timeout(&engine.Failure{Code: engine.Cancelled, Origin: engine.OriginControllerUser}), false, false},
		{"sibling cancel outside a sibling join", sibling, false, false},
		{"sibling cancel inside a sibling join", sibling, true, true},
		{"joined recoverable branches", errors.Join(timeout(nil), sibling), true, true},
		{"joined with one unrecoverable branch", errors.Join(timeout(nil), &engine.Failure{Code: engine.ProviderFailed}), false, false},
		{"empty join", errors.Join(), false, false},
		{"plain error", errors.New("boom"), false, false},
		{"task failure wrapper", &TaskFailure{Cause: timeout(nil)}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Recoverable(tc.err, tc.sibling); got != tc.want {
				t.Fatalf("Recoverable = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestRecoveryErrorKeepsCurrentBoundaryFirst(t *testing.T) {
	previous := &engine.Failure{Code: engine.TimedOut, Origin: engine.OriginAttemptDeadline, Message: "earlier timeout"}
	for _, tc := range []struct {
		name         string
		current      error
		code         engine.Code
		origin       engine.Origin
		keepsCurrent bool
	}{
		{"typed failure kept", &engine.Failure{Code: engine.StorageFailed, Origin: engine.OriginProtocol}, engine.StorageFailed, engine.OriginProtocol, true},
		{"contract error kept", &contract.Error{Code: "ContractInvalid"}, "", "", true},
		{"plain error classified as workflow failure", errors.New("boom"), engine.WorkflowFailed, engine.OriginDefinition, false},
		{"context cancel classified as user cancel", fmt.Errorf("stop: %w", context.Canceled), engine.Cancelled, engine.OriginControllerUser, false},
		{"context deadline classified as run deadline", fmt.Errorf("late: %w", context.DeadlineExceeded), engine.TimedOut, engine.OriginRunDeadline, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := RecoveryError(tc.current, previous)
			joined, ok := err.(interface{ Unwrap() []error })
			if !ok || len(joined.Unwrap()) != 2 || !errors.Is(err, previous) || !errors.Is(err, tc.current) {
				t.Fatalf("RecoveryError lost an error: %v", err)
			}
			first := joined.Unwrap()[0]
			if tc.keepsCurrent {
				if first != tc.current {
					t.Fatalf("first = %v, want current error unchanged", first)
				}
				return
			}
			f, ok := first.(*engine.Failure)
			if !ok || f.Code != tc.code || f.Origin != tc.origin || f.Phase != "triage-recovery" || f.DispatchAccepted != engine.AcceptedNo || f.Cause != tc.current {
				t.Fatalf("first = %#v, want %s/%s wrapper around the current error", first, tc.code, tc.origin)
			}
		})
	}
}
