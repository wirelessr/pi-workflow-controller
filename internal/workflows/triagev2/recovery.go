// Package triagev2 is the replacement Jira triage workflow. It is under
// construction and not registered.
package triagev2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

type RecoveryFailure struct {
	Stage      string                  `json:"stage"`
	TaskID     string                  `json:"task_id,omitempty"`
	Code       engine.Code             `json:"code"`
	Origin     engine.Origin           `json:"origin"`
	Dispatch   engine.DispatchAccepted `json:"dispatch"`
	RunID      string                  `json:"run_id"`
	StepID     string                  `json:"step_id"`
	AttemptID  string                  `json:"attempt_id"`
	Identity   runtime.Identity        `json:"identity"`
	Execution  *runtime.Execution      `json:"execution,omitempty"`
	Cleanup    *runtime.CleanupReport  `json:"cleanup,omitempty"`
	Diagnostic string                  `json:"diagnostic"`
}

// TaskFailure binds a Step failure to the owned handle and attempt it came
// from, so recovery can confirm exactly that owner before reporting it.
type TaskFailure struct {
	Cause    error
	Handle   *engine.SessionHandle
	Identity runtime.Identity
	Stage    string
	Attempt  string
}

func (f *TaskFailure) Error() string { return f.Cause.Error() }
func (f *TaskFailure) Unwrap() error { return f.Cause }

// Recoverable inspects each classified branch. Never let a timeout cause
// downgrade a storage/contract wrapper, or treat FailFastSibling as a user
// retry request.
func Recoverable(err error, sibling bool) bool {
	switch e := err.(type) {
	case *contract.Error:
		return false
	case *engine.Failure:
		allowed := e.Code == engine.TimedOut && e.Origin == engine.OriginAttemptDeadline || e.Code == engine.CompactionFailed && e.Origin == engine.OriginCompaction
		allowed = allowed || sibling && e.Code == engine.Cancelled && e.Origin == engine.OriginFailFastSibling
		if !allowed || e.LimitScope == "run" {
			return false
		}
		return !forbiddenCause(e.Cause)
	case interface{ Unwrap() []error }:
		children := e.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !Recoverable(child, sibling) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return Recoverable(e.Unwrap(), sibling)
	}
	return false
}

func forbiddenCause(err error) bool {
	switch e := err.(type) {
	case *contract.Error:
		return true
	case *engine.Failure:
		if e.Code == engine.StorageFailed || e.Code == engine.JournalFailed || e.Code == engine.CleanupFailed || e.LimitScope == "run" || e.Origin == engine.OriginRunDeadline || e.Code == engine.Cancelled && e.Origin != engine.OriginFailFastSibling {
			return true
		}
		return forbiddenCause(e.Cause)
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			if forbiddenCause(child) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return forbiddenCause(e.Unwrap())
	}
	return false
}

func SameIdentity(a, b runtime.Identity) bool {
	at, bt := a.SpawnTime, b.SpawnTime
	a.SpawnTime, b.SpawnTime = time.Time{}, time.Time{}
	return a == b && at.Equal(bt)
}

func ConfirmedFailureCleanup(r *engine.Run, err error) bool {
	switch e := err.(type) {
	case *engine.Failure:
		if e.Cleanup != nil {
			owner, ok := r.Snapshot().Sessions[e.HandleID]
			if !ok || owner.Identity.SessionID == "" || !SameIdentity(e.Cleanup.Identity, owner.Identity) || !e.Cleanup.ConfirmsLocalClose(owner.Identity.SessionID) {
				return false
			}
		}
		return ConfirmedFailureCleanup(r, e.Cause)
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			if !ConfirmedFailureCleanup(r, child) {
				return false
			}
		}
	case interface{ Unwrap() error }:
		return ConfirmedFailureCleanup(r, e.Unwrap())
	}
	return true
}

// RecoveryError keeps the current boundary first; a previously recovered
// timeout must not reclassify a later validation, cancellation, storage or
// cleanup failure.
func RecoveryError(current error, previous ...error) error {
	var failure *engine.Failure
	var contractError *contract.Error
	if !errors.As(current, &failure) && !errors.As(current, &contractError) {
		code, origin := engine.WorkflowFailed, engine.OriginDefinition
		if errors.Is(current, context.Canceled) {
			code, origin = engine.Cancelled, engine.OriginControllerUser
		}
		if errors.Is(current, context.DeadlineExceeded) {
			code, origin = engine.TimedOut, engine.OriginRunDeadline
		}
		current = &engine.Failure{Code: code, Origin: origin, Phase: "triage-recovery", Message: current.Error(), DispatchAccepted: engine.AcceptedNo, Cause: current}
	}
	return errors.Join(current, errors.Join(previous...))
}

func ConfirmRecovery(ctx context.Context, r *engine.Run, err error, sibling bool) (RecoveryFailure, error) {
	return ConfirmTaskRecovery(ctx, r, err, sibling, contract.Ref{})
}

// ConfirmTaskRecovery accepts a nonzero output as a committed sibling
// interrupted during close, not a failed attempt. Both paths retain the
// dispatch owner and strict-close gate.
func ConfirmTaskRecovery(ctx context.Context, r *engine.Run, err error, sibling bool, output contract.Ref) (RecoveryFailure, error) {
	var result RecoveryFailure
	if context.Cause(ctx) != nil {
		return result, RecoveryError(context.Cause(ctx), err)
	}
	if !Recoverable(err, sibling) || !ConfirmedFailureCleanup(r, err) {
		return result, err
	}
	var f *engine.Failure
	if !errors.As(err, &f) {
		return result, err
	}
	result = RecoveryFailure{Code: f.Code, Origin: f.Origin, Dispatch: f.DispatchAccepted, RunID: f.RunID, StepID: f.StepID, AttemptID: f.AttemptID, Diagnostic: err.Error()}
	task, owned := err.(*TaskFailure)
	if !owned {
		return result, err
	}
	committed := output != (contract.Ref{})
	if committed && (!sibling || f.Code != engine.Cancelled || f.Origin != engine.OriginFailFastSibling) {
		return result, err
	}
	undispatched := !committed && f.Code == engine.Cancelled && f.Origin == engine.OriginFailFastSibling && f.AttemptID == "" && task.Attempt == ""
	if task.Handle == nil {
		if !undispatched {
			return result, err
		}
		result.Stage = task.Stage
		return result, nil
	}
	identity, e := r.SessionIdentity(ctx, task.Handle)
	if e != nil {
		return result, RecoveryError(e, err)
	}
	if !SameIdentity(identity, task.Identity) || identity.SessionID == "" {
		return result, RecoveryError(fmt.Errorf("recovery identity mismatch"), err)
	}
	var execution *runtime.Execution
	if committed {
		attempt, ok := r.Snapshot().Attempts[task.Attempt]
		if !ok || task.Attempt != output.AttemptID || output.RunID != r.ID() || attempt.Identity.RunID != output.RunID || attempt.Identity.AttemptID != output.AttemptID || attempt.HandleID != identity.HandleID || attempt.State != engine.Succeeded || attempt.Failure != nil || attempt.Output == nil || *attempt.Output != output || attempt.Execution == nil || attempt.Execution.SessionID != identity.SessionID {
			return result, RecoveryError(fmt.Errorf("recovery requires exact succeeded sibling output and owner"), err)
		}
	} else if !undispatched {
		attempt, ok := r.Snapshot().Attempts[task.Attempt]
		if !ok || attempt.HandleID != identity.HandleID || f.HandleID != identity.HandleID || attempt.Identity.AttemptID != f.AttemptID || attempt.Identity.RunID != f.RunID || attempt.Identity.InvocationID != f.StepID || attempt.Output != nil {
			return result, RecoveryError(fmt.Errorf("recovery requires exact failed attempt identity without committed output"), err)
		}
		execution = attempt.Execution
	}
	report, e := r.CloseSessionReport(ctx, task.Handle)
	if e != nil {
		return result, RecoveryError(e, err)
	}
	if !SameIdentity(report.Identity, identity) || !report.ConfirmsLocalClose(identity.SessionID) {
		return result, RecoveryError(&engine.Failure{Code: engine.CleanupFailed, Origin: engine.OriginProtocol, Phase: "triage-recovery", Message: "recovery cleanup not confirmed", Cleanup: &report}, err)
	}
	if committed {
		return RecoveryFailure{}, nil
	}
	result.Stage, result.Identity, result.Execution, result.Cleanup = task.Stage, identity, execution, &report
	return result, nil
}

// RetryInputs is only for supplied-input work. Recovery never authorizes a
// remote operation, and RetryState feedback is not an input to the next task.
// A RunTaskStep inside run must set NoRepair, or the same session is
// repaired twice per retry.
func RetryInputs(ctx context.Context, r *engine.Run, scope *engine.Scope, key, output string, retries int, run func(context.Context, *engine.Scope) (contract.Ref, error), recovered func(context.Context, RecoveryFailure, error, bool) error) (contract.Ref, []RecoveryFailure, error) {
	var ref contract.Ref
	var failures []RecoveryFailure
	var causes []error
	_, err := scope.Retry(ctx, key, retries, func(ctx context.Context, s *engine.Scope, state engine.RetryState) (engine.RetryAction, error) {
		var err error
		ref, err = run(ctx, s)
		if err == nil {
			return engine.RetryAction{Result: engine.Result{Outputs: map[string]contract.Ref{output: ref}}}, nil
		}
		causes = append(causes, err)
		failure, e := ConfirmRecovery(ctx, r, err, false)
		if e != nil {
			causes = append(causes, e)
			return engine.RetryAction{}, e
		}
		failures = append(failures, failure)
		if recovered != nil {
			if e := recovered(ctx, failure, err, state.RetryCount < state.MaxRetries); e != nil {
				causes = append(causes, e)
				return engine.RetryAction{}, RecoveryError(e, err)
			}
		}
		return engine.RetryAction{Again: true, Feedback: &engine.Feedback{Message: failure.Diagnostic, SourceAttemptID: failure.AttemptID, SourceCode: string(failure.Code)}}, nil
	})
	if err != nil {
		return contract.Ref{}, failures, RecoveryError(err, causes...)
	}
	return ref, failures, nil
}
