package triagev2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// TaskStep is one Step in its own fresh session, closed with strict local
// confirmation after success.
type TaskStep struct {
	Scope  *engine.Scope
	Model  runtime.ModelSpec
	Stage  string
	Key    string
	Task   any
	Schema string
	Inputs []contract.Ref
	// Recovery captures the owned identity before dispatch and reports
	// failures as *TaskFailure for ConfirmTaskRecovery.
	Recovery bool
	// Validate is the semantic acceptance gate run on every published
	// contract; nil means schema validation only.
	Validate func(context.Context, contract.Ref) error
	// Feedback is given to the first attempt, for example a business retry's
	// reason; a contract repair re-attempt replaces it with the rejection.
	Feedback *engine.Feedback
	// Timeout bounds each attempt; zero means 30 minutes.
	Timeout time.Duration
	// NoRepair is for a Step that already sits inside an outer retry layer
	// (RetryInputs) with its own feedback loop: adding the contract repair
	// there would double-retry the same session, corrupting the outer
	// layer's failure accounting.
	NoRepair bool
}

// RunTaskStep allows a single repair retry covering both contract-shape
// (schema) failures and, when Validate is supplied, semantic acceptance
// failures. A rejected contract returns to the same session with the exact
// validator diagnostic as feedback; the repaired contract must pass the same
// gates in full. Execution failures (timeout, cancellation, provider) are
// never retried here.
func RunTaskStep(ctx context.Context, r *engine.Run, t TaskStep) (contract.Ref, error) {
	ts, err := OpenTaskSession(ctx, r, t)
	if err != nil {
		return contract.Ref{}, err
	}
	out, err := ts.Run(ctx, r, t)
	if err != nil {
		return out.Output, err
	}
	return CloseTaskStep(ctx, r, ts.Handle, ts.Identity, t.Stage, out, t.Recovery)
}

// TaskSession is a Step session that may serve more than one Step, such as
// consecutive investigation rounds below the capacity threshold.
type TaskSession struct {
	Handle   *engine.SessionHandle
	Identity runtime.Identity
}

// OpenTaskSession opens the session for t's role and model; with Recovery
// it captures the owned identity before any dispatch.
func OpenTaskSession(ctx context.Context, r *engine.Run, t TaskStep) (TaskSession, error) {
	h, err := r.OpenSession(ctx, engine.RoleSpec{Name: "triage-" + t.Stage, Model: t.Model})
	if err != nil {
		return TaskSession{}, err
	}
	ts := TaskSession{Handle: h}
	if t.Recovery {
		// A sibling may cancel after OpenSession. Capture this owned handle for
		// parent-context cleanup without permitting a cancelled Step dispatch.
		if ts.Identity, err = r.SessionIdentity(context.WithoutCancel(ctx), h); err != nil {
			return ts, err
		}
	}
	return ts, nil
}

// Run dispatches t on the session with the single contract repair and
// leaves the session open. A failure with Recovery is a *TaskFailure.
func (ts TaskSession) Run(ctx context.Context, r *engine.Run, t TaskStep) (engine.StepResult, error) {
	s, stage, key, recovery, validate, repairable := t.Scope, t.Stage, t.Key, t.Recovery, t.Validate, !t.NoRepair
	h, identity := ts.Handle, ts.Identity
	prompt, err := json.Marshal(t.Task)
	if err != nil {
		return engine.StepResult{}, err
	}
	timeout := t.Timeout
	if timeout == 0 {
		timeout = 30 * time.Minute
	}
	spec := engine.StepSpec{Key: key, Session: h, Prompt: string(prompt), Inputs: t.Inputs, Feedback: t.Feedback, Output: contract.Spec{SchemaID: t.Schema}, Timeout: timeout}
	var out engine.StepResult
	var lastFeedback *engine.Feedback
	// Mechanical contract-shape repair, one budgeted re-attempt on the same
	// session: the rejection diagnostic rides spec.Feedback so the repairing
	// agent sees the exact violation. Retried failures keep their typed code.
	// Execution failures (timeout, cancellation, provider) are never retried.
	// A manual second Step keeps successful steps from registering a retry
	// scope: retry accounting must reflect only real repairs.
	const repairBudget = 1
	var repairCount int
	var lastPublished contract.Ref
	for {
		attemptSpec := spec
		if repairCount > 0 && lastFeedback != nil {
			attemptSpec.Feedback = lastFeedback
		}
		// The first attempt dispatches from the owning scope unchanged; only
		// the repair re-attempt needs a fresh child scope, because a failed
		// step key is burned on its owning scope.
		attemptScope := s
		if repairCount > 0 {
			child, cerr := s.Child("contract-repair-" + key)
			if cerr != nil {
				err = cerr
				break
			}
			attemptScope = child
		}
		var e error
		out, e = attemptScope.Step(ctx, attemptSpec)
		if e == nil {
			if out.Output != (contract.Ref{}) {
				lastPublished = out.Output
			}
			if validate == nil {
				break
			}
			if ve := validate(ctx, out.Output); ve == nil {
				break
			} else if repairable && repairCount < repairBudget {
				lastFeedback = &engine.Feedback{Message: "Previous contract was published but rejected by acceptance validation. Fix exactly the reported violation and republish the same contract; do not change substance: " + ve.Error()}
				repairCount++
				continue
			} else {
				err = ve
				break
			}
		}
		var failure *engine.Failure
		if !repairable || !errors.As(e, &failure) || failure.Code != engine.ContractInvalid || repairCount >= repairBudget {
			err = e
			break
		}
		lastFeedback = &engine.Feedback{Message: "Previous contract rejected by schema validation. Fix exactly the reported violations and republish the same contract; do not change substance: " + failure.Message, SourceAttemptID: failure.AttemptID, SourceCode: string(failure.Code)}
		repairCount++
	}
	if err != nil {
		if recovery {
			// A sibling cancel can interrupt a repair after an earlier
			// attempt already published: that output stays committed, so
			// the join must see it rather than an empty dispatch failure.
			cancelled := false
			var f *engine.Failure
			if errors.As(err, &f) && f.Code == engine.Cancelled {
				cancelled = true
			}
			attemptID := out.AttemptID
			if cancelled && lastPublished != (contract.Ref{}) {
				attemptID = lastPublished.AttemptID
				return engine.StepResult{AttemptID: attemptID, Output: lastPublished}, &TaskFailure{Cause: err, Handle: h, Identity: identity, Stage: stage, Attempt: attemptID}
			}
			return engine.StepResult{}, &TaskFailure{Cause: err, Handle: h, Identity: identity, Stage: stage, Attempt: attemptID}
		}
		return engine.StepResult{}, err
	}
	return out, nil
}

func CloseTaskStep(ctx context.Context, r *engine.Run, h *engine.SessionHandle, identity runtime.Identity, stage string, out engine.StepResult, recovery bool) (contract.Ref, error) {
	if recovery && out.Execution.SessionID != identity.SessionID {
		return out.Output, fmt.Errorf("task execution identity mismatch")
	}
	closed, err := r.CloseSessionReport(ctx, h)
	if err != nil {
		if recovery {
			return out.Output, &TaskFailure{Cause: err, Handle: h, Identity: identity, Stage: stage, Attempt: out.AttemptID}
		}
		return out.Output, err
	}
	if !closed.ConfirmsLocalClose(out.Execution.SessionID) || recovery && closed.Identity != identity {
		return out.Output, fmt.Errorf("%s cleanup not confirmed", stage)
	}
	return out.Output, nil
}
