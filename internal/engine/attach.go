package engine

import (
	"context"
	"encoding/json"
	"path/filepath"

	"pi-workflow-controller/internal/contract"
)

// AttachSpec describes Controller-produced bytes to commit as an exact Ref.
// Data is the envelope data; Files are written by the Controller into the
// attempt's evidence/ or artifacts/ directory.
type AttachSpec struct {
	Key    string
	Output contract.Spec
	Data   any
	Files  []contract.ControllerFile
}

// Attach commits Controller-produced data through the same Stage, Publish and
// journal commit as Step, without a session or dispatch. The attempt counts
// against the run's total attempt limit and shares the scope's key namespace.
// Its Ref is authorized only after the durable commit, like any Step output.
func (s *Scope) Attach(ctx context.Context, spec AttachSpec) (ref contract.Ref, err error) {
	if s == nil || s.run == nil {
		return ref, newFailure(InvalidDefinition, "Attach", "scope is not initialized")
	}
	data, err := json.Marshal(spec.Data)
	if err != nil {
		return ref, newFailure(InvalidDefinition, "Attach", "attach data is not JSON: "+err.Error())
	}
	r := s.run
	r.mu.Lock()
	if err = s.checkLocked(ctx); err != nil {
		r.mu.Unlock()
		return ref, err
	}
	if !validName(spec.Key) || s.steps[spec.Key] || !r.schemas.Has(spec.Output.SchemaID) {
		r.mu.Unlock()
		return ref, newFailure(InvalidDefinition, "Attach", "invalid key or schema")
	}
	// Reject what Stage would refuse before an attempt is consumed.
	if err = r.store.CheckControllerFiles(data, spec.Files); err != nil {
		r.mu.Unlock()
		return ref, normalize(err, "Attach")
	}
	astate, inv, err := r.startAttemptLocked(s, spec.Key, "Attach")
	if err != nil {
		r.mu.Unlock()
		return ref, err
	}
	id := astate.Identity
	astate.Controller = true
	err = r.commitStartedLocked(&astate, &inv, func() {})
	r.mu.Unlock()
	op, done := r.operationContext(ctx)
	defer done()
	timeoutCause := newFailure(TimedOut, "attempt", "attempt deadline exceeded")
	timeoutCause.Origin = OriginAttemptDeadline
	timed, cancel := context.WithDeadlineCause(op, astate.StartedAt.Add(r.definition.Policy.AttemptTimeout), timeoutCause)
	defer cancel()
	attemptCtx := context.WithValue(timed, attemptContextKey{}, attemptOwner{Run: r, Identity: id})
	var attempt *contract.Attempt
	defer func() {
		if err != nil {
			f := normalize(err, "Attach")
			f.RunID, f.StepID, f.AttemptID = id.RunID, id.InvocationID, id.AttemptID
			f.DispatchAccepted = AcceptedNo
			err = r.recordError(f)
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		err = r.finishAttemptLocked(s, attempt, astate, inv, err, "Attach", func() {})
	}()
	if err != nil {
		return
	}
	request := contract.Request{Identity: id, Prompt: "Controller artifact: produced by the Controller, never dispatched to an agent", Output: contract.OutputSpec{SchemaID: spec.Output.SchemaID}}
	attempt, err = r.store.BeginAttempt(id, request)
	if err != nil {
		return
	}
	r.mu.Lock()
	err = r.storageLocked(attemptStepPath(id), "step", inv, id)
	r.mu.Unlock()
	if err != nil {
		return
	}
	if err = attemptError(attemptCtx, context.Cause(attemptCtx)); err != nil {
		return
	}
	if err = attempt.WriteControllerCandidate(data, spec.Files); err != nil {
		return
	}
	staged, e := attempt.Stage(attemptCtx, spec.Output)
	if e != nil {
		err = attemptError(attemptCtx, e)
		return
	}
	defer func() {
		if e := staged.Discard(); e != nil {
			_ = r.recordError(e)
			if err == nil {
				err = e
			}
		}
	}()
	published, e := attempt.Publish(attemptCtx, staged)
	if e != nil {
		err = attemptError(attemptCtx, e)
		return
	}
	if r.afterPublish != nil {
		r.afterPublish(published)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err = attemptError(attemptCtx, s.checkLocked(attemptCtx)); err != nil {
		return
	}
	if err = r.succeedAttemptLocked(s, attempt, &astate, &inv, published, func() {}); err != nil {
		return
	}
	return published, nil
}

// ControllerAttached reports whether ref is an exact committed publication
// that the Controller produced with Attach. An Agent Step output never is,
// whatever its schema or file names.
func (r *Run) ControllerAttached(ref contract.Ref) bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.publications[ref.AttemptID]
	return ok && p.Ref == ref && r.state.Attempts[ref.AttemptID].Controller
}

func attemptStepPath(id contract.Identity) string {
	return filepath.Join("steps", id.InvocationID, "step.json")
}
