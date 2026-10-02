package engine

import (
	"context"
	"encoding/json"
	"path/filepath"
	"time"

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
	Inputs []contract.Ref
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
	if r.totalAttempts >= r.definition.Policy.MaxTotalAttempts {
		f := newFailure(LimitExceeded, "Attach", "run attempt limit exceeded")
		f.LimitScope = "run"
		r.stopLocked(f)
		r.mu.Unlock()
		return ref, f
	}
	s.steps[spec.Key] = true
	r.totalAttempts++
	key := s.path + "/" + spec.Key
	invocation := r.invocations[key]
	if invocation == "" {
		invocation = contract.NewID()
		r.invocations[key] = invocation
	}
	inv := r.state.Invocations[invocation]
	// A token keeps the Store identity invariants; it is never dispatched.
	id := contract.Identity{RunID: r.ID(), InvocationID: invocation, AttemptID: contract.NewID(), DispatchToken: contract.NewID()}
	started := time.Now()
	astate := AttemptState{Identity: id, Scope: s.path, Key: spec.Key, Epoch: s.epoch, Controller: true, Number: inv.Attempts + 1, State: Preparing, StartedAt: started, DispatchAccepted: AcceptedNo}
	inv = InvocationState{ID: invocation, Scope: s.path, Key: spec.Key, Epoch: s.epoch, State: Running, LastAttemptID: id.AttemptID, Attempts: astate.Number, RetryActivationIDs: append([]string(nil), s.ancestors...)}
	r.retryAncestors[invocation] = append([]string(nil), s.ancestors...)
	err = r.commitLocked("AttemptStarted", astate, func(seq uint64) {
		astate.LastSeq = seq
		inv.LastSeq = seq
		r.state.Attempts[id.AttemptID] = astate
		r.state.Invocations[invocation] = inv
	}, id)
	r.mu.Unlock()
	op, done := r.operationContext(ctx)
	defer done()
	timeoutCause := newFailure(TimedOut, "attempt", "attempt deadline exceeded")
	timeoutCause.Origin = OriginAttemptDeadline
	timed, cancel := context.WithDeadlineCause(op, started.Add(r.definition.Policy.AttemptTimeout), timeoutCause)
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
	request := contract.Request{Identity: id, Prompt: "Controller artifact: produced by the Controller, never dispatched to an agent", Inputs: spec.Inputs, Output: contract.OutputSpec{SchemaID: spec.Output.SchemaID}}
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
	for _, input := range spec.Inputs {
		if _, err = r.resolve(attemptCtx, input); err != nil {
			err = attemptError(attemptCtx, err)
			return
		}
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
	if !ok || p.Ref != ref {
		return false
	}
	a, ok := r.state.Attempts[ref.AttemptID]
	return ok && a.Controller && a.HandleID == "" && a.State == Succeeded && a.Output != nil && *a.Output == ref
}

func attemptStepPath(id contract.Identity) string {
	return filepath.Join("steps", id.InvocationID, "step.json")
}
