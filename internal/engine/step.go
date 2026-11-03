package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

// feedbackDispatchNote marks a dispatch whose request carries feedback.
const feedbackDispatchNote = "This request carries request.feedback: read request.feedback.message before starting; it says why this attempt runs or what it must address."

// taskInstruction opens every dispatch's instructions unless a repair of a
// RepairKeepsWork Step replaces it with repairKeepsWorkInstruction, because
// redoing such a task would repeat its writes.
const (
	taskInstruction            = "First execute the task in request.prompt, using its inputs and feedback. Do not skip the task to construct an output. After the task finishes, read"
	repairKeepsWorkInstruction = "This is a contract repair: do not add or repeat any external write or local commit; only fix the violations request.feedback.message reports by rewriting this attempt's contract. Read"
)

func (s *Scope) Step(ctx context.Context, spec StepSpec) (StepResult, error) {
	return s.step(ctx, spec, false)
}

// step is Step; repair marks a StepWithRepair repair dispatch.
func (s *Scope) step(ctx context.Context, spec StepSpec, repair bool) (result StepResult, err error) {
	if s == nil || s.run == nil {
		return result, newFailure(InvalidDefinition, "Step", "scope is not initialized")
	}
	r := s.run
	r.mu.Lock()
	if err = s.checkLocked(ctx); err != nil {
		r.mu.Unlock()
		return
	}
	h := spec.Session
	if !validName(spec.Key) || s.steps[spec.Key] || !r.schemas.Has(spec.Output.SchemaID) || spec.Timeout < 0 || h == nil || h.run != r || h.unusable || h.closing.Load() || h.session == nil {
		r.mu.Unlock()
		return result, newFailure(InvalidDefinition, "Step", "invalid key, schema, timeout or handle")
	}
	if h.busy {
		r.mu.Unlock()
		return result, newFailure(SessionBusy, "Step", "session already leased")
	}
	astate, inv, err := r.startAttemptLocked(s, spec.Key, "Step")
	if err != nil {
		r.mu.Unlock()
		return result, err
	}
	h.busy = true
	id := astate.Identity
	started := astate.StartedAt
	timeout := spec.Timeout
	if timeout == 0 {
		timeout = r.definition.Policy.AttemptTimeout
	}
	astate.HandleID = h.id
	if spec.Feedback != nil {
		copy := *spec.Feedback
		copy.Refs = append([]contract.Ref(nil), copy.Refs...)
		astate.Feedback = &copy
	}
	err = r.commitStartedLocked(&astate, &inv, func() {
		v := r.state.Sessions[h.id]
		v.State = "Busy"
		r.state.Sessions[h.id] = v
	})
	r.mu.Unlock()
	result.AttemptID = id.AttemptID
	op, done := r.operationContext(ctx)
	defer done()
	timeoutCause := newFailure(TimedOut, "attempt", "attempt deadline exceeded")
	timeoutCause.Origin = OriginAttemptDeadline
	work, stopWork := context.WithCancelCause(op)
	defer stopWork(context.Canceled)
	timed, cancel := context.WithDeadlineCause(work, started.Add(timeout), timeoutCause)
	attemptCtx := context.WithValue(timed, attemptContextKey{}, attemptOwner{Run: r, Identity: id, HandleID: h.id})
	r.ownerMu.Lock()
	h.stopAttempt = stopWork
	if r.resourcesStopped {
		stopWork(context.Cause(r.ctx))
	}
	r.ownerMu.Unlock()
	defer cancel()
	var attempt *contract.Attempt
	var receipt runtime.Execution
	dispatched, settled := false, false
	defer func() {
		if err != nil {
			f := normalize(err, "Step")
			f.RunID = id.RunID
			f.StepID = id.InvocationID
			f.AttemptID = id.AttemptID
			f.HandleID = h.id
			if settled {
				f.DispatchAccepted = AcceptedYes
			} else if !dispatched {
				f.DispatchAccepted = AcceptedNo
			}
			r.mu.Lock()
			if r.state.Attempts[id.AttemptID].DispatchAccepted == AcceptedYes {
				f.DispatchAccepted = AcceptedYes
			}
			r.mu.Unlock()
			err = r.recordError(f)
			if !r.keepHandle(attemptCtx, h, f, dispatched, settled) {
				_ = r.closeHandle(h)
			}
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.ownerMu.Lock()
		h.stopAttempt = nil
		r.ownerMu.Unlock()
		h.busy = false
		v := r.state.Sessions[h.id]
		if v.State == "Busy" {
			v.State = "Idle"
		}
		if receipt.Token != "" {
			astate.Execution = &receipt
		}
		err = r.finishAttemptLocked(s, attempt, astate, inv, err, "Step", func() { r.state.Sessions[h.id] = v })
	}()
	if err != nil {
		return
	}
	request := contract.Request{Identity: id, Prompt: spec.Prompt, Inputs: spec.Inputs, Feedback: spec.Feedback, Output: contract.OutputSpec{SchemaID: spec.Output.SchemaID}}
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
	for _, ref := range spec.Inputs {
		if _, err = r.resolve(attemptCtx, ref); err != nil {
			err = attemptError(attemptCtx, err)
			return
		}
	}
	if spec.Feedback != nil {
		if err = r.validateFeedback(attemptCtx, spec.Feedback); err != nil {
			err = attemptError(attemptCtx, err)
			return
		}
	}
	if err = attemptError(attemptCtx, context.Cause(attemptCtx)); err != nil {
		return
	}
	r.mu.Lock()
	err = attemptError(attemptCtx, s.checkLocked(attemptCtx))
	if err == nil {
		err = r.commitLocked("AttemptWaitingSession", id, func(seq uint64) {
			astate.State = WaitingSession
			astate.LastSeq = seq
			r.state.Attempts[id.AttemptID] = astate
		}, id)
	}
	r.mu.Unlock()
	if err != nil {
		return
	}
	feedbackNote := ""
	if spec.Feedback != nil {
		// A live repair reran an unchanged request after diffing only its
		// prompt against the previous one and never read the feedback.
		feedbackNote = feedbackDispatchNote + "\n"
	}
	instruction := taskInstruction
	if repair && spec.RepairKeepsWork {
		instruction = repairKeepsWorkInstruction
	}
	message := fmt.Sprintf("Controller dispatch %s\nRead request JSON: %s\n%s%s output.schema.path, output.envelope.path and resources. Write evidence files into %s and artifact files into %s, creating either directory if absent; each files[] entry pairs a unique id with that file's path relative to the attempt directory. The envelope's data holds the task result. Write the complete envelope with exactly request.identity and output.schema_id to: %s\nWrite only this attempt's candidate; do not modify published inputs.\n", id.DispatchToken, attempt.RequestPath(), feedbackNote, instruction, filepath.Join(attempt.Dir(), "evidence"), filepath.Join(attempt.Dir(), "artifacts"), attempt.CandidatePath())
	dispatch := runtime.Dispatch{Token: id.DispatchToken, Message: message}
	if spec.Observe {
		sink := r.newEntrySink(h.id, id.AttemptID)
		dispatch.Entries = sink
		defer func() { result.Observation = sink.finish() }()
	}
	dispatched = true
	receipt, err = h.session.Execute(attemptCtx, dispatch)
	err = attemptError(attemptCtx, err)
	if err != nil {
		return
	}
	identity := h.session.Identity()
	if receipt.Token != id.DispatchToken || receipt.SessionID != identity.SessionID || receipt.SettledSeq <= receipt.StartSeq || receipt.PromptEntryID == "" || receipt.LastEntryID == "" || receipt.LastAssistantID == "" || receipt.StopReason != "stop" {
		err = &Failure{Code: AmbiguousExecution, Phase: "Execute", Message: "runtime returned incomplete execution evidence", Origin: OriginProtocol, DispatchAccepted: AcceptedUnknown}
		return
	}
	settled = true
	r.mu.Lock()
	err = attemptError(attemptCtx, s.checkLocked(attemptCtx))
	if err == nil {
		err = r.commitLocked("AttemptSettled", receipt, func(seq uint64) {
			astate.State = Validating
			astate.Execution = &receipt
			astate.DispatchAccepted = AcceptedYes
			astate.LastSeq = seq
			r.state.Attempts[id.AttemptID] = astate
		}, id)
	}
	r.mu.Unlock()
	if err != nil {
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
	confirmation, e := h.session.Confirm(attemptCtx, receipt)
	if e != nil {
		err = attemptError(attemptCtx, e)
		settled = false
		return
	}
	if confirmation.ActivityEpoch != receipt.ActivityEpoch || confirmation.Seq < receipt.SettledSeq {
		err = &Failure{Code: AmbiguousExecution, Phase: "Confirm", Message: "confirmation does not match execution epoch", Origin: OriginProtocol, DispatchAccepted: AcceptedYes}
		settled = false
		return
	}
	if err = attemptError(attemptCtx, context.Cause(attemptCtx)); err != nil {
		return
	}
	ref, e := attempt.Publish(attemptCtx, staged)
	if e != nil {
		err = attemptError(attemptCtx, e)
		return
	}
	if r.afterPublish != nil {
		r.afterPublish(ref)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err = attemptError(attemptCtx, s.checkLocked(attemptCtx)); err != nil {
		return
	}
	err = r.succeedAttemptLocked(s, attempt, &astate, &inv, ref, func() {
		v := r.state.Sessions[h.id]
		if v.State == "Busy" {
			v.State = "Idle"
			r.state.Sessions[h.id] = v
		}
	})
	if err != nil {
		return
	}
	result.Output = ref
	result.Execution = receipt
	return
}

// repairMessage asks for this attempt's contract with the reported
// violations fixed and the rest of the content kept.
func repairMessage(stage, violations string) string {
	return "Previous contract was rejected by " + stage + " validation. Write this attempt's contract, with this request's identity, fixing the reported violations; the content they do not name stays as it was: " + violations
}

// StepWithRepair runs spec and, up to repairs times, dispatches it again on
// the same handle after a ContractInvalid failure or a validate error. A
// repair's Feedback message carries the diagnostic and its Refs are those of
// spec.Feedback; with spec.RepairKeepsWork its dispatch tells the Agent to
// only rewrite the contract, without adding or repeating writes. Every other failure returns
// as is; exhausted repairs return the last ContractInvalid failure or validate
// error unwrapped. Repairs are not a Retry, and every attempt counts against
// the run attempt limit.
//
// Repair n runs in a child scope of s named "contract-repair-<key>" for n = 1
// and "contract-repair-<n>-<key>" after. A name collision surfaces only at the
// repair that needs the name, after earlier attempts ran, so callers avoid
// keys beginning with a number and a dash, and scope names beginning with
// "contract-repair-".
//
// validate, nil meaning schema only, sees every publication. Any error it
// returns, an I/O error included, is taken as a content diagnostic and
// repaired, so a consumer separates execution failures itself instead of
// returning them from validate. A check whose failure must end the Step, such
// as the identity or receipt of a write-type Step's side effect, runs once
// after success, never inside validate. A publication validate rejected stays
// committed.
//
// The results hold one entry per Step call in dispatch order. A Step that
// fails before its attempt starts, for example on a context cancelled during
// validate, still occupies an entry with an empty AttemptID, so the last entry
// is not necessarily the failing attempt; a repair whose Child fails appends
// none. On success the last entry is the accepted one.
func (s *Scope) StepWithRepair(ctx context.Context, spec StepSpec, repairs int, validate func(context.Context, contract.Ref) error) ([]StepResult, error) {
	if repairs < 0 {
		return nil, newFailure(InvalidDefinition, "StepWithRepair", "nonnegative repair budget required")
	}
	var results []StepResult
	scope, attempt := s, spec
	for n := 0; ; n++ {
		if n > 0 {
			name := "contract-repair-" + spec.Key
			if n > 1 {
				name = fmt.Sprintf("contract-repair-%d-%s", n, spec.Key)
			}
			child, err := s.Child(name)
			if err != nil {
				return results, err
			}
			scope = child
		}
		out, err := scope.step(ctx, attempt, n > 0)
		results = append(results, out)
		var feedback *Feedback
		if err == nil {
			if validate == nil {
				return results, nil
			}
			ve := validate(ctx, out.Output)
			if ve == nil {
				return results, nil
			}
			if n >= repairs {
				return results, ve
			}
			feedback = &Feedback{Message: repairMessage("acceptance", ve.Error())}
		} else {
			var failure *Failure
			if !errors.As(err, &failure) || failure.Code != ContractInvalid || n >= repairs {
				return results, err
			}
			feedback = &Feedback{Message: repairMessage("schema", failure.Message), SourceAttemptID: failure.AttemptID, SourceCode: string(failure.Code)}
		}
		if spec.Feedback != nil {
			feedback.Refs = spec.Feedback.Refs
		}
		attempt.Feedback = feedback
	}
}

func (r *Run) keepHandle(ctx context.Context, h *SessionHandle, f *Failure, dispatched, settled bool) bool {
	if fatal(f) {
		return false
	}
	switch f.Code {
	case ContractMissing, ContractInvalid, IdentityMismatch, LimitExceeded:
		return settled
	case ReferenceInvalid:
		return !dispatched || settled
	case DispatchRejected, ProviderFailed, OutputTruncated:
		state, err := h.session.Snapshot(ctx)
		return err == nil && state.Identity == h.session.Identity() && state.Model == h.role.Model && !state.Streaming && !state.Compacting && state.PendingCount == 0 && state.Health == "Online"
	case InvalidDefinition, SessionBusy:
		return !dispatched
	case Cancelled, TimedOut, CompactionFailed, InteractionRequired, ExtensionFailed, ModelChanged, ThinkingChanged, SessionChanged, PromptNotObserved, AmbiguousExecution, ProtocolFailed, RPCUnresponsive, ProcessExited:
		return false
	default:
		return !dispatched || settled
	}
}

// startAttemptLocked consumes one run attempt for key in s and builds its
// Preparing state. The caller has already checked the scope, key and schema.
// At the run attempt limit it stops the run and consumes nothing.
func (r *Run) startAttemptLocked(s *Scope, key, phase string) (AttemptState, InvocationState, error) {
	if r.totalAttempts >= r.definition.Policy.MaxTotalAttempts {
		f := newFailure(LimitExceeded, phase, "run attempt limit exceeded")
		f.LimitScope = "run"
		r.stopLocked(f)
		return AttemptState{}, InvocationState{}, f
	}
	s.steps[key] = true
	r.totalAttempts++
	path := s.path + "/" + key
	invocation := r.invocations[path]
	if invocation == "" {
		invocation = contract.NewID()
		r.invocations[path] = invocation
	}
	inv := r.state.Invocations[invocation]
	id := contract.Identity{RunID: r.ID(), InvocationID: invocation, AttemptID: contract.NewID(), DispatchToken: contract.NewID()}
	astate := AttemptState{Identity: id, Scope: s.path, Key: key, Epoch: s.epoch, Number: inv.Attempts + 1, State: Preparing, StartedAt: time.Now(), DispatchAccepted: AcceptedNo}
	inv = InvocationState{ID: invocation, Scope: s.path, Key: key, Epoch: s.epoch, State: Running, LastAttemptID: id.AttemptID, Attempts: astate.Number, RetryActivationIDs: append([]string(nil), s.ancestors...)}
	r.retryAncestors[invocation] = append([]string(nil), s.ancestors...)
	return astate, inv, nil
}

func (r *Run) commitStartedLocked(astate *AttemptState, inv *InvocationState, apply func()) error {
	id := astate.Identity
	return r.commitLocked("AttemptStarted", *astate, func(seq uint64) {
		astate.LastSeq = seq
		inv.LastSeq = seq
		r.state.Attempts[id.AttemptID] = *astate
		r.state.Invocations[id.InvocationID] = *inv
		apply()
	}, id)
}

// finishAttemptLocked records a terminal failure exactly once. A Succeeded
// attempt was already committed with its Ref, so a later error (for example a
// failed attempt snapshot) never submits a second terminal event.
func (r *Run) finishAttemptLocked(s *Scope, attempt *contract.Attempt, astate AttemptState, inv InvocationState, err error, phase string, apply func()) error {
	id := astate.Identity
	current, exists := r.state.Attempts[id.AttemptID]
	if exists && current.State == Succeeded {
		return err
	}
	if err == nil {
		err = newFailure(WorkflowFailed, phase, "attempt ended without a publication")
	}
	terminal, _ := outcome(err)
	astate.State = terminal
	// A Ref set before a failed success commit was never committed.
	astate.Output = nil
	astate.Failure = failureInfo(err)
	astate.FinishedAt = time.Now()
	astate.DispatchAccepted = normalize(err, "").DispatchAccepted
	inv.State = terminal
	inv.Provisional = terminal
	for _, a := range s.ancestors {
		if r.state.Retries[a].Active {
			inv.State = AwaitingScope
			break
		}
	}
	e := r.commitLocked("Attempt"+string(terminal), map[string]any{"attempt": astate, "invocation": inv}, func(seq uint64) {
		astate.LastSeq = seq
		inv.LastSeq = seq
		r.state.Attempts[id.AttemptID] = astate
		r.state.Invocations[id.InvocationID] = inv
		apply()
	}, id)
	if e != nil {
		r.state.Attempts[id.AttemptID] = astate
		r.state.Invocations[id.InvocationID] = inv
		r.state.StatePersisted = false
		err = e
	}
	if attempt != nil {
		if e = r.storageLocked(attemptSnapshotPath(attempt, id), "attempt_terminal", astate, id); e != nil {
			err = e
		}
	}
	return err
}

// succeedAttemptLocked commits a published Ref. Store.Publish is not
// authority: membership follows the durable journal commit and snapshots.
func (r *Run) succeedAttemptLocked(s *Scope, attempt *contract.Attempt, astate *AttemptState, inv *InvocationState, ref contract.Ref, apply func()) error {
	id := astate.Identity
	astate.State = Succeeded
	astate.Output = &ref
	astate.FinishedAt = time.Now()
	inv.State = Succeeded
	inv.Provisional = Succeeded
	for _, a := range s.ancestors {
		if r.state.Retries[a].Active {
			inv.State = AwaitingScope
			break
		}
	}
	err := r.commitLocked("AttemptSucceeded", map[string]any{"attempt": *astate, "invocation": *inv, "ref": ref}, func(seq uint64) {
		astate.LastSeq = seq
		inv.LastSeq = seq
		r.state.Attempts[id.AttemptID] = *astate
		r.state.Invocations[id.InvocationID] = *inv
		apply()
	}, id)
	if err != nil {
		return err
	}
	if err = r.storageLocked(attemptSnapshotPath(attempt, id), "attempt_terminal", *astate, id); err != nil {
		return err
	}
	r.publications[id.AttemptID] = publication{Ref: ref, Identity: id, Seq: astate.LastSeq}
	return nil
}

func attemptSnapshotPath(attempt *contract.Attempt, id contract.Identity) string {
	return filepath.Join("steps", id.InvocationID, "attempts", fmt.Sprintf("%04d-%s", attempt.Number(), id.AttemptID), "attempt.json")
}
