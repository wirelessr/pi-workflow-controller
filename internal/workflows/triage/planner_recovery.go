package triage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

type RecoveryPolicy struct {
	PlannerRetries int `json:"planner_retries"`
}

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

type RecoveryDelivery struct {
	ID       string               `json:"id"`
	Kind     string               `json:"kind"`
	Proposal contract.Ref         `json:"proposal"`
	Context  contract.Ref         `json:"context"`
	Results  []contract.Ref       `json:"results"`
	Failures []RecoveryFailure    `json:"failures"`
	Support  *supportContinuation `json:"support,omitempty"`
}

type PlannerRecovery struct {
	DispatchCycle   int                `json:"dispatch_cycle"`
	Policy          RecoveryPolicy     `json:"policy"`
	Deliveries      []RecoveryDelivery `json:"deliveries"`
	PlannerFailures []RecoveryFailure  `json:"planner_failures"`
}

// Safety is an evidence-backed Agent declaration, not a Controller verdict
// about remote jobs. A new task ID does not resolve an outstanding operation.
type RecoveryChoice struct {
	DeliveryID string     `json:"delivery_id"`
	Action     string     `json:"action"`
	Reason     string     `json:"reason"`
	Basis      []Evidence `json:"basis"`
}

const recoveryRequirements = `Copy recovery exactly, including its complete delivery/failure history. dispatch_cycle includes complete verification deliveries when M5 is enabled. One workers delivery is one round, including all-failed batches; consumed_batch still contains only the genuine new worker_results. Explicitly report hypothesis changes or an empty changes array. Timeout/compaction is execution failure, never incident disproof. Recovery metadata and pending phase refs are not a complete supporting context. Retain exact owners. For every unresolved failed delivery, declare recovery_choices with delivery_id, action (inspect, resume, redirect), reason and evidence basis. Unknown remote job status forbids blindly resubmitting work, even with another task ID. Inspect means use declared general workers only for authorized read-only status/evidence inspection, never restarting the uncertain operation. Resume requires evidence-backed safety established by the Agent; explain actual remote job status and why continuation is safe. Redirect explains an evidenced alternative which does not repeat uncertain work. A caller flag, local process exit or another task ID is not safety evidence. Support resume uses the original proposal and only unfinished phases, never reacquires accepted intake/wiki or promotes them to a complete context. Planner itself only reads supplied inputs; use workers for any new inspection. Yield/plan may retain pending failures without a safety choice.`

type taskFailure struct {
	cause    error
	handle   *engine.SessionHandle
	identity runtime.Identity
	stage    string
	attempt  string
}

func (f *taskFailure) Error() string { return f.cause.Error() }
func (f *taskFailure) Unwrap() error { return f.cause }

// Inspect each classified branch. Never let a timeout cause downgrade a
// storage/contract wrapper, or treat FailFastSibling as a user retry request.
func recoverable(err error, sibling bool) bool {
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
			if !recoverable(child, sibling) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return recoverable(e.Unwrap(), sibling)
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

func sameIdentity(a, b runtime.Identity) bool {
	at, bt := a.SpawnTime, b.SpawnTime
	a.SpawnTime, b.SpawnTime = time.Time{}, time.Time{}
	return a == b && at.Equal(bt)
}

func confirmedFailureCleanup(r *engine.Run, err error) bool {
	switch e := err.(type) {
	case *engine.Failure:
		if e.Cleanup != nil {
			owner, ok := r.Snapshot().Sessions[e.HandleID]
			if !ok || owner.Identity.SessionID == "" || !sameIdentity(e.Cleanup.Identity, owner.Identity) || !e.Cleanup.ConfirmsLocalClose(owner.Identity.SessionID) {
				return false
			}
		}
		return confirmedFailureCleanup(r, e.Cause)
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			if !confirmedFailureCleanup(r, child) {
				return false
			}
		}
	case interface{ Unwrap() error }:
		return confirmedFailureCleanup(r, e.Unwrap())
	}
	return true
}

// Keep the current boundary first; a previously recovered timeout must not
// reclassify a later validation, cancellation, storage or cleanup failure.
func recoveryError(current error, previous ...error) error {
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

func confirmRecovery(ctx context.Context, r *engine.Run, err error, sibling bool) (RecoveryFailure, error) {
	return confirmTaskRecovery(ctx, r, err, sibling, contract.Ref{})
}

// A nonzero output is a committed sibling interrupted during close, not a
// failed attempt. Both paths retain the dispatch owner and strict-close gate.
func confirmTaskRecovery(ctx context.Context, r *engine.Run, err error, sibling bool, output contract.Ref) (RecoveryFailure, error) {
	var result RecoveryFailure
	if context.Cause(ctx) != nil {
		return result, recoveryError(context.Cause(ctx), err)
	}
	if !recoverable(err, sibling) || !confirmedFailureCleanup(r, err) {
		return result, err
	}
	var f *engine.Failure
	if !errors.As(err, &f) {
		return result, err
	}
	result = RecoveryFailure{Code: f.Code, Origin: f.Origin, Dispatch: f.DispatchAccepted, RunID: f.RunID, StepID: f.StepID, AttemptID: f.AttemptID, Diagnostic: err.Error()}
	task, owned := err.(*taskFailure)
	if !owned {
		return result, err
	}
	committed := output != (contract.Ref{})
	if committed && (!sibling || f.Code != engine.Cancelled || f.Origin != engine.OriginFailFastSibling) {
		return result, err
	}
	undispatched := !committed && f.Code == engine.Cancelled && f.Origin == engine.OriginFailFastSibling && f.AttemptID == "" && task.attempt == ""
	if task.handle == nil {
		if !undispatched {
			return result, err
		}
		result.Stage = task.stage
		return result, nil
	}
	identity, e := r.SessionIdentity(ctx, task.handle)
	if e != nil {
		return result, recoveryError(e, err)
	}
	if !sameIdentity(identity, task.identity) || identity.SessionID == "" {
		return result, recoveryError(fmt.Errorf("recovery identity mismatch"), err)
	}
	var execution *runtime.Execution
	if committed {
		attempt, ok := r.Snapshot().Attempts[task.attempt]
		if !ok || task.attempt != output.AttemptID || output.RunID != r.ID() || attempt.Identity.RunID != output.RunID || attempt.Identity.AttemptID != output.AttemptID || attempt.HandleID != identity.HandleID || attempt.State != engine.Succeeded || attempt.Failure != nil || attempt.Output == nil || *attempt.Output != output || attempt.Execution == nil || attempt.Execution.SessionID != identity.SessionID {
			return result, recoveryError(fmt.Errorf("recovery requires exact succeeded sibling output and owner"), err)
		}
	} else if !undispatched {
		attempt, ok := r.Snapshot().Attempts[task.attempt]
		if !ok || attempt.HandleID != identity.HandleID || f.HandleID != identity.HandleID || attempt.Identity.AttemptID != f.AttemptID || attempt.Identity.RunID != f.RunID || attempt.Identity.InvocationID != f.StepID || attempt.Output != nil {
			return result, recoveryError(fmt.Errorf("recovery requires exact failed attempt identity without committed output"), err)
		}
		execution = attempt.Execution
	}
	report, e := r.CloseSessionReport(ctx, task.handle)
	if e != nil {
		return result, recoveryError(e, err)
	}
	if !sameIdentity(report.Identity, identity) || !report.ConfirmsLocalClose(identity.SessionID) {
		return result, recoveryError(&engine.Failure{Code: engine.CleanupFailed, Origin: engine.OriginProtocol, Phase: "triage-recovery", Message: "recovery cleanup not confirmed", Cleanup: &report}, err)
	}
	if committed {
		return RecoveryFailure{}, nil
	}
	result.Stage, result.Identity, result.Execution, result.Cleanup = task.stage, identity, execution, &report
	return result, nil
}

func newDelivery(kind string, p *plannerCaller) RecoveryDelivery {
	return RecoveryDelivery{ID: contract.NewID(), Kind: kind, Proposal: *p.last, Context: p.history.ref, Results: []contract.Ref{}, Failures: []RecoveryFailure{}}
}

func (p *plannerCaller) recoveryTask() *PlannerRecovery {
	if p.recovery == nil {
		return nil
	}
	copy := *p.recovery
	copy.DispatchCycle = len(copy.Deliveries) + verificationDeliveryCount(p.verification)
	copy.Deliveries = slices.Clone(p.recovery.Deliveries)
	copy.PlannerFailures = slices.Clone(p.recovery.PlannerFailures)
	return &copy
}

func recoverySuffix(v, prior PlannerState) ([]RecoveryDelivery, error) {
	if v.Recovery == nil {
		if prior.Recovery != nil {
			return nil, fmt.Errorf("planner cannot discard recovery metadata")
		}
		return nil, nil
	}
	if len(v.WorkerResults) < len(prior.WorkerResults) {
		return nil, fmt.Errorf("worker history shortened")
	}
	if v.Recovery.Policy.PlannerRetries < 0 {
		return nil, fmt.Errorf("explicit nonnegative planner retry budget required")
	}
	var retained []RecoveryDelivery
	var failures []RecoveryFailure
	if prior.Recovery != nil {
		if v.Recovery.Policy != prior.Recovery.Policy {
			return nil, fmt.Errorf("recovery policy changed")
		}
		retained, failures = prior.Recovery.Deliveries, prior.Recovery.PlannerFailures
	}
	if len(v.Recovery.Deliveries) < len(retained) || recoveryJSON(v.Recovery.Deliveries[:len(retained)]) != recoveryJSON(retained) && len(retained) > 0 || len(v.Recovery.PlannerFailures) < len(failures) || len(failures) > 0 && recoveryJSON(v.Recovery.PlannerFailures[:len(failures)]) != recoveryJSON(failures) {
		return nil, fmt.Errorf("recovery history prefix changed")
	}
	suffix := v.Recovery.Deliveries[len(retained):]
	if len(suffix) > 1 {
		return nil, fmt.Errorf("only one dispatch delivery may enter a snapshot")
	}
	if len(v.WikiResults) < len(prior.WikiResults) {
		return nil, fmt.Errorf("wiki history shortened")
	}
	workerBatch := v.WorkerResults[len(prior.WorkerResults):]
	wikiBatch := v.WikiResults[len(prior.WikiResults):]
	changedContext := v.Previous != nil && v.Context != prior.Context
	if len(suffix) == 0 && (len(workerBatch) > 0 || len(wikiBatch) > 0 || changedContext) {
		return nil, fmt.Errorf("results require explicit dispatch delivery")
	}
	seen := map[string]bool{}
	for _, d := range v.Recovery.Deliveries {
		if !nonblank(d.ID) || seen[d.ID] {
			return nil, fmt.Errorf("invalid delivery identity")
		}
		seen[d.ID] = true
		if d.Kind != "workers" && d.Kind != "wiki" && d.Kind != "support" {
			return nil, fmt.Errorf("invalid delivery kind")
		}
	}
	for _, d := range suffix {
		if v.Previous == nil || d.Proposal != *v.Previous || d.Context != prior.Context {
			return nil, fmt.Errorf("delivery proposal/context mismatch")
		}
		if d.Kind == "workers" && (!slices.Equal(d.Results, workerBatch) || len(d.Results)+len(d.Failures) == 0 || len(d.Results)+len(d.Failures) > 3 || len(wikiBatch) > 0 || changedContext) {
			return nil, fmt.Errorf("worker delivery differs from accepted batch")
		}
		if d.Kind == "wiki" && (!slices.Equal(d.Results, wikiBatch) || len(d.Results)+len(d.Failures) != 1 || len(workerBatch) > 0 || changedContext) {
			return nil, fmt.Errorf("wiki delivery differs from accepted search")
		}
		if d.Kind == "support" {
			if d.Support == nil || len(workerBatch) > 0 || len(wikiBatch) > 0 || len(d.Results)+len(d.Failures) != 1 {
				return nil, fmt.Errorf("invalid support phase delivery")
			}
			if changedContext != (len(d.Results) == 1) || len(d.Results) == 1 && d.Results[0] != v.Context {
				return nil, fmt.Errorf("partial support refs cannot replace context")
			}
		}
	}
	return suffix, nil
}

func (a *acceptance) checkRecoveryFailure(f RecoveryFailure) error {
	snapshot := a.run.Snapshot()
	if f.Code == engine.Cancelled && f.Origin == engine.OriginFailFastSibling && f.AttemptID == "" {
		if f.Execution != nil || !nonblank(f.Stage) {
			return fmt.Errorf("undispatched sibling has fabricated execution")
		}
		if f.Identity == (runtime.Identity{}) {
			if f.Cleanup != nil {
				return fmt.Errorf("cleanup without owned sibling handle")
			}
			return nil
		}
		owner, ok := snapshot.Sessions[f.Identity.HandleID]
		if !ok || !sameIdentity(owner.Identity, f.Identity) || f.Identity.SessionID == "" || f.Cleanup == nil || !sameIdentity(f.Cleanup.Identity, f.Identity) || !f.Cleanup.ConfirmsLocalClose(f.Identity.SessionID) {
			return fmt.Errorf("undispatched owned sibling cleanup not confirmed")
		}
		return nil
	}
	attempt, ok := snapshot.Attempts[f.AttemptID]
	owner, owned := snapshot.Sessions[f.Identity.HandleID]
	if !ok || !owned || attempt.Failure == nil || attempt.Output != nil || f.RunID != a.run.ID() || attempt.Identity.InvocationID != f.StepID || attempt.HandleID != f.Identity.HandleID || !sameIdentity(owner.Identity, f.Identity) || f.Identity.SessionID == "" || attempt.Failure.Code != f.Code || attempt.Failure.Origin != f.Origin || attempt.DispatchAccepted != f.Dispatch || !reflect.DeepEqual(attempt.Execution, f.Execution) || f.Cleanup == nil || !sameIdentity(f.Cleanup.Identity, f.Identity) || !f.Cleanup.ConfirmsLocalClose(f.Identity.SessionID) {
		return fmt.Errorf("recovery failure differs from owned failed attempt/cleanup")
	}
	if !(f.Code == engine.TimedOut && f.Origin == engine.OriginAttemptDeadline || f.Code == engine.CompactionFailed && f.Origin == engine.OriginCompaction || f.Code == engine.Cancelled && f.Origin == engine.OriginFailFastSibling) {
		return fmt.Errorf("unapproved recovery failure")
	}
	return nil
}

func (a *acceptance) checkRecovery(v, prior PlannerState, sources map[contract.Ref][]file) error {
	if _, err := recoverySuffix(v, prior); err != nil {
		return err
	}
	if v.Recovery == nil {
		if len(v.RecoveryChoices) > 0 {
			return fmt.Errorf("recovery choice without recovery metadata")
		}
		return nil
	}
	if v.Recovery.DispatchCycle != len(v.Recovery.Deliveries)+verificationDeliveryCount(v.Verification) {
		return fmt.Errorf("recovery dispatch cycle differs from delivered history")
	}
	for _, f := range v.Recovery.PlannerFailures {
		if f.Stage != "planner" || f.Origin == engine.OriginFailFastSibling {
			return fmt.Errorf("invalid Planner recovery failure")
		}
		if err := a.checkRecoveryFailure(f); err != nil {
			return err
		}
	}
	for _, d := range v.Recovery.Deliveries {
		for _, f := range d.Failures {
			if err := a.checkRecoveryFailure(f); err != nil {
				return err
			}
		}
	}
	known := map[string]RecoveryDelivery{}
	resolved := map[string]bool{}
	for ref := v.Previous; ref != nil; {
		p, err := readAccepted[PlannerState](a, *ref, PlannerSchema)
		if err != nil {
			return err
		}
		for _, c := range p.Data.RecoveryChoices {
			if c.Action == "resume" || c.Action == "redirect" {
				resolved[c.DeliveryID] = true
			}
		}
		ref = p.Data.Previous
	}
	for _, d := range v.Recovery.Deliveries {
		if len(d.Failures) > 0 && !resolved[d.ID] {
			known[d.ID] = d
		}
	}
	choices := map[string]bool{}
	for _, c := range v.RecoveryChoices {
		d, ok := known[c.DeliveryID]
		if !ok || choices[c.DeliveryID] || !nonblank(c.Reason) || len(c.Basis) == 0 {
			return fmt.Errorf("recovery choice requires unresolved delivery, reason and evidence basis")
		}
		choices[c.DeliveryID] = true
		if err := checkInvestigationBasis(c.Basis, sources); err != nil {
			return err
		}
		switch c.Action {
		case "inspect":
			if v.Ledger == nil || v.Ledger.Action != "workers" {
				return fmt.Errorf("recovery inspection requires explicit read-only workers")
			}
		case "resume":
			original, err := readAccepted[PlannerState](a, d.Proposal, PlannerSchema)
			if err != nil {
				return err
			}
			if d.Kind == "wiki" && (d.Context != v.Context || !reflect.DeepEqual(v.WikiTask, original.Data.WikiTask)) {
				return fmt.Errorf("wiki resume must retain original task")
			}
			if d.Kind == "support" && (d.Support == nil || d.Context != v.Context || !reflect.DeepEqual(v.SupportingWork, &d.Support.Work)) {
				return fmt.Errorf("support resume must retain original task and context")
			}
			if v.Ledger == nil || d.Kind == "workers" && v.Ledger.Action != "workers" || d.Kind == "support" && (v.Ledger.Action != "support" || d.Support == nil) || d.Kind == "wiki" && v.Ledger.Action != "wiki" && v.Ledger.Action != "reframe" {
				return fmt.Errorf("recovery resume action mismatch")
			}
		case "redirect":
		default:
			return fmt.Errorf("unsupported recovery choice")
		}
	}
	if v.Ledger != nil && v.Ledger.Action != "plan" && v.Ledger.Action != "yield" {
		for id := range known {
			if !choices[id] {
				return fmt.Errorf("unresolved remote work requires explicit safe continuation or inspection")
			}
		}
	}
	return nil
}

func (p *plannerCaller) planningStep(ctx context.Context) (contract.Ref, error) {
	if p.recovery == nil {
		return p.step(ctx)
	}
	ref, _, err := retryPlannerInputs(ctx, p.r, p.r.Root(), "planner-recovery-"+contract.NewID(), "planner", p.recovery.Policy.PlannerRetries, p.stepInScope, p.continuePlannerInputs)
	return ref, err
}

func (p *plannerCaller) continuePlannerInputs(ctx context.Context, failure RecoveryFailure, again bool) error {
	p.recovery.PlannerFailures = append(p.recovery.PlannerFailures, failure)
	if again {
		next, err := p.reopen(ctx, p.history.ref)
		if err != nil {
			return err
		}
		*p = *next
	}
	return nil
}

// Only supplied-input work may use this seam. Recovery never authorizes a
// remote operation, and RetryState feedback is not an input to the next task.
func retryPlannerInputs(ctx context.Context, r *engine.Run, scope *engine.Scope, key, output string, retries int, run func(context.Context, *engine.Scope) (contract.Ref, error), recovered func(context.Context, RecoveryFailure, bool) error) (contract.Ref, []RecoveryFailure, error) {
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
		failure, e := confirmRecovery(ctx, r, err, false)
		if e != nil {
			return engine.RetryAction{}, e
		}
		failures = append(failures, failure)
		if recovered != nil {
			if e := recovered(ctx, failure, state.RetryCount < state.MaxRetries); e != nil {
				return engine.RetryAction{}, recoveryError(e, err)
			}
		}
		return engine.RetryAction{Again: true, Feedback: &engine.Feedback{Message: failure.Diagnostic, SourceAttemptID: failure.AttemptID, SourceCode: string(failure.Code)}}, nil
	})
	if err != nil {
		return contract.Ref{}, failures, recoveryError(err, causes...)
	}
	return ref, failures, nil
}

func appendUniqueRefs(refs []contract.Ref, more ...contract.Ref) []contract.Ref {
	for _, ref := range more {
		if ref != (contract.Ref{}) && !slices.Contains(refs, ref) {
			refs = append(refs, ref)
		}
	}
	return refs
}

func (p *plannerCaller) receiveWorkers(ctx context.Context, prepared []preparedWorker, joined []engine.BranchResult, groupErr error) (int, error) {
	var failures []error
	if groupErr != nil && !recoverable(groupErr, true) {
		failures = append(failures, groupErr)
	}
	for _, branch := range joined {
		if branch.Err != nil && (!recoverable(branch.Err, true) || !confirmedFailureCleanup(p.r, branch.Err)) {
			failures = append(failures, branch.Err)
		}
	}
	if len(failures) > 0 {
		failures = append(failures, groupErr)
		for _, branch := range joined {
			failures = append(failures, branch.Err)
		}
		return 0, errors.Join(failures...)
	}
	if len(joined) != len(prepared) {
		return 0, errors.Join(groupErr, fmt.Errorf("worker join incomplete"))
	}
	delivery := newDelivery("workers", p)
	primary := false
	for i, branch := range joined {
		if branch.Err == nil || branch.Result.Outputs["worker"] != (contract.Ref{}) {
			continue
		}
		failure, err := confirmRecovery(ctx, p.r, branch.Err, true)
		if err != nil {
			return 0, err
		}
		failure.TaskID = prepared[i].request.Task.ID
		delivery.Failures = append(delivery.Failures, failure)
		p.recoveryErrors = append(p.recoveryErrors, branch.Err)
		if failure.Origin != engine.OriginFailFastSibling {
			primary = true
		}
	}
	if len(delivery.Failures) > 0 && !primary {
		return 0, errors.Join(groupErr, fmt.Errorf("sibling cancellation without recoverable primary"))
	}
	accepted := slices.Clone(p.workerResults)
	for i, branch := range joined {
		if branch.Err != nil && branch.Result.Outputs["worker"] == (contract.Ref{}) {
			continue
		}
		ref := branch.Result.Outputs["worker"]
		if branch.Err != nil {
			var failure *engine.Failure
			if !primary || !errors.As(branch.Err, &failure) || failure.Code != engine.Cancelled || failure.Origin != engine.OriginFailFastSibling {
				return 0, errors.Join(branch.Err, fmt.Errorf("committed sibling requires recoverable primary and FailFastSibling"))
			}
			if _, owned := branch.Err.(*taskFailure); owned {
				if _, err := confirmTaskRecovery(ctx, p.r, branch.Err, true, ref); err != nil {
					return 0, err
				}
			} else {
				// Parallel can cancel result validation after runWorker has
				// already completed its strict close successfully.
				snapshot := p.r.Snapshot()
				attempt, ok := snapshot.Attempts[ref.AttemptID]
				owner, hasOwner := snapshot.Sessions[attempt.HandleID]
				if !ok || !hasOwner || attempt.State != engine.Succeeded || attempt.Output == nil || *attempt.Output != ref || attempt.Failure != nil || owner.State != "Closed" || attempt.Execution == nil || attempt.Execution.SessionID != owner.Identity.SessionID {
					return 0, recoveryError(fmt.Errorf("sibling result requires exact succeeded output and closed owner"), branch.Err)
				}
			}
		}
		if err := acceptWorker(ctx, p.r, p.scope, *p.last, accepted, prepared[i], ref); err != nil {
			return 0, err
		}
		accepted = append(accepted, ref)
		delivery.Results = append(delivery.Results, ref)
	}
	p.workerResults, p.stopped = accepted, false
	p.recovery.Deliveries = append(p.recovery.Deliveries, delivery)
	return len(joined), nil
}

func recoveryJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
