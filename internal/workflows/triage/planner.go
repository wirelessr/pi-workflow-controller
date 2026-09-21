package triage

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const PlannerSchema = "triage.planner.v1"

// PlannerState is a full planning snapshot over one supporting context, not a
// dispatch authorization, verified claim, report or crash-resume checkpoint.
// Assessments and requirements are agent reasoning, never Go verdicts.
type PlannerState struct {
	Context             contract.Ref               `json:"context"`
	Previous            *contract.Ref              `json:"previous"`
	Hypotheses          []PlannerHypothesis        `json:"hypotheses"`
	Pending             []PlannerQuestion          `json:"pending"`
	Gaps                []string                   `json:"gaps"`
	Rationale           string                     `json:"rationale"`
	SupportingWork      *SupportingWork            `json:"supporting_work,omitempty"`
	WorkerTasks         []WorkerTask               `json:"worker_tasks,omitempty"`
	WorkerResults       []contract.Ref             `json:"worker_results,omitempty"`
	WikiTask            *InvestigationWikiTask     `json:"wiki_task,omitempty"`
	WikiResults         []contract.Ref             `json:"wiki_results,omitempty"`
	Ledger              *InvestigationLedger       `json:"ledger,omitempty"`
	Recovery            *PlannerRecovery           `json:"recovery,omitempty"`
	RecoveryChoices     []RecoveryChoice           `json:"recovery_choices,omitempty"`
	Checkpoint          *PlannerCheckpoint         `json:"checkpoint,omitempty"`
	Verification        *PlannerVerification       `json:"verification,omitempty"`
	VerificationRequest *VerificationRequest       `json:"verification_request,omitempty"`
	VerificationReview  *PlannerVerificationReview `json:"verification_review,omitempty"`
}

type PlannerHypothesis struct {
	ID         string     `json:"id"`
	Statement  string     `json:"statement"`
	Assessment string     `json:"assessment"`
	Evidence   []Evidence `json:"evidence"`
}

type PlannerQuestion struct {
	Question     string     `json:"question"`
	Requirements []string   `json:"requirements"`
	Basis        []Evidence `json:"basis"`
}

const plannerRequirements = `Read the exact committed supporting context and all needed evidence owners in request.inputs. This task only plans from those inputs; it does not dispatch workers, perform new acquisition or produce a final report. Load relevant existing skills for interpretation. If previous is supplied, reconstruct the current decision state from that committed Planner snapshot and its explicit inputs, not session memory, directory scans or other tasks' history. Submit a full current snapshot, not a delta: context, exact previous (null initially), hypotheses with stable IDs, statements, assessments and evidence refs, pending questions with concrete evidence requirements and basis refs, remaining gaps, and rationale explaining the current direction and any changes. Empty hypothesis/evidence lists are legitimate when prerequisites or evidence are missing; do not invent support. Keep supporting-context gaps visible: planning alone does not resolve them. Judge evidence applicability yourself and preserve true owners; cite only supplied committed ref+file_id, not local copies or invented files. Preserve partial/unavailable supporting-query limitations; small-window empty results and execution timeout are not incident-wide disproof. Ready context is not confirmed root cause, wiki patterns and model agreement are not runtime proof. Hypotheses/assessments remain unverified planning, not accepted claims. Pending free text is not dispatch authorization. To request one existing supporting task, optionally submit supporting_work with kind (resolve, refresh or update), reason, basis citing supplied committed evidence, and sources (nonempty selectors/reasons only for refresh, empty for the other kinds). Resolve remedies required wiki and unresolved identity/time on a needs-resolution context; it is not general hypothesis evidence acquisition. Refresh is narrow source work on incomplete intake. Update authorizes the complete intake update task, including agent-directed inventory changes, not per-source approvals. The Controller validates and dispatches the proposed task; do not execute it yourself or select commands, models or sessions. A changed context is the result of the previous snapshot's supporting_work; read that snapshot and new context, reassess planning yourself, and explicitly propose any further task rather than blindly copying consumed work. No drafts, publication, wiki write-back or final selection. You may propose multiple worker_tasks instead of supporting_work, never both: each has a stable id, explicit source_kind (code/db/logs/metrics/attachments/vision), responsibility (evidence-only/analysis), question, completion requirements, basis refs, depends_on task IDs, and search (null unless needed). Log/metric tasks require evidence-backed initial source/filter/finite UTC window, not approval for each query. Dependencies may name proposed tasks or delivered results; they become dispatchable only after accepted results exist. Never reuse a completed task ID for new work. Retain worker_results exactly as supplied in this task, including results not yet in previous; do not invent, omit or reorder them. Reassess hypotheses and remaining work using those results and their actual proposal/context/input owners; old results remain bound to their original context, not newly acquired evidence. Complete/incomplete is delivery, not truth or proof of a hypothesis. Cite a result's local evidence using that exact result ref. Worker raw evidence does not become supporting_work or Context provenance. Propose new task IDs for any further acquisition; pending/next prose alone never dispatches it.`

// plannerCaller keeps one session across successful planning Steps. The only
// fresh-session continuation here closes and verifies the old owner first.
// Supporting work and worker results retain their own accepted provenance;
// failure recovery remains a separate unit.
type plannerCaller struct {
	r               *engine.Run
	scope           Scope
	model           runtime.ModelSpec
	history         contextHistory
	handle          *engine.SessionHandle
	last            *contract.Ref
	session         string
	stopped         bool
	workerResults   []contract.Ref
	wikiResults     []contract.Ref
	adaptive        bool
	adaptiveNote    string
	capacity        *PlannerCapacityPolicy
	pendingFeedback []PlannerFeedback
	identity        runtime.Identity
	recovery        *PlannerRecovery
	recoveryErrors  []error
	verification    *PlannerVerification
	nativeFailures  []nativeRecoveryFailure
	reporting       *investigationReporting
}

func startPlanner(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref) (*plannerCaller, error) {
	return openPlanner(ctx, r, scope, model, contextRef, nil)
}

func openPlanner(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref, previous *contract.Ref) (*plannerCaller, error) {
	return openPlannerWithResults(ctx, r, scope, model, contextRef, previous, nil)
}

func openPlannerWithResults(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref, previous *contract.Ref, accepted []contract.Ref) (*plannerCaller, error) {
	return openPlannerWithEvidence(ctx, r, scope, model, contextRef, previous, accepted, nil)
}

func openPlannerWithEvidence(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref, previous *contract.Ref, accepted, wikiResults []contract.Ref) (*plannerCaller, error) {
	a := newAcceptance(ctx, r)
	records, err := a.loadWorkerResults(scope, accepted)
	if err != nil {
		return nil, err
	}
	h, err := a.loadContextHistory(scope, contextRef)
	if err != nil {
		return nil, err
	}
	if _, err := a.wikiSources(scope, h.sources, wikiResults, records); err != nil {
		return nil, err
	}
	// Revalidate the supplied chain rather than trusting an in-memory summary.
	var chain []contract.Ref
	seen := map[contract.Ref]bool{}
	for ref := previous; ref != nil; {
		if seen[*ref] {
			return nil, fmt.Errorf("cyclic planner lineage")
		}
		seen[*ref] = true
		p, err := readAccepted[PlannerState](a, *ref, PlannerSchema)
		if err != nil {
			return nil, err
		}
		chain = append(chain, *ref)
		ref = p.Data.Previous
	}
	var prior *contract.Ref
	adaptive := false
	var checkpoint *PlannerCheckpoint
	var recovery *PlannerRecovery
	var verification *PlannerVerification
	for i := len(chain) - 1; i >= 0; i-- {
		state, err := readAccepted[PlannerState](a, chain[i], PlannerSchema)
		if err != nil {
			return nil, err
		}
		owner := h
		if state.Data.Context != h.ref {
			owner, err = a.loadContextHistory(scope, state.Data.Context)
			if err != nil {
				return nil, err
			}
		}
		if err := a.checkPlannerWithWorkers(chain[i], owner, prior, records); err != nil {
			return nil, err
		}
		if len(state.Data.WikiResults) > len(wikiResults) || !slices.Equal(state.Data.WikiResults, wikiResults[:len(state.Data.WikiResults)]) {
			return nil, fmt.Errorf("planner wiki results differ from accepted history")
		}
		adaptive = state.Data.Ledger != nil
		checkpoint = state.Data.Checkpoint
		recovery = state.Data.Recovery
		verification = state.Data.Verification
		prior = &chain[i]
	}
	if err := a.checkPlannerContextChange(h, prior); err != nil {
		return nil, err
	}
	handle, err := r.OpenSession(ctx, engine.RoleSpec{Name: "triage-planner", Model: model, CWD: filepath.Join(r.Dir(), "triage-work")})
	if err != nil {
		return nil, err
	}
	var identity runtime.Identity
	if recovery != nil {
		identity, err = r.SessionIdentity(ctx, handle)
		if err != nil {
			return nil, err
		}
	}
	p := &plannerCaller{r: r, scope: scope, model: model, history: h, handle: handle, last: prior, workerResults: slices.Clone(accepted), wikiResults: slices.Clone(wikiResults), adaptive: adaptive, identity: identity, recovery: recovery, verification: verification}
	if checkpoint != nil {
		policy := checkpoint.Policy
		p.capacity, p.adaptiveNote = &policy, checkpoint.AdaptiveNote
	}
	return p, nil
}

func checkPlanner(ctx context.Context, r *engine.Run, ref contract.Ref, h contextHistory, previous *contract.Ref) error {
	return newAcceptance(ctx, r).checkPlanner(ref, h, previous)
}

func (a *acceptance) checkPlanner(ref contract.Ref, h contextHistory, previous *contract.Ref) error {
	return a.checkPlannerWithWorkers(ref, h, previous, nil)
}

func (a *acceptance) checkPlannerWithWorkers(ref contract.Ref, h contextHistory, previous *contract.Ref, records map[contract.Ref]workerRecord) error {
	p, err := readAccepted[PlannerState](a, ref, PlannerSchema)
	if err != nil {
		return err
	}
	v := p.Data
	if v.Context != h.ref || (v.Previous == nil) != (previous == nil) || (previous != nil && *v.Previous != *previous) {
		return fmt.Errorf("planner context/previous mismatch")
	}
	if err := a.checkPlannerContextChange(h, previous); err != nil {
		return err
	}
	sources, err := workerSources(h.sources, v.WorkerResults, records)
	if err != nil {
		return err
	}
	sources, err = a.wikiSources(h.value.Scope, sources, v.WikiResults, records)
	if err != nil {
		return err
	}
	sources, err = a.recoverySources(h.value.Scope, sources, v.Recovery)
	if err != nil {
		return err
	}
	planning := h
	planning.sources = sources
	if err := checkPlannerSnapshot(v, planning); err != nil {
		return err
	}
	if err := a.checkSupportingWork(h, v.SupportingWork); err != nil {
		return err
	}
	if previous != nil {
		prior, err := readAccepted[PlannerState](a, *previous, PlannerSchema)
		if err != nil {
			return err
		}
		retained := prior.Data.WorkerResults
		if len(v.WorkerResults) < len(retained) || !slices.Equal(v.WorkerResults[:len(retained)], retained) {
			return fmt.Errorf("planner cannot drop or reorder prior worker results")
		}
	}
	if err := checkWorkerTasks(v, h, sources, records); err != nil {
		return err
	}
	return a.checkInvestigation(v, h, sources)
}

func checkPlannerSnapshot(v PlannerState, h contextHistory) error {
	if !nonblank(v.Rationale) || !texts(v.Gaps) {
		return fmt.Errorf("planner requires rationale and nonblank gaps")
	}
	for _, gap := range h.value.Gaps {
		if !slices.Contains(v.Gaps, gap) {
			return fmt.Errorf("planning cannot remove supporting context gaps")
		}
	}
	evidence := func(items []Evidence) error {
		for _, e := range items {
			if e.Ref == nil || !hasFile(h.sources[*e.Ref], e.FileID) {
				return fmt.Errorf("planner evidence must name an exact supporting input owner/file")
			}
		}
		return nil
	}
	ids := map[string]bool{}
	for _, item := range v.Hypotheses {
		if !nonblank(item.ID) || ids[item.ID] || !nonblank(item.Statement) || !nonblank(item.Assessment) {
			return fmt.Errorf("planner hypotheses require unique IDs, statements and assessments")
		}
		ids[item.ID] = true
		if err := evidence(item.Evidence); err != nil {
			return err
		}
	}
	for _, item := range v.Pending {
		if !nonblank(item.Question) || len(item.Requirements) == 0 || !texts(item.Requirements) {
			return fmt.Errorf("planner questions require concrete evidence requirements")
		}
		if err := evidence(item.Basis); err != nil {
			return err
		}
	}
	return nil
}

func (p *plannerCaller) step(ctx context.Context) (contract.Ref, error) {
	return p.stepInScope(ctx, p.r.Root())
}

func (p *plannerCaller) stepInScope(ctx context.Context, executionScope *engine.Scope) (contract.Ref, error) {
	if p.stopped {
		return contract.Ref{}, fmt.Errorf("planner session is stopped")
	}
	inputs := []contract.Ref{p.history.ref}
	key := "planner-" + p.history.ref.AttemptID
	if p.last != nil {
		inputs = append(inputs, *p.last)
		key = "planner-" + p.last.AttemptID
	}
	a := newAcceptance(ctx, p.r)
	records, err := a.loadWorkerResults(p.scope, p.workerResults)
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	sources, err := workerSources(p.history.sources, p.workerResults, records)
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	sources, err = a.wikiSources(p.scope, sources, p.wikiResults, records)
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	sources, err = a.recoverySources(p.scope, sources, p.recovery)
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	inputs = appendSourceInputs(inputs, sources)
	if p.recovery != nil {
		for _, delivery := range p.recovery.Deliveries {
			inputs = appendUniqueRefs(inputs, delivery.Proposal, delivery.Context)
			if delivery.Support != nil {
				inputs = appendUniqueRefs(inputs, delivery.Support.refs()...)
			}
		}
	}
	if err := p.checkPendingClaims(ctx); err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	inputs, err = p.verificationInputs(a, inputs)
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	requirements := plannerRequirements
	if p.adaptive {
		requirements += "\n\n" + adaptiveRequirements
	}
	checkpoint, err := p.checkpointTask(a)
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	if checkpoint != nil {
		requirements += "\n\nCopy the supplied checkpoint object exactly into this full snapshot, including policy, dispatch_cycle, checkpoint_cycle, full_checkpoint, adaptive_note and all controller_feedback in order. Do not drop, rewrite or invent feedback, adjust these counters, or replace historical evidence owners. These are Controller continuation metadata, not hypothesis progress or proof. Every third completed dispatch cycle marks this same full snapshot as a full checkpoint; do not create another Step or publication. Unknown capacity is a diagnostic, not zero or a reason to conclude."
	}
	if p.recovery != nil {
		requirements += "\n\n" + recoveryRequirements
	}
	if p.verification != nil {
		requirements += "\n\n" + verificationPlannerRequirements
	}
	task := struct {
		stageTask
		WorkerResults []contract.Ref       `json:"worker_results"`
		WikiResults   *[]contract.Ref      `json:"wiki_results,omitempty"`
		AdaptiveNote  string               `json:"adaptive_note,omitempty"`
		Checkpoint    *PlannerCheckpoint   `json:"checkpoint,omitempty"`
		Recovery      *PlannerRecovery     `json:"recovery,omitempty"`
		Verification  *PlannerVerification `json:"verification,omitempty"`
	}{stageTask{Stage: "planner", Scope: p.scope, Previous: p.last, Gaps: p.history.value.Gaps, Requirements: requirements}, slices.Clone(p.workerResults), nil, p.adaptiveNote, checkpoint, p.recoveryTask(), p.verificationTask()}
	if p.adaptive {
		wikiResults := append([]contract.Ref{}, p.wikiResults...)
		task.WikiResults = &wikiResults
	}
	if task.WorkerResults == nil {
		task.WorkerResults = []contract.Ref{}
	}
	prompt, err := json.Marshal(task)
	if err != nil {
		return contract.Ref{}, err
	}
	adaptive := p.adaptive
	out, err := executionScope.Step(ctx, engine.StepSpec{Key: key, Session: p.handle, Prompt: string(prompt), Inputs: inputs, Output: contract.Spec{SchemaID: PlannerSchema}, Timeout: 30 * time.Minute})
	if err != nil && p.recovery != nil {
		p.stopped = true
		return contract.Ref{}, &taskFailure{cause: err, handle: p.handle, identity: p.identity, stage: "planner", attempt: out.AttemptID}
	}
	if err == nil && p.recovery != nil && out.Execution.SessionID != p.identity.SessionID {
		err = fmt.Errorf("planner execution identity mismatch")
	}
	if err == nil {
		a = newAcceptance(ctx, p.r)
		records, err = a.loadWorkerResults(p.scope, p.workerResults)
		if err == nil {
			err = a.checkPlannerWithWorkers(out.Output, p.history, p.last, records)
		}
		if err == nil {
			var state publication[PlannerState]
			state, err = readAccepted[PlannerState](a, out.Output, PlannerSchema)
			if err == nil && !slices.Equal(state.Data.WorkerResults, p.workerResults) {
				err = fmt.Errorf("planner worker_results differ from accepted deliveries")
			}
			if err == nil && (!slices.Equal(state.Data.WikiResults, p.wikiResults) || (p.adaptive && state.Data.Ledger == nil)) {
				err = fmt.Errorf("planner requires exact wiki deliveries and adaptive ledger")
			}
			if err == nil && !samePlannerCheckpoint(state.Data.Checkpoint, checkpoint) {
				err = fmt.Errorf("planner checkpoint differs from supplied continuation metadata")
			}
			if err == nil && recoveryJSON(state.Data.Recovery) != recoveryJSON(task.Recovery) {
				err = fmt.Errorf("planner recovery differs from supplied delivery metadata")
			}
			if err == nil && recoveryJSON(state.Data.Verification) != recoveryJSON(task.Verification) {
				err = fmt.Errorf("planner verification differs from supplied claim/feedback metadata")
			}
			if err == nil && state.Data.Ledger != nil {
				adaptive = true
			}
		}
	}
	if err == nil {
		reason := "Planning snapshot accepted for continuation only; no worker dispatch or verified conclusion"
		if checkpoint != nil && checkpoint.FullCheckpoint {
			reason += fmt.Sprintf("; full checkpoint at dispatch cycle %d", checkpoint.DispatchCycle)
		}
		err = executionScope.Decision(ctx, key+"-recorded", reason, []contract.Ref{p.history.ref, out.Output})
	}
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	p.last, p.session = &out.Output, out.Execution.SessionID
	p.adaptive = adaptive
	p.pendingFeedback = nil
	return out.Output, nil
}

func (p *plannerCaller) close(ctx context.Context) error {
	p.stopped = true
	report, err := p.r.CloseSessionReport(ctx, p.handle)
	if err != nil {
		return err
	}
	expected := p.session
	if p.recovery != nil {
		expected = p.identity.SessionID
	}
	if expected == "" || !report.ConfirmsLocalClose(expected) || p.recovery != nil && report.Identity != p.identity {
		return fmt.Errorf("planner cleanup not confirmed")
	}
	return nil
}

func (p *plannerCaller) handoff(ctx context.Context) (*plannerCaller, error) {
	if p.stopped || p.last == nil {
		return nil, fmt.Errorf("planner handoff requires an accepted state and usable session")
	}
	if err := p.close(ctx); err != nil {
		return nil, err
	}
	return p.reopen(ctx, p.history.ref)
}
