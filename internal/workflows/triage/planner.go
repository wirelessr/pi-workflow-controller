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
	Context        contract.Ref        `json:"context"`
	Previous       *contract.Ref       `json:"previous"`
	Hypotheses     []PlannerHypothesis `json:"hypotheses"`
	Pending        []PlannerQuestion   `json:"pending"`
	Gaps           []string            `json:"gaps"`
	Rationale      string              `json:"rationale"`
	SupportingWork *SupportingWork     `json:"supporting_work,omitempty"`
	WorkerTasks    []WorkerTask        `json:"worker_tasks,omitempty"`
	WorkerResults  []contract.Ref      `json:"worker_results,omitempty"`
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
	r             *engine.Run
	scope         Scope
	model         runtime.ModelSpec
	history       contextHistory
	handle        *engine.SessionHandle
	last          *contract.Ref
	session       string
	stopped       bool
	workerResults []contract.Ref
}

func startPlanner(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref) (*plannerCaller, error) {
	return openPlanner(ctx, r, scope, model, contextRef, nil)
}

func openPlanner(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref, previous *contract.Ref) (*plannerCaller, error) {
	return openPlannerWithResults(ctx, r, scope, model, contextRef, previous, nil)
}

func openPlannerWithResults(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref, previous *contract.Ref, accepted []contract.Ref) (*plannerCaller, error) {
	a := newAcceptance(ctx, r)
	records, err := a.loadWorkerResults(scope, accepted)
	if err != nil {
		return nil, err
	}
	h, err := a.loadContextHistory(scope, contextRef)
	if err != nil {
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
		prior = &chain[i]
	}
	if err := a.checkPlannerContextChange(h, prior); err != nil {
		return nil, err
	}
	handle, err := r.OpenSession(ctx, engine.RoleSpec{Name: "triage-planner", Model: model, CWD: filepath.Join(r.Dir(), "triage-work")})
	if err != nil {
		return nil, err
	}
	return &plannerCaller{r: r, scope: scope, model: model, history: h, handle: handle, last: prior, workerResults: slices.Clone(accepted)}, nil
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
	return checkWorkerTasks(v, h, sources, records)
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
	inputs = appendSourceInputs(inputs, sources)
	task := struct {
		stageTask
		WorkerResults []contract.Ref `json:"worker_results"`
	}{stageTask{Stage: "planner", Scope: p.scope, Previous: p.last, Gaps: p.history.value.Gaps, Requirements: plannerRequirements}, slices.Clone(p.workerResults)}
	if task.WorkerResults == nil {
		task.WorkerResults = []contract.Ref{}
	}
	prompt, err := json.Marshal(task)
	if err != nil {
		return contract.Ref{}, err
	}
	out, err := p.r.Root().Step(ctx, engine.StepSpec{Key: key, Session: p.handle, Prompt: string(prompt), Inputs: inputs, Output: contract.Spec{SchemaID: PlannerSchema}, Timeout: 30 * time.Minute})
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
		}
	}
	if err == nil {
		err = p.r.Root().Decision(ctx, key+"-recorded", "Planning snapshot accepted for continuation only; no worker dispatch or verified conclusion", []contract.Ref{p.history.ref, out.Output})
	}
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	p.last, p.session = &out.Output, out.Execution.SessionID
	return out.Output, nil
}

func (p *plannerCaller) close(ctx context.Context) error {
	p.stopped = true
	report, err := p.r.CloseSessionReport(ctx, p.handle)
	if err != nil {
		return err
	}
	if p.session == "" || !report.ConfirmsLocalClose(p.session) {
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
	return openPlannerWithResults(ctx, p.r, p.scope, p.model, p.history.ref, p.last, p.workerResults)
}
