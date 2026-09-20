package triage

import (
	"context"
	"fmt"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

type HypothesisChange struct {
	HypothesisID string     `json:"hypothesis_id"`
	Change       string     `json:"change"`
	Reason       string     `json:"reason"`
	Basis        []Evidence `json:"basis"`
}

type InvestigationReframe struct {
	Change string     `json:"change"`
	Reason string     `json:"reason"`
	Basis  []Evidence `json:"basis"`
}

type InvestigationLedger struct {
	Action        string                `json:"action"`
	Reason        string                `json:"reason"`
	Round         int                   `json:"round"`
	NoProgress    int                   `json:"no_progress"`
	ReframeRound  int                   `json:"reframe_round"`
	ReframeStreak int                   `json:"reframe_streak"`
	ConsumedBatch []contract.Ref        `json:"consumed_batch"`
	Changes       []HypothesisChange    `json:"changes"`
	Reframe       *InvestigationReframe `json:"reframe"`
}

const adaptiveRequirements = `This is the adaptive investigation Planner, not a verifier or report writer. Submit the full hypotheses/pending/gaps/worker_tasks/worker_results/wiki_results state and a ledger every time. Echo worker_results and wiki_results exactly as supplied. Choose an explicit ledger.action: workers, support, wiki, reframe, plan, or yield, and explain ledger.reason. Workers means dispatch only declared worker_tasks whose dependencies have accepted results; at most three are dispatched in declaration order. Adjust unfinished tasks freely in each new snapshot, but never reuse a completed ID. Support uses the existing supporting_work contract to remedy prerequisites, not first-missing-field closure. Its basis still names only supporting-context evidence; investigation wiki/worker delivery does not expand Context provenance or supporting_work eligibility. Wiki/reframe declares wiki_task with stable id, terms, previous_terms exactly from the latest delivered wiki (initially Context.Wiki), reason and evidence basis; reframe changes the terms. Wiki tasks do not replace intake or Context.Wiki. Preserve new incomplete wiki gaps even if the old Context.Wiki was complete. Interpret wiki applicability yourself; completed search is not runtime proof or hypothesis progress.
One round is one delivered worker batch followed by this Planner update. ledger.consumed_batch is exactly the new suffix of worker_results since previous; increment round only for that nonempty batch. For that batch explicitly report changes with hypothesis_id, change description, reason and supplied basis, or an empty changes array for no hypothesis progress. Do not call repeated successful searches, additional files, ordinary planning/support or a fresh session hypothesis progress. Go does not compare assessments or determine truth. Increment no_progress for an empty-changes worker round; reset it only for an explicitly reported hypothesis change. Without a worker batch preserve no_progress and report no changes. Preserve reframe_round/reframe_streak independently: on the update receiving the previous reframe action's wiki delivery, set them to that proposal's round/no_progress (even if the search is incomplete), not zero and not a success-derived progress event. Actual hypothesis progress resets reframe_streak to zero, retaining reframe_round. Otherwise echo these counters. Initially all counters are zero. Two no-progress worker rounds since the separate reframe boundary require action reframe, a concrete reframe change/reason/basis, a different source, counterexample, healthy control or problem premise, and new wiki terms. If no feasible authorized path exists, explicitly yield with reason and gaps instead of implying a conclusion. Reframe attempts do not erase incomplete search gaps. Extra planning, support, wiki tasks and session changes do not add rounds. plan has no dispatch. yield has no dispatch and hands only accepted investigation state to a later phase; explicitly explain needed next work or why no authorized path is feasible. It is never a verified claim, final report, root-cause confirmation or publication.`

func (a *acceptance) checkInvestigation(v PlannerState, h contextHistory, sources map[contract.Ref][]file) error {
	var prior PlannerState
	if v.Previous != nil {
		p, err := readAccepted[PlannerState](a, *v.Previous, PlannerSchema)
		if err != nil {
			return err
		}
		prior = p.Data
	}
	l := v.Ledger
	if l == nil {
		if prior.Ledger != nil || v.WikiTask != nil || len(v.WikiResults) != 0 {
			return fmt.Errorf("investigation state requires a retained ledger")
		}
		return checkPlannerCheckpoint(v, prior)
	}
	if !nonblank(l.Reason) {
		return fmt.Errorf("investigation action requires a reason")
	}
	if len(v.WorkerResults) < len(prior.WorkerResults) || !slices.Equal(v.WorkerResults[:len(prior.WorkerResults)], prior.WorkerResults) || len(v.WikiResults) < len(prior.WikiResults) || !slices.Equal(v.WikiResults[:len(prior.WikiResults)], prior.WikiResults) {
		return fmt.Errorf("investigation cannot drop or reorder delivered results")
	}
	batch := v.WorkerResults[len(prior.WorkerResults):]
	wikiBatch := v.WikiResults[len(prior.WikiResults):]
	if !slices.Equal(l.ConsumedBatch, batch) || len(batch) > 3 || len(wikiBatch) > 1 || (len(batch) > 0 && len(wikiBatch) > 0) {
		return fmt.Errorf("investigation consumed batch mismatch")
	}
	for _, ref := range batch {
		result, err := readAccepted[WorkerResult](a, ref, WorkerSchema)
		if err != nil {
			return err
		}
		if v.Previous == nil || result.Data.Proposal != *v.Previous {
			return fmt.Errorf("worker batch must bind the immediately preceding proposal")
		}
	}
	round, streak, reframeRound, reframeStreak := 0, 0, 0, 0
	if prior.Ledger != nil {
		round, streak = prior.Ledger.Round, prior.Ledger.NoProgress
		reframeRound, reframeStreak = prior.Ledger.ReframeRound, prior.Ledger.ReframeStreak
	}
	if len(wikiBatch) > 0 {
		result, err := readAccepted[WikiSearch](a, wikiBatch[0], WikiSchema)
		if err != nil {
			return err
		}
		if result.Data.Task == nil || v.Previous == nil || result.Data.Task.Proposal != *v.Previous {
			return fmt.Errorf("wiki delivery must bind the immediately preceding proposal")
		}
		if prior.Ledger != nil && prior.Ledger.Action == "reframe" {
			reframeRound, reframeStreak = round, streak
		}
	}
	if len(batch) > 0 {
		round++
		if len(l.Changes) == 0 {
			streak++
		} else {
			streak, reframeStreak = 0, 0
		}
	} else if len(l.Changes) != 0 {
		return fmt.Errorf("hypothesis progress requires a consumed worker round")
	}
	if l.Round != round || l.NoProgress != streak || l.ReframeRound != reframeRound || l.ReframeStreak != reframeStreak {
		return fmt.Errorf("investigation round/streak/reframe echo mismatch")
	}
	ids := map[string]bool{}
	for _, hypothesis := range append(slices.Clone(prior.Hypotheses), v.Hypotheses...) {
		ids[hypothesis.ID] = true
	}
	changed := map[string]bool{}
	for _, change := range l.Changes {
		if !ids[change.HypothesisID] || changed[change.HypothesisID] || !nonblank(change.Change) || !nonblank(change.Reason) || len(change.Basis) == 0 {
			return fmt.Errorf("hypothesis change requires a distinct declared ID, change, reason and basis")
		}
		changed[change.HypothesisID] = true
		if err := checkInvestigationBasis(change.Basis, sources); err != nil {
			return err
		}
	}
	if streak-reframeStreak >= 2 && l.Action != "reframe" && l.Action != "yield" {
		return fmt.Errorf("two rounds without reported hypothesis progress require reframe")
	}
	if streak-reframeStreak >= 2 && l.Action == "yield" && len(v.Gaps) == 0 {
		return fmt.Errorf("yield instead of required reframe must retain explicit gaps")
	}
	if l.Action != "workers" && len(v.WorkerTasks) != 0 || l.Action != "support" && v.SupportingWork != nil || (l.Action != "wiki" && l.Action != "reframe") && v.WikiTask != nil {
		return fmt.Errorf("investigation action and proposed work differ")
	}
	switch l.Action {
	case "workers":
		if len(v.WorkerTasks) == 0 {
			return fmt.Errorf("workers action requires explicit tasks")
		}
	case "support":
		if v.SupportingWork == nil {
			return fmt.Errorf("support action requires supporting_work")
		}
	case "wiki", "reframe":
		if v.WikiTask == nil {
			return fmt.Errorf("wiki/reframe action requires investigation wiki_task")
		}
	case "plan", "yield":
	default:
		return fmt.Errorf("unsupported investigation action %q", l.Action)
	}
	if (l.Reframe != nil) != (l.Action == "reframe") {
		return fmt.Errorf("reframe declaration must match action")
	}
	if l.Reframe != nil {
		if !nonblank(l.Reframe.Change) || !nonblank(l.Reframe.Reason) || len(l.Reframe.Basis) == 0 {
			return fmt.Errorf("reframe requires change, reason and basis")
		}
		if err := checkInvestigationBasis(l.Reframe.Basis, sources); err != nil {
			return err
		}
	}
	if len(v.WikiResults) > 0 {
		latest, err := readAccepted[WikiSearch](a, v.WikiResults[len(v.WikiResults)-1], WikiSchema)
		if err != nil {
			return err
		}
		for _, gap := range latest.Data.Gaps {
			if !slices.Contains(v.Gaps, gap) {
				return fmt.Errorf("planning cannot hide investigation wiki gaps behind old context completeness")
			}
		}
	}
	if err := a.checkWikiTask(v, h, sources); err != nil {
		return err
	}
	return checkPlannerCheckpoint(v, prior)
}

func (p *plannerCaller) workReady(ctx context.Context, models sliceModels) (int, error) {
	if p.stopped || p.last == nil {
		return 0, fmt.Errorf("worker batch requires an accepted state and usable planner")
	}
	p.stopped = true
	a := newAcceptance(ctx, p.r)
	records, err := a.loadWorkerResults(p.scope, p.workerResults)
	if err != nil {
		return 0, err
	}
	state, err := readAccepted[PlannerState](a, *p.last, PlannerSchema)
	if err != nil {
		return 0, err
	}
	if err := a.checkPlannerWithWorkers(*p.last, p.history, state.Data.Previous, records); err != nil {
		return 0, err
	}
	completed := map[string]bool{}
	for _, record := range records {
		completed[record.value.TaskID] = true
	}
	var ready []WorkerTask
	for _, task := range state.Data.WorkerTasks {
		if completed[task.ID] || slices.ContainsFunc(task.DependsOn, func(id string) bool { return !completed[id] }) {
			continue
		}
		ready = append(ready, task)
		if len(ready) == 3 {
			break
		}
	}
	prepared := make([]preparedWorker, len(ready))
	branches := make([]engine.Branch, len(ready))
	for i, task := range ready {
		prepared[i], err = prepareWorker(ctx, p.r, p.scope, p.history.ref, *p.last, p.workerResults, task.ID)
		if err != nil {
			return 0, err
		}
		work := prepared[i]
		branches[i] = engine.Branch{Name: fmt.Sprintf("worker-%d", i), Do: func(ctx context.Context, s *engine.Scope) (engine.Result, error) {
			ref, err := runWorker(ctx, p.r, s, models, work)
			if err != nil {
				return engine.Result{}, err
			}
			return engine.Result{Outputs: map[string]contract.Ref{"worker": ref}}, nil
		}}
	}
	if len(branches) == 0 {
		p.stopped = false
		return 0, nil
	}
	joined, err := p.r.Root().Parallel(ctx, "workers-"+p.last.AttemptID, engine.FailFast, branches)
	if err != nil {
		return 0, err
	}
	accepted := slices.Clone(p.workerResults)
	for i, branch := range joined {
		if branch.Err != nil {
			return 0, branch.Err
		}
		ref := branch.Result.Outputs["worker"]
		if err := acceptWorker(ctx, p.r, p.scope, *p.last, accepted, prepared[i], ref); err != nil {
			return 0, err
		}
		accepted = append(accepted, ref)
	}
	p.workerResults, p.stopped = accepted, false
	return len(joined), nil
}

// The returned Ref is investigation state only. A nonnil capacity policy opts
// into checkpoint/capacity continuation; recovery and verification remain separate.
func executeInvestigation(ctx context.Context, r *engine.Run, scope Scope, plannerModel runtime.ModelSpec, models sliceModels, contextRef contract.Ref, capacity *PlannerCapacityPolicy) (contract.Ref, error) {
	if capacity != nil {
		if err := capacity.check(); err != nil {
			return contract.Ref{}, err
		}
	}
	p, err := startPlanner(ctx, r, scope, plannerModel, contextRef)
	if err != nil {
		return contract.Ref{}, err
	}
	p.adaptive = true
	if capacity != nil {
		policy := *capacity
		p.capacity = &policy
	}
	return p.adapt(ctx, models)
}

func (p *plannerCaller) adapt(ctx context.Context, models sliceModels) (contract.Ref, error) {
	p.adaptive = true
	for {
		ref, err := p.step(ctx)
		if err != nil {
			return contract.Ref{}, err
		}
		state, err := readAccepted[PlannerState](newAcceptance(ctx, p.r), ref, PlannerSchema)
		if err != nil {
			p.stopped = true
			return contract.Ref{}, err
		}
		p.adaptiveNote = ""
		// These actions already close the Planner. Sampling would add an
		// unnecessary failure boundary or a second fresh session.
		if state.Data.Ledger.Action != "support" && state.Data.Ledger.Action != "yield" {
			p, err = p.capacityHandoff(ctx)
			if err != nil {
				return contract.Ref{}, err
			}
		}
		switch state.Data.Ledger.Action {
		case "workers":
			var count int
			count, err = p.workReady(ctx, models)
			if err == nil && count == 0 {
				p.adaptiveNote = "No declared task has all dependencies in accepted worker results. Revise pending tasks, propose an authorized supporting/wiki task, or explicitly yield with reasons and gaps. No worker batch ran; do not increment the round or report progress."
				if p.capacity != nil {
					p.pendingFeedback = append(p.pendingFeedback, PlannerFeedback{After: ref, Note: p.adaptiveNote})
				}
			}
		case "support":
			p, err = p.support(ctx, models)
		case "wiki", "reframe":
			_, err = p.searchWiki(ctx, models)
		case "yield":
			if err := p.close(ctx); err != nil {
				return contract.Ref{}, err
			}
			return ref, nil
		case "plan":
		}
		if err != nil {
			return contract.Ref{}, err
		}
	}
}
