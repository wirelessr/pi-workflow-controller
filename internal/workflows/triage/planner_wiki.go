package triage

import (
	"context"
	"fmt"
	"maps"
	"path/filepath"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/workflows/triagev2"
)

type InvestigationWikiTask struct {
	ID            string     `json:"id"`
	Terms         []string   `json:"terms"`
	PreviousTerms []string   `json:"previous_terms"`
	Reason        string     `json:"reason"`
	Basis         []Evidence `json:"basis"`
}

type WikiTaskBinding struct {
	Proposal contract.Ref   `json:"proposal"`
	Context  contract.Ref   `json:"context"`
	TaskID   string         `json:"task_id"`
	Inputs   []contract.Ref `json:"inputs"`
}

func checkInvestigationBasis(items []Evidence, sources map[contract.Ref][]file) error {
	for _, e := range items {
		if e.Ref == nil || !hasFile(sources[*e.Ref], e.FileID) {
			return fmt.Errorf("investigation basis must name an exact input owner/file")
		}
	}
	return nil
}

func (a *acceptance) checkWikiTask(v PlannerState, h contextHistory, sources map[contract.Ref][]file) error {
	task := v.WikiTask
	if task == nil {
		return nil
	}
	if !nonblank(task.ID) || !nonblank(task.Reason) || len(task.Terms) == 0 || !texts(task.Terms) || len(task.Basis) == 0 {
		return fmt.Errorf("investigation wiki task requires id, terms, reason and basis")
	}
	previous := h.value.Wiki
	for _, ref := range v.WikiResults {
		p, err := readAccepted[WikiSearch](a, ref, WikiSchema)
		if err != nil {
			return err
		}
		if p.Data.Task == nil || p.Data.Task.TaskID == task.ID {
			return fmt.Errorf("wiki task ID already completed or missing historical binding")
		}
		previous = ref
	}
	old, err := readAccepted[WikiSearch](a, previous, WikiSchema)
	if err != nil {
		return err
	}
	if !slices.Equal(task.PreviousTerms, old.Data.Queries) {
		return fmt.Errorf("wiki previous_terms differ from the supplied previous search")
	}
	if v.Ledger != nil && v.Ledger.Action == "reframe" && slices.Equal(task.Terms, task.PreviousTerms) {
		return fmt.Errorf("reframe requires explicitly changed wiki terms")
	}
	return checkInvestigationBasis(task.Basis, sources)
}

// Revalidate each search against its original proposal, never today's intake.
// Records and decoded publications live only in this synchronous pass.
func (a *acceptance) wikiSources(scope Scope, base map[contract.Ref][]file, refs []contract.Ref, records map[contract.Ref]workerRecord) (map[contract.Ref][]file, error) {
	sources := maps.Clone(base)
	history := map[contract.Ref][]file{}
	ids := map[string]bool{}
	seen := map[contract.Ref]bool{}
	for i, ref := range refs {
		if seen[ref] {
			return nil, fmt.Errorf("duplicate investigation wiki result")
		}
		seen[ref] = true
		result, err := readAccepted[WikiSearch](a, ref, WikiSchema)
		if err != nil {
			return nil, err
		}
		binding := result.Data.Task
		if binding == nil || ids[binding.TaskID] {
			return nil, fmt.Errorf("investigation wiki requires a distinct task binding")
		}
		ids[binding.TaskID] = true
		proposal, err := readAccepted[PlannerState](a, binding.Proposal, PlannerSchema)
		if err != nil {
			return nil, err
		}
		v := proposal.Data
		if v.WikiTask == nil || v.WikiTask.ID != binding.TaskID || binding.Context != v.Context || !slices.Equal(v.WikiResults, refs[:i]) {
			return nil, fmt.Errorf("investigation wiki proposal/task/context/history mismatch")
		}
		h, err := a.loadContextHistory(scope, binding.Context)
		if err != nil {
			return nil, err
		}
		owners, err := workerSources(h.sources, v.WorkerResults, records)
		if err != nil {
			return nil, err
		}
		maps.Copy(owners, history)
		owners, err = a.recoverySources(scope, owners, v.Recovery)
		if err != nil {
			return nil, err
		}
		planning := h
		planning.sources = owners
		if err := checkPlannerSnapshot(v, planning); err != nil {
			return nil, err
		}
		if err := a.checkPlannerContextChange(h, v.Previous); err != nil {
			return nil, err
		}
		if err := a.checkInvestigation(v, h, owners); err != nil {
			return nil, err
		}
		inputs := appendSourceInputs([]contract.Ref{binding.Proposal, h.ref}, owners)
		if !slices.Equal(binding.Inputs, inputs) {
			return nil, fmt.Errorf("investigation wiki inputs mismatch")
		}
		if _, err := checkWikiPublication(result, h.value.Intake); err != nil {
			return nil, err
		}
		for _, term := range v.WikiTask.Terms {
			if !slices.Contains(result.Data.Queries, term) {
				return nil, fmt.Errorf("investigation wiki omitted a dispatched search term")
			}
		}
		maps.Copy(history, owners)
		history[binding.Proposal] = nil
		history[ref] = result.Files
		maps.Copy(sources, history)
	}
	return sources, nil
}

const investigationWikiRequirements = `Load the existing wiki skill and perform this explicitly authorized read-only investigation wiki task. Read the exact proposal, context, prior searches and evidence owners in request.inputs. Do not refetch intake or change Context.Wiki. Return the supplied binding exactly in task, and bind intake to the supplied intake Ref. Search the proposed terms, retaining these and any additional actual terms in queries, original query/results, page snapshots actually read, partial/failure diagnostics and gaps. Wiki-only: no other WIP or session-history search, no write-back. Use the existing WikiSearch completion rules: partial, unavailable or not-run is not completed-no-matches. A completed search is not hypothesis progress or runtime proof. Reframe direction and applicability belong to the Planner.`

func (p *plannerCaller) searchWiki(ctx context.Context, models sliceModels) (contract.Ref, error) {
	if p.stopped || p.last == nil {
		return contract.Ref{}, fmt.Errorf("wiki dispatch requires an accepted state and usable planner")
	}
	p.stopped = true
	a := newAcceptance(ctx, p.r)
	records, err := a.loadWorkerResults(p.scope, p.workerResults)
	if err != nil {
		return contract.Ref{}, err
	}
	state, err := readAccepted[PlannerState](a, *p.last, PlannerSchema)
	if err != nil {
		return contract.Ref{}, err
	}
	if err := a.checkPlannerWithWorkers(*p.last, p.history, state.Data.Previous, records); err != nil {
		return contract.Ref{}, err
	}
	if state.Data.WikiTask == nil {
		return contract.Ref{}, fmt.Errorf("wiki dispatch requires an explicit investigation wiki task")
	}
	sources, err := workerSources(p.history.sources, p.workerResults, records)
	if err != nil {
		return contract.Ref{}, err
	}
	sources, err = a.wikiSources(p.scope, sources, p.wikiResults, records)
	if err != nil {
		return contract.Ref{}, err
	}
	sources, err = a.recoverySources(p.scope, sources, state.Data.Recovery)
	if err != nil {
		return contract.Ref{}, err
	}
	inputs := appendSourceInputs([]contract.Ref{*p.last, p.history.ref}, sources)
	request := struct {
		Workspace    string                `json:"workspace"`
		Stage        string                `json:"stage"`
		Scope        Scope                 `json:"scope"`
		Intake       contract.Ref          `json:"intake"`
		Task         InvestigationWikiTask `json:"task"`
		Binding      WikiTaskBinding       `json:"binding"`
		Requirements string                `json:"requirements"`
	}{filepath.Join(p.r.Dir(), "triage-work"), "wiki-investigation", p.scope, p.history.value.Intake, *state.Data.WikiTask, WikiTaskBinding{*p.last, p.history.ref, state.Data.WikiTask.ID, inputs}, investigationWikiRequirements}
	key := "investigation-wiki-" + p.last.AttemptID
	if err := p.r.Root().Decision(ctx, key+"-dispatch", "Dispatch explicit investigation wiki task", inputs); err != nil {
		return contract.Ref{}, err
	}
	if p.recovery != nil {
		request.Requirements += "\nRead the exact proposal recovery metadata and choices. Do not repeat a failed search or submit remote work unless the Agent has supplied an evidence-backed safe resume or nonoverlapping redirect. Preserve the original failed proposal/task and diagnostic binding in recovery metadata; this attempt has its own binding."
	}
	request.Requirements += "\n" + triageWorkspaceRequirements
	ref, err := triagev2.RunTaskStep(ctx, p.r, triagev2.TaskStep{Scope: p.r.Root(), Model: models.Analysis, Stage: request.Stage, Key: key, Task: request, Schema: WikiSchema, Inputs: inputs, Recovery: p.recovery != nil})
	if err != nil {
		if p.recovery == nil {
			return contract.Ref{}, err
		}
		failure, recoveryErr := triagev2.ConfirmRecovery(ctx, p.r, err, false)
		if recoveryErr != nil {
			return contract.Ref{}, recoveryErr
		}
		delivery := newDelivery("wiki", p)
		failure.TaskID = state.Data.WikiTask.ID
		delivery.Failures = []triagev2.RecoveryFailure{failure}
		p.recovery.Deliveries = append(p.recovery.Deliveries, delivery)
		p.recoveryErrors = append(p.recoveryErrors, err)
		p.nativeFailures = append(p.nativeFailures, nativeRecoveryFailure{failure, err})
		p.stopped = false
		return contract.Ref{}, nil
	}
	a = newAcceptance(ctx, p.r)
	records, err = a.loadWorkerResults(p.scope, p.workerResults)
	if err != nil {
		return contract.Ref{}, err
	}
	accepted := append(slices.Clone(p.wikiResults), ref)
	if _, err := a.wikiSources(p.scope, p.history.sources, accepted, records); err != nil {
		return contract.Ref{}, err
	}
	if err := p.r.Root().Decision(ctx, key+"-recorded", "Investigation wiki delivery accepted with actual completeness, not hypothesis progress", append(slices.Clone(inputs), ref)); err != nil {
		return contract.Ref{}, err
	}
	if p.recovery != nil {
		delivery := newDelivery("wiki", p)
		delivery.Results = []contract.Ref{ref}
		p.recovery.Deliveries = append(p.recovery.Deliveries, delivery)
	}
	p.wikiResults, p.stopped = accepted, false
	return ref, nil
}
