package triage

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"pi-workflow-controller/internal/contract"
)

// SupportingWork proposes one existing workflow task, bound to the enclosing
// PlannerState.Context and its caller scope. It grants no tool/model control.
type SupportingWork struct {
	Kind    string       `json:"kind"`
	Reason  string       `json:"reason"`
	Basis   []Evidence   `json:"basis"`
	Sources []intakeWork `json:"sources"`
}

func (a *acceptance) checkSupportingWork(h contextHistory, work *SupportingWork) error {
	if err := checkSupportingProposal(h, work); err != nil {
		return err
	}
	if work != nil && work.Kind == "refresh" {
		intake, err := a.checkIntake(h.value.Intake, h.value.Scope.Ticket)
		if err != nil {
			return err
		}
		return checkIntakeWork(intake, work.Sources)
	}
	return nil
}

func checkSupportingProposal(h contextHistory, work *SupportingWork) error {
	if work == nil {
		return nil
	}
	if !nonblank(work.Reason) || len(work.Basis) == 0 {
		return fmt.Errorf("supporting work requires reason and basis")
	}
	for _, e := range work.Basis {
		if e.Ref == nil || !hasFile(h.sources[*e.Ref], e.FileID) {
			return fmt.Errorf("supporting work basis must name an exact supporting input owner/file")
		}
	}
	if work.Kind != "refresh" && len(work.Sources) != 0 {
		return fmt.Errorf("only refresh accepts source selectors")
	}
	switch work.Kind {
	case "resolve":
		if h.value.Readiness == "ready" {
			return fmt.Errorf("supporting resolve requires needs-resolution context")
		}
	case "refresh", "update":
	default:
		return fmt.Errorf("unsupported supporting work kind %q", work.Kind)
	}
	return nil
}

// A context change consumes the immediately previous Planner proposal. Verify
// the task/version relationship, not the agent's rationale or applicability.
// Old planning states continue to be checked against their own context owners.
func (a *acceptance) checkPlannerContextChange(next contextHistory, previous *contract.Ref) error {
	if previous == nil {
		return nil
	}
	prior, err := readAccepted[PlannerState](a, *previous, PlannerSchema)
	if err != nil {
		return err
	}
	if prior.Data.Context == next.ref {
		return nil
	}
	work := prior.Data.SupportingWork
	if work == nil || next.value.Previous == nil || *next.value.Previous != prior.Data.Context {
		return fmt.Errorf("planner context change requires prior supporting proposal and direct context successor")
	}
	old, err := a.loadContextHistory(next.value.Scope, prior.Data.Context)
	if err != nil {
		return err
	}
	if err := a.checkSupportingWork(old, work); err != nil {
		return err
	}
	if work.Kind == "resolve" {
		wiki, err := a.checkWiki(old.value.Wiki, old.value.Intake)
		if err != nil {
			return err
		}
		if next.value.Intake != old.value.Intake || (next.value.Wiki == old.value.Wiki) != wikiComplete(wiki) {
			return fmt.Errorf("supporting resolve result changed intake or wiki task binding")
		}
		return nil
	}
	intake, err := a.checkIntake(next.value.Intake, next.value.Scope.Ticket)
	if err != nil {
		return err
	}
	if intake.Previous == nil || *intake.Previous != old.value.Intake || intake.Update != (work.Kind == "update") || (work.Kind == "refresh" && !slices.Equal(intake.Work, work.Sources)) {
		return fmt.Errorf("supporting result differs from proposed intake task")
	}
	return nil
}

// support consumes only the last accepted state's structured proposal. The old
// caller is stopped before dispatch; partial publications are not a new Planner
// checkpoint. The returned fresh caller still needs a successful planning Step.
func (p *plannerCaller) support(ctx context.Context, models sliceModels) (*plannerCaller, error) {
	if p.stopped || p.last == nil {
		return nil, fmt.Errorf("supporting dispatch requires an accepted state and usable planner")
	}
	p.stopped = true
	beforeAcceptance := newAcceptance(ctx, p.r)
	h, err := beforeAcceptance.loadContextHistory(p.scope, p.history.ref)
	if err != nil {
		return nil, err
	}
	state, err := readAccepted[PlannerState](beforeAcceptance, *p.last, PlannerSchema)
	if err != nil {
		return nil, err
	}
	records, err := beforeAcceptance.loadWorkerResults(p.scope, p.workerResults)
	if err != nil {
		return nil, err
	}
	if err := beforeAcceptance.checkPlannerWithWorkers(*p.last, h, state.Data.Previous, records); err != nil {
		return nil, err
	}
	work := state.Data.SupportingWork
	if work == nil {
		return nil, fmt.Errorf("supporting dispatch requires structured supporting_work, not pending text")
	}
	if err := p.close(ctx); err != nil {
		return nil, err
	}
	key := "supporting-" + p.last.AttemptID
	if err := p.r.Root().Decision(ctx, key+"-dispatch", "Accepted structured supporting task: "+work.Kind, []contract.Ref{*p.last, h.ref}); err != nil {
		return nil, err
	}
	sources := map[contract.Ref][]file{}
	for _, record := range records {
		maps.Copy(sources, record.sources)
	}
	before := ContextResult{Intake: h.value.Intake, Wiki: h.value.Wiki, Context: h.ref, Ready: h.value.Readiness == "ready"}
	var after ContextResult
	switch work.Kind {
	case "resolve":
		after, err = resolveWithProposal(ctx, p.r, p.scope, models, before, p.last, sources)
	case "refresh", "update":
		after, err = reviseSlice(ctx, p.r, p.scope, models, before, work.Sources, work.Kind == "update", p.last, sources)
	}
	if err != nil {
		return nil, err
	}
	afterAcceptance := newAcceptance(ctx, p.r)
	next, err := afterAcceptance.loadContextHistory(p.scope, after.Context)
	if err != nil {
		return nil, err
	}
	if err := afterAcceptance.checkPlannerContextChange(next, p.last); err != nil {
		return nil, fmt.Errorf("supporting result acceptance: %w", err)
	}
	if err := p.r.Root().Decision(ctx, key+"-recorded", "Supporting result accepted for Planner reassessment, not a verified claim or final report", []contract.Ref{*p.last, before.Context, after.Context}); err != nil {
		return nil, err
	}
	return openPlannerWithResults(ctx, p.r, p.scope, p.model, after.Context, p.last, p.workerResults)
}
