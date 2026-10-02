package triage

import (
	"context"
	"fmt"
	"maps"
	"reflect"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/workflows/triagev2"
)

// These refs have passed their phase's existing acceptance, not the final
// context transition. They retain their actual owners across a safe resume.
type supportContinuation struct {
	Proposal      contract.Ref   `json:"proposal"`
	Context       contract.Ref   `json:"context"`
	Work          SupportingWork `json:"work"`
	Intake        *contract.Ref  `json:"intake,omitempty"`
	Wiki          *contract.Ref  `json:"wiki,omitempty"`
	FailedPhase   string         `json:"failed_phase"`
	Authorization *contract.Ref  `json:"authorization,omitempty"`
}

func (c *supportContinuation) refs() []contract.Ref {
	refs := []contract.Ref{c.Proposal, c.Context}
	if c.Intake != nil {
		refs = triagev2.AppendUniqueRefs(refs, *c.Intake)
	}
	if c.Wiki != nil {
		refs = triagev2.AppendUniqueRefs(refs, *c.Wiki)
	}
	if c.Authorization != nil {
		refs = triagev2.AppendUniqueRefs(refs, *c.Authorization)
	}
	return refs
}

func (c *supportContinuation) step(ctx context.Context, r *engine.Run, models sliceModels, key string, task stageTask, schema string, inputs []contract.Ref) (contract.Ref, error) {
	if c == nil {
		return sliceStep(ctx, r, models, key, task, schema, inputs)
	}
	if schema == IntakeSchema && c.Intake != nil {
		return *c.Intake, nil
	}
	if schema == WikiSchema && c.Wiki != nil {
		return *c.Wiki, nil
	}
	c.FailedPhase = task.Stage
	if c.Authorization != nil {
		inputs = triagev2.AppendUniqueRefs(inputs, *c.Authorization)
		task.Requirements += "\nRead the exact continuation authorization input and its evidence-backed recovery choice. Continue only this unfinished phase of the original supporting proposal; do not repeat completed acquisition or resubmit remote work whose status remains unknown. Preserve all original owners and diagnostics."
	}
	return sliceStepRecovery(ctx, r, models, key, task, schema, inputs, true)
}

func (c *supportContinuation) accepted(schema string, ref contract.Ref) {
	if c == nil {
		return
	}
	switch schema {
	case IntakeSchema:
		c.Intake = &ref
	case WikiSchema:
		c.Wiki = &ref
	}
}

func (c *supportContinuation) key(key string) string {
	if c == nil {
		return key
	}
	if c.Authorization != nil {
		return key + "-resume-" + c.Authorization.AttemptID
	}
	return key + "-proposal-" + c.Proposal.AttemptID
}

func (a *acceptance) recoverySources(scope Scope, base map[contract.Ref][]file, recovery *PlannerRecovery) (map[contract.Ref][]file, error) {
	if recovery == nil {
		return base, nil
	}
	sources := maps.Clone(base)
	if sources == nil {
		sources = map[contract.Ref][]file{}
	}
	for _, d := range recovery.Deliveries {
		c := d.Support
		if c == nil {
			continue
		}
		original, err := readAccepted[PlannerState](a, c.Proposal, PlannerSchema)
		if err != nil {
			return nil, err
		}
		if original.Data.Context != c.Context || d.Context != c.Context || !reflect.DeepEqual(original.Data.SupportingWork, &c.Work) {
			return nil, fmt.Errorf("support continuation lost original proposal binding")
		}
		h, err := a.loadContextHistory(scope, c.Context)
		if err != nil {
			return nil, err
		}
		if err := a.checkSupportingWork(h, &c.Work); err != nil {
			return nil, err
		}
		intakeRef := h.value.Intake
		if c.Intake != nil {
			ih, err := a.loadIntakeHistory(*c.Intake, scope.Ticket)
			if err != nil {
				return nil, err
			}
			if c.Work.Kind == "resolve" || ih.value.Previous == nil || *ih.value.Previous != h.value.Intake || ih.value.Update != (c.Work.Kind == "update") || c.Work.Kind == "refresh" && !reflect.DeepEqual(ih.value.Work, c.Work.Sources) {
				return nil, fmt.Errorf("partial intake differs from supporting task")
			}
			intakeRef = *c.Intake
			maps.Copy(sources, ih.sources)
		}
		if c.Wiki != nil {
			if _, err := a.checkWiki(*c.Wiki, intakeRef); err != nil {
				return nil, err
			}
			wiki, err := readAccepted[WikiSearch](a, *c.Wiki, WikiSchema)
			if err != nil {
				return nil, err
			}
			sources[*c.Wiki] = wiki.Files
		}
	}
	return sources, nil
}

func supportResume(state PlannerState, current contract.Ref) (*supportContinuation, error) {
	if state.Recovery == nil {
		return nil, nil
	}
	for _, choice := range state.RecoveryChoices {
		if choice.Action != "resume" {
			continue
		}
		for _, d := range state.Recovery.Deliveries {
			if d.ID != choice.DeliveryID || d.Kind != "support" {
				continue
			}
			if d.Support == nil || d.Context != state.Context || !reflect.DeepEqual(state.SupportingWork, &d.Support.Work) {
				return nil, fmt.Errorf("support resume must retain original context/proposal/work")
			}
			c := *d.Support
			c.Authorization = &current
			return &c, nil
		}
	}
	return nil, nil
}
