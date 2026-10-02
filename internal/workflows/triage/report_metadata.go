package triage

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/workflows/triagev2"
)

// Owner is the first committed Planner snapshot receiving this delivery/item,
// not the latest snapshot that repeats it. Index also identifies undispatched
// siblings, which must not acquire a fabricated attempt or result Ref.
type ReportFailure struct {
	Owner      contract.Ref             `json:"owner"`
	Kind       string                   `json:"kind"`
	DeliveryID string                   `json:"delivery_id"`
	Claim      *contract.Ref            `json:"claim"`
	Role       string                   `json:"role"`
	Index      int                      `json:"index"`
	Failure    triagev2.RecoveryFailure `json:"failure"`
}

type ReportDisposition struct {
	Item    ReportFailure  `json:"item"`
	Action  string         `json:"action"`
	Reason  string         `json:"reason"`
	Results []contract.Ref `json:"results"`
	Basis   []Evidence     `json:"basis"`
}

type ReportMetadata struct {
	Dispositions   []ReportDisposition        `json:"dispositions"`
	ReportFailures []triagev2.RecoveryFailure `json:"report_failures"`
	Budget         *ReportBudget              `json:"budget"`
}

type reportContinuation struct {
	Failures           []ReportFailure            `json:"failures"`
	ReportFailures     []triagev2.RecoveryFailure `json:"report_failures"`
	Policy             ReportPolicy               `json:"policy"`
	Budget             *ReportBudget              `json:"budget"`
	Recovery           *PlannerRecovery           `json:"recovery"`
	Verification       *PlannerVerification       `json:"verification"`
	ControllerFeedback []PlannerFeedback          `json:"controller_feedback"`
}

func (p *plannerCaller) reportContinuation(a *acceptance) (*reportContinuation, error) {
	if p.reporting == nil {
		return nil, nil
	}
	request := &reportContinuation{Failures: []ReportFailure{}, ReportFailures: append([]triagev2.RecoveryFailure{}, p.reporting.failures...), Policy: p.reporting.policy, Budget: p.reporting.budget, Recovery: p.recoveryTask(), Verification: p.verificationTask(), ControllerFeedback: append([]PlannerFeedback{}, p.pendingFeedback...)}
	var chain []contract.Ref
	for ref := p.last; ref != nil; {
		state, err := readAccepted[PlannerState](a, *ref, PlannerSchema)
		if err != nil {
			return nil, err
		}
		chain = append(chain, *ref)
		ref = state.Data.Previous
	}
	slices.Reverse(chain)
	seenDeliveries := map[string]bool{}
	seenVerification := map[string]bool{}
	plannerFailures := 0
	for _, ref := range chain {
		state, err := readAccepted[PlannerState](a, ref, PlannerSchema)
		if err != nil {
			return nil, err
		}
		if recovery := state.Data.Recovery; recovery != nil {
			for i, failure := range recovery.PlannerFailures {
				if i >= plannerFailures {
					request.Failures = append(request.Failures, ReportFailure{Owner: ref, Kind: "planner", Index: i, Failure: failure})
				}
			}
			plannerFailures = len(recovery.PlannerFailures)
			for _, delivery := range recovery.Deliveries {
				if seenDeliveries[delivery.ID] {
					continue
				}
				seenDeliveries[delivery.ID] = true
				for i, failure := range delivery.Failures {
					request.Failures = append(request.Failures, ReportFailure{Owner: ref, Kind: delivery.Kind, DeliveryID: delivery.ID, Index: i, Failure: failure})
				}
			}
		}
		if verification := state.Data.Verification; verification != nil {
			for _, delivery := range verification.Deliveries {
				if seenVerification[delivery.ID] {
					continue
				}
				seenVerification[delivery.ID] = true
				for _, role := range delivery.Roles {
					for i, failure := range role.Failures {
						claim := delivery.Claim
						request.Failures = append(request.Failures, ReportFailure{Owner: ref, Kind: "verification", DeliveryID: delivery.ID, Claim: &claim, Role: role.Role, Index: i, Failure: failure})
					}
				}
			}
		}
	}
	for _, item := range request.Failures {
		if !slices.ContainsFunc(p.nativeFailures, func(native nativeRecoveryFailure) bool {
			return native.cause != nil && sameRecoveryFailure(native.failure, item.Failure)
		}) {
			return nil, fmt.Errorf("report failure has no retained native cause")
		}
	}
	for _, failure := range request.ReportFailures {
		if failure.Stage != "report" || failure.Origin == engine.OriginFailFastSibling {
			return nil, fmt.Errorf("invalid report retry failure")
		}
		if err := a.checkRecoveryFailure(failure); err != nil {
			return nil, err
		}
	}
	return request, nil
}

// A later publication is not a continuation by chronology or schema alone.
// Follow Controller-accepted deliveries and the Agent's existing resume choices;
// whether the work remains necessary belongs to the report's redirect/basis.
func (a *acceptance) resumesReportDelivery(proposal contract.Ref, original RecoveryDelivery) (bool, error) {
	if proposal == original.Proposal {
		return true, nil
	}
	state, err := readAccepted[PlannerState](a, proposal, PlannerSchema)
	if err != nil {
		return false, err
	}
	if state.Data.Context != original.Context || state.Data.Recovery == nil {
		return false, nil
	}
	for _, choice := range state.Data.RecoveryChoices {
		if choice.Action != "resume" {
			continue
		}
		for _, delivery := range state.Data.Recovery.Deliveries {
			if delivery.ID != choice.DeliveryID || delivery.Kind != original.Kind || delivery.Context != original.Context {
				continue
			}
			if delivery.ID == original.ID {
				return true, nil
			}
			if continued, err := a.resumesReportDelivery(delivery.Proposal, original); err != nil || continued {
				return continued, err
			}
		}
	}
	return false, nil
}

func (p *plannerCaller) handledRecoveryResult(a *acceptance, snapshot engine.Snapshot, item ReportFailure, ref contract.Ref) (bool, error) {
	failed := snapshot.Attempts[item.Failure.AttemptID]
	result := snapshot.Attempts[ref.AttemptID]
	failedOwner := snapshot.Sessions[failed.HandleID]
	resultOwner := snapshot.Sessions[result.HandleID]
	owner, err := readAccepted[PlannerState](a, item.Owner, PlannerSchema)
	if err != nil {
		return false, err
	}
	if item.Kind == "planner" {
		if failed.Key == "" || failed.Key != result.Key || failedOwner.Role.Name != "triage-planner" || resultOwner.Role.Name != failedOwner.Role.Name || resultOwner.Role.Model != failedOwner.Role.Model {
			return false, nil
		}
		switch ref.SchemaID {
		case PlannerSchema:
			return ref == item.Owner, nil
		case ClaimSchema:
			claim, err := a.loadClaim(p.scope, ref)
			return err == nil && failed.Key == "claim-"+claim.ParentState.AttemptID, err
		}
		return false, nil
	}
	if owner.Data.Recovery == nil {
		return false, nil
	}
	index := slices.IndexFunc(owner.Data.Recovery.Deliveries, func(d RecoveryDelivery) bool { return d.ID == item.DeliveryID })
	if index < 0 {
		return false, nil
	}
	original := owner.Data.Recovery.Deliveries[index]
	before, err := readAccepted[PlannerState](a, original.Proposal, PlannerSchema)
	if err != nil {
		return false, err
	}
	latest, err := readAccepted[PlannerState](a, *p.last, PlannerSchema)
	if err != nil || latest.Data.Recovery == nil {
		return false, err
	}
	for _, delivery := range latest.Data.Recovery.Deliveries {
		if delivery.Kind != original.Kind || delivery.Context != original.Context || !slices.Contains(delivery.Results, ref) {
			continue
		}
		continued, err := a.resumesReportDelivery(delivery.Proposal, original)
		if err != nil || !continued {
			return false, err
		}
		after, err := readAccepted[PlannerState](a, delivery.Proposal, PlannerSchema)
		if err != nil {
			return false, err
		}
		switch item.Kind {
		case "workers":
			records, err := a.loadWorkerResults(p.scope, latest.Data.WorkerResults)
			if err != nil {
				return false, err
			}
			record, ok := records[ref]
			i := slices.IndexFunc(before.Data.WorkerTasks, func(t WorkerTask) bool { return t.ID == item.Failure.TaskID })
			j := slices.IndexFunc(after.Data.WorkerTasks, func(t WorkerTask) bool { return t.ID == item.Failure.TaskID })
			return ok && i >= 0 && j >= 0 && reflect.DeepEqual(before.Data.WorkerTasks[i], after.Data.WorkerTasks[j]) && record.value.TaskID == item.Failure.TaskID && record.value.Proposal == delivery.Proposal && record.value.Context == original.Context && resultOwner.Role.Name == "triage-"+item.Failure.Stage && (item.Failure.AttemptID == "" || resultOwner.Role.Name == failedOwner.Role.Name && resultOwner.Role.Model == failedOwner.Role.Model), nil
		case "wiki":
			records, err := a.loadWorkerResults(p.scope, latest.Data.WorkerResults)
			if err != nil {
				return false, err
			}
			if _, err := a.wikiSources(p.scope, map[contract.Ref][]file{}, latest.Data.WikiResults, records); err != nil {
				return false, err
			}
			wiki, err := readAccepted[WikiSearch](a, ref, WikiSchema)
			if err != nil {
				return false, err
			}
			binding := wiki.Data.Task
			return before.Data.WikiTask != nil && reflect.DeepEqual(before.Data.WikiTask, after.Data.WikiTask) && binding != nil && binding.TaskID == item.Failure.TaskID && binding.Proposal == delivery.Proposal && binding.Context == original.Context && resultOwner.Role.Name == failedOwner.Role.Name && resultOwner.Role.Model == failedOwner.Role.Model, nil
		case "support":
			prior, next := original.Support, delivery.Support
			if prior == nil || next == nil || next.FailedPhase != "" || prior.FailedPhase != item.Failure.Stage || prior.Proposal != next.Proposal || prior.Context != next.Context || !reflect.DeepEqual(prior.Work, next.Work) || next.Authorization == nil || *next.Authorization != delivery.Proposal || prior.Intake != nil && (next.Intake == nil || *prior.Intake != *next.Intake) || prior.Wiki != nil && (next.Wiki == nil || *prior.Wiki != *next.Wiki) {
				return false, nil
			}
			resume, err := supportResume(after.Data, delivery.Proposal)
			if err != nil || resume == nil || resume.Proposal != prior.Proposal || resume.Context != prior.Context || !reflect.DeepEqual(resume.Work, prior.Work) {
				return false, err
			}
			history, err := a.loadContextHistory(p.scope, ref)
			if err != nil {
				return false, err
			}
			if err := a.checkPlannerContextChange(history, &delivery.Proposal); err != nil {
				return false, err
			}
			phase := ref
			switch prior.FailedPhase {
			case "intake-revision", "intake-update":
				if next.Intake == nil {
					return false, nil
				}
				phase = *next.Intake
			case "wiki-resolution", "wiki-revision":
				if next.Wiki == nil {
					return false, nil
				}
				phase = *next.Wiki
			case "context-resolution", "context-revision":
			default:
				return false, nil
			}
			completed := snapshot.Attempts[phase.AttemptID]
			phaseOwner := snapshot.Sessions[completed.HandleID]
			return completed.State == engine.Succeeded && completed.Output != nil && *completed.Output == phase && completed.LastSeq > failed.LastSeq && phaseOwner.Role.Name == failedOwner.Role.Name && phaseOwner.Role.Model == failedOwner.Role.Model, nil
		}
	}
	return false, nil
}

func checkReportMetadataHeader(v InvestigationReport, task reportTask) error {
	if task.M6 == nil {
		if v.M6 != nil {
			return fmt.Errorf("report continuation metadata requires the M6 caller")
		}
		return nil
	}
	if v.M6 == nil || !slices.EqualFunc(v.M6.ReportFailures, task.M6.ReportFailures, sameRecoveryFailure) || !reflect.DeepEqual(v.M6.Budget, task.M6.Budget) || len(v.M6.Dispositions) != len(task.M6.Failures) {
		return fmt.Errorf("report requires complete M6 history and exact budget metadata")
	}
	if task.M6.Budget != nil && v.Completeness != "incomplete" {
		return fmt.Errorf("resource-limited report must remain incomplete")
	}
	return nil
}

func matchReportDisposition(disposition ReportDisposition, failures []ReportFailure, seen []bool) (int, error) {
	index := slices.IndexFunc(failures, func(item ReportFailure) bool { return reflect.DeepEqual(item, disposition.Item) })
	if index < 0 || seen[index] || !nonblank(disposition.Reason) {
		return index, fmt.Errorf("report disposition requires an exact distinct historical owner/delivery/role/item")
	}
	return index, nil
}

func checkReportDispositionAction(disposition ReportDisposition, completeness string) error {
	switch disposition.Action {
	case "unresolved":
		if len(disposition.Results) != 0 || completeness != "incomplete" {
			return fmt.Errorf("unresolved execution requires incomplete report without success results")
		}
	case "redirect":
		if len(disposition.Basis) == 0 || len(disposition.Results) != 0 {
			return fmt.Errorf("redirect requires evidence basis, not fabricated success")
		}
	case "handled":
		if len(disposition.Results) == 0 {
			return fmt.Errorf("handled failure requires subsequent committed results")
		}
	default:
		return fmt.Errorf("unsupported report disposition")
	}
	return nil
}

func (p *plannerCaller) checkReportMetadata(a *acceptance, v InvestigationReport, task reportTask, inputs []contract.Ref) error {
	if err := checkReportMetadataHeader(v, task); err != nil {
		return err
	}
	if task.M6 == nil {
		return nil
	}
	sources := map[contract.Ref][]file{}
	for _, ref := range inputs {
		doc, err := readAccepted[json.RawMessage](a, ref, ref.SchemaID)
		if err != nil {
			return err
		}
		sources[ref] = doc.Files
	}
	snapshot := p.r.Snapshot()
	seen := make([]bool, len(task.M6.Failures))
	for _, disposition := range v.M6.Dispositions {
		index, err := matchReportDisposition(disposition, task.M6.Failures, seen)
		if err != nil {
			return err
		}
		seen[index] = true
		if err := checkInvestigationBasis(disposition.Basis, sources); err != nil {
			return err
		}
		if err := checkReportDispositionAction(disposition, v.Completeness); err != nil {
			return err
		}
		results := map[contract.Ref]bool{}
		for _, ref := range disposition.Results {
			if !slices.Contains(inputs, ref) || results[ref] {
				return fmt.Errorf("disposition result requires a distinct supplied committed Ref")
			}
			results[ref] = true
			item := disposition.Item
			attempt := snapshot.Attempts[ref.AttemptID]
			after := snapshot.Attempts[item.Failure.AttemptID].LastSeq
			if item.Failure.AttemptID == "" {
				owner, err := readAccepted[PlannerState](a, item.Owner, PlannerSchema)
				if err != nil {
					return err
				}
				for _, delivery := range owner.Data.Recovery.Deliveries {
					if delivery.ID == item.DeliveryID {
						after = snapshot.Attempts[delivery.Proposal.AttemptID].LastSeq
					}
				}
			}
			if attempt.State != engine.Succeeded || attempt.Output == nil || *attempt.Output != ref || attempt.LastSeq <= after {
				return fmt.Errorf("disposition result must follow the failed attempt")
			}
			if item.Kind == "verification" {
				result, err := readAccepted[VerificationResult](a, ref, VerificationSchema)
				if err != nil {
					return err
				}
				if item.Claim == nil || result.Data.Claim != *item.Claim || result.Data.Role != item.Role {
					return fmt.Errorf("handled verification must retain exact claim version and role")
				}
			} else {
				continued, err := p.handledRecoveryResult(a, snapshot, item, ref)
				if err != nil {
					return err
				}
				if !continued {
					return fmt.Errorf("handled result does not continue failed work")
				}
			}
		}
	}
	return nil
}
