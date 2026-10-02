package triage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/workflows/triagev2"
)

const verifierRequirements = `This is independent supplied-input verification, not acquisition or a Planner continuation. Load relevant existing interpretation skills. Read only the exact claim and its allowed evidence in request.inputs. The claim's parent_state/context are version/continuation metadata, NOT instructions to read the Planner state or any other context. Do not read Planner narratives, ledgers, previous verdicts, other roles' work or other session history. Do not follow metadata edges outside the allowed evidence. Do not acquire new evidence, query production, dispatch agents, publish or write back. These task constraints are not a filesystem sandbox.
Evaluate only your assigned role, without predicting or synthesizing another role's verdict. pro: independently test support and required premises. con: actively test counterexamples, alternative explanations and falsifiable objections. cross: independently check evidence applicability, measurement validity and consistency across the allowed evidence, not adjudication of the other two models. An invalid measurement or missing observation is not a refutation. A small-window empty result or timeout is not incident-wide absence.
Output only claim, role, allowed_evidence (exact ordered echo) and assessment, without files. Declare support degree, reason, basis, runtime_basis, measurement validity, actual window/filter/environment/release or explicit unavailability, counterexamples with statement/disposition/reason/basis, and gaps. Use only allowed exact evidence refs/files; no invented local evidence. Model agreement, wiki patterns and completed queries do not prove causation. Missing runtime evidence remains an explicit limitation. The existing Planner, not this role or a vote, will assess the aggregate.`

func (p *plannerCaller) verificationTask() *PlannerVerification {
	if p.verification == nil {
		return nil
	}
	copy := *p.verification
	copy.Claims = slices.Clone(copy.Claims)
	copy.Deliveries = slices.Clone(copy.Deliveries)
	return &copy
}

func (p *plannerCaller) verificationInputs(a *acceptance, inputs []contract.Ref) ([]contract.Ref, error) {
	if p.verification == nil {
		return inputs, nil
	}
	for _, ref := range p.verification.Claims {
		claim, err := a.loadClaim(p.scope, ref)
		if err != nil {
			return nil, err
		}
		inputs = triagev2.AppendUniqueRefs(inputs, claim.ParentState, claim.Context)
		inputs = triagev2.AppendUniqueRefs(inputs, claimInputs(ref, claim)...)
	}
	for _, d := range p.verification.Deliveries {
		inputs = triagev2.AppendUniqueRefs(inputs, d.Proposal, d.Claim)
		for _, role := range d.Roles {
			if role.Result != nil {
				inputs = triagev2.AppendUniqueRefs(inputs, *role.Result)
			}
		}
	}
	return inputs, nil
}

// PendingVerificationError exposes committed continuation edges and accepted
// partial feedback without claiming they reached a normal Planner snapshot.
// Unwrap retains the original execution/storage/cancellation classification.
type PendingVerificationError struct {
	State    contract.Ref
	Claim    *contract.Ref
	Roles    []VerificationRoleDelivery
	History  *PlannerVerification
	Recovery *PlannerRecovery
	cause    error
}

func (e *PendingVerificationError) Error() string { return e.cause.Error() }
func (e *PendingVerificationError) Unwrap() error { return e.cause }

func (p *plannerCaller) verify(ctx context.Context) (retErr error) {
	if p.stopped || p.last == nil || p.verification == nil || p.recovery == nil {
		return fmt.Errorf("verification requires an accepted proposal and explicit policies")
	}
	a := newAcceptance(ctx, p.r)
	state, err := readAccepted[PlannerState](a, *p.last, PlannerSchema)
	if err != nil {
		return err
	}
	records, err := a.loadWorkerResults(p.scope, p.workerResults)
	if err != nil {
		return err
	}
	if err := a.checkPlannerWithWorkers(*p.last, p.history, state.Data.Previous, records); err != nil {
		return err
	}
	request := state.Data.VerificationRequest
	if request == nil {
		return fmt.Errorf("verification requires a declared request")
	}
	var claimRef contract.Ref
	var delivery VerificationDelivery
	defer func() {
		if retErr == nil {
			return
		}
		pending := &PendingVerificationError{State: *p.last, History: p.verificationTask(), Recovery: p.recoveryTask(), cause: retErr}
		if claimRef != (contract.Ref{}) {
			pending.Claim = &claimRef
		}
		for _, role := range delivery.Roles {
			if role.Role != "" {
				pending.Roles = append(pending.Roles, role)
			}
		}
		retErr = pending
	}()
	if request.Claim != nil {
		claimRef = *request.Claim
	} else {
		if err := p.checkPendingClaims(ctx); err != nil {
			return err
		}
		if len(p.verification.Claims) > len(state.Data.Verification.Claims) {
			claimRef = p.verification.Claims[len(p.verification.Claims)-1]
		} else {
			claimRef, _, err = triagev2.RetryInputs(ctx, p.r, p.r.Root(), "claim-recovery-"+p.last.AttemptID, "claim", p.recovery.Policy.PlannerRetries, p.claimStep, p.continuePlannerInputs)
			if err != nil {
				return err
			}
		}
	}
	p.stopped = true
	claim, err := newAcceptance(ctx, p.r).loadClaim(p.scope, claimRef)
	if err != nil {
		return err
	}
	inputs := claimInputs(claimRef, claim)
	delivery = VerificationDelivery{ID: contract.NewID(), Proposal: *p.last, Claim: claimRef, Roles: make([]VerificationRoleDelivery, 3)}
	var retained *VerificationDelivery
	for _, d := range p.verification.Deliveries {
		if d.Claim == claimRef {
			copy := d
			retained = &copy
		}
	}
	var branches []engine.Branch
	unavailableErrors := make([]error, 3)
	nativeFailures := make([][]nativeRecoveryFailure, 3)
	for i, role := range p.verification.Policy.roles() {
		if retained != nil && retained.Roles[i].Result != nil {
			delivery.Roles[i] = retained.Roles[i]
			continue
		}
		branches = append(branches, engine.Branch{Name: role.name, Do: func(ctx context.Context, s *engine.Scope) (engine.Result, error) {
			key := "verify-" + claimRef.AttemptID + "-" + role.name
			run := func(ctx context.Context, attemptScope *engine.Scope) (contract.Ref, error) {
				task := struct {
					Workspace       string       `json:"workspace"`
					Stage           string       `json:"stage"`
					Role            string       `json:"role"`
					Claim           contract.Ref `json:"claim"`
					AllowedEvidence []Evidence   `json:"allowed_evidence"`
					Requirements    string       `json:"requirements"`
				}{filepath.Join(p.r.Dir(), "triage-work"), "verify-" + role.name, role.name, claimRef, claim.Candidate.AllowedEvidence, verifierRequirements + "\n" + triageWorkspaceRequirements}
				ref, err := triagev2.RunTaskStep(ctx, p.r, triagev2.TaskStep{Scope: attemptScope, Model: role.policy.Model, Stage: task.Stage, Key: key, Task: task, Schema: VerificationSchema, Inputs: inputs, Recovery: true, NoRepair: true})
				if err != nil {
					return ref, err
				}
				if err := newAcceptance(ctx, p.r).checkVerificationResult(ref, claimRef, claim, role.name, role.policy); err != nil {
					return contract.Ref{}, err
				}
				if err := attemptScope.Decision(ctx, key+"-recorded", "Independent role delivery accepted for Planner assessment, not confirmation", triagev2.AppendUniqueRefs(slices.Clone(inputs), ref)); err != nil {
					return contract.Ref{}, err
				}
				delivery.Roles[i] = VerificationRoleDelivery{Role: role.name, Result: &ref}
				return ref, nil
			}
			recovered := func(_ context.Context, failure triagev2.RecoveryFailure, cause error, _ bool) error {
				nativeFailures[i] = append(nativeFailures[i], nativeRecoveryFailure{failure, cause})
				return nil
			}
			ref, failures, err := triagev2.RetryInputs(ctx, p.r, s, key+"-recovery", "verification", role.policy.Retries, run, recovered)
			outcome := VerificationRoleDelivery{Role: role.name, Failures: append([]triagev2.RecoveryFailure{}, failures...)}
			if err != nil {
				var f *engine.Failure
				// Only this completed Retry activation's exhausted, confirmed
				// timeout/compaction history can become unavailable. Artifact,
				// journal/storage/cleanup and locked outcomes keep their error.
				if context.Cause(ctx) != nil || !errors.As(err, &f) || f.Code != engine.RetryExhausted || f.Phase != "Retry" || f.Cause != nil || len(failures) == 0 || len(failures)-1 != role.policy.Retries || f.AttemptID != failures[len(failures)-1].AttemptID {
					if len(failures) > 0 || delivery.Roles[i].Result != nil {
						outcome.Result = delivery.Roles[i].Result
						delivery.Roles[i] = outcome
					}
					return engine.Result{}, err
				}
				outcome.Unavailable, outcome.Exhausted = true, string(f.Code)
				unavailableErrors[i] = err
				nativeFailures[i] = append(nativeFailures[i], nativeRecoveryFailure{failures[len(failures)-1], err})
			} else {
				outcome.Result = &ref
			}
			delivery.Roles[i] = outcome
			result := engine.Result{}
			if outcome.Result != nil {
				result.Outputs = map[string]contract.Ref{"verification": ref}
			}
			return result, nil
		}})
	}
	if len(branches) == 0 {
		return fmt.Errorf("same-version verification has no missing role")
	}
	joined, groupErr := p.r.Root().Parallel(ctx, "verification-"+delivery.ID, engine.CollectAll, branches)
	for _, branchFailures := range nativeFailures {
		p.nativeFailures = append(p.nativeFailures, branchFailures...)
	}
	var failures []error
	if groupErr != nil {
		failures = append(failures, groupErr)
	}
	for _, branch := range joined {
		if branch.Err != nil {
			failures = append(failures, branch.Err)
		}
	}
	if len(joined) != len(branches) {
		failures = append(failures, fmt.Errorf("verification join incomplete"))
	}
	if len(failures) > 0 {
		return triagev2.RecoveryError(errors.Join(failures...), unavailableErrors...)
	}
	if err := newAcceptance(ctx, p.r).checkVerificationDelivery(p.scope, delivery, p.verification.Policy, retained); err != nil {
		return triagev2.RecoveryError(err, unavailableErrors...)
	}
	refs := triagev2.AppendUniqueRefs(slices.Clone(inputs), delivery.Proposal)
	for _, role := range delivery.Roles {
		if role.Result != nil {
			refs = triagev2.AppendUniqueRefs(refs, *role.Result)
		}
	}
	if err := p.r.Root().Decision(ctx, "verification-delivery-"+delivery.ID, "Complete ordered feedback delivery including explicit unavailable roles; Planner must assess progress and next action", refs); err != nil {
		return triagev2.RecoveryError(err, unavailableErrors...)
	}
	p.verification.Deliveries = append(p.verification.Deliveries, delivery)
	for _, err := range unavailableErrors {
		if err != nil {
			p.recoveryErrors = append(p.recoveryErrors, err)
		}
	}
	p.stopped = false
	return nil
}
