package triage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

const ClaimSchema = "triage.claim.v1"

type ClaimCandidate struct {
	ID              string     `json:"id"`
	Statement       string     `json:"statement"`
	Premises        []string   `json:"premises"`
	AllowedEvidence []Evidence `json:"allowed_evidence"`
}

type VerificationRequest struct {
	Candidate *ClaimCandidate `json:"candidate"`
	Claim     *contract.Ref   `json:"claim"`
	Reason    string          `json:"reason"`
}

// ParentState is a continuation edge, not permission for a fresh verifier to
// read the Planner narrative. The claim Ref fixes the candidate/evidence version.
type PureClaim struct {
	ParentState contract.Ref   `json:"parent_state"`
	Context     contract.Ref   `json:"context"`
	Candidate   ClaimCandidate `json:"candidate"`
}

func checkClaimCandidate(candidate ClaimCandidate, sources map[contract.Ref][]file) error {
	if !nonblank(candidate.ID) || !nonblank(candidate.Statement) || !texts(candidate.Premises) {
		return fmt.Errorf("claim requires ID, statement and nonblank premises")
	}
	if err := checkInvestigationBasis(candidate.AllowedEvidence, sources); err != nil {
		return err
	}
	seen := map[struct {
		Ref    contract.Ref
		FileID string
	}]bool{}
	for _, e := range candidate.AllowedEvidence {
		key := struct {
			Ref    contract.Ref
			FileID string
		}{*e.Ref, e.FileID}
		if seen[key] {
			return fmt.Errorf("duplicate allowed claim evidence")
		}
		seen[key] = true
	}
	return nil
}

func checkClaimProjection(claim PureClaim, parent PlannerState, files []file) error {
	request := parent.VerificationRequest
	if len(files) != 0 || parent.Ledger == nil || parent.Ledger.Action != "verify" || request == nil || request.Candidate == nil || request.Claim != nil || claim.Context != parent.Context || !reflect.DeepEqual(claim.Candidate, *request.Candidate) {
		return fmt.Errorf("pure claim must equal its parent state's candidate projection")
	}
	return nil
}

func (a *acceptance) loadClaim(scope Scope, ref contract.Ref) (PureClaim, error) {
	p, err := readAccepted[PureClaim](a, ref, ClaimSchema)
	if err != nil {
		return PureClaim{}, err
	}
	claim := p.Data
	snapshot := a.run.Snapshot()
	attempt := snapshot.Attempts[ref.AttemptID]
	owner, owned := snapshot.Sessions[attempt.HandleID]
	parentAttempt := snapshot.Attempts[claim.ParentState.AttemptID]
	parentOwner, parentOwned := snapshot.Sessions[parentAttempt.HandleID]
	if !owned || !parentOwned || owner.Role.Name != "triage-planner" || parentOwner.Role.Name != "triage-planner" || owner.Role.Model != parentOwner.Role.Model || attempt.Key != "claim-"+claim.ParentState.AttemptID {
		return claim, fmt.Errorf("claim must be produced by the proposing Planner role/model")
	}
	parent, err := readAccepted[PlannerState](a, claim.ParentState, PlannerSchema)
	if err != nil {
		return claim, err
	}
	if err := checkClaimProjection(claim, parent.Data, p.Files); err != nil {
		return claim, err
	}
	if _, err := a.loadContextHistory(scope, claim.Context); err != nil {
		return claim, err
	}
	// The proposing snapshot validates authorization and lineage. Here retain
	// its exact projection and re-resolve owners, without recursively replaying
	// every worker's Planner history for every historical claim.
	sources := map[contract.Ref][]file{}
	for _, e := range claim.Candidate.AllowedEvidence {
		if e.Ref == nil || e.Ref.SchemaID == PlannerSchema || e.Ref.SchemaID == ClaimSchema || e.Ref.SchemaID == VerificationSchema {
			return claim, fmt.Errorf("claim evidence cannot inject Planner or verifier publications")
		}
		owner, err := readAccepted[json.RawMessage](a, *e.Ref, e.Ref.SchemaID)
		if err != nil {
			return claim, err
		}
		sources[*e.Ref] = owner.Files
	}
	return claim, checkClaimCandidate(claim.Candidate, sources)
}

func claimInputs(ref contract.Ref, claim PureClaim) []contract.Ref {
	inputs := []contract.Ref{ref}
	for _, e := range claim.Candidate.AllowedEvidence {
		inputs = appendUniqueRefs(inputs, *e.Ref)
	}
	return inputs
}

func (p *plannerCaller) claimStep(ctx context.Context, executionScope *engine.Scope) (contract.Ref, error) {
	if p.stopped || p.last == nil || p.verification == nil || p.recovery == nil {
		return contract.Ref{}, fmt.Errorf("claim requires an accepted Planner proposal and explicit policies")
	}
	a := newAcceptance(ctx, p.r)
	state, err := readAccepted[PlannerState](a, *p.last, PlannerSchema)
	if err != nil {
		return contract.Ref{}, err
	}
	records, err := a.loadWorkerResults(p.scope, p.workerResults)
	if err != nil {
		return contract.Ref{}, err
	}
	if err := a.checkPlannerWithWorkers(*p.last, p.history, state.Data.Previous, records); err != nil {
		return contract.Ref{}, err
	}
	request := state.Data.VerificationRequest
	if request == nil || request.Candidate == nil || request.Claim != nil {
		return contract.Ref{}, fmt.Errorf("claim-producing Step requires a declared new candidate")
	}
	projection := PureClaim{ParentState: *p.last, Context: p.history.ref, Candidate: *request.Candidate}
	task := struct {
		Stage              string            `json:"stage"`
		Projection         PureClaim         `json:"projection"`
		Recovery           *PlannerRecovery  `json:"recovery"`
		ControllerFeedback []PlannerFeedback `json:"controller_feedback"`
		Requirements       string            `json:"requirements"`
	}{"planner-claim", projection, p.recoveryTask(), slices.Clone(p.pendingFeedback), "You are the same investigation Planner. Produce only the supplied pure claim projection, exactly: parent_state, context, candidate (ID, statement, premises, allowed_evidence). Read the exact committed parent state for the candidate; do not revise it. No acquisition, narrative, ledger, assessments, previous verdicts or files. This is a versioned candidate, not a confirmation. The Controller will deliver this claim and only its allowed evidence to fresh independent verifiers. Full investigation state remains at parent_state, not inside this output."}
	prompt, err := json.Marshal(task)
	if err != nil {
		return contract.Ref{}, err
	}
	inputs := appendUniqueRefs(claimInputs(*p.last, projection), p.history.ref)
	key := "claim-" + p.last.AttemptID
	out, err := executionScope.Step(ctx, engine.StepSpec{Key: key, Session: p.handle, Prompt: string(prompt), Inputs: inputs, Output: contract.Spec{SchemaID: ClaimSchema}, Timeout: 30 * time.Minute})
	if err != nil {
		p.stopped = true
		return contract.Ref{}, &taskFailure{cause: err, handle: p.handle, identity: p.identity, stage: "planner", attempt: out.AttemptID}
	}
	if out.Execution.SessionID != p.identity.SessionID {
		p.stopped = true
		return contract.Ref{}, fmt.Errorf("claim execution identity mismatch")
	}
	claim, err := newAcceptance(ctx, p.r).loadClaim(p.scope, out.Output)
	if err == nil && !reflect.DeepEqual(claim, projection) {
		err = fmt.Errorf("claim differs from dispatched projection")
	}
	if err == nil {
		err = executionScope.Decision(ctx, key+"-recorded", "Pure candidate projection accepted; no causal verdict", appendUniqueRefs(inputs, out.Output))
	}
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	p.session = out.Execution.SessionID
	p.verification.Claims = append(p.verification.Claims, out.Output)
	return out.Output, nil
}

// Reconstruct the pending claim through explicit committed edges. No search for
// a latest publication and no assumption that session memory survived handoff.
func (p *plannerCaller) checkPendingClaims(ctx context.Context) error {
	if p.verification == nil {
		return nil
	}
	a := newAcceptance(ctx, p.r)
	var accepted []contract.Ref
	if p.last != nil {
		state, err := readAccepted[PlannerState](a, *p.last, PlannerSchema)
		if err != nil {
			return err
		}
		if state.Data.Verification != nil {
			accepted = state.Data.Verification.Claims
		}
	}
	if len(p.verification.Claims) < len(accepted) || !slices.Equal(p.verification.Claims[:len(accepted)], accepted) || len(p.verification.Claims)-len(accepted) > 1 {
		return fmt.Errorf("pending claim history differs from committed state")
	}
	for _, ref := range p.verification.Claims[len(accepted):] {
		claim, err := a.loadClaim(p.scope, ref)
		if err != nil {
			return err
		}
		if p.last == nil || claim.ParentState != *p.last {
			return fmt.Errorf("pending claim requires exact parent continuation")
		}
	}
	return nil
}
