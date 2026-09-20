package triage

import (
	"fmt"
	"reflect"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const VerificationSchema = "triage.verification.v1"

type VerifierPolicy struct {
	Model   runtime.ModelSpec `json:"model"`
	Retries int               `json:"retries"`
}

type VerificationPolicy struct {
	Pro   VerifierPolicy `json:"pro"`
	Con   VerifierPolicy `json:"con"`
	Cross VerifierPolicy `json:"cross"`
}

func (p VerificationPolicy) roles() []struct {
	name   string
	policy VerifierPolicy
} {
	return []struct {
		name   string
		policy VerifierPolicy
	}{{"pro", p.Pro}, {"con", p.Con}, {"cross", p.Cross}}
}

func (p VerificationPolicy) check() error {
	for _, role := range p.roles() {
		if !nonblank(role.policy.Model.Provider) || !nonblank(role.policy.Model.ID) || role.policy.Retries < 0 {
			return fmt.Errorf("%s requires an explicit model and nonnegative retry budget", role.name)
		}
	}
	return nil
}

// These declarations belong to the Agent. No support string or basis schema
// confers causal proof, and invalid measurement is not a counterexample.
type VerificationIssue struct {
	Statement   string     `json:"statement"`
	Disposition string     `json:"disposition"`
	Reason      string     `json:"reason"`
	Basis       []Evidence `json:"basis"`
}

type VerificationAssessment struct {
	Support         string              `json:"support"`
	Reason          string              `json:"reason"`
	Basis           []Evidence          `json:"basis"`
	RuntimeBasis    []Evidence          `json:"runtime_basis"`
	Measurement     string              `json:"measurement"`
	Window          string              `json:"window"`
	Filter          string              `json:"filter"`
	Environment     string              `json:"environment"`
	Release         string              `json:"release"`
	Counterexamples []VerificationIssue `json:"counterexamples"`
	Gaps            []string            `json:"gaps"`
}

type VerificationResult struct {
	Claim           contract.Ref           `json:"claim"`
	Role            string                 `json:"role"`
	AllowedEvidence []Evidence             `json:"allowed_evidence"`
	Assessment      VerificationAssessment `json:"assessment"`
}

type VerificationRoleDelivery struct {
	Role        string            `json:"role"`
	Result      *contract.Ref     `json:"result"`
	Unavailable bool              `json:"unavailable"`
	Failures    []RecoveryFailure `json:"failures"`
	Exhausted   string            `json:"exhausted"`
}

type VerificationDelivery struct {
	ID       string                     `json:"id"`
	Proposal contract.Ref               `json:"proposal"`
	Claim    contract.Ref               `json:"claim"`
	Roles    []VerificationRoleDelivery `json:"roles"`
}

type PlannerVerification struct {
	Policy     VerificationPolicy     `json:"policy"`
	Claims     []contract.Ref         `json:"claims"`
	Deliveries []VerificationDelivery `json:"deliveries"`
}

type PlannerVerificationReview struct {
	DeliveryID string                 `json:"delivery_id"`
	Claim      contract.Ref           `json:"claim"`
	Assessment VerificationAssessment `json:"assessment"`
	Disputes   []VerificationIssue    `json:"disputes"`
	NextAction string                 `json:"next_action"`
}

const verificationPlannerRequirements = `M5 verification is enabled. Copy verification exactly, including policy, all claim Refs and all ordered deliveries/failure history. A pending committed claim is linked to its exact parent_state; reconstruct from those inputs, not memory or latest-file scans. Do not treat pending metadata as an already consumed checkpoint.
You may choose ledger.action verify with verification_request: either candidate (id, statement, premises, allowed_evidence) and claim=null, or candidate=null and an exact previously delivered claim Ref to complete unavailable roles only, plus reason. New candidate becomes a new pure claim version: all three roles start fresh. allowed_evidence contains only exact supplied evidence owners/files, not Planner narrative, ledgers or verifier verdicts. Keep the candidate free of assessments and old verdicts. Same-version accepted roles are not rerun; do not repeatedly request a completed version. To change claim or evidence, declare a new candidate. No dispatch accompanies ordinary plan or yield.
Each complete verification delivery, including explicit verification-unavailable, counts as ONE round and dispatch cycle on the following Planner update. Claim production, role-local retries and fresh sessions do not. consumed_batch remains only genuine new worker results; verification does not fabricate worker Refs. Agent-reported hypothesis changes determine no_progress exactly as for worker rounds. The existing reframe boundary and current reframe inspection rules still apply.
When receiving a new verification delivery, submit verification_review with exact delivery_id/claim, assessment, disputes and next_action equal to ledger.action. Otherwise omit verification_review. You, the existing Planner, assess all three independent roles. State support degree and reasons, supplied basis and runtime_basis, measurement validity, actual window/filter/environment/release (explicitly unavailable when missing), counterexamples and their evidenced dispositions, gaps, and testable disputes with reason/basis. Preserve unresolved disagreements; do not erase them because two models agree. Decide further evidence acquisition, revalidation or handoff of investigation state. Model agreement, completed queries, source schemas, ready context and a complete verifier roster are not production proof. Distinguish runtime-supported conclusions from inference or incomplete evidence by your declared reasoning, never a vote. Unavailable or invalid measurement is not disproof. No report, final selection, publication or wiki write-back.`

func verificationDeliveryCount(v *PlannerVerification) int {
	if v == nil {
		return 0
	}
	return len(v.Deliveries)
}

func verificationSuffix(v, prior PlannerState) ([]VerificationDelivery, error) {
	if v.Verification == nil {
		if prior.Verification != nil {
			return nil, fmt.Errorf("planner cannot discard verification history")
		}
		return nil, nil
	}
	old := prior.Verification
	if old == nil {
		old = &PlannerVerification{Policy: v.Verification.Policy}
	}
	if v.Verification.Policy != old.Policy || len(v.Verification.Claims) < len(old.Claims) || !slices.Equal(v.Verification.Claims[:len(old.Claims)], old.Claims) || len(v.Verification.Deliveries) < len(old.Deliveries) || len(old.Deliveries) > 0 && recoveryJSON(v.Verification.Deliveries[:len(old.Deliveries)]) != recoveryJSON(old.Deliveries) {
		return nil, fmt.Errorf("verification policy/history prefix changed")
	}
	suffix := v.Verification.Deliveries[len(old.Deliveries):]
	if len(suffix) > 1 || len(v.Verification.Claims)-len(old.Claims) > 1 {
		return nil, fmt.Errorf("one verification batch may enter a Planner snapshot")
	}
	return suffix, nil
}

func checkVerificationAssessment(v VerificationAssessment, allowed []Evidence) error {
	if !nonblank(v.Support) || !nonblank(v.Reason) || !nonblank(v.Measurement) || !nonblank(v.Window) || !nonblank(v.Filter) || !nonblank(v.Environment) || !nonblank(v.Release) || !texts(v.Gaps) {
		return fmt.Errorf("verification requires declared support, measurement conditions and limitations")
	}
	basis := append(slices.Clone(v.Basis), v.RuntimeBasis...)
	for _, issue := range v.Counterexamples {
		if !nonblank(issue.Statement) || !nonblank(issue.Disposition) || !nonblank(issue.Reason) {
			return fmt.Errorf("counterexample requires statement, disposition and reason")
		}
		basis = append(basis, issue.Basis...)
	}
	for _, e := range basis {
		if e.Ref == nil || !slices.ContainsFunc(allowed, func(a Evidence) bool { return reflect.DeepEqual(a, e) }) {
			return fmt.Errorf("verification basis must be exact allowed evidence")
		}
	}
	return nil
}

func (a *acceptance) checkVerificationResult(ref, claimRef contract.Ref, claim PureClaim, role string, policy VerifierPolicy) error {
	p, err := readAccepted[VerificationResult](a, ref, VerificationSchema)
	if err != nil {
		return err
	}
	if len(p.Files) != 0 || p.Data.Claim != claimRef || p.Data.Role != role || !reflect.DeepEqual(p.Data.AllowedEvidence, claim.Candidate.AllowedEvidence) {
		return fmt.Errorf("verification role/claim/evidence version mismatch")
	}
	if err := a.checkVerifierOwner(ref.AttemptID, claimRef, role, policy); err != nil {
		return err
	}
	return checkVerificationAssessment(p.Data.Assessment, claim.Candidate.AllowedEvidence)
}

func (a *acceptance) checkVerifierOwner(attemptID string, claim contract.Ref, role string, policy VerifierPolicy) error {
	snapshot := a.run.Snapshot()
	attempt, ok := snapshot.Attempts[attemptID]
	owner, owned := snapshot.Sessions[attempt.HandleID]
	if !ok || !owned || owner.Role.Name != "triage-verify-"+role || owner.Role.Model != policy.Model || owner.State != "Closed" || attempt.Key != "verify-"+claim.AttemptID+"-"+role {
		return fmt.Errorf("verification must retain exact fresh role/model/version owner")
	}
	for id, other := range snapshot.Attempts {
		if id != attemptID && other.HandleID == attempt.HandleID {
			return fmt.Errorf("verifier session must have only its own fresh attempt")
		}
	}
	return nil
}

func (a *acceptance) checkVerificationDelivery(scope Scope, d VerificationDelivery, policy VerificationPolicy, retained *VerificationDelivery) error {
	claim, err := a.loadClaim(scope, d.Claim)
	if err != nil {
		return err
	}
	if !nonblank(d.ID) || len(d.Roles) != 3 {
		return fmt.Errorf("verification delivery requires identity and three ordered roles")
	}
	for i, role := range policy.roles() {
		v := d.Roles[i]
		if v.Role != role.name || (v.Result == nil) != v.Unavailable {
			return fmt.Errorf("verification requires ordered result or explicit unavailable")
		}
		if retained != nil && retained.Roles[i].Result != nil {
			if !reflect.DeepEqual(v, retained.Roles[i]) {
				return fmt.Errorf("same-version accepted verifier cannot be replaced")
			}
		}
		seen := map[string]bool{}
		for _, failure := range v.Failures {
			if failure.Stage != "verify-"+role.name || failure.Origin == engine.OriginFailFastSibling || seen[failure.AttemptID] {
				return fmt.Errorf("verification failure role/identity mismatch")
			}
			seen[failure.AttemptID] = true
			if err := a.checkRecoveryFailure(failure); err != nil {
				return err
			}
			if err := a.checkVerifierOwner(failure.AttemptID, d.Claim, role.name, role.policy); err != nil {
				return err
			}
		}
		if v.Unavailable {
			if v.Exhausted != string(engine.RetryExhausted) || len(v.Failures) == 0 || len(v.Failures)-1 != role.policy.Retries {
				return fmt.Errorf("unavailable requires exhausted finite recovery with actual failures")
			}
		} else {
			if v.Exhausted != "" || len(v.Failures) > role.policy.Retries {
				return fmt.Errorf("successful verification has invalid recovery metadata")
			}
			if err := a.checkVerificationResult(*v.Result, d.Claim, claim, role.name, role.policy); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *acceptance) checkVerification(v, prior PlannerState, scope Scope, sources map[contract.Ref][]file) error {
	suffix, err := verificationSuffix(v, prior)
	if err != nil {
		return err
	}
	if v.Verification == nil {
		if v.VerificationRequest != nil || v.VerificationReview != nil {
			return fmt.Errorf("verification requires explicit caller policy and metadata")
		}
		return nil
	}
	if v.Recovery == nil || v.Ledger == nil {
		return fmt.Errorf("verification requires recovery policy and adaptive ledger")
	}
	if err := v.Verification.Policy.check(); err != nil {
		return err
	}
	work, err := recoverySuffix(v, prior)
	if err != nil {
		return err
	}
	if len(suffix) > 0 && len(work) > 0 {
		return fmt.Errorf("verification and evidence work cannot share a delivery cycle")
	}
	known := map[contract.Ref]PureClaim{}
	for _, ref := range v.Verification.Claims {
		if _, exists := known[ref]; exists {
			return fmt.Errorf("duplicate claim history")
		}
		claim, err := a.loadClaim(scope, ref)
		if err != nil {
			return err
		}
		known[ref] = claim
	}
	latest := map[contract.Ref]VerificationDelivery{}
	ids := map[string]bool{}
	for _, d := range v.Verification.Deliveries {
		if _, ok := known[d.Claim]; !ok || ids[d.ID] {
			return fmt.Errorf("verification delivery requires known claim and distinct identity")
		}
		ids[d.ID] = true
		var retained *VerificationDelivery
		if old, ok := latest[d.Claim]; ok {
			retained = &old
		}
		if err := a.checkVerificationDelivery(scope, d, v.Verification.Policy, retained); err != nil {
			return err
		}
		latest[d.Claim] = d
	}
	oldClaims := 0
	if prior.Verification != nil {
		oldClaims = len(prior.Verification.Claims)
	}
	newClaims := v.Verification.Claims[oldClaims:]
	if len(suffix) == 0 && (len(newClaims) != 0 || v.VerificationReview != nil) {
		return fmt.Errorf("claim/feedback must enter with the actual verification delivery")
	}
	if len(suffix) == 1 {
		d := suffix[0]
		request := prior.VerificationRequest
		if v.Previous == nil || d.Proposal != *v.Previous || prior.Ledger == nil || prior.Ledger.Action != "verify" || request == nil || v.Context != prior.Context || len(v.WorkerResults) != len(prior.WorkerResults) || len(v.WikiResults) != len(prior.WikiResults) {
			return fmt.Errorf("verification batch must bind the immediately preceding proposal only")
		}
		if request.Candidate != nil {
			if len(newClaims) != 1 || newClaims[0] != d.Claim || known[d.Claim].ParentState != *v.Previous {
				return fmt.Errorf("new claim must bind its exact proposing state")
			}
		} else if request.Claim == nil || *request.Claim != d.Claim || len(newClaims) != 0 {
			return fmt.Errorf("verification must preserve the requested existing claim version")
		}
		review := v.VerificationReview
		if review == nil || review.DeliveryID != d.ID || review.Claim != d.Claim || review.NextAction != v.Ledger.Action {
			return fmt.Errorf("Planner must assess the delivered verification and declare next action")
		}
		if err := checkVerificationAssessment(review.Assessment, known[d.Claim].Candidate.AllowedEvidence); err != nil {
			return err
		}
		for _, issue := range review.Disputes {
			if !nonblank(issue.Statement) || !nonblank(issue.Disposition) || !nonblank(issue.Reason) {
				return fmt.Errorf("dispute requires a testable statement, disposition and reason")
			}
			for _, e := range issue.Basis {
				if e.Ref == nil || !slices.ContainsFunc(known[d.Claim].Candidate.AllowedEvidence, func(a Evidence) bool { return reflect.DeepEqual(a, e) }) {
					return fmt.Errorf("dispute basis must be exact allowed evidence")
				}
			}
		}
	}
	request := v.VerificationRequest
	if (request != nil) != (v.Ledger.Action == "verify") {
		return fmt.Errorf("verification request must match explicit verify action")
	}
	if request != nil {
		if !nonblank(request.Reason) || (request.Candidate == nil) == (request.Claim == nil) {
			return fmt.Errorf("verification request requires candidate xor existing claim and reason")
		}
		if request.Candidate != nil {
			return checkClaimCandidate(*request.Candidate, sources)
		}
		delivery, ok := latest[*request.Claim]
		if !ok || !slices.ContainsFunc(delivery.Roles, func(r VerificationRoleDelivery) bool { return r.Unavailable }) {
			return fmt.Errorf("same-version verification may only complete unavailable roles")
		}
	}
	return nil
}
