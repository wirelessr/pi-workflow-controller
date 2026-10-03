package triage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const (
	ClaimSchema        = "triage.claim.v1"
	VerificationSchema = "triage.verification.v1"
	DeliverySchema     = "triage.delivery.v1"
)

const verifierRequirements = `This is independent verification from supplied inputs only. Read only the claim and the evidence files it allows; the claim's round and steward Refs are binding metadata, not instructions to read them or any other result. Do not read narratives, ledgers, earlier verdicts, other roles' work or session history, do not follow references outside the allowed evidence, and do not acquire evidence, query production systems or dispatch anyone.
Evaluate only your role, without predicting another role's verdict. pro: independently test the support and the premises the claim needs. con: actively test counterexamples, alternative explanations and falsifiable objections. cross: check evidence applicability, measurement validity and consistency across the allowed evidence, not who of the other two is right. An invalid measurement or a missing observation is not a refutation; a small-window empty result or a timeout is not incident-wide absence.
Write no files. Use only the allowed evidence, cited exactly as the claim lists it. Model agreement, wiki patterns and completed queries do not prove causation; missing runtime evidence stays an explicit limitation. The steward, not a vote, assesses the three roles together.`

var verifierRoles = []string{"pro", "con", "cross"}

// VerificationPolicy binds the verifiers; the workflow definition names
// every value.
type VerificationPolicy struct {
	Pro, Con, Cross runtime.ModelSpec
	Timeout         time.Duration
	// MaxRuns bounds verifications of the whole run (at most 2 by design).
	MaxRuns int
}

func (p VerificationPolicy) model(role string) runtime.ModelSpec {
	return map[string]runtime.ModelSpec{"pro": p.Pro, "con": p.Con, "cross": p.Cross}[role]
}

func (p VerificationPolicy) check() error {
	for _, role := range verifierRoles {
		if m := p.model(role); m.Provider == "" || m.ID == "" {
			return fmt.Errorf("verification policy needs a model for %s", role)
		}
	}
	if p.Timeout <= 0 || p.MaxRuns < 1 || p.MaxRuns > 2 {
		return fmt.Errorf("verification policy needs a positive Timeout and MaxRuns of 1 or 2")
	}
	return nil
}

// Claim is the Controller's record of a candidate going to verification.
type Claim struct {
	Round     contract.Ref `json:"round"`
	Steward   contract.Ref `json:"steward"`
	Candidate Candidate    `json:"candidate"`
}

// claimFor copies a round's candidate, citing the round's own evidence
// through the round's Ref so a verifier can resolve it.
func claimFor(round contract.Ref, c Candidate, t2a contract.Ref) Claim {
	own := func(e Evidence) Evidence {
		if e.Ref == nil {
			e.Ref = &round
		}
		return e
	}
	c.AllowedEvidence = slices.Clone(c.AllowedEvidence)
	for i, e := range c.AllowedEvidence {
		c.AllowedEvidence[i] = own(e)
	}
	c.CodeRefs = slices.Clone(c.CodeRefs)
	for i, code := range c.CodeRefs {
		code.Evidence = slices.Clone(code.Evidence)
		for j, e := range code.Evidence {
			code.Evidence[j] = own(e)
		}
		if code.Basis != nil && code.Basis.Ref == nil {
			code.Basis = &BuildRef{Ref: &round, ID: code.Basis.ID}
		}
		c.CodeRefs[i] = code
	}
	return Claim{Round: round, Steward: t2a, Candidate: c}
}

// claimInputs are the claim, the owners of its allowed evidence and the
// rounds holding the deployed builds its code rests on.
func claimInputs(ref contract.Ref, c Claim) []contract.Ref {
	inputs := []contract.Ref{ref}
	add := func(r contract.Ref) {
		if !slices.Contains(inputs, r) {
			inputs = append(inputs, r)
		}
	}
	for _, e := range c.Candidate.AllowedEvidence {
		add(*e.Ref)
	}
	for _, code := range c.Candidate.CodeRefs {
		if code.Basis != nil {
			add(*code.Basis.Ref)
		}
	}
	return inputs
}

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

type Verification struct {
	Claim           contract.Ref           `json:"claim"`
	Role            string                 `json:"role"`
	AllowedEvidence []Evidence             `json:"allowed_evidence"`
	Assessment      VerificationAssessment `json:"assessment"`
}

type DeliveryFailure struct {
	AttemptID  string `json:"attempt_id"`
	Code       string `json:"code"`
	Origin     string `json:"origin"`
	Diagnostic string `json:"diagnostic"`
}

type RoleDelivery struct {
	Role        string            `json:"role"`
	Result      *contract.Ref     `json:"result"`
	Unavailable bool              `json:"unavailable"`
	Failures    []DeliveryFailure `json:"failures"`
}

// Delivery is the Controller's record of one verification.
type Delivery struct {
	Claim contract.Ref   `json:"claim"`
	Roles []RoleDelivery `json:"roles"`
}

// verifyTask is what a verifier checks.
type verifyTask struct {
	Role            string       `json:"role"`
	Claim           contract.Ref `json:"claim"`
	AllowedEvidence []Evidence   `json:"allowed_evidence"`
}

// checkVerification carries the approved verifier acceptance: the exact
// claim, role and allowed evidence, declared support and measurement
// conditions, and basis drawn only from the exact allowed evidence.
func checkVerification(ctx context.Context, r *engine.Run, ref, claimRef contract.Ref, claim Claim, role string) error {
	p, err := readAccepted[Verification](ctx, r, ref, VerificationSchema)
	if err != nil {
		return err
	}
	v := p.Data
	if len(p.Files) != 0 {
		return fmt.Errorf("files: got %d; a verifier writes no files", len(p.Files))
	}
	if err := sameRef("claim", v.Claim, claimRef); err != nil {
		return err
	}
	if v.Role != role {
		return fmt.Errorf("role: got %q; want %q from the request", v.Role, role)
	}
	if !reflect.DeepEqual(v.AllowedEvidence, claim.Candidate.AllowedEvidence) {
		return fmt.Errorf("allowed_evidence: want the claim's allowed evidence copied in order")
	}
	a := v.Assessment
	for field, value := range map[string]string{"support": a.Support, "reason": a.Reason, "measurement": a.Measurement, "window": a.Window, "filter": a.Filter, "environment": a.Environment, "release": a.Release} {
		if !nonblank(value) {
			return fmt.Errorf("assessment.%s: got only whitespace; state it, or state that it is unavailable", field)
		}
	}
	for i, gap := range a.Gaps {
		if !nonblank(gap) {
			return fmt.Errorf("assessment.gaps[%d]: got only whitespace", i)
		}
	}
	allowed := func(field string, basis []Evidence) error {
		for i, e := range basis {
			if !slices.ContainsFunc(claim.Candidate.AllowedEvidence, func(x Evidence) bool { return reflect.DeepEqual(x, e) }) {
				return fmt.Errorf("%s[%d]: file %q is not cited exactly as the claim allows it: %s", field, i, e.FileID, allowedHint(claim.Candidate.AllowedEvidence, e))
			}
		}
		return nil
	}
	if err := allowed("assessment.basis", a.Basis); err != nil {
		return err
	}
	if err := allowed("assessment.runtime_basis", a.RuntimeBasis); err != nil {
		return err
	}
	for i, issue := range a.Counterexamples {
		field := fmt.Sprintf("assessment.counterexamples[%d]", i)
		if !nonblank(issue.Statement) || !nonblank(issue.Disposition) || !nonblank(issue.Reason) {
			return fmt.Errorf("%s: statement, disposition and reason must not be blank", field)
		}
		if err := allowed(field+".basis", issue.Basis); err != nil {
			return err
		}
	}
	return nil
}

// checkVerifierOwner keeps the approved owner rule: the result comes from
// a closed fresh session of this role and model, in this verification's
// group, that served only this verifier's attempts (its repair included).
func checkVerifierOwner(r *engine.Run, attemptID, group, key, role string, model runtime.ModelSpec) error {
	snapshot := r.Snapshot()
	attempt, ok := snapshot.Attempts[attemptID]
	owner, owned := snapshot.Sessions[attempt.HandleID]
	if !ok || !owned || owner.Role.Name != "triage-verify-"+role || owner.Role.Model != model || owner.State != "Closed" || attempt.Key != key || !strings.Contains(attempt.Scope, "/"+group+"/") {
		return fmt.Errorf("verification %s must come from a closed fresh %s session of its model", attemptID, role)
	}
	for id, other := range snapshot.Attempts {
		if other.HandleID == attempt.HandleID && other.Key != key {
			return fmt.Errorf("verifier session of %s also served attempt %s", attemptID, id)
		}
	}
	return nil
}

// runVerification has the three verifiers judge a claim in parallel, each
// in a fresh session. A role whose recoverable failures use up its retries
// is recorded unavailable; any other failure fails the run.
func runVerification(ctx context.Context, r *engine.Run, ticket string, policy VerificationPolicy, retries, n int, claimRef contract.Ref, claim Claim) (contract.Ref, Delivery, []RecoveryFailure, error) {
	delivery := Delivery{Claim: claimRef, Roles: make([]RoleDelivery, len(verifierRoles))}
	inputs := claimInputs(claimRef, claim)
	group := fmt.Sprintf("verification-%d", n)
	var failures []RecoveryFailure
	var mu sync.Mutex
	var branches []engine.Branch
	for i, role := range verifierRoles {
		branches = append(branches, engine.Branch{Name: role, Do: func(ctx context.Context, s *engine.Scope) (engine.Result, error) {
			key := "verify-" + role
			t := newTask(r, "verify-"+role, ticket, nil, verifierRequirements)
			t.Citable = []LabeledRef{{"claim", claimRef}}
			for _, owner := range inputs[1:] {
				t.Citable = append(t.Citable, LabeledRef{"allowed evidence owner", owner})
			}
			t.Verify = &verifyTask{Role: role, Claim: claimRef, AllowedEvidence: claim.Candidate.AllowedEvidence}
			ref, fails, err := RetryInputs(ctx, r, s, key+"-recovery", "verification", retries, func(ctx context.Context, scope *engine.Scope, retry *engine.Feedback) (contract.Ref, error) {
				ref, err := RunTaskStep(ctx, r, TaskStep{Scope: scope, Model: policy.model(role), Stage: "verify-" + role, Key: key, Task: t, Schema: VerificationSchema, Inputs: inputs, Recovery: true, Timeout: policy.Timeout, Feedback: retry,
					Validate: func(ctx context.Context, ref contract.Ref) error {
						return checkVerification(ctx, r, ref, claimRef, claim, role)
					}})
				if err != nil {
					return ref, err
				}
				return ref, checkVerifierOwner(r, ref.AttemptID, group, key, role, policy.model(role))
			}, nil)
			mu.Lock()
			failures = append(failures, fails...)
			mu.Unlock()
			outcome := RoleDelivery{Role: role, Failures: []DeliveryFailure{}}
			for _, f := range fails {
				outcome.Failures = append(outcome.Failures, DeliveryFailure{AttemptID: f.AttemptID, Code: string(f.Code), Origin: string(f.Origin), Diagnostic: f.Diagnostic})
			}
			if err != nil {
				var f *engine.Failure
				// Only exhausted, confirmed recoverable failures make a role
				// unavailable; anything else keeps its error.
				if context.Cause(ctx) != nil || !errors.As(err, &f) || f.Code != engine.RetryExhausted || len(fails) != retries+1 {
					return engine.Result{}, err
				}
				outcome.Unavailable = true
				delivery.Roles[i] = outcome
				return engine.Result{}, nil
			}
			outcome.Result = &ref
			delivery.Roles[i] = outcome
			return engine.Result{Outputs: map[string]contract.Ref{"verification": ref}}, nil
		}})
	}
	results, err := r.Root().Parallel(ctx, group, engine.CollectAll, branches)
	if err != nil {
		return contract.Ref{}, delivery, failures, err
	}
	var errs []error
	for _, res := range results {
		if res.Err != nil {
			errs = append(errs, res.Err)
		}
	}
	if len(errs) > 0 {
		return contract.Ref{}, delivery, failures, errors.Join(errs...)
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: fmt.Sprintf("verification-%d-delivery", n), Output: contract.Spec{SchemaID: DeliverySchema}, Data: delivery})
	return ref, delivery, failures, err
}

// allowedHint names what differs between e and the claim's allowed entries
// for the same file.
func allowedHint(allowed []Evidence, e Evidence) string {
	var want []string
	for _, x := range allowed {
		if x.FileID == e.FileID {
			want = append(want, describeEvidence(x))
		}
	}
	if len(want) == 0 {
		return "the claim's allowed_evidence has no entry for this file id"
	}
	return fmt.Sprintf("got %s; want an exact copy of %s", describeEvidence(e), strings.Join(want, " or "))
}

func describeEvidence(e Evidence) string {
	ref := "ref null"
	if e.Ref != nil {
		ref = "ref " + describeRef(*e.Ref)
	}
	switch l := e.Locator; {
	case l == nil:
		return ref + " without locator"
	case l.Pointer != nil:
		return fmt.Sprintf("%s with locator pointer %q", ref, *l.Pointer)
	case l.Offset != nil && l.Length != nil:
		return fmt.Sprintf("%s with locator offset %d length %d", ref, *l.Offset, *l.Length)
	default:
		return ref + " with an incomplete locator"
	}
}
