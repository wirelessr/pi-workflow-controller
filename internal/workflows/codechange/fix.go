package codechange

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

const (
	fixRole    = "code-change-fix"
	fixTimeout = 45 * time.Minute
)

// findingID is a finding as the fix Step's coverage check keys it.
type findingID struct {
	ID string `json:"id"`
}

// fixed is the part of the fix Step's contract the Controller checks.
type fixed struct {
	Dispositions []findingID `json:"dispositions"`
	pushed
}

// reportFindings is the only part of the deep review report this workflow
// reads: the IDs of its confirmed findings. What they say is the fixer's to
// judge.
type reportFindings struct {
	Findings []findingID `json:"findings"`
}

// fix runs the fix Step with runPush on the pull request the committed
// record verified, its last verified head the record's reported head. A
// failure before the dispatch has the pull request record as the Final;
// every failure from the dispatch on, while the run can still act, records
// the external objects that exist as the Final. Both return the original
// error.
func fix(ctx context.Context, r *engine.Run, p prepared, piCWD string, reviewed engine.Result) (engine.Result, error) {
	outputs := reviewed.Outputs
	failed := engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: "pull-request"}}
	report := outputs[deepReviewName+"-report"]
	pr, err := engine.Decode[pullRequestRecord](ctx, r, outputs["pull-request"])
	if err != nil {
		return failed, err
	}
	found, err := engine.Decode[reportFindings](ctx, r, report)
	if err != nil {
		return failed, err
	}
	prompt, err := json.Marshal(task{Role: "fixer", Requirements: baselineRequirements + "\n\n" + fixRequirements, Worktree: p.worktree.repo.Worktree()})
	if err != nil {
		return failed, err
	}
	_, result, err := runPush(ctx, r, p, pr, pr.ReportedHead, pushStep{scope: r.Root(), key: "fix", output: "fix", step: "fix", at: "the fix Step",
		role: engine.RoleSpec{Name: fixRole, Model: implementModel, CWD: piCWD}, who: "fixer", decision: "fix-verified", prompt: string(prompt),
		inputs: []contract.Ref{p.input, outputs["implement"], outputs["pull-request"], report}, schema: FixSchema, timeout: fixTimeout},
		outputs, failed, func(v fixed) error {
			return checkCoverage(v.Dispositions, found.Findings, func(f findingID) string { return fmt.Sprintf("id %q", f.ID) },
				"is not a finding of the deep review report in request.inputs; want only its findings")
		})
	if err != nil {
		return result, err
	}
	return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: "fix"}}, nil
}

// pushed is the receipt a Step after the pull request record reports: the
// change and the head. The branch and pull request are the record's.
type pushed struct {
	Correction string `json:"correction"`
	Head       string `json:"head"`
}

// receipt is v on the branch and pull request of the record pr, a committed
// head required to be built on the last verified head.
func (v pushed) receipt(pr pullRequestRecord) receipt {
	return receipt{change: v.Correction, head: v.Head, branch: pr.Branch, pull: reportedPull{URL: pr.URL, Number: pr.Number}, builtOnLast: true}
}

// pushStep is a Step after the pull request record that pushes to its
// branch, as runPush dispatches it: its contract is the output named output,
// the external objects record of its failure prefix followed by
// external-objects, with step as the record's step, and at names it in the
// diagnostic of the check before its dispatch.
type pushStep struct {
	scope                         *engine.Scope
	key, output, prefix, step, at string
	role                          engine.RoleSpec
	who, decision, prompt, schema string
	inputs                        []contract.Ref
	timeout                       time.Duration
}

// pushContract is the contract of a Step that runPush dispatches.
type pushContract interface {
	receipt(pr pullRequestRecord) receipt
}

// runPush runs ps on the pull request of the record pr in a fresh session,
// once the worktree HEAD read through the registration is still last, the
// last verified head. It checks the accepted contract's receipt once with the
// correction Step's checks, the pull request and its branch taken from the
// record and a committed head required to be built on last; records
// ps.decision in ps.scope; strictly closes the session; and succeeds only
// when the worktree HEAD read after that close is still the pushed head,
// returning the accepted contract. A failure before the dispatch returns
// before; every failure from the dispatch on, while the run can still act,
// records the external objects that exist as the Final. Both return the
// original error.
func runPush[T pushContract](ctx context.Context, r *engine.Run, p prepared, pr pullRequestRecord, last string, ps pushStep, outputs map[string]contract.Ref, before engine.Result, check func(T) error) (T, engine.Result, error) {
	var none T
	// Agents ran after the last read, and only their requirements kept them
	// from the worktree.
	current, err := p.worktree.head(ctx)
	if err != nil {
		return none, before, err
	}
	if current != last {
		return none, before, fmt.Errorf("%w: the worktree HEAD is %s before %s; want the verified head %s", errReceipt, current, ps.at, last)
	}
	h, err := r.OpenSession(ctx, ps.role)
	if err != nil {
		return none, before, err
	}
	identity, err := r.SessionIdentity(ctx, h)
	if err != nil {
		return none, before, err
	}
	out, v, err := stepDecoded(ctx, r, ps.scope, engine.StepSpec{Key: ps.key, Session: h, Prompt: ps.prompt, Inputs: ps.inputs,
		Output: contract.Spec{SchemaID: ps.schema}, Timeout: ps.timeout, RepairKeepsWork: true}, check)
	// The last contract that passed the schema, even a rejected one, gives
	// the head the external objects record shows.
	rc := v.receipt(pr)
	var reported *receipt
	if out.Output != (contract.Ref{}) {
		outputs[ps.output] = out.Output
		reported = &rc
	}
	hand := handoff{session: h, sessionID: identity.SessionID, head: last}
	if err == nil {
		err = p.pushTail(ctx, r, ps, hand, out, rc)
	}
	if err != nil {
		result, err := p.externalObjects(ctx, r, ps.scope, ps.prefix, hand, ps.step, outputs, reported, err)
		return v, result, err
	}
	return v, engine.Result{}, nil
}

// pushTail checks rc, the accepted contract's receipt of ps, against the
// worktree and GitHub, records ps.decision in ps.scope, strictly closes the
// session and requires the worktree HEAD read after that close to be the
// pushed head.
func (p prepared) pushTail(ctx context.Context, r *engine.Run, ps pushStep, hand handoff, out engine.StepResult, rc receipt) error {
	if _, err := p.verifyPullRequest(ctx, r, hand.head, rc); err != nil {
		return err
	}
	if err := ps.scope.Decision(ctx, ps.decision, "The reported head is the worktree HEAD, built on the verified head when committed, and the pull request of the pull request record, as GitHub shows it, is still open, not a draft, from the record's branch to the default branch, at that head or a descendant", []contract.Ref{out.Output}); err != nil {
		return err
	}
	return p.closeAtHead(ctx, r, hand.session, hand.sessionID, ps.role.Name, ps.who, rc.head)
}
