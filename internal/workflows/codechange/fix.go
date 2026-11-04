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
	Correction   string      `json:"correction"`
	Head         string      `json:"head"`
}

// reportFindings is the only part of the deep review report this workflow
// reads: the IDs of its confirmed findings. What they say is the fixer's to
// judge.
type reportFindings struct {
	Findings []findingID `json:"findings"`
}

// fix runs the fix Step in a fresh session with CWD piCWD on the pull request
// the committed record verified, once the worktree HEAD read through the
// registration is still the record's reported head, the last verified head.
// It checks the accepted contract's receipt once with the correction Step's
// checks, the pull request and its branch taken from the record, and
// requires a committed head to be built on that last verified head; then it
// strictly closes the session, and succeeds only when the worktree HEAD read
// after that close is still the pushed head. A failure before the dispatch
// has the pull request record as the Final; every failure from the dispatch
// on, while the run can still act, records the external objects that exist
// as the Final. Both return the original error.
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
	// The deep review's Agents ran after the last read, and only their
	// requirements kept them from the worktree.
	current, err := p.worktree.head(ctx)
	if err != nil {
		return failed, err
	}
	if current != pr.ReportedHead {
		return failed, fmt.Errorf("%w: the worktree HEAD is %s before the fix Step; want the verified head %s", errReceipt, current, pr.ReportedHead)
	}
	h, err := r.OpenSession(ctx, engine.RoleSpec{Name: fixRole, Model: implementModel, CWD: piCWD})
	if err != nil {
		return failed, err
	}
	identity, err := r.SessionIdentity(ctx, h)
	if err != nil {
		return failed, err
	}
	hand := handoff{session: h, sessionID: identity.SessionID, head: pr.ReportedHead}
	out, v, err := stepDecoded(ctx, r, r.Root(), engine.StepSpec{Key: "fix", Session: h, Prompt: string(prompt), Inputs: []contract.Ref{p.input, outputs["implement"], outputs["pull-request"], report},
		Output: contract.Spec{SchemaID: FixSchema}, Timeout: fixTimeout, RepairKeepsWork: true},
		func(v fixed) error {
			return checkCoverage(v.Dispositions, found.Findings, func(f findingID) string { return fmt.Sprintf("id %q", f.ID) },
				"is not a finding of the deep review report in request.inputs; want only its findings")
		})
	// The last contract that passed the schema, even a rejected one, gives
	// the head the external objects record shows; the branch and pull
	// request are the record's, which the Agent does not report.
	rc := receipt{change: v.Correction, head: v.Head, branch: pr.Branch, pull: reportedPull{URL: pr.URL, Number: pr.Number}, builtOnLast: true}
	var reported *receipt
	if out.Output != (contract.Ref{}) {
		outputs["fix"] = out.Output
		reported = &rc
	}
	if err == nil {
		err = p.fixTail(ctx, r, hand, out, rc)
	}
	if err != nil {
		return p.externalObjects(ctx, r, hand, "fix", outputs, reported, err)
	}
	return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: "fix"}}, nil
}

// fixTail checks rc, the accepted contract's receipt, against the worktree
// and GitHub, records its Decision, strictly closes the fixer's session and
// requires the worktree HEAD read after that close to be the pushed head.
func (p prepared) fixTail(ctx context.Context, r *engine.Run, hand handoff, out engine.StepResult, rc receipt) error {
	if _, err := p.verifyPullRequest(ctx, r, hand.head, rc); err != nil {
		return err
	}
	if err := r.Root().Decision(ctx, "fix-verified", "The reported head is the worktree HEAD, built on the verified head when committed, and the pull request of the pull request record, as GitHub shows it, is still open, not a draft, from the record's branch to the default branch, at that head or a descendant", []contract.Ref{out.Output}); err != nil {
		return err
	}
	return p.closeAtHead(ctx, r, hand.session, hand.sessionID, fixRole, "fixer", rc.head)
}
