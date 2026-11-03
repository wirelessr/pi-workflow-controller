package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const reviewTimeout = 30 * time.Minute

// reviewer is a required pre-PR reviewer. Its name is the Parallel branch,
// the task role and the role its contract must report.
type reviewer struct {
	name  string
	model runtime.ModelSpec
	focus string
}

var reviewers = []reviewer{
	{"basic", runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/glm-5p3-flash", Thinking: "high"}, basicFocus},
	{"consistency", runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/glm-5p3", Thinking: "high"}, consistencyFocus},
}

type findings struct {
	Role      string       `json:"role"`
	Implement contract.Ref `json:"implement"`
	Findings  []struct {
		ID string `json:"id"`
	} `json:"findings"`
}

// errHeadMoved is a worktree HEAD that moved away from the verified head
// while the reviewers ran.
var errHeadMoved = errors.New("code-change worktree HEAD moved during pre-PR review")

// preReview has both reviewers review the change on a snapshot pinned at
// head, the local head the implement receipt verified, in parallel and each
// in a fresh session with CWD piCWD. Either failing is an execution failure
// classified by the first failing reviewer in branch order. After both
// sessions are confirmed closed it requires the worktree HEAD to still be
// head, which detects only a moved HEAD, and removes the snapshot; on any
// failure the run's cleanup removes it after confirming every Pi exit. Every
// error after the reviewers ran keeps the accepted findings in the outputs,
// without a Final.
func preReview(ctx context.Context, r *engine.Run, p prepared, piCWD string, implemented engine.Result, head string) (engine.Result, error) {
	outputs := maps.Clone(implemented.Outputs)
	snapshot, err := p.worktree.createSnapshot(ctx, r, head)
	if err != nil {
		return engine.Result{Outputs: outputs}, err
	}
	t := task{Checkout: snapshot.Worktree(), Base: p.worktree.base, Head: head}
	inputs := []contract.Ref{p.input, outputs["implement"]}
	branches := make([]engine.Branch, len(reviewers))
	for i, rv := range reviewers {
		branches[i] = engine.Branch{Name: rv.name, Do: func(ctx context.Context, s *engine.Scope) (engine.Result, error) {
			return rv.review(ctx, r, s, piCWD, t, inputs)
		}}
	}
	joined, err := r.Root().Parallel(ctx, "pre-pr-review", engine.CollectAll, branches)
	var refs []contract.Ref
	var failed error
	for _, b := range joined {
		if ref, ok := b.Result.Outputs["review"]; ok {
			outputs["review-"+b.Name] = ref
			refs = append(refs, ref)
		}
		switch {
		case b.Err == nil:
		case failed == nil:
			failed = b.Err
		default:
			// Only the first failure classifies the run; later ones are
			// in the message.
			failed = fmt.Errorf("%w; %s reviewer: %v", failed, b.Name, b.Err)
		}
	}
	if err == nil {
		err = failed
	}
	if err != nil {
		return engine.Result{Outputs: outputs}, err
	}
	current, err := p.worktree.head(ctx)
	if err != nil {
		return engine.Result{Outputs: outputs}, err
	}
	if current != head {
		return engine.Result{Outputs: outputs}, fmt.Errorf("%w: the worktree HEAD is %s; want the verified head %s", errHeadMoved, current, head)
	}
	if err := snapshot.Cleanup(ctx); err != nil {
		return engine.Result{Outputs: outputs}, fmt.Errorf("snapshot cleanup: %w", err)
	}
	if err := r.Root().Decision(ctx, "pre-pr-reviewed", "Both pre-PR reviewers' findings were accepted, their sessions confirmed closed and the worktree HEAD is still the verified head", refs); err != nil {
		return engine.Result{Outputs: outputs}, err
	}
	return engine.Result{Outputs: outputs, Final: implemented.Final}, nil
}

// review runs one reviewer and strictly closes its session. An accepted
// contract is in the result even when the close fails.
func (rv reviewer) review(ctx context.Context, r *engine.Run, s *engine.Scope, piCWD string, t task, inputs []contract.Ref) (engine.Result, error) {
	role := "code-change-review-" + rv.name
	h, err := r.OpenSession(ctx, engine.RoleSpec{Name: role, Model: rv.model, CWD: piCWD})
	if err != nil {
		return engine.Result{}, err
	}
	t.Role = rv.name
	t.Requirements = baselineRequirements + "\n\n" + reviewRequirements + "\n" + rv.focus
	prompt, err := json.Marshal(t)
	if err != nil {
		return engine.Result{}, err
	}
	out, _, err := stepDecoded(ctx, r, s, engine.StepSpec{Key: "review", Session: h, Prompt: string(prompt), Inputs: inputs,
		Output: contract.Spec{SchemaID: ReviewSchema}, Timeout: reviewTimeout},
		func(v findings) error { return checkReview(v, rv.name, inputs[1]) })
	if err != nil {
		return engine.Result{}, err
	}
	return engine.Result{Outputs: map[string]contract.Ref{"review": out.Output}}, closeStrictly(ctx, r, h, out.Execution.SessionID, role, rv.name+" reviewer")
}

// checkReview is the acceptance a repair can fix: the dispatched role, the
// implement Ref echoed byte-exact and finding IDs unique in the contract.
func checkReview(v findings, role string, implement contract.Ref) error {
	var errs []error
	if v.Role != role {
		errs = append(errs, fmt.Errorf("role: got %q; want %q, the role in request.prompt", v.Role, role))
	}
	if err := checkEcho("implement", v.Implement, implement); err != nil {
		errs = append(errs, err)
	}
	seen := map[string]int{}
	for i, f := range v.Findings {
		if j, ok := seen[f.ID]; ok {
			errs = append(errs, fmt.Errorf("findings[%d].id: got %q, which findings[%d] already uses; want an ID unique among the findings", i, f.ID, j))
			continue
		}
		seen[f.ID] = i
	}
	return errors.Join(errs...)
}
