package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const (
	implementRole = "code-change-implement"
	// implementRepairs gives each Step at most three attempts.
	implementRepairs = 2
	implementTimeout = 90 * time.Minute
)

var implementModel = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/glm-5p3", Thinking: "high"}

// errDesignGaps ends a run whose implementer found the design doc not
// implementable; the final implement contract lists the gaps.
var errDesignGaps = errors.New("code-change: the implementer reported design doc gaps; the final implement contract lists them")

// errReceipt is a reported local head that the worktree does not confirm.
// It is an execution failure, never repaired.
var errReceipt = errors.New("code-change receipt mismatch")

type implementation struct {
	Readiness string       `json:"readiness"`
	Input     contract.Ref `json:"input"`
	LocalHead string       `json:"local_head"`
}

// implement runs the implementer on its own session with CWD piCWD and the
// worktree by absolute path, then checks the accepted contract's local head
// receipt against the worktree once. The session stays open for the
// correction Step that will reuse it.
func implement(ctx context.Context, r *engine.Run, p prepared, piCWD string) (engine.Result, error) {
	h, err := r.OpenSession(ctx, engine.RoleSpec{Name: implementRole, Model: implementModel, CWD: piCWD})
	if err != nil {
		return engine.Result{}, err
	}
	prompt, err := json.Marshal(task{Role: "implementer", Requirements: baselineRequirements + "\n\n" + implementRequirements, Worktree: p.worktree.repo.Worktree()})
	if err != nil {
		return engine.Result{}, err
	}
	var accepted implementation
	var readErr error
	results, err := r.Root().StepWithRepair(ctx, engine.StepSpec{Key: "implement", Session: h, Prompt: string(prompt), Inputs: []contract.Ref{p.input},
		Output: contract.Spec{SchemaID: ImplementSchema}, Timeout: implementTimeout, RepairKeepsWork: true}, implementRepairs,
		func(ctx context.Context, ref contract.Ref) error {
			v, err := engine.Decode[implementation](ctx, r, ref)
			if err != nil {
				// A schema-valid publication that cannot be read is an
				// execution failure, not a content diagnostic to repair.
				readErr = err
				return nil
			}
			accepted = v
			return checkImplementation(v, p.input)
		})
	if err != nil {
		return engine.Result{}, err
	}
	if readErr != nil {
		return engine.Result{}, readErr
	}
	ref := results[len(results)-1].Output
	// A receipt failure still reports the committed contracts, without a Final.
	outputs := map[string]contract.Ref{"input": p.input, "implement": ref}
	head, err := p.worktree.head(ctx)
	if err != nil {
		return engine.Result{Outputs: outputs}, err
	}
	base := p.worktree.base
	final := &engine.FinalSelection{Output: "implement"}
	if accepted.Readiness == "gaps" {
		if head != base {
			return engine.Result{Outputs: outputs}, fmt.Errorf("%w: readiness gaps: the worktree HEAD is %s; want it unchanged at base %s", errReceipt, head, base)
		}
		if err := r.Root().Decision(ctx, "design-gaps-reported", "The implementer reported design doc gaps and the worktree HEAD is the base", []contract.Ref{ref}); err != nil {
			return engine.Result{}, err
		}
		return engine.Result{Outputs: outputs, Final: final}, errDesignGaps
	}
	if accepted.LocalHead != head {
		return engine.Result{Outputs: outputs}, fmt.Errorf("%w: local_head: got %s; want the worktree HEAD %s", errReceipt, accepted.LocalHead, head)
	}
	fresh, err := p.worktree.isNew(ctx, head)
	if err != nil {
		return engine.Result{Outputs: outputs}, err
	}
	if !fresh {
		return engine.Result{Outputs: outputs}, fmt.Errorf("%w: local_head: got %s, which is base %s or one of its ancestors; want a commit that is neither", errReceipt, head, base)
	}
	if err := r.Root().Decision(ctx, "implementation-recorded", "The reported local head is the worktree HEAD, which is not the base or one of its ancestors", []contract.Ref{ref}); err != nil {
		return engine.Result{}, err
	}
	return engine.Result{Outputs: outputs, Final: final}, nil
}

// checkImplementation is the acceptance a repair can fix: the input Ref
// echoed byte-exact.
func checkImplementation(v implementation, input contract.Ref) error {
	if v.Input != input {
		got, _ := json.Marshal(v.Input)
		want, _ := json.Marshal(input)
		return fmt.Errorf("input: got %s; want the input Ref copied byte-exact from request.inputs: %s", got, want)
	}
	return nil
}
