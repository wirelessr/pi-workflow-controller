package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const (
	implementRole = "code-change-implement"
	// contractRepairs gives each Step at most three attempts.
	contractRepairs  = 2
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

// handoff is what a ready implement Step hands on: the implementer's
// session, which stays open for the correction Step that reuses it, the
// session ID it ran in and the verified head.
type handoff struct {
	session   *engine.SessionHandle
	sessionID string
	head      string
}

// implement runs the implementer on its own session with CWD piCWD and the
// worktree by absolute path, then checks the accepted contract's local head
// receipt against the worktree once. On ready it also returns the handoff.
func implement(ctx context.Context, r *engine.Run, p prepared, piCWD string) (engine.Result, handoff, error) {
	h, err := r.OpenSession(ctx, engine.RoleSpec{Name: implementRole, Model: implementModel, CWD: piCWD})
	if err != nil {
		return engine.Result{}, handoff{}, err
	}
	prompt, err := json.Marshal(task{Role: "implementer", Requirements: baselineRequirements + "\n\n" + implementRequirements, Worktree: p.worktree.repo.Worktree()})
	if err != nil {
		return engine.Result{}, handoff{}, err
	}
	out, accepted, err := stepDecoded(ctx, r, r.Root(), engine.StepSpec{Key: "implement", Session: h, Prompt: string(prompt), Inputs: []contract.Ref{p.input},
		Output: contract.Spec{SchemaID: ImplementSchema}, Timeout: implementTimeout, RepairKeepsWork: true},
		func(v implementation) error {
			// The acceptance a repair can fix: the input Ref echoed byte-exact.
			return checkEcho("input", v.Input, p.input)
		})
	if err != nil {
		return engine.Result{}, handoff{}, err
	}
	ref := out.Output
	// A receipt failure still reports the committed contracts, without a Final.
	outputs := map[string]contract.Ref{"input": p.input, "skills": p.skills.record, "implement": ref}
	head, err := p.worktree.head(ctx)
	if err != nil {
		return engine.Result{Outputs: outputs}, handoff{}, err
	}
	base := p.worktree.base
	final := &engine.FinalSelection{Output: "implement"}
	if accepted.Readiness == "gaps" {
		if head != base {
			return engine.Result{Outputs: outputs}, handoff{}, fmt.Errorf("%w: readiness gaps: the worktree HEAD is %s; want it unchanged at base %s", errReceipt, head, base)
		}
		if err := r.Root().Decision(ctx, "design-gaps-reported", "The implementer reported design doc gaps and the worktree HEAD is the base", []contract.Ref{ref}); err != nil {
			return engine.Result{}, handoff{}, err
		}
		return engine.Result{Outputs: outputs, Final: final}, handoff{}, errDesignGaps
	}
	if accepted.LocalHead != head {
		return engine.Result{Outputs: outputs}, handoff{}, fmt.Errorf("%w: local_head: got %s; want the worktree HEAD %s", errReceipt, accepted.LocalHead, head)
	}
	fresh, err := p.worktree.isNew(ctx, base, head)
	if err != nil {
		return engine.Result{Outputs: outputs}, handoff{}, err
	}
	if !fresh {
		return engine.Result{Outputs: outputs}, handoff{}, fmt.Errorf("%w: local_head: got %s, which is base %s or one of its ancestors; want a commit that is neither", errReceipt, head, base)
	}
	if err := r.Root().Decision(ctx, "implementation-recorded", "The reported local head is the worktree HEAD, which is not the base or one of its ancestors", []contract.Ref{ref}); err != nil {
		return engine.Result{}, handoff{}, err
	}
	return engine.Result{Outputs: outputs, Final: final}, handoff{session: h, sessionID: out.Execution.SessionID, head: head}, nil
}

// stepDecoded runs spec on s with the contract repairs and decodes each
// publication as T for check, the acceptance a repair can fix. A schema-valid
// publication that cannot be decoded is an execution failure, not a content
// diagnostic to repair: it ends the Step and its error is returned after it.
// It returns the attempt of the last publication it decoded and that
// contract, accepted or, on failure, rejected; both are zero when it decoded
// none.
func stepDecoded[T any](ctx context.Context, r *engine.Run, s *engine.Scope, spec engine.StepSpec, check func(T) error) (engine.StepResult, T, error) {
	var last T
	var lastRef contract.Ref
	var readErr error
	results, err := s.StepWithRepair(ctx, spec, contractRepairs, func(ctx context.Context, ref contract.Ref) error {
		v, err := engine.Decode[T](ctx, r, ref)
		if err != nil {
			readErr = err
			return nil
		}
		last, lastRef = v, ref
		return check(v)
	})
	if err == nil {
		err = readErr
	}
	var out engine.StepResult
	for _, res := range results {
		if lastRef != (contract.Ref{}) && res.Output == lastRef {
			out = res
		}
	}
	return out, last, err
}

// closeStrictly closes h and requires the close to confirm the exit of the
// process of sessionID. An error is classified by engine.CloseFailure; an
// unconfirmed close is a cleanup failure of phase, whose message names who.
func closeStrictly(ctx context.Context, r *engine.Run, h *engine.SessionHandle, sessionID, phase, who string) error {
	closed, err := r.CloseSessionReport(ctx, h)
	if err != nil {
		return engine.CloseFailure(err, phase, closed.Identity.HandleID)
	}
	if !closed.ConfirmsLocalClose(sessionID) {
		return &engine.Failure{Code: engine.CleanupFailed, Origin: engine.OriginProtocol, Phase: phase, Message: who + " session close not confirmed",
			DispatchAccepted: engine.AcceptedNo, HandleID: closed.Identity.HandleID, Cleanup: &closed}
	}
	return nil
}

// checkEcho requires field, a Ref the request gave, copied byte-exact. The
// diagnostic names each differing member, since a one-character slip in a
// 64-character digest is hard to find by comparing the two Refs.
func checkEcho(field string, got, want contract.Ref) error {
	if got != want {
		g, _ := json.Marshal(got)
		w, _ := json.Marshal(want)
		var members []string
		for _, m := range contract.RefMismatches(got, want) {
			members = append(members, fmt.Sprintf("%s.%s got %q, want %q", field, m.Field, m.Got, m.Want))
		}
		return fmt.Errorf("%s: got %s; want the %s Ref copied byte-exact from request.inputs: %s; differing: %s", field, g, field, w, strings.Join(members, "; "))
	}
	return nil
}
