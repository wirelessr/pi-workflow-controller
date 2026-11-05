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
	cleanupRole    = "code-change-cleanup"
	cleanupTimeout = 20 * time.Minute
)

var cleanupModel = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: "off"}

// cleanupOutcome is what became of the cleanup Step, for the summary.
type cleanupOutcome struct {
	// skipped names the check that kept it from its dispatch, "" when none
	// did.
	skipped string
	// accepted reports whether its contract was accepted and verified and
	// its session closed at the pushed head.
	accepted bool
}

// finish follows the rounds d ended: the cleanup Step, the final snapshot
// and the summary, which is the Final.
//
// Nothing follows rounds the run can no longer act on, cancelled, past its
// deadline or stopped by a fatal error, or rounds that ended on a receipt or
// identity check the worktree or GitHub did not confirm: d's result and
// error are returned as they are. The cleanup Step is dispatched unless the
// pull request is no longer open, a session the rounds opened is not
// confirmed closed, or the worktree HEAD is not the head last verified, which
// a round that failed after its dispatch can leave. A cleanup Step that
// fails on its own receipt or identity check, or once the run can no longer
// act, ends the run the same way with its own result.
// The final snapshot follows; a snapshot that cannot be taken is recorded in
// the summary as unavailable. A summary that cannot be attached leaves the
// result as it was, the cleanup contract its Final once accepted.
//
// The error is d's, with the cleanup Step's error added to its message, or
// the cleanup Step's alone when d has none, a HEAD that is not the verified
// one adding nothing to d's; then the final snapshot's, added the same way,
// and the summary's.
func (p prepared) finish(ctx context.Context, r *engine.Run, piCWD string, d roundsEnd, sp statePolicy) (engine.Result, error) {
	if ctx.Err() != nil || identityFailure(d.err) {
		return d.result, d.err
	}
	result, err := d.result, d.err
	var c cleanupOutcome
	switch {
	case errors.Is(d.err, errPullRequestNotOpen):
		c.skipped = "pull_request_not_open"
	case !d.closed:
		c.skipped = "session_unconfirmed"
	default:
		cleanup, cErr := p.cleanup(ctx, r, piCWD, d)
		var moved headMismatch
		switch {
		case errors.As(cErr, &moved):
			c.skipped = "unverified_head"
			if err == nil {
				err = cErr
			}
		case cErr != nil:
			if d.err != nil {
				err = fmt.Errorf("%w; the cleanup Step: %v", d.err, cErr)
			} else {
				err = cErr
			}
			if ctx.Err() != nil || identityFailure(cErr) {
				return cleanup, err
			}
		default:
			c.accepted = true
		}
		result = cleanup
	}
	result, unavailable := p.finalState(ctx, r, result, sp)
	if unavailable != nil {
		if err == nil {
			err = unavailable
		} else {
			err = fmt.Errorf("%w; the final snapshot: %v", err, unavailable)
		}
		if ctx.Err() != nil {
			return result, err
		}
	}
	ref, sErr := p.summarize(ctx, r, d, c, result.Outputs, unavailable, err)
	if sErr != nil {
		if err != nil {
			return result, fmt.Errorf("%w; %w", err, sErr)
		}
		return result, sErr
	}
	result.Outputs["summary"] = ref
	return engine.Result{Outputs: result.Outputs, Final: &engine.FinalSelection{Output: "summary"}}, err
}

// identityFailure reports whether err is a receipt or identity check that
// the worktree or GitHub did not confirm: the pull request is no longer
// written to.
func identityFailure(err error) bool {
	return errors.Is(err, errReceipt) || errors.Is(err, errUnverifiable)
}

// cleanup runs the cleanup Step with runPush on the pull request of the
// committed record, in a fresh session with CWD piCWD, after the rounds d
// ended: its last verified head is d's and its previous report d's last
// accepted one. On success the cleanup contract is the result's Final; a
// failure before the dispatch returns d's result, and a failure from it on,
// while the run can still act, records the external objects that exist as
// the Final in root. Both return the original error.
func (p prepared) cleanup(ctx context.Context, r *engine.Run, piCWD string, d roundsEnd) (engine.Result, error) {
	outputs := d.result.Outputs
	pr, err := engine.Decode[pullRequestRecord](ctx, r, outputs["pull-request"])
	if err != nil {
		return d.result, err
	}
	prompt, err := json.Marshal(task{Role: "cleaner", Requirements: baselineRequirements + "\n\n" + cleanupRequirements, Worktree: p.worktree.repo.Worktree()})
	if err != nil {
		return d.result, err
	}
	_, result, _, err := runPush(ctx, r, p, pr, d.last, pushStep{scope: r.Root(), key: "cleanup", output: "cleanup", step: "cleanup", at: "the cleanup Step",
		role: engine.RoleSpec{Name: cleanupRole, Model: cleanupModel, CWD: piCWD}, who: "cleaner", decision: "cleanup-verified", prompt: string(prompt),
		inputs: []contract.Ref{p.input, outputs["implement"], outputs["pull-request"], outputs[reportKey(d.accepted)]}, schema: CleanupSchema, timeout: cleanupTimeout},
		outputs, d.result, func(pushed) error { return nil }, nil)
	if err != nil {
		return result, err
	}
	return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: "cleanup"}}, nil
}
