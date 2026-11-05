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
	convergeRole    = "code-change-converge"
	convergeTimeout = 30 * time.Minute
)

var convergeModel = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/glm-5p3-flash", Thinking: "high"}

// roundPolicy bounds the convergence rounds: at most rounds of them, each
// started only while the run deadline leaves reserve at the time now gives.
type roundPolicy struct {
	rounds  int
	reserve time.Duration
	now     func() time.Time
}

// convergeRounds is the production policy. The reserve is one round's bound,
// the wait cap, the snapshot reads and the converge Step, 70 minutes, plus
// the 90 minutes CODE-CHANGE reserves for the work after the rounds. Every
// value awaits live data.
var convergeRounds = roundPolicy{rounds: 10, reserve: 160 * time.Minute, now: time.Now}

// errNotConverged ends rounds that ran out, at the round cap or the run
// deadline, without a round that meets the termination rule. Every round
// that ran was verified; the last accepted report is the Final.
var errNotConverged = errors.New("code-change: the pull request did not converge")

// errPullRequestNotOpen ends the rounds once a round's snapshot shows the
// pull request closed or merged; the last accepted report is the Final.
var errPullRequestNotOpen = errors.New("code-change: the pull request is no longer open")

// convergeReport is the part of a converge contract the Controller checks.
type convergeReport struct {
	Snapshot  contract.Ref `json:"snapshot"`
	Converged bool         `json:"converged"`
	Items     []struct {
		ID string `json:"id"`
	} `json:"items"`
	pushed
}

// admitted reports whether the run deadline, the run's creation plus its
// RunTimeout as the engine sets it, leaves reserve at now. Without a run
// deadline there is no time admission.
func admitted(s engine.Snapshot, now time.Time, reserve time.Duration) bool {
	return s.Policy.DisableRunTimeout || s.CreatedAt.Add(s.Policy.RunTimeout).Sub(now) >= reserve
}

// converge runs the convergence rounds after the fix Step in an ordinary
// loop, not a Retry, each round n in its own Child scope round-NNN of root,
// started only within rp's round cap and time admission. A round waits for
// the state of the pull request of the committed record and attaches its
// snapshot; ends the rounds with errPullRequestNotOpen when the snapshot
// shows the pull request closed or merged; expands the private skill afresh
// from memory for the round; runs the converge Step with runPush in a fresh
// session with CWD piCWD, its last verified head the previous report's; and
// reads the pull request once more.
//
// The rounds converge when a round claimed convergence, pushed nothing and
// that read shows its snapshot's head: the final snapshot follows and the
// round's report is the Final with a nil error. Rounds that run out end
// with the final snapshot and errNotConverged. Any other failure returns
// the original error and no further round runs. From a round's dispatch
// through the HEAD read after its session's close, a failed Step, receipt
// or identity check included, the round's external objects record is the
// Final while the run can still act. Before the dispatch, and once the
// session is closed and the receipt verified, at the read after the round,
// the converged Decision or the final snapshot, the last accepted report is
// the Final: the fix contract before any round's, the round's own after its
// receipt.
func (p prepared) converge(ctx context.Context, r *engine.Run, piCWD string, prior engine.Result, sp statePolicy, rp roundPolicy) (engine.Result, error) {
	outputs := prior.Outputs
	report := "fix"
	stopped := func() engine.Result {
		return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: report}}
	}
	pr, err := engine.Decode[pullRequestRecord](ctx, r, outputs["pull-request"])
	if err != nil {
		return stopped(), err
	}
	verified, err := engine.Decode[fixed](ctx, r, outputs["fix"])
	if err != nil {
		return stopped(), err
	}
	last := verified.Head
	var snapshot contract.Ref
	for n := 1; ; n++ {
		if n > rp.rounds {
			return p.unconverged(ctx, r, outputs, report, snapshot, "rounds-exhausted",
				fmt.Sprintf("No round claimed convergence without a push on an unchanged head within the round cap of %d", rp.rounds),
				fmt.Errorf("%w within the round cap of %d", errNotConverged, rp.rounds), sp)
		}
		if !admitted(r.Snapshot(), rp.now(), rp.reserve) {
			return p.unconverged(ctx, r, outputs, report, snapshot, "rounds-not-admitted",
				fmt.Sprintf("The run deadline leaves less than %s for round %d", rp.reserve, n),
				fmt.Errorf("%w: the run deadline leaves less than %s for round %d", errNotConverged, rp.reserve, n), sp)
		}
		name := fmt.Sprintf("round-%03d", n)
		s, err := r.Root().Child(name)
		if err != nil {
			return stopped(), err
		}
		snap, state, err := p.recordState(ctx, r, s, "snapshot", pr.Number, sp)
		if err != nil {
			return stopped(), err
		}
		outputs[name+"-snapshot"], snapshot = snap, snap
		if state.PullRequest.State != "open" || state.PullRequest.Merged {
			return stopped(), fmt.Errorf("%w: the %s snapshot shows state %q merged %t", errPullRequestNotOpen, name, state.PullRequest.State, state.PullRequest.Merged)
		}
		skill, err := p.skills.expandRound(r, n)
		if err != nil {
			return stopped(), err
		}
		prompt, err := json.Marshal(task{Role: "converger", Requirements: baselineRequirements + "\n\n" + convergeRequirements, Worktree: p.worktree.repo.Worktree(), Skills: []string{skill}, Round: n})
		if err != nil {
			return stopped(), err
		}
		v, result, err := runPush(ctx, r, p, pr, last, pushStep{scope: s, key: "converge", output: name + "-converge", prefix: name + "-", step: "converge", at: name,
			role: engine.RoleSpec{Name: convergeRole, Model: convergeModel, CWD: piCWD}, who: "converger", decision: "converge-verified", prompt: string(prompt),
			inputs: []contract.Ref{p.input, outputs["implement"], outputs["pull-request"], snap, outputs[report]}, schema: ConvergeSchema, timeout: convergeTimeout},
			outputs, stopped(), func(v convergeReport) error { return checkRound(v, snap) })
		if err != nil {
			return result, err
		}
		report, last = name+"-converge", v.Head
		after, err := p.headAfter(ctx, r.Dir(), pr.Number, sp)
		if err != nil {
			return stopped(), fmt.Errorf("after %s: %w", name, err)
		}
		if v.Converged && v.Correction == "no_change" && after == state.PullRequest.HeadSHA {
			if err := r.Root().Decision(ctx, "converged", fmt.Sprintf("Round %d claimed convergence without a push, and the pull request head read after its session closed, %s, is its snapshot's head", n, after), []contract.Ref{outputs[report], snap}); err != nil {
				return stopped(), err
			}
			return p.finalState(ctx, r, stopped(), sp)
		}
	}
}

// unconverged ends rounds that ran out: it records decision with reason on
// the last report and the last snapshot, if any, takes the final snapshot
// and returns cause, joined with any error of these, with the last report
// as the Final.
func (p prepared) unconverged(ctx context.Context, r *engine.Run, outputs map[string]contract.Ref, report string, snapshot contract.Ref, decision, reason string, cause error, sp statePolicy) (engine.Result, error) {
	result := engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: report}}
	refs := []contract.Ref{outputs[report]}
	if snapshot != (contract.Ref{}) {
		refs = append(refs, snapshot)
	}
	if err := r.Root().Decision(ctx, decision, reason, refs); err != nil {
		return result, fmt.Errorf("%w; %w", cause, err)
	}
	result, err := p.finalState(ctx, r, result, sp)
	if err != nil {
		return result, fmt.Errorf("%w; %w", cause, err)
	}
	return result, cause
}

// headAfter is the head of pull request number as one read within sp.reads
// shows it. A failed read wraps errStateRead; the run's cancellation,
// deadline or fatal error is returned as its cause.
func (p prepared) headAfter(ctx context.Context, dir string, number int, sp statePolicy) (string, error) {
	reads, cancel := context.WithTimeoutCause(ctx, sp.reads, errReadsCap)
	defer cancel()
	pr, err := getPullRequest(reads, dir, p.repository, number)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return "", cause
		}
		return "", fmt.Errorf("%w: %w", errStateRead, err)
	}
	return pr.Head.SHA, nil
}

// checkRound is the acceptance a repair can fix: the snapshot Ref echoed
// byte-exact and item IDs unique in the contract. What an item says is not
// judged.
func checkRound(v convergeReport, snapshot contract.Ref) error {
	var errs []error
	if err := checkEcho("snapshot", v.Snapshot, snapshot); err != nil {
		errs = append(errs, err)
	}
	seen := map[string]int{}
	for i, item := range v.Items {
		if j, ok := seen[item.ID]; ok {
			errs = append(errs, fmt.Errorf("items[%d].id: got %q, which items[%d] already uses; want an ID unique among the items", i, item.ID, j))
			continue
		}
		seen[item.ID] = i
	}
	return errors.Join(errs...)
}
