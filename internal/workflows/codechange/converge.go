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
	convergeRole    = "code-change-converge"
	convergeTimeout = 30 * time.Minute
)

var convergeModel = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/glm-5p3", Thinking: "high"}

// roundPolicy bounds the convergence rounds: at most rounds of them, each
// started only while the run deadline leaves reserve at the time now gives.
type roundPolicy struct {
	rounds  int
	reserve time.Duration
	now     func() time.Time
}

// convergeRounds is the production policy. The reserve is one round's bound
// plus what follows the rounds. The round's bound is 80 minutes: the wait
// cap, the snapshot reads, the converge Step and the read after the round.
// What follows is 90 minutes: the cleanup Step, the final snapshot's wait
// cap and reads, the summary's Attach and a 20-minute margin. The receipt
// reads, the repairs and the strict closes have no bound of their own and
// only the margin covers them, so the reserve leaves time for what follows
// the rounds only when they do not add up past it. Every value awaits live
// data.
var convergeRounds = roundPolicy{rounds: 10, reserve: 170 * time.Minute, now: time.Now}

// errNotConverged ends rounds that ran out, at the round cap or the run
// deadline, without a round that meets the termination rule. Every round
// that ran was verified; the summary is the Final.
var errNotConverged = errors.New("code-change: the pull request did not converge")

// errPullRequestNotOpen ends the rounds once a round's snapshot, or the read
// after a round, shows the pull request closed or merged; the summary is the
// Final and no cleanup Step runs.
var errPullRequestNotOpen = errors.New("code-change: the pull request is no longer open")

// convergeReport is the part of a converge contract the Controller checks
// and the summary copies.
type convergeReport struct {
	Snapshot      contract.Ref   `json:"snapshot"`
	Converged     bool           `json:"converged"`
	Unconverged   []string       `json:"unconverged"`
	Items         []itemClaim    `json:"items"`
	ThreadActions []threadAction `json:"thread_actions"`
	CIReruns      []ciRerun      `json:"ci_reruns"`
	pushed
}

// itemClaim is an item of a converge contract as the converger wrote it.
type itemClaim struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// threadAction is the receipt of a reply to or the resolution of a review
// thread: GraphQL node IDs, the comment's for a reply.
type threadAction struct {
	ThreadID  string `json:"thread_id"`
	Action    string `json:"action"`
	CommentID string `json:"comment_id"`
}

// ciRerun is the receipt of a rerun of a GitHub Actions workflow run.
type ciRerun struct {
	RunID int64 `json:"run_id"`
}

// admitted reports whether the run deadline, the run's creation plus its
// RunTimeout as the engine sets it, leaves reserve at now. Without a run
// deadline there is no time admission.
func admitted(s engine.Snapshot, now time.Time, reserve time.Duration) bool {
	return s.Policy.DisableRunTimeout || s.CreatedAt.Add(s.Policy.RunTimeout).Sub(now) >= reserve
}

// roundsEnd is how the convergence rounds ended, which decides what follows
// them.
type roundsEnd struct {
	// result and err are what the run returns when nothing follows the
	// rounds: the Final they leave and their error.
	result engine.Result
	err    error
	// kind is the summary's rounds_end, the name of the Decision that ends
	// the rounds when one does.
	kind string
	// started counts the rounds that began and accepted those whose
	// contract was accepted and verified; last is the head the Controller
	// last verified, the head of the last accepted report.
	started, accepted int
	last              string
	// closed reports whether every session the rounds opened is confirmed
	// closed.
	closed bool
}

func roundName(n int) string { return fmt.Sprintf("round-%03d", n) }

// reportKey names the last accepted report after accepted rounds: the fix
// contract before any round's.
func reportKey(accepted int) string {
	if accepted == 0 {
		return "fix"
	}
	return roundName(accepted) + "-converge"
}

// converge runs the convergence rounds after the fix Step in an ordinary
// loop, not a Retry, each round n in its own Child scope round-NNN of root,
// started only within rp's round cap and time admission. A round waits for
// the state of the pull request of the committed record and attaches its
// snapshot; ends the rounds with errPullRequestNotOpen when the snapshot
// shows the pull request closed or merged; expands the private skill afresh
// from memory for the round; runs the converge Step with runPush in a fresh
// session with CWD piCWD, its last verified head the previous report's, its
// thread and rerun receipts checked with verifyWrites after the push
// receipt; and reads the pull request once more, which ends the rounds with
// errPullRequestNotOpen too when it shows the pull request closed or merged.
//
// The rounds converge when a round claimed convergence, pushed nothing,
// reported no thread action and no rerun, and that read shows its
// snapshot's head, with a nil error. Rounds that run out end with
// errNotConverged. Any other failure returns the original error and no
// further round runs. From a round's dispatch through the HEAD read after
// its session's close, a failed Step, receipt or identity check included,
// the round's external objects record is the result's Final while the run
// can still act. Before the dispatch, and once the session is closed and
// the receipt verified, it is the last accepted report: the fix contract
// before any round's, the round's own after its receipt.
func (p prepared) converge(ctx context.Context, r *engine.Run, piCWD string, prior engine.Result, sp statePolicy, rp roundPolicy) roundsEnd {
	outputs := prior.Outputs
	d := roundsEnd{closed: true}
	stopped := func() engine.Result {
		return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: reportKey(d.accepted)}}
	}
	stop := func(kind string, err error) roundsEnd {
		d.result, d.err, d.kind = stopped(), err, kind
		return d
	}
	pr, err := engine.Decode[pullRequestRecord](ctx, r, outputs["pull-request"])
	if err != nil {
		return stop("round_failed", err)
	}
	verified, err := engine.Decode[fixed](ctx, r, outputs["fix"])
	if err != nil {
		return stop("round_failed", err)
	}
	d.last = verified.Head
	var snapshot contract.Ref
	for n := 1; ; n++ {
		if n > rp.rounds {
			return stop("rounds-exhausted", p.unconverged(ctx, r, outputs, reportKey(d.accepted), snapshot, "rounds-exhausted",
				fmt.Sprintf("No round claimed convergence without a push, a thread reply or resolution or a rerun, on an unchanged head, within the round cap of %d", rp.rounds),
				fmt.Errorf("%w within the round cap of %d", errNotConverged, rp.rounds)))
		}
		if !admitted(r.Snapshot(), rp.now(), rp.reserve) {
			return stop("rounds-not-admitted", p.unconverged(ctx, r, outputs, reportKey(d.accepted), snapshot, "rounds-not-admitted",
				fmt.Sprintf("The run deadline leaves less than %s for round %d", rp.reserve, n),
				fmt.Errorf("%w: the run deadline leaves less than %s for round %d", errNotConverged, rp.reserve, n)))
		}
		name := roundName(n)
		s, err := r.Root().Child(name)
		if err != nil {
			return stop("round_failed", err)
		}
		d.started = n
		snap, state, err := p.recordState(ctx, r, s, "snapshot", pr.Number, sp)
		if errors.Is(err, errStateRead) {
			return stop("state_read_failed", err)
		}
		if err != nil {
			return stop("round_failed", err)
		}
		outputs[name+"-snapshot"], snapshot = snap, snap
		if state.PullRequest.State != "open" || state.PullRequest.Merged {
			return stop("pull_request_not_open", fmt.Errorf("%w: the %s snapshot shows state %q merged %t", errPullRequestNotOpen, name, state.PullRequest.State, state.PullRequest.Merged))
		}
		skill, err := p.skills.expandRound(r, n)
		if err != nil {
			return stop("round_failed", err)
		}
		prompt, err := json.Marshal(task{Role: "converger", Requirements: baselineRequirements + "\n\n" + convergeRequirements, Worktree: p.worktree.repo.Worktree(), Skills: []string{skill}, Round: n})
		if err != nil {
			return stop("round_failed", err)
		}
		v, result, closed, err := runPush(ctx, r, p, pr, d.last, pushStep{scope: s, key: "converge", output: name + "-converge", prefix: name + "-", step: "converge", at: name,
			role: engine.RoleSpec{Name: convergeRole, Model: convergeModel, CWD: piCWD}, who: "converger", decision: "converge-verified", prompt: string(prompt),
			inputs: []contract.Ref{p.input, outputs["implement"], outputs["pull-request"], snap, outputs[reportKey(d.accepted)]}, schema: ConvergeSchema, timeout: convergeTimeout},
			outputs, stopped(), func(v convergeReport) error { return checkRound(v, snap) },
			func(ctx context.Context, v convergeReport) error { return p.verifyWrites(ctx, r.Dir(), pr, v) })
		d.closed = closed
		if err != nil {
			d.result, d.err, d.kind = result, err, "round_failed"
			return d
		}
		d.accepted, d.last = n, v.Head
		after, err := p.pullAfter(ctx, r.Dir(), pr.Number, sp)
		if err != nil {
			return stop("state_read_failed", fmt.Errorf("after %s: %w", name, err))
		}
		if after.State != "open" || *after.Merged {
			return stop("pull_request_not_open", fmt.Errorf("%w: the read after %s shows state %q merged %t", errPullRequestNotOpen, name, after.State, *after.Merged))
		}
		if v.Converged && v.Correction == "no_change" && len(v.ThreadActions) == 0 && len(v.CIReruns) == 0 && after.Head.SHA == state.PullRequest.HeadSHA {
			if err := r.Root().Decision(ctx, "converged", fmt.Sprintf("Round %d claimed convergence without a push, a thread reply or resolution or a rerun, and the pull request head read after its session closed, %s, is its snapshot's head", n, after.Head.SHA), []contract.Ref{outputs[reportKey(n)], snap}); err != nil {
				return stop("round_failed", err)
			}
			return stop("converged", nil)
		}
	}
}

// unconverged ends rounds that ran out: it records decision with reason on
// report, the last report, and the last snapshot, if any, and returns cause,
// joined with the Decision's error if it fails.
func (p prepared) unconverged(ctx context.Context, r *engine.Run, outputs map[string]contract.Ref, report string, snapshot contract.Ref, decision, reason string, cause error) error {
	refs := []contract.Ref{outputs[report]}
	if snapshot != (contract.Ref{}) {
		refs = append(refs, snapshot)
	}
	if err := r.Root().Decision(ctx, decision, reason, refs); err != nil {
		return fmt.Errorf("%w; %w", cause, err)
	}
	return cause
}

// pullAfter is pull request number as one read within sp.reads shows it. A
// failed read wraps errStateRead; the run's cancellation, deadline or fatal
// error is returned as its cause.
func (p prepared) pullAfter(ctx context.Context, dir string, number int, sp statePolicy) (pullRequest, error) {
	reads, cancel := context.WithTimeoutCause(ctx, sp.reads, errReadsCap)
	defer cancel()
	pr, err := getPullRequest(reads, dir, p.repository, number)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return pullRequest{}, cause
		}
		return pullRequest{}, fmt.Errorf("%w: %w", errStateRead, err)
	}
	return pr, nil
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

// reviewThreadQuery is a Controller constant and a query operation, which
// is what keeps the thread receipt read read-only.
const reviewThreadQuery = `query($id: ID!) {
  node(id: $id) {
    __typename
    ... on PullRequestReviewThread {
      isResolved
      pullRequest { number repository { nameWithOwner } }
      comments(last: 100) { totalCount nodes { id } }
    }
  }
}`

// verifyWrites checks once, with read-only reads, the thread actions and
// reruns that v, a round's accepted contract, reports, in contract order.
// Each thread, read once however many actions name it, must be a review
// thread of the record's pull request in the target repository; a reply's
// comment must be among its last 100 comments and a resolved thread must
// show as resolved. Each run must be a GitHub Actions run whose head is the
// record's branch of the target repository, past its first attempt. A
// mismatch wraps errReceipt; a failed or unreadable read, or a reply that is
// not among the last 100 comments of a thread with more, errUnverifiable;
// neither is repaired or retried. A confirmed receipt shows that the state
// exists, not that this round made it: who wrote a reply, resolved a thread
// or started an attempt, when, and what any of them is worth, is not checked.
func (p prepared) verifyWrites(ctx context.Context, dir string, pr pullRequestRecord, v convergeReport) error {
	threads := map[string]reviewThread{}
	for i, a := range v.ThreadActions {
		t, ok := threads[a.ThreadID]
		if !ok {
			var err error
			if t, err = readThread(ctx, dir, a.ThreadID); err != nil {
				return fmt.Errorf("%w: thread_actions[%d]: %w", errUnverifiable, i, err)
			}
			threads[a.ThreadID] = t
		}
		if err := t.confirms(i, a, p.repository, pr.Number); err != nil {
			return err
		}
	}
	for i, rerun := range v.CIReruns {
		if err := p.confirmRerun(ctx, dir, i, rerun.RunID, pr.Branch); err != nil {
			return err
		}
	}
	return nil
}

// reviewThread is a node as reviewThreadQuery reads it; the other fields
// are set only for a review thread. Pointers tell a missing field from its
// zero value.
type reviewThread struct {
	Typename    string `json:"__typename"`
	IsResolved  *bool  `json:"isResolved"`
	PullRequest *struct {
		Number     *int `json:"number"`
		Repository *struct {
			NameWithOwner string `json:"nameWithOwner"`
		} `json:"repository"`
	} `json:"pullRequest"`
	Comments *struct {
		TotalCount *int `json:"totalCount"`
		Nodes      []struct {
			ID string `json:"id"`
		} `json:"nodes"`
	} `json:"comments"`
}

// readThread reads node id with reviewThreadQuery. A node GitHub does not
// resolve without an error is read as a node of no type.
func readThread(ctx context.Context, dir, id string) (reviewThread, error) {
	raw, err := runtime.GitHubGraphQL(ctx, dir, reviewThreadQuery, map[string]string{"id": id}, nil)
	if err != nil {
		return reviewThread{}, fmt.Errorf("review thread query: %w", err)
	}
	var pages []struct {
		Errors []json.RawMessage `json:"errors"`
		Data   *struct {
			Node *reviewThread `json:"node"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &pages) != nil || len(pages) != 1 {
		return reviewThread{}, errors.New("review thread query: not a single page object")
	}
	if len(pages[0].Errors) > 0 || pages[0].Data == nil {
		return reviewThread{}, errors.New("review thread query: the page has errors or no data")
	}
	if pages[0].Data.Node == nil {
		return reviewThread{}, nil
	}
	t := *pages[0].Data.Node
	if t.Typename == "PullRequestReviewThread" && (t.IsResolved == nil || t.PullRequest == nil || t.PullRequest.Number == nil || t.PullRequest.Repository == nil ||
		t.Comments == nil || t.Comments.TotalCount == nil || t.Comments.Nodes == nil) {
		return reviewThread{}, errors.New("review thread query: a thread without isResolved, pull request number and repository, or comments with totalCount and nodes")
	}
	return t, nil
}

// confirms checks thread_actions[i], a, against t, the node its thread ID
// names, for pull request number of repository.
func (t reviewThread) confirms(i int, a threadAction, repository string, number int) error {
	if t.Typename != "PullRequestReviewThread" {
		kind := "no node"
		if t.Typename != "" {
			kind = "a " + t.Typename
		}
		return fmt.Errorf("%w: thread_actions[%d].thread_id: got %q, which GitHub resolves to %s; want a review thread of pull request %d", errReceipt, i, a.ThreadID, kind, number)
	}
	if !strings.EqualFold(t.PullRequest.Repository.NameWithOwner, repository) || *t.PullRequest.Number != number {
		return fmt.Errorf("%w: thread_actions[%d].thread_id: got %q, a review thread of pull request %d in %q; want one of pull request %d in %s", errReceipt, i, a.ThreadID, *t.PullRequest.Number, t.PullRequest.Repository.NameWithOwner, number, repository)
	}
	if a.Action == "resolve" {
		if !*t.IsResolved {
			return fmt.Errorf("%w: thread_actions[%d]: got resolve, but GitHub shows thread %q not resolved", errReceipt, i, a.ThreadID)
		}
		return nil
	}
	// The schema admits only reply and resolve.
	for _, c := range t.Comments.Nodes {
		if c.ID == a.CommentID {
			return nil
		}
	}
	if *t.Comments.TotalCount > len(t.Comments.Nodes) {
		return fmt.Errorf("%w: thread_actions[%d].comment_id: got %q, which is not among the last 100 of the %d comments of thread %q, the only ones the Controller reads", errUnverifiable, i, a.CommentID, *t.Comments.TotalCount, a.ThreadID)
	}
	return fmt.Errorf("%w: thread_actions[%d].comment_id: got %q, which is not a comment of thread %q", errReceipt, i, a.CommentID, a.ThreadID)
}

// confirmRerun checks ci_reruns[i], run id, against its GitHub Actions run,
// read at that ID in p.repository: its head repository is p.repository, its
// head branch is branch, and its run attempt is at least 2. The request path
// already scopes the run to p.repository; the head repository tells the
// record's branch from a fork's branch of the same name.
func (p prepared) confirmRerun(ctx context.Context, dir string, i int, id int64, branch string) error {
	raw, err := runtime.GitHubGET(ctx, dir, fmt.Sprintf("repos/%s/actions/runs/%d", p.repository, id), false)
	if err != nil {
		return fmt.Errorf("%w: ci_reruns[%d]: workflow run GET: %w", errUnverifiable, i, err)
	}
	var run struct {
		HeadBranch     *string `json:"head_branch"`
		RunAttempt     *int    `json:"run_attempt"`
		HeadRepository *struct {
			FullName string `json:"full_name"`
		} `json:"head_repository"`
	}
	if json.Unmarshal(raw, &run) != nil || run.HeadBranch == nil || run.RunAttempt == nil || run.HeadRepository == nil {
		return fmt.Errorf("%w: ci_reruns[%d]: workflow run GET: not a run with head_branch, run_attempt and head_repository", errUnverifiable, i)
	}
	var mismatches []string
	if !strings.EqualFold(run.HeadRepository.FullName, p.repository) {
		mismatches = append(mismatches, fmt.Sprintf("head_repository %q; want %s", run.HeadRepository.FullName, p.repository))
	}
	if *run.HeadBranch != branch {
		mismatches = append(mismatches, fmt.Sprintf("head_branch %q; want the record's branch %q", *run.HeadBranch, branch))
	}
	if *run.RunAttempt < 2 {
		mismatches = append(mismatches, fmt.Sprintf("run_attempt %d; want a rerun, attempt 2 or later", *run.RunAttempt))
	}
	if len(mismatches) > 0 {
		return fmt.Errorf("%w: ci_reruns[%d].run_id: got %d: %s", errReceipt, i, id, strings.Join(mismatches, "; "))
	}
	return nil
}
