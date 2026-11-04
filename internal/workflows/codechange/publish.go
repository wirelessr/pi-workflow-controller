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
	"pi-workflow-controller/internal/github"
)

const publishTimeout = 45 * time.Minute

// errUnverifiable is a pull request receipt the Controller could not check
// because a GitHub read failed or returned something it cannot read. Like a
// mismatch it is an execution failure, never repaired or retried.
var errUnverifiable = errors.New("code-change cannot verify the pull request receipt")

// published is the part of the correction Step's contract the Controller
// checks.
type published struct {
	Dispositions []disposition `json:"dispositions"`
	Correction   string        `json:"correction"`
	Branch       string        `json:"branch"`
	Head         string        `json:"head"`
	PullRequest  reportedPull  `json:"pull_request"`
}

// disposition is keyed by review role and finding ID, since an ID is unique
// only within one review contract.
type disposition struct {
	Review string `json:"review"`
	ID     string `json:"id"`
}

type reportedPull struct {
	URL    string `json:"url"`
	Number int    `json:"number"`
}

// pullRequestRecord is the verified pull request's identity as GitHub showed
// it and the heads downstream needs; head_sha may be newer than the reported
// head.
type pullRequestRecord struct {
	URL           string `json:"url"`
	Number        int    `json:"number"`
	Branch        string `json:"branch"`
	HeadSHA       string `json:"head_sha"`
	ReportedHead  string `json:"reported_head"`
	CompareStatus string `json:"compare_status"`
}

// publish runs the correction Step on the implementer's session with both
// pre-PR review contracts as Feedback, then checks the accepted contract's
// receipt once, records the pull request GitHub shows and strictly closes the
// session. It succeeds only when the worktree HEAD read after that close is
// still the pushed head. Every failure from the Step on, while the run can
// still act, records the external objects that exist as the Final and
// returns the original error.
func publish(ctx context.Context, r *engine.Run, p prepared, hand handoff, reviewed engine.Result) (engine.Result, error) {
	outputs := reviewed.Outputs
	reviews := []contract.Ref{outputs["review-basic"], outputs["review-consistency"]}
	findingKeys, err := reviewFindings(ctx, r, reviews)
	if err != nil {
		return engine.Result{Outputs: outputs}, err
	}
	prompt, err := json.Marshal(task{Role: "implementer", Requirements: baselineRequirements + "\n\n" + publishRequirements, Worktree: p.worktree.repo.Worktree()})
	if err != nil {
		return engine.Result{Outputs: outputs}, err
	}
	out, v, err := stepDecoded(ctx, r, r.Root(), engine.StepSpec{Key: "publish", Session: hand.session, Prompt: string(prompt), Inputs: []contract.Ref{p.input, outputs["implement"]},
		Feedback: &engine.Feedback{Message: publishFeedback, Refs: reviews}, Output: contract.Spec{SchemaID: PublishSchema}, Timeout: publishTimeout, RepairKeepsWork: true},
		func(v published) error { return checkDispositions(v, findingKeys) })
	// The last contract that passed the schema, even a rejected one, is the
	// receipt the external objects record shows.
	var reported *published
	if out.Output != (contract.Ref{}) {
		outputs["publish"] = out.Output
		reported = &v
	}
	if err == nil {
		err = p.publishTail(ctx, r, hand, out, v, outputs)
	}
	if err != nil {
		return p.externalObjects(ctx, r, hand, outputs, reported, err)
	}
	return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: "publish"}}, nil
}

// publishTail checks the accepted receipt v, attaches the pull request
// record to outputs with its Decision, strictly closes the implementer's
// session and requires the worktree HEAD read after that close to be the
// pushed head.
func (p prepared) publishTail(ctx context.Context, r *engine.Run, hand handoff, out engine.StepResult, v published, outputs map[string]contract.Ref) error {
	record, err := p.verifyPullRequest(ctx, r, hand.head, v)
	if err != nil {
		return err
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "pull-request", Output: contract.Spec{SchemaID: PullRequestSchema}, Data: record})
	if err != nil {
		return err
	}
	outputs["pull-request"] = ref
	if err := r.Root().Decision(ctx, "pull-request-verified", "The reported head is the worktree HEAD and the pull request GitHub shows is open, not a draft, from the reported branch to the default branch, at that head or a descendant", []contract.Ref{out.Output, ref}); err != nil {
		return err
	}
	if err := closeStrictly(ctx, r, hand.session, out.Execution.SessionID, implementRole, "implementer"); err != nil {
		return err
	}
	// With the session confirmed closed, its process no longer moves the
	// HEAD. The removal relies only on the HEAD being the verified head;
	// uncommitted and untracked files and the worktree's reflog go with it.
	current, err := p.worktree.head(ctx)
	if err != nil {
		return err
	}
	if current != v.Head {
		return fmt.Errorf("%w: the worktree HEAD is %s after the implementer session closed; want the pushed head %s", errReceipt, current, v.Head)
	}
	return nil
}

// reviewFindings reads the finding keys of both accepted review contracts, in
// contract order, before anything is dispatched.
func reviewFindings(ctx context.Context, r *engine.Run, reviews []contract.Ref) ([]disposition, error) {
	var keys []disposition
	for _, ref := range reviews {
		v, err := engine.Decode[findings](ctx, r, ref)
		if err != nil {
			return nil, err
		}
		for _, f := range v.Findings {
			keys = append(keys, disposition{Review: v.Role, ID: f.ID})
		}
	}
	return keys, nil
}

// checkDispositions is the acceptance a repair can fix: exactly one
// disposition for each finding of both review contracts, keyed by review role
// and ID, and none for anything else. What a disposition says is not judged.
func checkDispositions(v published, findingKeys []disposition) error {
	want := map[disposition]bool{}
	for _, k := range findingKeys {
		want[k] = true
	}
	var errs []error
	seen := map[disposition]int{}
	for i, d := range v.Dispositions {
		switch j, ok := seen[d]; {
		case !want[d]:
			errs = append(errs, fmt.Errorf("dispositions[%d]: got review %q id %q, which is not a finding of that review; want only findings of the review contracts in request.feedback.refs", i, d.Review, d.ID))
		case ok:
			errs = append(errs, fmt.Errorf("dispositions[%d]: got review %q id %q, which dispositions[%d] already covers; want exactly one disposition per finding", i, d.Review, d.ID, j))
		default:
			seen[d] = i
		}
	}
	for _, k := range findingKeys {
		if _, ok := seen[k]; !ok {
			errs = append(errs, fmt.Errorf("dispositions: got none for review %q id %q; want exactly one disposition per finding", k.Review, k.ID))
		}
	}
	return errors.Join(errs...)
}

// verifyPullRequest checks the accepted receipt v once, local reads first:
// head is the worktree HEAD, read through the registration; a committed
// correction made head a commit that is neither last, the verified head, nor
// one of its ancestors, and no change left it at last, which the pull request
// check below still requires to be the pull request's head or one of its
// ancestors. Then GitHub reads:
// the pull request exists in the target repository under the reported URL
// and number; it is open, not merged and not a draft; its base is the
// default branch the input recorded; its head is the reported branch of the
// target repository; and head is the pull request's head or one of its
// ancestors. A mismatch wraps errReceipt and a failed or unreadable GitHub
// read errUnverifiable; neither is repaired or retried.
func (p prepared) verifyPullRequest(ctx context.Context, r *engine.Run, last string, v published) (pullRequestRecord, error) {
	w := p.worktree
	current, err := w.head(ctx)
	if err != nil {
		return pullRequestRecord{}, err
	}
	if v.Head != current {
		return pullRequestRecord{}, fmt.Errorf("%w: head: got %s; want the worktree HEAD %s", errReceipt, v.Head, current)
	}
	// The schema admits only committed and no_change.
	if v.Correction == "committed" {
		fresh, err := w.isNew(ctx, last, v.Head)
		if err != nil {
			return pullRequestRecord{}, err
		}
		if !fresh {
			return pullRequestRecord{}, fmt.Errorf("%w: head: got %s with correction committed, which is the verified head %s or one of its ancestors; want a new commit", errReceipt, v.Head, last)
		}
	} else if v.Head != last {
		return pullRequestRecord{}, fmt.Errorf("%w: head: got %s with correction no_change; want the verified head %s", errReceipt, v.Head, last)
	}
	pr, err := getPullRequest(ctx, r.Dir(), p.repository, v.PullRequest.Number)
	if err != nil {
		return pullRequestRecord{}, fmt.Errorf("%w: %w", errUnverifiable, err)
	}
	var mismatches []string
	if pr.HTMLURL != v.PullRequest.URL || pr.Number != v.PullRequest.Number {
		mismatches = append(mismatches, fmt.Sprintf("pull_request: got %s number %d; GitHub shows %s number %d", v.PullRequest.URL, v.PullRequest.Number, pr.HTMLURL, pr.Number))
	}
	if !strings.EqualFold(pr.Base.repository(), p.repository) {
		mismatches = append(mismatches, fmt.Sprintf("pull_request: the base repository is %q; want %s", pr.Base.repository(), p.repository))
	}
	if pr.State != "open" || *pr.Merged || *pr.Draft {
		mismatches = append(mismatches, fmt.Sprintf("pull_request: state %q merged %t draft %t; want an open pull request that is not merged and not a draft", pr.State, *pr.Merged, *pr.Draft))
	}
	if pr.Base.Ref != p.defaultBranch {
		mismatches = append(mismatches, fmt.Sprintf("pull_request: base %q; want the default branch %q", pr.Base.Ref, p.defaultBranch))
	}
	if !strings.EqualFold(pr.Head.repository(), p.repository) || pr.Head.Ref != v.Branch {
		mismatches = append(mismatches, fmt.Sprintf("branch: got %q; the pull request head is %q in %q, want %q in %s", v.Branch, pr.Head.Ref, pr.Head.repository(), v.Branch, p.repository))
	}
	if len(mismatches) > 0 {
		return pullRequestRecord{}, fmt.Errorf("%w: %s", errReceipt, strings.Join(mismatches, "; "))
	}
	ancestor, status, err := github.Ancestor(ctx, r.Dir(), p.repository, v.Head, pr.Head.SHA)
	if err != nil {
		return pullRequestRecord{}, fmt.Errorf("%w: %w", errUnverifiable, err)
	}
	if !ancestor {
		return pullRequestRecord{}, fmt.Errorf("%w: head: got %s, which is not the pull request head %s or one of its ancestors (compare status %s)", errReceipt, v.Head, pr.Head.SHA, status)
	}
	return pullRequestRecord{URL: pr.HTMLURL, Number: pr.Number, Branch: pr.Head.Ref, HeadSHA: pr.Head.SHA, ReportedHead: v.Head, CompareStatus: status}, nil
}

// externalObjectsRecord is what exists after a failure from the correction
// Step on, as the Controller observed it with reads only. It judges and
// undoes nothing.
type externalObjectsRecord struct {
	Failure      string            `json:"failure"`
	Session      sessionObserved   `json:"session"`
	Worktree     worktreeObserved  `json:"worktree"`
	RemoteBranch branchObserved    `json:"remote_branch"`
	PullRequests pullsObserved     `json:"pull_requests"`
	Reported     *reportedObserved `json:"reported,omitempty"`
}

// reportedObserved is the receipt of the last correction contract that
// passed the schema and what GitHub shows for its branch and pull request.
// A branch pushed under another name than the worktree's is seen only here.
type reportedObserved struct {
	Branch       string         `json:"branch"`
	Head         string         `json:"head"`
	PullRequest  reportedPull   `json:"pull_request"`
	RemoteBranch branchObserved `json:"remote_branch"`
	ObservedPull pullObserved   `json:"observed_pull_request"`
}

type sessionObserved struct {
	ExitConfirmed bool   `json:"exit_confirmed"`
	Detail        string `json:"detail,omitempty"`
}

type worktreeObserved struct {
	Status string  `json:"status"`
	Head   string  `json:"head,omitempty"`
	Branch *string `json:"branch,omitempty"`
	Detail string  `json:"detail,omitempty"`
}

type branchObserved struct {
	Status string `json:"status"`
	SHA    string `json:"sha,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type pullsObserved struct {
	Status string            `json:"status"`
	Items  []pullObservation `json:"items"`
	Detail string            `json:"detail,omitempty"`
}

type pullObserved struct {
	Status string           `json:"status"`
	Item   *pullObservation `json:"item,omitempty"`
	Detail string           `json:"detail,omitempty"`
}

type pullObservation struct {
	Number  int    `json:"number"`
	HTMLURL string `json:"html_url"`
	State   string `json:"state"`
	Draft   *bool  `json:"draft"`
	HeadRef string `json:"head_ref"`
	HeadSHA string `json:"head_sha"`
}

func observation(pr pullRequest) pullObservation {
	return pullObservation{Number: pr.Number, HTMLURL: pr.HTMLURL, State: pr.State, Draft: pr.Draft, HeadRef: pr.Head.Ref, HeadSHA: pr.Head.SHA}
}

// externalObjects records, after cause failed the run from the correction
// Step on, what exists: whether the implementer's session exit is confirmed,
// the worktree HEAD and branch read through the registration, that branch
// and its pull requests on GitHub, and reported, the last contract that
// passed the schema if any, with its branch and pull request on GitHub. The
// record is the Final and cause is returned unchanged; a failed read is
// recorded as unknown and does not change cause. While the run is
// cancelled, past its deadline or stopped by a fatal error it can do none of
// this and returns cause alone.
func (p prepared) externalObjects(ctx context.Context, r *engine.Run, hand handoff, outputs map[string]contract.Ref, reported *published, cause error) (engine.Result, error) {
	if ctx.Err() != nil {
		return engine.Result{Outputs: outputs}, cause
	}
	record := externalObjectsRecord{Failure: cause.Error()}
	// The session is closed first, so the Agent no longer writes while the
	// Controller looks.
	closed, err := r.CloseSessionReport(ctx, hand.session)
	switch {
	case err != nil:
		record.Session.Detail = err.Error()
	case !closed.ConfirmsLocalClose(hand.sessionID):
		record.Session.Detail = "the close report does not confirm the process exit"
	default:
		record.Session.ExitConfirmed = true
	}
	branch := ""
	record.Worktree.Status = "unknown"
	if head, err := p.worktree.head(ctx); err != nil {
		record.Worktree.Detail = err.Error()
	} else if branch, err = p.worktree.branch(ctx); err != nil {
		record.Worktree.Head, record.Worktree.Detail = head, err.Error()
	} else {
		record.Worktree.Status, record.Worktree.Head, record.Worktree.Branch = "read", head, &branch
	}
	// Without a worktree branch to look up, both stay unknown; the worktree
	// entry says why.
	record.RemoteBranch, record.PullRequests = branchObserved{Status: "unknown"}, pullsObserved{Status: "unknown", Items: []pullObservation{}}
	if record.Worktree.Status == "read" && branch != "" {
		record.RemoteBranch = p.observeBranch(ctx, r, branch)
		if pulls, err := branchPulls(ctx, r.Dir(), p.repository, branch); err != nil {
			record.PullRequests.Detail = err.Error()
		} else {
			record.PullRequests.Status = "listed"
			for _, pr := range pulls {
				record.PullRequests.Items = append(record.PullRequests.Items, observation(pr))
			}
		}
	}
	if reported != nil {
		record.Reported = &reportedObserved{Branch: reported.Branch, Head: reported.Head, PullRequest: reported.PullRequest, RemoteBranch: p.observeBranch(ctx, r, reported.Branch)}
		if pr, err := getPullRequest(ctx, r.Dir(), p.repository, reported.PullRequest.Number); err != nil {
			record.Reported.ObservedPull = pullObserved{Status: "unknown", Detail: err.Error()}
		} else {
			item := observation(pr)
			record.Reported.ObservedPull = pullObserved{Status: "read", Item: &item}
		}
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "external-objects", Output: contract.Spec{SchemaID: ExternalObjectsSchema}, Data: record})
	if err != nil {
		return engine.Result{Outputs: outputs}, errors.Join(cause, err)
	}
	outputs["external-objects"] = ref
	return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: "external-objects"}}, cause
}

// observeBranch is branch on GitHub as one read shows it.
func (p prepared) observeBranch(ctx context.Context, r *engine.Run, branch string) branchObserved {
	sha, err := remoteBranch(ctx, r.Dir(), p.repository, branch)
	switch {
	case err != nil:
		return branchObserved{Status: "unknown", Detail: err.Error()}
	case sha == "":
		return branchObserved{Status: "absent"}
	}
	return branchObserved{Status: "present", SHA: sha}
}
