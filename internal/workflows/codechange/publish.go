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

// errUnverifiable is a receipt the Controller could not check because a
// GitHub read failed or returned something it cannot read. Like a mismatch
// it is an execution failure, never repaired or retried.
var errUnverifiable = errors.New("code-change cannot verify a receipt")

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

// receipt is what the Controller checks of a push Step's accepted contract:
// the reported change and head, and the branch and pull request they must be
// on. builtOnLast also requires a committed head to have the verified head
// as an ancestor, for a Step that must not rewrite the history already
// pushed and reviewed.
type receipt struct {
	change, head, branch string
	pull                 reportedPull
	builtOnLast          bool
}

// receipt is the correction Step's receipt, all of it reported by the Agent.
func (v published) receipt() receipt {
	return receipt{change: v.Correction, head: v.Head, branch: v.Branch, pull: v.PullRequest}
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
	var reported *receipt
	if out.Output != (contract.Ref{}) {
		outputs["publish"] = out.Output
		rc := v.receipt()
		reported = &rc
	}
	if err == nil {
		err = p.publishTail(ctx, r, hand, out, v, outputs)
	}
	if err != nil {
		result, _, err := p.externalObjects(ctx, r, r.Root(), "", hand, "publish", outputs, reported, err)
		return result, err
	}
	return engine.Result{Outputs: outputs}, nil
}

// publishTail checks the accepted receipt v, attaches the pull request
// record to outputs with its Decision, strictly closes the implementer's
// session and requires the worktree HEAD read after that close to be the
// pushed head.
func (p prepared) publishTail(ctx context.Context, r *engine.Run, hand handoff, out engine.StepResult, v published, outputs map[string]contract.Ref) error {
	record, err := p.verifyPullRequest(ctx, r, hand.head, v.receipt())
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
	return p.closeAtHead(ctx, r, hand.session, out.Execution.SessionID, implementRole, "implementer", v.Head)
}

// closeAtHead strictly closes h, the session of who, and requires the
// worktree HEAD read after that close to be head, the pushed head the
// receipt check verified.
func (p prepared) closeAtHead(ctx context.Context, r *engine.Run, h *engine.SessionHandle, sessionID, phase, who, head string) error {
	if err := closeStrictly(ctx, r, h, sessionID, phase, who); err != nil {
		return err
	}
	// With the session confirmed closed, its process no longer moves the
	// HEAD. The removal relies only on the HEAD being the verified head;
	// uncommitted and untracked files and the worktree's reflog go with it.
	current, err := p.worktree.head(ctx)
	if err != nil {
		return err
	}
	if current != head {
		return fmt.Errorf("%w: the worktree HEAD is %s after the %s session closed; want the pushed head %s", errReceipt, current, who, head)
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

// checkDispositions is the correction Step's coverage check, keyed by
// review role and ID.
func checkDispositions(v published, findingKeys []disposition) error {
	return checkCoverage(v.Dispositions, findingKeys, func(d disposition) string { return fmt.Sprintf("review %q id %q", d.Review, d.ID) },
		"is not a finding of that review; want only findings of the review contracts in request.feedback.refs")
}

// checkCoverage is the acceptance a repair can fix: exactly one of got for
// each key of want and none for anything else. name is a key as a
// diagnostic shows it and unknown says why a key outside want is rejected.
// What a disposition says is not judged.
func checkCoverage[K comparable](got, want []K, name func(K) string, unknown string) error {
	wanted := map[K]bool{}
	for _, k := range want {
		wanted[k] = true
	}
	var errs []error
	seen := map[K]int{}
	for i, d := range got {
		switch j, ok := seen[d]; {
		case !wanted[d]:
			errs = append(errs, fmt.Errorf("dispositions[%d]: got %s, which %s", i, name(d), unknown))
		case ok:
			errs = append(errs, fmt.Errorf("dispositions[%d]: got %s, which dispositions[%d] already covers; want exactly one disposition per finding", i, name(d), j))
		default:
			seen[d] = i
		}
	}
	for _, k := range want {
		if _, ok := seen[k]; !ok {
			errs = append(errs, fmt.Errorf("dispositions: got none for %s; want exactly one disposition per finding", name(k)))
		}
	}
	return errors.Join(errs...)
}

// verifyPullRequest checks the accepted receipt rc once, local reads first:
// head is the worktree HEAD, read through the registration; a committed
// change made head a commit that is neither last, the verified head, nor
// one of its ancestors, and with builtOnLast has last as an ancestor; no
// change left it at last, which the pull request
// check below still requires to be the pull request's head or one of its
// ancestors. Then GitHub reads:
// the pull request exists in the target repository under the receipt's URL
// and number; it is open, not merged and not a draft; its base is the
// default branch the input recorded; its head is the receipt's branch of the
// target repository; and head is the pull request's head or one of its
// ancestors. A mismatch wraps errReceipt and a failed or unreadable GitHub
// read errUnverifiable; neither is repaired or retried.
func (p prepared) verifyPullRequest(ctx context.Context, r *engine.Run, last string, rc receipt) (pullRequestRecord, error) {
	w := p.worktree
	current, err := w.head(ctx)
	if err != nil {
		return pullRequestRecord{}, err
	}
	if rc.head != current {
		return pullRequestRecord{}, fmt.Errorf("%w: head: got %s; want the worktree HEAD %s", errReceipt, rc.head, current)
	}
	// The schemas admit only committed and no_change.
	if rc.change == "committed" {
		fresh, err := w.isNew(ctx, last, rc.head)
		if err != nil {
			return pullRequestRecord{}, err
		}
		if !fresh {
			return pullRequestRecord{}, fmt.Errorf("%w: head: got %s with correction committed, which is the verified head %s or one of its ancestors; want a new commit", errReceipt, rc.head, last)
		}
		if rc.builtOnLast {
			// The same count reversed: commits of last that head lacks.
			rewritten, err := w.isNew(ctx, rc.head, last)
			if err != nil {
				return pullRequestRecord{}, err
			}
			if rewritten {
				return pullRequestRecord{}, fmt.Errorf("%w: head: got %s with correction committed, which does not have the verified head %s as an ancestor; want a commit built on it", errReceipt, rc.head, last)
			}
		}
	} else if rc.head != last {
		return pullRequestRecord{}, fmt.Errorf("%w: head: got %s with correction no_change; want the verified head %s", errReceipt, rc.head, last)
	}
	pr, err := getPullRequest(ctx, r.Dir(), p.repository, rc.pull.Number)
	if err != nil {
		return pullRequestRecord{}, fmt.Errorf("%w: %w", errUnverifiable, err)
	}
	var mismatches []string
	if pr.HTMLURL != rc.pull.URL || pr.Number != rc.pull.Number {
		mismatches = append(mismatches, fmt.Sprintf("pull_request: got %s number %d; GitHub shows %s number %d", rc.pull.URL, rc.pull.Number, pr.HTMLURL, pr.Number))
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
	if !strings.EqualFold(pr.Head.repository(), p.repository) || pr.Head.Ref != rc.branch {
		mismatches = append(mismatches, fmt.Sprintf("branch: got %q; the pull request head is %q in %q, want %q in %s", rc.branch, pr.Head.Ref, pr.Head.repository(), rc.branch, p.repository))
	}
	if len(mismatches) > 0 {
		return pullRequestRecord{}, fmt.Errorf("%w: %s", errReceipt, strings.Join(mismatches, "; "))
	}
	ancestor, status, err := github.Ancestor(ctx, r.Dir(), p.repository, rc.head, pr.Head.SHA)
	if err != nil {
		return pullRequestRecord{}, fmt.Errorf("%w: %w", errUnverifiable, err)
	}
	if !ancestor {
		return pullRequestRecord{}, fmt.Errorf("%w: head: got %s, which is not the pull request head %s or one of its ancestors (compare status %s)", errReceipt, rc.head, pr.Head.SHA, status)
	}
	return pullRequestRecord{URL: pr.HTMLURL, Number: pr.Number, Branch: pr.Head.Ref, HeadSHA: pr.Head.SHA, ReportedHead: rc.head, CompareStatus: status}, nil
}

// externalObjectsRecord is what exists after a failure from the dispatch of
// a Step that writes to GitHub through its closing, as the Controller
// observed it with reads only. It judges and undoes nothing.
type externalObjectsRecord struct {
	Failure      string            `json:"failure"`
	Step         string            `json:"step"`
	Session      sessionObserved   `json:"session"`
	Worktree     worktreeObserved  `json:"worktree"`
	RemoteBranch branchObserved    `json:"remote_branch"`
	PullRequests pullsObserved     `json:"pull_requests"`
	Reported     *reportedObserved `json:"reported,omitempty"`
}

// reportedObserved is the receipt of the failed Step's last contract that
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

// externalObjects records, after cause failed the run from the dispatch of
// step, a Step that writes to GitHub, through its closing, what exists: whether the exit of that Step's
// session, hand's, is confirmed, the worktree HEAD and branch read through
// the registration, that branch and its pull requests on GitHub, and
// reported, the receipt of the Step's last contract that passed the schema
// if any, with its branch and pull request on GitHub. The record, attached
// in s and named prefix followed by external-objects in the outputs, is the
// Final and cause is returned unchanged; a failed read is recorded as
// unknown and does not change cause. While the run is cancelled, past its
// deadline or stopped by a fatal error it can do none of this and returns
// cause alone. The bool reports whether the session's exit was confirmed.
func (p prepared) externalObjects(ctx context.Context, r *engine.Run, s *engine.Scope, prefix string, hand handoff, step string, outputs map[string]contract.Ref, reported *receipt, cause error) (engine.Result, bool, error) {
	if ctx.Err() != nil {
		return engine.Result{Outputs: outputs}, false, cause
	}
	record := externalObjectsRecord{Failure: cause.Error(), Step: step}
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
		record.Reported = &reportedObserved{Branch: reported.branch, Head: reported.head, PullRequest: reported.pull, RemoteBranch: p.observeBranch(ctx, r, reported.branch)}
		if pr, err := getPullRequest(ctx, r.Dir(), p.repository, reported.pull.Number); err != nil {
			record.Reported.ObservedPull = pullObserved{Status: "unknown", Detail: err.Error()}
		} else {
			item := observation(pr)
			record.Reported.ObservedPull = pullObserved{Status: "read", Item: &item}
		}
	}
	ref, err := s.Attach(ctx, engine.AttachSpec{Key: "external-objects", Output: contract.Spec{SchemaID: ExternalObjectsSchema}, Data: record})
	if err != nil {
		return engine.Result{Outputs: outputs}, record.Session.ExitConfirmed, errors.Join(cause, err)
	}
	outputs[prefix+"external-objects"] = ref
	return engine.Result{Outputs: outputs, Final: &engine.FinalSelection{Output: prefix + "external-objects"}}, record.Session.ExitConfirmed, cause
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
