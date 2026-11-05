package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

// workBranch is the branch the fake Agent creates for the work.
const workBranch = "ABC-1-change"

func pullURL(n int) string { return fmt.Sprintf("https://github.com/owner/repo/pull/%d", n) }

// openPull records the next pull request from branch to main as GitHub
// shows it once the Agent opened it, and returns its number.
func (f *fixture) openPull(t *testing.T, branch string) int {
	t.Helper()
	dir := filepath.Join(f.dir, "prs")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := len(entries) + 1
	raw, err := json.Marshal(map[string]any{"number": n, "html_url": pullURL(n), "state": "open", "draft": false, "merged": false,
		"base": map[string]any{"ref": "main", "repo": map[string]any{"full_name": "owner/repo"}},
		"head": map[string]any{"ref": branch, "sha": f.originTip(branch), "repo": map[string]any{"full_name": "owner/repo"}}})
	if err != nil {
		t.Fatal(err)
	}
	f.write(filepath.Join(dir, fmt.Sprint(n)+".json"), string(raw))
	return n
}

// pulls is how many pull requests the fake Agent opened.
func (f *fixture) pulls(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.dir, "prs"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return len(entries)
}

// originTip is branch's tip in the origin, "" when it has no such branch.
func (f *fixture) originTip(branch string) string {
	return f.gitIn(f.origin, "for-each-ref", "--format=%(objectname)", "refs/heads/"+branch)
}

// setOriginBranch moves branch in the origin to a new commit on parent with
// parent's tree, standing for a push the Agent did not make, and returns it.
func (f *fixture) setOriginBranch(branch, parent string) string {
	commit := f.gitIn(f.origin, "commit-tree", parent+"^{tree}", "-p", parent, "-m", "pushed by someone else")
	f.gitIn(f.origin, "update-ref", "refs/heads/"+branch, commit)
	return commit
}

// dispositionsFor reads the review contracts in c's Feedback as the Agent
// does and leaves every finding as it is.
func dispositionsFor(t *testing.T, c agentCall) []map[string]any {
	t.Helper()
	out := []map[string]any{}
	for _, ref := range c.request.Feedback.Refs {
		var envelope struct {
			Data struct {
				Role     string `json:"role"`
				Findings []struct {
					ID string `json:"id"`
				} `json:"findings"`
			} `json:"data"`
		}
		if err := protocol.ReadJSON(ref.Path, &envelope); err != nil {
			t.Fatal(err)
		}
		for _, finding := range envelope.Data.Findings {
			out = append(out, map[string]any{"review": envelope.Data.Role, "id": finding.ID, "action": "not_fixed", "reason": "Not worth changing"})
		}
	}
	return out
}

func publishData(dispositions []map[string]any, correction, branch, head string, n int) map[string]any {
	return map[string]any{"dispositions": dispositions, "correction": correction, "branch": branch, "head": head,
		"pull_request":      map[string]any{"url": pullURL(n), "number": n},
		"ticket_transition": map[string]any{"ticket": "ABC-1", "target": "In Review", "status": "done", "detail": "Moved to In Review"}}
}

// publishWork is the fake Agent's publication: it puts a detached worktree
// HEAD on workBranch, pushes the branch to the origin, opens a pull request
// and returns the contract that reports them, with every finding not fixed.
func (f *fixture) publishWork(t *testing.T, c agentCall, correction string) map[string]any {
	t.Helper()
	wt := c.task.Worktree
	if f.gitIn(wt, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD" {
		f.gitIn(wt, "checkout", "-q", "-b", workBranch)
	}
	branch := f.gitIn(wt, "rev-parse", "--abbrev-ref", "HEAD")
	f.gitIn(wt, "push", "-q", "origin", "HEAD:refs/heads/"+branch)
	n := f.openPull(t, branch)
	return publishData(dispositionsFor(t, c), correction, branch, f.gitIn(wt, "rev-parse", "HEAD"), n)
}

// acceptPublish publishes the work without a correction.
func (f *fixture) acceptPublish(t *testing.T, c agentCall) string {
	writeCandidate(t, c, f.publishWork(t, c, "no_change"))
	return ""
}

func oneFinding(t *testing.T, c agentCall) string {
	writeCandidate(t, c, reviewData(c, map[string]any{"id": "f1", "summary": "A finding", "detail": "Why it is one"}))
	return ""
}

func (a a1Run) publishCalls() []agentCall {
	var out []agentCall
	for _, c := range a.calls {
		if c.request.Output.SchemaID == PublishSchema {
			out = append(out, c)
		}
	}
	return out
}

// output reads the data of the run's output key.
func output[T any](t *testing.T, a a1Run, key string) T {
	t.Helper()
	var envelope struct {
		Data T `json:"data"`
	}
	ref, ok := a.report.Result.Outputs[key]
	if !ok {
		t.Fatalf("no output %s in %+v", key, a.report.Result.Outputs)
	}
	if err := protocol.ReadJSON(ref.Path, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Data
}

// The correction Step runs on the implementer's session with both review
// contracts as Feedback; its receipt is checked once against the worktree and
// GitHub, the pull request is recorded and the session strictly closed.
// Only the workflow's successful return, after the fix Step
// (TestFixSucceeds), sets the removal, so the worktree is still there
// when it returns and the run's cleanup removes it.
func TestPublishSucceeds(t *testing.T) {
	for _, correction := range []string{"committed", "no_change"} {
		t.Run(correction, func(t *testing.T) {
			f := newFixture(t)
			var implemented, pushed string
			var checkoutAtReturn error
			a := f.run(t, agents{
				implement: f.implementReady(&implemented),
				review:    oneFinding,
				publish: func(t *testing.T, c agentCall) string {
					if correction == "committed" {
						f.commit(t, c.task.Worktree, "fixed\n")
					}
					data := f.publishWork(t, c, correction)
					data["dispositions"].([]map[string]any)[0]["action"] = "fixed"
					pushed = data["head"].(string)
					writeCandidate(t, c, data)
					return ""
				},
				returned: func(r *engine.Run) { _, checkoutAtReturn = os.Lstat(filepath.Join(r.Dir(), worktreeDir, "checkout")) },
			})
			report := a.report
			if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil {
				t.Fatalf("outcome %s exit %d failure %v", report.Outcome, report.ExitCode, report.Failure)
			}
			if (pushed == implemented) != (correction == "no_change") {
				t.Fatalf("pushed %s, implemented %s", pushed, implemented)
			}
			a.requireExitsConfirmed(t)
			outputs := report.Result.Outputs
			calls := a.publishCalls()
			if len(calls) != 1 {
				t.Fatalf("publisher dispatches %d", len(calls))
			}
			c := calls[0]
			implementAttempt, publishAttempt := a.attempt(a.calls[0]), a.attempt(c)
			if publishAttempt.Scope != "root" || publishAttempt.Key != "publish" || publishAttempt.HandleID != implementAttempt.HandleID || *publishAttempt.Output != outputs["publish"] {
				t.Fatalf("publish attempt scope %s key %s handle %s (implementer %s)", publishAttempt.Scope, publishAttempt.Key, publishAttempt.HandleID, implementAttempt.HandleID)
			}
			if !reflect.DeepEqual(c.task, task{Role: "implementer", Requirements: baselineRequirements + "\n\n" + publishRequirements, Worktree: a.worktree()}) {
				t.Fatalf("publish task %+v", c.task)
			}
			reviews := []contract.Ref{outputs["review-basic"], outputs["review-consistency"]}
			if !reflect.DeepEqual(c.request.Inputs, []contract.Ref{outputs["input"], outputs["implement"]}) || c.request.Output.SchemaID != PublishSchema ||
				c.request.Feedback == nil || c.request.Feedback.Message != publishFeedback || !reflect.DeepEqual(c.request.Feedback.Refs, reviews) {
				t.Fatalf("publish request inputs %+v feedback %+v schema %s", c.request.Inputs, c.request.Feedback, c.request.Output.SchemaID)
			}
			// Equal heads need no compare read, neither for the receipt nor
			// for the deep review's reads that follow it; every read is a GET.
			const get = "api --method GET --hostname github.com -H Accept: application/vnd.github+json "
			reads := []string{get + "repos/owner/repo", get + "repos/owner/repo/pulls/1", get + "repos/owner/repo/pulls/1",
				get + "repos/owner/repo/issues/1/comments?per_page=100 --paginate --slurp", get + "repos/owner/repo/pulls/1/comments?per_page=100 --paginate --slurp",
				get + "repos/owner/repo/pulls/1/reviews?per_page=100 --paginate --slurp"}
			if calls := f.ghCalls(); len(calls) < len(reads) || !slices.Equal(calls[:len(reads)], reads) {
				t.Fatalf("gh calls %q", calls)
			}
			want := pullRequestRecord{URL: pullURL(1), Number: 1, Branch: workBranch, HeadSHA: pushed, ReportedHead: pushed, CompareStatus: "identical"}
			if got := output[pullRequestRecord](t, a, "pull-request"); got != want {
				t.Fatalf("pull request record %+v, want %+v", got, want)
			}
			// The deep review's Decisions follow (TestDeepReviewSucceeds).
			first := []decision{{Scope: "root", Name: "implementation-recorded", Refs: []contract.Ref{outputs["implement"]}},
				{Scope: "root", Name: "pre-pr-reviewed", Refs: reviews},
				{Scope: "root", Name: "pull-request-verified", Refs: []contract.Ref{outputs["publish"], outputs["pull-request"]}}}
			if got := a.decisions(t); len(got) < 3 || !reflect.DeepEqual(got[:3], first) {
				t.Fatalf("decisions %+v, want %+v first", got, first)
			}
			// One session served the implementer and the correction; the
			// workflow closed it once, after recording the pull request.
			closed, finalizing, decided := a.journalIndex(t)
			if at := closed[implementAttempt.HandleID]; len(at) != 1 || at[0] < decided["pull-request-verified"] || at[0] > finalizing {
				t.Fatalf("implementer session closed at %v, verified at %d, finalizing at %d", at, decided["pull-request-verified"], finalizing)
			}
			if checkoutAtReturn != nil {
				t.Fatalf("worktree at the workflow's return: %v", checkoutAtReturn)
			}
			requireMissing(t, a.worktree())
			requireMissing(t, filepath.Join(a.run.Dir(), worktreeDir, "repository.git", "worktrees", "checkout"))
			if info, err := os.Stat(filepath.Join(a.run.Dir(), worktreeDir, "repository.git")); err != nil || !info.IsDir() {
				t.Fatalf("repository removed: %v", err)
			}
			if f.originTip(workBranch) != pushed || f.originTip("main") != f.head || f.pulls(t) != 1 {
				t.Fatalf("origin %s %s, pulls %d", f.originTip(workBranch), f.originTip("main"), f.pulls(t))
			}
		})
	}
}

// A pull request head pushed past the reported head passes with one compare
// read, and the record shows both heads.
func TestPublishPullRequestAhead(t *testing.T) {
	f := newFixture(t)
	var implemented, pushed, newer string
	a := f.run(t, agents{
		implement: f.implementReady(&implemented),
		publish: func(t *testing.T, c agentCall) string {
			f.commit(t, c.task.Worktree, "fixed\n")
			data := f.publishWork(t, c, "committed")
			pushed = data["head"].(string)
			newer = f.setOriginBranch(workBranch, pushed)
			writeCandidate(t, c, data)
			return ""
		},
	})
	if a.report.Outcome != engine.Succeeded {
		t.Fatalf("outcome %s: %v", a.report.Outcome, a.report.Failure)
	}
	record := output[pullRequestRecord](t, a, "pull-request")
	if record.HeadSHA != newer || record.ReportedHead != pushed || record.CompareStatus != "ahead" {
		t.Fatalf("record %+v, want head %s reported %s", record, newer, pushed)
	}
	// One compare for the receipt and one for the deep review that follows
	// it; every read is a GET.
	const get = "api --method GET --hostname github.com -H Accept: application/vnd.github+json "
	compare := get + "repos/owner/repo/compare/" + pushed + "..." + newer
	reads := []string{get + "repos/owner/repo", get + "repos/owner/repo/pulls/1", compare, get + "repos/owner/repo/pulls/1", compare,
		get + "repos/owner/repo/issues/1/comments?per_page=100 --paginate --slurp", get + "repos/owner/repo/pulls/1/comments?per_page=100 --paginate --slurp",
		get + "repos/owner/repo/pulls/1/reviews?per_page=100 --paginate --slurp"}
	if calls := f.ghCalls(); len(calls) < len(reads) || !slices.Equal(calls[:len(reads)], reads) {
		t.Fatalf("gh calls %q", calls)
	}
}

// A receipt the worktree or GitHub does not confirm, or that a GitHub read
// cannot check, fails the run once the contract was accepted, with one case
// per check: no repair, and the external objects that exist recorded as the
// Final with the original error, shown in full for the first case.
func TestPublishReceiptRejected(t *testing.T) {
	type state = receiptState
	for i, tc := range []struct {
		name string
		// publish writes like the Agent and returns the contract.
		publish func(t *testing.T, f *fixture, c agentCall, s state) map[string]any
		want    func(f *fixture, s state) string
		// unverifiable is a GitHub read that failed rather than a mismatch.
		unverifiable bool
	}{
		{"head-not-the-worktree-head", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			f.commit(t, c.task.Worktree, "fixed\n")
			data := f.publishWork(t, c, "committed")
			data["head"] = s.implemented
			return data
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.implemented + "; want the worktree HEAD " + s.head
		}, false},
		{"committed-without-a-commit", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			return f.publishWork(t, c, "committed")
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.implemented + " with correction committed, which is the verified head " + s.implemented + " or one of its ancestors; want a new commit"
		}, false},
		{"no-change-with-a-commit", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			f.commit(t, c.task.Worktree, "fixed\n")
			return f.publishWork(t, c, "no_change")
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.head + " with correction no_change; want the verified head " + s.implemented
		}, false},
		{"pull-request-not-found", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.publishWork(t, c, "no_change")
			data["pull_request"] = map[string]any{"url": pullURL(2), "number": 2}
			return data
		}, func(*fixture, state) string { return "cannot verify the pull request receipt: pull request GET: " }, true},
		{"pull-request-not-an-object", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			f.write(filepath.Join(f.dir, "gh-body-pull"), "[]")
			return f.publishWork(t, c, "no_change")
		}, func(*fixture, state) string {
			return "cannot verify the pull request receipt: pull request GET: not a pull request object"
		}, true},
		{"pull-request-without-draft", patchPull(`{"draft":null}`), func(*fixture, state) string {
			return "cannot verify the pull request receipt: pull request GET: no draft or merged field"
		}, true},
		{"pull-request-without-merged", patchPull(`{"merged":null}`), func(*fixture, state) string {
			return "cannot verify the pull request receipt: pull request GET: no draft or merged field"
		}, true},
		// The head SHA goes into the compare path and the record only once
		// it is a full commit SHA.
		{"pull-request-head-not-a-sha", patchPull(`{"head":{"sha":"main"}}`), func(*fixture, state) string {
			return `cannot verify the pull request receipt: pull request GET: head.sha "main" is not a full commit SHA`
		}, true},
		{"url-mismatch", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.publishWork(t, c, "no_change")
			data["pull_request"] = map[string]any{"url": pullURL(9), "number": 1}
			return data
		}, func(*fixture, state) string {
			return "receipt mismatch: pull_request: got " + pullURL(9) + " number 1; GitHub shows " + pullURL(1) + " number 1"
		}, false},
		{"base-repository", patchPull(`{"base":{"repo":{"full_name":"other/repo"}}}`), func(*fixture, state) string {
			return `receipt mismatch: pull_request: the base repository is "other/repo"; want owner/repo`
		}, false},
		{"closed", patchPull(`{"state":"closed"}`), func(*fixture, state) string {
			return `receipt mismatch: pull_request: state "closed" merged false draft false; want an open pull request that is not merged and not a draft`
		}, false},
		{"merged", patchPull(`{"merged":true}`), func(*fixture, state) string {
			return `receipt mismatch: pull_request: state "open" merged true draft false; want`
		}, false},
		{"draft", patchPull(`{"draft":true}`), func(*fixture, state) string {
			return `receipt mismatch: pull_request: state "open" merged false draft true; want`
		}, false},
		{"base-not-default-branch", patchPull(`{"base":{"ref":"release"}}`), func(*fixture, state) string {
			return `receipt mismatch: pull_request: base "release"; want the default branch "main"`
		}, false},
		{"head-in-a-fork", patchPull(`{"head":{"repo":{"full_name":"fork/repo"}}}`), func(*fixture, state) string {
			return `receipt mismatch: branch: got "` + workBranch + `"; the pull request head is "` + workBranch + `" in "fork/repo", want "` + workBranch + `" in owner/repo`
		}, false},
		{"other-branch", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.publishWork(t, c, "no_change")
			data["branch"] = "other"
			return data
		}, func(*fixture, state) string {
			return `receipt mismatch: branch: got "other"; the pull request head is "` + workBranch + `" in "owner/repo", want "other" in owner/repo`
		}, false},
		{"head-behind", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.publishWork(t, c, "no_change")
			f.gitIn(f.origin, "update-ref", "refs/heads/"+workBranch, f.head)
			return data
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.implemented + ", which is not the pull request head " + f.head + " or one of its ancestors (compare status behind)"
		}, false},
		// Ahead alone is not enough: the reported head must be the merge base.
		{"compare-other-merge-base", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.publishWork(t, c, "no_change")
			f.setOriginBranch(workBranch, s.implemented)
			f.write(filepath.Join(f.dir, "gh-body-compare"), `{"status":"ahead","merge_base_commit":{"sha":"`+f.head+`"}}`)
			return data
		}, func(*fixture, state) string { return "or one of its ancestors (compare status ahead)" }, false},
		{"head-not-pushed", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			f.publishWork(t, c, "no_change")
			return publishData(dispositionsFor(t, c), "committed", workBranch, f.commit(t, c.task.Worktree, "fixed\n"), 1)
		}, func(*fixture, state) string { return "cannot verify the pull request receipt: compare GET: " }, true},
		{"compare-malformed", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.publishWork(t, c, "no_change")
			f.setOriginBranch(workBranch, s.implemented)
			f.write(filepath.Join(f.dir, "gh-body-compare"), `{"status":"unknown"}`)
			return data
		}, func(*fixture, state) string {
			return `cannot verify the pull request receipt: compare GET: status "unknown" is not ahead, behind, diverged or identical`
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var s state
			a := f.run(t, agents{
				implement: f.implementReady(&s.implemented),
				publish: func(t *testing.T, c agentCall) string {
					writeCandidate(t, c, tc.publish(t, f, c, s))
					return ""
				},
			})
			report := a.report
			s.head = f.gitIn(a.worktree(), "rev-parse", "HEAD")
			want := tc.want(f, s)
			if report.Outcome != engine.Failed || report.ExitCode != 1 || errors.Is(report.Failure, errReceipt) == tc.unverifiable || errors.Is(report.Failure, errUnverifiable) != tc.unverifiable || !strings.Contains(report.Failure.Error(), want) {
				t.Fatalf("outcome %s exit %d failure %v, want %q (unverifiable %v)", report.Outcome, report.ExitCode, report.Failure, want, tc.unverifiable)
			}
			if len(a.publishCalls()) != 1 {
				t.Fatalf("publisher dispatches %d, want no repair", len(a.publishCalls()))
			}
			if i > 0 {
				return
			}
			if report.Final == nil || report.Final.Output != "external-objects" || report.Final.Ref != report.Result.Outputs["external-objects"] {
				t.Fatalf("final %+v", report.Final)
			}
			if _, ok := report.Result.Outputs["pull-request"]; ok || report.Result.Outputs["publish"] != *a.attempt(a.publishCalls()[0]).Output {
				t.Fatalf("outputs %+v", report.Result.Outputs)
			}
			if got := a.decisions(t); len(got) != 2 || got[1].Name != "pre-pr-reviewed" {
				t.Fatalf("decisions %+v", got)
			}
			a.requireSessionsClosed(t, 3)
			a.requireAttempts(t, 4, "input", "skills", "external-objects")
			requireKept(t, a.run)
			// The worktree branch and the reported one are the same here.
			branch, tip := workBranch, f.originTip(workBranch)
			pr := pullObservation{Number: 1, HTMLURL: pullURL(1), State: "open", Draft: new(bool), HeadRef: workBranch, HeadSHA: tip}
			wantRecord := externalObjectsRecord{Failure: report.Failure.Error(), Step: "publish", Session: sessionObserved{ExitConfirmed: true}, Worktree: worktreeObserved{Status: "read", Head: s.head, Branch: &branch},
				RemoteBranch: branchObserved{Status: "present", SHA: tip}, PullRequests: pullsObserved{Status: "listed", Items: []pullObservation{pr}},
				Reported: &reportedObserved{Branch: workBranch, Head: s.implemented, PullRequest: reportedPull{URL: pullURL(1), Number: 1},
					RemoteBranch: branchObserved{Status: "present", SHA: tip}, ObservedPull: pullObserved{Status: "read", Item: &pr}}}
			if record := output[externalObjectsRecord](t, a, "external-objects"); !reflect.DeepEqual(record, wantRecord) {
				t.Fatalf("external objects %+v, want %+v", record, wantRecord)
			}
			if f.pulls(t) != 1 {
				t.Fatalf("pull requests %d", f.pulls(t))
			}
		})
	}
}

// receiptState is the implemented head and, after the run, the worktree HEAD.
type receiptState struct {
	implemented, head string
}

// patchPull publishes, or fixes, without a change while GitHub shows the
// pull request with patch applied.
func patchPull(patch string) func(t *testing.T, f *fixture, c agentCall, s receiptState) map[string]any {
	return func(t *testing.T, f *fixture, c agentCall, s receiptState) map[string]any {
		f.write(filepath.Join(f.dir, "pr-patch.json"), patch)
		if c.request.Output.SchemaID == FixSchema {
			return f.fixWork(t, c, "no_change")
		}
		return f.publishWork(t, c, "no_change")
	}
}

// A contract the schema or the disposition check rejects (the diagnostics
// are in TestCheckDispositions) is rewritten on the same session under the
// RepairKeepsWork dispatch text, with the review
// contracts still as Feedback: the Agent pushes and opens the pull request
// once, and only the final contract's receipt is checked, so a wrong head in
// a rejected contract does not matter.
func TestPublishRepair(t *testing.T) {
	for _, tc := range []struct {
		name       string
		bad        func(data map[string]any)
		diagnostic string
	}{
		{"schema", func(data map[string]any) { delete(data, "ticket_transition") }, "ticket_transition"},
		{"missing-disposition", func(data map[string]any) {
			data["dispositions"] = data["dispositions"].([]map[string]any)[:1]
			data["head"] = strings.Repeat("ab", 20)
		}, `dispositions: got none for review "consistency" id "f1"; want exactly one disposition per finding`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented string
			var good map[string]any
			a := f.run(t, agents{
				implement: f.implementReady(&implemented),
				review:    oneFinding,
				publish: func(t *testing.T, c agentCall) string {
					if good == nil {
						good = f.publishWork(t, c, "no_change")
						bad := maps.Clone(good)
						tc.bad(bad)
						writeCandidate(t, c, bad)
						return ""
					}
					writeCandidate(t, c, good)
					return ""
				},
			})
			report := a.report
			if report.Outcome != engine.Succeeded {
				t.Fatalf("outcome %s failure %v", report.Outcome, report.Failure)
			}
			calls := a.publishCalls()
			if len(calls) != 2 {
				t.Fatalf("publisher dispatches %d", len(calls))
			}
			first, repair := a.attempt(calls[0]), a.attempt(calls[1])
			if repair.HandleID != first.HandleID || first.HandleID != a.attempt(a.calls[0]).HandleID || repair.Scope != "root/contract-repair-publish" || report.Result.Outputs["publish"] != *repair.Output {
				t.Fatalf("repair handle %s (first %s) scope %s", repair.HandleID, first.HandleID, repair.Scope)
			}
			reviews := []contract.Ref{report.Result.Outputs["review-basic"], report.Result.Outputs["review-consistency"]}
			if fb := calls[1].request.Feedback; fb == nil || !strings.Contains(fb.Message, tc.diagnostic) || !reflect.DeepEqual(fb.Refs, reviews) {
				t.Fatalf("repair feedback %+v, want %q and the review Refs", fb, tc.diagnostic)
			}
			if !reflect.DeepEqual(calls[1].task, calls[0].task) || !reflect.DeepEqual(calls[1].request.Inputs, calls[0].request.Inputs) {
				t.Fatal("repair task or inputs changed")
			}
			a.requireRepairKeepsWork(t, calls...)
			// One push, one pull request, and one receipt read after the repo
			// GET, before the deep review reads the pull request.
			if f.pulls(t) != 1 || f.originTip(workBranch) != implemented {
				t.Fatalf("pull requests %d, branch %s", f.pulls(t), f.originTip(workBranch))
			}
			const get = "api --method GET --hostname github.com -H Accept: application/vnd.github+json "
			if calls := f.ghCalls(); len(calls) < 4 || !slices.Equal(calls[:4], []string{get + "repos/owner/repo", get + "repos/owner/repo/pulls/1", get + "repos/owner/repo/pulls/1",
				get + "repos/owner/repo/issues/1/comments?per_page=100 --paginate --slurp"}) {
				t.Fatalf("gh calls %q", calls)
			}
		})
	}
}

// Every finding of both reviews, keyed by role and ID, needs exactly one
// disposition and nothing else may have one; all violations are reported
// together, each with its field, what was received and what is wanted.
func TestCheckDispositions(t *testing.T) {
	keys := []disposition{{"basic", "f1"}, {"consistency", "f1"}, {"consistency", "f2"}}
	if err := checkDispositions(published{Dispositions: []disposition{{"consistency", "f2"}, {"basic", "f1"}, {"consistency", "f1"}}}, keys); err != nil {
		t.Fatalf("complete dispositions rejected: %v", err)
	}
	if err := checkDispositions(published{}, nil); err != nil {
		t.Fatalf("no findings and no dispositions rejected: %v", err)
	}
	err := checkDispositions(published{Dispositions: []disposition{{"basic", "f1"}, {"basic", "f1"}, {"basic", "f2"}, {"consistency", "f2"}}}, keys)
	for _, diagnostic := range []string{
		`dispositions[1]: got review "basic" id "f1", which dispositions[0] already covers; want exactly one disposition per finding`,
		`dispositions[2]: got review "basic" id "f2", which is not a finding of that review; want only findings of the review contracts in request.feedback.refs`,
		`dispositions: got none for review "consistency" id "f1"; want exactly one disposition per finding`,
	} {
		if err == nil || !slices.Contains(strings.Split(err.Error(), "\n"), diagnostic) {
			t.Fatalf("checkDispositions = %v, want %q", err, diagnostic)
		}
	}
	if n := len(strings.Split(err.Error(), "\n")); n != 3 {
		t.Fatalf("checkDispositions reported %d violations: %v", n, err)
	}
}

// A correction Step that fails after it may have written fails the run with
// its own error; the Controller closes the session, then reads the worktree
// branch through the registration and records that branch and its pull
// requests on GitHub as the Final, and with them the receipt of the last
// contract that passed the schema, rejected or not, with its branch and pull
// request on GitHub. A read that fails is recorded as unknown and leaves the
// error as it is. Nothing is closed or deleted. The GitHub fixture refuses
// the worktree branch reads until the implementer's session closed.
func TestPublishFailureRecordsExternalObjects(t *testing.T) {
	// rejected writes, every time, a contract with a disposition for a
	// finding no review has, reporting the work pushed to workBranch.
	rejected := func(t *testing.T, f *fixture, c agentCall) {
		if f.pulls(t) == 0 {
			if f.gitIn(c.task.Worktree, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD" {
				// Detached, the work goes to workBranch by refspec only.
				f.gitIn(c.task.Worktree, "push", "-q", "origin", "HEAD:refs/heads/"+workBranch)
				f.openPull(t, workBranch)
			} else {
				f.publishWork(t, c, "no_change")
			}
		}
		writeCandidate(t, c, publishData([]map[string]any{{"review": "basic", "id": "none", "action": "fixed", "reason": "r"}}, "no_change", workBranch, f.originTip(workBranch), 1))
	}
	for _, tc := range []struct {
		name string
		// publish acts like the Agent and returns the prompt answer.
		publish func(t *testing.T, f *fixture, c agentCall) string
		code    engine.Code
		want    func(f *fixture, head string) externalObjectsRecord
	}{
		{"repairs-exhausted", func(t *testing.T, f *fixture, c agentCall) string {
			f.gitIn(c.task.Worktree, "checkout", "-q", "-B", workBranch)
			rejected(t, f, c)
			return ""
		}, engine.WorkflowFailed, func(f *fixture, head string) externalObjectsRecord {
			record := published1(f, head)
			record.Reported = reported1(head)
			return record
		}},
		{"provider-failure", func(t *testing.T, f *fixture, c agentCall) string {
			f.publishWork(t, c, "no_change")
			return "provider-error"
		}, engine.ProviderFailed, published1},
		// Only the exact branch ref counts, not one its name is a prefix of.
		{"branch-not-pushed", func(t *testing.T, f *fixture, c agentCall) string {
			f.gitIn(c.task.Worktree, "checkout", "-q", "-b", workBranch)
			f.setOriginBranch(workBranch+"-old", f.head)
			return "provider-error"
		}, engine.ProviderFailed, func(f *fixture, head string) externalObjectsRecord {
			branch := workBranch
			return externalObjectsRecord{Worktree: worktreeObserved{Status: "read", Head: head, Branch: &branch}, RemoteBranch: branchObserved{Status: "absent"}, PullRequests: pullsObserved{Status: "listed", Items: []pullObservation{}}}
		}},
		// A detached HEAD has no branch to look up; the work the Agent pushed
		// to a branch by refspec is seen only through the reported receipt.
		{"detached-refspec-push", func(t *testing.T, f *fixture, c agentCall) string {
			rejected(t, f, c)
			return ""
		}, engine.WorkflowFailed, func(f *fixture, head string) externalObjectsRecord {
			detached := ""
			return externalObjectsRecord{Worktree: worktreeObserved{Status: "read", Head: head, Branch: &detached}, RemoteBranch: branchObserved{Status: "unknown"},
				PullRequests: pullsObserved{Status: "unknown", Items: []pullObservation{}}, Reported: reported1(head)}
		}},
		{"lookups-fail", func(t *testing.T, f *fixture, c agentCall) string {
			f.publishWork(t, c, "no_change")
			f.write(filepath.Join(f.dir, "gh-fail-refs"), "")
			f.write(filepath.Join(f.dir, "gh-fail-pulls"), "")
			return "provider-error"
		}, engine.ProviderFailed, func(f *fixture, head string) externalObjectsRecord {
			branch := workBranch
			return externalObjectsRecord{Worktree: worktreeObserved{Status: "read", Head: head, Branch: &branch},
				RemoteBranch: branchObserved{Status: "unknown", Detail: "branch GET: "},
				PullRequests: pullsObserved{Status: "unknown", Items: []pullObservation{}, Detail: "pull request list GET: "}}
		}},
		// The checkout's .git file points at another repository on another
		// branch; the Controller still reads the registration.
		{"dot-git-redirected", func(t *testing.T, f *fixture, c agentCall) string {
			f.publishWork(t, c, "no_change")
			other := filepath.Join(f.dir, "other")
			if err := os.Mkdir(other, 0700); err != nil {
				t.Fatal(err)
			}
			f.gitIn(other, "init", "-q", "--template=", "-b", "elsewhere")
			f.gitIn(other, "commit", "-q", "--allow-empty", "-m", "other")
			f.write(filepath.Join(c.task.Worktree, ".git"), "gitdir: "+filepath.Join(other, ".git")+"\n")
			return "provider-error"
		}, engine.ProviderFailed, published1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.write(filepath.Join(f.dir, "gh-require-closed"), "")
			var implemented string
			a := f.run(t, agents{
				implement: f.implementReady(&implemented),
				publish:   func(t *testing.T, c agentCall) string { return tc.publish(t, f, c) },
				close: func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
					report, err := s.Close(ctx)
					if role == implementRole {
						f.write(filepath.Join(f.dir, "implementer-closed"), "")
					}
					return report, err
				},
			})
			report := a.report
			if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Snapshot.Failure == nil || report.Snapshot.Failure.Code != tc.code {
				t.Fatalf("outcome %s exit %d failure %+v, want code %s", report.Outcome, report.ExitCode, report.Snapshot.Failure, tc.code)
			}
			if report.Final == nil || report.Final.Output != "external-objects" || report.Final.Ref != report.Result.Outputs["external-objects"] {
				t.Fatalf("final %+v", report.Final)
			}
			record := output[externalObjectsRecord](t, a, "external-objects")
			want := tc.want(f, implemented)
			want.Failure, want.Step, want.Session = report.Failure.Error(), "publish", sessionObserved{ExitConfirmed: true}
			// The reported contract is the last publish attempt's.
			calls := a.publishCalls()
			if ref, ok := report.Result.Outputs["publish"]; ok != (want.Reported != nil) || ok && ref != *a.attempt(calls[len(calls)-1]).Output {
				t.Fatalf("outputs %+v", report.Result.Outputs)
			}
			// A detail ending in ": " stands for any read error after it.
			for _, d := range [][2]*string{{&want.RemoteBranch.Detail, &record.RemoteBranch.Detail}, {&want.PullRequests.Detail, &record.PullRequests.Detail}} {
				if strings.HasSuffix(*d[0], ": ") && strings.HasPrefix(*d[1], *d[0]) {
					*d[0] = *d[1]
				}
			}
			if !reflect.DeepEqual(record, want) {
				t.Fatalf("external objects %+v, want %+v", record, want)
			}
			a.requireSessionsClosed(t, 3)
			requireKept(t, a.run)
			if got := a.decisions(t); len(got) != 2 {
				t.Fatalf("decisions %+v", got)
			}
		})
	}
}

// published1 is the record of workBranch pushed at head with pull request 1.
func published1(f *fixture, head string) externalObjectsRecord {
	branch := workBranch
	return externalObjectsRecord{Worktree: worktreeObserved{Status: "read", Head: head, Branch: &branch}, RemoteBranch: branchObserved{Status: "present", SHA: head},
		PullRequests: pullsObserved{Status: "listed", Items: []pullObservation{pull1(head)}}}
}

// pull1 is pull request 1 from workBranch at head as GitHub shows it.
func pull1(head string) pullObservation {
	return pullObservation{Number: 1, HTMLURL: pullURL(1), State: "open", Draft: new(bool), HeadRef: workBranch, HeadSHA: head}
}

// reported1 is the receipt of workBranch at head with pull request 1, both
// on GitHub.
func reported1(head string) *reportedObserved {
	pr := pull1(head)
	return &reportedObserved{Branch: workBranch, Head: head, PullRequest: reportedPull{URL: pullURL(1), Number: 1},
		RemoteBranch: branchObserved{Status: "present", SHA: head}, ObservedPull: pullObserved{Status: "read", Item: &pr}}
}

// Cancelling while the correction Step runs ends the run Cancelled once the
// session's exit is confirmed. The run can no longer read or record, so
// there is no external objects record; the branch and pull request stay, as
// does the worktree.
func TestPublishCancelled(t *testing.T) {
	f := newFixture(t)
	var implemented string
	a := f.run(t, agents{
		implement: f.implementReady(&implemented),
		publish: func(t *testing.T, c agentCall) string {
			f.publishWork(t, c, "no_change")
			return "hold"
		},
	})
	report := a.report
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	if _, ok := report.Result.Outputs["external-objects"]; ok || len(report.Result.Outputs) != 5 {
		t.Fatalf("outputs %+v", report.Result.Outputs)
	}
	// The cancellation alone, with no failed attempt to read or record.
	if strings.Contains(report.Failure.Error(), "\n") {
		t.Fatalf("failure %v", report.Failure)
	}
	a.requireSessionsClosed(t, 3)
	a.requireAttempts(t, 4)
	requireKept(t, a.run)
	if f.originTip(workBranch) != implemented || f.pulls(t) != 1 {
		t.Fatalf("origin branch %s pulls %d", f.originTip(workBranch), f.pulls(t))
	}
}

// Every failure after the receipt check passed records the external objects
// as the Final with its own error and the verified receipt, and keeps the
// worktree, because the removal is set only on the workflow's successful
// return: the pull request record or its Decision failing, a close of the
// implementer's session the run cannot confirm, and a HEAD that moved after
// the receipt check, found by reading it again after the close.
func TestPublishTailFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		as   func(f *fixture, as *agents)
		// want is the failure; code, when set, is its class.
		want string
		code engine.Code
		// recorded is whether the pull request record is in the outputs.
		recorded, confirmed bool
	}{
		{"record-attach-fails", func(f *fixture, as *agents) {
			// Taking the key makes the record's Attach fail.
			as.before = func(ctx context.Context, r *engine.Run) error {
				_, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "pull-request", Output: contract.Spec{SchemaID: InputSchema},
					Data: inputRecord{Repository: "owner/repo", DefaultBranch: "main", BaseSHA: strings.Repeat("a", 40), Worktree: "/w", DesignDoc: designDocRecord{Path: "p", Resolved: "/p"}}})
				return err
			}
		}, "invalid key or schema", "", false, true},
		{"decision-fails", func(f *fixture, as *agents) {
			as.before = func(ctx context.Context, r *engine.Run) error {
				_, err := r.Root().Child("pull-request-verified")
				return err
			}
		}, "invalid or duplicate scope/decision name", "", true, true},
		{"close-unconfirmed", func(f *fixture, as *agents) {
			as.close = func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
				report, err := s.Close(ctx)
				if role == implementRole {
					report.WaitCompleted = false
				}
				return report, err
			}
		}, "implementer session close not confirmed", engine.CleanupFailed, true, false},
		{"close-times-out", func(f *fixture, as *agents) {
			released := make(chan struct{})
			as.close = func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
				report, err := s.Close(ctx)
				if role == implementRole {
					<-ctx.Done()
					<-released
				}
				return report, err
			}
			as.returned = func(*engine.Run) { close(released) }
		}, "context deadline exceeded", engine.CleanupFailed, true, false},
		{"head-moved", func(f *fixture, as *agents) {
			as.publish = func(t *testing.T, c agentCall) string {
				// The gh helper commits in the worktree while the pull
				// request is read, after the receipt check read the HEAD.
				f.write(filepath.Join(f.dir, "gh-move-head"), c.task.Worktree)
				return f.acceptPublish(t, c)
			}
		}, "code-change receipt mismatch: the worktree HEAD is ", "", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented string
			as := agents{implement: f.implementReady(&implemented)}
			tc.as(f, &as)
			a := f.run(t, as)
			report := a.report
			if report.Outcome != engine.Failed || !strings.Contains(report.Failure.Error(), tc.want) {
				t.Fatalf("outcome %s failure %v, want %q", report.Outcome, report.Failure, tc.want)
			}
			if tc.code != "" {
				var failure *engine.Failure
				handle := a.attempt(a.calls[0]).HandleID
				if !errors.As(report.Failure, &failure) || failure.Code != tc.code || failure.Phase != implementRole || failure.HandleID != handle || failure.Message != tc.want {
					t.Fatalf("failure %+v, want a cleanup failure of handle %s", failure, handle)
				}
			}
			if report.Final == nil || report.Final.Output != "external-objects" {
				t.Fatalf("final %+v", report.Final)
			}
			if _, ok := report.Result.Outputs["pull-request"]; ok != tc.recorded || report.Result.Outputs["publish"] == (contract.Ref{}) {
				t.Fatalf("outputs %+v", report.Result.Outputs)
			}
			record := output[externalObjectsRecord](t, a, "external-objects")
			if record.Failure != report.Failure.Error() || record.Session.ExitConfirmed != tc.confirmed || record.Reported == nil || record.Reported.Head != implemented {
				t.Fatalf("external objects %+v", record)
			}
			if tc.confirmed {
				a.requireSessionsClosed(t, 3)
			}
			requireKept(t, a.run)
		})
	}
}

// One attempt less than the correction Step or the pull request record needs
// stops the run with a run-scope LimitExceeded: before the correction Step is
// dispatched, or after it pushed and opened the pull request, when the record
// cannot be attached. The run is stopped, so there
// is no external objects record; the worktree is kept.
func TestPublishLimits(t *testing.T) {
	for _, tc := range []struct {
		name     string
		attempts int
		pushed   bool
	}{{"before-the-correction", 5, false}, {"before-the-record", 6, true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented string
			a := f.run(t, agents{
				implement: f.implementReady(&implemented),
				policy: func(p *engine.RunPolicy) {
					p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = 3, 3, tc.attempts
				},
			})
			report := a.report
			var failure *engine.Failure
			if report.Outcome != engine.Failed || !errors.As(report.Failure, &failure) || failure.Code != engine.LimitExceeded || failure.LimitScope != "run" || !strings.Contains(failure.Message, "run attempt limit exceeded") {
				t.Fatalf("outcome %s failure %v, want a run LimitExceeded", report.Outcome, report.Failure)
			}
			if _, ok := report.Result.Outputs["external-objects"]; ok || report.Final != nil || strings.Contains(report.Failure.Error(), "\n") {
				t.Fatalf("outputs %+v final %+v failure %v", report.Result.Outputs, report.Final, report.Failure)
			}
			if pushed := f.pulls(t) == 1; pushed != tc.pushed || len(a.publishCalls()) != map[bool]int{false: 0, true: 1}[tc.pushed] {
				t.Fatalf("pull requests %d publisher dispatches %d", f.pulls(t), len(a.publishCalls()))
			}
			a.requireExitsConfirmed(t)
			requireKept(t, a.run)
		})
	}
}
