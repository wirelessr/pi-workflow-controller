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

// reportIDs reads the finding IDs of the deep review report in c's inputs as
// the fixer does.
func reportIDs(t *testing.T, c agentCall) []string {
	t.Helper()
	var envelope struct {
		Data struct {
			Findings []struct {
				ID string `json:"id"`
			} `json:"findings"`
		} `json:"data"`
	}
	if err := protocol.ReadJSON(c.request.Inputs[3].Path, &envelope); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, f := range envelope.Data.Findings {
		ids = append(ids, f.ID)
	}
	return ids
}

func fixData(ids []string, correction, head string) map[string]any {
	dispositions := []map[string]any{}
	for _, id := range ids {
		dispositions = append(dispositions, map[string]any{"id": id, "action": "not_fixed", "reason": "Not worth changing"})
	}
	return map[string]any{"dispositions": dispositions, "notes": []string{}, "correction": correction, "head": head}
}

// fixWork is the fake fixer's work: committed commits in the worktree and
// pushes the HEAD to workBranch; it returns the contract reporting the
// worktree HEAD with every finding of the report not fixed.
func (f *fixture) fixWork(t *testing.T, c agentCall, correction string) map[string]any {
	t.Helper()
	wt := c.task.Worktree
	if correction == "committed" {
		f.commit(t, wt, "fixed after the deep review\n")
		f.gitIn(wt, "push", "-q", "origin", "HEAD:refs/heads/"+workBranch)
	}
	return fixData(reportIDs(t, c), correction, f.gitIn(wt, "rev-parse", "HEAD"))
}

// acceptFix fixes nothing.
func (f *fixture) acceptFix(t *testing.T, c agentCall) string {
	writeCandidate(t, c, f.fixWork(t, c, "no_change"))
	return ""
}

func (a a1Run) fixCalls() []agentCall {
	var out []agentCall
	for _, c := range a.calls {
		if c.request.Output.SchemaID == FixSchema {
			out = append(out, c)
		}
	}
	return out
}

// started is the journal index of each handle's SessionStarting.
func (a a1Run) startedAt(t *testing.T) map[string]int {
	t.Helper()
	out := map[string]int{}
	for i, e := range a.events(t) {
		if e.Kind == "SessionStarting" {
			var d struct {
				HandleID string `json:"handle_id"`
			}
			if err := json.Unmarshal(e.Details, &d); err != nil {
				t.Fatal(err)
			}
			out[d.HandleID] = i
		}
	}
	return out
}

// After the deep review succeeds, the fix Step runs in a fresh session in
// PWC_PI_CWD, opened once every other session is closed, on the report and
// the pull request record. Its receipt is checked once against the worktree
// and GitHub with the pull request identity taken from the record; then the
// workflow records fix-verified, strictly closes the session and re-reads
// the HEAD, and only its successful return sets the removal, so the worktree
// is still there when it returns and the run's cleanup removes it. A commit
// someone else pushed meanwhile, merged by the fixer before its push, keeps
// the verified head an ancestor and passes.
func TestFixSucceeds(t *testing.T) {
	for _, tc := range []struct {
		name, correction string
		// newer is pushed to the branch by someone else after the fixer
		// committed, and merged before the push.
		newer bool
	}{{"committed", "committed", false}, {"no-change", "no_change", false}, {"committed-merging-newer-commits", "committed", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented, newer, pushed string
			var checkoutAtReturn error
			as := agents{
				implement:  f.implementReady(&implemented),
				deepReview: findDeepReview("code-1", "code-2"),
				fix: func(t *testing.T, c agentCall) string {
					wt := c.task.Worktree
					var data map[string]any
					if tc.newer {
						f.commit(t, wt, "fixed after the deep review\n")
						newer = f.setOriginBranch(workBranch, implemented)
						f.gitIn(wt, "pull", "-q", "--no-rebase", "--no-edit", "origin", workBranch)
						f.gitIn(wt, "push", "-q", "origin", "HEAD:refs/heads/"+workBranch)
						data = fixData(reportIDs(t, c), "committed", f.gitIn(wt, "rev-parse", "HEAD"))
					} else {
						data = f.fixWork(t, c, tc.correction)
					}
					data["dispositions"].([]map[string]any)[0]["action"] = "fixed"
					pushed = data["head"].(string)
					writeCandidate(t, c, data)
					return ""
				},
				returned: func(r *engine.Run) { _, checkoutAtReturn = os.Lstat(filepath.Join(r.Dir(), worktreeDir, "checkout")) },
			}
			a := f.run(t, as)
			report := a.report
			if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil {
				t.Fatalf("outcome %s exit %d failure %v", report.Outcome, report.ExitCode, report.Failure)
			}
			outputs := report.Result.Outputs
			if report.Result.Final == nil || *report.Result.Final != (engine.FinalSelection{Output: "fix"}) || report.Final == nil || report.Final.Ref != outputs["fix"] {
				t.Fatalf("final selection %+v delivery %+v", report.Result.Final, report.Final)
			}
			if (pushed == implemented) != (tc.correction == "no_change") {
				t.Fatalf("pushed %s, implemented %s", pushed, implemented)
			}
			calls := a.fixCalls()
			if len(calls) != 1 {
				t.Fatalf("fixer dispatches %d", len(calls))
			}
			c := calls[0]
			attempt := a.attempt(c)
			if attempt.Scope != "root" || attempt.Key != "fix" || *attempt.Output != outputs["fix"] {
				t.Fatalf("fix attempt scope %s key %s output %+v", attempt.Scope, attempt.Key, attempt.Output)
			}
			if role := report.Snapshot.Sessions[attempt.HandleID].Role; role.Name != fixRole || role.Model != implementModel || role.CWD != f.piCWD {
				t.Fatalf("fixer role %+v", role)
			}
			if c.task != (task{Role: "fixer", Requirements: baselineRequirements + "\n\n" + fixRequirements, Worktree: a.worktree()}) {
				t.Fatalf("fix task %+v", c.task)
			}
			if !reflect.DeepEqual(c.request.Inputs, []contract.Ref{outputs["input"], outputs["implement"], outputs["pull-request"], outputs["deep-review-report"]}) || c.request.Feedback != nil || c.request.Output.SchemaID != FixSchema {
				t.Fatalf("fix request inputs %+v feedback %+v schema %s", c.request.Inputs, c.request.Feedback, c.request.Output.SchemaID)
			}
			// A fresh session, opened after every other one closed, and closed
			// by the workflow once, after its Decision.
			closed, finalizing, decided := a.journalIndex(t)
			started := a.startedAt(t)[attempt.HandleID]
			for _, other := range a.calls {
				if h := a.attempt(other).HandleID; h != attempt.HandleID && (len(closed[h]) == 0 || closed[h][0] > started) {
					t.Fatalf("session %s closed at %v, fixer started at %d", h, closed[h], started)
				}
			}
			if at := closed[attempt.HandleID]; len(at) != 1 || at[0] < decided["fix-verified"] || at[0] > finalizing {
				t.Fatalf("fixer session closed at %v, verified at %d, finalizing at %d", at, decided["fix-verified"], finalizing)
			}
			if got := a.decisions(t); !reflect.DeepEqual(got[len(got)-1], decision{Scope: "root", Name: "fix-verified", Refs: []contract.Ref{outputs["fix"]}}) {
				t.Fatalf("last decision %+v", got[len(got)-1])
			}
			// The receipt check's one PR GET, and no compare: the pushed head
			// is the pull request head.
			const get = "api --method GET --hostname github.com -H Accept: application/vnd.github+json "
			gh := f.ghCalls()
			if last := slices.Index(gh, get+"repos/owner/repo/pulls/1/reviews?per_page=100 --paginate --slurp"); last < 0 || !slices.Equal(gh[last+1:], []string{get + "repos/owner/repo/pulls/1"}) {
				t.Fatalf("gh calls %q", gh)
			}
			a.requireExitsConfirmed(t)
			if checkoutAtReturn != nil {
				t.Fatalf("worktree at the workflow's return: %v", checkoutAtReturn)
			}
			requireMissing(t, a.worktree())
			if f.originTip(workBranch) != pushed || f.pulls(t) != 1 {
				t.Fatalf("origin %s, pulls %d", f.originTip(workBranch), f.pulls(t))
			}
			if tc.newer {
				// A merge of the fixer's commit and the newer one.
				if parents := strings.Fields(f.gitIn(f.origin, "rev-list", "--parents", "-n", "1", pushed)); len(parents) != 3 || parents[2] != newer {
					t.Fatalf("pushed %v, want a merge with %s", parents, newer)
				}
			}
		})
	}
}

// A worktree HEAD that moved after the deep review's checks, while only its
// requirements kept the deep review's Agents from the worktree, fails the
// run before the fix session is opened, with the pull request record as the
// Final and no external objects record.
func TestFixHeadMovedBeforeDispatch(t *testing.T) {
	f := newFixture(t)
	var implemented, worktree, moved string
	a := f.run(t, agents{
		implement: func(t *testing.T, c agentCall) string {
			worktree = c.task.Worktree
			return f.implementReady(&implemented)(t, c)
		},
		deepReview: func(t *testing.T, c agentCall) string {
			if c.review.Stage == "validate" {
				moved = f.commit(t, worktree, "moved during the deep review\n")
			}
			return acceptDeepReview(t, c)
		},
	})
	report := a.report
	requireFailure(t, report, "code-change receipt mismatch: the worktree HEAD is "+moved+" before the fix Step; want the verified head "+implemented)
	if !errors.Is(report.Failure, errReceipt) || report.Final == nil || report.Final.Output != "pull-request" || report.Final.Ref != report.Result.Outputs["pull-request"] {
		t.Fatalf("failure %v final %+v", report.Failure, report.Final)
	}
	if _, ok := report.Result.Outputs["external-objects"]; ok || len(a.fixCalls()) != 0 {
		t.Fatalf("outputs %+v fixer dispatches %d", report.Result.Outputs, len(a.fixCalls()))
	}
	for _, s := range report.Snapshot.Sessions {
		if s.Role.Name == fixRole {
			t.Fatalf("fixer session %+v opened", s)
		}
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
}

// A contract the schema or the disposition check rejects is rewritten on the
// same session under the RepairKeepsWork dispatch text, with the diagnostic
// as Feedback: here an ID the report has no finding for, the diagnostic this
// Step words itself; the missing and duplicate ones come from the check the
// correction Step shares (TestCheckDispositions). The fixer pushes once and
// only the final contract's receipt is checked.
func TestFixRepair(t *testing.T) {
	for _, tc := range []struct {
		name       string
		bad        func(data map[string]any)
		diagnostic string
	}{
		// The fix contract reports no branch: the record's is the one.
		{"schema", func(data map[string]any) { data["branch"] = workBranch }, "additional properties 'branch' not allowed"},
		{"unknown-finding", func(data map[string]any) {
			data["dispositions"] = append(slices.Clone(data["dispositions"].([]map[string]any)), map[string]any{"id": "code-9", "action": "fixed", "reason": "r"})
			data["head"] = strings.Repeat("ab", 20)
		}, `dispositions[2]: got id "code-9", which is not a finding of the deep review report in request.inputs; want only its findings`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented string
			var good map[string]any
			a := f.run(t, agents{
				implement:  f.implementReady(&implemented),
				deepReview: findDeepReview("code-1", "code-2"),
				fix: func(t *testing.T, c agentCall) string {
					if good == nil {
						good = f.fixWork(t, c, "committed")
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
			if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "fix" {
				t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
			}
			calls := a.fixCalls()
			if len(calls) != 2 {
				t.Fatalf("fixer dispatches %d", len(calls))
			}
			first, repair := a.attempt(calls[0]), a.attempt(calls[1])
			if repair.HandleID != first.HandleID || repair.Scope != "root/contract-repair-fix" || report.Result.Outputs["fix"] != *repair.Output {
				t.Fatalf("repair handle %s (first %s) scope %s", repair.HandleID, first.HandleID, repair.Scope)
			}
			if fb := calls[1].request.Feedback; fb == nil || !strings.Contains(fb.Message, tc.diagnostic) || len(fb.Refs) != 0 {
				t.Fatalf("repair feedback %+v, want %q", fb, tc.diagnostic)
			}
			if calls[1].task != calls[0].task || !reflect.DeepEqual(calls[1].request.Inputs, calls[0].request.Inputs) {
				t.Fatal("repair task or inputs changed")
			}
			a.requireRepairKeepsWork(t, calls...)
			if got := f.gitIn(f.origin, "rev-list", "--count", implemented+".."+workBranch); got != "1" || f.originTip(workBranch) != good["head"] {
				t.Fatalf("commits pushed after the verified head: %s, want the single commit of the first attempt", got)
			}
		})
	}
}

// A fix receipt the worktree or GitHub does not confirm fails the run once
// the contract was accepted: no repair, and the external objects that exist
// recorded as the Final with the original error, shown in full for the first
// case. The cases are the fix Step's own inputs and what changes between the
// deep review and the fix Step; the checks it shares with the correction
// Step are covered by TestPublishReceiptRejected. The pull request and its
// branch come from the pull request record, and a committed head must keep
// the verified head as an ancestor, so a rewritten history fails even when
// it is the pull request head.
func TestFixReceiptRejected(t *testing.T) {
	type state = receiptState
	for i, tc := range []struct {
		name string
		// fix writes like the Agent, or changes GitHub, and returns the contract.
		fix  func(t *testing.T, f *fixture, c agentCall, s state) map[string]any
		want func(f *fixture, s state) string
	}{
		{"head-not-the-worktree-head", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.fixWork(t, c, "committed")
			data["head"] = s.implemented
			return data
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.implemented + "; want the worktree HEAD " + s.head
		}},
		{"committed-without-a-commit", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.fixWork(t, c, "no_change")
			data["correction"] = "committed"
			return data
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.implemented + " with correction committed, which is the verified head " + s.implemented + " or one of its ancestors; want a new commit"
		}},
		{"no-change-with-a-commit", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			data := f.fixWork(t, c, "committed")
			data["correction"] = "no_change"
			return data
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.head + " with correction no_change; want the verified head " + s.implemented
		}},
		// The reviewed commit amended and force pushed: the new head is the
		// pull request head, but not built on the verified head.
		{"history-rewritten", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			wt := c.task.Worktree
			f.write(filepath.Join(wt, "tracked.txt"), "rewritten after the deep review\n")
			f.gitIn(wt, "commit", "-q", "-a", "--amend", "-m", "rewritten")
			f.gitIn(wt, "push", "-q", "-f", "origin", "HEAD:refs/heads/"+workBranch)
			return fixData(reportIDs(t, c), "committed", f.gitIn(wt, "rev-parse", "HEAD"))
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.head + " with correction committed, which does not have the verified head " + s.implemented + " as an ancestor; want a commit built on it"
		}},
		{"url-changed", patchPull(`{"html_url":"https://github.com/owner/repo/pull/7"}`), func(*fixture, state) string {
			return "receipt mismatch: pull_request: got " + pullURL(1) + " number 1; GitHub shows https://github.com/owner/repo/pull/7 number 1"
		}},
		{"closed", patchPull(`{"state":"closed"}`), func(*fixture, state) string {
			return `receipt mismatch: pull_request: state "closed" merged false draft false; want an open pull request that is not merged and not a draft`
		}},
		{"base-changed", patchPull(`{"base":{"ref":"release"}}`), func(*fixture, state) string {
			return `receipt mismatch: pull_request: base "release"; want the default branch "main"`
		}},
		// Someone else's force push leaves the verified head off the branch.
		{"head-not-an-ancestor", func(t *testing.T, f *fixture, c agentCall, s state) map[string]any {
			f.setOriginBranch(workBranch, f.head)
			return f.fixWork(t, c, "no_change")
		}, func(f *fixture, s state) string {
			return "receipt mismatch: head: got " + s.implemented + ", which is not the pull request head " + f.originTip(workBranch) + " or one of its ancestors (compare status diverged)"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var s state
			var reported string
			a := f.run(t, agents{
				implement: f.implementReady(&s.implemented),
				fix: func(t *testing.T, c agentCall) string {
					data := tc.fix(t, f, c, s)
					reported = data["head"].(string)
					writeCandidate(t, c, data)
					return ""
				},
			})
			report := a.report
			s.head = f.gitIn(a.worktree(), "rev-parse", "HEAD")
			want := tc.want(f, s)
			if report.Outcome != engine.Failed || report.ExitCode != 1 || !errors.Is(report.Failure, errReceipt) || !strings.Contains(report.Failure.Error(), want) {
				t.Fatalf("outcome %s exit %d failure %v, want %q", report.Outcome, report.ExitCode, report.Failure, want)
			}
			if tc.name == "history-rewritten" && f.originTip(workBranch) != s.head {
				t.Fatalf("origin %s, want the rewritten head %s as the pull request head", f.originTip(workBranch), s.head)
			}
			if len(a.fixCalls()) != 1 {
				t.Fatalf("fixer dispatches %d, want no repair", len(a.fixCalls()))
			}
			if report.Final == nil || report.Final.Output != "external-objects" || report.Final.Ref != report.Result.Outputs["external-objects"] || report.Result.Outputs["fix"] != *a.attempt(a.fixCalls()[0]).Output {
				t.Fatalf("final %+v outputs %+v", report.Final, report.Result.Outputs)
			}
			if got := a.decisions(t); slices.ContainsFunc(got, func(d decision) bool { return d.Name == "fix-verified" }) {
				t.Fatalf("decisions %+v", got)
			}
			requireKept(t, a.run)
			// The record shows the head the contract reported on the record's
			// branch.
			if record := output[externalObjectsRecord](t, a, "external-objects"); record.Step != "fix" || record.Reported == nil || record.Reported.Branch != workBranch || record.Reported.Head != reported {
				t.Fatalf("external objects %+v", record)
			}
			if i > 0 {
				return
			}
			a.requireExitsConfirmed(t)
			// The worktree HEAD is the pushed tip here; the contract reported
			// the verified head instead.
			tip := f.originTip(workBranch)
			wantRecord := published1(f, tip)
			wantRecord.Failure, wantRecord.Step, wantRecord.Session = report.Failure.Error(), "fix", sessionObserved{ExitConfirmed: true}
			wantRecord.Reported = reported1(tip)
			wantRecord.Reported.Head = reported
			if record := output[externalObjectsRecord](t, a, "external-objects"); s.head != tip || !reflect.DeepEqual(record, wantRecord) {
				t.Fatalf("external objects %+v, want %+v", record, wantRecord)
			}
		})
	}
}

// A fix Step whose repairs are exhausted after it pushed fails the run with
// its own error, and the Controller records, once the fixer's session is
// closed, the worktree branch and its pull requests on GitHub with the
// receipt of the last contract that passed the schema: the head it reported
// on the record's branch and pull request. Nothing is closed or deleted.
func TestFixFailureRecordsExternalObjects(t *testing.T) {
	f := newFixture(t)
	var implemented string
	a := f.run(t, agents{
		implement: f.implementReady(&implemented),
		fix: func(t *testing.T, c agentCall) string {
			if c.request.Feedback == nil {
				f.fixWork(t, c, "committed")
			}
			writeCandidate(t, c, fixData([]string{"code-9"}, "committed", f.gitIn(c.task.Worktree, "rev-parse", "HEAD")))
			return ""
		},
	})
	report := a.report
	if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Snapshot.Failure == nil || report.Snapshot.Failure.Code != engine.WorkflowFailed {
		t.Fatalf("outcome %s exit %d failure %+v", report.Outcome, report.ExitCode, report.Snapshot.Failure)
	}
	if report.Final == nil || report.Final.Output != "external-objects" || report.Final.Ref != report.Result.Outputs["external-objects"] {
		t.Fatalf("final %+v", report.Final)
	}
	calls := a.fixCalls()
	if len(calls) != 1+contractRepairs || report.Result.Outputs["fix"] != *a.attempt(calls[len(calls)-1]).Output {
		t.Fatalf("fixer dispatches %d outputs %+v", len(calls), report.Result.Outputs)
	}
	head := f.originTip(workBranch)
	want := published1(f, head)
	want.Failure, want.Step, want.Session, want.Reported = report.Failure.Error(), "fix", sessionObserved{ExitConfirmed: true}, reported1(head)
	if record := output[externalObjectsRecord](t, a, "external-objects"); !reflect.DeepEqual(record, want) {
		t.Fatalf("external objects %+v, want %+v", record, want)
	}
	if head == implemented {
		t.Fatal("the fixer did not push")
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
	if f.pulls(t) != 1 {
		t.Fatalf("pull requests %d", f.pulls(t))
	}
}

// Every failure after the fix receipt check passed records the external
// objects as the Final with its own error and keeps the worktree, because
// the removal is set only on the workflow's successful return: the
// fix-verified Decision failing and a close of the fixer's session the run
// cannot confirm. A HEAD moved after the close is the correction Step's
// shared check (TestPublishTailFailure).
func TestFixTailFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		as   func(f *fixture, as *agents)
		want string
		// code, when set, is the failure's class.
		code      engine.Code
		confirmed bool
	}{
		{"decision-fails", func(f *fixture, as *agents) {
			as.before = func(ctx context.Context, r *engine.Run) error {
				_, err := r.Root().Child("fix-verified")
				return err
			}
		}, "invalid or duplicate scope/decision name", "", true},
		{"close-unconfirmed", func(f *fixture, as *agents) {
			as.close = func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
				report, err := s.Close(ctx)
				if role == fixRole {
					report.WaitCompleted = false
				}
				return report, err
			}
		}, "fixer session close not confirmed", engine.CleanupFailed, false},
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
				handle := a.attempt(a.fixCalls()[0]).HandleID
				if !errors.As(report.Failure, &failure) || failure.Code != tc.code || failure.Phase != fixRole || failure.HandleID != handle || failure.Message != tc.want {
					t.Fatalf("failure %+v, want a cleanup failure of handle %s", failure, handle)
				}
			}
			if report.Final == nil || report.Final.Output != "external-objects" {
				t.Fatalf("final %+v", report.Final)
			}
			record := output[externalObjectsRecord](t, a, "external-objects")
			if record.Step != "fix" || record.Failure != report.Failure.Error() || record.Session.ExitConfirmed != tc.confirmed || record.Reported == nil || record.Reported.Head != implemented {
				t.Fatalf("external objects %+v", record)
			}
			if tc.confirmed {
				a.requireExitsConfirmed(t)
			}
			requireKept(t, a.run)
		})
	}
}

// Cancelling while the fix Step runs ends the run Cancelled once every
// session's exit is confirmed, with no Final and no external objects record;
// the pushed branch, the pull request and the worktree stay.
func TestFixCancelled(t *testing.T) {
	f := newFixture(t)
	var implemented, pushed string
	a := f.run(t, agents{
		implement: f.implementReady(&implemented),
		fix: func(t *testing.T, c agentCall) string {
			pushed = f.fixWork(t, c, "committed")["head"].(string)
			return "hold"
		},
	})
	report := a.report
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	if _, ok := report.Result.Outputs["external-objects"]; ok {
		t.Fatalf("outputs %+v", report.Result.Outputs)
	}
	if _, ok := report.Result.Outputs["fix"]; ok || strings.Contains(report.Failure.Error(), "\n") {
		t.Fatalf("outputs %+v failure %v", report.Result.Outputs, report.Failure)
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
	if f.originTip(workBranch) != pushed || f.pulls(t) != 1 {
		t.Fatalf("origin branch %s pulls %d", f.originTip(workBranch), f.pulls(t))
	}
}

// The run succeeds within exactly three live sessions, nine sessions and
// fourteen attempts when the fix Step uses all three of its attempts: input,
// implementer, two pre-PR reviewers, correction, pull-request, the deep
// review's five and the fixer's three. One session less stops the run before
// the fix Step is dispatched, one attempt less at its last repair, with a
// run-scope LimitExceeded, no Final and no external objects record.
func TestFixLimits(t *testing.T) {
	for _, tc := range []struct {
		name               string
		sessions, attempts int
		// dispatched is how many fix attempts ran; want is the failure, "" for success.
		dispatched int
		want       string
	}{
		{"exact", 9, 14, 3, ""},
		{"sessions", 8, 14, 0, "session limit exceeded"},
		{"attempts", 9, 13, 2, "run attempt limit exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented string
			dispatched := 0
			a := f.run(t, agents{
				implement: f.implementReady(&implemented),
				fix: func(t *testing.T, c agentCall) string {
					data := f.fixWork(t, c, "no_change")
					if dispatched++; dispatched < 1+contractRepairs {
						data["dispositions"] = []map[string]any{{"id": fmt.Sprint("code-", dispatched), "action": "fixed", "reason": "r"}}
					}
					writeCandidate(t, c, data)
					return ""
				},
				policy: func(p *engine.RunPolicy) {
					p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = 3, tc.sessions, tc.attempts
				},
			})
			report := a.report
			if len(a.fixCalls()) != tc.dispatched {
				t.Fatalf("fixer dispatches %d, want %d", len(a.fixCalls()), tc.dispatched)
			}
			a.requireExitsConfirmed(t)
			if tc.want == "" {
				if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "fix" {
					t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
				}
				return
			}
			var failure *engine.Failure
			if report.Outcome != engine.Failed || !errors.As(report.Failure, &failure) || failure.Code != engine.LimitExceeded || failure.LimitScope != "run" || !strings.Contains(failure.Message, tc.want) {
				t.Fatalf("outcome %s failure %v, want a run LimitExceeded %q", report.Outcome, report.Failure, tc.want)
			}
			if _, ok := report.Result.Outputs["external-objects"]; ok || report.Final != nil {
				t.Fatalf("outputs %+v final %+v", report.Result.Outputs, report.Final)
			}
			requireKept(t, a.run)
		})
	}
}
