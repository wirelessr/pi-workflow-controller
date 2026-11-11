package codechange

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// roundSnapshot is the round's pull request state snapshot in c's inputs.
func roundSnapshot(t *testing.T, c agentCall) contract.Ref {
	t.Helper()
	for _, ref := range c.request.Inputs {
		if ref.SchemaID == PRStateSchema {
			return ref
		}
	}
	t.Fatalf("no snapshot in %+v", c.request.Inputs)
	return contract.Ref{}
}

// qualityScan is the quality scan item with status and a reason.
func qualityScan(status string) map[string]any {
	return map[string]any{"id": "quality-scan", "status": status, "reason": "The scan's result or why it did not run"}
}

func convergeData(t *testing.T, c agentCall, converged bool, correction, head string) map[string]any {
	data := map[string]any{"snapshot": roundSnapshot(t, c), "converged": converged, "items": []any{qualityScan("passed")}, "correction": correction, "head": head,
		"thread_actions": []any{}, "ci_reruns": []any{}}
	if !converged {
		data["unconverged"] = []string{"A check still fails"}
	}
	return data
}

// convergeWork is the fake converger's round: committed commits in the
// worktree and pushes the HEAD to workBranch; it returns the contract
// reporting the worktree HEAD.
func (f *fixture) convergeWork(t *testing.T, c agentCall, converged bool, correction string) map[string]any {
	t.Helper()
	wt := c.task.Worktree
	if correction == "committed" {
		f.commit(t, wt, fmt.Sprintf("round %d\n", c.task.Round))
		f.gitIn(wt, "push", "-q", "origin", "HEAD:refs/heads/"+workBranch)
	}
	return convergeData(t, c, converged, correction, f.gitIn(wt, "rev-parse", "HEAD"))
}

// acceptConverge claims convergence without a change.
func (f *fixture) acceptConverge(t *testing.T, c agentCall) string {
	writeCandidate(t, c, f.convergeWork(t, c, true, "no_change"))
	return ""
}

func (a a1Run) convergeCalls() []agentCall {
	var out []agentCall
	for _, c := range a.calls {
		if c.request.Output.SchemaID == ConvergeSchema {
			out = append(out, c)
		}
	}
	return out
}

// implementPublished is the implementer, followed by what the seam lets the
// test do for the correction Step: the commit is put on workBranch, pushed,
// and GitHub shows pull request 1 from it.
func (f *fixture) implementPublished(t *testing.T, c agentCall) string {
	head := f.commit(t, c.task.Worktree, "implemented\n")
	f.gitIn(c.task.Worktree, "checkout", "-q", "-b", workBranch)
	f.gitIn(c.task.Worktree, "push", "-q", "origin", "HEAD:refs/heads/"+workBranch)
	f.openPull(t, workBranch)
	writeCandidate(t, c, readyData(c, head))
	return ""
}

// convergeSeam starts the run at the convergence rounds (Q22 of the C4
// map): throughFix, then the same rounds, final snapshot and summary as
// execute's. The seam never sets the worktree's removal; the full-flow tests
// check where execute does.
func (f *fixture) convergeSeam(ctx context.Context, r *engine.Run, in engine.Input, rounds roundPolicy) (engine.Result, error) {
	p, result, err := f.throughFix(ctx, r, in)
	if err != nil {
		return result, err
	}
	return p.finish(ctx, r, p.converge(ctx, r, f.piCWD, result, testState, rounds), testState)
}

// throughFix runs the real preparation with the private skill, the real
// implement Step with implementPublished, a strict close of the
// implementer's session and the real pull-request Attach at its head, for
// the correction Step that does not run, and the real fix Step. The fix
// Step's deep review report is a stand-in, the pull request record, which
// holds no findings: the deep review does not run, and the fixer only reads
// the report's finding IDs.
func (f *fixture) throughFix(ctx context.Context, r *engine.Run, in engine.Input) (prepared, engine.Result, error) {
	p, err := prepare(ctx, r, in, f.piCWD, f.skills, f.fileSource())
	if err != nil {
		return p, engine.Result{}, err
	}
	result, hand, err := implement(ctx, r, p, f.piCWD)
	if err != nil {
		return p, result, err
	}
	if err := closeStrictly(ctx, r, hand.session, hand.sessionID, implementRole, "implementer"); err != nil {
		return p, result, err
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "pull-request", Output: contract.Spec{SchemaID: PullRequestSchema},
		Data: pullRequestRecord{URL: pullURL(1), Number: 1, Branch: workBranch, HeadSHA: hand.head, ReportedHead: hand.head, CompareStatus: "identical"}})
	if err != nil {
		return p, result, err
	}
	result.Outputs["pull-request"], result.Outputs[deepReviewName+"-report"] = ref, ref
	result, err = fix(ctx, r, p, f.piCWD, result)
	return p, result, err
}

// seam runs as from the seam; its rounds get more than the fixture's
// default deadline, which a loaded race run can exceed.
func (f *fixture) seam(as agents) agents {
	as.implement = f.implementPublished
	as.body = f.convergeSeam
	as.deadline = 3 * time.Minute
	return as
}

// roundBehavior is what the fake converger does in one round: claim
// convergence, push a commit, take in the branch's newer commits by a fast
// forward and report them as its commit, or have someone else push to the
// branch.
type roundBehavior struct{ claim, push, adopt, moved bool }

func (f *fixture) behave(rounds ...roundBehavior) func(t *testing.T, c agentCall) string {
	return func(t *testing.T, c agentCall) string {
		b := rounds[c.task.Round-1]
		correction := "no_change"
		if b.push {
			correction = "committed"
		}
		data := f.convergeWork(t, c, b.claim, correction)
		if b.adopt {
			f.gitIn(c.task.Worktree, "pull", "-q", "--ff-only", "origin", workBranch)
			data = convergeData(t, c, b.claim, "committed", f.gitIn(c.task.Worktree, "rev-parse", "HEAD"))
		}
		if b.moved {
			f.setOriginBranch(workBranch, f.originTip(workBranch))
		}
		writeCandidate(t, c, data)
		return ""
	}
}

// requireRetryless checks the run's journal has no Retry event.
func requireRetryless(t *testing.T, a a1Run) {
	t.Helper()
	for _, e := range a.events(t) {
		if strings.HasPrefix(e.Kind, "Retry") {
			t.Fatalf("event %s %s", e.Kind, e.Details)
		}
	}
}

// roundSkill is the SKILL.md the converger of round n is given, expanded
// for that round.
func roundSkill(a a1Run, n int) string {
	return filepath.Join(a.run.Dir(), fmt.Sprintf("%s-%03d", skillsExpansion, n), convergeSkill, "SKILL.md")
}

// A round ends the rounds only when the converger claimed convergence, its
// contract reports no push and the pull request head read after its session
// closed is the head_sha of the round's snapshot, not the head its wait
// last polled; each condition failing alone runs another round. Every round
// is an ordinary Child round-NNN of root, not a Retry, with its own
// snapshot, a fresh session for the converger in PWC_PI_CWD with the
// round's own expansion of the private skill and the comment cleanup in
// its requirements, and its converge-verified Decision; the final snapshot
// and the summary, which is the Final, follow the rounds.
func TestConvergeTermination(t *testing.T) {
	claim, open := roundBehavior{claim: true}, roundBehavior{}
	for _, tc := range []struct {
		name   string
		rounds []roundBehavior
		// polledBehind moves the branch during the first wait's only poll, so
		// the first snapshot's head_sha is newer than its last polled head.
		polledBehind bool
	}{
		{"converged-in-round-1", []roundBehavior{claim}, false},
		{"claim-with-a-push", []roundBehavior{{claim: true, push: true}, claim}, false},
		// The commit the round reports is the head of its snapshot, which
		// someone else pushed during the first round: the head is unchanged,
		// the receipt still ends the claim.
		{"claim-with-a-push-on-an-unchanged-head", []roundBehavior{{moved: true}, {claim: true, adopt: true}, claim}, false},
		{"claim-on-a-moved-head", []roundBehavior{{claim: true, moved: true}, claim}, false},
		{"no-claim", []roundBehavior{open, claim}, false},
		{"converged-in-round-3", []roundBehavior{{push: true}, open, claim}, false},
		{"claim-on-the-snapshot-head-past-the-polled-head", []roundBehavior{claim}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			if tc.polledBehind {
				f.write(filepath.Join(f.dir, "gh-move-on-checks"), "1")
			}
			a := f.run(t, f.seam(agents{converge: f.behave(tc.rounds...)}))
			report := a.report
			n := len(tc.rounds)
			final := roundName(n) + "-converge"
			outputs := report.Result.Outputs
			if report.Outcome != engine.Succeeded || report.Failure != nil || report.Final == nil || report.Final.Output != "summary" || report.Final.Ref != outputs["summary"] {
				t.Fatalf("outcome %s failure %v final %+v, want the summary", report.Outcome, report.Failure, report.Final)
			}
			if s := output[summaryRecord](t, a, "summary"); s.RoundsEnd != "converged" || s.Claim == nil || s.Claim.Output != final || len(s.Rounds) != n {
				t.Fatalf("summary %+v", s)
			}
			if s := output[prStateRecord](t, a, "round-001-snapshot"); tc.polledBehind == (s.Wait.LastPolledHead == s.PullRequest.HeadSHA) {
				t.Fatalf("first snapshot head_sha %s, last polled head %s", s.PullRequest.HeadSHA, s.Wait.LastPolledHead)
			}
			calls := a.convergeCalls()
			if len(calls) != n {
				t.Fatalf("converge dispatches %d, want %d", len(calls), n)
			}
			closed, _, _ := a.journalIndex(t)
			started := a.startedAt(t)
			var want []decision
			previous := ""
			for i, c := range calls {
				k := i + 1
				name := roundName(k)
				attempt := a.attempt(c)
				if attempt.Scope != "root/"+name || attempt.Key != "converge" || *attempt.Output != outputs[name+"-converge"] {
					t.Fatalf("round %d attempt scope %s key %s", k, attempt.Scope, attempt.Key)
				}
				if role := report.Snapshot.Sessions[attempt.HandleID].Role; role.Name != convergeRole || role.Model != convergeModel || role.CWD != f.piCWD {
					t.Fatalf("converger role %+v", role)
				}
				// Fresh: opened after the previous session closed, closed once.
				if at := closed[attempt.HandleID]; len(at) != 1 || (previous != "" && closed[previous][0] > started[attempt.HandleID]) {
					t.Fatalf("round %d session closed at %v, started at %d", k, at, started[attempt.HandleID])
				}
				previous = attempt.HandleID
				if !reflect.DeepEqual(c.task, task{Role: "converger", Requirements: baselineRequirements + "\n\n" + convergeRequirements, Worktree: a.worktree(), Skills: []string{roundSkill(a, k)}, Round: k}) {
					t.Fatalf("round %d task %+v", k, c.task)
				}
				previousReport := "fix"
				if k > 1 {
					previousReport = roundName(k-1) + "-converge"
				}
				if !reflect.DeepEqual(c.request.Inputs, []contract.Ref{outputs["input"], outputs["implement"], outputs["pull-request"], outputs[name+"-snapshot"], outputs[previousReport]}) || c.request.Feedback != nil {
					t.Fatalf("round %d inputs %+v feedback %+v", k, c.request.Inputs, c.request.Feedback)
				}
				want = append(want, decision{Scope: "root/" + name, Name: "converge-verified", Refs: []contract.Ref{outputs[name+"-converge"]}})
			}
			if !strings.Contains(calls[0].task.Requirements, commentCleanup) {
				t.Fatalf("requirements without the comment cleanup: %s", calls[0].task.Requirements)
			}
			want = append(want, decision{Scope: "root", Name: "converged", Refs: []contract.Ref{outputs[final], outputs[roundName(n)+"-snapshot"]}})
			got := a.decisions(t)
			if fixed := slices.IndexFunc(got, func(d decision) bool { return d.Name == "fix-verified" }); fixed < 0 || !reflect.DeepEqual(got[fixed+1:], want) {
				t.Fatalf("decisions %+v, want %+v after fix-verified", got, want)
			}
			for _, key := range []string{"snapshot", "converge", "external-objects"} {
				if _, ok := outputs[roundName(n+1)+"-"+key]; ok {
					t.Fatalf("outputs %+v past round %d", outputs, n)
				}
			}
			if _, ok := outputs["final-snapshot"]; !ok {
				t.Fatalf("outputs %+v", outputs)
			}
			requireRetryless(t, a)
			a.requireExitsConfirmed(t)
		})
	}
}

// Admission is the run's creation plus its RunTimeout, a wall-clock
// estimate of the run deadline, less now, against the reserve; without a
// run deadline every round is admitted.
func TestConvergeAdmitted(t *testing.T) {
	created := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	deadline := created.Add(8 * time.Hour)
	for _, tc := range []struct {
		name    string
		now     time.Time
		disable bool
		want    bool
	}{
		{"at-creation", created, false, true},
		{"reserve-left", deadline.Add(-160 * time.Minute), false, true},
		{"less-than-reserve", deadline.Add(-160*time.Minute + time.Nanosecond), false, false},
		{"past-the-deadline", deadline.Add(time.Hour), false, false},
		{"no-run-deadline", deadline.Add(time.Hour), true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := engine.Snapshot{CreatedAt: created, Policy: engine.RunPolicy{RunTimeout: 8 * time.Hour, DisableRunTimeout: tc.disable}}
			if got := admitted(s, tc.now, 160*time.Minute); got != tc.want {
				t.Fatalf("admitted at %s = %t", tc.now, got)
			}
		})
	}
}

// The rounds end unconverged at the round cap, here 2, with
// rounds-exhausted, or once a round is not admitted, the run deadline
// leaving less than the reserve at the time the round policy's clock gives,
// with rounds-not-admitted. The Decision cites the last accepted report,
// the fix contract when no round was admitted, and the last snapshot; the
// final snapshot is still taken, the summary is the Final and the run fails
// with errNotConverged. A run without a deadline admits rounds the clock
// puts past it.
func TestConvergeRoundsEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		cap  int
		// late is the 1-based clock read from which the clock is past the
		// reserve; 0 never.
		late    int
		disable bool
		rounds  []roundBehavior
		// previous is the last accepted report.
		previous, kind string
		refs           []string
		want           string
	}{
		{"round-cap", 2, 0, false, []roundBehavior{{push: true}, {}}, "round-002-converge", "rounds-exhausted", []string{"round-002-converge", "round-002-snapshot"}, " within the round cap of 2"},
		{"no-round-admitted", 10, 1, false, nil, "fix", "rounds-not-admitted", []string{"fix"}, ": the run deadline leaves less than 2h30m0s for round 1"},
		{"second-round-not-admitted", 10, 2, false, []roundBehavior{{push: true}}, "round-001-converge", "rounds-not-admitted", []string{"round-001-converge", "round-001-snapshot"}, ": the run deadline leaves less than 2h30m0s for round 2"},
		{"no-run-deadline", 10, 1, true, []roundBehavior{{}, {claim: true}}, "round-002-converge", "converged", []string{"round-002-converge", "round-002-snapshot"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			reads := 0
			as := f.seam(agents{
				converge: f.behave(tc.rounds...),
				rounds: func(r *engine.Run) roundPolicy {
					s := r.Snapshot()
					past := s.CreatedAt.Add(s.Policy.RunTimeout - convergeRounds.reserve + time.Second)
					return roundPolicy{rounds: tc.cap, reserve: convergeRounds.reserve, now: func() time.Time {
						if reads++; tc.late != 0 && reads >= tc.late {
							return past
						}
						return s.CreatedAt
					}}
				},
				policy: func(p *engine.RunPolicy) { p.DisableRunTimeout = tc.disable },
			})
			a := f.run(t, as)
			report := a.report
			outputs := report.Result.Outputs
			if report.Final == nil || report.Final.Output != "summary" || report.Final.Ref != outputs["summary"] || len(a.convergeCalls()) != len(tc.rounds) {
				t.Fatalf("final %+v converge dispatches %d failure %v", report.Final, len(a.convergeCalls()), report.Failure)
			}
			s := output[summaryRecord](t, a, "summary")
			if s.RoundsEnd != tc.kind || (s.Claim == nil) != (tc.previous == "fix") || s.FinalSnapshot.Status != "recorded" || len(s.Rounds) != len(tc.rounds) {
				t.Fatalf("summary %+v", s)
			}
			for k, round := range s.Rounds {
				if round.Report == nil || *round.Report != outputs[fmt.Sprintf("round-%03d-converge", k+1)] {
					t.Fatalf("summary round %d %+v", k+1, round)
				}
			}
			if last := s.Reports[len(s.Reports)-1]; last.Output != tc.previous || last.Ref != outputs[tc.previous] || last.Head != s.FinalHead || len(s.Reports) != map[bool]int{false: 4, true: 3}[tc.previous == "fix"] {
				t.Fatalf("summary reports %+v, want %s last", s.Reports, tc.previous)
			}
			var refs []contract.Ref
			for _, key := range tc.refs {
				refs = append(refs, outputs[key])
			}
			last := []decision{{Scope: "root", Name: tc.kind, Refs: refs}}
			if got := a.decisions(t); !reflect.DeepEqual(got[len(got)-len(last):], last) {
				t.Fatalf("last decisions %+v", got[len(got)-len(last):])
			}
			if tc.want == "" {
				if report.Outcome != engine.Succeeded {
					t.Fatalf("outcome %s failure %v", report.Outcome, report.Failure)
				}
				a.requireExitsConfirmed(t)
				return
			}
			want := "code-change: the pull request did not converge" + tc.want
			if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Failure.Error() != want || s.Failure != want || !errors.Is(report.Failure, errNotConverged) || report.Snapshot.Failure.Code != engine.WorkflowFailed {
				t.Fatalf("outcome %s exit %d failure %v, want %q", report.Outcome, report.ExitCode, report.Failure, want)
			}
			if _, ok := outputs[fmt.Sprintf("round-%03d-snapshot", len(tc.rounds)+1)]; ok {
				t.Fatalf("outputs %+v", outputs)
			}
			a.requireExitsConfirmed(t)
			requireKept(t, a.run)
		})
	}
}

// A round's receipt that the worktree or GitHub does not confirm ends the
// rounds: the round's external objects record, step converge, attached in
// the round's scope and named after it, is the Final with the original
// error, not repaired, and no other round, final snapshot or summary
// follows. The receipt check is the fix Step's, which
// TestFixReceiptRejected and TestPublishReceiptRejected cover, so only a
// rewritten history, which a round must not push either, and a reported
// head that is not the worktree HEAD in a later round are here; the other
// failures after a round's dispatch are in TestSummaryAfterRoundFailure.
func TestConvergeRoundFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		round int
		// act is the failing round's converger.
		act  func(t *testing.T, f *fixture, c agentCall) map[string]any
		want string
	}{
		{"history-rewritten", 1, func(t *testing.T, f *fixture, c agentCall) map[string]any {
			wt := c.task.Worktree
			f.write(filepath.Join(wt, "tracked.txt"), "rewritten in a round\n")
			f.gitIn(wt, "commit", "-q", "-a", "--amend", "-m", "rewritten")
			f.gitIn(wt, "push", "-q", "-f", "origin", "HEAD:refs/heads/"+workBranch)
			return convergeData(t, c, false, "committed", f.gitIn(wt, "rev-parse", "HEAD"))
		}, "as an ancestor; want a commit built on it"},
		{"head-not-the-worktree-head", 2, func(t *testing.T, f *fixture, c agentCall) map[string]any {
			data := f.convergeWork(t, c, true, "committed")
			data["head"] = f.gitIn(c.task.Worktree, "rev-parse", "HEAD~1")
			return data
		}, "code-change receipt mismatch: head: got "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
				if c.task.Round < tc.round {
					writeCandidate(t, c, f.convergeWork(t, c, false, "no_change"))
					return ""
				}
				writeCandidate(t, c, tc.act(t, f, c))
				return ""
			}}))
			report := a.report
			name := roundName(tc.round)
			outputs := report.Result.Outputs
			if report.Outcome != engine.Failed || !strings.Contains(report.Failure.Error(), tc.want) || report.Final == nil || report.Final.Output != name+"-external-objects" || report.Final.Ref != outputs[name+"-external-objects"] {
				t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
			}
			record := output[externalObjectsRecord](t, a, name+"-external-objects")
			if !errors.Is(report.Failure, errReceipt) || len(a.convergeCalls()) != tc.round {
				t.Fatalf("failure %v converge dispatches %d, want no repair", report.Failure, len(a.convergeCalls()))
			}
			last := a.convergeCalls()[len(a.convergeCalls())-1]
			if record.Step != "converge" || record.Failure != report.Failure.Error() || !record.Session.ExitConfirmed || record.Reported == nil || record.Reported.Branch != workBranch {
				t.Fatalf("external objects %+v, failure %v", record, report.Failure)
			}
			if at := report.Snapshot.Attempts[outputs[name+"-external-objects"].AttemptID]; at.Scope != "root/"+name || at.Key != "external-objects" || outputs[name+"-converge"] != *a.attempt(last).Output {
				t.Fatalf("external objects attempt %+v", at)
			}
			for _, key := range []string{"final-snapshot", "summary", "external-objects", roundName(tc.round+1) + "-snapshot"} {
				if _, ok := outputs[key]; ok {
					t.Fatalf("outputs %+v", outputs)
				}
			}
			a.requireExitsConfirmed(t)
		})
	}
}

// A round that stops before its dispatch ends the rounds without opening
// the round's session, so no external objects record follows: a round
// snapshot showing the pull request merged or closed, errPullRequestNotOpen;
// a round snapshot that cannot be read, errStateRead, recording no snapshot;
// the round's skill expansion colliding with a directory that already
// exists, errSkillsExpansion; and a worktree HEAD that moved between rounds,
// errReceipt against the head the previous round pushed. The receipt
// mismatch ends the run with the last accepted report as the Final; the
// others are followed by the final snapshot and the summary, which is the
// Final. A final snapshot that cannot be read either adds its error to the
// round's, which keeps its classification.
func TestConvergeStopsBeforeDispatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		// round is the round that stops; the earlier ones push. previous is
		// the last accepted report.
		round    int
		previous string
		// setup prepares the fake GitHub before the run, act the first
		// round's converger after its push.
		setup    func(f *fixture)
		act      func(f *fixture, c agentCall)
		sentinel error
		want     func(f *fixture, a a1Run) string
		snapshot bool
		// kind is the summary's rounds_end, and none is set when there is
		// no summary.
		kind string
		none bool
	}{
		{"merged-before-round-1", 1, "fix", func(f *fixture) {
			f.write(filepath.Join(f.dir, "pr-patch.json"), `{"state":"closed","merged":true}`)
			f.write(filepath.Join(f.dir, "pr-patch-after-checks"), "1")
		}, nil, errPullRequestNotOpen, func(*fixture, a1Run) string {
			return `code-change: the pull request is no longer open: the round-001 snapshot shows state "closed" merged true`
		}, true, "pull_request_not_open", false},
		// The first round's wait and snapshot read the check runs twice; the
		// second round's wait reads them next.
		{"closed-before-round-2", 2, "round-001-converge", func(f *fixture) {
			f.write(filepath.Join(f.dir, "pr-patch.json"), `{"state":"closed"}`)
			f.write(filepath.Join(f.dir, "pr-patch-after-checks"), "3")
		}, nil, errPullRequestNotOpen, func(*fixture, a1Run) string {
			return `code-change: the pull request is no longer open: the round-002 snapshot shows state "closed" merged false`
		}, true, "pull_request_not_open", false},
		// Every review threads query fails, the final snapshot's too.
		{"snapshot-unreadable", 1, "fix", func(f *fixture) { f.write(filepath.Join(f.dir, "gh-fail-threads"), "1") }, nil, errStateRead, func(f *fixture, a a1Run) string {
			return commandFailure(a.report, "code-change cannot read the pull request state: review threads query: ")
		}, false, "state_read_failed", false},
		{"skills-expansion-collides", 2, "round-001-converge", nil, func(f *fixture, c agentCall) {
			if err := os.Mkdir(filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(c.task.Skills[0]))), skillsExpansion+"-002"), 0700); err != nil {
				f.t.Error(err)
			}
		}, errSkillsExpansion, func(*fixture, a1Run) string {
			return "code-change cannot expand the private skill for round 2: reserve code change skills 002: "
		}, true, "round_failed", false},
		{"head-moved-before-round-2", 2, "round-001-converge", nil, func(f *fixture, c agentCall) {
			f.write(filepath.Join(f.dir, "gh-move-head-at-checks"), "3 "+c.task.Worktree)
		}, errReceipt, func(f *fixture, a a1Run) string {
			return "code-change receipt mismatch: the worktree HEAD is " + f.gitIn(a.worktree(), "rev-parse", "HEAD") + " before round-002; want the verified head " + f.originTip(workBranch)
		}, true, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			if tc.setup != nil {
				tc.setup(f)
			}
			a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
				data := f.convergeWork(t, c, false, "committed")
				if tc.act != nil {
					tc.act(f, c)
				}
				writeCandidate(t, c, data)
				return ""
			}}))
			report := a.report
			outputs := report.Result.Outputs
			name := roundName(tc.round)
			previous := tc.previous
			requireFailure(t, report, tc.want(f, a))
			if !errors.Is(report.Failure, tc.sentinel) || len(a.convergeCalls()) != tc.round-1 {
				t.Fatalf("failure %v converge dispatches %d", report.Failure, len(a.convergeCalls()))
			}
			sessions := 0
			for _, s := range report.Snapshot.Sessions {
				if s.Role.Name == convergeRole {
					sessions++
				}
			}
			if sessions != tc.round-1 {
				t.Fatalf("converger sessions %d, want %d", sessions, tc.round-1)
			}
			if _, ok := outputs[name+"-snapshot"]; ok != tc.snapshot {
				t.Fatalf("outputs %+v", outputs)
			}
			if _, ok := outputs[name+"-external-objects"]; ok {
				t.Fatalf("outputs %+v", outputs)
			}
			if tc.none {
				if report.Final == nil || report.Final.Output != previous || report.Final.Ref != outputs[previous] {
					t.Fatalf("final %+v", report.Final)
				}
				for _, key := range []string{"final-snapshot", "summary"} {
					if _, ok := outputs[key]; ok {
						t.Fatalf("outputs %+v", outputs)
					}
				}
				a.requireExitsConfirmed(t)
				return
			}
			if report.Final == nil || report.Final.Output != "summary" || report.Final.Ref != outputs["summary"] {
				t.Fatalf("final %+v", report.Final)
			}
			s := output[summaryRecord](t, a, "summary")
			if s.RoundsEnd != tc.kind || s.Failure != report.Failure.Error() || len(s.Rounds) != tc.round || s.Reports[len(s.Reports)-1].Output != previous {
				t.Fatalf("summary %+v", s)
			}
			// The final snapshot reads the threads too, which fail in one case.
			if _, ok := outputs["final-snapshot"]; ok != tc.snapshot || (s.FinalSnapshot.Status == "recorded") != tc.snapshot ||
				strings.Contains(report.Failure.Error(), "; the final snapshot: "+s.FinalSnapshot.Detail) == tc.snapshot {
				t.Fatalf("outputs %+v final snapshot %+v failure %v", outputs, s.FinalSnapshot, report.Failure)
			}
			a.requireExitsConfirmed(t)
		})
	}
}

// The read of the pull request after a round's session closed and its
// receipt was verified ends the rounds, with no external objects record or
// further round, even after a claim of convergence on an unchanged head,
// when it fails, with errStateRead; or when it shows the pull request
// closed, like a round's snapshot does, with errPullRequestNotOpen. The
// summary is the Final, the round's report the last accepted one.
func TestConvergeReadAfterRoundFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		// after acts once the round's receipt was verified, when its
		// converger's session closes.
		after    func(f *fixture)
		want     string
		sentinel error
		kind     string
	}{
		{"read-fails", func(f *fixture) {
			f.write(filepath.Join(f.dir, "gh-fail-pull-at"), fmt.Sprint(pullReads(f.dir)+1))
		}, "after round-001: code-change cannot read the pull request state: pull request GET: ", errStateRead, "state_read_failed"},
		{"pull-request-closed", func(f *fixture) {
			f.write(filepath.Join(f.dir, "pr-patch.json"), `{"state":"closed"}`)
		}, `code-change: the pull request is no longer open: the read after round-001 shows state "closed" merged false`, errPullRequestNotOpen, "pull_request_not_open"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			a := f.run(t, f.seam(agents{close: func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
				if role == convergeRole {
					tc.after(f)
				}
				return s.Close(ctx)
			}}))
			report := a.report
			outputs := report.Result.Outputs
			want := tc.want
			if tc.sentinel == errStateRead {
				want = commandFailure(report, want)
			}
			requireFailure(t, report, want)
			if !errors.Is(report.Failure, tc.sentinel) || report.Final == nil || report.Final.Output != "summary" || len(a.convergeCalls()) != 1 {
				t.Fatalf("failure %v final %+v converge dispatches %d", report.Failure, report.Final, len(a.convergeCalls()))
			}
			for _, key := range []string{"round-001-external-objects", "round-002-snapshot"} {
				if _, ok := outputs[key]; ok {
					t.Fatalf("outputs %+v", outputs)
				}
			}
			if got := a.decisions(t); got[len(got)-1].Name == "converged" {
				t.Fatalf("decisions %+v", got)
			}
			if s := output[summaryRecord](t, a, "summary"); s.RoundsEnd != tc.kind || s.Claim == nil || s.Claim.Output != "round-001-converge" || s.Reports[len(s.Reports)-1].Output != "round-001-converge" {
				t.Fatalf("summary %+v", s)
			}
			a.requireExitsConfirmed(t)
		})
	}
}

// threadNodeJSON is a review thread of pull request number in repository as the
// review thread query shows it: of its total comments, comments are the
// last ones.
func threadNodeJSON(number int, repository string, resolved bool, total int, comments ...string) map[string]any {
	nodes := []any{}
	for _, c := range comments {
		nodes = append(nodes, map[string]any{"id": c})
	}
	return map[string]any{"__typename": "PullRequestReviewThread", "isResolved": resolved,
		"pullRequest": map[string]any{"number": number, "repository": map[string]any{"nameWithOwner": repository}},
		"comments":    map[string]any{"totalCount": total, "nodes": nodes}}
}

// runJSON is a GitHub Actions run from branch of headRepository as its GET
// shows it.
func runJSON(headRepository, branch string, attempt int) map[string]any {
	return map[string]any{"head_repository": map[string]any{"full_name": headRepository}, "head_branch": branch, "run_attempt": attempt,
		"head_sha": strings.Repeat("a", 40), "status": "completed", "conclusion": "failure"}
}

// setThread and setRun set a review thread and an Actions run as GitHub
// shows them.
func (f *fixture) setThread(t *testing.T, id string, v any) {
	f.writeJSON(t, "thread-"+url.PathEscape(id)+".json", v)
}

func (f *fixture) setRun(t *testing.T, id int64, v any) {
	f.writeJSON(t, fmt.Sprintf("run-%d.json", id), v)
}

// receiptReads is how many review thread queries and Actions run GETs the
// gh calls hold.
func (f *fixture) receiptReads() [2]int {
	var n [2]int
	for _, line := range f.ghCalls() {
		if strings.Contains(line, "query=<review thread> ") {
			n[0]++
		}
		if strings.Contains(line, "repos/owner/repo/actions/runs/") {
			n[1]++
		}
	}
	return n
}

// The thread actions and reruns of a round's accepted contract are checked
// once with read-only reads, in contract order, each thread read once
// however many actions name it: a thread must be a review thread of the
// record's pull request in the target repository, a reply's comment among
// its last 100 comments and a resolved thread resolved; a run must be a
// GitHub Actions run from the record's branch of the target repository past
// its first attempt. A mismatch is errReceipt; a failed or unreadable read,
// and a reply that is not among the last 100 comments of a thread with more,
// errUnverifiable. The first failure ends the check.
func TestConvergeWritesChecked(t *testing.T) {
	reply := func(thread, comment string) threadAction {
		return threadAction{ThreadID: thread, Action: "reply", CommentID: comment}
	}
	resolve := func(thread string) threadAction { return threadAction{ThreadID: thread, Action: "resolve"} }
	thread := func(resolved bool, comments ...string) map[string]any {
		return threadNodeJSON(1, "owner/repo", resolved, len(comments), comments...)
	}
	var last100 []string
	for i := range 100 {
		last100 = append(last100, fmt.Sprintf("PRRC_%d", i))
	}
	for _, tc := range []struct {
		name    string
		setup   func(t *testing.T, f *fixture)
		threads []threadAction
		reruns  []int64
		// sentinel is nil when the receipts are confirmed.
		sentinel error
		want     string
		// reads, when set, is the thread queries and run GETs made.
		reads *[2]int
	}{
		{"confirmed", func(t *testing.T, f *fixture) {
			f.setThread(t, "PRRT_a", threadNodeJSON(1, "Owner/Repo", true, 150, append(last100[1:], "PRRC_reply")...))
			f.setThread(t, "PRRT_b+/=", thread(true, "PRRC_b"))
			f.setRun(t, 41, runJSON("owner/repo", workBranch, 3))
			f.setRun(t, 42, runJSON("Owner/Repo", workBranch, 2))
		}, []threadAction{reply("PRRT_a", "PRRC_reply"), resolve("PRRT_b+/="), resolve("PRRT_a")}, []int64{41, 42}, nil, "", &[2]int{2, 2}},
		{"thread-unknown", func(t *testing.T, f *fixture) { f.setRun(t, 42, runJSON("owner/repo", workBranch, 2)) },
			[]threadAction{resolve("PRRT_a")}, []int64{42}, errUnverifiable, "thread_actions[0]: review thread query: acquisition ", &[2]int{1, 0}},
		{"thread-no-node", func(t *testing.T, f *fixture) {
			f.write(filepath.Join(f.dir, "gh-body-thread"), `[{"data":{"node":null}}]`)
		},
			[]threadAction{resolve("PRRT_a")}, nil, errReceipt, `thread_actions[0].thread_id: got "PRRT_a", which GitHub resolves to no node; want a review thread of pull request 1`, nil},
		{"not-a-thread", func(t *testing.T, f *fixture) {
			f.setThread(t, "PRRC_a", map[string]any{"__typename": "PullRequestReviewComment"})
		},
			[]threadAction{resolve("PRRC_a")}, nil, errReceipt, `got "PRRC_a", which GitHub resolves to a PullRequestReviewComment; want a review thread of pull request 1`, nil},
		{"thread-of-another-pull-request", func(t *testing.T, f *fixture) { f.setThread(t, "PRRT_a", threadNodeJSON(2, "owner/repo", true, 0)) },
			[]threadAction{resolve("PRRT_a")}, nil, errReceipt, `got "PRRT_a", a review thread of pull request 2 in "owner/repo"; want one of pull request 1 in owner/repo`, nil},
		{"thread-of-another-repository", func(t *testing.T, f *fixture) { f.setThread(t, "PRRT_a", threadNodeJSON(1, "other/repo", true, 0)) },
			[]threadAction{resolve("PRRT_a")}, nil, errReceipt, `a review thread of pull request 1 in "other/repo"; want one of pull request 1 in owner/repo`, nil},
		{"reply-not-in-thread", func(t *testing.T, f *fixture) { f.setThread(t, "PRRT_a", thread(false, "PRRC_1")) },
			[]threadAction{reply("PRRT_a", "PRRC_9")}, nil, errReceipt, `thread_actions[0].comment_id: got "PRRC_9", which is not a comment of thread "PRRT_a"`, nil},
		{"reply-before-the-last-100", func(t *testing.T, f *fixture) {
			f.setThread(t, "PRRT_a", threadNodeJSON(1, "owner/repo", false, 150, last100...))
		},
			[]threadAction{reply("PRRT_a", "PRRC_9x")}, nil, errUnverifiable, `got "PRRC_9x", which is not among the last 100 of the 150 comments of thread "PRRT_a", the only ones the Controller reads`, nil},
		{"resolve-not-applied", func(t *testing.T, f *fixture) { f.setThread(t, "PRRT_a", thread(false, "PRRC_1")) },
			[]threadAction{reply("PRRT_a", "PRRC_1"), resolve("PRRT_a")}, nil, errReceipt, `thread_actions[1]: got resolve, but GitHub shows thread "PRRT_a" not resolved`, nil},
		{"thread-query-errors", func(t *testing.T, f *fixture) {
			f.write(filepath.Join(f.dir, "gh-body-thread"), `[{"data":{"node":null},"errors":[{"type":"NOT_FOUND"}]}]`)
		}, []threadAction{resolve("PRRT_a")}, nil, errUnverifiable, "thread_actions[0]: review thread query: the page has errors or no data", nil},
		{"thread-pages", func(t *testing.T, f *fixture) {
			page, _ := json.Marshal(map[string]any{"data": map[string]any{"node": thread(true)}})
			f.write(filepath.Join(f.dir, "gh-body-thread"), "["+string(page)+","+string(page)+"]")
		}, []threadAction{resolve("PRRT_a")}, nil, errUnverifiable, "thread_actions[0]: review thread query: not a single page object", nil},
		{"thread-unreadable", func(t *testing.T, f *fixture) {
			v := thread(true)
			delete(v, "comments")
			f.setThread(t, "PRRT_a", v)
		}, []threadAction{resolve("PRRT_a")}, nil, errUnverifiable, "thread_actions[0]: review thread query: a thread without isResolved", nil},
		{"rerun-not-found", func(*testing.T, *fixture) {}, nil, []int64{42}, errUnverifiable, "ci_reruns[0]: workflow run GET: acquisition ", nil},
		{"rerun-without-head-repository", func(t *testing.T, f *fixture) {
			v := runJSON("owner/repo", workBranch, 2)
			v["head_repository"] = nil
			f.setRun(t, 42, v)
		}, nil, []int64{42}, errUnverifiable, "ci_reruns[0]: workflow run GET: not a run with head_branch, run_attempt and head_repository", nil},
		{"rerun-from-another-head-repository", func(t *testing.T, f *fixture) { f.setRun(t, 42, runJSON("fork/repo", workBranch, 2)) }, nil, []int64{42}, errReceipt,
			`ci_reruns[0].run_id: got 42: head_repository "fork/repo"; want owner/repo`, nil},
		{"rerun-on-another-branch", func(t *testing.T, f *fixture) { f.setRun(t, 42, runJSON("owner/repo", "main", 2)) }, nil, []int64{42}, errReceipt,
			`ci_reruns[0].run_id: got 42: head_branch "main"; want the record's branch "ABC-1-change"`, nil},
		{"second-rerun-first-attempt", func(t *testing.T, f *fixture) {
			f.setRun(t, 41, runJSON("owner/repo", workBranch, 2))
			f.setRun(t, 42, runJSON("owner/repo", workBranch, 1))
		}, nil, []int64{41, 42}, errReceipt, "ci_reruns[1].run_id: got 42: run_attempt 1; want a rerun, attempt 2 or later", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			tc.setup(t, f)
			v := convergeReport{ThreadActions: tc.threads}
			for _, id := range tc.reruns {
				v.CIReruns = append(v.CIReruns, ciRerun{RunID: id})
			}
			err := prepared{repository: "owner/repo"}.verifyWrites(context.Background(), f.dir, pullRequestRecord{Number: 1, Branch: workBranch}, v)
			if tc.sentinel == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, tc.sentinel) || (tc.sentinel == errReceipt) == errors.Is(err, errUnverifiable) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want %v with %q", err, tc.sentinel, tc.want)
			}
			if got := f.receiptReads(); tc.reads != nil && got != *tc.reads {
				t.Fatalf("thread queries and run GETs %v, want %v", got, *tc.reads)
			}
		})
	}
}

// A round that claims convergence without a push but reports a thread reply
// and resolution, or a rerun, does not end the rounds: round 1 reports the
// reply and resolution of one thread, read once, round 2 a rerun, and round
// 3, which writes nothing, ends them.
func TestConvergeWrites(t *testing.T) {
	f := newFixture(t)
	a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
		data := f.convergeWork(t, c, true, "no_change")
		switch c.task.Round {
		case 1:
			f.setThread(t, "PRRT_1", threadNodeJSON(1, "owner/repo", true, 2, "PRRC_1", "PRRC_2"))
			data["thread_actions"] = []any{map[string]any{"thread_id": "PRRT_1", "action": "reply", "comment_id": "PRRC_2"}, map[string]any{"thread_id": "PRRT_1", "action": "resolve"}}
		case 2:
			f.setRun(t, 42, runJSON("owner/repo", workBranch, 2))
			data["ci_reruns"] = []any{map[string]any{"run_id": 42}}
		}
		writeCandidate(t, c, data)
		return ""
	}}))
	report := a.report
	outputs := report.Result.Outputs
	if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "summary" || len(a.convergeCalls()) != 3 {
		t.Fatalf("outcome %s failure %v final %+v converge dispatches %d", report.Outcome, report.Failure, report.Final, len(a.convergeCalls()))
	}
	first, second := output[convergeReport](t, a, "round-001-converge"), output[convergeReport](t, a, "round-002-converge")
	if !reflect.DeepEqual(first.ThreadActions, []threadAction{{ThreadID: "PRRT_1", Action: "reply", CommentID: "PRRC_2"}, {ThreadID: "PRRT_1", Action: "resolve"}}) ||
		len(first.CIReruns) != 0 || len(second.ThreadActions) != 0 || !reflect.DeepEqual(second.CIReruns, []ciRerun{{RunID: 42}}) {
		t.Fatalf("round 1 %+v %+v round 2 %+v %+v", first.ThreadActions, first.CIReruns, second.ThreadActions, second.CIReruns)
	}
	var want []decision
	for n := 1; n <= 3; n++ {
		want = append(want, decision{Scope: "root/" + roundName(n), Name: "converge-verified", Refs: []contract.Ref{outputs[roundName(n)+"-converge"]}})
	}
	want = append(want, decision{Scope: "root", Name: "converged", Refs: []contract.Ref{outputs["round-003-converge"], outputs["round-003-snapshot"]}})
	if got := a.decisions(t); !reflect.DeepEqual(got[len(got)-4:], want) {
		t.Fatalf("decisions %+v", got)
	}
	if got := f.receiptReads(); got != [2]int{1, 1} {
		t.Fatalf("thread queries and run GETs %v", got)
	}
	a.requireExitsConfirmed(t)
}

// A thread or rerun receipt that GitHub does not confirm ends the rounds
// like a push receipt mismatch, also in a round that pushed: no repair, no
// converge-verified, the round's external objects record as the Final with
// the original error, and no further round or final snapshot.
func TestConvergeWritesRejected(t *testing.T) {
	f := newFixture(t)
	f.setThread(t, "PRRT_1", threadNodeJSON(1, "owner/repo", false, 1, "PRRC_1"))
	a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
		data := f.convergeWork(t, c, true, "committed")
		data["thread_actions"] = []any{map[string]any{"thread_id": "PRRT_1", "action": "reply", "comment_id": "PRRC_9"}}
		writeCandidate(t, c, data)
		return ""
	}}))
	report := a.report
	outputs := report.Result.Outputs
	requireFailure(t, report, `code-change receipt mismatch: thread_actions[0].comment_id: got "PRRC_9", which is not a comment of thread "PRRT_1"`)
	if !errors.Is(report.Failure, errReceipt) || report.Final == nil || report.Final.Output != "round-001-external-objects" || len(a.convergeCalls()) != 1 {
		t.Fatalf("failure %v final %+v converge dispatches %d", report.Failure, report.Final, len(a.convergeCalls()))
	}
	if record := output[externalObjectsRecord](t, a, "round-001-external-objects"); record.Step != "converge" || record.Failure != report.Failure.Error() || !record.Session.ExitConfirmed {
		t.Fatalf("external objects %+v", record)
	}
	if got := a.decisions(t); got[len(got)-1].Name != "fix-verified" {
		t.Fatalf("last decision %+v", got[len(got)-1])
	}
	for _, key := range []string{"round-002-snapshot", "final-snapshot", "summary"} {
		if _, ok := outputs[key]; ok {
			t.Fatalf("outputs %+v", outputs)
		}
	}
	a.requireExitsConfirmed(t)
}

// The Controller never judges the items: a claim with the quality scan
// failed converges like one with it passed, and the summary's claim keeps
// the item as the converger wrote it.
func TestConvergeQualityScanNotJudged(t *testing.T) {
	f := newFixture(t)
	item := map[string]any{"id": "quality-scan", "status": "failed", "reason": "The page shows a broken layout"}
	a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
		data := f.convergeWork(t, c, true, "no_change")
		data["items"] = []any{item}
		writeCandidate(t, c, data)
		return ""
	}}))
	report := a.report
	if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "summary" {
		t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
	}
	got := output[struct {
		Claim struct {
			Items []map[string]any `json:"items"`
		} `json:"claim"`
	}](t, a, "summary")
	if !reflect.DeepEqual(got.Claim.Items, []map[string]any{item}) {
		t.Fatalf("items %+v", got.Claim.Items)
	}
}

// A converge contract the schema or the round's own check rejects is
// rewritten on the same session under the RepairKeepsWork dispatch text,
// with the diagnostic as Feedback. The schema holds the item rules: a
// skipped item only for an id it lists, always with a reason, not_checked
// with a reason, exactly one quality scan, and the unconverged items exactly
// when convergence is not claimed; and a comment ID for a reply. The round's
// check holds the snapshot Ref echo and unique item ids. Two bad contracts
// precede each round's good one.
func TestConvergeContractRepair(t *testing.T) {
	lint := func(status string) map[string]any { return map[string]any{"id": "lint", "status": status} }
	variants := []struct {
		name       string
		bad        func(data map[string]any)
		diagnostic string
	}{
		{"skipped-other-id", func(d map[string]any) {
			d["items"] = []any{qualityScan("passed"), map[string]any{"id": "lint", "status": "skipped", "reason": "r"}}
		}, "at '/items/1/id': value must be 'quality-scan'"},
		{"skipped-without-reason", func(d map[string]any) { d["items"] = []any{map[string]any{"id": "quality-scan", "status": "skipped"}} }, "at '/items/0': missing property 'reason'"},
		{"not-checked-without-reason", func(d map[string]any) { d["items"] = []any{qualityScan("passed"), lint("not_checked")} }, "at '/items/1': missing property 'reason'"},
		{"quality-scan-missing", func(d map[string]any) { d["items"] = []any{lint("passed")} }, "at '/items': min 1 items required to match contains schema"},
		{"quality-scan-twice", func(d map[string]any) { d["items"] = []any{qualityScan("passed"), qualityScan("failed")} }, "at '/items': max 1 items required to match contains schema"},
		{"unconverged-missing", func(d map[string]any) { d["converged"] = false; delete(d, "unconverged") }, "missing property 'unconverged'"},
		{"unconverged-with-a-claim", func(d map[string]any) { d["unconverged"] = []string{"x"} }, "at '/unconverged': false schema"},
		{"duplicate-id", func(d map[string]any) { d["items"] = []any{lint("passed"), qualityScan("passed"), lint("failed")} },
			`items[2].id: got "lint", which items[0] already uses; want an ID unique among the items`},
		{"snapshot-not-echoed", func(d map[string]any) {
			ref := d["snapshot"].(contract.Ref)
			ref.AttemptID = "other"
			d["snapshot"] = ref
		}, "snapshot: got "},
		{"reply-without-comment", func(d map[string]any) {
			d["thread_actions"] = []any{map[string]any{"thread_id": "PRRT_1", "action": "reply"}}
		},
			"at '/thread_actions/0': missing property 'comment_id'"},
	}
	// wrote is the number, in the run's calls, of the attempt that wrote
	// each variant; the next call is its repair.
	wrote := make([]int, len(variants))
	f := newFixture(t)
	rounds := map[int]int{}
	next := 0
	a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
		data := f.convergeWork(t, c, true, "no_change")
		if rounds[c.task.Round]++; rounds[c.task.Round] <= 2 && next < len(variants) {
			variants[next].bad(data)
			wrote[next] = c.n
			next++
		} else if next < len(variants) {
			data["converged"] = false
			data["unconverged"] = []string{"Another round is needed"}
		}
		writeCandidate(t, c, data)
		return ""
	}}))
	report := a.report
	last := (len(variants) + 1) / 2
	if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "summary" || output[summaryRecord](t, a, "summary").Claim.Output != roundName(last)+"-converge" {
		t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
	}
	for i, v := range variants {
		repair := a.calls[wrote[i]]
		if fb := repair.request.Feedback; repair.task.Round != a.calls[wrote[i]-1].task.Round || fb == nil || !strings.Contains(fb.Message, v.diagnostic) {
			t.Errorf("%s: repair feedback %+v, want %q", v.name, fb, v.diagnostic)
		}
	}
	for n := 1; n <= last; n++ {
		var round []agentCall
		for _, c := range a.convergeCalls() {
			if c.task.Round == n {
				round = append(round, c)
			}
		}
		if len(round) != map[bool]int{false: 3, true: 2}[n == last && len(variants)%2 == 1] {
			t.Fatalf("round %d dispatches %d", n, len(round))
		}
		a.requireRepairKeepsWork(t, round...)
		if at := a.attempt(round[1]); at.Scope != "root/"+roundName(n)+"/contract-repair-converge" || at.HandleID != a.attempt(round[0]).HandleID {
			t.Fatalf("round %d repair scope %s", n, at.Scope)
		}
	}
}

// Cancelling while a round's converger runs ends the run Cancelled once
// every session's exit is confirmed, with no Final and no external objects
// record; the pushed branch and the worktree stay.
func TestConvergeCancelled(t *testing.T) {
	f := newFixture(t)
	var pushed string
	a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
		pushed = f.convergeWork(t, c, true, "committed")["head"].(string)
		return "hold"
	}}))
	report := a.report
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	for _, key := range []string{"round-001-external-objects", "final-snapshot", "summary"} {
		if _, ok := report.Result.Outputs[key]; ok {
			t.Fatalf("outputs %+v", report.Result.Outputs)
		}
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
	if f.originTip(workBranch) != pushed {
		t.Fatalf("origin branch %s, want %s", f.originTip(workBranch), pushed)
	}
}

// Each round's converger gets its own expansion of the private skill's
// converge subtree, written from the bytes read before the first network
// call: what the first round's converger changes in its copy does not reach
// the second round's. Other subtrees are not expanded. The skills record
// is attached in root after the input record, with the files read and their
// digest, and is listed in the outputs.
func TestConvergeSkillsExpanded(t *testing.T) {
	f := newFixture(t)
	original := map[string]string{"converge/SKILL.md": "# Converge\n\nFollow references/round.md.\n", "converge/references/round.md": "One round.\n"}
	if err := os.MkdirAll(filepath.Join(f.skills, convergeSkill, "references"), 0700); err != nil {
		t.Fatal(err)
	}
	for path, data := range original {
		f.write(filepath.Join(f.skills, filepath.FromSlash(path)), data)
	}
	if err := os.MkdirAll(filepath.Join(f.skills, "sync"), 0700); err != nil {
		t.Fatal(err)
	}
	f.write(filepath.Join(f.skills, "sync", "SKILL.md"), "Upstream text")
	// expanded is each round's expansion as its converger found it.
	expanded := map[int]map[string]string{}
	a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
		dir := filepath.Dir(filepath.Dir(c.task.Skills[0]))
		files := map[string]string{}
		if err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				rel, _ := filepath.Rel(dir, path)
				data, _ := os.ReadFile(path)
				files[filepath.ToSlash(rel)] = string(data)
			}
			return err
		}); err != nil {
			t.Error(err)
		}
		expanded[c.task.Round] = files
		if c.task.Round == 1 {
			f.write(c.task.Skills[0], "Changed by the first round\n")
			f.write(filepath.Join(dir, convergeSkill, "references", "extra.md"), "Added by the first round\n")
		}
		writeCandidate(t, c, f.convergeWork(t, c, c.task.Round == 2, "no_change"))
		return ""
	}}))
	report := a.report
	if report.Outcome != engine.Succeeded || len(a.convergeCalls()) != 2 {
		t.Fatalf("outcome %s failure %v converge dispatches %d", report.Outcome, report.Failure, len(a.convergeCalls()))
	}
	for n := 1; n <= 2; n++ {
		if got := a.convergeCalls()[n-1].task.Skills; !reflect.DeepEqual(got, []string{roundSkill(a, n)}) || !reflect.DeepEqual(expanded[n], original) {
			t.Fatalf("round %d skills %v expanded %q, want %q", n, got, expanded[n], original)
		}
	}
	outputs := report.Result.Outputs
	record := output[skillsRecord](t, a, "skills")
	var paths []string
	for _, file := range record.Files {
		paths = append(paths, file.Path)
	}
	if !slices.Equal(paths, []string{"converge/SKILL.md", "converge/references/round.md"}) || len(record.Digest) != 64 {
		t.Fatalf("skills record %+v", record)
	}
	in, sk := report.Snapshot.Attempts[outputs["input"].AttemptID], report.Snapshot.Attempts[outputs["skills"].AttemptID]
	if sk.Key != "skills" || sk.Scope != "root" || !sk.StartedAt.After(in.FinishedAt) {
		t.Fatalf("skills attempt %+v, input finished at %s", sk, in.FinishedAt)
	}
}

// The dispatched fix requirements are pinned byte for byte, so a change to a
// constant they share with other Steps shows up here and is made on purpose.
func TestFixRequirementsUnchanged(t *testing.T) {
	sum := sha256.Sum256([]byte(baselineRequirements + "\n\n" + fixRequirements))
	if got := hex.EncodeToString(sum[:]); got != "14e9626f77e34f769155f65eb5a2d1dea97027c53c5e4468965cd1366f34e3cb" {
		t.Fatalf("fix requirements sha256 %s", got)
	}
}

// The worst case runs within the production policy: every Step that
// repairs uses its three attempts and the rounds run to the production cap
// of 10, the tenth exhausting its repairs. That is 19 sessions
// (implementer, two pre-PR reviewers, the deep review's five, fixer and ten
// convergers) and 66 attempts (input, skills, 3 for the implementer, 6 for
// the pre-PR reviewers, 3 for the correction, pull-request, the deep
// review's 5, 3 for the fixer, 10 round snapshots, 30 for the convergers,
// the tenth round's external objects record, final-snapshot and summary),
// within the 3 live sessions, 22 sessions and 80 attempts of Definition. A
// round that converges takes no external objects record and a round that
// fails otherwise ends the rounds, so no path takes more.
func TestConvergeLimits(t *testing.T) {
	f := newFixture(t)
	seen := map[string]int{}
	var head string
	var published map[string]any
	rejected := map[string]any{"rejected": true}
	// third reports whether c is its Step's third attempt, the first two
	// writing a contract the schema rejects.
	third := func(t *testing.T, c agentCall) bool {
		key := fmt.Sprint(c.request.Output.SchemaID, c.task.Role, c.task.Round)
		if seen[key]++; seen[key] < 3 {
			writeCandidate(t, c, rejected)
			return false
		}
		return true
	}
	a := f.run(t, agents{
		implement: func(t *testing.T, c agentCall) string {
			if head == "" {
				head = f.commit(t, c.task.Worktree, "implemented\n")
			}
			if third(t, c) {
				writeCandidate(t, c, readyData(c, head))
			}
			return ""
		},
		review: func(t *testing.T, c agentCall) string {
			if third(t, c) {
				return acceptReview(t, c)
			}
			return ""
		},
		publish: func(t *testing.T, c agentCall) string {
			if published == nil {
				published = f.publishWork(t, c, "no_change")
			}
			if third(t, c) {
				writeCandidate(t, c, published)
			}
			return ""
		},
		fix: func(t *testing.T, c agentCall) string {
			if third(t, c) {
				return f.acceptFix(t, c)
			}
			return ""
		},
		converge: func(t *testing.T, c agentCall) string {
			if third(t, c) {
				if c.task.Round == 10 {
					writeCandidate(t, c, rejected)
				} else {
					writeCandidate(t, c, f.convergeWork(t, c, false, "no_change"))
				}
			}
			return ""
		},
		deadline: 5 * time.Minute,
	})
	report := a.report
	policy := Definition("", "").Policy
	if policy.MaxLiveSessions != 3 || policy.MaxTotalSessions != 22 || policy.MaxTotalAttempts != 80 {
		t.Fatalf("policy %+v", policy)
	}
	if report.Outcome != engine.Failed || report.Final == nil || report.Final.Output != "summary" || len(a.convergeCalls()) != 30 || len(report.Snapshot.Sessions) != 19 || len(report.Snapshot.Attempts) != 66 {
		t.Fatalf("outcome %s failure %v final %+v converge dispatches %d sessions %d attempts %d", report.Outcome, report.Failure, report.Final, len(a.convergeCalls()), len(report.Snapshot.Sessions), len(report.Snapshot.Attempts))
	}
	outputs := report.Result.Outputs
	for _, key := range []string{"round-010-external-objects", "final-snapshot", "summary"} {
		if _, ok := outputs[key]; !ok {
			t.Fatalf("outputs %+v", outputs)
		}
	}
	a.requireExitsConfirmed(t)
}
