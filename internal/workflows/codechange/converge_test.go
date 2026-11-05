package codechange

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
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
	data := map[string]any{"snapshot": roundSnapshot(t, c), "converged": converged, "items": []any{qualityScan("passed")}, "correction": correction, "head": head}
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

func roundName(n int) string { return fmt.Sprintf("round-%03d", n) }

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
// map): the real preparation with the private skill, the real implement
// Step with implementPublished, a strict close of the implementer's session
// and the real pull-request Attach at its head, for the correction Step that
// does not run; the real fix Step; then the same converge execute runs. The
// fix Step's deep review report is a stand-in, the pull request record,
// which holds no findings: the deep review does not run, and the fixer only
// reads the report's finding IDs. The seam never sets the worktree's
// removal; the full-flow tests check where execute does.
func (f *fixture) convergeSeam(ctx context.Context, r *engine.Run, in engine.Input, rounds roundPolicy) (engine.Result, error) {
	p, err := prepare(ctx, r, in, f.piCWD, f.skills, f.fileSource())
	if err != nil {
		return engine.Result{}, err
	}
	result, hand, err := implement(ctx, r, p, f.piCWD)
	if err != nil {
		return result, err
	}
	if err := closeStrictly(ctx, r, hand.session, hand.sessionID, implementRole, "implementer"); err != nil {
		return result, err
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "pull-request", Output: contract.Spec{SchemaID: PullRequestSchema},
		Data: pullRequestRecord{URL: pullURL(1), Number: 1, Branch: workBranch, HeadSHA: hand.head, ReportedHead: hand.head, CompareStatus: "identical"}})
	if err != nil {
		return result, err
	}
	result.Outputs["pull-request"], result.Outputs[deepReviewName+"-report"] = ref, ref
	if result, err = fix(ctx, r, p, f.piCWD, result); err != nil {
		return result, err
	}
	return p.converge(ctx, r, f.piCWD, result, testState, rounds)
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
// round's own expansion of the private skill, and its converge-verified
// Decision; the round's report is the Final and the final snapshot follows
// the rounds.
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
			if report.Outcome != engine.Succeeded || report.Failure != nil || report.Final == nil || report.Final.Output != final || report.Final.Ref != outputs[final] {
				t.Fatalf("outcome %s failure %v final %+v, want %s", report.Outcome, report.Failure, report.Final, final)
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
// with rounds-not-admitted: with no round admitted the fix contract is the
// Final, otherwise the last round's report. The Decision cites that report
// and the last snapshot, the final snapshot is still taken and the run fails
// with errNotConverged. A run without a deadline admits rounds the clock
// puts past it.
func TestConvergeRoundsEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		cap  int
		// late is the 1-based clock read from which the clock is past the
		// reserve; 0 never.
		late     int
		disable  bool
		rounds   []roundBehavior
		final    string
		decision string
		refs     []string
		want     string
	}{
		{"round-cap", 2, 0, false, []roundBehavior{{push: true}, {}}, "round-002-converge", "rounds-exhausted", []string{"round-002-converge", "round-002-snapshot"}, " within the round cap of 2"},
		{"no-round-admitted", 10, 1, false, nil, "fix", "rounds-not-admitted", []string{"fix"}, ": the run deadline leaves less than 2h40m0s for round 1"},
		{"second-round-not-admitted", 10, 2, false, []roundBehavior{{push: true}}, "round-001-converge", "rounds-not-admitted", []string{"round-001-converge", "round-001-snapshot"}, ": the run deadline leaves less than 2h40m0s for round 2"},
		{"no-run-deadline", 10, 1, true, []roundBehavior{{}, {claim: true}}, "round-002-converge", "", nil, ""},
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
			if report.Final == nil || report.Final.Output != tc.final || report.Final.Ref != outputs[tc.final] || len(a.convergeCalls()) != len(tc.rounds) {
				t.Fatalf("final %+v converge dispatches %d failure %v", report.Final, len(a.convergeCalls()), report.Failure)
			}
			if _, ok := outputs["final-snapshot"]; !ok {
				t.Fatalf("outputs %+v", outputs)
			}
			if tc.want == "" {
				if report.Outcome != engine.Succeeded {
					t.Fatalf("outcome %s failure %v", report.Outcome, report.Failure)
				}
				return
			}
			requireFailure(t, report, "code-change: the pull request did not converge"+tc.want)
			var refs []contract.Ref
			for _, key := range tc.refs {
				refs = append(refs, outputs[key])
			}
			if got := a.decisions(t); !errors.Is(report.Failure, errNotConverged) || !reflect.DeepEqual(got[len(got)-1], decision{Scope: "root", Name: tc.decision, Refs: refs}) {
				t.Fatalf("failure %v last decision %+v", report.Failure, got[len(got)-1])
			}
			if _, ok := outputs[roundName(len(tc.rounds)+1)+"-snapshot"]; ok {
				t.Fatalf("outputs %+v", outputs)
			}
			a.requireExitsConfirmed(t)
		})
	}
}

// A round failing from its dispatch on ends the rounds: the round's external
// objects record, step converge, attached in the round's scope and named
// after it, is the Final with the original error, and no other round or final
// snapshot follows. A receipt the worktree or GitHub does not confirm is not
// repaired; the receipt check is the fix Step's, which TestFixReceiptRejected
// and TestPublishReceiptRejected cover, so only a rewritten history, which a
// round must not push either, and a reported head that is not the worktree
// HEAD in a later round are here.
func TestConvergeRoundFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		round int
		// act is the failing round's converger.
		act  func(t *testing.T, f *fixture, c agentCall) map[string]any
		want string
	}{
		{"repairs-exhausted", 1, func(t *testing.T, f *fixture, c agentCall) map[string]any {
			if c.request.Feedback == nil {
				f.convergeWork(t, c, false, "committed")
			}
			data := convergeData(t, c, true, "committed", f.gitIn(c.task.Worktree, "rev-parse", "HEAD"))
			data["items"] = []any{qualityScan("passed"), map[string]any{"id": "lint", "status": "passed"}, map[string]any{"id": "lint", "status": "failed"}}
			return data
		}, `items[2].id: got "lint", which items[1] already uses`},
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
			if tc.name != "repairs-exhausted" && (!errors.Is(report.Failure, errReceipt) || len(a.convergeCalls()) != tc.round) {
				t.Fatalf("failure %v converge dispatches %d, want no repair", report.Failure, len(a.convergeCalls()))
			}
			record := output[externalObjectsRecord](t, a, name+"-external-objects")
			last := a.convergeCalls()[len(a.convergeCalls())-1]
			if record.Step != "converge" || record.Failure != report.Failure.Error() || !record.Session.ExitConfirmed || record.Reported == nil || record.Reported.Branch != workBranch {
				t.Fatalf("external objects %+v", record)
			}
			if at := report.Snapshot.Attempts[outputs[name+"-external-objects"].AttemptID]; at.Scope != "root/"+name || at.Key != "external-objects" || outputs[name+"-converge"] != *a.attempt(last).Output {
				t.Fatalf("external objects attempt %+v", at)
			}
			for _, key := range []string{"final-snapshot", "external-objects", roundName(tc.round+1) + "-snapshot"} {
				if _, ok := outputs[key]; ok {
					t.Fatalf("outputs %+v", outputs)
				}
			}
			a.requireExitsConfirmed(t)
		})
	}
}

// A round that stops before its dispatch ends the rounds without opening
// the round's session, so no external objects record and no final snapshot
// follow, and the last accepted report is the Final: a round snapshot
// showing the pull request merged or closed, errPullRequestNotOpen; a round
// snapshot that cannot be read, errStateRead, recording no snapshot; the
// round's skill expansion colliding with a directory that already exists,
// errSkillsExpansion; and a worktree HEAD that moved between rounds,
// errReceipt against the head the previous round pushed.
func TestConvergeStopsBeforeDispatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		// round is the round that stops; the earlier ones push.
		round int
		// setup prepares the fake GitHub before the run, act the first
		// round's converger after its push.
		setup    func(f *fixture)
		act      func(f *fixture, c agentCall)
		sentinel error
		want     func(f *fixture, a a1Run) string
		snapshot bool
	}{
		{"merged-before-round-1", 1, func(f *fixture) {
			f.write(filepath.Join(f.dir, "pr-patch.json"), `{"state":"closed","merged":true}`)
			f.write(filepath.Join(f.dir, "pr-patch-after-checks"), "1")
		}, nil, errPullRequestNotOpen, func(*fixture, a1Run) string {
			return `code-change: the pull request is no longer open: the round-001 snapshot shows state "closed" merged true`
		}, true},
		// The first round's wait and snapshot read the check runs twice; the
		// second round's wait reads them next.
		{"closed-before-round-2", 2, func(f *fixture) {
			f.write(filepath.Join(f.dir, "pr-patch.json"), `{"state":"closed"}`)
			f.write(filepath.Join(f.dir, "pr-patch-after-checks"), "3")
		}, nil, errPullRequestNotOpen, func(*fixture, a1Run) string {
			return `code-change: the pull request is no longer open: the round-002 snapshot shows state "closed" merged false`
		}, true},
		{"snapshot-unreadable", 1, func(f *fixture) { f.write(filepath.Join(f.dir, "gh-fail-threads"), "1") }, nil, errStateRead, func(f *fixture, a a1Run) string {
			return commandFailure(a.report, "code-change cannot read the pull request state: review threads query: ")
		}, false},
		{"skills-expansion-collides", 2, nil, func(f *fixture, c agentCall) {
			if err := os.Mkdir(filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(c.task.Skills[0]))), skillsExpansion+"-002"), 0700); err != nil {
				f.t.Error(err)
			}
		}, errSkillsExpansion, func(*fixture, a1Run) string {
			return "code-change cannot expand the private skill for round 2: reserve code change skills 002: "
		}, true},
		{"head-moved-before-round-2", 2, nil, func(f *fixture, c agentCall) {
			f.write(filepath.Join(f.dir, "gh-move-head-at-checks"), "3 "+c.task.Worktree)
		}, errReceipt, func(f *fixture, a a1Run) string {
			return "code-change receipt mismatch: the worktree HEAD is " + f.gitIn(a.worktree(), "rev-parse", "HEAD") + " before round-002; want the verified head " + f.originTip(workBranch)
		}, true},
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
			final := "fix"
			if tc.round > 1 {
				final = roundName(tc.round-1) + "-converge"
			}
			requireFailure(t, report, tc.want(f, a))
			if !errors.Is(report.Failure, tc.sentinel) || report.Final == nil || report.Final.Output != final || report.Final.Ref != outputs[final] || len(a.convergeCalls()) != tc.round-1 {
				t.Fatalf("failure %v final %+v converge dispatches %d", report.Failure, report.Final, len(a.convergeCalls()))
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
			for _, key := range []string{name + "-external-objects", "final-snapshot"} {
				if _, ok := outputs[key]; ok {
					t.Fatalf("outputs %+v", outputs)
				}
			}
			a.requireExitsConfirmed(t)
		})
	}
}

// A read of the pull request after a round's session closed and its
// receipt was verified that fails ends the rounds with errStateRead and the
// round's report, the last accepted one, as the Final: no external objects
// record, no further round and no final snapshot.
func TestConvergeReadAfterRoundFails(t *testing.T) {
	f := newFixture(t)
	a := f.run(t, f.seam(agents{converge: func(t *testing.T, c agentCall) string {
		data := f.convergeWork(t, c, true, "no_change")
		// The receipt check reads the pull request next, the read after the
		// round once more.
		f.write(filepath.Join(f.dir, "gh-fail-pull-at"), fmt.Sprint(pullReads(f.dir)+2))
		writeCandidate(t, c, data)
		return ""
	}}))
	report := a.report
	outputs := report.Result.Outputs
	requireFailure(t, report, commandFailure(report, "after round-001: code-change cannot read the pull request state: pull request GET: "))
	if !errors.Is(report.Failure, errStateRead) || report.Final == nil || report.Final.Output != "round-001-converge" || report.Final.Ref != outputs["round-001-converge"] || len(a.convergeCalls()) != 1 {
		t.Fatalf("failure %v final %+v converge dispatches %d", report.Failure, report.Final, len(a.convergeCalls()))
	}
	for _, key := range []string{"round-001-external-objects", "round-002-snapshot", "final-snapshot"} {
		if _, ok := outputs[key]; ok {
			t.Fatalf("outputs %+v", outputs)
		}
	}
	if got := a.decisions(t); got[len(got)-1].Name != "converge-verified" {
		t.Fatalf("last decision %+v", got[len(got)-1])
	}
	a.requireExitsConfirmed(t)
}

// The Controller never judges the items: a claim with the quality scan
// failed converges like one with it passed, and the Final report keeps the
// item as the converger wrote it.
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
	if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "round-001-converge" {
		t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
	}
	got := output[struct {
		Items []map[string]any `json:"items"`
	}](t, a, "round-001-converge")
	if !reflect.DeepEqual(got.Items, []map[string]any{item}) {
		t.Fatalf("items %+v", got.Items)
	}
}

// A converge contract the schema or the round's own check rejects is
// rewritten on the same session under the RepairKeepsWork dispatch text,
// with the diagnostic as Feedback. The schema holds the item rules: a
// skipped item only for an id it lists, always with a reason, not_checked
// with a reason, exactly one quality scan, and the unconverged items exactly
// when convergence is not claimed; the round's check holds the snapshot Ref
// echo and unique item ids. Two bad contracts precede each round's good one.
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
	if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "round-005-converge" {
		t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
	}
	for i, v := range variants {
		repair := a.calls[wrote[i]]
		if fb := repair.request.Feedback; repair.task.Round != a.calls[wrote[i]-1].task.Round || fb == nil || !strings.Contains(fb.Message, v.diagnostic) {
			t.Errorf("%s: repair feedback %+v, want %q", v.name, fb, v.diagnostic)
		}
	}
	for n := 1; n <= 5; n++ {
		var round []agentCall
		for _, c := range a.convergeCalls() {
			if c.task.Round == n {
				round = append(round, c)
			}
		}
		if len(round) != map[bool]int{false: 3, true: 2}[n == 5] {
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
	if _, ok := report.Result.Outputs["round-001-external-objects"]; ok {
		t.Fatalf("outputs %+v", report.Result.Outputs)
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

// The dispatched fix requirements are byte for byte those before the
// integration sentence became the constant the converger shares: evidence
// that the extraction preserved them.
func TestFixRequirementsUnchanged(t *testing.T) {
	sum := sha256.Sum256([]byte(baselineRequirements + "\n\n" + fixRequirements))
	if got := hex.EncodeToString(sum[:]); got != "1709defac58cc931f11dd35ee2ce23b070a000db0bd660e49c951d60aa0753b8" {
		t.Fatalf("fix requirements sha256 %s", got)
	}
}

// The worst case of this version runs within the production policy: every
// Step that repairs uses its three attempts and the rounds run to the
// production cap of 10, the last claiming convergence. That is 19 sessions
// (implementer, two pre-PR reviewers, the deep review's five, fixer and ten
// convergers) and 64 attempts (input, skills, 3 for the implementer, 6 for
// the pre-PR reviewers, 3 for the correction, pull-request, the deep
// review's 5, 3 for the fixer, 10 round snapshots, 30 for the convergers
// and final-snapshot), within the 3 live sessions, 22 sessions and 80
// attempts of Definition; a failing round's external objects record
// replaces the final snapshot, so no path takes more.
func TestConvergeLimits(t *testing.T) {
	f := newFixture(t)
	seen := map[string]int{}
	var head string
	var published map[string]any
	// third reports whether c is its Step's third attempt, the first two
	// writing a contract the schema rejects.
	third := func(t *testing.T, c agentCall) bool {
		key := fmt.Sprint(c.request.Output.SchemaID, c.task.Role, c.task.Round)
		if seen[key]++; seen[key] < 3 {
			writeCandidate(t, c, map[string]any{"rejected": true})
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
				writeCandidate(t, c, f.convergeWork(t, c, c.task.Round == 10, "no_change"))
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
	if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "round-010-converge" || len(a.convergeCalls()) != 30 || len(report.Snapshot.Sessions) != 19 || len(report.Snapshot.Attempts) != 64 {
		t.Fatalf("outcome %s failure %v final %+v converge dispatches %d sessions %d attempts %d", report.Outcome, report.Failure, report.Final, len(a.convergeCalls()), len(report.Snapshot.Sessions), len(report.Snapshot.Attempts))
	}
	a.requireExitsConfirmed(t)
}
