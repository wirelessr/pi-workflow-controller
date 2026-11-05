package codechange

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

func cleanupData(correction, head string) map[string]any {
	return map[string]any{"notes": []string{}, "correction": correction, "head": head}
}

// cleanupWork is the fake cleaner's work: committed commits in the worktree
// and pushes the HEAD to workBranch; it returns the contract reporting the
// worktree HEAD.
func (f *fixture) cleanupWork(t *testing.T, c agentCall, correction string) map[string]any {
	t.Helper()
	wt := c.task.Worktree
	if correction == "committed" {
		f.commit(t, wt, "comments cleaned up\n")
		f.gitIn(wt, "push", "-q", "origin", "HEAD:refs/heads/"+workBranch)
	}
	return cleanupData(correction, f.gitIn(wt, "rev-parse", "HEAD"))
}

// acceptCleanup cleans up nothing.
func (f *fixture) acceptCleanup(t *testing.T, c agentCall) string {
	writeCandidate(t, c, f.cleanupWork(t, c, "no_change"))
	return ""
}

func (a a1Run) cleanupCalls() []agentCall {
	var out []agentCall
	for _, c := range a.calls {
		if c.request.Output.SchemaID == CleanupSchema {
			out = append(out, c)
		}
	}
	return out
}

func refPtr(r contract.Ref) *contract.Ref { return &r }

// After a round that converged, the cleanup Step runs in a fresh session in
// PWC_PI_CWD, opened after the converger's closed, with the input,
// implement, pull request record and the round's report, and its receipt
// is checked once like the fix Step's. The final snapshot follows it and
// the summary is the Final. The summary copies, without judging: the
// round's items, a failed one beside the claim of convergence, skipped and
// not_checked ones with their reasons; the final snapshot's failed check
// run; and each stage's last report with its head. When the cleaner pushed
// after the claim, the claim's statement says the final head had no
// convergence round; otherwise there is no statement.
func TestCleanupStepSucceeds(t *testing.T) {
	items := []itemClaim{{ID: "quality-scan", Status: "skipped", Reason: "No browser session is logged in"}, {ID: "lint", Status: "failed", Reason: "Two warnings"},
		{ID: "build", Status: "not_checked", Reason: "The build tool is missing"}, {ID: "tests", Status: "passed"}}
	for _, correction := range []string{"committed", "no_change"} {
		t.Run(correction, func(t *testing.T) {
			f := newFixture(t)
			f.writeJSON(t, "checks.json", []any{pages(checksPage(1, checkRunJSON(1, "completed", "failure")))})
			var cleaned string
			a := f.run(t, f.seam(agents{
				converge: func(t *testing.T, c agentCall) string {
					data := f.convergeWork(t, c, true, "no_change")
					data["items"] = items
					writeCandidate(t, c, data)
					return ""
				},
				cleanup: func(t *testing.T, c agentCall) string {
					data := f.cleanupWork(t, c, correction)
					cleaned = data["head"].(string)
					writeCandidate(t, c, data)
					return ""
				},
			}))
			report := a.report
			outputs := report.Result.Outputs
			if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil || report.Final == nil || report.Final.Output != "summary" || report.Final.Ref != outputs["summary"] {
				t.Fatalf("outcome %s exit %d failure %v final %+v", report.Outcome, report.ExitCode, report.Failure, report.Final)
			}
			calls := a.cleanupCalls()
			if len(calls) != 1 {
				t.Fatalf("cleanup dispatches %d", len(calls))
			}
			c := calls[0]
			attempt := a.attempt(c)
			if attempt.Scope != "root" || attempt.Key != "cleanup" || *attempt.Output != outputs["cleanup"] {
				t.Fatalf("cleanup attempt scope %s key %s", attempt.Scope, attempt.Key)
			}
			if role := report.Snapshot.Sessions[attempt.HandleID].Role; role.Name != cleanupRole || role.Model != cleanupModel || role.CWD != f.piCWD {
				t.Fatalf("cleaner role %+v", role)
			}
			if !reflect.DeepEqual(c.task, task{Role: "cleaner", Requirements: baselineRequirements + "\n\n" + cleanupRequirements, Worktree: a.worktree()}) {
				t.Fatalf("cleanup task %+v", c.task)
			}
			if !reflect.DeepEqual(c.request.Inputs, []contract.Ref{outputs["input"], outputs["implement"], outputs["pull-request"], outputs["round-001-converge"]}) || c.request.Feedback != nil {
				t.Fatalf("cleanup inputs %+v feedback %+v", c.request.Inputs, c.request.Feedback)
			}
			// Fresh: opened after the converger's session closed, closed once
			// after its Decision.
			converger := a.attempt(a.convergeCalls()[0]).HandleID
			closed, _, decided := a.journalIndex(t)
			if at := closed[attempt.HandleID]; closed[converger][0] > a.startedAt(t)[attempt.HandleID] || len(at) != 1 || at[0] < decided["cleanup-verified"] {
				t.Fatalf("cleaner session closed at %v, converger's at %v", at, closed[converger])
			}
			if got := a.decisions(t); !reflect.DeepEqual(got[len(got)-2:], []decision{{Scope: "root", Name: "converged", Refs: []contract.Ref{outputs["round-001-converge"], outputs["round-001-snapshot"]}},
				{Scope: "root", Name: "cleanup-verified", Refs: []contract.Ref{outputs["cleanup"]}}}) {
				t.Fatalf("decisions %+v", got)
			}
			if f.originTip(workBranch) != cleaned {
				t.Fatalf("origin %s, want %s", f.originTip(workBranch), cleaned)
			}
			cleanupAt, snapshotAt := report.Snapshot.Attempts[outputs["cleanup"].AttemptID], report.Snapshot.Attempts[outputs["final-snapshot"].AttemptID]
			if !snapshotAt.StartedAt.After(cleanupAt.FinishedAt) {
				t.Fatalf("final snapshot at %s, cleanup Step finished at %s", snapshotAt.StartedAt, cleanupAt.FinishedAt)
			}
			head := output[pullRequestRecord](t, a, "pull-request").ReportedHead
			conclusion := "failure"
			want := summaryRecord{RoundsEnd: "converged",
				PullRequest: pullRequestSummary{Ref: outputs["pull-request"], URL: pullURL(1), Number: 1, Branch: workBranch, ReportedHead: head, HeadSHA: head},
				Reports: []reportSummary{{"implement", outputs["implement"], head}, {"deep-review-report", outputs["deep-review-report"], head}, {"fix", outputs["fix"], head},
					{"round-001-converge", outputs["round-001-converge"], head}, {"cleanup", outputs["cleanup"], cleaned}},
				FinalHead: cleaned, Statements: []string{},
				Rounds:  []roundSummary{{Round: 1, Snapshot: refPtr(outputs["round-001-snapshot"]), Report: refPtr(outputs["round-001-converge"])}},
				Claim:   &claimSummary{Output: "round-001-converge", Converged: true, Items: items},
				Cleanup: cleanupSummary{Report: refPtr(outputs["cleanup"])},
				FinalSnapshot: finalSummary{Status: "recorded", Ref: refPtr(outputs["final-snapshot"]), HeadSHA: cleaned,
					CheckRuns: []checkRunSummary{{Name: "check-1", Status: "completed", Conclusion: &conclusion}}, Statuses: []statusSummary{}, UnresolvedThreads: []string{}}}
			if correction == "committed" {
				want.Statements = []string{"The convergence claim of round-001-converge is for head " + head + "; the final head " + cleaned + " had no convergence round"}
			}
			if got := output[summaryRecord](t, a, "summary"); !reflect.DeepEqual(got, want) {
				t.Fatalf("summary\n%+v\nwant\n%+v", got, want)
			}
			a.requireExitsConfirmed(t)
		})
	}
}

// Run end to end with a commit at every stage that pushes, the
// implementation, the correction, the fix, an unconverged round and the
// cleanup Step, and someone else's push after the cleaner's, the summary
// holds each stage's last report with its own head, the deep review's the
// head the correction reported, the final head the cleaner's, the claim's
// statement for the round's head and the final snapshot's statement for the
// head it shows.
func TestCleanupStepSummary(t *testing.T) {
	f := newFixture(t)
	var implemented, corrected, fixedHead, round, cleaned, moved string
	a := f.run(t, agents{
		implement: f.implementReady(&implemented),
		publish: func(t *testing.T, c agentCall) string {
			corrected = f.commit(t, c.task.Worktree, "corrected\n")
			writeCandidate(t, c, f.publishWork(t, c, "committed"))
			return ""
		},
		fix: func(t *testing.T, c agentCall) string {
			data := f.fixWork(t, c, "committed")
			fixedHead = data["head"].(string)
			writeCandidate(t, c, data)
			return ""
		},
		converge: func(t *testing.T, c agentCall) string {
			data := f.convergeWork(t, c, false, "committed")
			round = data["head"].(string)
			writeCandidate(t, c, data)
			return ""
		},
		cleanup: func(t *testing.T, c agentCall) string {
			data := f.cleanupWork(t, c, "committed")
			cleaned = data["head"].(string)
			moved = f.setOriginBranch(workBranch, cleaned)
			writeCandidate(t, c, data)
			return ""
		},
		rounds: func(*engine.Run) roundPolicy {
			return roundPolicy{rounds: 1, reserve: convergeRounds.reserve, now: convergeRounds.now}
		},
	})
	report := a.report
	outputs := report.Result.Outputs
	requireFailure(t, report, "code-change: the pull request did not converge within the round cap of 1")
	if report.Final == nil || report.Final.Output != "summary" || report.Final.Ref != outputs["summary"] {
		t.Fatalf("final %+v", report.Final)
	}
	if heads := map[string]bool{implemented: true, corrected: true, fixedHead: true, round: true, cleaned: true, moved: true}; len(heads) != 6 {
		t.Fatalf("heads %s %s %s %s %s %s, want each stage's own", implemented, corrected, fixedHead, round, cleaned, moved)
	}
	want := summaryRecord{Failure: report.Failure.Error(), RoundsEnd: "rounds-exhausted",
		PullRequest: pullRequestSummary{Ref: outputs["pull-request"], URL: pullURL(1), Number: 1, Branch: workBranch, ReportedHead: corrected, HeadSHA: corrected},
		Reports: []reportSummary{{"implement", outputs["implement"], implemented}, {"publish", outputs["publish"], corrected}, {"deep-review-report", outputs["deep-review-report"], corrected},
			{"fix", outputs["fix"], fixedHead}, {"round-001-converge", outputs["round-001-converge"], round}, {"cleanup", outputs["cleanup"], cleaned}},
		FinalHead: cleaned,
		Statements: []string{"The convergence claim of round-001-converge is for head " + round + "; the final head " + cleaned + " had no convergence round",
			"The final snapshot shows the pull request at head " + moved + ", not the final head " + cleaned},
		Rounds: []roundSummary{{Round: 1, Snapshot: refPtr(outputs["round-001-snapshot"]), Report: refPtr(outputs["round-001-converge"])}},
		Claim: &claimSummary{Output: "round-001-converge", Unconverged: []string{"A check still fails"},
			Items: []itemClaim{{ID: "quality-scan", Status: "passed", Reason: "The scan's result or why it did not run"}}},
		Cleanup:       cleanupSummary{Report: refPtr(outputs["cleanup"])},
		FinalSnapshot: finalSummary{Status: "recorded", Ref: refPtr(outputs["final-snapshot"]), HeadSHA: moved, CheckRuns: []checkRunSummary{}, Statuses: []statusSummary{}, UnresolvedThreads: []string{}}}
	if got := output[summaryRecord](t, a, "summary"); !reflect.DeepEqual(got, want) {
		t.Fatalf("summary\n%+v\nwant\n%+v", got, want)
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
}

// A round that fails after its dispatch, here with its repairs exhausted,
// ends the rounds with the round's own error and the summary as the Final,
// citing the round's record. Once the external objects record confirmed the
// converger's exit, the cleanup Step runs on the last accepted report, the
// fix contract, while the worktree HEAD is the head verified last. A round
// that pushed before its repairs ran out leaves a HEAD no receipt verified:
// the cleanup Step is not dispatched, the summary says why and shows the
// final snapshot's head beside the final head, and that check adds nothing
// to the round's error. A converger's close that is not confirmed, which a
// runtime reporting an incomplete Wait stands for, skips the cleanup Step
// too, the run failing with the close's CleanupFailed.
func TestCleanupStepAfterRoundFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		as      func(f *fixture) agents
		skipped string
		code    engine.Code
	}{
		{"repairs-exhausted", func(f *fixture) agents {
			return agents{converge: func(t *testing.T, c agentCall) string {
				writeCandidate(t, c, map[string]any{"rejected": true})
				return ""
			}}
		}, "", engine.ContractInvalid},
		{"repairs-exhausted-after-a-push", func(f *fixture) agents {
			return agents{converge: func(t *testing.T, c agentCall) string {
				if c.request.Feedback == nil {
					f.convergeWork(t, c, false, "committed")
				}
				data := convergeData(t, c, true, "committed", f.gitIn(c.task.Worktree, "rev-parse", "HEAD"))
				data["items"] = []any{qualityScan("passed"), map[string]any{"id": "lint", "status": "passed"}, map[string]any{"id": "lint", "status": "failed"}}
				writeCandidate(t, c, data)
				return ""
			}}
		}, "unverified_head", engine.WorkflowFailed},
		{"close-unconfirmed", func(f *fixture) agents {
			return agents{close: func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
				report, err := s.Close(ctx)
				if role == convergeRole {
					report.WaitCompleted = false
				}
				return report, err
			}}
		}, "session_unconfirmed", engine.CleanupFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			a := f.run(t, f.seam(tc.as(f)))
			report := a.report
			outputs := report.Result.Outputs
			if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Snapshot.Failure.Code != tc.code || report.Final == nil || report.Final.Output != "summary" {
				t.Fatalf("outcome %s exit %d failure %+v %+v final %+v", report.Outcome, report.ExitCode, report.Failure, report.Snapshot.Failure, report.Final)
			}
			record := output[externalObjectsRecord](t, a, "round-001-external-objects")
			if record.Step != "converge" || record.Session.ExitConfirmed != (tc.skipped != "session_unconfirmed") || record.Failure != report.Failure.Error() {
				t.Fatalf("external objects %+v, failure %v", record, report.Failure)
			}
			calls := a.cleanupCalls()
			if len(calls) != map[bool]int{true: 1, false: 0}[tc.skipped == ""] || (len(calls) == 1 && calls[0].request.Inputs[3] != outputs["fix"]) {
				t.Fatalf("cleanup dispatches %d", len(calls))
			}
			fixedHead := output[fixed](t, a, "fix").Head
			statements := []string{}
			if head := snapshot(t, report).PullRequest.HeadSHA; head != fixedHead {
				statements = append(statements, "The final snapshot shows the pull request at head "+head+", not the final head "+fixedHead)
			}
			s := output[summaryRecord](t, a, "summary")
			if s.RoundsEnd != "round_failed" || s.Failure != report.Failure.Error() || s.Claim != nil || s.Cleanup.Skipped != tc.skipped || s.FinalHead != fixedHead || !reflect.DeepEqual(s.Statements, statements) ||
				(len(statements) == 1) != (tc.skipped == "unverified_head") ||
				!reflect.DeepEqual(s.Rounds, []roundSummary{{Round: 1, Snapshot: refPtr(outputs["round-001-snapshot"]), ExternalObjects: refPtr(outputs["round-001-external-objects"])}}) || s.FinalSnapshot.Status != "recorded" {
				t.Fatalf("summary %+v", s)
			}
			if tc.skipped != "session_unconfirmed" {
				a.requireExitsConfirmed(t)
			}
			requireKept(t, a.run)
		})
	}
}

// A cleanup receipt the worktree or GitHub does not confirm fails the run
// once the contract was accepted: no repair, no cleanup-verified, the
// external objects record of step cleanup in root as the Final with the
// original error, and no final snapshot or summary.
func TestCleanupStepReceiptRejected(t *testing.T) {
	f := newFixture(t)
	a := f.run(t, f.seam(agents{cleanup: func(t *testing.T, c agentCall) string {
		data := f.cleanupWork(t, c, "committed")
		data["head"] = f.gitIn(c.task.Worktree, "rev-parse", "HEAD~1")
		writeCandidate(t, c, data)
		return ""
	}}))
	report := a.report
	outputs := report.Result.Outputs
	requireFailure(t, report, "code-change receipt mismatch: head: got ")
	if !errors.Is(report.Failure, errReceipt) || report.Final == nil || report.Final.Output != "external-objects" || len(a.cleanupCalls()) != 1 {
		t.Fatalf("failure %v final %+v cleanup dispatches %d", report.Failure, report.Final, len(a.cleanupCalls()))
	}
	record := output[externalObjectsRecord](t, a, "external-objects")
	if at := report.Snapshot.Attempts[outputs["external-objects"].AttemptID]; at.Scope != "root" || record.Step != "cleanup" || record.Failure != report.Failure.Error() || !record.Session.ExitConfirmed ||
		record.Reported == nil || record.Reported.Branch != workBranch {
		t.Fatalf("external objects %+v at %s", record, at.Scope)
	}
	if got := a.decisions(t); got[len(got)-1].Name != "converged" {
		t.Fatalf("last decision %+v", got[len(got)-1])
	}
	for _, key := range []string{"final-snapshot", "summary"} {
		if _, ok := outputs[key]; ok {
			t.Fatalf("outputs %+v", outputs)
		}
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
}

// A cleanup Step that fails otherwise from its dispatch on, here with its
// repairs exhausted after it pushed, is followed by the final snapshot and
// the summary, which is the Final and cites the external objects record of
// step cleanup in root. After rounds that converged the run fails with the
// cleanup Step's error alone. The final head stays the head verified last,
// the round's; the summary states the pushed head the final snapshot shows
// and that the cleanup Step's writes are unverified.
func TestCleanupStepFailure(t *testing.T) {
	f := newFixture(t)
	a := f.run(t, f.seam(agents{cleanup: func(t *testing.T, c agentCall) string {
		if c.request.Feedback == nil {
			f.cleanupWork(t, c, "committed")
		}
		writeCandidate(t, c, map[string]any{"rejected": true})
		return ""
	}}))
	report := a.report
	outputs := report.Result.Outputs
	record := output[externalObjectsRecord](t, a, "external-objects")
	var failure *engine.Failure
	if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Failure.Error() != record.Failure || !errors.As(report.Failure, &failure) || failure.Code != engine.ContractInvalid {
		t.Fatalf("outcome %s exit %d failure %v, want %q", report.Outcome, report.ExitCode, report.Failure, record.Failure)
	}
	if report.Final == nil || report.Final.Output != "summary" || len(a.cleanupCalls()) != 1+contractRepairs || record.Step != "cleanup" || !record.Session.ExitConfirmed {
		t.Fatalf("final %+v cleanup dispatches %d external objects %+v", report.Final, len(a.cleanupCalls()), record)
	}
	round, pushed := output[convergeReport](t, a, "round-001-converge").Head, f.originTip(workBranch)
	s := output[summaryRecord](t, a, "summary")
	if s.Failure != record.Failure || s.RoundsEnd != "converged" || !reflect.DeepEqual(s.Cleanup, cleanupSummary{ExternalObjects: refPtr(outputs["external-objects"])}) ||
		s.FinalHead != round || s.Reports[len(s.Reports)-1].Output != "round-001-converge" || s.FinalSnapshot.HeadSHA != pushed || pushed == round ||
		!reflect.DeepEqual(s.Statements, []string{"The final snapshot shows the pull request at head " + pushed + ", not the final head " + round,
			"The cleanup Step was dispatched and failed; its writes are unverified; see cleanup.external_objects"}) {
		t.Fatalf("summary %+v", s)
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
}

// A summary the Attach rejects, here over the candidate byte limit while
// the round's contract and the snapshots it copies are each within it,
// leaves the cleanup contract, once accepted, as the Final: never the
// convergence report, whose head the cleaner may have moved. The run's
// error is the rounds' with the Attach's joined to it, which classifies it.
func TestCleanupStepSummaryOverLimit(t *testing.T) {
	f := newFixture(t)
	var runs []any
	for i := 1; i <= 1600; i++ {
		run := checkRunJSON(i, "completed", "success")
		run["name"] = strings.Repeat("n", 300)
		runs = append(runs, run)
	}
	f.writeJSON(t, "gh-body-checks", pages(checksPage(len(runs), runs...)))
	as := f.seam(agents{converge: func(t *testing.T, c agentCall) string {
		data := f.convergeWork(t, c, false, "no_change")
		data["items"] = []any{map[string]any{"id": "quality-scan", "status": "passed", "reason": strings.Repeat("r", 600000)}}
		writeCandidate(t, c, data)
		return ""
	}})
	as.rounds = func(*engine.Run) roundPolicy {
		return roundPolicy{rounds: 1, reserve: convergeRounds.reserve, now: convergeRounds.now}
	}
	a := f.run(t, as)
	report := a.report
	outputs := report.Result.Outputs
	requireFailure(t, report, "code-change: the pull request did not converge within the round cap of 1; ")
	// Both errors are wrapped; the Attach's is the first classified one.
	if !errors.Is(report.Failure, errNotConverged) || !strings.Contains(report.Failure.Error(), "controller data exceeds candidate byte limit") || report.Snapshot.Failure.Code != engine.LimitExceeded {
		t.Fatalf("failure %v %+v, want the summary Attach's candidate byte limit", report.Failure, report.Snapshot.Failure)
	}
	if report.Final == nil || report.Final.Output != "cleanup" || report.Final.Ref != outputs["cleanup"] {
		t.Fatalf("final %+v", report.Final)
	}
	if _, ok := outputs["final-snapshot"]; !ok {
		t.Fatalf("outputs %+v", outputs)
	}
	if _, ok := outputs["summary"]; ok {
		t.Fatalf("outputs %+v", outputs)
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
}

// Cancelling while the cleanup Step runs ends the run Cancelled once every
// session's exit is confirmed, with no Final, no external objects record,
// no final snapshot and no summary; the pushed branch and the worktree
// stay.
func TestCleanupStepCancelled(t *testing.T) {
	f := newFixture(t)
	var pushed string
	a := f.run(t, f.seam(agents{cleanup: func(t *testing.T, c agentCall) string {
		pushed = f.cleanupWork(t, c, "committed")["head"].(string)
		return "hold"
	}}))
	report := a.report
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	for _, key := range []string{"external-objects", "final-snapshot", "summary"} {
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
