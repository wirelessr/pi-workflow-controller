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

func refPtr(r contract.Ref) *contract.Ref { return &r }

// After a round that converged, the final snapshot follows the round and
// the summary is the Final; nothing is dispatched after the round. The
// summary copies, without judging: the round's items, a failed one beside
// the claim of convergence, skipped and not_checked ones with their
// reasons; the final snapshot's failed check run; and each stage's last
// report with its head, the round's last, whose head is the final head.
// There is no statement.
func TestSummarySucceeds(t *testing.T) {
	items := []itemClaim{{ID: "quality-scan", Status: "skipped", Reason: "No browser session is logged in"}, {ID: "lint", Status: "failed", Reason: "Two warnings"},
		{ID: "build", Status: "not_checked", Reason: "The build tool is missing"}, {ID: "tests", Status: "passed"}}
	f := newFixture(t)
	f.writeJSON(t, "checks.json", []any{pages(checksPage(1, checkRunJSON(1, "completed", "failure")))})
	a := f.run(t, f.seam(agents{
		converge: func(t *testing.T, c agentCall) string {
			data := f.convergeWork(t, c, true, "no_change")
			data["items"] = items
			writeCandidate(t, c, data)
			return ""
		},
	}))
	report := a.report
	outputs := report.Result.Outputs
	if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil || report.Final == nil || report.Final.Output != "summary" || report.Final.Ref != outputs["summary"] {
		t.Fatalf("outcome %s exit %d failure %v final %+v", report.Outcome, report.ExitCode, report.Failure, report.Final)
	}
	calls := a.convergeCalls()
	if len(calls) != 1 || a.calls[len(a.calls)-1].request.Output.SchemaID != ConvergeSchema {
		t.Fatalf("converge dispatches %d, last dispatch %s", len(calls), a.calls[len(a.calls)-1].request.Output.SchemaID)
	}
	if got := a.decisions(t); !reflect.DeepEqual(got[len(got)-1], decision{Scope: "root", Name: "converged", Refs: []contract.Ref{outputs["round-001-converge"], outputs["round-001-snapshot"]}}) {
		t.Fatalf("decisions %+v", got)
	}
	roundAt, snapshotAt := report.Snapshot.Attempts[outputs["round-001-converge"].AttemptID], report.Snapshot.Attempts[outputs["final-snapshot"].AttemptID]
	if !snapshotAt.StartedAt.After(roundAt.FinishedAt) {
		t.Fatalf("final snapshot at %s, converge Step finished at %s", snapshotAt.StartedAt, roundAt.FinishedAt)
	}
	head := output[pullRequestRecord](t, a, "pull-request").ReportedHead
	conclusion := "failure"
	want := summaryRecord{RoundsEnd: "converged",
		PullRequest: pullRequestSummary{Ref: outputs["pull-request"], URL: pullURL(1), Number: 1, Branch: workBranch, ReportedHead: head, HeadSHA: head},
		Reports: []reportSummary{{"implement", outputs["implement"], head}, {"deep-review-report", outputs["deep-review-report"], head}, {"fix", outputs["fix"], head},
			{"round-001-converge", outputs["round-001-converge"], head}},
		FinalHead: head, Statements: []string{},
		Rounds: []roundSummary{{Round: 1, Snapshot: refPtr(outputs["round-001-snapshot"]), Report: refPtr(outputs["round-001-converge"])}},
		Claim:  &claimSummary{Output: "round-001-converge", Converged: true, Items: items},
		FinalSnapshot: finalSummary{Status: "recorded", Ref: refPtr(outputs["final-snapshot"]), HeadSHA: head,
			CheckRuns: []checkRunSummary{{Name: "check-1", Status: "completed", Conclusion: &conclusion}}, Statuses: []statusSummary{}, UnresolvedThreads: []string{}}}
	if got := output[summaryRecord](t, a, "summary"); !reflect.DeepEqual(got, want) {
		t.Fatalf("summary\n%+v\nwant\n%+v", got, want)
	}
	a.requireExitsConfirmed(t)
}

// Run end to end with a commit at every stage that pushes, the
// implementation, the correction, the fix and an unconverged round, and
// someone else's push after the round's, the summary holds each stage's
// last report with its own head, the deep review's the head the correction
// reported, the final head the round's, and the final snapshot's statement
// for the head it shows.
func TestSummaryHeads(t *testing.T) {
	f := newFixture(t)
	var implemented, corrected, fixedHead, round, moved string
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
			moved = f.setOriginBranch(workBranch, round)
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
	if heads := map[string]bool{implemented: true, corrected: true, fixedHead: true, round: true, moved: true}; len(heads) != 5 {
		t.Fatalf("heads %s %s %s %s %s, want each stage's own", implemented, corrected, fixedHead, round, moved)
	}
	want := summaryRecord{Failure: report.Failure.Error(), RoundsEnd: "rounds-exhausted",
		PullRequest: pullRequestSummary{Ref: outputs["pull-request"], URL: pullURL(1), Number: 1, Branch: workBranch, ReportedHead: corrected, HeadSHA: corrected},
		Reports: []reportSummary{{"implement", outputs["implement"], implemented}, {"publish", outputs["publish"], corrected}, {"deep-review-report", outputs["deep-review-report"], corrected},
			{"fix", outputs["fix"], fixedHead}, {"round-001-converge", outputs["round-001-converge"], round}},
		FinalHead:  round,
		Statements: []string{"The final snapshot shows the pull request at head " + moved + ", not the final head " + round},
		Rounds:     []roundSummary{{Round: 1, Snapshot: refPtr(outputs["round-001-snapshot"]), Report: refPtr(outputs["round-001-converge"])}},
		Claim: &claimSummary{Output: "round-001-converge", Unconverged: []string{"A check still fails"},
			Items: []itemClaim{{ID: "quality-scan", Status: "passed", Reason: "The scan's result or why it did not run"}}},
		FinalSnapshot: finalSummary{Status: "recorded", Ref: refPtr(outputs["final-snapshot"]), HeadSHA: moved, CheckRuns: []checkRunSummary{}, Statuses: []statusSummary{}, UnresolvedThreads: []string{}}}
	if got := output[summaryRecord](t, a, "summary"); !reflect.DeepEqual(got, want) {
		t.Fatalf("summary\n%+v\nwant\n%+v", got, want)
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
}

// A round that fails after its dispatch, here with its repairs exhausted,
// ends the rounds with the round's own error and the summary as the Final,
// citing the round's record, with the final head the head verified last,
// the fix contract's. A round that pushed before its repairs ran out leaves
// a HEAD no receipt verified: the summary shows the final snapshot's head
// beside the final head. A converger's close that is not confirmed, which a
// runtime reporting an incomplete Wait stands for, fails the run with the
// close's CleanupFailed. Nothing is dispatched after the round.
func TestSummaryAfterRoundFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		as   func(f *fixture) agents
		// pushed is whether the round pushed a head no receipt verified;
		// confirmed whether the converger's exit is confirmed.
		pushed, confirmed bool
		code              engine.Code
	}{
		{"repairs-exhausted", func(f *fixture) agents {
			return agents{converge: func(t *testing.T, c agentCall) string {
				writeCandidate(t, c, map[string]any{"rejected": true})
				return ""
			}}
		}, false, true, engine.ContractInvalid},
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
		}, true, true, engine.WorkflowFailed},
		{"close-unconfirmed", func(f *fixture) agents {
			return agents{close: func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
				report, err := s.Close(ctx)
				if role == convergeRole {
					report.WaitCompleted = false
				}
				return report, err
			}}
		}, false, false, engine.CleanupFailed},
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
			if record.Step != "converge" || record.Session.ExitConfirmed != tc.confirmed || record.Failure != report.Failure.Error() {
				t.Fatalf("external objects %+v, failure %v", record, report.Failure)
			}
			if last := a.calls[len(a.calls)-1]; last.request.Output.SchemaID != ConvergeSchema {
				t.Fatalf("last dispatch %s, want the round's", last.request.Output.SchemaID)
			}
			fixedHead := output[fixed](t, a, "fix").Head
			statements := []string{}
			if head := snapshot(t, report).PullRequest.HeadSHA; head != fixedHead {
				statements = append(statements, "The final snapshot shows the pull request at head "+head+", not the final head "+fixedHead)
			}
			s := output[summaryRecord](t, a, "summary")
			if s.RoundsEnd != "round_failed" || s.Failure != report.Failure.Error() || s.Claim != nil || s.FinalHead != fixedHead || !reflect.DeepEqual(s.Statements, statements) ||
				(len(statements) == 1) != tc.pushed ||
				!reflect.DeepEqual(s.Rounds, []roundSummary{{Round: 1, Snapshot: refPtr(outputs["round-001-snapshot"]), ExternalObjects: refPtr(outputs["round-001-external-objects"])}}) || s.FinalSnapshot.Status != "recorded" {
				t.Fatalf("summary %+v", s)
			}
			if tc.confirmed {
				a.requireExitsConfirmed(t)
			}
			requireKept(t, a.run)
		})
	}
}

// A summary the Attach rejects, here over the candidate byte limit while
// the round's contract and the snapshots it copies are each within it,
// leaves the rounds' result as it was: the last accepted report, the
// round's contract, is the Final. The run's error is the rounds' with the
// Attach's joined to it, which classifies it.
func TestSummaryOverLimit(t *testing.T) {
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
	if report.Final == nil || report.Final.Output != "round-001-converge" || report.Final.Ref != outputs["round-001-converge"] {
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

// A cancellation that arrives once the rounds converged, before the final
// snapshot, ends the run Cancelled with no final snapshot, no summary and
// no Final, and finish returns an error although the rounds have none, so
// the worktree is kept. Execute calls finish right after the rounds with
// nothing in between to cancel from, so the test runs the same rounds and
// finish from throughFix, cancels between them, and sets the removal the
// way execute does after finish returns no error.
func TestSummaryCancelledAfterConverged(t *testing.T) {
	f := newFixture(t)
	var d roundsEnd
	var finished error
	as := f.seam(agents{})
	as.body = func(ctx context.Context, r *engine.Run, in engine.Input, rounds roundPolicy) (engine.Result, error) {
		p, result, err := f.throughFix(ctx, r, in)
		if err != nil {
			return result, err
		}
		d = p.converge(ctx, r, f.piCWD, result, testState, rounds)
		r.Cancel(engine.OriginControllerUser)
		if result, finished = p.finish(ctx, r, d, testState); finished != nil {
			return result, finished
		}
		p.worktree.remove.Store(true)
		return result, nil
	}
	a := f.run(t, as)
	report := a.report
	if d.kind != "converged" || d.err != nil {
		t.Fatalf("rounds ended %q with %v, want converged", d.kind, d.err)
	}
	if finished == nil {
		t.Fatal("finish returned no error after the cancellation")
	}
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	if got := a.decisions(t); got[len(got)-1].Name != "converged" {
		t.Fatalf("decisions %+v", got)
	}
	for _, key := range []string{"final-snapshot", "summary"} {
		if _, ok := report.Result.Outputs[key]; ok {
			t.Fatalf("outputs %+v", report.Result.Outputs)
		}
	}
	for _, at := range report.Snapshot.Attempts {
		if at.Key == "final-snapshot" || at.Key == "summary" {
			t.Fatalf("attempt %+v", at)
		}
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
}
