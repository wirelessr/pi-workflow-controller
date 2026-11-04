package codechange

import (
	"context"
	"encoding/json"
	"errors"
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
	"pi-workflow-controller/internal/workflows/review"
)

// reviewTask is the part of review's stage task the fake deep review reads.
type reviewTask struct {
	Stage           string            `json:"stage"`
	Pin             json.RawMessage   `json:"pin"`
	Snapshots       map[string]string `json:"snapshots"`
	Missing         []string          `json:"missing"`
	RequiredSources []json.RawMessage `json:"required_sources"`
	Expected        []json.RawMessage `json:"expected_reviewers"`
}

// deepRequirement is the one requirement the fake Prepare finds, and
// deepAssessment its assessment, met at the pinned head's tracked.txt.
var (
	deepRequirement = map[string]any{"id": "req-1", "kind": "requirement", "statement": "tracked.txt holds the change", "source_ids": []string{"metadata"}}
	deepAssessment  = map[string]any{"requirement_id": "req-1", "status": "satisfied", "reason": "Checked at the pinned head",
		"evidence": []any{map[string]any{"path": "tracked.txt", "line": 1, "end_line": 1, "detail": "Read at the pinned head"}}}
)

// acceptDeepReview is each deep review stage writing the smallest contract
// review's acceptance takes: Prepare copies the required sources and their
// snapshot bytes and finds one requirement, the reviewers find nothing and
// Validate reports complete unless a source is unavailable (limited) or a
// reviewer failed (incomplete), with the report appendix review checks.
func acceptDeepReview(t *testing.T, c agentCall) string {
	t.Helper()
	switch c.request.Output.SchemaID {
	case review.PrepareSchema:
		data, files := prepareData(t, c)
		writeCandidateFiles(t, c, data, files)
	case review.ReviewerSchema:
		requirements := []any{}
		if c.review.Stage == "code" {
			requirements = []any{deepAssessment}
		}
		writeCandidateFiles(t, c, map[string]any{"pin": c.review.Pin, "role": c.review.Stage, "context": c.request.Inputs[0], "coverage": []string{"The pinned changed files"},
			"limitations": []string{}, "findings": []any{}, "requirements": requirements}, nil)
	default:
		data, report := validationData(t, c)
		writeCandidateFiles(t, c, data, map[string][]byte{"report": report})
	}
	return ""
}

func prepareData(t *testing.T, c agentCall) (map[string]any, map[string][]byte) {
	t.Helper()
	files := map[string][]byte{"context": []byte("Pinned context and requirements")}
	for _, raw := range c.review.RequiredSources {
		var s struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			FileID string `json:"file_id"`
		}
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatal(err)
		}
		if s.Status == "available" {
			snapshot, err := os.ReadFile(c.review.Snapshots[s.ID])
			if err != nil {
				t.Fatal(err)
			}
			files[s.FileID] = snapshot
		}
	}
	return map[string]any{"pin": c.review.Pin, "sources": c.review.RequiredSources, "requirements": []any{deepRequirement}, "open_questions": []string{}, "context_file": "context"}, files
}

func validationData(t *testing.T, c agentCall) (map[string]any, []byte) {
	t.Helper()
	var prepared struct {
		Data json.RawMessage `json:"data"`
	}
	if err := protocol.ReadJSON(c.request.Inputs[0].Path, &prepared); err != nil {
		t.Fatal(err)
	}
	var sources struct {
		Sources []struct {
			Status string `json:"status"`
		} `json:"sources"`
	}
	if err := json.Unmarshal(prepared.Data, &sources); err != nil {
		t.Fatal(err)
	}
	completeness := "complete"
	for _, s := range sources.Sources {
		if s.Status != "available" {
			completeness = "limited"
		}
	}
	for _, raw := range c.review.Expected {
		var row struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		if row.Status != "succeeded" {
			completeness = "incomplete"
		}
	}
	conclusion, limitations := "no_confirmed_findings", []string{}
	if completeness != "complete" {
		conclusion, limitations = "undetermined", []string{"Part of the pull request was not reviewed"}
	}
	data := map[string]any{"pin": c.review.Pin, "context": c.request.Inputs[0], "reviewers": c.review.Expected, "findings": []any{}, "dispositions": []any{},
		"requirements": []any{deepAssessment}, "completeness": completeness, "conclusion": conclusion, "limitations": limitations, "report_file": "report"}
	body, err := json.Marshal(map[string]any{"prepared": prepared.Data, "validation": data})
	if err != nil {
		t.Fatal(err)
	}
	return data, []byte("# Review report\n\n<!-- pwc-review-data -->\n```json\n" + string(body) + "\n```\n")
}

// writeCandidateFiles writes data with files, the context and report as
// artifacts and every other file as evidence.
func writeCandidateFiles(t *testing.T, c agentCall, data any, files map[string][]byte) {
	t.Helper()
	entries := []contract.FileEntry{}
	for _, id := range slices.Sorted(maps.Keys(files)) {
		kind, dir := "evidence", "evidence"
		if id == "context" || id == "report" {
			kind, dir = "artifact", "artifacts"
		}
		path := filepath.Join(dir, id)
		full := filepath.Join(filepath.Dir(c.candidate), path)
		if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, files[id], 0600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, contract.FileEntry{ID: id, Kind: kind, Path: path})
	}
	if err := protocol.WriteEnvelope(c.candidate, c.request, data, entries); err != nil {
		t.Fatal(err)
	}
}

// deepReviewCall reports whether c is a deep review stage's dispatch.
func deepReviewCall(c agentCall) bool {
	return slices.Contains([]string{review.PrepareSchema, review.ReviewerSchema, review.ValidationSchema}, c.request.Output.SchemaID)
}

// deepReviewCalls are the deep review's dispatches in order.
func (a a1Run) deepReviewCalls() []agentCall {
	var out []agentCall
	for _, c := range a.calls {
		if deepReviewCall(c) {
			out = append(out, c)
		}
	}
	return out
}

// reportData is the part of the deep review report the tests read.
type reportData struct {
	Pin struct {
		HeadSHA string `json:"head_sha"`
	} `json:"pin"`
	Completeness string `json:"completeness"`
}

// deepReviewOutputs are review's outputs as this workflow names them.
var deepReviewOutputs = []string{"deep-review-code", "deep-review-prepare", "deep-review-report", "deep-review-scale", "deep-review-simplicity"}

// After the pull request is verified and every session this workflow opened
// is closed, the code-review flow runs in its own scope root/deep-review, on
// the recorded URL at the committed reported head. Its outputs join this
// workflow's with the deep-review- prefix, and its report is the Final.
func TestDeepReviewSucceeds(t *testing.T) {
	f := newFixture(t)
	var implemented string
	a := f.run(t, agents{implement: f.implementReady(&implemented)})
	report := a.report
	if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil {
		t.Fatalf("outcome %s exit %d failure %v", report.Outcome, report.ExitCode, report.Failure)
	}
	outputs := report.Result.Outputs
	if keys := slices.Sorted(maps.Keys(outputs)); !slices.Equal(keys, slices.Sorted(slices.Values(append([]string{"implement", "input", "publish", "pull-request", "review-basic", "review-consistency"}, deepReviewOutputs...)))) {
		t.Fatalf("outputs %v", keys)
	}
	// Each review output under its new name is that stage's accepted contract.
	calls := a.deepReviewCalls()
	if len(calls) != 5 {
		t.Fatalf("deep review dispatches %d", len(calls))
	}
	for _, c := range calls {
		attempt := a.attempt(c)
		key := "deep-review-" + c.review.Stage
		if c.review.Stage == "validate" {
			key = "deep-review-report"
		}
		if *attempt.Output != outputs[key] || !strings.HasPrefix(attempt.Scope, "root/deep-review") {
			t.Fatalf("%s attempt scope %s output %+v, want %s %+v", c.review.Stage, attempt.Scope, attempt.Output, key, outputs[key])
		}
	}
	if report.Result.Final == nil || *report.Result.Final != (engine.FinalSelection{Output: "deep-review-report", FileID: "report"}) || report.Final == nil || report.Final.Ref != outputs["deep-review-report"] {
		t.Fatalf("final selection %+v delivery %+v", report.Result.Final, report.Final)
	}
	record := output[pullRequestRecord](t, a, "pull-request")
	if got := output[reportData](t, a, "deep-review-report"); got.Pin.HeadSHA != implemented || record.ReportedHead != implemented {
		t.Fatalf("report %+v record %+v, want pinned at %s", got, record, implemented)
	}
	// No session this workflow opened, the implementer's or a pre-PR
	// reviewer's, was still open when the deep review accepted its context.
	closed, _, decided := a.journalIndex(t)
	accepted, ok := decided["context-accepted"]
	for _, c := range a.calls {
		if at := closed[a.attempt(c).HandleID]; !deepReviewCall(c) && (!ok || len(at) == 0 || at[0] > accepted) {
			t.Fatalf("%s session closed at %v, deep review context accepted at %d", c.request.Output.SchemaID, at, accepted)
		}
	}
	// The deep review adds no Decision of this workflow's own.
	var root []decision
	for _, d := range a.decisions(t) {
		if d.Scope == "root" {
			root = append(root, d)
		}
	}
	if want := []decision{{Scope: "root", Name: "implementation-recorded", Refs: []contract.Ref{outputs["implement"]}},
		{Scope: "root", Name: "pre-pr-reviewed", Refs: []contract.Ref{outputs["review-basic"], outputs["review-consistency"]}},
		{Scope: "root", Name: "pull-request-verified", Refs: []contract.Ref{outputs["publish"], outputs["pull-request"]}}}; !reflect.DeepEqual(root, want) {
		t.Fatalf("root decisions %+v, want %+v", root, want)
	}
	a.requireExitsConfirmed(t)
}

// A pull request head newer than the reported head, pushed by someone else
// before or after the receipt check, does not move the deep review: it is
// pinned to the reported head after one compare, the newer commits are a
// missing source and the report is limited, which is still a success.
func TestDeepReviewPinnedToReportedHead(t *testing.T) {
	for _, tc := range []struct {
		name string
		// afterCheck pushes once the receipt check passed, not before it.
		afterCheck bool
	}{{"pushed-before-the-check", false}, {"pushed-after-the-check", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented, newer string
			as := agents{implement: f.implementReady(&implemented)}
			if tc.afterCheck {
				as.close = func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
					if role == implementRole {
						newer = f.setOriginBranch(workBranch, implemented)
					}
					return s.Close(ctx)
				}
			} else {
				as.publish = func(t *testing.T, c agentCall) string {
					data := f.publishWork(t, c, "no_change")
					newer = f.setOriginBranch(workBranch, implemented)
					writeCandidate(t, c, data)
					return ""
				}
			}
			a := f.run(t, as)
			report := a.report
			if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "deep-review-report" {
				t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
			}
			record := output[pullRequestRecord](t, a, "pull-request")
			recorded := map[bool]string{false: newer, true: implemented}[tc.afterCheck]
			if record.ReportedHead != implemented || record.HeadSHA != recorded {
				t.Fatalf("record %+v, want reported %s head %s", record, implemented, recorded)
			}
			got := output[reportData](t, a, "deep-review-report")
			prepare := a.deepReviewCalls()[0]
			if got.Pin.HeadSHA != implemented || got.Completeness != "limited" || !slices.Equal(prepare.review.Missing, []string{"unreviewed-head-commits"}) {
				t.Fatalf("report %+v missing %v, want limited at %s", got, prepare.review.Missing, implemented)
			}
			if calls := f.ghCalls(); !slices.ContainsFunc(calls, func(c string) bool { return strings.HasSuffix(c, " repos/owner/repo/compare/"+implemented+"..."+newer) }) {
				t.Fatalf("gh calls %q, want the deep review's compare", calls)
			}
			a.requireSessionsClosed(t, 8)
		})
	}
}

// An incomplete report, a required deep reviewer having failed, is an
// execution failure: the report is the Final with the reviewer's error, the
// pull request and worktree stay, and nothing runs after it.
func TestDeepReviewIncomplete(t *testing.T) {
	f := newFixture(t)
	var implemented string
	a := f.run(t, agents{
		implement: f.implementReady(&implemented),
		deepReview: func(t *testing.T, c agentCall) string {
			if c.review.Stage == "code" {
				return "provider-error"
			}
			return acceptDeepReview(t, c)
		},
	})
	report := a.report
	if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Snapshot.Failure == nil || report.Snapshot.Failure.Code != engine.ProviderFailed {
		t.Fatalf("outcome %s exit %d failure %+v", report.Outcome, report.ExitCode, report.Snapshot.Failure)
	}
	outputs := report.Result.Outputs
	if report.Final == nil || report.Final.Output != "deep-review-report" || report.Final.Ref != outputs["deep-review-report"] || report.Final.ArtifactPath == "" {
		t.Fatalf("final %+v", report.Final)
	}
	if _, ok := outputs["deep-review-code"]; ok || output[reportData](t, a, "deep-review-report").Completeness != "incomplete" {
		t.Fatalf("outputs %+v", outputs)
	}
	a.requireSessionsClosed(t, 8)
	requireKept(t, a.run)
	if f.pulls(t) != 1 || f.originTip(workBranch) != implemented {
		t.Fatalf("pulls %d branch %s", f.pulls(t), f.originTip(workBranch))
	}
}

// A deep review that ends without a report fails the run with the review's
// error and the verified pull request record as the Final, with no further
// Attach or session; the pull request and worktree stay. A head the branch
// no longer contains, after a force push once the receipt check passed,
// fails acquisition before any review session; a rejected Prepare contract
// fails after one.
func TestDeepReviewWithoutReport(t *testing.T) {
	for _, tc := range []struct {
		name string
		as   func(f *fixture, as *agents, newer *string)
		want func(implemented, newer string) string
		// sessions are the review's, opened before the failure.
		sessions int
	}{
		{"head-not-an-ancestor", func(f *fixture, as *agents, newer *string) {
			as.close = func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
				if role == implementRole {
					*newer = f.setOriginBranch(workBranch, f.head)
				}
				return s.Close(ctx)
			}
		}, func(implemented, newer string) string {
			return "expected head: got " + implemented + ", which is not the PR head " + newer + " or one of its ancestors (compare status diverged)"
		}, 0},
		{"prepare-rejected", func(f *fixture, as *agents, newer *string) {
			as.deepReview = func(t *testing.T, c agentCall) string {
				data, files := prepareData(t, c)
				data["requirements"] = []any{}
				writeCandidateFiles(t, c, data, files)
				return ""
			}
		}, func(string, string) string { return "prepare acceptance: empty requirements require an open question" }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented, newer string
			as := agents{implement: f.implementReady(&implemented)}
			tc.as(f, &as, &newer)
			a := f.run(t, as)
			report := a.report
			requireFailure(t, report, tc.want(implemented, newer))
			if report.Snapshot.Failure == nil || report.Snapshot.Failure.Code != engine.WorkflowFailed {
				t.Fatalf("failure %+v", report.Snapshot.Failure)
			}
			outputs := report.Result.Outputs
			if report.Final == nil || report.Final.Output != "pull-request" || report.Final.Ref != outputs["pull-request"] || report.Final.Step != "pull-request" {
				t.Fatalf("final %+v", report.Final)
			}
			want := []string{"implement", "input", "publish", "pull-request", "review-basic", "review-consistency"}
			if tc.sessions > 0 {
				want = append(want, "deep-review-prepare")
			}
			if keys := slices.Sorted(maps.Keys(outputs)); !slices.Equal(keys, slices.Sorted(slices.Values(want))) {
				t.Fatalf("outputs %v, want %v", keys, want)
			}
			// No session after the review's, and no Attach after the record.
			a.requireSessionsClosed(t, 3+tc.sessions)
			a.requireAttempts(t, 4+tc.sessions, "input", "pull-request")
			requireKept(t, a.run)
			requireMissing(t, filepath.Join(a.run.Dir(), deepReviewName, "checkout"))
			if f.pulls(t) != 1 {
				t.Fatalf("pulls %d", f.pulls(t))
			}
		})
	}
}

// Cancelling during the deep review ends the run Cancelled with no Final
// once every session's exit is confirmed; the pull request and worktree stay.
func TestDeepReviewCancelled(t *testing.T) {
	f := newFixture(t)
	var implemented string
	a := f.run(t, agents{
		implement:  f.implementReady(&implemented),
		deepReview: func(*testing.T, agentCall) string { return "hold" },
	})
	report := a.report
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	if _, ok := report.Result.Outputs["external-objects"]; ok || len(a.deepReviewCalls()) != 1 {
		t.Fatalf("outputs %+v deep review calls %d", report.Result.Outputs, len(a.deepReviewCalls()))
	}
	a.requireSessionsClosed(t, 4)
	requireKept(t, a.run)
	if f.pulls(t) != 1 || f.originTip(workBranch) != implemented {
		t.Fatalf("pulls %d branch %s", f.pulls(t), f.originTip(workBranch))
	}
}

// One session or attempt less than the whole run needs (TestPrePRReviewSucceeds
// runs at exactly three live sessions, eight sessions and eleven attempts)
// stops the run with a run-scope LimitExceeded and no Final at the deep
// review's validation session or Step, after its other four stages were
// dispatched. The pull request and worktree stay.
func TestDeepReviewLimits(t *testing.T) {
	for _, tc := range []struct {
		name               string
		sessions, attempts int
		want               string
	}{
		{"sessions", 7, 11, "session limit exceeded"},
		{"attempts", 8, 10, "run attempt limit exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var implemented string
			a := f.run(t, agents{
				implement: f.implementReady(&implemented),
				policy: func(p *engine.RunPolicy) {
					p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = 3, tc.sessions, tc.attempts
				},
			})
			report := a.report
			if len(a.deepReviewCalls()) != 4 {
				t.Fatalf("deep review dispatches %d, want 4", len(a.deepReviewCalls()))
			}
			var failure *engine.Failure
			if report.Outcome != engine.Failed || !errors.As(report.Failure, &failure) || failure.Code != engine.LimitExceeded || failure.LimitScope != "run" || !strings.Contains(failure.Message, tc.want) {
				t.Fatalf("outcome %s failure %v, want a run LimitExceeded %q", report.Outcome, report.Failure, tc.want)
			}
			if report.Final != nil {
				t.Fatalf("final %+v", report.Final)
			}
			a.requireExitsConfirmed(t)
			requireKept(t, a.run)
			if f.pulls(t) != 1 {
				t.Fatalf("pulls %d", f.pulls(t))
			}
		})
	}
}
