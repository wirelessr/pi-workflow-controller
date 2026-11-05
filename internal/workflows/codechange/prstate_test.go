package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/testutil/protocol"
)

// testState is the final state policy of the full-flow tests: no settle
// window, so the fake GitHub's terminal state settles at the first poll.
var testState = statePolicy{wait: 30 * time.Second, settle: 0, poll: 10 * time.Millisecond, reads: time.Minute}

// stateSeam starts the run at the final state: the real preparation, a
// branch on the origin with pull request 1 from it as GitHub shows it, the
// real pull-request Attach, then the final state wait and snapshot with sp.
// The full flow before it, which the other tests cover, only produces these.
// It returns the pull request head.
func (f *fixture) stateSeam(t *testing.T, sp statePolicy) (engine.Definition, string) {
	t.Helper()
	head := f.setOriginBranch(workBranch, f.head)
	f.openPull(t, workBranch)
	return f.definition(func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
		p, err := prepare(ctx, r, in, f.piCWD, f.skills, f.fileSource())
		if err != nil {
			return engine.Result{}, err
		}
		ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "pull-request", Output: contract.Spec{SchemaID: PullRequestSchema},
			Data: pullRequestRecord{URL: pullURL(1), Number: 1, Branch: workBranch, HeadSHA: head, ReportedHead: head, CompareStatus: "identical"}})
		if err != nil {
			return engine.Result{}, err
		}
		result := engine.Result{Outputs: map[string]contract.Ref{"input": p.input, "pull-request": ref}, Final: &engine.FinalSelection{Output: "pull-request"}}
		return p.finalState(ctx, r, result, sp)
	}), head
}

func (f *fixture) writeJSON(t *testing.T, name string, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	f.write(filepath.Join(f.dir, name), string(raw))
}

// pages is a slurped paginated read.
func pages(p ...any) []any { return p }

func checksPage(total int, runs ...any) map[string]any {
	return map[string]any{"total_count": total, "check_runs": append([]any{}, runs...)}
}

func checkRunJSON(id int, status string, conclusion any) map[string]any {
	return map[string]any{"id": id, "name": fmt.Sprint("check-", id), "status": status, "conclusion": conclusion, "head_sha": "ignored",
		"app": map[string]any{"slug": "ci", "name": "CI"}, "html_url": fmt.Sprint("https://github.com/owner/repo/runs/", id), "details_url": fmt.Sprint("https://ci.example/", id),
		"output": map[string]any{"title": "not copied", "summary": "not copied"}}
}

func statusesPage(total int, statuses ...any) map[string]any {
	return map[string]any{"state": "pending", "sha": "ignored", "total_count": total, "statuses": append([]any{}, statuses...)}
}

func statusJSON(context, state string) map[string]any {
	return map[string]any{"context": context, "state": state, "target_url": "https://ci.example/" + context, "description": "not copied", "id": 1}
}

func threadsPage(total int, nodes ...any) map[string]any {
	return map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"reviewThreads": map[string]any{
		"totalCount": total, "nodes": append([]any{}, nodes...), "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}}}}}}
}

func threadJSON(id string, resolved bool, line any, url ...string) map[string]any {
	comments := []any{}
	for _, u := range url {
		comments = append(comments, map[string]any{"url": u, "body": "not copied"})
	}
	return map[string]any{"id": id, "isResolved": resolved, "isOutdated": line == nil, "path": "tracked.txt", "line": line, "comments": map[string]any{"nodes": comments}}
}

// snapshot reads the final-snapshot record of report.
func snapshot(t *testing.T, report engine.Report) prStateRecord {
	t.Helper()
	ref, ok := report.Result.Outputs["final-snapshot"]
	if !ok {
		t.Fatalf("no final-snapshot in %+v", report.Result.Outputs)
	}
	var envelope struct {
		Data prStateRecord `json:"data"`
	}
	if err := protocol.ReadJSON(ref.Path, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Data
}

// requireAttached checks the run's Controller Attaches are exactly keys.
func requireAttached(t *testing.T, report engine.Report, keys ...string) {
	t.Helper()
	var got []string
	for _, a := range report.Snapshot.Attempts {
		if !a.Controller {
			t.Fatalf("Agent attempt %+v", a)
		}
		got = append(got, a.Key)
	}
	if slices.Sort(got); !slices.Equal(got, slices.Sorted(slices.Values(keys))) {
		t.Fatalf("Attaches %v, want %v", got, keys)
	}
}

// requireSnapshotSucceeded checks the seam run succeeded with the pull
// request record as its Final and the snapshot attached.
func requireSnapshotSucceeded(t *testing.T, report engine.Report) prStateRecord {
	t.Helper()
	if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil || report.Final == nil || report.Final.Output != "pull-request" {
		t.Fatalf("outcome %s exit %d failure %v final %+v", report.Outcome, report.ExitCode, report.Failure, report.Final)
	}
	requireAttached(t, report, "input", "skills", "pull-request", "final-snapshot")
	return snapshot(t, report)
}

func parseStamp(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// checksTimes are the times of the check runs reads, in order.
func (f *fixture) checksTimes(t *testing.T) []time.Time {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.dir, "checks-times"))
	if err != nil {
		t.Fatal(err)
	}
	var out []time.Time
	for _, line := range strings.Fields(string(raw)) {
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, time.Unix(0, n))
	}
	return out
}

// The wait's test: a check run is terminal only once completed, a commit
// status only as success, failure or error; every other value, unknown and
// empty ones included, is not.
func TestStateTerminal(t *testing.T) {
	run := func(status string) checkRunObserved { return checkRunObserved{Status: status} }
	status := func(state string) statusObserved { return statusObserved{State: state} }
	for _, tc := range []struct {
		name     string
		runs     []checkRunObserved
		statuses []statusObserved
		want     bool
	}{
		{"none", nil, nil, true},
		{"completed", []checkRunObserved{run("completed"), run("completed")}, nil, true},
		{"terminal-states", nil, []statusObserved{status("success"), status("failure"), status("error")}, true},
		{"both", []checkRunObserved{run("completed")}, []statusObserved{status("failure")}, true},
		{"run-not-completed", []checkRunObserved{run("completed"), run("queued")}, nil, false},
		{"run-unknown", []checkRunObserved{run("finished")}, nil, false},
		{"status-pending", []checkRunObserved{run("completed")}, []statusObserved{status("success"), status("pending")}, false},
		{"status-unknown", nil, []statusObserved{status("done")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := terminal(tc.runs, tc.statuses); got != tc.want {
				t.Fatalf("terminal = %v, want %v", got, tc.want)
			}
		})
	}
}

// A check run's conclusion does not change what the Controller does: the
// wait's test gives the same answer whatever it is, and a run whose check
// runs end with every kind of conclusion settles like any other, with each
// conclusion copied as GitHub gave it.
func TestStateConclusionDoesNotChangeBehavior(t *testing.T) {
	conclusions := []any{"success", "failure", "cancelled", "action_required", "neutral", "timed_out", "skipped", "stale", "startup_failure", "", nil}
	for _, status := range []string{"completed", "in_progress"} {
		for _, c := range conclusions {
			run := checkRunObserved{Status: status}
			if c != nil {
				v := c.(string)
				run.Conclusion = &v
			}
			if got := terminal([]checkRunObserved{run}, nil); got != (status == "completed") {
				t.Fatalf("terminal for status %s conclusion %v = %v", status, c, got)
			}
		}
	}
	t.Run("seam", func(t *testing.T) {
		f := newFixture(t)
		def, head := f.stateSeam(t, testState)
		var running, done []any
		for i, c := range conclusions {
			running = append(running, checkRunJSON(i+1, "in_progress", nil))
			done = append(done, checkRunJSON(i+1, "completed", c))
		}
		// Not terminal at the first poll, terminal from the second on.
		f.writeJSON(t, "checks.json", []any{pages(checksPage(len(running), running...)), pages(checksPage(len(done), done...))})
		f.writeJSON(t, "statuses.json", []any{pages(statusesPage(1, statusJSON("lint", "pending"))), pages(statusesPage(2, statusJSON("lint", "failure"), statusJSON("deploy", "error")))})
		_, report := f.execute(def, prompt)
		s := requireSnapshotSucceeded(t, report)
		if s.Wait.Outcome != "settled" || s.Wait.Polls != 2 || s.Wait.FailedPolls != 0 || s.Wait.LastPolledHead != head || s.PullRequest.HeadSHA != head {
			t.Fatalf("wait %+v head %s", s.Wait, s.PullRequest.HeadSHA)
		}
		if len(s.CheckRuns) != len(conclusions) || len(s.Statuses) != 2 {
			t.Fatalf("snapshot check runs %+v statuses %+v", s.CheckRuns, s.Statuses)
		}
		for i, run := range s.CheckRuns {
			var copied any
			if run.Conclusion != nil {
				copied = *run.Conclusion
			}
			if copied != conclusions[i] || run.Status != "completed" {
				t.Fatalf("check run %d: %+v, want conclusion %v", i, run, conclusions[i])
			}
		}
	})
}

// Every page of the check runs, the combined status and the review threads
// is read and merged; only the threads GitHub shows as not resolved are
// kept, and every recorded field is GitHub's value as given, nulls included.
func TestStateSnapshotPagination(t *testing.T) {
	f := newFixture(t)
	def, head := f.stateSeam(t, testState)
	bare := checkRunJSON(3, "completed", nil)
	bare["app"], bare["html_url"], bare["details_url"] = nil, nil, nil
	f.writeJSON(t, "checks.json", []any{pages(checksPage(3, checkRunJSON(1, "completed", "success"), checkRunJSON(2, "completed", "failure")), checksPage(3, bare))})
	noLink := statusJSON("deploy", "error")
	noLink["target_url"] = nil
	f.writeJSON(t, "statuses.json", []any{pages(statusesPage(2, statusJSON("lint", "success")), statusesPage(2, noLink))})
	f.writeJSON(t, "gh-body-threads", pages(
		threadsPage(3, threadJSON("PRRT_1", false, 4, "https://github.com/owner/repo/pull/1#discussion_r1", "https://github.com/owner/repo/pull/1#discussion_r2"), threadJSON("PRRT_2", true, 5, "https://github.com/owner/repo/pull/1#discussion_r3")),
		threadsPage(3, threadJSON("PRRT_3", false, nil))))
	_, report := f.execute(def, prompt)
	s := requireSnapshotSucceeded(t, report)
	str := func(v string) *string { return &v }
	line := 4
	want := prStateRecord{
		PullRequest: pullStateObserved{Number: 1, URL: pullURL(1), State: "open", HeadSHA: head, HeadRef: workBranch, BaseRef: "main"},
		CheckRuns: []checkRunObserved{
			{ID: 1, Name: "check-1", App: str("ci"), Status: "completed", Conclusion: str("success"), HTMLURL: str("https://github.com/owner/repo/runs/1"), DetailsURL: str("https://ci.example/1")},
			{ID: 2, Name: "check-2", App: str("ci"), Status: "completed", Conclusion: str("failure"), HTMLURL: str("https://github.com/owner/repo/runs/2"), DetailsURL: str("https://ci.example/2")},
			{ID: 3, Name: "check-3", Status: "completed"},
		},
		Statuses: []statusObserved{{Context: "lint", State: "success", TargetURL: str("https://ci.example/lint")}, {Context: "deploy", State: "error"}},
		UnresolvedThreads: []threadObserved{
			{ID: "PRRT_1", Path: "tracked.txt", Line: &line, URL: str("https://github.com/owner/repo/pull/1#discussion_r1")},
			{ID: "PRRT_3", Path: "tracked.txt", Outdated: true},
		},
	}
	got := s
	got.Wait, got.ObservedAt = waitObserved{}, ""
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		t.Fatalf("snapshot\n%s\nwant\n%s", gotJSON, wantJSON)
	}
	started, ended, observed := parseStamp(t, s.Wait.StartedAt), parseStamp(t, s.Wait.EndedAt), parseStamp(t, s.ObservedAt)
	if s.Wait.Outcome != "settled" || s.Wait.Polls != 1 || s.Wait.LastPolledHead != head || ended.Before(started) || observed.Before(ended) {
		t.Fatalf("wait %+v observed %s", s.Wait, s.ObservedAt)
	}
	// One poll, then the snapshot's reads of that head and the threads.
	const get = "api --method GET --hostname github.com -H Accept: application/vnd.github+json repos/owner/repo"
	state := []string{get + "/pulls/1", get + "/commits/" + head + "/check-runs?per_page=100 --paginate --slurp", get + "/commits/" + head + "/status?per_page=100 --paginate --slurp"}
	wantCalls := append(append(slices.Clone(state), state...), "api graphql --hostname github.com -f query=<review threads> -f name=repo -f owner=owner -F number=1 --paginate --slurp")
	if gh := f.ghCalls(); len(gh) < len(wantCalls) || !slices.Equal(gh[len(gh)-len(wantCalls):], wantCalls) {
		t.Fatalf("gh calls %q, want them to end with %q", gh, wantCalls)
	}
}

// An answer that is not complete and readable is a read failure: every page
// must report the same total count, the lists must have that many items,
// keys (check run id, status context, thread id) must not repeat, and a field
// the record cannot hold as null must be there.
func TestStateDecodeRejected(t *testing.T) {
	raw := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	without := func(m map[string]any, keys ...string) map[string]any {
		for _, k := range keys {
			delete(m, k)
		}
		return m
	}
	nullStatus := checkRunJSON(1, "completed", "success")
	nullStatus["status"] = nil
	checks := func(b []byte) error { _, err := decodeCheckRuns(b); return err }
	statuses := func(b []byte) error { _, err := decodeStatuses(b); return err }
	threads := func(b []byte) error { _, err := decodeThreads(b); return err }
	for _, tc := range []struct {
		name   string
		decode func([]byte) error
		raw    []byte
		want   string
	}{
		{"check-runs-total-count", checks, raw(pages(checksPage(3, checkRunJSON(1, "completed", "success")), checksPage(3, checkRunJSON(2, "completed", "success")))), "2 check_runs across 2 pages; total_count is 3"},
		{"check-runs-page-totals-differ", checks, raw(pages(checksPage(1, checkRunJSON(1, "completed", "success")), checksPage(2, checkRunJSON(2, "completed", "success")))), "page 2: total_count 2; page 1 reported 1"},
		{"check-runs-not-slurped", checks, raw(checksPage(0)), "not a list of page objects"},
		{"check-runs-page-not-an-object", checks, raw(pages([]any{})), "not a list of page objects"},
		{"check-runs-no-list", checks, raw(pages(map[string]any{"total_count": 0})), "page 1: no check_runs list"},
		{"check-runs-no-total", checks, raw(pages(map[string]any{"check_runs": []any{}})), "page 1: no integer total_count"},
		{"check-runs-duplicate-id", checks, raw(pages(checksPage(2, checkRunJSON(1, "completed", "success")), checksPage(2, checkRunJSON(1, "completed", "success")))), `page 2: check_runs key "1" repeats an earlier item`},
		{"check-run-no-id", checks, raw(pages(checksPage(1, without(checkRunJSON(1, "completed", "success"), "id")))), "page 1: a check run without id, name or status"},
		{"check-run-no-name", checks, raw(pages(checksPage(1, without(checkRunJSON(1, "completed", "success"), "name")))), "page 1: a check run without id, name or status"},
		{"check-run-null-status", checks, raw(pages(checksPage(1, nullStatus))), "page 1: a check run without id, name or status"},
		{"statuses-total-count", statuses, raw(pages(statusesPage(2, statusJSON("lint", "success")))), "1 statuses across 1 pages; total_count is 2"},
		{"statuses-duplicate-context", statuses, raw(pages(statusesPage(2, statusJSON("lint", "success"), statusJSON("lint", "failure")))), `page 1: statuses key "lint" repeats an earlier item`},
		{"status-no-state", statuses, raw(pages(statusesPage(1, without(statusJSON("lint", "success"), "state")))), "page 1: a status without context or state"},
		{"status-no-context", statuses, raw(pages(statusesPage(1, without(statusJSON("lint", "success"), "context")))), "page 1: a status without context or state"},
		{"threads-not-slurped", threads, raw(threadsPage(0)), "not a list of page objects"},
		{"threads-errors", threads, raw(pages(map[string]any{"errors": []any{map[string]any{"message": "rate limited"}}, "data": nil})), "page 1 has errors"},
		{"threads-no-pull-request", threads, raw(pages(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": nil}}})), "page 1 has no repository pull request review threads"},
		{"threads-total-count", threads, raw(pages(threadsPage(2, threadJSON("PRRT_1", false, 1)), threadsPage(2))), "1 threads across 2 pages; totalCount is 2"},
		{"threads-page-totals-differ", threads, raw(pages(threadsPage(1, threadJSON("PRRT_1", false, 1)), threadsPage(2, threadJSON("PRRT_2", false, 1)))), "page 2: totalCount 2; page 1 reported 1"},
		{"threads-duplicate-id", threads, raw(pages(threadsPage(2, threadJSON("PRRT_1", true, 1)), threadsPage(2, threadJSON("PRRT_1", false, 1)))), `page 2: thread "PRRT_1" repeats an earlier one`},
		{"thread-no-resolved-state", threads, raw(pages(threadsPage(1, without(threadJSON("PRRT_1", false, 1), "isResolved")))), "page 1: a thread without id, isResolved, isOutdated or path"},
		{"thread-no-outdated", threads, raw(pages(threadsPage(1, without(threadJSON("PRRT_1", false, 1), "isOutdated")))), "page 1: a thread without id, isResolved, isOutdated or path"},
		{"thread-no-path", threads, raw(pages(threadsPage(1, without(threadJSON("PRRT_1", false, 1), "path")))), "page 1: a thread without id, isResolved, isOutdated or path"},
		{"thread-no-id", threads, raw(pages(threadsPage(1, without(threadJSON("PRRT_1", false, 1), "id")))), "page 1: a thread without id, isResolved, isOutdated or path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.decode(tc.raw); err == nil || err.Error() != tc.want {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
	draft, merged := false, false
	pr := pullRequest{Number: 1, HTMLURL: pullURL(1), State: "open", Draft: &draft, Merged: &merged, Head: prSide{Ref: workBranch, SHA: strings.Repeat("a", 40)}, Base: prSide{Ref: "main"}}
	if _, err := pullState(pr, 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*pullRequest)
		want   string
	}{
		{"number", func(p *pullRequest) { p.Number = 2 }, "pull request GET: number 2, want 1"},
		{"no-url", func(p *pullRequest) { p.HTMLURL = "" }, "pull request GET: no html_url, state, head ref or base ref"},
		{"no-state", func(p *pullRequest) { p.State = "" }, "pull request GET: no html_url, state, head ref or base ref"},
		{"no-head-ref", func(p *pullRequest) { p.Head.Ref = "" }, "pull request GET: no html_url, state, head ref or base ref"},
		{"no-base-ref", func(p *pullRequest) { p.Base.Ref = "" }, "pull request GET: no html_url, state, head ref or base ref"},
	} {
		t.Run("pull-request-"+tc.name, func(t *testing.T) {
			changed := pr
			tc.change(&changed)
			if _, err := pullState(changed, 1); err == nil || err.Error() != tc.want {
				t.Fatalf("error %v, want %q", err, tc.want)
			}
		})
	}
}

// A snapshot read that fails, or whose answer the Controller cannot read in
// full, fails the run with errStateRead, its read's diagnostic and a summary
// of the wait before it, keeps the Final it was given and attaches nothing:
// there is no partial snapshot. An answer every read gets also fails each
// poll, which only ends the wait at its cap; the other cases settle at the
// first poll and fail a later read.
func TestStateReadRejected(t *testing.T) {
	short := statePolicy{wait: 2 * time.Second, settle: 0, poll: 10 * time.Millisecond, reads: time.Minute}
	const settled = "; the wait before it settled after 1 polls, 0 failed"
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, f *fixture)
		want  string
		// everyPoll is an answer the polls get too.
		everyPoll bool
	}{
		{"pull-request-read", func(t *testing.T, f *fixture) { f.write(filepath.Join(f.dir, "gh-fail-pull-after-checks"), "1") }, "pull request GET: ", false},
		{"check-runs-read", func(t *testing.T, f *fixture) { f.writeJSON(t, "checks.json", []any{pages(checksPage(0)), nil}) }, "check runs GET: ", false},
		{"statuses-read", func(t *testing.T, f *fixture) { f.writeJSON(t, "statuses.json", []any{pages(statusesPage(0)), nil}) }, "commit statuses GET: ", false},
		{"threads-read", func(t *testing.T, f *fixture) { f.write(filepath.Join(f.dir, "gh-fail-threads"), "") }, "review threads query: ", false},
		{"pull-request-number", func(t *testing.T, f *fixture) { f.write(filepath.Join(f.dir, "pr-patch.json"), `{"number": 2}`) }, "pull request GET: number 2, want 1" + settled, false},
		{"threads-errors", func(t *testing.T, f *fixture) {
			f.writeJSON(t, "gh-body-threads", pages(map[string]any{"errors": []any{map[string]any{"message": "rate limited"}}, "data": nil}))
		}, "review threads query: page 1 has errors" + settled, false},
		{"check-runs-duplicate-id", func(t *testing.T, f *fixture) {
			f.writeJSON(t, "gh-body-checks", pages(checksPage(2, checkRunJSON(1, "completed", "success")), checksPage(2, checkRunJSON(1, "completed", "success"))))
		}, `check runs GET: page 2: check_runs key "1" repeats an earlier item; the wait before it timed_out after `, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			sp := testState
			if tc.everyPoll {
				sp = short
			}
			def, _ := f.stateSeam(t, sp)
			tc.setup(t, f)
			_, report := f.execute(def, prompt)
			want := tc.want
			if strings.HasSuffix(want, ": ") {
				want = commandFailure(report, want)
			}
			requireFailure(t, report, "code-change cannot read the pull request state: "+want)
			if !errors.Is(report.Failure, errStateRead) || report.Final == nil || report.Final.Output != "pull-request" {
				t.Fatalf("failure %v final %+v", report.Failure, report.Final)
			}
			// Every poll failed; how many fit in the cap depends on how long
			// each takes, and a poll the cap cut short is not counted.
			var polls, failed int
			_, summary, _ := strings.Cut(report.Failure.Error(), "timed_out after ")
			if n, err := fmt.Sscanf(summary, "%d polls, %d failed", &polls, &failed); tc.everyPoll && (err != nil || n != 2 || polls != failed ||
				polls > 0 && !strings.HasSuffix(summary, `failed, the last with: check runs GET: page 2: check_runs key "1" repeats an earlier item`)) {
				t.Fatalf("failure %v, want the wait's failed polls and last error", report.Failure)
			}
			requireAttached(t, report, "input", "skills", "pull-request")
			if _, ok := report.Result.Outputs["final-snapshot"]; ok {
				t.Fatalf("outputs %+v", report.Result.Outputs)
			}
		})
	}
}

// The wait's summary in a read failure names its outcome, its polls and
// failed polls, and the last poll error when there was one.
func TestStateWaitSummary(t *testing.T) {
	for _, tc := range []struct {
		wait waitObserved
		want string
	}{
		{waitObserved{Outcome: "settled", Polls: 1}, "the wait before it settled after 1 polls, 0 failed"},
		{waitObserved{Outcome: "timed_out", Polls: 3, FailedPolls: 2, LastPollError: "check runs GET: x"}, "the wait before it timed_out after 3 polls, 2 failed, the last with: check runs GET: x"},
	} {
		if got := tc.wait.summary(); got != tc.want {
			t.Fatalf("summary %q, want %q", got, tc.want)
		}
	}
}

// The snapshot's reads have their own bound: a read still running when it
// passes, here the blocked threads query unless an earlier read was slower,
// is stopped and fails the run with errStateRead, not the run's deadline.
func TestStateReadsBound(t *testing.T) {
	f := newFixture(t)
	def, _ := f.stateSeam(t, statePolicy{wait: 30 * time.Second, settle: 0, poll: 10 * time.Millisecond, reads: 3 * time.Second})
	f.write(filepath.Join(f.dir, "gh-block-threads"), "")
	_, report := f.execute(def, prompt)
	requireFailure(t, report, "the pull request state reads exceeded their bound")
	if !errors.Is(report.Failure, errStateRead) || !errors.Is(report.Failure, errReadsCap) || report.Final == nil || report.Final.Output != "pull-request" {
		t.Fatalf("failure %v final %+v", report.Failure, report.Final)
	}
	requireAttached(t, report, "input", "skills", "pull-request")
}

// waitFixture is the fake GitHub showing pull request 1 from a branch on the
// origin, for calling awaitState without a run; it returns the head.
func waitFixture(t *testing.T) (*fixture, string) {
	t.Helper()
	f := newFixture(t)
	head := f.setOriginBranch(workBranch, f.head)
	f.openPull(t, workBranch)
	return f, head
}

// The wait settles only once every poll for the settle window found the
// state terminal on the same head: a poll that is not terminal, a failed
// poll and a new head each restart the window. Each case repeats its change
// over several polls, so that however long a poll takes the window cannot
// close before the last change. The wait records the head it settled on.
func TestStateWaitSettle(t *testing.T) {
	const settle = 700 * time.Millisecond
	sp := statePolicy{wait: 30 * time.Second, settle: settle, poll: 20 * time.Millisecond, reads: time.Minute}
	done := pages(checksPage(1, checkRunJSON(1, "completed", "success")))
	running := pages(checksPage(1, checkRunJSON(1, "in_progress", nil)))
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, f *fixture)
		// restart is the check runs read whose poll began the last window.
		restart, failed int
	}{
		{"terminal-from-the-start", func(t *testing.T, f *fixture) {}, 1, 0},
		{"not-terminal-restarts", func(t *testing.T, f *fixture) {
			f.writeJSON(t, "checks.json", []any{done, running, done, running, done})
		}, 5, 0},
		{"failed-poll-restarts", func(t *testing.T, f *fixture) { f.writeJSON(t, "checks.json", []any{done, nil, done, nil, done}) }, 5, 2},
		// The first three check runs reads each move the head the next poll
		// reads; the fourth poll is the first on the last head.
		{"new-head-restarts", func(t *testing.T, f *fixture) { f.write(filepath.Join(f.dir, "gh-move-on-checks"), "3") }, 4, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, head := waitFixture(t)
			tc.setup(t, f)
			w, err := prepared{repository: "owner/repo"}.awaitState(context.Background(), f.dir, 1, sp)
			if err != nil {
				t.Fatal(err)
			}
			times := f.checksTimes(t)
			ended := parseStamp(t, w.EndedAt)
			if w.Outcome != "settled" || w.FailedPolls != tc.failed || w.Polls < tc.restart+1 || len(times) != w.Polls {
				t.Fatalf("wait %+v, check runs reads %d", w, len(times))
			}
			if gap := ended.Sub(times[tc.restart-1]); gap < settle {
				t.Fatalf("settled %s after check runs read %d, want at least %s", gap, tc.restart, settle)
			}
			if tc.failed > 0 && !strings.Contains(w.LastPollError, "check runs GET: ") {
				t.Fatalf("last poll error %q", w.LastPollError)
			}
			want := head
			if tc.name == "new-head-restarts" {
				raw, err := os.ReadFile(filepath.Join(f.dir, "gh-moved"))
				if err != nil {
					t.Fatal(err)
				}
				// gh-moved holds the last head the branch moved to.
				want, _, _ = strings.Cut(string(raw), " ")
				if want == head {
					t.Fatalf("the head did not move from %s", head)
				}
			}
			if w.LastPolledHead != want {
				t.Fatalf("settled on %s, want %s", w.LastPolledHead, want)
			}
		})
	}
	// The snapshot reads the head the pull request shows after the wait and
	// the check runs of that head; with no settle window the wait settled on
	// the head before the move, and the record keeps both.
	t.Run("snapshot-of-the-new-head", func(t *testing.T) {
		f := newFixture(t)
		def, head := f.stateSeam(t, testState)
		f.write(filepath.Join(f.dir, "gh-move-on-checks"), "1")
		_, report := f.execute(def, prompt)
		s := requireSnapshotSucceeded(t, report)
		raw, err := os.ReadFile(filepath.Join(f.dir, "gh-moved"))
		if err != nil {
			t.Fatal(err)
		}
		moved, _, _ := strings.Cut(string(raw), " ")
		gh := f.ghCalls()
		if s.Wait.LastPolledHead != head || s.PullRequest.HeadSHA != moved || moved == head || !strings.Contains(gh[len(gh)-3], "/commits/"+moved+"/check-runs") {
			t.Fatalf("snapshot head %s, wait %+v, from %s to %s, reads %q", s.PullRequest.HeadSHA, s.Wait, head, moved, gh[len(gh)-3:])
		}
	})
}

// A state that never becomes terminal ends the wait at its cap, which is
// not an error; the seam records the timeout in the snapshot it still takes.
func TestStateWaitTimeout(t *testing.T) {
	const waitCap = time.Second
	sp := statePolicy{wait: waitCap, settle: 0, poll: 20 * time.Millisecond, reads: time.Minute}
	// The first poll finds a status pending and every later one fails; the
	// head stays that of the last poll that read the state in full.
	t.Run("status-pending-then-failing", func(t *testing.T) {
		f, head := waitFixture(t)
		f.writeJSON(t, "statuses.json", []any{pages(statusesPage(1, statusJSON("lint", "pending"))), nil})
		w, err := prepared{repository: "owner/repo"}.awaitState(context.Background(), f.dir, 1, sp)
		if err != nil {
			t.Fatal(err)
		}
		started, ended := parseStamp(t, w.StartedAt), parseStamp(t, w.EndedAt)
		// How many polls fit in the cap depends on how long each takes.
		if w.Outcome != "timed_out" || ended.Sub(started) < waitCap || (w.Polls > 0) != (w.LastPolledHead == head) || w.Polls > 0 && w.FailedPolls != w.Polls-1 {
			t.Fatalf("wait %+v", w)
		}
	})
	t.Run("seam-check-run-in-progress", func(t *testing.T) {
		f := newFixture(t)
		def, _ := f.stateSeam(t, sp)
		f.writeJSON(t, "checks.json", []any{pages(checksPage(1, checkRunJSON(1, "in_progress", nil)))})
		_, report := f.execute(def, prompt)
		s := requireSnapshotSucceeded(t, report)
		started, ended := parseStamp(t, s.Wait.StartedAt), parseStamp(t, s.Wait.EndedAt)
		if s.Wait.Outcome != "timed_out" || s.Wait.FailedPolls != 0 || ended.Sub(started) < waitCap || len(s.CheckRuns) != 1 || s.CheckRuns[0].Status != "in_progress" {
			t.Fatalf("wait %+v check runs %+v", s.Wait, s.CheckRuns)
		}
	})
}

// Cancelling the run while the wait sleeps between polls ends it Cancelled
// at once, with no Final and no snapshot.
func TestStateWaitCancelled(t *testing.T) {
	f := newFixture(t)
	def, _ := f.stateSeam(t, statePolicy{wait: time.Minute, settle: 0, poll: 30 * time.Second, reads: time.Minute})
	f.writeJSON(t, "checks.json", []any{pages(checksPage(1, checkRunJSON(1, "queued", nil)))})
	s := f.start(def, prompt)
	deadline := time.Now().Add(15 * time.Second)
	for !slices.ContainsFunc(f.ghCalls(), func(c string) bool { return strings.Contains(c, "/status?") }) {
		if time.Now().After(deadline) {
			t.Fatal("the wait did not poll")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	cancelled := time.Now()
	s.run.Cancel(engine.OriginControllerUser)
	report := s.wait(t)
	if elapsed := time.Since(cancelled); elapsed > 5*time.Second {
		t.Fatalf("cancellation did not reach the wait: returned after %s", elapsed)
	}
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	requireAttached(t, report, "input", "skills", "pull-request")
	if n := len(f.checksTimes(t)); n != 1 {
		t.Fatalf("check runs reads %d after the cancellation, want the first poll's only", n)
	}
}

// The snapshot is never truncated: one over the Attach limit fails the run
// instead of recording part of it.
func TestStateSnapshotOverLimit(t *testing.T) {
	f := newFixture(t)
	def, _ := f.stateSeam(t, testState)
	var runs []any
	for i := 1; i <= 4000; i++ {
		run := checkRunJSON(i, "completed", "success")
		run["name"] = strings.Repeat("n", 300)
		runs = append(runs, run)
	}
	f.writeJSON(t, "gh-body-checks", pages(checksPage(len(runs), runs...)))
	_, report := f.execute(def, prompt)
	if report.Outcome != engine.Failed || report.ExitCode != 1 || errors.Is(report.Failure, errStateRead) || report.Final == nil || report.Final.Output != "pull-request" {
		t.Fatalf("outcome %s exit %d failure %v final %+v", report.Outcome, report.ExitCode, report.Failure, report.Final)
	}
	var failure *engine.Failure
	if !errors.As(report.Failure, &failure) || failure.Code != engine.LimitExceeded || !strings.Contains(failure.Message, "controller data exceeds candidate byte limit") {
		t.Fatalf("failure %v, want the Attach's candidate byte limit", report.Failure)
	}
	requireAttached(t, report, "input", "skills", "pull-request")
}

// In the full flow, a read failure of the final snapshot after a converged
// round and the cleanup Step fails the run with errStateRead and keeps the
// worktree: the removal is set only after the final snapshot. The summary
// is the Final, the final snapshot in it unavailable with the read's error.
func TestStateFinalReadFailureKeepsWorktree(t *testing.T) {
	f := newFixture(t)
	// The first query is the round's snapshot, the second the final one.
	f.write(filepath.Join(f.dir, "gh-fail-threads"), "2")
	var implemented string
	a := f.run(t, agents{implement: f.implementReady(&implemented)})
	report := a.report
	requireFailure(t, report, commandFailure(report, "code-change cannot read the pull request state: review threads query: "))
	if !errors.Is(report.Failure, errStateRead) || report.Final == nil || report.Final.Output != "summary" || report.Final.Ref != report.Result.Outputs["summary"] {
		t.Fatalf("failure %v final %+v", report.Failure, report.Final)
	}
	if _, ok := report.Result.Outputs["final-snapshot"]; ok {
		t.Fatalf("outputs %+v", report.Result.Outputs)
	}
	if s := output[summaryRecord](t, a, "summary"); s.RoundsEnd != "converged" || s.Cleanup.Report == nil || s.Failure != report.Failure.Error() ||
		!reflect.DeepEqual(s.FinalSnapshot, finalSummary{Status: "unavailable", Detail: report.Failure.Error(), CheckRuns: []checkRunSummary{}, Statuses: []statusSummary{}, UnresolvedThreads: []string{}}) {
		t.Fatalf("summary %+v", s)
	}
	a.requireExitsConfirmed(t)
	requireKept(t, a.run)
}
