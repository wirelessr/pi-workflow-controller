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
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

func reviewData(c agentCall, items ...map[string]any) map[string]any {
	if items == nil {
		items = []map[string]any{}
	}
	return map[string]any{"role": c.task.Role, "implement": c.request.Inputs[1], "findings": items, "limitations": []string{}}
}

// acceptReview is a reviewer that finds nothing.
func acceptReview(t *testing.T, c agentCall) string {
	writeCandidate(t, c, reviewData(c))
	return ""
}

// implementReady commits once and reports ready, storing the commit in head.
func (f *fixture) implementReady(head *string) func(t *testing.T, c agentCall) string {
	return func(t *testing.T, c agentCall) string {
		*head = f.commit(t, c.task.Worktree, "implemented\n")
		writeCandidate(t, c, readyData(c, *head))
		return ""
	}
}

func (a a1Run) snapshotRepo() string {
	return filepath.Join(a.run.Dir(), snapshotDir, "repository.git")
}

// requireSnapshotRemoved checks the snapshot's checkout and registration are
// gone, while its repository, which holds no work, stays.
func (a a1Run) requireSnapshotRemoved(t *testing.T) {
	t.Helper()
	requireMissing(t, filepath.Join(a.run.Dir(), snapshotDir, "checkout"))
	requireMissing(t, filepath.Join(a.snapshotRepo(), "worktrees", "checkout"))
	if info, err := os.Stat(a.snapshotRepo()); err != nil || !info.IsDir() {
		t.Fatalf("snapshot repository: %v", err)
	}
}

// reviewCalls are the reviewers' dispatches by role, in dispatch order.
func (a a1Run) reviewCalls() map[string][]agentCall {
	out := map[string][]agentCall{}
	for _, c := range a.calls {
		if c.task.Role == "basic" || c.task.Role == "consistency" {
			out[c.task.Role] = append(out[c.task.Role], c)
		}
	}
	return out
}

func (a a1Run) attempt(c agentCall) engine.AttemptState {
	return a.report.Snapshot.Attempts[c.request.Identity.AttemptID]
}

// await waits until done holds for the run's snapshot.
func await(t *testing.T, r *engine.Run, what string, done func(engine.Snapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !done(r.Snapshot()) {
		if time.Now().After(deadline) {
			t.Fatalf("%s never happened", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitAttempt waits until the run records c's attempt in state.
func awaitAttempt(t *testing.T, r *engine.Run, c agentCall, state engine.State) {
	t.Helper()
	await(t, r, "attempt "+c.request.Identity.AttemptID+" "+string(state), func(s engine.Snapshot) bool {
		return s.Attempts[c.request.Identity.AttemptID].State == state
	})
}

// otherReviewer is the reviewer that is not role.
func otherReviewer(role string) string {
	if role == "basic" {
		return "consistency"
	}
	return "basic"
}

// Both reviewers run in parallel, each in a fresh session in PWC_PI_CWD,
// reading a Controller-made snapshot of the verified head with their own git,
// within exactly the limits the whole run needs: three live sessions, since
// the implementer's stays open for the correction, and the eight sessions and
// eleven attempts the deep review brings the run to (TestDeepReviewLimits runs
// one less of each). The workflow closes both strictly and removes the
// snapshot before its Decision.
func TestPrePRReviewSucceeds(t *testing.T) {
	f := newFixture(t)
	var head string
	var snapshotAtReturn error
	inFlight := 0
	a := f.run(t, agents{
		implement: f.implementReady(&head),
		review: func(t *testing.T, c agentCall) string {
			if got := f.gitIn(c.task.Checkout, "diff", "--name-only", c.task.Base, c.task.Head); got != "tracked.txt" {
				t.Fatalf("diff in the snapshot: %q", got)
			}
			if got := f.gitIn(c.task.Checkout, "log", "--format=%H", c.task.Base+".."+c.task.Head); got != head {
				t.Fatalf("log in the snapshot: %q, want %s", got, head)
			}
			item := map[string]any{"id": "f1", "summary": "A finding", "detail": "Why it is one"}
			if c.task.Role == "basic" {
				item["path"], item["line"] = "tracked.txt", 1
			} else {
				item["design_ref"] = "Design"
			}
			writeCandidate(t, c, reviewData(c, item))
			// The second reviewer's dispatch arrives while the first waits.
			if inFlight++; inFlight == 1 {
				return "defer"
			}
			return ""
		},
		policy: func(p *engine.RunPolicy) {
			p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = 3, 8, 11
		},
		returned: func(r *engine.Run) { _, snapshotAtReturn = os.Lstat(filepath.Join(r.Dir(), snapshotDir, "checkout")) },
	})
	report := a.report
	if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil {
		t.Fatalf("outcome %s exit %d failure %v", report.Outcome, report.ExitCode, report.Failure)
	}
	a.requireExitsConfirmed(t)
	calls := a.reviewCalls()
	implementAttempt := a.attempt(a.calls[0])
	want := map[string]contract.Ref{"input": a.calls[0].request.Inputs[0], "implement": *implementAttempt.Output}
	reviewHandles := map[string]bool{}
	for _, rv := range reviewers {
		if len(calls[rv.name]) != 1 {
			t.Fatalf("%s dispatches: %d", rv.name, len(calls[rv.name]))
		}
		c := calls[rv.name][0]
		attempt := a.attempt(c)
		want["review-"+rv.name] = *attempt.Output
		if attempt.Scope != "root/pre-pr-review/"+rv.name || attempt.Key != "review" || attempt.HandleID == implementAttempt.HandleID {
			t.Fatalf("%s attempt scope %s key %s handle %s", rv.name, attempt.Scope, attempt.Key, attempt.HandleID)
		}
		reviewHandles[attempt.HandleID] = true
		if role := report.Snapshot.Sessions[attempt.HandleID].Role; role.Name != "code-change-review-"+rv.name || role.Model != rv.model {
			t.Fatalf("%s role %+v", rv.name, role)
		}
		// The task names the checkout, base and head, and no worktree.
		if c.task != (task{Role: rv.name, Requirements: baselineRequirements + "\n\n" + reviewRequirements + "\n" + rv.focus, Checkout: filepath.Join(a.run.Dir(), snapshotDir, "checkout"), Base: f.head, Head: head}) {
			t.Fatalf("%s task %+v", rv.name, c.task)
		}
		if !reflect.DeepEqual(c.request.Inputs, []contract.Ref{want["input"], want["implement"]}) || c.request.Feedback != nil || c.request.Output.SchemaID != ReviewSchema {
			t.Fatalf("%s request inputs %+v feedback %+v schema %s", rv.name, c.request.Inputs, c.request.Feedback, c.request.Output.SchemaID)
		}
	}
	// This workflow's own sessions, the implementer's and both reviewers',
	// run in PWC_PI_CWD and not the engine default; they are the first
	// three, before the deep review's.
	var cwds []string
	for i := range 3 {
		cwds = append(cwds, a.hellos[i].CWD, a.runtime.specs[i].CWD)
	}
	for _, c := range a.calls {
		if !deepReviewCall(c) {
			cwds = append(cwds, report.Snapshot.Sessions[a.attempt(c).HandleID].Role.CWD)
		}
	}
	for _, cwd := range cwds {
		if cwd != f.piCWD || cwd == a.defaultCWD {
			t.Fatalf("cwds %q, want PWC_PI_CWD %q", cwds, f.piCWD)
		}
	}
	for key, ref := range want {
		if report.Result.Outputs[key] != ref {
			t.Fatalf("outputs %+v, want %+v among them", report.Result.Outputs, want)
		}
	}
	reviewed := []decision{{Scope: "root", Name: "implementation-recorded", Refs: []contract.Ref{want["implement"]}},
		{Scope: "root", Name: "pre-pr-reviewed", Refs: []contract.Ref{want["review-basic"], want["review-consistency"]}}}
	if got := a.decisions(t); len(got) < 2 || !reflect.DeepEqual(got[:2], reviewed) {
		t.Fatalf("decisions %+v, want %+v first", got, reviewed)
	}
	// The journal: a CollectAll group of both reviewers, whose sessions the
	// workflow closed before its Decision, before the deep review's group.
	var group map[string]any
	for _, e := range a.events(t) {
		if e.Kind == "GroupStarted" && group == nil {
			if err := json.Unmarshal(e.Details, &group); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !reflect.DeepEqual(group, map[string]any{"scope": "root/pre-pr-review", "mode": "collect_all", "branches": float64(2)}) {
		t.Fatalf("group %+v", group)
	}
	closed, _, decided := a.journalIndex(t)
	for h := range reviewHandles {
		if at := closed[h]; len(at) != 1 || at[0] > decided["pre-pr-reviewed"] {
			t.Fatalf("reviewer session %s closed at events %v, Decision at %d", h, at, decided["pre-pr-reviewed"])
		}
	}
	// The workflow removed the snapshot before returning.
	if !errors.Is(snapshotAtReturn, os.ErrNotExist) {
		t.Fatalf("snapshot checkout at workflow return: %v", snapshotAtReturn)
	}
	a.requireSnapshotRemoved(t)
}

// The snapshot is the verified head, not the live worktree: uncommitted and
// untracked files are absent, and a HEAD moved after the Controller read it
// for the receipt is not what the reviewers see. The Controller reads the
// worktree HEAD again after the review, which detects only a moved HEAD: the
// uncommitted changes pass, the moved HEAD fails the run with the findings
// kept.
func TestPrePRReviewSnapshotPinned(t *testing.T) {
	for _, tc := range []struct {
		name  string
		moved bool
	}{{"uncommitted", false}, {"head-moved", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var head string
			a := f.run(t, agents{
				implement: func(t *testing.T, c agentCall) string {
					head = f.commit(t, c.task.Worktree, "implemented\n")
					f.write(filepath.Join(c.task.Worktree, "tracked.txt"), "uncommitted\n")
					f.write(filepath.Join(c.task.Worktree, "untracked.txt"), "untracked\n")
					if tc.moved {
						f.write(filepath.Join(f.dir, "git-move-head"), c.task.Worktree)
					}
					writeCandidate(t, c, readyData(c, head))
					return ""
				},
				review: func(t *testing.T, c agentCall) string {
					if got := f.gitIn(c.task.Checkout, "rev-parse", "HEAD"); got != head || c.task.Head != head {
						t.Fatalf("snapshot HEAD %s task head %s, want the verified head %s", got, c.task.Head, head)
					}
					if raw, err := os.ReadFile(filepath.Join(c.task.Checkout, "tracked.txt")); err != nil || string(raw) != "implemented\n" {
						t.Fatalf("snapshot tracked.txt %q %v", raw, err)
					}
					requireMissing(t, filepath.Join(c.task.Checkout, "untracked.txt"))
					return acceptReview(t, c)
				},
			})
			report := a.report
			if len(a.reviewCalls()) != 2 {
				t.Fatalf("reviewer calls %v", a.reviewCalls())
			}
			if !tc.moved {
				if report.Outcome != engine.Succeeded {
					t.Fatalf("outcome %s: %v", report.Outcome, report.Failure)
				}
				a.requireSessionsClosed(t, 8)
				return
			}
			a.requireSessionsClosed(t, 3)
			moved := f.gitIn(a.worktree(), "rev-parse", "HEAD")
			if f.gitIn(a.worktree(), "rev-parse", "HEAD~1") != head || moved == head {
				t.Fatalf("worktree HEAD did not move past %s", head)
			}
			requireFailure(t, report, "code-change worktree HEAD moved during pre-PR review: the worktree HEAD is "+moved+"; want the verified head "+head)
			if !errors.Is(report.Failure, errHeadMoved) || report.Final != nil || len(report.Result.Outputs) != 4 {
				t.Fatalf("failure %v final %+v outputs %+v", report.Failure, report.Final, report.Result.Outputs)
			}
			a.requireDecision(t, "implementation-recorded")
			a.requireSnapshotRemoved(t)
		})
	}
}

// A filter and hooks the implementer configured in the worktree repository,
// and a .gitattributes the implementer committed, do not apply while the
// Controller checks out the snapshot: the checkout has the committed bytes,
// although the same filter runs on a checkout in that repository, tried when
// the workflow returns, before the run's cleanup removes the worktree.
func TestPrePRReviewSnapshotIgnoresWorktreeConfig(t *testing.T) {
	f := newFixture(t)
	sentinel := filepath.Join(f.dir, "hostile-ran")
	hostile := filepath.Join(f.dir, "hostile")
	f.write(hostile, "#!/bin/sh\nprintf ran > '"+sentinel+"'\ncat\n")
	if err := os.Chmod(hostile, 0700); err != nil {
		t.Fatal(err)
	}
	hooks := filepath.Join(f.dir, "hooks")
	if err := os.Mkdir(hooks, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"post-checkout", "reference-transaction"} {
		f.write(filepath.Join(hooks, name), "#!/bin/sh\nprintf ran > '"+sentinel+"'\n")
		if err := os.Chmod(filepath.Join(hooks, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	var head string
	var files map[string]string
	var ranBefore, ranOnCheckout error
	a := f.run(t, agents{
		implement: func(t *testing.T, c agentCall) string {
			for _, kv := range [][2]string{{"filter.hostile.clean", "cat"}, {"filter.hostile.smudge", hostile}, {"filter.hostile.required", "true"}, {"core.hooksPath", hooks}} {
				f.gitIn(c.task.Worktree, "config", kv[0], kv[1])
			}
			f.commit(t, c.task.Worktree, "implemented\n")
			files = f.writeAttributes(t, c.task.Worktree)
			attributes := filepath.Join(c.task.Worktree, ".gitattributes")
			raw, err := os.ReadFile(attributes)
			if err != nil {
				t.Fatal(err)
			}
			f.write(attributes, string(raw)+"* filter=hostile\n")
			f.gitIn(c.task.Worktree, "add", ".gitattributes")
			// Not commit -a: it would add the files again under the attributes.
			f.gitIn(c.task.Worktree, "commit", "-qm", "filter")
			head = f.gitIn(c.task.Worktree, "rev-parse", "HEAD")
			if err := os.Remove(sentinel); err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			writeCandidate(t, c, readyData(c, head))
			return ""
		},
		review: func(t *testing.T, c agentCall) string {
			requireMissing(t, sentinel)
			requireBytes(t, c.task.Checkout, files)
			if raw, err := os.ReadFile(filepath.Join(c.task.Checkout, "tracked.txt")); err != nil || string(raw) != "implemented\n" {
				t.Fatalf("snapshot tracked.txt %q %v", raw, err)
			}
			return acceptReview(t, c)
		},
		// The configuration is live in the worktree repository.
		returned: func(r *engine.Run) {
			_, ranBefore = os.Lstat(sentinel)
			worktree := filepath.Join(r.Dir(), worktreeDir, "checkout")
			if ranOnCheckout = os.Remove(filepath.Join(worktree, "tracked.txt")); ranOnCheckout == nil {
				f.gitIn(worktree, "checkout", "--", "tracked.txt")
				_, ranOnCheckout = os.Stat(sentinel)
			}
		},
	})
	if a.report.Outcome != engine.Succeeded {
		t.Fatalf("outcome %s: %v", a.report.Outcome, a.report.Failure)
	}
	if !errors.Is(ranBefore, os.ErrNotExist) {
		t.Fatalf("the filter or a hook ran before the workflow returned: %v", ranBefore)
	}
	if ranOnCheckout != nil {
		t.Fatalf("the worktree repository's filter did not run on its own checkout: %v", ranOnCheckout)
	}
}

// A snapshot that cannot be created fails the run before any reviewer
// starts, with the worktree kept and the partial snapshot removed. A missing
// object fails the checkout instead of being fetched, and git removes what
// its failed checkout created. A cleanup registration that fails after the
// checkout exists leaves the removal to the Controller.
func TestPrePRReviewSnapshotFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		before func(ctx context.Context, r *engine.Run) error
		// missing deletes an object the head tree needs.
		missing bool
		want    func(report engine.Report) string
	}{
		{"missing-object", nil, true, func(report engine.Report) string { return commandFailure(report, "snapshot git worktree: ") }},
		{"registration-failure", func(ctx context.Context, r *engine.Run) error {
			return r.AddCleanup(ctx, snapshotDir, func(context.Context) error { return nil })
		}, false, func(engine.Report) string { return "duplicate resource name" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var head string
			a := f.run(t, agents{before: tc.before, implement: func(t *testing.T, c agentCall) string {
				head = f.commit(t, c.task.Worktree, "implemented\n")
				if tc.missing {
					blob := f.gitIn(c.task.Worktree, "rev-parse", "HEAD:tracked.txt")
					if err := os.Remove(filepath.Join(filepath.Dir(c.task.Worktree), "repository.git", "objects", blob[:2], blob[2:])); err != nil {
						t.Fatal(err)
					}
				}
				writeCandidate(t, c, readyData(c, head))
				return ""
			}})
			report := a.report
			requireFailure(t, report, tc.want(report))
			if report.Final != nil || len(report.Result.Outputs) != 2 || report.Result.Outputs["implement"] != *a.attempt(a.calls[0]).Output {
				t.Fatalf("final %+v outputs %+v", report.Final, report.Result.Outputs)
			}
			a.requireSessionsClosed(t, 1)
			a.requireAttempts(t, 1)
			a.requireDecision(t, "implementation-recorded")
			a.requireSnapshotRemoved(t)
			if got := f.gitIn(a.worktree(), "rev-parse", "HEAD"); got != head {
				t.Fatalf("worktree HEAD %s, want %s", got, head)
			}
		})
	}
}

// Either required reviewer failing fails the run: no pre-pr-reviewed
// Decision, no Final, the other reviewer's accepted findings stay in the
// outputs, every session's exit is confirmed and the run's cleanup removes
// the snapshot. When both fail, the first reviewer's failure classifies the
// run and the other's is in the message.
func TestPrePRReviewerFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		// fail is how each failing reviewer fails: its repairs exhausted or
		// a provider failure.
		fail map[string]string
		code engine.Code
	}{
		{"basic-repairs-exhausted", map[string]string{"basic": "exhausted"}, engine.WorkflowFailed},
		{"consistency-provider-failure", map[string]string{"consistency": "provider"}, engine.ProviderFailed},
		{"both-basic-exhausted", map[string]string{"basic": "exhausted", "consistency": "provider"}, engine.WorkflowFailed},
		{"both-basic-provider", map[string]string{"basic": "provider", "consistency": "exhausted"}, engine.ProviderFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var head string
			// failing holds the reviewers that have not failed yet.
			failing := maps.Clone(tc.fail)
			dispatched := map[string]int{}
			a := f.run(t, agents{
				implement: f.implementReady(&head),
				review: func(t *testing.T, c agentCall) string {
					dispatched[c.task.Role]++
					switch tc.fail[c.task.Role] {
					case "exhausted":
						data := reviewData(c)
						data["role"] = otherReviewer(c.task.Role)
						writeCandidate(t, c, data)
						return ""
					case "provider":
						return "provider-error"
					}
					writeCandidate(t, c, reviewData(c))
					// The accepted reviewer waits for the other's failure,
					// which must not cancel it.
					if len(failing) == 0 {
						return ""
					}
					return "defer"
				},
				release: func(t *testing.T, r *engine.Run, c agentCall) bool {
					switch tc.fail[c.task.Role] {
					case "":
						return false
					case "exhausted":
						if dispatched[c.task.Role] < 1+contractRepairs {
							return false
						}
					case "provider":
						awaitAttempt(t, r, c, engine.Failed)
					}
					delete(failing, c.task.Role)
					return len(failing) == 0
				},
			})
			report := a.report
			if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Final != nil || report.Snapshot.Failure == nil || report.Snapshot.Failure.Code != tc.code {
				t.Fatalf("outcome %s exit %d final %+v failure %+v, want code %s", report.Outcome, report.ExitCode, report.Final, report.Snapshot.Failure, tc.code)
			}
			calls := a.reviewCalls()
			want := map[string]contract.Ref{"input": a.calls[0].request.Inputs[0], "implement": *a.attempt(a.calls[0]).Output}
			attempts := 1
			for _, rv := range reviewers {
				n := 1
				switch tc.fail[rv.name] {
				case "":
					// The other reviewer's failure did not cancel it.
					accepted := a.attempt(calls[rv.name][0])
					if accepted.Output == nil {
						t.Fatalf("%s attempt %s has no output: %+v", rv.name, accepted.State, accepted.Failure)
					}
					want["review-"+rv.name] = *accepted.Output
				case "exhausted":
					n = 1 + contractRepairs
					if diagnostic := `role: got "` + otherReviewer(rv.name) + `"; want "` + rv.name + `"`; !strings.Contains(report.Failure.Error(), diagnostic) {
						t.Fatalf("failure %v, want %q", report.Failure, diagnostic)
					}
				case "provider":
					if !strings.Contains(report.Failure.Error(), "ProviderFailed") {
						t.Fatalf("failure %v, want the provider failure", report.Failure)
					}
				}
				if len(calls[rv.name]) != n {
					t.Fatalf("%s dispatches %d, want %d", rv.name, len(calls[rv.name]), n)
				}
				attempts += n
			}
			if !reflect.DeepEqual(report.Result.Outputs, want) {
				t.Fatalf("outputs %+v, want %+v", report.Result.Outputs, want)
			}
			a.requireAttempts(t, attempts)
			a.requireSessionsClosed(t, 3)
			a.requireDecision(t, "implementation-recorded")
			a.requireSnapshotRemoved(t)
			if got := f.gitIn(a.worktree(), "rev-parse", "HEAD"); got != head {
				t.Fatalf("worktree HEAD %s, want %s", got, head)
			}
		})
	}
}

// A reviewer close that the run cannot confirm fails the run as a cleanup
// failure of that reviewer's session with its accepted findings kept: a
// runtime report without a completed Wait, or a close that outlasts the
// cleanup budget, whose error the engine does not classify.
func TestPrePRReviewerCloseUnconfirmed(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		// confirmed is whether the run's cleanup confirms every exit later.
		confirmed bool
	}{
		{"wait-incomplete", "basic reviewer session close not confirmed", false},
		{"timeout", "context deadline exceeded", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var head string
			released := make(chan struct{})
			a := f.run(t, agents{
				implement: f.implementReady(&head),
				close: func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error) {
					report, err := s.Close(ctx)
					if role == "code-change-review-basic" {
						if tc.name == "timeout" {
							<-ctx.Done()
							<-released
						} else {
							report.WaitCompleted = false
						}
					}
					return report, err
				},
				returned: func(*engine.Run) { close(released) },
			})
			report := a.report
			var failure *engine.Failure
			if report.Outcome != engine.Failed || report.Final != nil || !errors.As(report.Failure, &failure) {
				t.Fatalf("outcome %s final %+v failure %v", report.Outcome, report.Final, report.Failure)
			}
			basic := a.attempt(a.reviewCalls()["basic"][0])
			if failure.Code != engine.CleanupFailed || failure.Phase != "code-change-review-basic" || failure.HandleID != basic.HandleID || failure.Message != tc.message {
				t.Fatalf("failure %+v, want a cleanup failure of handle %s", failure, basic.HandleID)
			}
			if report.Result.Outputs["review-basic"] != *basic.Output || len(report.Result.Outputs) != 4 {
				t.Fatalf("outputs %+v, want the accepted findings", report.Result.Outputs)
			}
			a.requireDecision(t, "implementation-recorded")
			if tc.confirmed {
				a.requireSessionsClosed(t, 3)
			}
		})
	}
}

// Cancelling while the reviewers run ends the run Cancelled once every
// session's exit is confirmed, with the findings already accepted in the
// outputs; the snapshot is removed, the worktree kept.
func TestPrePRReviewCancelled(t *testing.T) {
	f := newFixture(t)
	var head string
	var first agentCall
	a := f.run(t, agents{
		implement: f.implementReady(&head),
		review: func(t *testing.T, c agentCall) string {
			if first.n == 0 {
				first = c
				return acceptReview(t, c)
			}
			return "hold"
		},
		// The second reviewer holds only after the first one's session closed.
		release: func(t *testing.T, r *engine.Run, c agentCall) bool {
			if c.n == first.n {
				await(t, r, "the first reviewer's close", func(s engine.Snapshot) bool {
					return s.Sessions[s.Attempts[c.request.Identity.AttemptID].HandleID].State == "Closed"
				})
			}
			return true
		},
	})
	report := a.report
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	want := map[string]contract.Ref{"input": a.calls[0].request.Inputs[0], "implement": *a.attempt(a.calls[0]).Output, "review-" + first.task.Role: *a.attempt(first).Output}
	if !reflect.DeepEqual(report.Result.Outputs, want) {
		t.Fatalf("outputs %+v, want %+v", report.Result.Outputs, want)
	}
	a.requireSessionsClosed(t, 3)
	a.requireDecision(t, "implementation-recorded")
	a.requireSnapshotRemoved(t)
	if got := f.gitIn(a.worktree(), "rev-parse", "HEAD"); got != head {
		t.Fatalf("worktree HEAD %s, want %s", got, head)
	}
}

// A failing pre-pr-reviewed Decision fails the run with every accepted
// contract in the outputs and no Final. The workflow had already removed the
// snapshot, before the Decision.
func TestPrePRReviewDecisionFailure(t *testing.T) {
	f := newFixture(t)
	var head string
	var snapshotAtReturn error
	a := f.run(t, agents{
		// Taking the name makes the workflow's Decision fail.
		before: func(ctx context.Context, r *engine.Run) error {
			_, err := r.Root().Child("pre-pr-reviewed")
			return err
		},
		implement: f.implementReady(&head),
		returned:  func(r *engine.Run) { _, snapshotAtReturn = os.Lstat(filepath.Join(r.Dir(), snapshotDir, "checkout")) },
	})
	report := a.report
	requireFailure(t, report, "invalid or duplicate scope/decision name")
	if report.Final != nil || len(report.Result.Outputs) != 4 {
		t.Fatalf("final %+v outputs %+v", report.Final, report.Result.Outputs)
	}
	if !errors.Is(snapshotAtReturn, os.ErrNotExist) {
		t.Fatalf("snapshot checkout at workflow return: %v", snapshotAtReturn)
	}
	a.requireSessionsClosed(t, 3)
	a.requireDecision(t, "implementation-recorded")
}

// A reviewer contract the schema or the acceptance rejects is rewritten on
// the same session with the diagnostic as Feedback, under the ordinary
// dispatch text: reviewing again writes nothing.
func TestPrePRReviewRepair(t *testing.T) {
	item := func(id string) map[string]any { return map[string]any{"id": id, "summary": "s", "detail": "d"} }
	for _, tc := range []struct {
		name       string
		bad        func(c agentCall) map[string]any
		diagnostic string
	}{
		{"line-without-path", func(c agentCall) map[string]any {
			lined := item("f1")
			lined["line"] = 3
			return reviewData(c, lined)
		}, "'path' required, if 'line' exists"},
		{"duplicate-id", func(c agentCall) map[string]any {
			return reviewData(c, item("f1"), item("f2"), item("f1"))
		}, `findings[2].id: got "f1"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var head string
			a := f.run(t, agents{
				implement: f.implementReady(&head),
				review: func(t *testing.T, c agentCall) string {
					if c.task.Role == "basic" && c.request.Feedback == nil {
						writeCandidate(t, c, tc.bad(c))
						return ""
					}
					return acceptReview(t, c)
				},
			})
			report := a.report
			if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Output != "deep-review-report" {
				t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
			}
			a.requireAttempts(t, 10, "input", "pull-request")
			basic := a.reviewCalls()["basic"]
			if len(basic) != 2 {
				t.Fatalf("basic dispatches %d", len(basic))
			}
			first, repair := a.attempt(basic[0]), a.attempt(basic[1])
			if repair.HandleID != first.HandleID || repair.Scope != "root/pre-pr-review/basic/contract-repair-review" || report.Result.Outputs["review-basic"] != *repair.Output {
				t.Fatalf("repair handle %s (first %s) scope %s output %+v", repair.HandleID, first.HandleID, repair.Scope, report.Result.Outputs["review-basic"])
			}
			if fb := basic[1].request.Feedback; fb == nil || !strings.Contains(fb.Message, tc.diagnostic) || len(fb.Refs) != 0 {
				t.Fatalf("repair feedback %+v, want %q", fb, tc.diagnostic)
			}
			if basic[1].task != basic[0].task || !reflect.DeepEqual(basic[1].request.Inputs, basic[0].request.Inputs) {
				t.Fatalf("repair task or inputs changed")
			}
			var repairDispatch string
			for _, d := range a.runtime.dispatches {
				if strings.Contains(d, basic[1].request.Identity.AttemptID) {
					repairDispatch = d
				}
			}
			if repairDispatch == "" || strings.Contains(repairDispatch, "This is a contract repair") || !strings.Contains(repairDispatch, "First execute the task in request.prompt") {
				t.Fatalf("repair dispatch %q, want the ordinary task instruction", repairDispatch)
			}
		})
	}
}

// One less than the live sessions, sessions or attempts pre-PR review needs
// (three, three and four with the input Attach; TestPrePRReviewSucceeds runs
// at those live sessions) stops the run with a run-scope
// LimitExceeded after every started session's exit is confirmed.
func TestPrePRReviewLimits(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		live, sessions, attempts int
		want                     string
	}{
		{"live", 2, 3, 4, "session limit exceeded"},
		{"sessions", 3, 2, 4, "session limit exceeded"},
		{"attempts", 3, 3, 3, "run attempt limit exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var head string
			a := f.run(t, agents{
				implement: f.implementReady(&head),
				policy: func(p *engine.RunPolicy) {
					p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = tc.live, tc.sessions, tc.attempts
				},
			})
			report := a.report
			var failure *engine.Failure
			if report.Outcome != engine.Failed || !errors.As(report.Failure, &failure) || failure.Code != engine.LimitExceeded || failure.LimitScope != "run" || !strings.Contains(failure.Message, tc.want) {
				t.Fatalf("outcome %s failure %v, want a run LimitExceeded %q", report.Outcome, report.Failure, tc.want)
			}
			if report.Final != nil {
				t.Fatalf("final %+v", report.Final)
			}
			a.requireExitsConfirmed(t)
			a.requireSnapshotRemoved(t)
		})
	}
}

// Every field of the echoed implement Ref and the role must match, and
// finding IDs must be unique; all violations are reported together, each
// with its field, what was received and what is wanted.
func TestCheckReview(t *testing.T) {
	implement := contract.Ref{RunID: "run", AttemptID: "attempt", Path: "/runs/implement.json", SchemaID: ImplementSchema, SHA256: strings.Repeat("1", 64), ManifestSHA256: strings.Repeat("2", 64)}
	ids := func(ids ...string) findings {
		v := findings{Role: "basic", Implement: implement}
		for _, id := range ids {
			v.Findings = append(v.Findings, struct {
				ID string `json:"id"`
			}{id})
		}
		return v
	}
	if err := checkReview(ids("a", "b"), "basic", implement); err != nil {
		t.Fatalf("valid review rejected: %v", err)
	}
	v := ids("a", "b", "a")
	v.Role = "consistency"
	v.Implement.SHA256 = strings.Repeat("3", 64)
	got, _ := json.Marshal(v.Implement)
	want, _ := json.Marshal(implement)
	err := checkReview(v, "basic", implement)
	for _, diagnostic := range []string{
		`role: got "consistency"; want "basic", the role in request.prompt`,
		"implement: got " + string(got) + "; want the implement Ref copied byte-exact from request.inputs: " + string(want),
		`findings[2].id: got "a", which findings[0] already uses; want an ID unique among the findings`,
	} {
		if err == nil || !slices.Contains(strings.Split(err.Error(), "\n"), diagnostic) {
			t.Fatalf("checkReview = %v, want %q", err, diagnostic)
		}
	}
}
