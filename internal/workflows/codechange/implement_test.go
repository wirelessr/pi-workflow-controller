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
	"strings"
	"sync"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

// TestCodeChangeProtocolSubprocess is the fake Pi: it speaks the real RPC
// protocol, and the test host acts as the Agent at each prompt.
func TestCodeChangeProtocolSubprocess(t *testing.T) {
	if os.Getenv("PWC_CC_PROTOCOL") != "1" {
		return
	}
	for _, arg := range os.Args {
		if arg == "--version" {
			fmt.Println("0.84.3")
			os.Exit(0)
		}
	}
	if err := protocol.Serve(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

// recordingRuntime wraps only the runtime ownership boundary to record what
// the engine asked of it; the sessions are real RPC children.
type recordingRuntime struct {
	runtime.Runtime
	mu         sync.Mutex
	specs      []runtime.SessionSpec
	dispatches []string
	// close, when set, replaces the close of the role's sessions; it stands
	// for a runtime that reports a close the engine does not confirm.
	close func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error)
}

type recordingSession struct {
	runtime.Session
	owner *recordingRuntime
	role  string
}

func (r *recordingRuntime) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	r.mu.Lock()
	r.specs = append(r.specs, spec)
	r.mu.Unlock()
	s, err := r.Runtime.Start(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &recordingSession{Session: s, owner: r, role: spec.Name}, nil
}

func (s *recordingSession) Close(ctx context.Context) (runtime.CleanupReport, error) {
	if s.owner.close != nil {
		return s.owner.close(ctx, s.role, s.Session)
	}
	return s.Session.Close(ctx)
}

func (s *recordingSession) Execute(ctx context.Context, dispatch runtime.Dispatch) (runtime.Execution, error) {
	s.owner.mu.Lock()
	s.owner.dispatches = append(s.owner.dispatches, dispatch.Message)
	s.owner.mu.Unlock()
	return s.Session.Execute(ctx, dispatch)
}

// agentCall is one dispatch as the fake Agent sees it.
type agentCall struct {
	n         int
	request   contract.Request
	task      task
	candidate string
}

type a1Run struct {
	report  engine.Report
	run     *engine.Run
	hellos  []protocol.Control
	calls   []agentCall
	runtime *recordingRuntime
	// defaultCWD is the engine's Pi default directory, which the implementer
	// must not get.
	defaultCWD string
}

// agents act as the Agent at each dispatch: they may change the worktree,
// write the candidate and return the fake Pi's answer: "" to settle, "hold"
// to keep the prompt running, which cancels the run, "defer" to answer it
// later with settle, or another prompt barrier answer.
type agents struct {
	implement func(t *testing.T, c agentCall) string
	// review answers both reviewers; nil accepts with no findings.
	review func(t *testing.T, c agentCall) string
	// release reports whether the deferred prompts are settled after c was
	// answered; nil settles them after any answer that is not "defer".
	release func(t *testing.T, r *engine.Run, c agentCall) bool
	// policy adjusts the workflow's run policy.
	policy func(*engine.RunPolicy)
	// before runs before the workflow, to take a name the workflow needs.
	before func(ctx context.Context, r *engine.Run) error
	// returned observes the workflow's return, before the run's cleanup.
	returned func(r *engine.Run)
	// close is the recording runtime's close of a role's sessions.
	close func(ctx context.Context, role string, s runtime.Session) (runtime.CleanupReport, error)
}

// run executes the workflow with only its GitHub git remote replaced, over
// the real engine, Store, runtime and Pi RPC, with as acting as the Agent.
func (f *fixture) run(t *testing.T, as agents) a1Run {
	t.Helper()
	bridge := filepath.Join(f.dir, "bridge")
	defaultCWD := filepath.Join(f.dir, "default-cwd")
	for _, d := range []string{bridge, defaultCWD} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	host, err := protocol.NewHost(ctx, 16)
	if err != nil {
		t.Fatal(err)
	}
	var r *engine.Run
	done := make(chan struct{})
	protocol.RegisterCleanup(t, host, func() <-chan struct{} {
		cancel()
		if r != nil {
			r.Cancel(engine.OriginControllerUser)
			return done
		}
		return nil
	}, 8*time.Second, "fixture host close: ", "fixture run did not join")
	def := f.definition(func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
		if as.before != nil {
			if err := as.before(ctx, r); err != nil {
				return engine.Result{}, err
			}
		}
		result, err := execute(ctx, r, in, f.piCWD, f.fileSource())
		if as.returned != nil {
			as.returned(r)
		}
		return result, err
	})
	if as.policy != nil {
		as.policy(&def.Policy)
	}
	def.Policy.Runtime.StartupTimeout = 5 * time.Second
	def.Policy.Runtime.CleanupTimeout = 3 * time.Second
	def.Policy.Runtime.AbortGrace = 50 * time.Millisecond
	def.Policy.Runtime.HealthInterval = time.Hour
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pi, err := runtime.New(runtime.Options{Executable: exe, Args: []string{"-test.run=^TestCodeChangeProtocolSubprocess$", "--"},
		Env:       []string{"PWC_CC_PROTOCOL=1", "PWC_ENGINE_MANUAL_CANDIDATE=1", "PWC_ENGINE_CONTROL=" + host.Addr().String(), "GORACE=atexit_sleep_ms=0"},
		BridgeDir: bridge, Policy: def.Policy.Runtime, Observe: func(ctx context.Context, o runtime.Observation) error { return r.Observe(ctx, o) }})
	if err != nil {
		t.Fatal(err)
	}
	rt := &recordingRuntime{Runtime: pi, close: as.close}
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	// The fixture deadline cancels the run through the cleanup above.
	r, err = engine.New(context.Background(), def, engine.Input{Prompt: prompt, LaunchCWD: f.launch},
		engine.Options{BaseDir: filepath.Join(f.dir, "base"), Schemas: registry, Runtime: rt, PiVersion: "0.84.3", PiDefaultCWD: defaultCWD})
	if err != nil {
		t.Fatal(err)
	}
	out := a1Run{run: r, runtime: rt, defaultCWD: defaultCWD}
	go func() { out.report = r.Execute(); close(done) }()
	var deferred []protocol.Event
	for {
		select {
		case <-done:
			return out
		case <-ctx.Done():
			t.Fatal("workflow exceeded the fixture deadline")
		case e, ok := <-host.Events():
			if !ok {
				t.Fatal("fixture host events closed")
			}
			if e.Err != nil {
				t.Fatalf("fixture host event: %v", e.Err)
			}
			switch e.Message.Type {
			case "hello":
				out.hellos = append(out.hellos, e.Message)
			case "held":
				r.Cancel(engine.OriginControllerUser)
			case "prompt":
				c := agentCall{n: len(out.calls) + 1, candidate: e.Message.CandidatePath}
				if err := protocol.ReadJSON(e.Message.RequestPath, &c.request); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(c.request.Prompt), &c.task); err != nil {
					t.Fatalf("request prompt is not a task: %v", err)
				}
				out.calls = append(out.calls, c)
				agent := as.implement
				if c.task.Role != "implementer" {
					agent = as.review
					if agent == nil {
						agent = acceptReview
					}
				}
				ack := agent(t, c)
				if ack == "defer" {
					deferred = append(deferred, e)
					continue
				}
				if ack == "" {
					ack = "settle"
				}
				if err := e.Reply(protocol.Control{Type: ack}); err != nil {
					t.Fatal(err)
				}
				if as.release == nil || as.release(t, r, c) {
					for _, d := range deferred {
						if err := d.Reply(protocol.Control{Type: "settle"}); err != nil {
							t.Fatal(err)
						}
					}
					deferred = nil
				}
			}
		}
	}
}

// commit is the fake Agent's local commit in the worktree; it returns HEAD.
func (f *fixture) commit(t *testing.T, worktree, content string) string {
	t.Helper()
	f.write(filepath.Join(worktree, "tracked.txt"), content)
	f.gitIn(worktree, "commit", "-qam", "change")
	return f.gitIn(worktree, "rev-parse", "HEAD")
}

func readyData(c agentCall, head string) map[string]any {
	return map[string]any{"readiness": "ready", "input": c.request.Inputs[0], "ticket": "ABC-1", "plan": "Change tracked.txt",
		"report":     map[string]any{"changes": "Updated tracked.txt", "verification": []any{map[string]any{"name": "unit tests", "status": "not_checked", "detail": "the fixture has no test suite"}}},
		"local_head": head}
}

func gapsData(c agentCall) map[string]any {
	return map[string]any{"readiness": "gaps", "input": c.request.Inputs[0], "gaps": []string{"the design doc names no ticket"}}
}

func writeCandidate(t *testing.T, c agentCall, data any) {
	t.Helper()
	if err := protocol.WriteEnvelope(c.candidate, c.request, data, []contract.FileEntry{}); err != nil {
		t.Fatal(err)
	}
}

// requireSessionsClosed checks the run started n sessions and confirmed each
// one's process exit before returning, without cleanup errors.
func (a a1Run) requireSessionsClosed(t *testing.T, n int) {
	t.Helper()
	if len(a.report.Snapshot.Sessions) != n || len(a.hellos) != n || len(a.runtime.specs) != n {
		t.Fatalf("sessions %d hellos %d starts %d, want %d", len(a.report.Snapshot.Sessions), len(a.hellos), len(a.runtime.specs), n)
	}
	a.requireExitsConfirmed(t)
}

// requireExitsConfirmed checks the run confirmed the process exit of every
// session it started before returning, without cleanup errors.
func (a a1Run) requireExitsConfirmed(t *testing.T) {
	t.Helper()
	// The run's cleanup reports every handle, also those the workflow closed.
	for _, hello := range a.hellos {
		confirmed := false
		for _, c := range a.report.Cleanup {
			confirmed = confirmed || c.ConfirmsLocalClose(hello.SessionID)
		}
		if !confirmed {
			t.Fatalf("session %s exit not confirmed: %+v", hello.SessionID, a.report.Cleanup)
		}
	}
	if len(a.report.CleanupErrors) != 0 {
		t.Fatalf("cleanup errors: %v", a.report.CleanupErrors)
	}
}

// requireAttempts checks the run's accounting: the input Attach plus steps
// Agent attempts.
func (a a1Run) requireAttempts(t *testing.T, steps int) {
	t.Helper()
	if len(a.report.Snapshot.Attempts) != steps+1 || len(a.calls) != steps || len(a.runtime.dispatches) != steps {
		t.Fatalf("attempts %d calls %d dispatches %d, want %d implementer attempts", len(a.report.Snapshot.Attempts), len(a.calls), len(a.runtime.dispatches), steps)
	}
}

func (a a1Run) worktree() string {
	return filepath.Join(a.run.Dir(), worktreeDir, "checkout")
}

type decision struct {
	Scope string         `json:"scope"`
	Name  string         `json:"name"`
	Refs  []contract.Ref `json:"refs"`
}

type event struct {
	Kind    string          `json:"kind"`
	Details json.RawMessage `json:"details"`
}

// events are the run's journal events in commit order.
func (a a1Run) events(t *testing.T) []event {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(a.run.Dir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var out []event
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var e event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

// decisions are the run's Decision events as committed to the journal.
func (a a1Run) decisions(t *testing.T) []decision {
	t.Helper()
	var out []decision
	for _, e := range a.events(t) {
		if e.Kind == "Decision" {
			var d decision
			if err := json.Unmarshal(e.Details, &d); err != nil {
				t.Fatal(err)
			}
			out = append(out, d)
		}
	}
	return out
}

// requireDecision checks the run's only Decision is name on the implement contract.
func (a a1Run) requireDecision(t *testing.T, name string) {
	t.Helper()
	a.requireDecisions(t, decision{Scope: "root", Name: name, Refs: []contract.Ref{a.report.Result.Outputs["implement"]}})
}

func (a a1Run) requireDecisions(t *testing.T, want ...decision) {
	t.Helper()
	if got := a.decisions(t); !reflect.DeepEqual(got, want) {
		t.Fatalf("decisions %+v, want %+v", got, want)
	}
}

// requireNoSnapshot checks the run never created the pre-PR review snapshot.
func (a a1Run) requireNoSnapshot(t *testing.T) {
	t.Helper()
	requireMissing(t, filepath.Join(a.run.Dir(), snapshotDir))
}

func TestImplementReadyKeepsWorktree(t *testing.T) {
	f := newFixture(t)
	var head string
	a := f.run(t, agents{implement: func(t *testing.T, c agentCall) string {
		head = f.commit(t, c.task.Worktree, "implemented\n")
		writeCandidate(t, c, readyData(c, head))
		return ""
	}})
	report := a.report
	if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil {
		t.Fatalf("outcome %s exit %d failure %v", report.Outcome, report.ExitCode, report.Failure)
	}
	if report.Final == nil || report.Final.Output != "implement" || report.Final.Step != "implement" || report.Final.Ref != report.Result.Outputs["implement"] {
		t.Fatalf("final: %+v", report.Final)
	}
	// The implementer's session and both reviewers'.
	a.requireSessionsClosed(t, 3)
	a.requireAttempts(t, 3)
	a.requireDecisions(t, decision{Scope: "root", Name: "implementation-recorded", Refs: []contract.Ref{report.Result.Outputs["implement"]}},
		decision{Scope: "root", Name: "pre-pr-reviewed", Refs: []contract.Ref{report.Result.Outputs["review-basic"], report.Result.Outputs["review-consistency"]}})
	// The implementer runs in PWC_PI_CWD, not the engine default, and is
	// given the worktree by absolute path.
	spec := a.runtime.specs[0]
	for _, cwd := range []string{spec.CWD, a.hellos[0].CWD, report.Snapshot.Sessions[report.Snapshot.Attempts[report.Final.Ref.AttemptID].HandleID].Role.CWD} {
		if cwd != f.piCWD || cwd == a.defaultCWD {
			t.Fatalf("implementer cwd %q, want PWC_PI_CWD %q", cwd, f.piCWD)
		}
	}
	c := a.calls[0]
	if c.task.Worktree != a.worktree() || !reflect.DeepEqual(c.request.Inputs, []contract.Ref{report.Result.Outputs["input"]}) {
		t.Fatalf("task worktree %q inputs %+v", c.task.Worktree, c.request.Inputs)
	}
	if got := f.gitIn(a.worktree(), "rev-parse", "HEAD"); got != head {
		t.Fatalf("worktree HEAD %s, want the Agent's commit %s", got, head)
	}
	// After the worktree is handed over, the Controller's git never runs on
	// its repository except to read the worktree registration, with lazy
	// fetches and replace refs disabled; the snapshot's git, under the same
	// policy and environment, only creates the snapshot in its own repository.
	repository := filepath.Join(a.run.Dir(), worktreeDir, "repository.git")
	readPolicy := append(append(controllerPolicy(filepath.Join(repository, "worktrees", "checkout")), fileProtocol...), "--no-lazy-fetch", "--no-replace-objects")
	snapshotPolicy := append(controllerPolicy(a.snapshotRepo()), fileProtocol...)
	calls := f.gitCalls()
	handed := slices.IndexFunc(calls, func(c gitCall) bool {
		return subcommand(c.Args) == "worktree" && slices.Contains(c.Args, "--git-dir="+repository)
	})
	if handed < 0 {
		t.Fatalf("no worktree creation in %q", calls)
	}
	var reads, snapshot []string
	for _, call := range calls[handed+1:] {
		sub := subcommand(call.Args)
		policy := call.Args[slices.Index(call.Args, "--no-pager"):slices.Index(call.Args, sub)]
		switch {
		case !slices.Contains(call.Env, "GIT_CONFIG_GLOBAL=/dev/null"):
			t.Fatalf("git call %q env %q", call.Args, call.Env)
		case slices.Equal(policy, readPolicy):
			reads = append(reads, sub)
		case slices.Equal(policy, snapshotPolicy):
			snapshot = append(snapshot, sub)
		default:
			t.Fatalf("git call %q after the worktree was handed over, want a registration read or the snapshot's creation", call.Args)
		}
	}
	// The receipt's HEAD and history, then the HEAD after the review.
	if !reflect.DeepEqual(reads, []string{"rev-parse", "rev-list", "rev-parse"}) || !reflect.DeepEqual(snapshot, []string{"init", "worktree"}) {
		t.Fatalf("registration reads %v, snapshot git %v", reads, snapshot)
	}
}

// Gaps reported on an unchanged worktree are a legitimate incomplete result:
// the contract is the Final and the run fails with the named error.
func TestImplementGapsDeliverFinal(t *testing.T) {
	f := newFixture(t)
	a := f.run(t, agents{implement: func(t *testing.T, c agentCall) string {
		writeCandidate(t, c, gapsData(c))
		return ""
	}})
	report := a.report
	if report.Outcome != engine.Failed || report.ExitCode != 1 || !errors.Is(report.Failure, errDesignGaps) {
		t.Fatalf("outcome %s exit %d failure %v, want Failed with errDesignGaps", report.Outcome, report.ExitCode, report.Failure)
	}
	if report.Final == nil || report.Final.Output != "implement" || report.Final.Ref != report.Result.Outputs["implement"] {
		t.Fatalf("final: %+v", report.Final)
	}
	a.requireSessionsClosed(t, 1)
	a.requireAttempts(t, 1)
	a.requireDecision(t, "design-gaps-reported")
	a.requireNoSnapshot(t)
	if got := f.gitIn(a.worktree(), "rev-parse", "HEAD"); got != f.head {
		t.Fatalf("worktree HEAD %s, want base %s", got, f.head)
	}
}

// A receipt the worktree does not confirm, or cannot be read, fails the run
// once the contract was accepted: no repair, no Decision, no Final, the
// committed contracts and the worktree kept.
func TestImplementReceiptRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		// act changes the worktree like the Agent and returns the contract.
		act  func(t *testing.T, f *fixture, c agentCall) map[string]any
		want string
		// unreadable is a read failure rather than a mismatch.
		unreadable bool
	}{
		{"unreadable-head", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			head := f.commit(t, c.task.Worktree, "implemented\n")
			f.write(filepath.Join(filepath.Dir(c.task.Worktree), "repository.git", "worktrees", "checkout", "HEAD"), "not a head\n")
			return readyData(c, head)
		}, "worktree HEAD: ", true},
		{"unreadable-history", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			f.write(filepath.Join(f.dir, "git-fail"), "rev-list")
			return readyData(c, f.commit(t, c.task.Worktree, "implemented\n"))
		}, "worktree history: ", true},
		{"gaps-after-commit", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			f.commit(t, c.task.Worktree, "implemented\n")
			return gapsData(c)
		}, "; want it unchanged at base", false},
		{"not-the-worktree-head", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			first := f.commit(t, c.task.Worktree, "first\n")
			f.commit(t, c.task.Worktree, "second\n")
			return readyData(c, first)
		}, "; want the worktree HEAD", false},
		{"base-reported", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			return readyData(c, f.head)
		}, " or one of its ancestors;", false},
		{"ancestor-of-base", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			f.gitIn(c.task.Worktree, "reset", "-q", "--soft", "HEAD~1")
			return readyData(c, f.gitIn(c.task.Worktree, "rev-parse", "HEAD"))
		}, " or one of its ancestors;", false},
		// A replace ref gives the base a history without its parent, which
		// the Controller's read ignores.
		{"ancestor-of-replaced-base", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			f.gitIn(c.task.Worktree, "reset", "-q", "--soft", "HEAD~1")
			f.gitIn(c.task.Worktree, "replace", f.head, f.gitIn(c.task.Worktree, "commit-tree", "-m", "orphan", f.head+"^{tree}"))
			return readyData(c, f.gitIn(c.task.Worktree, "rev-parse", "HEAD"))
		}, " or one of its ancestors;", false},
		{"unknown-commit", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			f.commit(t, c.task.Worktree, "implemented\n")
			return readyData(c, strings.Repeat("ab", 20))
		}, "; want the worktree HEAD", false},
		// The checkout's .git file points at another repository whose HEAD
		// is reported; the Controller still reads the registration.
		{"dot-git-redirected", func(t *testing.T, f *fixture, c agentCall) map[string]any {
			f.commit(t, c.task.Worktree, "implemented\n")
			other := filepath.Join(f.dir, "other")
			if err := os.Mkdir(other, 0700); err != nil {
				t.Fatal(err)
			}
			f.gitIn(other, "init", "-q", "--template=")
			f.gitIn(other, "commit", "-q", "--allow-empty", "-m", "other")
			f.write(filepath.Join(c.task.Worktree, ".git"), "gitdir: "+filepath.Join(other, ".git")+"\n")
			return readyData(c, f.gitIn(other, "rev-parse", "HEAD"))
		}, "; want the worktree HEAD", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var call agentCall
			a := f.run(t, agents{implement: func(t *testing.T, c agentCall) string {
				call = c
				writeCandidate(t, c, tc.act(t, f, c))
				return ""
			}})
			report := a.report
			if report.Outcome != engine.Failed || report.ExitCode != 1 || errors.Is(report.Failure, errReceipt) == tc.unreadable || !strings.Contains(report.Failure.Error(), tc.want) {
				t.Fatalf("outcome %s exit %d failure %v, want Failed with %q (errReceipt: %v)", report.Outcome, report.ExitCode, report.Failure, tc.want, !tc.unreadable)
			}
			if report.Final != nil {
				t.Fatalf("final delivered: %+v", report.Final)
			}
			implement := report.Snapshot.Attempts[call.request.Identity.AttemptID].Output
			if implement == nil || !reflect.DeepEqual(report.Result.Outputs, map[string]contract.Ref{"input": call.request.Inputs[0], "implement": *implement}) {
				t.Fatalf("outputs %+v, want the input and the accepted implement contract", report.Result.Outputs)
			}
			if got := a.decisions(t); len(got) != 0 {
				t.Fatalf("decisions recorded: %+v", got)
			}
			a.requireSessionsClosed(t, 1)
			a.requireAttempts(t, 1)
			a.requireNoSnapshot(t)
			if info, err := os.Stat(filepath.Join(a.run.Dir(), worktreeDir, "repository.git", "worktrees", "checkout")); err != nil || !info.IsDir() {
				t.Fatalf("worktree registration not kept: %v", err)
			}
		})
	}
}

// Every field of the echoed input Ref must match.
func TestCheckImplementationEcho(t *testing.T) {
	input := contract.Ref{RunID: "run", AttemptID: "attempt", Path: "/runs/input.json", SchemaID: InputSchema, SHA256: strings.Repeat("1", 64), ManifestSHA256: strings.Repeat("2", 64)}
	if err := checkEcho("input", input, input); err != nil {
		t.Fatalf("exact echo rejected: %v", err)
	}
	for name, change := range map[string]func(*contract.Ref){
		"run_id":          func(r *contract.Ref) { r.RunID = "other" },
		"attempt_id":      func(r *contract.Ref) { r.AttemptID = "other" },
		"path":            func(r *contract.Ref) { r.Path = "/runs/other.json" },
		"schema_id":       func(r *contract.Ref) { r.SchemaID = ImplementSchema },
		"sha256":          func(r *contract.Ref) { r.SHA256 = strings.Repeat("3", 64) },
		"manifest_sha256": func(r *contract.Ref) { r.ManifestSHA256 = strings.Repeat("3", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			echoed := input
			change(&echoed)
			if err := checkEcho("input", echoed, input); err == nil || !strings.HasPrefix(err.Error(), "input: got ") {
				t.Fatalf("changed %s accepted: %v", name, err)
			}
		})
	}
}

// A contract the schema or the echo check rejects is rewritten on the same
// session with the diagnostic as Feedback, under the repair dispatch text;
// the Agent commits once and the repairs only rewrite the contract.
func TestImplementRepair(t *testing.T) {
	for _, tc := range []struct {
		name string
		// bad is the first attempt's contract.
		bad func(c agentCall, head string) map[string]any
		// diagnostic is part of the repair Feedback.
		diagnostic func(input contract.Ref) string
	}{
		{"schema", func(c agentCall, head string) map[string]any {
			data := readyData(c, head)
			delete(data, "local_head")
			return data
		}, func(contract.Ref) string { return "local_head" }},
		{"echo", func(c agentCall, head string) map[string]any {
			data := readyData(c, head)
			ref := c.request.Inputs[0]
			ref.AttemptID = "other-attempt"
			data["input"] = ref
			return data
		}, func(input contract.Ref) string {
			got := input
			got.AttemptID = "other-attempt"
			raw, _ := json.Marshal(got)
			return "input: got " + string(raw) + "; want the input Ref copied byte-exact"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			var head string
			a := f.run(t, agents{implement: func(t *testing.T, c agentCall) string {
				if c.n == 1 {
					head = f.commit(t, c.task.Worktree, "implemented\n")
					writeCandidate(t, c, tc.bad(c, head))
				} else {
					writeCandidate(t, c, readyData(c, head))
				}
				return ""
			}})
			report := a.report
			if report.Outcome != engine.Succeeded || report.Final == nil || report.Final.Ref != report.Result.Outputs["implement"] {
				t.Fatalf("outcome %s failure %v final %+v", report.Outcome, report.Failure, report.Final)
			}
			// Two implementer attempts and one per reviewer.
			a.requireAttempts(t, 4)
			if got := f.gitIn(a.worktree(), "rev-list", "--count", f.head+"..HEAD"); got != "1" {
				t.Fatalf("commits on base: %s, want the single commit of the first attempt", got)
			}
			fb := a.calls[1].request.Feedback
			if fb == nil || !strings.Contains(fb.Message, tc.diagnostic(a.calls[0].request.Inputs[0])) {
				t.Fatalf("repair feedback %+v, want %q", fb, tc.diagnostic(a.calls[0].request.Inputs[0]))
			}
			const rewrite = "This is a contract repair: do not add or repeat any external write or local commit"
			if strings.Contains(a.runtime.dispatches[0], rewrite) || !strings.Contains(a.runtime.dispatches[1], rewrite) {
				t.Fatalf("dispatches %q, want only the repair to use the RepairKeepsWork instruction", a.runtime.dispatches)
			}
		})
	}
}

// Cancelling while the implementer runs ends the run Cancelled, after the
// session's exit is confirmed, with the worktree kept.
func TestImplementCancelled(t *testing.T) {
	f := newFixture(t)
	a := f.run(t, agents{implement: func(t *testing.T, c agentCall) string {
		f.commit(t, c.task.Worktree, "partial\n")
		return "hold"
	}})
	report := a.report
	if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil {
		t.Fatalf("outcome %s exit %d final %+v", report.Outcome, report.ExitCode, report.Final)
	}
	a.requireSessionsClosed(t, 1)
	a.requireAttempts(t, 1)
	a.requireNoSnapshot(t)
	if raw, err := os.ReadFile(filepath.Join(a.worktree(), "tracked.txt")); err != nil || string(raw) != "partial\n" {
		t.Fatalf("worktree not kept: %q %v", raw, err)
	}
}
