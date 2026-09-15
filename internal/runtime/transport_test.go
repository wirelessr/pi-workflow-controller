package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestCloseSlowObserverRemovesDiscoveryBeforeObserverStops(t *testing.T) {
	for _, cooperative := range []bool{true, false} {
		t.Run(fmt.Sprint(cooperative), func(t *testing.T) {
			entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			f := mustFixture(t, "normal", func(o *Options) {
				o.Policy.CleanupTimeout = 600 * time.Millisecond
				o.Observe = func(ctx context.Context, _ Observation) error {
					once.Do(func() {
						close(entered)
						<-ctx.Done()
						close(cancelled)
						if !cooperative {
							<-release
						}
					})
					return ctx.Err()
				}
			})
			t.Cleanup(func() { close(release); <-f.s.observerDone })
			select {
			case <-entered:
			case <-f.ctx.Done():
				t.Fatal("observer did not enter")
			}
			path := filepath.Join(f.bridge, "fixture-session.json")
			if err := os.WriteFile(f.s.Identity().SessionFile, []byte("preserved history"), 0600); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			var report CleanupReport
			var err error
			go func() { report, err = f.s.Close(context.Background()); close(done) }()
			select {
			case <-cancelled:
			case <-f.ctx.Done():
				t.Fatal("observer was not cancelled")
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("discovery still exists when observer is cancelled: %v", err)
			}
			select {
			case <-done:
			case <-f.ctx.Done():
				t.Fatal("Close did not finish")
			}
			_ = requireCode(t, err, CleanupFailed)
			if !report.WaitCompleted || report.DiscoveryError != "" || len(report.DiscoveryRemoved) != 1 {
				t.Fatalf("observer blocked owned cleanup: %+v", report)
			}
			diagnostics := strings.Join(report.Unconfirmed, ";")
			if !strings.Contains(diagnostics, "observer delivery unconfirmed") || strings.Contains(diagnostics, "observer worker did not stop") == cooperative {
				t.Fatalf("incorrect observer confirmation: %+v", report)
			}
			if cooperative {
				select {
				case <-f.s.observerDone:
				default:
					t.Fatal("cooperative observer not joined")
				}
			}
			if b, err := os.ReadFile(f.s.Identity().SessionFile); err != nil || string(b) != "preserved history" {
				t.Fatalf("cleanup modified history: %q %v", b, err)
			}
		})
	}
}

// This entry point only models startup barriers; dispatch/tool/entries tests use TestPiSubprocess.
func TestStartupPiSubprocess(t *testing.T) {
	phase := os.Getenv("PWC_STARTUP_PHASE")
	if phase == "" {
		return
	}
	version := false
	dir := ""
	for i, arg := range os.Args {
		if arg == "--version" {
			version = true
		}
		if arg == "--session-dir" && i+1 < len(os.Args) {
			dir = os.Args[i+1]
		}
	}
	if version && phase != "version" {
		fmt.Println("0.84.3")
		os.Exit(0)
	}
	conn, err := net.Dial("tcp", os.Getenv("PWC_CONTROL"))
	if err != nil {
		os.Exit(3)
	}
	ctl := json.NewEncoder(conn)
	_ = ctl.Encode(control{Type: "hello", PID: os.Getpid()})
	if version {
		_ = ctl.Encode(control{Type: "held"})
		var c control
		if json.NewDecoder(conn).Decode(&c) != nil {
			os.Exit(4)
		}
		fmt.Println("0.84.3")
		os.Exit(0)
	}
	sid := "fixture-session"
	file := filepath.Join(dir, "history.jsonl")
	if phase != "discovery" {
		b, _ := json.Marshal(discovery{SessionID: sid, SessionFile: file, PID: os.Getppid(), PiPID: os.Getpid()})
		if os.WriteFile(filepath.Join(os.Getenv("PI_BRIDGE_DIR"), sid+".json"), b, 0600) != nil {
			os.Exit(5)
		}
	}
	state := map[string]any{"sessionId": sid, "sessionFile": file, "model": map[string]any{"provider": "fixture", "id": "model"}, "thinkingLevel": "high", "isStreaming": false, "isCompacting": false, "pendingMessageCount": 0}
	commands := make(chan map[string]any)
	controls := make(chan control)
	go func() {
		d := json.NewDecoder(os.Stdin)
		for {
			var c map[string]any
			if d.Decode(&c) != nil {
				os.Exit(0)
			}
			commands <- c
		}
	}()
	go func() {
		d := json.NewDecoder(conn)
		for {
			var c control
			if d.Decode(&c) != nil {
				os.Exit(0)
			}
			controls <- c
		}
	}()
	out := json.NewEncoder(os.Stdout)
	reply := func(c map[string]any, data any) {
		_ = out.Encode(map[string]any{"type": "response", "id": c["id"], "command": c["type"], "success": true, "data": data})
	}
	var held map[string]any
	for {
		select {
		case c := <-commands:
			switch c["type"] {
			case "get_state":
				if phase == "readiness" {
					held = c
					_ = ctl.Encode(control{Type: "held"})
				} else {
					reply(c, state)
					_ = ctl.Encode(control{Type: "discovery"})
				}
			case "abort", "abort_bash":
				reply(c, nil)
			}
		case c := <-controls:
			if c.Type == "release-state" && held != nil {
				reply(held, state)
				held = nil
			}
			_ = ctl.Encode(control{Type: "barrier", ID: c.ID})
		}
	}
}

type startupResult struct {
	s   Session
	err error
}

func newStartupFixture(t *testing.T, phase string, parent context.Context, policy Policy) (*fixture, <-chan startupResult) {
	t.Helper()
	f := &fixture{t: t, dir: t.TempDir(), events: make(chan control, 16)}
	f.bridge = filepath.Join(f.dir, "bridge")
	if err := os.Mkdir(f.bridge, 0700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.ctx, f.cancel = context.WithTimeout(context.Background(), 10*time.Second)
	ctx, cancel := context.WithCancel(parent)
	accepted := make(chan net.Conn, 1)
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		c, err := listener.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{Executable: exe, Args: []string{"-test.run=^TestStartupPiSubprocess$", "--"}, Env: []string{"GORACE=atexit_sleep_ms=0", "PWC_STARTUP_PHASE=" + phase, "PWC_CONTROL=" + listener.Addr().String()}, BridgeDir: f.bridge, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	done, finished := make(chan startupResult, 1), make(chan struct{})
	var started Session
	go func() {
		s, err := p.Start(ctx, SessionSpec{HandleID: "startup-handle", Name: "test", Model: ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}, CWD: f.dir, SessionDir: filepath.Join(f.dir, "session", "pi")})
		started = s
		done <- startupResult{s: s, err: err}
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		<-finished
		if started != nil {
			_, _ = started.Close(context.Background())
		}
		f.cancel()
		if f.conn != nil {
			_ = f.conn.Close()
		}
		_ = listener.Close()
		<-acceptDone
		select {
		case conn := <-accepted:
			_ = conn.Close()
		default:
		}
	})
	select {
	case f.conn = <-accepted:
	case r := <-done:
		t.Fatalf("Start returned before startup barrier: %v", r.err)
	case <-f.ctx.Done():
		t.Fatal("startup fixture accept timeout")
	}
	f.encoder = json.NewEncoder(f.conn)
	go func() {
		d := json.NewDecoder(f.conn)
		for {
			var c control
			if d.Decode(&c) != nil {
				return
			}
			select {
			case f.events <- c:
			case <-f.ctx.Done():
				return
			}
		}
	}()
	f.pid = f.next("hello").PID
	return f, done
}

func startupPolicy() Policy {
	p := DefaultPolicy()
	p.StartupTimeout = 3 * time.Second
	p.RPCTimeout = 40 * time.Millisecond
	p.AbortGrace = 100 * time.Millisecond
	p.CleanupTimeout = time.Second
	p.HealthInterval = time.Hour
	return p
}

func TestStartupReadinessUsesStartupBudget(t *testing.T) {
	for _, release := range []bool{true, false} {
		t.Run(fmt.Sprint(release), func(t *testing.T) {
			policy := startupPolicy()
			if !release {
				policy.StartupTimeout = 500 * time.Millisecond
			}
			f, done := newStartupFixture(t, "readiness", context.Background(), policy)
			f.next("held")
			select {
			case r := <-done:
				t.Fatalf("readiness used ordinary response timeout: %+v", r)
			case <-time.After(3 * policy.RPCTimeout):
			}
			if release {
				f.send(control{Type: "release-state"})
			}
			var r startupResult
			select {
			case r = <-done:
			case <-f.ctx.Done():
				t.Fatal("Start did not finish")
			}
			if !release {
				got := requireCode(t, r.err, StartFailed)
				if got.Origin != Protocol || got.Phase != "readiness" || !errors.Is(got, context.DeadlineExceeded) || got.Cleanup == nil || !got.Cleanup.WaitCompleted || len(got.Cleanup.DiscoveryRemoved) != 1 {
					t.Fatalf("incorrect startup timeout/cleanup: %+v", got)
				}
			} else {
				if r.err != nil {
					t.Fatal(r.err)
				}
				ordinary := make(chan error, 1)
				go func() { _, err := r.s.Snapshot(f.ctx); ordinary <- err }()
				f.next("held")
				select {
				case err := <-ordinary:
					_ = requireCode(t, err, RPCUnresponsive)
				case <-time.After(10 * policy.RPCTimeout):
					t.Fatal("ordinary query inherited startup timeout")
				}
				report, _ := r.s.Close(context.Background())
				if !report.WaitCompleted || len(report.DiscoveryRemoved) != 1 {
					t.Fatalf("ordinary timeout leaked child/discovery: %+v", report)
				}
			}
			if _, err := os.Stat(filepath.Join(f.bridge, "fixture-session.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("startup test left discovery: %v", err)
			}
		})
	}
}

func TestStartupContextCauseClassification(t *testing.T) {
	for _, phase := range []string{"version", "readiness", "discovery"} {
		for _, tc := range []struct {
			name   string
			code   Code
			origin Origin
		}{{"caller", Cancelled, ControllerUser}, {"sigterm", Cancelled, SignalTERM}, {"run-deadline", TimedOut, RunDeadline}, {"startup-deadline", StartFailed, Protocol}} {
			t.Run(phase+"/"+tc.name, func(t *testing.T) {
				root := errors.New("controller stop cause")
				cause := &Failure{Code: tc.code, Origin: tc.origin, Cause: root, Message: "typed parent stop", RunID: "run-id"}
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				parent := ctx
				if tc.origin == RunDeadline {
					deadlineCtx, stop := context.WithTimeoutCause(ctx, 500*time.Millisecond, cause)
					defer stop()
					parent = deadlineCtx
				}
				policy := startupPolicy()
				if tc.name == "startup-deadline" {
					policy.StartupTimeout = 500 * time.Millisecond
				}
				f, done := newStartupFixture(t, phase, parent, policy)
				if phase == "discovery" {
					f.next("discovery")
					// owner.json is written only after the readiness response has been validated.
					for {
						if b, err := os.ReadFile(filepath.Join(f.dir, "session", "owner.json")); err == nil && json.Valid(b) {
							break
						}
						select {
						case r := <-done:
							t.Fatalf("Start failed before discovery phase: %v", r.err)
						case <-f.ctx.Done():
							t.Fatal("readiness owner not written")
						case <-time.After(time.Millisecond):
						}
					}
				} else {
					f.next("held")
				}
				select {
				case r := <-done:
					t.Fatalf("Start ended before parent/startup deadline: %v", r.err)
				case <-time.After(3 * policy.RPCTimeout):
				}
				if tc.code == Cancelled {
					cancel(cause)
				}
				var r startupResult
				select {
				case r = <-done:
				case <-f.ctx.Done():
					t.Fatal("Start ignored context termination")
				}
				code := tc.code
				if tc.name == "startup-deadline" && phase == "discovery" {
					code = BridgeUnavailable
				}
				got := requireCode(t, r.err, code)
				if got.Origin != tc.origin || got.Phase != phase || got.HandleID != "startup-handle" || got.DispatchAccepted != AcceptedNo || r.s != nil {
					t.Fatalf("incorrect startup classification: %+v", got)
				}
				if tc.name == "startup-deadline" {
					if !errors.Is(got, context.DeadlineExceeded) {
						t.Fatalf("startup deadline cause lost: %+v", got)
					}
				} else if !errors.Is(got, cause) || !errors.Is(got, root) || got.RunID != cause.RunID {
					t.Fatalf("typed parent cause lost: %+v", got)
				}
				if phase == "version" {
					if got.Cleanup != nil {
						t.Fatal("version probe invented persistent-session cleanup")
					}
					if _, err := os.Stat(filepath.Join(f.dir, "session")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("version failure spawned persistent session: %v", err)
					}
				} else if got.Cleanup == nil || !got.Cleanup.WaitCompleted || !got.Cleanup.AbortAcknowledged || !got.Cleanup.AbortBashAcknowledged || got.Cleanup.DiscoveryError != "" || len(got.Cleanup.Unconfirmed) != 0 {
					t.Fatalf("startup termination left cleanup unconfirmed: %+v", got.Cleanup)
				}
				if err := syscall.Kill(f.pid, 0); !errors.Is(err, syscall.ESRCH) {
					t.Fatalf("startup termination left child %d: %v", f.pid, err)
				}
				if _, err := os.Stat(filepath.Join(f.bridge, "fixture-session.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("startup termination left discovery: %v", err)
				}
			})
		}
	}
}

func TestReadinessRechecksStateAndObservedActivity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		state      map[string]any
		start, end map[string]any
	}{
		{"streaming", map[string]any{"isStreaming": true}, nil, nil},
		{"pending", map[string]any{"pendingMessageCount": 1}, map[string]any{"type": "queue_update", "steering": []string{"queued"}, "followUp": []string{}}, map[string]any{"type": "queue_update", "steering": []string{}, "followUp": []string{}}},
		{"agent", nil, map[string]any{"type": "agent_start"}, map[string]any{"type": "agent_settled"}},
		{"retry", nil, map[string]any{"type": "auto_retry_start"}, map[string]any{"type": "auto_retry_end", "success": true}},
		{"tool", nil, map[string]any{"type": "tool_execution_start", "toolCallId": "t"}, map[string]any{"type": "tool_execution_end", "toolCallId": "t"}},
		{"compaction-failed", map[string]any{"isCompacting": true}, map[string]any{"type": "compaction_start"}, map[string]any{"type": "compaction_end", "aborted": false, "errorMessage": "pre-dispatch failure"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			f.send(control{Type: "state", State: tc.state})
			if tc.start != nil {
				f.send(control{Type: "event", Event: tc.start})
			}
			f.send(control{Type: "hold-state"})
			d := Dispatch{Token: randomID()}
			d.Message = "Controller " + d.Token
			done := make(chan executionResult, 1)
			go func() { r, err := f.s.Execute(f.ctx, d); done <- executionResult{r, err} }()
			f.next("held")
			f.send(control{Type: "release-one-state"})
			// A second state query instead of a prompt proves readiness remained blocked.
			f.next("held")
			f.send(control{Type: "state", State: map[string]any{"isStreaming": false, "isCompacting": false, "pendingMessageCount": 0}})
			if tc.end != nil {
				f.send(control{Type: "event", Event: tc.end})
			}
			f.send(control{Type: "release-state"})
			f.next("prompt")
			f.send(control{Type: "message", Message: assistant("stop")})
			f.send(control{Type: "settle"})
			if result := f.result(done); result.err != nil {
				t.Fatal(result.err)
			}
		})
	}
}

func TestAttemptDeadlineIncludesReadinessAndExecution(t *testing.T) {
	for _, waiting := range []bool{true, false} {
		t.Run(fmt.Sprint(waiting), func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			if waiting {
				f.send(control{Type: "state", State: map[string]any{"isStreaming": true}})
			}
			ctx, cancel := context.WithTimeout(f.ctx, 150*time.Millisecond)
			defer cancel()
			d := Dispatch{Token: randomID()}
			d.Message = "Controller " + d.Token
			_, err := f.s.Execute(ctx, d)
			got := requireCode(t, err, TimedOut)
			if got.Origin != AttemptDeadline || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost deadline cause: %+v", got)
			}
			want := AcceptedYes
			if waiting {
				want = AcceptedNo
			}
			if got.DispatchAccepted != want {
				t.Fatalf("accepted=%s want=%s", got.DispatchAccepted, want)
			}
			report, _ := f.s.Close(context.Background())
			if !report.WaitCompleted {
				t.Fatalf("deadline leaked child: %+v", report)
			}
		})
	}
}

// The pipe sees the elapsed deadline before cancellation is published. A distinct
// Done channel keeps context's propagation on this external boundary's AfterFunc.
type delayedRPCDeadlineContext struct {
	context.Context
	deadline      time.Time
	done          chan struct{}
	writeReturned chan struct{}
	waiting       chan struct{}
	returnedOnce  sync.Once
	waitingOnce   sync.Once
}

func (c *delayedRPCDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (c *delayedRPCDeadlineContext) Done() <-chan struct{} {
	select {
	case <-c.writeReturned:
		c.waitingOnce.Do(func() { close(c.waiting) })
	default:
	}
	return c.done
}
func (c *delayedRPCDeadlineContext) AfterFunc(f func()) func() bool {
	stop := context.AfterFunc(c.Context, f)
	return func() bool {
		stopped := stop()
		c.returnedOnce.Do(func() { close(c.writeReturned) })
		return stopped
	}
}

func TestRPCWriteDeadlinePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		code   Code
		origin Origin
	}{
		{"attempt-deadline", TimedOut, AttemptDeadline},
		{"run-deadline", TimedOut, RunDeadline},
		{"sigint", Cancelled, SignalINT},
		{"sigterm", Cancelled, SignalTERM},
		{"rpc-before-context", RPCUnresponsive, Protocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer func(f *os.File) { _ = f.Close() }(r)
			defer func(f *os.File) { _ = f.Close() }(w)
			parent, cancel := context.WithCancelCause(context.Background())
			ctx := &delayedRPCDeadlineContext{
				Context: parent, deadline: time.Now().Add(-time.Second), done: make(chan struct{}),
				writeReturned: make(chan struct{}), waiting: make(chan struct{}),
			}
			stop := func(cause error) {
				cancel(cause)
				select {
				case <-ctx.done:
				default:
					close(ctx.done)
				}
			}
			defer stop(nil)
			if tc.code == RPCUnresponsive {
				ctx.deadline = time.Now().Add(time.Hour)
			}
			s := &session{in: w, life: context.Background(), options: Options{Policy: DefaultPolicy()}, writeLease: make(chan struct{}, 1)}
			s.options.Policy.RPCTimeout = 25 * time.Millisecond
			s.writeLease <- struct{}{}
			result := make(chan error, 1)
			finished := make(chan struct{})
			// Exceed the real pipe capacity without a reader, so an RPC-owned
			// deadline must fail the write, not a later response wait.
			go func() {
				defer close(finished)
				result <- s.write(ctx, []byte(strings.Repeat("x", 2<<20)+"\n"), false)
			}()
			defer func() { stop(nil); _ = w.Close(); <-finished }()
			watchdog := time.NewTimer(3 * time.Second)
			defer watchdog.Stop()
			root := errors.New("outer stop cause")
			cause := &Failure{Code: tc.code, Origin: tc.origin, Cause: root, RunID: "run-id", AttemptID: "attempt-id"}
			if tc.code != RPCUnresponsive {
				select {
				case <-ctx.waiting:
					if ctx.Err() != nil || context.Cause(ctx) != nil {
						t.Fatal("cancellation was published before pipe timeout")
					}
					stop(cause)
				case err := <-result:
					t.Fatalf("pipe deadline escaped before context cause was published: %v", err)
				case <-watchdog.C:
					t.Fatal("write did not reach context cancellation barrier")
				}
			}
			select {
			case err := <-result:
				got := requireCode(t, err, tc.code)
				if got.Origin != tc.origin {
					t.Fatalf("origin=%s want=%s", got.Origin, tc.origin)
				}
				if tc.code == RPCUnresponsive {
					if !errors.Is(err, os.ErrDeadlineExceeded) || ctx.Err() != nil {
						t.Fatalf("RPC write timeout lost pipe cause or waited for context: %+v", got)
					}
				} else if err != cause || !errors.Is(err, root) || got.RunID != cause.RunID || got.AttemptID != cause.AttemptID {
					t.Fatalf("typed context cause changed: %+v, want %+v", got, cause)
				}
			case <-watchdog.C:
				t.Fatal("write did not finish")
			}
			if len(s.writeLease) != 1 {
				t.Fatal("write lease was not released")
			}
		})
	}
}

func TestCloseReleasesUnacknowledgedPrompt(t *testing.T) {
	f := mustFixture(t, "noack", func(o *Options) { o.Policy.PromptAckTimeout = time.Minute })
	_, done := f.execute()
	report, err := f.s.Close(f.ctx)
	if err != nil || !report.WaitCompleted {
		t.Fatalf("close: %+v %v", report, err)
	}
	_ = requireCode(t, f.result(done).err, ProcessExited)
}

func TestHealthRemainsOnlineWithoutProviderOutput(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, done := f.execute()
	for i := 0; i < 3; i++ {
		state, err := f.s.Snapshot(f.ctx)
		if err != nil || state.Health != "Online" || !state.Streaming || state.LastResponseTime.IsZero() {
			t.Fatalf("provider silence changed RPC health: %+v %v", state, err)
		}
	}
	f.assertWaiting(done)
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(done); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestOutOfOrderResponses(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	f.send(control{Type: "hold-state"})
	type result struct {
		r response
		e error
	}
	a, b := make(chan result, 1), make(chan result, 1)
	go func() { r, e := f.s.request(f.ctx, "get_state", nil, time.Second, false); a <- result{r, e} }()
	f.next("held")
	go func() { r, e := f.s.request(f.ctx, "get_state", nil, time.Second, false); b <- result{r, e} }()
	f.next("held")
	f.send(control{Type: "release-state"})
	first, second := <-a, <-b
	if first.e != nil || second.e != nil || first.r.seq <= second.r.seq {
		t.Fatalf("request association failed: %+v %+v", first, second)
	}
}
func TestWaiterCancellationUnregisters(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	f.send(control{Type: "hold-state"})
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { _, e := f.s.request(ctx, "get_state", nil, time.Second, false); done <- e }()
	f.next("held")
	cancel()
	err := requireCode(t, <-done, InvalidDefinition)
	if err.Origin != Definition {
		t.Fatalf("bare cancellation origin = %s", err.Origin)
	}
	f.send(control{Type: "release-state"})
	if _, e := f.s.Snapshot(f.ctx); e != nil {
		t.Fatal(e)
	}
	f.s.mu.Lock()
	count := len(f.s.pending)
	f.s.mu.Unlock()
	if count != 0 {
		t.Fatalf("pending request leak: %d", count)
	}
}
func TestProcessExitReleasesWaiter(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	f.send(control{Type: "hold-state"})
	done := make(chan error, 1)
	go func() { _, e := f.s.Snapshot(f.ctx); done <- e }()
	f.next("held")
	if e := f.s.cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = requireCode(t, <-done, ProcessExited)
	r, _ := f.s.Close(f.ctx)
	if !r.WaitCompleted {
		t.Fatal("Wait missing")
	}
}
func TestProtocolFailuresReleaseExecute(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{{"json", "{broken}\n"}, {"fields", "{\"type\":\"response\"}\n"}, {"oversize", strings.Repeat(" ", 1025) + "\n"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", func(o *Options) { o.Policy.MaxFrameBytes = 1024 })
			_, ch := f.execute()
			f.send(control{Type: "raw", Raw: tc.raw})
			_ = requireCode(t, f.result(ch).err, ProtocolFailed)
		})
	}
}
func TestManualCompactionBeforeDispatch(t *testing.T) {
	for _, aborted := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "cancelled"}[aborted], func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			f.send(control{Type: "state", State: map[string]any{"isCompacting": true}})
			f.event("compaction_start", map[string]any{"reason": "manual"})
			if _, e := f.s.Snapshot(f.ctx); e != nil {
				t.Fatal(e)
			}
			d := Dispatch{Token: randomID()}
			d.Message = "Controller " + d.Token
			done := make(chan executionResult, 1)
			go func() { r, e := f.s.Execute(f.ctx, d); done <- executionResult{r, e} }()
			f.send(control{Type: "state", State: map[string]any{"isCompacting": false}})
			f.event("compaction_end", map[string]any{"reason": "manual", "aborted": aborted})
			f.next("prompt")
			f.send(control{Type: "message", Message: assistant("stop")})
			f.send(control{Type: "settle"})
			if r := f.result(done); r.err != nil {
				t.Fatal(r.err)
			}
		})
	}
}
func TestObservationDoesNotBlockReaderAndOverflowCloses(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	f := mustFixture(t, "normal", func(o *Options) {
		// Dispatching/DispatchAccepted plus agent_start/user message fit;
		// the following turn_start must overflow the blocked observer.
		o.Policy.ObservationQueue = 4
		o.Observe = func(ctx context.Context, o Observation) error {
			once.Do(func() { close(entered) })
			<-ctx.Done()
			return nil
		}
	})
	select {
	case <-entered:
	case <-f.ctx.Done():
		t.Fatal("observer not entered")
	}
	_, ch := f.execute()
	if _, e := f.s.Snapshot(f.ctx); e != nil {
		t.Fatalf("stdout reader blocked by observer: %v", e)
	}
	f.event("turn_start", nil)
	_ = requireCode(t, f.result(ch).err, LimitExceeded)
	r, _ := f.s.Close(f.ctx)
	if !r.WaitCompleted || len(r.DiscoveryRemoved) != 1 || r.DiscoveryError != "" {
		t.Fatalf("overflow left owned process/discovery: %+v", r)
	}
	if _, err := os.Stat(filepath.Join(f.bridge, "fixture-session.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("overflow left discovery: %v", err)
	}
}
func TestStderrDrainBounded(t *testing.T) {
	f := mustFixture(t, "normal", func(o *Options) { o.Policy.MaxStderrBytes = 1024; o.Policy.StderrTailBytes = 128 })
	f.send(control{Type: "stderr", Raw: strings.Repeat("x", 100<<10) + "TAIL"})
	if _, e := f.s.Snapshot(f.ctx); e != nil {
		t.Fatal(e)
	}
	r, e := f.s.Close(f.ctx)
	if e != nil {
		t.Fatal(e)
	}
	if !r.StderrTruncated || len(r.StderrTail) > 128 || !strings.HasSuffix(r.StderrTail, "TAIL") {
		t.Fatalf("stderr %+v", r)
	}
	info, e := os.Stat(filepath.Join(f.dir, "session", "stderr.log"))
	if e != nil || info.Size() != 1024 {
		t.Fatalf("stderr file %v %v", info, e)
	}
}
func TestDiscoveryOwnershipMismatchAndRecoveryRetained(t *testing.T) {
	for _, kind := range []string{"mismatch", "claim"} {
		t.Run(kind, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			path := filepath.Join(f.bridge, "fixture-session.json")
			if kind == "mismatch" {
				if e := os.WriteFile(path, []byte(`{"sessionId":"fixture-session","piPid":1,"pid":1}`), 0600); e != nil {
					t.Fatal(e)
				}
			} else {
				if e := os.Rename(path, path+".recovering"); e != nil {
					t.Fatal(e)
				}
				path += ".recovering"
			}
			r, e := f.s.Close(f.ctx)
			_ = requireCode(t, e, CleanupFailed)
			if !r.WaitCompleted || r.DiscoveryError == "" {
				t.Fatalf("cleanup %+v", r)
			}
			if _, e := os.Stat(path); e != nil {
				t.Fatal("removed unowned/claimed discovery", e)
			}
		})
	}
}
func TestExternalSentinelSurvives(t *testing.T) {
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	sentinel := exec.Command(exe, "-test.run=^TestPiSubprocess$", "--")
	sentinel.Env = append(os.Environ(), "PWC_HELPER=1", "PWC_TOOL=1", "GORACE=atexit_sleep_ms=0")
	sentinel.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	in, e := sentinel.StdinPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = sentinel.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = syscall.Kill(-sentinel.Process.Pid, syscall.SIGKILL); _ = in.Close(); _ = sentinel.Wait() })
	f := mustFixture(t, "normal", nil)
	if _, e = f.s.Close(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e = syscall.Kill(sentinel.Process.Pid, 0); e != nil {
		t.Fatal("external sentinel was killed", e)
	}
}
func TestCancelledAttemptClosesHandle(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	ctx, cancel := context.WithCancelCause(f.ctx)
	d := Dispatch{Token: randomID()}
	d.Message = "Controller " + d.Token
	ch := make(chan executionResult, 1)
	go func() { r, e := f.s.Execute(ctx, d); ch <- executionResult{r, e} }()
	f.next("prompt")
	cause := &Failure{Code: Cancelled, Origin: ControllerUser, Message: "Controller cancellation"}
	cancel(cause)
	e := f.resultAfterCancel(ch)
	if got := requireCode(t, e, Cancelled); got.Origin != ControllerUser {
		t.Fatalf("cancellation origin = %s", got.Origin)
	}
	r, _ := f.s.Close(context.Background())
	if !r.WaitCompleted {
		t.Fatal("cancel leaked process")
	}
}
func (f *fixture) resultAfterCancel(ch <-chan executionResult) error {
	f.t.Helper()
	select {
	case r := <-ch:
		return r.err
	case <-time.After(3 * time.Second):
		f.t.Fatal("cancel did not unblock Execute")
	}
	return nil
}
func TestNoPromptCompletionFromEarlierSettled(t *testing.T) {
	f := mustFixture(t, "no-token", nil)
	d, ch := f.execute()
	f.send(control{Type: "message", Message: map[string]any{"role": "user", "content": "other", "timestamp": 0}})
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	f.event("agent_start", nil)
	f.send(control{Type: "message", Message: map[string]any{"role": "user", "content": d.Message, "timestamp": 1}})
	f.assertWaiting(ch)
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(ch); r.err != nil {
		t.Fatal(r.err)
	}
}
func TestHandleReuseAndLease(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	for round := 0; round < 2; round++ {
		_, ch := f.execute()
		_, e := f.s.Execute(f.ctx, Dispatch{Token: randomID(), Message: "busy"})
		_ = requireCode(t, e, SessionBusy)
		f.send(control{Type: "message", Message: assistant("stop")})
		f.send(control{Type: "settle"})
		r := f.result(ch)
		if r.err != nil {
			t.Fatal(r.err)
		}
		if _, e = f.s.Confirm(f.ctx, r.receipt); e != nil {
			t.Fatal(e)
		}
	}
}
func TestStartupMissingExecutable(t *testing.T) {
	p, e := New(Options{Executable: filepath.Join(t.TempDir(), "absent")})
	if e != nil {
		t.Fatal(e)
	}
	_, e = p.Start(context.Background(), SessionSpec{HandleID: "h", Model: ModelSpec{Provider: "x", ID: "y", Thinking: "high"}, CWD: t.TempDir(), SessionDir: filepath.Join(t.TempDir(), "pi")})
	_ = requireCode(t, e, StartFailed)
}

func TestPromptAckTimeoutStartsAfterCompleteWrite(t *testing.T) {
	// Leave headroom for the large frame under race/coverage instrumentation,
	// while holding the pipe longer than the entire acknowledgement budget.
	const ackTimeout = 500 * time.Millisecond
	f := mustFixture(t, "write-block", func(o *Options) {
		o.Policy.RPCTimeout = 5 * time.Second
		o.Policy.PromptAckTimeout = ackTimeout
	})
	d := Dispatch{Token: randomID()}
	d.Message = "Controller " + d.Token + strings.Repeat("x", 2<<20)
	done := make(chan executionResult, 1)
	go func() { r, e := f.s.Execute(f.ctx, d); done <- executionResult{r, e} }()
	f.next("blocked")
	select {
	case r := <-done:
		t.Fatalf("ack timed out before full pipe write: %+v", r)
	case <-time.After(3 * ackTimeout):
	}
	f.send(control{Type: "unblock-stdin"})
	f.next("prompt")
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(done); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestUnsupportedVersionDoesNotStartPersistentSession(t *testing.T) {
	f, e := newFixture(t, "bad-version", nil)
	_ = requireCode(t, e, UnsupportedPiVersion)
	if f.pid != 0 {
		t.Fatal("unsupported version started a persistent child")
	}
}

func TestHealthProbeIsBoundedAndDoesNotOverlap(t *testing.T) {
	f := mustFixture(t, "normal", func(o *Options) { o.Policy.HealthInterval = 30 * time.Millisecond })
	f.send(control{Type: "hold-state"})
	f.next("held")
	select {
	case <-f.s.failed:
	case <-f.ctx.Done():
		t.Fatal("health query never timed out")
	}
	f.s.mu.Lock()
	err := *f.s.fatal
	f.s.mu.Unlock()
	_ = requireCode(t, &err, RPCUnresponsive)
	state, snapshotErr := f.s.Snapshot(f.ctx)
	_ = requireCode(t, snapshotErr, RPCUnresponsive)
	if state.Health != "Unresponsive" || state.Identity.PID != f.pid || state.LastResponseTime.IsZero() {
		t.Fatalf("lost unresponsive health evidence: %+v", state)
	}
	report, _ := f.s.Close(f.ctx)
	if !report.WaitCompleted {
		t.Fatal("unresponsive process not reaped")
	}
	select {
	case event := <-f.events:
		t.Fatalf("overlapping health probe: %+v", event)
	default:
	}
}

func TestCloseWithPendingHealthProbe(t *testing.T) {
	for _, active := range []bool{false, true} {
		name := "idle"
		if active {
			name = "active-attempt"
		}
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var observations []Observation
			accepted := make(chan struct{}, 1)
			f := mustFixture(t, "normal", func(o *Options) {
				o.Policy.HealthInterval = 30 * time.Millisecond
				o.Policy.RPCTimeout = 5 * time.Second
				o.Policy.PromptObservationTimeout = 5 * time.Second
				o.Observe = func(_ context.Context, observation Observation) error {
					mu.Lock()
					observations = append(observations, observation)
					mu.Unlock()
					if observation.Kind == "DispatchAccepted" {
						accepted <- struct{}{}
					}
					return nil
				}
			})
			root := errors.New("root failure before pending health probe cleanup")
			cause := &Failure{Code: LimitExceeded, Origin: Definition, LimitScope: "run", RunID: "run-id", Cause: root, DispatchAccepted: AcceptedNo}
			ctx, cancel := context.WithCancelCause(f.ctx)
			defer cancel(nil)
			done := make(chan executionResult, 1)
			if active {
				d := Dispatch{Token: randomID()}
				d.Message = "Controller " + d.Token
				go func() { r, err := f.s.Execute(ctx, d); done <- executionResult{r, err} }()
				f.next("prompt")
				select {
				case <-accepted:
				case <-f.ctx.Done():
					t.Fatal("prompt was not accepted")
				}
			}
			// After acceptance Execute only reads entries until settled, so this
			// get_state must come from the real health loop, not a test Snapshot.
			f.send(control{Type: "hold-state"})
			f.next("held")
			select {
			case result := <-done:
				t.Fatalf("attempt ended before root cancellation: %+v", result)
			default:
			}
			if active {
				cancel(cause)
			}
			report, err := f.s.Close(context.Background())
			if err != nil || !report.WaitCompleted || !report.ProcessExited || !report.AbortAcknowledged || !report.AbortBashAcknowledged || report.WaitError != "" || report.KillError != "" || report.DiscoveryError != "" || len(report.Unconfirmed) != 0 || len(report.DiscoveryRemoved) != 1 {
				t.Fatalf("pending health probe leaked owned cleanup: %+v, %v", report, err)
			}
			if active {
				result := f.result(done)
				got := requireCode(t, result.err, cause.Code)
				if got.Origin != cause.Origin || got.LimitScope != cause.LimitScope || got.RunID != cause.RunID || !errors.Is(result.err, cause) || !errors.Is(result.err, root) {
					t.Fatalf("health probe replaced typed root cause: %+v, want %+v", got, cause)
				}
				if got.DispatchAccepted != AcceptedYes || got.HandleID != f.s.Identity().HandleID || got.Cleanup == nil || !got.Cleanup.WaitCompleted || !got.Cleanup.ProcessExited {
					t.Fatalf("attempt lost accepted dispatch/cleanup: %+v", got)
				}
			}
			f.s.mu.Lock()
			pending, fatal := len(f.s.pending), f.s.fatal
			f.s.mu.Unlock()
			if pending != 0 {
				t.Fatalf("Close left pending RPC requests: %d", pending)
			}
			if !active && fatal != nil {
				t.Errorf("idle probe shutdown invented a fatal error: %+v", fatal)
			}
			if _, err := os.Stat(filepath.Join(f.bridge, "fixture-session.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("Close retained discovery: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			closed := false
			for _, observation := range observations {
				if observation.Kind == "SessionClosed" {
					closed = true
				}
				if observation.Kind == "RuntimeFailed" && (!active || observation.Failure == nil || observation.Failure.Code != cause.Code || !errors.Is(observation.Failure, root)) {
					t.Errorf("probe shutdown emitted a false runtime failure: %+v", observation)
				}
			}
			if !closed {
				t.Fatal("observer did not deliver SessionClosed")
			}
		})
	}
}

func TestPendingHealthProbePreservesProtocolFailure(t *testing.T) {
	failed := make(chan Observation, 1)
	f := mustFixture(t, "normal", func(o *Options) {
		o.Policy.HealthInterval = 30 * time.Millisecond
		o.Policy.RPCTimeout = 5 * time.Second
		o.Observe = func(_ context.Context, observation Observation) error {
			if observation.Kind == "RuntimeFailed" {
				failed <- observation
			}
			return nil
		}
	})
	f.send(control{Type: "hold-state"})
	f.next("held")
	f.send(control{Type: "state", State: map[string]any{"isStreaming": "false"}})
	// A protocol failure may close the child before its control ack.
	if err := f.encoder.Encode(control{Type: "release-state"}); err != nil {
		t.Fatal(err)
	}
	select {
	case observation := <-failed:
		_ = requireCode(t, observation.Failure, ProtocolFailed)
	case <-f.ctx.Done():
		t.Fatal("health probe swallowed a real protocol failure")
	}
	report, err := f.s.Close(context.Background())
	if err != nil || !report.WaitCompleted || !report.ProcessExited || len(report.Unconfirmed) != 0 {
		t.Fatalf("protocol failure leaked owned cleanup: %+v, %v", report, err)
	}
	_, err = f.s.Snapshot(f.ctx)
	_ = requireCode(t, err, ProtocolFailed)
}

func TestHalfFrameExitIsProtocolFailure(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, ch := f.execute()
	if e := f.encoder.Encode(control{Type: "raw-exit", Raw: `{"type":"agent_settled"}`}); e != nil {
		t.Fatal(e)
	}
	_ = requireCode(t, f.result(ch).err, ProtocolFailed)
}

func TestTransformedPromptWithoutNonceCannotComplete(t *testing.T) {
	f := mustFixture(t, "no-token", nil)
	_, ch := f.execute()
	f.event("agent_start", nil)
	f.send(control{Type: "message", Message: map[string]any{"role": "user", "content": "extension transformed input", "timestamp": 1}})
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	_ = requireCode(t, f.result(ch).err, PromptNotObserved)
}

func TestUnexpectedExitReportsUnconfirmedDetachedTool(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, ch := f.execute()
	f.send(control{Type: "tool"})
	if _, e := f.s.Snapshot(f.ctx); e != nil {
		t.Fatal(e)
	}
	if e := f.s.cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	_ = requireCode(t, f.result(ch).err, ProcessExited)
	report, e := f.s.Close(f.ctx)
	_ = requireCode(t, e, CleanupFailed)
	if !strings.Contains(strings.Join(report.Unconfirmed, ";"), "tool termination") {
		t.Fatalf("false complete cleanup: %+v", report)
	}
}

func TestUnsettledForeignPromptCannotDonateSettled(t *testing.T) {
	f := mustFixture(t, "no-token", nil)
	d, ch := f.execute()
	f.event("agent_start", nil)
	f.send(control{Type: "message", Message: map[string]any{"role": "user", "content": "foreign", "timestamp": 0}})
	f.send(control{Type: "message", Message: assistant("stop")})
	f.event("agent_end", nil)
	f.event("agent_start", nil)
	f.send(control{Type: "message", Message: map[string]any{"role": "user", "content": d.Message, "timestamp": 1}})
	_ = requireCode(t, f.result(ch).err, AmbiguousExecution)
}

func TestUnknownAndStreamingEventsDoNotFillCoreQueue(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f := mustFixture(t, "normal", func(o *Options) {
		o.Policy.ObservationQueue = 8
		o.Observe = func(ctx context.Context, observation Observation) error {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		}
	})
	select {
	case <-entered:
	case <-f.ctx.Done():
		t.Fatal("observer did not enter")
	}
	_, done := f.execute()
	for _, kind := range []string{"message_update", "tool_execution_update", "bash_execution_update", "future_event", "agent_settled_v2"} {
		f.send(control{Type: "raw", Raw: strings.Repeat(`{"type":"`+kind+`"}`+"\n", 64)})
	}
	f.assertWaiting(done)
	close(release)
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	r := f.result(done)
	if r.err != nil {
		t.Fatal(r.err)
	}
	f.send(control{Type: "raw", Raw: strings.Repeat("{\"type\":\"future_event\"}\n", 64)})
	if _, err := f.s.Confirm(f.ctx, r.receipt); err != nil {
		t.Fatalf("unknown event invalidated completed dispatch: %v", err)
	}
}

func TestPendingLimitAndCloseReleaseWaiters(t *testing.T) {
	f := mustFixture(t, "normal", func(o *Options) { o.Policy.ObservationQueue = 2 })
	f.send(control{Type: "hold-state"})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := f.s.request(f.ctx, "get_state", nil, time.Minute, false)
			results <- err
		}()
		f.next("held")
	}
	_, err := f.s.request(f.ctx, "get_state", nil, time.Minute, false)
	_ = requireCode(t, err, LimitExceeded)
	// Cleanup must not need a free ordinary-request slot to stop the child.
	report, _ := f.s.Close(f.ctx)
	if !report.WaitCompleted {
		t.Fatalf("Close did not reap: %+v", report)
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			_ = requireCode(t, err, ProcessExited)
		case <-f.ctx.Done():
			t.Fatal("Close left a pending waiter")
		}
	}
	f.s.mu.Lock()
	count := len(f.s.pending)
	f.s.mu.Unlock()
	if count != 0 {
		t.Fatalf("pending requests after Close: %d", count)
	}
}

func TestClosePreservesTypedRootCause(t *testing.T) {
	for _, code := range []Code{LimitExceeded, StorageFailed, JournalFailed} {
		for _, waiting := range []string{"pending-rpc", "entries-observer", "closed-write"} {
			t.Run(string(code)+"/"+waiting, func(t *testing.T) {
				accepted := make(chan struct{}, 1)
				f := mustFixture(t, "normal", func(o *Options) {
					o.Policy.RPCTimeout = 5 * time.Second
					o.Policy.PromptObservationTimeout = 5 * time.Second
					o.Observe = func(_ context.Context, observation Observation) error {
						if observation.Kind == "DispatchAccepted" {
							accepted <- struct{}{}
						}
						return nil
					}
				})
				root := errors.New("root failure before owned session cleanup")
				cause := &Failure{Code: code, Origin: Storage, RunID: "run-id", Cause: root, DispatchAccepted: AcceptedNo}
				if code == LimitExceeded {
					cause.Origin, cause.LimitScope = Definition, "run"
				}
				ctx, cancel := context.WithCancelCause(f.ctx)
				defer cancel(nil)
				done := make(chan error, 1)
				switch waiting {
				case "pending-rpc":
					f.send(control{Type: "hold-state"})
					go func() { _, err := f.s.Snapshot(ctx); done <- err }()
					f.next("held")
				case "entries-observer":
					d := Dispatch{Token: randomID()}
					d.Message = "Controller " + d.Token
					go func() { _, err := f.s.Execute(ctx, d); done <- err }()
					f.next("prompt")
					select {
					case <-accepted:
					case <-f.ctx.Done():
						t.Fatal("prompt was not accepted")
					}
					// A delta request with e1 proves Execute consumed the prompt entry.
					leaf := "e1"
					f.send(control{Type: "entries-barrier", Leaf: &leaf})
					if _, err := f.s.Snapshot(f.ctx); err != nil {
						t.Fatal(err)
					}
				}
				select {
				case err := <-done:
					t.Fatalf("operation ended before root cancellation: %v", err)
				default:
				}
				// Like engine ownership cleanup, cancel the operation before starting Close.
				cancel(cause)
				report, err := f.s.Close(context.Background())
				if err != nil || !report.WaitCompleted || !report.ProcessExited || !report.AbortAcknowledged || !report.AbortBashAcknowledged || report.WaitError != "" || report.KillError != "" || report.DiscoveryError != "" || len(report.Unconfirmed) != 0 || len(report.DiscoveryRemoved) != 1 {
					t.Fatalf("root cancellation leaked owned cleanup: %+v, %v", report, err)
				}
				if waiting == "closed-write" {
					_, err := f.s.Snapshot(ctx)
					done <- err
				}
				select {
				case err := <-done:
					got := requireCode(t, err, code)
					if got.Origin != cause.Origin || got.LimitScope != cause.LimitScope || got.RunID != cause.RunID || !errors.Is(err, cause) || !errors.Is(err, root) {
						t.Fatalf("Close replaced typed root cause: %+v, want %+v", got, cause)
					}
					if waiting == "entries-observer" && (got.DispatchAccepted != AcceptedYes || got.HandleID != f.s.Identity().HandleID || got.Cleanup == nil || !got.Cleanup.WaitCompleted) {
						t.Fatalf("entries observer lost accepted dispatch/cleanup: %+v", got)
					}
				case <-f.ctx.Done():
					t.Fatal("Close did not release operation")
				}
				f.s.mu.Lock()
				pending := len(f.s.pending)
				f.s.mu.Unlock()
				if pending != 0 {
					t.Fatalf("Close left pending RPC requests: %d", pending)
				}
				if _, err := os.Stat(filepath.Join(f.bridge, "fixture-session.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("Close retained discovery: %v", err)
				}
			})
		}
	}
}

func TestCloseDeadlineDoesNotExtendAnotherCallersWait(t *testing.T) {
	f := mustFixture(t, "no-abort-ack", nil)
	done := make(chan CleanupReport, 1)
	go func() { r, _ := f.s.Close(context.Background()); done <- r }()
	f.next("abort-held")
	f.next("abort-held")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := f.s.Close(ctx)
	_ = requireCode(t, err, CleanupFailed)
	if !errors.Is(err, context.Canceled) || r.WaitCompleted || len(r.Unconfirmed) == 0 {
		t.Fatalf("deadline returned a false final outcome: %+v %v", r, err)
	}
	select {
	case r = <-done:
	case <-f.ctx.Done():
		t.Fatal("owner Close did not finish")
	}
	if !r.WaitCompleted || r.AbortAcknowledged || r.AbortBashAcknowledged {
		t.Fatalf("incorrect final cleanup outcome: %+v", r)
	}
	final, err := f.s.Close(context.Background())
	_ = requireCode(t, err, CleanupFailed)
	if !final.WaitCompleted {
		t.Fatal("caller cancellation replaced the shared Close outcome")
	}
}

func TestStateParserRejectsMissingOrInvalidRequiredFields(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"sessionId", nil}, {"sessionFile", nil}, {"model", nil}, {"thinkingLevel", nil},
		{"isStreaming", nil}, {"isCompacting", nil}, {"pendingMessageCount", nil},
		{"pendingMessageCount", -1}, {"isStreaming", "false"},
	} {
		t.Run(tc.name+"/"+fmt.Sprint(tc.value), func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			f.send(control{Type: "state", State: map[string]any{tc.name: tc.value}})
			_, err := f.s.Snapshot(f.ctx)
			_ = requireCode(t, err, ProtocolFailed)
		})
	}
}

func TestEntriesParserRejectsMissingOrInvalidRequiredFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		data map[string]any
	}{
		{"missing-entries", map[string]any{"leafId": nil}},
		{"null-entries", map[string]any{"entries": nil, "leafId": nil}},
		{"missing-leaf", map[string]any{"entries": []any{}}},
		{"missing-entry-id", map[string]any{"entries": []any{map[string]any{"type": "message", "parentId": nil}}, "leafId": nil}},
		{"missing-entry-parent", map[string]any{"entries": []any{map[string]any{"type": "custom", "id": "e"}}, "leafId": "e"}},
		{"missing-message", map[string]any{"entries": []any{map[string]any{"type": "message", "id": "e", "parentId": nil}}, "leafId": "e"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			f.send(control{Type: "entries-response", State: tc.data})
			d := Dispatch{Token: randomID()}
			d.Message = "Controller " + d.Token
			_, err := f.s.Execute(f.ctx, d)
			failure := requireCode(t, err, ProtocolFailed)
			if failure.DispatchAccepted != AcceptedNo {
				t.Fatalf("malformed baseline was dispatched: %+v", failure)
			}
		})
	}
}

func TestEventParserRejectsMissingOrInvalidRequiredFields(t *testing.T) {
	for _, raw := range []string{
		`{"type":"queue_update"}`, `{"type":"queue_update","steering":null,"followUp":[]}`,
		`{"type":"auto_retry_end"}`, `{"type":"compaction_end"}`,
		`{"type":"tool_execution_start"}`, `{"type":"tool_execution_end"}`,
		`{"type":"thinking_level_changed"}`, `{"type":"extension_ui_request","id":"x"}`,
		`{"type":"message_end","message":{"role":"user","content":null}}`,
		`{"type":"message_end","message":{"role":"user","content":17}}`,
		`{"type":"message_end","message":{"role":"user","content":[{"type":"text"}]}}`,
		`{"type":"message_end","message":{"role":"assistant","provider":"fixture","model":"model"}}`,
	} {
		t.Run(raw, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, done := f.execute()
			f.send(control{Type: "raw", Raw: raw + "\n"})
			_ = requireCode(t, f.result(done).err, ProtocolFailed)
		})
	}
}
