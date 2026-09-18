package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type control struct {
	Type         string         `json:"type"`
	ID           string         `json:"id,omitempty"`
	Event        map[string]any `json:"event,omitempty"`
	Message      map[string]any `json:"message,omitempty"`
	Entry        map[string]any `json:"entry,omitempty"`
	State        map[string]any `json:"state,omitempty"`
	Leaf         *string        `json:"leaf,omitempty"`
	Raw          string         `json:"raw,omitempty"`
	PID          int            `json:"pid,omitempty"`
	Count        int            `json:"count,omitempty"`
	FullQueries  int            `json:"fullQueries,omitempty"`
	HistoryBytes int            `json:"historyBytes,omitempty"`
}

func TestPiSubprocess(t *testing.T) {
	if os.Getenv("PWC_HELPER") == "" {
		return
	}
	if os.Getenv("PWC_TOOL") == "1" {
		if address := os.Getenv("PWC_TOOL_CONTROL"); address != "" {
			conn, err := net.Dial("tcp", address)
			if err != nil {
				os.Exit(3)
			}
			go func() {
				_, _ = io.Copy(io.Discard, conn)
				os.Exit(0)
			}()
		}
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}
	for _, arg := range os.Args {
		if arg == "--version" {
			if os.Getenv("PWC_MODE") == "bad-version" {
				fmt.Println("0.84.2")
				os.Exit(0)
			}
			fmt.Println("0.84.3")
			os.Exit(0)
		}
	}
	mode := os.Getenv("PWC_MODE")
	arg := func(key string) string {
		for i, v := range os.Args {
			if v == key && i+1 < len(os.Args) {
				return os.Args[i+1]
			}
		}
		return ""
	}
	dir := arg("--session-dir")
	sid := "fixture-session"
	file := filepath.Join(dir, "history.jsonl")
	conn, err := net.Dial("tcp", os.Getenv("PWC_CONTROL"))
	if err != nil {
		os.Exit(3)
	}
	defer func(conn net.Conn) { _ = conn.Close() }(conn)
	ctl := json.NewEncoder(conn)
	out := json.NewEncoder(os.Stdout)
	state := map[string]any{"sessionId": sid, "sessionFile": file, "model": map[string]any{"provider": "fixture", "id": "model"}, "thinkingLevel": "high", "isStreaming": false, "isCompacting": false, "pendingMessageCount": 0}
	if mode == "startup-model" {
		state["model"] = map[string]any{"provider": "fixture", "id": "wrong"}
	}
	if mode != "no-discovery" {
		b, _ := json.Marshal(discovery{SessionID: sid, SessionFile: file, PID: os.Getppid(), PiPID: os.Getpid()})
		_ = os.WriteFile(filepath.Join(os.Getenv("PI_BRIDGE_DIR"), sid+".json"), b, 0600)
	}
	_ = ctl.Encode(control{Type: "hello", PID: os.Getpid()})
	commands := make(chan map[string]any)
	readPermit := make(chan struct{}, 1)
	if mode == "write-block" {
		readPermit <- struct{}{}
	}
	controls := make(chan control)
	go func() {
		d := json.NewDecoder(os.Stdin)
		for {
			if mode == "write-block" {
				<-readPermit
			}
			var v map[string]any
			if d.Decode(&v) != nil {
				os.Exit(0)
			}
			commands <- v
		}
	}()
	go func() {
		d := json.NewDecoder(conn)
		for {
			var v control
			if d.Decode(&v) != nil {
				return
			}
			controls <- v
		}
	}()
	var entries []map[string]any
	leaf := ""
	counter := 0
	appendEntry := func(e map[string]any) {
		counter++
		if e["id"] == nil {
			e["id"] = fmt.Sprintf("e%d", counter)
		}
		if e["parentId"] == nil {
			e["parentId"] = nil
			if leaf != "" {
				e["parentId"] = leaf
			}
		}
		entries = append(entries, e)
		leaf = e["id"].(string)
	}
	emit := func(e map[string]any) { _ = out.Encode(e) }
	appendMessage := func(m map[string]any) {
		emit(map[string]any{"type": "message_end", "message": m})
		appendEntry(map[string]any{"type": "message", "message": m})
	}
	reply := func(c map[string]any, data any, success bool) {
		emit(map[string]any{"type": "response", "id": c["id"], "command": c["type"], "success": success, "data": data, "error": "fixture rejection"})
	}
	var prompt map[string]any
	var held []map[string]any
	holdState := false
	holdStats := false
	stats := map[string]any{"sessionId": sid, "sessionFile": file}
	var entriesResponse map[string]any
	stateCount := 0
	entriesCount, fullQueries := 0, 0
	var entriesBarrier *control
	var tool *exec.Cmd
	var toolIn io.WriteCloser
	for {
		select {
		case c := <-commands:
			switch c["type"] {
			case "get_state":
				stateCount++
				if holdState {
					held = append(held, c)
					_ = ctl.Encode(control{Type: "held"})
				} else {
					reply(c, state, true)
				}
			case "get_session_stats":
				if holdStats {
					_ = ctl.Encode(control{Type: "stats-held"})
				} else {
					reply(c, stats, true)
				}
			case "get_entries":
				entriesCount++
				if c["since"] == nil {
					fullQueries++
				}
				if entriesBarrier != nil && entriesCount >= entriesBarrier.Count && (entriesBarrier.Leaf == nil || c["since"] == *entriesBarrier.Leaf) {
					_ = ctl.Encode(control{Type: "barrier", ID: entriesBarrier.ID, Count: entriesCount})
					entriesBarrier = nil
				}
				if entriesResponse != nil {
					reply(c, entriesResponse, true)
					continue
				}
				result := entries
				if since, ok := c["since"].(string); ok {
					found := false
					for i, e := range entries {
						if e["id"] == since {
							result = entries[i+1:]
							found = true
							break
						}
					}
					if !found {
						reply(c, nil, false)
						continue
					}
				}
				if result == nil {
					result = []map[string]any{}
				}
				var l any
				if leaf != "" {
					l = leaf
				}
				reply(c, map[string]any{"entries": result, "leafId": l}, true)
			case "prompt":
				prompt = c
				_ = ctl.Encode(control{Type: "prompt"})
				if mode == "reject" {
					reply(c, nil, false)
					continue
				}
				if mode == "disconnect" {
					os.Exit(0)
				}
				if mode == "noack" {
					continue
				}
				if mode != "event-before-ack" && !strings.HasPrefix(mode, "dialog-") {
					reply(c, nil, true)
				}
				if mode == "no-token" {
					continue
				}
				if strings.HasPrefix(mode, "dialog-") {
					emit(map[string]any{"type": "extension_ui_request", "id": "dialog", "method": strings.TrimPrefix(mode, "dialog-")})
					continue
				}
				emit(map[string]any{"type": "agent_start"})
				state["isStreaming"] = true
				m := map[string]any{"role": "user", "content": c["message"], "timestamp": 1}
				if mode == "entry-lag" {
					emit(map[string]any{"type": "message_end", "message": m})
				} else {
					appendMessage(m)
				}
			case "abort":
				if mode == "no-abort-ack" {
					_ = ctl.Encode(control{Type: "abort-held"})
					continue
				}
				if tool != nil {
					_ = syscall.Kill(-tool.Process.Pid, syscall.SIGKILL)
					_ = tool.Wait()
					_ = toolIn.Close()
					tool = nil
					emit(map[string]any{"type": "tool_execution_end", "toolCallId": "bash"})
				}
				state["isStreaming"] = false
				reply(c, nil, true)
			case "abort_bash":
				if mode == "no-abort-ack" {
					_ = ctl.Encode(control{Type: "abort-held"})
					continue
				}
				reply(c, nil, true)
			case "extension_ui_response":
				_ = ctl.Encode(control{Type: "dialog", Event: c})
				reply(prompt, nil, true)
			default:
				reply(c, nil, false)
			}
			if mode == "write-block" {
				if stateCount == 3 && c["type"] == "get_state" {
					_ = ctl.Encode(control{Type: "blocked"})
				} else {
					readPermit <- struct{}{}
				}
			}
		case c := <-controls:
			switch c.Type {
			case "wait-entries", "entries-barrier":
				entriesBarrier = &c
				if c.Type == "entries-barrier" {
					// Wake the entries observer without changing the append prefix.
					emit(map[string]any{"type": "compaction_end", "aborted": false})
				}
				continue
			case "entries-stats":
				c.Count, c.FullQueries = entriesCount, fullQueries
				history, _ := json.Marshal(map[string]any{"entries": entries, "leafId": leaf})
				c.HistoryBytes = len(history)
			case "unblock-stdin":
				readPermit <- struct{}{}
			case "recover-after":
				emit(c.Event)
				if c.Event["type"] == "thinking_level_changed" {
					emit(map[string]any{"type": "thinking_level_changed", "level": "high"})
				}
				appendMessage(map[string]any{"role": "user", "content": "queued human followup", "timestamp": 3})
				appendMessage(map[string]any{"role": "assistant", "provider": "fixture", "model": "model", "stopReason": "stop", "content": []any{map[string]any{"type": "text", "text": "later success"}}, "timestamp": 4})
				state["isStreaming"] = false
				emit(map[string]any{"type": "agent_settled"})
			case "event":
				emit(c.Event)
			case "messages", "unmatched-messages", "unmatched-entries":
				for i := 0; i < c.Count; i++ {
					switch c.Type {
					case "messages":
						appendMessage(c.Message)
					case "unmatched-entries":
						appendEntry(map[string]any{"type": "message", "message": c.Message})
					default:
						emit(map[string]any{"type": "message_end", "message": c.Message})
					}
				}
				if c.Type == "unmatched-entries" {
					emit(map[string]any{"type": "entry_appended"})
				}
			case "retry-outcome":
				appendMessage(assistant("error"))
				emit(map[string]any{"type": "auto_retry_start"})
				appendMessage(c.Message)
				if c.Message["stopReason"] == "error" {
					emit(map[string]any{"type": "auto_retry_end", "success": false, "finalError": "exhausted"})
				} else {
					emit(map[string]any{"type": "auto_retry_end", "success": true})
				}
				if c.Event != nil {
					emit(map[string]any{"type": "compaction_start"})
					emit(c.Event)
				}
				state["isStreaming"] = false
				emit(map[string]any{"type": "agent_settled"})
			case "message":
				appendMessage(c.Message)
			case "entry":
				appendEntry(c.Entry)
			case "entries-response":
				entriesResponse = c.State
			case "state":
				for k, v := range c.State {
					state[k] = v
				}
			case "leaf":
				leaf = *c.Leaf
			case "raw-exit":
				_, _ = os.Stdout.WriteString(c.Raw)
				os.Exit(0)
			case "raw":
				_, _ = os.Stdout.WriteString(c.Raw)
			case "stderr":
				_, _ = os.Stderr.WriteString(c.Raw)
			case "ack":
				reply(prompt, nil, true)
			case "entry-lag":
				appendEntry(map[string]any{"type": "message", "message": map[string]any{"role": "user", "content": prompt["message"], "timestamp": 1}})
			case "settle":
				state["isStreaming"] = false
				emit(map[string]any{"type": "agent_end", "messages": []any{}, "willRetry": false})
				emit(map[string]any{"type": "agent_settled"})
			case "stats":
				for k, v := range c.State {
					stats[k] = v
				}
			case "hold-stats":
				holdStats = true
			case "hold-state":
				holdState = true
			case "release-one-state":
				if len(held) == 0 {
					os.Exit(6)
				}
				reply(held[0], state, true)
				held = held[1:]
			case "release-state":
				holdState = false
				for i := len(held) - 1; i >= 0; i-- {
					reply(held[i], state, true)
				}
				held = nil
			case "exit":
				os.Exit(c.Count)
			case "wait-tool":
				if tool == nil {
					os.Exit(5)
				}
				_ = tool.Wait()
				_ = toolIn.Close()
				tool = nil
				emit(map[string]any{"type": "tool_execution_end", "toolCallId": "bash"})
			case "tool":
				exe, _ := os.Executable()
				tool = exec.Command(exe, "-test.run=^TestPiSubprocess$", "--")
				tool.Env = append(os.Environ(), "PWC_TOOL=1")
				tool.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
				toolIn, _ = tool.StdinPipe()
				if tool.Start() != nil {
					os.Exit(4)
				}
				c.PID = tool.Process.Pid
				emit(map[string]any{"type": "tool_execution_start", "toolCallId": "bash", "toolName": "bash"})
			}
			_ = ctl.Encode(control{Type: "barrier", ID: c.ID, PID: c.PID, Count: c.Count, FullQueries: c.FullQueries, HistoryBytes: c.HistoryBytes})
		}
	}
}

type fixture struct {
	t           *testing.T
	s           *session
	pi          *Pi
	conn        net.Conn
	encoder     *json.Encoder
	events      chan control
	mu          sync.Mutex
	pid         int
	dir, bridge string
	ctx         context.Context
	cancel      context.CancelFunc
	stopTool    func()
	toolReady   <-chan struct{}
}

func newFixture(t *testing.T, mode string, configure func(*Options)) (*fixture, error) {
	t.Helper()
	f := &fixture{t: t, dir: t.TempDir(), events: make(chan control, 256)}
	f.bridge = filepath.Join(f.dir, "bridge")
	if err := os.Mkdir(f.bridge, 0700); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.ctx, f.cancel = context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		f.cancel()
		if f.conn != nil {
			_ = f.conn.Close()
		}
		_ = listener.Close()
	})
	toolListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// A socket lifeline is safe even after the tool PID has been reaped and reused.
	// Register it before startup so early assertions cannot leave a detached grandchild.
	var toolConn net.Conn
	toolAccepted := make(chan struct{})
	f.toolReady = toolAccepted
	go func() {
		toolConn, _ = toolListener.Accept()
		close(toolAccepted)
	}()
	var stopToolOnce sync.Once
	f.stopTool = func() {
		stopToolOnce.Do(func() {
			_ = toolListener.Close()
			<-toolAccepted
			if toolConn != nil {
				_ = toolConn.Close()
			}
		})
	}
	t.Cleanup(f.stopTool)
	accepted := make(chan net.Conn, 1)
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		c, e := listener.Accept()
		if e == nil {
			accepted <- c
		}
	}()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	policy := DefaultPolicy()
	policy.StartupTimeout = time.Second
	policy.RPCTimeout = 200 * time.Millisecond
	policy.PromptAckTimeout = 200 * time.Millisecond
	policy.PromptObservationTimeout = 200 * time.Millisecond
	policy.AbortGrace = 200 * time.Millisecond
	policy.CleanupTimeout = 2 * time.Second
	policy.HealthInterval = time.Hour
	o := Options{Executable: exe, Args: []string{"-test.run=^TestPiSubprocess$", "--"}, Env: []string{"GORACE=atexit_sleep_ms=0", "PWC_HELPER=1", "PWC_MODE=" + mode, "PWC_CONTROL=" + listener.Addr().String(), "PWC_TOOL_CONTROL=" + toolListener.Addr().String()}, BridgeDir: f.bridge, Policy: policy}
	if configure != nil {
		configure(&o)
	}
	p, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	f.pi = p
	done := make(chan struct{})
	var started Session
	var startErr error
	go func() {
		started, startErr = p.Start(f.ctx, SessionSpec{HandleID: randomID(), Name: "test", Model: ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}, CWD: f.dir, SessionDir: filepath.Join(f.dir, "session", "pi")})
		close(done)
	}()
	t.Cleanup(func() {
		f.cancel()
		<-done
		if started != nil {
			_, _ = started.Close(context.Background())
		}
		// Start owns the sole Wait, including readiness failure. Never signal its reaped PID.
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
	case <-done:
		if startErr != nil {
			return f, startErr
		}
		// Successful readiness and Accept may both be ready in this select.
		select {
		case f.conn = <-accepted:
		case <-f.ctx.Done():
			t.Fatal("started without control connection")
		}
	case <-f.ctx.Done():
		t.Fatal("fixture accept timeout")
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
	hello := f.next("hello")
	f.pid = hello.PID
	select {
	case <-done:
	case <-f.ctx.Done():
		t.Fatal("startup timeout")
	}
	if started != nil {
		f.s = started.(*session)
	}
	return f, startErr
}
func mustFixture(t *testing.T, mode string, configure func(*Options)) *fixture {
	t.Helper()
	f, e := newFixture(t, mode, configure)
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func (f *fixture) next(kind string) control {
	f.t.Helper()
	select {
	case c := <-f.events:
		if c.Type != kind {
			f.t.Fatalf("expected %s got %+v", kind, c)
		}
		return c
	case <-f.ctx.Done():
		f.t.Fatalf("waiting for %s: %v", kind, f.ctx.Err())
	}
	return control{}
}
func (f *fixture) send(c control) control {
	f.t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	c.ID = randomID()
	if e := f.encoder.Encode(c); e != nil {
		f.t.Fatal(e)
	}
	r := f.next("barrier")
	if r.ID != c.ID {
		f.t.Fatal("barrier mismatch")
	}
	if c.Type == "tool" {
		select {
		case <-f.toolReady:
		case <-f.ctx.Done():
			f.t.Fatal("tool lifeline did not connect")
		}
	}
	return r
}
func (f *fixture) event(kind string, fields map[string]any) {
	if fields == nil {
		fields = map[string]any{}
	}
	fields["type"] = kind
	f.send(control{Type: "event", Event: fields})
}
func assistant(stop string) map[string]any {
	return map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": "result"}}, "provider": "fixture", "model": "model", "stopReason": stop, "timestamp": 2}
}

type executionResult struct {
	receipt Execution
	err     error
}

func (f *fixture) execute() (Dispatch, <-chan executionResult) {
	d := Dispatch{Token: randomID()}
	d.Message = "Controller dispatch " + d.Token
	ch := make(chan executionResult, 1)
	go func() { r, e := f.s.Execute(f.ctx, d); ch <- executionResult{r, e} }()
	f.next("prompt")
	return d, ch
}
func (f *fixture) result(ch <-chan executionResult) executionResult {
	f.t.Helper()
	select {
	case r := <-ch:
		return r
	case <-f.ctx.Done():
		f.t.Fatal("Execute did not return")
	}
	return executionResult{}
}
func requireCode(t *testing.T, err error, code Code) *Failure {
	t.Helper()
	var f *Failure
	if !errors.As(err, &f) || f.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
	return f
}
func (f *fixture) assertWaiting(ch <-chan executionResult) {
	f.t.Helper()
	if _, e := f.s.Snapshot(f.ctx); e != nil {
		f.t.Fatal(e)
	}
	select {
	case r := <-ch:
		f.t.Fatalf("premature Execute: %+v", r)
	default:
	}
}

func TestExecuteAndConfirm(t *testing.T) {
	for _, mode := range []string{"normal", "event-before-ack", "entry-lag"} {
		t.Run(mode, func(t *testing.T) {
			f := mustFixture(t, mode, nil)
			_, ch := f.execute()
			if mode == "event-before-ack" {
				f.send(control{Type: "ack"})
			}
			if mode == "entry-lag" {
				f.send(control{Type: "entry-lag"})
			}
			if _, e := os.Stat(f.s.Identity().SessionFile); !errors.Is(e, os.ErrNotExist) {
				t.Fatalf("fixture history unexpectedly exists: %v", e)
			}
			f.send(control{Type: "message", Message: assistant("stop")})
			f.event("agent_end", nil)
			f.assertWaiting(ch)
			f.send(control{Type: "settle"})
			r := f.result(ch)
			if r.err != nil {
				t.Fatal(r.err)
			}
			if r.receipt.PromptEntryID == "" || r.receipt.SettledSeq <= r.receipt.StartSeq {
				t.Fatalf("invalid evidence: %+v", r.receipt)
			}
			if _, e := f.s.Confirm(f.ctx, r.receipt); e != nil {
				t.Fatal(e)
			}
			report, e := f.s.Close(f.ctx)
			if e != nil || !report.WaitCompleted || !report.SIGKILL || len(report.DiscoveryRemoved) != 1 {
				t.Fatalf("cleanup %+v %v", report, e)
			}
		})
	}
}
func ptr[T any](value T) *T { return &value }

func TestContextUsageQuery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fields  map[string]any
		tokens  *int64
		window  *int64
		percent *float64
	}{
		{name: "omitted"},
		{name: "null", fields: map[string]any{"contextUsage": nil}},
		{name: "post-compaction", fields: map[string]any{"contextUsage": map[string]any{"tokens": nil, "contextWindow": 100000, "percent": nil}}, window: ptr(int64(100000))},
		{name: "zero", fields: map[string]any{"contextUsage": map[string]any{"tokens": 0, "contextWindow": 100000, "percent": 0}}, tokens: ptr(int64(0)), window: ptr(int64(100000)), percent: ptr(float64(0))},
		{name: "over-capacity", fields: map[string]any{"tokens": map[string]any{"total": 9999999}, "contextUsage": map[string]any{"tokens": 105000, "contextWindow": 100000, "percent": 105}}, tokens: ptr(int64(105000)), window: ptr(int64(100000)), percent: ptr(float64(105))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, done := f.execute()
			f.send(control{Type: "message", Message: assistant("stop")})
			f.send(control{Type: "settle"})
			execution := f.result(done)
			if execution.err != nil {
				t.Fatal(execution.err)
			}
			f.send(control{Type: "stats", State: tc.fields})
			before := time.Now()
			usage, err := f.s.ContextUsage(f.ctx)
			if err != nil || usage.Identity != f.s.Identity() || usage.SampledAt.Before(before) || usage.Seq <= execution.receipt.SettledSeq || usage.ActivityEpoch != execution.receipt.ActivityEpoch || !reflect.DeepEqual(usage.Tokens, tc.tokens) || !reflect.DeepEqual(usage.ContextWindow, tc.window) || !reflect.DeepEqual(usage.Percent, tc.percent) {
				t.Fatalf("usage=%+v error=%v", usage, err)
			}
			if _, err := f.s.Confirm(f.ctx, execution.receipt); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContextUsageRejectsMalformedAndForeignStats(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields map[string]any
		code   Code
	}{
		{"wrong-session", map[string]any{"sessionId": "foreign"}, SessionChanged},
		{"wrong-file", map[string]any{"sessionFile": "/foreign/history.jsonl"}, SessionChanged},
		{"missing-identity", map[string]any{"sessionId": nil}, ProtocolFailed},
		{"wrong-identity-type", map[string]any{"sessionFile": 12}, ProtocolFailed},
		{"usage-array", map[string]any{"contextUsage": []any{}}, ProtocolFailed},
		{"missing-fields", map[string]any{"contextUsage": map[string]any{}}, ProtocolFailed},
		{"missing-tokens", map[string]any{"contextUsage": map[string]any{"contextWindow": 100, "percent": 1}}, ProtocolFailed},
		{"missing-percent", map[string]any{"contextUsage": map[string]any{"contextWindow": 100, "tokens": 1}}, ProtocolFailed},
		{"zero-window", map[string]any{"contextUsage": map[string]any{"contextWindow": 0, "tokens": 1, "percent": 1}}, ProtocolFailed},
		{"negative-token", map[string]any{"contextUsage": map[string]any{"contextWindow": 100, "tokens": -1, "percent": 1}}, ProtocolFailed},
		{"wrong-token-type", map[string]any{"contextUsage": map[string]any{"contextWindow": 100, "tokens": "1", "percent": 1}}, ProtocolFailed},
		{"negative-percent", map[string]any{"contextUsage": map[string]any{"contextWindow": 100, "tokens": 1, "percent": -1}}, ProtocolFailed},
		{"wrong-percent-type", map[string]any{"contextUsage": map[string]any{"contextWindow": 100, "tokens": 1, "percent": "1"}}, ProtocolFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			f.send(control{Type: "stats", State: tc.fields})
			usage, err := f.s.ContextUsage(f.ctx)
			requireCode(t, err, tc.code)
			if !reflect.DeepEqual(usage, ContextUsage{}) {
				t.Fatalf("invalid stats returned usage: %+v", usage)
			}
			_, err = f.s.Snapshot(f.ctx)
			requireCode(t, err, tc.code)
		})
	}
}

func TestContextUsageBoundedLifecycle(t *testing.T) {
	for _, mode := range []string{"timeout", "cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			f.send(control{Type: "hold-stats"})
			ctx, cancel := context.WithCancelCause(f.ctx)
			defer cancel(context.Canceled)
			done := make(chan error, 1)
			go func() { _, err := f.s.ContextUsage(ctx); done <- err }()
			f.next("stats-held")
			want := RPCUnresponsive
			if mode == "cancel" {
				cancel(&Failure{Code: Cancelled, Origin: ControllerUser, Message: "fixture cancellation"})
				want = Cancelled
			}
			if mode == "close" {
				report, err := f.s.Close(f.ctx)
				if err != nil || !report.WaitCompleted || !report.ProcessExited {
					t.Fatalf("cleanup=%+v error=%v", report, err)
				}
				want = ProcessExited
			}
			select {
			case err := <-done:
				requireCode(t, err, want)
			case <-f.ctx.Done():
				t.Fatal("stats query did not return")
			}
		})
	}
}

func TestSettledBeforeAckStillRequiresAck(t *testing.T) {
	f := mustFixture(t, "event-before-ack", nil)
	_, done := f.execute()
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	f.assertWaiting(done)
	f.send(control{Type: "ack"})
	r := f.result(done)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if _, err := f.s.Confirm(f.ctx, r.receipt); err != nil {
		t.Fatal(err)
	}
}

func TestPromptRequiresBothEventAndEntryEvidence(t *testing.T) {
	for _, mode := range []string{"no-token", "entry-lag"} {
		t.Run(mode, func(t *testing.T) {
			f := mustFixture(t, mode, nil)
			d, done := f.execute()
			if mode == "no-token" {
				f.send(control{Type: "entry", Entry: map[string]any{"type": "message", "message": map[string]any{"role": "user", "content": d.Message, "timestamp": 1}}})
			}
			f.send(control{Type: "message", Message: assistant("stop")})
			f.send(control{Type: "settle"})
			_ = requireCode(t, f.result(done).err, PromptNotObserved)
		})
	}
}

func TestDuplicateEntryTokenAndReusedDispatchToken(t *testing.T) {
	for _, duplicateEntry := range []bool{true, false} {
		t.Run(fmt.Sprint(duplicateEntry), func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			d, done := f.execute()
			if duplicateEntry {
				f.send(control{Type: "entry", Entry: map[string]any{"type": "message", "message": map[string]any{"role": "user", "content": d.Message, "timestamp": 3}}})
				_ = requireCode(t, f.result(done).err, AmbiguousExecution)
				return
			}
			f.send(control{Type: "message", Message: assistant("stop")})
			f.send(control{Type: "settle"})
			if r := f.result(done); r.err != nil {
				t.Fatal(r.err)
			}
			_, err := f.s.Execute(f.ctx, d)
			got := requireCode(t, err, InvalidDefinition)
			if got.DispatchAccepted != AcceptedNo {
				t.Fatalf("reused token dispatched: %+v", got)
			}
			if _, err := f.s.Snapshot(f.ctx); err != nil {
				t.Fatal("definition rejection closed usable handle", err)
			}
		})
	}
}

func TestDispatchAcceptance(t *testing.T) {
	for _, tc := range []struct {
		mode     string
		code     Code
		accepted DispatchAccepted
	}{{"reject", DispatchRejected, AcceptedNo}, {"no-token", PromptNotObserved, AcceptedYes}, {"noack", RPCUnresponsive, AcceptedUnknown}, {"disconnect", ProcessExited, AcceptedUnknown}} {
		t.Run(tc.mode, func(t *testing.T) {
			f := mustFixture(t, tc.mode, nil)
			_, ch := f.execute()
			r := f.result(ch)
			e := requireCode(t, r.err, tc.code)
			if e.DispatchAccepted != tc.accepted {
				t.Fatalf("accepted=%s", e.DispatchAccepted)
			}
			if tc.mode == "reject" {
				if _, e := f.s.Snapshot(f.ctx); e != nil {
					t.Fatal("rejected handle not reusable", e)
				}
			} else {
				report, _ := f.s.Close(f.ctx)
				if !report.WaitCompleted {
					t.Fatalf("child not reaped: %+v", report)
				}
			}
		})
	}
}
func TestTerminalOutcomes(t *testing.T) {
	for _, tc := range []struct {
		stop string
		code Code
		keep bool
	}{{"stop", "", true}, {"error", ProviderFailed, true}, {"length", OutputTruncated, true}, {"aborted", Cancelled, false}} {
		t.Run(tc.stop, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, ch := f.execute()
			f.send(control{Type: "message", Message: assistant(tc.stop)})
			if tc.stop != "aborted" {
				f.send(control{Type: "settle"})
			}
			r := f.result(ch)
			if tc.code == "" {
				if r.err != nil {
					t.Fatal(r.err)
				}
			} else {
				_ = requireCode(t, r.err, tc.code)
			}
			if tc.keep {
				if _, e := f.s.Snapshot(f.ctx); e != nil {
					t.Fatal(e)
				}
			} else {
				report, _ := f.s.Close(f.ctx)
				if !report.WaitCompleted {
					t.Fatal("not reaped")
				}
			}
		})
	}
}
func TestProviderRetryAndCompactionContinuation(t *testing.T) {
	for _, kind := range []string{"retry", "compaction"} {
		t.Run(kind, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, ch := f.execute()
			f.send(control{Type: "message", Message: assistant("error")})
			f.event("agent_end", nil)
			if kind == "retry" {
				f.event("auto_retry_start", nil)
			} else {
				f.event("compaction_start", nil)
			}
			f.assertWaiting(ch)
			if kind == "retry" {
				f.send(control{Type: "message", Message: assistant("stop")})
				f.event("auto_retry_end", map[string]any{"success": true})
			} else {
				f.event("compaction_end", map[string]any{"aborted": false, "willRetry": true})
				f.send(control{Type: "message", Message: assistant("stop")})
			}
			f.send(control{Type: "settle"})
			if r := f.result(ch); r.err != nil {
				t.Fatal(r.err)
			}
		})
	}
}
func TestStickyFailures(t *testing.T) {
	for _, tc := range []struct {
		name, event string
		fields      map[string]any
		code        Code
	}{{"retry-cancel", "auto_retry_end", map[string]any{"success": false, "finalError": "Retry cancelled"}, Cancelled}, {"retry-failed", "auto_retry_end", map[string]any{"success": false, "finalError": "exhausted"}, ProviderFailed}, {"compact-cancel", "compaction_end", map[string]any{"aborted": true}, Cancelled}, {"compact-error", "compaction_end", map[string]any{"aborted": false, "errorMessage": "failed"}, CompactionFailed}, {"thinking", "thinking_level_changed", map[string]any{"level": "low"}, ThinkingChanged}, {"extension", "extension_error", map[string]any{"error": "fixture"}, ExtensionFailed}} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, ch := f.execute()
			tc.fields["type"] = tc.event
			f.send(control{Type: "recover-after", Event: tc.fields})
			r := f.result(ch)
			e := requireCode(t, r.err, tc.code)
			if tc.code == Cancelled && e.Origin != ExternalAgentAbort {
				t.Fatalf("origin %s", e.Origin)
			}
		})
	}
}
func TestSteeringAndDuplicateToken(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		t.Run(fmt.Sprint(duplicate), func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			d, ch := f.execute()
			text := "human steer"
			if duplicate {
				text = d.Message
			}
			f.send(control{Type: "message", Message: map[string]any{"role": "user", "content": text, "timestamp": 3}})
			if duplicate {
				_ = requireCode(t, f.result(ch).err, AmbiguousExecution)
				return
			}
			f.send(control{Type: "message", Message: assistant("stop")})
			f.send(control{Type: "settle"})
			r := f.result(ch)
			if r.err != nil || r.receipt.ExtraUserInputs != 1 {
				t.Fatalf("result %+v", r)
			}
		})
	}
}
func TestConfirmInvalidation(t *testing.T) {
	for _, kind := range []string{"activity", "thinking-entry", "model-entry", "session", "branch", "thinking-back"} {
		t.Run(kind, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, ch := f.execute()
			f.send(control{Type: "message", Message: assistant("stop")})
			f.send(control{Type: "settle"})
			r := f.result(ch)
			if r.err != nil {
				t.Fatal(r.err)
			}
			expected := AmbiguousExecution
			switch kind {
			case "activity":
				f.event("agent_start", nil)
			case "thinking-entry":
				f.send(control{Type: "entry", Entry: map[string]any{"type": "thinking_level_change", "thinkingLevel": "low"}})
				expected = ThinkingChanged
			case "model-entry":
				f.send(control{Type: "entry", Entry: map[string]any{"type": "model_change", "provider": "fixture", "modelId": "other"}})
				expected = ModelChanged
			case "session":
				f.send(control{Type: "state", State: map[string]any{"sessionId": "other"}})
				expected = SessionChanged
			case "branch":
				empty := ""
				f.send(control{Type: "leaf", Leaf: &empty})
			case "thinking-back":
				expected = ThinkingChanged
				f.event("thinking_level_changed", map[string]any{"level": "low"})
				f.event("thinking_level_changed", map[string]any{"level": "high"})
			}
			_, e := f.s.Confirm(f.ctx, r.receipt)
			_ = requireCode(t, e, expected)
			report, _ := f.s.Close(f.ctx)
			if !report.WaitCompleted {
				t.Fatal("not reaped")
			}
		})
	}
}
func TestDialogsCancelled(t *testing.T) {
	for _, method := range []string{"select", "confirm", "input", "editor"} {
		t.Run(method, func(t *testing.T) {
			f := mustFixture(t, "dialog-"+method, nil)
			_, ch := f.execute()
			d := f.next("dialog")
			if d.Event["cancelled"] != true {
				t.Fatalf("dialog approved: %+v", d)
			}
			_ = requireCode(t, f.result(ch).err, InteractionRequired)
			r, _ := f.s.Close(f.ctx)
			if !r.WaitCompleted {
				t.Fatal("not reaped")
			}
		})
	}
}
func TestStartupFailureReapsChild(t *testing.T) {
	for _, mode := range []string{"startup-model", "no-discovery"} {
		t.Run(mode, func(t *testing.T) {
			f, e := newFixture(t, mode, nil)
			if e == nil {
				t.Fatal("unexpected startup success")
			}
			if e := syscall.Kill(f.pid, 0); !errors.Is(e, syscall.ESRCH) {
				t.Fatalf("startup leaked pid %d: %v", f.pid, e)
			}
			if _, e := os.Stat(filepath.Join(f.bridge, "fixture-session.json")); !errors.Is(e, os.ErrNotExist) {
				t.Fatalf("startup owned discovery not removed: %v", e)
			}
		})
	}
}
func TestCleanupReportConfirmsLocalClose(t *testing.T) {
	for _, tc := range []struct {
		name, expected string
		change         func(*CleanupReport)
		want           bool
	}{
		{"clean", "session", nil, true},
		{"wrong-session", "other", nil, false},
		{"missing-expected", "", nil, false},
		{"missing-reported", "session", func(r *CleanupReport) { r.Identity.SessionID = "" }, false},
		{"matching-empty-ids", "", func(r *CleanupReport) { r.Identity.SessionID = "" }, true},
		{"zero-report", "session", func(r *CleanupReport) { *r = CleanupReport{} }, false},
		{"wait-incomplete", "session", func(r *CleanupReport) { r.WaitCompleted = false }, false},
		{"process-not-exited", "session", func(r *CleanupReport) { r.ProcessExited = false }, false},
		{"unconfirmed", "session", func(r *CleanupReport) { r.Unconfirmed = []string{"observer"} }, false},
		{"wait-error", "session", func(r *CleanupReport) { r.WaitError = "exit status 3" }, false},
		{"kill-error", "session", func(r *CleanupReport) { r.KillError = "permission denied" }, false},
		{"discovery-error", "session", func(r *CleanupReport) { r.DiscoveryError = "ownership changed" }, false},
		{"empty-unconfirmed", "session", func(r *CleanupReport) { r.Unconfirmed = []string{} }, true},
		{"other-identity-fields", "session", func(r *CleanupReport) {
			r.Identity.HandleID, r.Identity.SessionFile, r.Identity.PID = "handle", "session.jsonl", 42
		}, true},
		{"diagnostics", "session", func(r *CleanupReport) {
			r.AbortAcknowledged, r.AbortBashAcknowledged, r.SIGKILL = true, true, true
			r.DiscoveryRemoved = []string{"owned.json"}
			r.StderrTruncated, r.StderrTail = true, "diagnostic"
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := CleanupReport{Identity: Identity{SessionID: "session"}, WaitCompleted: true, ProcessExited: true}
			if tc.change != nil {
				tc.change(&report)
			}
			if got := report.ConfirmsLocalClose(tc.expected); got != tc.want {
				t.Fatalf("confirmed=%t want=%t report=%+v", got, tc.want, report)
			}
		})
	}
}

func TestCloseExitStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		kill   bool
	}{
		{"clean", 0, false},
		{"nonzero", 3, false},
		{"sigkill", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			if tc.kill {
				if err := f.s.cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
			} else if err := f.encoder.Encode(control{Type: "exit", Count: tc.status}); err != nil {
				t.Fatal(err)
			}
			// Wait for the child to close stdout before Close sends its final signal.
			// Close still owns the sole process Wait.
			select {
			case <-f.s.readerDone:
			case <-f.ctx.Done():
				t.Fatal("child stdout did not close")
			}
			report, err := f.s.Close(f.ctx)
			if !report.WaitCompleted || !report.ProcessExited || report.Identity != f.s.Identity() || report.DiscoveryError != "" || len(report.Unconfirmed) != 0 {
				t.Fatalf("close report=%+v error=%v", report, err)
			}
			// Some systems return EPERM when signalling an already exited group.
			// That remains a cleanup error, independently of the child's exit status.
			if report.KillError != "" {
				if report.KillError != syscall.EPERM.Error() {
					t.Fatalf("unexpected kill error: %+v", report)
				}
				requireCode(t, err, CleanupFailed)
			} else if err != nil {
				t.Fatal(err)
			}
			wantWaitError := ""
			if tc.status != 0 {
				wantWaitError = fmt.Sprintf("exit status %d", tc.status)
			}
			if report.WaitError != wantWaitError || report.ConfirmsLocalClose(f.s.Identity().SessionID) != (tc.status == 0 && report.KillError == "") {
				t.Fatalf("exit status not reflected by strict confirmation: %+v", report)
			}
			again, againErr := f.s.Close(f.ctx)
			if !errors.Is(againErr, err) || !reflect.DeepEqual(again, report) {
				t.Fatalf("close outcome changed: %+v %v", again, againErr)
			}
		})
	}
}

func TestConcurrentCloseAndOwnership(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	sentinel := filepath.Join(f.bridge, "external.json")
	original := []byte(`{"sessionId":"external","piPid":1,"pid":1}`)
	if e := os.WriteFile(sentinel, original, 0600); e != nil {
		t.Fatal(e)
	}
	var reports [2]CleanupReport
	var errs [2]error
	var wg sync.WaitGroup
	for i := range reports {
		wg.Add(1)
		go func(i int) { defer wg.Done(); reports[i], errs[i] = f.s.Close(f.ctx) }(i)
	}
	wg.Wait()
	if !reflect.DeepEqual(reports[0], reports[1]) || errs[0] != errs[1] {
		t.Fatalf("inconsistent close: %+v / %+v", reports, errs)
	}
	if errs[0] != nil || !reports[0].WaitCompleted {
		t.Fatalf("cleanup %+v %v", reports[0], errs[0])
	}
	b, e := os.ReadFile(sentinel)
	if e != nil || string(b) != string(original) {
		t.Fatalf("external discovery modified: %s %v", b, e)
	}
}
func TestToolGroupAbort(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, _ = f.execute()
	r := f.send(control{Type: "tool"})
	if r.PID == 0 {
		t.Fatal("missing tool pid")
	}
	if _, e := f.s.Snapshot(f.ctx); e != nil {
		t.Fatal(e)
	}
	report, e := f.s.Close(f.ctx)
	if e != nil || !report.AbortAcknowledged || !report.WaitCompleted {
		t.Fatalf("cleanup %+v %v", report, e)
	}
	if e := syscall.Kill(r.PID, 0); !errors.Is(e, syscall.ESRCH) {
		t.Fatalf("detached tool leaked: %v", e)
	}
}

func TestToolFixtureLifelineStopsGrandchildWithoutNumericSignal(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	f.send(control{Type: "tool"})
	f.stopTool()
	// The tool retains its stdin; only closing the independent lifeline can release Wait here.
	f.send(control{Type: "wait-tool"})
	report, err := f.s.Close(f.ctx)
	if err != nil || !report.WaitCompleted {
		t.Fatalf("lifeline cleanup failed: %+v %v", report, err)
	}
	// Idempotent fixture cleanup must not signal the already reaped tool PID.
	f.stopTool()
}

func TestExecutionContextCauseClassification(t *testing.T) {
	root := errors.New("root branch failed")
	for _, tc := range []struct {
		name   string
		cause  error
		code   Code
		origin Origin
	}{
		{"bare-cancel", context.Canceled, InvalidDefinition, Definition},
		{"unclassified", root, InvalidDefinition, Definition},
		{"controller", &Failure{Code: Cancelled, Origin: ControllerUser}, Cancelled, ControllerUser},
		{"sigint", &Failure{Code: Cancelled, Origin: SignalINT}, Cancelled, SignalINT},
		{"sigterm", &Failure{Code: Cancelled, Origin: SignalTERM}, Cancelled, SignalTERM},
		{"sibling", &Failure{Code: Cancelled, Origin: FailFastSibling, Cause: root}, Cancelled, FailFastSibling},
		{"run-deadline", &Failure{Code: TimedOut, Origin: RunDeadline}, TimedOut, RunDeadline},
		{"attempt-deadline", &Failure{Code: TimedOut, Origin: AttemptDeadline}, TimedOut, AttemptDeadline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			ctx, cancel := context.WithCancelCause(f.ctx)
			defer cancel(nil)
			d := Dispatch{Token: randomID()}
			d.Message = "Controller " + d.Token
			done := make(chan executionResult, 1)
			go func() { r, err := f.s.Execute(ctx, d); done <- executionResult{r, err} }()
			f.next("prompt")
			// Fence the subprocess's prompt events before issuing the Controller stop cause.
			if _, err := f.s.Snapshot(f.ctx); err != nil {
				t.Fatal(err)
			}
			cancel(tc.cause)
			err := f.result(done).err
			got := requireCode(t, err, tc.code)
			cause := tc.cause
			var typed *Failure
			if errors.As(cause, &typed) {
				cause = typed.Cause
			}
			if got.Origin != tc.origin || (cause != nil && !errors.Is(err, cause)) {
				t.Fatalf("lost context cause: %+v", got)
			}
			report, _ := f.s.Close(context.Background())
			if !report.WaitCompleted {
				t.Fatalf("context termination leaked child: %+v", report)
			}
		})
	}
}

func TestHandleReuseUsesIncrementalHistory(t *testing.T) {
	const limit = 1024
	f := mustFixture(t, "normal", func(o *Options) { o.Policy.MaxFrameBytes = limit })
	fullQueries := 0
	for round := 0; round < 8; round++ {
		_, done := f.execute()
		f.send(control{Type: "message", Message: assistant("stop")})
		f.send(control{Type: "settle"})
		r := f.result(done)
		if r.err != nil {
			t.Fatalf("round %d: %v", round, r.err)
		}
		if _, err := f.s.Confirm(f.ctx, r.receipt); err != nil {
			t.Fatal(err)
		}
		stats := f.send(control{Type: "entries-stats"})
		if round == 0 {
			fullQueries = stats.FullQueries
		} else if stats.FullQueries != fullQueries {
			t.Fatalf("round %d reread full history: %d -> %d", round, fullQueries, stats.FullQueries)
		}
		if round == 7 && stats.HistoryBytes <= limit {
			t.Fatalf("history never exceeded frame limit: %d <= %d", stats.HistoryBytes, limit)
		}
	}
}

func TestMatchedMessageLifetimeDoesNotExhaustBacklog(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, done := f.execute()
	for batch := 0; batch < 33; batch++ {
		f.send(control{Type: "messages", Count: 32, Message: assistant("toolUse")})
		leaf := fmt.Sprintf("e%d", 1+(batch+1)*32)
		// A query using this cursor proves the previous batch has been consumed.
		f.send(control{Type: "entries-barrier", Leaf: &leaf})
	}
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	r := f.result(done)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if _, err := f.s.Confirm(f.ctx, r.receipt); err != nil {
		t.Fatal(err)
	}
}

func TestUnmatchedMessageBacklogStillFails(t *testing.T) {
	for _, kind := range []string{"unmatched-messages", "unmatched-entries"} {
		t.Run(kind, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, done := f.execute()
			leaf := "e1"
			f.send(control{Type: "entries-barrier", Leaf: &leaf})
			if err := f.encoder.Encode(control{Type: kind, Count: 1025, Message: assistant("toolUse")}); err != nil {
				t.Fatal(err)
			}
			got := requireCode(t, f.result(done).err, LimitExceeded)
			if got.Cleanup == nil || !got.Cleanup.WaitCompleted {
				t.Fatalf("backlog failure did not automatically close: %+v", got)
			}
		})
	}
}

func TestEmptyEntriesDoNotConsumeFrameBudget(t *testing.T) {
	f := mustFixture(t, "normal", func(o *Options) { o.Policy.MaxFrameBytes = 1024 })
	_, done := f.execute()
	leaf := "e1"
	barrier := f.send(control{Type: "entries-barrier", Leaf: &leaf})
	for i := 0; i < 80; i++ {
		barrier = f.send(control{Type: "entries-barrier", Leaf: &leaf, Count: barrier.Count + 1})
	}
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(done); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestDeltaOnlyDoesNotDriveEntriesPolling(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, done := f.execute()
	leaf := "e1"
	f.send(control{Type: "entries-barrier", Leaf: &leaf})
	before := f.send(control{Type: "entries-stats"})
	start := time.Now()
	for i := 0; i < 128; i++ {
		f.send(control{Type: "raw", Raw: strings.Repeat("{\"type\":\"message_update\"}\n", 64)})
		if _, err := f.s.Snapshot(f.ctx); err != nil {
			t.Fatal(err)
		}
	}
	after := f.send(control{Type: "entries-stats"})
	// Permit fallback ticks and one in-flight read, not one read per delta batch.
	bound := 2 + int(time.Since(start)/(50*time.Millisecond))
	if queries := after.Count - before.Count; queries > bound {
		t.Fatalf("delta stream caused %d entries queries, fallback bound %d", queries, bound)
	}
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(done); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestUnchangedEntriesPollingBacksOff(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, done := f.execute()
	leaf := "e1"
	f.send(control{Type: "entries-barrier", Leaf: &leaf})
	stats := f.send(control{Type: "entries-stats"})
	start := time.Now()
	f.send(control{Type: "wait-entries", Count: stats.Count + 5})
	if elapsed := time.Since(start); elapsed < 700*time.Millisecond {
		t.Fatalf("unchanged entries still busy-poll: five queries in %s", elapsed)
	}
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(done); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestSettledEntryLagCompletesWithoutAnotherEvent(t *testing.T) {
	f := mustFixture(t, "entry-lag", func(o *Options) { o.Policy.PromptObservationTimeout = 2 * time.Second })
	_, done := f.execute()
	m := assistant("stop")
	f.event("message_end", map[string]any{"message": m})
	f.send(control{Type: "settle"})
	stats := f.send(control{Type: "entries-stats"})
	// Ensure a post-settled query really saw the still-empty append history.
	f.send(control{Type: "wait-entries", Count: stats.Count + 1})
	f.send(control{Type: "entry-lag"})
	f.send(control{Type: "entry", Entry: map[string]any{"type": "message", "message": m}})
	r := f.result(done)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if _, err := f.s.Confirm(f.ctx, r.receipt); err != nil {
		t.Fatal(err)
	}
}

func TestCompressedLineageStillRejectsContradictions(t *testing.T) {
	for _, kind := range []string{"fork", "rewind", "hash", "order", "duplicate-token"} {
		t.Run(kind, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			d, done := f.execute()
			f.send(control{Type: "messages", Count: 32, Message: assistant("toolUse")})
			leaf := "e33"
			f.send(control{Type: "entries-barrier", Leaf: &leaf})
			switch kind {
			case "fork":
				f.send(control{Type: "entry", Entry: map[string]any{"type": "custom", "parentId": "e2"}})
			case "rewind":
				leaf = "e2"
				f.send(control{Type: "leaf", Leaf: &leaf})
			case "hash":
				f.event("message_end", map[string]any{"message": assistant("toolUse")})
				f.send(control{Type: "entry", Entry: map[string]any{"type": "message", "message": assistant("stop")}})
			case "order":
				f.event("message_end", map[string]any{"message": assistant("toolUse")})
				f.event("message_end", map[string]any{"message": assistant("stop")})
				f.send(control{Type: "entry", Entry: map[string]any{"type": "message", "message": assistant("stop")}})
				f.send(control{Type: "entry", Entry: map[string]any{"type": "message", "message": assistant("toolUse")}})
			case "duplicate-token":
				f.send(control{Type: "entry", Entry: map[string]any{"type": "message", "message": map[string]any{"role": "user", "content": d.Message}}})
			}
			if kind == "hash" || kind == "order" {
				f.send(control{Type: "settle"})
			}
			_ = requireCode(t, f.result(done).err, AmbiguousExecution)
		})
	}
}

func TestNativeRetryTerminalOrdering(t *testing.T) {
	for _, tc := range []struct {
		stop string
		code Code
	}{{"stop", ""}, {"length", OutputTruncated}, {"error", ProviderFailed}} {
		t.Run(tc.stop, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, done := f.execute()
			f.send(control{Type: "retry-outcome", Message: assistant(tc.stop)})
			r := f.result(done)
			if tc.code == "" {
				if r.err != nil {
					t.Fatal(r.err)
				}
			} else {
				_ = requireCode(t, r.err, tc.code)
			}
			if _, err := f.s.Snapshot(f.ctx); err != nil {
				t.Fatalf("settled retry outcome closed reusable handle: %v", err)
			}
		})
	}
}

func TestRetrySuccessOnlyClearsItsOriginalProviderError(t *testing.T) {
	for _, kind := range []string{"without-start", "newer-error"} {
		t.Run(kind, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, done := f.execute()
			f.send(control{Type: "message", Message: assistant("error")})
			if kind == "newer-error" {
				f.event("auto_retry_start", nil)
				f.send(control{Type: "message", Message: assistant("error")})
			}
			f.send(control{Type: "message", Message: assistant("stop")})
			f.event("auto_retry_end", map[string]any{"success": true})
			f.send(control{Type: "settle"})
			_ = requireCode(t, f.result(done).err, ProviderFailed)
		})
	}
}

func TestBaselineSettingsAreNotDispatchDrift(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	f.send(control{Type: "entry", Entry: map[string]any{"type": "thinking_level_change", "thinkingLevel": "off"}})
	f.send(control{Type: "entry", Entry: map[string]any{"type": "thinking_level_change", "thinkingLevel": "high"}})
	_, done := f.execute()
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(done); r.err != nil {
		t.Fatal(r.err)
	}
}

func TestConfirmPreservesUnsafeFailureAtLocalBoundary(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, done := f.execute()
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	r := f.result(done)
	if r.err != nil {
		t.Fatal(r.err)
	}
	f.send(control{Type: "hold-state"})
	confirmed := make(chan error, 1)
	go func() { _, err := f.s.Confirm(f.ctx, r.receipt); confirmed <- err }()
	f.next("held")
	f.event("compaction_end", map[string]any{"aborted": false, "errorMessage": "late summary failure"})
	f.send(control{Type: "release-state"})
	select {
	case err := <-confirmed:
		got := requireCode(t, err, CompactionFailed)
		if got.Cleanup == nil || !got.Cleanup.WaitCompleted {
			t.Fatalf("unsafe confirmation did not close: %+v", got)
		}
	case <-f.ctx.Done():
		t.Fatal("Confirm did not return")
	}
}

func TestExhaustedRetryThenCompactionFailureCloses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		event  map[string]any
		code   Code
		origin Origin
	}{
		{"error", map[string]any{"type": "compaction_end", "aborted": false, "errorMessage": "summary failed"}, CompactionFailed, Compaction},
		{"cancel", map[string]any{"type": "compaction_end", "aborted": true}, Cancelled, ExternalAgentAbort},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := mustFixture(t, "normal", nil)
			_, done := f.execute()
			f.send(control{Type: "retry-outcome", Message: assistant("error"), Event: tc.event})
			got := requireCode(t, f.result(done).err, tc.code)
			if got.Origin != tc.origin || got.Cleanup == nil || !got.Cleanup.WaitCompleted {
				t.Fatalf("unsafe failure did not automatically close: %+v", got)
			}
			_ = requireCode(t, got.Cause, ProviderFailed)
		})
	}
}

func TestTerminalErrorCannotBeClearedByUnprovenStop(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, done := f.execute()
	f.send(control{Type: "message", Message: assistant("error")})
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	err := requireCode(t, f.result(done).err, ProviderFailed)
	if err.Origin != Provider {
		t.Fatalf("wrong provider failure origin: %+v", err)
	}
	if state, err := f.s.Snapshot(f.ctx); err != nil || state.Health != "Online" {
		t.Fatalf("settled provider error was treated as offline: %+v %v", state, err)
	}
}
