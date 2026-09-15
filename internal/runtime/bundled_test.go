package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"pi-workflow-controller/internal/testutil/bundled"
)

type bundledFixture struct {
	h *bundled.Harness
	s *session
}

func bundledOptions(h *bundled.Harness) Options {
	p := DefaultPolicy()
	p.StartupTimeout = 20 * time.Second
	p.RPCTimeout = 3 * time.Second
	p.PromptAckTimeout = 5 * time.Second
	p.PromptObservationTimeout = 2 * time.Second
	p.CleanupTimeout = 8 * time.Second
	p.AbortGrace = time.Second
	return Options{Executable: h.Launch.Executable, Args: h.Launch.Args, Env: h.Launch.Env, BridgeDir: h.BridgeDir, Policy: p}
}
func bundledSpec(h *bundled.Harness, name string) SessionSpec {
	return SessionSpec{HandleID: name, Name: name, Model: ModelSpec{Provider: bundled.Provider, ID: bundled.Model, Thinking: "off"}, CWD: filepath.Join(h.Root, "work"), SessionDir: filepath.Join(h.Root, name, "pi")}
}
func openBundled(t *testing.T, config bundled.Config) *bundledFixture {
	t.Helper()
	h := bundled.New(t, config)
	p, err := New(bundledOptions(h))
	if err != nil {
		t.Fatal(err)
	}
	s, err := p.Start(h.Ctx, bundledSpec(h, "bundled"))
	if err != nil {
		var f *Failure
		errors.As(err, &f)
		h.Save("startup-failure.json", f)
		t.Fatal(err)
	}
	f := &bundledFixture{h: h, s: s.(*session)}
	t.Cleanup(func() {
		report, err := s.Close(context.Background())
		h.Save("cleanup.json", report)
		if err != nil {
			t.Logf("cleanup warning: %v", err)
		}
	})
	select {
	case request := <-h.FixtureRequests:
		var isolation struct {
			AgentDir            string `json:"agentDir"`
			BridgeDir           string `json:"bridgeDir"`
			Home                string `json:"home"`
			AutoName            string `json:"autoName"`
			CredentialEnvAbsent bool   `json:"credentialEnvAbsent"`
			SessionID           string `json:"sessionId"`
		}
		if request.Path != "/fixture/isolation" {
			t.Fatalf("missing extension startup isolation proof: %+v", request)
		}
		if err := json.Unmarshal(request.Body, &isolation); err != nil {
			t.Fatal(err)
		}
		if isolation.AgentDir != h.AgentDir || isolation.BridgeDir != h.BridgeDir || isolation.Home != filepath.Join(h.Root, "home") || isolation.AutoName != "0" || !isolation.CredentialEnvAbsent || isolation.SessionID != s.Identity().SessionID {
			t.Fatalf("Pi child isolation mismatch: %+v", isolation)
		}
	case <-h.Ctx.Done():
		t.Fatal("extension startup isolation barrier timed out")
	}
	return f
}
func (f *bundledFixture) execute(ctx context.Context, action string) (Dispatch, <-chan executionResult) {
	d := Dispatch{Token: randomID()}
	d.Message = "Controller dispatch " + d.Token + "\ncase:" + action
	done := make(chan executionResult, 1)
	go func() { receipt, err := f.s.Execute(ctx, d); done <- executionResult{receipt, err} }()
	return d, done
}
func (f *bundledFixture) result(done <-chan executionResult) executionResult {
	f.h.T.Helper()
	select {
	case result := <-done:
		f.h.Save("outcome-"+randomID()+".json", struct {
			Receipt Execution
			Error   string
		}{result.receipt, fmt.Sprint(result.err)})
		return result
	case <-f.h.Ctx.Done():
		f.h.T.Fatal("bundled Execute deadline")
	}
	return executionResult{}
}
func (f *bundledFixture) event(kind string, after int) (bundled.Record, int) {
	return f.h.WaitRecord(f.h.Ctx, after, func(r bundled.Record) bool { return r.Direction == "out" && r.Frame["type"] == kind })
}
func (f *bundledFixture) waiting(done <-chan executionResult) {
	f.h.T.Helper()
	if _, err := f.s.Snapshot(f.h.Ctx); err != nil {
		f.h.T.Fatal(err)
	}
	select {
	case result := <-done:
		f.h.T.Fatalf("premature completion: %+v", result)
	default:
	}
}
func (f *bundledFixture) success(done <-chan executionResult) Execution {
	f.h.T.Helper()
	result := f.result(done)
	if result.err != nil {
		f.h.T.Fatal(result.err)
	}
	if result.receipt.PromptEntryID == "" || result.receipt.SettledSeq <= result.receipt.StartSeq || result.receipt.StopReason != "stop" {
		f.h.T.Fatalf("invalid receipt: %+v", result.receipt)
	}
	if _, err := f.s.Confirm(f.h.Ctx, result.receipt); err != nil {
		f.h.T.Fatal(err)
	}
	return result.receipt
}
func (f *bundledFixture) closed() CleanupReport {
	f.h.T.Helper()
	r, err := f.s.Close(context.Background())
	f.h.Save("cleanup-asserted.json", r)
	if err != nil || !r.WaitCompleted || !r.ProcessExited || len(r.DiscoveryRemoved) != 1 || r.DiscoveryError != "" {
		f.h.T.Fatalf("cleanup: %+v %v", r, err)
	}
	f.s.mu.Lock()
	pending := len(f.s.pending)
	f.s.mu.Unlock()
	if pending != 0 {
		f.h.T.Fatalf("pending waiters: %d", pending)
	}
	return r
}

func TestBundledStartupMemoryReuseCleanup(t *testing.T) {
	release := make(chan struct{})
	defer closeIfOpen(release)
	f := openBundled(t, bundled.Config{Respond: func(_ context.Context, r bundled.Request) bundled.Reply {
		reply := bundled.Reply{Text: fmt.Sprintf("reply %d", r.Index)}
		if r.Index == 1 {
			reply.Barrier = release
		}
		return reply
	}})
	state, err := f.s.Snapshot(f.h.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.Model != bundledSpec(f.h, "bundled").Model || !state.HubVisible || state.Identity.SessionID == "" || state.Identity.PID != state.Identity.PGID || state.Identity.ParentPID != os.Getpid() {
		t.Fatalf("startup binding: %+v", state)
	}
	discoveryPath := filepath.Join(f.h.BridgeDir, state.Identity.SessionID+".json")
	raw, err := os.ReadFile(discoveryPath)
	if err != nil {
		t.Fatal(err)
	}
	var own discovery
	if err := json.Unmarshal(raw, &own); err != nil {
		t.Fatal(err)
	}
	if own.SessionID != state.Identity.SessionID || own.SessionFile != state.Identity.SessionFile || own.PiPID != state.Identity.PID || own.PID != os.Getpid() {
		t.Fatalf("visible ownership: %+v", own)
	}
	f.h.Save("startup.json", state)
	rpcState, err := f.s.query(f.h.Ctx, "get_state", nil)
	if err != nil {
		t.Fatal(err)
	}
	var details struct {
		SessionName string `json:"sessionName"`
	}
	if err := json.Unmarshal(rpcState.frame.Data, &details); err != nil || details.SessionName != "bundled" {
		t.Fatalf("exact visible name: %+v %v", details, err)
	}
	f.h.Save("discovery-before.json", own)
	d, done := f.execute(f.h.Ctx, "normal")
	request := f.h.NextRequest()
	if !strings.Contains(string(request.Body), d.Token) {
		t.Fatal("provider did not receive dispatch nonce")
	}
	entries, err := f.s.query(f.h.Ctx, "get_entries", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(entries.frame.Data), d.Token) {
		t.Fatal("memory entries lack current nonce")
	}
	if _, err := os.Stat(state.Identity.SessionFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first assistant barrier must precede disk flush: %v", err)
	}
	f.h.Save("first-provider-memory.json", json.RawMessage(entries.frame.Data))
	f.h.Save("first-provider-disk.json", map[string]any{"exists": false})
	f.waiting(done)
	close(release)
	first := f.success(done)
	d2, done := f.execute(f.h.Ctx, "second")
	request = f.h.NextRequest()
	if !strings.Contains(string(request.Body), d.Token) || !strings.Contains(string(request.Body), d2.Token) {
		t.Fatal("same handle lost prior context")
	}
	second := f.success(done)
	if second.Token != d2.Token || second.PromptEntryID == first.PromptEntryID || second.StartSeq <= first.SettledSeq {
		t.Fatalf("stale receipt reused: %+v / %+v", first, second)
	}
	before, err := os.ReadFile(state.Identity.SessionFile)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(f.h.BridgeDir, "external.json")
	sentinelBytes, err := json.Marshal(map[string]any{"sessionId": "external", "pid": os.Getpid(), "piPid": os.Getpid(), "sessionFile": state.Identity.SessionFile})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sentinel, sentinelBytes, 0600); err != nil {
		t.Fatal(err)
	}
	f.closed()
	after, err := os.ReadFile(state.Identity.SessionFile)
	if err != nil || string(after) != string(before) {
		t.Fatal("history not preserved", err)
	}
	after, err = os.ReadFile(sentinel)
	if err != nil || string(after) != string(sentinelBytes) {
		t.Fatal("foreign discovery changed", err)
	}
	p, err := New(bundledOptions(f.h))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := p.Start(f.h.Ctx, bundledSpec(f.h, "isolated"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r, _ := s2.Close(context.Background()); f.h.Save("second-cleanup.json", r) })
	fresh := &bundledFixture{h: f.h, s: s2.(*session)}
	if s2.Identity().SessionID == state.Identity.SessionID {
		t.Fatal("fresh handle reused session identity")
	}
	_, done = fresh.execute(f.h.Ctx, "fresh")
	request = f.h.NextRequest()
	if strings.Contains(string(request.Body), d.Token) || strings.Contains(string(request.Body), d2.Token) {
		t.Fatal("new handle inherited old context")
	}
	fresh.success(done)
	fresh.closed()
	files, err := os.ReadDir(f.h.BridgeDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".json") && file.Name() != "external.json" {
			t.Fatalf("discovery cleanup/recovery: %v", files)
		}
	}
	after, err = os.ReadFile(sentinel)
	if err != nil || string(after) != string(sentinelBytes) {
		t.Fatal("new isolated session touched live sentinel discovery", err)
	}
}
func closeIfOpen(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
func TestBundledInputActions(t *testing.T) {
	for _, action := range []string{"handled", "transform"} {
		t.Run(action, func(t *testing.T) {
			f := openBundled(t, bundled.Config{})
			d, done := f.execute(f.h.Ctx, action)
			ack, _ := f.h.WaitRecord(f.h.Ctx, 0, func(r bundled.Record) bool { return r.Direction == "out" && r.Frame["command"] == "prompt" })
			if ack.Frame["success"] != true {
				t.Fatal("input hook did not acknowledge", ack)
			}
			if action == "transform" {
				request := f.h.NextRequest()
				if strings.Contains(string(request.Body), d.Token) {
					t.Fatal("transform retained nonce")
				}
			}
			err := requireCode(t, f.result(done).err, PromptNotObserved)
			if err.DispatchAccepted != AcceptedYes || err.Cleanup == nil || !err.Cleanup.WaitCompleted {
				t.Fatalf("acceptance/close: %+v", err)
			}
			f.closed()
			if action == "handled" && f.h.RequestCount() != 0 {
				t.Fatal("handled input reached provider")
			}
			records := f.h.Records()
			for _, r := range records {
				if r.Direction == "out" && r.Frame["type"] == "message_end" {
					b, _ := json.Marshal(r.Frame["message"])
					if strings.Contains(string(b), d.Token) {
						t.Fatal("handled/transformed nonce was observed")
					}
				}
			}
		})
	}
}
func TestBundledDialogs(t *testing.T) {
	for _, method := range []string{"select", "confirm", "input", "editor"} {
		t.Run(method, func(t *testing.T) {
			f := openBundled(t, bundled.Config{})
			_, done := f.execute(f.h.Ctx, method)
			dialog, index := f.h.WaitRecord(f.h.Ctx, 0, func(r bundled.Record) bool {
				return r.Direction == "out" && r.Frame["type"] == "extension_ui_request" && r.Frame["method"] == method
			})
			response, _ := f.h.WaitRecord(f.h.Ctx, index, func(r bundled.Record) bool {
				return r.Direction == "in" && r.Frame["type"] == "extension_ui_response" && r.Frame["id"] == dialog.Frame["id"]
			})
			if response.Frame["cancelled"] != true {
				t.Fatal("dialog was approved")
			}
			for _, r := range f.h.Records()[:index] {
				if r.Direction == "out" && r.Frame["command"] == "prompt" {
					t.Fatal("dialog must precede prompt ack")
				}
			}
			_ = requireCode(t, f.result(done).err, InteractionRequired)
			f.closed()
			if f.h.RequestCount() != 0 {
				t.Fatal("dialog dispatched provider request")
			}
		})
	}
}
func TestBundledBusyRPCRejection(t *testing.T) {
	release := make(chan struct{})
	defer closeIfOpen(release)
	f := openBundled(t, bundled.Config{Respond: func(context.Context, bundled.Request) bundled.Reply {
		return bundled.Reply{Text: "complete", Barrier: release}
	}})
	_, done := f.execute(f.h.Ctx, "normal")
	f.h.NextRequest()
	extra := "rejected " + randomID()
	reply, err := f.s.request(f.h.Ctx, "prompt", map[string]any{"message": extra}, f.s.options.Policy.RPCTimeout, false)
	if err != nil || *reply.frame.Success || !strings.Contains(reply.frame.Error, "streaming") {
		t.Fatalf("real busy rejection: %+v %v", reply.frame, err)
	}
	entries, err := f.s.query(f.h.Ctx, "get_entries", nil)
	if err != nil || strings.Contains(string(entries.frame.Data), extra) {
		t.Fatal("rejected RPC appended user", err)
	}
	f.waiting(done)
	close(release)
	f.success(done)
	count := 0
	for _, r := range f.h.Records() {
		if r.Direction == "in" && r.Frame["type"] == "prompt" && r.Frame["message"] == extra {
			count++
			if _, ok := r.Frame["streamingBehavior"]; ok {
				t.Fatal("busy test queued input")
			}
		}
	}
	if count != 1 {
		t.Fatalf("rejected prompt was resent: %d", count)
	}
	f.closed()
}
func TestBundledProviderRetry(t *testing.T) {
	for _, mode := range []string{"success", "exhaust", "cancel-sleep"} {
		t.Run(mode, func(t *testing.T) {
			release := make(chan struct{})
			defer closeIfOpen(release)
			delay := 25
			if mode == "cancel-sleep" {
				delay = 30000
			}
			f := openBundled(t, bundled.Config{MaxRetries: 1, RetryDelayMS: delay, Respond: func(_ context.Context, r bundled.Request) bundled.Reply {
				if r.Index == 2 && mode == "success" {
					return bundled.Reply{Text: "retry recovered", Barrier: release}
				}
				reply := bundled.Reply{Status: 503, Text: "service unavailable: bundled retry"}
				if r.Index == 2 {
					reply.Barrier = release
				}
				return reply
			}})
			ctx, cancel := context.WithCancelCause(f.h.Ctx)
			defer cancel(nil)
			_, done := f.execute(ctx, "normal")
			_, start := f.event("auto_retry_start", 0)
			f.waiting(done)
			for _, r := range f.h.Records()[:start] {
				if r.Direction == "out" && r.Frame["type"] == "agent_settled" {
					t.Fatal("retry settled before recovery")
				}
			}
			switch mode {
			case "cancel-sleep":
				// The event proves Pi entered its agent-level delay; abort uses the runtime writer.
				cancel(&Failure{Code: Cancelled, Origin: ControllerUser, Message: "cancel during agent retry delay"})
				got := requireCode(t, f.result(done).err, Cancelled)
				if got.Origin != ControllerUser {
					t.Fatalf("cancel origin: %+v", got)
				}
				end, _ := f.event("auto_retry_end", start)
				if end.Frame["success"] != false || end.Frame["finalError"] != "Retry cancelled" {
					t.Fatalf("retry cancellation: %+v", end)
				}
			case "success":
				f.h.NextRequest()
				request := f.h.NextRequest()
				if request.Index != 2 {
					t.Fatalf("provider retry attempts: %+v", request)
				}
				f.waiting(done)
				close(release)
				f.success(done)
				end, endIndex := f.event("auto_retry_end", start)
				if end.Frame["success"] != true {
					t.Fatal("retry recovery not proven", end)
				}
				f.event("agent_settled", endIndex)
			default:
				f.h.NextRequest()
				request := f.h.NextRequest()
				if request.Index != 2 {
					t.Fatalf("retry request index: %d", request.Index)
				}
				f.waiting(done)
				close(release)
				_ = requireCode(t, f.result(done).err, ProviderFailed)
				end, endIndex := f.event("auto_retry_end", start)
				if end.Frame["success"] != false {
					t.Fatal("exhaustion missing", end)
				}
				f.event("agent_settled", endIndex)
				if _, err := f.s.Snapshot(f.h.Ctx); err != nil {
					t.Fatal("exhausted settled handle closed", err)
				}
			}
			f.closed()
			requests := 0
			for _, r := range f.h.Records() {
				if r.Direction == "in" && r.Frame["type"] == "prompt" {
					requests++
				}
			}
			if requests != 1 {
				t.Fatalf("Controller resent prompt during Pi retry: %d", requests)
			}
			if mode == "cancel-sleep" {
				f.h.NextRequest()
				select {
				case request := <-f.h.Requests:
					t.Fatalf("provider request after cancellation: %+v", request)
				default:
				}
			}
		})
	}
}

func TestBundledAutoCompaction(t *testing.T) {
	for _, mode := range []string{"success", "aborted", "failed"} {
		t.Run(mode, func(t *testing.T) {
			releaseSummary := make(chan struct{})
			defer closeIfOpen(releaseSummary)
			f := openBundled(t, bundled.Config{ContextWindow: 2048, Respond: func(_ context.Context, r bundled.Request) bundled.Reply {
				switch r.Index {
				case 1:
					return bundled.Reply{Text: strings.Repeat("eligible warmup history ", 40)}
				case 2:
					return bundled.Reply{Status: 400, Text: "context_length_exceeded: bundled overflow"}
				case 3:
					if mode == "failed" {
						return bundled.Reply{Status: 400, Text: "summary invalid request: permanent failure", Barrier: releaseSummary}
					}
					return bundled.Reply{Text: "## Goal\nContinue the current dispatch.\n## Progress\nWarmup complete.", Barrier: releaseSummary}
				default:
					return bundled.Reply{Text: "overflow continuation completed"}
				}
			}})
			_, warmup := f.execute(f.h.Ctx, "warmup")
			f.success(warmup)
			f.h.NextRequest()
			baseline := len(f.h.Records())
			action := "compact"
			if mode == "aborted" {
				action = "compact-cancel"
			}
			d, done := f.execute(f.h.Ctx, action)
			request := f.h.NextRequest()
			if request.Index != 2 || !strings.Contains(string(request.Body), d.Token) {
				t.Fatal("overflow not on current dispatch", request)
			}
			_, userIndex := f.h.WaitRecord(f.h.Ctx, baseline, func(r bundled.Record) bool {
				if r.Direction != "out" || r.Frame["type"] != "message_start" {
					return false
				}
				m, _ := r.Frame["message"].(map[string]any)
				b, _ := json.Marshal(m["content"])
				return m["role"] == "user" && strings.Contains(string(b), d.Token)
			})
			start, startIndex := f.event("compaction_start", userIndex)
			if start.Frame["reason"] != "overflow" {
				t.Fatalf("unexpected preflight/manual compaction: %+v", start)
			}
			if mode != "aborted" {
				summary := f.h.NextRequest()
				if summary.Index != 3 {
					t.Fatalf("summary request: %+v", summary)
				}
				f.waiting(done)
				close(releaseSummary)
			}
			end, endIndex := f.event("compaction_end", startIndex)
			if end.Frame["reason"] != "overflow" {
				t.Fatalf("wrong compaction end: %+v", end)
			}
			switch mode {
			case "success":
				if end.Frame["aborted"] != false || end.Frame["willRetry"] != true || end.Frame["result"] == nil {
					t.Fatalf("no compaction continuation: %+v", end)
				}
				f.success(done)
				f.event("agent_settled", endIndex)
			case "aborted":
				if end.Frame["aborted"] != true {
					t.Fatalf("hook did not abort: %+v", end)
				}
				if _, exists := end.Frame["result"]; exists {
					t.Fatal("Pi aborted result should be omitted, not null")
				}
				_ = requireCode(t, f.result(done).err, Cancelled)
			case "failed":
				if end.Frame["aborted"] != false || end.Frame["errorMessage"] == nil {
					t.Fatalf("no compaction failure: %+v", end)
				}
				_ = requireCode(t, f.result(done).err, CompactionFailed)
			}
			f.closed()
		})
	}
}

func TestBundledProcessAndPipeFailure(t *testing.T) {
	for _, mode := range []string{"process-exit", "pipe-failure"} {
		t.Run(mode, func(t *testing.T) {
			release := make(chan struct{})
			defer closeIfOpen(release)
			held := make(chan struct{}, 1)
			f := openBundled(t, bundled.Config{Fixture: func(ctx context.Context, r bundled.Request) {
				if r.Path == "/fixture/hold-input" {
					held <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
					}
				}
			}})
			_, done := f.execute(f.h.Ctx, "hold-input")
			select {
			case <-held:
			case <-f.h.Ctx.Done():
				t.Fatal("input ack barrier not reached")
			}
			for _, r := range f.h.Records() {
				if r.Direction == "out" && r.Frame["command"] == "prompt" {
					t.Fatal("prompt was acknowledged before barrier")
				}
			}
			code := ProcessExited
			if mode == "process-exit" {
				if err := f.s.cmd.Process.Kill(); err != nil {
					t.Fatal(err)
				}
			} else {
				code = ProtocolFailed
				if err := f.s.out.Close(); err != nil {
					t.Fatal(err)
				}
			}
			got := requireCode(t, f.result(done).err, code)
			if got.DispatchAccepted != AcceptedUnknown || got.Cleanup == nil || !got.Cleanup.WaitCompleted || !got.Cleanup.ProcessExited {
				t.Fatalf("unknown ack did not release/clean: %+v", got)
			}
			if got.Cleanup.DiscoveryError != "" || len(got.Cleanup.DiscoveryRemoved) != 1 {
				t.Fatalf("failure left discovery: %+v", got.Cleanup)
			}
			f.s.mu.Lock()
			pending := len(f.s.pending)
			f.s.mu.Unlock()
			if pending != 0 {
				t.Fatalf("pending waiters after failure: %d", pending)
			}
			f.h.Save("failure.json", got)
		})
	}
}

func TestBundledPartialStartup(t *testing.T) {
	for _, mode := range []string{"missing-executable", "invalid-model", "unsupported-thinking"} {
		t.Run(mode, func(t *testing.T) {
			h := bundled.New(t, bundled.Config{})
			options := bundledOptions(h)
			spec := bundledSpec(h, "startup")
			code := ProcessExited
			if mode == "missing-executable" {
				options.Executable = filepath.Join(h.Root, "absent")
				code = StartFailed
			}
			if mode == "invalid-model" {
				spec.Model.Provider = "pwc-provider-does-not-exist"
				spec.Model.ID = "pwc-model-does-not-exist"
			}
			if mode == "unsupported-thinking" {
				spec.Model.Thinking = "high"
				code = ThinkingChanged
			}
			p, err := New(options)
			if err != nil {
				t.Fatal(err)
			}
			started, err := p.Start(h.Ctx, spec)
			if started != nil {
				t.Cleanup(func() { _, _ = started.Close(context.Background()) })
				t.Fatal("invalid startup unexpectedly succeeded")
			}
			got := requireCode(t, err, code)
			h.Save("startup-failure.json", got)
			if got.DispatchAccepted != AcceptedNo {
				t.Fatalf("startup invented accepted dispatch: %+v", got)
			}
			if mode == "missing-executable" {
				if got.Cleanup != nil {
					t.Fatal("version failure invented owned session")
				}
				return
			}
			if got.Cleanup == nil || !got.Cleanup.WaitCompleted || !got.Cleanup.ProcessExited || got.Cleanup.DiscoveryError != "" {
				t.Fatalf("partial startup leaked owned child: %+v", got)
			}
			files, err := os.ReadDir(h.BridgeDir)
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if strings.HasSuffix(file.Name(), ".json") || strings.HasSuffix(file.Name(), ".recovering") {
					t.Fatalf("partial startup discovery retained: %s", file.Name())
				}
			}
		})
	}
}

type bundledTool struct {
	listener net.Listener
	conn     net.Conn
	reader   *bufio.Reader
	accepted chan struct{}
	pid      int
	nonce    string
	command  []string
}

func newBundledTool(t *testing.T) *bundledTool {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	path, err := filepath.Abs("../../testdata/bundled/tool.py")
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	tool := &bundledTool{listener: listener, accepted: make(chan struct{}), nonce: randomID()}
	tool.command = []string{python, "-I", path, fmt.Sprint(listener.Addr().(*net.TCPAddr).Port), tool.nonce}
	go func() { tool.conn, _ = listener.Accept(); close(tool.accepted) }()
	t.Cleanup(func() {
		_ = listener.Close()
		<-tool.accepted
		if tool.conn != nil {
			_ = tool.conn.Close()
		}
	})
	return tool
}
func (tool *bundledTool) ready(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-tool.accepted:
	case <-ctx.Done():
		t.Fatal("bash child did not connect")
	}
	if tool.conn == nil {
		t.Fatal("bash lifeline closed before accept")
	}
	_ = tool.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	tool.reader = bufio.NewReader(tool.conn)
	raw, err := tool.reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var hello struct {
		PID   int    `json:"pid"`
		Nonce string `json:"nonce"`
	}
	if err := json.Unmarshal(raw, &hello); err != nil || hello.Nonce != tool.nonce || hello.PID <= 0 {
		t.Fatalf("tool identity: %s %v", raw, err)
	}
	tool.pid = hello.PID
}
func (tool *bundledTool) ping(t *testing.T) {
	t.Helper()
	_ = tool.conn.SetDeadline(time.Now().Add(5 * time.Second))
	text := tool.nonce + "\n"
	if _, err := tool.conn.Write([]byte(text)); err != nil {
		t.Fatal("external sentinel stopped", err)
	}
	raw, err := tool.reader.ReadString('\n')
	if err != nil || raw != text {
		t.Fatalf("external sentinel changed: %q %v", raw, err)
	}
}
func TestBundledBashCancellation(t *testing.T) {
	bundled.Require(t)
	// Register both socket lifelines before spawning either child, independently of Close.
	tool := newBundledTool(t)
	sentinel := newBundledTool(t)
	external := exec.Command(sentinel.command[0], sentinel.command[1:]...)
	external.Env = []string{"PATH=/usr/bin:/bin"}
	external.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := external.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sentinel.listener.Close()
		<-sentinel.accepted
		if sentinel.conn != nil {
			_ = sentinel.conn.Close()
		} else {
			_ = external.Process.Kill()
		}
		_ = external.Wait()
	})
	f := openBundled(t, bundled.Config{Respond: func(_ context.Context, r bundled.Request) bundled.Reply {
		if r.Index == 1 {
			var quoted []string
			for _, arg := range tool.command {
				quoted = append(quoted, "'"+strings.ReplaceAll(arg, "'", "'\\''")+"'")
			}
			return bundled.Reply{ToolName: "bash", ToolArguments: map[string]any{"command": "exec " + strings.Join(quoted, " ")}}
		}
		return bundled.Reply{Text: "must not publish cancelled attempt"}
	}})
	sentinel.ready(t, f.h.Ctx)
	sentinel.ping(t)
	marker := filepath.Join(f.h.Root, "external-sentinel.txt")
	if err := os.WriteFile(marker, []byte(sentinel.nonce), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(f.h.Ctx)
	defer cancel(nil)
	_, done := f.execute(ctx, "bash")
	started, index := f.event("tool_execution_start", 0)
	if started.Frame["toolName"] != "bash" {
		t.Fatal("not the native bash tool", started)
	}
	tool.ready(t, f.h.Ctx)
	f.waiting(done)
	cancel(&Failure{Code: Cancelled, Origin: ControllerUser, Message: "abort native bash"})
	got := requireCode(t, f.result(done).err, Cancelled)
	if got.Origin != ControllerUser {
		t.Fatal("lost controller cancellation origin")
	}
	ended, _ := f.event("tool_execution_end", index)
	if ended.Frame["toolCallId"] != started.Frame["toolCallId"] {
		t.Fatal("wrong bash cancellation terminal")
	}
	report := f.closed()
	if !report.AbortAcknowledged || !report.AbortBashAcknowledged || !report.SIGKILL {
		t.Fatalf("bash cleanup not confirmed: %+v", report)
	}
	_ = tool.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err := tool.reader.ReadByte()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("native bash child lifeline still open: %v", err)
	}
	sentinel.ping(t)
	raw, err := os.ReadFile(marker)
	if err != nil || string(raw) != sentinel.nonce {
		t.Fatal("external marker changed", err)
	}
	f.h.Save("bash-evidence.json", map[string]any{"toolPID": tool.pid, "sentinelPID": sentinel.pid, "toolSocketEOF": true, "sentinelPingAfterClose": true, "markerUnchanged": true})
}

func TestBundledIndependentCleanupInsurance(t *testing.T) {
	f := openBundled(t, bundled.Config{})
	f.h.Stop()
	select {
	case <-f.s.processDone:
	case <-time.After(10 * time.Second):
		t.Fatal("independent lifeline failed to stop owned Pi")
	}
	report, _ := f.s.Close(context.Background())
	if !report.WaitCompleted || !report.ProcessExited || report.DiscoveryError != "" || len(report.DiscoveryRemoved) != 1 {
		t.Fatalf("insurance cleanup: %+v", report)
	}
	f.h.Save("insurance-cleanup.json", report)
}

func TestBundledTerminalOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply bundled.Reply
		code  Code
	}{
		{"stop", bundled.Reply{Text: "complete"}, ""},
		{"error", bundled.Reply{Status: 400, Text: "invalid request: test terminal failure"}, ProviderFailed},
		{"length", bundled.Reply{Text: "truncated", Finish: "length", OutputTokens: bundled.MaxTokens}, OutputTruncated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := openBundled(t, bundled.Config{Script: []bundled.Reply{tc.reply}})
			_, done := f.execute(f.h.Ctx, "normal")
			if tc.code == "" {
				f.success(done)
			} else {
				_ = requireCode(t, f.result(done).err, tc.code)
			}
			f.event("agent_settled", 0)
			if _, err := f.s.Snapshot(f.h.Ctx); err != nil {
				t.Fatal("settled terminal handle should remain usable", err)
			}
			f.closed()
		})
	}
}
