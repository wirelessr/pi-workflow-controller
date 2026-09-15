// Package bundled runs the installed Pi executable against an isolated HTTP provider.
// It is test infrastructure only; it never substitutes the Pi RPC boundary.
package bundled

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const Provider = "pwc-bundled"
const Model = "pwc-exact-model"
const MaxTokens = 128

type Request struct {
	Index int
	Path  string
	Body  json.RawMessage
}
type Reply struct {
	Status        int
	Text, Finish  string
	OutputTokens  int
	ToolName      string
	ToolArguments map[string]any
	Barrier       <-chan struct{}
}
type Config struct {
	ContextWindow int
	RetryDelayMS  int
	MaxRetries    int
	Respond       func(context.Context, Request) Reply
	Script        []Reply
	Fixture       func(context.Context, Request)
}
type Record struct {
	Direction string         `json:"direction"`
	PID       int            `json:"pid"`
	Frame     map[string]any `json:"frame"`
}

// Launch contains the corresponding runtime.Options fields without importing runtime,
// so package runtime's private-writer tests can use this harness without an import cycle.
// External-package tests can use bundled/options.New to obtain runtime.Options directly.
type Launch struct {
	Executable string
	Args, Env  []string
}
type Harness struct {
	T                                    *testing.T
	Root, Artifacts, AgentDir, BridgeDir string
	Launch                               Launch
	Requests                             chan Request
	FixtureRequests                      chan Request
	Server                               *httptest.Server
	Ctx                                  context.Context
	cancel                               context.CancelFunc
	listener                             net.Listener
	mu                                   sync.Mutex
	conns                                []net.Conn
	records                              []Record
	traceBytes                           int
	changed                              chan struct{}
	stopped                              bool
	workers                              sync.WaitGroup
	stopOnce                             sync.Once
	providerCount                        int
	fixtureCount                         int
}

func Require(t *testing.T) {
	t.Helper()
	if os.Getenv("PWC_BUNDLED_PI") != "1" {
		t.Skip("bundled Pi integration skipped: opt in with PWC_BUNDLED_PI=1; requires installed Pi 0.84.3 and deployed WebUI")
	}
}

func New(t *testing.T, config Config) *Harness {
	t.Helper()
	Require(t)
	if config.ContextWindow == 0 {
		config.ContextWindow = 8192
	}
	if config.RetryDelayMS == 0 {
		config.RetryDelayMS = 25
	}
	config.Script = append([]Reply(nil), config.Script...)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	pi := filepath.Join(home, ".nvm/versions/node/v22.23.0/bin/pi")
	webui := filepath.Join(home, ".pi/agent/extensions/pi-webui-extension/index.ts")
	for _, path := range []string{pi, webui} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("bundled prerequisite unavailable: %s: %v", path, err)
		}
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	_, source, _, _ := goruntime.Caller(0)
	fixtureDir := filepath.Join(filepath.Dir(source), "../../../testdata/bundled")
	fixtureDir, err = filepath.Abs(fixtureDir)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(home, "WIP/pi-workflow-controller/verification/m5-bundled-dev")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	artifacts, err := os.MkdirTemp(base, strings.ReplaceAll(t.Name(), "/", "-")+"-")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	h := &Harness{T: t, Root: t.TempDir(), Artifacts: artifacts, Ctx: ctx, cancel: cancel, Requests: make(chan Request, 64), FixtureRequests: make(chan Request, 64), changed: make(chan struct{})}
	h.AgentDir, h.BridgeDir = filepath.Join(h.Root, "agent"), filepath.Join(h.Root, "bridge")
	for _, path := range []string{h.AgentDir, h.BridgeDir, filepath.Join(h.Root, "home"), filepath.Join(h.Root, "work"), filepath.Join(h.Root, "tmp")} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	h.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	h.workers.Add(1)
	go h.accept()
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.serve(config, w, r) }))
	t.Cleanup(func() {
		// This insurance does not call runtime.Close. Lifeline EOF kills only the still-owned PG.
		h.Stop()
		h.Server.CloseClientConnections()
		h.Server.Close()
		h.copyArtifacts()
	})
	settings := map[string]any{"enableInstallTelemetry": false, "defaultProjectTrust": "never", "packages": []any{}, "extensions": []any{}, "skills": []any{}, "prompts": []any{}, "themes": []any{}, "compaction": map[string]any{"enabled": true, "reserveTokens": 64, "keepRecentTokens": 1}, "retry": map[string]any{"enabled": true, "maxRetries": config.MaxRetries, "baseDelayMs": config.RetryDelayMS, "provider": map[string]any{"maxRetries": 0, "timeoutMs": 45000}}}
	models := map[string]any{"providers": map[string]any{Provider: map[string]any{"api": "openai-completions", "apiKey": "pwc-local-placeholder", "baseUrl": h.Server.URL + "/v1", "models": []any{map[string]any{"id": Model, "name": "Bundled exact model", "reasoning": false, "contextWindow": config.ContextWindow, "maxTokens": MaxTokens, "cost": map[string]any{"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}, "compat": map[string]any{"maxTokensField": "max_tokens", "supportsUsageInStreaming": true}}}}}}
	h.writeJSON(filepath.Join(h.AgentDir, "settings.json"), settings)
	h.writeJSON(filepath.Join(h.AgentDir, "models.json"), models)
	h.writeJSON(filepath.Join(h.AgentDir, "auth.json"), map[string]any{})
	env := []string{"HOME=" + filepath.Join(h.Root, "home"), "PATH=" + filepath.Dir(pi) + ":/usr/bin:/bin:/opt/homebrew/bin", "TMPDIR=" + filepath.Join(h.Root, "tmp"), "PI_CODING_AGENT_DIR=" + h.AgentDir, "PI_BRIDGE_DIR=" + h.BridgeDir, "PI_AUTO_NAME=0", "PI_OFFLINE=1", "PI_SKIP_VERSION_CHECK=1", "PI_TELEMETRY=0", "PI_HTTP_HOST=127.0.0.1", "PWC_FIXTURE_URL=" + h.Server.URL}
	// runtime merges Env with os.Environ. Erase inherited values even before env -i,
	// including credential names not known to this harness.
	var erased []string
	for _, v := range os.Environ() {
		key, _, _ := strings.Cut(v, "=")
		erased = append(erased, key+"=")
	}
	args := append([]string{"-i"}, env...)
	args = append(args, python, "-I", filepath.Join(fixtureDir, "launcher.py"), pi, fmt.Sprint(h.listener.Addr().(*net.TCPAddr).Port), artifacts, "--no-extensions", "-e", webui, "-e", filepath.Join(fixtureDir, "fixture.ts"), "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-approve", "--system-prompt", "Bundled integration test. Follow the user request.")
	h.Launch = Launch{Executable: "/usr/bin/env", Args: args, Env: erased}
	h.Save("launch.json", map[string]any{"pi": pi, "webui": webui, "args": args, "version": "0.84.3", "provider": Provider, "model": Model, "contextWindow": config.ContextWindow, "maxTokens": MaxTokens})
	t.Logf("bundled raw artifacts: %s", artifacts)
	return h
}

func (h *Harness) accept() {
	defer h.workers.Done()
	for {
		conn, err := h.listener.Accept()
		if err != nil {
			return
		}
		h.mu.Lock()
		if h.stopped {
			h.mu.Unlock()
			_ = conn.Close()
			return
		}
		h.conns = append(h.conns, conn)
		h.workers.Add(1)
		h.mu.Unlock()
		go func() {
			defer h.workers.Done()
			defer func(conn net.Conn) { _ = conn.Close() }(conn)
			reader := bufio.NewScanner(conn)
			reader.Buffer(make([]byte, 4096), 8<<20)
			// SIGKILL may interrupt the sidecar's final trace record. Preserve raw
			// pipe files, but never interpret a partial side-channel record as an RPC event.
			reader.Split(func(data []byte, atEOF bool) (int, []byte, error) {
				if i := bytes.IndexByte(data, '\n'); i >= 0 {
					return i + 1, data[:i], nil
				}
				if atEOF && len(data) > 0 {
					h.T.Log("tee ended with a partial side-channel record; raw pipe artifacts retained")
					return len(data), nil, nil
				}
				return 0, nil, nil
			})
			for reader.Scan() {
				var record Record
				if err := json.Unmarshal(reader.Bytes(), &record); err != nil {
					h.T.Errorf("invalid tee record: %v", err)
					return
				}
				h.mu.Lock()
				h.traceBytes += len(reader.Bytes())
				if len(h.records) >= 10000 || h.traceBytes > 16<<20 {
					h.mu.Unlock()
					h.T.Error("bounded trace record limit exceeded")
					return
				}
				h.records = append(h.records, record)
				close(h.changed)
				h.changed = make(chan struct{})
				h.mu.Unlock()
			}
			if err := reader.Err(); err != nil {
				h.mu.Lock()
				stopped := h.stopped
				h.mu.Unlock()
				if !stopped {
					h.T.Errorf("tee read: %v", err)
				}
			}
		}()
	}
}
func (h *Harness) Stop() {
	h.stopOnce.Do(func() {
		h.cancel()
		h.mu.Lock()
		h.stopped = true
		_ = h.listener.Close()
		for _, c := range h.conns {
			_ = c.Close()
		}
		h.mu.Unlock()
		h.workers.Wait()
	})
}
func (h *Harness) RequestCount() int { h.mu.Lock(); defer h.mu.Unlock(); return h.providerCount }

func (h *Harness) Records() []Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Record(nil), h.records...)
}
func (h *Harness) WaitRecord(ctx context.Context, after int, match func(Record) bool) (Record, int) {
	h.T.Helper()
	for {
		h.mu.Lock()
		for i := after; i < len(h.records); i++ {
			if match(h.records[i]) {
				record := h.records[i]
				h.mu.Unlock()
				return record, i + 1
			}
		}
		after = len(h.records)
		changed := h.changed
		h.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			h.T.Fatalf("RPC trace barrier: %v", ctx.Err())
		}
	}
}
func (h *Harness) NextRequest() Request {
	h.T.Helper()
	select {
	case r := <-h.Requests:
		return r
	case <-h.Ctx.Done():
		h.T.Fatal("provider request barrier timed out")
	}
	return Request{}
}
func (h *Harness) serve(config Config, w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		http.Error(w, "invalid bounded body", 400)
		return
	}
	h.mu.Lock()
	fixture := strings.HasPrefix(r.URL.Path, "/fixture/")
	var index int
	if fixture {
		h.fixtureCount++
		index = h.fixtureCount
	} else {
		h.providerCount++
		index = h.providerCount
	}
	h.mu.Unlock()
	request := Request{Index: index, Path: r.URL.Path, Body: raw}
	kind := "provider"
	if fixture {
		kind = "fixture"
	}
	h.writeJSON(filepath.Join(h.Artifacts, fmt.Sprintf("%s-%03d-request.json", kind, index)), request)
	channel := h.Requests
	if fixture {
		channel = h.FixtureRequests
	}
	select {
	case channel <- request:
	case <-h.Ctx.Done():
		return
	}
	if fixture {
		if config.Fixture != nil {
			config.Fixture(r.Context(), request)
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.URL.Path != "/v1/chat/completions" {
		http.Error(w, "unexpected provider path", 404)
		return
	}
	reply := Reply{Text: "bundled complete", Finish: "stop"}
	if config.Respond != nil {
		reply = config.Respond(r.Context(), request)
	} else if config.Script != nil {
		if index > len(config.Script) {
			h.T.Errorf("provider script exhausted at request %d", index)
			reply = Reply{Status: 400, Text: "provider script exhausted"}
		} else {
			reply = config.Script[index-1]
		}
	}
	select {
	case <-h.Ctx.Done():
		return
	default:
	}
	if reply.Barrier != nil {
		select {
		case <-reply.Barrier:
		case <-r.Context().Done():
			return
		case <-h.Ctx.Done():
			return
		}
	}
	h.writeJSON(filepath.Join(h.Artifacts, fmt.Sprintf("provider-%03d-reply.json", index)), struct {
		Status                 int
		Text, Finish, ToolName string
		OutputTokens           int
		ToolArguments          map[string]any
	}{reply.Status, reply.Text, reply.Finish, reply.ToolName, reply.OutputTokens, reply.ToolArguments})
	if reply.Status >= 400 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(reply.Status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": reply.Text, "type": "test_provider_error"}})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	send := func(delta map[string]any, finish any, usage any) {
		data := map[string]any{"id": fmt.Sprintf("pwc-%d", index), "object": "chat.completion.chunk", "created": 1, "model": Model, "choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}}
		if usage != nil {
			data["usage"] = usage
		}
		b, _ := json.Marshal(data)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
	}
	send(map[string]any{"role": "assistant"}, nil, nil)
	finish := reply.Finish
	if finish == "" {
		finish = "stop"
	}
	if reply.ToolName != "" {
		args, _ := json.Marshal(reply.ToolArguments)
		send(map[string]any{"tool_calls": []any{map[string]any{"index": 0, "id": fmt.Sprintf("call_%d", index), "type": "function", "function": map[string]any{"name": reply.ToolName, "arguments": string(args)}}}}, nil, nil)
		finish = "tool_calls"
	} else {
		send(map[string]any{"content": reply.Text}, nil, nil)
	}
	output := reply.OutputTokens
	if output == 0 {
		output = 8
	}
	send(map[string]any{}, finish, map[string]any{"prompt_tokens": 64, "completion_tokens": output, "total_tokens": 64 + output})
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
}
func (h *Harness) Save(name string, value any) {
	h.T.Helper()
	h.writeJSON(filepath.Join(h.Artifacts, name), value)
}
func (h *Harness) writeJSON(path string, value any) {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		err = os.WriteFile(path, append(raw, '\n'), 0600)
	}
	if err != nil {
		h.T.Errorf("artifact %s: %v", path, err)
	}
}
func (h *Harness) copyArtifacts() {
	h.Save("trace.json", h.Records())
	err := filepath.WalkDir(h.Root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(h.Root, path)
		target := filepath.Join(h.Artifacts, "owned", relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > 8<<20 {
			return fmt.Errorf("artifact too large: %s", relative)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, raw, 0600)
	})
	if err != nil {
		h.T.Errorf("save owned artifacts: %v", err)
	}
}
