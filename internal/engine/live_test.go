package engine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

// No launcher, isolated agent directory, provider substitute, or Session wrapper.
// Start is the only external boundary adapter; returned receipts are untouched.
type engLiveRuntime struct {
	runtime.Runtime
	t                 *testing.T
	artifacts, bridge string
	started           chan runtime.Identity
}

func (r *engLiveRuntime) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	if err := engLivePreflight(r.t, r.bridge, r.artifacts, spec.HandleID); err != nil {
		return nil, err
	}
	engLiveSave(r.t, r.artifacts, "launch-"+spec.HandleID+".json", map[string]any{"at": time.Now().UTC(), "spec": spec, "runtime_options": "defaults; Observe delegates to Run.Observe", "bridge": r.bridge})
	s, err := r.Runtime.Start(ctx, spec)
	if s != nil {
		id := s.Identity()
		r.t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			report, err := s.Close(ctx)
			engLiveSave(r.t, r.artifacts, "insurance-"+id.HandleID+".json", map[string]any{"at": time.Now().UTC(), "report": report, "error": fmt.Sprint(err)})
			if err != nil {
				r.t.Errorf("own Pi cleanup insurance: %v", err)
			}
		})
		engLiveSave(r.t, r.artifacts, "identity-"+id.HandleID+".json", id)
		r.started <- id
	}
	return s, err
}

// This deliberately follows deployed index.ts isPidAlive and session-helpers.js:
// any failed kill(parent PID, 0) allows delete/resume, regardless of piPid.
// It is a read-only check, NOT a lock against concurrent shared Pi startup.
func engLivePreflight(t *testing.T, bridge, artifacts, label string) error {
	t.Helper()
	entries, err := os.ReadDir(bridge)
	if err != nil {
		return fmt.Errorf("BLOCKED shared discovery: %w", err)
	}
	var rows []map[string]any
	var blocked []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".recovering") {
			blocked = append(blocked, name+": recovery claim exists")
			rows = append(rows, map[string]any{"file": name, "action": "BLOCKED recovering"})
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(bridge, name))
		if err != nil {
			return fmt.Errorf("BLOCKED discovery changed/unreadable %s: %w", name, err)
		}
		var d map[string]any
		row := map[string]any{"file": name}
		if json.Unmarshal(raw, &d) != nil || d == nil || d["pid"] == nil || d["pid"] == false || d["pid"] == "" || d["pid"] == float64(0) {
			row["action"] = "skip: no parsed truthy pid"
		} else {
			pid, ok := d["pid"].(float64)
			if !ok || pid <= 0 || pid > 2147483647 || pid != float64(int(pid)) {
				row["action"] = "BLOCKED unsupported parent pid"
				blocked = append(blocked, name+": unsupported parent pid")
			} else {
				e := syscall.Kill(int(pid), 0)
				row["parent_pid"], row["kill_zero_error"] = int(pid), fmt.Sprint(e)
				row["action"] = "skip: parent alive"
				if e != nil {
					row["action"] = "BLOCKED: dead parent permits delete/resume"
					blocked = append(blocked, name+": dead parent permits delete/resume")
				}
			}
		}
		rows = append(rows, row)
	}
	engLiveSave(t, artifacts, "preflight-"+label+".json", map[string]any{"at": time.Now().UTC(), "bridge": bridge, "criterion": "deployed parent pid kill(0); no history read; no lock", "rows": rows, "blocked": blocked})
	if len(blocked) > 0 {
		return fmt.Errorf("BLOCKED unsafe shared discovery: %s", strings.Join(blocked, "; "))
	}
	return nil
}

func engLiveSave(t *testing.T, dir, name string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, name), append(raw, '\n'), 0600)
	}
	if err != nil {
		t.Errorf("save %s: %v", name, err)
	}
}

type engLiveTool struct {
	PID   int    `json:"pid"`
	PGID  int    `json:"pgid"`
	PPID  int    `json:"ppid"`
	Nonce string `json:"nonce"`
	Port  int    `json:"port"`
}
type engLiveArrival struct {
	tool engLiveTool
	err  error
}

func engLiveListener(t *testing.T) (*net.TCPListener, <-chan engLiveArrival) {
	t.Helper()
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	arrivals := make(chan engLiveArrival, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			arrivals <- engLiveArrival{err: err}
			return
		}
		defer func(conn net.Conn) { _ = conn.Close() }(conn)
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		var tool engLiveTool
		err = json.NewDecoder(io.LimitReader(conn, 4096)).Decode(&tool)
		arrivals <- engLiveArrival{tool, err}
	}()
	return l, arrivals
}

func engLiveProbe(t *testing.T, tool engLiveTool, nonce string) net.Conn {
	t.Helper()
	if tool.Nonce != nonce || tool.PID <= 1 || tool.PGID <= 1 || tool.Port <= 0 || tool.Port > 65535 {
		t.Fatalf("invalid helper identity: %+v", tool)
	}
	pgid, err := syscall.Getpgid(tool.PID)
	if err != nil || pgid != tool.PGID {
		t.Fatalf("helper PG identity: %+v actual=%d err=%v", tool, pgid, err)
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(tool.Port)), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	engLivePing(t, conn, nonce)
	return conn
}
func engLivePing(t *testing.T, conn net.Conn, nonce string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintln(conn, nonce); err != nil {
		t.Fatal(err)
	}
	got, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || got != nonce+"\n" {
		t.Fatalf("active socket barrier: %q %v", got, err)
	}
	_ = conn.SetDeadline(time.Time{})
}

func engLiveHub(t *testing.T, artifacts, label, path string) []byte {
	t.Helper()
	port := os.Getenv("PI_HUB_PORT")
	if port == "" {
		port = "8730"
	}
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 || n > 65535 {
		t.Fatal("BLOCKED invalid PI_HUB_PORT")
	}
	url := "http://127.0.0.1:" + port + path
	started := time.Now().UTC()
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get(url)
	if err != nil {
		t.Fatalf("BLOCKED existing hub GET %s: %v", path, err)
	}
	defer func(body io.ReadCloser) { _ = body.Close() }(response.Body)
	raw, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		t.Fatal(err)
	}
	engLiveSave(t, artifacts, label+"-http.json", map[string]any{"method": "GET", "url": url, "started_at": started, "finished_at": time.Now().UTC(), "status": response.StatusCode})
	if response.StatusCode != 200 || !json.Valid(raw) {
		t.Fatalf("hub GET %s status=%d (body not logged)", path, response.StatusCode)
	}
	// The aggregate list is filtered before persistence; never fetch others' history.
	if path != "/api/sessions" {
		if err := os.WriteFile(filepath.Join(artifacts, label+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return raw
}
func engLiveHubOwn(t *testing.T, artifacts, label string, id runtime.Identity, absent string) {
	t.Helper()
	raw := engLiveHub(t, artifacts, label+"-sessions", "/api/sessions")
	var list struct {
		Sessions []json.RawMessage `json:"sessions"`
	}
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatal(err)
	}
	var own []json.RawMessage
	for _, item := range list.Sessions {
		var s struct {
			PID int    `json:"pid"`
			SID string `json:"sessionId"`
		}
		if err := json.Unmarshal(item, &s); err != nil {
			t.Fatal(err)
		}
		if absent != "" && s.SID == absent {
			t.Fatal("old own session resurrected in hub: " + absent)
		}
		// Hub lists the discovery parent pid; /api/status returns the Pi pid.
		if s.PID == id.ParentPID {
			if s.SID != id.SessionID {
				t.Fatalf("hub ownership mismatch for own Pi %d", id.PID)
			}
			own = append(own, item)
		}
	}
	engLiveSave(t, artifacts, label+"-own-sessions.json", own)
	if len(own) != 1 {
		t.Fatalf("existing hub own PID %d matched %d entries", id.PID, len(own))
	}
	statusRaw := engLiveHub(t, artifacts, label+"-status", "/s/"+id.SessionID+"/api/status")
	var status struct {
		PID  int    `json:"pid"`
		SID  string `json:"sessionId"`
		File string `json:"sessionFile"`
	}
	if err := json.Unmarshal(statusRaw, &status); err != nil {
		t.Fatal(err)
	}
	if status.PID != id.PID || status.SID != id.SessionID || status.File != id.SessionFile {
		t.Fatalf("own hub status identity mismatch: %+v", status)
	}
}

func engLiveToolHistory(t *testing.T, artifacts string, id runtime.Identity, command, token string) {
	t.Helper()
	raw := engLiveHub(t, artifacts, "active-history", "/s/"+id.SessionID+"/api/history?limit=0")
	var history struct {
		History []struct {
			Role       string `json:"role"`
			Text       string `json:"text"`
			ToolCallID string `json:"toolCallId"`
			ToolCalls  []struct {
				ID        string `json:"id"`
				Name      string `json:"name"`
				Arguments struct {
					Command string `json:"command"`
				} `json:"arguments"`
			} `json:"toolCalls"`
		} `json:"history"`
	}
	if err := json.Unmarshal(raw, &history); err != nil {
		t.Fatal(err)
	}
	userSeen, callID := false, ""
	for _, entry := range history.History {
		if entry.Role == "user" && strings.Contains(entry.Text, token) {
			userSeen = true
		}
		for _, call := range entry.ToolCalls {
			if userSeen && entry.Role == "assistant" && call.Name == "bash" && call.Arguments.Command == command {
				callID = call.ID
			}
		}
		if callID != "" && entry.ToolCallID == callID {
			t.Fatal("bash already finished before active history barrier")
		}
	}
	if callID == "" {
		t.Fatal("hub history lacks current dispatch's exact native bash tool call")
	}
	engLiveSave(t, artifacts, "active-tool-call.json", map[string]any{"at": time.Now().UTC(), "session_id": id.SessionID, "dispatch_token": token, "tool_call_id": callID, "command": command})
}

func engLiveAncestry(t *testing.T, artifacts string, tool engLiveTool, pi runtime.Identity) {
	t.Helper()
	pid := tool.PID
	var chain []string
	for i := 0; i < 16 && pid > 1; i++ {
		raw, err := exec.Command("ps", "-o", "pid=,ppid=,pgid=", "-p", strconv.Itoa(pid)).Output()
		if err != nil {
			t.Fatal(err)
		}
		chain = append(chain, strings.TrimSpace(string(raw)))
		if pid == pi.PID {
			engLiveSave(t, artifacts, "active-ancestry.json", map[string]any{"at": time.Now().UTC(), "ps_columns": "pid ppid pgid", "chain": chain})
			return
		}
		fields := strings.Fields(string(raw))
		if len(fields) != 3 {
			t.Fatalf("bad ps identity: %q", raw)
		}
		pid, err = strconv.Atoi(fields[1])
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("helper is not descended from own Pi: %v", chain)
}

func engLiveGone(t *testing.T, artifacts, label, bridge string, id runtime.Identity, tool engLiveTool, sentinel net.Conn, nonce string) [32]byte {
	t.Helper()
	piErr, groupErr, toolErr, toolGroupErr := syscall.Kill(id.PID, 0), syscall.Kill(-id.PGID, 0), syscall.Kill(tool.PID, 0), syscall.Kill(-tool.PGID, 0)
	_, discoveryErr := os.Stat(filepath.Join(bridge, id.SessionID+".json"))
	_, claimErr := os.Stat(filepath.Join(bridge, id.SessionID+".json.recovering"))
	history, err := os.ReadFile(id.SessionFile)
	if err != nil || len(history) == 0 {
		t.Fatalf("own history not retained: %v", err)
	}
	hash := sha256.Sum256(history)
	engLivePing(t, sentinel, nonce)
	engLiveSave(t, artifacts, label+".json", map[string]any{"at": time.Now().UTC(), "identity": id, "tool": tool, "pi_pid_error": fmt.Sprint(piErr), "pi_pg_error": fmt.Sprint(groupErr), "tool_pid_error": fmt.Sprint(toolErr), "tool_pg_error": fmt.Sprint(toolGroupErr), "discovery_error": fmt.Sprint(discoveryErr), "claim_error": fmt.Sprint(claimErr), "history_sha256": fmt.Sprintf("%x", hash), "history_bytes": len(history), "sentinel_ping": "alive"})
	if piErr != syscall.ESRCH || groupErr != syscall.ESRCH || toolErr != syscall.ESRCH || toolGroupErr != syscall.ESRCH || !errors.Is(discoveryErr, os.ErrNotExist) || !errors.Is(claimErr, os.ErrNotExist) {
		t.Fatalf("own cleanup incomplete: Pi=%v PG=%v tool=%v toolPG=%v discovery=%v claim=%v", piErr, groupErr, toolErr, toolGroupErr, discoveryErr, claimErr)
	}
	return hash
}

func TestEngineLiveCancellationTimeoutRecovery(t *testing.T) {
	if os.Getenv("PWC_LIVE_PI") != "1" {
		t.Skip("live-provider/existing-hub cancellation, attempt timeout, native bash, sentinel and recovery skipped: opt in PWC_LIVE_PI=1; requires safe shared discovery and controlled non-parallel execution")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	bridge := filepath.Join(home, ".pi/agent/extensions/pi-webui-extension/data")
	if configured := os.Getenv("PI_BRIDGE_DIR"); configured != "" && filepath.Clean(configured) != bridge {
		t.Fatal("BLOCKED PI_BRIDGE_DIR is not deployed shared discovery")
	}
	if os.Getenv("PI_CODING_AGENT_DIR") != "" {
		t.Fatal("BLOCKED PI_CODING_AGENT_DIR override; live tests require default agent resources")
	}
	base := filepath.Join(home, "WIP/pi-workflow-controller/verification/m5-live-engine")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(base, "live-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("live raw artifacts: %s", root)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	helper, err := filepath.Abs(filepath.Join(cwd, "../../testdata/live/tool.py"))
	if err != nil {
		t.Fatal(err)
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"index.ts", "session-helpers.js"} {
		raw, err := os.ReadFile(filepath.Join(home, ".pi/agent/extensions/pi-webui-extension", source))
		if err != nil {
			t.Fatal(err)
		}
		engLiveSave(t, root, source+"-source.json", map[string]any{"path": filepath.Join(home, ".pi/agent/extensions/pi-webui-extension", source), "sha256": fmt.Sprintf("%x", sha256.Sum256(raw))})
	}
	if err := engLivePreflight(t, bridge, root, "initial"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		cancel bool
		state  State
		code   Code
		origin Origin
		exit   int
	}{
		{"controller-cancel", true, CancelledState, Cancelled, OriginControllerUser, 130},
		{"attempt-timeout", false, TimedOutState, TimedOut, OriginAttemptDeadline, 1},
	} {
		if !t.Run(tc.name, func(t *testing.T) {
			artifacts := filepath.Join(root, tc.name)
			work := filepath.Join(artifacts, "work")
			if err := os.MkdirAll(work, 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			t.Cleanup(cancel)
			// Sentinel is owned solely by the test, and remains unreaped until our kill/Wait.
			sl, sch := engLiveListener(t)
			sentinelNonce := contract.NewID()
			sentinelCmd := exec.Command(python, "-I", helper, strconv.Itoa(sl.Addr().(*net.TCPAddr).Port), sentinelNonce)
			sentinelCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			sentinelLog, err := os.OpenFile(filepath.Join(artifacts, "sentinel-output.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				t.Fatal(err)
			}
			sentinelCmd.Stdout, sentinelCmd.Stderr = sentinelLog, sentinelLog
			if err := sentinelCmd.Start(); err != nil {
				_ = sentinelLog.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				killErr := syscall.Kill(-sentinelCmd.Process.Pid, syscall.SIGKILL)
				waitErr := sentinelCmd.Wait()
				_ = sentinelLog.Close()
				engLiveSave(t, artifacts, "sentinel-test-cleanup.json", map[string]any{"at": time.Now().UTC(), "pid": sentinelCmd.Process.Pid, "kill_error": fmt.Sprint(killErr), "wait_error": fmt.Sprint(waitErr), "wait_completed": sentinelCmd.ProcessState != nil})
			})
			var sentinelTool engLiveTool
			select {
			case a := <-sch:
				if a.err != nil {
					t.Fatal(a.err)
				}
				sentinelTool = a.tool
			case <-ctx.Done():
				t.Fatal("sentinel barrier timed out")
			}
			sentinel := engLiveProbe(t, sentinelTool, sentinelNonce)
			if sentinelTool.PID != sentinelCmd.Process.Pid || sentinelTool.PGID != sentinelCmd.Process.Pid {
				t.Fatal("sentinel not in independent owned PG")
			}
			engLiveSave(t, artifacts, "sentinel-active.json", sentinelTool)

			l, arrivals := engLiveListener(t)
			nonce := contract.NewID()
			quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
			command := "exec " + quote(python) + " -I " + quote(helper) + " " + strconv.Itoa(l.Addr().(*net.TCPAddr).Port) + " " + nonce
			appendPrompt := "This is a bounded controller cancellation test, not an investigation or knowledge-management task. Read the Controller request, then execute request.prompt using the native bash tool before writing any output. The requested command is intentionally long-running and the Controller will stop it. Do not background, modify, wrap, or retry the command. Do not perform unrelated work."
			model := runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/gpt-oss-120b", Thinking: "low"}
			const stepTimeout = 90 * time.Second
			var run *Run
			pi, err := runtime.New(runtime.Options{Observe: func(ctx context.Context, o runtime.Observation) error { return run.Observe(ctx, o) }})
			if err != nil {
				t.Fatal(err)
			}
			boundary := &engLiveRuntime{Runtime: pi, t: t, artifacts: artifacts, bridge: bridge, started: make(chan runtime.Identity, 2)}
			definition := Definition{Name: "live-bash", Version: "test-only-v1", Policy: DefaultRunPolicy(), Execute: func(ctx context.Context, r *Run, _ Input) (Result, error) {
				h, err := r.OpenSession(ctx, RoleSpec{Name: "live-bash", Model: model, AppendPrompt: appendPrompt})
				if err != nil {
					return Result{}, err
				}
				_, err = r.Root().Step(ctx, StepSpec{Key: "active-bash", Session: h, Prompt: "Cancellation test: do not write candidate.json or inspect schemas. Immediately call the native bash tool with timeout 600 and this exact command, then await controller cancellation:\n" + command, Timeout: stepTimeout, Output: contract.Spec{SchemaID: engTestSchema}})
				return Result{}, err
			}}
			run, err = New(ctx, definition, Input{Prompt: "Live native bash cancellation boundary", LaunchCWD: work}, Options{BaseDir: artifacts, Schemas: engTestSchemas(t), Runtime: boundary, PiVersion: "0.84.3"})
			if err != nil {
				t.Fatal(err)
			}
			done := engTestExecuteAsync(t, run)
			var id runtime.Identity
			select {
			case id = <-boundary.started:
			case report := <-done:
				engLiveSave(t, artifacts, "early-report.json", report)
				t.Fatalf("Pi did not start: %v", report.Failure)
			case <-ctx.Done():
				t.Fatal("Pi start exceeded test bound")
			}
			if id.PGID == sentinelTool.PGID || id.PGID == syscall.Getpgrp() {
				t.Fatal("Pi/sentinel/controller PG not independent")
			}
			engLiveHubOwn(t, artifacts, "started", id, "")
			var tool engLiveTool
			select {
			case a := <-arrivals:
				if a.err != nil {
					t.Fatal(a.err)
				}
				tool = a.tool
			case report := <-done:
				engLiveSave(t, artifacts, "early-report.json", report)
				t.Fatalf("FAIL provider did not activate bash before Step ended: %v", report.Failure)
			case <-ctx.Done():
				t.Fatal("bash barrier exceeded test bound")
			}
			active := engLiveProbe(t, tool, nonce)
			engLiveAncestry(t, artifacts, tool, id)
			snapshot := run.Snapshot()
			if len(snapshot.Attempts) != 1 {
				t.Fatalf("active attempt count=%d", len(snapshot.Attempts))
			}
			var attempt AttemptState
			for _, a := range snapshot.Attempts {
				attempt = a
			}
			deadline := attempt.StartedAt.Add(stepTimeout)
			engLiveToolHistory(t, artifacts, id, command, attempt.Identity.DispatchToken)
			engLivePing(t, active, nonce)
			barrierAt := time.Now().UTC()
			if !barrierAt.Before(deadline) || attempt.Output != nil || attempt.Execution != nil {
				t.Fatal("tool was not active before actual Step deadline, or already settled/published")
			}
			engLiveSave(t, artifacts, "active-barrier.json", map[string]any{"at": barrierAt, "deadline": deadline, "step_timeout": stepTimeout.String(), "tool": tool, "attempt": attempt, "controller_cancel": tc.cancel})
			if tc.cancel {
				run.Cancel(OriginControllerUser)
			}
			var report Report
			select {
			case report = <-done:
			case <-ctx.Done():
				t.Fatal("engine did not finish within bounded live case")
			}
			engLiveSave(t, artifacts, "engine-report.json", report)
			engTestReport(t, report, tc.state, tc.exit)
			engDeadlineFailure(t, report.Failure, tc.code, tc.origin)
			engTestPersisted(t, run, report)
			terminal := report.Snapshot.Attempts[attempt.Identity.AttemptID]
			if terminal.State != tc.state || terminal.Failure == nil || terminal.Failure.Origin != tc.origin || terminal.DispatchAccepted != AcceptedYes || terminal.Output != nil || terminal.Execution != nil || len(report.Result.Outputs) != 0 || len(run.publications) != 0 {
				t.Fatalf("terminal boundary mismatch: %+v", terminal)
			}
			if !tc.cancel && terminal.FinishedAt.Before(deadline) {
				t.Fatal("timeout terminated before real Step deadline")
			}
			if len(report.Cleanup) != 1 {
				t.Fatalf("cleanup count=%d", len(report.Cleanup))
			}
			cleanup := report.Cleanup[0]
			if !cleanup.AbortAcknowledged || !cleanup.AbortBashAcknowledged || !cleanup.SIGKILL || !cleanup.WaitCompleted || !cleanup.ProcessExited || len(cleanup.Unconfirmed) != 0 || len(cleanup.DiscoveryRemoved) != 1 || cleanup.WaitError != "" || cleanup.KillError != "" || cleanup.DiscoveryError != "" {
				t.Fatalf("real cleanup report: %+v", cleanup)
			}
			// Socket EOF must come from controller cleanup, before test insurance closes it.
			_ = active.SetReadDeadline(time.Now().Add(5 * time.Second))
			var one [1]byte
			n, e := active.Read(one[:])
			if n != 0 || e != io.EOF {
				t.Fatalf("active native bash connection survived controller cleanup: n=%d err=%v", n, e)
			}
			hash := engLiveGone(t, artifacts, "after-cleanup", bridge, id, tool, sentinel, sentinelNonce)
			history, err := os.ReadFile(id.SessionFile)
			if err != nil || !bytes.Contains(history, []byte(attempt.Identity.DispatchToken)) {
				t.Fatal("own history lost current dispatch token")
			}

			// A fresh engine/Store session exercises the deployed startup recovery scan.
			// No provider prompt is needed: readiness is after extension recovery returns.
			var recovery *Run
			nextPi, err := runtime.New(runtime.Options{Observe: func(ctx context.Context, o runtime.Observation) error { return recovery.Observe(ctx, o) }})
			if err != nil {
				t.Fatal(err)
			}
			nextBoundary := &engLiveRuntime{Runtime: nextPi, t: t, artifacts: artifacts, bridge: bridge, started: make(chan runtime.Identity, 1)}
			release := make(chan struct{})
			nextDef := Definition{Name: "live-recovery-check", Version: "test-only-v1", Policy: DefaultRunPolicy(), Execute: func(ctx context.Context, r *Run, _ Input) (Result, error) {
				_, err := r.OpenSession(ctx, RoleSpec{Name: "recovery-probe", Model: model})
				if err != nil {
					return Result{}, err
				}
				select {
				case <-release:
					return Result{}, nil
				case <-ctx.Done():
					return Result{}, context.Cause(ctx)
				}
			}}
			recovery, err = New(ctx, nextDef, Input{Prompt: "Read-only startup recovery verification", LaunchCWD: work}, Options{BaseDir: artifacts, Schemas: engTestSchemas(t), Runtime: nextBoundary, PiVersion: "0.84.3"})
			if err != nil {
				t.Fatal(err)
			}
			nextDone := engTestExecuteAsync(t, recovery)
			var next runtime.Identity
			select {
			case next = <-nextBoundary.started:
			case rep := <-nextDone:
				engLiveSave(t, artifacts, "recovery-report.json", rep)
				t.Fatalf("recovery startup: %v", rep.Failure)
			case <-ctx.Done():
				t.Fatal("recovery start timed out")
			}
			if next.SessionID == id.SessionID || next.SessionFile == id.SessionFile {
				t.Fatal("fresh session reused old history")
			}
			engLiveHubOwn(t, artifacts, "recovery", next, id.SessionID)
			engLiveHub(t, artifacts, "recovery-history", "/s/"+next.SessionID+"/api/history?limit=0")
			if after := engLiveGone(t, artifacts, "after-next-start", bridge, id, tool, sentinel, sentinelNonce); after != hash {
				t.Fatalf("old history changed after next startup: %x -> %x", hash, after)
			}
			close(release)
			var nextReport Report
			select {
			case nextReport = <-nextDone:
			case <-ctx.Done():
				t.Fatal("recovery cleanup exceeded test bound")
			}
			engLiveSave(t, artifacts, "recovery-report.json", nextReport)
			engTestReport(t, nextReport, Succeeded, 0)
			engTestPersisted(t, recovery, nextReport)
			if len(nextReport.Cleanup) != 1 || !nextReport.Cleanup[0].WaitCompleted || !nextReport.Cleanup[0].ProcessExited || len(nextReport.Cleanup[0].DiscoveryRemoved) != 1 || syscall.Kill(-next.PGID, 0) != syscall.ESRCH {
				t.Fatalf("recovery probe cleanup: %+v", nextReport.Cleanup)
			}
			if _, err := os.Stat(filepath.Join(bridge, next.SessionID+".json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("recovery probe discovery remains: %v", err)
			}
			if after := engLiveGone(t, artifacts, "after-next-cleanup", bridge, id, tool, sentinel, sentinelNonce); after != hash {
				t.Fatal("old history changed after recovery probe cleanup")
			}
		}) {
			t.Fatal("live sequence stopped after failed/blocked case; no further shared Pi startup")
		}
	}
}
