package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
	"pi-workflow-controller/internal/tui"
	"pi-workflow-controller/internal/workflows"
	"pi-workflow-controller/internal/workflows/triage"
)

const cliFinalKey = "final\x1b]52;c;SECRET\a"

func cliProtocolOptions(t *testing.T, control, bridge, fault string) cliOptions {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	policy := engine.DefaultRunPolicy()
	policy.RunTimeout = 20 * time.Second
	policy.AttemptTimeout = 15 * time.Second
	policy.Runtime.StartupTimeout = 5 * time.Second
	policy.Runtime.RPCTimeout = 2 * time.Second
	policy.Runtime.PromptAckTimeout = 2 * time.Second
	policy.Runtime.PromptObservationTimeout = 5 * time.Second
	policy.Runtime.AbortGrace = time.Second
	policy.Runtime.CleanupTimeout = 4 * time.Second
	policy.Runtime.HealthInterval = time.Hour
	definition := engine.Definition{Name: "protocol", Version: "cli-test-v1", Policy: policy, Execute: func(ctx context.Context, r *engine.Run, input engine.Input) (engine.Result, error) {
		for _, name := range []string{"result.json", "cleanup.json"} {
			if strings.Contains(fault, strings.TrimSuffix(name, ".json")) {
				if err := os.Mkdir(filepath.Join(r.Dir(), name), 0700); err != nil {
					return engine.Result{}, err
				}
			}
		}
		h, err := r.OpenSession(ctx, engine.RoleSpec{Name: "worker", Model: runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}})
		if err != nil {
			return engine.Result{}, err
		}
		if strings.Contains(fault, "discovery") {
			for _, session := range r.Snapshot().Sessions {
				path := filepath.Join(bridge, session.Identity.SessionID+".json")
				if err := os.Remove(path); err != nil {
					return engine.Result{}, err
				}
				if err := os.Mkdir(path, 0700); err != nil {
					return engine.Result{}, err
				}
			}
		}
		first, err := r.Root().Step(ctx, engine.StepSpec{Key: "first", Session: h, Prompt: input.Prompt, Output: contract.Spec{SchemaID: "data.v1"}})
		if err != nil {
			return engine.Result{}, err
		}
		value, err := engine.Decode[protocol.Data](ctx, r, first.Output)
		if err != nil {
			return engine.Result{}, err
		}
		if value != (protocol.Data{Value: input.Prompt}) {
			return engine.Result{}, fmt.Errorf("first Ref data mismatch: %+v", value)
		}
		second, err := r.Root().Step(ctx, engine.StepSpec{Key: "second", Session: h, Prompt: "read the upstream Ref", Inputs: []contract.Ref{first.Output}, Feedback: &engine.Feedback{Message: input.Prompt, SourceAttemptID: first.AttemptID}, Output: contract.Spec{SchemaID: "data.v1"}})
		if err != nil {
			return engine.Result{}, err
		}
		value, err = engine.Decode[protocol.Data](ctx, r, second.Output)
		if err != nil {
			return engine.Result{}, err
		}
		if value != (protocol.Data{Value: input.Prompt + "/second", Source: first.AttemptID}) {
			return engine.Result{}, fmt.Errorf("second Ref data mismatch: %+v", value)
		}
		final := &engine.FinalSelection{Output: cliFinalKey}
		if fault == "artifact" {
			final.FileID = "readable"
		}
		return engine.Result{Outputs: map[string]contract.Ref{cliFinalKey: second.Output}, Final: final}, nil
	}}
	return cliOptions{
		definitions: []engine.Definition{definition},
		resources:   []contract.Resource{{URI: "https://fixture.local/data.json", JSON: json.RawMessage(`{"type":"object","required":["value","source"],"additionalProperties":false,"properties":{"value":{"type":"string"},"source":{"type":"string"}}}`)}},
		schemas:     []contract.SchemaDefinition{{ID: "data.v1", URI: "https://fixture.local/data.json"}},
		engine:      engine.Options{RuntimeOptions: runtime.Options{Executable: executable, Args: []string{"-test.run=^TestCLIProtocolSubprocess$", "--"}, Env: []string{"PWC_CLI_PI=1", "PWC_ENGINE_CONTROL=" + control, "GORACE=atexit_sleep_ms=0", "PI_CODING_AGENT_DIR=" + filepath.Join(bridge, "agent")}, BridgeDir: bridge}},
	}
}

func TestCLIRegisteredTriage(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(fmt.Sprintf("sources-complete-%t", complete), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			host, err := protocol.NewHost(ctx, 16)
			if err != nil {
				t.Fatal(err)
			}
			base := t.TempDir()
			service := protocol.SourceWorkspace(t)
			t.Setenv("PWC_PI_CWD", service)
			if err := os.Mkdir(filepath.Join(base, "bridge"), 0700); err != nil {
				t.Fatal(err)
			}
			opts := cliProtocolOptions(t, host.Addr().String(), filepath.Join(base, "bridge"), "")
			opts.definitions, opts.resources, opts.schemas = workflows.Definitions(), workflows.Resources(), workflows.Schemas()
			opts.engine.BaseDir = base
			opts.engine.RuntimeOptions.Env = append(opts.engine.RuntimeOptions.Env, "PWC_ENGINE_MANUAL_CANDIDATE=1")
			const original = "  匿名 CLI 調查\n保留來源與不確定性。  "
			scope := triage.Scope{Ticket: "CASE-17", TenantIDs: []string{}}
			var inputScope any = scope
			if complete {
				inputScope = map[string]any{"ticket": scope.Ticket}
			}
			raw, err := json.Marshal(map[string]any{"scope": inputScope, "request": original})
			if err != nil {
				t.Fatal(err)
			}
			var stdout, stderr bytes.Buffer
			done := make(chan int, 1)
			go func() {
				done <- run([]string{"run", "jira-triage", string(raw)}, strings.NewReader(""), &stdout, &stderr, opts)
			}()
			joined := false
			t.Cleanup(func() {
				if err := host.Close(); err != nil {
					t.Error(err)
				}
				if !joined {
					select {
					case <-done:
					case <-time.After(time.Minute):
						t.Error("CLI did not join after provider close")
					}
				}
			})
			var requests []contract.Request
			var runDir string
			gap := "Identity and incident time remain unresolved; no runtime authorization"
			gaps := []string{gap}
			if !complete {
				gaps = append(gaps, "Raw issue unavailable")
			}
			available := func(id string) triage.Source { return triage.Source{Status: "available", FileID: id} }
			missing := triage.Source{Status: "missing", Reason: "Raw issue unavailable"}
		loop:
			for {
				select {
				case code := <-done:
					joined = true
					if code != 0 {
						t.Fatalf("CLI failed: exit=%d stderr=%s stdout=%s", code, &stderr, &stdout)
					}
					break loop
				case <-ctx.Done():
					t.Fatal("CLI fixture deadline")
				case event := <-host.Events():
					if event.Err != nil {
						t.Fatal(event.Err)
					}
					if event.Message.Type == "hello" {
						if event.Message.CWD != service {
							t.Fatalf("Pi cwd=%q, want configured service %q", event.Message.CWD, service)
						}
						continue
					}
					if event.Message.Type != "prompt" {
						t.Fatalf("unexpected control %s", event.Message.Type)
					}
					var req contract.Request
					cliReadJSON(t, event.Message.RequestPath, &req)
					requests = append(requests, req)
					var task struct {
						Workspace      string                      `json:"workspace"`
						Requirements   string                      `json:"requirements"`
						Stage          string                      `json:"stage"`
						Request        string                      `json:"request"`
						Scope          triage.Scope                `json:"scope"`
						RuntimeAllowed bool                        `json:"runtime_resolution_allowed"`
						State          contract.Ref                `json:"state"`
						Context        contract.Ref                `json:"context"`
						Renderer       string                      `json:"renderer"`
						Checkpoint     *triage.PlannerCheckpoint   `json:"checkpoint"`
						Recovery       *triage.PlannerRecovery     `json:"recovery"`
						Verification   *triage.PlannerVerification `json:"verification"`
						M6             triage.ReportMetadata       `json:"m6"`
					}
					if err := json.Unmarshal([]byte(req.Prompt), &task); err != nil {
						t.Fatal(err)
					}
					if !filepath.IsAbs(task.Workspace) || filepath.Base(task.Workspace) != "triage-work" || !strings.Contains(task.Requirements, "Do not write to those repositories") {
						t.Fatal("triage task lacks run-owned workspace guidance")
					}
					if info, err := os.Stat(task.Workspace); err != nil || !info.IsDir() {
						t.Fatalf("triage workspace missing: %v", err)
					}
					if !strings.HasPrefix(event.Message.CandidatePath, filepath.Dir(task.Workspace)+string(os.PathSeparator)) || !filepath.IsAbs(event.Message.RequestPath) {
						t.Fatal("Step paths left owned run")
					}
					if err := os.WriteFile(filepath.Join(task.Workspace, "scratch.txt"), []byte("anonymous scratch"), 0600); err != nil {
						t.Fatal(err)
					}
					if task.Request != original {
						t.Fatal("registered CLI lost original request")
					}
					if task.Stage != "planner-report" && (task.Scope.Ticket != scope.Ticket || task.Scope.Stack != "" || task.Scope.Pop != "" || task.Scope.Binding != "" || len(task.Scope.TenantIDs) != 0) {
						t.Fatal("caller scope changed")
					}
					if task.RuntimeAllowed {
						t.Fatal("partial scope authorized runtime query")
					}
					files := map[string][]byte{}
					var data any
					switch req.Output.SchemaID {
					case triage.IntakeSchema:
						v := triage.Intake{Ticket: scope.Ticket, URL: "https://jira.example.invalid/browse/CASE-17", FetchedAt: "2025-01-01T00:00:00Z", Issue: missing, Fields: missing, Comments: []triage.CommentPage{}, Linked: []triage.LinkedIssue{}, Attachments: []triage.Attachment{}, Gaps: []string{"Raw issue unavailable"}}
						if complete {
							files["issue"] = []byte(`{"key":"CASE-17","fields":{"description":"Anonymous symptom","comment":{"total":0},"issuelinks":[],"attachment":[]}}`)
							files["fields"] = []byte(`[{"id":"description","name":"Description"}]`)
							files["comments"] = []byte(`{"startAt":0,"total":0,"comments":[]}`)
							v.Issue, v.Fields = available("issue"), available("fields")
							v.Comments = []triage.CommentPage{{Start: 0, Source: available("comments")}}
							v.Complete, v.Gaps = true, []string{}
						}
						data = v
					case triage.WikiSchema:
						files["search"] = []byte(`{"query":"anonymous symptom","matches":[],"complete":true}`)
						data = triage.WikiSearch{Intake: req.Inputs[0], Queries: []string{"anonymous symptom"}, Scope: "wiki-only", Status: "completed-no-matches", Pages: []triage.Source{}, Search: available("search"), Gaps: []string{}}
					case triage.ContextSchema:
						files["resolution"] = []byte("Inspected committed local sources; identity/time not established. No production queries.\n")
						e := []triage.Evidence{{FileID: "resolution"}}
						f := triage.Fact{Evidence: []triage.Evidence{}}
						data = triage.Context{Intake: req.Inputs[0], Wiki: req.Inputs[1], Scope: scope, Problem: "Anonymous symptom", Identity: triage.Identity{Status: "unresolved", Stack: f, Pop: f, Binding: f, TenantID: f, OrgKey: f, UserKey: f, Release: f}, Time: triage.TimeResolution{Status: "unresolved", Anchors: []triage.TimeAnchor{}}, Observations: []triage.Fact{}, Attempts: []triage.ResolutionAttempt{{Kind: "identity", Source: "committed local sources", Outcome: gap, Evidence: e}, {Kind: "time", Source: "committed local sources", Outcome: gap, Evidence: e}}, AttachmentComplete: true, WikiStatus: "completed-no-matches", Gaps: gaps, Readiness: "needs-resolution"}
					case triage.PlannerSchema:
						data = triage.PlannerState{Context: req.Inputs[0], Hypotheses: []triage.PlannerHypothesis{}, Pending: []triage.PlannerQuestion{}, Gaps: gaps, Rationale: "Preserve missing prerequisites", WorkerResults: []contract.Ref{}, WikiResults: []contract.Ref{}, Ledger: &triage.InvestigationLedger{Action: "yield", Reason: "Missing prerequisites", ConsumedBatch: []contract.Ref{}, Changes: []triage.HypothesisChange{}}, Checkpoint: task.Checkpoint, Recovery: task.Recovery, Verification: task.Verification}
					case triage.ReportSchema:
						task.M6.Dispositions = []triage.ReportDisposition{}
						data = triage.InvestigationReport{Request: original, State: task.State, Context: task.Context, Claims: []triage.ReportClaim{}, Completeness: "incomplete", Closure: "Needs authorized identity/time evidence", Gaps: gaps, NextSteps: []string{"Resolve prerequisites"}, ReportFile: triage.ReportFileID, M6: &task.M6}
						runDir = filepath.Dir(filepath.Dir(task.Renderer))
					default:
						t.Fatalf("unexpected schema %s", req.Output.SchemaID)
					}
					if req.Output.SchemaID == triage.PlannerSchema {
						encoded, err := json.Marshal(data)
						if err != nil {
							t.Fatal(err)
						}
						var snapshot map[string]json.RawMessage
						if err := json.Unmarshal(encoded, &snapshot); err != nil {
							t.Fatal(err)
						}
						for _, key := range []string{"worker_tasks", "worker_results", "wiki_results"} {
							snapshot[key] = json.RawMessage(`[]`)
						}
						data = snapshot
					}
					entries := []contract.FileEntry{}
					for id, body := range files {
						rel := "evidence/" + id + ".txt"
						path := filepath.Join(filepath.Dir(event.Message.CandidatePath), rel)
						if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(path, body, 0600); err != nil {
							t.Fatal(err)
						}
						entries = append(entries, contract.FileEntry{ID: id, Kind: "evidence", Path: rel})
					}
					if err := protocol.WriteEnvelope(event.Message.CandidatePath, req, data, entries); err != nil {
						t.Fatal(err)
					}
					if req.Output.SchemaID == triage.ReportSchema {
						cmd := exec.CommandContext(ctx, "python3", "-B", task.Renderer, event.Message.RequestPath, event.Message.CandidatePath)
						cmd.WaitDelay = time.Second
						if out, err := cmd.CombinedOutput(); err != nil {
							t.Fatalf("fixed report renderer: %v %s", err, out)
						}
					}
					if err := event.Reply(protocol.Control{Type: "settle"}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if len(requests) != 5 {
				t.Fatalf("pipeline stages=%d", len(requests))
			}
			var state engine.Snapshot
			cliReadJSON(t, filepath.Join(runDir, "run.json"), &state)
			for _, session := range state.Sessions {
				if session.Role.CWD != service || session.State != "Closed" {
					t.Fatal("persisted session cwd/close changed")
				}
			}
			if state.Input.Prompt != string(raw) || len(state.Attempts) != 5 {
				t.Fatal("CLI did not persist exact input/pipeline")
			}
			var result struct {
				Outputs map[string]contract.Ref `json:"outputs"`
				Final   *engine.FinalDelivery   `json:"final"`
			}
			cliReadJSON(t, filepath.Join(runDir, "result.json"), &result)
			if result.Final == nil || result.Final.Ref != result.Outputs["report"] || result.Final.Output != "report" {
				t.Fatal("CLI omitted committed final report")
			}
			producer := state.Sessions[result.Final.HandleID]
			if producer.Role.Name != "triage-planner" || producer.Role.Model != (runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/glm-5p3", Thinking: "high"}) || producer.State != "Closed" {
				t.Fatal("CLI final lost approved producer or cleanup")
			}
			var report contract.Publication[triage.InvestigationReport]
			cliReadJSON(t, result.Final.Ref.Path, &report)
			body, err := os.ReadFile(filepath.Join(filepath.Dir(result.Final.Ref.Path), report.Files[0].Path))
			if err != nil || report.Data.Request != original || !strings.Contains(string(body), original) || report.Data.Completeness != "incomplete" {
				t.Fatalf("CLI report lost request/limitations: %v", err)
			}
		})
	}
}

func TestCLIProtocolSubprocess(t *testing.T) {
	if os.Getenv("PWC_CLI_PI") != "1" {
		return
	}
	if marker := os.Getenv("PWC_CLI_PI_MARKER"); marker != "" {
		if err := os.WriteFile(marker, []byte("started"), 0600); err != nil {
			t.Fatal(err)
		}
		os.Exit(2)
	}
	for _, arg := range os.Args {
		if arg == "--version" {
			fmt.Println("0.84.3")
			os.Exit(0)
		}
	}
	if err := serveCLIProtocol(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

// Gate only the external RPC boundary, so the real runtime must await cleanup.
func serveCLIProtocol() error {
	address := os.Getenv("PWC_CLI_CLEANUP_CONTROL")
	if address == "" {
		return protocol.Serve()
	}
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return err
	}
	defer func(conn net.Conn) { _ = conn.Close() }(conn)
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(reader)
	defer func(f *os.File) { _ = f.Close() }(writer)
	output := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = output }()
	forwarded := make(chan error, 1)
	go func() {
		forwarded <- func() error {
			in, out := json.NewDecoder(reader), json.NewEncoder(output)
			for {
				var raw json.RawMessage
				if err := in.Decode(&raw); err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				var response struct{ Type, Command string }
				if err := json.Unmarshal(raw, &response); err != nil {
					return err
				}
				if response.Type == "response" && response.Command == "abort_bash" {
					if err := json.NewEncoder(conn).Encode(protocol.Control{Type: "cleanup-blocked"}); err != nil {
						return err
					}
					var ack protocol.Control
					if err := json.NewDecoder(conn).Decode(&ack); err != nil {
						return err
					}
					if ack.Type != "release" {
						return fmt.Errorf("unexpected cleanup ack: %+v", ack)
					}
				}
				if err := out.Encode(raw); err != nil {
					return err
				}
			}
		}()
	}()
	err = protocol.Serve()
	_ = writer.Close()
	return errors.Join(err, <-forwarded)
}

type cliSubprocessConfig struct{ Control, Bridge, Fault, OutputControl, OutputError, CleanupControl, TTYPath string }

// Only the external executable boundary is replaced; this calls the same run
// entry point as main, with real argv, signal.Notify, runtime, and Store.
func TestCLIApplicationSubprocess(t *testing.T) {
	configJSON := os.Getenv("PWC_CLI_APPLICATION")
	if configJSON == "" {
		return
	}
	var config cliSubprocessConfig
	if err := json.Unmarshal([]byte(configJSON), &config); err != nil {
		t.Fatal(err)
	}
	opts := cliProtocolOptions(t, config.Control, config.Bridge, config.Fault)
	if config.CleanupControl != "" {
		opts.engine.RuntimeOptions.Env = append(opts.engine.RuntimeOptions.Env, "PWC_CLI_CLEANUP_CONTROL="+config.CleanupControl)
	}
	var output io.Writer = os.Stdout
	if config.OutputError != "" && config.OutputError != "tty-active" {
		output = &cliFailOutputWriter{output: output, phase: config.OutputError}
	}
	if config.OutputControl != "" {
		conn, err := net.DialTimeout("tcp", config.OutputControl, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer func(conn net.Conn) { _ = conn.Close() }(conn)
		if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if config.OutputError == "tty-active" {
			go func() {
				if err := cliReplaceTTYOutput(conn, config.TTYPath); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(2)
				}
			}()
		} else {
			output = &cliBarrierWriter{output: output, conn: conn}
		}
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Exit(run(os.Args[i+1:], os.Stdin, output, os.Stderr, opts))
		}
	}
	t.Fatal("missing argv separator")
}

// Replace only the OS output boundary, not Tea or the cmd writer. fd 1 remains
// a terminal, but its next write receives a real EBADF.
func cliReplaceTTYOutput(conn net.Conn, path string) error {
	readonly, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(readonly)
	if err := json.NewEncoder(conn).Encode(protocol.Control{Type: "output-armed"}); err != nil {
		return err
	}
	var ack protocol.Control
	if err := json.NewDecoder(conn).Decode(&ack); err != nil {
		return err
	}
	if ack.Type != "release" {
		return fmt.Errorf("unexpected output ack: %+v", ack)
	}
	return syscall.Dup2(int(readonly.Fd()), int(os.Stdout.Fd()))
}

type cliBarrierWriter struct {
	output io.Writer
	conn   net.Conn
	once   sync.Once
	err    error
}

func (w *cliBarrierWriter) Write(p []byte) (int, error) {
	w.once.Do(func() {
		w.err = json.NewEncoder(w.conn).Encode(protocol.Control{Type: "output-blocked"})
		if w.err != nil {
			return
		}
		var ack protocol.Control
		w.err = json.NewDecoder(w.conn).Decode(&ack)
		if w.err == nil && ack.Type != "release" {
			w.err = fmt.Errorf("unexpected output ack: %+v", ack)
		}
	})
	if w.err != nil {
		return 0, w.err
	}
	return w.output.Write(p)
}

type cliFailOutputWriter struct {
	output io.Writer
	phase  string
}

func (w *cliFailOutputWriter) Write(p []byte) (int, error) {
	if w.phase == "display" || w.phase == "report" && bytes.HasPrefix(p, []byte("Outcome:")) {
		return 0, errors.New("closed output\x1b]52;c;SECRET\a")
	}
	return w.output.Write(p)
}

type cliControl struct {
	conn net.Conn
	in   *json.Decoder
	out  *json.Encoder
}

func (c *cliControl) next(t *testing.T, kind string) protocol.Control {
	t.Helper()
	var message protocol.Control
	if err := c.in.Decode(&message); err != nil {
		t.Fatalf("waiting for %s: %v", kind, err)
	}
	if message.Type != kind {
		t.Fatalf("control=%+v, want %s", message, kind)
	}
	return message
}
func (c *cliControl) exited(t *testing.T) {
	t.Helper()
	var message protocol.Control
	if err := c.in.Decode(&message); !errors.Is(err, io.EOF) {
		t.Fatalf("fixture control still open after cleanup: %+v, %v", message, err)
	}
}

func (c *cliControl) send(t *testing.T, kind string) {
	t.Helper()
	if err := c.out.Encode(protocol.Control{Type: kind}); err != nil {
		t.Fatal(err)
	}
}

type cliProcess struct {
	cmd                      *exec.Cmd
	done                     chan struct{}
	err                      error
	stdout, stderr           bytes.Buffer
	home, bridge             string
	listener, outputListener *net.TCPListener
	cleanupListener          *net.TCPListener
	controls                 []*cliControl
	deadline                 time.Time
}

type cliProcessOptions struct {
	stdin, stdout  *os.File
	cleanupBarrier bool
	ttyPath        string
	env            []string
}

func startCLIProcess(t *testing.T, prompt, fault, outputError string, blocked bool, options ...cliProcessOptions) *cliProcess {
	t.Helper()
	p := &cliProcess{home: t.TempDir(), done: make(chan struct{}), deadline: time.Now().Add(30 * time.Second)}
	p.bridge = filepath.Join(p.home, "bridge")
	if err := os.Mkdir(p.bridge, 0700); err != nil {
		t.Fatal(err)
	}
	listen := func() *net.TCPListener {
		l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Close() })
		if err := l.SetDeadline(p.deadline); err != nil {
			t.Fatal(err)
		}
		return l
	}
	p.listener = listen()
	config := cliSubprocessConfig{Control: p.listener.Addr().String(), Bridge: p.bridge, Fault: fault, OutputError: outputError}
	if blocked {
		p.outputListener = listen()
		config.OutputControl = p.outputListener.Addr().String()
	}
	for _, option := range options {
		config.TTYPath = option.ttyPath
		if option.cleanupBarrier {
			p.cleanupListener = listen()
			config.CleanupControl = p.cleanupListener.Addr().String()
		}
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p.cmd = exec.Command(executable, "-test.run=^TestCLIApplicationSubprocess$", "--", "run", "protocol", prompt)
	p.cmd.Dir = p.home
	p.cmd.Env = append(os.Environ(), "HOME="+p.home, "PWC_CLI_APPLICATION="+string(raw), "GORACE=atexit_sleep_ms=0")
	p.cmd.Stdout, p.cmd.Stderr = &p.stdout, &p.stderr
	for _, option := range options {
		if option.stdin != nil {
			p.cmd.Stdin = option.stdin
		}
		if option.stdout != nil {
			p.cmd.Stdout = option.stdout
		}
		p.cmd.Env = append(p.cmd.Env, option.env...)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		// Closing each test-owned lifeline exits only that fixture process. The
		// CLI's exec.Cmd is the sole signal target and has exactly one Wait.
		for _, c := range p.controls {
			_ = c.conn.Close()
		}
		select {
		case <-p.done:
			return
		default:
		}
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(6 * time.Second):
			_ = p.cmd.Process.Kill()
			select {
			case <-p.done:
			case <-time.After(3 * time.Second):
				t.Error("CLI subprocess did not join during cleanup")
			}
		}
	})
	return p
}
func (p *cliProcess) accept(t *testing.T, l *net.TCPListener) *cliControl {
	t.Helper()
	conn, err := l.Accept()
	if err != nil {
		select {
		case <-p.done:
			t.Fatalf("accept: %v; exit=%v stdout=%s stderr=%s", err, p.err, &p.stdout, &p.stderr)
		default:
			t.Fatal(err)
		}
	}
	c := &cliControl{conn: conn, in: json.NewDecoder(conn), out: json.NewEncoder(conn)}
	p.controls = append(p.controls, c)
	if err := conn.SetDeadline(p.deadline); err != nil {
		t.Fatal(err)
	}
	return c
}
func (p *cliProcess) wait(t *testing.T, want int) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(time.Until(p.deadline)):
		t.Fatal("CLI did not exit before fixture deadline")
	}
	code := p.cmd.ProcessState.ExitCode()
	if code != want {
		t.Fatalf("exit=%d want=%d err=%v\nstdout=%s\nstderr=%s", code, want, p.err, &p.stdout, &p.stderr)
	}
	assertCLIPlain(t, p.stdout.String())
	assertCLIPlain(t, p.stderr.String())
}
func cliRunDir(hello protocol.Control) string {
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(hello.History))))
}

// Only the durable state predicate advances the test. The ticker bounds disk
// sampling; elapsed time never substitutes for the RPC or output barriers.
func waitCLIState(t *testing.T, p *cliProcess, dir string, ready func(engine.Snapshot) bool) engine.Snapshot {
	t.Helper()
	timer := time.NewTimer(time.Until(p.deadline))
	defer timer.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		var snapshot engine.Snapshot
		if err := protocol.ReadJSON(filepath.Join(dir, "run.json"), &snapshot); err != nil {
			t.Fatal(err)
		}
		if ready(snapshot) {
			return snapshot
		}
		select {
		case <-tick.C:
		case <-p.done:
			t.Fatalf("CLI exited before expected state: %+v; stdout=%s stderr=%s", snapshot, &p.stdout, &p.stderr)
		case <-timer.C:
			t.Fatalf("durable state deadline: %+v", snapshot)
		}
	}
}

func assertCLIUnsettled(t *testing.T, p *cliProcess, dir string, prompt protocol.Control) contract.Request {
	t.Helper()
	var request contract.Request
	cliReadJSON(t, prompt.RequestPath, &request)
	raw, err := os.ReadFile(prompt.CandidatePath)
	if err != nil || !json.Valid(raw) {
		t.Fatalf("candidate not present at barrier: %s, %v", raw, err)
	}
	s := waitCLIState(t, p, dir, func(s engine.Snapshot) bool {
		return s.Attempts[request.Identity.AttemptID].DispatchAccepted == engine.AcceptedYes
	})
	a := s.Attempts[request.Identity.AttemptID]
	if s.State != engine.Running || a.State != engine.Running || a.Output != nil || a.Execution != nil || !a.FinishedAt.IsZero() {
		t.Fatalf("unsettled candidate advanced engine: %+v", s)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(prompt.CandidatePath), "published")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsettled candidate published: %v", err)
	}
	select {
	case <-p.done:
		t.Fatal("CLI exited with unsettled candidate")
	default:
	}
	return request
}

func assertCLICleanup(t *testing.T, p *cliProcess, dir string, hello protocol.Control, persisted bool, discoveryFault bool) {
	t.Helper()
	if persisted {
		var cleanup struct {
			Reports []runtime.CleanupReport `json:"reports"`
			Errors  []*engine.FailureInfo   `json:"errors"`
		}
		cliReadJSON(t, filepath.Join(dir, "cleanup.json"), &cleanup)
		if len(cleanup.Reports) != 1 {
			t.Fatalf("cleanup reports: %+v", cleanup)
		}
		c := cleanup.Reports[0]
		if c.Identity.PID != hello.PID || c.Identity.SessionID != hello.SessionID || c.Identity.SessionFile != hello.History || !c.ProcessExited || !c.WaitCompleted || !c.AbortAcknowledged || !c.AbortBashAcknowledged || c.WaitError != "" || c.KillError != "" || len(c.Unconfirmed) != 0 {
			t.Fatalf("incomplete owned cleanup: %+v", c)
		}
		if discoveryFault {
			if c.DiscoveryError == "" || len(cleanup.Errors) != 1 {
				t.Fatalf("missing discovery cleanup warning: %+v", cleanup)
			}
		} else if c.DiscoveryError != "" || len(cleanup.Errors) != 0 || !reflect.DeepEqual(c.DiscoveryRemoved, []string{filepath.Join(p.bridge, hello.SessionID+".json")}) {
			t.Fatalf("unexpected cleanup warning: %+v", cleanup)
		}
	}
	if !discoveryFault {
		if _, err := os.Stat(filepath.Join(p.bridge, hello.SessionID+".json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owned discovery retained: %v", err)
		}
	}
	history, err := os.ReadFile(hello.History)
	if err != nil || len(history) == 0 {
		t.Fatalf("history removed: %q, %v", history, err)
	}
	var s engine.Snapshot
	cliReadJSON(t, filepath.Join(dir, "run.json"), &s)
	wantMessages := 0
	for _, attempt := range s.Attempts {
		wantMessages++
		if attempt.State == engine.Succeeded {
			wantMessages++
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(history))
	var parent any
	for i := range wantMessages {
		var entry struct {
			ID       string `json:"id"`
			ParentID any    `json:"parentId"`
			Message  struct {
				Role string `json:"role"`
			} `json:"message"`
		}
		if err := decoder.Decode(&entry); err != nil {
			t.Fatalf("history entry %d: %v", i, err)
		}
		role := "user"
		if i%2 != 0 {
			role = "assistant"
		}
		if entry.ID != fmt.Sprintf("e%d", i+1) || entry.ParentID != parent || entry.Message.Role != role {
			t.Fatalf("history lineage/role mismatch: %+v", entry)
		}
		parent = entry.ID
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected extra history entry: %+v, %v", extra, err)
	}
	if len(s.Sessions) != 1 {
		t.Fatalf("session count: %+v", s.Sessions)
	}
	for _, session := range s.Sessions {
		if session.State != "Closed" || session.Identity.PID != hello.PID {
			t.Fatalf("session not closed: %+v", session)
		}
	}
}

func TestCLIProtocolSuccess(t *testing.T) {
	for _, mode := range []string{"json-only", "artifact", "cwd-default", "cwd-relative"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("PWC_PI_CWD", "")
			wantCWD := ""
			switch mode {
			case "json-only":
				if err := os.Unsetenv("PWC_PI_CWD"); err != nil {
					t.Fatal(err)
				}
			case "cwd-default":
				wantCWD = protocol.SourceWorkspace(t)
				t.Setenv("PWC_PI_CWD", wantCWD)
			case "cwd-relative":
				t.Setenv("PWC_PI_CWD", ".")
			}
			prompt := " 中文 'quoted' $(touch PWNED) ; ../task \x1b[31mred\x1b[0m\x1b]52;c;SECRET\a "
			p := startCLIProcess(t, prompt, mode, "", false)
			control := p.accept(t, p.listener)
			hello := control.next(t, "hello")
			dir := cliRunDir(hello)
			var requests []contract.Request
			for i := range 2 {
				message := control.next(t, "prompt")
				requests = append(requests, assertCLIUnsettled(t, p, dir, message))
				if mode == "artifact" && i == 1 {
					// Modify only Pi's external candidate before releasing its RPC barrier.
					var envelope map[string]json.RawMessage
					cliReadJSON(t, message.CandidatePath, &envelope)
					envelope["files"] = json.RawMessage(`[{"id":"readable","kind":"artifact","path":"artifacts/different-name.md"}]`)
					raw, err := json.Marshal(envelope)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(filepath.Dir(message.CandidatePath), "artifacts/different-name.md"), []byte("Final readable CLI artifact\n"), 0600); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(message.CandidatePath, raw, 0600); err != nil {
						t.Fatal(err)
					}
				}
				control.send(t, "settle")
			}
			p.wait(t, 0)
			if p.stderr.Len() != 0 {
				t.Fatalf("stderr=%s", &p.stderr)
			}
			var s engine.Snapshot
			cliReadJSON(t, filepath.Join(dir, "run.json"), &s)
			if s.State != engine.Succeeded || s.WorkflowOutcome != engine.Succeeded || !s.StatePersisted || len(s.Attempts) != 2 || s.ControllerVersion != "M6" || s.PiVersion != "0.84.3" || s.WorkflowVersion != "cli-test-v1" {
				t.Fatalf("incorrect terminal snapshot: %+v", s)
			}
			canonicalHome, err := filepath.EvalSymlinks(p.home)
			if err != nil {
				t.Fatal(err)
			}
			if !regexp.MustCompile(`^pw-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}$`).MatchString(s.TaskID) || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(s.RunID) || dir != filepath.Join(canonicalHome, "WIP", s.TaskID, "runs", s.RunID) {
				t.Fatalf("not an automatically named default run path: %s, %+v", dir, s)
			}
			tasks, err := os.ReadDir(filepath.Join(p.home, "WIP"))
			if err != nil || len(tasks) != 1 {
				t.Fatalf("task dirs=%v err=%v", tasks, err)
			}
			var input struct {
				Prompt    string `json:"prompt"`
				LaunchCWD string `json:"launch_cwd"`
			}
			cliReadJSON(t, filepath.Join(dir, "input.json"), &input)
			cwd, err := filepath.EvalSymlinks(p.home)
			if err != nil {
				t.Fatal(err)
			}
			if wantCWD == "" {
				wantCWD = cwd
			}
			if hello.CWD != wantCWD {
				t.Fatalf("CLI child cwd=%q, want %q", hello.CWD, wantCWD)
			}
			for _, session := range s.Sessions {
				if session.Role.CWD != wantCWD {
					t.Fatal("CLI persisted Role.CWD differs from child")
				}
			}
			if input.Prompt != prompt || input.LaunchCWD != cwd || s.Input.Prompt != prompt || s.Input.LaunchCWD != cwd || requests[0].Prompt != prompt || requests[1].Feedback.Message != prompt {
				t.Fatalf("raw prompt/cwd altered: input=%+v snapshot=%+v requests=%+v", input, s.Input, requests)
			}
			if _, err := os.Stat(filepath.Join(p.home, "PWNED")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("prompt shell expansion: %v", err)
			}
			var result struct {
				RunID   string                  `json:"run_id"`
				Outputs map[string]contract.Ref `json:"outputs"`
				Final   *engine.FinalDelivery   `json:"final"`
			}
			cliReadJSON(t, filepath.Join(dir, "result.json"), &result)
			first, second := s.Attempts[requests[0].Identity.AttemptID], s.Attempts[requests[1].Identity.AttemptID]
			if first.Output == nil || second.Output == nil || first.State != engine.Succeeded || second.State != engine.Succeeded || first.Execution == nil || second.Execution == nil {
				t.Fatalf("missing committed attempts: %+v", s.Attempts)
			}
			if !reflect.DeepEqual(requests[1].Inputs, []contract.Ref{*first.Output}) || first.HandleID != second.HandleID || first.Identity == second.Identity || first.Identity.DispatchToken == second.Identity.DispatchToken || first.Execution.SettledSeq >= second.Execution.StartSeq || len(result.Outputs) != 1 || result.RunID != s.RunID || result.Outputs[cliFinalKey] != *second.Output {
				t.Fatalf("Ref/session/receipt handoff mismatch: first=%+v second=%+v result=%+v", first, second, result)
			}
			wantFinal := &engine.FinalDelivery{Output: cliFinalKey, Ref: *second.Output, HandleID: second.HandleID, Scope: second.Scope, Step: "second"}
			if mode == "artifact" {
				wantFinal.ArtifactPath = filepath.Join(filepath.Dir(second.Output.Path), "artifacts/different-name.md")
				raw, err := os.ReadFile(wantFinal.ArtifactPath)
				if err != nil || string(raw) != "Final readable CLI artifact\n" {
					t.Fatalf("artifact not committed: %q %v", raw, err)
				}
			}
			if !reflect.DeepEqual(result.Final, wantFinal) {
				t.Fatalf("result.json final=%+v, want %+v", result.Final, wantFinal)
			}
			for i, attempt := range []engine.AttemptState{first, second} {
				raw, err := os.ReadFile(attempt.Output.Path)
				if err != nil {
					t.Fatal(err)
				}
				hash := sha256.Sum256(raw)
				if hex.EncodeToString(hash[:]) != attempt.Output.SHA256 {
					t.Fatal("published Ref digest mismatch")
				}
				var envelope struct {
					Data protocol.Data `json:"data"`
				}
				if err := json.Unmarshal(raw, &envelope); err != nil {
					t.Fatal(err)
				}
				want := protocol.Data{Value: prompt}
				if i == 1 {
					want = protocol.Data{Value: prompt + "/second", Source: first.Identity.AttemptID}
				}
				if envelope.Data != want {
					t.Fatalf("published data=%+v want=%+v", envelope.Data, want)
				}
			}
			if !strings.HasPrefix(p.stdout.String(), "Run path: "+dir+"\n") {
				t.Fatalf("actual run path not printed at startup: %s", &p.stdout)
			}
			for _, text := range []string{"Run path: " + dir + "\n", "Outcome: Succeeded  Exit code: 0", "final: " + second.Output.Path, "Final output (verified before cleanup): final", "Result index path: " + filepath.Join(dir, "result.json"), "Contract: " + second.Output.Path, "Final node: root/second", "Role: worker", "Session ID: " + hello.SessionID, "Session file: " + hello.History, "Session state: Closed (process exit confirmed;", "Wait completed=true", "Process exited=true"} {
				if !strings.Contains(p.stdout.String(), text) {
					t.Errorf("missing %q in stdout=%s", text, &p.stdout)
				}
			}
			if mode == "artifact" && !strings.Contains(p.stdout.String(), "Readable artifact: "+wantFinal.ArtifactPath) {
				t.Fatalf("missing readable artifact: %s", &p.stdout)
			}
			if mode == "json-only" && strings.Contains(p.stdout.String(), "Readable artifact:") {
				t.Fatalf("JSON-only final inferred artifact: %s", &p.stdout)
			}
			assertCLICleanup(t, p, dir, hello, true, false)
			control.exited(t)
		})
	}
}

func TestCLIProtocolSignals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		signal syscall.Signal
		exit   int
		origin engine.Origin
		fault  string
	}{
		{"SIGINT", syscall.SIGINT, 130, engine.OriginSignalINT, ""},
		{"SIGTERM", syscall.SIGTERM, 143, engine.OriginSignalTERM, ""},
		{"SIGINT with finalization errors", syscall.SIGINT, 130, engine.OriginSignalINT, "result cleanup"},
		{"SIGTERM with all cleanup errors", syscall.SIGTERM, 143, engine.OriginSignalTERM, "result cleanup discovery"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := startCLIProcess(t, "cancel candidate", tc.fault, "", false)
			control := p.accept(t, p.listener)
			hello := control.next(t, "hello")
			dir := cliRunDir(hello)
			message := control.next(t, "prompt")
			control.send(t, "hold")
			control.next(t, "held")
			request := assertCLIUnsettled(t, p, dir, message)
			if err := p.cmd.Process.Signal(tc.signal); err != nil {
				t.Fatal(err)
			}
			p.wait(t, tc.exit)
			var s engine.Snapshot
			cliReadJSON(t, filepath.Join(dir, "run.json"), &s)
			a := s.Attempts[request.Identity.AttemptID]
			if s.State != engine.CancelledState || s.WorkflowOutcome != engine.CancelledState || s.Failure == nil || s.Failure.Code != engine.Cancelled || s.Failure.Origin != tc.origin || len(s.Attempts) != 1 || a.State != engine.CancelledState || a.Output != nil || a.Failure == nil || a.Failure.Origin != tc.origin || a.DispatchAccepted != engine.AcceptedYes {
				t.Fatalf("signal cancellation lost: %+v", s)
			}
			if _, err := os.Stat(filepath.Join(filepath.Dir(message.CandidatePath), "published")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("cancelled candidate published: %v", err)
			}
			if !strings.Contains(p.stdout.String(), fmt.Sprintf("Outcome: Cancelled  Exit code: %d", tc.exit)) || !strings.Contains(p.stdout.String(), "origin="+string(tc.origin)) {
				t.Fatalf("missing signal report: %s", &p.stdout)
			}
			if tc.fault != "" {
				if s.StatePersisted || len(s.FinalizationErrors) != 2 {
					t.Fatalf("missing failed result/cleanup finalization errors: %+v", s)
				}
				for _, text := range []string{"FinalizationFailed", "phase=failed_result", "phase=cleanup", "state_persisted=false", "Wait completed=true", "Process exited=true"} {
					if !strings.Contains(p.stdout.String(), text) {
						t.Errorf("missing %q: %s", text, &p.stdout)
					}
				}
			} else {
				var result struct {
					Outputs map[string]contract.Ref `json:"outputs"`
				}
				cliReadJSON(t, filepath.Join(dir, "result.json"), &result)
				if len(result.Outputs) != 0 || !s.StatePersisted {
					t.Fatalf("cancelled run has output or unpersisted state: %+v %+v", result, s)
				}
			}
			if strings.Contains(tc.fault, "discovery") && !strings.Contains(p.stdout.String(), "Cleanup warning: CleanupFailed") {
				t.Fatalf("cleanup warning hidden by signal: %s", &p.stdout)
			}
			assertCLICleanup(t, p, dir, hello, !strings.Contains(tc.fault, "cleanup"), strings.Contains(tc.fault, "discovery"))
			control.exited(t)
		})
	}
}

func TestCLIBlockedOutputDoesNotBlockEngine(t *testing.T) {
	for _, signal := range []syscall.Signal{0, syscall.SIGINT, syscall.SIGTERM} {
		t.Run(fmt.Sprint(signal), func(t *testing.T) {
			p := startCLIProcess(t, "blocked display", "", "", true)
			output := p.accept(t, p.outputListener)
			output.next(t, "output-blocked")
			control := p.accept(t, p.listener)
			hello := control.next(t, "hello")
			dir := cliRunDir(hello)
			message := control.next(t, "prompt")
			assertCLIUnsettled(t, p, dir, message)
			wantState, wantExit := engine.Succeeded, 0
			if signal == 0 {
				control.send(t, "settle")
				message = control.next(t, "prompt")
				assertCLIUnsettled(t, p, dir, message)
				control.send(t, "settle")
			} else {
				control.send(t, "hold")
				control.next(t, "held")
				if err := p.cmd.Process.Signal(signal); err != nil {
					t.Fatal(err)
				}
				wantState, wantExit = engine.CancelledState, 130
				if signal == syscall.SIGTERM {
					wantExit = 143
				}
			}
			s := waitCLIState(t, p, dir, func(s engine.Snapshot) bool { return !s.FinishedAt.IsZero() })
			if s.State != wantState || !s.StatePersisted {
				t.Fatalf("core could not finish with output blocked: %+v", s)
			}
			assertCLICleanup(t, p, dir, hello, true, false)
			control.exited(t)
			select {
			case <-p.done:
				t.Fatal("CLI returned before blocked consumer was released")
			default:
			}
			output.send(t, "release")
			p.wait(t, wantExit)
			if !strings.Contains(p.stdout.String(), fmt.Sprintf("Outcome: %s  Exit code: %d", wantState, wantExit)) || !strings.Contains(p.stdout.String(), "Process exited=true") {
				t.Fatalf("missing Report after release: %s", &p.stdout)
			}
		})
	}
}

func TestCLIProtocolFilesystemWarnings(t *testing.T) {
	for _, tc := range []struct {
		fault   string
		outcome engine.State
		texts   []string
	}{
		{"result", engine.Failed, []string{"StorageFailed", "phase=result", "phase=failed_result", "FinalizationFailed", "state_persisted=false"}},
		{"cleanup", engine.Succeeded, []string{"FinalizationFailed", "phase=cleanup", "state_persisted=false"}},
		{"discovery", engine.Succeeded, []string{"Cleanup warning: CleanupFailed", "Discovery warning:", "discovery is not a regular file"}},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			p := startCLIProcess(t, "filesystem failure", tc.fault, "", false)
			control := p.accept(t, p.listener)
			hello := control.next(t, "hello")
			dir := cliRunDir(hello)
			for range 2 {
				control.next(t, "prompt")
				control.send(t, "settle")
			}
			p.wait(t, 1)
			var s engine.Snapshot
			cliReadJSON(t, filepath.Join(dir, "run.json"), &s)
			if s.State != tc.outcome || s.WorkflowOutcome != tc.outcome || len(s.Attempts) != 2 {
				t.Fatalf("wrong outcome: %+v", s)
			}
			for _, a := range s.Attempts {
				if a.State != engine.Succeeded || a.Output == nil {
					t.Fatalf("finalization changed committed attempt: %+v", a)
				}
			}
			for _, text := range append(tc.texts, fmt.Sprintf("Outcome: %s  Exit code: 1", tc.outcome), "Wait completed=true", "Process exited=true") {
				if !strings.Contains(p.stdout.String(), text) {
					t.Errorf("missing %q: %s", text, &p.stdout)
				}
			}
			assertCLICleanup(t, p, dir, hello, tc.fault != "cleanup", tc.fault == "discovery")
			control.exited(t)
		})
	}
}

func assertCLIReportFallback(t *testing.T, p *cliProcess, dir, diagnostics string, outcome engine.State, exit int) {
	t.Helper()
	var snapshot engine.Snapshot
	cliReadJSON(t, filepath.Join(dir, "run.json"), &snapshot)
	var result struct {
		Outputs map[string]contract.Ref `json:"outputs"`
		Final   *engine.FinalDelivery   `json:"final"`
	}
	cliReadJSON(t, filepath.Join(dir, "result.json"), &result)
	var cleanup struct {
		Reports []runtime.CleanupReport `json:"reports"`
	}
	cliReadJSON(t, filepath.Join(dir, "cleanup.json"), &cleanup)
	report := engine.Report{Outcome: outcome, ExitCode: exit, Snapshot: snapshot, Result: engine.Result{Outputs: result.Outputs}, Final: result.Final, Cleanup: cleanup.Reports}
	if outcome == engine.CancelledState {
		report.Failure = &engine.Failure{Code: engine.Cancelled, Phase: "run", Message: "controller cancelled run", Origin: engine.OriginControllerUser, DispatchAccepted: engine.AcceptedNo}
	}
	want := diagnostics + tui.FormatReport(report, dir)
	if p.stderr.String() != want {
		t.Fatalf("incomplete stderr report:\ngot=%q\nwant=%q", p.stderr.String(), want)
	}
}

func TestCLIRealBrokenStdoutPipe(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	// The output barrier controls when the write occurs, not its result: the
	// subprocess still writes fd 1 through os.Stdout and receives real EPIPE.
	p := startCLIProcess(t, "broken stdout pipe", "", "", true, cliProcessOptions{stdout: writer, cleanupBarrier: true})
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output := p.accept(t, p.outputListener)
	output.next(t, "output-blocked")
	control := p.accept(t, p.listener)
	hello := control.next(t, "hello")
	dir := cliRunDir(hello)
	message := control.next(t, "prompt")
	control.send(t, "hold")
	control.next(t, "held")
	request := assertCLIUnsettled(t, p, dir, message)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	output.send(t, "release")
	cleanup := p.accept(t, p.cleanupListener)
	cleanup.next(t, "cleanup-blocked")
	select {
	case <-p.done:
		t.Fatal("broken stdout bypassed cleanup")
	default:
	}
	cleanup.send(t, "release")
	p.wait(t, 130)
	var s engine.Snapshot
	cliReadJSON(t, filepath.Join(dir, "run.json"), &s)
	a := s.Attempts[request.Identity.AttemptID]
	if s.State != engine.CancelledState || s.WorkflowOutcome != engine.CancelledState || s.Failure == nil || s.Failure.Origin != engine.OriginControllerUser || !s.StatePersisted || len(s.Attempts) != 1 || a.State != engine.CancelledState || a.Output != nil || a.DispatchAccepted != engine.AcceptedYes {
		t.Fatalf("broken stdout lost explicit cancellation: %+v", s)
	}
	assertCLIReportFallback(t, p, dir, "Display error: write /dev/stdout: broken pipe\nReport output error: write /dev/stdout: broken pipe\n", engine.CancelledState, 130)
	assertCLICleanup(t, p, dir, hello, true, false)
	control.exited(t)
	cleanup.exited(t)
}

func TestCLIReportOutputFailure(t *testing.T) {
	p := startCLIProcess(t, "report writer error", "", "report", false)
	control := p.accept(t, p.listener)
	hello := control.next(t, "hello")
	for range 2 {
		control.next(t, "prompt")
		control.send(t, "settle")
	}
	p.wait(t, 1)
	assertCLIReportFallback(t, p, cliRunDir(hello), "Report output error: closed output\n", engine.Succeeded, 0)
	assertCLICleanup(t, p, cliRunDir(hello), hello, true, false)
	control.exited(t)
}

func TestCLIDisplayOutputFailureCancelsAndJoins(t *testing.T) {
	p := startCLIProcess(t, "display writer error", "", "display", true)
	output := p.accept(t, p.outputListener)
	output.next(t, "output-blocked")
	control := p.accept(t, p.listener)
	hello := control.next(t, "hello")
	dir := cliRunDir(hello)
	message := control.next(t, "prompt")
	control.send(t, "hold")
	control.next(t, "held")
	request := assertCLIUnsettled(t, p, dir, message)
	output.send(t, "release")
	p.wait(t, 130)
	var s engine.Snapshot
	cliReadJSON(t, filepath.Join(dir, "run.json"), &s)
	if s.State != engine.CancelledState || s.Failure == nil || s.Failure.Origin != engine.OriginControllerUser || len(s.Attempts) != 1 || s.Attempts[request.Identity.AttemptID].Output != nil {
		t.Fatalf("display failure did not cancel active Step: %+v", s)
	}
	if p.stdout.Len() != 0 {
		t.Fatalf("stdout=%q", &p.stdout)
	}
	assertCLIReportFallback(t, p, dir, "Display error: closed output\nReport output error: closed output\n", engine.CancelledState, 130)
	assertCLICleanup(t, p, dir, hello, true, false)
	control.exited(t)
}
