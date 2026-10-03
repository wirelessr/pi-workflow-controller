package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

type protocolControl = protocol.Control
type protocolData = protocol.Data

// This subprocess replaces only the external Pi RPC boundary, not engine or Store.
func TestEngineProtocolSubprocess(t *testing.T) {
	if os.Getenv("PWC_ENGINE_PROTOCOL") != "1" {
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

func readProtocolJSON(path string, value any) error { return protocol.ReadJSON(path, value) }

func TestEngineProtocolPreflightAccounting(t *testing.T) {
	for _, exhaust := range []bool{false, true} {
		t.Run(fmt.Sprint(exhaust), func(t *testing.T) {
			dir := t.TempDir()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			schemas, err := contract.NewRegistry(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			policy := engine.DefaultRunPolicy()
			policy.MaxLiveSessions = 1
			policy.MaxTotalSessions = 2
			policy.Runtime.StartupTimeout = 5 * time.Second
			var rejected []*runtime.Failure
			definition := engine.Definition{Name: "preflight", Version: "v1", Policy: policy, Execute: func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
				role := engine.RoleSpec{Name: "worker", Model: runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}}
				count := 1
				if exhaust {
					count = 2
				}
				for i := 0; i < count; i++ {
					h, err := r.OpenSession(ctx, role)
					var f *runtime.Failure
					if h != nil || !errors.As(err, &f) || f.Code != runtime.BridgeUnavailable || f.Phase != "preflight" || f.Origin != runtime.Protocol || f.HandleID == "" || f.DispatchAccepted != runtime.AcceptedNo || f.Cleanup != nil || !errors.Is(err, os.ErrNotExist) {
						return engine.Result{}, fmt.Errorf("preflight error/identity lost: handle=%v error=%+v", h, err)
					}
					rejected = append(rejected, f)
					snapshot := r.Snapshot()
					if len(snapshot.Sessions) != i+1 || len(snapshot.Attempts) != 0 || snapshot.Sessions[f.HandleID].State != "Closed" {
						return engine.Result{}, fmt.Errorf("preflight did not retain and close the allocated handle: %+v", snapshot.Sessions)
					}
					if _, err := os.Stat(filepath.Join(r.Dir(), "sessions", f.HandleID)); !errors.Is(err, os.ErrNotExist) {
						return engine.Result{}, fmt.Errorf("preflight created persistent resources: %v", err)
					}
				}
				if exhaust {
					h, err := r.OpenSession(ctx, role)
					var f *runtime.Failure
					if h != nil || !errors.As(err, &f) || f.Code != runtime.LimitExceeded || f.LimitScope != "run" {
						return engine.Result{}, fmt.Errorf("preflight refunded the total-session budget: %v", err)
					}
				}
				return engine.Result{}, rejected[0]
			}}
			r, err := engine.New(context.Background(), definition, engine.Input{Prompt: "anonymous startup", LaunchCWD: dir}, engine.Options{BaseDir: dir, Schemas: schemas, RuntimeOptions: runtime.Options{
				Executable: executable, Args: []string{"-test.run=^TestEngineProtocolSubprocess$", "--"}, Env: []string{"PWC_ENGINE_PROTOCOL=1", "GORACE=atexit_sleep_ms=0"}, BridgeDir: filepath.Join(dir, "absent"),
			}})
			if err != nil {
				t.Fatal(err)
			}
			report := r.Execute()
			var failure *runtime.Failure
			code := runtime.BridgeUnavailable
			if exhaust {
				code = runtime.LimitExceeded
			}
			if report.Outcome != engine.Failed || report.ExitCode != 1 || !errors.As(report.Failure, &failure) || failure.Code != code || len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 {
				t.Fatalf("preflight final outcome: %+v", report)
			}
			if len(report.Snapshot.Sessions) != len(rejected) || len(report.Snapshot.Attempts) != 0 {
				t.Fatal("finalization lost startup accounting")
			}
			for _, cleanup := range report.Cleanup {
				if cleanup.WaitCompleted || cleanup.ProcessExited || cleanup.Identity.PID != 0 || len(cleanup.Unconfirmed) != 0 {
					t.Fatalf("preflight invented process cleanup: %+v", cleanup)
				}
			}
			raw, err := os.ReadFile(filepath.Join(r.Dir(), "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(strings.NewReader(string(raw)))
			starts, closes := map[string]int{}, map[string]int{}
			for {
				var event struct {
					Kind    string `json:"kind"`
					Details struct {
						HandleID string `json:"handle_id"`
					} `json:"details"`
				}
				if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if event.Kind == "SessionStarting" {
					starts[event.Details.HandleID]++
				}
				if event.Kind == "SessionClosed" {
					if starts[event.Details.HandleID] != 1 {
						t.Fatal("session closed without its starting event")
					}
					closes[event.Details.HandleID]++
				}
			}
			if len(starts) != len(rejected) || len(closes) != len(rejected) {
				t.Fatal("journal lost preflight session accounting")
			}
			for _, rejected := range rejected {
				if starts[rejected.HandleID] != 1 || closes[rejected.HandleID] != 1 {
					t.Fatal("preflight session journal events missing or duplicated")
				}
			}
		})
	}
}

func TestEngineProtocolLegacyNULCWD(t *testing.T) {
	for _, name := range []string{"explicitabsNUL", "LaunchabsNUL"} {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			bridge := filepath.Join(base, "bridge")
			if err := os.Mkdir(bridge, 0700); err != nil {
				t.Fatal(err)
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			schemas, err := contract.NewRegistry(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			badCWD := base + "\x00"
			input := engine.Input{Prompt: "anonymous legacy cwd", LaunchCWD: base}
			role := engine.RoleSpec{Name: "worker", Model: runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}}
			if name == "explicitabsNUL" {
				role.CWD = badCWD
			} else {
				input.LaunchCWD = badCWD
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			definition := engine.Definition{Name: "legacy-cwd", Version: "v1", Policy: engine.DefaultRunPolicy(), Execute: func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
				h, err := r.OpenSession(ctx, role)
				if h != nil || err == nil {
					return engine.Result{}, fmt.Errorf("invalid cwd accepted: handle=%v error=%v", h, err)
				}
				return engine.Result{}, err
			}}
			r, err := engine.New(ctx, definition, input, engine.Options{BaseDir: base, Schemas: schemas, RuntimeOptions: runtime.Options{
				Executable: executable, Args: []string{"-test.run=^TestEngineProtocolSubprocess$", "--"}, Env: []string{"PWC_ENGINE_PROTOCOL=1", "GORACE=atexit_sleep_ms=0"}, BridgeDir: bridge,
			}})
			if err != nil {
				t.Fatal(err)
			}
			report := r.Execute()
			var failure *runtime.Failure
			if !errors.As(report.Failure, &failure) {
				t.Fatalf("missing typed failure: %+v", report)
			}
			var persisted engine.Snapshot
			if err := protocol.ReadJSON(filepath.Join(r.Dir(), "run.json"), &persisted); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(r.Dir(), "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(strings.NewReader(string(raw)))
			var starts, closes []string
			for {
				var event struct {
					Kind    string `json:"kind"`
					Details struct {
						HandleID string          `json:"handle_id"`
						Role     engine.RoleSpec `json:"role"`
					} `json:"details"`
				}
				if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if event.Kind == "SessionStarting" {
					starts = append(starts, event.Details.HandleID)
					if event.Details.Role.CWD != badCWD {
						t.Fatal("journal changed legacy cwd")
					}
				}
				if event.Kind == "SessionClosed" {
					closes = append(closes, event.Details.HandleID)
				}
			}
			observed, err := json.Marshal(map[string]any{
				"typedCode": failure.Code, "phase": failure.Phase, "origin": failure.Origin, "dispatchAccepted": failure.DispatchAccepted,
				"sessionstarting": len(starts), "closed": len(closes), "sessions": len(persisted.Sessions), "attempts": len(persisted.Attempts), "cleanup": report.Cleanup,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("legacy cwd observation: %s", observed)
			if report.Outcome != engine.Failed || report.ExitCode != 1 || len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 {
				t.Fatalf("legacy cwd outcome: %+v", report)
			}
			if failure.Code != runtime.StartFailed || failure.Phase != "spawn" || failure.Origin != runtime.Protocol || failure.DispatchAccepted != runtime.AcceptedNo || failure.HandleID == "" || failure.Cause == nil {
				t.Fatalf("legacy cwd failure moved before runtime spawn: %+v", failure)
			}
			if !reflect.DeepEqual(persisted.Sessions, report.Snapshot.Sessions) || persisted.Input != input || len(persisted.Attempts) != 0 || len(persisted.Sessions) != 1 {
				t.Fatal("legacy cwd lost persisted input/session/attempt accounting")
			}
			if len(starts) != 1 || len(closes) != 1 || starts[0] != failure.HandleID || closes[0] != failure.HandleID || persisted.Sessions[failure.HandleID].State != "Closed" || persisted.Sessions[failure.HandleID].Role.CWD != badCWD {
				t.Fatal("legacy cwd lost allocated and closed handle")
			}
			if info, err := os.Stat(filepath.Join(r.Dir(), "sessions", failure.HandleID, "pi")); err != nil || !info.IsDir() {
				t.Fatalf("legacy cwd did not reach persistent spawn: %v", err)
			}
			if len(report.Cleanup) != 1 {
				t.Fatalf("legacy cwd cleanup count: %d", len(report.Cleanup))
			}
			for _, cleanup := range report.Cleanup {
				if cleanup.WaitCompleted || cleanup.ProcessExited || cleanup.Identity.PID != 0 || len(cleanup.Unconfirmed) != 0 || cleanup.WaitError != "" || cleanup.KillError != "" || cleanup.DiscoveryError != "" {
					t.Fatalf("legacy cwd invented process cleanup/Wait: %+v", cleanup)
				}
			}
		})
	}
}

// physical is the path a child reports from getcwd: symlinks resolved (the
// macOS temp dir is under the /var link), while roles keep the given path.
// The CWD test resolves inline because it runs inside the engine goroutine.
func physical(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func TestEngineProtocolCWD(t *testing.T) {
	for _, name := range []string{"launch", "default", "explicit", "relative", "space", "tilde", "null", "symlink", "missing", "file", "nul", "nul-clean", "unused-missing", "unused-file", "unused-nul"} {
		t.Run(name, func(t *testing.T) {
			service := protocol.SourceWorkspace(t)
			launch := filepath.Dir(service)
			base := t.TempDir()
			bridge := filepath.Join(base, "bridge")
			if err := os.Mkdir(bridge, 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			host, err := protocol.NewHost(ctx, 8)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := host.Close(); err != nil {
					t.Error(err)
				}
			}()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			schemas, err := contract.NewRegistry(nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defaultCWD, explicit, want := service, "", service
			bad := ""
			switch name {
			case "launch":
				defaultCWD, want = "", launch
			case "explicit":
				explicit, want = filepath.Join(launch, "sibling"), filepath.Join(launch, "sibling")
			case "relative":
				defaultCWD = "service"
			case "space", "tilde", "null":
				defaultCWD = map[string]string{"space": " service ", "tilde": "~", "null": "null"}[name]
				// These names are literal, not configuration sentinels.
				launch = t.TempDir()
				want = filepath.Join(launch, defaultCWD)
				if err := os.Mkdir(want, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				// The role keeps the configured link; only the child's getcwd resolves it.
				defaultCWD = filepath.Join(base, "link")
				if err := os.Symlink(service, defaultCWD); err != nil {
					t.Fatal(err)
				}
				want = defaultCWD
			case "missing", "unused-missing":
				defaultCWD, bad = filepath.Join(base, "absent"), "spawn"
			case "file", "unused-file":
				defaultCWD, bad = filepath.Join(service, "source.txt"), "spawn"
			case "nul", "unused-nul":
				defaultCWD, bad = service+"\x00", "definition"
			case "nul-clean":
				defaultCWD, bad = "\x00/../service", "definition"
			}
			if strings.HasPrefix(name, "unused-") {
				explicit, want, bad = service, service, ""
			}
			role := engine.RoleSpec{Name: "worker", CWD: explicit, Model: runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}}
			originalRole := role
			input := engine.Input{Prompt: "anonymous cwd", LaunchCWD: launch}
			originalInput := input
			policy := engine.DefaultRunPolicy()
			var hellos []protocol.Control
			definition := engine.Definition{Name: "cwd", Version: "v1", Policy: policy, Execute: func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
				for range 2 {
					h, err := r.OpenSession(ctx, role)
					if bad != "" {
						var f *runtime.Failure
						if h != nil || !errors.As(err, &f) {
							return engine.Result{}, fmt.Errorf("bad cwd accepted: %v", err)
						}
						if bad == "spawn" && (f.Code != runtime.StartFailed || f.Phase != "spawn" || f.DispatchAccepted != runtime.AcceptedNo || f.HandleID == "" || f.Cause == nil) {
							return engine.Result{}, fmt.Errorf("lost spawn failure: %+v", f)
						}
						if bad == "definition" && f.Code != runtime.InvalidDefinition {
							return engine.Result{}, fmt.Errorf("invalid NUL: %+v", f)
						}
						return engine.Result{}, err
					}
					if err != nil {
						return engine.Result{}, err
					}
					select {
					case event := <-host.Events():
						physical, _ := filepath.EvalSymlinks(want)
						if event.Err != nil || event.Message.Type != "hello" || event.Message.CWD != physical {
							return engine.Result{}, fmt.Errorf("actual child cwd: %+v %v, want %q", event.Message, event.Err, physical)
						}
						hellos = append(hellos, event.Message)
					case <-ctx.Done():
						return engine.Result{}, context.Cause(ctx)
					}
					closed, err := r.CloseSessionReport(ctx, h)
					if err != nil || !closed.ConfirmsLocalClose(hellos[len(hellos)-1].SessionID) {
						return engine.Result{}, fmt.Errorf("close/Wait not confirmed: %+v %v", closed, err)
					}
				}
				return engine.Result{}, nil
			}}
			opts := engine.Options{BaseDir: base, Schemas: schemas, PiDefaultCWD: defaultCWD, RuntimeOptions: runtime.Options{Executable: executable, Args: []string{"-test.run=^TestEngineProtocolSubprocess$", "--"}, Env: []string{"PWC_ENGINE_PROTOCOL=1", "PWC_ENGINE_CONTROL=" + host.Addr().String(), "GORACE=atexit_sleep_ms=0"}, BridgeDir: bridge}}
			t.Chdir(t.TempDir())
			r, err := engine.New(ctx, definition, input, opts)
			if err != nil {
				t.Fatal(err)
			}
			opts.PiDefaultCWD, input.LaunchCWD = "changed-after-New", base
			t.Setenv("PWC_PI_CWD", "engine-must-not-read-env")
			t.Chdir(t.TempDir())
			report := r.Execute()
			if (report.ExitCode != 0) != (bad != "") || len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 {
				t.Fatalf("cwd outcome: %+v", report)
			}
			if bad != "" {
				var failure *runtime.Failure
				wantCode := runtime.StartFailed
				if bad == "definition" {
					wantCode = runtime.InvalidDefinition
				}
				if !errors.As(report.Failure, &failure) || failure.Code != wantCode {
					t.Fatalf("incorrect cwd rejection: %v", report.Failure)
				}
			}
			if role != originalRole || report.Snapshot.Input != originalInput {
				t.Fatal("caller role or immutable input changed")
			}
			var persisted engine.Snapshot
			if err := protocol.ReadJSON(filepath.Join(r.Dir(), "run.json"), &persisted); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(persisted.Sessions, report.Snapshot.Sessions) || persisted.Input != originalInput || len(persisted.Attempts) != 0 {
				t.Fatal("persisted effective role/input/accounting mismatch")
			}
			count := 2
			switch bad {
			case "spawn":
				count, want = 1, defaultCWD
			case "definition":
				count = 0
			}
			if len(persisted.Sessions) != count {
				t.Fatalf("session count=%d, want %d", len(persisted.Sessions), count)
			}
			for _, session := range persisted.Sessions {
				if session.Role.CWD != want || session.State != "Closed" {
					t.Fatalf("persisted session cwd/close: %+v", session)
				}
			}
			if bad == "" && (len(hellos) != 2 || hellos[0].SessionID == hellos[1].SessionID) {
				t.Fatal("fresh session not exercised")
			}
			if bad != "" {
				for _, cleanup := range report.Cleanup {
					if cleanup.WaitCompleted || cleanup.ProcessExited || cleanup.Identity.PID != 0 {
						t.Fatal("spawn rejection invented process Wait")
					}
				}
			}
		})
	}
}

func TestEngineProtocolHandoff(t *testing.T) {
	service := protocol.SourceWorkspace(t)
	for _, mode := range []string{"usage", "attempt-timeout", "cleanup-error"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			bridge := filepath.Join(dir, "bridge")
			if err := os.Mkdir(bridge, 0700); err != nil {
				t.Fatal(err)
			}
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			deadline := time.Now().Add(20 * time.Second)
			if err := listener.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			policy := engine.DefaultRunPolicy()
			policy.DisableRunTimeout = true
			policy.RunTimeout = time.Nanosecond
			policy.AttemptTimeout = 5 * time.Second
			policy.MaxTotalSessions = 2
			policy.MaxTotalAttempts = 3
			policy.Runtime.StartupTimeout = 5 * time.Second
			policy.Runtime.RPCTimeout = time.Second
			policy.Runtime.AbortGrace = time.Second
			policy.Runtime.CleanupTimeout = 3 * time.Second
			policy.Runtime.HealthInterval = time.Hour
			var run *engine.Run
			pi, err := runtime.New(runtime.Options{Executable: executable, Args: []string{"-test.run=^TestEngineProtocolSubprocess$", "--"}, Env: []string{"PWC_ENGINE_PROTOCOL=1", "PWC_ENGINE_CONTROL=" + listener.Addr().String(), "GORACE=atexit_sleep_ms=0"}, BridgeDir: bridge, Policy: policy.Runtime, Observe: func(ctx context.Context, o runtime.Observation) error { return run.Observe(ctx, o) }})
			if err != nil {
				t.Fatal(err)
			}
			const uri = "https://fixture.local/handoff.json"
			schemas, err := contract.NewRegistry([]contract.Resource{{URI: uri, JSON: json.RawMessage(`{"type":"object","required":["value","source"],"additionalProperties":false,"properties":{"value":{"type":"string"},"source":{"type":"string"}}}`)}}, []contract.SchemaDefinition{{ID: "data.v1", URI: uri}})
			if err != nil {
				t.Fatal(err)
			}
			var first, expired, resumed engine.StepResult
			var cleaned runtime.CleanupReport
			var timeoutErr error
			candidate := make(chan contract.Ref, 1)
			cleanupVerified := make(chan struct{})
			replaced := false
			definition := engine.Definition{Name: "handoff", Version: "v1", Policy: policy, Execute: func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
				role := engine.RoleSpec{Name: "worker", Model: runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}}
				h, err := r.OpenSession(ctx, role)
				if err != nil {
					return engine.Result{}, err
				}
				first, err = r.Root().Step(ctx, engine.StepSpec{Key: "committed-state", Session: h, Prompt: "saved state", Output: contract.Spec{SchemaID: "data.v1"}})
				if err != nil {
					return engine.Result{}, err
				}
				state, err := engine.Decode[protocolData](ctx, r, first.Output)
				if err != nil || state.Value != "saved state" {
					return engine.Result{}, fmt.Errorf("state commit: %+v %v", state, err)
				}
				foreign := first.Output
				foreign.RunID = "foreign-run"
				if _, err := engine.Decode[protocolData](ctx, r, foreign); err == nil {
					return engine.Result{}, errors.New("foreign checkpoint accepted")
				}
				if mode == "attempt-timeout" {
					expired, timeoutErr = r.Root().Step(ctx, engine.StepSpec{Key: "interrupted", Session: h, Prompt: "unfinished state", Inputs: []contract.Ref{first.Output}, Timeout: 500 * time.Millisecond, Output: contract.Spec{SchemaID: "data.v1"}})
					var failure *engine.Failure
					if !errors.As(timeoutErr, &failure) || failure.Code != engine.TimedOut || failure.Origin != engine.OriginAttemptDeadline || expired.Output != (contract.Ref{}) {
						return engine.Result{}, fmt.Errorf("timeout classification: %+v %v", expired, timeoutErr)
					}
					var ref contract.Ref
					select {
					case ref = <-candidate:
					case <-ctx.Done():
						return engine.Result{}, context.Cause(ctx)
					}
					raw, err := os.ReadFile(ref.Path)
					if err != nil {
						return engine.Result{}, err
					}
					ref.SHA256 = fmt.Sprintf("%x", sha256.Sum256(raw))
					ref.ManifestSHA256 = first.Output.ManifestSHA256
					if _, err := engine.Decode[protocolData](ctx, r, ref); err == nil {
						return engine.Result{}, errors.New("uncommitted candidate accepted")
					}
				} else {
					usage, err := r.SessionContextUsage(ctx, h)
					if err != nil || usage.Identity.SessionID != first.Execution.SessionID || usage.Percent == nil || *usage.Percent != 95 || usage.Tokens == nil || *usage.Tokens != 95000 {
						return engine.Result{}, fmt.Errorf("usage: %+v %v", usage, err)
					}
				}
				cleaned, err = r.CloseSessionReport(ctx, h)
				// The fixture workflow owns the recovery policy, not the engine.
				if err != nil {
					return engine.Result{}, err
				}
				if !cleaned.ConfirmsLocalClose(first.Execution.SessionID) {
					return engine.Result{}, errors.New("cleanup evidence blocks replacement")
				}
				copy, err := r.CloseSessionReport(ctx, h)
				if err != nil || !reflect.DeepEqual(copy, cleaned) {
					return engine.Result{}, errors.New("cleanup outcome not stable")
				}
				copy.DiscoveryRemoved[0] = "caller mutation"
				again, err := r.CloseSessionReport(ctx, h)
				if err != nil || !reflect.DeepEqual(again, cleaned) {
					return engine.Result{}, errors.New("cleanup report alias")
				}
				if _, err := r.SessionContextUsage(ctx, h); err == nil {
					return engine.Result{}, errors.New("closed handle reusable")
				}
				close(cleanupVerified)
				fresh, err := r.OpenSession(ctx, role)
				if err != nil {
					return engine.Result{}, err
				}
				replaced = true
				resumed, err = r.Root().Step(ctx, engine.StepSpec{Key: "resumed", Session: fresh, Prompt: "continue from committed state", Inputs: []contract.Ref{first.Output}, Output: contract.Spec{SchemaID: "data.v1"}})
				if err != nil {
					return engine.Result{}, err
				}
				state, err = engine.Decode[protocolData](ctx, r, resumed.Output)
				if err != nil || state != (protocolData{Value: "saved state/second", Source: first.AttemptID}) {
					return engine.Result{}, fmt.Errorf("recovered state: %+v %v", state, err)
				}
				return engine.Result{Outputs: map[string]contract.Ref{"final": resumed.Output}}, nil
			}}
			run, err = engine.New(ctx, definition, engine.Input{Prompt: "handoff", LaunchCWD: dir}, engine.Options{BaseDir: dir, Schemas: schemas, Runtime: pi, PiDefaultCWD: service})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			var report engine.Report
			go func() { report = run.Execute(); close(done) }()
			var connections []net.Conn
			t.Cleanup(func() {
				run.Cancel(engine.OriginControllerUser)
				for _, conn := range connections {
					_ = conn.Close()
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("handoff fixture did not join")
				}
			})
			var hellos []protocolControl
			var requests []contract.Request
			sessions := 2
			if mode == "cleanup-error" {
				sessions = 1
			}
			for session := 0; session < sessions; session++ {
				conn, err := listener.Accept()
				if err != nil {
					t.Fatal(err)
				}
				connections = append(connections, conn)
				if err := conn.SetDeadline(deadline); err != nil {
					t.Fatal(err)
				}
				decoder, encoder := json.NewDecoder(conn), json.NewEncoder(conn)
				next := func(kind string) protocolControl {
					t.Helper()
					var c protocolControl
					if err := decoder.Decode(&c); err != nil || c.Type != kind {
						t.Fatalf("control want=%s got=%+v err=%v", kind, c, err)
					}
					return c
				}
				hello := next("hello")
				if hello.CWD != physical(t, service) {
					t.Fatalf("handoff child cwd=%q, want %q", hello.CWD, physical(t, service))
				}
				hellos = append(hellos, hello)
				if session == 1 {
					select {
					case <-cleanupVerified:
					case <-ctx.Done():
						t.Fatal("replacement without cleanup gate")
					}
					if !cleaned.WaitCompleted || !cleaned.ProcessExited || len(cleaned.Unconfirmed) != 0 {
						t.Fatal("replacement started before verified Wait")
					}
					if hello.SessionID == hellos[0].SessionID || hello.History == hellos[0].History {
						t.Fatal("replacement reused session identity or history")
					}
				}
				prompts := 1
				if session == 0 && mode == "attempt-timeout" {
					prompts = 2
				}
				for i := 0; i < prompts; i++ {
					message := next("prompt")
					var request contract.Request
					if err := readProtocolJSON(message.RequestPath, &request); err != nil {
						t.Fatal(err)
					}
					requests = append(requests, request)
					ack := "settle"
					if mode == "cleanup-error" {
						path := filepath.Join(bridge, hello.SessionID+".json")
						if err := os.Rename(path, path+".recovering"); err != nil {
							t.Fatal(err)
						}
					}
					if prompts == 2 && i == 1 {
						ack = "hold"
						candidate <- contract.Ref{RunID: request.Identity.RunID, AttemptID: request.Identity.AttemptID, Path: message.CandidatePath, SchemaID: "data.v1"}
					}
					if err := encoder.Encode(protocolControl{Type: ack}); err != nil {
						t.Fatal(err)
					}
					if ack == "hold" {
						next("held")
					}
				}
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("handoff exceeded fixture deadline")
			}
			if mode == "cleanup-error" {
				if replaced || report.ExitCode == 0 || cleaned.DiscoveryError == "" || !cleaned.WaitCompleted || len(report.Snapshot.Sessions) != 1 {
					t.Fatalf("cleanup error allowed replacement: %+v cleaned=%+v", report, cleaned)
				}
				return
			}
			wantAttempts := 2
			if mode == "attempt-timeout" {
				wantAttempts = 3
			}
			if !replaced || report.ExitCode != 0 || report.Outcome != engine.Succeeded || len(report.Snapshot.Sessions) != 2 || len(report.Snapshot.Attempts) != wantAttempts {
				t.Fatalf("handoff outcome/accounting: %+v", report)
			}
			if first.Output.RunID != resumed.Output.RunID || first.AttemptID == resumed.AttemptID || first.Execution.SessionID == resumed.Execution.SessionID || resumed.Execution.PromptEntryID != "e1" || !reflect.DeepEqual(requests[len(requests)-1].Inputs, []contract.Ref{first.Output}) {
				t.Fatalf("handoff provenance first=%+v resumed=%+v requests=%+v", first, resumed, requests)
			}
			if mode == "attempt-timeout" {
				attempt := report.Snapshot.Attempts[expired.AttemptID]
				if attempt.State != engine.TimedOutState || attempt.Output != nil || attempt.Failure == nil || attempt.Failure.Origin != engine.OriginAttemptDeadline {
					t.Fatalf("failed attempt history lost: %+v", attempt)
				}
			}
			for _, cleanup := range report.Cleanup {
				if !cleanup.WaitCompleted || !cleanup.ProcessExited || len(cleanup.Unconfirmed) != 0 {
					t.Fatalf("cleanup incomplete: %+v", cleanup)
				}
			}
		})
	}
}

func TestEngineProtocolWorkflow(t *testing.T) {
	for _, name := range []string{"sequential", "controller-cancel", "run-attempt-limit"} {
		t.Run(name, func(t *testing.T) {
			runLimit := name == "run-attempt-limit"
			cancelAtPrompt := name != "sequential"
			dir := t.TempDir()
			bridge := filepath.Join(dir, "bridge")
			if err := os.Mkdir(bridge, 0700); err != nil {
				t.Fatal(err)
			}
			listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer func(listener *net.TCPListener) { _ = listener.Close() }(listener)
			deadline := time.Now().Add(20 * time.Second)
			if err := listener.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			policy := engine.DefaultRunPolicy()
			if runLimit {
				policy.MaxTotalAttempts = 1
			}
			policy.RunTimeout = 15 * time.Second
			policy.AttemptTimeout = 10 * time.Second
			policy.Runtime.StartupTimeout = 5 * time.Second
			policy.Runtime.RPCTimeout = 2 * time.Second
			policy.Runtime.PromptAckTimeout = 2 * time.Second
			policy.Runtime.PromptObservationTimeout = 5 * time.Second
			policy.Runtime.AbortGrace = time.Second
			policy.Runtime.CleanupTimeout = 4 * time.Second
			policy.Runtime.HealthInterval = time.Hour
			var run *engine.Run
			pi, err := runtime.New(runtime.Options{
				Executable: executable, Args: []string{"-test.run=^TestEngineProtocolSubprocess$", "--"},
				Env:       []string{"PWC_ENGINE_PROTOCOL=1", "PWC_ENGINE_CONTROL=" + listener.Addr().String(), fmt.Sprintf("PWC_ENGINE_HOLD_ACK=%t", cancelAtPrompt), "GORACE=atexit_sleep_ms=0", "PI_CODING_AGENT_DIR=" + filepath.Join(dir, "agent")},
				BridgeDir: bridge, Policy: policy.Runtime,
				Observe: func(ctx context.Context, observation runtime.Observation) error { return run.Observe(ctx, observation) },
			})
			if err != nil {
				t.Fatal(err)
			}
			const uri = "https://fixture.local/data.json"
			schemas, err := contract.NewRegistry([]contract.Resource{{URI: uri, JSON: json.RawMessage(`{"type":"object","required":["value","source"],"additionalProperties":false,"properties":{"value":{"type":"string"},"source":{"type":"string"}}}`)}}, []contract.SchemaDefinition{{ID: "data.v1", URI: uri}})
			if err != nil {
				t.Fatal(err)
			}
			var steps []engine.StepResult
			var activeErr, limitErr error
			limitTrigger := make(chan struct{})
			downstream := false
			definition := engine.Definition{Name: "protocol", Version: "v1", Policy: policy, Execute: func(ctx context.Context, run *engine.Run, input engine.Input) (engine.Result, error) {
				handle, err := run.OpenSession(ctx, engine.RoleSpec{Name: "worker", Model: runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}})
				if err != nil {
					return engine.Result{}, err
				}
				if runLimit {
					limiter, err := run.OpenSession(ctx, engine.RoleSpec{Name: "limiter", Model: runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}})
					if err != nil {
						return engine.Result{}, err
					}
					activeDone := make(chan struct{})
					go func() {
						first, err := run.Root().Step(ctx, engine.StepSpec{Key: "first", Session: handle, Prompt: input.Prompt, Output: contract.Spec{SchemaID: "data.v1"}})
						steps = append(steps, first)
						activeErr = err
						close(activeDone)
					}()
					select {
					case <-limitTrigger:
						_, limitErr = run.Root().Step(ctx, engine.StepSpec{Key: "over-limit", Session: limiter, Prompt: "must not dispatch", Output: contract.Spec{SchemaID: "data.v1"}})
					case <-ctx.Done():
						limitErr = context.Cause(ctx)
					}
					<-activeDone
					// Returning the victim's error must not replace the run's fatal cause.
					return engine.Result{}, activeErr
				}
				first, err := run.Root().Step(ctx, engine.StepSpec{Key: "first", Session: handle, Prompt: input.Prompt, Output: contract.Spec{SchemaID: "data.v1"}})
				steps = append(steps, first)
				if err != nil {
					return engine.Result{}, err
				}
				value, err := engine.Decode[protocolData](ctx, run, first.Output)
				if err != nil {
					return engine.Result{}, err
				}
				if value != (protocolData{Value: input.Prompt}) {
					return engine.Result{}, fmt.Errorf("first committed data: %+v", value)
				}
				downstream = true
				second, err := run.Root().Step(ctx, engine.StepSpec{Key: "second", Session: handle, Prompt: "read the upstream Ref", Inputs: []contract.Ref{first.Output}, Output: contract.Spec{SchemaID: "data.v1"}})
				steps = append(steps, second)
				if err != nil {
					return engine.Result{}, err
				}
				value, err = engine.Decode[protocolData](ctx, run, second.Output)
				if err != nil {
					return engine.Result{}, err
				}
				if value != (protocolData{Value: input.Prompt + "/second", Source: first.AttemptID}) {
					return engine.Result{}, fmt.Errorf("second committed data: %+v", value)
				}
				return engine.Result{Outputs: map[string]contract.Ref{"final": second.Output}}, nil
			}}
			run, err = engine.New(ctx, definition, engine.Input{Prompt: "first payload", LaunchCWD: dir}, engine.Options{BaseDir: dir, Schemas: schemas, Runtime: pi, PiVersion: "0.84.3"})
			if err != nil {
				t.Fatal(err)
			}
			controllerCancel := func() { run.Cancel(engine.OriginControllerUser) }
			done := make(chan struct{})
			var report engine.Report
			go func() { report = run.Execute(); close(done) }()
			var conn, limiterConn net.Conn
			t.Cleanup(func() {
				controllerCancel()
				if conn != nil {
					_ = conn.Close()
				}
				if limiterConn != nil {
					_ = limiterConn.Close()
				}
				select {
				case <-done:
				case <-time.After(6 * time.Second):
					t.Error("engine Execute did not join during fixture cleanup")
				}
			})
			conn, err = listener.Accept()
			if err != nil {
				select {
				case <-done:
					t.Fatalf("control accept: %v; engine report: %+v", err, report)
				default:
					t.Fatal(err)
				}
			}
			if err := conn.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			decoder, encoder := json.NewDecoder(conn), json.NewEncoder(conn)
			next := func(kind string) protocolControl {
				t.Helper()
				var message protocolControl
				if err := decoder.Decode(&message); err != nil {
					t.Fatalf("waiting for %s: %v", kind, err)
				}
				if message.Type != kind {
					t.Fatalf("got control %+v, want %s", message, kind)
				}
				return message
			}
			hello := next("hello")
			hellos := map[int]protocolControl{hello.PID: hello}
			if runLimit {
				limiterConn, err = listener.Accept()
				if err != nil {
					t.Fatal(err)
				}
				if err := limiterConn.SetDeadline(deadline); err != nil {
					t.Fatal(err)
				}
				var limiterHello protocolControl
				if err := json.NewDecoder(limiterConn).Decode(&limiterHello); err != nil || limiterHello.Type != "hello" {
					t.Fatalf("limiter startup: %+v, %v", limiterHello, err)
				}
				hellos[limiterHello.PID] = limiterHello
			}
			waitDispatch := func(request contract.Request, observedKind string, wantState engine.State, wantAccepted engine.DispatchAccepted) engine.AttemptState {
				t.Helper()
				for {
					snapshot := run.Snapshot()
					attempt, ok := snapshot.Attempts[request.Identity.AttemptID]
					if !ok || attempt.Identity != request.Identity || attempt.HandleID == "" || attempt.Number != 1 {
						t.Fatalf("prompt attempt identity mismatch: request=%+v attempt=%+v", request.Identity, attempt)
					}
					journal, err := os.ReadFile(filepath.Join(run.Dir(), "events.jsonl"))
					if err != nil {
						t.Fatal(err)
					}
					journalIn := json.NewDecoder(strings.NewReader(string(journal)))
					observed := false
					var transition runtime.Observation
					// Snapshot.LastSeq bounds the durable prefix, excluding concurrent appends.
					for seq := uint64(1); seq <= snapshot.LastSeq; seq++ {
						var event struct {
							Seq     uint64          `json:"seq"`
							RunID   string          `json:"run_id"`
							Kind    string          `json:"kind"`
							Details json.RawMessage `json:"details"`
						}
						if err := journalIn.Decode(&event); err != nil {
							t.Fatal(err)
						}
						if event.Seq != seq || event.RunID != request.Identity.RunID {
							t.Fatalf("invalid journal identity/sequence: %+v", event)
						}
						if event.Kind == "AttemptSettled" || event.Kind == "AttemptSucceeded" {
							t.Fatalf("held candidate settled or published: %+v", event)
						}
						if event.Kind != "SessionObservation" {
							continue
						}
						var observation runtime.Observation
						if err := json.Unmarshal(event.Details, &observation); err != nil {
							t.Fatal(err)
						}
						if observation.Kind == "agent_settled" {
							t.Fatal("hold fixture unexpectedly emitted agent_settled")
						}
						if event.Seq == attempt.LastSeq {
							transition = observation
						}
						if observation.Kind == observedKind && observation.HandleID == attempt.HandleID && observation.DispatchToken == request.Identity.DispatchToken {
							observed = true
						}
					}
					if observed {
						wantKind := "Dispatching"
						if wantAccepted == engine.AcceptedYes {
							wantKind = "DispatchAccepted"
						}
						if attempt.State != wantState || attempt.DispatchAccepted != wantAccepted || attempt.Execution != nil || attempt.Output != nil || !attempt.FinishedAt.IsZero() || attempt.Failure != nil {
							t.Fatalf("unsettled dispatch state after %s: %+v, want %s/%s", observedKind, attempt, wantState, wantAccepted)
						}
						if transition.Kind != wantKind || transition.HandleID != attempt.HandleID || transition.DispatchToken != request.Identity.DispatchToken {
							t.Fatalf("attempt transition is not backed by token-matched %s journal evidence: attempt=%+v observation=%+v", wantKind, attempt, transition)
						}
						return attempt
					}
					select {
					case <-run.Changes():
					case <-done:
						t.Fatalf("engine ended before %s: %+v", observedKind, report)
					case <-ctx.Done():
						t.Fatalf("waiting for %s exceeded deadline: %+v", observedKind, attempt)
					}
				}
			}
			var requests []contract.Request
			var candidates []string
			count := 2
			if cancelAtPrompt {
				count = 1
			}
			for i := 0; i < count; i++ {
				prompt := next("prompt")
				var request contract.Request
				if err := readProtocolJSON(prompt.RequestPath, &request); err != nil {
					t.Fatal(err)
				}
				requests = append(requests, request)
				candidates = append(candidates, prompt.CandidatePath)
				if raw, err := os.ReadFile(prompt.CandidatePath); err != nil || !json.Valid(raw) {
					t.Fatalf("candidate missing at prompt barrier: %s, %v", raw, err)
				}
				ack := "settle"
				var dispatching engine.AttemptState
				if cancelAtPrompt {
					// A real agent_start and candidate cannot substitute for the prompt ack.
					dispatching = waitDispatch(request, "agent_start", engine.Dispatching, engine.AcceptedUnknown)
					ack = "hold"
					if runLimit {
						ack = "hold-entries"
					}
				}
				if err := encoder.Encode(protocolControl{Type: ack}); err != nil {
					t.Fatal(err)
				}
				if cancelAtPrompt {
					next("held")
					running := waitDispatch(request, "DispatchAccepted", engine.Running, engine.AcceptedYes)
					if running.Identity != dispatching.Identity || running.HandleID != dispatching.HandleID || running.LastSeq <= dispatching.LastSeq {
						t.Fatalf("ack did not advance the current dispatch: before=%+v after=%+v", dispatching, running)
					}
					select {
					case <-done:
						t.Fatalf("engine completed with an unsettled candidate: %+v", report)
					default:
					}
					if _, err := os.Stat(filepath.Join(filepath.Dir(prompt.CandidatePath), "published")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("unsettled candidate was published: %v", err)
					}
					if runLimit {
						// The actual entries RPC is pending before root cancellation starts Close.
						next("entries-held")
						close(limitTrigger)
					} else {
						controllerCancel()
					}
				}
			}
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("engine Execute exceeded test deadline")
			}
			if len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 || len(report.Cleanup) != len(hellos) {
				t.Fatalf("cleanup/finalization failed: %+v", report)
			}
			var cleanup runtime.CleanupReport
			for _, cleaned := range report.Cleanup {
				owned, ok := hellos[cleaned.Identity.PID]
				discoveryPath := filepath.Join(bridge, owned.SessionID+".json")
				if !ok || owned.PID <= 0 || cleaned.Identity.SessionFile != owned.History || !cleaned.WaitCompleted || !cleaned.ProcessExited || !cleaned.AbortAcknowledged || !cleaned.AbortBashAcknowledged || cleaned.WaitError != "" || cleaned.KillError != "" || cleaned.DiscoveryError != "" || len(cleaned.Unconfirmed) != 0 || !reflect.DeepEqual(cleaned.DiscoveryRemoved, []string{discoveryPath}) {
					t.Fatalf("owned process cleanup incomplete: %+v", cleaned)
				}
				if _, err := os.Stat(discoveryPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("discovery retained: %v", err)
				}
				if cleaned.Identity.PID == hello.PID {
					cleanup = cleaned
				}
			}
			history, err := os.ReadFile(hello.History)
			if err != nil {
				t.Fatalf("history removed during cleanup: %v", err)
			}
			historyDecoder := json.NewDecoder(strings.NewReader(string(history)))
			var parent any
			messages := 0
			for {
				var entry struct {
					ID       string `json:"id"`
					ParentID any    `json:"parentId"`
				}
				if err := historyDecoder.Decode(&entry); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				messages++
				if entry.ID != fmt.Sprintf("e%d", messages) || entry.ParentID != parent {
					t.Fatalf("history lineage mismatch: %+v, parent %v", entry, parent)
				}
				parent = entry.ID
			}
			if cancelAtPrompt {
				var failure *engine.Failure
				if runLimit {
					var activeFailure *engine.Failure
					if report.Outcome != engine.Failed || report.ExitCode == 0 || !errors.As(report.Failure, &failure) || failure.Code != engine.LimitExceeded || failure.LimitScope != "run" || failure.Origin != engine.OriginDefinition || failure.DispatchAccepted != engine.AcceptedNo || failure.AttemptID != "" || limitErr == nil || !errors.Is(report.Failure, limitErr) {
						t.Fatalf("root run limit lost: report=%+v limit=%v", report, limitErr)
					}
					if !errors.As(activeErr, &activeFailure) || activeFailure.Code != engine.Cancelled || activeFailure.Origin != failure.Origin || activeFailure.DispatchAccepted != engine.AcceptedYes || activeFailure.AttemptID != requests[0].Identity.AttemptID || !errors.Is(activeErr, limitErr) || activeFailure.Cleanup == nil || !activeFailure.Cleanup.WaitCompleted {
						t.Fatalf("active Step lost accepted dispatch/root cause: %+v; root=%v", activeFailure, limitErr)
					}
					if report.Snapshot.Failure == nil || report.Snapshot.Failure.Code != engine.LimitExceeded || report.Snapshot.Failure.LimitScope != "run" || report.Snapshot.WorkflowOutcome != engine.Failed {
						t.Fatalf("terminal snapshot replaced root failure: %+v", report.Snapshot)
					}
				} else if report.Outcome != engine.CancelledState || report.ExitCode == 0 || !errors.As(report.Failure, &failure) || failure.Code != engine.Cancelled || failure.Origin != engine.OriginControllerUser {
					t.Fatalf("controller cancellation lost: %+v", report)
				}
				if downstream || len(steps) != 1 || steps[0].Output != (contract.Ref{}) || len(report.Result.Outputs) != 0 || len(report.Snapshot.Attempts) != 1 || messages != 1 {
					t.Fatalf("cancelled attempt reached downstream or published: steps=%+v report=%+v messages=%d", steps, report, messages)
				}
				attempt := report.Snapshot.Attempts[steps[0].AttemptID]
				if attempt.State != engine.CancelledState || attempt.Identity != requests[0].Identity || attempt.DispatchAccepted != engine.AcceptedYes || attempt.Output != nil || attempt.Failure == nil || attempt.Failure.Code != engine.Cancelled || attempt.Failure.Origin != failure.Origin {
					t.Fatalf("cancelled attempt state: %+v", attempt)
				}
				if _, err := os.Stat(filepath.Join(filepath.Dir(candidates[0]), "published")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("cancelled candidate was published: %v", err)
				}
			} else {
				if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil || !downstream || len(steps) != 2 || len(report.Snapshot.Attempts) != 2 || messages != 4 {
					t.Fatalf("sequential workflow failed: %+v; steps=%+v messages=%d", report, steps, messages)
				}
				if !reflect.DeepEqual(requests[1].Inputs, []contract.Ref{steps[0].Output}) || requests[0].Identity == requests[1].Identity || candidates[0] == candidates[1] || report.Result.Outputs["final"] != steps[1].Output {
					t.Fatalf("incorrect Ref handoff: requests=%+v steps=%+v", requests, steps)
				}
				for i, step := range steps {
					attempt := report.Snapshot.Attempts[step.AttemptID]
					if attempt.State != engine.Succeeded || attempt.Output == nil || *attempt.Output != step.Output || step.Execution.SessionID != cleanup.Identity.SessionID || step.Execution.PromptEntryID != fmt.Sprintf("e%d", i*2+1) || step.Execution.LastAssistantID != fmt.Sprintf("e%d", i*2+2) || step.Execution.LastEntryID != step.Execution.LastAssistantID || step.Execution.Token != requests[i].Identity.DispatchToken || step.Execution.SettledSeq <= step.Execution.StartSeq || step.Execution.StopReason != "stop" {
						t.Fatalf("incomplete committed execution evidence: %+v / %+v", attempt, step)
					}
				}
				if steps[1].Execution.StartSeq <= steps[0].Execution.SettledSeq {
					t.Fatal("second dispatch did not follow first settlement")
				}
			}
			var persistedResult struct {
				Outputs map[string]contract.Ref `json:"outputs"`
			}
			if err := readProtocolJSON(filepath.Join(run.Dir(), "result.json"), &persistedResult); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(persistedResult.Outputs, report.Result.Outputs) {
				t.Fatalf("persisted result differs from report: %+v / %+v", persistedResult, report.Result)
			}
			if len(report.Snapshot.Sessions) != len(hellos) || report.Snapshot.State != report.Outcome || !report.Snapshot.StatePersisted {
				t.Fatalf("incorrect terminal snapshot: %+v", report.Snapshot)
			}
			for _, session := range report.Snapshot.Sessions {
				if _, owned := hellos[session.Identity.PID]; session.State != "Closed" || !owned {
					t.Fatalf("session was not closed: %+v", session)
				}
			}
		})
	}
}
