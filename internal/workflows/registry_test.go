package workflows_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/bundled"
	bundledoptions "pi-workflow-controller/internal/testutil/bundled/options"
	"pi-workflow-controller/internal/testutil/protocol"
	"pi-workflow-controller/internal/workflows"
)

func TestRegistryContainsSmokeAndCodeReview(t *testing.T) {
	registry, err := engine.NewRegistry(workflows.Definitions())
	if err != nil {
		t.Fatal(err)
	}
	definitions := registry.Definitions()
	if len(definitions) != 3 || definitions[0].Name != "code-review" || definitions[1].Name != "jira-triage" || definitions[2].Name != "smoke-echo" {
		t.Fatalf("unexpected product workflows: %+v", definitions)
	}
	if _, err := registry.Lookup("smoke-echo"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Lookup("jira-triage"); err != nil {
		t.Fatal(err)
	}
	schemas, err := contract.NewRegistry(workflows.Resources(), workflows.Schemas())
	if err != nil || !schemas.Has("smoke.echo.v1") || !schemas.Has("triage.report.v1") || !schemas.Has("triage.planner.v1") {
		t.Fatalf("smoke schemas unavailable: %v", err)
	}
}

// This is not a live-model binding test: only Start's model is translated to
// the localhost catalog. The production Definition, Step, Decode, Decision,
// Store and the bundled Pi Execute/Confirm paths remain unchanged.
type smokeLocalModel struct {
	runtime.Runtime
	requested []runtime.SessionSpec
}

func (r *smokeLocalModel) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	r.requested = append(r.requested, spec)
	spec.Model = runtime.ModelSpec{Provider: bundled.Provider, ID: bundled.Model, Thinking: "off"}
	return r.Runtime.Start(ctx, spec)
}

func TestSmokePreflightSubprocess(t *testing.T) {
	if os.Getenv("PWC_SMOKE_PREFLIGHT") != "1" {
		return
	}
	for _, arg := range os.Args {
		if arg == "--version" {
			_, _ = os.Stdout.WriteString("0.84.3\n")
			os.Exit(0)
		}
	}
	os.Exit(2)
}

func TestSmokeEchoSharedPreflight(t *testing.T) {
	registry, err := engine.NewRegistry(workflows.Definitions())
	if err != nil {
		t.Fatal(err)
	}
	definition, err := registry.Lookup("smoke-echo")
	if err != nil {
		t.Fatal(err)
	}
	schemas, err := contract.NewRegistry(workflows.Resources(), workflows.Schemas())
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir, bridge := t.TempDir(), t.TempDir()
	claim := filepath.Join(bridge, "other.json.recovering")
	if err := os.WriteFile(claim, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := engine.New(context.Background(), definition, engine.Input{Prompt: "anonymous echo", LaunchCWD: dir}, engine.Options{BaseDir: dir, Schemas: schemas, RuntimeOptions: runtime.Options{
		Executable: executable, Args: []string{"-test.run=^TestSmokePreflightSubprocess$", "--"}, Env: []string{"PWC_SMOKE_PREFLIGHT=1", "GORACE=atexit_sleep_ms=0"}, BridgeDir: bridge,
	}})
	if err != nil {
		t.Fatal(err)
	}
	report := r.Execute()
	var failure *runtime.Failure
	if report.Outcome != engine.Failed || report.ExitCode != 1 || !errors.As(report.Failure, &failure) || failure.Code != runtime.BridgeUnavailable || failure.Phase != "preflight" || failure.HandleID == "" || failure.DispatchAccepted != runtime.AcceptedNo {
		t.Fatalf("smoke bypassed common preflight: %+v", report)
	}
	if len(report.Snapshot.Sessions) != 1 || report.Snapshot.Sessions[failure.HandleID].State != "Closed" || len(report.Snapshot.Attempts) != 0 || report.Final != nil || len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 {
		t.Fatalf("smoke preflight lifecycle: %+v", report)
	}
	if _, err := os.Stat(filepath.Join(r.Dir(), "sessions", failure.HandleID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("smoke created persistent resources: %v", err)
	}
	if raw, err := os.ReadFile(claim); err != nil || string(raw) != "keep" {
		t.Fatalf("smoke modified foreign discovery: %v", err)
	}
}

func TestSmokeEchoBundledDefinition(t *testing.T) {
	for _, tc := range []struct {
		name, prompt string
		mismatch     bool
	}{
		{"match-unicode-quotes-shell-data", `  繁體 café é "quoted" 'single' \\ $(touch forbidden) ` + "`touch forbidden`" + ` ; | & < > $HOME * ? [] {}  `, false},
		{"mismatch-valid-contract", "exact echo, not a paraphrase", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type produced struct {
				request  contract.Request
				original []byte
			}
			written := make(chan produced, 1)
			h := bundled.New(t, bundled.Config{Respond: func(_ context.Context, r bundled.Request) bundled.Reply {
				if r.Index == 2 {
					return bundled.Reply{Text: "echo candidate written"}
				}
				if r.Index != 1 {
					t.Errorf("unexpected provider continuation %d", r.Index)
					return bundled.Reply{Status: 400, Text: "unexpected continuation"}
				}
				var body struct {
					Messages []struct {
						Role    string          `json:"role"`
						Content json.RawMessage `json:"content"`
					} `json:"messages"`
				}
				if err := json.Unmarshal(r.Body, &body); err != nil {
					t.Error(err)
					return bundled.Reply{Status: 400, Text: err.Error()}
				}
				var message string
				for _, m := range body.Messages {
					if m.Role == "user" {
						var content []struct {
							Text string `json:"text"`
						}
						if err := json.Unmarshal(m.Content, &content); err != nil || len(content) != 1 {
							t.Errorf("user content: %s: %v", m.Content, err)
							return bundled.Reply{Status: 400, Text: "invalid user content"}
						}
						message = content[0].Text
					}
				}
				_, candidatePath, request, err := protocol.ParseDispatch(message)
				if err != nil {
					t.Error(err)
					return bundled.Reply{Status: 400, Text: err.Error()}
				}
				encoded, _ := json.Marshal(tc.prompt)
				if request.Prompt != "Write data.echo equal to this JSON string, exactly: "+string(encoded) || !strings.HasPrefix(message, "Controller dispatch "+request.Identity.DispatchToken+"\n") || request.Output.SchemaID != "smoke.echo.v1" {
					t.Errorf("prompt/identity not forwarded as exact JSON data: %+v", request)
				}
				value := tc.prompt
				if tc.mismatch {
					value += "/wrong"
				}
				meta := struct {
					contract.Identity
					Version  int    `json:"version"`
					SchemaID string `json:"schema_id"`
				}{request.Identity, 1, request.Output.SchemaID}
				original, err := json.Marshal(map[string]any{"meta": meta, "data": map[string]string{"echo": value}, "files": []any{}})
				if err != nil {
					t.Error(err)
					return bundled.Reply{Status: 400, Text: err.Error()}
				}
				written <- produced{request, original}
				return bundled.Reply{ToolName: "write", ToolArguments: map[string]any{"path": candidatePath, "content": string(original)}}
			}})
			registry, err := engine.NewRegistry(workflows.Definitions())
			if err != nil {
				t.Fatal(err)
			}
			definition, err := registry.Lookup("smoke-echo")
			if err != nil {
				t.Fatal(err)
			}
			schemas, err := contract.NewRegistry(workflows.Resources(), workflows.Schemas())
			if err != nil {
				t.Fatal(err)
			}
			var run *engine.Run
			ro := bundledoptions.New(h)
			ro.Observe = func(ctx context.Context, o runtime.Observation) error { return run.Observe(ctx, o) }
			pi, err := runtime.New(ro)
			if err != nil {
				t.Fatal(err)
			}
			adapter := &smokeLocalModel{Runtime: pi}
			run, err = engine.New(h.Ctx, definition, engine.Input{Prompt: tc.prompt, LaunchCWD: filepath.Join(h.Root, "work")}, engine.Options{BaseDir: h.Root, Schemas: schemas, Runtime: adapter, PiVersion: "0.84.3"})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			var report engine.Report
			go func() { report = run.Execute(); close(done) }()
			t.Cleanup(func() {
				run.Cancel(engine.OriginControllerUser)
				select {
				case <-done:
				case <-time.After(20 * time.Second):
					t.Error("engine did not join during cleanup")
				}
			})
			select {
			case <-done:
			case <-h.Ctx.Done():
				t.Fatal("smoke Definition timed out")
			}
			h.Save("smoke-report.json", report)
			h.Save("model-adapter.json", map[string]any{"requested": adapter.requested, "effective": runtime.ModelSpec{Provider: bundled.Provider, ID: bundled.Model, Thinking: "off"}, "live_model_binding_verified": false})
			if len(adapter.requested) != 1 || adapter.requested[0].Model != (runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/gpt-oss-120b", Thinking: "low"}) {
				t.Fatalf("production role changed or session retried: %+v", adapter.requested)
			}
			if len(report.FinalizationErrors) != 0 || len(report.CleanupErrors) != 0 || !report.Snapshot.StatePersisted {
				t.Fatalf("smoke persistence/cleanup: %+v", report)
			}
			if tc.mismatch {
				if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Failure == nil || !strings.Contains(report.Failure.Error(), "smoke-echo output did not match the input prompt") || len(report.Result.Outputs) != 0 {
					t.Fatalf("workflow accepted a schema-valid mismatch: %+v", report)
				}
			} else if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil || len(report.Result.Outputs) != 1 {
				t.Fatalf("exact echo failed: %+v", report)
			}
			if len(report.Snapshot.Attempts) != 1 || h.RequestCount() != 2 {
				t.Fatalf("unexpected attempts/provider calls: %+v / %d", report.Snapshot.Attempts, h.RequestCount())
			}
			var output produced
			select {
			case output = <-written:
			default:
				t.Fatal("provider never produced write arguments")
			}
			request, original := output.request, output.original
			var attempt engine.AttemptState
			for _, a := range report.Snapshot.Attempts {
				attempt = a
			}
			if attempt.State != engine.Succeeded || attempt.Output == nil || attempt.Identity != request.Identity || attempt.Execution == nil || attempt.Execution.Token != request.Identity.DispatchToken || attempt.Execution.PromptEntryID == "" || attempt.Execution.SettledSeq <= attempt.Execution.StartSeq {
				t.Fatalf("smoke did not consume a committed Step: %+v", attempt)
			}
			raw, err := os.ReadFile(attempt.Output.Path)
			if err != nil || !bytes.Equal(raw, original) {
				t.Fatalf("published echo differs from actual write tool arguments: %s %v", raw, err)
			}
			if !tc.mismatch && report.Result.Outputs["echo"] != *attempt.Output {
				t.Fatal("final Result did not retain committed Ref")
			}
			if tc.mismatch {
				if report.Final != nil {
					t.Fatal("rejected smoke output exposed final delivery")
				}
			} else if report.Final == nil || report.Final.Ref != *attempt.Output || report.Final.HandleID != attempt.HandleID || report.Final.Step != "echo" || report.Final.ArtifactPath != "" {
				t.Fatalf("bundled Pi final delivery did not identify the committed echo producer: %+v", report.Final)
			}
			var input struct {
				Prompt string `json:"prompt"`
			}
			if err := protocol.ReadJSON(filepath.Join(run.Dir(), "input.json"), &input); err != nil || input.Prompt != tc.prompt {
				t.Fatalf("original prompt not preserved: %+v %v", input, err)
			}
			if _, err := os.Stat(filepath.Join(h.Root, "work", "forbidden")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("prompt was executed as shell instructions: %v", err)
			}
			var diskResult struct {
				RunID   string                  `json:"run_id"`
				Outputs map[string]contract.Ref `json:"outputs"`
			}
			if err := protocol.ReadJSON(filepath.Join(run.Dir(), "result.json"), &diskResult); err != nil || diskResult.RunID != run.ID() || !reflect.DeepEqual(diskResult.Outputs, report.Result.Outputs) {
				t.Fatalf("persisted Result: %+v %v", diskResult, err)
			}
			var disk engine.Snapshot
			if err := protocol.ReadJSON(filepath.Join(run.Dir(), "run.json"), &disk); err != nil || !reflect.DeepEqual(disk, report.Snapshot) {
				t.Fatalf("snapshot/report mismatch: %v", err)
			}
			journal, err := os.ReadFile(filepath.Join(run.Dir(), "events.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			var settled, succeeded, decision, finalizing, finished uint64
			for i, line := range bytes.Split(bytes.TrimSpace(journal), []byte("\n")) {
				var event struct {
					Seq     uint64          `json:"seq"`
					RunID   string          `json:"run_id"`
					Kind    string          `json:"kind"`
					Details json.RawMessage `json:"details"`
				}
				if err := json.Unmarshal(line, &event); err != nil || event.Seq != uint64(i+1) || event.RunID != run.ID() {
					t.Fatalf("journal ordering/identity: %s %v", line, err)
				}
				switch event.Kind {
				case "AttemptSettled":
					var receipt runtime.Execution
					if err := json.Unmarshal(event.Details, &receipt); err != nil || receipt != *attempt.Execution {
						t.Fatalf("journal receipt mismatch: %v", err)
					}
					settled = event.Seq
				case "AttemptSucceeded":
					var d struct {
						Ref contract.Ref `json:"ref"`
					}
					if err := json.Unmarshal(event.Details, &d); err != nil || d.Ref != *attempt.Output {
						t.Fatalf("journal publication mismatch: %v", err)
					}
					succeeded = event.Seq
				case "Decision":
					var d struct {
						Name string         `json:"name"`
						Refs []contract.Ref `json:"refs"`
					}
					if err := json.Unmarshal(event.Details, &d); err != nil || d.Name != "echo-verified" || !reflect.DeepEqual(d.Refs, []contract.Ref{*attempt.Output}) {
						t.Fatalf("acceptance Decision mismatch: %+v %v", d, err)
					}
					decision = event.Seq
				case "RunFinalizing":
					finalizing = event.Seq
				case "RunFinished":
					finished = event.Seq
				}
			}
			if settled <= 0 || settled >= succeeded || succeeded >= finalizing || finalizing >= finished || finished != disk.LastSeq || (tc.mismatch && decision != 0) || (!tc.mismatch && (succeeded >= decision || decision >= finalizing)) {
				t.Fatalf("settled/publication/acceptance/finalization order: %d/%d/%d/%d/%d", settled, succeeded, decision, finalizing, finished)
			}
			writes := 0
			for _, record := range h.Records() {
				if record.Direction != "out" || record.Frame["type"] != "tool_execution_end" {
					continue
				}
				if record.Frame["toolName"] != "write" || record.Frame["isError"] != false {
					t.Fatalf("unexpected agent tool: %+v", record.Frame)
				}
				writes++
			}
			if writes != 1 {
				t.Fatalf("expected one real Pi write, got %d", writes)
			}
			if len(report.Cleanup) != 1 {
				t.Fatalf("cleanup missing: %+v", report.Cleanup)
			}
			cleanup := report.Cleanup[0]
			if !cleanup.ProcessExited || !cleanup.WaitCompleted || len(cleanup.DiscoveryRemoved) != 1 || len(cleanup.Unconfirmed) != 0 {
				t.Fatalf("cleanup incomplete: %+v", cleanup)
			}
			if _, err := os.Stat(cleanup.DiscoveryRemoved[0]); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("discovery retained: %v", err)
			}
			history, err := os.ReadFile(cleanup.Identity.SessionFile)
			if err != nil || !bytes.Contains(history, []byte(request.Identity.DispatchToken)) {
				t.Fatalf("history not retained: %v", err)
			}
			t.Logf("production Definition via localhost model adapter: outcome=%s attempt=%s publication_seq=%d acceptance_seq=%d", report.Outcome, attempt.State, succeeded, decision)
		})
	}
}
