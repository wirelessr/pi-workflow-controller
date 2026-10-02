package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

const engTestSchema = "engine.test.v1"

type engTestData struct {
	Value    string        `json:"value"`
	Status   string        `json:"status,omitempty"`
	Reviewed *contract.Ref `json:"reviewed,omitempty"`
}

type engTestCall struct {
	Spec          runtime.SessionSpec
	Dispatch      runtime.Dispatch
	Request       contract.Request
	RequestPath   string
	CandidatePath string
	Number        int
}

type engTestReply struct {
	Data    engTestData
	Raw     []byte
	Missing bool
	Err     error
	// Entries are delivered to the dispatch's entry sink, if it has one,
	// before the reply, as the runtime delivers session entries.
	Entries []runtime.EntryBatch
}

// Only the process/RPC boundary is replaced. Every dispatch reads the engine's
// actual request and writes an attempt-local candidate for the real Store.
type engTestRuntime struct {
	mu       sync.Mutex
	sessions []*engTestSession
	execute  func(context.Context, engTestCall) engTestReply
	confirm  func(context.Context, engTestCall, runtime.Execution) (runtime.Confirmation, error)
	snapshot func(context.Context, runtime.SessionSpec) (runtime.SessionState, error)
	usage    func(context.Context, runtime.Identity) (runtime.ContextUsage, error)
	startErr error
}

type engTestSession struct {
	owner            *engTestRuntime
	spec             runtime.SessionSpec
	id               runtime.Identity
	mu               sync.Mutex
	calls            []engTestCall
	last             runtime.Execution
	closes, confirms int
}

func (f *engTestRuntime) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(spec.SessionDir, 0700); err != nil {
		return nil, err
	}
	s := &engTestSession{owner: f, spec: spec, id: runtime.Identity{
		HandleID: spec.HandleID, SessionID: "session-" + spec.HandleID,
		SessionFile: filepath.Join(spec.SessionDir, "history.jsonl"), Executable: "engine-test-runtime",
	}}
	f.mu.Lock()
	f.sessions = append(f.sessions, s)
	f.mu.Unlock()
	return s, f.startErr
}

func (s *engTestSession) Identity() runtime.Identity { return s.id }

func (s *engTestSession) ContextUsage(ctx context.Context) (runtime.ContextUsage, error) {
	if s.owner.usage != nil {
		return s.owner.usage(ctx, s.id)
	}
	return runtime.ContextUsage{Identity: s.id, SampledAt: time.Now()}, context.Cause(ctx)
}

func (s *engTestSession) Snapshot(ctx context.Context) (runtime.SessionState, error) {
	if s.owner.snapshot != nil {
		return s.owner.snapshot(ctx, s.spec)
	}
	if err := context.Cause(ctx); err != nil {
		return runtime.SessionState{}, err
	}
	return runtime.SessionState{Identity: s.id, Model: s.spec.Model, Health: "Online", HubVisible: true}, nil
}

func (s *engTestSession) Execute(ctx context.Context, dispatch runtime.Dispatch) (runtime.Execution, error) {
	var requestPath, candidatePath string
	for _, line := range strings.Split(dispatch.Message, "\n") {
		if path, ok := strings.CutPrefix(line, "Read request JSON: "); ok {
			requestPath = path
		}
		if _, path, ok := strings.Cut(line, "to: "); ok {
			candidatePath = path
		}
	}
	if !strings.HasPrefix(dispatch.Message, "Controller dispatch "+dispatch.Token+"\n") ||
		!filepath.IsAbs(requestPath) || !filepath.IsAbs(candidatePath) ||
		filepath.Dir(requestPath) != filepath.Dir(candidatePath) {
		return runtime.Execution{}, fmt.Errorf("invalid dispatch envelope: %q", dispatch.Message)
	}
	var request contract.Request
	if err := engTestReadJSON(requestPath, &request); err != nil {
		return runtime.Execution{}, err
	}
	if request.Identity.DispatchToken != dispatch.Token || request.Output.SchemaID != engTestSchema {
		return runtime.Execution{}, fmt.Errorf("dispatch/request identity mismatch: %+v", request)
	}
	resources := []contract.SchemaResource{request.Output.Schema, request.Output.Envelope}
	for _, resource := range request.Output.Resources {
		resources = append(resources, resource)
	}
	for _, resource := range resources {
		raw, err := os.ReadFile(resource.Path)
		if err != nil {
			return runtime.Execution{}, err
		}
		digest := sha256.Sum256(raw)
		if hex.EncodeToString(digest[:]) != resource.SHA256 {
			return runtime.Execution{}, fmt.Errorf("schema digest mismatch: %s", resource.Path)
		}
	}
	s.mu.Lock()
	call := engTestCall{Spec: s.spec, Dispatch: dispatch, Request: request, RequestPath: requestPath, CandidatePath: candidatePath, Number: len(s.calls) + 1}
	s.calls = append(s.calls, call)
	s.mu.Unlock()
	reply := engTestReply{Data: engTestData{Value: request.Prompt}}
	if s.owner.execute != nil {
		reply = s.owner.execute(ctx, call)
	}
	if dispatch.Entries != nil {
		for _, batch := range reply.Entries {
			dispatch.Entries.Entries(batch)
		}
	}
	if reply.Err != nil {
		return runtime.Execution{}, reply.Err
	}
	if !reply.Missing {
		raw := reply.Raw
		if raw == nil {
			var err error
			raw, err = json.Marshal(struct {
				Meta struct {
					contract.Identity
					Version  int    `json:"version"`
					SchemaID string `json:"schema_id"`
				} `json:"meta"`
				Data  engTestData `json:"data"`
				Files []any       `json:"files"`
			}{Meta: struct {
				contract.Identity
				Version  int    `json:"version"`
				SchemaID string `json:"schema_id"`
			}{request.Identity, 1, request.Output.SchemaID}, Data: reply.Data, Files: []any{}})
			if err != nil {
				return runtime.Execution{}, err
			}
		}
		if err := os.WriteFile(candidatePath, raw, 0600); err != nil {
			return runtime.Execution{}, err
		}
	}
	receipt := runtime.Execution{SessionID: s.id.SessionID, Token: dispatch.Token,
		StartSeq: uint64(call.Number * 10), SettledSeq: uint64(call.Number*10 + 5), ActivityEpoch: uint64(call.Number),
		PromptEntryID: "prompt-" + dispatch.Token, LastEntryID: "assistant-" + dispatch.Token,
		LastAssistantID: "assistant-" + dispatch.Token, StopReason: "stop"}
	s.mu.Lock()
	s.last = receipt
	s.mu.Unlock()
	return receipt, nil
}

func (s *engTestSession) Confirm(ctx context.Context, receipt runtime.Execution) (runtime.Confirmation, error) {
	s.mu.Lock()
	if len(s.calls) == 0 {
		s.mu.Unlock()
		return runtime.Confirmation{}, fmt.Errorf("Confirm called before Execute")
	}
	last := s.last
	call := s.calls[len(s.calls)-1]
	s.confirms++
	s.mu.Unlock()
	if receipt != last {
		return runtime.Confirmation{}, fmt.Errorf("Confirm received stale receipt: %+v", receipt)
	}
	if s.owner.confirm != nil {
		return s.owner.confirm(ctx, call, receipt)
	}
	return runtime.Confirmation{Seq: receipt.SettledSeq + 1, ActivityEpoch: receipt.ActivityEpoch}, context.Cause(ctx)
}

func (s *engTestSession) Close(context.Context) (runtime.CleanupReport, error) {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	return runtime.CleanupReport{Identity: s.id, AbortAcknowledged: true, AbortBashAcknowledged: true, WaitCompleted: true, ProcessExited: true}, nil
}

func (f *engTestRuntime) allSessions() []*engTestSession {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*engTestSession(nil), f.sessions...)
}

func (s *engTestSession) history() ([]engTestCall, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]engTestCall(nil), s.calls...), s.closes, s.confirms
}

func engTestReadJSON(path string, value any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}

func engTestSchemas(t *testing.T) *contract.Registry {
	t.Helper()
	const uri = "https://engine.test/output.json"
	r, err := contract.NewRegistry([]contract.Resource{{URI: uri, JSON: json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"type":"object","required":["value"],"additionalProperties":false,
		"properties":{"value":{"type":"string","minLength":1},"status":{"enum":["pass","reject"]},"reviewed":{"type":"object"}}
	}`)}}, []contract.SchemaDefinition{{ID: engTestSchema, URI: uri}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func engTestNew(t *testing.T, base string, fake *engTestRuntime, workflow Workflow) (*Run, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	if base == "" {
		base = t.TempDir()
	}
	r, err := New(ctx, Definition{Name: "integration", Version: "v1", Policy: DefaultRunPolicy(), Execute: workflow},
		Input{Prompt: `原始 prompt "quotes" $(not-a-command)`, LaunchCWD: base},
		Options{Schemas: engTestSchemas(t), Runtime: fake, BaseDir: base})
	if err != nil {
		t.Fatal(err)
	}
	return r, ctx
}

func TestEngineContextUsageLease(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			fake := &engTestRuntime{usage: func(ctx context.Context, id runtime.Identity) (runtime.ContextUsage, error) {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return runtime.ContextUsage{}, context.Cause(ctx)
				}
				if fail {
					return runtime.ContextUsage{}, &runtime.Failure{Code: runtime.RPCUnresponsive, Origin: runtime.Protocol, Message: "stats unavailable"}
				}
				return runtime.ContextUsage{Identity: id, Seq: 42}, nil
			}}
			var handle *SessionHandle
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				handle = h
				beforeIdentity := run.Snapshot()
				identity, err := run.SessionIdentity(ctx, h)
				if err != nil || identity != h.session.Identity() || identity.SessionID == "" || !reflect.DeepEqual(beforeIdentity, run.Snapshot()) {
					t.Fatalf("owned identity read changed accounting or returned wrong identity: %+v %v", identity, err)
				}
				for _, invalid := range []*SessionHandle{nil, {run: &Run{}}} {
					if _, err := run.SessionIdentity(ctx, invalid); !engTestCode(err, InvalidDefinition) {
						t.Errorf("invalid identity handle: %v", err)
					}
					if _, err := run.SessionContextUsage(ctx, invalid); !engTestCode(err, InvalidDefinition) {
						t.Errorf("invalid usage handle: %v", err)
					}
					if _, err := run.CloseSessionReport(ctx, invalid); !engTestCode(err, InvalidDefinition) {
						t.Errorf("invalid close handle: %v", err)
					}
				}
				if _, err := run.SessionIdentity(ctx, &SessionHandle{run: run, id: "unowned"}); !engTestCode(err, IdentityMismatch) {
					t.Errorf("unowned identity accepted: %v", err)
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := run.SessionIdentity(cancelled, h); !errors.Is(err, context.Canceled) {
					t.Errorf("identity swallowed caller cancellation: %v", err)
				}
				done := make(chan error, 1)
				go func() {
					usage, err := run.SessionContextUsage(ctx, h)
					if !fail && (usage.Identity != h.session.Identity() || usage.Seq != 42) {
						err = fmt.Errorf("usage identity lost: %+v", usage)
					}
					done <- err
				}()
				engDeadlineReceive(t, entered)
				beforeIdentity = run.Snapshot()
				gotIdentity, identityErr := run.SessionIdentity(ctx, h)
				if identityErr != nil || gotIdentity != identity || !reflect.DeepEqual(beforeIdentity, run.Snapshot()) {
					t.Errorf("identity read used busy RPC lease or changed committed state: %+v %v", gotIdentity, identityErr)
				}
				// Snapshot and rejected lease requests must not wait on the RPC.
				if len(run.Snapshot().Attempts) != 0 {
					t.Error("usage consumed an attempt")
				}
				if _, err := run.SessionContextUsage(ctx, h); !engTestCode(err, SessionBusy) {
					t.Errorf("duplicate usage: %v", err)
				}
				if _, err := engTestStep(ctx, run.Root(), h, "busy"); !engTestCode(err, SessionBusy) {
					t.Errorf("Step during usage: %v", err)
				}
				if _, err := run.CloseSessionReport(ctx, h); !engTestCode(err, SessionBusy) {
					t.Errorf("close during usage: %v", err)
				}
				close(release)
				err = engDeadlineReceive(t, done)
				if fail {
					if !engTestCode(err, RPCUnresponsive) {
						t.Errorf("usage failure: %v", err)
					}
					if _, err := run.SessionContextUsage(ctx, h); !engTestCode(err, InvalidDefinition) {
						t.Errorf("failed handle reused: %v", err)
					}
					return Result{}, err
				}
				if err != nil {
					return Result{}, err
				}
				step, err := engTestStep(ctx, run.Root(), h, "state")
				if err != nil {
					return Result{}, err
				}
				if _, err := run.CloseSessionReport(ctx, h); err != nil {
					return Result{}, err
				}
				if _, err := run.SessionContextUsage(ctx, h); !engTestCode(err, InvalidDefinition) {
					t.Errorf("closed handle reused: %v", err)
				}
				beforeIdentity = run.Snapshot()
				gotIdentity, identityErr = run.SessionIdentity(ctx, h)
				if identityErr != nil || gotIdentity != identity || !reflect.DeepEqual(beforeIdentity, run.Snapshot()) {
					t.Errorf("closed identity lost committed owner or changed accounting: %+v %v", gotIdentity, identityErr)
				}
				return engTestResult(step), nil
			})
			report := engDeadlineReceive(t, engTestExecuteAsync(t, r))
			if fail {
				engTestReport(t, report, Failed, 1)
			} else {
				engTestReport(t, report, Succeeded, 0)
			}
			if _, err := r.SessionIdentity(context.Background(), handle); !engTestCode(err, InvalidDefinition) {
				t.Errorf("identity after Execute: %v", err)
			}
			if _, err := r.SessionContextUsage(context.Background(), handle); !engTestCode(err, InvalidDefinition) {
				t.Errorf("usage after Execute: %v", err)
			}
			if _, err := r.CloseSessionReport(context.Background(), handle); !engTestCode(err, InvalidDefinition) {
				t.Errorf("close after Execute: %v", err)
			}
			_, closes, _ := fake.allSessions()[0].history()
			if closes != 1 {
				t.Errorf("close count=%d", closes)
			}
		})
	}
}

func engTestRole(name string) RoleSpec {
	return RoleSpec{Name: name, Model: runtime.ModelSpec{Provider: "fixture", ID: "model-" + name, Thinking: "high"}, AppendPrompt: "fixture role " + name}
}

func engTestStep(ctx context.Context, scope *Scope, h *SessionHandle, key string, inputs ...contract.Ref) (StepResult, error) {
	return scope.Step(ctx, StepSpec{Key: key, Session: h, Prompt: key, Inputs: inputs, Output: contract.Spec{SchemaID: engTestSchema}})
}

func engTestResult(step StepResult) Result {
	return Result{Outputs: map[string]contract.Ref{"output": step.Output}}
}

func engTestCode(err error, code Code) bool {
	var f *Failure
	return errors.As(err, &f) && f.Code == code
}

func engTestExecuteAsync(t *testing.T, run *Run) <-chan Report {
	t.Helper()
	done := make(chan Report, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		done <- run.Execute()
	}()
	// Join before TempDir cleanup even when the test's barrier assertion fails.
	t.Cleanup(func() {
		run.Cancel(OriginControllerUser)
		select {
		case <-joined:
		case <-time.After(20 * time.Second):
			t.Error("engine did not join after test cleanup cancellation")
		}
	})
	return done
}

func engTestReport(t *testing.T, report Report, state State, exit int) {
	t.Helper()
	if state == Succeeded && report.Failure != nil {
		t.Errorf("successful report retained failure: %v", report.Failure)
	}
	if report.Outcome != state || report.ExitCode != exit {
		t.Fatalf("outcome/exit = %s/%d, want %s/%d; failure=%v cleanup=%v finalization=%v", report.Outcome, report.ExitCode, state, exit, report.Failure, report.CleanupErrors, report.FinalizationErrors)
	}
	if len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 || !report.Snapshot.StatePersisted {
		t.Fatalf("unexpected persistence/cleanup failure: %+v", report)
	}
}

func engTestEvents(t *testing.T, r *Run) []Event {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(r.Dir(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var events []Event
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if event.Seq != uint64(len(events)+1) || event.RunID != r.ID() {
			t.Fatalf("invalid journal sequence/identity: %+v", event)
		}
		events = append(events, event)
	}
	return events
}

func engTestPersisted(t *testing.T, r *Run, report Report) {
	t.Helper()
	var disk Snapshot
	if err := engTestReadJSON(filepath.Join(r.Dir(), "run.json"), &disk); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disk, report.Snapshot) {
		t.Fatal("disk snapshot differs from Report snapshot")
	}
	events := engTestEvents(t, r)
	if events[len(events)-1].Kind != "RunFinished" || events[len(events)-1].Seq != disk.LastSeq {
		t.Fatalf("journal and snapshot disagree: %+v", events[len(events)-1])
	}
	terminals := make(map[string]int)
	for _, event := range events {
		switch event.Kind {
		case "AttemptSucceeded", "AttemptFailed", "AttemptCancelled", "AttemptTimedOut":
			raw, err := json.Marshal(event.Details)
			if err != nil {
				t.Fatal(err)
			}
			var details struct {
				Attempt AttemptState `json:"attempt"`
			}
			if err := json.Unmarshal(raw, &details); err != nil {
				t.Fatal(err)
			}
			terminals[details.Attempt.Identity.AttemptID]++
		}
	}
	for id, attempt := range disk.Attempts {
		if terminals[id] != 1 {
			t.Errorf("attempt %s has %d terminal events, want 1", id, terminals[id])
		}
		path := filepath.Join(r.Dir(), "steps", attempt.Identity.InvocationID, "attempts", fmt.Sprintf("%04d-%s", attempt.Number, id), "attempt.json")
		var persisted AttemptState
		if err := engTestReadJSON(path, &persisted); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(persisted, attempt) {
			t.Errorf("attempt snapshot mismatch: %s", path)
		}
	}
}

func TestEngineSequentialPublishedHandoff(t *testing.T) {
	fake := &engTestRuntime{execute: func(ctx context.Context, call engTestCall) engTestReply {
		if call.Spec.Name == "producer" {
			return engTestReply{Data: engTestData{Value: "version-one"}}
		}
		if len(call.Request.Inputs) != 1 {
			return engTestReply{Err: fmt.Errorf("consumer inputs: %+v", call.Request.Inputs)}
		}
		var envelope struct {
			Data engTestData `json:"data"`
		}
		if err := engTestReadJSON(call.Request.Inputs[0].Path, &envelope); err != nil {
			return engTestReply{Err: err}
		}
		return engTestReply{Data: engTestData{Value: envelope.Data.Value + "/consumed"}}
	}}
	var produced, consumed StepResult
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, input Input) (Result, error) {
		a, err := run.OpenSession(ctx, engTestRole("producer"))
		if err != nil {
			return Result{}, err
		}
		b, err := run.OpenSession(ctx, engTestRole("consumer"))
		if err != nil {
			return Result{}, err
		}
		produced, err = engTestStep(ctx, run.Root(), a, "produce")
		if err != nil {
			return Result{}, err
		}
		consumed, err = engTestStep(ctx, run.Root(), b, "consume", produced.Output)
		if err != nil {
			return Result{}, err
		}
		value, err := Decode[engTestData](ctx, run, consumed.Output)
		if err != nil {
			return Result{}, err
		}
		if value.Value != "version-one/consumed" {
			return Result{}, fmt.Errorf("decoded wrong version: %+v", value)
		}
		if err := run.Root().Decision(ctx, "accepted", "exact published version", []contract.Ref{produced.Output, consumed.Output}); err != nil {
			return Result{}, err
		}
		return engTestResult(consumed), nil
	})
	report := r.Execute()
	engTestReport(t, report, Succeeded, 0)
	engTestPersisted(t, r, report)
	if len(report.Snapshot.Attempts) != 2 || produced.Output == consumed.Output {
		t.Fatalf("incorrect handoff: %+v", report.Snapshot.Attempts)
	}
	for _, inv := range report.Snapshot.Invocations {
		raw, err := json.Marshal(inv)
		if err != nil || len(inv.RetryActivationIDs) != 0 || strings.Contains(string(raw), `"retry_activation_ids"`) {
			t.Errorf("non-retry invocation must omit activation IDs: %s, error=%v", raw, err)
		}
	}
	for _, s := range fake.allSessions() {
		calls, closes, confirms := s.history()
		if len(calls) != 1 || closes != 1 || confirms != 1 {
			t.Errorf("session lifecycle: calls=%d closes=%d confirms=%d", len(calls), closes, confirms)
		}
		if s.spec.Model != engTestRole(s.spec.Name).Model || s.spec.CWD != report.Snapshot.Input.LaunchCWD {
			t.Errorf("role binding changed: %+v", s.spec)
		}
	}
	var result struct {
		RunID   string                  `json:"run_id"`
		Outputs map[string]contract.Ref `json:"outputs"`
	}
	if err := engTestReadJSON(filepath.Join(r.Dir(), "result.json"), &result); err != nil {
		t.Fatal(err)
	}
	if result.RunID != r.ID() || !reflect.DeepEqual(result.Outputs, report.Result.Outputs) {
		t.Fatalf("wrong result file: %+v", result)
	}
}

func TestEngineParallelDeclarationOrder(t *testing.T) {
	for _, order := range [][]int{{3, 1, 0, 2}, {0, 2, 1, 3}, {2, 3, 1, 0}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			entered := make(chan string, 4)
			finished := make(chan string, 4)
			releases := make(map[string]chan struct{})
			for i := 0; i < 4; i++ {
				releases[fmt.Sprint(i)] = make(chan struct{})
			}
			fake := &engTestRuntime{execute: func(ctx context.Context, call engTestCall) engTestReply {
				entered <- call.Spec.Name
				select {
				case <-releases[call.Spec.Name]:
					return engTestReply{Data: engTestData{Value: call.Spec.Name}}
				case <-ctx.Done():
					return engTestReply{Err: context.Cause(ctx)}
				}
			}}
			var joined []BranchResult
			r, ctx := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				var branches []Branch
				for i := 0; i < 4; i++ {
					name := fmt.Sprint(i)
					h, err := run.OpenSession(ctx, engTestRole(name))
					if err != nil {
						return Result{}, err
					}
					branches = append(branches, Branch{Name: name, Do: func(ctx context.Context, scope *Scope) (Result, error) {
						step, err := engTestStep(ctx, scope, h, "work")
						finished <- name
						if err != nil {
							return Result{}, err
						}
						return engTestResult(step), nil
					}})
				}
				var err error
				joined, err = run.Root().Parallel(ctx, "four", FailFast, branches)
				if err != nil {
					return Result{}, err
				}
				outputs := make(map[string]contract.Ref)
				for _, branch := range joined {
					outputs[branch.Name] = branch.Result.Outputs["output"]
				}
				return Result{Outputs: outputs}, nil
			})
			done := engTestExecuteAsync(t, r)
			for i := 0; i < 4; i++ {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("branches did not reach dispatch barrier")
				}
			}
			for _, i := range order {
				name := fmt.Sprint(i)
				close(releases[name])
				select {
				case got := <-finished:
					if got != name {
						t.Errorf("finished %s, want %s", got, name)
					}
				case <-ctx.Done():
					t.Fatal("branch did not finish")
				}
			}
			var report Report
			select {
			case report = <-done:
			case <-ctx.Done():
				t.Fatal("parallel did not join")
			}
			engTestReport(t, report, Succeeded, 0)
			engTestPersisted(t, r, report)
			for i, branch := range joined {
				if branch.Name != fmt.Sprint(i) || branch.Err != nil {
					t.Errorf("declaration order changed: %+v", joined)
				}
			}
			if len(report.Snapshot.Invocations) != 4 || len(report.Snapshot.Sessions) != 4 {
				t.Fatalf("branches shared identity: %+v", report.Snapshot)
			}
		})
	}
}

func TestEngineSchemaFeedbackAndContentRejectShareBudget(t *testing.T) {
	for _, invalidFirst := range []bool{false, true} {
		for _, budget := range []int{1, 2} {
			t.Run(fmt.Sprintf("invalid-first=%t/budget=%d", invalidFirst, budget), func(t *testing.T) {
				fake := &engTestRuntime{execute: func(ctx context.Context, call engTestCall) engTestReply {
					switch call.Spec.Name {
					case "worker":
						invalid := call.Number == 2
						if invalidFirst {
							invalid = call.Number == 1
						}
						if invalid {
							return engTestReply{Raw: []byte(`{"broken":`)}
						}
						return engTestReply{Data: engTestData{Value: fmt.Sprintf("worker-v%d", call.Number)}}
					case "reviewer":
						if len(call.Request.Inputs) != 1 {
							return engTestReply{Err: fmt.Errorf("reviewer requires exact worker ref")}
						}
						status := "pass"
						if call.Number == 1 {
							status = "reject"
						}
						ref := call.Request.Inputs[0]
						return engTestReply{Data: engTestData{Value: "review", Status: status, Reviewed: &ref}}
					default:
						return engTestReply{Data: engTestData{Value: "final"}}
					}
				}}
				var counts []int
				var expectedFeedback *Feedback
				var finalInputs []contract.Ref
				r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
					a, err := run.OpenSession(ctx, engTestRole("worker"))
					if err != nil {
						return Result{}, err
					}
					b, err := run.OpenSession(ctx, engTestRole("reviewer"))
					if err != nil {
						return Result{}, err
					}
					c, err := run.OpenSession(ctx, engTestRole("final"))
					if err != nil {
						return Result{}, err
					}
					pair, err := run.Root().Retry(ctx, "produce-review", budget, func(ctx context.Context, scope *Scope, state RetryState) (RetryAction, error) {
						counts = append(counts, state.RetryCount)
						if !reflect.DeepEqual(state.Feedback, expectedFeedback) {
							return RetryAction{}, fmt.Errorf("feedback not forwarded: got=%+v want=%+v", state.Feedback, expectedFeedback)
						}
						produced, err := scope.Step(ctx, StepSpec{Key: "produce", Session: a, Prompt: "original task", Feedback: state.Feedback, Output: contract.Spec{SchemaID: engTestSchema}})
						if err != nil {
							if !engTestCode(err, ContractInvalid) || produced.Output != (contract.Ref{}) {
								return RetryAction{}, fmt.Errorf("invalid output unexpectedly consumable: %+v: %w", produced, err)
							}
							expectedFeedback = &Feedback{Message: "repair invalid JSON", SourceAttemptID: produced.AttemptID, SourceCode: string(ContractInvalid)}
							return RetryAction{Again: true, Feedback: expectedFeedback}, nil
						}
						reviewed, err := engTestStep(ctx, scope, b, "review", produced.Output)
						if err != nil {
							return RetryAction{}, err
						}
						verdict, err := Decode[engTestData](ctx, run, reviewed.Output)
						if err != nil {
							return RetryAction{}, err
						}
						if verdict.Reviewed == nil || *verdict.Reviewed != produced.Output {
							return RetryAction{}, fmt.Errorf("stale review: %+v", verdict)
						}
						refs := []contract.Ref{produced.Output, reviewed.Output}
						if err := scope.Decision(ctx, "verdict", verdict.Status, refs); err != nil {
							return RetryAction{}, err
						}
						if verdict.Status == "reject" {
							expectedFeedback = &Feedback{Message: "repair content", SourceAttemptID: reviewed.AttemptID, SourceCode: "ContentRejected", Refs: refs}
							return RetryAction{Again: true, Feedback: expectedFeedback}, nil
						}
						return RetryAction{Result: Result{Outputs: map[string]contract.Ref{"worker": produced.Output, "review": reviewed.Output}}}, nil
					})
					if err != nil {
						return Result{}, err
					}
					finalInputs = []contract.Ref{pair.Outputs["worker"], pair.Outputs["review"]}
					step, err := engTestStep(ctx, run.Root(), c, "final", finalInputs...)
					if err != nil {
						return Result{}, err
					}
					return engTestResult(step), nil
				})
				report := r.Execute()
				if budget == 1 {
					engTestReport(t, report, Failed, 1)
					if !engTestCode(report.Failure, RetryExhausted) {
						t.Fatalf("want RetryExhausted: %v", report.Failure)
					}
				} else {
					engTestReport(t, report, Succeeded, 0)
				}
				engTestPersisted(t, r, report)
				wantCounts := []int{0, 1}
				if budget == 2 {
					wantCounts = append(wantCounts, 2)
				}
				if !reflect.DeepEqual(counts, wantCounts) {
					t.Fatalf("retry budget drift: %v", counts)
				}
				for _, s := range fake.allSessions() {
					calls, closes, confirms := s.history()
					if closes != 1 {
						t.Errorf("%s closes=%d", s.spec.Name, closes)
					}
					switch s.spec.Name {
					case "worker":
						if len(calls) != budget+1 || confirms != budget {
							t.Fatalf("worker calls/confirms=%d/%d", len(calls), confirms)
						}
						for i, call := range calls {
							if call.Request.Prompt != "original task" || (i == 0) != (call.Request.Feedback == nil) {
								t.Errorf("lost prompt/feedback: %+v", call.Request)
							}
							if i > 0 {
								fb := call.Request.Feedback
								wantSchema := (invalidFirst && i == 1) || (!invalidFirst && i == 2)
								if wantSchema && (fb.SourceCode != string(ContractInvalid) || len(fb.Refs) != 0 || fb.SourceAttemptID != calls[i-1].Request.Identity.AttemptID) {
									t.Errorf("schema feedback mismatch: %+v", fb)
								}
								if !wantSchema && (fb.SourceCode != "ContentRejected" || len(fb.Refs) != 2) {
									t.Errorf("content feedback mismatch: %+v", fb)
								}
								if call.Request.Identity.InvocationID != calls[0].Request.Identity.InvocationID || call.Dispatch.Token == calls[i-1].Dispatch.Token || call.CandidatePath == calls[i-1].CandidatePath {
									t.Errorf("retry identity/path reused incorrectly: %+v", call)
								}
							}
						}
					case "reviewer":
						want := 1
						if budget == 2 {
							want = 2
						}
						if len(calls) != want {
							t.Fatalf("invalid output reached reviewer: calls=%d want=%d", len(calls), want)
						}
					case "final":
						want := 0
						if budget == 2 {
							want = 1
						}
						if len(calls) != want {
							t.Fatalf("final ran before acceptance: %d", len(calls))
						}
						if want == 1 && !reflect.DeepEqual(calls[0].Request.Inputs, finalInputs) {
							t.Fatalf("final received stale pair: %+v", calls[0])
						}
					}
				}
			})
		}
	}
}

func TestEngineFreshSessionBetweenRounds(t *testing.T) {
	fake := &engTestRuntime{}
	var refs []contract.Ref
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		for i := 0; i < 2; i++ {
			h, err := run.OpenSession(ctx, engTestRole("worker"))
			if err != nil {
				return Result{}, err
			}
			scope, err := run.Root().Child(fmt.Sprintf("round-%d", i))
			if err != nil {
				return Result{}, err
			}
			step, err := engTestStep(ctx, scope, h, "work", refs...)
			if err != nil {
				return Result{}, err
			}
			refs = append(refs, step.Output)
			if err := run.CloseSession(ctx, h); err != nil {
				return Result{}, err
			}
			if err := run.CloseSession(ctx, h); err != nil {
				return Result{}, err
			}
		}
		return Result{Outputs: map[string]contract.Ref{"last": refs[1]}}, nil
	})
	report := r.Execute()
	engTestReport(t, report, Succeeded, 0)
	engTestPersisted(t, r, report)
	sessions := fake.allSessions()
	if len(sessions) != 2 {
		t.Fatalf("sessions=%d", len(sessions))
	}
	if sessions[0].id == sessions[1].id || sessions[0].spec.SessionDir == sessions[1].spec.SessionDir {
		t.Fatal("fresh session reused identity/directory")
	}
	for i, s := range sessions {
		calls, closes, _ := s.history()
		if len(calls) != 1 || calls[0].Number != 1 || closes != 1 {
			t.Fatalf("fresh-session lifecycle: calls=%+v closes=%d", calls, closes)
		}
		if !reflect.DeepEqual(calls[0].Request.Inputs, refs[:i]) && (i != 0 || len(calls[0].Request.Inputs) != 0) {
			t.Errorf("round %d inherited non-explicit inputs: %+v", i, calls[0])
		}
	}
}

func TestEngineNestedRetryStableInvocationsAndAncestorFinalization(t *testing.T) {
	fake := &engTestRuntime{}
	var outerIDs, innerIDs []string
	var innerCounts []int
	var heldSnapshots, preparingSnapshots []Snapshot
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		return run.Root().Retry(ctx, "outer", 1, func(ctx context.Context, scope *Scope, outer RetryState) (RetryAction, error) {
			outerIDs = append(outerIDs, outer.ActivationID)
			result, err := scope.Retry(ctx, "inner", 1, func(ctx context.Context, inner *Scope, state RetryState) (RetryAction, error) {
				innerIDs = append(innerIDs, state.ActivationID)
				innerCounts = append(innerCounts, state.RetryCount)
				child, err := inner.Child("stable-child")
				if err != nil {
					return RetryAction{}, err
				}
				step, err := engTestStep(ctx, child, h, "work")
				if err != nil {
					return RetryAction{}, err
				}
				if state.RetryCount == 0 {
					return RetryAction{Again: true, Feedback: &Feedback{Message: "inner retry", SourceAttemptID: step.AttemptID, Refs: []contract.Ref{step.Output}}}, nil
				}
				return RetryAction{Result: engTestResult(step)}, nil
			})
			if err != nil {
				return RetryAction{}, err
			}
			if outer.RetryCount == 0 {
				if _, err := engTestStep(ctx, scope, h, "only-first-outer-epoch"); err != nil {
					return RetryAction{}, err
				}
			}
			held := run.Snapshot()
			heldSnapshots = append(heldSnapshots, held)
			for _, invocation := range held.Invocations {
				if invocation.State != AwaitingScope || invocation.Provisional != Succeeded {
					return RetryAction{}, fmt.Errorf("ancestor finalized invocation early: %+v", invocation)
				}
			}
			if outer.RetryCount == 0 {
				return RetryAction{Again: true, Feedback: &Feedback{Message: "outer retry"}}, nil
			}
			return RetryAction{Result: result}, nil
		})
	})
	r.beforeIO = func(path, phase string) {
		if path == "run.json" && phase == "AttemptStarted" {
			preparingSnapshots = append(preparingSnapshots, r.snapshotLocked())
		}
	}
	report := r.Execute()
	engTestReport(t, report, Succeeded, 0)
	engTestPersisted(t, r, report)
	if len(outerIDs) != 2 || outerIDs[0] != outerIDs[1] || !reflect.DeepEqual(innerCounts, []int{0, 1, 0, 1}) || len(innerIDs) != 4 || innerIDs[0] != innerIDs[1] || innerIDs[2] != innerIDs[3] || innerIDs[0] == innerIDs[2] {
		t.Fatalf("activation/budget isolation: outer=%v inner=%v counts=%v", outerIDs, innerIDs, innerCounts)
	}
	if len(report.Snapshot.Invocations) != 2 || len(report.Snapshot.Attempts) != 5 {
		t.Fatalf("logical invocation instability: %+v", report.Snapshot.Invocations)
	}
	if len(preparingSnapshots) != 5 || len(heldSnapshots) != 2 {
		t.Fatalf("missing snapshots: preparing=%d held=%d", len(preparingSnapshots), len(heldSnapshots))
	}
	expectedInnerIDs := []string{innerIDs[0], innerIDs[0], innerIDs[0], innerIDs[2], innerIDs[2], innerIDs[0], innerIDs[2]}
	for i, snapshot := range append(preparingSnapshots, heldSnapshots...) {
		for _, invocation := range snapshot.Invocations {
			want := []string{outerIDs[0]}
			if invocation.Key == "work" {
				want = append(want, expectedInnerIDs[i])
			}
			if !reflect.DeepEqual(invocation.RetryActivationIDs, want) {
				t.Errorf("snapshot %d invocation %s: activations=%v want=%v", i, invocation.Key, invocation.RetryActivationIDs, want)
			}
			if i < len(preparingSnapshots) && invocation.LastSeq == snapshot.LastSeq && snapshot.Attempts[invocation.LastAttemptID].State != Preparing {
				t.Errorf("snapshot %d was not captured in Preparing", i)
			}
		}
	}
	for id, invocation := range report.Snapshot.Invocations {
		want := 4
		if invocation.Key == "only-first-outer-epoch" {
			want = 1
		}
		wantIDs := []string{outerIDs[0]}
		if invocation.Key == "work" {
			wantIDs = append(wantIDs, innerIDs[2])
		}
		if invocation.Attempts != want || invocation.State != Succeeded || !reflect.DeepEqual(invocation.RetryActivationIDs, wantIDs) {
			t.Errorf("final invocation: %+v, want retry activations %v", invocation, wantIDs)
		}
		var numbers []int
		epochs := make(map[string]bool)
		for _, attempt := range report.Snapshot.Attempts {
			if attempt.Identity.InvocationID == id {
				numbers = append(numbers, attempt.Number)
				epochs[attempt.Epoch] = true
			}
		}
		sort.Ints(numbers)
		for i, number := range numbers {
			if number != i+1 {
				t.Errorf("attempt numbers=%v", numbers)
			}
		}
		if len(epochs) != want {
			t.Errorf("execution epoch reused: %v", epochs)
		}
		finished := 0
		for _, event := range engTestEvents(t, r) {
			if event.Kind != "InvocationFinished" {
				continue
			}
			raw, err := json.Marshal(event.Details)
			if err != nil {
				t.Fatal(err)
			}
			var got InvocationState
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got.ID == id {
				finished++
				if event.Seq <= heldSnapshots[len(heldSnapshots)-1].LastSeq {
					t.Errorf("InvocationFinished before outer returned: %+v", event)
				}
			}
		}
		if finished != 1 {
			t.Errorf("invocation %s finished %d times", id, finished)
		}
	}
}

func TestEngineDisplayMetadataCopyOwnership(t *testing.T) {
	var scope *Scope
	var want []string
	var feedback, wantFeedback *Feedback
	var attemptID string
	fake := &engTestRuntime{}
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		producer, err := engTestStep(ctx, run.Root(), h, "producer")
		if err != nil {
			return Result{}, err
		}
		message := "\x1b]52;c;SECRET\a" + strings.Repeat("界", 300) + "\x1b[31m raw feedback\x1b[0m"
		feedback = &Feedback{Message: message, SourceAttemptID: producer.AttemptID, SourceCode: "Direct", Refs: []contract.Ref{producer.Output}}
		wantFeedback = &Feedback{Message: message, SourceAttemptID: producer.AttemptID, SourceCode: "Direct", Refs: []contract.Ref{producer.Output}}
		return run.Root().Retry(ctx, "retry", 0, func(ctx context.Context, child *Scope, state RetryState) (RetryAction, error) {
			scope = child
			want = []string{state.ActivationID}
			step, err := child.Step(ctx, StepSpec{Key: "work", Session: h, Prompt: "work", Feedback: feedback, Output: contract.Spec{SchemaID: engTestSchema}})
			attemptID = step.AttemptID
			feedback.Message = "caller changed feedback after Step"
			feedback.Refs[0].Path = "caller changed ref after Step"
			return RetryAction{Result: engTestResult(step)}, err
		})
	})
	checked := false
	r.beforeIO = func(path, phase string) {
		if path != "run.json" || phase != "AttemptStarted" {
			return
		}
		for id, inv := range r.state.Invocations {
			if inv.Key != "work" {
				continue
			}
			checked = true
			a := r.state.Attempts[inv.LastAttemptID]
			if a.State != Preparing || !reflect.DeepEqual(inv.RetryActivationIDs, want) || !reflect.DeepEqual(a.Feedback, wantFeedback) {
				t.Errorf("Preparing metadata=%+v, feedback=%+v, want activations=%v, feedback=%+v", inv, a.Feedback, want, wantFeedback)
				continue
			}
			// The persistence hook holds r.mu; all mutations are restored before commit.
			feedback.Message, feedback.SourceAttemptID, feedback.SourceCode = "changed input", "changed source", "changed code"
			feedback.Refs[0].Path = "changed input ref"
			if !reflect.DeepEqual(a.Feedback, wantFeedback) {
				t.Error("attempt metadata aliases StepSpec feedback")
			}
			*feedback = *wantFeedback
			feedback.Refs = append([]contract.Ref(nil), wantFeedback.Refs...)
			a.Feedback.Message = "changed metadata"
			a.Feedback.Refs[0].Path = "changed metadata ref"
			if !reflect.DeepEqual(feedback, wantFeedback) {
				t.Error("metadata mutation changed StepSpec feedback")
			}
			*a.Feedback = *wantFeedback
			a.Feedback.Refs = append([]contract.Ref(nil), wantFeedback.Refs...)
			scope.ancestors[0] = "changed scope"
			if !reflect.DeepEqual(inv.RetryActivationIDs, want) {
				t.Error("invocation aliases scope ancestors")
			}
			scope.ancestors[0] = want[0]
			inv.RetryActivationIDs[0] = "changed metadata"
			if !reflect.DeepEqual(scope.ancestors, want) || !reflect.DeepEqual(r.retryAncestors[id], want) {
				t.Error("public metadata aliases private retry ancestry")
			}
			inv.RetryActivationIDs[0] = want[0]
			owned := r.snapshotLocked()
			owned.Invocations[id].RetryActivationIDs[0] = "changed snapshot"
			owned.Attempts[inv.LastAttemptID].Feedback.Message = "changed snapshot feedback"
			owned.Attempts[inv.LastAttemptID].Feedback.Refs[0].Path = "changed snapshot ref"
			if !reflect.DeepEqual(a.Feedback, wantFeedback) {
				t.Error("snapshot mutation changed engine-owned feedback")
			}
			if !reflect.DeepEqual(inv.RetryActivationIDs, want) {
				t.Error("snapshot mutation changed engine-owned metadata")
			}
			raw, err := json.Marshal(inv)
			if err != nil || !strings.Contains(string(raw), `"retry_activation_ids":["`+want[0]+`"]`) {
				t.Errorf("retry activation JSON: %s, error=%v", raw, err)
			}
		}
	}
	report := r.Execute()
	engTestReport(t, report, Succeeded, 0)
	engTestPersisted(t, r, report)
	if !checked {
		t.Fatal("Preparing ownership check did not run")
	}
	for id, inv := range report.Snapshot.Invocations {
		if inv.Key != "work" {
			continue
		}
		snapshot := r.Snapshot()
		if !reflect.DeepEqual(snapshot.Attempts[attemptID].Feedback, wantFeedback) {
			t.Fatalf("missing final feedback metadata: %+v", snapshot.Attempts[attemptID])
		}
		snapshot.Attempts[attemptID].Feedback.Message = "changed consumer feedback"
		snapshot.Attempts[attemptID].Feedback.Refs[0].Path = "changed consumer ref"
		if !reflect.DeepEqual(r.Snapshot().Attempts[attemptID].Feedback, wantFeedback) || !reflect.DeepEqual(report.Snapshot.Attempts[attemptID].Feedback, wantFeedback) {
			t.Error("Snapshot or Report shares feedback metadata")
		}
		if !reflect.DeepEqual(snapshot.Invocations[id].RetryActivationIDs, want) {
			t.Fatalf("missing final activation metadata: %+v", snapshot.Invocations[id])
		}
		snapshot.Invocations[id].RetryActivationIDs[0] = "changed consumer snapshot"
		if !reflect.DeepEqual(r.Snapshot().Invocations[id].RetryActivationIDs, want) || !reflect.DeepEqual(report.Snapshot.Invocations[id].RetryActivationIDs, want) {
			t.Error("Snapshot or Report shares retry activation metadata")
		}
	}
	calls, _, _ := fake.allSessions()[0].history()
	if len(calls) != 2 || calls[1].Request.Identity.AttemptID != attemptID {
		t.Fatalf("expected producer and feedback consumer dispatches: %+v", calls)
	}
	var request contract.Request
	if err := engTestReadJSON(calls[1].RequestPath, &request); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(request.Feedback, wantFeedback) || !reflect.DeepEqual(calls[1].Request.Feedback, wantFeedback) {
		t.Fatal("direct Step feedback did not reach request/dispatch unchanged")
	}
	started := false
	for _, event := range engTestEvents(t, r) {
		if event.Kind != "AttemptStarted" {
			continue
		}
		raw, err := json.Marshal(event.Details)
		if err != nil {
			t.Fatal(err)
		}
		var attempt AttemptState
		if err := json.Unmarshal(raw, &attempt); err != nil {
			t.Fatal(err)
		}
		if attempt.Identity.AttemptID == attemptID {
			started = true
			if attempt.State != Preparing || !reflect.DeepEqual(attempt.Feedback, wantFeedback) {
				t.Fatalf("AttemptStarted did not commit raw feedback: %+v", attempt)
			}
		}
	}
	if !started {
		t.Fatal("direct feedback was not journaled at Preparing")
	}
}

func TestEnginePreflightRejectsDuplicateNamesWithoutAttempts(t *testing.T) {
	fake := &engTestRuntime{}
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		child, err := run.Root().Child("child")
		if err != nil {
			return Result{}, err
		}
		if duplicate, err := run.Root().Child("child"); duplicate != nil || !engTestCode(err, InvalidDefinition) {
			return Result{}, fmt.Errorf("duplicate child accepted: %v", err)
		}
		called := false
		branch := Branch{Name: "same", Do: func(context.Context, *Scope) (Result, error) { called = true; return Result{}, nil }}
		if joined, err := run.Root().Parallel(ctx, "duplicate", CollectAll, []Branch{branch, branch}); joined != nil || !engTestCode(err, InvalidDefinition) || called {
			return Result{}, fmt.Errorf("duplicate branches started: %v", err)
		}
		if len(run.Snapshot().Attempts) != 0 {
			return Result{}, fmt.Errorf("preflight created attempts")
		}
		invalidSpecs := []StepSpec{
			{Key: "unregistered", Session: h, Output: contract.Spec{SchemaID: "unknown"}},
			{Key: "negative-timeout", Session: h, Output: contract.Spec{SchemaID: engTestSchema}, Timeout: -time.Second},
			{Key: "no-session", Output: contract.Spec{SchemaID: engTestSchema}},
		}
		for _, spec := range invalidSpecs {
			step, err := child.Step(ctx, spec)
			if !engTestCode(err, InvalidDefinition) || step != (StepResult{}) || len(run.Snapshot().Attempts) != 0 {
				return Result{}, fmt.Errorf("structural preflight consumed attempt: %+v %v", step, err)
			}
		}
		step, err := engTestStep(ctx, child, h, "work")
		if err != nil {
			return Result{}, err
		}
		if duplicate, err := engTestStep(ctx, child, h, "work"); duplicate != (StepResult{}) || !engTestCode(err, InvalidDefinition) {
			return Result{}, fmt.Errorf("duplicate step accepted: %v", err)
		}
		// The rejected duplicate group must not reserve its name or start work.
		if _, err := run.Root().Parallel(ctx, "duplicate", CollectAll, []Branch{branch}); err != nil {
			return Result{}, err
		}
		if !called || len(run.Snapshot().Attempts) != 1 {
			return Result{}, fmt.Errorf("preflight changed live work")
		}
		return engTestResult(step), nil
	})
	report := r.Execute()
	engTestReport(t, report, Succeeded, 0)
	engTestPersisted(t, r, report)
	calls, _, _ := fake.allSessions()[0].history()
	if len(calls) != 1 {
		t.Fatalf("preflight dispatched %d prompts", len(calls))
	}
}

func TestEngineSessionBusyDoesNotDisturbLease(t *testing.T) {
	for _, phase := range []string{"execute", "confirm"} {
		t.Run(phase, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			wait := func(ctx context.Context) error {
				close(entered)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			}
			fake := &engTestRuntime{}
			if phase == "execute" {
				fake.execute = func(ctx context.Context, call engTestCall) engTestReply {
					return engTestReply{Data: engTestData{Value: "held"}, Err: wait(ctx)}
				}
			} else {
				fake.confirm = func(ctx context.Context, _ engTestCall, receipt runtime.Execution) (runtime.Confirmation, error) {
					return runtime.Confirmation{Seq: receipt.SettledSeq + 1, ActivityEpoch: receipt.ActivityEpoch}, wait(ctx)
				}
			}
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				joined, err := run.Root().Parallel(ctx, "lease", CollectAll, []Branch{
					{Name: "holder", Do: func(ctx context.Context, scope *Scope) (Result, error) {
						step, err := engTestStep(ctx, scope, h, "work")
						if err != nil {
							return Result{}, err
						}
						return engTestResult(step), nil
					}},
					{Name: "contender", Do: func(ctx context.Context, scope *Scope) (Result, error) {
						defer close(release)
						select {
						case <-entered:
						case <-ctx.Done():
							return Result{}, context.Cause(ctx)
						}
						step, err := engTestStep(ctx, scope, h, "work")
						if !engTestCode(err, SessionBusy) || step != (StepResult{}) {
							return Result{}, fmt.Errorf("busy step accepted: %+v %v", step, err)
						}
						if err := run.CloseSession(ctx, h); !engTestCode(err, SessionBusy) {
							return Result{}, fmt.Errorf("busy close disturbed holder: %v", err)
						}
						if _, err := run.CloseSessionReport(ctx, h); !engTestCode(err, SessionBusy) {
							return Result{}, fmt.Errorf("busy close report disturbed holder: %v", err)
						}
						if _, err := run.SessionContextUsage(ctx, h); !engTestCode(err, SessionBusy) {
							return Result{}, fmt.Errorf("busy usage query disturbed holder: %v", err)
						}
						if len(run.Snapshot().Attempts) != 1 {
							return Result{}, fmt.Errorf("busy refusal consumed attempt")
						}
						_, closes, _ := fake.allSessions()[0].history()
						if closes != 0 {
							return Result{}, fmt.Errorf("busy refusal closed holder")
						}
						return Result{}, nil
					}},
				})
				if err != nil {
					return Result{}, err
				}
				for _, branch := range joined {
					if branch.Err != nil {
						return Result{}, branch.Err
					}
				}
				return joined[0].Result, nil
			})
			report := r.Execute()
			engTestReport(t, report, Succeeded, 0)
			engTestPersisted(t, r, report)
			calls, closes, confirms := fake.allSessions()[0].history()
			if len(calls) != 1 || closes != 1 || confirms != 1 {
				t.Fatalf("lease lifecycle: calls=%d closes=%d confirms=%d", len(calls), closes, confirms)
			}
		})
	}
}

func TestEngineFailFastJoinsAndPreservesRootCause(t *testing.T) {
	rootCause := errors.New("root branch rejected work")
	entered := make(chan struct{})
	cancelSeen := make(chan error, 1)
	release := make(chan struct{})
	siblingReturned := make(chan struct{})
	guard, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fake := &engTestRuntime{execute: func(ctx context.Context, call engTestCall) engTestReply {
		if call.Spec.Name == "root-error" {
			select {
			case <-entered:
				return engTestReply{Err: rootCause}
			case <-ctx.Done():
				return engTestReply{Err: context.Cause(ctx)}
			}
		}
		close(entered)
		<-ctx.Done()
		cause := context.Cause(ctx)
		cancelSeen <- cause
		select {
		case <-release:
		case <-guard.Done():
		}
		return engTestReply{Err: cause}
	}}
	var joined []BranchResult
	r, ctx := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		a, err := run.OpenSession(ctx, engTestRole("root-error"))
		if err != nil {
			return Result{}, err
		}
		b, err := run.OpenSession(ctx, engTestRole("sibling"))
		if err != nil {
			return Result{}, err
		}
		joined, err = run.Root().Parallel(ctx, "fail-fast", FailFast, []Branch{
			{Name: "root-error", Do: func(ctx context.Context, scope *Scope) (Result, error) {
				_, err := engTestStep(ctx, scope, a, "work")
				return Result{}, err
			}},
			{Name: "sibling", Do: func(ctx context.Context, scope *Scope) (Result, error) {
				defer close(siblingReturned)
				_, err := engTestStep(ctx, scope, b, "work")
				return Result{}, err
			}},
		})
		select {
		case <-siblingReturned:
		default:
			return Result{}, fmt.Errorf("FailFast returned without joining sibling")
		}
		return Result{}, err
	})
	done := engTestExecuteAsync(t, r)
	select {
	case cause := <-cancelSeen:
		var f *Failure
		if !errors.As(cause, &f) || f.Code != Cancelled || f.Origin != OriginFailFastSibling || !errors.Is(cause, rootCause) || !strings.Contains(f.Message, "root-error") {
			t.Errorf("sibling lost typed root cause: %v", cause)
		}
	case <-ctx.Done():
		t.Fatal("sibling was not cancelled")
	}
	select {
	case report := <-done:
		t.Fatalf("returned before sibling release: %+v", report)
	default:
	}
	close(release)
	var report Report
	select {
	case report = <-done:
	case <-ctx.Done():
		t.Fatal("FailFast did not join")
	}
	engTestReport(t, report, Failed, 1)
	engTestPersisted(t, r, report)
	if !errors.Is(report.Failure, rootCause) || engTestCode(report.Failure, Cancelled) {
		t.Fatalf("sibling replaced root failure: %v", report.Failure)
	}
	if len(joined) != 2 || !errors.Is(joined[0].Err, rootCause) || !engTestCode(joined[1].Err, Cancelled) {
		t.Fatalf("branch results lost: %+v", joined)
	}
	for _, attempt := range report.Snapshot.Attempts {
		want := Failed
		if attempt.Failure.Origin == OriginFailFastSibling {
			want = CancelledState
		}
		if attempt.State != want {
			t.Errorf("wrong branch terminal: %+v", attempt)
		}
	}
}

func TestEngineFailFastCallbackContextErrorPreservesRootCause(t *testing.T) {
	for _, mode := range []string{"plain", "wrapped", "nested-retry"} {
		t.Run(mode, func(t *testing.T) {
			rootCause := errors.New("original root branch failure")
			entered := make(chan struct{})
			var joined []BranchResult
			var groupErr, retryErr error
			r, _ := engTestNew(t, "", &engTestRuntime{}, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				joined, groupErr = run.Root().Parallel(ctx, "callbacks", FailFast, []Branch{
					{Name: "root-error", Do: func(ctx context.Context, _ *Scope) (Result, error) {
						select {
						case <-entered:
							return Result{}, rootCause
						case <-ctx.Done():
							return Result{}, context.Cause(ctx)
						}
					}},
					{Name: "sibling", Do: func(ctx context.Context, scope *Scope) (Result, error) {
						if mode == "nested-retry" {
							_, retryErr = scope.Retry(ctx, "nested", 1, func(ctx context.Context, _ *Scope, _ RetryState) (RetryAction, error) {
								close(entered)
								<-ctx.Done()
								return RetryAction{}, ctx.Err()
							})
							return Result{}, retryErr
						}
						close(entered)
						<-ctx.Done()
						if mode == "wrapped" {
							return Result{}, fmt.Errorf("callback wrapper: %w", ctx.Err())
						}
						return Result{}, ctx.Err()
					}},
				})
				return Result{}, groupErr
			})
			report := r.Execute()
			engTestReport(t, report, Failed, 1)
			engTestPersisted(t, r, report)
			if groupErr != rootCause || report.Failure != rootCause || len(joined) != 2 || joined[0].Err != rootCause {
				t.Fatalf("original root replaced: group=%v report=%v branches=%+v", groupErr, report.Failure, joined)
			}
			cancelled := map[string]error{"branch": joined[1].Err}
			if mode == "nested-retry" {
				cancelled["retry"] = retryErr
			}
			for boundary, err := range cancelled {
				var f *Failure
				if !errors.As(err, &f) || f.Code != Cancelled || f.Origin != OriginFailFastSibling || !errors.Is(err, rootCause) {
					t.Errorf("%s lost sibling cancellation/root chain: %v", boundary, err)
				}
			}
			groupEvents := 0
			for _, event := range engTestEvents(t, r) {
				if event.Kind != "GroupJoined" {
					continue
				}
				groupEvents++
				raw, err := json.Marshal(event.Details)
				if err != nil {
					t.Fatal(err)
				}
				var details struct {
					Results []struct {
						Name    string       `json:"name"`
						Failure *FailureInfo `json:"failure"`
					} `json:"results"`
				}
				if err := json.Unmarshal(raw, &details); err != nil {
					t.Fatal(err)
				}
				if len(details.Results) != 2 {
					t.Fatalf("GroupJoined lost branches: %+v", details)
				}
				root, sibling := details.Results[0], details.Results[1]
				if root.Name != "root-error" || root.Failure == nil || root.Failure.Code != WorkflowFailed || root.Failure.Message != rootCause.Error() {
					t.Errorf("GroupJoined lost original failure: %+v", root)
				}
				if sibling.Name != "sibling" || sibling.Failure == nil || sibling.Failure.Code != Cancelled || sibling.Failure.Origin != OriginFailFastSibling || !strings.Contains(sibling.Failure.Message, "root-error") {
					t.Errorf("GroupJoined lost sibling disposition: %+v", sibling)
				}
			}
			if groupEvents != 1 {
				t.Errorf("GroupJoined count=%d, want 1", groupEvents)
			}
		})
	}
}

func TestEngineRunAttemptLimitCancelsActiveStep(t *testing.T) {
	entered := make(chan struct{})
	var runtimeCause error
	fake := &engTestRuntime{execute: func(ctx context.Context, _ engTestCall) engTestReply {
		close(entered)
		<-ctx.Done()
		runtimeCause = context.Cause(ctx)
		return engTestReply{Err: &Failure{Code: Cancelled, Origin: OriginDefinition, DispatchAccepted: AcceptedYes, LimitScope: "run", Message: "accepted dispatch cancelled", Cause: runtimeCause}}
	}}
	var joined []BranchResult
	var active StepResult
	var limitErr error
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		a, err := run.OpenSession(ctx, engTestRole("active"))
		if err != nil {
			return Result{}, err
		}
		b, err := run.OpenSession(ctx, engTestRole("limit"))
		if err != nil {
			return Result{}, err
		}
		joined, err = run.Root().Parallel(ctx, "limit", CollectAll, []Branch{
			{Name: "active", Do: func(ctx context.Context, scope *Scope) (Result, error) {
				var err error
				active, err = engTestStep(ctx, scope, a, "active-step")
				return Result{}, err
			}},
			{Name: "limit", Do: func(ctx context.Context, scope *Scope) (Result, error) {
				select {
				case <-entered:
				case <-ctx.Done():
					return Result{}, context.Cause(ctx)
				}
				_, limitErr = engTestStep(ctx, scope, b, "over-budget")
				return Result{}, limitErr
			}},
		})
		return Result{}, err
	})
	r.definition.Policy.MaxTotalAttempts = 1
	report := r.Execute()
	engTestReport(t, report, Failed, 1)
	engTestPersisted(t, r, report)
	var limit *Failure
	if !errors.As(limitErr, &limit) || limit.Code != LimitExceeded || limit.LimitScope != "run" || limit.DispatchAccepted != AcceptedNo || !errors.Is(report.Failure, limitErr) || !errors.Is(runtimeCause, limitErr) {
		t.Fatalf("run limit root lost: limit=%v runtime=%v report=%v", limitErr, runtimeCause, report.Failure)
	}
	if len(joined) != 2 || !engTestCode(joined[0].Err, Cancelled) || !errors.Is(joined[0].Err, limitErr) || !errors.Is(joined[1].Err, limitErr) {
		t.Fatalf("active step was not collateral cancellation: %+v", joined)
	}
	if len(report.Snapshot.Attempts) != 1 || active.Output != (contract.Ref{}) {
		t.Fatalf("limit consumed an attempt or active step published: %+v", report.Snapshot.Attempts)
	}
	attempt := report.Snapshot.Attempts[active.AttemptID]
	if attempt.State != CancelledState || attempt.Failure == nil || attempt.Failure.Code != Cancelled || attempt.Failure.LimitScope != "run" || attempt.Output != nil || attempt.DispatchAccepted != AcceptedYes || attempt.Failure.DispatchAccepted != AcceptedYes {
		t.Fatalf("active attempt mislabeled as root failure: %+v", attempt)
	}
	var affected *Failure
	if !errors.As(joined[0].Err, &affected) || affected.DispatchAccepted != AcceptedYes || affected.RunID != attempt.Identity.RunID || affected.StepID != attempt.Identity.InvocationID || affected.AttemptID != active.AttemptID || affected.HandleID != attempt.HandleID {
		t.Errorf("affected failure lost its own acceptance/identity: %+v", affected)
	}
	if limit.RunID != "" || limit.StepID != "" || limit.AttemptID != "" || limit.HandleID != "" || report.Snapshot.Failure.DispatchAccepted != AcceptedNo {
		t.Errorf("affected metadata contaminated run-limit root: root=%+v snapshot=%+v", limit, report.Snapshot.Failure)
	}
	invocation := report.Snapshot.Invocations[attempt.Identity.InvocationID]
	if invocation.State != CancelledState {
		t.Errorf("active invocation state=%s, want Cancelled", invocation.State)
	}
	for _, session := range fake.allSessions() {
		calls, closes, confirms := session.history()
		wantCalls := 0
		if session.spec.Name == "active" {
			wantCalls = 1
		}
		if len(calls) != wantCalls || closes != 1 || confirms != 0 {
			t.Errorf("%s lifecycle: calls=%d closes=%d confirms=%d", session.spec.Name, len(calls), closes, confirms)
		}
	}
}

func TestEngineFatalObservationPreservesSourceAttempt(t *testing.T) {
	for _, code := range []Code{StorageFailed, JournalFailed, LimitExceeded} {
		for _, returnCause := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/context-cause=%t", code, returnCause), func(t *testing.T) {
				entered := make(chan struct{})
				root := &Failure{Code: code, Origin: OriginStorage, DispatchAccepted: AcceptedYes, Message: "runtime fatal observation"}
				if code == LimitExceeded {
					root.Origin, root.LimitScope = OriginDefinition, "run"
				}
				var run *Run
				var sourceID contract.Identity
				var joined []BranchResult
				fake := &engTestRuntime{execute: func(ctx context.Context, call engTestCall) engTestReply {
					if call.Spec.Name == "source" {
						sourceID = call.Request.Identity
						root.RunID, root.HandleID = sourceID.RunID, call.Spec.HandleID
						select {
						case <-entered:
						case <-ctx.Done():
							return engTestReply{Err: context.Cause(ctx)}
						}
						if err := run.Observe(ctx, runtime.Observation{HandleID: call.Spec.HandleID, DispatchToken: call.Dispatch.Token, Seq: 100, Kind: "RuntimeFailed", Failure: root}); err != nil {
							return engTestReply{Err: err}
						}
						<-ctx.Done()
						if !returnCause {
							return engTestReply{Err: root}
						}
					} else {
						close(entered)
						<-ctx.Done()
					}
					return engTestReply{Err: context.Cause(ctx)}
				}}
				run, _ = engTestNew(t, "", fake, func(ctx context.Context, r *Run, _ Input) (Result, error) {
					var branches []Branch
					for _, name := range []string{"source", "sibling"} {
						h, err := r.OpenSession(ctx, engTestRole(name))
						if err != nil {
							return Result{}, err
						}
						branches = append(branches, Branch{Name: name, Do: func(ctx context.Context, scope *Scope) (Result, error) {
							_, err := engTestStep(ctx, scope, h, "work")
							return Result{}, err
						}})
					}
					var err error
					joined, err = r.Root().Parallel(ctx, "fatal", CollectAll, branches)
					return Result{}, err
				})
				report := run.Execute()
				engTestReport(t, report, Failed, 1)
				engTestPersisted(t, run, report)
				engPersistClosed(t, fake, report, 2)
				var failure *Failure
				if !errors.As(report.Failure, &failure) || failure.Code != code || failure.Origin != root.Origin || failure.RunID != sourceID.RunID || failure.HandleID != root.HandleID || failure.AttemptID != "" || !errors.Is(report.Failure, root) {
					t.Fatalf("observation root identity/chain changed: %+v", failure)
				}
				if len(joined) != 2 || len(report.Snapshot.Attempts) != 2 {
					t.Fatalf("missing source/sibling: branches=%+v attempts=%+v", joined, report.Snapshot.Attempts)
				}
				for i, branch := range joined {
					wantCode, wantState := code, Failed
					if i == 1 {
						wantCode, wantState = Cancelled, CancelledState
					}
					var f *Failure
					if !errors.As(branch.Err, &f) || f.Code != wantCode || f.Origin != root.Origin || !errors.Is(branch.Err, root) {
						t.Errorf("%s lost disposition/root chain: %+v", branch.Name, f)
						continue
					}
					attempt, ok := report.Snapshot.Attempts[f.AttemptID]
					if !ok || attempt.State != wantState || attempt.Failure == nil || attempt.Failure.Code != wantCode || attempt.Failure.HandleID != attempt.HandleID || f.HandleID != attempt.HandleID || f.StepID != attempt.Identity.InvocationID || attempt.Output != nil {
						t.Errorf("%s wrong affected identity/terminal: %+v", branch.Name, attempt)
					}
					if (f.AttemptID == sourceID.AttemptID) != (i == 0) {
						t.Errorf("%s attributed to wrong attempt: %+v", branch.Name, f)
					}
				}
				for _, session := range fake.allSessions() {
					calls, _, confirms := session.history()
					if len(calls) != 1 || confirms != 0 {
						t.Errorf("%s did not cancel inside Execute: calls/confirms=%d/%d", session.spec.Name, len(calls), confirms)
					}
				}
			})
		}
	}
}

func TestEngineCollectAllCanHandleOrdinaryBranchFailure(t *testing.T) {
	ordinary := &Failure{Code: ProviderFailed, Origin: OriginProvider, DispatchAccepted: AcceptedYes, Message: "provider exhausted"}
	failed := make(chan struct{})
	fake := &engTestRuntime{execute: func(ctx context.Context, call engTestCall) engTestReply {
		if call.Spec.Name == "fails" {
			return engTestReply{Err: ordinary}
		}
		select {
		case <-failed:
		case <-ctx.Done():
			return engTestReply{Err: fmt.Errorf("CollectAll cancelled survivor: %w", context.Cause(ctx))}
		}
		if err := context.Cause(ctx); err != nil {
			return engTestReply{Err: err}
		}
		return engTestReply{Data: engTestData{Value: "survived"}}
	}}
	var joined []BranchResult
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		a, err := run.OpenSession(ctx, engTestRole("fails"))
		if err != nil {
			return Result{}, err
		}
		b, err := run.OpenSession(ctx, engTestRole("succeeds"))
		if err != nil {
			return Result{}, err
		}
		joined, err = run.Root().Parallel(ctx, "collect", CollectAll, []Branch{
			{Name: "fails", Do: func(ctx context.Context, scope *Scope) (Result, error) {
				defer close(failed)
				_, err := engTestStep(ctx, scope, a, "work")
				return Result{}, err
			}},
			{Name: "succeeds", Do: func(ctx context.Context, scope *Scope) (Result, error) {
				step, err := engTestStep(ctx, scope, b, "work")
				if err != nil {
					return Result{}, err
				}
				return engTestResult(step), nil
			}},
		})
		if err != nil {
			return Result{}, err
		}
		if !errors.Is(joined[0].Err, ordinary) || joined[1].Err != nil {
			return Result{}, fmt.Errorf("CollectAll branch outcomes: %+v", joined)
		}
		return joined[1].Result, nil
	})
	report := r.Execute()
	engTestReport(t, report, Succeeded, 0)
	engTestPersisted(t, r, report)
	if len(joined) != 2 || len(report.Result.Outputs) != 1 {
		t.Fatalf("lost collected results: %+v", joined)
	}
	states := make(map[State]int)
	for _, attempt := range report.Snapshot.Attempts {
		states[attempt.State]++
	}
	if states[Failed] != 1 || states[Succeeded] != 1 {
		t.Fatalf("ordinary failure escalated/lost: %v", states)
	}
}

func TestEngineTypedFailureDisposition(t *testing.T) {
	type disposition struct {
		name     string
		code     Code
		origin   Origin
		state    State
		exit     int
		keep     bool
		phase    string
		accepted DispatchAccepted
	}
	cases := []disposition{
		{"controller", Cancelled, OriginControllerUser, CancelledState, 130, false, "execute", AcceptedUnknown},
		{"sigint", Cancelled, OriginSignalINT, CancelledState, 130, false, "execute", AcceptedUnknown},
		{"sigterm", Cancelled, OriginSignalTERM, CancelledState, 143, false, "execute", AcceptedUnknown},
		{"assistant-aborted", Cancelled, OriginExternalAgentAbort, CancelledState, 130, false, "execute", AcceptedYes},
		{"provider-retry-cancelled", Cancelled, OriginExternalAgentAbort, CancelledState, 130, false, "execute", AcceptedYes},
		{"compaction-aborted", Cancelled, OriginExternalAgentAbort, CancelledState, 130, false, "execute", AcceptedYes},
		{"run-deadline", TimedOut, OriginRunDeadline, TimedOutState, 1, false, "execute", AcceptedUnknown},
		{"attempt-deadline", TimedOut, OriginAttemptDeadline, TimedOutState, 1, false, "execute", AcceptedUnknown},
		{"provider-settled-idle", ProviderFailed, OriginProvider, Failed, 1, true, "execute", AcceptedYes},
		{"terminal-length", OutputTruncated, OriginProvider, Failed, 1, true, "execute", AcceptedYes},
		{"compaction-failed", CompactionFailed, OriginCompaction, Failed, 1, false, "execute", AcceptedYes},
		{"interaction-required", InteractionRequired, OriginProtocol, Failed, 1, false, "execute", AcceptedYes},
		{"extension-failed", ExtensionFailed, OriginProtocol, Failed, 1, false, "execute", AcceptedYes},
		{"model-changed", ModelChanged, OriginProtocol, Failed, 1, false, "execute", AcceptedYes},
		{"thinking-changed", ThinkingChanged, OriginProtocol, Failed, 1, false, "execute", AcceptedYes},
		{"session-changed", SessionChanged, OriginProtocol, Failed, 1, false, "execute", AcceptedYes},
		{"prompt-not-observed", PromptNotObserved, OriginProtocol, Failed, 1, false, "execute", AcceptedUnknown},
		{"ambiguous", AmbiguousExecution, OriginProtocol, Failed, 1, false, "execute", AcceptedUnknown},
		{"protocol-failed", ProtocolFailed, OriginProtocol, Failed, 1, false, "execute", AcceptedUnknown},
		{"rpc-unresponsive", RPCUnresponsive, OriginProtocol, Failed, 1, false, "execute", AcceptedUnknown},
		{"process-exited", ProcessExited, OriginProtocol, Failed, 1, false, "execute", AcceptedUnknown},
		{"dispatch-rejected", DispatchRejected, OriginProtocol, Failed, 1, true, "execute", AcceptedNo},
		{"contract-missing", ContractMissing, OriginContract, Failed, 1, true, "candidate", AcceptedYes},
		{"invalid-json", ContractInvalid, OriginContract, Failed, 1, true, "candidate", AcceptedYes},
		{"schema-invalid", ContractInvalid, OriginContract, Failed, 1, true, "candidate", AcceptedYes},
		{"wrong-identity", IdentityMismatch, OriginContract, Failed, 1, true, "candidate", AcceptedYes},
		{"output-size-limit", LimitExceeded, OriginContract, Failed, 1, true, "candidate", AcceptedYes},
		{"reference-before-dispatch", ReferenceInvalid, OriginDefinition, Failed, 1, true, "input", AcceptedNo},
		{"confirm-ambiguity", AmbiguousExecution, OriginProtocol, Failed, 1, false, "confirm", AcceptedYes},
		{"start-failed", StartFailed, OriginProtocol, Failed, 1, false, "start", AcceptedNo},
		{"bridge-unavailable", BridgeUnavailable, OriginProtocol, Failed, 1, false, "start", AcceptedNo},
		{"unsupported-version", UnsupportedPiVersion, OriginProtocol, Failed, 1, false, "start", AcceptedNo},
		{"unclassified", WorkflowFailed, OriginDefinition, Failed, 1, false, "execute", AcceptedNo},
		{"bare-context-canceled", WorkflowFailed, OriginDefinition, Failed, 1, false, "execute", AcceptedNo},
		{"callback-bare-context-canceled", WorkflowFailed, OriginDefinition, Failed, 1, true, "callback", AcceptedNo},
		{"storage-fatal", StorageFailed, OriginStorage, Failed, 1, false, "execute", AcceptedUnknown},
		{"journal-fatal", JournalFailed, OriginStorage, Failed, 1, false, "execute", AcceptedUnknown},
		{"run-limit-fatal", LimitExceeded, OriginDefinition, Failed, 1, false, "execute", AcceptedUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := error(&Failure{Code: tc.code, Origin: tc.origin, DispatchAccepted: tc.accepted, Message: tc.name})
			if tc.name == "run-limit-fatal" {
				source.(*Failure).LimitScope = "run"
			}
			if tc.name == "unclassified" {
				source = errors.New("unclassified external failure")
			}
			if strings.Contains(tc.name, "bare-context") {
				source = context.Canceled
			}
			wrapped := fmt.Errorf("outer wrapper: %w", fmt.Errorf("inner wrapper: %w", source))
			fake := &engTestRuntime{}
			if tc.phase == "start" {
				fake.startErr = wrapped
			}
			if tc.phase == "execute" {
				fake.execute = func(context.Context, engTestCall) engTestReply { return engTestReply{Err: wrapped} }
			}
			if tc.phase == "confirm" {
				fake.confirm = func(context.Context, engTestCall, runtime.Execution) (runtime.Confirmation, error) {
					return runtime.Confirmation{}, wrapped
				}
			}
			if tc.phase == "candidate" {
				fake.execute = func(ctx context.Context, call engTestCall) engTestReply {
					switch tc.name {
					case "contract-missing":
						return engTestReply{Missing: true}
					case "invalid-json":
						return engTestReply{Raw: []byte(`{"bad":`)}
					case "schema-invalid":
						return engTestReply{Data: engTestData{Value: ""}}
					case "output-size-limit":
						return engTestReply{Raw: []byte(strings.Repeat(" ", (1<<20)+1))}
					case "wrong-identity":
						id := call.Request.Identity
						raw, err := json.Marshal(map[string]any{"meta": map[string]any{"version": 1, "run_id": id.RunID, "invocation_id": id.InvocationID, "attempt_id": id.AttemptID, "dispatch_token": "wrong-token", "schema_id": engTestSchema}, "data": engTestData{Value: "valid"}, "files": []any{}})
						return engTestReply{Raw: raw, Err: err}
					}
					return engTestReply{Err: fmt.Errorf("unknown candidate case")}
				}
			}
			var observed error
			var earlyCloses int
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if tc.phase == "start" {
					observed = err
					_, earlyCloses, _ = fake.allSessions()[0].history()
					return Result{}, err
				}
				if err != nil {
					return Result{}, err
				}
				if tc.phase == "callback" {
					_, earlyCloses, _ = fake.allSessions()[0].history()
					observed = wrapped
					return Result{}, wrapped
				}
				var inputs []contract.Ref
				if tc.phase == "input" {
					inputs = []contract.Ref{{RunID: run.ID(), AttemptID: "not-committed"}}
				}
				step, err := engTestStep(ctx, run.Root(), h, "failure", inputs...)
				observed = err
				_, earlyCloses, _ = fake.allSessions()[0].history()
				if err == nil || step.Output != (contract.Ref{}) {
					return Result{}, fmt.Errorf("failed step returned output: %+v %v", step, err)
				}
				return Result{}, err
			})
			report := r.Execute()
			engTestReport(t, report, tc.state, tc.exit)
			engTestPersisted(t, r, report)
			wantCloses := 1
			if tc.keep {
				wantCloses = 0
			}
			if earlyCloses != wantCloses {
				t.Errorf("handle closes before finalization=%d, want %d", earlyCloses, wantCloses)
			}
			info := report.Snapshot.Failure
			if info == nil || info.Code != tc.code || info.Origin != tc.origin {
				t.Fatalf("typed disposition lost: %+v, error=%v", info, report.Failure)
			}
			if tc.phase != "candidate" && tc.phase != "input" && (!errors.Is(observed, source) || !errors.Is(report.Failure, source)) {
				t.Errorf("wrapped cause lost: observed=%v report=%v", observed, report.Failure)
			}
			wantAttempts := 1
			if tc.phase == "start" || tc.phase == "callback" {
				wantAttempts = 0
			}
			if len(report.Snapshot.Attempts) != wantAttempts {
				t.Fatalf("attempt count=%d want=%d", len(report.Snapshot.Attempts), wantAttempts)
			}
			for _, attempt := range report.Snapshot.Attempts {
				if attempt.State != tc.state || attempt.DispatchAccepted != tc.accepted || attempt.Failure == nil || attempt.Failure.Code != tc.code || attempt.Failure.Origin != tc.origin {
					t.Errorf("wrong attempt disposition: %+v", attempt)
				}
				if attempt.Output != nil {
					t.Errorf("failed attempt published output: %+v", attempt)
				}
			}
			calls, closes, confirms := fake.allSessions()[0].history()
			wantCalls := wantAttempts
			if tc.phase == "input" {
				wantCalls = 0
			}
			if len(calls) != wantCalls || closes != 1 {
				t.Errorf("dispatch automatically retried/cleanup lost: calls=%d closes=%d", len(calls), closes)
			}
			wantConfirms := 0
			if tc.phase == "confirm" {
				wantConfirms = 1
			}
			if confirms != wantConfirms {
				t.Errorf("unexpected Confirm count=%d want=%d", confirms, wantConfirms)
			}
		})
	}
}

func TestEngineProviderFailureRequiresIdleBindingToKeepHandle(t *testing.T) {
	for _, mode := range []string{"streaming", "compacting", "pending", "model-drift", "identity-drift", "offline", "snapshot-error"} {
		t.Run(mode, func(t *testing.T) {
			fake := &engTestRuntime{execute: func(context.Context, engTestCall) engTestReply {
				return engTestReply{Err: &Failure{Code: ProviderFailed, Origin: OriginProvider, DispatchAccepted: AcceptedYes, Message: "failed"}}
			}}
			fake.snapshot = func(ctx context.Context, spec runtime.SessionSpec) (runtime.SessionState, error) {
				state := runtime.SessionState{Identity: fake.allSessions()[0].Identity(), Model: spec.Model, Health: "Online"}
				switch mode {
				case "streaming":
					state.Streaming = true
				case "compacting":
					state.Compacting = true
				case "pending":
					state.PendingCount = 1
				case "model-drift":
					state.Model.ID = "other"
				case "identity-drift":
					state.Identity.SessionID = "other"
				case "offline":
					state.Health = "Offline"
				case "snapshot-error":
					return state, errors.New("snapshot failed")
				}
				return state, nil
			}
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				_, err = engTestStep(ctx, run.Root(), h, "failed")
				if !engTestCode(err, ProviderFailed) {
					return Result{}, fmt.Errorf("expected ProviderFailed, got %v", err)
				}
				_, closes, _ := fake.allSessions()[0].history()
				if closes != 1 {
					return Result{}, fmt.Errorf("unsafe handle was retained after %s", mode)
				}
				if step, err := engTestStep(ctx, run.Root(), h, "reuse"); !engTestCode(err, InvalidDefinition) || step != (StepResult{}) {
					return Result{}, fmt.Errorf("closed handle reused: %+v %v", step, err)
				}
				return Result{}, nil
			})
			report := r.Execute()
			engTestReport(t, report, Succeeded, 0)
			engTestPersisted(t, r, report)
		})
	}
}

func TestEngineMultipleRunsIsolateDirectoriesRefsAndCleanup(t *testing.T) {
	base := t.TempDir()
	fake := &engTestRuntime{}
	firstReady := make(chan contract.Ref, 1)
	secondDone := make(chan struct{})
	var foreign contract.Ref
	first, ctx := engTestNew(t, base, fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		step, err := engTestStep(ctx, run.Root(), h, "first")
		if err != nil {
			return Result{}, err
		}
		firstReady <- step.Output
		select {
		case <-secondDone:
		case <-ctx.Done():
			return Result{}, context.Cause(ctx)
		}
		if _, err := Decode[engTestData](ctx, run, step.Output); err != nil {
			return Result{}, err
		}
		for _, session := range fake.allSessions() {
			if session.id.SessionID == step.Execution.SessionID {
				_, closes, _ := session.history()
				if closes != 0 {
					return Result{}, fmt.Errorf("second run cleanup closed first run session")
				}
			}
		}
		return engTestResult(step), nil
	})
	done := engTestExecuteAsync(t, first)
	select {
	case foreign = <-firstReady:
	case <-ctx.Done():
		t.Fatal("first run did not publish")
	}
	second, _ := engTestNew(t, base, fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		if _, err := Decode[engTestData](ctx, run, foreign); !engTestCode(err, ReferenceInvalid) {
			return Result{}, fmt.Errorf("cross-run Decode accepted: %v", err)
		}
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		if step, err := engTestStep(ctx, run.Root(), h, "foreign-input", foreign); !engTestCode(err, ReferenceInvalid) || step.Output != (contract.Ref{}) {
			return Result{}, fmt.Errorf("cross-run input accepted: %+v %v", step, err)
		}
		step, err := engTestStep(ctx, run.Root(), h, "own-output")
		if err != nil {
			return Result{}, err
		}
		return engTestResult(step), nil
	})
	secondReport := second.Execute()
	close(secondDone)
	var firstReport Report
	select {
	case firstReport = <-done:
	case <-ctx.Done():
		t.Fatal("first run did not finish")
	}
	engTestReport(t, firstReport, Succeeded, 0)
	engTestReport(t, secondReport, Succeeded, 0)
	engTestPersisted(t, first, firstReport)
	engTestPersisted(t, second, secondReport)
	if first.ID() == second.ID() || first.Dir() == second.Dir() || firstReport.Snapshot.TaskID == secondReport.Snapshot.TaskID {
		t.Fatal("runs shared identity/directory")
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("exclusive task directories=%d", len(entries))
	}
	for _, s := range fake.allSessions() {
		calls, closes, _ := s.history()
		if len(calls) != 1 || closes != 1 {
			t.Errorf("cross-run dispatch or cleanup leak: calls=%d closes=%d", len(calls), closes)
		}
	}
	if len(firstReport.Snapshot.Attempts) != 1 || len(secondReport.Snapshot.Attempts) != 2 {
		t.Fatal("attempt registries contaminated")
	}
}

func TestEngineStepAndNoStepRetryPersistBeforeWorkflowReturns(t *testing.T) {
	checkpoints := make(chan string)
	release := make(chan struct{})
	barrier := func(ctx context.Context, phase string) error {
		select {
		case checkpoints <- phase:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	r, ctx := engTestNew(t, "", &engTestRuntime{}, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		if err := barrier(ctx, "before-step"); err != nil {
			return Result{}, err
		}
		step, err := engTestStep(ctx, run.Root(), h, "work")
		if err != nil {
			return Result{}, err
		}
		if err := barrier(ctx, "after-step"); err != nil {
			return Result{}, err
		}
		_, err = run.Root().Retry(ctx, "no-step", 0, func(ctx context.Context, _ *Scope, _ RetryState) (RetryAction, error) {
			return RetryAction{}, barrier(ctx, "retry-active")
		})
		if err != nil {
			return Result{}, err
		}
		if err := barrier(ctx, "retry-finished"); err != nil {
			return Result{}, err
		}
		return engTestResult(step), nil
	})
	done := engTestExecuteAsync(t, r)
	var previousSeq uint64
	for _, phase := range []string{"before-step", "after-step", "retry-active", "retry-finished"} {
		select {
		case got := <-checkpoints:
			if got != phase {
				t.Fatalf("barrier=%s, want %s", got, phase)
			}
		case report := <-done:
			t.Fatalf("workflow finished before %s: %+v", phase, report)
		case <-ctx.Done():
			t.Fatalf("workflow did not reach %s", phase)
		}
		snapshot := r.Snapshot()
		var disk Snapshot
		if err := engTestReadJSON(filepath.Join(r.Dir(), "run.json"), &disk); err != nil {
			t.Fatal(err)
		}
		events := engTestEvents(t, r)
		last := events[len(events)-1]
		if snapshot.State != Running || !snapshot.StatePersisted || !reflect.DeepEqual(snapshot, disk) || snapshot.LastSeq != last.Seq || snapshot.LastSeq <= previousSeq {
			t.Fatalf("%s live/disk/journal mismatch: live=%+v disk=%+v last=%+v", phase, snapshot, disk, last)
		}
		for _, event := range events {
			if event.Kind == "RunFinalizing" || event.Kind == "RunFinished" {
				t.Fatalf("%s relies on finalization: %+v", phase, event)
			}
		}
		if len(snapshot.Sessions) != 1 {
			t.Fatalf("%s sessions=%+v", phase, snapshot.Sessions)
		}
		for _, session := range snapshot.Sessions {
			if session.State != "Idle" {
				t.Errorf("%s session=%s, want Idle", phase, session.State)
			}
		}
		if phase != "before-step" {
			if len(snapshot.Attempts) != 1 {
				t.Fatalf("%s attempts=%+v", phase, snapshot.Attempts)
			}
			for _, attempt := range snapshot.Attempts {
				if attempt.State != Succeeded {
					t.Errorf("%s step not complete: %+v", phase, attempt)
				}
			}
		}
		wantKind := map[string]string{"before-step": "SessionReady", "after-step": "AttemptSucceeded", "retry-active": "RetryStarted", "retry-finished": "RetryFinished"}[phase]
		if last.Kind != wantKind {
			t.Errorf("%s last event=%s, want %s", phase, last.Kind, wantKind)
		}
		if phase == "retry-active" || phase == "retry-finished" {
			if len(snapshot.Retries) != 1 {
				t.Fatalf("%s retries=%+v", phase, snapshot.Retries)
			}
			for _, retry := range snapshot.Retries {
				if retry.Active != (phase == "retry-active") {
					t.Errorf("%s retry active=%t", phase, retry.Active)
				}
			}
			if phase == "retry-finished" {
				raw, err := json.Marshal(last.Details)
				if err != nil {
					t.Fatal(err)
				}
				var finished RetryStatus
				if err := json.Unmarshal(raw, &finished); err != nil {
					t.Fatal(err)
				}
				if finished.Active || finished.ActivationID == "" || !reflect.DeepEqual(finished, snapshot.Retries[finished.ActivationID]) {
					t.Errorf("RetryFinished disagrees with inactive snapshot: %+v", finished)
				}
			}
		}
		// Drain at each barrier so RetryStarted cannot mask a missing finish notification.
		select {
		case <-r.Changes():
		default:
			t.Errorf("%s did not notify snapshot consumers", phase)
		}
		previousSeq = snapshot.LastSeq
		select {
		case release <- struct{}{}:
		case <-ctx.Done():
			t.Fatalf("cannot release %s", phase)
		}
	}
	select {
	case report := <-done:
		engTestReport(t, report, Succeeded, 0)
		engTestPersisted(t, r, report)
	case <-ctx.Done():
		t.Fatal("workflow did not finish after barriers")
	}
}

func TestEngineWorkflowAPIsRejectAfterExecute(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("callback-error=%t", fail), func(t *testing.T) {
			callbackErr := errors.New("ordinary callback error")
			fake := &engTestRuntime{}
			var h *SessionHandle
			var step StepResult
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				var err error
				h, err = run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				step, err = engTestStep(ctx, run.Root(), h, "work")
				if err != nil {
					return Result{}, err
				}
				if fail {
					return Result{}, callbackErr
				}
				return engTestResult(step), nil
			})
			report := r.Execute()
			if fail {
				engTestReport(t, report, Failed, 1)
				if !errors.Is(report.Failure, callbackErr) {
					t.Fatalf("callback cause lost: %v", report.Failure)
				}
			} else {
				engTestReport(t, report, Succeeded, 0)
			}
			engTestPersisted(t, r, report)
			before := r.Snapshot()
			files := make(map[string]string)
			for _, name := range []string{"events.jsonl", "run.json"} {
				raw, err := os.ReadFile(filepath.Join(r.Dir(), name))
				if err != nil {
					t.Fatal(err)
				}
				files[name] = string(raw)
			}
			select {
			case <-r.Changes():
			default:
			}
			ctx := context.Background()
			called := false
			for _, api := range []struct {
				name string
				call func() error
			}{
				{"Child", func() error { _, err := r.Root().Child("late-child"); return err }},
				{"Retry", func() error {
					_, err := r.Root().Retry(ctx, "late-retry", 0, func(context.Context, *Scope, RetryState) (RetryAction, error) {
						called = true
						return RetryAction{}, nil
					})
					return err
				}},
				{"Decision", func() error { return r.Root().Decision(ctx, "late-decision", "reason", []contract.Ref{step.Output}) }},
				{"Decode", func() error { _, err := Decode[engTestData](ctx, r, step.Output); return err }},
				{"OpenSession", func() error { _, err := r.OpenSession(ctx, engTestRole("late-worker")); return err }},
				{"Step", func() error { _, err := engTestStep(ctx, r.Root(), h, "late-step"); return err }},
				{"Parallel", func() error {
					_, err := r.Root().Parallel(ctx, "late-group", CollectAll, []Branch{{Name: "late-branch", Do: func(context.Context, *Scope) (Result, error) {
						called = true
						return Result{}, nil
					}}})
					return err
				}},
				{"CloseSession", func() error { return r.CloseSession(ctx, h) }},
				{"CloseSessionReport", func() error { _, err := r.CloseSessionReport(ctx, h); return err }},
				{"SessionContextUsage", func() error { _, err := r.SessionContextUsage(ctx, h); return err }},
			} {
				t.Run(api.name, func(t *testing.T) {
					err := api.call()
					var f *Failure
					if !errors.As(err, &f) || f.Code != InvalidDefinition || f.Origin != OriginDefinition || f.Phase != "run" || f.Message != "workflow APIs are not active" || f.Cause != nil {
						t.Errorf("API did not reject at lifecycle guard: %v", err)
					}
					if called || !reflect.DeepEqual(before, r.Snapshot()) {
						t.Error("inactive API invoked callback or mutated snapshot")
					}
					for name, want := range files {
						raw, err := os.ReadFile(filepath.Join(r.Dir(), name))
						if err != nil || string(raw) != want {
							t.Errorf("inactive API changed %s: %v", name, err)
						}
					}
					select {
					case <-r.Changes():
						t.Error("inactive API emitted a change notification")
					default:
					}
					if len(fake.allSessions()) != 1 {
						t.Error("inactive API started a runtime session")
					}
					calls, closes, confirms := fake.allSessions()[0].history()
					if len(calls) != 1 || closes != 1 || confirms != 1 {
						t.Errorf("inactive API touched runtime: calls=%d closes=%d confirms=%d", len(calls), closes, confirms)
					}
				})
			}
		})
	}
}

func TestEngineFinalDelivery(t *testing.T) {
	for _, name := range []string{"earlier-producer", "json-only", "no-selection", "unknown-key", "unknown-file-id", "evidence-kind", "wrong-digest", "wrong-manifest-digest", "tampered-artifact", "uncommitted-ref", "invalid-unselected-success", "incomplete", "incomplete-invalid-unselected", "incomplete-invalid-selected", "failed-no-selection"} {
		t.Run(name, func(t *testing.T) {
			const fileID = "readable-report"
			const artifact = "artifacts/not-the-file-id.txt"
			ordinary := errors.New("required reviewer failed")
			fake := &engTestRuntime{execute: func(_ context.Context, call engTestCall) engTestReply {
				if name == "json-only" {
					return engTestReply{Data: engTestData{Value: call.Spec.Name}}
				}
				if err := os.WriteFile(filepath.Join(filepath.Dir(call.CandidatePath), artifact), []byte("incomplete report\n"), 0600); err != nil {
					return engTestReply{Err: err}
				}
				if err := os.WriteFile(filepath.Join(filepath.Dir(call.CandidatePath), "evidence/source.txt"), []byte("source"), 0600); err != nil {
					return engTestReply{Err: err}
				}
				raw, err := engPersistEnvelope(call.Request.Identity, call.Spec.Name, []map[string]string{
					{"id": fileID, "kind": "artifact", "path": artifact},
					{"id": "source", "kind": "evidence", "path": "evidence/source.txt"},
				})
				return engTestReply{Raw: raw, Err: err}
			}}
			var first, last StepResult
			var selection *FinalSelection
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				a, err := run.OpenSession(ctx, engTestRole("selected-producer"))
				if err != nil {
					return Result{}, err
				}
				scope, err := run.Root().Child("delivery")
				if err != nil {
					return Result{}, err
				}
				first, err = engTestStep(ctx, scope, a, "produce")
				if err != nil {
					return Result{}, err
				}
				if err := run.CloseSession(ctx, a); err != nil {
					return Result{}, err
				}
				b, err := run.OpenSession(ctx, engTestRole("later-session"))
				if err != nil {
					return Result{}, err
				}
				last, err = engTestStep(ctx, run.Root(), b, "later-step", first.Output)
				if err != nil {
					return Result{}, err
				}
				selection = &FinalSelection{Output: "selected", FileID: fileID}
				result := Result{Outputs: map[string]contract.Ref{"selected": first.Output, "later": last.Output}, Final: selection}
				switch name {
				case "json-only":
					selection.FileID = ""
				case "no-selection":
					result.Final = nil
				case "failed-no-selection":
					result.Final = nil
					bad := first.Output
					bad.SHA256 = strings.Repeat("0", 64)
					result.Outputs["selected"] = bad
				case "unknown-key":
					selection.Output = "missing"
				case "unknown-file-id":
					selection.FileID = "not-the-file-id.txt"
				case "evidence-kind":
					selection.FileID = "source"
				case "wrong-digest", "incomplete-invalid-selected":
					bad := first.Output
					bad.SHA256 = strings.Repeat("0", 64)
					result.Outputs["selected"] = bad
				case "wrong-manifest-digest":
					bad := first.Output
					bad.ManifestSHA256 = strings.Repeat("0", 64)
					result.Outputs["selected"] = bad
				case "tampered-artifact":
					if err := os.WriteFile(filepath.Join(filepath.Dir(first.Output.Path), artifact), []byte("tampered"), 0600); err != nil {
						return Result{}, err
					}
				case "uncommitted-ref":
					result.Outputs["selected"], err = engPersistUnregistered(ctx, run)
					selection.FileID = ""
					if err != nil {
						return Result{}, err
					}
				case "invalid-unselected-success", "incomplete-invalid-unselected":
					bad := last.Output
					bad.SHA256 = strings.Repeat("0", 64)
					result.Outputs["later"] = bad
				}
				if name == "earlier-producer" || name == "incomplete" {
					// Cleanup removes only the external publication files, after final validation.
					if err := run.AddCleanup(ctx, "remove-publication", func(context.Context) error {
						return os.RemoveAll(filepath.Dir(first.Output.Path))
					}); err != nil {
						return Result{}, err
					}
				}
				if strings.HasPrefix(name, "incomplete") || name == "failed-no-selection" {
					return result, ordinary
				}
				return result, nil
			})
			barriers := 0
			r.afterPublish = func(ref contract.Ref) {
				barriers++
				// A real Store rename is not yet an engine publication capability.
				final, err := r.resolveFinal(context.Background(), Result{Outputs: map[string]contract.Ref{"selected": ref}, Final: &FinalSelection{Output: "selected"}})
				if final != nil || !engTestCode(err, ReferenceInvalid) {
					t.Errorf("pre-commit final accepted: %+v, %v", final, err)
				}
			}
			report := r.Execute()
			valid := name == "earlier-producer" || name == "json-only" || name == "no-selection"
			wantState, wantExit := Failed, 1
			if valid {
				wantState, wantExit = Succeeded, 0
			}
			engTestReport(t, report, wantState, wantExit)
			engTestPersisted(t, r, report)
			engPersistClosed(t, fake, report, 2)
			if barriers != 2 {
				t.Fatalf("publication barriers=%d, want 2", barriers)
			}
			if strings.HasPrefix(name, "incomplete") || name == "failed-no-selection" {
				if !errors.Is(report.Failure, ordinary) {
					t.Errorf("original workflow failure lost: %v", report.Failure)
				}
			} else if !valid && !engTestCode(report.Failure, ReferenceInvalid) {
				t.Errorf("selection refused for wrong reason: %v", report.Failure)
			}
			if name == "incomplete-invalid-selected" && !engTestCode(report.Failure, ReferenceInvalid) {
				t.Errorf("failed workflow did not validate its explicit final: %v", report.Failure)
			}
			if (name == "incomplete-invalid-unselected" || name == "failed-no-selection") && engTestCode(report.Failure, ReferenceInvalid) {
				t.Errorf("failed workflow validated non-final diagnostic refs: %v", report.Failure)
			}
			wantFinal := name == "earlier-producer" || name == "json-only" || name == "incomplete" || name == "incomplete-invalid-unselected"
			if wantFinal {
				producer := report.Snapshot.Attempts[first.AttemptID]
				later := report.Snapshot.Attempts[last.AttemptID]
				want := &FinalDelivery{Output: "selected", Ref: first.Output, HandleID: producer.HandleID, Scope: "root/delivery", Step: "produce"}
				if name != "json-only" {
					want.ArtifactPath = filepath.Join(filepath.Dir(first.Output.Path), artifact)
				}
				if !reflect.DeepEqual(report.Final, want) {
					t.Fatalf("final=%+v, want %+v", report.Final, want)
				}
				if producer.HandleID == later.HandleID || !producer.FinishedAt.Before(later.FinishedAt) || producer.LastSeq >= later.LastSeq {
					t.Fatalf("fixture did not finish a distinct later producer: %+v / %+v", producer, later)
				}
				if report.Snapshot.Sessions[report.Final.HandleID].Role.Name != "selected-producer" {
					t.Fatal("selected final points to last session")
				}
				if name == "earlier-producer" || name == "incomplete" {
					if _, err := os.Stat(report.Final.Ref.Path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("cleanup did not remove publication before Execute returned: %v", err)
					}
				}
			} else if report.Final != nil {
				t.Fatalf("unverified or unselected final fabricated: %+v", report.Final)
			}
			var disk struct {
				RunID   string                  `json:"run_id"`
				Outputs map[string]contract.Ref `json:"outputs"`
				Final   *FinalDelivery          `json:"final"`
			}
			if err := engTestReadJSON(filepath.Join(r.Dir(), "result.json"), &disk); err != nil {
				t.Fatal(err)
			}
			if disk.RunID != r.ID() || !reflect.DeepEqual(disk.Outputs, report.Result.Outputs) || !reflect.DeepEqual(disk.Final, report.Final) {
				t.Fatalf("persisted delivery differs from Report: %+v / %+v", disk, report.Final)
			}
			if report.Final != nil {
				if _, err := Decode[engTestData](context.Background(), r, report.Final.Ref); !engTestCode(err, InvalidDefinition) {
					t.Errorf("display delivery authorized workflow APIs after Execute: %v", err)
				}
			}
			if _, err := r.store.Read(context.Background(), first.Output); err == nil || !strings.Contains(err.Error(), "store closed") {
				t.Fatalf("Execute returned before Store closure: %v", err)
			}
			if err := os.RemoveAll(filepath.Join(r.Dir(), "steps")); err != nil {
				t.Fatal(err)
			}
			selection.Output, selection.FileID = "caller-mutated", "caller-mutated"
			if !reflect.DeepEqual(disk.Final, report.Final) {
				t.Fatal("resolved delivery depends on disk or mutable selection after Store close")
			}
		})
	}
}

func TestEngineFinalDeliveryRootStop(t *testing.T) {
	for _, stop := range []string{"cancel", "deadline", "fatal-runtime", "fatal-callback"} {
		t.Run(stop, func(t *testing.T) {
			root := &Failure{Code: StorageFailed, Origin: OriginStorage, Message: "external storage failed"}
			fake := &engTestRuntime{execute: func(_ context.Context, call engTestCall) engTestReply {
				if call.Request.Prompt == "fatal" {
					return engTestReply{Err: root}
				}
				return engTestReply{Data: engTestData{Value: "valid"}}
			}}
			var ref contract.Ref
			workflow := func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("producer"))
				if err != nil {
					return Result{}, err
				}
				step, err := engTestStep(ctx, run.Root(), h, "produce")
				if err != nil {
					return Result{}, err
				}
				ref = step.Output
				result := Result{Outputs: map[string]contract.Ref{"selected": ref}, Final: &FinalSelection{Output: "selected"}}
				switch stop {
				case "cancel":
					run.Cancel(OriginControllerUser)
				case "deadline":
					<-ctx.Done()
				case "fatal-runtime":
					_, err = engTestStep(ctx, run.Root(), h, "fatal")
				case "fatal-callback":
					err = root
				}
				return result, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			policy := DefaultRunPolicy()
			if stop == "deadline" {
				policy.RunTimeout = time.Second
			}
			base := t.TempDir()
			r, err := New(ctx, Definition{Name: "final-stop", Version: "1", Policy: policy, Execute: workflow}, Input{Prompt: "final stop", LaunchCWD: base}, Options{BaseDir: base, Schemas: engTestSchemas(t), Runtime: fake})
			if err != nil {
				t.Fatal(err)
			}
			report := r.Execute()
			wantState, wantExit := Failed, 1
			switch stop {
			case "cancel":
				wantState, wantExit = CancelledState, 130
			case "deadline":
				wantState = TimedOutState
			}
			engTestReport(t, report, wantState, wantExit)
			engPersistClosed(t, fake, report, 1)
			if report.Final != nil {
				t.Errorf("root stop fabricated final metadata: %+v", report.Final)
			}
			if strings.HasPrefix(stop, "fatal") && !errors.Is(report.Failure, root) {
				t.Errorf("fatal root lost: %v", report.Failure)
			}
			var disk struct {
				Final *FinalDelivery `json:"final"`
			}
			if err := engTestReadJSON(filepath.Join(r.Dir(), "result.json"), &disk); err != nil {
				t.Fatal(err)
			}
			if disk.Final != nil || report.Result.Outputs["selected"] != ref {
				t.Errorf("stop confused diagnostic inventory with final delivery: %+v", disk.Final)
			}
		})
	}
}

func TestEngineRetryBudgetBoundaries(t *testing.T) {
	for _, max := range []int{0, 1, 3} {
		for _, exhaust := range []bool{false, true} {
			t.Run(fmt.Sprintf("max=%d/exhaust=%t", max, exhaust), func(t *testing.T) {
				fake := &engTestRuntime{}
				var counts []int
				var lastID string
				r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
					h, err := run.OpenSession(ctx, engTestRole("worker"))
					if err != nil {
						return Result{}, err
					}
					return run.Root().Retry(ctx, "budget", max, func(ctx context.Context, scope *Scope, state RetryState) (RetryAction, error) {
						counts = append(counts, state.RetryCount)
						if state.MaxRetries != max {
							return RetryAction{}, fmt.Errorf("MaxRetries=%d", state.MaxRetries)
						}
						step, err := engTestStep(ctx, scope, h, "work")
						if err != nil {
							return RetryAction{}, err
						}
						lastID = step.AttemptID
						if !exhaust && state.RetryCount == max {
							return RetryAction{Result: engTestResult(step)}, nil
						}
						return RetryAction{Again: true, Feedback: &Feedback{Message: fmt.Sprintf("reason-%d", state.RetryCount), SourceAttemptID: step.AttemptID}}, nil
					})
				})
				report := r.Execute()
				if exhaust {
					engTestReport(t, report, Failed, 1)
					var f *Failure
					if !errors.As(report.Failure, &f) || f.Code != RetryExhausted || f.AttemptID != lastID || f.Message != fmt.Sprintf("reason-%d", max) {
						t.Fatalf("last rejection not retained: %v", report.Failure)
					}
				} else {
					engTestReport(t, report, Succeeded, 0)
				}
				engTestPersisted(t, r, report)
				if len(counts) != max+1 || len(report.Snapshot.Attempts) != max+1 {
					t.Fatalf("callback/attempt counts: %v/%d", counts, len(report.Snapshot.Attempts))
				}
				for i, count := range counts {
					if count != i {
						t.Errorf("counts=%v", counts)
					}
				}
			})
		}
	}
}
