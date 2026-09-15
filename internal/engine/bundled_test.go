package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/bundled"
	bundledoptions "pi-workflow-controller/internal/testutil/bundled/options"
	"pi-workflow-controller/internal/testutil/protocol"
)

// These barriers wrap only the external Session boundary. Receipts and
// confirmations are returned unchanged by the installed Pi runtime.
type engBundledRuntime struct {
	runtime.Runtime
	barriers chan engBundledBarrier
}
type engBundledBarrier struct {
	phase        string
	call         engTestCall
	identity     runtime.Identity
	receipt      runtime.Execution
	confirmation runtime.Confirmation
	release      chan struct{}
}
type engBundledSession struct {
	runtime.Session
	owner *engBundledRuntime
	call  engTestCall
}

func (r *engBundledRuntime) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	s, err := r.Runtime.Start(ctx, spec)
	if s == nil {
		return nil, err
	}
	return &engBundledSession{Session: s, owner: r}, err
}
func (s *engBundledSession) barrier(ctx context.Context, phase string, receipt runtime.Execution, confirmation runtime.Confirmation) {
	b := engBundledBarrier{phase, s.call, s.Identity(), receipt, confirmation, make(chan struct{})}
	select {
	case s.owner.barriers <- b:
	case <-ctx.Done():
		return
	}
	select {
	case <-b.release:
	case <-ctx.Done():
	}
}
func (s *engBundledSession) Execute(ctx context.Context, dispatch runtime.Dispatch) (runtime.Execution, error) {
	call, err := engBundledCall(dispatch.Message)
	if err != nil {
		return runtime.Execution{}, err
	}
	s.call = call
	receipt, err := s.Session.Execute(ctx, dispatch)
	if err == nil {
		s.barrier(ctx, "executed", receipt, runtime.Confirmation{})
	}
	return receipt, err
}
func (s *engBundledSession) Confirm(ctx context.Context, receipt runtime.Execution) (runtime.Confirmation, error) {
	s.barrier(ctx, "staged", receipt, runtime.Confirmation{})
	confirmation, err := s.Session.Confirm(ctx, receipt)
	if err == nil {
		s.barrier(ctx, "confirmed", receipt, confirmation)
	}
	return confirmation, err
}
func engBundledCall(message string) (engTestCall, error) {
	var call engTestCall
	var err error
	call.RequestPath, call.CandidatePath, call.Request, err = protocol.ParseDispatch(message)
	if err != nil {
		return call, err
	}
	call.Dispatch = runtime.Dispatch{Token: call.Request.Identity.DispatchToken, Message: message}
	return call, nil
}

func engBundledReceipt(t *testing.T, h *bundled.Harness, b engBundledBarrier) {
	t.Helper()
	r := b.receipt
	if r.Token != b.call.Request.Identity.DispatchToken || r.SessionID != b.identity.SessionID || r.StartSeq >= r.SettledSeq || r.StopReason != "stop" || r.ExtraUserInputs != 0 {
		t.Fatalf("receipt does not belong to this dispatch: %+v", b)
	}
	var seq, userSeq, toolSeq uint64
	entries := map[string]map[string]any{}
	settled := false
	for _, record := range h.Records() {
		if record.Direction != "out" || record.PID != b.identity.PID {
			continue
		}
		seq++
		f := record.Frame
		if seq == r.SettledSeq {
			settled = f["type"] == "agent_settled"
		}
		if f["type"] == "message_end" {
			m, _ := f["message"].(map[string]any)
			raw, _ := json.Marshal(m["content"])
			if m["role"] == "user" && bytes.Contains(raw, []byte(r.Token)) {
				userSeq = seq
			}
		}
		if seq > r.StartSeq && seq < r.SettledSeq && f["type"] == "tool_execution_end" && f["toolName"] == "write" && f["isError"] == false {
			toolSeq = seq
		}
		if f["command"] == "get_entries" && f["success"] == true {
			data, _ := f["data"].(map[string]any)
			list, _ := data["entries"].([]any)
			for _, item := range list {
				e := item.(map[string]any)
				entries[e["id"].(string)] = e
			}
		}
	}
	prompt, assistant := entries[r.PromptEntryID], entries[r.LastAssistantID]
	p, _ := prompt["message"].(map[string]any)
	a, _ := assistant["message"].(map[string]any)
	text, _ := json.Marshal(p["content"])
	if !settled || r.StartSeq >= userSeq || userSeq >= toolSeq || toolSeq >= r.SettledSeq || p["role"] != "user" || !bytes.Contains(text, []byte(r.Token)) || a["role"] != "assistant" || a["stopReason"] != "stop" || entries[r.LastEntryID] == nil {
		t.Fatalf("receipt lacks token/entries/write/settled evidence: receipt=%+v user=%d tool=%d settled=%t", r, userSeq, toolSeq, settled)
	}
	// The receipt's leaf must descend from this exact prompt, not merely exist.
	for id := r.LastEntryID; id != r.PromptEntryID; {
		e := entries[id]
		parent, ok := e["parentId"].(string)
		if !ok || parent == id {
			t.Fatalf("receipt lineage does not reach current prompt: %s", id)
		}
		id = parent
	}
	h.Save("receipt-"+r.Token+"-"+b.phase+".json", b.receipt)
}

func TestEngineBundledPublicationAndCancellation(t *testing.T) {
	for _, name := range []string{"snapshot-reuse-isolation", "cancel-candidate", "timeout-candidate", "cancel-staged", "cancel-confirmed"} {
		t.Run(name, func(t *testing.T) {
			blocked := make(chan struct{})
			h := bundled.New(t, bundled.Config{Respond: func(_ context.Context, request bundled.Request) bundled.Reply {
				var body struct {
					Messages []struct {
						Role    string          `json:"role"`
						Content json.RawMessage `json:"content"`
					} `json:"messages"`
				}
				if err := json.Unmarshal(request.Body, &body); err != nil || len(body.Messages) == 0 {
					t.Errorf("provider request: %v", err)
					return bundled.Reply{Status: 400, Text: "invalid messages"}
				}
				if body.Messages[len(body.Messages)-1].Role == "tool" {
					reply := bundled.Reply{Text: "candidate written"}
					if strings.HasSuffix(name, "-candidate") {
						reply.Barrier = blocked
					}
					return reply
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
				call, err := engBundledCall(message)
				if err != nil {
					t.Error(err)
					return bundled.Reply{Status: 400, Text: err.Error()}
				}
				raw, err := engPersistEnvelope(call.Request.Identity, call.Request.Prompt, nil)
				if err != nil {
					t.Error(err)
					return bundled.Reply{Status: 400, Text: err.Error()}
				}
				// Only Pi's actual write tool creates the candidate. The provider
				// returns tool arguments, never writes a candidate on its behalf.
				return bundled.Reply{ToolName: "write", ToolArguments: map[string]any{"path": call.CandidatePath, "content": string(raw)}}
			}})
			ro := bundledoptions.New(h)
			var run *Run
			ro.Observe = func(ctx context.Context, o runtime.Observation) error { return run.Observe(ctx, o) }
			pi, err := runtime.New(ro)
			if err != nil {
				t.Fatal(err)
			}
			boundary := &engBundledRuntime{Runtime: pi, barriers: make(chan engBundledBarrier, 1)}
			policy := DefaultRunPolicy()
			policy.Runtime, policy.RunTimeout, policy.AttemptTimeout = ro.Policy, 45*time.Second, 15*time.Second
			if name == "timeout-candidate" {
				policy.AttemptTimeout = 5 * time.Second
			}
			count := 1
			if name == "snapshot-reuse-isolation" {
				count = 3
			}
			var steps []StepResult
			definition := Definition{Name: "bundled-engine", Version: "1", Policy: policy, Execute: func(ctx context.Context, r *Run, input Input) (Result, error) {
				var handle *SessionHandle
				for i := 0; i < count; i++ {
					if i != 1 {
						var err error
						handle, err = r.OpenSession(ctx, RoleSpec{Name: fmt.Sprintf("worker-%d", i), Model: runtime.ModelSpec{Provider: bundled.Provider, ID: bundled.Model, Thinking: "off"}})
						if err != nil {
							return Result{}, err
						}
					}
					step, err := r.Root().Step(ctx, StepSpec{Key: fmt.Sprintf("step-%d", i), Session: handle, Prompt: fmt.Sprintf("%s/%d", input.Prompt, i), Output: contract.Spec{SchemaID: engTestSchema}})
					steps = append(steps, step)
					if err != nil {
						return Result{}, err
					}
					data, err := Decode[engTestData](ctx, r, step.Output)
					if err != nil || data.Value != fmt.Sprintf("%s/%d", input.Prompt, i) {
						return Result{}, fmt.Errorf("committed Decode changed staged data: %+v %v", data, err)
					}
					if err := r.Root().Decision(ctx, fmt.Sprintf("decoded-%d", i), "decoded committed snapshot", []contract.Ref{step.Output}); err != nil {
						return Result{}, err
					}
				}
				return engTestResult(steps[len(steps)-1]), nil
			}}
			run, err = New(h.Ctx, definition, Input{Prompt: `原始 "quoted" $(touch forbidden) ; | &`, LaunchCWD: filepath.Join(h.Root, "work")}, Options{BaseDir: h.Root, Schemas: engTestSchemas(t), Runtime: boundary, PiVersion: "0.84.3"})
			if err != nil {
				t.Fatal(err)
			}
			var renamed []contract.Ref
			run.afterPublish = func(ref contract.Ref) {
				// The real rename has happened, but only the following durable
				// AttemptSucceeded commit may authorize Decode or final Result.
				renamed = append(renamed, ref)
				if _, err := Decode[engTestData](h.Ctx, run, ref); !engTestCode(err, ReferenceInvalid) {
					t.Errorf("rename-only Ref was consumable before journal commit: %v", err)
				}
			}
			run.beforeIO = func(path, phase string) {
				if path != "events.jsonl" || phase != "RunFinalizing" || name != "snapshot-reuse-isolation" {
					return
				}
				var result struct {
					Outputs map[string]contract.Ref `json:"outputs"`
				}
				if err := engTestReadJSON(filepath.Join(run.Dir(), "result.json"), &result); err != nil || len(result.Outputs) != 1 || result.Outputs["output"] != steps[len(steps)-1].Output {
					t.Errorf("success finalization preceded validated Result persistence: %+v %v", result, err)
				}
			}
			done := engTestExecuteAsync(t, run)
			next := func(phase string) engBundledBarrier {
				t.Helper()
				select {
				case b := <-boundary.barriers:
					if b.phase != phase {
						t.Fatalf("barrier=%s, want %s", b.phase, phase)
					}
					return b
				case report := <-done:
					t.Fatalf("engine ended before %s: %+v", phase, report)
				case <-h.Ctx.Done():
					t.Fatalf("waiting for %s: %v", phase, h.Ctx.Err())
				}
				return engBundledBarrier{}
			}
			var calls []engTestCall
			var receipts []runtime.Execution
			if strings.HasSuffix(name, "-candidate") {
				h.NextRequest()
				request := h.NextRequest()
				var body map[string]any
				if err := json.Unmarshal(request.Body, &body); err != nil {
					t.Fatal(err)
				}
				messages := body["messages"].([]any)
				content := messages[1].(map[string]any)["content"].([]any)
				message := content[0].(map[string]any)["text"].(string)
				call, err := engBundledCall(message)
				if err != nil {
					t.Fatal(err)
				}
				calls = append(calls, call)
				raw, err := os.ReadFile(call.CandidatePath)
				if err != nil || !json.Valid(raw) {
					t.Fatalf("actual tool did not write candidate: %s %v", raw, err)
				}
				h.WaitRecord(h.Ctx, 0, func(r bundled.Record) bool {
					return r.Direction == "out" && r.Frame["type"] == "tool_execution_end" && r.Frame["toolName"] == "write" && r.Frame["isError"] == false
				})
				attempt := run.Snapshot().Attempts[call.Request.Identity.AttemptID]
				if attempt.Execution != nil || attempt.Output != nil {
					t.Fatalf("unsettled candidate became success: %+v", attempt)
				}
				for _, record := range h.Records() {
					if record.Direction == "out" && record.Frame["type"] == "agent_settled" {
						t.Fatal("candidate barrier already settled")
					}
				}
				if _, err := os.Stat(filepath.Join(filepath.Dir(call.CandidatePath), "validation.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("Stage preceded the current receipt: %v", err)
				}
				if name == "cancel-candidate" {
					run.Cancel(OriginControllerUser)
				}
			} else {
				for i := 0; i < count; i++ {
					executed := next("executed")
					calls = append(calls, executed.call)
					receipts = append(receipts, executed.receipt)
					engBundledReceipt(t, h, executed)
					if _, err := os.Stat(filepath.Join(filepath.Dir(executed.call.CandidatePath), "validation.json")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("Stage ran before Execute returned: %v", err)
					}
					close(executed.release)
					staged := next("staged")
					if staged.receipt != executed.receipt {
						t.Fatal("Confirm did not receive this Execute receipt")
					}
					attempt := run.Snapshot().Attempts[staged.call.Request.Identity.AttemptID]
					if attempt.State != Validating || attempt.Execution == nil || *attempt.Execution != staged.receipt || attempt.DispatchAccepted != AcceptedYes || attempt.Output != nil {
						t.Fatalf("Stage barrier attempt: %+v", attempt)
					}
					var validation struct {
						Identity contract.Identity `json:"identity"`
						Valid    bool              `json:"valid"`
					}
					if err := engTestReadJSON(filepath.Join(filepath.Dir(staged.call.CandidatePath), "validation.json"), &validation); err != nil || !validation.Valid || validation.Identity != staged.call.Request.Identity {
						t.Fatalf("Stage proof: %+v %v", validation, err)
					}
					paths, err := filepath.Glob(filepath.Join(run.Dir(), ".staging", "*", "contract.json"))
					if err != nil || len(paths) != 1 {
						t.Fatalf("private snapshot: %v %v", paths, err)
					}
					original, err := os.ReadFile(staged.call.CandidatePath)
					if err != nil {
						t.Fatal(err)
					}
					frozen, err := os.ReadFile(paths[0])
					if err != nil || !bytes.Equal(original, frozen) {
						t.Fatalf("Stage bytes differ: %v", err)
					}
					if _, err := os.Stat(filepath.Join(filepath.Dir(staged.call.CandidatePath), "published")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("published before Confirm: %v", err)
					}
					if name == "cancel-staged" {
						run.Cancel(OriginControllerUser)
						close(staged.release)
						break
					}
					changed, err := engPersistEnvelope(staged.call.Request.Identity, "changed after Stage", nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(staged.call.CandidatePath, changed, 0600); err != nil {
						t.Fatal(err)
					}
					close(staged.release)
					confirmed := next("confirmed")
					if confirmed.receipt != executed.receipt || confirmed.confirmation.ActivityEpoch != confirmed.receipt.ActivityEpoch || confirmed.confirmation.Seq < confirmed.receipt.SettledSeq {
						t.Fatalf("real Confirm mismatch: %+v", confirmed)
					}
					h.Save("confirmation-"+confirmed.receipt.Token+".json", confirmed.confirmation)
					if _, err := os.Stat(filepath.Join(filepath.Dir(staged.call.CandidatePath), "published")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("published before Confirm returned: %v", err)
					}
					if name == "cancel-confirmed" {
						run.Cancel(OriginControllerUser)
					}
					close(confirmed.release)
				}
			}
			var report Report
			select {
			case report = <-done:
			case <-h.Ctx.Done():
				t.Fatal("engine did not finish")
			}
			h.Save("engine-report.json", report)
			wantState, wantExit := Succeeded, 0
			if strings.HasPrefix(name, "cancel-") {
				wantState, wantExit = CancelledState, 130
				engDeadlineFailure(t, report.Failure, Cancelled, OriginControllerUser)
			}
			if name == "timeout-candidate" {
				wantState, wantExit = TimedOutState, 1
				engDeadlineFailure(t, report.Failure, TimedOut, OriginAttemptDeadline)
			}
			engTestReport(t, report, wantState, wantExit)
			engTestPersisted(t, run, report)
			if len(report.Snapshot.Attempts) != count || len(steps) != count {
				t.Fatalf("unexpected attempt count: %+v", report.Snapshot.Attempts)
			}
			staging, err := os.ReadDir(filepath.Join(run.Dir(), ".staging"))
			if err != nil || len(staging) != 0 {
				t.Fatalf("staging leaked: %v %v", staging, err)
			}
			for i, call := range calls {
				attempt := report.Snapshot.Attempts[call.Request.Identity.AttemptID]
				if attempt.State != wantState || attempt.Identity != call.Request.Identity || attempt.DispatchAccepted != AcceptedYes {
					t.Fatalf("terminal attempt: %+v", attempt)
				}
				if wantState != Succeeded {
					if (attempt.Execution == nil) != strings.HasSuffix(name, "-candidate") {
						t.Fatalf("cancellation lost its exact receipt boundary: %+v", attempt)
					}
					if steps[i].Output != (contract.Ref{}) || attempt.Output != nil || len(report.Result.Outputs) != 0 || len(run.publications) != 0 {
						t.Fatal("cancelled/expired candidate escaped as committed output")
					}
					if _, err := os.Stat(filepath.Join(filepath.Dir(call.CandidatePath), "published")); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("cancelled candidate published: %v", err)
					}
				} else {
					published, err := os.ReadFile(steps[i].Output.Path)
					original, _ := engPersistEnvelope(call.Request.Identity, call.Request.Prompt, nil)
					if err != nil || !bytes.Equal(published, original) {
						t.Fatalf("mutable candidate published instead of staged bytes: %s %v", published, err)
					}
					p := run.publications[steps[i].AttemptID]
					if p.Ref != steps[i].Output || p.Identity != call.Request.Identity || p.Seq != attempt.LastSeq {
						t.Fatalf("committed membership differs from terminal journal: %+v", p)
					}
				}
			}
			if count == 3 {
				if receipts[0].SessionID != receipts[1].SessionID || receipts[1].StartSeq <= receipts[0].SettledSeq || receipts[0].PromptEntryID == receipts[1].PromptEntryID || receipts[0].LastAssistantID == receipts[1].LastAssistantID || receipts[0].Token == receipts[1].Token || receipts[2].SessionID == receipts[0].SessionID {
					t.Fatalf("reuse/isolation receipts: %+v", receipts)
				}
				for _, index := range []int{3, 5} {
					var request bundled.Request
					if err := engTestReadJSON(filepath.Join(h.Artifacts, fmt.Sprintf("provider-%03d-request.json", index)), &request); err != nil {
						t.Fatal(err)
					}
					for _, previous := range receipts[:index/2] {
						if bytes.Contains(request.Body, []byte(previous.Token)) != (index == 3) {
							t.Fatalf("provider context isolation at request %d for token %s", index, previous.Token)
						}
					}
				}
			}
			var result struct {
				RunID   string                  `json:"run_id"`
				Outputs map[string]contract.Ref `json:"outputs"`
			}
			if err := engTestReadJSON(filepath.Join(run.Dir(), "result.json"), &result); err != nil || result.RunID != run.ID() || !reflect.DeepEqual(result.Outputs, report.Result.Outputs) {
				t.Fatalf("persisted Result mismatch: %+v %v", result, err)
			}
			settledSeq, successSeq := map[string]uint64{}, map[string]uint64{}
			for _, e := range engTestEvents(t, run) {
				raw, _ := json.Marshal(e.Details)
				switch e.Kind {
				case "AttemptSettled":
					var receipt runtime.Execution
					if err := json.Unmarshal(raw, &receipt); err != nil {
						t.Fatal(err)
					}
					settledSeq[receipt.Token] = e.Seq
				case "AttemptSucceeded":
					var d struct {
						Attempt AttemptState `json:"attempt"`
						Ref     contract.Ref `json:"ref"`
					}
					if err := json.Unmarshal(raw, &d); err != nil {
						t.Fatal(err)
					}
					if settledSeq[d.Attempt.Identity.DispatchToken] == 0 || settledSeq[d.Attempt.Identity.DispatchToken] >= e.Seq || run.publications[d.Ref.AttemptID].Seq != e.Seq {
						t.Fatalf("publication/journal order: %+v", e)
					}
					successSeq[d.Ref.AttemptID] = e.Seq
				case "Decision":
					var d struct {
						Refs []contract.Ref `json:"refs"`
					}
					if err := json.Unmarshal(raw, &d); err != nil {
						t.Fatal(err)
					}
					if len(d.Refs) != 1 || successSeq[d.Refs[0].AttemptID] == 0 || successSeq[d.Refs[0].AttemptID] >= e.Seq {
						t.Fatalf("Decode/Decision preceded committed publication: %+v", e)
					}
				}
			}
			if len(successSeq) != len(run.publications) || len(renamed) != len(successSeq) {
				t.Fatal("journal/committed membership differ")
			}
			wantSessions := 1
			if count == 3 {
				wantSessions = 2
			}
			if len(report.Cleanup) != wantSessions {
				t.Fatalf("cleanup count: %+v", report.Cleanup)
			}
			for _, cleanup := range report.Cleanup {
				if !cleanup.WaitCompleted || !cleanup.ProcessExited || !cleanup.AbortAcknowledged || !cleanup.AbortBashAcknowledged || len(cleanup.Unconfirmed) != 0 || len(cleanup.DiscoveryRemoved) != 1 || cleanup.WaitError != "" || cleanup.KillError != "" || cleanup.DiscoveryError != "" {
					t.Fatalf("owned process cleanup: %+v", cleanup)
				}
				history, err := os.ReadFile(cleanup.Identity.SessionFile)
				if err != nil || len(history) == 0 {
					t.Fatalf("history not retained: %v", err)
				}
				for _, call := range calls {
					if report.Snapshot.Attempts[call.Request.Identity.AttemptID].HandleID == cleanup.Identity.HandleID && !bytes.Contains(history, []byte(call.Dispatch.Token)) {
						t.Fatalf("history lost current token %s", call.Dispatch.Token)
					}
				}
				if report.Snapshot.Sessions[cleanup.Identity.HandleID].State != "Closed" {
					t.Fatal("snapshot session not closed")
				}
				if _, err := os.Stat(cleanup.DiscoveryRemoved[0]); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("owned discovery retained: %v", err)
				}
			}
		})
	}
}
