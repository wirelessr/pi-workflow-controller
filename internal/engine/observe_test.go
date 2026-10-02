package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

func engObserveEntry(id string, calls ...string) json.RawMessage {
	content := []any{}
	for _, args := range calls {
		content = append(content, map[string]any{"type": "toolCall", "id": "call-" + id, "name": "bash", "arguments": json.RawMessage(args)})
	}
	raw, _ := json.Marshal(map[string]any{"id": id, "parentId": nil, "type": "message", "message": map[string]any{"role": "assistant", "content": content}})
	return raw
}

func engObserveLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var sources []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		var line struct {
			Source string          `json:"source"`
			Entry  json.RawMessage `json:"entry"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil || len(line.Entry) == 0 {
			t.Fatalf("observation line %q: %v", scanner.Text(), err)
		}
		var e struct{ ID string }
		_ = json.Unmarshal(line.Entry, &e)
		sources = append(sources, e.ID+":"+line.Source)
	}
	return sources
}

func TestEngineStepObservation(t *testing.T) {
	long := `{"command":"` + strings.Repeat("é", argumentBytes) + `"}`
	for _, tc := range []struct {
		name    string
		observe bool
		fail    bool
	}{{"observed success", true, false}, {"observed failure", true, true}, {"not observed", false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &engTestRuntime{execute: func(_ context.Context, call engTestCall) engTestReply {
				reply := engTestReply{Data: engTestData{Value: call.Request.Prompt}, Entries: []runtime.EntryBatch{
					{Entries: []json.RawMessage{engObserveEntry("e1", `{"command":"ls"}`), engObserveEntry("e2", long)}, Source: runtime.EntriesVerified},
					{Entries: []json.RawMessage{engObserveEntry("e2", long), engObserveEntry("e3")}, Source: runtime.EntriesSessionFile, TailUncertain: tc.fail},
				}}
				if tc.fail {
					reply.Err = &Failure{Code: TimedOut, Origin: OriginAttemptDeadline, Message: "fixture deadline"}
				}
				return reply
			}}
			var step StepResult
			var stepErr error
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				step, stepErr = run.Root().Step(ctx, StepSpec{Key: "observed", Session: h, Prompt: "observed", Output: contract.Spec{SchemaID: engTestSchema}, Observe: tc.observe})
				return Result{}, nil
			})
			report := r.Execute()
			if report.Outcome != Succeeded || (stepErr != nil) != tc.fail {
				t.Fatalf("outcome = %s step error = %v", report.Outcome, stepErr)
			}
			if !tc.observe {
				if step.Observation != nil {
					t.Fatal("unobserved Step returned an observation")
				}
				return
			}
			obs := step.Observation
			if obs == nil || obs.Entries != 3 || !obs.Complete || !obs.Unverified || obs.TailUncertain != tc.fail || obs.Error != "" {
				t.Fatalf("observation = %+v", obs)
			}
			if len(obs.Calls) != 2 || obs.Calls[0] != (ToolCall{EntryID: "e1", Tool: "bash", Arguments: `{"command":"ls"}`}) || obs.Calls[1].EntryID != "e2" || !obs.Calls[1].Truncated || len(obs.Calls[1].Arguments) > argumentBytes || !strings.HasPrefix(long, obs.Calls[1].Arguments) {
				t.Fatalf("calls = %+v", obs.Calls)
			}
			if got := strings.Join(engObserveLines(t, obs.Path), " "); got != "e1:rpc e2:rpc e3:session-file" {
				t.Fatalf("observation file = %s", got)
			}
			if !strings.HasPrefix(obs.Path, r.Dir()) {
				t.Fatalf("observation outside the run: %s", obs.Path)
			}
		})
	}
}

func TestEngineEntrySinkLimits(t *testing.T) {
	r, _ := engTestNew(t, "", &engTestRuntime{}, func(context.Context, *Run, Input) (Result, error) { return Result{}, nil })
	t.Cleanup(func() { r.Execute() })
	sink := r.newEntrySink("handle", "attempt-calls")
	calls := make([]string, observationCalls+1)
	for i := range calls {
		calls[i] = fmt.Sprintf(`{"n":%d}`, i)
	}
	sink.Entries(runtime.EntryBatch{Entries: []json.RawMessage{engObserveEntry("many", calls...)}, Source: runtime.EntriesVerified})
	obs := sink.finish()
	if obs.Complete || len(obs.Calls) != observationCalls || !strings.Contains(obs.Error, "tool calls") || obs.Entries != 1 {
		t.Fatalf("call limit observation = complete %t calls %d error %q", obs.Complete, len(obs.Calls), obs.Error)
	}
	// A sink whose file cannot be created records nothing and says why.
	if err := os.WriteFile(r.Dir()+"/sessions/blocked", nil, 0600); err != nil {
		t.Fatal(err)
	}
	blocked := r.newEntrySink("blocked", "attempt")
	blocked.Entries(runtime.EntryBatch{Entries: []json.RawMessage{engObserveEntry("x")}, Source: runtime.EntriesVerified})
	if obs := blocked.finish(); obs.Complete || obs.Entries != 0 || obs.Error == "" {
		t.Fatalf("unwritable observation = %+v", obs)
	}
}
