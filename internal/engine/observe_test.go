package engine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

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
	scanner.Buffer(nil, 4<<20)
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
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
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
					{Entries: []json.RawMessage{engObserveEntry("e2", long), engObserveEntry("e3")}, Source: runtime.EntriesSessionFile},
				}}
				if tc.fail {
					reply.Entries = append(reply.Entries, runtime.EntryBatch{Source: runtime.EntriesSessionFile, Err: errors.New("process exit was not confirmed")})
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
			if report.Outcome != Succeeded || (stepErr != nil) != tc.fail || tc.fail && !engTestCode(stepErr, TimedOut) {
				t.Fatalf("outcome = %s step error = %v", report.Outcome, stepErr)
			}
			if !tc.observe {
				if step.Observation != nil {
					t.Fatal("unobserved Step returned an observation")
				}
				return
			}
			obs := step.Observation
			wantGaps := "[]"
			if tc.fail {
				wantGaps = "[process exit was not confirmed]"
			}
			if obs == nil || obs.Entries != 3 || fmt.Sprint(obs.Gaps) != wantGaps {
				t.Fatalf("observation = %+v", obs)
			}
			if len(obs.Calls) != 2 || obs.Calls[0] != (ToolCall{EntryID: "e1", Tool: "bash", Arguments: `{"command":"ls"}`}) || obs.Calls[1].EntryID != "e2" || !obs.Calls[1].Truncated || len(obs.Calls[1].Arguments) > argumentBytes || !strings.HasPrefix(long, obs.Calls[1].Arguments) || !utf8.ValidString(obs.Calls[1].Arguments) {
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
	entries := func(n int, size int) []json.RawMessage {
		out := make([]json.RawMessage, n)
		for i := range out {
			out[i], _ = json.Marshal(map[string]any{"id": fmt.Sprintf("n%d", i), "parentId": nil, "type": "message", "message": map[string]any{"role": "user", "content": strings.Repeat("x", size)}})
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		batch   []json.RawMessage
		budget  int64
		entries int
		gap     string
	}{
		{"tool calls", []json.RawMessage{engObserveEntry("many", strings.Split(strings.Repeat(`{"n":1}|`, observationCalls+1), "|")[:observationCalls+1]...)}, 0, 1, "tool calls"},
		{"entry count", entries(observationEntries+1, 0), 0, observationEntries, "entries"},
		{"file bytes", entries(observationBytes/(1<<20)+1, 1<<20), 0, -1, "observation file limit"},
		{"run budget", entries(2, 0), runObservationBytes, 0, "observation budget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r.observedBytes.Store(tc.budget)
			t.Cleanup(func() { r.observedBytes.Store(0) })
			sink := r.newEntrySink("handle", "attempt-"+strings.ReplaceAll(tc.name, " ", "-"))
			sink.Entries(runtime.EntryBatch{Entries: tc.batch, Source: runtime.EntriesVerified})
			sink.Entries(runtime.EntryBatch{Entries: []json.RawMessage{engObserveEntry("after-stop")}, Source: runtime.EntriesSessionFile})
			obs := sink.finish()
			if tc.entries < 0 {
				// As many whole lines as fit under the file limit.
				info, err := os.Stat(obs.Path)
				if err != nil || info.Size() > observationBytes || obs.Entries < observationBytes/(1<<20)-2 {
					t.Fatalf("file size %v entries %d: %v", info.Size(), obs.Entries, err)
				}
				tc.entries = obs.Entries
			}
			if obs.Entries != tc.entries || len(obs.Gaps) != 1 || !strings.Contains(obs.Gaps[0], tc.gap) {
				t.Fatalf("observation = entries %d gaps %q", obs.Entries, obs.Gaps)
			}
			if lines := engObserveLines(t, obs.Path); len(lines) != tc.entries {
				t.Fatalf("file lines = %d, want only whole recorded entries", len(lines))
			}
		})
	}
	// A sink whose file cannot be created records nothing and says why.
	if err := os.WriteFile(r.Dir()+"/sessions/blocked", nil, 0600); err != nil {
		t.Fatal(err)
	}
	blocked := r.newEntrySink("blocked", "attempt")
	blocked.Entries(runtime.EntryBatch{Entries: []json.RawMessage{engObserveEntry("x")}, Source: runtime.EntriesVerified})
	if obs := blocked.finish(); obs.Entries != 0 || len(obs.Gaps) != 1 || !strings.Contains(obs.Gaps[0], "could not be created") {
		t.Fatalf("unwritable observation = %+v", obs)
	}
}
