package runtime

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"testing"
)

type recordingSink struct {
	mu      sync.Mutex
	batches []EntryBatch
}

func (r *recordingSink) Entries(b EntryBatch) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b.Entries = append([]json.RawMessage(nil), b.Entries...)
	r.batches = append(r.batches, b)
}

// sources maps each delivered entry id to the sources it arrived from, in
// first-delivery order.
func (r *recordingSink) sources(t *testing.T) ([]string, map[string][]EntrySource, bool) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	var order []string
	from := map[string][]EntrySource{}
	uncertain := false
	for _, b := range r.batches {
		uncertain = uncertain || b.TailUncertain
		for _, raw := range b.Entries {
			var e struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(raw, &e); err != nil || e.ID == "" {
				t.Fatalf("sink received a non-entry: %s", raw)
			}
			if from[e.ID] == nil {
				order = append(order, e.ID)
			}
			from[e.ID] = append(from[e.ID], b.Source)
		}
	}
	return order, from, uncertain
}

func toolCallMessage() map[string]any {
	return map[string]any{"role": "assistant", "provider": "fixture", "model": "model", "stopReason": "toolUse", "timestamp": 2,
		"content": []any{map[string]any{"type": "toolCall", "id": "call-1", "name": "bash", "arguments": map[string]any{"command": "echo audit"}}}}
}

func TestEntrySinkReceivesVerifiedEntries(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	sink := &recordingSink{}
	d := Dispatch{Token: randomID(), Entries: sink}
	d.Message = "Controller dispatch " + d.Token
	ch := make(chan executionResult, 1)
	go func() { r, e := f.s.Execute(f.ctx, d); ch <- executionResult{r, e} }()
	f.next("prompt")
	f.send(control{Type: "message", Message: toolCallMessage()})
	f.send(control{Type: "message", Message: map[string]any{"role": "toolResult", "toolCallId": "call-1", "content": []any{map[string]any{"type": "text", "text": "audit"}}, "timestamp": 3}})
	f.send(control{Type: "message", Message: assistant("stop")})
	f.event("agent_end", nil)
	f.send(control{Type: "settle"})
	r := f.result(ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if _, err := f.s.Confirm(f.ctx, r.receipt); err != nil {
		t.Fatal(err)
	}
	order, from, uncertain := sink.sources(t)
	if len(order) != 4 || uncertain {
		t.Fatalf("entries = %v uncertain=%t, want prompt, tool call, tool result and final answer", order, uncertain)
	}
	for _, id := range order {
		if len(from[id]) != 1 || from[id][0] != EntriesVerified {
			t.Fatalf("entry %s sources = %v, want exactly one verified delivery", id, from[id])
		}
	}
	if order[0] != r.receipt.PromptEntryID || order[3] != r.receipt.LastEntryID {
		t.Fatalf("entries %v do not span prompt %s to last %s", order, r.receipt.PromptEntryID, r.receipt.LastEntryID)
	}
	// The read after a failure starts at the trace cursor, without lineage checks.
	drained := &recordingSink{}
	f.send(control{Type: "entry", Entry: map[string]any{"type": "message", "message": map[string]any{"role": "user", "content": "queued input", "timestamp": 4}}})
	f.s.drainEntries(&dispatchTrace{cursor: r.receipt.LastEntryID, sink: drained})
	if got, from, _ := drained.sources(t); len(got) != 1 || from[got[0]][0] != EntriesUnverified {
		t.Fatalf("drained entries = %v %v, want the one entry after the cursor, unverified", got, from)
	}
	// A later dispatch on the same session delivers only its own entries.
	second := &recordingSink{}
	d2 := Dispatch{Token: randomID(), Entries: second}
	d2.Message = "Controller dispatch " + d2.Token
	go func() { r, e := f.s.Execute(f.ctx, d2); ch <- executionResult{r, e} }()
	f.next("prompt")
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(ch); r.err != nil {
		t.Fatal(r.err)
	}
	if again, _, _ := second.sources(t); len(again) != 2 || again[0] == order[0] {
		t.Fatalf("second dispatch entries = %v", again)
	}
}

func TestEntrySinkAfterCancellation(t *testing.T) {
	for _, mode := range []string{"normal", "no-abort-ack"} {
		t.Run(mode, func(t *testing.T) {
			f := mustFixture(t, mode, nil)
			sink := &recordingSink{}
			ctx, cancel := context.WithCancelCause(f.ctx)
			d := Dispatch{Token: randomID(), Entries: sink}
			d.Message = "Controller dispatch " + d.Token
			ch := make(chan executionResult, 1)
			go func() { r, e := f.s.Execute(ctx, d); ch <- executionResult{r, e} }()
			f.next("prompt")
			// An entry with no matching event is not read by the completion
			// loop; only the read after the failure can find it.
			f.send(control{Type: "unmatched-entries", Count: 1, Message: toolCallMessage()})
			// Written after the last read the runtime can make, then found in
			// the session file once the process exit is confirmed.
			f.send(control{Type: "write-history", Entry: map[string]any{"id": "late", "parentId": "e2", "type": "message", "message": assistant("aborted")}})
			cancel(&Failure{Code: Cancelled, Origin: ControllerUser, Message: "attempt deadline"})
			_ = requireCode(t, f.resultAfterCancel(ch), Cancelled)
			order, from, uncertain := sink.sources(t)
			if mode == "no-abort-ack" {
				if !uncertain || from["late"] != nil {
					t.Fatalf("unconfirmed exit must report an uncertain tail and skip the session file: %v %v", order, from)
				}
				return
			}
			if uncertain || len(order) != 3 || order[2] != "late" || from["late"][0] != EntriesSessionFile {
				t.Fatalf("entries = %v sources = %v uncertain=%t, want prompt, tool call, then the late entry from the session file", order, from, uncertain)
			}
			if got := from[order[1]][0]; got != EntriesVerified && got != EntriesUnverified {
				t.Fatalf("tool call entry source = %s", got)
			}
		})
	}
}

func TestSessionFileEntriesBaseline(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/history.jsonl"
	write := func(text string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"type":"session","id":"s"}` + "\n" + `{"id":"a","parentId":null,"type":"message"}` + "\n" + `{"id":"b","parentId":"a","type":"message"}` + "\n" + `{"id":"c","parentId":"b","type":"message"}` + "\n" + `{"id":"partial"`)
	for _, tc := range []struct {
		baseline string
		want     []string
		err      bool
	}{
		{"", []string{"a", "b", "c"}, false},
		{"a", []string{"b", "c"}, false},
		{"c", nil, false},
		{"missing", nil, true},
	} {
		entries, err := sessionFileEntries(path, tc.baseline)
		var ids []string
		for _, raw := range entries {
			var e struct{ ID string }
			_ = json.Unmarshal(raw, &e)
			ids = append(ids, e.ID)
		}
		if (err != nil) != tc.err || len(ids) != len(tc.want) {
			t.Fatalf("baseline %q: ids %v err %v, want %v", tc.baseline, ids, err, tc.want)
		}
		for i := range ids {
			if ids[i] != tc.want[i] {
				t.Fatalf("baseline %q: ids %v, want %v", tc.baseline, ids, tc.want)
			}
		}
	}
	if _, err := sessionFileEntries(dir+"/absent.jsonl", ""); err == nil {
		t.Fatal("missing session file read as empty")
	}
}
