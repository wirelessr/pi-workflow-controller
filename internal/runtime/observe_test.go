package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
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
		uncertain = uncertain || b.Err != nil
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
	f.s.drainEntries(&dispatchTrace{cursor: r.receipt.LastEntryID, sink: drained, sent: true})
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
	// An unacknowledged abort still ends in SIGKILL and a confirmed Wait, so
	// the session file is read in both modes.
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
			f.send(control{Type: "unmatched-entries", Count: 1, Message: toolCallMessage()})
			// Written after the last read the runtime can make, then found in
			// the session file once the process exit is confirmed.
			f.send(control{Type: "write-history", Entry: map[string]any{"id": "late", "parentId": "e2", "type": "message", "message": assistant("aborted")}})
			cancel(&Failure{Code: Cancelled, Origin: ControllerUser, Message: "attempt deadline"})
			_ = requireCode(t, f.resultAfterCancel(ch), Cancelled)
			order, from, uncertain := sink.sources(t)
			if uncertain || len(order) != 3 || order[2] != "late" || from["late"][0] != EntriesSessionFile {
				t.Fatalf("entries = %v sources = %v uncertain=%t, want prompt, tool call, then the late entry from the session file", order, from, uncertain)
			}
		})
	}
}

func TestEntrySinkSecondDispatchReadsOnlyItsSessionFileTail(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	_, first := f.execute()
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	if r := f.result(first); r.err != nil {
		t.Fatal(r.err)
	}
	f.send(control{Type: "write-history"})
	ch := make(chan executionResult, 1)
	sink := &recordingSink{}
	ctx, cancel := context.WithCancelCause(f.ctx)
	d := Dispatch{Token: randomID(), Entries: sink}
	d.Message = "Controller dispatch " + d.Token
	go func() { r, e := f.s.Execute(ctx, d); ch <- executionResult{r, e} }()
	f.next("prompt")
	f.send(control{Type: "write-history", Entry: map[string]any{"id": "late", "parentId": "e3", "type": "message", "message": assistant("aborted")}})
	cancel(&Failure{Code: Cancelled, Origin: ControllerUser, Message: "attempt deadline"})
	_ = requireCode(t, f.resultAfterCancel(ch), Cancelled)
	order, from, uncertain := sink.sources(t)
	if uncertain || len(order) != 2 || order[1] != "late" {
		t.Fatalf("entries = %v sources = %v, want only this dispatch's prompt and the late entry", order, from)
	}
	for _, id := range []string{"e1", "e2"} {
		if from[id] != nil {
			t.Fatalf("an earlier dispatch's entry %s leaked into this observation: %v", id, from)
		}
	}
}

func TestEntrySinkNothingBeforePrompt(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	// A malformed baseline fails the dispatch before any prompt is sent.
	f.send(control{Type: "entries-response", State: map[string]any{"entries": []any{map[string]any{"id": "x", "parentId": "missing", "type": "message", "message": map[string]any{"role": "user", "content": "foreign"}}}, "leafId": "x"}})
	f.send(control{Type: "write-history"})
	sink := &recordingSink{}
	d := Dispatch{Token: randomID(), Entries: sink}
	d.Message = "Controller dispatch " + d.Token
	if _, err := f.s.Execute(f.ctx, d); err == nil {
		t.Fatal("dispatch with a broken baseline succeeded")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.batches) != 0 {
		t.Fatalf("unsent dispatch observed %+v", sink.batches)
	}
}

func TestEntrySinkConfirmFailure(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	sink := &recordingSink{}
	d := Dispatch{Token: randomID(), Entries: sink}
	d.Message = "Controller dispatch " + d.Token
	ch := make(chan executionResult, 1)
	go func() { r, e := f.s.Execute(f.ctx, d); ch <- executionResult{r, e} }()
	f.next("prompt")
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "settle"})
	r := f.result(ch)
	if r.err != nil {
		t.Fatal(r.err)
	}
	// Activity after the receipt fails Confirm, which closes the session.
	f.send(control{Type: "message", Message: assistant("stop")})
	f.send(control{Type: "write-history"})
	if _, err := f.s.Confirm(f.ctx, r.receipt); err == nil {
		t.Fatal("Confirm accepted activity after the receipt")
	}
	order, from, uncertain := sink.sources(t)
	if uncertain || len(order) != 3 || from[order[2]][len(from[order[2]])-1] != EntriesSessionFile {
		t.Fatalf("entries = %v sources = %v uncertain=%t", order, from, uncertain)
	}
}

func TestSessionFileEntries(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/history.jsonl"
	lines := `{"type":"session","id":"s"}` + "\n" + `{"id":"a","parentId":null,"type":"message"}` + "\n" + `{"id":"b","parentId":"a","type":"message"}` + "\n" + `{"id":"c","parentId":"b","type":"message"}` + "\n"
	offsetB := int64(len(`{"type":"session","id":"s"}` + "\n" + `{"id":"a","parentId":null,"type":"message"}` + "\n"))
	for _, tc := range []struct {
		name     string
		text     string
		baseline string
		offset   int64
		want     string
		err      string
	}{
		{"from the start", lines, "", 0, "a b c", ""},
		{"after the baseline", lines, "a", 0, "b c", ""},
		{"baseline is the last entry", lines, "c", 0, "", ""},
		{"from the dispatch offset", lines, "a", offsetB, "b c", ""},
		{"baseline missing", lines, "missing", 0, "", "baseline"},
		{"file shorter than the offset", lines, "a", 1 << 20, "", "shorter"},
		{"incomplete last line", lines + `{"id":"d","parentId":"c"`, "", 0, "a b c", "incomplete"},
		{"unparseable line after the baseline", lines + "{broken\n" + `{"id":"d","parentId":"c","type":"message"}` + "\n", "a", 0, "b c d", "could not be parsed"},
		{"offset entries not continuing from the baseline", lines, "b", offsetB, "", "do not continue"},
		{"many entries in batches", manyEntries(300), "", 0, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			var ids []string
			var sizes []int
			err := sessionFileEntries(path, tc.baseline, tc.offset, func(batch []json.RawMessage) {
				sizes = append(sizes, len(batch))
				for _, raw := range batch {
					var e struct{ ID string }
					if json.Unmarshal(raw, &e) != nil {
						t.Fatalf("delivered a non-entry %q", raw)
					}
					ids = append(ids, e.ID)
				}
			})
			if tc.name == "many entries in batches" {
				if err != nil || fmt.Sprint(sizes) != "[256 44]" {
					t.Fatalf("batches %v err %v, want [256 44]", sizes, err)
				}
				return
			}
			if got := strings.Join(ids, " "); got != tc.want || (tc.err == "") != (err == nil) || err != nil && !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("ids %q err %v, want %q %q", got, err, tc.want, tc.err)
			}
		})
	}
	if err := sessionFileEntries(dir+"/absent.jsonl", "", 0, func([]json.RawMessage) {}); err == nil {
		t.Fatal("missing session file read as empty")
	}
	if err := os.Symlink(path, dir+"/link.jsonl"); err != nil {
		t.Fatal(err)
	}
	if err := sessionFileEntries(dir+"/link.jsonl", "", 0, func([]json.RawMessage) {}); err == nil {
		t.Fatal("symlinked session file was followed")
	}
}

func manyEntries(n int) string {
	var b strings.Builder
	parent := "null"
	for i := range n {
		fmt.Fprintf(&b, `{"id":"m%d","parentId":%s,"type":"message"}`+"\n", i, parent)
		parent = fmt.Sprintf(`"m%d"`, i)
	}
	return b.String()
}

// A provider failure keeps the session: entries are drained, not read from
// the file, and nothing is closed.
func TestEntrySinkKeptSessionFailure(t *testing.T) {
	f := mustFixture(t, "normal", nil)
	sink := &recordingSink{}
	d := Dispatch{Token: randomID(), Entries: sink}
	d.Message = "Controller dispatch " + d.Token
	ch := make(chan executionResult, 1)
	go func() { r, e := f.s.Execute(f.ctx, d); ch <- executionResult{r, e} }()
	f.next("prompt")
	f.send(control{Type: "message", Message: assistant("error")})
	f.send(control{Type: "settle"})
	_ = requireCode(t, f.result(ch).err, ProviderFailed)
	if _, err := f.s.Snapshot(f.ctx); err != nil {
		t.Fatalf("kept session is unusable: %v", err)
	}
	order, from, uncertain := sink.sources(t)
	if uncertain || len(order) != 2 {
		t.Fatalf("entries = %v sources = %v uncertain=%t", order, from, uncertain)
	}
	for _, sources := range from {
		for _, source := range sources {
			if source == EntriesSessionFile {
				t.Fatalf("kept session read its file: %v", from)
			}
		}
	}
}
