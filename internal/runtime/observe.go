package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"syscall"
)

// sessionFileLimit bounds the post-close read of Pi's session file. A larger
// file is not read; the batch reports an uncertain tail instead.
const sessionFileLimit = 64 << 20

// drainEntries is one best-effort audit read after a failure: an independent
// short deadline, no lineage checks, and any error only ends the read. It runs
// before Close so entries recorded up to the failure are not lost when the
// process is killed.
func (s *session) drainEntries(t *dispatchTrace) {
	if t == nil || t.sink == nil {
		return
	}
	s.mu.Lock()
	cursor := t.cursor
	s.mu.Unlock()
	fields := map[string]any{}
	if cursor != "" {
		fields["since"] = cursor
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.options.Policy.AbortGrace)
	defer cancel()
	r, err := s.request(ctx, "get_entries", fields, s.options.Policy.AbortGrace, true)
	if err != nil || r.frame.Success == nil || !*r.frame.Success {
		return
	}
	var data struct {
		Entries []json.RawMessage `json:"entries"`
	}
	if json.Unmarshal(r.frame.Data, &data) != nil || len(data.Entries) == 0 {
		return
	}
	t.sink.Entries(EntryBatch{Entries: data.Entries, Source: EntriesUnverified})
}

// readSessionFile recovers entries written after the drain, including those
// after abort, once Close has confirmed the process exited and the file can
// no longer grow. Only entries after this dispatch's baseline are returned.
func (s *session) readSessionFile(t *dispatchTrace, report CleanupReport) {
	if t == nil || t.sink == nil {
		return
	}
	if !report.ConfirmsLocalClose(s.id.SessionID) {
		t.sink.Entries(EntryBatch{Source: EntriesSessionFile, TailUncertain: true})
		return
	}
	entries, err := sessionFileEntries(s.id.SessionFile, t.baselineLeaf)
	t.sink.Entries(EntryBatch{Entries: entries, Source: EntriesSessionFile, TailUncertain: err != nil})
}

func sessionFileEntries(path, baseline string) ([]json.RawMessage, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > sessionFileLimit {
		return nil, errors.New("session file is not a regular file within the read limit")
	}
	raw, err := io.ReadAll(io.LimitReader(f, sessionFileLimit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > sessionFileLimit {
		return nil, errors.New("session file grew past the read limit")
	}
	var entries []json.RawMessage
	found := baseline == ""
	for _, line := range bytes.Split(raw, []byte("\n")) {
		var e struct {
			ID       string          `json:"id"`
			Type     string          `json:"type"`
			ParentID json.RawMessage `json:"parentId"`
		}
		// The session header and any partial last line are not entries.
		if json.Unmarshal(line, &e) != nil || e.ID == "" || e.Type == "" || e.ParentID == nil {
			continue
		}
		if !found {
			found = e.ID == baseline
			continue
		}
		entries = append(entries, json.RawMessage(bytes.Clone(line)))
	}
	if !found {
		return nil, errors.New("dispatch baseline entry not found in the session file")
	}
	return entries, nil
}
