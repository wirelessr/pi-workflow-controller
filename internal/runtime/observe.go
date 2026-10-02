package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
)

const (
	// sessionFileLimit bounds how much of Pi's session file is read after
	// Close, counted from where this dispatch's entries begin.
	sessionFileLimit = 64 << 20
	// sessionFileBatch entries are handed to the sink at a time, so the
	// read never holds the whole file in memory.
	sessionFileBatch = 256
)

// drainEntries is one best-effort read on a failure that keeps the session
// alive: an independent short deadline, no lineage checks, and any error only
// ends the read. Paths that close the session read its file instead.
func (s *session) drainEntries(t *dispatchTrace) {
	if t == nil || t.sink == nil || !t.sent {
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
		if err == nil {
			err = errors.New("get_entries rejected")
		}
		t.sink.Entries(EntryBatch{Source: EntriesUnverified, Err: fmt.Errorf("entries after the failure could not be read: %w", err)})
		return
	}
	var data struct {
		Entries []json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal(r.frame.Data, &data); err != nil {
		t.sink.Entries(EntryBatch{Source: EntriesUnverified, Err: fmt.Errorf("entries after the failure were malformed: %w", err)})
		return
	}
	if len(data.Entries) > 0 {
		t.sink.Entries(EntryBatch{Entries: data.Entries, Source: EntriesUnverified})
	}
}

// readSessionFile recovers entries written after the last poll, including
// those after abort, once the process has exited and the file can no longer
// grow. Only entries after this dispatch's baseline are delivered.
func (s *session) readSessionFile(t *dispatchTrace, report CleanupReport) {
	if t == nil || t.sink == nil || !t.sent {
		return
	}
	if report.Identity.SessionID != s.id.SessionID || !report.WaitCompleted || !report.ProcessExited {
		t.sink.Entries(EntryBatch{Source: EntriesSessionFile, Err: errors.New("process exit was not confirmed, so the session file was not read and later entries may be missing")})
		return
	}
	if err := sessionFileEntries(s.id.SessionFile, t.baselineLeaf, t.fileOffset, func(batch []json.RawMessage) {
		t.sink.Entries(EntryBatch{Entries: batch, Source: EntriesSessionFile})
	}); err != nil {
		t.sink.Entries(EntryBatch{Source: EntriesSessionFile, Err: fmt.Errorf("session file: %w", err)})
	}
}

func sessionFileSize(path string) int64 {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return 0
	}
	return info.Size()
}

// sessionFileEntries streams the entries after baseline, starting at offset:
// Pi only appends, so bytes past the size seen at dispatch are this
// dispatch's. A file that has not reached offset was replaced.
func sessionFileEntries(path, baseline string, offset int64, deliver func([]json.RawMessage)) error {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("not a regular file")
	}
	if info.Size() < offset {
		return errors.New("shorter than when the dispatch started")
	}
	if info.Size()-offset > sessionFileLimit {
		return fmt.Errorf("more than %d bytes were written during the dispatch", sessionFileLimit)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	// Past a nonzero offset every entry is new; from the start, skip through
	// the baseline entry, which Pi writes with the history before it.
	found := baseline == "" || offset > 0
	reader := bufio.NewReaderSize(io.LimitReader(f, sessionFileLimit), 64<<10)
	var batch []json.RawMessage
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] != '\n' && err == io.EOF {
			if len(batch) > 0 {
				deliver(batch)
			}
			// SIGKILL can cut the last write short.
			return errors.New("the last line is incomplete, so the last entry may be missing")
		}
		if len(line) > 0 {
			var e struct {
				ID       string          `json:"id"`
				Type     string          `json:"type"`
				ParentID json.RawMessage `json:"parentId"`
			}
			// The session header is not an entry.
			if json.Unmarshal(line, &e) == nil && e.ID != "" && e.Type != "" && e.ParentID != nil {
				if found {
					batch = append(batch, json.RawMessage(line[:len(line)-1]))
				} else {
					found = e.ID == baseline
				}
			}
		}
		if len(batch) == sessionFileBatch || err != nil && len(batch) > 0 {
			deliver(batch)
			batch = nil
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if !found {
		return errors.New("the dispatch baseline entry is not in the file")
	}
	return nil
}
