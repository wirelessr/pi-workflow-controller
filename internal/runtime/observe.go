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
	// Close: from the dispatch offset, or the whole file when the offset
	// is 0.
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
// dispatch's, and the first of them must continue from baseline. From
// offset 0 it skips through the baseline entry, which Pi writes with the
// history before it.
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
		return fmt.Errorf("more than %d bytes to read", sessionFileLimit)
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	found, first := baseline == "" || offset > 0, offset > 0 && baseline != ""
	reader := bufio.NewReaderSize(io.LimitReader(f, info.Size()-offset), 64<<10)
	var batch []json.RawMessage
	unparsed := 0
	flush := func() {
		if len(batch) > 0 {
			deliver(batch)
			batch = nil
		}
	}
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 && line[len(line)-1] != '\n' {
			flush()
			if err == nil || err == io.EOF {
				// SIGKILL can cut the last write short.
				err = errors.New("the last line is incomplete, so the last entry may be missing")
			}
			return err
		}
		if len(line) > 1 {
			var e struct {
				ID       string          `json:"id"`
				Type     string          `json:"type"`
				ParentID json.RawMessage `json:"parentId"`
			}
			switch {
			case json.Unmarshal(line, &e) != nil || e.ID == "" || e.Type == "":
				if found {
					unparsed++
				}
			case e.ParentID == nil:
				// The session header is not an entry.
			case first:
				var parent string
				if json.Unmarshal(e.ParentID, &parent) != nil || parent != baseline {
					return errors.New("the entries written during the dispatch do not continue from its baseline")
				}
				first = false
				batch = append(batch, json.RawMessage(line[:len(line)-1]))
			case found:
				batch = append(batch, json.RawMessage(line[:len(line)-1]))
			default:
				found = e.ID == baseline
			}
		}
		if len(batch) == sessionFileBatch {
			flush()
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			flush()
			return err
		}
	}
	flush()
	switch {
	case !found:
		return errors.New("the dispatch baseline entry is not in the file")
	case unparsed > 0:
		return fmt.Errorf("%d lines after the dispatch baseline could not be parsed", unparsed)
	}
	return nil
}
