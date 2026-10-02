package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"unicode/utf8"

	"pi-workflow-controller/internal/runtime"
)

// Observation is the audit record of one observed Step attempt: its raw
// session entries in a run-owned JSONL file, one {"source","entry"} object
// per line, and a bounded index of the tool calls they contain. It is
// recorded for audit only; it never decides completion. Commit it with
// Attach when a downstream consumer needs an exact Ref.
type Observation struct {
	Path    string     `json:"path"`
	Entries int        `json:"entries"`
	Calls   []ToolCall `json:"calls"`
	// Complete is false when an entry or a tool call was dropped by a limit
	// or a write error; the audit then lacks coverage.
	Complete bool `json:"complete"`
	// Unverified is true when some entries came from the reads after a
	// failure, which skip the completion lineage checks.
	Unverified bool `json:"unverified"`
	// TailUncertain is true when the last entries may be missing: process
	// exit was not confirmed or the session file could not be read.
	TailUncertain bool   `json:"tail_uncertain"`
	Error         string `json:"error,omitempty"`
}

type ToolCall struct {
	EntryID   string `json:"entry_id"`
	Tool      string `json:"tool"`
	Arguments string `json:"arguments"`
	Truncated bool   `json:"truncated,omitempty"`
}

const (
	observationBytes   = 32 << 20
	observationEntries = 1 << 16
	observationCalls   = 4096
	argumentBytes      = 2 << 10
)

// entrySink records entries as the runtime delivers them. It never fails the
// Step; problems only reduce coverage.
type entrySink struct {
	mu      sync.Mutex
	file    *os.File
	written int64
	seen    map[string]bool
	obs     Observation
}

func (r *Run) newEntrySink(handleID, attemptID string) *entrySink {
	rel := filepath.Join("sessions", handleID, "observations", attemptID+".jsonl")
	s := &entrySink{seen: map[string]bool{}, obs: Observation{Path: filepath.Join(r.Dir(), rel), Calls: []ToolCall{}, Complete: true}}
	err := r.fs.MkdirAll(filepath.Dir(rel), 0700)
	if err == nil {
		s.file, err = r.fs.OpenFile(rel, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0600)
	}
	if err != nil {
		s.fail(fmt.Errorf("open observation file: %w", err))
	}
	return s
}

func (s *entrySink) fail(err error) {
	s.obs.Complete = false
	if s.obs.Error == "" {
		s.obs.Error = err.Error()
	}
}

func (s *entrySink) Entries(batch runtime.EntryBatch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if batch.TailUncertain {
		s.obs.TailUncertain = true
	}
	for _, raw := range batch.Entries {
		var e struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Message struct {
				Role    string `json:"role"`
				Content []struct {
					Type      string          `json:"type"`
					Name      string          `json:"name"`
					Arguments json.RawMessage `json:"arguments"`
				} `json:"content"`
			} `json:"message"`
		}
		_ = json.Unmarshal(raw, &e)
		if e.ID != "" && s.seen[e.ID] {
			continue
		}
		if s.obs.Entries >= observationEntries {
			s.fail(fmt.Errorf("more than %d observed entries", observationEntries))
			return
		}
		if e.ID != "" {
			s.seen[e.ID] = true
		}
		line, err := json.Marshal(struct {
			Source runtime.EntrySource `json:"source"`
			Entry  json.RawMessage     `json:"entry"`
		}{batch.Source, raw})
		if err != nil {
			s.fail(fmt.Errorf("encode observed entry %q: %w", e.ID, err))
			continue
		}
		line = append(line, '\n')
		if s.file == nil || s.written+int64(len(line)) > observationBytes {
			s.fail(fmt.Errorf("observation file limit of %d bytes reached", observationBytes))
			continue
		}
		if _, err := s.file.Write(line); err != nil {
			s.fail(fmt.Errorf("write observation file: %w", err))
			continue
		}
		s.written += int64(len(line))
		s.obs.Entries++
		if batch.Source != runtime.EntriesVerified {
			s.obs.Unverified = true
		}
		if e.Type != "message" || e.Message.Role != "assistant" {
			continue
		}
		for _, block := range e.Message.Content {
			if block.Type != "toolCall" {
				continue
			}
			if len(s.obs.Calls) >= observationCalls {
				s.fail(fmt.Errorf("more than %d tool calls", observationCalls))
				break
			}
			args, truncated := string(block.Arguments), false
			if len(args) > argumentBytes {
				cut := argumentBytes
				for cut > 0 && !utf8.RuneStart(args[cut]) {
					cut--
				}
				args, truncated = args[:cut], true
			}
			s.obs.Calls = append(s.obs.Calls, ToolCall{EntryID: e.ID, Tool: block.Name, Arguments: args, Truncated: truncated})
		}
	}
}

func (s *entrySink) finish() *Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			s.fail(fmt.Errorf("close observation file: %w", err))
		}
		s.file = nil
	}
	obs := s.obs
	obs.Calls = append([]ToolCall{}, s.obs.Calls...)
	return &obs
}
