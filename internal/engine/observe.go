package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"pi-workflow-controller/internal/runtime"
)

// Observation is the audit record of one observed Step attempt: its raw
// session entries in a run-owned JSONL file, one {"source","entry"} object
// per line, and an index of the tool calls they contain (a structural
// projection; Go does not interpret the calls). It is recorded for audit
// only and never decides completion. Commit it with Attach when a
// downstream consumer needs an exact Ref.
type Observation struct {
	Path    string     `json:"path"`
	Entries int        `json:"entries"`
	Calls   []ToolCall `json:"calls"`
	// Gaps says why coverage may be incomplete: a limit or write error
	// stopped recording, or entries after a failure could not be recovered.
	// Empty means every entry the runtime delivered was recorded.
	Gaps []string `json:"gaps"`
}

type ToolCall struct {
	EntryID   string `json:"entry_id"`
	Tool      string `json:"tool"`
	Arguments string `json:"arguments"`
	Truncated bool   `json:"truncated,omitempty"`
}

const (
	observationBytes    = 32 << 20
	observationEntries  = 1 << 16
	observationCalls    = 4096
	argumentBytes       = 2 << 10
	runObservationBytes = 1 << 30
)

// entrySink records entries as the runtime delivers them. It never fails the
// Step: the first entry it cannot record stops recording and becomes a gap.
type entrySink struct {
	mu      sync.Mutex
	file    *os.File
	budget  *atomic.Int64
	written int64
	stopped bool
	seen    map[string]bool
	obs     Observation
}

func (r *Run) newEntrySink(handleID, attemptID string) *entrySink {
	rel := filepath.Join("sessions", handleID, "observations", attemptID+".jsonl")
	s := &entrySink{budget: &r.observedBytes, seen: map[string]bool{}, obs: Observation{Path: filepath.Join(r.Dir(), rel), Calls: []ToolCall{}, Gaps: []string{}}}
	err := r.fs.MkdirAll(filepath.Dir(rel), 0700)
	if err == nil {
		s.file, err = r.fs.OpenFile(rel, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0600)
	}
	if err != nil {
		s.stop(fmt.Sprintf("observation file could not be created: %v", err))
	}
	return s
}

func (s *entrySink) stop(gap string) {
	s.obs.Gaps = append(s.obs.Gaps, gap)
	s.stopped = true
	if s.file != nil {
		// Keep only whole lines: a failed write may have left part of one.
		if err := s.file.Truncate(s.written); err != nil {
			s.obs.Gaps = append(s.obs.Gaps, fmt.Sprintf("observation file may end with a partial line: %v", err))
		}
		_ = s.file.Close()
		s.file = nil
	}
}

func (s *entrySink) Entries(batch runtime.EntryBatch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if batch.Err != nil {
		s.obs.Gaps = append(s.obs.Gaps, batch.Err.Error())
	}
	for _, raw := range batch.Entries {
		if s.stopped {
			return
		}
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
			s.stop(fmt.Sprintf("more than %d entries; later entries were not recorded", observationEntries))
			return
		}
		line, err := json.Marshal(struct {
			Source runtime.EntrySource `json:"source"`
			Entry  json.RawMessage     `json:"entry"`
		}{batch.Source, raw})
		if err != nil {
			s.stop(fmt.Sprintf("entry %q could not be encoded: %v", e.ID, err))
			return
		}
		line = append(line, '\n')
		size := int64(len(line))
		if s.written+size > observationBytes {
			s.stop(fmt.Sprintf("the %d-byte observation file limit was reached; later entries were not recorded", observationBytes))
			return
		}
		if s.budget.Add(size) > runObservationBytes {
			s.budget.Add(-size)
			s.stop(fmt.Sprintf("the run's %d-byte observation budget was used up; later entries were not recorded", runObservationBytes))
			return
		}
		if _, err := s.file.Write(line); err != nil {
			s.budget.Add(-size)
			s.stop(fmt.Sprintf("observation file write failed: %v", err))
			return
		}
		s.written += size
		s.obs.Entries++
		if e.ID != "" {
			s.seen[e.ID] = true
		}
		if e.Type != "message" || e.Message.Role != "assistant" {
			continue
		}
		for _, block := range e.Message.Content {
			if block.Type != "toolCall" {
				continue
			}
			if len(s.obs.Calls) >= observationCalls {
				s.stop(fmt.Sprintf("more than %d tool calls; later entries were not recorded", observationCalls))
				return
			}
			args, truncated := block.Arguments, false
			if len(args) > argumentBytes {
				cut := argumentBytes
				for cut > 0 && !utf8.RuneStart(args[cut]) {
					cut--
				}
				args, truncated = args[:cut], true
			}
			s.obs.Calls = append(s.obs.Calls, ToolCall{EntryID: e.ID, Tool: block.Name, Arguments: string(args), Truncated: truncated})
		}
	}
}

func (s *entrySink) finish() *Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		if err := s.file.Close(); err != nil {
			s.obs.Gaps = append(s.obs.Gaps, fmt.Sprintf("observation file could not be closed: %v", err))
		}
		s.file = nil
	}
	s.stopped = true
	obs := s.obs
	obs.Calls = append([]ToolCall{}, s.obs.Calls...)
	obs.Gaps = append([]string{}, s.obs.Gaps...)
	return &obs
}
