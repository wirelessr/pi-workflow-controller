package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type seenMessage struct {
	hash [32]byte
	seq  uint64
}
type entry struct {
	Type     string          `json:"type"`
	ID       string          `json:"id"`
	ParentID *string         `json:"parentId"`
	Message  json.RawMessage `json:"message"`
	Provider string          `json:"provider"`
	ModelID  string          `json:"modelId"`
	Thinking string          `json:"thinkingLevel"`
}

func (e *entry) UnmarshalJSON(raw []byte) error {
	type plain entry
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, (*plain)(e)) != nil || json.Unmarshal(raw, &fields) != nil ||
		e.ID == "" || e.Type == "" || fields["parentId"] == nil {
		return failure(ProtocolFailed, "entry requires id, type and parentId")
	}
	switch e.Type {
	case "message":
		_, err := parseMessage(e.Message)
		return err
	case "model_change":
		if e.Provider == "" || e.ModelID == "" {
			return failure(ProtocolFailed, "model entry requires provider and modelId")
		}
	case "thinking_level_change":
		if e.Thinking == "" {
			return failure(ProtocolFailed, "thinking entry requires thinkingLevel")
		}
	}
	return nil
}

type dispatchTrace struct {
	token                                             string
	start, tokenSeq, settled                          uint64
	lastAnySettled, previousUserSeq                   uint64
	tokenEvents, users                                int
	messages                                          []seenMessage
	entryMessages                                     [][32]byte
	matchedSeq                                        uint64
	mismatch                                          bool
	sticky                                            *Failure
	unresolved                                        Code
	unresolvedSeq, retryErrorSeq                      uint64
	cursor, baselineLeaf, leaf, prompt, lastAssistant string
	lastStop                                          string
	assistantAfterPrompt                              bool
	tokens                                            int
	changed                                           chan struct{}
}

// Matched hashes are no longer needed: append order and the latest event seq
// summarize the verified prefix without retaining lifetime message evidence.
func (t *dispatchTrace) matchMessages() {
	for len(t.messages) > 0 && len(t.entryMessages) > 0 {
		m := t.messages[0]
		t.mismatch = t.mismatch || m.hash != t.entryMessages[0]
		t.matchedSeq = m.seq
		t.messages[0] = seenMessage{}
		t.messages = t.messages[1:]
		t.entryMessages[0] = [32]byte{}
		t.entryMessages = t.entryMessages[1:]
	}
	if len(t.messages) == 0 {
		t.messages = nil
	}
	if len(t.entryMessages) == 0 {
		t.entryMessages = nil
	}
}

func messageHash(m message) [32]byte {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(m.raw))
	decoder.UseNumber()
	_ = decoder.Decode(&value)
	b, _ := json.Marshal(value)
	return sha256.Sum256(b)
}
func (s *session) eventLocked(f wireFrame) error {
	// Unknown extensions to the protocol are not evidence of dispatch activity.
	switch f.Type {
	case "agent_start", "agent_end", "agent_settled", "turn_start", "turn_end",
		"message_start", "message_end", "message_update",
		"compaction_start", "compaction_end", "auto_retry_start", "auto_retry_end",
		"tool_execution_start", "tool_execution_end", "tool_execution_update", "bash_execution_update",
		"queue_update", "thinking_level_changed", "extension_error", "extension_ui_request", "entry_appended", "session_info_changed":
	default:
		return nil
	}
	// Streaming updates affect epoch but do not enter the durable observation queue.
	s.epoch++
	if f.Type != "message_update" && f.Type != "tool_execution_update" && f.Type != "bash_execution_update" {
		s.emitLocked(f.Type)
	}
	t := s.trace
	sticky := func(code Code, text string, origin Origin) {
		if t != nil && (t.sticky == nil || (t.sticky.Code == ProviderFailed && code != ProviderFailed)) {
			prior := t.sticky
			t.sticky = failure(code, text)
			t.sticky.Origin = origin
			if prior != nil {
				t.sticky.Cause = prior
			}
		}
	}
	switch f.Type {
	case "agent_start":
		if t != nil && t.settled > 0 {
			sticky(AmbiguousExecution, "new run after dispatch settled", Protocol)
		}
		s.active = true
	case "agent_settled":
		s.active = false
		if t != nil {
			t.lastAnySettled = s.seq
		}
		if t != nil && t.tokenSeq > 0 {
			if t.settled > 0 {
				sticky(AmbiguousExecution, "multiple settled events after one dispatch", Protocol)
			} else {
				t.settled = s.seq
			}
		}
	case "compaction_start":
		s.compacting = true
	case "compaction_end":
		if f.Aborted == nil {
			return failure(ProtocolFailed, "compaction_end requires aborted")
		}
		s.compacting = false
		if *f.Aborted {
			sticky(Cancelled, "compaction aborted", ExternalAgentAbort)
		} else if f.ErrorMessage != "" {
			sticky(CompactionFailed, f.ErrorMessage, Compaction)
		} else if f.WillRetry && t != nil {
			t.unresolved = ""
		}
	case "auto_retry_start":
		s.retrying = true
		if t != nil && t.unresolved == ProviderFailed {
			t.retryErrorSeq = t.unresolvedSeq
		}
	case "auto_retry_end":
		if f.Success == nil {
			return failure(ProtocolFailed, "auto_retry_end requires success")
		}
		s.retrying = false
		if !*f.Success {
			if f.FinalError == "Retry cancelled" {
				sticky(Cancelled, "provider retry cancelled", ExternalAgentAbort)
			} else {
				sticky(ProviderFailed, f.FinalError, Provider)
			}
		} else if t != nil && t.unresolved == ProviderFailed && t.retryErrorSeq != 0 && t.unresolvedSeq == t.retryErrorSeq {
			t.unresolved = ""
		}
		if t != nil {
			t.retryErrorSeq = 0
		}
	case "tool_execution_start":
		if f.ToolCallID == "" {
			return failure(ProtocolFailed, "tool event requires toolCallId")
		}
		if len(s.tools) >= s.options.Policy.ObservationQueue {
			return failure(LimitExceeded, "active tool evidence limit exhausted")
		}
		s.tools[f.ToolCallID] = true
	case "tool_execution_end":
		if f.ToolCallID == "" {
			return failure(ProtocolFailed, "tool event requires toolCallId")
		}
		delete(s.tools, f.ToolCallID)
	case "queue_update":
		if f.Steering == nil || f.FollowUp == nil {
			return failure(ProtocolFailed, "queue_update requires steering and followUp arrays")
		}
		s.queue = len(*f.Steering) + len(*f.FollowUp)
	case "thinking_level_changed":
		if f.Level == "" {
			return failure(ProtocolFailed, "thinking change requires level")
		}
		if f.Level != s.spec.Model.Thinking {
			sticky(ThinkingChanged, "thinking level drift", Protocol)
		}
	case "extension_error":
		sticky(ExtensionFailed, f.Error, Protocol)
		if t == nil {
			s.failLocked(failure(ExtensionFailed, f.Error))
		}
	case "extension_ui_request":
		if f.ID == "" || f.Method == "" {
			return failure(ProtocolFailed, "UI request requires id and method")
		}
		switch f.Method {
		case "select", "confirm", "input", "editor":
			sticky(InteractionRequired, "native RPC dialog requires interaction", Protocol)
			// One bounded worker, not the stdout reader, writes the cancellation before Close.
			if s.fatal == nil {
				err := failure(InteractionRequired, "native RPC dialog requires interaction")
				s.dialogDone = make(chan struct{})
				s.failLocked(err)
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), s.options.Policy.AbortGrace)
					defer cancel()
					b, _ := json.Marshal(map[string]any{"type": "extension_ui_response", "id": f.ID, "cancelled": true})
					_ = s.write(ctx, append(b, '\n'), true)
					close(s.dialogDone)
				}()
			}
		}
	case "message_end":
		m, err := parseMessage(f.Message)
		if err != nil {
			return err
		}
		if t == nil {
			break
		}
		if m.Role == "assistant" {
			if m.Provider != s.spec.Model.Provider || m.Model != s.spec.Model.ID {
				sticky(ModelChanged, "assistant model drift", Protocol)
			}
			switch m.StopReason {
			case "aborted":
				sticky(Cancelled, "assistant aborted", ExternalAgentAbort)
			case "error":
				t.unresolved = ProviderFailed
				t.unresolvedSeq = s.seq
			case "length":
				t.unresolved = OutputTruncated
				t.unresolvedSeq = s.seq
			case "stop", "toolUse":
			default:
				return failure(ProtocolFailed, "invalid terminal stopReason")
			}
		}
		if m.Role == "user" || m.Role == "assistant" || m.Role == "toolResult" {
			if len(t.messages) >= s.options.Policy.ObservationQueue {
				return failure(LimitExceeded, "dispatch event evidence limit exhausted")
			}
			t.messages = append(t.messages, seenMessage{hash: messageHash(m), seq: s.seq})
			t.matchMessages()
		}
		if m.Role == "user" {
			t.users++
		}
		if m.Role == "user" && strings.Contains(messageText(m), t.token) {
			if t.previousUserSeq > t.lastAnySettled {
				sticky(AmbiguousExecution, "dispatch overlapped an unsettled foreign prompt", Protocol)
			}
			t.tokenEvents++
			if t.tokenEvents > 1 {
				sticky(AmbiguousExecution, "duplicate dispatch token event", Protocol)
			}
			t.tokenSeq = s.seq
		}
		if m.Role == "user" && t.tokenSeq == 0 {
			t.previousUserSeq = s.seq
		}
	}
	if t != nil && t.settled > 0 && s.seq > t.settled {
		switch f.Type {
		case "message_start", "message_end", "message_update", "turn_start", "turn_end", "agent_start", "agent_end", "compaction_start", "compaction_end", "auto_retry_start", "auto_retry_end", "tool_execution_start", "tool_execution_end", "queue_update":
			sticky(AmbiguousExecution, "new activity after dispatch settled", Protocol)
		}
	}
	if t != nil {
		switch f.Type {
		case "message_end", "agent_settled", "compaction_end", "auto_retry_end", "thinking_level_changed", "extension_error", "extension_ui_request", "entry_appended", "session_info_changed":
			select {
			case t.changed <- struct{}{}:
			default:
			}
		}
	}
	return nil
}
func (s *session) readEntries(ctx context.Context, t *dispatchTrace, baseline bool) error {
	fields := map[string]any{}
	s.mu.Lock()
	cursor := t.cursor
	s.mu.Unlock()
	if cursor != "" {
		fields["since"] = cursor
	}
	r, err := s.request(ctx, "get_entries", fields, s.options.Policy.RPCTimeout, false)
	if err != nil {
		return err
	}
	if !*r.frame.Success {
		if _, err = s.Snapshot(ctx); err != nil {
			return err
		}
		return failure(AmbiguousExecution, "session append cursor is no longer available")
	}
	var data struct {
		Entries []entry `json:"entries"`
		LeafID  *string `json:"leafId"`
	}
	var required map[string]json.RawMessage
	if json.Unmarshal(r.frame.Data, &data) != nil || json.Unmarshal(r.frame.Data, &required) != nil || required["entries"] == nil || bytes.Equal(required["entries"], []byte("null")) || required["leafId"] == nil {
		return failure(ProtocolFailed, "invalid get_entries response")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range data.Entries {
		parent := ""
		if e.ParentID != nil {
			parent = *e.ParentID
		}
		// Pi allocates stable unique IDs. Under the single-dispatcher contract,
		// every append extends this verified prefix; rewinds/forks fail closed.
		if parent != t.cursor || e.ID == t.cursor || e.ID == t.baselineLeaf || e.ID == t.prompt || e.ID == t.lastAssistant {
			return failure(AmbiguousExecution, "append does not extend verified lineage")
		}
		switch e.Type {
		case "model_change":
			if !baseline && (e.Provider != s.spec.Model.Provider || e.ModelID != s.spec.Model.ID) {
				return failure(ModelChanged, "model change entry drift")
			}
		case "thinking_level_change":
			if !baseline && e.Thinking != s.spec.Model.Thinking {
				return failure(ThinkingChanged, "thinking change entry drift")
			}
		case "message":
			m, err := parseMessage(e.Message)
			if err != nil {
				return err
			}
			if !baseline && (m.Role == "user" || m.Role == "assistant" || m.Role == "toolResult") {
				if len(t.entryMessages) >= s.options.Policy.ObservationQueue {
					return failure(LimitExceeded, "unmatched entry evidence limit exhausted")
				}
				t.entryMessages = append(t.entryMessages, messageHash(m))
				t.matchMessages()
			}
			if m.Role == "assistant" {
				if !baseline {
					t.lastAssistant = e.ID
					t.lastStop = m.StopReason
					t.assistantAfterPrompt = t.prompt != ""
				}
				if !baseline && (m.Provider != s.spec.Model.Provider || m.Model != s.spec.Model.ID) {
					return failure(ModelChanged, "assistant entry binding drift")
				}
			}
			if !baseline && m.Role == "user" && strings.Contains(messageText(m), t.token) {
				t.tokens++
				t.prompt = e.ID
				if t.tokens > 1 {
					return failure(AmbiguousExecution, "duplicate token entry")
				}
			}
		}
		t.cursor = e.ID
	}
	t.leaf = ""
	if data.LeafID != nil {
		t.leaf = *data.LeafID
	}
	if t.leaf != t.cursor {
		return failure(AmbiguousExecution, "active branch detached from verified append lineage")
	}
	if baseline {
		t.baselineLeaf = t.cursor
	}
	if !t.mismatch && len(t.messages) == 0 && len(t.entryMessages) == 0 {
		s.entryCursor = t.cursor
	}
	return nil
}
func (s *session) Execute(ctx context.Context, d Dispatch) (receipt Execution, err error) {
	select {
	case <-s.executeLease:
	default:
		return receipt, failure(SessionBusy, "another Execute holds the session")
	}
	defer func() { s.executeLease <- struct{}{} }()
	accepted := AcceptedNo
	defer func() {
		if err == nil {
			return
		}
		var f *Failure
		if errors.As(err, &f) {
			copy := *f
			if cause := context.Cause(ctx); cause != nil && errors.Is(err, cause) {
				copy.Cause = err
			}
			copy.DispatchAccepted = accepted
			copy.HandleID = s.spec.HandleID
			err = &copy
		}
		keep := false
		if errors.As(err, &f) {
			keep = f.Code == DispatchRejected || f.Code == ProviderFailed || f.Code == OutputTruncated || (f.Code == InvalidDefinition && accepted == AcceptedNo)
		}
		if !keep {
			s.invalidate(err)
			report, _ := s.Close(context.Background())
			if errors.As(err, &f) {
				f.StderrTail = report.StderrTail
				f.Cleanup = &report
			}
		}
	}()
	if len(d.Token) < 32 || !strings.Contains(d.Message, d.Token) || strings.HasPrefix(d.Message, "/") {
		return receipt, failure(InvalidDefinition, "dispatch requires a nonce-bearing non-command envelope")
	}
	s.mu.Lock()
	if s.usedTokens[d.Token] {
		s.mu.Unlock()
		return receipt, failure(InvalidDefinition, "dispatch token cannot be reused")
	}
	if len(s.usedTokens) >= s.options.Policy.ObservationQueue {
		s.mu.Unlock()
		return receipt, failure(LimitExceeded, "session dispatch token limit exhausted")
	}
	s.usedTokens[d.Token] = true
	s.receipt = nil
	s.trace = nil
	s.mu.Unlock()
	for {
		state, e := s.Snapshot(ctx)
		if e != nil {
			return receipt, e
		}
		s.mu.Lock()
		idle := !s.active && !s.compacting && !s.retrying && len(s.tools) == 0 && s.queue == 0 && !state.Streaming && !state.Compacting && state.PendingCount == 0
		s.mu.Unlock()
		if idle {
			break
		}
		if e = s.pause(ctx, 50*time.Millisecond); e != nil {
			return receipt, e
		}
	}
	s.mu.Lock()
	t := &dispatchTrace{token: d.Token, cursor: s.entryCursor, baselineLeaf: s.entryCursor, changed: make(chan struct{}, 1)}
	s.mu.Unlock()
	if err = s.readEntries(ctx, t, true); err != nil {
		return receipt, err
	}
	// Recheck state after the append baseline; an intervening event forces a new preflight.
	state, e := s.Snapshot(ctx)
	if e != nil {
		return receipt, e
	}
	s.mu.Lock()
	if s.active || s.compacting || s.retrying || len(s.tools) > 0 || s.queue > 0 || state.Streaming || state.Compacting || state.PendingCount > 0 {
		s.mu.Unlock()
		return receipt, failure(DispatchRejected, "session became busy before dispatch")
	}
	t.start = s.seq
	s.trace = t
	s.emitLocked("Dispatching")
	s.mu.Unlock()
	accepted = AcceptedUnknown
	ack, e := s.request(ctx, "prompt", map[string]any{"message": d.Message}, s.options.Policy.PromptAckTimeout, false)
	if e != nil {
		return receipt, e
	}
	if !*ack.frame.Success {
		accepted = AcceptedNo
		if _, e = s.Snapshot(ctx); e != nil {
			return receipt, e
		}
		if e = s.readEntries(ctx, t, false); e != nil {
			return receipt, e
		}
		s.mu.Lock()
		sticky := t.sticky
		s.mu.Unlock()
		if sticky != nil {
			return receipt, sticky
		}
		return receipt, failure(DispatchRejected, ack.frame.Error)
	}
	accepted = AcceptedYes
	s.mu.Lock()
	s.emitLocked("DispatchAccepted")
	s.mu.Unlock()
	observeUntil := time.Now().Add(s.options.Policy.PromptObservationTimeout)
	backoff := 50 * time.Millisecond
	var settleUntil time.Time
	for {
		if ctx.Err() != nil {
			return receipt, contextError(ctx)
		}
		select {
		case <-t.changed:
		default:
		}
		s.mu.Lock()
		previousCursor := t.cursor
		s.mu.Unlock()
		if e = s.readEntries(ctx, t, false); e != nil {
			return receipt, e
		}
		s.mu.Lock()
		sticky := t.sticky
		observed := t.tokens == 1 && t.tokenEvents == 1
		settled := t.settled > t.tokenSeq && t.tokenSeq > 0
		paired := len(t.messages) == 0 && len(t.entryMessages) == 0
		progress := t.cursor != previousCursor
		s.mu.Unlock()
		if sticky != nil && sticky.Code != ProviderFailed {
			return receipt, sticky
		}
		if !observed && time.Now().After(observeUntil) {
			return receipt, failure(PromptNotObserved, "accepted prompt not correlated to user event and entry")
		}
		if observed && settled && settleUntil.IsZero() {
			settleUntil = time.Now().Add(s.options.Policy.PromptObservationTimeout)
		}
		if observed && settled && (paired || time.Now().After(settleUntil)) {
			receipt, e = s.finish(ctx, t)
			if e != nil {
				return Execution{}, e
			}
			return receipt, nil
		}
		if progress {
			backoff = 50 * time.Millisecond
		}
		delay := backoff
		if !observed {
			delay = min(delay, time.Until(observeUntil))
		} else if !settleUntil.IsZero() {
			delay = min(delay, time.Until(settleUntil))
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return receipt, contextError(ctx)
		case <-s.failed:
			timer.Stop()
			s.mu.Lock()
			e = s.fatal
			s.mu.Unlock()
			return receipt, e
		case <-s.life.Done():
			timer.Stop()
			s.mu.Lock()
			fatal := s.fatal
			s.mu.Unlock()
			if fatal != nil {
				return receipt, fatal
			}
			if ctx.Err() != nil {
				return receipt, contextError(ctx)
			}
			return receipt, failure(ProcessExited, "session closed while observing entries")
		case <-t.changed:
			timer.Stop()
			backoff = 50 * time.Millisecond
		case <-timer.C:
			backoff = min(backoff*2, 5*time.Second)
		}
	}
}
func (s *session) finish(ctx context.Context, t *dispatchTrace) (Execution, error) {
	s.mu.Lock()
	epoch := s.epoch
	s.mu.Unlock()
	state, err := s.Snapshot(ctx)
	if err != nil {
		return Execution{}, err
	}
	if err = s.readEntries(ctx, t, false); err != nil {
		return Execution{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fatal != nil {
		return Execution{}, s.fatal
	}
	if t.sticky != nil && t.sticky.Code != ProviderFailed {
		return Execution{}, t.sticky
	}
	if epoch != s.epoch || state.Streaming || state.Compacting || state.PendingCount > 0 || s.active || s.compacting || s.retrying || s.queue > 0 || len(s.tools) > 0 {
		return Execution{}, failure(AmbiguousExecution, "activity changed while collecting settled receipt")
	}
	if err = s.lineageLocked(t); err != nil {
		return Execution{}, err
	}
	if t.sticky != nil {
		return Execution{}, t.sticky
	}
	if t.unresolved != "" {
		f := failure(t.unresolved, "terminal failure without proven retry recovery")
		f.Origin = Provider
		return Execution{}, f
	}
	if t.lastStop != "stop" {
		return Execution{}, failure(AmbiguousExecution, "no final stop assistant for this dispatch")
	}
	r := Execution{SessionID: s.id.SessionID, Token: t.token, StartSeq: t.start, SettledSeq: t.settled, ActivityEpoch: s.epoch, PromptEntryID: t.prompt, LastEntryID: t.cursor, LastAssistantID: t.lastAssistant, StopReason: t.lastStop, ExtraUserInputs: t.users - 1}
	s.receipt = &r
	return r, nil
}
func (s *session) lineageLocked(t *dispatchTrace) error {
	if t.tokens != 1 || t.tokenEvents != 1 || t.prompt == "" || t.settled <= t.tokenSeq {
		return failure(AmbiguousExecution, "missing unique prompt/settled correlation")
	}
	if len(t.messages) != 0 || len(t.entryMessages) != 0 || t.mismatch {
		return failure(AmbiguousExecution, "event and entry messages differ")
	}
	if t.matchedSeq >= t.settled {
		return failure(AmbiguousExecution, "message occurred after settled")
	}
	if t.leaf != t.cursor || t.lastAssistant == "" || !t.assistantAfterPrompt {
		return failure(AmbiguousExecution, "active branch lacks prompt or final assistant")
	}
	return nil
}
func (s *session) Confirm(ctx context.Context, r Execution) (confirmation Confirmation, err error) {
	select {
	case <-s.executeLease:
	default:
		return confirmation, failure(SessionBusy, "Execute or Confirm in progress")
	}
	defer func() { s.executeLease <- struct{}{} }()
	defer func() {
		if err != nil {
			s.invalidate(err)
			report, _ := s.Close(context.Background())
			var f *Failure
			if errors.As(err, &f) {
				copy := *f
				if cause := context.Cause(ctx); cause != nil && errors.Is(err, cause) {
					copy.Cause = err
				}
				copy.StderrTail = report.StderrTail
				copy.Cleanup = &report
				copy.DispatchAccepted = AcceptedYes
				copy.HandleID = s.spec.HandleID
				err = &copy
			}
		}
	}()
	s.mu.Lock()
	valid := s.receipt != nil && *s.receipt == r && s.epoch == r.ActivityEpoch
	t := s.trace
	var sticky *Failure
	if t != nil {
		sticky = t.sticky
	}
	s.mu.Unlock()
	if sticky != nil {
		return confirmation, sticky
	}
	if !valid {
		return confirmation, failure(AmbiguousExecution, "receipt is not current")
	}
	state, e := s.Snapshot(ctx)
	if e != nil {
		return confirmation, e
	}
	if e = s.readEntries(ctx, t, false); e != nil {
		return confirmation, e
	}
	// Last state query brackets entries; the local mutex below is the confirmation point.
	state, e = s.Snapshot(ctx)
	if e != nil {
		return confirmation, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fatal != nil {
		return confirmation, s.fatal
	}
	if t.sticky != nil && t.sticky.Code != ProviderFailed {
		return confirmation, t.sticky
	}
	if s.epoch != r.ActivityEpoch || t.cursor != r.LastEntryID || state.Streaming || state.Compacting || state.PendingCount > 0 || s.active || s.compacting || s.retrying || s.queue > 0 || len(s.tools) > 0 {
		return confirmation, failure(AmbiguousExecution, "activity changed after execution receipt")
	}
	if t.sticky != nil {
		return confirmation, t.sticky
	}
	if e = s.lineageLocked(t); e != nil {
		return confirmation, e
	}
	return Confirmation{Seq: s.seq, ActivityEpoch: s.epoch}, nil
}
