package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"time"
)

func (s *session) consume(raw []byte) error {
	var f wireFrame
	if err := json.Unmarshal(raw, &f); err != nil || f.Type == "" {
		return failure(ProtocolFailed, "RPC object requires type")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	if f.Type == "response" {
		if f.ID == "" || f.Command == "" || f.Success == nil {
			return failure(ProtocolFailed, "response requires id, command and success")
		}
		s.lastResponse = time.Now()
		if p, ok := s.pending[f.ID]; ok {
			if p.command != f.Command {
				return failure(ProtocolFailed, "response command does not match request")
			}
			delete(s.pending, f.ID)
			p.ch <- response{frame: f, seq: s.seq}
		}
		return nil
	}
	epoch := s.epoch
	if err := s.eventLocked(f); err != nil {
		return err
	}
	if s.epoch != epoch {
		s.wakeLocked()
	}
	return nil
}

// The write deadline is independent of the response timeout, which starts only after full LF write.
func (s *session) write(ctx context.Context, b []byte, cleanup bool) error {
	writeCtx, cancel := context.WithTimeout(ctx, s.options.Policy.RPCTimeout)
	defer cancel()
	var stopped <-chan struct{}
	if !cleanup {
		stopped = s.life.Done()
	}
	select {
	case <-stopped:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.fatal != nil {
			return s.fatal
		}
		if ctx.Err() != nil {
			return contextError(ctx)
		}
		return failure(ProcessExited, "session is closing")
	case <-writeCtx.Done():
		if ctx.Err() != nil {
			return contextError(ctx)
		}
		return failure(RPCUnresponsive, "RPC writer unavailable before write deadline")
	case <-s.writeLease:
	}
	defer func() { s.writeLease <- struct{}{} }()
	if ctx.Err() != nil {
		return contextError(ctx)
	}
	s.mu.Lock()
	f := s.fatal
	closing := s.closing
	s.mu.Unlock()
	if !cleanup {
		if f != nil {
			return f
		}
		if closing {
			if ctx.Err() != nil {
				return contextError(ctx)
			}
			return failure(ProcessExited, "session is closing")
		}
	}
	deadline := time.Now().Add(s.options.Policy.RPCTimeout)
	d, ok := ctx.Deadline()
	contextDeadline := ok && !d.After(deadline)
	if contextDeadline {
		deadline = d
	}
	if err := s.in.SetWriteDeadline(deadline); err != nil {
		if ctx.Err() != nil {
			return contextError(ctx)
		}
		return err
	}
	cancelled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = s.in.SetWriteDeadline(time.Now()); close(cancelled) })
	n, err := s.in.Write(b)
	if !stop() {
		<-cancelled
	}
	_ = s.in.SetWriteDeadline(time.Time{})
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	if contextDeadline && errors.Is(err, os.ErrDeadlineExceeded) {
		// The pipe's timer can beat context cancellation at the same deadline.
		// Wait for the owning context to publish its cause rather than inventing an RPC failure.
		<-ctx.Done()
	}
	if ctx.Err() != nil {
		return contextError(ctx)
	}
	if err != nil {
		f := failure(RPCUnresponsive, "RPC command write failed")
		f.Cause = err
		return f
	}
	return nil
}
func (s *session) request(ctx context.Context, command string, fields map[string]any, timeout time.Duration, cleanup bool) (response, error) {
	id := randomID()
	body := map[string]any{"id": id, "type": command}
	for k, v := range fields {
		body[k] = v
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return response{}, err
	}
	raw = append(raw, '\n')
	if len(raw)-1 > s.options.Policy.MaxFrameBytes {
		return response{}, failure(LimitExceeded, "outgoing RPC frame exceeds limit")
	}
	p := pendingRequest{command: command, ch: make(chan response, 1)}
	s.mu.Lock()
	if !cleanup && len(s.pending) >= s.options.Policy.ObservationQueue {
		s.mu.Unlock()
		return response{}, failure(LimitExceeded, "too many pending RPC requests")
	}
	s.pending[id] = p
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.pending, id); s.mu.Unlock() }()
	if err = s.write(ctx, raw, cleanup); err != nil {
		return response{}, err
	}
	var timeoutDone <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutDone = timer.C
	}
	var failed, processDone, stopped <-chan struct{}
	if !cleanup {
		failed = s.failed
		stopped = s.life.Done()
	} else {
		processDone = s.processDone
	}
	select {
	case r := <-p.ch:
		return r, nil
	case <-ctx.Done():
		return response{}, contextError(ctx)
	case <-failed:
		s.mu.Lock()
		defer s.mu.Unlock()
		return response{}, s.fatal
	case <-stopped:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.fatal != nil {
			return response{}, s.fatal
		}
		if ctx.Err() != nil {
			return response{}, contextError(ctx)
		}
		return response{}, failure(ProcessExited, "session closed while awaiting response")
	case <-processDone:
		return response{}, failure(ProcessExited, "Pi exited while awaiting response")
	case <-timeoutDone:
		if ctx.Err() != nil {
			return response{}, contextError(ctx)
		}
		return response{}, failure(RPCUnresponsive, "RPC response timeout after full command write")
	}
}
func (s *session) query(ctx context.Context, command string, fields map[string]any) (response, error) {
	r, err := s.request(ctx, command, fields, s.options.Policy.RPCTimeout, false)
	if err == nil && !*r.frame.Success {
		err = failure(ProtocolFailed, "query rejected: "+r.frame.Error)
	}
	// The health probe uses the session lifetime. Its intentional shutdown is
	// not a protocol fault that may replace an active attempt's stop cause.
	probeStopped := ctx == s.life && ctx.Err() != nil && errors.Is(err, context.Cause(ctx))
	if err != nil && !probeStopped {
		s.invalidate(err)
	}
	return r, err
}
func (s *session) invalidate(err error) {
	var f *Failure
	if !errors.As(err, &f) {
		f = failure(ProtocolFailed, "runtime operation failed")
		f.Cause = err
	}
	copy := *f
	s.mu.Lock()
	s.failLocked(&copy)
	s.mu.Unlock()
}

// ContextUsage deliberately does not change health polling or execution receipts.
func (s *session) ContextUsage(ctx context.Context) (ContextUsage, error) {
	r, err := s.query(ctx, "get_session_stats", nil)
	if err != nil {
		return ContextUsage{}, err
	}
	var w struct {
		SessionID   string          `json:"sessionId"`
		SessionFile string          `json:"sessionFile"`
		Usage       json.RawMessage `json:"contextUsage"`
	}
	bad := func() (ContextUsage, error) {
		err := failure(ProtocolFailed, "get_session_stats has malformed context usage or identity")
		s.invalidate(err)
		return ContextUsage{}, err
	}
	if json.Unmarshal(r.frame.Data, &w) != nil || w.SessionID == "" || w.SessionFile == "" {
		return bad()
	}
	usage := ContextUsage{}
	if len(w.Usage) > 0 && string(w.Usage) != "null" {
		var fields struct {
			Tokens  json.RawMessage `json:"tokens"`
			Window  *int64          `json:"contextWindow"`
			Percent json.RawMessage `json:"percent"`
		}
		if json.Unmarshal(w.Usage, &fields) != nil || fields.Window == nil || *fields.Window <= 0 || len(fields.Tokens) == 0 || len(fields.Percent) == 0 {
			return bad()
		}
		if json.Unmarshal(fields.Tokens, &usage.Tokens) != nil || json.Unmarshal(fields.Percent, &usage.Percent) != nil {
			return bad()
		}
		if usage.Tokens != nil && *usage.Tokens < 0 || usage.Percent != nil && (*usage.Percent < 0 || math.IsNaN(*usage.Percent) || math.IsInf(*usage.Percent, 0)) {
			return bad()
		}
		usage.ContextWindow = fields.Window
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if w.SessionID != s.id.SessionID || w.SessionFile != s.id.SessionFile {
		err := failure(SessionChanged, "session identity changed")
		s.failLocked(err)
		return ContextUsage{}, err
	}
	if s.fatal != nil {
		return ContextUsage{}, s.fatal
	}
	usage.Identity, usage.SampledAt = s.id, time.Now()
	usage.Seq, usage.ActivityEpoch = r.seq, s.epoch
	return usage, nil
}

type wireState struct {
	Model *struct {
		Provider string `json:"provider"`
		ID       string `json:"id"`
	} `json:"model"`
	Thinking    string `json:"thinkingLevel"`
	Streaming   *bool  `json:"isStreaming"`
	Compacting  *bool  `json:"isCompacting"`
	Pending     *int   `json:"pendingMessageCount"`
	SessionID   string `json:"sessionId"`
	SessionFile string `json:"sessionFile"`
}

func (s *session) Snapshot(ctx context.Context) (SessionState, error) { return s.snapshot(ctx, false) }
func (s *session) snapshot(ctx context.Context, startup bool) (SessionState, error) {
	var r response
	var err error
	if startup {
		// Initialization owns the response budget; ordinary RPC timeouts start after readiness.
		r, err = s.request(ctx, "get_state", nil, 0, false)
		if err == nil && !*r.frame.Success {
			err = failure(ProtocolFailed, "query rejected: "+r.frame.Error)
		}
	} else {
		r, err = s.query(ctx, "get_state", nil)
	}
	if err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		health := "Exited"
		if s.fatal != nil && s.fatal.Code == RPCUnresponsive {
			health = "Unresponsive"
		}
		return SessionState{Identity: s.id, Health: health, Model: s.spec.Model, HubVisible: s.ready,
			LastResponseTime: s.lastResponse, Seq: s.seq, ActivityEpoch: s.epoch}, err
	}
	var w wireState
	if json.Unmarshal(r.frame.Data, &w) != nil || w.Model == nil || w.Model.ID == "" || w.Model.Provider == "" || w.Thinking == "" || w.Streaming == nil || w.Compacting == nil || w.Pending == nil || *w.Pending < 0 || w.SessionID == "" || w.SessionFile == "" {
		err = failure(ProtocolFailed, "get_state missing required fields")
		s.invalidate(err)
		return SessionState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !startup && (w.SessionID != s.id.SessionID || w.SessionFile != s.id.SessionFile) {
		err = failure(SessionChanged, "session identity changed")
	}
	if w.Model.Provider != s.spec.Model.Provider || w.Model.ID != s.spec.Model.ID {
		err = failure(ModelChanged, "selected model changed")
	}
	if w.Thinking != s.spec.Model.Thinking {
		err = failure(ThinkingChanged, "selected thinking level changed")
	}
	if err != nil {
		s.failLocked(err.(*Failure))
		return SessionState{}, err
	}
	if s.fatal != nil {
		return SessionState{}, s.fatal
	}
	id := s.id
	id.SessionID = w.SessionID
	id.SessionFile = w.SessionFile
	return SessionState{Identity: id, Health: "Online", Model: s.spec.Model, Streaming: *w.Streaming, Compacting: *w.Compacting, PendingCount: *w.Pending, HubVisible: s.ready, LastResponseTime: s.lastResponse, Seq: r.seq, ActivityEpoch: s.epoch}, nil
}
