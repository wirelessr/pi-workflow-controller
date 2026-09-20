package engine

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

type SessionHandle struct {
	run            *Run
	id             string
	role           RoleSpec
	session        runtime.Session // published by closing ready
	ready          chan struct{}
	busy, unusable bool                    // control sequence only
	stopAttempt    context.CancelCauseFunc // ownership lock; cancelled before Close
	closing        atomic.Bool
	closed         atomic.Bool
	closeOnce      sync.Once
	closeDone      chan struct{}
	startCleanup   *runtime.CleanupReport // published by ready
	report         runtime.CleanupReport  // immutable after closeDone
	closeErr       error
}

func (r *Run) ownedHandles() []*SessionHandle {
	r.ownerMu.Lock()
	defer r.ownerMu.Unlock()
	handles := make([]*SessionHandle, 0, len(r.handles))
	for _, h := range r.handles {
		handles = append(handles, h)
	}
	return handles
}
func (r *Run) OpenSession(ctx context.Context, role RoleSpec) (*SessionHandle, error) {
	r.mu.Lock()
	if err := r.checkLocked(ctx); err != nil {
		r.mu.Unlock()
		return nil, err
	}
	if role.CWD == "" {
		role.CWD = r.input.LaunchCWD
	}
	if !validName(role.Name) || role.Model.Provider == "" || role.Model.ID == "" || role.Model.Thinking == "" || !filepath.IsAbs(role.CWD) {
		r.mu.Unlock()
		return nil, newFailure(InvalidDefinition, "OpenSession", "explicit role/model/thinking and absolute cwd required")
	}
	r.ownerMu.Lock()
	if r.resourcesStopped {
		r.ownerMu.Unlock()
		r.mu.Unlock()
		return nil, context.Cause(r.ctx)
	}
	live := 0
	for _, h := range r.handles {
		if !h.closed.Load() {
			live++
		}
	}
	p := r.definition.Policy
	if live >= p.MaxLiveSessions || len(r.handles) >= p.MaxTotalSessions {
		r.ownerMu.Unlock()
		f := newFailure(LimitExceeded, "OpenSession", "session limit exceeded")
		f.LimitScope = "run"
		r.stopLocked(f)
		r.mu.Unlock()
		return nil, f
	}
	h := &SessionHandle{run: r, id: contract.NewID(), role: role, ready: make(chan struct{}), closeDone: make(chan struct{})}
	r.handles[h.id] = h
	r.ownerMu.Unlock()
	err := r.commitLocked("SessionStarting", map[string]any{"handle_id": h.id, "role": role}, func(uint64) {
		r.state.Sessions[h.id] = SessionStatus{ID: h.id, Role: role, State: "Starting", Health: "Offline"}
	})
	r.mu.Unlock()
	op, done := r.operationContext(ctx)
	defer done()
	if err == nil {
		h.session, err = r.runtime.Start(op, runtime.SessionSpec{HandleID: h.id, Name: role.Name, Model: role.Model, CWD: role.CWD, SessionDir: filepath.Join(r.Dir(), "sessions", h.id, "pi"), AppendPrompt: role.AppendPrompt})
	}
	if err == nil && h.session == nil {
		err = newFailure(StartFailed, "OpenSession", "runtime returned no session")
	}
	if err != nil {
		var f *Failure
		if errors.As(err, &f) && f.Cleanup != nil {
			copy := *f.Cleanup
			h.startCleanup = &copy
		}
	}
	close(h.ready)
	r.mu.Lock()
	if err == nil {
		err = r.checkLocked(op)
	}
	if err == nil {
		err = r.commitLocked("SessionReady", h.session.Identity(), func(uint64) {
			v := r.state.Sessions[h.id]
			v.Identity = h.session.Identity()
			v.State = "Idle"
			v.Health = "Online"
			r.state.Sessions[h.id] = v
		})
	}
	r.mu.Unlock()
	if err != nil {
		_ = r.closeHandle(h)
		return nil, r.recordError(err)
	}
	return h, nil
}

// SessionIdentity reads the committed identity of an owned handle, without RPC
// or attempt accounting. It remains available after local close while the run
// still accepts workflow operations.
func (r *Run) SessionIdentity(ctx context.Context, h *SessionHandle) (runtime.Identity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkLocked(ctx); err != nil {
		return runtime.Identity{}, err
	}
	if h == nil || h.run != r {
		return runtime.Identity{}, newFailure(InvalidDefinition, "SessionIdentity", "foreign or nil handle")
	}
	v, ok := r.state.Sessions[h.id]
	if !ok || v.Identity.HandleID != h.id || v.Identity.SessionID == "" {
		return runtime.Identity{}, newFailure(IdentityMismatch, "SessionIdentity", "owned session has no committed identity")
	}
	return v.Identity, nil
}

// SessionContextUsage leases an idle owned handle without holding the control
// lock across RPC. It does not consume an attempt or add a health probe.
func (r *Run) SessionContextUsage(ctx context.Context, h *SessionHandle) (runtime.ContextUsage, error) {
	r.mu.Lock()
	if err := r.checkLocked(ctx); err != nil {
		r.mu.Unlock()
		return runtime.ContextUsage{}, err
	}
	if h == nil || h.run != r || h.unusable || h.closing.Load() || h.session == nil {
		r.mu.Unlock()
		return runtime.ContextUsage{}, newFailure(InvalidDefinition, "SessionContextUsage", "invalid or unavailable handle")
	}
	if h.busy {
		r.mu.Unlock()
		return runtime.ContextUsage{}, newFailure(SessionBusy, "SessionContextUsage", "session already leased")
	}
	h.busy = true
	r.mu.Unlock()
	op, done := r.operationContext(ctx)
	defer done()
	usage, err := h.session.ContextUsage(op)
	r.mu.Lock()
	if err == nil {
		err = r.checkLocked(op)
	}
	if err != nil {
		h.unusable = true
	}
	r.mu.Unlock()
	if err != nil {
		err = r.recordError(err)
		_ = r.closeHandle(h)
	}
	r.mu.Lock()
	h.busy = false
	r.mu.Unlock()
	if err != nil {
		return runtime.ContextUsage{}, err
	}
	return usage, nil
}

func (r *Run) CloseSession(ctx context.Context, h *SessionHandle) error {
	_, err := r.CloseSessionReport(ctx, h)
	return err
}

// CloseSessionReport returns the same closeOnce outcome as CloseSession. Cleanup
// retains its independent bounded budget even when the workflow caller cancels.
func (r *Run) CloseSessionReport(ctx context.Context, h *SessionHandle) (runtime.CleanupReport, error) {
	r.mu.Lock()
	if err := r.checkLocked(ctx); err != nil {
		r.mu.Unlock()
		return runtime.CleanupReport{}, err
	}
	if h == nil || h.run != r {
		r.mu.Unlock()
		return runtime.CleanupReport{}, newFailure(InvalidDefinition, "CloseSession", "foreign or nil handle")
	}
	if h.busy {
		r.mu.Unlock()
		return runtime.CleanupReport{}, newFailure(SessionBusy, "CloseSession", "handle leased by a step")
	}
	h.unusable = true
	r.mu.Unlock()
	err := r.closeHandle(h)
	select {
	case <-h.closeDone:
		report := h.report
		report.Unconfirmed = append([]string(nil), report.Unconfirmed...)
		report.DiscoveryRemoved = append([]string(nil), report.DiscoveryRemoved...)
		return report, err
	default:
		return runtime.CleanupReport{Identity: runtime.Identity{HandleID: h.id}, Unconfirmed: []string{"Close outcome not available before cleanup deadline"}}, err
	}
}
func (r *Run) closeHandle(h *SessionHandle) error {
	ctx, cancel := context.WithTimeout(context.Background(), r.definition.Policy.Runtime.CleanupTimeout)
	defer cancel()
	err := r.closeHandleContext(ctx, h)
	r.mu.Lock()
	defer r.mu.Unlock()
	if e := r.closedStateLocked(h); e != nil && err == nil {
		err = e
	}
	return err
}
func (r *Run) closeHandleContext(ctx context.Context, h *SessionHandle) error {
	h.closeOnce.Do(func() {
		h.closing.Store(true)
		go func() {
			defer close(h.closeDone)
			select {
			case <-h.ready:
			case <-ctx.Done():
				h.closeErr = context.Cause(ctx)
				// A conforming Runtime.Start returns on cancellation and owns partial startup
				// cleanup. Keep a late successful return from escaping ownership as well.
				go func() {
					<-h.ready
					if h.session != nil {
						late, cancel := context.WithTimeout(context.Background(), r.definition.Policy.Runtime.CleanupTimeout)
						defer cancel()
						_, _ = h.session.Close(late)
					}
				}()
				return
			}
			if h.session != nil {
				h.report, h.closeErr = h.session.Close(ctx)
			} else if h.startCleanup != nil {
				h.report = *h.startCleanup
			}
			if h.closeErr == nil && (len(h.report.Unconfirmed) > 0 || h.report.KillError != "" || h.report.DiscoveryError != "") {
				h.closeErr = newFailure(CleanupFailed, "cleanup", "runtime cleanup incomplete")
			}
			if h.closeErr == nil && !h.report.ProcessExited && h.session != nil {
				h.closeErr = newFailure(CleanupFailed, "cleanup", "runtime did not confirm process exit")
			}
			h.closed.Store(h.report.ProcessExited || (h.session == nil && h.startCleanup == nil))
		}()
	})
	select {
	case <-h.closeDone:
		return h.closeErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func (r *Run) closedStateLocked(h *SessionHandle) error {
	select {
	case <-h.closeDone:
	default:
		return nil
	}
	v := r.state.Sessions[h.id]
	if r.finished || v.State == "Closed" || v.State == "Unresponsive" {
		return nil
	}
	return r.commitLocked("SessionClosed", map[string]any{"handle_id": h.id, "cleanup": h.report, "failure": failureInfo(h.closeErr)}, func(uint64) {
		v.State = "Closed"
		if !h.closed.Load() {
			v.State = "Unresponsive"
		}
		v.Health = "Offline"
		r.state.Sessions[h.id] = v
	})
}
func (r *Run) beginCleanup() {
	r.cleanupOnce.Do(func() {
		r.ownerMu.Lock()
		r.resourcesStopped = true
		handles := make([]*SessionHandle, 0, len(r.handles))
		for _, h := range r.handles {
			if h.stopAttempt != nil {
				h.stopAttempt(context.Cause(r.ctx))
			}
			handles = append(handles, h)
		}
		r.ownerMu.Unlock()
		go func() {
			defer close(r.cleanupDone)
			ctx, cancel := context.WithTimeout(context.Background(), r.definition.Policy.Runtime.CleanupTimeout)
			defer cancel()
			reports := make([]runtime.CleanupReport, len(handles))
			errs := make([]error, len(handles))
			var wg sync.WaitGroup
			for i, h := range handles {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = r.closeHandleContext(ctx, h)
					select {
					case <-h.closeDone:
						reports[i] = h.report
					default:
					}
				}()
			}
			wg.Wait()
			r.cleanupReports = reports
			errs = append(errs, r.cleanupResources(ctx, handles)...)
			for _, err := range errs {
				if err != nil {
					f := newFailure(CleanupFailed, "cleanup", "session cleanup failed")
					f.Cause = err
					r.cleanupErrors = append(r.cleanupErrors, f)
				}
			}
		}()
	})
}

// Observe's arbitration lock is independent of journal I/O and its bounded queue.
func (r *Run) Observe(ctx context.Context, o runtime.Observation) error {
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	if r.observationsClosed {
		return nil
	}
	if o.Failure != nil && fatal(o.Failure) {
		f := normalize(o.Failure, "observation")
		f.HandleID = o.HandleID
		r.observationFailure(f)
	}
	select {
	case r.observations <- o:
		return nil
	default:
		f := newFailure(LimitExceeded, "observation", "runtime observation queue exhausted")
		f.LimitScope = "run"
		r.observationFailure(f)
		return f
	}
}
func (r *Run) observationFailure(err error) {
	if r.outcomeLocked {
		if r.pendingFatal == nil {
			r.pendingFatal = err
		}
	} else {
		r.registerStop(err)
	}
}
func (r *Run) observeLoop() {
	defer close(r.observationDone)
	for {
		select {
		case o := <-r.observations:
			r.applyObservation(o)
		case <-r.observationStop:
			for {
				select {
				case o := <-r.observations:
					r.applyObservation(o)
				default:
					return
				}
			}
		}
	}
}
func (r *Run) applyObservation(o runtime.Observation) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	_ = r.commitLocked("SessionObservation", o, func(seq uint64) {
		v, ok := r.state.Sessions[o.HandleID]
		if !ok {
			return
		}
		if o.Seq < v.RuntimeSeq {
			return
		}
		v.RuntimeSeq = o.Seq
		if v.State != "Closed" {
			if o.Kind == "RuntimeFailed" {
				v.Health = "Unresponsive"
			}
			if o.Kind == "auto_retry_start" {
				v.ProviderRetries++
			}
		}
		r.state.Sessions[o.HandleID] = v
		for id, a := range r.state.Attempts {
			if a.HandleID != o.HandleID || a.Identity.DispatchToken != o.DispatchToken {
				continue
			}
			if a.State != WaitingSession && a.State != Dispatching && a.State != Running {
				continue
			}
			switch o.Kind {
			case "Dispatching":
				a.State = Dispatching
				a.DispatchAccepted = AcceptedUnknown
			case "DispatchAccepted":
				a.State = Running
				a.DispatchAccepted = AcceptedYes
			default:
				continue
			}
			a.LastSeq = seq
			r.state.Attempts[id] = a
		}
	})
}
