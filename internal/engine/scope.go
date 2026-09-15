package engine

import (
	"context"
	"errors"
	"sync"

	"pi-workflow-controller/internal/contract"
)

func callbackError(ctx context.Context, err error) error {
	if classified(err) != nil {
		return err
	}
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
	}
	return err
}

type Scope struct {
	run         *Run
	parent      *Scope
	path, epoch string
	names       map[string]bool
	steps       map[string]bool
	ancestors   []string
	closed      bool
}

func (r *Run) newScope(path string, parent *Scope) *Scope {
	s := &Scope{run: r, parent: parent, path: path, epoch: contract.NewID(), names: make(map[string]bool), steps: make(map[string]bool)}
	if parent != nil {
		s.ancestors = append([]string(nil), parent.ancestors...)
	}
	return s
}
func (s *Scope) checkLocked(ctx context.Context) error {
	if err := s.run.checkLocked(ctx); err != nil {
		return err
	}
	for p := s; p != nil; p = p.parent {
		if p.closed {
			return newFailure(InvalidDefinition, "scope", "scope execution has ended")
		}
	}
	return nil
}
func (s *Scope) reserveLocked(name string) error {
	if !validName(name) || s.names[name] {
		return newFailure(InvalidDefinition, "scope", "invalid or duplicate scope/decision name")
	}
	s.names[name] = true
	return nil
}
func (s *Scope) Child(name string) (*Scope, error) {
	if s == nil || s.run == nil {
		return nil, newFailure(InvalidDefinition, "Child", "scope is not initialized")
	}
	r := s.run
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := s.checkLocked(r.ctx); err != nil {
		return nil, err
	}
	if err := s.reserveLocked(name); err != nil {
		return nil, err
	}
	return r.newScope(s.path+"/"+name, s), nil
}
func (s *Scope) Decision(ctx context.Context, name, reason string, refs []contract.Ref) error {
	if s == nil || s.run == nil {
		return newFailure(InvalidDefinition, "Decision", "scope is not initialized")
	}
	r := s.run
	r.mu.Lock()
	err := s.checkLocked(ctx)
	if err == nil {
		err = s.reserveLocked(name)
	}
	r.mu.Unlock()
	if err != nil {
		return err
	}
	op, done := r.operationContext(ctx)
	defer done()
	for _, ref := range refs {
		if _, err = r.resolve(op, ref); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err = s.checkLocked(op); err != nil {
		return err
	}
	return r.commitLocked("Decision", map[string]any{"scope": s.path, "name": name, "reason": reason, "refs": refs}, func(uint64) {})
}
func (s *Scope) Retry(ctx context.Context, name string, maxRetries int, fn func(context.Context, *Scope, RetryState) (RetryAction, error)) (result Result, retErr error) {
	if s == nil || s.run == nil {
		return Result{}, newFailure(InvalidDefinition, "Retry", "scope is not initialized")
	}
	r := s.run
	r.mu.Lock()
	err := s.checkLocked(ctx)
	if err == nil && (maxRetries < 0 || fn == nil) {
		err = newFailure(InvalidDefinition, "Retry", "nonnegative retry budget and callback required")
	}
	if err == nil {
		err = s.reserveLocked(name)
	}
	if err != nil {
		r.mu.Unlock()
		return Result{}, err
	}
	id := contract.NewID()
	path := s.path + "/" + name
	state := RetryState{ActivationID: id, MaxRetries: maxRetries}
	err = r.commitLocked("RetryStarted", map[string]any{"scope": path, "retry": state}, func(uint64) { r.state.Retries[id] = RetryStatus{Scope: path, RetryState: state, Active: true} })
	r.mu.Unlock()
	op, done := r.operationContext(ctx)
	defer done()
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		v := r.state.Retries[id]
		v.Active = false
		e := r.commitLocked("RetryFinished", v, func(uint64) { r.state.Retries[id] = v })
		if e == nil {
			e = r.finishInvocationsLocked()
		}
		if e != nil {
			result = Result{}
			retErr = e
		}
	}()
	for err == nil {
		r.mu.Lock()
		err = s.checkLocked(op)
		child := r.newScope(path, s)
		child.ancestors = append(child.ancestors, id)
		r.mu.Unlock()
		if err != nil {
			break
		}
		action, callErr := fn(op, child, state)
		r.mu.Lock()
		child.closed = true
		r.mu.Unlock()
		if callErr != nil {
			return Result{}, r.recordError(callbackError(op, callErr))
		}
		r.mu.Lock()
		err = s.checkLocked(op)
		r.mu.Unlock()
		if err != nil {
			break
		}
		if !action.Again {
			if _, err = r.validateResult(op, action.Result); err != nil {
				break
			}
			return action.Result, nil
		}
		if err = r.validateFeedback(op, action.Feedback); err != nil {
			break
		}
		if state.RetryCount >= maxRetries {
			f := newFailure(RetryExhausted, "Retry", action.Feedback.Message)
			f.AttemptID = action.Feedback.SourceAttemptID
			return Result{}, f
		}
		state.RetryCount++
		copy := *action.Feedback
		copy.Refs = append([]contract.Ref(nil), copy.Refs...)
		state.Feedback = &copy
		r.mu.Lock()
		err = s.checkLocked(op)
		if err == nil {
			err = r.commitLocked("RetryScheduled", map[string]any{"scope": path, "retry": state}, func(uint64) { r.state.Retries[id] = RetryStatus{Scope: path, RetryState: state, Active: true} })
		}
		r.mu.Unlock()
	}
	return Result{}, err
}
func (r *Run) finishInvocationsLocked() error {
	for id, v := range r.state.Invocations {
		if v.State != AwaitingScope {
			continue
		}
		active := false
		for _, a := range r.retryAncestors[id] {
			if r.state.Retries[a].Active {
				active = true
				break
			}
		}
		if active {
			continue
		}
		v.State = v.Provisional
		if err := r.commitLocked("InvocationFinished", v, func(seq uint64) { v.LastSeq = seq; r.state.Invocations[id] = v }); err != nil {
			return err
		}
	}
	return nil
}
func (s *Scope) Parallel(ctx context.Context, name string, mode JoinMode, branches []Branch) ([]BranchResult, error) {
	if s == nil || s.run == nil {
		return nil, newFailure(InvalidDefinition, "Parallel", "scope is not initialized")
	}
	r := s.run
	r.mu.Lock()
	err := s.checkLocked(ctx)
	if err == nil && mode != FailFast && mode != CollectAll {
		err = newFailure(InvalidDefinition, "Parallel", "invalid join mode")
	}
	names := map[string]bool{}
	for _, b := range branches {
		if !validName(b.Name) || names[b.Name] || b.Do == nil {
			err = newFailure(InvalidDefinition, "Parallel", "invalid or duplicate branch")
		}
		names[b.Name] = true
	}
	if err == nil {
		err = s.reserveLocked(name)
	}
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	group := r.newScope(s.path+"/"+name, s)
	err = r.commitLocked("GroupStarted", map[string]any{"scope": group.path, "mode": mode, "branches": len(branches)}, func(uint64) {})
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	op, done := r.operationContext(ctx)
	defer done()
	branchCtx, cancel := context.WithCancelCause(op)
	defer cancel(context.Canceled)
	results := make([]BranchResult, len(branches))
	var wg sync.WaitGroup
	var first error
	var firstMu sync.Mutex
	for i, b := range branches {
		child := r.newScope(group.path+"/"+b.Name, group)
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, e := b.Do(branchCtx, child)
			e = callbackError(branchCtx, e)
			if e == nil {
				_, e = r.validateResult(branchCtx, result)
			}
			r.mu.Lock()
			child.closed = true
			r.mu.Unlock()
			results[i] = BranchResult{Name: b.Name, Result: result, Err: e}
			if e != nil {
				_ = r.recordError(e)
				firstMu.Lock()
				if first == nil {
					first = e
					if mode == FailFast {
						f := newFailure(Cancelled, "Parallel", "sibling cancelled by branch "+b.Name)
						f.Origin = OriginFailFastSibling
						f.Cause = e
						cancel(f)
					}
				}
				firstMu.Unlock()
			}
		}()
	}
	wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	group.closed = true
	err = s.checkLocked(op)
	summaries := make([]any, len(results))
	for i, b := range results {
		summaries[i] = map[string]any{"name": b.Name, "result": b.Result, "failure": failureInfo(b.Err)}
	}
	journalErr := r.commitLocked("GroupJoined", map[string]any{"scope": group.path, "results": summaries}, func(uint64) {})
	if err != nil {
		return results, err
	}
	if journalErr != nil {
		return results, journalErr
	}
	if mode == FailFast {
		return results, first
	}
	return results, nil
}
