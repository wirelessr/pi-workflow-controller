package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

// Keep the request/candidate fixture intact; only process startup and shutdown
// need additional barriers at the runtime boundary.
type engDeadlineRuntime struct {
	*engTestRuntime
	afterStart func(context.Context, runtime.Session) error
	close      func(context.Context, runtime.Session) (runtime.CleanupReport, error)
}

func (f *engDeadlineRuntime) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	s, err := f.engTestRuntime.Start(ctx, spec)
	if s == nil {
		return nil, err
	}
	wrapped := &engDeadlineSession{Session: s, close: f.close}
	if err == nil && f.afterStart != nil {
		err = f.afterStart(ctx, s)
	}
	return wrapped, err
}

type engDeadlineSession struct {
	runtime.Session
	close func(context.Context, runtime.Session) (runtime.CleanupReport, error)
}

func (s *engDeadlineSession) Close(ctx context.Context) (runtime.CleanupReport, error) {
	if s.close != nil {
		return s.close(ctx, s.Session)
	}
	return s.Session.Close(ctx)
}

func engDeadlineNew(t *testing.T, parent context.Context, policy RunPolicy, fake runtime.Runtime, workflow Workflow) *Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	t.Cleanup(cancel)
	base := t.TempDir()
	r, err := New(ctx, Definition{Name: "deadline", Version: "v1", Policy: policy, Execute: workflow},
		Input{Prompt: "deadline races", LaunchCWD: base},
		Options{Schemas: engTestSchemas(t), Runtime: fake, BaseDir: base})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func engDeadlineReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("deadline test barrier did not complete")
		var zero T
		return zero
	}
}

func engDeadlineFailure(t *testing.T, err error, code Code, origin Origin) {
	t.Helper()
	var f *Failure
	if !errors.As(err, &f) || f.Code != code || f.Origin != origin {
		t.Errorf("failure = %+v, want %s/%s", err, code, origin)
	}
}

func TestEngineAttemptDeadlineCoversPreparingThroughConfirm(t *testing.T) {
	for _, phase := range []string{"preparing", "input-read", "execute-wait", "confirm"} {
		for _, override := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/step-timeout=%t", phase, override), func(t *testing.T) {
				const timeout = 300 * time.Millisecond
				policy := DefaultRunPolicy()
				policy.AttemptTimeout = timeout
				stepTimeout := time.Duration(0)
				if override {
					policy.AttemptTimeout = 5 * time.Second
					stepTimeout = timeout
				}
				fake := &engTestRuntime{}
				var observed error
				var result StepResult
				var timerCause error
				barrierHit := false
				wait := func(ctx context.Context) error {
					barrierHit = true
					<-ctx.Done()
					timerCause = context.Cause(ctx)
					return timerCause
				}
				if phase == "execute-wait" {
					fake.execute = func(ctx context.Context, _ engTestCall) engTestReply {
						return engTestReply{Err: wait(ctx)}
					}
				}
				if phase == "confirm" {
					fake.confirm = func(ctx context.Context, _ engTestCall, _ runtime.Execution) (runtime.Confirmation, error) {
						return runtime.Confirmation{}, wait(ctx)
					}
				}
				r := engDeadlineNew(t, context.Background(), policy, fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
					h, err := run.OpenSession(ctx, engTestRole("worker"))
					if err != nil {
						return Result{}, err
					}
					var inputs []contract.Ref
					if phase == "input-read" {
						producer, err := run.Root().Step(ctx, StepSpec{Key: "producer", Session: h, Prompt: "producer", Timeout: 5 * time.Second, Output: contract.Spec{SchemaID: engTestSchema}})
						if err != nil {
							return Result{}, err
						}
						if _, err := Decode[engTestData](ctx, run, producer.Output); err != nil {
							return Result{}, err
						}
						inputs = []contract.Ref{producer.Output}
					}
					result, observed = run.Root().Step(ctx, StepSpec{Key: "expires", Session: h, Prompt: "expires", Inputs: inputs, Timeout: stepTimeout, Output: contract.Spec{SchemaID: engTestSchema}})
					return Result{}, observed
				})
				r.beforeIO = func(path, ioPhase string) {
					if (phase != "preparing" || path != "run.json" || ioPhase != "AttemptStarted") &&
						(phase != "input-read" || ioPhase != "step") {
						return
					}
					for _, attempt := range r.state.Attempts {
						if attempt.Key != "expires" || attempt.State != Preparing {
							continue
						}
						// The hook holds r.mu. Wait on a real timer for the recorded
						// Preparing deadline, not a sleep or a new timeout budget.
						clock, cancel := context.WithDeadline(context.Background(), attempt.StartedAt.Add(timeout))
						defer cancel()
						_ = wait(clock)
					}
				}
				report := engDeadlineReceive(t, engTestExecuteAsync(t, r))
				engTestReport(t, report, TimedOutState, 1)
				engTestPersisted(t, r, report)
				engDeadlineFailure(t, observed, TimedOut, OriginAttemptDeadline)
				engDeadlineFailure(t, report.Failure, TimedOut, OriginAttemptDeadline)
				if !barrierHit || result.AttemptID == "" || result.Output != (contract.Ref{}) {
					t.Fatalf("barrier=%t, expired result=%+v", barrierHit, result)
				}
				if phase == "execute-wait" || phase == "confirm" {
					if !errors.Is(observed, timerCause) || !errors.Is(report.Failure, timerCause) {
						t.Error("attempt timer cause was not retained")
					}
				}
				attempt := report.Snapshot.Attempts[result.AttemptID]
				if attempt.State != TimedOutState || attempt.Number != 1 || attempt.Output != nil {
					t.Errorf("expired attempt = %+v", attempt)
				}
				calls, closes, confirms := fake.allSessions()[0].history()
				wantCalls, wantConfirms := 1, 0
				if phase == "preparing" {
					wantCalls = 0
				}
				if phase == "input-read" || phase == "confirm" {
					wantConfirms = 1
				}
				if len(calls) != wantCalls || confirms != wantConfirms || closes != 1 {
					t.Errorf("Execute/Confirm/Close = %d/%d/%d, want %d/%d/1", len(calls), confirms, closes, wantCalls, wantConfirms)
				}
				if (phase == "preparing" || phase == "input-read") && attempt.DispatchAccepted != AcceptedNo {
					t.Errorf("pre-dispatch timeout accepted = %s", attempt.DispatchAccepted)
				}
			})
		}
	}
}

func TestEngineRunDeadlineIncludesOpenSession(t *testing.T) {
	policy := DefaultRunPolicy()
	policy.RunTimeout = 300 * time.Millisecond
	fake := &engDeadlineRuntime{engTestRuntime: &engTestRuntime{}}
	started := make(chan struct{})
	var startCause, openErr error
	fake.afterStart = func(ctx context.Context, _ runtime.Session) error {
		close(started)
		<-ctx.Done()
		startCause = context.Cause(ctx)
		return startCause
	}
	r := engDeadlineNew(t, context.Background(), policy, fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		_, openErr = run.OpenSession(ctx, engTestRole("starting"))
		return Result{}, openErr
	})
	done := engTestExecuteAsync(t, r)
	engDeadlineReceive(t, started)
	report := engDeadlineReceive(t, done)
	engTestReport(t, report, TimedOutState, 1)
	engTestPersisted(t, r, report)
	engDeadlineFailure(t, openErr, TimedOut, OriginRunDeadline)
	engDeadlineFailure(t, report.Failure, TimedOut, OriginRunDeadline)
	if !errors.Is(openErr, startCause) || !errors.Is(report.Failure, startCause) {
		t.Error("startup lost the run deadline cause")
	}
	if len(report.Snapshot.Attempts) != 0 {
		t.Errorf("startup timeout created attempts: %+v", report.Snapshot.Attempts)
	}
	engPersistClosed(t, fake.engTestRuntime, report, 1)
}

func TestEngineRootStopCleansUpBeforeWorkflowReturns(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			policy := DefaultRunPolicy()
			if deadline {
				policy.RunTimeout = 300 * time.Millisecond
			}
			entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			fake := &engDeadlineRuntime{engTestRuntime: &engTestRuntime{}, close: func(ctx context.Context, s runtime.Session) (runtime.CleanupReport, error) {
				close(closed)
				return s.Close(ctx)
			}}
			r := engDeadlineNew(t, context.Background(), policy, fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				if _, err := run.OpenSession(ctx, engTestRole("worker")); err != nil {
					return Result{}, err
				}
				close(entered)
				<-release
				return Result{}, nil
			})
			done := engTestExecuteAsync(t, r)
			t.Cleanup(unblock)
			engDeadlineReceive(t, entered)
			if !deadline {
				r.Cancel(OriginControllerUser)
			}
			engDeadlineReceive(t, closed)
			select {
			case report := <-done:
				t.Fatalf("Execute returned before workflow barrier release: %+v", report)
			default:
			}
			unblock()
			report := engDeadlineReceive(t, done)
			state, exit, code, origin := CancelledState, 130, Cancelled, OriginControllerUser
			if deadline {
				state, exit, code, origin = TimedOutState, 1, TimedOut, OriginRunDeadline
			}
			engTestReport(t, report, state, exit)
			engDeadlineFailure(t, report.Failure, code, origin)
			engTestPersisted(t, r, report)
			engPersistClosed(t, fake.engTestRuntime, report, 1)
		})
	}
}

func TestEngineSF01RootStopClosesIdleSessionBeforeJournalRelease(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline bool
		state    State
		exit     int
		code     Code
		origin   Origin
	}{
		{"controller-cancel", false, CancelledState, 130, Cancelled, OriginControllerUser},
		{"run-deadline", true, TimedOutState, 1, TimedOut, OriginRunDeadline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := DefaultRunPolicy()
			if tc.deadline {
				policy.RunTimeout = 300 * time.Millisecond
			}
			ready := make(chan Snapshot, 1)
			entered := make(chan error, 1)
			release, returned := make(chan struct{}), make(chan struct{})
			closing, closed := make(chan struct{}), make(chan error, 1)
			unblock := sync.OnceFunc(func() { close(release) })
			fake := &engDeadlineRuntime{engTestRuntime: &engTestRuntime{}, close: func(ctx context.Context, s runtime.Session) (runtime.CleanupReport, error) {
				close(closing)
				report, err := s.Close(ctx)
				closed <- err
				return report, err
			}}
			r := engDeadlineNew(t, context.Background(), policy, fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				defer close(returned)
				if _, err := run.OpenSession(ctx, engTestRole("idle")); err != nil {
					return Result{}, err
				}
				ready <- run.Snapshot()
				return Result{}, run.Root().Decision(ctx, "blocked", "SF01 journal barrier", nil)
			})
			r.beforeIO = func(path, phase string) {
				if path == "events.jsonl" && phase == "Decision" {
					// appendLocked holds r.mu here, before any journal filesystem I/O.
					entered <- context.Cause(r.ctx)
					<-release
				}
			}
			done := engTestExecuteAsync(t, r)
			t.Cleanup(unblock)
			snapshot := engDeadlineReceive(t, ready)
			if len(snapshot.Sessions) != 1 || len(snapshot.Attempts) != 0 {
				t.Fatalf("expected one established session without attempts: %+v", snapshot)
			}
			for _, session := range snapshot.Sessions {
				if session.State != "Idle" || session.Health != "Online" {
					t.Fatalf("session not idle before core event: %+v", session)
				}
			}
			if cause := engDeadlineReceive(t, entered); cause != nil {
				t.Fatalf("run stopped before journal barrier: %v", cause)
			}
			if r.mu.TryLock() {
				r.mu.Unlock()
				t.Fatal("journal barrier does not hold r.mu")
			}
			if !tc.deadline {
				cancelled := make(chan struct{})
				go func() {
					r.Cancel(OriginControllerUser)
					close(cancelled)
				}()
				engDeadlineReceive(t, cancelled)
			}
			engDeadlineReceive(t, r.ctx.Done())
			cause := context.Cause(r.ctx)
			engDeadlineFailure(t, cause, tc.code, tc.origin)
			if tc.deadline && r.ctx.Err() != context.DeadlineExceeded {
				t.Fatalf("run stopped without its real deadline timer: %v", r.ctx.Err())
			}
			engDeadlineReceive(t, closing)
			if err := engDeadlineReceive(t, closed); err != nil {
				t.Fatalf("runtime Close failed while journal was blocked: %v", err)
			}
			// Join real cleanup so the Close wrapper has returned before IO resumes.
			engDeadlineReceive(t, r.cleanupDone)
			calls, closes, confirms := fake.allSessions()[0].history()
			if len(calls) != 0 || closes != 1 || confirms != 0 {
				t.Fatalf("idle Execute/Close/Confirm before IO release = %d/%d/%d, want 0/1/0", len(calls), closes, confirms)
			}
			select {
			case <-returned:
				t.Fatal("workflow callback returned before journal barrier release")
			default:
			}
			select {
			case report := <-done:
				t.Fatalf("Execute returned before journal barrier release: %+v", report)
			default:
			}
			unblock()
			engDeadlineReceive(t, returned)
			report := engDeadlineReceive(t, done)
			engTestReport(t, report, tc.state, tc.exit)
			engDeadlineFailure(t, report.Failure, tc.code, tc.origin)
			if !errors.Is(report.Failure, cause) {
				t.Errorf("journal release lost root stop cause: %v, want %v", report.Failure, cause)
			}
			engTestPersisted(t, r, report)
			engPersistClosed(t, fake.engTestRuntime, report, 1)
		})
	}
}

func TestEngineSF04FinalizingClosesIdleSessionBeforeIORelease(t *testing.T) {
	for _, path := range []string{"events.jsonl", "run.json"} {
		t.Run(path, func(t *testing.T) {
			policy := DefaultRunPolicy()
			policy.Runtime.CleanupTimeout = 5 * time.Second
			ready := make(chan Snapshot, 1)
			entered, releaseIO := make(chan struct{}), make(chan struct{})
			closed := make(chan error, 1)
			unblock := sync.OnceFunc(func() { close(releaseIO) })
			fake := &engDeadlineRuntime{engTestRuntime: &engTestRuntime{}, close: func(ctx context.Context, s runtime.Session) (runtime.CleanupReport, error) {
				report, err := s.Close(ctx)
				closed <- err
				return report, err
			}}
			r := engDeadlineNew(t, context.Background(), policy, fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				if _, err := run.OpenSession(ctx, engTestRole("idle")); err != nil {
					return Result{}, err
				}
				ready <- run.Snapshot()
				return Result{}, nil
			})
			r.beforeIO = func(ioPath, phase string) {
				if ioPath == path && phase == "RunFinalizing" {
					close(entered)
					<-releaseIO
				}
			}
			done := engTestExecuteAsync(t, r)
			t.Cleanup(unblock)
			snapshot := engDeadlineReceive(t, ready)
			if len(snapshot.Sessions) != 1 || len(snapshot.Attempts) != 0 {
				t.Fatalf("expected one established session without attempts: %+v", snapshot)
			}
			for _, session := range snapshot.Sessions {
				if session.State != "Idle" || session.Health != "Online" {
					t.Fatalf("session not idle before finalization: %+v", session)
				}
			}
			engDeadlineReceive(t, entered)
			if r.mu.TryLock() {
				r.mu.Unlock()
				t.Fatal("RunFinalizing IO barrier does not hold r.mu")
			}
			if err := engDeadlineReceive(t, closed); err != nil {
				t.Fatalf("runtime Close failed while finalization IO was blocked: %v", err)
			}
			// Join real cleanup before late cancellation can trigger any cleanup itself.
			engDeadlineReceive(t, r.cleanupDone)
			engDeadlineReceive(t, r.ctx.Done())
			calls, closes, confirms := fake.allSessions()[0].history()
			if len(calls) != 0 || closes != 1 || confirms != 0 {
				t.Fatalf("idle Execute/Close/Confirm before IO release = %d/%d/%d, want 0/1/0", len(calls), closes, confirms)
			}
			cancelled := make(chan struct{})
			go func() {
				r.Cancel(OriginSignalTERM)
				close(cancelled)
			}()
			engDeadlineReceive(t, cancelled)
			select {
			case report := <-done:
				t.Fatalf("Execute returned before finalization IO barrier release: %+v", report)
			default:
			}
			// Snapshot also needs r.mu; inspect outcome and events only after IO resumes.
			unblock()
			report := engDeadlineReceive(t, done)
			engTestReport(t, report, Succeeded, 0)
			snapshot = r.Snapshot()
			if snapshot.State != Succeeded || snapshot.WorkflowOutcome != Succeeded || snapshot.Failure != nil {
				t.Errorf("late Cancel changed locked outcome: %+v", snapshot)
			}
			engTestPersisted(t, r, report)
			engPersistClosed(t, fake.engTestRuntime, report, 1)
			counts := make(map[string]int)
			for _, event := range engTestEvents(t, r) {
				if event.Kind != "RunFinalizing" && event.Kind != "RunFinished" {
					continue
				}
				counts[event.Kind]++
				details, ok := event.Details.(map[string]any)
				if !ok || details["outcome"] != string(Succeeded) || details["failure"] != nil {
					t.Errorf("late Cancel changed finalization event: %+v", event)
				}
			}
			if counts["RunFinalizing"] != 1 || counts["RunFinished"] != 1 {
				t.Errorf("finalization event counts = %v, want one RunFinalizing and one RunFinished", counts)
			}
		})
	}
}

func TestEngineCancellationRetainsExactCause(t *testing.T) {
	for _, tc := range []struct {
		name     string
		origin   Origin
		exit     int
		external bool
	}{
		{"external-context-cause", OriginExternalAgentAbort, 130, true},
		{"controller", OriginControllerUser, 130, false},
		{"SIGINT", OriginSignalINT, 130, false},
		{"SIGTERM", OriginSignalTERM, 143, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			entered := make(chan struct{})
			var executeCause, stepErr error
			fake := &engTestRuntime{execute: func(ctx context.Context, _ engTestCall) engTestReply {
				close(entered)
				<-ctx.Done()
				executeCause = context.Cause(ctx)
				return engTestReply{Err: executeCause}
			}}
			r := engDeadlineNew(t, parent, DefaultRunPolicy(), fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				_, stepErr = engTestStep(ctx, run.Root(), h, "cancelled")
				return Result{}, stepErr
			})
			done := engTestExecuteAsync(t, r)
			engDeadlineReceive(t, entered)
			var cause error
			if tc.external {
				cause = fmt.Errorf("external boundary: %w", &Failure{Code: Cancelled, Origin: tc.origin, Message: "external abort", DispatchAccepted: AcceptedYes})
				cancel(cause)
			} else {
				r.Cancel(tc.origin)
				cause = context.Cause(r.ctx)
			}
			report := engDeadlineReceive(t, done)
			engTestReport(t, report, CancelledState, tc.exit)
			engTestPersisted(t, r, report)
			for name, err := range map[string]error{"Execute context": executeCause, "Step": stepErr, "Report": report.Failure} {
				engDeadlineFailure(t, err, Cancelled, tc.origin)
				if cause == nil || !errors.Is(err, cause) {
					t.Errorf("%s lost exact cancellation cause: %v, want %v", name, err, cause)
				}
			}
			for _, attempt := range report.Snapshot.Attempts {
				if attempt.State != CancelledState || attempt.Failure == nil || attempt.Failure.Origin != tc.origin {
					t.Errorf("cancelled attempt = %+v", attempt)
				}
			}
			engPersistClosed(t, fake, report, 1)
		})
	}
}

func TestEngineFirstRootStopSurvivesLaterStorageFailure(t *testing.T) {
	entered := make(chan struct{})
	var stepErr error
	fake := &engTestRuntime{execute: func(ctx context.Context, _ engTestCall) engTestReply {
		close(entered)
		<-ctx.Done()
		return engTestReply{Err: context.Cause(ctx)}
	}}
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		_, stepErr = engTestStep(ctx, run.Root(), h, "cancelled")
		return Result{}, stepErr
	})
	faultHit := false
	r.beforeIO = func(path, phase string) {
		if phase == "attempt_terminal" {
			faultHit = true
			if err := os.Mkdir(filepath.Join(r.Dir(), path), 0700); err != nil {
				t.Errorf("create real storage fault: %v", err)
			}
		}
	}
	done := engTestExecuteAsync(t, r)
	engDeadlineReceive(t, entered)
	r.Cancel(OriginSignalTERM)
	cause := context.Cause(r.ctx)
	report := engDeadlineReceive(t, done)
	if !faultHit || !engTestCode(stepErr, StorageFailed) {
		t.Fatalf("later storage failure not exercised: hit=%t err=%v", faultHit, stepErr)
	}
	if report.Outcome != CancelledState || report.ExitCode != 143 || !errors.Is(report.Failure, cause) {
		t.Errorf("first root stop overwritten: %+v; first cause=%v", report, cause)
	}
	engDeadlineFailure(t, report.Failure, Cancelled, OriginSignalTERM)
	if report.Snapshot.StatePersisted || len(report.FinalizationErrors) != 0 || len(report.CleanupErrors) != 0 {
		t.Errorf("pre-finalization storage failure classification: %+v", report)
	}
	var disk Snapshot
	if err := engTestReadJSON(filepath.Join(r.Dir(), "run.json"), &disk); err != nil {
		t.Fatal(err)
	}
	if disk.WorkflowOutcome != CancelledState || disk.Failure == nil || disk.Failure.Origin != OriginSignalTERM {
		t.Errorf("persisted root stop overwritten: %+v", disk)
	}
	engPersistClosed(t, fake, report, 1)
}

func TestEngineUnconfirmedCleanupRetainsLiveSlot(t *testing.T) {
	policy := DefaultRunPolicy()
	policy.MaxLiveSessions = 1
	fake := &engDeadlineRuntime{engTestRuntime: &engTestRuntime{}, close: func(ctx context.Context, s runtime.Session) (runtime.CleanupReport, error) {
		return runtime.CleanupReport{Identity: s.Identity(), Unconfirmed: []string{"process exit unconfirmed"}}, errors.New("cleanup unavailable")
	}}
	var closeErr, openErr error
	r := engDeadlineNew(t, context.Background(), policy, fake, func(ctx context.Context, r *Run, _ Input) (Result, error) {
		h, err := r.OpenSession(ctx, engTestRole("first"))
		if err != nil {
			return Result{}, err
		}
		closeErr = r.CloseSession(ctx, h)
		_, openErr = r.OpenSession(ctx, engTestRole("replacement"))
		return Result{}, nil
	})
	report := r.Execute()
	if closeErr == nil || !engTestCode(openErr, LimitExceeded) || report.Outcome != Failed || report.ExitCode != 1 || len(report.CleanupErrors) != 1 || len(fake.allSessions()) != 1 {
		t.Fatalf("unconfirmed process freed slot: close=%v open=%v report=%+v", closeErr, openErr, report)
	}
	for _, s := range report.Snapshot.Sessions {
		if s.State != "Unresponsive" {
			t.Errorf("unconfirmed session claimed %s", s.State)
		}
	}
}

func TestEngineDeadlineBeforeOutcomeLockRejectsSuccess(t *testing.T) {
	policy := DefaultRunPolicy()
	policy.RunTimeout = 300 * time.Millisecond
	r := engDeadlineNew(t, context.Background(), policy, &engTestRuntime{}, func(context.Context, *Run, Input) (Result, error) {
		return Result{}, nil
	})
	barrierHit := false
	r.beforeIO = func(path, phase string) {
		if path == "result.json" && phase == "result" {
			barrierHit = true
			<-r.ctx.Done()
		}
	}
	report := engDeadlineReceive(t, engTestExecuteAsync(t, r))
	engTestReport(t, report, TimedOutState, 1)
	engTestPersisted(t, r, report)
	engDeadlineFailure(t, report.Failure, TimedOut, OriginRunDeadline)
	if !barrierHit {
		t.Error("result persistence barrier was not reached")
	}
}

func TestEngineFinalizingLocksSuccessAcrossLateCancelAndDeadline(t *testing.T) {
	for _, lateCancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("late-cancel=%t", lateCancel), func(t *testing.T) {
			policy := DefaultRunPolicy()
			policy.RunTimeout = 2 * time.Second
			policy.Runtime.CleanupTimeout = 5 * time.Second
			closing, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			var originalDeadline context.Context
			fake := &engDeadlineRuntime{engTestRuntime: &engTestRuntime{}, close: func(ctx context.Context, s runtime.Session) (runtime.CleanupReport, error) {
				close(closing)
				select {
				case <-release:
				case <-ctx.Done():
					return runtime.CleanupReport{}, context.Cause(ctx)
				}
				// Run.ctx is cancelled on Finalizing even for success. A separate
				// timer is needed to prove cleanup crosses the original deadline.
				select {
				case <-originalDeadline.Done():
				case <-ctx.Done():
					return runtime.CleanupReport{}, context.Cause(ctx)
				}
				if err := context.Cause(ctx); err != nil {
					return runtime.CleanupReport{}, err
				}
				return s.Close(ctx)
			}}
			r := engDeadlineNew(t, context.Background(), policy, fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				step, err := engTestStep(ctx, run.Root(), h, "success")
				return engTestResult(step), err
			})
			var cancel context.CancelFunc
			originalDeadline, cancel = context.WithDeadline(context.Background(), r.deadline)
			defer cancel()
			done := engTestExecuteAsync(t, r)
			t.Cleanup(unblock)
			engDeadlineReceive(t, closing)
			snapshot := r.Snapshot()
			if snapshot.State != Finalizing || snapshot.WorkflowOutcome != Succeeded {
				t.Fatalf("cleanup began before success lock: %+v", snapshot)
			}
			if lateCancel {
				r.Cancel(OriginSignalTERM)
				if snapshot = r.Snapshot(); snapshot.State != Finalizing || snapshot.WorkflowOutcome != Succeeded || snapshot.Failure != nil {
					t.Errorf("late Cancel changed locked outcome: %+v", snapshot)
				}
			}
			unblock()
			report := engDeadlineReceive(t, done)
			engTestReport(t, report, Succeeded, 0)
			engTestPersisted(t, r, report)
			engPersistClosed(t, fake.engTestRuntime, report, 1)
			if originalDeadline.Err() != context.DeadlineExceeded || report.Snapshot.FinishedAt.Before(r.deadline) {
				t.Errorf("cleanup did not cross original deadline: finished=%s deadline=%s", report.Snapshot.FinishedAt, r.deadline)
			}
		})
	}
}
