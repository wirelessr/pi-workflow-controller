package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

// Options is construction-time wiring, not workflow configuration. A nil Runtime
// constructs the real Pi runtime with this run's policy and observation callback.
type Options struct {
	BaseDir           string
	Schemas           *contract.Registry
	Runtime           runtime.Runtime
	RuntimeOptions    runtime.Options
	ControllerVersion string
	PiVersion         string
	// PiDefaultCWD is used only when a role leaves CWD empty. Relative paths
	// are anchored to Input.LaunchCWD, without changing persisted input.
	PiDefaultCWD string
	// SyncFile is fixed at construction and shared with this run's Store.
	// Nil uses os.File.Sync. Callers must support concurrent file syncs.
	SyncFile func(*os.File) error
}
type publication struct {
	Ref      contract.Ref
	Identity contract.Identity
	Seq      uint64
}
type Run struct {
	mu                                   sync.Mutex
	definition                           Definition
	input                                Input
	piDefaultCWD                         string
	store                                *contract.Store
	schemas                              *contract.Registry
	fs                                   *os.Root
	syncFile                             func(*os.File) error
	runtime                              runtime.Runtime
	ctx                                  context.Context
	cancel                               context.CancelCauseFunc
	deadlineCancel                       context.CancelFunc
	deadline                             time.Time
	root                                 *Scope
	state                                Snapshot
	started, accepting, locked, finished bool
	// Stop arbitration and resource ownership never wait for persistence.
	stopMu                            sync.Mutex
	rootStop                          error
	outcomeLocked, observationsClosed bool
	pendingFatal                      error
	ownerMu                           sync.Mutex
	resourcesStopped                  bool
	journalBroken                     error
	journalBytes                      int64
	changed                           chan struct{}
	handles                           map[string]*SessionHandle
	totalAttempts                     int
	publications                      map[string]publication
	invocations                       map[string]string
	retryAncestors                    map[string][]string
	observations                      chan runtime.Observation
	observationStop                   chan struct{}
	observationDone                   chan struct{}
	workflowDone                      chan struct{}
	resourceCleanups                  []resourceCleanup
	cleanupOnce                       sync.Once
	cleanupDone                       chan struct{}
	cleanupReports                    []runtime.CleanupReport
	cleanupErrors                     []error
	finalErrors                       []error
	// Private deterministic barriers: tests change real filesystem paths, never writers.
	beforeIO     func(path, phase string)
	afterPublish func(contract.Ref)
}

func New(ctx context.Context, def Definition, input Input, opts Options) (*Run, error) {
	if _, err := NewRegistry([]Definition{def}); err != nil {
		return nil, err
	}
	if opts.Schemas == nil || !filepath.IsAbs(input.LaunchCWD) {
		return nil, newFailure(InvalidDefinition, "New", "compiled schemas and absolute launch cwd required")
	}
	if err := contract.ValidatePrompt(input.Prompt, def.Policy.MaxPromptBytes); err != nil {
		return nil, normalize(err, "New")
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	defaultCWD := opts.PiDefaultCWD
	// Preserve invalid paths for selected-role validation, including NUL
	// components that lexical cleaning could otherwise remove.
	if defaultCWD != "" && !strings.ContainsRune(defaultCWD, '\x00') && !filepath.IsAbs(defaultCWD) {
		defaultCWD = filepath.Join(input.LaunchCWD, defaultCWD)
	}
	if opts.SyncFile == nil {
		opts.SyncFile = (*os.File).Sync
	}
	created := time.Now()
	p := def.Policy
	store, err := contract.NewStore(opts.Schemas, contract.Options{SyncFile: opts.SyncFile, BaseDir: opts.BaseDir, Prompt: input.Prompt, LaunchCWD: input.LaunchCWD, Workflow: def.Name, WorkflowVersion: def.Version, ControllerVersion: opts.ControllerVersion, PiVersion: opts.PiVersion, Limits: contract.Limits{MaxPromptBytes: p.MaxPromptBytes, MaxCandidateBytes: p.MaxCandidateBytes, MaxJSONDepth: p.MaxJSONDepth, MaxFileBytes: p.MaxFileBytes, MaxAttemptFileBytes: p.MaxAttemptFileBytes, MaxAttemptFiles: p.MaxAttemptFiles}})
	if err != nil {
		return nil, normalize(err, "New")
	}
	fs, err := os.OpenRoot(store.Dir())
	if err != nil {
		_ = store.Close()
		return nil, normalize(err, "New")
	}
	var deadline time.Time
	timed, deadlineCancel := ctx, context.CancelFunc(func() {})
	if !p.DisableRunTimeout {
		deadline = created.Add(p.RunTimeout)
		deadlineCause := newFailure(TimedOut, "run", "run deadline exceeded")
		deadlineCause.Origin = OriginRunDeadline
		timed, deadlineCancel = context.WithDeadlineCause(ctx, deadline, deadlineCause)
	}
	life, cancel := context.WithCancelCause(timed)
	r := &Run{definition: def, input: input, store: store, schemas: opts.Schemas, fs: fs, syncFile: opts.SyncFile, ctx: life, cancel: cancel, deadlineCancel: deadlineCancel, deadline: deadline, accepting: true, changed: make(chan struct{}, 1), handles: make(map[string]*SessionHandle), publications: make(map[string]publication), invocations: make(map[string]string), retryAncestors: make(map[string][]string), observations: make(chan runtime.Observation, p.Runtime.ObservationQueue), observationStop: make(chan struct{}), observationDone: make(chan struct{}), cleanupDone: make(chan struct{})}
	r.piDefaultCWD = defaultCWD
	r.workflowDone = make(chan struct{})
	r.root = r.newScope("root", nil)
	r.state = Snapshot{Version: 1, TaskID: store.TaskID(), RunID: store.RunID(), Workflow: def.Name, WorkflowVersion: def.Version, ControllerVersion: opts.ControllerVersion, PiVersion: opts.PiVersion, Input: input, Policy: p, CreatedAt: created, State: Created, StatePersisted: true, Attempts: make(map[string]AttemptState), Invocations: make(map[string]InvocationState), Sessions: make(map[string]SessionStatus), Retries: make(map[string]RetryStatus)}
	r.runtime = opts.Runtime
	if r.runtime == nil {
		ro := opts.RuntimeOptions
		ro.Policy = p.Runtime
		ro.Observe = r.Observe
		r.runtime, err = runtime.New(ro)
	}
	if err == nil {
		var f *os.File
		f, err = fs.OpenFile("events.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			err = f.Close()
		}
	}
	if err == nil {
		err = r.commitLocked("RunCreated", nil, func(uint64) {})
	}
	if err != nil {
		cancel(err)
		deadlineCancel()
		_ = fs.Close()
		_ = store.Close()
		return nil, normalize(err, "New")
	}
	return r, nil
}
func (r *Run) Dir() string { return r.store.Dir() }
func (r *Run) ID() string  { return r.store.RunID() }
func (r *Run) Root() *Scope {
	if r == nil {
		return nil
	}
	return r.root
}
func (r *Run) Cancel(origin Origin) {
	f := newFailure(Cancelled, "run", "controller cancelled run")
	f.Origin = origin
	r.stopLocked(f)
}

// The name denotes the control-sequence caller; arbitration has its own short lock.
func (r *Run) stopLocked(err error) {
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	r.registerStop(err)
}
func (r *Run) registerStop(err error) {
	if r.outcomeLocked || r.rootStop != nil || err == nil {
		return
	}
	if cause := context.Cause(r.ctx); cause != nil {
		err = cause
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		f := newFailure(Cancelled, "run", "parent context cancelled")
		f.Origin = OriginControllerUser
		if err == context.DeadlineExceeded {
			f.Code = TimedOut
			f.Origin = OriginRunDeadline
		}
		f.Cause = err
		err = f
	}
	r.rootStop = err
	r.cancel(err)
}
func (r *Run) rootCause() error {
	r.stopMu.Lock()
	defer r.stopMu.Unlock()
	return r.rootStop
}
func (r *Run) checkLocked(ctx context.Context) error {
	if r.store == nil {
		return newFailure(InvalidDefinition, "run", "run is not initialized")
	}
	if cause := r.rootCause(); cause != nil {
		return cause
	}
	if !r.accepting || !r.started || r.locked {
		return newFailure(InvalidDefinition, "run", "workflow APIs are not active")
	}
	if cause := context.Cause(r.ctx); cause != nil {
		r.stopLocked(cause)
		return r.rootCause()
	}
	return context.Cause(ctx)
}
func (r *Run) operationContext(ctx context.Context) (context.Context, func()) {
	c, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(r.ctx, func() { cancel(context.Cause(r.ctx)) })
	if cause := context.Cause(r.ctx); cause != nil {
		cancel(cause)
	}
	return c, func() { stop(); cancel(context.Canceled) }
}
func (r *Run) recordError(err error) error {
	if err == nil {
		return nil
	}
	f := normalize(err, "")
	if fatal(f) {
		r.stopLocked(f)
	}
	return f
}

// Execute is single-use and joins all engine-owned work before closing persistence.
func (r *Run) Execute() Report {
	r.mu.Lock()
	if r.store == nil || r.started {
		r.mu.Unlock()
		e := newFailure(InvalidDefinition, "Execute", "run is uninitialized or already executed")
		return Report{Outcome: Failed, ExitCode: 1, Failure: e}
	}
	r.started = true
	err := r.commitLocked("RunStarted", nil, func(uint64) { r.state.State = Running })
	r.mu.Unlock()
	go r.observeLoop()
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-r.ctx.Done():
			r.stopLocked(context.Cause(r.ctx))
			r.beginCleanup()
		case <-watchDone:
		}
	}()
	var result Result
	if err == nil && context.Cause(r.ctx) == nil {
		result, err = r.definition.Execute(r.ctx, r, r.input)
	}
	close(r.workflowDone)
	r.mu.Lock()
	r.accepting = false
	r.mu.Unlock()
	var final *FinalDelivery
	if r.rootCause() == nil && context.Cause(r.ctx) == nil && !fatal(err) {
		if err == nil {
			final, err = r.validateResult(r.ctx, result)
		} else {
			// A failed review may still explicitly deliver a valid incomplete report.
			var deliveryErr error
			final, deliveryErr = r.resolveFinal(r.ctx, result)
			if deliveryErr != nil {
				err = errors.Join(err, deliveryErr)
			}
		}
	}
	r.mu.Lock()
	if cause := context.Cause(r.ctx); cause != nil {
		r.stopLocked(cause)
	}
	if r.rootCause() == nil && err == nil {
		err = r.storageLocked("result.json", "result", struct {
			RunID   string                  `json:"run_id"`
			Outputs map[string]contract.Ref `json:"outputs"`
			Final   *FinalDelivery          `json:"final,omitempty"`
		}{r.ID(), result.Outputs, final})
	}
	if cause := context.Cause(r.ctx); cause != nil {
		r.stopLocked(cause)
	}
	if !r.deadline.IsZero() && !time.Now().Before(r.deadline) && r.rootCause() == nil {
		f := newFailure(TimedOut, "run", "run deadline exceeded")
		f.Origin = OriginRunDeadline
		r.stopLocked(f)
	}
	r.stopMu.Lock()
	r.registerStop(context.Cause(r.ctx))
	if r.rootStop != nil {
		err = r.rootStop
	}
	chosen, exit := outcome(err)
	r.locked = true
	r.outcomeLocked = true
	r.stopMu.Unlock()
	r.state.WorkflowOutcome = chosen
	r.state.Failure = failureInfo(err)
	// Lock the outcome, then stop resources before potentially blocking I/O.
	r.cancel(err)
	r.beginCleanup()
	_ = r.commitLocked("RunFinalizing", map[string]any{"outcome": chosen, "failure": failureInfo(err)}, func(uint64) { r.state.State = Finalizing })
	r.mu.Unlock()
	<-r.cleanupDone
	close(watchDone)
	r.stopMu.Lock()
	r.observationsClosed = true
	r.stopMu.Unlock()
	close(r.observationStop)
	<-r.observationDone
	r.mu.Lock()
	for _, h := range r.ownedHandles() {
		_ = r.closedStateLocked(h)
	}
	for _, e := range r.cleanupErrors {
		r.state.CleanupErrors = append(r.state.CleanupErrors, failureInfo(e))
	}
	r.stopMu.Lock()
	if r.pendingFatal != nil {
		r.finalErrorLocked(r.pendingFatal)
	}
	r.pendingFatal = nil
	r.stopMu.Unlock()
	if chosen != Succeeded {
		_ = r.storageLocked("result.json", "failed_result", struct {
			RunID   string                  `json:"run_id"`
			Outputs map[string]contract.Ref `json:"outputs"`
			Final   *FinalDelivery          `json:"final,omitempty"`
		}{r.ID(), result.Outputs, final})
	}
	_ = r.storageLocked("cleanup.json", "cleanup", struct {
		Reports []runtime.CleanupReport `json:"reports"`
		Errors  []*FailureInfo          `json:"errors"`
	}{r.cleanupReports, r.state.CleanupErrors})
	finished := time.Now()
	finishErr := r.commitLocked("RunFinished", map[string]any{"outcome": chosen, "cleanup_errors": r.state.CleanupErrors, "finalization_errors": r.state.FinalizationErrors}, func(uint64) { r.state.State = chosen; r.state.FinishedAt = finished })
	if finishErr != nil {
		r.state.State = chosen
		r.state.FinishedAt = finished
	}
	r.finished = true
	if (len(r.cleanupErrors) > 0 || len(r.finalErrors) > 0) && exit == 0 {
		exit = 1
	}
	report := Report{Final: final, Outcome: chosen, ExitCode: exit, Result: result, Failure: err, Cleanup: append([]runtime.CleanupReport(nil), r.cleanupReports...), CleanupErrors: append([]error(nil), r.cleanupErrors...), FinalizationErrors: append([]error(nil), r.finalErrors...), Snapshot: r.snapshotLocked()}
	r.notifyLocked()
	r.mu.Unlock()
	r.deadlineCancel()
	closeErr := errors.Join(r.fs.Close(), r.store.Close())
	if closeErr != nil {
		report.FinalizationErrors = append(report.FinalizationErrors, closeErr)
		if report.ExitCode == 0 {
			report.ExitCode = 1
		}
	}
	return report
}
