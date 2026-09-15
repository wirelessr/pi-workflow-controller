package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Options struct {
	Executable string
	Args       []string
	Env        []string
	BridgeDir  string
	Policy     Policy
	Observe    Observer
}
type Pi struct{ options Options }

func New(options Options) (*Pi, error) {
	if options.Executable == "" {
		options.Executable = "pi"
	}
	if options.Policy == (Policy{}) {
		options.Policy = DefaultPolicy()
	}
	if !options.Policy.valid() {
		return nil, failure(InvalidDefinition, "runtime policy requires positive limits")
	}
	if options.BridgeDir == "" {
		options.BridgeDir = os.Getenv("PI_BRIDGE_DIR")
	}
	if options.BridgeDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		options.BridgeDir = filepath.Join(home, ".pi/agent/extensions/pi-webui-extension/data")
	}
	options.Args = append([]string(nil), options.Args...)
	options.Env = append([]string(nil), options.Env...)
	return &Pi{options: options}, nil
}
func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type session struct {
	options                                                      Options
	spec                                                         SessionSpec
	cmd                                                          *exec.Cmd
	in, out, errPipe                                             *os.File
	stderrFile                                                   *os.File
	mu                                                           sync.Mutex
	id                                                           Identity
	seq, epoch                                                   uint64
	lastResponse                                                 time.Time
	fatal                                                        *Failure
	closing                                                      bool
	stdoutEnded                                                  bool
	ready                                                        bool
	active, compacting, retrying                                 bool
	tools                                                        map[string]bool
	queue                                                        int
	trace                                                        *dispatchTrace
	entryCursor                                                  string
	receipt                                                      *Execution
	usedTokens                                                   map[string]bool
	pending                                                      map[string]pendingRequest
	writeLease                                                   chan struct{}
	executeLease                                                 chan struct{}
	changed                                                      chan struct{}
	failed                                                       chan struct{}
	processDone, readerDone, stderrDone, observerDone, probeDone chan struct{}
	waitErr                                                      error
	obs                                                          chan Observation
	obsLife                                                      context.Context
	obsCancel                                                    context.CancelFunc
	observationsClosed                                           bool
	life                                                         context.Context
	cancel                                                       context.CancelFunc
	closeOnce                                                    sync.Once
	closeDone                                                    chan struct{}
	report                                                       CleanupReport
	closeErr                                                     error
	stderrTail                                                   []byte
	stderrWritten                                                int
	stderrTruncated                                              bool
	dialogDone                                                   chan struct{}
	stderrErr                                                    error
}

func (p *Pi) Start(ctx context.Context, spec SessionSpec) (Session, error) {
	if spec.HandleID == "" || spec.Model.Provider == "" || spec.Model.ID == "" || spec.Model.Thinking == "" || !filepath.IsAbs(spec.CWD) || !filepath.IsAbs(spec.SessionDir) {
		return nil, failure(InvalidDefinition, "session requires handle, explicit model/thinking and absolute directories")
	}
	startCtx, cancel := context.WithTimeout(ctx, p.options.Policy.StartupTimeout)
	defer cancel()
	phase := "version"
	startupError := func(err error) *Failure {
		parentEnded := ctx.Err() != nil
		if parentEnded {
			err = contextError(ctx)
		} else if startCtx.Err() != nil {
			code := StartFailed
			if phase == "discovery" {
				code = BridgeUnavailable
			}
			err = &Failure{Code: code, Message: "Pi startup deadline exceeded", Cause: context.Cause(startCtx), Origin: Protocol}
		}
		var f *Failure
		if !errors.As(err, &f) {
			f = &Failure{Code: StartFailed, Message: "Pi startup failed", Cause: err, Origin: Protocol}
		}
		copy := *f
		if parentEnded {
			// Keep the original typed cause reachable after adding startup metadata.
			copy.Cause = err
		}
		if copy.Phase == "" {
			copy.Phase = phase
		}
		copy.HandleID = spec.HandleID
		copy.DispatchAccepted = AcceptedNo
		return &copy
	}
	// A bounded version probe precedes any persistent Pi startup.
	version := exec.CommandContext(startCtx, p.options.Executable, append(append([]string{}, p.options.Args...), "--version")...)
	version.Env = childEnv(p.options)
	version.WaitDelay = p.options.Policy.CleanupTimeout
	vb := &limitedBuffer{limit: 4096}
	version.Stdout = vb
	version.Stderr = vb
	if err := version.Run(); err != nil {
		return nil, startupError(&Failure{Code: StartFailed, Message: "Pi version probe failed", Cause: err, Origin: Protocol})
	}
	if startCtx.Err() != nil {
		return nil, startupError(context.Cause(startCtx))
	}
	if strings.TrimSpace(string(vb.data)) != "0.84.3" {
		return nil, startupError(failure(UnsupportedPiVersion, "only Pi 0.84.3 is supported"))
	}
	phase = "spawn"
	if err := os.MkdirAll(spec.SessionDir, 0700); err != nil {
		return nil, startupError(err)
	}
	sf, err := os.OpenFile(filepath.Join(filepath.Dir(spec.SessionDir), "stderr.log"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, startupError(err)
	}
	life, stop := context.WithCancel(context.Background())
	obsLife, obsCancel := context.WithCancel(context.Background())
	s := &session{options: p.options, spec: spec, stderrFile: sf, life: life, cancel: stop, obsLife: obsLife, obsCancel: obsCancel,
		pending: make(map[string]pendingRequest), tools: make(map[string]bool), usedTokens: make(map[string]bool),
		writeLease: make(chan struct{}, 1), executeLease: make(chan struct{}, 1), changed: make(chan struct{}), failed: make(chan struct{}),
		processDone: make(chan struct{}), readerDone: make(chan struct{}), stderrDone: make(chan struct{}), observerDone: make(chan struct{}), probeDone: make(chan struct{}), closeDone: make(chan struct{}), obs: make(chan Observation, p.options.Policy.ObservationQueue+1)}
	s.writeLease <- struct{}{}
	s.executeLease <- struct{}{}
	args := append([]string{}, p.options.Args...)
	args = append(args, "--mode", "rpc", "--provider", spec.Model.Provider, "--model", spec.Model.ID, "--thinking", spec.Model.Thinking, "--session-dir", spec.SessionDir, "--name", spec.Name)
	if spec.AppendPrompt != "" {
		args = append(args, "--append-system-prompt", spec.AppendPrompt)
	}
	s.cmd = exec.Command(p.options.Executable, args...)
	s.cmd.Dir = spec.CWD
	s.cmd.Env = childEnv(p.options)
	s.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var childIn, childOut, childErr *os.File
	childIn, s.in, err = os.Pipe()
	if err == nil {
		s.out, childOut, err = os.Pipe()
	}
	if err == nil {
		s.errPipe, childErr, err = os.Pipe()
	}
	if err != nil {
		for _, f := range []*os.File{childIn, s.in, s.out, childOut, s.errPipe, childErr, sf} {
			if f != nil {
				_ = f.Close()
			}
		}
		stop()
		obsCancel()
		return nil, startupError(err)
	}
	s.cmd.Stdin = childIn
	s.cmd.Stdout = childOut
	s.cmd.Stderr = childErr
	err = startCtx.Err()
	if err == nil {
		err = s.cmd.Start()
	}
	_ = childIn.Close()
	_ = childOut.Close()
	_ = childErr.Close()
	if err != nil {
		_ = s.in.Close()
		_ = s.out.Close()
		_ = s.errPipe.Close()
		_ = sf.Close()
		stop()
		obsCancel()
		return nil, startupError(&Failure{Code: StartFailed, Message: "Pi spawn failed", Cause: err, Origin: Protocol})
	}
	s.id = Identity{HandleID: spec.HandleID, PID: s.cmd.Process.Pid, PGID: s.cmd.Process.Pid, ParentPID: os.Getpid(), SpawnTime: time.Now(), Executable: s.cmd.Path}
	go s.readLoop()
	go s.drainStderr()
	go s.observeLoop()
	// Close retains the unreaped PID until after signalling the owned process group.
	// Reaping in a background waiter would race numeric PGID reuse with SIGKILL.
	go s.probeLoop()
	failStart := func(err error) (Session, error) {
		f := startupError(err)
		report, _ := s.Close(context.Background())
		f.StderrTail = report.StderrTail
		f.Cleanup = &report
		return nil, f
	}
	phase = "readiness"
	state, err := s.snapshot(startCtx, true)
	if err != nil {
		return failStart(err)
	}
	s.mu.Lock()
	s.id.SessionID = state.Identity.SessionID
	s.id.SessionFile = state.Identity.SessionFile
	s.mu.Unlock()
	owner, err := json.Marshal(s.Identity())
	if err != nil {
		return failStart(err)
	}
	if err = os.WriteFile(filepath.Join(filepath.Dir(spec.SessionDir), "owner.json"), owner, 0600); err != nil {
		return failStart(err)
	}
	phase = "discovery"
	for {
		if startCtx.Err() != nil {
			return failStart(context.Cause(startCtx))
		}
		visible, err := s.discoveryVisible()
		if err != nil {
			return failStart(err)
		}
		if visible {
			break
		}
		if err = s.pause(startCtx, 50*time.Millisecond); err != nil {
			return failStart(&Failure{Code: BridgeUnavailable, Message: "owned discovery was not observed", Cause: err, Origin: Protocol, DispatchAccepted: AcceptedNo})
		}
	}
	s.mu.Lock()
	s.ready = true
	s.emitLocked("SessionReady")
	s.mu.Unlock()
	return s, nil
}
func childEnv(o Options) []string {
	env := append(os.Environ(), o.Env...)
	result := make([]string, 0, len(env)+1)
	for _, v := range env {
		if !strings.HasPrefix(v, "PI_HTTP_PORT=") && !strings.HasPrefix(v, "PI_BRIDGE_DIR=") {
			result = append(result, v)
		}
	}
	return append(result, "PI_BRIDGE_DIR="+o.BridgeDir)
}

// exec may write stdout and stderr concurrently during the version probe.
type limitedBuffer struct {
	mu    sync.Mutex
	data  []byte
	limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := min(len(p), b.limit-len(b.data))
	b.data = append(b.data, p[:n]...)
	return len(p), nil
}
func (s *session) Identity() Identity { s.mu.Lock(); defer s.mu.Unlock(); return s.id }
func (s *session) wakeLocked()        { close(s.changed); s.changed = make(chan struct{}) }
func (s *session) failLocked(f *Failure) {
	if s.fatal != nil {
		return
	}
	f.HandleID = s.spec.HandleID
	s.fatal = f
	if s.options.Observe != nil && !s.observationsClosed {
		select {
		case s.obs <- Observation{HandleID: s.spec.HandleID, Kind: "RuntimeFailed", Seq: s.seq, ActivityEpoch: s.epoch, Time: time.Now(), Failure: f}:
		default:
		}
	}
	if f.Code == RPCUnresponsive || f.Code == ProcessExited {
		s.emitLocked("HealthChanged")
	}
	close(s.failed)
	s.wakeLocked()
	if !s.closing {
		go func() { _, _ = s.Close(context.Background()) }()
	}
}
func (s *session) emitLocked(kind string) {
	if s.options.Observe == nil || s.observationsClosed {
		return
	}
	o := Observation{HandleID: s.spec.HandleID, Kind: kind, Seq: s.seq, ActivityEpoch: s.epoch, Time: time.Now()}
	if s.trace != nil {
		o.DispatchToken = s.trace.token
	}
	if len(s.obs) < s.options.Policy.ObservationQueue {
		s.obs <- o
		return
	}
	f := failure(LimitExceeded, "runtime observation queue exhausted")
	f.LimitScope = "run"
	s.failLocked(f)
}
func (s *session) observeLoop() {
	defer close(s.observerDone)
	for {
		select {
		case <-s.obsLife.Done():
			return
		case o, ok := <-s.obs:
			if !ok {
				return
			}
			if s.options.Observe != nil {
				if err := s.options.Observe(s.obsLife, o); err != nil {
					if s.obsLife.Err() != nil {
						return
					}
					s.mu.Lock()
					f := failure(JournalFailed, "runtime observer failed")
					f.Cause = err
					s.failLocked(f)
					s.mu.Unlock()
					return
				}
			}
		}
	}
}
func (s *session) probeLoop() {
	defer close(s.probeDone)
	t := time.NewTicker(s.options.Policy.HealthInterval)
	defer t.Stop()
	for {
		select {
		case <-s.life.Done():
			return
		case <-t.C:
			s.mu.Lock()
			probe := s.ready && !s.closing && time.Since(s.lastResponse) >= s.options.Policy.HealthInterval
			s.mu.Unlock()
			if probe {
				_, err := s.Snapshot(s.life)
				if err != nil {
					return
				}
			}
		}
	}
}
func (s *session) pause(ctx context.Context, d time.Duration) error {
	s.mu.Lock()
	changed := s.changed
	fatal := s.fatal
	s.mu.Unlock()
	if fatal != nil {
		return fatal
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return contextError(ctx)
	case <-s.failed:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.fatal
	case <-changed:
		return nil
	case <-t.C:
		return nil
	}
}
func contextError(ctx context.Context) error {
	cause := context.Cause(ctx)
	var f *Failure
	if errors.As(cause, &f) {
		return cause
	}
	// An untyped cancellation does not establish Controller or external abort intent.
	code := InvalidDefinition
	origin := Definition
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		code = TimedOut
		origin = AttemptDeadline
	}
	return &Failure{Code: code, Message: "runtime operation context ended", Cause: cause, Origin: origin, DispatchAccepted: AcceptedUnknown}
}
func (s *session) drainStderr() {
	defer close(s.stderrDone)
	defer func(f *os.File) { _ = f.Close() }(s.stderrFile)
	buf := make([]byte, 32<<10)
	for {
		n, err := s.errPipe.Read(buf)
		if n > 0 {
			s.mu.Lock()
			remain := max(0, s.options.Policy.MaxStderrBytes-s.stderrWritten)
			write := min(n, remain)
			s.stderrWritten += write
			if write < n {
				s.stderrTruncated = true
			}
			tail := append(s.stderrTail, buf[:n]...)
			if len(tail) > s.options.Policy.StderrTailBytes {
				tail = tail[len(tail)-s.options.Policy.StderrTailBytes:]
			}
			s.stderrTail = append([]byte(nil), tail...)
			s.mu.Unlock()
			if write > 0 {
				if _, e := s.stderrFile.Write(buf[:write]); e != nil {
					s.mu.Lock()
					s.stderrErr = e
					s.mu.Unlock()
				}
			}
		}
		if err != nil {
			return
		}
	}
}
func (s *session) readLoop() {
	defer close(s.readerDone)
	err := readFrames(s.out, s.options.Policy.MaxFrameBytes, s.consume)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stdoutEnded = errors.Is(err, io.EOF)
	if s.closing {
		return
	}
	var f *Failure
	if errors.As(err, &f) {
		s.failLocked(f)
	} else {
		code := ProtocolFailed
		if errors.Is(err, io.EOF) {
			code = ProcessExited
		}
		f = failure(code, fmt.Sprintf("RPC stream ended: %v", err))
		f.Cause = err
		s.failLocked(f)
	}
}
