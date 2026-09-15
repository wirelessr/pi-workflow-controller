package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type discovery struct {
	SessionID   string `json:"sessionId"`
	SessionFile string `json:"sessionFile"`
	PID         int    `json:"pid"`
	PiPID       int    `json:"piPid"`
}

func readDiscovery(path string) (discovery, error) {
	var d discovery
	info, err := os.Lstat(path)
	if err != nil {
		return d, err
	}
	if !info.Mode().IsRegular() {
		return d, fmt.Errorf("discovery is not a regular file: %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return d, err
	}
	defer func(f *os.File) { _ = f.Close() }(f)
	b, err := io.ReadAll(io.LimitReader(f, 64<<10+1))
	if err != nil {
		return d, err
	}
	if len(b) > 64<<10 {
		return d, errors.New("discovery exceeds limit")
	}
	err = json.Unmarshal(b, &d)
	return d, err
}
func inside(dir, path string) bool {
	r, err := filepath.Rel(dir, path)
	return err == nil && r != "." && r != ".." && !strings.HasPrefix(r, ".."+string(os.PathSeparator)) && !filepath.IsAbs(r)
}
func owned(d discovery, id Identity, dir string) bool {
	if d.PiPID != id.PID || d.PID != id.ParentPID || !inside(dir, d.SessionFile) {
		return false
	}
	if id.SessionID != "" {
		return d.SessionID == id.SessionID && d.SessionFile == id.SessionFile
	}
	return d.SessionID != ""
}
func (s *session) discoveryVisible() (bool, error) {
	id := s.Identity()
	if filepath.Base(id.SessionID) != id.SessionID || id.SessionID == "." || id.SessionID == ".." || !inside(s.spec.SessionDir, id.SessionFile) {
		return false, failure(SessionChanged, "session identity is outside owned directory")
	}
	path := filepath.Join(s.options.BridgeDir, id.SessionID+".json")
	d, err := readDiscovery(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, nil
	} // The extension may be midway through its non-atomic write.
	return owned(d, id, s.spec.SessionDir), nil
}
func (s *session) removeDiscovery(ctx context.Context, report *CleanupReport) {
	if ctx.Err() != nil {
		report.DiscoveryError = "discovery cleanup deadline exceeded"
		return
	}
	id := s.Identity()
	if id.SessionID != "" {
		if filepath.Base(id.SessionID) != id.SessionID || id.SessionID == "." || id.SessionID == ".." {
			report.DiscoveryError = "unsafe session ID"
			return
		}
		path := filepath.Join(s.options.BridgeDir, id.SessionID+".json")
		if _, err := os.Lstat(path + ".recovering"); err == nil {
			report.DiscoveryError = "owned discovery has a recovery claim"
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			report.DiscoveryError = err.Error()
			return
		}
		d, err := readDiscovery(path)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		if err != nil {
			report.DiscoveryError = err.Error()
			return
		}
		if !owned(d, id, s.spec.SessionDir) {
			report.DiscoveryError = "discovery ownership mismatch"
			return
		}
		if err = os.Remove(path); err != nil {
			report.DiscoveryError = err.Error()
		} else {
			report.DiscoveryRemoved = append(report.DiscoveryRemoved, path)
		}
		return
	}
	// Before identity readiness, only the child PID + parent PID + run directory can establish ownership.
	dir, err := os.Open(s.options.BridgeDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			report.DiscoveryError = err.Error()
		}
		return
	}
	defer func(dir *os.File) { _ = dir.Close() }(dir)
	for {
		if ctx.Err() != nil {
			report.DiscoveryError = "discovery cleanup deadline exceeded"
			return
		}
		names, err := dir.Readdirnames(128)
		for _, name := range names {
			if !strings.HasSuffix(name, ".json") && !strings.HasSuffix(name, ".json.recovering") {
				continue
			}
			path := filepath.Join(s.options.BridgeDir, name)
			d, e := readDiscovery(path)
			if e != nil {
				if len(report.Unconfirmed) >= s.options.Policy.ObservationQueue {
					report.DiscoveryError = "discovery diagnostic limit exhausted"
					return
				}
				report.Unconfirmed = append(report.Unconfirmed, "could not inspect startup discovery: "+name)
				continue
			}
			if !owned(d, id, s.spec.SessionDir) {
				continue
			}
			if strings.HasSuffix(name, ".recovering") {
				report.DiscoveryError = "owned discovery has a recovery claim"
				continue
			}
			if e = os.Remove(path); e != nil {
				report.DiscoveryError = e.Error()
			} else {
				report.DiscoveryRemoved = append(report.DiscoveryRemoved, path)
			}
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			report.DiscoveryError = err.Error()
			return
		}
	}
}
func (s *session) Close(ctx context.Context) (CleanupReport, error) {
	s.closeOnce.Do(func() { go s.closeOwned(ctx) })
	// The first caller supplies the common budget; each caller can stop waiting at its own deadline.
	select {
	case <-s.closeDone:
	case <-ctx.Done():
		r := CleanupReport{Identity: s.Identity(), Unconfirmed: []string{"Close outcome not available before caller deadline"}}
		return r, &Failure{Code: CleanupFailed, Message: r.Unconfirmed[0], Cause: context.Cause(ctx), Origin: Protocol, DispatchAccepted: AcceptedUnknown}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.report
	r.Unconfirmed = append([]string(nil), r.Unconfirmed...)
	r.DiscoveryRemoved = append([]string(nil), r.DiscoveryRemoved...)
	return r, s.closeErr
}
func (s *session) closeOwned(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, s.options.Policy.CleanupTimeout)
	defer cancel()
	stopObserverCancel := context.AfterFunc(ctx, s.obsCancel)
	defer func() { stopObserverCancel(); s.obsCancel() }()
	s.mu.Lock()
	s.closing = true
	s.wakeLocked()
	s.mu.Unlock()
	s.cancel()
	report := CleanupReport{Identity: s.Identity()}
	s.mu.Lock()
	dialogDone := s.dialogDone
	s.mu.Unlock()
	if dialogDone != nil {
		select {
		case <-dialogDone:
		case <-ctx.Done():
			report.Unconfirmed = append(report.Unconfirmed, "dialog cancellation not written")
		}
	}
	s.mu.Lock()
	stdoutEnded := s.stdoutEnded
	s.mu.Unlock()
	if !stdoutEnded {
		abortCtx, abortCancel := context.WithTimeout(ctx, s.options.Policy.AbortGrace)
		type result struct {
			command string
			ok      bool
		}
		results := make(chan result, 2)
		for _, command := range []string{"abort", "abort_bash"} {
			go func(command string) {
				r, e := s.request(abortCtx, command, nil, s.options.Policy.AbortGrace, true)
				results <- result{command: command, ok: e == nil && r.frame.Success != nil && *r.frame.Success}
			}(command)
		}
		for i := 0; i < 2; i++ {
			r := <-results
			if r.command == "abort" {
				report.AbortAcknowledged = r.ok
			} else {
				report.AbortBashAcknowledged = r.ok
			}
		}
		abortCancel()
		if !report.AbortAcknowledged {
			report.Unconfirmed = append(report.Unconfirmed, "abort not acknowledged")
		}
		if !report.AbortBashAcknowledged {
			report.Unconfirmed = append(report.Unconfirmed, "abort_bash not acknowledged")
		}
	}
	// Do not signal a numeric PID after Wait has released process ownership.
	s.mu.Lock()
	if s.compacting {
		report.Unconfirmed = append(report.Unconfirmed, "compaction cancellation unconfirmed")
	}
	if len(s.tools) > 0 {
		report.Unconfirmed = append(report.Unconfirmed, "active tool termination unconfirmed")
	}
	if err := syscall.Kill(-s.id.PGID, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		report.KillError = err.Error()
	} else {
		report.SIGKILL = err == nil
	}
	s.mu.Unlock()
	// closeOnce guarantees this is the only Wait, and it begins only after the last signal.
	go func() { err := s.cmd.Wait(); s.mu.Lock(); s.waitErr = err; close(s.processDone); s.mu.Unlock() }()
	_ = s.in.Close()
	select {
	case <-s.processDone:
		report.WaitCompleted = true
		report.ProcessExited = true
	case <-ctx.Done():
		report.Unconfirmed = append(report.Unconfirmed, "Wait did not complete before cleanup deadline")
	}
	if report.WaitCompleted {
		s.removeDiscovery(ctx, &report)
	} else {
		report.Unconfirmed = append(report.Unconfirmed, "discovery retained because process exit is unconfirmed")
	}
	// Let buffered diagnostics drain before closing inherited descriptors; descendants may keep them open.
	select {
	case <-s.readerDone:
	case <-ctx.Done():
	}
	_ = s.out.Close()
	select {
	case <-s.stderrDone:
	case <-ctx.Done():
	}
	_ = s.errPipe.Close()
	s.mu.Lock()
	s.emitLocked("SessionClosed")
	s.observationsClosed = true
	close(s.obs)
	s.mu.Unlock()
	// Reserve most of the remaining budget for cancellation and worker joins.
	deadline, _ := ctx.Deadline()
	drainCtx, drainCancel := context.WithTimeout(ctx, max(0, time.Until(deadline)/4))
	select {
	case <-s.observerDone:
	case <-drainCtx.Done():
		report.Unconfirmed = append(report.Unconfirmed, "observer delivery unconfirmed before drain deadline")
	}
	drainCancel()
	s.obsCancel()
	for _, worker := range []struct {
		name string
		done <-chan struct{}
	}{{"reader", s.readerDone}, {"stderr", s.stderrDone}, {"probe", s.probeDone}, {"observer", s.observerDone}} {
		select {
		case <-worker.done:
		case <-ctx.Done():
			report.Unconfirmed = append(report.Unconfirmed, worker.name+" worker did not stop before cleanup deadline")
		}
	}
	s.mu.Lock()
	waitErr := s.waitErr
	report.StderrTail = string(s.stderrTail)
	report.StderrTruncated = s.stderrTruncated
	if s.stderrErr != nil {
		report.Unconfirmed = append(report.Unconfirmed, "stderr log write failed: "+s.stderrErr.Error())
	}
	s.mu.Unlock()
	if report.WaitCompleted && waitErr != nil {
		expected := false
		// Wait's expected SIGKILL status is recorded as process termination, not a cleanup failure.
		if s.cmd.ProcessState != nil {
			if ws, ok := s.cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
				expected = ws.Signaled() && ws.Signal() == syscall.SIGKILL
			}
		}
		if !expected {
			report.WaitError = waitErr.Error()
		}
	}
	var err error
	if report.KillError != "" || report.DiscoveryError != "" || len(report.Unconfirmed) > 0 {
		err = failure(CleanupFailed, "owned process cleanup has unconfirmed or failed operations")
	}
	s.mu.Lock()
	s.report = report
	s.closeErr = err
	close(s.closeDone)
	s.mu.Unlock()
}
