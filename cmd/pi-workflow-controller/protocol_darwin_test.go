//go:build darwin

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/testutil/protocol"
)

type cliPTY struct {
	master, slave *os.File
	before        unix.Termios
	mu            sync.Mutex
	output        bytes.Buffer
	reportTermios *unix.Termios
	err           error
	stop, done    chan struct{}
	stopOnce      sync.Once
	collectOnce   sync.Once
}

func newCLIPTY(t *testing.T, drain ...bool) *cliPTY {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = master.Close() })
	fd := int(master.Fd())
	for _, request := range []uint{unix.TIOCPTYGRANT, unix.TIOCPTYUNLK} {
		if err := unix.IoctlSetInt(fd, request, 0); err != nil {
			t.Fatal(err)
		}
	}
	// x/sys has no Darwin PTY-name wrapper. Keep this ioctl buffer test-only.
	var name [128]byte
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.TIOCPTYGNAME, uintptr(unsafe.Pointer(&name[0]))); errno != 0 { //nolint:staticcheck // SA1019: x/sys v0.45.0 has no Darwin PTY-name buffer wrapper.
		t.Fatal(errno)
	}
	end := bytes.IndexByte(name[:], 0)
	if end <= 0 {
		t.Fatalf("invalid PTY name: %q", name)
	}
	slave, err := os.OpenFile(string(name[:end]), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slave.Close() })
	if err := unix.IoctlSetWinsize(int(slave.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: 50, Col: 160}); err != nil {
		t.Fatal(err)
	}
	before, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TIOCGETA)
	if err != nil {
		t.Fatal(err)
	}
	if before.Lflag&(unix.ICANON|unix.ECHO|unix.ISIG) != unix.ICANON|unix.ECHO|unix.ISIG || before.Oflag&(unix.OPOST|unix.ONLCR) != unix.OPOST|unix.ONLCR {
		t.Fatalf("expected cooked PTY baseline: %+v", before)
	}
	p := &cliPTY{master: master, slave: slave, before: *before, stop: make(chan struct{}), done: make(chan struct{})}
	if len(drain) == 0 || drain[0] {
		p.resume()
	}
	t.Cleanup(func() { p.join(t) })
	return p
}

func (p *cliPTY) resume() {
	p.collectOnce.Do(func() { go p.collect(int(p.master.Fd())) })
}

func (p *cliPTY) collect(fd int) {
	defer close(p.done)
	var buf [8192]byte
	for {
		// Nonblocking drain lets the parent keep the slave open to inspect its
		// termios after child exit, without requiring PTY EOF to join the reader.
		n, err := unix.Read(fd, buf[:])
		if n > 0 {
			p.mu.Lock()
			p.output.Write(buf[:n])
			if p.reportTermios == nil && bytes.Contains(p.output.Bytes(), []byte("  Exit code:")) {
				p.reportTermios, p.err = unix.IoctlGetTermios(int(p.slave.Fd()), unix.TIOCGETA)
			}
			p.mu.Unlock()
			continue
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			p.mu.Lock()
			p.err = err
			p.mu.Unlock()
			return
		}
		select {
		case <-p.stop:
			return
		default:
		}
		if _, err := unix.Poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 20); err != nil && !errors.Is(err, unix.EINTR) {
			p.mu.Lock()
			p.err = err
			p.mu.Unlock()
			return
		}
	}
}

func (p *cliPTY) text() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.output.String()
}

func (p *cliPTY) join(t *testing.T) {
	t.Helper()
	p.resume()
	p.stopOnce.Do(func() { close(p.stop) })
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
		t.Fatal("PTY reader did not join")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		t.Fatalf("PTY reader: %v", p.err)
	}
}

func (p *cliPTY) ready(t *testing.T, process *cliProcess) {
	t.Helper()
	timer := time.NewTimer(time.Until(process.deadline))
	defer timer.Stop()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		state, err := unix.IoctlGetTermios(int(p.slave.Fd()), unix.TIOCGETA)
		if err != nil {
			t.Fatal(err)
		}
		text := p.text()
		if state.Lflag&(unix.ICANON|unix.ECHO|unix.ISIG) == 0 && strings.Contains(text, "\x1b[?1049h") && strings.Contains(text, "Workflow controller") {
			return
		}
		select {
		case <-tick.C:
		case <-process.done:
			t.Fatalf("CLI exited before TTY ready: %q", text)
		case <-timer.C:
			t.Fatalf("TTY readiness deadline: termios=%+v output=%q", state, text)
		}
	}
}

func (p *cliPTY) restored(t *testing.T, atReport bool) {
	t.Helper()
	after, err := unix.IoctlGetTermios(int(p.slave.Fd()), unix.TIOCGETA)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p.before, *after) {
		t.Errorf("terminal not restored: before=%+v after=%+v", p.before, *after)
	}
	if atReport && (p.reportTermios == nil || !reflect.DeepEqual(p.before, *p.reportTermios)) {
		t.Errorf("report observed before termios restore: before=%+v at report=%+v", p.before, p.reportTermios)
	}
}

func cliPTYReport(report string, onlcr bool) (string, error) {
	if !onlcr {
		return report, nil
	}
	// ONLCR can produce CRCRLF at a Darwin PTY queue boundary. This surface
	// cannot attribute that extra CR to app or kernel; the literal TTY must
	// independently pass assertCLIPlain on untouched application bytes.
	for i := 0; i < len(report); i++ {
		switch report[i] {
		case '\r':
			start := i
			for i < len(report) && report[i] == '\r' {
				i++
			}
			if i-start > 2 || i == len(report) || report[i] != '\n' {
				return "", fmt.Errorf("terminal control U+000D outside cooked CRLF/CRCRLF at byte %d", start)
			}
		case '\n':
			return "", fmt.Errorf("report was written without cooked newline processing at byte %d", i)
		}
	}
	report = strings.ReplaceAll(report, "\r\r\n", "\r\n")
	return strings.ReplaceAll(report, "\r\n", "\n"), nil
}

func TestCLIPTYReportProbe(t *testing.T) {
	mode := os.Getenv("PWC_TEST_PTY_REPORT_MODE")
	if mode == "" {
		return
	}
	report, err := cliPTYReport(os.Getenv("PWC_TEST_PTY_REPORT"), mode == "onlcr")
	if err != nil {
		t.Fatal(err)
	}
	assertCLIPlain(t, report)
}

func TestCLIPTYReportRejectsUnsafeBytes(t *testing.T) {
	for _, mode := range []string{"literal", "onlcr"} {
		for _, tc := range []struct{ name, text, diagnostic string }{
			{"bare CR", "x\ry", "terminal control U+000D"},
			{"triple CR", "x\r\r\r\n", "terminal control U+000D"},
			{"four CR", "x\r\r\r\r\n", "terminal control U+000D"},
			{"ANSI", "x\x1b[31my", "terminal control U+001B"},
			{"Cf", "x\u202ey", "terminal control U+202E"},
			{"SECRET", "xSECRETy", "terminal control payload leaked"},
			{"CRLF", "x\r\n", "terminal control U+000D"},
			{"CRCRLF", "x\r\r\n", "terminal control U+000D"},
			{"bare LF", "x\n", "without cooked newline processing"},
			{"trailing CR", "x\r", "terminal control U+000D"},
			{"unpaired double CR", "x\r\ry", "terminal control U+000D"},
			{"invalid UTF8", "x\xff", "invalid UTF8"},
		} {
			if mode == "onlcr" && (tc.name == "CRLF" || tc.name == "CRCRLF") || mode == "literal" && tc.name == "bare LF" {
				continue
			}
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				executable, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCLIPTYReportProbe$")
				cmd.Env = append(os.Environ(), "PWC_TEST_PTY_REPORT_MODE="+mode, "PWC_TEST_PTY_REPORT="+tc.text, "GORACE=atexit_sleep_ms=0")
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 1 || !bytes.Contains(output, []byte(tc.diagnostic)) {
					t.Fatalf("unsafe bytes were not rejected by the real assertion: err=%v output=%q", err, output)
				}
			})
		}
	}
}

func TestCLIPTYNewlineBoundary(t *testing.T) {
	for _, offset := range []int{2046, 2047, 2048} {
		for _, mode := range []struct {
			name string
			off  uint64
		}{
			{"onlcr", 0},
			{"literal", unix.ONLCR},
			{"opost-off", unix.OPOST},
		} {
			t.Run(fmt.Sprintf("LF%d/%s", offset, mode.name), func(t *testing.T) {
				pty := newCLIPTY(t, false)
				pty.before.Oflag &^= mode.off
				if err := unix.IoctlSetTermios(int(pty.slave.Fd()), unix.TIOCSETA, &pty.before); err != nil {
					t.Fatal(err)
				}
				input := strings.Repeat("A", offset) + "\nEND\n"
				assertCLIPlain(t, input)
				written := make(chan error, 1)
				go func() {
					// A blocking write returns short when a signal (such as Go's
					// preemption SIGURG) interrupts it after a partial transfer.
					buf := []byte(input)
					var err error
					for len(buf) > 0 {
						var n int
						n, err = unix.Write(int(pty.slave.Fd()), buf)
						if n > 0 {
							buf = buf[n:]
						}
						if errors.Is(err, unix.EINTR) {
							err = nil
							continue
						}
						if err != nil || n == 0 {
							break
						}
					}
					if err == nil && len(buf) > 0 {
						err = fmt.Errorf("short PTY write: %d/%d", len(input)-len(buf), len(input))
					}
					written <- err
				}()
				t.Cleanup(func() {
					pty.resume()
					select {
					case err := <-written:
						if err != nil {
							t.Error(err)
						}
					case <-time.After(3 * time.Second):
						t.Error("PTY writer did not join")
					}
				})
				deadline := time.Now().Add(3 * time.Second)
				for {
					queued, err := unix.IoctlGetInt(int(pty.slave.Fd()), unix.TIOCOUTQ)
					if err != nil {
						t.Fatal(err)
					}
					if queued > 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("PTY queue boundary deadline: queued=%d", queued)
					}
					time.Sleep(time.Millisecond)
				}
				pty.resume()
				select {
				case err := <-written:
					written <- err
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("PTY boundary write deadline")
				}
				onlcr := mode.name == "onlcr"
				want := input
				if onlcr {
					want = strings.ReplaceAll(input, "\n", "\r\n")
					if offset == 2047 {
						want = strings.Repeat("A", offset) + "\r\r\nEND\r\n"
					}
				}
				// The reader drains nonblocking; under load the tail can reach the
				// master after the writer returns, so wait for it before joining.
				for drained := time.Now().Add(3 * time.Second); len(pty.text()) < len(want) && time.Now().Before(drained); {
					time.Sleep(time.Millisecond)
				}
				pty.join(t)
				pty.restored(t, false)
				raw := pty.text()
				t.Logf("pure LF syscall input=%d raw=%d boundary=%q CRCRLF=%d", len(input), len(raw), raw[max(0, len(raw)-10):], strings.Count(raw, "\r\r\n"))
				t.Logf("raw PTY hex: %x", []byte(raw))
				if raw != want {
					t.Fatalf("unexpected transport bytes: %q", raw)
				}
				report, err := cliPTYReport(raw, onlcr)
				if err != nil {
					t.Fatal(err)
				}
				assertCLIPlain(t, report)
				if report != input {
					t.Fatal("transport decoding did not preserve application bytes")
				}
			})
		}
	}
}

func TestCLIRealTTY(t *testing.T) {
	for _, tc := range []struct {
		name, key, fault string
		signal           syscall.Signal
		exit             int
		origin           engine.Origin
	}{
		{name: "success"},
		{name: "q", key: "q", exit: 130, origin: engine.OriginControllerUser},
		{name: "control-C byte", key: "\x03", exit: 130, origin: engine.OriginSignalINT},
		{name: "SIGTERM", signal: syscall.SIGTERM, exit: 143, origin: engine.OriginSignalTERM},
		{name: "SIGTERM cleanup and finalization warnings", signal: syscall.SIGTERM, exit: 143, origin: engine.OriginSignalTERM, fault: "result cleanup discovery"},
	} {
		for _, mode := range []struct {
			name  string
			onlcr bool
		}{{"literal", false}, {"onlcr", true}} {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				pty := newCLIPTY(t, false)
				if !mode.onlcr {
					pty.before.Oflag &^= unix.ONLCR
					if err := unix.IoctlSetTermios(int(pty.slave.Fd()), unix.TIOCSETA, &pty.before); err != nil {
						t.Fatal(err)
					}
				}
				pty.restored(t, false)
				pty.resume()
				p := startCLIProcess(t, "real terminal", tc.fault, "", false, cliProcessOptions{
					stdin: pty.slave, stdout: pty.slave, cleanupBarrier: true,
					env: []string{"TERM=xterm-256color", "NO_COLOR=1"},
				})
				control := p.accept(t, p.listener)
				hello := control.next(t, "hello")
				dir := cliRunDir(hello)
				message := control.next(t, "prompt")
				request := assertCLIUnsettled(t, p, dir, message)
				pty.ready(t, p)
				if tc.exit == 0 {
					control.send(t, "settle")
					assertCLIUnsettled(t, p, dir, control.next(t, "prompt"))
					control.send(t, "settle")
				} else {
					control.send(t, "hold")
					control.next(t, "held")
					if tc.key != "" {
						// ISIG is off at the ready barrier: 0x03 exercises UV's key
						// decoder, not the kernel's SIGINT delivery path.
						if _, err := pty.master.Write([]byte(tc.key)); err != nil {
							t.Fatal(err)
						}
					} else if err := p.cmd.Process.Signal(tc.signal); err != nil {
						t.Fatal(err)
					}
				}
				cleanup := p.accept(t, p.cleanupListener)
				cleanup.next(t, "cleanup-blocked")
				state, err := unix.IoctlGetTermios(int(pty.slave.Fd()), unix.TIOCGETA)
				if err != nil {
					t.Fatal(err)
				}
				if state.Lflag&(unix.ICANON|unix.ECHO|unix.ISIG) != 0 {
					t.Fatalf("terminal restored before cleanup acknowledgment: %+v", state)
				}
				select {
				case <-p.done:
					t.Fatal("CLI exited before cleanup acknowledgment")
				default:
				}
				if text := pty.text(); strings.Contains(text, "  Exit code:") || strings.Contains(text, "\x1b[?1049l") {
					t.Fatalf("report/terminal teardown bypassed cleanup barrier: %q", text)
				}
				cleanup.send(t, "release")
				p.wait(t, tc.exit)
				pty.join(t)
				pty.restored(t, true)
				if p.stderr.Len() != 0 {
					t.Fatalf("stderr=%s", &p.stderr)
				}
				text := pty.text()
				outcome := engine.CancelledState
				if tc.exit == 0 {
					outcome = engine.Succeeded
				}
				header := fmt.Sprintf("Outcome: %s  Exit code: %d", outcome, tc.exit)
				reportAt := strings.Index(text, header)
				exitAlt := strings.LastIndex(text, "\x1b[?1049l")
				if reportAt < 0 || exitAlt < 0 || exitAlt >= reportAt || strings.Count(text, header) != 1 || strings.Count(text, "\x1b[?1049l") != 1 {
					t.Fatalf("final report must follow alt-screen exit exactly once: %q", text)
				}
				report := text[reportAt:]
				if mode.onlcr {
					// Keep write-time cooked newline evidence separate from the
					// exact termios snapshot taken when the report is read.
					report, err = cliPTYReport(report, true)
					if err != nil {
						t.Fatal(err)
					}
				}
				assertCLIPlain(t, report)
				for _, want := range []string{"Run path: " + dir + "\n", "Cleanup report 1:", "Wait completed=true", "Process exited=true"} {
					if !strings.Contains(report, want) {
						t.Errorf("missing %q in report=%s", want, report)
					}
				}
				var s engine.Snapshot
				cliReadJSON(t, filepath.Join(dir, "run.json"), &s)
				if s.State != outcome || s.WorkflowOutcome != outcome {
					t.Fatalf("wrong terminal outcome: %+v", s)
				}
				if tc.exit != 0 {
					a := s.Attempts[request.Identity.AttemptID]
					if len(s.Attempts) != 1 || s.Failure == nil || s.Failure.Code != engine.Cancelled || s.Failure.Origin != tc.origin || a.State != engine.CancelledState || a.Output != nil || a.Failure == nil || a.Failure.Origin != tc.origin || a.DispatchAccepted != engine.AcceptedYes {
						t.Fatalf("key/signal provenance or cancellation lost: %+v", s)
					}
					if !strings.Contains(report, "origin="+string(tc.origin)) {
						t.Fatalf("report lost cancellation origin: %s", report)
					}
				} else {
					if len(s.Attempts) != 2 || s.Failure != nil {
						t.Fatalf("success lost committed attempts: %+v", s)
					}
					for _, a := range s.Attempts {
						if a.State != engine.Succeeded || a.Output == nil {
							t.Fatalf("uncommitted successful attempt: %+v", a)
						}
					}
					if !strings.Contains(report, "  final: ") {
						t.Fatalf("missing final Ref: %s", report)
					}
				}
				if tc.fault != "" {
					if s.StatePersisted || len(s.FinalizationErrors) != 2 || len(s.CleanupErrors) != 1 {
						t.Fatalf("warning state missing: %+v", s)
					}
					for _, want := range []string{"FinalizationFailed", "phase=failed_result", "phase=cleanup", "state_persisted=false", "Cleanup warning: CleanupFailed", "Discovery warning:"} {
						if !strings.Contains(report, want) {
							t.Errorf("warning lost after terminal restore: %q in %s", want, report)
						}
					}
				} else if !s.StatePersisted {
					t.Fatalf("terminal state not persisted: %+v", s)
				}
				assertCLICleanup(t, p, dir, hello, !strings.Contains(tc.fault, "cleanup"), strings.Contains(tc.fault, "discovery"))
				control.exited(t)
				cleanup.exited(t)
			})
		}
	}
}

func TestCLIRequiresBothStreamsTTY(t *testing.T) {
	for _, inputTTY := range []bool{true, false} {
		t.Run(fmt.Sprintf("stdinTTY=%t", inputTTY), func(t *testing.T) {
			pty := newCLIPTY(t, inputTTY)
			option := cliProcessOptions{env: []string{"TERM=xterm-256color"}}
			if inputTTY {
				option.stdin = pty.slave
			} else {
				// Darwin ONLCR can emit a CR before returning EAGAIN, then emit
				// another when the LF is retried. Inspect application bytes instead.
				pty.before.Oflag &^= unix.ONLCR
				if err := unix.IoctlSetTermios(int(pty.slave.Fd()), unix.TIOCSETA, &pty.before); err != nil {
					t.Fatal(err)
				}
				option.stdout = pty.slave
			}
			p := startCLIProcess(t, "mixed terminal streams", "", "", false, option)
			control := p.accept(t, p.listener)
			hello := control.next(t, "hello")
			for range 2 {
				control.next(t, "prompt")
				control.send(t, "settle")
			}
			if !inputTTY {
				waitCLIPTYPredicate(t, p, func() bool {
					var snapshot engine.Snapshot
					return protocol.ReadJSON(filepath.Join(cliRunDir(hello), "run.json"), &snapshot) == nil && snapshot.State == engine.Succeeded
				})
				pty.resume()
			}
			p.wait(t, 0)
			pty.join(t)
			pty.restored(t, !inputTTY)
			text := p.stdout.String()
			if !inputTTY {
				text = pty.text()
			}
			assertCLIPlain(t, text)
			if !strings.HasPrefix(text, "Run path: "+cliRunDir(hello)+"\n") || !strings.Contains(text, "Outcome: Succeeded  Exit code: 0") || p.stderr.Len() != 0 {
				t.Fatalf("mixed streams did not automatically select plain mode: stdout=%q stderr=%q", text, &p.stderr)
			}
			assertCLICleanup(t, p, cliRunDir(hello), hello, true, false)
			control.exited(t)
		})
	}
}

// Readiness is a kernel/file predicate; the ticker only bounds sampling.
func waitCLIPTYPredicate(t *testing.T, p *cliProcess, ready func() bool) {
	t.Helper()
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	timer := time.NewTimer(time.Until(p.deadline))
	defer timer.Stop()
	for !ready() {
		select {
		case <-tick.C:
		case <-p.done:
			t.Fatalf("CLI exited before PTY barrier: %v; stderr=%s", p.err, &p.stderr)
		case <-timer.C:
			t.Fatal("PTY barrier deadline")
		}
	}
}

func fillCLIPTY(t *testing.T, pty *cliPTY) {
	t.Helper()
	fd, err := unix.Open(pty.slave.Name(), unix.O_WRONLY|unix.O_NONBLOCK|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func(fd int) { _ = unix.Close(fd) }(fd)
	filled := 0
	for _, size := range []int{4096, 1} {
		buf := bytes.Repeat([]byte("x"), size)
		for {
			n, err := unix.Write(fd, buf)
			if n > 0 {
				filled += n
			}
			if errors.Is(err, unix.EAGAIN) {
				break
			}
			if err != nil && !errors.Is(err, unix.EINTR) {
				t.Fatal(err)
			}
		}
	}
	t.Logf("PTY kernel output full: %d filler bytes, one-byte write returned EAGAIN", filled)
}

func writeCLIPTY(t *testing.T, pty *cliPTY, p *cliProcess, text string) {
	t.Helper()
	buf := []byte(text)
	for len(buf) > 0 {
		n, err := unix.Write(int(pty.master.Fd()), buf)
		if n > 0 {
			buf = buf[n:]
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EINTR) {
			t.Fatal(err)
		}
		if time.Now().After(p.deadline) {
			t.Fatal("PTY input write deadline")
		}
		if errors.Is(err, unix.EAGAIN) {
			_, err = unix.Poll([]unix.PollFd{{Fd: int32(pty.master.Fd()), Events: unix.POLLOUT}}, 20)
			if err != nil && !errors.Is(err, unix.EINTR) {
				t.Fatal(err)
			}
		}
	}
}

func TestCLIRealTTYBlockedOutputCancellation(t *testing.T) {
	for _, tc := range []struct {
		name, key string
		origin    engine.Origin
		flood     bool
	}{
		{"q", "q", engine.OriginControllerUser, false},
		{"raw control-C", "\x03", engine.OriginSignalINT, false},
		{"q behind input flood", "q", engine.OriginControllerUser, true},
		{"raw control-C behind input flood", "\x03", engine.OriginSignalINT, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pty := newCLIPTY(t, false)
			fillCLIPTY(t, pty)
			trace := filepath.Join(t.TempDir(), "tea.trace")
			p := startCLIProcess(t, "blocked terminal", "", "", false, cliProcessOptions{
				stdin: pty.slave, stdout: pty.slave,
				// Suppress startup queries: the first blocked write is the
				// renderer flush, while it owns cursedRenderer.mu.
				env: []string{"TERM=xterm-256color", "TERM_PROGRAM=Apple_Terminal", "TEA_TRACE=" + trace, "NO_COLOR=1"},
			})
			t.Cleanup(pty.resume)
			control := p.accept(t, p.listener)
			hello := control.next(t, "hello")
			dir := cliRunDir(hello)
			message := control.next(t, "prompt")
			control.send(t, "hold")
			control.next(t, "held")
			assertCLIUnsettled(t, p, dir, message)
			waitCLIPTYPredicate(t, p, func() bool {
				state, err := unix.IoctlGetTermios(int(pty.slave.Fd()), unix.TIOCGETA)
				if err != nil {
					t.Fatal(err)
				}
				log, _ := os.ReadFile(trace)
				return state.Lflag&(unix.ICANON|unix.ECHO|unix.ISIG) == 0 && bytes.Contains(log, []byte("cursed_renderer.go:"))
			})
			input := tc.key
			if tc.flood {
				input = strings.Repeat("j", 32<<10) + input
			}
			writeCLIPTY(t, pty, p, input)
			s := waitCLIState(t, p, dir, func(s engine.Snapshot) bool { return !s.FinishedAt.IsZero() })
			if s.State != engine.CancelledState || s.Failure == nil || s.Failure.Origin != tc.origin || len(s.Attempts) != 1 || !s.StatePersisted {
				t.Fatalf("cancellation blocked by renderer: %+v", s)
			}
			assertCLICleanup(t, p, dir, hello, true, false)
			control.exited(t)
			t.Log("terminal run.json, owned cleanup and fixture EOF observed before any output drain")
			select {
			case <-p.done:
				t.Fatal("CLI returned while stdout still full")
			default:
			}
			pty.resume()
			p.wait(t, 130)
			pty.join(t)
			pty.restored(t, true)
			text := pty.text()
			reportAt := strings.Index(text, "Outcome: Cancelled  Exit code: 130")
			exitAlt := strings.LastIndex(text, "\x1b[?1049l")
			if reportAt < 0 || exitAlt < 0 || exitAlt >= reportAt || !strings.Contains(text[reportAt:], "Process exited=true") || p.stderr.Len() != 0 {
				t.Fatalf("missing restored report: %q; stderr=%s", text, &p.stderr)
			}
		})
	}
}

func TestCLIRealTTYReadOnlyOutput(t *testing.T) {
	pty := newCLIPTY(t)
	readonly, err := os.Open(pty.slave.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func(f *os.File) { _ = f.Close() }(readonly)
	p := startCLIProcess(t, "readonly terminal", "", "", false, cliProcessOptions{
		stdin: pty.slave, stdout: readonly, env: []string{"TERM=xterm-256color", "NO_COLOR=1"},
	})
	p.wait(t, 130)
	pty.join(t)
	pty.restored(t, false)
	dirs, err := filepath.Glob(filepath.Join(p.home, "WIP", "*", "runs", "*"))
	if err != nil || len(dirs) != 1 {
		t.Fatalf("run dirs=%v err=%v", dirs, err)
	}
	var s engine.Snapshot
	cliReadJSON(t, filepath.Join(dirs[0], "run.json"), &s)
	if s.State != engine.CancelledState || !s.StatePersisted || s.Failure == nil || s.Failure.Origin != engine.OriginControllerUser || len(s.Attempts) > 1 {
		t.Fatalf("readonly renderer did not cancel early: %+v", s)
	}
	for _, session := range s.Sessions {
		if session.State != "Closed" {
			t.Fatalf("session leaked: %+v", session)
		}
	}
	for _, text := range []string{"Display error:", "bad file descriptor", "Report output error:", "Outcome: Cancelled  Exit code: 130"} {
		if !strings.Contains(p.stderr.String(), text) {
			t.Errorf("missing %q: %s", text, &p.stderr)
		}
	}
}

func TestCLIRealTTYInputReadError(t *testing.T) {
	pty := newCLIPTY(t)
	writeonly, err := os.OpenFile(pty.slave.Name(), os.O_WRONLY|syscall.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func(f *os.File) { _ = f.Close() }(writeonly)
	p := startCLIProcess(t, "unreadable terminal", "", "", false, cliProcessOptions{
		stdin: writeonly, stdout: pty.slave, env: []string{"TERM=xterm-256color", "NO_COLOR=1"},
	})
	// Supply readiness whether the reader uses kqueue or select. No cancel
	// key is sent: only the actual input error may cancel this run.
	writeCLIPTY(t, pty, p, "x\n")
	p.wait(t, 130)
	pty.join(t)
	pty.restored(t, true)
	if !strings.Contains(p.stderr.String(), "Display error:") || !strings.Contains(p.stderr.String(), "bad file descriptor") || !strings.Contains(pty.text(), "Outcome: Cancelled  Exit code: 130") {
		t.Fatalf("input failure was lost: stderr=%s terminal=%q", &p.stderr, pty.text())
	}
}

func TestCLIRealTTYActiveWriteError(t *testing.T) {
	pty := newCLIPTY(t)
	p := startCLIProcess(t, "active terminal write error", "", "tty-active", true, cliProcessOptions{
		stdin: pty.slave, stdout: pty.slave, cleanupBarrier: true, ttyPath: pty.slave.Name(),
		env: []string{"TERM=xterm-256color", "NO_COLOR=1"},
	})
	output := p.accept(t, p.outputListener)
	output.next(t, "output-armed")
	control := p.accept(t, p.listener)
	hello := control.next(t, "hello")
	dir := cliRunDir(hello)
	message := control.next(t, "prompt")
	control.send(t, "hold")
	control.next(t, "held")
	request := assertCLIUnsettled(t, p, dir, message)
	pty.ready(t, p)
	output.send(t, "release")
	cleanup := p.accept(t, p.cleanupListener)
	cleanup.next(t, "cleanup-blocked")
	select {
	case <-p.done:
		t.Fatal("renderer write failure bypassed cleanup")
	default:
	}
	cleanup.send(t, "release")
	p.wait(t, 130)
	pty.join(t)
	pty.restored(t, false)
	var s engine.Snapshot
	cliReadJSON(t, filepath.Join(dir, "run.json"), &s)
	a := s.Attempts[request.Identity.AttemptID]
	if s.State != engine.CancelledState || !s.StatePersisted || s.Failure == nil || s.Failure.Origin != engine.OriginControllerUser || len(s.Attempts) != 1 || a.State != engine.CancelledState || a.Output != nil || a.DispatchAccepted != engine.AcceptedYes {
		t.Fatalf("active renderer error lost cancellation: %+v", s)
	}
	assertCLIReportFallback(t, p, dir, "Display error: write /dev/stdout: bad file descriptor\nReport output error: write /dev/stdout: bad file descriptor\n", engine.CancelledState, 130)
	assertCLICleanup(t, p, dir, hello, true, false)
	control.exited(t)
	cleanup.exited(t)
}

func TestCLIRealTTYIgnoresPasteAndTerminalReplyCancellationText(t *testing.T) {
	for _, input := range []string{"\x1b[200~q\x03\x1b[201~", "\x1bP>|q-terminal\x1b\\"} {
		t.Run(fmt.Sprintf("%q", input), func(t *testing.T) {
			pty := newCLIPTY(t)
			if err := unix.IoctlSetWinsize(int(pty.slave.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Row: 10, Col: 160}); err != nil {
				t.Fatal(err)
			}
			p := startCLIProcess(t, "parser boundary", "", "", false, cliProcessOptions{stdin: pty.slave, stdout: pty.slave, env: []string{"TERM=xterm-256color", "NO_COLOR=1"}})
			control := p.accept(t, p.listener)
			hello := control.next(t, "hello")
			dir := cliRunDir(hello)
			message := control.next(t, "prompt")
			control.send(t, "hold")
			control.next(t, "held")
			pty.ready(t, p)
			writeCLIPTY(t, pty, p, input+"j")
			// The following scroll is an ordered parser/consumer/UI barrier.
			waitCLIPTYPredicate(t, p, func() bool { return strings.Contains(pty.text(), "2-10/") })
			assertCLIUnsettled(t, p, dir, message)
			writeCLIPTY(t, pty, p, "q")
			p.wait(t, 130)
			pty.join(t)
			pty.restored(t, true)
			assertCLICleanup(t, p, dir, hello, true, false)
			control.exited(t)
		})
	}
}
