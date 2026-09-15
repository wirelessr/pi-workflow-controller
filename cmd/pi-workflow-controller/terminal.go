package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	tea "charm.land/bubbletea/v2"
	uv "github.com/charmbracelet/ultraviolet"
	"github.com/charmbracelet/x/term"
	"github.com/muesli/cancelreader"

	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/tui"
)

// Tea ignores renderer write errors. Record at the IO boundary and cancel the
// engine without going through Tea's event loop or renderer locks.
type terminalFailure struct {
	mu  sync.Mutex
	err error
	run *engine.Run
}

func (f *terminalFailure) record(err error) {
	if err == nil {
		return
	}
	f.mu.Lock()
	first := f.err == nil
	if first {
		f.err = err
	}
	f.mu.Unlock()
	if first {
		f.run.Cancel(engine.OriginControllerUser)
	}
}

func (f *terminalFailure) load() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// Keep term.File (Read/Write/Fd) and Name intact for Tea's terminal detection.
type terminalWriter struct {
	*os.File
	failure terminalFailure
}

func (w *terminalWriter) Write(p []byte) (int, error) {
	n, err := w.File.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.failure.record(err)
	return n, err
}

func (w *terminalWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

type terminalReader struct {
	io.Reader
	failure terminalFailure
}

func (r *terminalReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	// StreamEvents treats EOF as success, and may select shutdown before its
	// error channel. Preserve actual input failures at Read instead.
	if !errors.Is(err, cancelreader.ErrCanceled) {
		r.failure.record(err)
	}
	return n, err
}

func terminalDisplay(r *engine.Run, reports <-chan engine.Report, stdin, stdout *os.File) (displayErr error) {
	state, err := term.MakeRaw(stdin.Fd())
	if err != nil {
		return fmt.Errorf("entering raw mode: %w", err)
	}
	defer func() {
		if err := term.Restore(stdin.Fd(), state); err != nil {
			displayErr = errors.Join(displayErr, fmt.Errorf("restoring terminal: %w", err))
		}
	}()
	reader, err := uv.NewCancelReader(stdin)
	if err != nil {
		return fmt.Errorf("creating terminal reader: %w", err)
	}
	input := &terminalReader{Reader: reader, failure: terminalFailure{run: r}}
	output := &terminalWriter{File: stdout, failure: terminalFailure{run: r}}
	program := tea.NewProgram(tui.New(r, reports), tea.WithInput(nil), tea.WithOutput(output), tea.WithoutSignalHandler())
	ctx, stop := context.WithCancel(context.Background())
	events := make(chan uv.Event)
	presentation := make(chan uv.Event, 64)
	scanned, consumed, sent := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(scanned)
		defer close(events)
		input.failure.record(uv.NewTerminalReader(input, os.Getenv("TERM")).StreamEvents(ctx, events))
	}()
	go func() {
		defer close(consumed)
		defer close(presentation)
		// UV's sendEvents does not select ctx. Keep draining until the scanner
		// exits, even after shutdown, or StreamEvents cannot join its reader.
		for event := range events {
			if key, ok := event.(uv.KeyPressEvent); ok {
				switch key.String() {
				case "q":
					r.Cancel(engine.OriginControllerUser)
				case "ctrl+c":
					r.Cancel(engine.OriginSignalINT)
				}
			}
			// Only presentation is lossy. A blocked renderer must neither stop
			// cancellation nor accumulate an unbounded queue or goroutines.
			select {
			case presentation <- event:
			default:
			}
		}
	}()
	go func() {
		defer close(sent)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-presentation:
				if !ok {
					return
				}
				program.Send(event)
			}
		}
	}()
	_, programErr := program.Run()
	if programErr != nil {
		r.Cancel(engine.OriginControllerUser)
	}
	stop()
	reader.Cancel()
	<-scanned
	<-consumed
	// Program.Run cancels Tea's context even on initialization errors,
	// releasing any outstanding Send before this join.
	<-sent
	return errors.Join(programErr, output.failure.load(), input.failure.load(), reader.Close())
}
