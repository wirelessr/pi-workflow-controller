package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Event retains the connection which supplied a control message.
type Event struct {
	Message Control
	Err     error
	peer    *peer
}

type peer struct {
	conn net.Conn
	mu   sync.Mutex
}

func (e Event) Reply(reply Control) error {
	e.peer.mu.Lock()
	defer e.peer.mu.Unlock()
	return json.NewEncoder(e.peer.conn).Encode(reply)
}

// Host owns only the test control transport, not the engine or its processes.
// Call Close even after cancellation to release sockets and join all pumps.
type Host struct {
	listener *net.TCPListener
	ctx      context.Context
	cancel   context.CancelFunc
	events   chan Event
	accepted chan struct{}
	readers  sync.WaitGroup
	mu       sync.Mutex
	peers    map[*peer]struct{}
	errors   []error
	close    sync.Once
	closeErr error
}

func NewHost(ctx context.Context, buffer int) (*Host, error) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := listener.SetDeadline(deadline); err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	h := &Host{listener: listener, ctx: ctx, cancel: cancel, events: make(chan Event, buffer), accepted: make(chan struct{}), peers: map[*peer]struct{}{}}
	go h.accept()
	return h, nil
}

// RegisterCleanup leaves cancellation and the engine's completion channel with
// the caller. Resolve both at cleanup time, including an unstarted run.
func RegisterCleanup(t testing.TB, host *Host, stop func() <-chan struct{}, wait time.Duration, closePrefix, joinFailure string) {
	t.Helper()
	t.Cleanup(func() {
		done := stop()
		if err := host.Close(); err != nil {
			t.Errorf("%s%v", closePrefix, err)
		}
		if done != nil {
			select {
			case <-done:
			case <-time.After(wait):
				t.Error(joinFailure)
			}
		}
	})
}

func (h *Host) Addr() net.Addr       { return h.listener.Addr() }
func (h *Host) Events() <-chan Event { return h.events }

func (h *Host) send(event Event) {
	select {
	case h.events <- event:
	case <-h.ctx.Done():
	}
}

func (h *Host) failed(p *peer, err error) {
	// Retain errors even when cancellation or a full event queue prevents delivery.
	h.mu.Lock()
	h.errors = append(h.errors, err)
	h.mu.Unlock()
	h.send(Event{peer: p, Err: err})
}

func (h *Host) accept() {
	defer close(h.accepted)
	for {
		conn, err := h.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				h.failed(nil, err)
			}
			return
		}
		if deadline, ok := h.ctx.Deadline(); ok {
			if err := conn.SetDeadline(deadline); err != nil {
				_ = conn.Close()
				h.failed(nil, err)
				continue
			}
		}
		p := &peer{conn: conn}
		h.mu.Lock()
		h.peers[p] = struct{}{}
		h.mu.Unlock()
		h.readers.Add(1)
		go h.read(p)
	}
}

func (h *Host) read(p *peer) {
	defer h.readers.Done()
	defer func() {
		_ = p.conn.Close()
		h.mu.Lock()
		delete(h.peers, p)
		h.mu.Unlock()
	}()
	decoder := json.NewDecoder(p.conn)
	for {
		var message Control
		if err := decoder.Decode(&message); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				h.failed(p, err)
			}
			return
		}
		h.send(Event{Message: message, peer: p})
	}
}

// Close unblocks publishers before waiting for accept/read goroutines. It also
// reports decode errors which could not be delivered on Events.
func (h *Host) Close() error {
	h.close.Do(func() {
		h.cancel()
		if err := h.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			h.mu.Lock()
			h.errors = append(h.errors, err)
			h.mu.Unlock()
		}
		<-h.accepted
		h.mu.Lock()
		for p := range h.peers {
			if err := p.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				h.errors = append(h.errors, err)
			}
		}
		h.mu.Unlock()
		h.readers.Wait()
		close(h.events)
		h.closeErr = errors.Join(h.errors...)
	})
	return h.closeErr
}
