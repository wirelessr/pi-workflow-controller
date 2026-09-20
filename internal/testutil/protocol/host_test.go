package protocol_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/testutil/protocol"
)

const hostTestTimeout = 5 * time.Second

type hostFixture struct {
	host   *protocol.Host
	cancel context.CancelFunc
	peers  []*net.TCPConn
}

func newHostFixture(t *testing.T, ctx context.Context, buffer int) *hostFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	host, err := protocol.NewHost(ctx, buffer)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	f := &hostFixture{host: host, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		for _, peer := range f.peers {
			_ = peer.Close()
		}
		f.close(t, 1)
	})
	return f
}

func (f *hostFixture) dial(t *testing.T) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", f.host.Addr().String(), hostTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	peer := conn.(*net.TCPConn)
	f.peers = append(f.peers, peer)
	if err := peer.SetDeadline(time.Now().Add(hostTestTimeout)); err != nil {
		t.Fatal(err)
	}
	return peer
}

func hostEvent(t *testing.T, host *protocol.Host) protocol.Event {
	t.Helper()
	timer := time.NewTimer(hostTestTimeout)
	defer timer.Stop()
	select {
	case event, ok := <-host.Events():
		if !ok {
			t.Fatal("Events closed before expected event")
		}
		return event
	case <-timer.C:
		t.Fatal("timed out waiting for host event")
		return protocol.Event{}
	}
}

func (f *hostFixture) close(t *testing.T, callers int) []error {
	t.Helper()
	results := make(chan error, callers)
	var workers sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			results <- f.host.Close()
		}()
	}
	close(start)
	timer := time.NewTimer(hostTestTimeout)
	defer timer.Stop()
	var errs []error
	for len(errs) < callers {
		select {
		case err := <-results:
			errs = append(errs, err)
		case <-timer.C:
			t.Error("Host.Close did not join its accept/read pumps before timeout")
			f.cancel()
			for _, peer := range f.peers {
				_ = peer.Close()
			}
			// Rescue a blocked publisher before joining our own Close callers.
			events := f.host.Events()
			for len(errs) < callers {
				select {
				case err := <-results:
					errs = append(errs, err)
				case _, ok := <-events:
					if !ok {
						events = nil
					}
				}
			}
		}
	}
	workers.Wait()
	return errs
}

func requireHostClosed(t *testing.T, f *hostFixture) {
	t.Helper()
	// Buffered events remain readable after Close, but no pump may publish again.
	timer := time.NewTimer(hostTestTimeout)
	defer timer.Stop()
closed:
	for {
		select {
		case _, ok := <-f.host.Events():
			if !ok {
				break closed
			}
		case <-timer.C:
			t.Error("Events was not closed after Host.Close returned")
			break closed
		}
	}
	conn, err := net.DialTimeout("tcp", f.host.Addr().String(), hostTestTimeout)
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Error("listener still accepts after Close")
	}
	for _, peer := range f.peers {
		var b [1]byte
		_, err := peer.Read(b[:])
		var netErr net.Error
		if err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
			t.Errorf("peer not closed by Host.Close: %v", err)
		}
	}
}

func TestHostEventsAndConcurrentReplies(t *testing.T) {
	for _, tc := range []struct {
		name           string
		peers, replies int
	}{
		{"single-peer", 1, 1},
		{"multiple-peers", 3, 1},
		{"concurrent-same-peer", 1, 16},
		{"concurrent-multiple-peers", 3, 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newHostFixture(t, context.Background(), tc.peers)
			events := make([]protocol.Event, tc.peers)
			for i := 0; i < tc.peers; i++ {
				peer := f.dial(t)
				message := protocol.Control{Type: "hello", PID: i + 1, SessionID: fmt.Sprintf("peer-%d", i), History: "history", RequestPath: "request", CandidatePath: "candidate"}
				if i > 0 {
					message.Data = json.RawMessage(`{"tokens":95}`)
				}
				if err := json.NewEncoder(peer).Encode(message); err != nil {
					t.Fatal(err)
				}
				event := hostEvent(t, f.host)
				if event.Err != nil || !reflect.DeepEqual(event.Message, message) {
					t.Fatalf("event message differs from sender, error = %v", event.Err)
				}
				events[i] = event
			}
			payload := strings.Repeat("quoted \" JSON \\ newline\n", 1024)
			results := make(chan error, tc.peers*tc.replies)
			start := make(chan struct{})
			var workers sync.WaitGroup
			// Reply workers belong to this caller, never to Host.Close.
			defer func() {
				for _, peer := range f.peers {
					_ = peer.Close()
				}
				workers.Wait()
			}()
			for i := len(events) - 1; i >= 0; i-- {
				for n := 0; n < tc.replies; n++ {
					workers.Add(1)
					go func(event protocol.Event, n int) {
						defer workers.Done()
						<-start
						results <- event.Reply(protocol.Control{Type: "reply", PID: n, SessionID: event.Message.SessionID, History: payload})
					}(events[i], n)
				}
			}
			close(start)
			for i, peer := range f.peers {
				decoder := json.NewDecoder(peer)
				seen := make(map[int]bool)
				for n := 0; n < tc.replies; n++ {
					var reply protocol.Control
					if err := decoder.Decode(&reply); err != nil {
						t.Fatalf("peer %d reply %d is not complete JSON: %v", i, n, err)
					}
					if reply.Type != "reply" || reply.SessionID != events[i].Message.SessionID || reply.History != payload || reply.PID < 0 || reply.PID >= tc.replies || seen[reply.PID] {
						t.Fatalf("peer %d received wrong, corrupt or duplicate reply %d", i, reply.PID)
					}
					seen[reply.PID] = true
				}
			}
			workers.Wait()
			for n := 0; n < tc.peers*tc.replies; n++ {
				if err := <-results; err != nil {
					t.Errorf("Reply: %v", err)
				}
			}
			for _, err := range f.close(t, 1) {
				if err != nil {
					t.Errorf("Close: %v", err)
				}
			}
			requireHostClosed(t, f)
		})
	}
}

func TestHostDecodeTermination(t *testing.T) {
	for _, tc := range []struct {
		name, tail string
		kind       string
	}{
		{"clean-eof", "", "none"},
		{"partial-json", `{"Type":`, "partial"},
		{"malformed-json", `{"Type":!}`, "syntax"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newHostFixture(t, context.Background(), 1)
			peer := f.dial(t)
			if err := json.NewEncoder(peer).Encode(protocol.Control{Type: "hello"}); err != nil {
				t.Fatal(err)
			}
			if event := hostEvent(t, f.host); event.Err != nil || event.Message.Type != "hello" {
				t.Fatalf("hello: %v", event.Err)
			}
			if _, err := io.WriteString(peer, tc.tail); err != nil {
				t.Fatal(err)
			}
			if err := peer.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			check := func(where string, err error) {
				t.Helper()
				var syntax *json.SyntaxError
				switch tc.kind {
				case "none":
					if err != nil {
						t.Errorf("%s: EOF must be benign, got %v", where, err)
					}
				case "partial":
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Errorf("%s: want io.ErrUnexpectedEOF, got %v", where, err)
					}
				case "syntax":
					if !errors.As(err, &syntax) {
						t.Errorf("%s: want *json.SyntaxError, got %v", where, err)
					}
				}
			}
			if tc.kind != "none" {
				check("Events", hostEvent(t, f.host).Err)
			}
			// Remote EOF proves read teardown happened before Close can mask it.
			var b [1]byte
			if _, err := peer.Read(b[:]); !errors.Is(err, io.EOF) {
				t.Fatalf("host reader did not close peer: %v", err)
			}
			for _, err := range f.close(t, 4) {
				check("concurrent Close", err)
			}
			for _, err := range f.close(t, 1) {
				check("repeated Close", err)
			}
			select {
			case event, ok := <-f.host.Events():
				if ok {
					t.Errorf("unexpected extra event: %v", event.Err)
				}
			default:
				t.Error("Events not closed after Close")
			}
			requireHostClosed(t, f)
		})
	}
}

func TestHostCloseWithUnconsumedEvents(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		buffer                     int
		cancelBeforeNew, cancelNow bool
	}{
		{"already-cancelled", 0, true, false},
		{"unbuffered", 0, false, false},
		{"full-buffer", 1, false, false},
		{"cancelled-unbuffered", 0, false, true},
		{"cancelled-full-buffer", 1, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelBeforeNew {
				cancel()
			}
			f := newHostFixture(t, ctx, tc.buffer)
			if !tc.cancelBeforeNew {
				peer := f.dial(t)
				encoder := json.NewEncoder(peer)
				if err := encoder.Encode(protocol.Control{Type: "accepted"}); err != nil {
					t.Fatal(err)
				}
				if event := hostEvent(t, f.host); event.Err != nil || event.Message.Type != "accepted" {
					t.Fatalf("accept barrier: %v", event.Err)
				}
				for i := 0; i < tc.buffer+2; i++ {
					if err := encoder.Encode(protocol.Control{Type: "unconsumed", PID: i}); err != nil {
						t.Fatal(err)
					}
				}
				if tc.buffer > 0 {
					timer := time.NewTimer(hostTestTimeout)
					defer timer.Stop()
					for len(f.host.Events()) != tc.buffer {
						select {
						case <-timer.C:
							t.Fatal("host did not fill event queue")
						default:
							runtime.Gosched()
						}
					}
				}
			}
			if tc.cancelNow {
				cancel()
			}
			for _, err := range f.close(t, 4) {
				if err != nil {
					t.Errorf("Close with unconsumed events: %v", err)
				}
			}
			requireHostClosed(t, f)
		})
	}
}

func TestHostCancelledDecodeErrorIsRetained(t *testing.T) {
	f := newHostFixture(t, context.Background(), 1)
	peer := f.dial(t)
	if _, err := io.WriteString(peer, "{\"Type\":\"queued\"}\n{\"Type\":!}\n"); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(hostTestTimeout)
	defer timer.Stop()
	for len(f.host.Events()) != 1 {
		select {
		case <-timer.C:
			t.Fatal("host did not queue message")
		default:
			runtime.Gosched()
		}
	}
	f.cancel()
	// Cancellation releases a full-queue error publisher; EOF joins read teardown.
	var b [1]byte
	if _, err := peer.Read(b[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("cancelled reader did not close peer: %v", err)
	}
	for _, err := range f.close(t, 1) {
		var syntax *json.SyntaxError
		if !errors.As(err, &syntax) {
			t.Errorf("Close lost decode error under cancellation/full queue: %v", err)
		}
	}
	event := hostEvent(t, f.host)
	if event.Message.Type != "queued" || event.Err != nil {
		t.Errorf("buffered event changed during Close: %v", event.Err)
	}
	requireHostClosed(t, f)
}

func TestHostDeadlineClosesIdlePeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f := newHostFixture(t, ctx, 2)
	peer := f.dial(t)
	if err := json.NewEncoder(peer).Encode(protocol.Control{Type: "accepted"}); err != nil {
		t.Fatal(err)
	}
	if event := hostEvent(t, f.host); event.Err != nil || event.Message.Type != "accepted" {
		t.Fatalf("accept barrier: %v", event.Err)
	}
	// Wait for actual remote teardown, not a sleep followed by a racing Close.
	var b [1]byte
	if _, err := peer.Read(b[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("deadline did not close idle peer: %v", err)
	}
	for _, err := range f.close(t, 1) {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Errorf("Close did not retain socket deadline error: %v", err)
		}
	}
	requireHostClosed(t, f)
}

func TestWriteEnvelopePreservesDeclarations(t *testing.T) {
	request := contract.Request{
		Identity: contract.Identity{RunID: "run", InvocationID: "invocation", AttemptID: "attempt", DispatchToken: "token"},
		Output:   contract.OutputSpec{SchemaID: "fixture/v1"},
		Prompt:   "not part of envelope",
	}
	for _, tc := range []struct {
		name, data, files string
		entries           []contract.FileEntry
	}{
		{"nil-files", `null`, `null`, nil},
		{"empty-files", `"quoted \" value"`, `[]`, []contract.FileEntry{}},
		{"ordered-files", `{"count":7,"nested":[true,null,"text"]}`, `[{"id":"second","kind":"evidence","path":"missing/b"},{"id":"first","kind":"artifact","path":"missing/a"}]`, []contract.FileEntry{{ID: "second", Kind: "evidence", Path: "missing/b"}, {ID: "first", Kind: "artifact", Path: "missing/a"}}},
		{"duplicate-ids", `false`, `[{"id":"same","kind":"artifact","path":"a"},{"id":"same","kind":"evidence","path":"b"}]`, []contract.FileEntry{{ID: "same", Kind: "artifact", Path: "a"}, {ID: "same", Kind: "evidence", Path: "b"}}},
		{"invalid-declarations", `[1,2]`, `[{"id":"","kind":"unknown","path":"../escape"},{"id":"bad/id","kind":"","path":"/anonymous/missing"},{"id":"","kind":"","path":""}]`, []contract.FileEntry{{Kind: "unknown", Path: "../escape"}, {ID: "bad/id", Path: "/anonymous/missing"}, {}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.json")
			if err := protocol.WriteEnvelope(path, request, json.RawMessage(tc.data), tc.entries); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			want := `{"meta":{"version":1,"schema_id":"fixture/v1","run_id":"run","invocation_id":"invocation","attempt_id":"attempt","dispatch_token":"token"},"data":` + tc.data + `,"files":` + tc.files + `}`
			var gotValue, wantValue any
			if err := json.Unmarshal(raw, &gotValue); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(want), &wantValue); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(gotValue, wantValue) {
				t.Error("envelope changed exact meta, data or file declarations")
			}
		})
	}
}

var errEnvelopeMarshal = errors.New("fixture marshal failure")

type failingEnvelopeData struct{}

func (failingEnvelopeData) MarshalJSON() ([]byte, error) { return nil, errEnvelopeMarshal }

func TestWriteEnvelopeMarshalFailurePreservesFile(t *testing.T) {
	for _, tc := range []struct {
		name string
		data any
	}{
		{"marshal-error", failingEnvelopeData{}},
		{"unsupported-type", make(chan int)},
		{"unsupported-value", math.NaN()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "candidate.json")
			const original = "existing candidate must survive"
			if err := os.WriteFile(path, []byte(original), 0600); err != nil {
				t.Fatal(err)
			}
			err := protocol.WriteEnvelope(path, contract.Request{}, tc.data, nil)
			if err == nil {
				t.Error("expected marshal error")
			}
			if tc.name == "marshal-error" {
				var marshalErr *json.MarshalerError
				if !errors.As(err, &marshalErr) || !errors.Is(err, errEnvelopeMarshal) {
					t.Errorf("marshal error cause was lost: %v", err)
				}
			}
			raw, readErr := os.ReadFile(path)
			if readErr != nil || string(raw) != original {
				t.Errorf("marshal failure truncated existing candidate: read error = %v", readErr)
			}
		})
	}
}

func TestWriteEnvelopeFilesystemBoundary(t *testing.T) {
	for _, name := range []string{"existing-parent", "overwrite-file", "missing-parent", "parent-is-file", "target-is-directory"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "candidate.json")
			wantError := false
			switch name {
			case "overwrite-file":
				if err := os.WriteFile(path, []byte(strings.Repeat("old", 1000)), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-parent":
				path = filepath.Join(dir, "missing", "candidate.json")
				wantError = true
			case "parent-is-file":
				parent := filepath.Join(dir, "file")
				if err := os.WriteFile(parent, []byte("parent"), 0600); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(parent, "candidate.json")
				wantError = true
			case "target-is-directory":
				path = dir
				wantError = true
			}
			err := protocol.WriteEnvelope(path, contract.Request{}, "value", []contract.FileEntry{})
			if wantError {
				var pathErr *os.PathError
				if !errors.As(err, &pathErr) || pathErr.Path != path {
					t.Errorf("want filesystem error retaining target path, got %v", err)
				}
				if name == "missing-parent" {
					if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
						t.Errorf("writer created missing parent: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil || !json.Valid(raw) {
				t.Errorf("write did not replace complete envelope: %v", err)
			}
		})
	}
}
