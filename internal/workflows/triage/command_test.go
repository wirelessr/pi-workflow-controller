package triage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
)

func buildAcquisitionHelper(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "jira-triage-acquire")
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-p", "1", "-buildvcs=false", "-o", binary, "../../../cmd/jira-triage-acquire")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build helper: %v\n%s", err, out)
	}
	return binary
}

func invokeAcquisitionHelper(t *testing.T, ctx context.Context, binary, request string, options acquisitionOptions) {
	t.Helper()
	cmd := exec.CommandContext(ctx, binary, request)
	cmd.Dir = t.TempDir()
	cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0")
	cmd.Stdin = bytes.NewReader(testJSON(options))
	if out, err := cmd.CombinedOutput(); err != nil || len(out) != 0 {
		t.Fatalf("acquisition command: %v\n%s", err, out)
	}
}

func commandRequest(t *testing.T, root string) string {
	t.Helper()
	req := contract.Request{
		Identity: contract.Identity{RunID: "anonymous-run", InvocationID: "invocation", AttemptID: "attempt", DispatchToken: "dispatch"},
		Prompt:   string(testJSON(stageTask{Stage: "intake", Scope: testScope()})),
		Output:   contract.OutputSpec{SchemaID: IntakeSchema},
	}
	path := filepath.Join(root, "request.json")
	if err := os.WriteFile(path, testJSON(req), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAcquisitionCommand(t *testing.T) {
	binary := buildAcquisitionHelper(t)
	for _, mode := range []string{"complete", "page-failure", "attachment-partial", "invalid-config", "oversized-config", "wrong-schema", "wrong-stage", "missing-identity", "candidate-collision", "evidence-collision", "evidence-symlink", "request-fifo"} {
		t.Run(mode, func(t *testing.T) {
			var requests atomic.Int32
			server := acquireFixture(t, mode, []byte("event text"), "text/plain", func(*http.Request) { requests.Add(1) })
			root := acquireRoot(t)
			request := commandRequest(t, root)
			config := testJSON(acquisitionOptions{BaseURL: server.URL, Authorization: "must-not-send"})
			switch mode {
			case "invalid-config":
				config = []byte(`{"authorization":"must-not-send",oops}`)
			case "oversized-config":
				config = bytes.Repeat([]byte("x"), (64<<10)+1)
			case "wrong-schema", "wrong-stage", "missing-identity":
				raw, err := os.ReadFile(request)
				if err != nil {
					t.Fatal(err)
				}
				var req contract.Request
				if err := json.Unmarshal(raw, &req); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "wrong-schema":
					req.Output.SchemaID = ContextSchema
				case "wrong-stage":
					req.Prompt = string(testJSON(stageTask{Stage: "context", Scope: testScope()}))
				case "missing-identity":
					req.Identity.DispatchToken = ""
				}
				if err := os.WriteFile(request, testJSON(req), 0600); err != nil {
					t.Fatal(err)
				}
			case "candidate-collision", "evidence-collision":
				path := filepath.Join(root, "candidate.json")
				if mode == "evidence-collision" {
					path = filepath.Join(root, "evidence", "acquisition-metadata")
				}
				if err := os.WriteFile(path, []byte("owned"), 0600); err != nil {
					t.Fatal(err)
				}
			case "evidence-symlink":
				if err := os.Remove(filepath.Join(root, "evidence")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), filepath.Join(root, "evidence")); err != nil {
					t.Fatal(err)
				}
			case "request-fifo":
				if err := os.Remove(request); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(request, 0600); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, request)
			cmd.Dir = t.TempDir()
			cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0")
			cmd.Stdin = bytes.NewReader(config)
			output, err := cmd.CombinedOutput()
			wantSuccess := mode == "complete" || mode == "page-failure" || mode == "attachment-partial"
			if (err == nil) != wantSuccess || ctx.Err() != nil || strings.Contains(string(output), "must-not-send") {
				t.Fatalf("command status/diagnostic: %v %s", err, output)
			}
			raw, readErr := os.ReadFile(filepath.Join(root, "candidate.json"))
			if !wantSuccess {
				if requests.Load() != 0 {
					t.Fatal("invalid input performed HTTP")
				}
				if mode == "candidate-collision" {
					if readErr != nil || string(raw) != "owned" {
						t.Fatal("candidate overwritten")
					}
				} else if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatal("failed command left candidate")
				}
				return
			}
			var candidate publication[Intake]
			if readErr != nil || json.Unmarshal(raw, &candidate) != nil || candidate.Data.Complete != (mode == "complete") || requests.Load() != 6 {
				t.Fatalf("acquisition output: %s %v", raw, readErr)
			}
			metadata := acquireMetadata(t, root)
			if metadata.Partial != !candidate.Data.Complete {
				t.Fatal("partial diagnostics lost")
			}
		})
	}
}

func TestAcquisitionCommandStdinSignal(t *testing.T) {
	binary := buildAcquisitionHelper(t)
	root := acquireRoot(t)
	request := commandRequest(t, root)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, request)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("stdin helper did not join")
		}
	})
	// Fill the bounded input without EOF. No acquisition can begin; the next
	// read still needs the lookahead byte or EOF even after consuming this data.
	if _, err := stdin.Write(bytes.Repeat([]byte(" "), AcquisitionConfigLimit)); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		done <- err
		var exit *exec.ExitError
		if ctx.Err() != nil || !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
			t.Fatalf("stdin helper did not exit from SIGTERM: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("signal did not interrupt stdin")
	}
	if _, err := os.Stat(filepath.Join(root, "candidate.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stdin cancellation produced candidate")
	}
}

func TestAcquisitionFileLimitSubprocess(t *testing.T) {
	binary := os.Getenv("PWC_TRIAGE_FILE_LIMIT_BINARY")
	if binary == "" {
		return
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 4096, Max: 4096}); err != nil {
		os.Exit(3)
	}
	if err := syscall.Exec(binary, []string{binary, os.Getenv("PWC_TRIAGE_FILE_LIMIT_REQUEST")}, os.Environ()); err != nil {
		os.Exit(4)
	}
}

func TestAcquisitionCommandWriteFailure(t *testing.T) {
	binary := buildAcquisitionHelper(t)
	_, raw := intakeFixture("complete")
	var issue map[string]any
	if err := json.Unmarshal(raw["issue"], &issue); err != nil {
		t.Fatal(err)
	}
	attachments := []any{}
	for i := 0; i < 40; i++ {
		attachments = append(attachments, map[string]any{"id": strings.Repeat("a", i+1), "size": 0})
	}
	issue["fields"].(map[string]any)["attachment"] = attachments
	raw["issue"] = testJSON(issue)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := "issue"
		switch r.URL.Path {
		case "/rest/api/3/field":
			id = "fields"
		case "/rest/api/3/issue/CASE-18":
			id = "linked"
		case "/rest/api/3/issue/CASE-17/comment":
			id = "page-" + r.URL.Query().Get("startAt")
		}
		_, _ = w.Write(raw[id])
	}))
	defer server.Close()
	root := acquireRoot(t)
	request := commandRequest(t, root)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestAcquisitionFileLimitSubprocess$")
	cmd.Env = append(os.Environ(), "PWC_TRIAGE_FILE_LIMIT_BINARY="+binary, "PWC_TRIAGE_FILE_LIMIT_REQUEST="+request, "GORACE=atexit_sleep_ms=0")
	cmd.Stdin = bytes.NewReader(testJSON(acquisitionOptions{BaseURL: server.URL, MaxBytes: 4096}))
	output, err := cmd.CombinedOutput()
	if err == nil || ctx.Err() != nil || !strings.Contains(string(output), "candidate.json: file too large") {
		t.Fatalf("expected candidate filesystem failure: %v %s", err, output)
	}
	if _, err := os.Stat(filepath.Join(root, "candidate.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed candidate not removed")
	}
	if _, err := os.Stat(filepath.Join(root, "evidence", "issue")); err != nil {
		t.Fatal("raw evidence removed")
	}
	if !acquireMetadata(t, root).Partial {
		t.Fatal("acquisition diagnostics lost")
	}
}

func TestAcquisitionCommandSignal(t *testing.T) {
	binary := buildAcquisitionHelper(t)
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"key":`)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	root := acquireRoot(t)
	request := commandRequest(t, root)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, request)
	cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0")
	cmd.Stdin = bytes.NewReader(testJSON(acquisitionOptions{BaseURL: server.URL}))
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("helper did not join")
		}
	})
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("HTTP not started")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		done <- err
		if err == nil || !strings.Contains(output.String(), "context canceled") {
			t.Fatalf("signal ignored: %v %s", err, output.String())
		}
	case <-ctx.Done():
		t.Fatal("helper cancellation timed out")
	}
	if _, err := os.Stat(filepath.Join(root, "candidate.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("cancelled acquisition produced candidate")
	}
	metadata := acquireMetadata(t, root)
	if !metadata.Partial || len(metadata.Records) != 1 || metadata.Records[0].Source.Status != "partial" {
		t.Fatal("interrupted diagnostics lost")
	}
}
