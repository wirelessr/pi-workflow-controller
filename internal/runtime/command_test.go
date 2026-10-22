package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"pi-workflow-controller/internal/testutil/pathexec"
)

// The real test binary is the external command; tests select its behavior with
// PWC_COMMAND_HELPER, which the sanitized environment preserves.
func TestCommandHelper(t *testing.T) {
	mode := os.Getenv("PWC_COMMAND_HELPER")
	if mode == "" {
		return
	}
	args, _ := pathexec.Args()
	dir := os.Getenv("PWC_COMMAND_DIR")
	// An attached descendant holds the output pipes open; a detached one does not.
	sleeper := func(attached bool) *exec.Cmd {
		child := exec.Command("/bin/sleep", "60")
		if attached {
			child.Stdout, child.Stderr = os.Stdout, os.Stderr
		}
		if child.Start() != nil {
			os.Exit(2)
		}
		// Publish atomically so an existence check implies complete pid bytes.
		if os.WriteFile(filepath.Join(dir, "pid.tmp"), []byte(strconv.Itoa(child.Process.Pid)), 0600) != nil || os.Rename(filepath.Join(dir, "pid.tmp"), filepath.Join(dir, "pid")) != nil {
			os.Exit(2)
		}
		return child
	}
	switch mode {
	case "echo":
		cwd, err := os.Getwd()
		if err != nil {
			os.Exit(2)
		}
		data, err := json.Marshal(map[string]any{"args": args, "env": os.Environ(), "cwd": cwd})
		if err != nil {
			os.Exit(2)
		}
		_, _ = os.Stdout.Write(data)
		os.Exit(0)
	case "fail":
		fmt.Fprintln(os.Stderr, "SECRET failure credential")
		os.Exit(3)
	case "sentinel":
		_ = os.WriteFile(filepath.Join(dir, "started"), nil, 0600)
		os.Exit(0)
	case "background":
		sleeper(false)
		_, _ = os.Stdout.Write([]byte("done"))
		os.Exit(0)
	case "oversized":
		child := sleeper(true)
		block := bytes.Repeat([]byte{'x'}, 1<<20)
		for i := 0; i < 65; i++ {
			if _, err := os.Stdout.Write(block); err != nil {
				os.Exit(2)
			}
		}
		_ = child.Wait()
		os.Exit(0)
	case "block":
		child := sleeper(true)
		fmt.Fprintln(os.Stderr, "SECRET blocked credential")
		_ = child.Wait()
		os.Exit(2)
	}
	os.Exit(2)
}

type commandFixture struct {
	dir, exe string
}

func newCommandFixture(t *testing.T, mode string) commandFixture {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	f := commandFixture{dir: t.TempDir(), exe: exe}
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	t.Setenv("PWC_COMMAND_HELPER", mode)
	t.Setenv("PWC_COMMAND_DIR", f.dir)
	return f
}

func (f commandFixture) run(ctx context.Context, env ...string) ([]byte, error) {
	return command(ctx, f.dir, f.exe, []string{"-test.run=^TestCommandHelper$", "--", "fixed", "argv"}, env)
}

func (f commandFixture) waitPID(t *testing.T, done <-chan error) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if raw, err := os.ReadFile(filepath.Join(f.dir, "pid")); err == nil {
			pid, err := strconv.Atoi(string(raw))
			if err != nil {
				t.Fatal(err)
			}
			return pid
		}
		if done != nil {
			select {
			case err := <-done:
				t.Fatalf("command ended before its descendant started: %v", err)
			default:
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("command did not start its descendant")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func requireReaped(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			t.Fatalf("owned descendant %d survived", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type commandEcho struct {
	Args []string `json:"args"`
	Env  []string `json:"env"`
	CWD  string   `json:"cwd"`
}

func decodeEcho(t *testing.T, raw []byte, err error) commandEcho {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	var echo commandEcho
	if err := json.Unmarshal(raw, &echo); err != nil {
		t.Fatalf("helper output %q: %v", raw, err)
	}
	return echo
}

func TestCommandSuccessEnvironmentAndDirectory(t *testing.T) {
	f := newCommandFixture(t, "echo")
	for key, value := range map[string]string{
		"GIT_DIR": "/foreign", "GIT_CONFIG_GLOBAL": "/foreign", "GIT_TERMINAL_PROMPT": "1",
		"GH_HOST": "example.invalid", "GH_ENTERPRISE_TOKEN": "foreign", "GH_PAGER": "less",
		"GITHUB_API_URL": "https://example.invalid", "GITHUB_SERVER_URL": "https://example.invalid",
		"GH_TOKEN": "kept-token", "GH_CONFIG_DIR": "/kept-config", "GITHUB_TOKEN": "kept-github",
	} {
		t.Setenv(key, value)
	}
	caller := []string{"GIT_CONFIG_NOSYSTEM=1", "CALLER_LAST=1"}
	raw, err := f.run(context.Background(), caller...)
	echo := decodeEcho(t, raw, err)
	if !reflect.DeepEqual(echo.Args, []string{"fixed", "argv"}) {
		t.Fatalf("argv: %q", echo.Args)
	}
	dir, err := filepath.EvalSymlinks(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	if cwd, err := filepath.EvalSymlinks(echo.CWD); err != nil || cwd != dir {
		t.Fatalf("cwd: %q, want %q: %v", echo.CWD, dir, err)
	}
	want := []string{"GH_HOST=github.com", "GH_PROMPT_DISABLED=1", "GH_PAGER=cat", "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "CALLER_LAST=1"}
	// Failures name only fixture values; the inherited environment may hold credentials.
	if len(echo.Env) < len(want) || !reflect.DeepEqual(echo.Env[len(echo.Env)-len(want):], want) {
		t.Fatalf("fixed and caller environment must end the environment, want suffix %q", want)
	}
	env := map[string]bool{}
	for _, v := range echo.Env {
		env[v] = true
	}
	for _, v := range []string{
		"GIT_DIR=/foreign", "GIT_CONFIG_GLOBAL=/foreign", "GIT_TERMINAL_PROMPT=1",
		"GH_HOST=example.invalid", "GH_ENTERPRISE_TOKEN=foreign", "GH_PAGER=less",
		"GITHUB_API_URL=https://example.invalid", "GITHUB_SERVER_URL=https://example.invalid",
	} {
		if env[v] {
			t.Fatalf("inherited %s was not removed", v)
		}
	}
	for _, v := range []string{"GH_TOKEN=kept-token", "GH_CONFIG_DIR=/kept-config", "GITHUB_TOKEN=kept-github"} {
		if !env[v] {
			t.Fatalf("%s was not inherited", v)
		}
	}
}

func TestCommandFixedEntryPoints(t *testing.T) {
	for _, tc := range []struct {
		name, executable string
		run              func(context.Context, string) ([]byte, error)
		want, callerEnv  []string
	}{
		{"github-get", "gh", func(ctx context.Context, dir string) ([]byte, error) {
			return GitHubGET(ctx, dir, "repos/o/r/pulls/1", false)
		}, []string{"api", "--method", "GET", "--hostname", "github.com", "-H", "Accept: application/vnd.github+json", "repos/o/r/pulls/1"}, nil},
		{"github-get-paginate", "gh", func(ctx context.Context, dir string) ([]byte, error) {
			return GitHubGET(ctx, dir, "repos/o/r/issues/1/comments?per_page=100", true)
		}, []string{"api", "--method", "GET", "--hostname", "github.com", "-H", "Accept: application/vnd.github+json", "repos/o/r/issues/1/comments?per_page=100", "--paginate", "--slurp"}, nil},
		{"git", "git", func(ctx context.Context, dir string) ([]byte, error) {
			return Git(ctx, dir, []string{"CALLER_GIT_ENV=1"}, "--no-pager", "rev-parse", "HEAD")
		}, []string{
			"-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential", "-c", "credential.interactive=false",
			"-c", "gc.auto=0", "-c", "maintenance.auto=false", "-c", "core.fsmonitor=false",
			"--no-pager", "rev-parse", "HEAD",
		}, []string{"CALLER_GIT_ENV=1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCommandFixture(t, "echo")
			pathexec.Install(t, tc.executable, "TestCommandHelper")
			raw, err := tc.run(context.Background(), f.dir)
			echo := decodeEcho(t, raw, err)
			if !reflect.DeepEqual(echo.Args, tc.want) {
				t.Fatalf("argv: %q, want %q", echo.Args, tc.want)
			}
			dir, err := filepath.EvalSymlinks(f.dir)
			if err != nil {
				t.Fatal(err)
			}
			if cwd, err := filepath.EvalSymlinks(echo.CWD); err != nil || cwd != dir {
				t.Fatalf("cwd: %q, want %q: %v", echo.CWD, dir, err)
			}
			// The shell wrapper does not preserve environment order; the
			// direct command test pins it.
			for _, want := range tc.callerEnv {
				if !slices.Contains(echo.Env, want) {
					t.Fatalf("caller environment %s was not passed", want)
				}
			}
		})
	}
}

// An endpoint gh would parse as an option is rejected before gh starts.
func TestGitHubGETRejectsEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "-X", "--method=POST"} {
		t.Run(endpoint, func(t *testing.T) {
			f := newCommandFixture(t, "sentinel")
			pathexec.Install(t, "gh", "TestCommandHelper")
			if raw, err := GitHubGET(context.Background(), f.dir, endpoint, false); raw != nil || err == nil || err.Error() != "invalid GitHub API endpoint" {
				t.Fatalf("endpoint %q accepted: %q %v", endpoint, raw, err)
			}
			if _, err := os.Stat(filepath.Join(f.dir, "started")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("gh started for a rejected endpoint: %v", err)
			}
		})
	}
	t.Run("sentinel-control", func(t *testing.T) {
		f := newCommandFixture(t, "sentinel")
		pathexec.Install(t, "gh", "TestCommandHelper")
		if _, err := GitHubGET(context.Background(), f.dir, "repos/o/r/pulls/1", false); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(f.dir, "started")); err != nil {
			t.Fatalf("accepted endpoint did not start gh: %v", err)
		}
	})
}

// The values review used before the definition moved here; a caller that
// modifies its copy does not change the next caller's.
func TestGitConfigIsolation(t *testing.T) {
	want := []string{"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_ATTR_NOSYSTEM=1"}
	got := GitConfigIsolation()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GitConfigIsolation() = %q, want %q", got, want)
	}
	got[0] = "GIT_CONFIG_NOSYSTEM=0"
	if again := GitConfigIsolation(); !reflect.DeepEqual(again, want) {
		t.Fatalf("shared definition modified through a caller's copy: %q", again)
	}
}

func TestCommandFailures(t *testing.T) {
	for _, mode := range []string{"fail", "missing", "oversized", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			helper := mode
			if mode == "deadline" {
				helper = "block"
			}
			f := newCommandFixture(t, helper)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()
			}
			var raw []byte
			var err error
			if mode == "missing" {
				raw, err = command(ctx, f.dir, filepath.Join(f.dir, "missing-executable"), nil, nil)
			} else {
				raw, err = f.run(ctx)
			}
			if raw != nil || err == nil || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "fixed") {
				t.Fatalf("failure leaked output, argv or stderr: %q %v", raw, err)
			}
			want := map[string]string{
				"fail":      "acquisition command failed",
				"missing":   "acquisition command could not start",
				"oversized": ErrCommandOutputLimit.Error(),
				"deadline":  context.DeadlineExceeded.Error(),
			}[mode]
			// Darwin reports EPERM for a group holding only its zombie leader;
			// after a failed Wait that is reported as a cleanup failure.
			if mode == "fail" && errors.Is(err, syscall.EPERM) {
				want = "acquisition process group cleanup failed: " + syscall.EPERM.Error()
			}
			if err.Error() != want {
				t.Fatalf("got %v, want %q", err, want)
			}
			if mode == "oversized" && !errors.Is(err, ErrCommandOutputLimit) {
				t.Fatalf("output limit lost: %v", err)
			}
			if mode == "deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline lost: %v", err)
			}
			if mode == "oversized" || mode == "deadline" {
				requireReaped(t, f.waitPID(t, nil))
			}
		})
	}
}

func TestCommandReapsSuccessfulGroup(t *testing.T) {
	f := newCommandFixture(t, "background")
	raw, err := f.run(context.Background())
	if err != nil || string(raw) != "done" {
		t.Fatalf("successful command: %q %v", raw, err)
	}
	requireReaped(t, f.waitPID(t, nil))
}

func TestCommandCancellation(t *testing.T) {
	t.Run("before-start", func(t *testing.T) {
		f := newCommandFixture(t, "sentinel")
		cause := errors.New("caller stopped before start")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		if raw, err := f.run(ctx); raw != nil || err != cause {
			t.Fatalf("cancelled command: %q %v", raw, err)
		}
		if _, err := os.Stat(filepath.Join(f.dir, "started")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cancelled command started: %v", err)
		}
	})
	t.Run("running", func(t *testing.T) {
		f := newCommandFixture(t, "block")
		cause := errors.New("caller stopped running command")
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		done := make(chan error, 1)
		go func() {
			raw, err := f.run(ctx)
			if raw != nil {
				err = fmt.Errorf("cancelled command returned output %q: %w", raw, err)
			}
			done <- err
		}()
		pid := f.waitPID(t, done)
		cancel(cause)
		select {
		case err := <-done:
			if err != cause {
				t.Fatalf("cancellation cause: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancelled command did not join")
		}
		requireReaped(t, pid)
	})
}
