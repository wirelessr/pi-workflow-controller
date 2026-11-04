package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ErrCommandOutputLimit is the cause when a gh or git command's stdout exceeds the fixed cap.
var ErrCommandOutputLimit = errors.New("acquisition command stdout limit exceeded")

const (
	commandOutputLimit = 64 << 20
	commandDeadline    = 20 * time.Minute
)

// Git credentials come only from gh's helper, passed on each invocation;
// nothing is written to any git config. Automatic gc, maintenance and
// fsmonitor are disabled because they start detached daemons that the
// process-group kill cannot reach.
var gitFixed = []string{
	"-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential", "-c", "credential.interactive=false",
	"-c", "gc.auto=0", "-c", "maintenance.auto=false", "-c", "core.fsmonitor=false",
}

// GitHubGET runs gh api --method GET --hostname github.com
// -H "Accept: application/vnd.github+json" endpoint, adding --paginate --slurp when paginate is set.
// An empty endpoint or one gh would parse as an option is rejected before gh starts.
func GitHubGET(ctx context.Context, dir, endpoint string, paginate bool) ([]byte, error) {
	if endpoint == "" || strings.HasPrefix(endpoint, "-") {
		return nil, errors.New("invalid GitHub API endpoint")
	}
	args := []string{"api", "--method", "GET", "--hostname", "github.com", "-H", "Accept: application/vnd.github+json", endpoint}
	if paginate {
		args = append(args, "--paginate", "--slurp")
	}
	return command(ctx, dir, "gh", args, nil)
}

// graphQLName is a GraphQL variable name.
var graphQLName = regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]*$`)

// GitHubGraphQL runs gh api graphql --hostname github.com -f query=<query>,
// then each of strs as -f name=value and each of ints as -F name=<n>, each
// group in name order, then --paginate --slurp: the result is the array of
// pages, which the caller checks. To page, the query declares $endCursor and
// selects pageInfo of the connection.
//
// The read-only guarantee comes from query being a Controller constant that
// is a query operation. The check that the document starts with the query
// keyword only catches a programming error: it is not a parser and admits a
// constant that also holds another operation. Before gh starts it rejects
// such a document, a variable name that is not a GraphQL name, is query or
// operationName (gh sends those two as request fields, not variables), and
// a name in both groups; a NUL in the document or a value fails at exec,
// also before gh starts. -f values are sent as strings and never expanded
// from a file.
func GitHubGraphQL(ctx context.Context, dir, query string, strs map[string]string, ints map[string]int) ([]byte, error) {
	rest, ok := strings.CutPrefix(strings.TrimLeft(query, " \t\r\n"), "query")
	if !ok || rest == "" || !strings.ContainsRune(" \t\r\n({", rune(rest[0])) {
		return nil, errors.New("invalid GitHub GraphQL query: want a query operation")
	}
	variable := func(name string) bool {
		return graphQLName.MatchString(name) && name != "query" && name != "operationName"
	}
	args := []string{"api", "graphql", "--hostname", "github.com", "-f", "query=" + query}
	for _, name := range slices.Sorted(maps.Keys(strs)) {
		if _, both := ints[name]; !variable(name) || both {
			return nil, errors.New("invalid GitHub GraphQL variable")
		}
		args = append(args, "-f", name+"="+strs[name])
	}
	for _, name := range slices.Sorted(maps.Keys(ints)) {
		if !variable(name) {
			return nil, errors.New("invalid GitHub GraphQL variable")
		}
		args = append(args, "-F", name+"="+strconv.Itoa(ints[name]))
	}
	return command(ctx, dir, "gh", append(args, "--paginate", "--slurp"), nil)
}

// Git runs git from PATH in dir with the gh credential helper and the
// gc/maintenance/fsmonitor overrides prepended to args. env is appended after
// the sanitized environment.
//
// The inherited environment keeps GH_TOKEN and GITHUB_TOKEN, so every git
// subprocess, including helpers and filters from git config, can read them;
// callers decide which system and global git config to allow. The
// process-group kill does not reach descendants that call setsid. A non-zero
// exit is reported without its exit code or stdout.
func Git(ctx context.Context, dir string, env []string, args ...string) ([]byte, error) {
	return command(ctx, dir, "git", append(append([]string{}, gitFixed...), args...), env)
}

// GitConfigIsolation is the Git env that ignores system and global
// configuration and system attributes, for calls that must not run the
// user's hooks, filters or URL rewrites. The global attributes file is not
// covered; callers also pass core.attributesFile=/dev/null.
func GitConfigIsolation() []string {
	return []string{"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_ATTR_NOSYSTEM=1"}
}

type commandOutput struct {
	data   []byte
	cancel context.CancelCauseFunc
}

func (b *commandOutput) Write(p []byte) (int, error) {
	if len(p) > commandOutputLimit-len(b.data) {
		b.cancel(ErrCommandOutputLimit)
		return 0, ErrCommandOutputLimit
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

// This is private plumbing for the fixed gh/git commands, not a workflow
// executor. Neither argv nor external stderr is included in errors.
func command(parent context.Context, dir, executable string, args, env []string) ([]byte, error) {
	deadline, stop := context.WithTimeout(parent, commandDeadline)
	defer stop()
	ctx, cancel := context.WithCancelCause(deadline)
	defer cancel(nil)
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	cmd := exec.Command(executable, args...)
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	for _, v := range os.Environ() {
		key, _, _ := strings.Cut(v, "=")
		if strings.HasPrefix(key, "GIT_") || strings.HasPrefix(key, "GH_") && key != "GH_TOKEN" && key != "GH_CONFIG_DIR" || key == "GITHUB_API_URL" || key == "GITHUB_SERVER_URL" {
			continue
		}
		cmd.Env = append(cmd.Env, v)
	}
	cmd.Env = append(cmd.Env, "GH_HOST=github.com", "GH_PROMPT_DISABLED=1", "GH_PAGER=cat", "GIT_TERMINAL_PROMPT=0")
	cmd.Env = append(cmd.Env, env...)
	stdout, stdoutChild, err := os.Pipe()
	if err != nil {
		return nil, errors.New("acquisition stdout pipe failed")
	}
	defer func(f *os.File) { _ = f.Close() }(stdout)
	defer func(f *os.File) { _ = f.Close() }(stdoutChild)
	stderr, stderrChild, err := os.Pipe()
	if err != nil {
		return nil, errors.New("acquisition stderr pipe failed")
	}
	defer func(f *os.File) { _ = f.Close() }(stderr)
	defer func(f *os.File) { _ = f.Close() }(stderrChild)
	cmd.Stdout, cmd.Stderr = stdoutChild, stderrChild
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if err := cmd.Start(); err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, errors.New("acquisition command could not start")
	}
	_ = stdoutChild.Close()
	_ = stderrChild.Close()
	output := &commandOutput{cancel: cancel}
	readDone := make(chan error, 2)
	go func() { _, err := io.Copy(output, stdout); readDone <- err }()
	// Discarding stderr provides constant memory without persisting credentials
	// that a failed HTTP transport or credential helper might have printed.
	go func() { _, err := io.Copy(io.Discard, stderr); readDone <- err }()
	readers := 0
drain:
	for readers < 2 {
		select {
		case readErr := <-readDone:
			readers++
			if readErr != nil {
				cancel(errors.New("acquisition command output read failed"))
				break drain
			}
		case <-ctx.Done():
			break drain
		}
	}
	// Retain the unreaped leader until after signalling its owned group, so
	// SIGKILL cannot race PID/PGID reuse. EOF only triggers conservative group
	// shutdown; success still requires the command's own successful Wait.
	outputClosed := readers == 2
	killErr := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	_ = stdout.Close()
	_ = stderr.Close()
	waitErr := cmd.Wait()
	for readers < 2 {
		<-readDone
		readers++
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	// Darwin returns EPERM for a group containing only its zombie leader.
	// Accept that case only after both pipes closed and Wait proved success.
	if killErr != nil && !errors.Is(killErr, syscall.ESRCH) && (!errors.Is(killErr, syscall.EPERM) || !outputClosed || waitErr != nil) {
		return nil, fmt.Errorf("acquisition process group cleanup failed: %w", killErr)
	}
	if waitErr != nil {
		return nil, errors.New("acquisition command failed")
	}
	return output.data, nil
}
