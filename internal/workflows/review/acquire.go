package review

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

// Checkout is a pinned PR acquisition, not a view of a caller's local repository.
// Missing contains unavailable source/content keys, including external gitlink
// content. Missing content is never represented by an empty successful snapshot.
type Checkout struct {
	URL, Repository                                             string
	Number                                                      int
	BaseSHA, HeadSHA, MergeBase, DiffRange, ContextID, Worktree string
	Snapshots                                                   map[string]string
	Missing                                                     []string

	mu                                     sync.Mutex
	root, repository, worktree             string
	rootInfo, repositoryInfo, worktreeInfo os.FileInfo
	cleaned                                bool
	snapshotHashes                         map[string][32]byte
}

var (
	prPath     = regexp.MustCompile(`^/([A-Za-z0-9][A-Za-z0-9-]*)/([A-Za-z0-9_.-]+)/pull/([1-9][0-9]*)/?$`)
	shaPattern = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
)

// ParsePRURL accepts only a literal github.com HTTPS pull-request URL.
func ParsePRURL(raw string) (repository string, number int, err error) {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(raw, "#%") {
		return "", 0, errors.New("invalid GitHub PR URL")
	}
	m := prPath.FindStringSubmatch(u.Path)
	if m == nil || m[2] == "." || m[2] == ".." {
		return "", 0, errors.New("invalid GitHub PR path")
	}
	n, e := strconv.Atoi(m[3])
	if e != nil || n <= 0 {
		return "", 0, errors.New("invalid GitHub PR number")
	}
	return m[1] + "/" + m[2], n, nil
}

// acquisitionSource is solely the external GitHub boundary. Tests may replace
// gh and the GitHub transport endpoint, but never the local git operations.
type acquisitionSource struct {
	gh       string
	ghPrefix []string
	remote   func(string) string
}

func Acquire(ctx context.Context, root, prURL string) (*Checkout, error) {
	return acquire(ctx, root, prURL, acquisitionSource{
		gh:     "gh",
		remote: func(repository string) string { return "https://github.com/" + repository + ".git" },
	})
}

type prMetadata struct {
	Number int        `json:"number"`
	URL    string     `json:"html_url"`
	Base   prRevision `json:"base"`
	Head   prRevision `json:"head"`
}
type prRevision struct {
	SHA  string `json:"sha"`
	Ref  string `json:"ref"`
	Repo struct {
		FullName string `json:"full_name"`
	} `json:"repo"`
}

func acquire(ctx context.Context, root, prURL string, source acquisitionSource) (result *Checkout, err error) {
	repository, number, err := ParsePRURL(prURL)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if !filepath.IsAbs(root) {
		return nil, errors.New("acquisition root must be absolute")
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("acquisition root must be a private directory, not a symlink")
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	c := &Checkout{URL: fmt.Sprintf("https://github.com/%s/pull/%d", repository, number), Repository: repository, Number: number,
		root: root, rootInfo: info, repository: filepath.Join(root, "repository.git"), worktree: filepath.Join(root, "checkout"), Snapshots: make(map[string]string), snapshotHashes: make(map[string][32]byte)}
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if cleanupErr := c.Cleanup(cleanupCtx); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("acquisition cleanup: %w", cleanupErr))
			}
		}
	}()
	// Reserve all paths before starting children, so an existing repository or
	// another run can never become an implicit acquisition target.
	for _, path := range []string{c.repository, c.worktree, filepath.Join(root, "snapshots")} {
		if err = os.Mkdir(path, 0700); err != nil {
			return nil, err
		}
		switch path {
		case c.repository:
			c.repositoryInfo, err = os.Lstat(path)
		case c.worktree:
			c.worktreeInfo, err = os.Lstat(path)
		}
		if err != nil {
			return nil, err
		}
	}

	endpoint := fmt.Sprintf("repos/%s/pulls/%d", repository, number)
	metadata, err := source.api(ctx, root, endpoint, false)
	if err != nil {
		return nil, fmt.Errorf("PR metadata: %w", err)
	}
	var pr prMetadata
	if json.Unmarshal(metadata, &pr) != nil {
		return nil, errors.New("invalid PR metadata JSON")
	}
	metaRepo, metaNumber, parseErr := ParsePRURL(pr.URL)
	if parseErr != nil || !strings.EqualFold(metaRepo, repository) || metaNumber != number || pr.Number != number || !strings.EqualFold(pr.Base.Repo.FullName, repository) {
		return nil, errors.New("PR metadata identity mismatch")
	}
	for _, revision := range []prRevision{pr.Base, pr.Head} {
		validated, _, e := ParsePRURL("https://github.com/" + revision.Repo.FullName + "/pull/1")
		if e != nil || validated != revision.Repo.FullName || revision.Ref == "" || !shaPattern.MatchString(revision.SHA) {
			return nil, errors.New("invalid PR revision metadata")
		}
	}
	c.BaseSHA, c.HeadSHA = strings.ToLower(pr.Base.SHA), strings.ToLower(pr.Head.SHA)
	if err = c.snapshot("metadata", "json", metadata); err != nil {
		return nil, err
	}

	if _, err = c.git(ctx, "init", "--bare", "--template=", c.repository); err != nil {
		return nil, err
	}
	for _, pin := range []struct{ repository, sha, ref string }{
		{pr.Base.Repo.FullName, c.BaseSHA, "refs/review/base"},
		{pr.Head.Repo.FullName, c.HeadSHA, "refs/review/head"},
	} {
		// Keep a promisor remote for lazy blob reads during the full checkout;
		// fetch only pinned commits, never mutable branches or refs/pull/*.
		remote := strings.TrimPrefix(pin.ref, "refs/review/")
		for _, setting := range [][2]string{{"url", source.remote(pin.repository)}, {"promisor", "true"}, {"partialclonefilter", "blob:none"}} {
			if _, err = c.git(ctx, "config", "remote."+remote+"."+setting[0], setting[1]); err != nil {
				return nil, err
			}
		}
		if _, err = c.git(ctx, "fetch", "--filter=blob:none", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head", remote, pin.sha+":"+pin.ref); err != nil {
			return nil, err
		}
		actual, e := c.git(ctx, "rev-parse", "--verify", pin.sha+"^{commit}")
		if e != nil {
			return nil, e
		}
		if strings.TrimSpace(string(actual)) != pin.sha {
			return nil, errors.New("fetched revision is not the pinned commit")
		}
	}
	mb, err := c.git(ctx, "merge-base", "--all", c.BaseSHA, c.HeadSHA)
	if err != nil {
		return nil, fmt.Errorf("merge-base: %w", err)
	}
	bases := strings.Fields(string(mb))
	if len(bases) != 1 || !shaPattern.MatchString(bases[0]) {
		return nil, errors.New("missing or ambiguous merge-base")
	}
	c.MergeBase = strings.ToLower(bases[0])
	c.DiffRange = c.MergeBase + ".." + c.HeadSHA
	id := sha256.Sum256([]byte(fmt.Sprintf("%s\n%d\n%s\n%s\n%s", strings.ToLower(repository), number, c.BaseSHA, c.HeadSHA, c.MergeBase)))
	c.ContextID = hex.EncodeToString(id[:])

	// Preserve gitlinks as pinned evidence, but never execute their transports.
	// Missing external content is explicit review uncertainty, not empty code.
	tree, err := c.git(ctx, "ls-tree", "-r", "-z", c.HeadSHA)
	if err != nil {
		return nil, err
	}
	var submodules []string
	for _, entry := range bytes.Split(tree, []byte{0}) {
		if bytes.HasPrefix(entry, []byte("160000 ")) {
			submodules = append(submodules, string(entry))
		}
	}
	if len(submodules) > 0 {
		data, e := json.Marshal(submodules)
		if e != nil {
			return nil, e
		}
		if err = c.snapshot("submodules", "json", data); err != nil {
			return nil, err
		}
		c.Missing = append(c.Missing, "submodule-content")
	}
	if _, err = c.git(ctx, "worktree", "add", "--detach", c.worktree, c.HeadSHA); err != nil {
		return nil, err
	}
	c.Worktree = c.worktree
	if diff, e := c.git(ctx, "diff", "--binary", "--no-ext-diff", "--no-textconv", c.MergeBase, c.HeadSHA, "--"); e != nil {
		return nil, e
	} else if err = c.snapshot("diff", "diff", diff); err != nil {
		return nil, err
	}
	names, err := c.git(ctx, "diff", "--name-only", "-z", "--no-ext-diff", "--no-textconv", c.MergeBase, c.HeadSHA, "--")
	if err != nil {
		return nil, err
	}
	files := []string{}
	if len(names) > 0 {
		if names[len(names)-1] != 0 {
			return nil, errors.New("changed-files is not NUL terminated")
		}
		for _, name := range bytes.Split(names[:len(names)-1], []byte{0}) {
			if !utf8.Valid(name) {
				return nil, errors.New("changed-files contains a non-UTF-8 path")
			}
			files = append(files, string(name))
		}
	}
	encoded, err := json.Marshal(files)
	if err != nil {
		return nil, err
	}
	if err = c.snapshot("changed-files", "json", encoded); err != nil {
		return nil, err
	}

	for _, comments := range []struct{ key, endpoint string }{
		{"issues", fmt.Sprintf("repos/%s/issues/%d/comments?per_page=100", repository, number)},
		{"inline", endpoint + "/comments?per_page=100"},
		{"reviews", endpoint + "/reviews?per_page=100"},
	} {
		data, e := source.api(ctx, root, comments.endpoint, true)
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if e != nil {
			c.Missing = append(c.Missing, comments.key)
			continue
		}
		if err = c.snapshot(comments.key, "json", data); err != nil {
			return nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	return c, nil
}

func (s acquisitionSource) api(ctx context.Context, root, endpoint string, paginate bool) ([]byte, error) {
	args := append([]string{}, s.ghPrefix...)
	args = append(args, "api", "--method", "GET", "--hostname", "github.com", "-H", "Accept: application/vnd.github+json", endpoint)
	if paginate {
		args = append(args, "--paginate", "--slurp")
	}
	data, err := acquisitionCommand(ctx, root, s.gh, args, false)
	if err != nil || !paginate {
		return data, err
	}
	var pages []json.RawMessage
	if json.Unmarshal(data, &pages) != nil || pages == nil || len(pages) == 0 {
		return nil, errors.New("invalid comment pagination")
	}
	items := []json.RawMessage{}
	for _, page := range pages {
		var values []json.RawMessage
		if json.Unmarshal(page, &values) != nil || values == nil {
			return nil, errors.New("invalid comments page")
		}
		for _, value := range values {
			if len(value) == 0 || value[0] != '{' {
				return nil, errors.New("invalid comment object")
			}
		}
		items = append(items, values...)
	}
	return json.Marshal(items)
}

func (c *Checkout) snapshot(key, extension string, data []byte) error {
	path := filepath.Join(c.root, "snapshots", key+"."+extension)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if err = errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	c.Snapshots[key] = path
	c.snapshotHashes[key] = sha256.Sum256(data)
	return nil
}

// Cleanup removes only the originally reserved worktree and its registration.
// Public fields are descriptive: changing Worktree never changes deletion scope.
// The private repository and snapshots remain available as acquisition history.
func (c *Checkout) Cleanup(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cleaned || c.worktreeInfo == nil {
		return nil
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	r, err := os.OpenRoot(c.root)
	if err != nil {
		return err
	}
	defer func(r *os.Root) { _ = r.Close() }(r)
	for _, check := range []struct {
		path string
		info os.FileInfo
	}{{".", c.rootInfo}, {"repository.git", c.repositoryInfo}, {"checkout", c.worktreeInfo}} {
		info, e := r.Lstat(check.path)
		if check.path == "checkout" && errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		if !info.IsDir() || check.info == nil || !os.SameFile(info, check.info) {
			return errors.New("acquisition cleanup ownership mismatch")
		}
	}
	// The fresh bare repo has exactly this reserved worktree name. Rooted
	// removal neither follows a modified .git link nor prunes other worktrees.
	if err = r.RemoveAll("checkout"); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err = r.RemoveAll("repository.git/worktrees/checkout"); err != nil {
		return err
	}
	c.cleaned = true
	return nil
}

func (c *Checkout) git(ctx context.Context, args ...string) ([]byte, error) {
	fixed := []string{
		"--no-pager", "--git-dir=" + c.repository,
		"-c", "core.hooksPath=/dev/null", "-c", "core.attributesFile=/dev/null",
		"-c", "core.fsmonitor=false", "-c", "core.autocrlf=false", "-c", "core.sparseCheckout=false",
		"-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential",
		"-c", "credential.interactive=false", "-c", "http.sslVerify=true", "-c", "http.followRedirects=false",
		"-c", "protocol.allow=never", "-c", "protocol.https.allow=always", "-c", "protocol.http.allow=always",
		"-c", "gc.auto=0", "-c", "maintenance.auto=false", "-c", "submodule.recurse=false",
	}
	data, err := acquisitionCommand(ctx, c.root, "git", append(fixed, args...), true)
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", args[0], err)
	}
	return data, nil
}

const acquisitionOutputLimit = 64 << 20

var errAcquisitionOutputLimit = errors.New("acquisition command stdout limit exceeded")

type acquisitionOutput struct {
	data   []byte
	cancel context.CancelCauseFunc
}

func (b *acquisitionOutput) Write(p []byte) (int, error) {
	if len(p) > acquisitionOutputLimit-len(b.data) {
		b.cancel(errAcquisitionOutputLimit)
		return 0, errAcquisitionOutputLimit
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

// This is private plumbing for the fixed gh/git acquisition commands, not a
// workflow executor. Neither argv nor external stderr is included in errors.
func acquisitionCommand(parent context.Context, root, executable string, args []string, git bool) ([]byte, error) {
	deadline, stop := context.WithTimeout(parent, 20*time.Minute)
	defer stop()
	ctx, cancel := context.WithCancelCause(deadline)
	defer cancel(nil)
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	cmd := exec.Command(executable, args...)
	cmd.Dir = root
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	for _, v := range os.Environ() {
		key, _, _ := strings.Cut(v, "=")
		if strings.HasPrefix(key, "GIT_") || strings.HasPrefix(key, "GH_") && key != "GH_TOKEN" && key != "GH_CONFIG_DIR" || key == "GITHUB_API_URL" || key == "GITHUB_SERVER_URL" {
			continue
		}
		cmd.Env = append(cmd.Env, v)
	}
	cmd.Env = append(cmd.Env, "GH_HOST=github.com", "GH_PROMPT_DISABLED=1", "GH_PAGER=cat", "GIT_TERMINAL_PROMPT=0")
	if git {
		cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_ATTR_NOSYSTEM=1")
	}
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
	output := &acquisitionOutput{cancel: cancel}
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
