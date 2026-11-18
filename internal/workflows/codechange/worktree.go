package codechange

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/github"
	"pi-workflow-controller/internal/gitrepo"
	"pi-workflow-controller/internal/runtime"
)

const (
	worktreeDir      = "code-change"
	worktreeResource = "code-change-worktree"
	// snapshotDir is both the snapshot's run directory and its cleanup name.
	snapshotDir = "code-change-snapshot"
)

// gitSource is where the Controller's git fetches the target repository.
// Production reaches GitHub over https only; tests serve a local repository
// and additionally allow the file protocol.
type gitSource struct {
	remote    func(repository string) string
	allowFile bool
}

// worktree is the Controller-owned checkout the Agent works in. When the
// run ends it is kept while it may hold work no remote has, unless remove
// was set; published is the last head verified on the pull request.
// repository and runDir are where the cleanup reads GitHub.
type worktree struct {
	repo               *gitrepo.Repo
	base               string
	allowFile          bool
	repository, runDir string
	remove             atomic.Bool
	published          atomic.Pointer[publishedHead]
}

// publishedHead is a head verified as the head of pull request pull or one
// of its ancestors.
type publishedHead struct {
	head string
	pull int
}

// emptyTree is git's well-known empty tree, which git knows without the
// object being stored; the Controller's repositories use SHA-1 like GitHub.
const emptyTree = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"

// git ignores the user's system and global configuration and attributes, so
// no hooks, filters or URL rewrites run while the Controller builds the
// worktree, and adds review's acquisition transport and checkout limits
// except that plain http is not allowed. It also reads attributes from the
// empty tree instead of the checked-out tree, so a .gitattributes in the
// repository cannot convert line endings or encodings or expand idents in
// the bytes a checkout writes. Nothing is written to any config: the
// Agent's git later uses the user's global configuration unchanged.
func (w *worktree) git(ctx context.Context, args ...string) ([]byte, error) {
	return w.gitAt(ctx, w.repo.Dir(), args...)
}

// gitAt is git with the same policy on another git directory of the repository.
func (w *worktree) gitAt(ctx context.Context, gitDir string, args ...string) ([]byte, error) {
	return w.gitEnv(ctx, gitDir, nil, args...)
}

// gitEnv is gitAt with env added to the configuration isolation.
func (w *worktree) gitEnv(ctx context.Context, gitDir string, env []string, args ...string) ([]byte, error) {
	fixed := []string{
		"--no-pager", "--git-dir=" + gitDir, "--attr-source=" + emptyTree,
		"-c", "core.hooksPath=/dev/null", "-c", "core.attributesFile=/dev/null",
		"-c", "core.autocrlf=false", "-c", "core.sparseCheckout=false",
		"-c", "http.sslVerify=true", "-c", "http.followRedirects=false",
		"-c", "protocol.allow=never", "-c", "protocol.https.allow=always",
		"-c", "submodule.recurse=false",
	}
	if w.allowFile {
		fixed = append(fixed, "-c", "protocol.file.allow=always")
	}
	return runtime.Git(ctx, w.repo.Root(), append(runtime.GitConfigIsolation(), env...), append(fixed, args...)...)
}

// errGitNoLazyFetch is a git without the option the receipt reads rely on.
var errGitNoLazyFetch = errors.New("code-change needs a git that supports --no-lazy-fetch (2.45 or newer)")

// checkGit runs that option under the Controller's git isolation, so an
// unsupported git fails before a worktree exists and the Agent has work to
// lose.
func checkGit(ctx context.Context, dir string) error {
	_, err := runtime.Git(ctx, dir, runtime.GitConfigIsolation(), "--no-lazy-fetch", "version")
	if err != nil && context.Cause(ctx) == nil {
		return fmt.Errorf("%w: %w", errGitNoLazyFetch, err)
	}
	return err
}

// read runs plumbing on the worktree's registration after the Agent had the
// repository. It never uses the checkout's .git file, or the registration's
// commondir file, which the Agent may point elsewhere: the common directory
// is the worktree repository's. It disables lazy fetches, so no remote,
// helper or command from a configuration the Agent may have changed runs,
// and replace refs, which would substitute the history it reads.
func (w *worktree) read(ctx context.Context, args ...string) (string, error) {
	out, err := w.gitEnv(ctx, w.repo.WorktreeGitDir(), []string{"GIT_COMMON_DIR=" + w.repo.Dir()}, append([]string{"--no-lazy-fetch", "--no-replace-objects"}, args...)...)
	return strings.TrimSpace(string(out)), err
}

// head is the worktree's current HEAD commit.
func (w *worktree) head(ctx context.Context) (string, error) {
	head, err := w.read(ctx, "rev-parse", "--verify", "--end-of-options", "HEAD^{commit}")
	if err != nil {
		return "", fmt.Errorf("worktree HEAD: %w", err)
	}
	return head, nil
}

// isNew reports whether head is neither last nor one of its ancestors.
func (w *worktree) isNew(ctx context.Context, last, head string) (bool, error) {
	out, err := w.read(ctx, "rev-list", "--count", "--end-of-options", last+".."+head)
	if err != nil {
		return false, fmt.Errorf("worktree history: %w", err)
	}
	n, err := strconv.Atoi(out)
	if err != nil {
		return false, fmt.Errorf("worktree history: rev-list count %q", out)
	}
	return n > 0, nil
}

// branch is the branch the worktree HEAD is on, or "" when it is detached.
func (w *worktree) branch(ctx context.Context) (string, error) {
	// Without --end-of-options, which this mode prints back; HEAD is fixed.
	out, err := w.read(ctx, "rev-parse", "--symbolic-full-name", "HEAD")
	if err != nil {
		return "", fmt.Errorf("worktree branch: %w", err)
	}
	if out == "HEAD" {
		return "", nil
	}
	branch, ok := strings.CutPrefix(out, "refs/heads/")
	if !ok || branch == "" {
		return "", fmt.Errorf("worktree branch: HEAD is %q, not a branch", out)
	}
	return branch, nil
}

// cleanup runs only after the workflow joined and Pi exit was confirmed. It
// removes the worktree and its repository when remove was set or they hold
// no work, and keeps both otherwise, including when that cannot be read.
func (w *worktree) cleanup(ctx context.Context) error {
	if !w.remove.Load() {
		held, err := w.holdsWork(ctx)
		if err != nil {
			return fmt.Errorf("worktree kept: %w", err)
		}
		if held {
			return nil
		}
	}
	return w.repo.Cleanup(ctx)
}

// holdsWork reports whether the worktree may hold work no remote has. The
// floor is the published head, or before any the base. Work is a commit
// that the floor lacks on HEAD or on any ref but the remote-tracking ones
// (other branches, tags, including fetched ones, the stash), another
// worktree of the repository, a submodule's repository, an index entry
// marked assume-unchanged or skip-worktree, a change to a tracked file, or
// an untracked file that is not ignored; the ignore rules are the Agent's to
// change. A published floor counts only while GitHub still shows it as the
// pull request's head or one of its ancestors.
//
// The status read also runs on the Agent's repository configuration, so an
// attributes file in the repository, from which it could apply filters,
// counts as work instead of being read; runtime.Git already turns the
// filesystem monitor off. It compares raw bytes, so content an Agent's
// filter rewrote counts as a change. Commits only in a reflog are not work.
func (w *worktree) holdsWork(ctx context.Context) (bool, error) {
	floor := w.base
	published := w.published.Load()
	if published != nil {
		floor = published.head
	}
	ahead, err := w.read(ctx, "rev-list", "--count", "--exclude=refs/remotes/*", "--all", "--not", "--end-of-options", floor)
	if err != nil {
		return false, fmt.Errorf("worktree history: %w", err)
	}
	if ahead != "0" {
		return true, nil
	}
	worktrees, err := os.ReadDir(filepath.Join(w.repo.Dir(), "worktrees"))
	if err != nil {
		return false, fmt.Errorf("worktree registrations: %w", err)
	}
	if len(worktrees) != 1 {
		return true, nil
	}
	// A submodule's commits are invisible to the reads below, which do not
	// enter submodules.
	for _, path := range []string{filepath.Join("info", "attributes"), "modules"} {
		for _, dir := range []string{w.repo.Dir(), w.repo.WorktreeGitDir()} {
			_, err := os.Lstat(filepath.Join(dir, path))
			if err == nil {
				return true, nil
			}
			if !errors.Is(err, os.ErrNotExist) {
				return false, fmt.Errorf("worktree %s: %w", path, err)
			}
		}
	}
	index, err := w.read(ctx, "--work-tree="+w.repo.Worktree(), "ls-files", "-v")
	if err != nil {
		return false, fmt.Errorf("worktree index: %w", err)
	}
	for _, line := range strings.Split(index, "\n") {
		if line != "" && (line[0] == 'S' || ('a' <= line[0] && line[0] <= 'z')) {
			return true, nil
		}
	}
	status, err := w.read(ctx, "-c", "core.untrackedCache=false", "-c", "core.ignoreStat=false", "--no-optional-locks", "--work-tree="+w.repo.Worktree(),
		"status", "--porcelain", "--no-renames", "--untracked-files=normal", "--ignore-submodules=all")
	if err != nil {
		return false, fmt.Errorf("worktree status: %w", err)
	}
	if status != "" || published == nil {
		return status != "", nil
	}
	pr, err := getPullRequest(ctx, w.runDir, w.repository, published.pull)
	if err != nil {
		return false, fmt.Errorf("worktree published head: %w", err)
	}
	on, _, err := github.Ancestor(ctx, w.runDir, w.repository, published.head, pr.Head.SHA)
	if err != nil {
		return false, fmt.Errorf("worktree published head: %w", err)
	}
	return !on, nil
}

// createWorktree fetches the default branch tip and checks it out detached, so
// the Agent creates its own branch and upstream. A failure before the
// worktree is registered removes the partial worktree, which no Agent has
// seen yet.
func createWorktree(ctx context.Context, r *engine.Run, source gitSource, repository, branch string) (_ *worktree, err error) {
	root := filepath.Join(r.Dir(), worktreeDir)
	if err := os.Mkdir(root, 0700); err != nil {
		return nil, err
	}
	repo, err := gitrepo.Reserve(root)
	if err != nil {
		return nil, err
	}
	w := &worktree{repo: repo, allowFile: source.allowFile, repository: repository, runDir: r.Dir()}
	defer removeUnregistered(repo, "worktree cleanup", &err)
	if err = repo.Init(ctx, w.git); err != nil {
		return nil, fmt.Errorf("git init: %w", err)
	}
	if err = repo.AddRemote(ctx, w.git, "origin", source.remote(repository)); err != nil {
		return nil, fmt.Errorf("git remote: %w", err)
	}
	// A normal clone's tracking refspec and origin/HEAD, so the Agent's later
	// fetches update origin/<branch> and tools can find the default branch.
	if _, err = w.git(ctx, "config", "remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"); err != nil {
		return nil, fmt.Errorf("git remote: %w", err)
	}
	tracking := "refs/remotes/origin/" + branch
	if w.base, err = repo.FetchBranch(ctx, w.git, "origin", branch, tracking); err != nil {
		return nil, fmt.Errorf("git fetch %q: %w", branch, err)
	}
	if _, err = w.git(ctx, "symbolic-ref", "refs/remotes/origin/HEAD", tracking); err != nil {
		return nil, fmt.Errorf("git remote: %w", err)
	}
	if _, err = w.git(ctx, "worktree", "add", "--detach", repo.Worktree(), w.base); err != nil {
		return nil, fmt.Errorf("git worktree: %w", err)
	}
	if err = r.AddCleanup(ctx, worktreeResource, w.cleanup); err != nil {
		return nil, err
	}
	return w, nil
}

// removeUnregistered removes repo's checkout when *err reports a failure
// before its cleanup was registered, while no Agent has seen it.
func removeUnregistered(repo *gitrepo.Repo, what string, err *error) {
	if *err == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if cleanupErr := repo.Cleanup(ctx); cleanupErr != nil {
		*err = errors.Join(*err, fmt.Errorf("%s: %w", what, cleanupErr))
	}
}

// createSnapshot checks out head detached for the pre-PR reviewers in a
// fresh Controller-owned bare repository that borrows the worktree
// repository's objects through alternates. A checkout in the worktree
// repository would read the configuration the Agent may have changed and run
// its filters; the fresh repository has only the Controller's configuration
// and no remote, so a missing object fails instead of being fetched. The
// snapshot holds no work, so its registered cleanup always removes it; a
// failure before registration removes it at once.
func (w *worktree) createSnapshot(ctx context.Context, r *engine.Run, head string) (_ *gitrepo.Repo, err error) {
	root := filepath.Join(r.Dir(), snapshotDir)
	if err := os.Mkdir(root, 0700); err != nil {
		return nil, err
	}
	repo, err := gitrepo.Reserve(root)
	if err != nil {
		return nil, err
	}
	defer removeUnregistered(repo, "snapshot cleanup", &err)
	// Another Controller-owned checkout, under the same git policy.
	s := &worktree{repo: repo, allowFile: w.allowFile}
	if err = repo.Init(ctx, s.git); err != nil {
		return nil, fmt.Errorf("snapshot git init: %w", err)
	}
	alternates := filepath.Join(repo.Dir(), "objects", "info", "alternates")
	if err = os.WriteFile(alternates, []byte(filepath.Join(w.repo.Dir(), "objects")+"\n"), 0600); err != nil {
		return nil, fmt.Errorf("snapshot alternates: %w", err)
	}
	if _, err = s.git(ctx, "worktree", "add", "--detach", repo.Worktree(), head); err != nil {
		return nil, fmt.Errorf("snapshot git worktree: %w", err)
	}
	if err = r.AddCleanup(ctx, snapshotDir, repo.Cleanup); err != nil {
		return nil, err
	}
	return repo, nil
}
