// Package gitrepo owns a fresh private bare repository and one reserved
// worktree path. Git runs only through the caller's Runner, so process
// execution and read-only or writable policy stay with the caller.
package gitrepo

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var fullSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// Runner executes one git invocation against the repository with the caller's policy.
type Runner func(ctx context.Context, args ...string) ([]byte, error)

// Repo is a reserved bare repository and worktree path under a private root.
type Repo struct {
	mu                              sync.Mutex
	root, dir, worktree             string
	rootInfo, dirInfo, worktreeInfo os.FileInfo
	cleaned                         bool
}

// Reserve validates root and exclusively creates root/repository.git and
// root/checkout, so an existing repository can never become an implicit target.
// A failure after repository.git was created returns no Repo and leaves that
// directory in place; nothing it did not create is removed.
func Reserve(root string) (*Repo, error) {
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
	r := &Repo{root: root, rootInfo: info, dir: filepath.Join(root, "repository.git"), worktree: filepath.Join(root, "checkout")}
	if err = os.Mkdir(r.dir, 0700); err != nil {
		return nil, err
	}
	if r.dirInfo, err = os.Lstat(r.dir); err != nil {
		return nil, err
	}
	if err = os.Mkdir(r.worktree, 0700); err != nil {
		return nil, err
	}
	if r.worktreeInfo, err = os.Lstat(r.worktree); err != nil {
		return nil, err
	}
	return r, nil
}

// Root is the canonical private root.
func (r *Repo) Root() string { return r.root }

// Dir is the bare repository.
func (r *Repo) Dir() string { return r.dir }

// Worktree is the reserved worktree path. Create the worktree only at this path,
// so Git registers it under the name checkout; Cleanup removes exactly this
// path and that registration.
func (r *Repo) Worktree() string { return r.worktree }

// WorktreeGitDir is Git's registration for the reserved worktree.
func (r *Repo) WorktreeGitDir() string { return filepath.Join(r.dir, "worktrees", "checkout") }

// Init creates the bare repository without templates, so no hooks are installed.
func (r *Repo) Init(ctx context.Context, git Runner) error {
	_, err := git(ctx, "init", "--bare", "--template=", r.dir)
	return err
}

// AddRemote records a promisor remote for lazy blob reads after a blob-less fetch.
func (r *Repo) AddRemote(ctx context.Context, git Runner, name, url string) error {
	for _, setting := range [][2]string{{"url", url}, {"promisor", "true"}, {"partialclonefilter", "blob:none"}} {
		if _, err := git(ctx, "config", "remote."+name+"."+setting[0], setting[1]); err != nil {
			return err
		}
	}
	return nil
}

// FetchPinned fetches only the pinned commit into ref, never a mutable branch,
// and requires the fetched object to be exactly that commit. Inputs git could
// parse as options or as a different refspec are rejected before git runs.
func (r *Repo) FetchPinned(ctx context.Context, git Runner, remote, sha, ref string) error {
	if !fullSHA.MatchString(sha) {
		return errors.New("pinned fetch requires a full lowercase commit SHA")
	}
	if !strings.HasPrefix(ref, "refs/") {
		return errors.New("pinned fetch ref must start with refs/")
	}
	if remote == "" || strings.HasPrefix(remote, "-") {
		return errors.New("pinned fetch remote must be a name")
	}
	if _, err := git(ctx, "fetch", "--filter=blob:none", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head", remote, sha+":"+ref); err != nil {
		return err
	}
	actual, err := git(ctx, "rev-parse", "--verify", sha+"^{commit}")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(actual)) != sha {
		return errors.New("fetched revision is not the pinned commit")
	}
	return nil
}

// Cleanup removes only the originally reserved worktree and its registration.
// The repository objects and other files under root remain as history.
// Init, AddRemote and FetchPinned are not synchronized with Cleanup; call it
// only after every Runner call has joined.
func (r *Repo) Cleanup(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cleaned {
		return nil
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	root, err := os.OpenRoot(r.root)
	if err != nil {
		return err
	}
	defer func(root *os.Root) { _ = root.Close() }(root)
	for _, check := range []struct {
		path string
		info os.FileInfo
	}{{".", r.rootInfo}, {"repository.git", r.dirInfo}, {"checkout", r.worktreeInfo}} {
		info, e := root.Lstat(check.path)
		if check.path == "checkout" && errors.Is(e, os.ErrNotExist) {
			continue
		}
		if e != nil {
			return e
		}
		if !info.IsDir() || !os.SameFile(info, check.info) {
			return errors.New("acquisition cleanup ownership mismatch")
		}
	}
	// The fresh bare repo has exactly this reserved worktree name. Rooted
	// removal neither follows a modified .git link nor prunes other worktrees.
	if err = root.RemoveAll("checkout"); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err = root.RemoveAll("repository.git/worktrees/checkout"); err != nil {
		return err
	}
	r.cleaned = true
	return nil
}
