package codechange

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/gitrepo"
	"pi-workflow-controller/internal/runtime"
)

const (
	worktreeDir      = "code-change"
	worktreeResource = "code-change-worktree"
)

// gitSource is where the Controller's git fetches the target repository.
// Production reaches GitHub over https only; tests serve a local repository
// and additionally allow the file protocol.
type gitSource struct {
	remote    func(repository string) string
	allowFile bool
}

// worktree is the Controller-owned checkout the Agent works in. It is kept
// when the run ends unless remove was set, because until its commits are
// pushed it holds the only copy of the Agent's work.
type worktree struct {
	repo      *gitrepo.Repo
	base      string
	allowFile bool
	remove    atomic.Bool
}

// git ignores the user's system and global configuration and attributes, so
// no hooks, filters or URL rewrites run while the Controller builds the
// worktree, and adds review's acquisition transport and checkout limits
// except that plain http is not allowed. Nothing is written to any config:
// the Agent's git later uses the user's global configuration unchanged.
func (w *worktree) git(ctx context.Context, args ...string) ([]byte, error) {
	fixed := []string{
		"--no-pager", "--git-dir=" + w.repo.Dir(),
		"-c", "core.hooksPath=/dev/null", "-c", "core.attributesFile=/dev/null",
		"-c", "core.autocrlf=false", "-c", "core.sparseCheckout=false",
		"-c", "http.sslVerify=true", "-c", "http.followRedirects=false",
		"-c", "protocol.allow=never", "-c", "protocol.https.allow=always",
		"-c", "submodule.recurse=false",
	}
	if w.allowFile {
		fixed = append(fixed, "-c", "protocol.file.allow=always")
	}
	return runtime.Git(ctx, w.repo.Root(), runtime.GitConfigIsolation(), append(fixed, args...)...)
}

// cleanup runs only after the workflow joined and Pi exit was confirmed.
func (w *worktree) cleanup(ctx context.Context) error {
	if !w.remove.Load() {
		return nil
	}
	return w.repo.Cleanup(ctx)
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
	w := &worktree{repo: repo, allowFile: source.allowFile}
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if cleanupErr := repo.Cleanup(cleanupCtx); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("worktree cleanup: %w", cleanupErr))
			}
		}
	}()
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
