package gitrepo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func privateDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func requireFile(t *testing.T, path, want string) {
	t.Helper()
	if raw, err := os.ReadFile(path); err != nil || string(raw) != want {
		t.Fatalf("%s: %q %v, want %q", path, raw, err, want)
	}
}

func requireMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s retained: %v", path, err)
	}
}

func TestReserve(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		is         error
		setup      func(t *testing.T) string
	}{
		{"relative", "acquisition root must be absolute", nil, func(*testing.T) string { return "relative/root" }},
		{"missing", "", os.ErrNotExist, func(t *testing.T) string { return filepath.Join(privateDir(t), "missing") }},
		{"file", "acquisition root must be a private directory, not a symlink", nil, func(t *testing.T) string {
			path := filepath.Join(privateDir(t), "file")
			writeFile(t, path, "")
			return path
		}},
		{"symlink", "acquisition root must be a private directory, not a symlink", nil, func(t *testing.T) string {
			path := filepath.Join(privateDir(t), "link")
			if err := os.Symlink(privateDir(t), path); err != nil {
				t.Fatal(err)
			}
			return path
		}},
		{"group-access", "acquisition root must be a private directory, not a symlink", nil, func(t *testing.T) string {
			dir := privateDir(t)
			if err := os.Chmod(dir, 0750); err != nil {
				t.Fatal(err)
			}
			return dir
		}},
		{"world-access", "acquisition root must be a private directory, not a symlink", nil, func(t *testing.T) string {
			dir := privateDir(t)
			if err := os.Chmod(dir, 0705); err != nil {
				t.Fatal(err)
			}
			return dir
		}},
		{"existing-repository", "", os.ErrExist, func(t *testing.T) string {
			dir := privateDir(t)
			if err := os.Mkdir(filepath.Join(dir, "repository.git"), 0700); err != nil {
				t.Fatal(err)
			}
			writeFile(t, filepath.Join(dir, "repository.git", "sentinel"), "keep")
			return dir
		}},
		{"existing-checkout", "", os.ErrExist, func(t *testing.T) string {
			dir := privateDir(t)
			if err := os.Mkdir(filepath.Join(dir, "checkout"), 0700); err != nil {
				t.Fatal(err)
			}
			writeFile(t, filepath.Join(dir, "checkout", "sentinel"), "keep")
			return dir
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tc.setup(t)
			r, err := Reserve(root)
			if r != nil || err == nil {
				t.Fatalf("accepted root: %+v %v", r, err)
			}
			if tc.want != "" && err.Error() != tc.want {
				t.Fatalf("got %q, want %q", err, tc.want)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Fatalf("got %v, want %v", err, tc.is)
			}
			switch tc.name {
			case "existing-repository":
				requireFile(t, filepath.Join(root, "repository.git", "sentinel"), "keep")
				requireMissing(t, filepath.Join(root, "checkout"))
			case "existing-checkout":
				requireFile(t, filepath.Join(root, "checkout", "sentinel"), "keep")
				// The repository reserved before the failure is retained, never removed.
				if info, err := os.Lstat(filepath.Join(root, "repository.git")); err != nil || !info.IsDir() {
					t.Fatalf("reserved repository: %v", err)
				}
			case "relative", "missing":
			default:
				if entries, err := os.ReadDir(root); err == nil && len(entries) != 0 {
					t.Fatalf("rejected root was initialized: %v", entries)
				}
			}
		})
	}
	t.Run("canonical-ownership", func(t *testing.T) {
		real := privateDir(t)
		link := filepath.Join(privateDir(t), "alias")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(real, "root"), 0700); err != nil {
			t.Fatal(err)
		}
		r, err := Reserve(filepath.Join(link, "root"))
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := filepath.EvalSymlinks(filepath.Join(real, "root"))
		if err != nil {
			t.Fatal(err)
		}
		if r.Root() != canonical || r.Dir() != filepath.Join(canonical, "repository.git") || r.Worktree() != filepath.Join(canonical, "checkout") || r.WorktreeGitDir() != filepath.Join(canonical, "repository.git", "worktrees", "checkout") {
			t.Fatalf("paths not canonical: %q %q %q %q", r.Root(), r.Dir(), r.Worktree(), r.WorktreeGitDir())
		}
		for path, info := range map[string]os.FileInfo{r.Root(): r.rootInfo, r.Dir(): r.dirInfo, r.Worktree(): r.worktreeInfo} {
			actual, err := os.Lstat(path)
			if err != nil || info == nil || !os.SameFile(actual, info) {
				t.Fatalf("reserved path ownership not recorded: %s: %v", path, err)
			}
			if path != r.Root() && (!actual.IsDir() || actual.Mode().Perm() != 0700) {
				t.Fatalf("reserved path mode: %s: %v", path, actual.Mode())
			}
		}
		if entries, err := os.ReadDir(r.Root()); err != nil || len(entries) != 2 {
			t.Fatalf("reserve created extra paths: %v %v", entries, err)
		}
	})
}

// recordedGit fails at call failAt and otherwise returns outputs by call index.
type recordedGit struct {
	calls   int
	failAt  int
	err     error
	outputs []string
}

func (g *recordedGit) run(_ context.Context, _ ...string) ([]byte, error) {
	i := g.calls
	g.calls++
	if g.err != nil && i == g.failAt {
		return []byte("partial output"), g.err
	}
	if i < len(g.outputs) {
		return []byte(g.outputs[i]), nil
	}
	return nil, nil
}

// The real-git fixture proves the successful sequences; review's external Git
// fixture pins their exact argv. This covers failures and input rejection.
func TestRepoGitSequences(t *testing.T) {
	sha := strings.Repeat("a", 40)
	cause := fmt.Errorf("git step: %w", errors.New("external git failure"))
	addRemote := func(ctx context.Context, r *Repo, git Runner) error {
		return r.AddRemote(ctx, git, "base", "https://example.invalid/o/r.git")
	}
	fetch := func(remote, sha, ref string) func(context.Context, *Repo, Runner) error {
		return func(ctx context.Context, r *Repo, git Runner) error { return r.FetchPinned(ctx, git, remote, sha, ref) }
	}
	for _, tc := range []struct {
		name  string
		run   func(context.Context, *Repo, Runner) error
		git   recordedGit
		calls int
		want  string
	}{
		{"add-remote-error", addRemote, recordedGit{failAt: 1, err: cause}, 2, ""},
		{"fetch-error", fetch("base", sha, "refs/pin/base"), recordedGit{failAt: 0, err: cause}, 1, ""},
		{"verify-error", fetch("base", sha, "refs/pin/base"), recordedGit{failAt: 1, err: cause}, 2, ""},
		{"pin-mismatch", fetch("base", sha, "refs/pin/base"), recordedGit{outputs: []string{"", strings.Repeat("b", 40) + "\n"}}, 2, "fetched revision is not the pinned commit"},
		{"short-sha", fetch("base", sha[:39], "refs/pin/base"), recordedGit{}, 0, "pinned fetch requires a full lowercase commit SHA"},
		{"uppercase-sha", fetch("base", strings.Repeat("A", 40), "refs/pin/base"), recordedGit{}, 0, "pinned fetch requires a full lowercase commit SHA"},
		{"sha-revision-expression", fetch("base", sha[:38]+"^0", "refs/pin/base"), recordedGit{}, 0, "pinned fetch requires a full lowercase commit SHA"},
		{"ref-outside-refs", fetch("base", sha, "pin/base"), recordedGit{}, 0, "pinned fetch ref must start with refs/"},
		{"empty-ref", fetch("base", sha, ""), recordedGit{}, 0, "pinned fetch ref must start with refs/"},
		{"empty-remote", fetch("", sha, "refs/pin/base"), recordedGit{}, 0, "pinned fetch remote must be a name"},
		{"option-remote", fetch("--upload-pack=/bin/false", sha, "refs/pin/base"), recordedGit{}, 0, "pinned fetch remote must be a name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := Reserve(privateDir(t))
			if err != nil {
				t.Fatal(err)
			}
			git := tc.git
			err = tc.run(context.Background(), r, git.run)
			if tc.git.err != nil {
				// The Runner error is returned as-is, never re-wrapped.
				if err != tc.git.err {
					t.Fatalf("got %v, want the Runner error itself", err)
				}
			} else if err == nil || err.Error() != tc.want {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
			if git.calls != tc.calls {
				t.Fatalf("git calls: %d, want %d", git.calls, tc.calls)
			}
		})
	}
}

// runGit is the external boundary: real git with global configuration and
// hooks disabled.
func runGit(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Dir = dir
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "GIT_") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.com", "GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %v: %w: %s", args, err, out)
	}
	return out, nil
}

type checkoutFixture struct {
	repo *Repo
	git  Runner
	sha  string
}

func newCheckoutFixture(t *testing.T) checkoutFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	source := privateDir(t)
	sourceGit := func(args ...string) string {
		t.Helper()
		out, err := runGit(ctx, source, args...)
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	sourceGit("init", "--template=")
	writeFile(t, filepath.Join(source, "tracked.txt"), "tracked\n")
	sourceGit("add", "tracked.txt")
	sourceGit("commit", "-m", "fixture")
	var err error
	f := checkoutFixture{sha: sourceGit("rev-parse", "HEAD")}
	if f.repo, err = Reserve(privateDir(t)); err != nil {
		t.Fatal(err)
	}
	f.git = func(ctx context.Context, args ...string) ([]byte, error) {
		return runGit(ctx, f.repo.Root(), append([]string{"--git-dir=" + f.repo.Dir()}, args...)...)
	}
	if err := f.repo.Init(ctx, f.git); err != nil {
		t.Fatal(err)
	}
	if err := f.repo.AddRemote(ctx, f.git, "origin", "file://"+source); err != nil {
		t.Fatal(err)
	}
	if err := f.repo.FetchPinned(ctx, f.git, "origin", f.sha, "refs/pin/head"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.git(ctx, "worktree", "add", "--detach", f.repo.Worktree(), f.sha); err != nil {
		t.Fatal(err)
	}
	requireFile(t, filepath.Join(f.repo.Worktree(), "tracked.txt"), "tracked\n")
	if info, err := os.Stat(f.repo.WorktreeGitDir()); err != nil || !info.IsDir() {
		t.Fatalf("worktree registration: %v", err)
	}
	return f
}

func (f checkoutFixture) requireHistory(t *testing.T) {
	t.Helper()
	raw, err := f.git(context.Background(), "rev-parse", "refs/pin/head")
	if err != nil || strings.TrimSpace(string(raw)) != f.sha {
		t.Fatalf("repository history lost: %q %v", raw, err)
	}
}

// errAfterFirst reports cancellation only after Cleanup's first check, so
// the stop lands between removing the checkout and its registration.
type errAfterFirst struct {
	context.Context
	calls int
}

func (c *errAfterFirst) Err() error {
	c.calls++
	if c.calls > 1 {
		return context.Canceled
	}
	return nil
}

func TestRepoCleanup(t *testing.T) {
	t.Run("owned-only-and-idempotent", func(t *testing.T) {
		f := newCheckoutFixture(t)
		writeFile(t, filepath.Join(f.repo.Worktree(), "untracked"), "owned dirty file")
		writeFile(t, filepath.Join(f.repo.Root(), "history"), "keep")
		if err := f.repo.Cleanup(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireMissing(t, f.repo.Worktree())
		requireMissing(t, f.repo.WorktreeGitDir())
		requireFile(t, filepath.Join(f.repo.Root(), "history"), "keep")
		f.requireHistory(t)
		writeFile(t, filepath.Join(f.repo.Root(), "checkout"), "later foreign file")
		if err := f.repo.Cleanup(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireFile(t, filepath.Join(f.repo.Root(), "checkout"), "later foreign file")
	})
	t.Run("missing-checkout", func(t *testing.T) {
		f := newCheckoutFixture(t)
		if err := os.RemoveAll(f.repo.Worktree()); err != nil {
			t.Fatal(err)
		}
		if err := f.repo.Cleanup(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireMissing(t, f.repo.WorktreeGitDir())
		f.requireHistory(t)
	})
	for _, target := range []string{"root", "repository", "checkout", "checkout-symlink"} {
		t.Run("replaced-"+target, func(t *testing.T) {
			f := newCheckoutFixture(t)
			path := map[string]string{"root": f.repo.Root(), "repository": f.repo.Dir(), "checkout": f.repo.Worktree(), "checkout-symlink": f.repo.Worktree()}[target]
			moved := path + ".moved"
			if err := os.Rename(path, moved); err != nil {
				t.Fatal(err)
			}
			outside := privateDir(t)
			writeFile(t, filepath.Join(outside, "foreign"), "keep")
			if target == "checkout-symlink" {
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				writeFile(t, filepath.Join(path, "foreign"), "keep")
			}
			if err := f.repo.Cleanup(context.Background()); err == nil || err.Error() != "acquisition cleanup ownership mismatch" {
				t.Fatalf("cleanup accepted replacement %s: %v", target, err)
			}
			requireFile(t, filepath.Join(path, "foreign"), "keep")
			requireFile(t, filepath.Join(outside, "foreign"), "keep")
			if err := os.RemoveAll(path); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(moved, path); err != nil {
				t.Fatal(err)
			}
			requireFile(t, filepath.Join(f.repo.Worktree(), "tracked.txt"), "tracked\n")
			if info, err := os.Stat(f.repo.WorktreeGitDir()); err != nil || !info.IsDir() {
				t.Fatalf("rejected cleanup removed registration: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := f.repo.Cleanup(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled cleanup: %v", err)
			}
			requireFile(t, filepath.Join(f.repo.Worktree(), "tracked.txt"), "tracked\n")
			if err := f.repo.Cleanup(context.Background()); err != nil {
				t.Fatal(err)
			}
			requireMissing(t, f.repo.Worktree())
			requireMissing(t, f.repo.WorktreeGitDir())
			f.requireHistory(t)
		})
	}
	t.Run("cancelled-between-removals", func(t *testing.T) {
		f := newCheckoutFixture(t)
		ctx := &errAfterFirst{Context: context.Background()}
		if err := f.repo.Cleanup(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation between removals: %v", err)
		}
		requireMissing(t, f.repo.Worktree())
		if info, err := os.Stat(f.repo.WorktreeGitDir()); err != nil || !info.IsDir() {
			t.Fatalf("registration removed after cancellation: %v", err)
		}
		if err := f.repo.Cleanup(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireMissing(t, f.repo.WorktreeGitDir())
		f.requireHistory(t)
	})
}
