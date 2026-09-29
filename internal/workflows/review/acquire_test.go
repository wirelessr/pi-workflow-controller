package review

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestParsePRURL(t *testing.T) {
	for _, tc := range []struct {
		url, repo string
		number    int
	}{
		{"https://github.com/owner/repo/pull/1", "owner/repo", 1},
		{"https://github.com/Owner-1/re.po_2-/pull/2147483647/", "Owner-1/re.po_2-", 2147483647},
		{"http://github.com/o/r/pull/1", "", 0},
		{"https://github.com.evil/o/r/pull/1", "", 0},
		{"https://github.com:443/o/r/pull/1", "", 0},
		{"https://user@github.com/o/r/pull/1", "", 0},
		{"https://github.com/o/r/pull/0", "", 0},
		{"https://github.com/o/r/pull/-1", "", 0},
		{"https://github.com/o/r/pull/+1", "", 0},
		{"https://github.com/o/r/pull/01", "", 0},
		{"https://github.com/o/r/pull/999999999999999999999999", "", 0},
		{"https://github.com/o/r/pull/1/files", "", 0},
		{"https://github.com/o/r/pull/1//", "", 0},
		{"https://github.com/o/r/pull/1?x=1", "", 0},
		{"https://github.com/o/r/pull/1?", "", 0},
		{"https://github.com/o/r/pull/1#x", "", 0},
		{"https://github.com/o/r/pull/1#", "", 0},
		{"https://github.com/o/r/pull/%31", "", 0},
		{"https://github.com/o/../pull/1", "", 0},
		{"https://github.com/o/r%2Fx/pull/1", "", 0},
		{"https://github.com/o/r/pull/1\n", "", 0},
		{" https://github.com/o/r/pull/1", "", 0},
		{"https://github.com/o/r/pull/1 ", "", 0},
		{"https://github.com/o/r/issues/1", "", 0},
		{"https://github.com/o/r/pull/1/../2", "", 0},
	} {
		t.Run(tc.url, func(t *testing.T) {
			repo, n, err := ParsePRURL(tc.url)
			if repo != tc.repo || n != tc.number || (err == nil) != (tc.repo != "") {
				t.Fatalf("ParsePRURL = %q, %d, %v", repo, n, err)
			}
		})
	}
}

type acquisitionGit struct {
	sourceRepo, baseRepo, headRepo, base, head, mergeBase string
}

type acquisitionFixture struct {
	acquisitionGit
	dir, root string
	source    acquisitionSource
	metadata  map[string]any
	requests  chan string

	gitMu      sync.Mutex
	gitHistory []string
	gitMode    string
}

func fixtureWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}
func fixtureGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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
		t.Fatalf("fixture git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func newAcquisitionGit(t *testing.T, dir, sourceRepo string) acquisitionGit {
	t.Helper()
	f := acquisitionGit{sourceRepo: sourceRepo}
	fixtureGit(t, f.sourceRepo, "init", "--template=", "-b", "actual-base")
	fixtureWrite(t, filepath.Join(f.sourceRepo, "shared.txt"), []byte("common\n"))
	fixtureGit(t, f.sourceRepo, "add", ".")
	fixtureGit(t, f.sourceRepo, "commit", "-m", "common")
	f.mergeBase = fixtureGit(t, f.sourceRepo, "rev-parse", "HEAD")
	fixtureWrite(t, filepath.Join(f.sourceRepo, "base-only.txt"), []byte("base\n"))
	fixtureGit(t, f.sourceRepo, "add", ".")
	fixtureGit(t, f.sourceRepo, "commit", "-m", "base")
	f.base = fixtureGit(t, f.sourceRepo, "rev-parse", "HEAD")
	f.baseRepo = filepath.Join(dir, "base.git")
	fixtureGit(t, dir, "clone", "--bare", "--template=", f.sourceRepo, f.baseRepo)
	fixtureGit(t, f.sourceRepo, "checkout", "-b", "topic", f.mergeBase)
	fixtureWrite(t, filepath.Join(f.sourceRepo, "shared.txt"), []byte("pinned head\n"))
	fixtureWrite(t, filepath.Join(f.sourceRepo, "odd\nname.txt"), []byte("newline path\n"))
	fixtureWrite(t, filepath.Join(f.sourceRepo, "binary.dat"), []byte{0, 1, 2, 3})
	fixtureGit(t, f.sourceRepo, "add", ".")
	fixtureGit(t, f.sourceRepo, "commit", "-m", "head")
	f.head = fixtureGit(t, f.sourceRepo, "rev-parse", "HEAD")
	f.headRepo = filepath.Join(dir, "fork.git")
	fixtureGit(t, dir, "clone", "--bare", "--template=", f.sourceRepo, f.headRepo)
	return f
}

func newAcquisitionFixture(t *testing.T) *acquisitionFixture {
	t.Helper()
	return newAcquisitionFixtureFromSeed(t, nil)
}

func newAcquisitionFixturePaths(t *testing.T) *acquisitionFixture {
	t.Helper()
	f := &acquisitionFixture{dir: t.TempDir(), root: t.TempDir(), requests: make(chan string, 100)}
	if err := os.Chmod(f.root, 0700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(f.root)
	if err != nil {
		t.Fatal(err)
	}
	f.root = canonical
	return f
}

func newAcquisitionFixtureFromSeed(t *testing.T, seed *acquisitionGit) *acquisitionFixture {
	t.Helper()
	// Acquisition strips GIT_* identity; avoid host email resolution for its reflog.
	t.Setenv("EMAIL", "fixture@pwc.invalid")
	f := newAcquisitionFixturePaths(t)
	if seed == nil {
		f.acquisitionGit = newAcquisitionGit(t, f.dir, t.TempDir())
	} else {
		f.acquisitionGit = acquisitionGit{sourceRepo: t.TempDir(), baseRepo: filepath.Join(f.dir, "base.git"), headRepo: filepath.Join(f.dir, "fork.git"), base: seed.base, head: seed.head, mergeBase: seed.mergeBase}
		fixtureGit(t, f.dir, "clone", "--no-hardlinks", "--template=", seed.sourceRepo, f.sourceRepo)
		fixtureGit(t, f.sourceRepo, "branch", "actual-base", seed.base)
		// Git preserves tree modes, but checkout creation uses the process umask.
		for _, name := range []string{"shared.txt", "odd\nname.txt", "binary.dat"} {
			if err := os.Chmod(filepath.Join(f.sourceRepo, name), 0600); err != nil {
				t.Fatal(err)
			}
		}
		fixtureGit(t, f.dir, "clone", "--bare", "--no-hardlinks", "--template=", seed.baseRepo, f.baseRepo)
		fixtureGit(t, f.dir, "clone", "--bare", "--no-hardlinks", "--template=", seed.headRepo, f.headRepo)
	}
	f.configureAPI(t)
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	handler := &cgi.Handler{Path: gitPath, Args: []string{"http-backend"}, Env: []string{"GIT_PROJECT_ROOT=" + f.dir, "GIT_HTTP_EXPORT_ALL=1", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null"}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests <- r.URL.Path
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	f.source.remote = func(repository string) string {
		switch repository {
		case "owner/repo":
			return server.URL + "/base.git"
		case "fork/repo":
			return server.URL + "/fork.git"
		default:
			t.Errorf("unvalidated remote: %q", repository)
			return server.URL + "/invalid.git"
		}
	}
	return f
}

func (f *acquisitionFixture) configureAPI(t *testing.T) {
	t.Helper()
	f.metadata = map[string]any{
		"number": 17, "html_url": "https://github.com/owner/repo/pull/17",
		"base": map[string]any{"sha": f.base, "ref": "actual-base", "repo": map[string]string{"full_name": "owner/repo"}},
		"head": map[string]any{"sha": f.head, "ref": "topic", "repo": map[string]string{"full_name": "fork/repo"}},
	}
	f.saveMetadata(t)
	fixtureWrite(t, filepath.Join(f.dir, "issues.json"), []byte(`[[{"id":1}],[{"id":2}]]`))
	fixtureWrite(t, filepath.Join(f.dir, "inline.json"), []byte(`[[]]`))
	fixtureWrite(t, filepath.Join(f.dir, "reviews.json"), []byte(`[[{"id":3}]]`))
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PWC_ACQUIRE_FIXTURE", f.dir)
	f.source.gh = executable
	f.source.ghPrefix = []string{"-test.run=^TestAcquisitionGHHelper$", "--"}
}

func (f *acquisitionFixture) saveMetadata(t *testing.T) {
	t.Helper()
	data, err := json.Marshal(f.metadata)
	if err != nil {
		t.Fatal(err)
	}
	fixtureWrite(t, filepath.Join(f.dir, "metadata.json"), data)
}
func (f *acquisitionFixture) acquire(ctx context.Context) (*Checkout, error) {
	return acquire(ctx, f.root, "https://github.com/owner/repo/pull/17/", f.source)
}

func TestAcquisitionSeedIsolation(t *testing.T) {
	seed := newAcquisitionGit(t, t.TempDir(), t.TempDir())
	seedState := func() map[string]string {
		state := map[string]string{}
		for _, repo := range []string{seed.sourceRepo, seed.baseRepo, seed.headRepo} {
			err := filepath.Walk(repo, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.Mode().IsRegular() {
					raw, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					state[path] = fmt.Sprintf("%v:%x", info.Mode(), sha256.Sum256(raw))
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		return state
	}
	wantDiff := fixtureGit(t, seed.sourceRepo, "diff", "--binary", "--no-ext-diff", "--no-textconv", seed.mergeBase, seed.head, "--")
	before := seedState()
	for _, name := range []string{"case-A", "case-B"} {
		t.Run(name, func(t *testing.T) {
			f := newAcquisitionFixtureFromSeed(t, &seed)
			for _, repo := range []string{filepath.Join(f.sourceRepo, ".git"), f.baseRepo, f.headRepo} {
				if _, err := os.Lstat(filepath.Join(repo, "objects", "info", "alternates")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("clone uses alternates: %s: %v", repo, err)
				}
				if err := filepath.Walk(repo, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if !info.IsDir() && (!info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Nlink != 1) {
						return fmt.Errorf("clone shares file identity: %s", path)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			c, err := f.acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := c.Cleanup(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if c.BaseSHA != seed.base || c.HeadSHA != seed.head || c.MergeBase != seed.mergeBase || c.DiffRange != seed.mergeBase+".."+seed.head {
				t.Fatalf("clone changed pin: %+v", c)
			}
			for path, want := range map[string][]byte{"shared.txt": []byte("pinned head\n"), "odd\nname.txt": []byte("newline path\n"), "binary.dat": {0, 1, 2, 3}} {
				for _, root := range []string{f.sourceRepo, c.Worktree} {
					if raw, err := os.ReadFile(filepath.Join(root, path)); err != nil || !bytes.Equal(raw, want) {
						t.Fatalf("clone bytes %q: %q %v", path, raw, err)
					}
				}
				if info, err := os.Stat(filepath.Join(f.sourceRepo, path)); err != nil || info.Mode().Perm() != 0600 {
					t.Fatalf("source mode changed: %v %v", info, err)
				}
			}
			if raw, err := os.ReadFile(c.Snapshots["diff"]); err != nil || strings.TrimSpace(string(raw)) != wantDiff {
				t.Fatalf("clone changed binary/newline diff: %q %v", raw, err)
			}
			checkError(t, c.Verify(context.Background()), "")
			if name == "case-A" {
				fixtureWrite(t, filepath.Join(f.sourceRepo, "shared.txt"), []byte("case A only\n"))
				fixtureGit(t, f.sourceRepo, "commit", "-am", "case A")
				fixtureGit(t, f.sourceRepo, "push", f.headRepo, "topic")
				fixtureGit(t, f.sourceRepo, "checkout", "actual-base")
				fixtureWrite(t, filepath.Join(f.sourceRepo, "base-only.txt"), []byte("case A base\n"))
				fixtureGit(t, f.sourceRepo, "commit", "-am", "case A base")
				fixtureGit(t, f.sourceRepo, "push", f.baseRepo, "actual-base")
				if fixtureGit(t, f.headRepo, "rev-parse", "topic") == seed.head || fixtureGit(t, f.baseRepo, "rev-parse", "actual-base") == seed.base {
					t.Fatal("case A mutation did not change both clone refs")
				}
				fixtureWrite(t, filepath.Join(c.Worktree, "shared.txt"), []byte("dirty checkout\n"))
				f.metadata["number"] = 99
				f.saveMetadata(t)
			}
			if err := c.Cleanup(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(c.Worktree); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("owned checkout retained: %v", err)
			}
		})
		if !reflect.DeepEqual(before, seedState()) {
			t.Fatalf("%s changed seed files, refs or modes", name)
		}
	}
}

func TestAcquisitionFixtureIdentityScope(t *testing.T) {
	t.Setenv("EMAIL", "parent@example.invalid")
	t.Run("fixture", func(t *testing.T) {
		f := newAcquisitionFixture(t)
		if f.source.gitOutput != nil {
			t.Fatal("identity coverage requires real Git")
		}
		c, err := f.acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { checkError(t, c.Cleanup(context.Background()), "") })
		for _, repo := range []string{f.sourceRepo, c.Worktree} {
			for _, revision := range []string{f.base, f.head, f.mergeBase} {
				got := fixtureGit(t, repo, "show", "-s", "--format=%an%n%ae%n%cn%n%ce", revision)
				if got != "Fixture\nfixture@example.com\nFixture\nfixture@example.com" {
					t.Fatalf("fallback replaced explicit commit identity: %q", got)
				}
			}
		}
		if got := fixtureGit(t, c.Worktree, "reflog", "show", "-1", "--format=%ge", "HEAD"); got != "fixture@pwc.invalid" {
			t.Fatalf("acquisition reflog inherited host identity: %q", got)
		}
		checkError(t, c.Verify(context.Background()), "")
	})
	if got := os.Getenv("EMAIL"); got != "parent@example.invalid" {
		t.Fatalf("fixture email leaked to parent scope: %q", got)
	}
}

func TestAcquirePinnedForkSnapshotAndOwnedCleanup(t *testing.T) {
	f := newAcquisitionFixture(t)
	if f.source.gitOutput != nil {
		t.Fatal("dedicated acquisition must retain the default real Git adapter")
	}
	// Advance both real source branches after the metadata snapshot was taken.
	fixtureWrite(t, filepath.Join(f.sourceRepo, "shared.txt"), []byte("new mutable head\n"))
	fixtureGit(t, f.sourceRepo, "commit", "-am", "advance head")
	fixtureGit(t, f.sourceRepo, "push", f.headRepo, "topic")
	fixtureGit(t, f.sourceRepo, "checkout", "actual-base")
	fixtureWrite(t, filepath.Join(f.sourceRepo, "base-only.txt"), []byte("new mutable base\n"))
	fixtureGit(t, f.sourceRepo, "commit", "-am", "advance base")
	fixtureGit(t, f.sourceRepo, "push", f.baseRepo, "actual-base")
	before := fixtureGit(t, f.sourceRepo, "status", "--porcelain")
	c, err := f.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Cleanup(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if c.gitOutput != nil {
		t.Fatal("default acquisition selected a replacement Git command")
	}
	if c.Repository != "owner/repo" || c.Number != 17 || c.URL != "https://github.com/owner/repo/pull/17" || c.BaseSHA != f.base || c.HeadSHA != f.head || c.MergeBase != f.mergeBase || c.DiffRange != f.mergeBase+".."+f.head || len(c.ContextID) != 64 {
		t.Fatalf("bad pins: %+v", c)
	}
	if got := fixtureGit(t, c.Worktree, "rev-parse", "HEAD"); got != f.head {
		t.Fatalf("checkout = %s", got)
	}
	if got := fixtureGit(t, c.Worktree, "rev-parse", "--abbrev-ref", "HEAD"); got != "HEAD" {
		t.Fatalf("not detached: %s", got)
	}
	if got := fixtureGit(t, c.Worktree, "status", "--porcelain"); got != "" {
		t.Fatalf("dirty checkout: %s", got)
	}
	if got := fixtureGit(t, f.sourceRepo, "status", "--porcelain"); got != before {
		t.Fatal("source repository modified")
	}
	if len(c.Snapshots) != 6 || len(c.Missing) != 0 {
		t.Fatalf("snapshots/missing: %+v / %v", c.Snapshots, c.Missing)
	}
	for key, path := range c.Snapshots {
		if !filepath.IsAbs(path) || filepath.Dir(path) != filepath.Join(f.root, "snapshots") {
			t.Fatalf("bad snapshot path %s: %s", key, path)
		}
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("snapshot mode: %v %v", info, err)
		}
	}
	metadata, _ := os.ReadFile(c.Snapshots["metadata"])
	if !bytes.Contains(metadata, []byte(`"ref":"actual-base"`)) {
		t.Fatal("actual base branch was discarded")
	}
	diff, _ := os.ReadFile(c.Snapshots["diff"])
	if !bytes.Contains(diff, []byte("GIT binary patch")) || !bytes.Contains(diff, []byte("+pinned head")) || bytes.Contains(diff, []byte("base-only")) || bytes.Contains(diff, []byte("new mutable")) {
		t.Fatalf("not pinned merge-base diff: %s", diff)
	}
	var changed []string
	data, _ := os.ReadFile(c.Snapshots["changed-files"])
	if err := json.Unmarshal(data, &changed); err != nil || !reflect.DeepEqual(changed, []string{"binary.dat", "odd\nname.txt", "shared.txt"}) {
		t.Fatalf("changed files: %q %v", changed, err)
	}
	data, _ = os.ReadFile(c.Snapshots["issues"])
	if string(data) != `[{"id":1},{"id":2}]` {
		t.Fatalf("pagination lost: %s", data)
	}
	data, _ = os.ReadFile(c.Snapshots["inline"])
	if string(data) != `[]` {
		t.Fatalf("empty successful comments: %s", data)
	}
	calls, _ := os.ReadFile(filepath.Join(f.dir, "calls"))
	if string(calls) != "metadata\nissues\ninline\nreviews\n" {
		t.Fatalf("unexpected API calls: %s", calls)
	}
	paths := []string{}
	for len(f.requests) > 0 {
		paths = append(paths, <-f.requests)
	}
	if !strings.Contains(strings.Join(paths, "\n"), "/fork.git/git-upload-pack") {
		t.Fatalf("fork was not fetched: %v", paths)
	}

	outside := t.TempDir()
	fixtureWrite(t, filepath.Join(outside, "sentinel"), []byte("keep"))
	originalWorktree := c.Worktree
	c.Worktree = outside
	fixtureWrite(t, filepath.Join(originalWorktree, "untracked"), []byte("owned dirty file"))
	if err := c.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{originalWorktree, filepath.Join(f.root, "repository.git", "worktrees", "checkout")} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("worktree retained: %s: %v", path, err)
		}
	}
	for _, path := range []string{filepath.Join(outside, "sentinel"), c.Snapshots["metadata"], filepath.Join(f.root, "repository.git", "objects")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("history/foreign path removed: %s: %v", path, err)
		}
	}
	if got := fixtureGit(t, f.root, "--git-dir="+filepath.Join(f.root, "repository.git"), "rev-parse", "refs/review/base"); got != f.base {
		t.Fatalf("base history lost: %s", got)
	}
}

func TestAcquireCommentsMissing(t *testing.T) {
	for _, tc := range []struct {
		name, issues string
		remove       bool
	}{
		{"unavailable", "", true},
		{"invalid-json", `{`, false},
		{"not-pages", `[]`, false},
		{"null-page", `[null]`, false},
		{"not-comments", `[[null]]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAcquisitionFixture(t)
			if tc.remove {
				if err := os.Remove(filepath.Join(f.dir, "issues.json")); err != nil {
					t.Fatal(err)
				}
			} else {
				fixtureWrite(t, filepath.Join(f.dir, "issues.json"), []byte(tc.issues))
			}
			c, err := f.acquire(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer func(c *Checkout, ctx context.Context) { _ = c.Cleanup(ctx) }(c, context.Background())
			if !reflect.DeepEqual(c.Missing, []string{"issues"}) || c.Snapshots["issues"] != "" || len(c.Snapshots) != 5 {
				t.Fatalf("missing falsely represented as empty: %+v", c)
			}
			if _, err := os.Stat(filepath.Join(f.root, "snapshots", "issues.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed comments snapshot exists: %v", err)
			}
		})
	}
}

func TestAcquireMetadataValidationAndPartialCleanup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*acquisitionFixture)
	}{
		{"wrong-number", func(f *acquisitionFixture) { f.metadata["number"] = 18 }},
		{"wrong-url", func(f *acquisitionFixture) { f.metadata["html_url"] = "https://github.com/other/repo/pull/17" }},
		{"wrong-base-repo", func(f *acquisitionFixture) {
			f.metadata["base"].(map[string]any)["repo"] = map[string]string{"full_name": "other/repo"}
		}},
		{"invalid-head-sha", func(f *acquisitionFixture) { f.metadata["head"].(map[string]any)["sha"] = "HEAD" }},
		{"invalid-base-sha", func(f *acquisitionFixture) { f.metadata["base"].(map[string]any)["sha"] = strings.Repeat("z", 40) }},
		{"unsafe-fork", func(f *acquisitionFixture) {
			f.metadata["head"].(map[string]any)["repo"] = map[string]string{"full_name": "evil/../../repo"}
		}},
		{"deleted-fork", func(f *acquisitionFixture) { f.metadata["head"].(map[string]any)["repo"] = nil }},
		{"unfetchable-pin", func(f *acquisitionFixture) { f.metadata["head"].(map[string]any)["sha"] = strings.Repeat("a", 40) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f *acquisitionFixture
			if tc.name == "unfetchable-pin" {
				f = newAcquisitionFixture(t)
			} else {
				f = newExternalGitFixture(t)
				f.source.gitOutput = func(_ context.Context, _ string, args []string) ([]byte, error) {
					t.Fatalf("Git used before metadata rejection: %v", args)
					return nil, nil
				}
			}
			tc.mutate(f)
			f.saveMetadata(t)
			c, err := f.acquire(context.Background())
			if err == nil || c != nil {
				t.Fatalf("accepted bad metadata: %+v %v", c, err)
			}
			if tc.name != "unfetchable-pin" {
				wantErr := "invalid PR revision metadata"
				switch tc.name {
				case "wrong-number", "wrong-url", "wrong-base-repo":
					wantErr = "PR metadata identity mismatch"
				}
				if err.Error() != wantErr {
					t.Fatalf("metadata rejection: got %v, want %q", err, wantErr)
				}
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("stderr leaked: %v", err)
			}
			if _, err := os.Lstat(filepath.Join(f.root, "checkout")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial checkout retained: %v", err)
			}
			if _, err := os.Stat(filepath.Join(f.root, "repository.git")); err != nil {
				t.Fatalf("private repository removed: %v", err)
			}
		})
	}
}

func TestAcquireRejectsIncompleteOrAmbiguousHistory(t *testing.T) {
	for _, scenario := range []string{"unrelated", "ambiguous"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAcquisitionFixture(t)
			switch scenario {
			case "unrelated":
				f.head = fixtureGit(t, f.sourceRepo, "commit-tree", f.head+"^{tree}", "-m", "unrelated root")
			case "ambiguous":
				base, head := f.base, f.head
				f.base = fixtureGit(t, f.sourceRepo, "commit-tree", base+"^{tree}", "-p", base, "-p", head, "-m", "merge A")
				f.head = fixtureGit(t, f.sourceRepo, "commit-tree", head+"^{tree}", "-p", head, "-p", base, "-m", "merge B")
			}
			fixtureGit(t, f.sourceRepo, "push", "--force", f.baseRepo, f.base+":refs/heads/actual-base")
			fixtureGit(t, f.sourceRepo, "push", "--force", f.headRepo, f.head+":refs/heads/topic")
			f.metadata["base"].(map[string]any)["sha"] = f.base
			f.metadata["head"].(map[string]any)["sha"] = f.head
			f.saveMetadata(t)
			c, err := f.acquire(context.Background())
			if c != nil || err == nil {
				t.Fatalf("accepted %s history: %+v %v", scenario, c, err)
			}
			if _, err := os.Lstat(filepath.Join(f.root, "checkout")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial worktree retained: %v", err)
			}
			if _, err := os.Stat(filepath.Join(f.root, "snapshots", "metadata.json")); err != nil {
				t.Fatalf("metadata history lost: %v", err)
			}
		})
	}
}

func TestAcquireSubmoduleGitlinksWithoutContentOrTransport(t *testing.T) {
	f := newAcquisitionFixture(t)
	transports := make(chan string, 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transports <- r.URL.Path
		http.Error(w, "submodule transport must not run", http.StatusForbidden)
	}))
	defer server.Close()
	paths := []string{"nested", "vendor/other"}
	shas := []string{strings.Repeat("a", 40), strings.Repeat("b", 40)}
	var modules strings.Builder
	want := []string{}
	for i, path := range paths {
		fmt.Fprintf(&modules, "[submodule %q]\n path = %s\n url = %s/%d.git\n", path, path, server.URL, i)
		fixtureGit(t, f.sourceRepo, "update-index", "--add", "--cacheinfo", "160000,"+shas[i]+","+path)
		want = append(want, "160000 commit "+shas[i]+"\t"+path)
	}
	fixtureWrite(t, filepath.Join(f.sourceRepo, ".gitmodules"), []byte(modules.String()))
	fixtureGit(t, f.sourceRepo, "add", ".gitmodules")
	fixtureGit(t, f.sourceRepo, "commit", "-m", "gitlinks without external content")
	f.head = fixtureGit(t, f.sourceRepo, "rev-parse", "HEAD")
	fixtureGit(t, f.sourceRepo, "push", f.headRepo, "topic")
	f.metadata["head"].(map[string]any)["sha"] = f.head
	f.saveMetadata(t)
	if err := os.Remove(filepath.Join(f.dir, "issues.json")); err != nil {
		t.Fatal(err)
	}
	c, err := f.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Cleanup(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if !reflect.DeepEqual(c.Missing, []string{"submodule-content", "issues"}) || c.Snapshots["submodule-content"] != "" {
		t.Fatalf("external content uncertainty lost: snapshots=%v missing=%v", c.Snapshots, c.Missing)
	}
	path := c.Snapshots["submodules"]
	if path != filepath.Join(f.root, "snapshots", "submodules.json") {
		t.Fatalf("gitlink snapshot path: %q", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := json.Unmarshal(raw, &got); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("gitlinks: %q, want %q: %v", got, want, err)
	}
	for _, path := range paths {
		entries, err := os.ReadDir(filepath.Join(c.Worktree, path))
		if err != nil || len(entries) != 0 {
			t.Fatalf("submodule is not an empty placeholder: %s: %v %v", path, entries, err)
		}
	}
	checkError(t, c.Verify(context.Background()), "")
	select {
	case path := <-transports:
		t.Fatalf("external submodule transport attempted: %s", path)
	default:
	}
	for len(f.requests) > 0 {
		path := <-f.requests
		if path != "/base.git/info/refs" && path != "/base.git/git-upload-pack" && path != "/fork.git/info/refs" && path != "/fork.git/git-upload-pack" {
			t.Fatalf("unexpected Git transport: %s", path)
		}
	}
	if err := c.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	retained, err := os.ReadFile(c.Snapshots["submodules"])
	if err != nil || !bytes.Equal(raw, retained) || !reflect.DeepEqual(c.Missing, []string{"submodule-content", "issues"}) {
		t.Fatalf("cleanup lost gitlinks or uncertainty: %q %v %v", retained, c.Missing, err)
	}
}

func TestAcquirePartialFetchHydratesOnlyNeededBlobs(t *testing.T) {
	f := newAcquisitionFixture(t)
	for _, repo := range []string{f.baseRepo, f.headRepo} {
		fixtureGit(t, repo, "config", "uploadpack.allowFilter", "true")
		fixtureGit(t, repo, "config", "uploadpack.allowAnySHA1InWant", "true")
	}
	// This blob belongs only to the base-side branch, not the checkout or merge-base diff.
	baseBlob := fixtureGit(t, f.sourceRepo, "rev-parse", f.base+":base-only.txt")
	c, err := f.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Cleanup(context.Background()); err != nil {
			t.Error(err)
		}
	})
	missing := fixtureGit(t, c.Worktree, "rev-list", "--objects", "--all", "--missing=print")
	if !strings.Contains("\n"+missing+"\n", "\n?"+baseBlob+"\n") {
		t.Fatalf("base-only blob was not left missing (%s):\n%s", baseBlob, missing)
	}
	for path, want := range map[string][]byte{
		"shared.txt":    []byte("pinned head\n"),
		"odd\nname.txt": []byte("newline path\n"),
		"binary.dat":    {0, 1, 2, 3},
	} {
		raw, err := os.ReadFile(filepath.Join(c.Worktree, path))
		if err != nil || !bytes.Equal(raw, want) {
			t.Fatalf("head checkout incomplete: %q: %q %v", path, raw, err)
		}
	}
	if _, err := os.Stat(filepath.Join(c.Worktree, "base-only.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("base-only file unexpectedly in head: %v", err)
	}
	diff, err := os.ReadFile(c.Snapshots["diff"])
	if err != nil {
		t.Fatal(err)
	}
	wantDiff := fixtureGit(t, f.sourceRepo, "diff", "--binary", "--no-ext-diff", "--no-textconv", f.mergeBase, f.head, "--")
	if strings.TrimSpace(string(diff)) != wantDiff || bytes.Contains(diff, []byte("base-only.txt")) {
		t.Fatalf("partial fetch produced incomplete diff:\n%s\nwant:\n%s", diff, wantDiff)
	}
	checkError(t, c.Verify(context.Background()), "")
	for len(f.requests) > 0 {
		<-f.requests
	}
	if got := fixtureGit(t, c.Worktree, "show", c.BaseSHA+":base-only.txt"); got != "base" {
		t.Fatalf("lazy base blob hydration: %q", got)
	}
	hydrated := false
	for len(f.requests) > 0 {
		if strings.HasSuffix(<-f.requests, "/git-upload-pack") {
			hydrated = true
		}
	}
	if !hydrated {
		t.Fatal("reading missing blob did not use the real HTTP backend")
	}
	after := fixtureGit(t, c.Worktree, "rev-list", "--objects", "--all", "--missing=print")
	if strings.Contains("\n"+after+"\n", "\n?"+baseBlob+"\n") {
		t.Fatalf("hydrated blob remains missing: %s", after)
	}
	checkError(t, c.Verify(context.Background()), "")
}

func TestAcquireMetadataCommandBoundaries(t *testing.T) {
	for _, scenario := range []string{"unavailable", "oversized", "deadline"} {
		t.Run(scenario, func(t *testing.T) {
			f := newExternalGitFixture(t)
			f.source.gitOutput = func(_ context.Context, _ string, args []string) ([]byte, error) {
				t.Fatalf("Git used before metadata command rejection: %v", args)
				return nil, nil
			}
			if err := os.Remove(filepath.Join(f.dir, "metadata.json")); err != nil {
				t.Fatal(err)
			}
			fixtureWrite(t, filepath.Join(f.dir, "metadata-mode"), []byte(scenario))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "deadline" {
				ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()
			}
			c, err := f.acquire(ctx)
			if c != nil || err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("metadata failure: %+v %v", c, err)
			}
			wantErr := "PR metadata: acquisition command failed"
			switch scenario {
			case "deadline":
				wantErr = "PR metadata: " + context.DeadlineExceeded.Error()
			case "oversized":
				wantErr = "PR metadata: " + errAcquisitionOutputLimit.Error()
			}
			if scenario == "unavailable" && errors.Is(err, syscall.EPERM) {
				wantErr = "PR metadata: acquisition process group cleanup failed: " + syscall.EPERM.Error()
			}
			if err.Error() != wantErr {
				t.Fatalf("metadata command rejection: got %v, want %q", err, wantErr)
			}
			if scenario == "deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline lost: %v", err)
			}
			if scenario == "oversized" && !errors.Is(err, errAcquisitionOutputLimit) {
				t.Fatalf("output limit lost: %v", err)
			}
			if _, err := os.Stat(filepath.Join(f.root, "checkout")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed metadata checkout retained: %v", err)
			}
			if _, err := os.Stat(filepath.Join(f.root, "snapshots", "metadata.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid metadata published: %v", err)
			}
		})
	}
}

func TestCleanupRejectsReplacementWorktree(t *testing.T) {
	f := newAcquisitionFixture(t)
	c, err := f.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	original := c.Worktree
	moved := filepath.Join(f.root, "moved")
	if err := os.Rename(original, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(original, 0700); err != nil {
		t.Fatal(err)
	}
	fixtureWrite(t, filepath.Join(original, "foreign"), []byte("keep"))
	if err := c.Cleanup(context.Background()); err == nil {
		t.Fatal("cleanup accepted replacement inode")
	}
	if _, err := os.Stat(filepath.Join(original, "foreign")); err != nil {
		t.Fatal("replacement removed")
	}
	if err := os.RemoveAll(original); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(moved, original); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Cleanup(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled cleanup: %v", err)
	}
	if err := c.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireReapsSuccessfulCommandGroup(t *testing.T) {
	f := newAcquisitionFixture(t)
	fixtureWrite(t, filepath.Join(f.dir, "metadata-mode"), []byte("background"))
	c, err := f.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func(c *Checkout, ctx context.Context) { _ = c.Cleanup(ctx) }(c, context.Background())
	data, err := os.ReadFile(filepath.Join(f.dir, "background"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("successful command left descendant %d alive", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAcquireCancellationKillsGroupAndCleansOwnedCheckout(t *testing.T) {
	f := newAcquisitionFixture(t)
	fixtureWrite(t, filepath.Join(f.dir, "block"), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := f.acquire(ctx); done <- err }()
	ready := filepath.Join(f.dir, "ready")
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("acquisition ended before barrier: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("comment command did not reach barrier")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(f.root, "checkout", "shared.txt")); err != nil {
		t.Fatalf("barrier was not after checkout: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "SECRET") {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition did not join")
	}
	pidBytes, err := os.ReadFile(ready)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(pidBytes))
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("owned descendant %d survived cancellation", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Lstat(filepath.Join(f.root, "checkout")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled checkout retained: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.root, "snapshots", "diff.diff")); err != nil {
		t.Fatalf("history removed: %v", err)
	}
}

func TestAcquireRejectsExistingPathsAndCancelledParent(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if c, err := Acquire(ctx, root, "https://github.com/o/r/pull/1"); c != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled acquire: %+v %v", c, err)
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 0 {
		t.Fatal("cancelled acquire initialized paths")
	}
	if err := os.Mkdir(filepath.Join(root, "repository.git"), 0700); err != nil {
		t.Fatal(err)
	}
	fixtureWrite(t, filepath.Join(root, "repository.git", "sentinel"), []byte("keep"))
	if c, err := Acquire(context.Background(), root, "https://github.com/o/r/pull/1"); c != nil || err == nil {
		t.Fatalf("existing repo accepted: %+v %v", c, err)
	}
	data, err := os.ReadFile(filepath.Join(root, "repository.git", "sentinel"))
	if err != nil || string(data) != "keep" {
		t.Fatal("existing repo changed")
	}
}

func TestAcquireSanitizesGitConfiguration(t *testing.T) {
	f := newAcquisitionFixture(t)
	outside := t.TempDir()
	sentinel := filepath.Join(outside, "ran")
	hooks := filepath.Join(outside, "hooks")
	if err := os.Mkdir(hooks, 0700); err != nil {
		t.Fatal(err)
	}
	script := []byte("#!/bin/sh\nprintf unsafe > '" + sentinel + "'\nexit 1\n")
	for _, name := range []string{"post-checkout", "reference-transaction"} {
		if err := os.WriteFile(filepath.Join(hooks, name), script, 0700); err != nil {
			t.Fatal(err)
		}
	}
	global := filepath.Join(outside, "global")
	fixtureWrite(t, global, []byte("[core]\n hooksPath = "+hooks+"\n[filter \"evil\"]\n smudge = "+filepath.Join(hooks, "post-checkout")+"\n required = true\n"))
	fixtureWrite(t, filepath.Join(f.sourceRepo, ".gitattributes"), []byte("*.txt filter=evil\n"))
	fixtureGit(t, f.sourceRepo, "add", ".gitattributes")
	fixtureGit(t, f.sourceRepo, "commit", "-m", "attributes")
	f.head = fixtureGit(t, f.sourceRepo, "rev-parse", "HEAD")
	fixtureGit(t, f.sourceRepo, "push", f.headRepo, "topic")
	f.metadata["head"].(map[string]any)["sha"] = f.head
	f.saveMetadata(t)
	t.Setenv("GIT_CONFIG_GLOBAL", global)
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.hooksPath")
	t.Setenv("GIT_CONFIG_VALUE_0", hooks)
	t.Setenv("GIT_DIR", f.baseRepo)
	t.Setenv("GIT_WORK_TREE", outside)
	t.Setenv("GIT_INDEX_FILE", filepath.Join(outside, "index"))
	t.Setenv("GIT_TEMPLATE_DIR", outside)
	t.Setenv("GIT_EXTERNAL_DIFF", filepath.Join(hooks, "post-checkout"))
	t.Setenv("GIT_SSL_NO_VERIFY", "1")
	c, err := f.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func(c *Checkout, ctx context.Context) { _ = c.Cleanup(ctx) }(c, context.Background())
	if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("global hook/filter executed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(c.Worktree, "shared.txt"))
	if err != nil || string(data) != "pinned head\n" {
		t.Fatalf("checkout skipped or filtered files: %q %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(outside, "index")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign index modified: %v", err)
	}
}

func TestAcquisitionGitResultAdapter(t *testing.T) {
	t.Run("raw-results-and-instance-isolation", func(t *testing.T) {
		f := newExternalGitFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		output := f.source.gitOutput
		f.source.gitOutput = func(got context.Context, cwd string, argv []string) ([]byte, error) {
			if got != ctx || cwd != f.root {
				return nil, errors.New("Git boundary lost context or cwd")
			}
			raw, err := output(got, cwd, argv)
			// The callback must not alias the caller's dynamic argv or later calls.
			for i := range argv {
				argv[i] = "callback-owned"
			}
			return raw, err
		}
		c, err := f.acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { checkError(t, c.Cleanup(context.Background()), "") })
		f.source.gitOutput = func(context.Context, string, []string) ([]byte, error) {
			return nil, errors.New("caller replaced its callback after acquisition")
		}
		checkError(t, c.Verify(ctx), "")
		history := f.gitCalls(t)
		history[0] = "caller-owned"
		if f.gitCalls(t)[0] != "acquire-0" {
			t.Fatal("caller overwrote fixture command history")
		}
		id := sha256.Sum256([]byte("owner/repo\n17\n" + externalGitBase + "\n" + externalGitHead + "\n" + externalGitMergeBase))
		if c.BaseSHA != externalGitBase || c.HeadSHA != externalGitHead || c.MergeBase != externalGitMergeBase || c.DiffRange != externalGitMergeBase+".."+externalGitHead || c.ContextID != fmt.Sprintf("%x", id) || len(c.Snapshots) != 6 || len(c.Missing) != 0 {
			t.Fatalf("raw acquisition results were not parsed: %+v", c)
		}
		for key, path := range c.Snapshots {
			raw, err := os.ReadFile(path)
			if err != nil || c.snapshotHashes[key] != sha256.Sum256(raw) {
				t.Fatalf("SUT did not record snapshot hash %s: %v", key, err)
			}
		}
		for path, info := range map[string]os.FileInfo{c.root: c.rootInfo, c.repository: c.repositoryInfo, c.worktree: c.worktreeInfo} {
			actual, err := os.Stat(path)
			if err != nil || info == nil || !os.SameFile(actual, info) {
				t.Fatalf("SUT did not record reserved path ownership: %s: %v", path, err)
			}
		}
		for key, want := range map[string]string{"diff": externalGitDiff, "changed-files": `["binary.dat","odd\nname.txt","shared.txt"]`, "issues": `[{"id":1},{"id":2}]`, "inline": `[]`} {
			if raw, err := os.ReadFile(c.Snapshots[key]); err != nil || string(raw) != want {
				t.Fatalf("raw snapshot %s: %q %v", key, raw, err)
			}
		}
		if raw, err := output(ctx, c.Worktree, []string{"-c", "core.hooksPath=/dev/null", "rev-parse", "HEAD"}); err != nil || strings.TrimSpace(string(raw)) != externalGitHead {
			t.Fatalf("independent fixed HEAD query: %q %v", raw, err)
		}
		verifyHead := []string{"--git-dir=" + filepath.Join(c.repository, "worktrees", "checkout"), "--work-tree=" + c.worktree, "rev-parse", "HEAD"}
		beforeArgs := append([]string{}, verifyHead...)
		if raw, err := c.git(ctx, verifyHead...); err != nil || strings.TrimSpace(string(raw)) != externalGitHead {
			t.Fatalf("valid HEAD query rejected: %q %v", raw, err)
		}
		if !reflect.DeepEqual(verifyHead, beforeArgs) {
			t.Fatal("callback overwrote caller argv")
		}
		beforeCalls := f.gitCalls(t)
		for _, args := range [][]string{{"unexpected-command"}, {"rev-parse", "--verify", strings.Repeat("4", 40) + "^{commit}"}, append([]string{"-c", "core.hooksPath=unexpected"}, verifyHead...)} {
			if raw, err := c.git(ctx, args...); err == nil || raw != nil {
				t.Fatalf("unexpected argv accepted: %v: %q %v", args, raw, err)
			}
		}
		command := []string{"-c", "core.hooksPath=/dev/null", "rev-parse", "HEAD"}
		if raw, err := output(ctx, c.root, command); err == nil || raw != nil {
			t.Fatalf("HEAD query accepted wrong cwd: %q %v", raw, err)
		}
		if !reflect.DeepEqual(beforeCalls, f.gitCalls(t)) {
			t.Fatal("unknown command/cwd advanced fixture receipts")
		}
		fixtureWrite(t, c.Snapshots["diff"], []byte("changed snapshot"))
		checkError(t, c.Verify(ctx), "acquisition snapshot diff changed")
		if !reflect.DeepEqual(beforeCalls, f.gitCalls(t)) {
			t.Fatal("snapshot verification was bypassed")
		}
		registration := filepath.Join(c.repository, "worktrees", "checkout")
		if info, err := os.Stat(registration); err != nil || !info.IsDir() {
			t.Fatalf("cleanup registration never existed: %v", err)
		}
		checkError(t, c.Cleanup(context.Background()), "")
		for _, path := range []string{c.Worktree, registration} {
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("real cleanup did not remove %s: %v", path, err)
			}
		}
	})
	for _, tc := range []struct {
		name   string
		cancel bool
	}{
		{"command-error", false},
		{"context-cause", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExternalGitFixture(t)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("external Git result failure")
			output := f.source.gitOutput
			f.source.gitOutput = func(got context.Context, cwd string, argv []string) ([]byte, error) {
				if got != ctx || cwd != f.root {
					return nil, errors.New("Git boundary lost context or cwd")
				}
				if _, err := output(got, cwd, argv); err != nil {
					return nil, err
				}
				if tc.cancel {
					cancel(cause)
				}
				return []byte("partial command output"), cause
			}
			c, err := f.acquire(ctx)
			if c != nil || !errors.Is(err, cause) || !strings.HasPrefix(err.Error(), "git init:") {
				t.Fatalf("external error lost its cause or became a Checkout: %+v %v", c, err)
			}
			if tc.cancel && context.Cause(ctx) != cause {
				t.Fatal("caller context lost cancellation cause")
			}
			if !reflect.DeepEqual(f.gitCalls(t), []string{"acquire-0"}) {
				t.Fatal("acquisition continued after command failure")
			}
			if _, err := os.Stat(filepath.Join(f.root, "checkout")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed acquisition retained checkout: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		mode, want string
		verify     bool
	}{
		{"pin-result", "fetched revision is not the pinned commit", false},
		{"merge-result", "missing or ambiguous merge-base", false},
		{"names-unterminated", "changed-files is not NUL terminated", false},
		{"names-utf8", "changed-files contains a non-UTF-8 path", false},
		{"unknown-pin", "git fetch:", false},
		{"head-result", "review checkout HEAD changed", true},
		{"status-result", "review checkout was modified", true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			f := newExternalGitFixture(t)
			if len(f.gitCalls(t)) != 0 {
				t.Fatal("fixture inherited command history")
			}
			if tc.mode == "unknown-pin" {
				f.metadata["head"].(map[string]any)["sha"] = strings.Repeat("4", 40)
				f.saveMetadata(t)
			} else if !tc.verify {
				f.setGitMode(tc.mode)
			}
			c, err := f.acquire(context.Background())
			if tc.verify {
				checkError(t, err, "")
				t.Cleanup(func() { checkError(t, c.Cleanup(context.Background()), "") })
				checkError(t, c.Verify(context.Background()), "")
				before := len(f.gitCalls(t))
				f.setGitMode(tc.mode)
				checkError(t, c.Verify(context.Background()), tc.want)
				wantCalls := []string{"verify-head"}
				if tc.mode == "status-result" {
					wantCalls = append(wantCalls, "verify-status")
				}
				if got := f.gitCalls(t)[before:]; !reflect.DeepEqual(got, wantCalls) {
					t.Fatalf("Verify rejection bypassed raw result: %v", got)
				}
			} else {
				checkError(t, err, tc.want)
				if c != nil {
					t.Fatal("invalid external result became a Checkout")
				}
				for _, path := range []string{"checkout", "repository.git/worktrees/checkout"} {
					if _, err := os.Stat(filepath.Join(f.root, path)); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("failed acquisition cleanup: %s: %v", path, err)
					}
				}
			}
		})
	}
}

const (
	externalGitBase      = "1111111111111111111111111111111111111111"
	externalGitHead      = "2222222222222222222222222222222222222222"
	externalGitMergeBase = "3333333333333333333333333333333333333333"
	externalGitTree      = "100644 blob aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\tbinary.dat\x00" +
		"100644 blob bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\todd\nname.txt\x00" +
		"100644 blob cccccccccccccccccccccccccccccccccccccccc\tshared.txt\x00"
	externalGitNames = "binary.dat\x00odd\nname.txt\x00shared.txt\x00"
	externalGitDiff  = "diff --git a/binary.dat b/binary.dat\nnew file mode 100644\n" +
		"index 0000000000000000000000000000000000000000..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n" +
		"GIT binary patch\nliteral 4\nLcmZQzWMXDv01f~L\n\nliteral 0\nHcmV?d00001\n\n" +
		"diff --git \"a/odd\\nname.txt\" \"b/odd\\nname.txt\"\nnew file mode 100644\n" +
		"--- /dev/null\n+++ \"b/odd\\nname.txt\"\n@@ -0,0 +1 @@\n+newline path\n" +
		"diff --git a/shared.txt b/shared.txt\n--- a/shared.txt\n+++ b/shared.txt\n@@ -1 +1 @@\n-common\n+pinned head\n"
)

// This fixture supplies only fixed external responses, not a Git object database.
// The SUT reserves paths and records ownership, snapshots, hashes and ContextID.
func newExternalGitFixture(t *testing.T) *acquisitionFixture {
	t.Helper()
	f := newAcquisitionFixturePaths(t)
	f.base, f.head, f.mergeBase = externalGitBase, externalGitHead, externalGitMergeBase
	f.configureAPI(t)
	f.source.gitOutput = f.gitOutput
	// The existing protocol.allow=never rejects file transport if the callback
	// is accidentally unset; this fixture must never fall back to the network.
	f.source.remote = func(repository string) string { return "file://" + filepath.Join(f.dir, repository+".git") }
	return f
}

func (f *acquisitionFixture) gitCalls(t *testing.T) []string {
	t.Helper()
	f.gitMu.Lock()
	defer f.gitMu.Unlock()
	return append([]string(nil), f.gitHistory...)
}

func (f *acquisitionFixture) setGitMode(mode string) {
	f.gitMu.Lock()
	defer f.gitMu.Unlock()
	f.gitMode = mode
}

func (f *acquisitionFixture) gitOutput(ctx context.Context, cwd string, args []string) ([]byte, error) {
	f.gitMu.Lock()
	defer f.gitMu.Unlock()
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	invalid := errors.New("unexpected external Git fixture command, cwd or state")
	root := f.root
	repository, worktree := filepath.Join(root, "repository.git"), filepath.Join(root, "checkout")
	registration := filepath.Join(repository, "worktrees", "checkout")
	calls, mode := f.gitHistory, f.gitMode
	switch mode {
	case "", "pin-result", "merge-result", "names-unterminated", "names-utf8", "head-result", "status-result":
	default:
		return nil, invalid
	}
	fixed := []string{
		"--no-pager", "--git-dir=" + repository,
		"-c", "core.hooksPath=/dev/null", "-c", "core.attributesFile=/dev/null",
		"-c", "core.fsmonitor=false", "-c", "core.autocrlf=false", "-c", "core.sparseCheckout=false",
		"-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential",
		"-c", "credential.interactive=false", "-c", "http.sslVerify=true", "-c", "http.followRedirects=false",
		"-c", "protocol.allow=never", "-c", "protocol.https.allow=always", "-c", "protocol.http.allow=always",
		"-c", "gc.auto=0", "-c", "maintenance.auto=false", "-c", "submodule.recurse=false",
	}
	steps := []struct {
		args   []string
		output string
	}{
		{[]string{"init", "--bare", "--template=", repository}, ""},
		{[]string{"config", "remote.base.url", "file://" + filepath.Join(f.dir, "owner/repo.git")}, ""},
		{[]string{"config", "remote.base.promisor", "true"}, ""},
		{[]string{"config", "remote.base.partialclonefilter", "blob:none"}, ""},
		{[]string{"fetch", "--filter=blob:none", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head", "base", externalGitBase + ":refs/review/base"}, ""},
		{[]string{"rev-parse", "--verify", externalGitBase + "^{commit}"}, externalGitBase + "\n"},
		{[]string{"config", "remote.head.url", "file://" + filepath.Join(f.dir, "fork/repo.git")}, ""},
		{[]string{"config", "remote.head.promisor", "true"}, ""},
		{[]string{"config", "remote.head.partialclonefilter", "blob:none"}, ""},
		{[]string{"fetch", "--filter=blob:none", "--no-tags", "--no-recurse-submodules", "--no-write-fetch-head", "head", externalGitHead + ":refs/review/head"}, ""},
		{[]string{"rev-parse", "--verify", externalGitHead + "^{commit}"}, externalGitHead + "\n"},
		{[]string{"merge-base", "--all", externalGitBase, externalGitHead}, externalGitMergeBase + "\n"},
		{[]string{"ls-tree", "-r", "-z", externalGitHead}, externalGitTree},
		{[]string{"worktree", "add", "--detach", worktree, externalGitHead}, ""},
		{[]string{"diff", "--binary", "--no-ext-diff", "--no-textconv", externalGitMergeBase, externalGitHead, "--"}, externalGitDiff},
		{[]string{"diff", "--name-only", "-z", "--no-ext-diff", "--no-textconv", externalGitMergeBase, externalGitHead, "--"}, externalGitNames},
	}
	key, output := "", ""
	if reflect.DeepEqual(args, []string{"-c", "core.hooksPath=/dev/null", "rev-parse", "HEAD"}) {
		if cwd != worktree || len(calls) < len(steps) {
			return nil, invalid
		}
		key, output = "next-head", externalGitHead+"\n"
	} else {
		if cwd != root || len(args) < len(fixed) || !reflect.DeepEqual(args[:len(fixed)], fixed) {
			return nil, invalid
		}
		args = args[len(fixed):]
		if len(calls) < len(steps) {
			i := len(calls)
			if !reflect.DeepEqual(args, steps[i].args) {
				return nil, invalid
			}
			key, output = fmt.Sprintf("acquire-%d", i), steps[i].output
			switch i {
			case 0:
				if err := os.Mkdir(filepath.Join(repository, "objects"), 0700); err != nil {
					return nil, err
				}
			case 5:
				if mode == "pin-result" {
					output = externalGitHead + "\n"
				}
			case 11:
				if mode == "merge-result" {
					output = "not-a-commit\n"
				}
			case 13:
				if err := os.Mkdir(filepath.Join(repository, "worktrees"), 0700); err != nil {
					return nil, err
				}
				if err := os.Mkdir(registration, 0700); err != nil {
					return nil, err
				}
				for name, body := range map[string]string{"shared.txt": "pinned head\n", "odd\nname.txt": "newline path\n", "binary.dat": "\x00\x01\x02\x03"} {
					file, err := os.OpenFile(filepath.Join(worktree, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
					if err != nil {
						return nil, err
					}
					_, writeErr := file.WriteString(body)
					if err := errors.Join(writeErr, file.Close()); err != nil {
						return nil, err
					}
				}
			case 15:
				switch mode {
				case "names-unterminated":
					output = strings.TrimSuffix(externalGitNames, "\x00")
				case "names-utf8":
					output = "\xff\x00"
				}
			}
		} else {
			verify := []string{"--git-dir=" + registration, "--work-tree=" + worktree}
			switch {
			case reflect.DeepEqual(args, append(append([]string{}, verify...), "rev-parse", "HEAD")):
				key, output = "verify-head", externalGitHead+"\n"
				if mode == "head-result" {
					output = externalGitBase + "\n"
				}
			case reflect.DeepEqual(args, append(append([]string{}, verify...), "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching", "--ignore-submodules=none")):
				key = "verify-status"
				if mode == "status-result" {
					output = " M shared.txt\n"
				}
			default:
				return nil, invalid
			}
		}
	}
	if key == "next-head" || strings.HasPrefix(key, "verify-") {
		for _, path := range []string{worktree, registration} {
			if info, err := os.Stat(path); err != nil || !info.IsDir() {
				return nil, invalid
			}
		}
	}
	f.gitHistory = append(f.gitHistory, key)
	return []byte(output), nil
}

// Both acquisition fixtures use this raw GitHub boundary. The default fixture
// still serves Git HTTP requests with real git over temporary repositories.
func TestAcquisitionGHHelper(t *testing.T) {
	dir := os.Getenv("PWC_ACQUIRE_FIXTURE")
	if dir == "" {
		return
	}
	args := os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		return
	}
	args = args[1:]
	if len(args) < 8 || !reflect.DeepEqual(args[:7], []string{"api", "--method", "GET", "--hostname", "github.com", "-H", "Accept: application/vnd.github+json"}) {
		fmt.Fprintln(os.Stderr, "SECRET invalid API arguments")
		os.Exit(2)
	}
	key := ""
	switch args[7] {
	case "repos/owner/repo/pulls/17":
		key = "metadata"
	case "repos/owner/repo/issues/17/comments?per_page=100":
		key = "issues"
	case "repos/owner/repo/pulls/17/comments?per_page=100":
		key = "inline"
	case "repos/owner/repo/pulls/17/reviews?per_page=100":
		key = "reviews"
	default:
		os.Exit(2)
	}
	if key != "metadata" && !reflect.DeepEqual(args[8:], []string{"--paginate", "--slurp"}) {
		os.Exit(2)
	}
	if key == "metadata" && len(args) != 8 {
		os.Exit(2)
	}
	calls, err := os.OpenFile(filepath.Join(dir, "calls"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	_, _ = fmt.Fprintln(calls, key)
	_ = calls.Close()
	if _, err := os.Stat(filepath.Join(dir, "block")); err == nil && key == "issues" {
		child := exec.Command("/bin/sleep", "60")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if child.Start() != nil {
			os.Exit(2)
		}
		// Publish atomically so the reader's existence check implies the pid
		// bytes are complete; a truncated create is readable under load.
		if err := os.WriteFile(filepath.Join(dir, "ready.tmp"), []byte(strconv.Itoa(child.Process.Pid)), 0600); err != nil {
			os.Exit(2)
		}
		if err := os.Rename(filepath.Join(dir, "ready.tmp"), filepath.Join(dir, "ready")); err != nil {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "SECRET cancellation credential")
		_ = child.Wait()
		os.Exit(2)
	}
	if key == "metadata" {
		mode, _ := os.ReadFile(filepath.Join(dir, "metadata-mode"))
		switch string(mode) {
		case "background":
			child := exec.Command("/bin/sleep", "60")
			if child.Start() != nil {
				os.Exit(2)
			}
			if os.WriteFile(filepath.Join(dir, "background"), []byte(strconv.Itoa(child.Process.Pid)), 0600) != nil {
				os.Exit(2)
			}
		case "oversized":
			block := bytes.Repeat([]byte{'x'}, 1<<20)
			for i := 0; i < 65; i++ {
				if _, err := os.Stdout.Write(block); err != nil {
					os.Exit(2)
				}
			}
			os.Exit(0)
		case "deadline":
			time.Sleep(time.Minute)
			os.Exit(2)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, key+".json"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "SECRET unavailable credential")
		os.Exit(2)
	}
	_, _ = os.Stdout.Write(data)
	os.Exit(0)
}
