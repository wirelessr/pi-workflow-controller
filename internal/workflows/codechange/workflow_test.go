package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
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

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/pathexec"
	"pi-workflow-controller/internal/workflows/review"
)

// TestGHHelper is the GitHub API boundary: the gh PATH wrapper runs it in a
// real subprocess that accepts only a REST GET of the target repository, its
// pull requests, a compare, its branch refs, a branch's pull request list or,
// paginated, a pull request's comments, which are always none, a commit's
// check runs or combined status, a GitHub Actions run, and the review
// threads GraphQL query of pull request 1 and the review thread query of a
// node. Pull requests
// are the records the fake Agent wrote under prs/, with the base and head SHAs
// read from the local origin at query time like GitHub; compare and refs come
// from the real git on that origin. Per kind of read, gh-fail-<kind>
// fails it, gh-body-<kind> replaces its body and pr-patch.json is merged into
// a pull request, only once there were as many check runs reads as
// pr-patch-after-checks says when it exists; gh-move-head commits in that
// worktree during a pull request read. With gh-require-closed, a branch ref or pull request list
// read fails until implementer-closed exists. The k-th check runs or status
// read answers entry k of checks.json or statuses.json, the last entry once
// k is past it, and fails for a null entry; without the file there are none.
// Each check runs read appends its time to checks-times; each of the first
// as many as gh-move-on-checks says first moves the pull request branch on
// the origin to a new commit and writes it to gh-moved, and the one
// gh-move-head-at-checks numbers first commits in the worktree it names. Once there were as many check runs
// reads as gh-fail-pull-after-checks says, a pull request read fails, and
// so does the one gh-fail-pull-at numbers among the pull request reads.
// gh-fail-threads fails the review threads query, from the query it
// numbers on when it holds a number. gh-block-threads blocks it like
// gh-block. The review thread query of a node ID answers the node in
// thread-<ID>.json, the ID path-escaped, and fails like GitHub's error for
// a node it cannot resolve without it; gh-body-thread replaces its body. A
// GitHub Actions run GET answers run-<ID>.json and fails without it.
func TestGHHelper(t *testing.T) {
	dir := os.Getenv("PWC_CC_GH")
	if dir == "" {
		return
	}
	args, ok := pathexec.Args()
	if !ok {
		return
	}
	calls, err := os.OpenFile(filepath.Join(dir, "gh-calls"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	logged := slices.Clone(args)
	if len(logged) > 5 && logged[5] == "query="+reviewThreadsQuery {
		logged[5] = "query=<review threads>"
	}
	if len(logged) > 5 && logged[5] == "query="+reviewThreadQuery {
		logged[5] = "query=<review thread>"
	}
	_, _ = fmt.Fprintln(calls, strings.Join(logged, " "))
	_ = calls.Close()
	if len(args) == 10 && args[1] == "graphql" && args[5] == "query="+reviewThreadQuery {
		id, ok := strings.CutPrefix(args[7], "id=")
		if !ok || !slices.Equal(args, []string{"api", "graphql", "--hostname", "github.com", "-f", "query=" + reviewThreadQuery, "-f", "id=" + id, "--paginate", "--slurp"}) {
			os.Exit(2)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "gh-body-thread"))
		if err != nil {
			node, err := os.ReadFile(filepath.Join(dir, "thread-"+url.PathEscape(id)+".json"))
			if err != nil {
				os.Exit(1)
			}
			raw = []byte(`[{"data":{"node":` + string(node) + `}}]`)
		}
		_, _ = os.Stdout.Write(raw)
		os.Exit(0)
	}
	if len(args) > 1 && args[1] == "graphql" {
		want := []string{"api", "graphql", "--hostname", "github.com", "-f", "query=" + reviewThreadsQuery, "-f", "name=repo", "-f", "owner=owner", "-F", "number=1", "--paginate", "--slurp"}
		if !slices.Equal(args, want) {
			os.Exit(2)
		}
		if raw, err := os.ReadFile(filepath.Join(dir, "gh-fail-threads")); err == nil {
			from := 1
			if len(raw) > 0 {
				if from, err = strconv.Atoi(string(raw)); err != nil {
					os.Exit(2)
				}
			}
			calls, err := os.ReadFile(filepath.Join(dir, "gh-calls"))
			if err != nil {
				os.Exit(2)
			}
			if strings.Count(string(calls), "query=<review threads>") >= from {
				os.Exit(1)
			}
		}
		if _, err := os.Stat(filepath.Join(dir, "gh-block-threads")); err == nil {
			block(dir)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "gh-body-threads"))
		if err != nil {
			raw = []byte(`[{"data":{"repository":{"pullRequest":{"reviewThreads":{"totalCount":0,"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}]`)
		}
		_, _ = os.Stdout.Write(raw)
		os.Exit(0)
	}
	if len(args) < 8 || !reflect.DeepEqual(args[:7], []string{"api", "--method", "GET", "--hostname", "github.com", "-H", "Accept: application/vnd.github+json"}) {
		os.Exit(2)
	}
	if _, err := os.Stat(filepath.Join(dir, "gh-block")); err == nil {
		block(dir)
	}
	endpoint := args[7]
	kind, arg := ghKind(endpoint)
	paginated := kind == "comments" || kind == "checks" || kind == "statuses"
	if kind == "" || paginated != reflect.DeepEqual(args[8:], []string{"--paginate", "--slurp"}) || (!paginated && len(args) != 8) {
		os.Exit(2)
	}
	if _, err := os.Stat(filepath.Join(dir, "gh-fail-"+kind)); err == nil {
		os.Exit(1)
	}
	if _, err := os.Stat(filepath.Join(dir, "gh-require-closed")); err == nil && (kind == "refs" || kind == "pulls") {
		if _, err := os.Stat(filepath.Join(dir, "implementer-closed")); err != nil {
			os.Exit(1)
		}
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "gh-body-"+kind)); err == nil {
		_, _ = os.Stdout.Write(raw)
		os.Exit(0)
	}
	origin := filepath.Join(dir, "origin.git")
	var out any
	switch kind {
	case "comments":
		out = []any{[]any{}}
	case "run":
		raw, err := os.ReadFile(filepath.Join(dir, "run-"+arg+".json"))
		if err != nil {
			os.Exit(1)
		}
		_, _ = os.Stdout.Write(raw)
		os.Exit(0)
	case "checks", "statuses":
		k := stateReads(dir, kind)
		if kind == "checks" {
			stateChecks(dir, origin, k)
		}
		file, none := "checks.json", `[{"total_count":0,"check_runs":[]}]`
		if kind == "statuses" {
			file, none = "statuses.json", `[{"state":"pending","sha":"`+arg+`","total_count":0,"statuses":[]}]`
		}
		body := []byte(none)
		if raw, err := os.ReadFile(filepath.Join(dir, file)); err == nil {
			var seq []json.RawMessage
			if json.Unmarshal(raw, &seq) != nil || len(seq) == 0 {
				os.Exit(2)
			}
			body = seq[min(k, len(seq))-1]
			if string(body) == "null" {
				os.Exit(1)
			}
		}
		_, _ = os.Stdout.Write(body)
		os.Exit(0)
	case "repo":
		data, err := os.ReadFile(filepath.Join(dir, "repo.json"))
		if err != nil {
			os.Exit(2)
		}
		_, _ = os.Stdout.Write(data)
		os.Exit(0)
	case "pull":
		if raw, err := os.ReadFile(filepath.Join(dir, "gh-fail-pull-after-checks")); err == nil {
			if n, err := strconv.Atoi(string(raw)); err != nil || stateReads(dir, "checks") >= n {
				os.Exit(1)
			}
		}
		if raw, err := os.ReadFile(filepath.Join(dir, "gh-fail-pull-at")); err == nil {
			if n, err := strconv.Atoi(string(raw)); err != nil || pullReads(dir) == n {
				os.Exit(1)
			}
		}
		if target, err := os.ReadFile(filepath.Join(dir, "gh-move-head")); err == nil {
			if originGit(string(target), "-c", "core.hooksPath=/dev/null", "commit", "-q", "--allow-empty", "-m", "moved") == "" {
				_ = os.Remove(filepath.Join(dir, "gh-move-head"))
			}
		}
		pr, ok := readPull(dir, origin, arg)
		if !ok {
			os.Exit(1)
		}
		patched := true
		if raw, err := os.ReadFile(filepath.Join(dir, "pr-patch-after-checks")); err == nil {
			n, err := strconv.Atoi(string(raw))
			if err != nil {
				os.Exit(2)
			}
			patched = stateReads(dir, "checks") >= n
		}
		if raw, err := os.ReadFile(filepath.Join(dir, "pr-patch.json")); err == nil && patched {
			var patch map[string]any
			if json.Unmarshal(raw, &patch) != nil {
				os.Exit(2)
			}
			merge(pr, patch)
		}
		out = pr
	case "compare":
		commit, head, _ := strings.Cut(arg, "...")
		if originGit(origin, "cat-file", "-e", commit+"^{commit}") != "" || originGit(origin, "cat-file", "-e", head+"^{commit}") != "" {
			os.Exit(1)
		}
		status, base := "diverged", ""
		switch {
		case commit == head:
			status, base = "identical", commit
		case originGit(origin, "merge-base", "--is-ancestor", commit, head) == "":
			status, base = "ahead", commit
		case originGit(origin, "merge-base", "--is-ancestor", head, commit) == "":
			status, base = "behind", head
		default:
			base = originOutput(origin, "merge-base", commit, head)
		}
		out = map[string]any{"status": status, "merge_base_commit": map[string]any{"sha": base}, "commits": []any{}}
	case "refs":
		refs := []any{}
		for _, line := range strings.Split(originOutput(origin, "for-each-ref", "--format=%(refname) %(objectname)", "refs/heads/"), "\n") {
			ref, sha, _ := strings.Cut(line, " ")
			if strings.HasPrefix(ref, "refs/heads/"+arg) {
				refs = append(refs, map[string]any{"ref": ref, "object": map[string]any{"sha": sha}})
			}
		}
		out = refs
	case "pulls":
		pulls := []any{}
		entries, _ := os.ReadDir(filepath.Join(dir, "prs"))
		for _, e := range entries {
			pr, ok := readPull(dir, origin, strings.TrimSuffix(e.Name(), ".json"))
			if ok && "owner:"+pr["head"].(map[string]any)["ref"].(string) == arg {
				pulls = append(pulls, pr)
			}
		}
		out = pulls
	}
	raw, err := json.Marshal(out)
	if err != nil {
		os.Exit(2)
	}
	_, _ = os.Stdout.Write(raw)
	os.Exit(0)
}

// stateReads is how many check runs or status reads gh-calls holds, the
// current one included.
func stateReads(dir, kind string) int {
	raw, err := os.ReadFile(filepath.Join(dir, "gh-calls"))
	if err != nil {
		os.Exit(2)
	}
	suffix := map[string]string{"checks": "/check-runs?per_page=100", "statuses": "/status?per_page=100"}[kind]
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, suffix+" --paginate") {
			n++
		}
	}
	return n
}

// pullReads is how many pull request reads gh-calls holds, the current one
// included.
func pullReads(dir string) int {
	raw, err := os.ReadFile(filepath.Join(dir, "gh-calls"))
	if err != nil {
		os.Exit(2)
	}
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if fields := strings.Fields(line); len(fields) == 9 && fields[1] == "--method" {
			if kind, _ := ghKind(fields[8]); kind == "pull" {
				n++
			}
		}
	}
	return n
}

// stateChecks records the time of check runs read k and, while k is at most
// gh-move-on-checks, moves pull request 1's branch on origin.
func stateChecks(dir, origin string, k int) {
	times, err := os.OpenFile(filepath.Join(dir, "checks-times"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	_, _ = fmt.Fprintln(times, time.Now().UnixNano())
	_ = times.Close()
	if raw, err := os.ReadFile(filepath.Join(dir, "gh-move-head-at-checks")); err == nil {
		at, worktree, _ := strings.Cut(string(raw), " ")
		if n, err := strconv.Atoi(at); err != nil || (n == k && originGit(worktree, "-c", "core.hooksPath=/dev/null", "commit", "-q", "--allow-empty", "-m", "moved between rounds") != "") {
			os.Exit(2)
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "gh-move-on-checks"))
	if err != nil {
		return
	}
	if n, err := strconv.Atoi(string(raw)); err != nil || k > n {
		return
	}
	pr, ok := readPull(dir, origin, "1")
	if !ok {
		os.Exit(2)
	}
	ref := "refs/heads/" + pr["head"].(map[string]any)["ref"].(string)
	tip := originOutput(origin, "rev-parse", ref)
	cmd := exec.Command(os.Getenv("PWC_CC_GIT"), "-C", origin, "commit-tree", tip+"^{tree}", "-p", tip, "-m", "moved during the wait")
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.com", "GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.com")
	out, err := cmd.Output()
	if err != nil {
		os.Exit(2)
	}
	moved := strings.TrimSpace(string(out))
	if originGit(origin, "update-ref", ref, moved) != "" || os.WriteFile(filepath.Join(dir, "gh-moved"), []byte(fmt.Sprintf("%s %d", moved, time.Now().UnixNano())), 0600) != nil {
		os.Exit(2)
	}
}

// ghKind is the kind of read endpoint is and its argument: the pull request
// number, the compare range, the branch ref prefix, the head filter or the
// commit of a check runs or status read.
func ghKind(endpoint string) (string, string) {
	const repo = "repos/owner/repo"
	rest, ok := strings.CutPrefix(endpoint, repo)
	switch {
	case !ok:
		return "", ""
	case rest == "":
		return "repo", ""
	case slices.Contains([]string{"/issues/1/comments?per_page=100", "/pulls/1/comments?per_page=100", "/pulls/1/reviews?per_page=100"}, rest):
		return "comments", ""
	case strings.HasPrefix(rest, "/pulls/"):
		return "pull", strings.TrimPrefix(rest, "/pulls/")
	case strings.HasPrefix(rest, "/commits/") && strings.HasSuffix(rest, "/check-runs?per_page=100"):
		return "checks", strings.TrimSuffix(strings.TrimPrefix(rest, "/commits/"), "/check-runs?per_page=100")
	case strings.HasPrefix(rest, "/commits/") && strings.HasSuffix(rest, "/status?per_page=100"):
		return "statuses", strings.TrimSuffix(strings.TrimPrefix(rest, "/commits/"), "/status?per_page=100")
	case strings.HasPrefix(rest, "/actions/runs/"):
		return "run", strings.TrimPrefix(rest, "/actions/runs/")
	case strings.HasPrefix(rest, "/compare/"):
		return "compare", strings.TrimPrefix(rest, "/compare/")
	case strings.HasPrefix(rest, "/git/matching-refs/heads/"):
		branch, err := url.PathUnescape(strings.TrimPrefix(rest, "/git/matching-refs/heads/"))
		if err != nil {
			return "", ""
		}
		return "refs", branch
	case strings.HasPrefix(rest, "/pulls?"):
		query, err := url.ParseQuery(strings.TrimPrefix(rest, "/pulls?"))
		if err != nil || query.Get("state") != "all" || query.Get("per_page") != "100" || len(query) != 3 {
			return "", ""
		}
		return "pulls", query.Get("head")
	}
	return "", ""
}

// readPull is the pull request record number with its base and head SHAs
// read from origin, or the recorded ones once a branch is gone.
func readPull(dir, origin, number string) (map[string]any, bool) {
	raw, err := os.ReadFile(filepath.Join(dir, "prs", number+".json"))
	if err != nil {
		return nil, false
	}
	var pr map[string]any
	if json.Unmarshal(raw, &pr) != nil {
		os.Exit(2)
	}
	for _, side := range []string{"base", "head"} {
		s := pr[side].(map[string]any)
		if sha := originOutput(origin, "for-each-ref", "--format=%(objectname)", "refs/heads/"+s["ref"].(string)); sha != "" {
			s["sha"] = sha
		}
	}
	return pr, true
}

// merge sets every field of patch in v, recursing into objects.
func merge(v, patch map[string]any) {
	for k, p := range patch {
		if sub, ok := p.(map[string]any); ok {
			if into, ok := v[k].(map[string]any); ok {
				merge(into, sub)
				continue
			}
		}
		if p == nil {
			delete(v, k)
			continue
		}
		v[k] = p
	}
}

// originGit runs the real git in dir and returns "" on success.
func originGit(dir string, args ...string) string {
	cmd := exec.Command(os.Getenv("PWC_CC_GIT"), append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.com", "GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.com")
	if out, err := cmd.CombinedOutput(); err != nil {
		return string(out) + err.Error()
	}
	return ""
}

func originOutput(dir string, args ...string) string {
	out, err := exec.Command(os.Getenv("PWC_CC_GIT"), append([]string{"-C", dir}, args...)...).Output()
	if err != nil {
		os.Exit(2)
	}
	return strings.TrimSpace(string(out))
}

// TestGitHelper records each Controller git call, optionally blocks or fails
// one subcommand or acts as a git without --no-lazy-fetch, and otherwise
// replaces itself with the real git. For the deep review's git only, it
// replaces the GitHub remote review uses with the local origin and allows the
// file protocol; the call is recorded as review made it. Before a failure it can replace the
// reserved checkout with a symlink to git-swap's target, so the workflow's
// cleanup of the partial worktree fails. Before the implement receipt's
// history read, which runs after the HEAD it verifies was read, it can commit
// in git-move-head's worktree, so that worktree's HEAD moves after the
// Controller read it.
func TestGitHelper(t *testing.T) {
	real := os.Getenv("PWC_CC_GIT")
	if real == "" {
		return
	}
	args, ok := pathexec.Args()
	if !ok {
		return
	}
	dir := os.Getenv("PWC_CC_GIT_DIR")
	var env []string
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "GIT_") {
			env = append(env, v)
		}
	}
	line, err := json.Marshal(gitCall{Args: args, Env: env})
	if err != nil {
		os.Exit(2)
	}
	log, err := os.OpenFile(filepath.Join(dir, "git-calls"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	_, _ = log.Write(append(line, '\n'))
	_ = log.Close()
	if _, err := os.Stat(filepath.Join(dir, "git-old")); err == nil && slices.Contains(args, "--no-lazy-fetch") {
		_, _ = fmt.Fprintln(os.Stderr, "unknown option: --no-lazy-fetch")
		os.Exit(129)
	}
	sub := subcommand(args)
	if target, err := os.ReadFile(filepath.Join(dir, "git-move-head")); err == nil && sub == "rev-list" {
		move := exec.Command(real, "-C", string(target), "-c", "core.hooksPath=/dev/null", "commit", "-q", "--allow-empty", "-m", "moved")
		move.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.com", "GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.com")
		if move.Run() != nil {
			os.Exit(2)
		}
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "git-block")); err == nil && string(raw) == sub {
		block(dir)
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "git-fail")); err == nil && string(raw) == sub {
		if target, err := os.ReadFile(filepath.Join(dir, "git-swap")); err == nil {
			if os.Remove("checkout") != nil || os.Symlink(string(target), "checkout") != nil {
				os.Exit(2)
			}
		}
		os.Exit(3)
	}
	if deepReviewGit(args) {
		for i, a := range args {
			if a == "https://github.com/owner/repo.git" {
				args[i] = "file://" + filepath.Join(dir, "origin.git")
			}
		}
		args = append([]string{"-c", "protocol.file.allow=always"}, args...)
	}
	if err := syscall.Exec(real, append([]string{"git"}, args...), os.Environ()); err != nil {
		os.Exit(2)
	}
}

// deepReviewGit reports whether args is a git call on the deep review's own
// repository, which review.Run creates under the deep-review directory.
func deepReviewGit(args []string) bool {
	return slices.ContainsFunc(args, func(a string) bool {
		return strings.HasPrefix(a, "--git-dir=") && strings.HasSuffix(a, string(filepath.Separator)+filepath.Join(deepReviewName, "repository.git"))
	})
}

// block publishes that the command started, then waits for the cancellation
// to kill it.
func block(dir string) {
	if os.WriteFile(filepath.Join(dir, "blocked"), nil, 0600) != nil {
		os.Exit(2)
	}
	time.Sleep(time.Minute)
	os.Exit(2)
}

type gitCall struct {
	Args []string `json:"args"`
	Env  []string `json:"env"`
}

// subcommand skips the global options every Controller call starts with.
func subcommand(args []string) string {
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "-c":
			i++
		case strings.HasPrefix(args[i], "-"):
		default:
			return args[i]
		}
	}
	return ""
}

type fixture struct {
	t                  *testing.T
	dir, launch, piCWD string
	// skills is the private skill directory, with a converge subtree.
	skills                 string
	origin, source, gitBin string
	head, historyBlob      string
}

const fixtureRepo = `{"full_name":"owner/repo","default_branch":"main","permissions":{"push":true}}`

// newFixture serves the target repository's git from a local bare repository
// over file:// and its API from the gh helper; everything else is real.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	gitBin, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if dir, err = filepath.EvalSymlinks(dir); err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, dir: dir, launch: filepath.Join(dir, "launch"), piCWD: filepath.Join(dir, "pi-cwd"), skills: filepath.Join(dir, "skills"), source: filepath.Join(dir, "source"), origin: filepath.Join(dir, "origin.git"), gitBin: gitBin}
	for _, d := range []string{f.launch, f.piCWD, f.source, filepath.Join(dir, "home"), filepath.Join(dir, "xdg"), filepath.Join(dir, "base"), f.skills, filepath.Join(f.skills, convergeSkill)} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	// The user's global git configuration never reaches the fixture.
	t.Setenv("HOME", filepath.Join(dir, "home"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, "xdg"))
	t.Setenv("NODE_TLS_REJECT_UNAUTHORIZED", "1")
	f.sourceGit("init", "--template=", "-b", "main")
	f.write(filepath.Join(f.source, "tracked.txt"), "base\n")
	f.write(filepath.Join(f.source, "history.txt"), "only in history\n")
	f.sourceGit("add", ".")
	f.sourceGit("commit", "-m", "history")
	f.historyBlob = f.sourceGit("rev-parse", "HEAD:history.txt")
	f.sourceGit("rm", "-q", "history.txt")
	f.sourceGit("commit", "-m", "base")
	f.sourceGit("checkout", "-b", "other")
	f.write(filepath.Join(f.source, "tracked.txt"), "other branch\n")
	f.sourceGit("commit", "-am", "other")
	f.sourceGit("checkout", "main")
	f.head = f.sourceGit("rev-parse", "main")
	f.sourceGit("clone", "--bare", "--template=", f.source, f.origin)
	// Without this the local upload-pack ignores the blob filter.
	f.gitIn(f.origin, "config", "uploadpack.allowFilter", "true")
	if err := os.MkdirAll(filepath.Join(f.launch, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	f.write(filepath.Join(f.launch, "docs", "design one.md"), "# Design\n\nexact \x00 bytes\n")
	f.write(filepath.Join(dir, "repo.json"), fixtureRepo)
	f.write(filepath.Join(f.skills, convergeSkill, "SKILL.md"), "# Converge\n\nOne round.\n")
	pathexec.Install(t, "gh", "TestGHHelper")
	t.Setenv("PWC_CC_GH", dir)
	pathexec.Install(t, "git", "TestGitHelper")
	t.Setenv("PWC_CC_GIT", gitBin)
	t.Setenv("PWC_CC_GIT_DIR", dir)
	return f
}

func (f *fixture) write(path, data string) {
	f.t.Helper()
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) sourceGit(args ...string) string {
	f.t.Helper()
	return f.gitIn(f.source, args...)
}

// gitIn runs the real git directly, bypassing the recording wrapper.
func (f *fixture) gitIn(dir string, args ...string) string {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, f.gitBin, append([]string{"-c", "core.hooksPath=/dev/null"}, args...)...)
	cmd.Dir = dir
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "GIT_") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_AUTHOR_NAME=Fixture", "GIT_AUTHOR_EMAIL=fixture@example.com", "GIT_COMMITTER_NAME=Fixture", "GIT_COMMITTER_EMAIL=fixture@example.com")
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("fixture git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (f *fixture) remote(repository string) string {
	if repository != "owner/repo" {
		f.t.Errorf("remote for unvalidated repository %q", repository)
	}
	return "file://" + f.origin
}

// source replaces only the GitHub git remote, allowing the file protocol for it.
func (f *fixture) fileSource() gitSource { return gitSource{remote: f.remote, allowFile: true} }

// prepareOnly is the Controller preparation with only its GitHub git remote
// replaced, ending with the input as Final, so preparation tests need no Pi.
func (f *fixture) prepareOnly(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
	p, err := prepare(ctx, r, in, f.piCWD, f.skills, f.fileSource())
	if err != nil {
		return engine.Result{}, err
	}
	return engine.Result{Outputs: map[string]contract.Ref{"input": p.input}, Final: &engine.FinalSelection{Output: "input"}}, nil
}

// schemaRegistry has this workflow's schemas and review's, which the
// production registry also registers.
func schemaRegistry(t *testing.T) *contract.Registry {
	t.Helper()
	registry, err := contract.NewRegistry(append(Resources(), review.Resources()...), append(Schemas(), review.Schemas()...))
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

type started struct {
	run  *engine.Run
	done chan engine.Report
}

// start runs def through the real engine and Store. The runtime points at a
// missing executable: no Pi can start.
func (f *fixture) start(def engine.Definition, prompt string) started {
	f.t.Helper()
	registry := schemaRegistry(f.t)
	bridge := filepath.Join(f.dir, "bridge")
	if err := os.MkdirAll(bridge, 0700); err != nil {
		f.t.Fatal(err)
	}
	pi, err := runtime.New(runtime.Options{Executable: filepath.Join(f.dir, "no-pi"), BridgeDir: bridge, Policy: def.Policy.Runtime})
	if err != nil {
		f.t.Fatal(err)
	}
	r, err := engine.New(context.Background(), def, engine.Input{Prompt: prompt, LaunchCWD: f.launch}, engine.Options{BaseDir: filepath.Join(f.dir, "base"), Schemas: registry, Runtime: pi, PiVersion: "0.84.3"})
	if err != nil {
		f.t.Fatal(err)
	}
	s := started{run: r, done: make(chan engine.Report, 1)}
	go func() { s.done <- r.Execute() }()
	return s
}

func (s started) wait(t *testing.T) engine.Report {
	t.Helper()
	select {
	case report := <-s.done:
		return report
	case <-time.After(60 * time.Second):
		s.run.Cancel(engine.OriginControllerUser)
		t.Fatal("workflow did not finish")
		return engine.Report{}
	}
}

func (f *fixture) execute(def engine.Definition, prompt string) (*engine.Run, engine.Report) {
	f.t.Helper()
	s := f.start(def, prompt)
	return s.run, s.wait(f.t)
}

func (f *fixture) definition(execute engine.Workflow) engine.Definition {
	def := Definition(f.piCWD, f.skills)
	if execute != nil {
		def.Execute = execute
	}
	return def
}

const prompt = "owner/repo docs/design one.md"

func (f *fixture) ghCalls() []string {
	f.t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.dir, "gh-calls"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
}

func (f *fixture) gitCalls() []gitCall {
	f.t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.dir, "git-calls"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		f.t.Fatal(err)
	}
	var calls []gitCall
	for _, line := range strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n") {
		var call gitCall
		if err := json.Unmarshal([]byte(line), &call); err != nil {
			f.t.Fatal(err)
		}
		calls = append(calls, call)
	}
	return calls
}

func requireFailure(t *testing.T, report engine.Report, want string) {
	t.Helper()
	if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Failure == nil || !strings.Contains(report.Failure.Error(), want) {
		t.Fatalf("outcome %s exit %d failure %v, want Failed with %q", report.Outcome, report.ExitCode, report.Failure, want)
	}
}

// requireUntouched proves a rejection happened before any workflow side
// effect: no worktree directory, no git, no session and no attempt.
func (f *fixture) requireUntouched(t *testing.T, r *engine.Run, report engine.Report) {
	t.Helper()
	if _, err := os.Lstat(filepath.Join(r.Dir(), worktreeDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree directory created: %v", err)
	}
	if calls := f.gitCalls(); len(calls) != 0 {
		t.Fatalf("git ran: %v", calls)
	}
	if len(report.Snapshot.Sessions) != 0 || len(report.Snapshot.Attempts) != 0 || report.Final != nil {
		t.Fatalf("sessions %d attempts %d final %+v", len(report.Snapshot.Sessions), len(report.Snapshot.Attempts), report.Final)
	}
}

func requireMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s retained: %v", path, err)
	}
}

func TestDefinitionPolicy(t *testing.T) {
	def := Definition("/abs", "/skills")
	want := engine.DefaultRunPolicy()
	want.RunTimeout = 8 * time.Hour
	want.MaxLiveSessions, want.MaxTotalSessions, want.MaxTotalAttempts = 3, 22, 80
	want.Runtime.CleanupTimeout = 2 * time.Minute
	if def.Name != "code-change" || def.Version != "1" || !reflect.DeepEqual(def.Policy, want) {
		t.Fatalf("definition: %s %s %+v", def.Name, def.Version, def.Policy)
	}
	// The description names the whole flow, through the convergence rounds
	// with their comment cleanup and the summary.
	if !strings.HasSuffix(def.Description, "having a fresh converger do one round of the private skill and the comment cleanup, until a round claims convergence without any write on an unchanged head or the rounds end unconverged, then take a final snapshot of the pull request state, and deliver a final summary that sets the reports beside what the Controller observed, judging none of them") {
		t.Fatalf("description: %s", def.Description)
	}
	// The production wait and read bounds, round cap, time admission and
	// clock, which await live data.
	if prStatePolicy != (statePolicy{wait: 30 * time.Minute, settle: 3 * time.Minute, poll: 30 * time.Second, reads: 10 * time.Minute}) {
		t.Fatalf("state policy %+v", prStatePolicy)
	}
	if convergeRounds.rounds != 10 || convergeRounds.reserve != 150*time.Minute || reflect.ValueOf(convergeRounds.now).Pointer() != reflect.ValueOf(time.Now).Pointer() {
		t.Fatalf("round policy %+v", convergeRounds)
	}
	if SkillsDirEnv != "PWC_CODE_CHANGE_SKILLS_DIR" {
		t.Fatalf("skills env %s", SkillsDirEnv)
	}
	if _, err := engine.NewRegistry([]engine.Definition{def}); err != nil {
		t.Fatal(err)
	}
	if got := githubRemote("o/r"); got != "https://github.com/o/r.git" {
		t.Fatalf("githubRemote: %s", got)
	}
}

func TestParsePrompt(t *testing.T) {
	for _, tc := range []struct {
		name, prompt, repository, path string
	}{
		{"plain", "owner/repo design.md", "owner/repo", "design.md"},
		{"surrounding-and-inner-whitespace", " \towner/repo \t /abs/design.md  ", "owner/repo", "/abs/design.md"},
		{"path-with-spaces", "Owner-1/re.po_2 docs/my design doc.md", "Owner-1/re.po_2", "docs/my design doc.md"},
		{"tilde-is-literal", "owner/repo ~/design.md", "owner/repo", "~/design.md"},
		{"only-repository", "owner/repo", "", ""},
		{"only-repository-padded", "  owner/repo  ", "", ""},
		{"empty", "", "", ""},
		{"no-name", "owner/ design.md", "", ""},
		{"no-owner", "/repo design.md", "", ""},
		{"dot-name", "owner/. design.md", "", ""},
		{"dot-dot-name", "owner/.. design.md", "", ""},
		{"extra-segment", "owner/repo/x design.md", "", ""},
		{"option-owner", "-owner/repo design.md", "", ""},
		{"url", "https://github.com/owner/repo design.md", "", ""},
		{"no-slash", "repo design.md", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repository, path, err := parsePrompt(tc.prompt)
			if repository != tc.repository || path != tc.path || (err == nil) != (tc.repository != "") {
				t.Fatalf("parsePrompt(%q) = %q, %q, %v", tc.prompt, repository, path, err)
			}
		})
	}
}

// The production Execute rejects in a fixed order: prompt, PWC_PI_CWD, TLS,
// design doc, private skill directory, repository. Each case breaks every
// later check too, so only the earliest may report; the skill directory is
// unset unless a case sets it.
func TestRejectionOrderBeforeSideEffects(t *testing.T) {
	const noPush = `{"full_name":"owner/repo","default_branch":"main","permissions":{"push":false}}`
	piCWD := func(f *fixture) string { return f.piCWD }
	for _, tc := range []struct {
		name, prompt, tls, want string
		piCWD                   func(f *fixture) string
		get                     bool
		skills                  func(f *fixture) string
	}{
		{"prompt", "owner/repo", "0", "code-change prompt must be", func(*fixture) string { return "" }, false, nil},
		{"pi-cwd-unset", "owner/repo missing.md", "0", `requires PWC_PI_CWD to be an absolute path, got ""`, func(*fixture) string { return "" }, false, nil},
		{"pi-cwd-relative", "owner/repo missing.md", "0", `requires PWC_PI_CWD to be an absolute path, got "pi-cwd"`, func(*fixture) string { return "pi-cwd" }, false, nil},
		{"pi-cwd-missing", "owner/repo missing.md", "0", "code-change PWC_PI_CWD: stat ", func(f *fixture) string { return filepath.Join(f.dir, "absent") }, false, nil},
		{"pi-cwd-file", "owner/repo missing.md", "0", "requires PWC_PI_CWD to be a directory", func(f *fixture) string { return filepath.Join(f.dir, "repo.json") }, false, nil},
		{"pi-cwd-symlink-followed", "owner/repo missing.md", "0", "TLS preflight blocked", func(f *fixture) string {
			link := filepath.Join(f.dir, "pi-cwd-link")
			if err := os.Symlink(f.piCWD, link); err != nil {
				f.t.Fatal(err)
			}
			return link
		}, false, nil},
		{"tls", "owner/repo missing.md", "0", "TLS preflight blocked", piCWD, false, nil},
		{"design-doc", "owner/repo missing.md", "1", `design doc "missing.md"`, piCWD, false, nil},
		{"skills-unset", prompt, "1", `code-change PWC_CODE_CHANGE_SKILLS_DIR: skill directory must be an absolute path, got ""`, piCWD, false, nil},
		{"skills-relative", prompt, "1", `code-change PWC_CODE_CHANGE_SKILLS_DIR: skill directory must be an absolute path, got "skills"`, piCWD, false, func(*fixture) string { return "skills" }},
		{"repository", prompt, "1", "no push permission", piCWD, true, func(f *fixture) string { return f.skills }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.write(filepath.Join(f.dir, "repo.json"), noPush)
			t.Setenv("NODE_TLS_REJECT_UNAUTHORIZED", tc.tls)
			skills := ""
			if tc.skills != nil {
				skills = tc.skills(f)
			}
			r, report := f.execute(Definition(tc.piCWD(f), skills), tc.prompt)
			requireFailure(t, report, tc.want)
			f.requireUntouched(t, r, report)
			requireMissing(t, filepath.Join(r.Dir(), skillsExpansion))
			if got := len(f.ghCalls()); got != map[bool]int{false: 0, true: 1}[tc.get] {
				t.Fatalf("gh calls: %d", got)
			}
		})
	}
}

// A git without --no-lazy-fetch, which the receipt reads need, is rejected
// after the repository GET and before any worktree, session or attempt.
func TestGitWithoutNoLazyFetchRejected(t *testing.T) {
	f := newFixture(t)
	f.write(filepath.Join(f.dir, "git-old"), "")
	r, report := f.execute(Definition(f.piCWD, f.skills), prompt)
	requireFailure(t, report, "code-change needs a git that supports --no-lazy-fetch (2.45 or newer)")
	if !errors.Is(report.Failure, errGitNoLazyFetch) {
		t.Fatalf("failure %v, want errGitNoLazyFetch", report.Failure)
	}
	if calls := f.gitCalls(); len(calls) != 1 || subcommand(calls[0].Args) != "version" {
		t.Fatalf("git calls %q, want only the capability check", calls)
	}
	if got := len(f.ghCalls()); got != 1 {
		t.Fatalf("gh calls: %d", got)
	}
	if _, err := os.Lstat(filepath.Join(r.Dir(), worktreeDir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worktree directory created: %v", err)
	}
	if len(report.Snapshot.Sessions) != 0 || len(report.Snapshot.Attempts) != 0 || report.Final != nil {
		t.Fatalf("sessions %d attempts %d final %+v", len(report.Snapshot.Sessions), len(report.Snapshot.Attempts), report.Final)
	}
}

func TestDesignDocRejections(t *testing.T) {
	for _, tc := range []struct {
		name, path, want string
		setup            func(f *fixture)
	}{
		{"missing", "docs/missing.md", "no such file or directory", nil},
		{"directory", "docs", "path must contain only directories and a regular file, without symlinks", nil},
		{"root", "/", "not a file path", nil},
		{"final-symlink", "docs/link.md", "path must contain only directories and a regular file, without symlinks", func(f *fixture) {
			if err := os.Symlink(filepath.Join(f.launch, "docs", "design one.md"), filepath.Join(f.launch, "docs", "link.md")); err != nil {
				f.t.Fatal(err)
			}
		}},
		{"over-limit", "docs/big.md", "file exceeds byte limit", func(f *fixture) {
			f.write(filepath.Join(f.launch, "docs", "big.md"), strings.Repeat("x", designDocLimit+1))
		}},
		{"fifo", "docs/fifo", "path must contain only directories and a regular file, without symlinks", func(f *fixture) {
			if err := syscall.Mkfifo(filepath.Join(f.launch, "docs", "fifo"), 0600); err != nil {
				f.t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			if tc.setup != nil {
				tc.setup(f)
			}
			r, report := f.execute(f.definition(nil), "owner/repo "+tc.path)
			requireFailure(t, report, fmt.Sprintf("design doc %q: ", tc.path))
			if !strings.Contains(report.Failure.Error(), tc.want) {
				t.Fatalf("failure %v, want %q", report.Failure, tc.want)
			}
			f.requireUntouched(t, r, report)
			if calls := f.ghCalls(); len(calls) != 0 {
				t.Fatalf("GET before design doc rejection: %v", calls)
			}
		})
	}
}

func TestReadDesignDocResolution(t *testing.T) {
	dir := t.TempDir()
	write := func(path, data string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	launch := filepath.Join(dir, "launch")
	write(filepath.Join(launch, "~", "doc.md"), "literal tilde")
	write(filepath.Join(dir, "real", "doc.md"), "through a linked directory")
	write(filepath.Join(launch, "limit.md"), strings.Repeat("x", designDocLimit))
	if err := os.Symlink(filepath.Join(dir, "real"), filepath.Join(launch, "linked")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path, resolved, data string
	}{
		{"relative-to-launch", "~/doc.md", filepath.Join(launch, "~", "doc.md"), "literal tilde"},
		{"absolute", filepath.Join(dir, "real", "doc.md"), filepath.Join(dir, "real", "doc.md"), "through a linked directory"},
		{"cleaned", "./x/../linked/doc.md", filepath.Join(launch, "linked", "doc.md"), "through a linked directory"},
		{"at-limit", "limit.md", filepath.Join(launch, "limit.md"), strings.Repeat("x", designDocLimit)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := readDesignDoc(context.Background(), launch, tc.path)
			if err != nil || doc.resolved != tc.resolved || string(doc.data) != tc.data {
				t.Fatalf("readDesignDoc(%q) = %q %d bytes %v", tc.path, doc.resolved, len(doc.data), err)
			}
		})
	}
}

func TestRepositoryMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, repo, want string
	}{
		{"case-only-difference-accepted", `{"full_name":"Owner/Repo","default_branch":"main","permissions":{"push":true}}`, ""},
		{"push-false", `{"full_name":"owner/repo","default_branch":"main","permissions":{"push":false}}`, `the authenticated identity has no push permission on "owner/repo"`},
		{"push-missing", `{"full_name":"owner/repo","default_branch":"main","permissions":{"pull":true}}`, `has no permissions.push field`},
		{"permissions-missing", `{"full_name":"owner/repo","default_branch":"main"}`, `has no permissions.push field`},
		{"push-null", `{"full_name":"owner/repo","default_branch":"main","permissions":{"push":null}}`, `has no permissions.push field`},
		{"renamed", `{"full_name":"owner/renamed","default_branch":"main","permissions":{"push":true}}`, `full_name is "owner/renamed", want "owner/repo"`},
		{"transferred", `{"full_name":"other/repo","default_branch":"main","permissions":{"push":true}}`, `full_name is "other/repo", want "owner/repo"`},
		{"full-name-missing", `{"default_branch":"main","permissions":{"push":true}}`, `full_name is "", want "owner/repo"`},
		{"not-json", `not json`, "repository metadata is not a JSON object"},
		{"array", `[]`, "repository metadata is not a JSON object"},
		{"default-branch-missing", `{"full_name":"owner/repo","permissions":{"push":true}}`, `has no default_branch`},
		{"gh-failure", "", "repository metadata: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			if tc.repo == "" {
				if err := os.Remove(filepath.Join(f.dir, "repo.json")); err != nil {
					t.Fatal(err)
				}
			} else {
				f.write(filepath.Join(f.dir, "repo.json"), tc.repo)
			}
			r, report := f.execute(f.definition(f.prepareOnly), prompt)
			if calls := f.ghCalls(); len(calls) != 1 {
				t.Fatalf("gh calls: %v", calls)
			}
			if tc.want == "" {
				if report.Outcome != engine.Succeeded {
					t.Fatalf("outcome %s: %v", report.Outcome, report.Failure)
				}
				return
			}
			want := tc.want
			if tc.name == "gh-failure" {
				want = commandFailure(report, want)
			}
			requireFailure(t, report, want)
			f.requireUntouched(t, r, report)
		})
	}
}

// commandFailure is prefix and the runtime's error for a command that exited
// non-zero. Darwin reports EPERM for a group holding only its zombie leader.
func commandFailure(report engine.Report, prefix string) string {
	if errors.Is(report.Failure, syscall.EPERM) {
		return prefix + "acquisition process group cleanup failed"
	}
	return prefix + "acquisition command failed"
}

// controllerPolicy is this workflow's own git policy on every Controller call,
// written out literally; fixture runs append the file protocol allowance.
func controllerPolicy(repoDir string) []string {
	return []string{
		"--no-pager", "--git-dir=" + repoDir, "--attr-source=4b825dc642cb6eb9a060e54bf8d69288fbee4904",
		"-c", "core.hooksPath=/dev/null", "-c", "core.attributesFile=/dev/null",
		"-c", "core.autocrlf=false", "-c", "core.sparseCheckout=false",
		"-c", "http.sslVerify=true", "-c", "http.followRedirects=false",
		"-c", "protocol.allow=never", "-c", "protocol.https.allow=always",
		"-c", "submodule.recurse=false",
	}
}

var fileProtocol = []string{"-c", "protocol.file.allow=always"}

// requirePolicy checks that the first Controller git call is the capability
// check and every later one carries exactly the workflow policy plus extra
// before its subcommand, and that all have the environment that disables
// system and global configuration.
func (f *fixture) requirePolicy(t *testing.T, repoDir string, extra []string) {
	t.Helper()
	calls := f.gitCalls()
	if len(calls) < 2 {
		t.Fatalf("Controller git calls %q, want the capability check and more", calls)
	}
	want := append(controllerPolicy(repoDir), extra...)
	for n, call := range calls {
		if n == 0 {
			if !slices.Equal(call.Args[len(call.Args)-2:], []string{"--no-lazy-fetch", "version"}) || subcommand(call.Args) != "version" {
				t.Fatalf("first git call %q, want the --no-lazy-fetch capability check", call.Args)
			}
		} else if i := slices.Index(call.Args, "--no-pager"); i < 0 || len(call.Args) <= i+len(want) || !reflect.DeepEqual(call.Args[i:i+len(want)], want) || call.Args[i+len(want)] == "-c" {
			t.Fatalf("git call %q, want the policy %q then the subcommand", call.Args, want)
		}
		for _, env := range []string{"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_ATTR_NOSYSTEM=1"} {
			if !slices.Contains(call.Env, env) {
				t.Fatalf("git call %q lacks %s: %q", call.Args, env, call.Env)
			}
		}
	}
}

func TestSuccessRecordsInputAndKeepsWorktree(t *testing.T) {
	f := newFixture(t)
	var record contract.Publication[inputRecord]
	var evidence []byte
	def := f.definition(func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
		result, err := f.prepareOnly(ctx, r, in)
		if err != nil {
			return result, err
		}
		ref := result.Outputs["input"]
		raw, err := engine.ReadContract(ctx, r, ref)
		if err != nil {
			return result, err
		}
		if record, err = contract.DecodePublication[inputRecord](raw); err != nil {
			return result, err
		}
		if len(record.Files) == 1 {
			evidence, err = os.ReadFile(filepath.Join(filepath.Dir(ref.Path), record.Files[0].Path))
		}
		return result, err
	})
	r, report := f.execute(def, " owner/repo\t docs/design one.md ")
	if report.Outcome != engine.Succeeded || report.ExitCode != 0 || report.Failure != nil || len(report.CleanupErrors) != 0 {
		t.Fatalf("outcome %s exit %d failure %v cleanup %v", report.Outcome, report.ExitCode, report.Failure, report.CleanupErrors)
	}
	root := filepath.Join(r.Dir(), worktreeDir)
	repoDir, checkout := filepath.Join(root, "repository.git"), filepath.Join(root, "checkout")
	want := inputRecord{Repository: "owner/repo", DefaultBranch: "main", BaseSHA: f.head, Worktree: checkout,
		DesignDoc: designDocRecord{Path: "docs/design one.md", Resolved: filepath.Join(f.launch, "docs", "design one.md")}}
	if record.Data != want {
		t.Fatalf("input record: %+v, want %+v", record.Data, want)
	}
	if !reflect.DeepEqual(record.Files, []contract.FileEntry{{ID: "design-doc", Kind: "evidence", Path: "evidence/design-doc"}}) || string(evidence) != "# Design\n\nexact \x00 bytes\n" {
		t.Fatalf("design doc evidence: %+v %q", record.Files, evidence)
	}
	if report.Final == nil || report.Final.Output != "input" || report.Final.Ref != report.Result.Outputs["input"] || report.Final.Step != "input" {
		t.Fatalf("final: %+v", report.Final)
	}
	// Two Controller attempts, the input and the skills, and no session.
	if len(report.Snapshot.Sessions) != 0 || len(report.Snapshot.Attempts) != 2 {
		t.Fatalf("sessions %d attempts %d", len(report.Snapshot.Sessions), len(report.Snapshot.Attempts))
	}
	for _, attempt := range report.Snapshot.Attempts {
		if !attempt.Controller || (attempt.Key != "input" && attempt.Key != "skills") {
			t.Fatalf("attempt: %+v", attempt)
		}
	}
	// Kept: detached at the recorded base, with a normal clone's tracking
	// configuration.
	if got := f.gitIn(checkout, "rev-parse", "HEAD"); got != record.Data.BaseSHA {
		t.Fatalf("worktree HEAD %s, want base_sha %s", got, record.Data.BaseSHA)
	}
	if got := f.gitIn(checkout, "rev-parse", "--abbrev-ref", "HEAD"); got != "HEAD" {
		t.Fatalf("worktree not detached: %s", got)
	}
	if raw, err := os.ReadFile(filepath.Join(checkout, "tracked.txt")); err != nil || string(raw) != "base\n" {
		t.Fatalf("checkout content: %q %v", raw, err)
	}
	if got := f.gitIn(checkout, "symbolic-ref", "refs/remotes/origin/HEAD"); got != "refs/remotes/origin/main" {
		t.Fatalf("origin/HEAD: %s", got)
	}
	if got := f.gitIn(checkout, "config", "--get-all", "remote.origin.fetch"); got != "+refs/heads/*:refs/remotes/origin/*" {
		t.Fatalf("fetch refspec: %q", got)
	}
	// The fetch was blob-less: a blob only in history was never transferred.
	if got := f.gitIn(root, "--git-dir="+repoDir, "rev-list", "--objects", "--missing=print", "--all"); !slices.Contains(strings.Split(got, "\n"), "?"+f.historyBlob) {
		t.Fatalf("history blob %s transferred:\n%s", f.historyBlob, got)
	}
	if _, err := os.Lstat(filepath.Join(repoDir, "hooks")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hooks directory installed: %v", err)
	}
	// Nothing persistent changes how the Agent's git later uses the user's
	// global configuration or credentials.
	config := strings.ToLower(f.gitIn(checkout, "config", "--local", "--list"))
	for _, key := range []string{"credential", "url.", "include", "hookspath", "pushurl", "user.", "insteadof"} {
		if strings.Contains(config, key) {
			t.Fatalf("repository config contains %q:\n%s", key, config)
		}
	}
	f.requirePolicy(t, repoDir, fileProtocol)
}

// The production Definition reaches GitHub over https only. The fetch fails
// at the git boundary, so nothing reaches the network.
func TestProductionGitPolicy(t *testing.T) {
	f := newFixture(t)
	f.write(filepath.Join(f.dir, "git-fail"), "fetch")
	r, report := f.execute(Definition(f.piCWD, f.skills), prompt)
	requireFailure(t, report, commandFailure(report, `git fetch "main": `))
	repoDir := filepath.Join(r.Dir(), worktreeDir, "repository.git")
	f.requirePolicy(t, repoDir, nil)
	if got := f.gitIn(f.dir, "--git-dir="+repoDir, "config", "--get", "remote.origin.url"); got != "https://github.com/owner/repo.git" {
		t.Fatalf("remote url: %s", got)
	}
}

// Hooks, filters and attributes from the user's global configuration never
// apply during worktree creation. eol=crlf in the global attributes file would
// change the checkout bytes if that file were read.
func TestGlobalGitConfigurationIgnored(t *testing.T) {
	f := newFixture(t)
	hooks := filepath.Join(f.dir, "hooks")
	sentinel := filepath.Join(f.dir, "hook-ran")
	if err := os.Mkdir(hooks, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"post-checkout", "reference-transaction"} {
		if err := os.WriteFile(filepath.Join(hooks, name), []byte("#!/bin/sh\nprintf ran > '"+sentinel+"'\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	global := "[core]\n\thooksPath = " + hooks + "\n[filter \"hostile\"]\n\tsmudge = " + filepath.Join(hooks, "post-checkout") + "\n\trequired = true\n"
	f.write(filepath.Join(f.dir, "home", ".gitconfig"), global)
	if err := os.MkdirAll(filepath.Join(f.dir, "xdg", "git"), 0700); err != nil {
		t.Fatal(err)
	}
	f.write(filepath.Join(f.dir, "xdg", "git", "config"), global)
	f.write(filepath.Join(f.dir, "xdg", "git", "attributes"), "* filter=hostile eol=crlf\n")
	r, report := f.execute(f.definition(f.prepareOnly), prompt)
	if report.Outcome != engine.Succeeded {
		t.Fatalf("outcome %s: %v", report.Outcome, report.Failure)
	}
	if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("global hook or filter ran: %v", err)
	}
	if raw, err := os.ReadFile(filepath.Join(r.Dir(), worktreeDir, "checkout", "tracked.txt")); err != nil || string(raw) != "base\n" {
		t.Fatalf("checkout content: %q %v", raw, err)
	}
}

// writeAttributes commits files whose checkout bytes a .gitattributes in the
// same tree would change in dir, then that .gitattributes, so the files were
// added without it, and returns the files' committed bytes.
func (f *fixture) writeAttributes(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := map[string]string{"crlf.txt": "a\nb\n", "ident.txt": "$Id$\n", "encoding.txt": "plain\n"}
	for name, data := range files {
		f.write(filepath.Join(dir, name), data)
	}
	f.gitIn(dir, "add", "crlf.txt", "ident.txt", "encoding.txt")
	f.gitIn(dir, "commit", "-qm", "files")
	f.write(filepath.Join(dir, ".gitattributes"), "crlf.txt text eol=crlf\nident.txt ident\nencoding.txt working-tree-encoding=UTF-16LE\n")
	f.gitIn(dir, "add", ".gitattributes")
	f.gitIn(dir, "commit", "-qm", "attributes")
	return files
}

func requireBytes(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, want := range files {
		if raw, err := os.ReadFile(filepath.Join(dir, name)); err != nil || string(raw) != want {
			t.Fatalf("%s checked out as %q (%v), want the committed %q", name, raw, err, want)
		}
	}
}

// A .gitattributes in the fetched tree does not change the bytes the
// Controller checks out: no line ending or encoding conversion and no ident
// expansion.
func TestRepositoryAttributesIgnored(t *testing.T) {
	f := newFixture(t)
	files := f.writeAttributes(t, f.source)
	f.gitIn(f.origin, "fetch", "-q", f.source, "main:main")
	r, report := f.execute(f.definition(f.prepareOnly), prompt)
	if report.Outcome != engine.Succeeded {
		t.Fatalf("outcome %s: %v", report.Outcome, report.Failure)
	}
	requireBytes(t, filepath.Join(r.Dir(), worktreeDir, "checkout"), files)
}

// Before the worktree is registered no Agent has seen it, so a failure
// removes the partial worktree and keeps the repository history.
func TestPartialWorktreeFailureRemovesCheckout(t *testing.T) {
	for _, tc := range []struct {
		name, branch, fail, want string
		lastCall                 string
		source                   func(f *fixture) gitSource
	}{
		{"missing-branch", "absent", "", `git fetch "absent": acquisition command failed`, "fetch", nil},
		// protocol.allow=never refuses the file transport the fixture serves.
		{"file-protocol-not-allowed", "main", "", `git fetch "main": acquisition command failed`, "fetch", func(f *fixture) gitSource { return gitSource{remote: f.remote} }},
		{"worktree-add", "main", "worktree", "git worktree: acquisition command failed", "worktree", nil},
		{"init", "main", "init", "git init: acquisition command failed", "init", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.write(filepath.Join(f.dir, "repo.json"), `{"full_name":"owner/repo","default_branch":"`+tc.branch+`","permissions":{"push":true}}`)
			if tc.fail != "" {
				f.write(filepath.Join(f.dir, "git-fail"), tc.fail)
			}
			source := f.fileSource()
			if tc.source != nil {
				source = tc.source(f)
			}
			r, report := f.execute(f.definition(func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
				return execute(ctx, r, in, f.piCWD, f.skills, source, testState, convergeRounds)
			}), prompt)
			want := tc.want
			if errors.Is(report.Failure, syscall.EPERM) {
				want = "acquisition process group cleanup failed"
			}
			requireFailure(t, report, want)
			root := filepath.Join(r.Dir(), worktreeDir)
			requireMissing(t, filepath.Join(root, "checkout"))
			requireMissing(t, filepath.Join(root, "repository.git", "worktrees", "checkout"))
			if info, err := os.Stat(filepath.Join(root, "repository.git")); err != nil || !info.IsDir() {
				t.Fatalf("repository history removed: %v", err)
			}
			calls := f.gitCalls()
			if len(calls) == 0 || subcommand(calls[len(calls)-1].Args) != tc.lastCall {
				t.Fatalf("git stopped after %q, want %s", calls, tc.lastCall)
			}
			if len(report.Snapshot.Attempts) != 0 || len(report.Snapshot.Sessions) != 0 || len(report.CleanupErrors) != 0 {
				t.Fatalf("attempts %d sessions %d cleanup %v", len(report.Snapshot.Attempts), len(report.Snapshot.Sessions), report.CleanupErrors)
			}
		})
	}
}

// A partial worktree that cannot be removed is reported with the failure,
// and cleanup never follows a replaced checkout.
func TestPartialWorktreeCleanupFailureReported(t *testing.T) {
	f := newFixture(t)
	outside := filepath.Join(f.dir, "outside")
	if err := os.Mkdir(outside, 0700); err != nil {
		t.Fatal(err)
	}
	f.write(filepath.Join(outside, "foreign"), "keep")
	f.write(filepath.Join(f.dir, "git-swap"), outside)
	f.write(filepath.Join(f.dir, "git-fail"), "worktree")
	r, report := f.execute(f.definition(f.prepareOnly), prompt)
	requireFailure(t, report, "git worktree: ")
	if !strings.Contains(report.Failure.Error(), "worktree cleanup: acquisition cleanup ownership mismatch") {
		t.Fatalf("cleanup failure not reported: %v", report.Failure)
	}
	if raw, err := os.ReadFile(filepath.Join(outside, "foreign")); err != nil || string(raw) != "keep" {
		t.Fatalf("cleanup followed the replaced checkout: %q %v", raw, err)
	}
	if target, err := os.Readlink(filepath.Join(r.Dir(), worktreeDir, "checkout")); err != nil || target != outside {
		t.Fatalf("replaced checkout: %q %v", target, err)
	}
}

// A failed registration leaves ownership with the workflow, which removes the
// worktree it has not handed to anyone.
func TestCleanupRegistrationFailureRemovesCheckout(t *testing.T) {
	f := newFixture(t)
	other := false
	r, report := f.execute(f.definition(func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
		if err := r.AddCleanup(ctx, worktreeResource, func(context.Context) error { other = true; return nil }); err != nil {
			return engine.Result{}, err
		}
		return f.prepareOnly(ctx, r, in)
	}), prompt)
	requireFailure(t, report, "duplicate resource name")
	root := filepath.Join(r.Dir(), worktreeDir)
	requireMissing(t, filepath.Join(root, "checkout"))
	requireMissing(t, filepath.Join(root, "repository.git", "worktrees", "checkout"))
	if !other || len(report.Snapshot.Attempts) != 0 {
		t.Fatalf("pre-registered cleanup ran %v, attempts %d", other, len(report.Snapshot.Attempts))
	}
}

// requireKept checks the run kept the worktree's checkout and registration.
func requireKept(t *testing.T, r *engine.Run) {
	t.Helper()
	root := filepath.Join(r.Dir(), worktreeDir)
	if _, err := os.Stat(filepath.Join(root, "checkout", "tracked.txt")); err != nil {
		t.Fatalf("worktree not kept: %v", err)
	}
	if info, err := os.Stat(filepath.Join(root, "repository.git", "worktrees", "checkout")); err != nil || !info.IsDir() {
		t.Fatalf("worktree registration not kept: %v", err)
	}
}

// After registration the worktree is kept however the run ends, unless the
// workflow set remove.
func TestRegisteredWorktreeKeptUnlessRemoveSet(t *testing.T) {
	t.Run("failure-after-registration", func(t *testing.T) {
		f := newFixture(t)
		r, report := f.execute(f.definition(func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
			// Taking the key makes the input Attach fail after registration.
			if _, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "input", Output: contract.Spec{SchemaID: InputSchema},
				Data: inputRecord{Repository: "owner/repo", DefaultBranch: "main", BaseSHA: strings.Repeat("a", 40), Worktree: "/w", DesignDoc: designDocRecord{Path: "p", Resolved: "/p"}}}); err != nil {
				return engine.Result{}, err
			}
			return f.prepareOnly(ctx, r, in)
		}), prompt)
		requireFailure(t, report, "invalid key or schema")
		if len(report.CleanupErrors) != 0 {
			t.Fatal(report.CleanupErrors)
		}
		requireKept(t, r)
	})
	t.Run("cancelled-after-registration", func(t *testing.T) {
		f := newFixture(t)
		r, report := f.execute(f.definition(func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
			result, err := f.prepareOnly(ctx, r, in)
			if err != nil {
				return result, err
			}
			r.Cancel(engine.OriginControllerUser)
			return result, context.Cause(ctx)
		}), prompt)
		if report.Outcome != engine.CancelledState || len(report.CleanupErrors) != 0 {
			t.Fatalf("outcome %s cleanup %v", report.Outcome, report.CleanupErrors)
		}
		requireKept(t, r)
	})
	t.Run("remove-set", func(t *testing.T) {
		f := newFixture(t)
		var input contract.Ref
		r, report := f.execute(f.definition(func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
			p, err := prepare(ctx, r, in, f.piCWD, f.skills, f.fileSource())
			if err != nil {
				return engine.Result{}, err
			}
			p.worktree.remove.Store(true)
			input = p.input
			return engine.Result{Outputs: map[string]contract.Ref{"input": p.input}, Final: &engine.FinalSelection{Output: "input"}}, nil
		}), prompt)
		if report.Outcome != engine.Succeeded || len(report.CleanupErrors) != 0 {
			t.Fatalf("outcome %s %v cleanup %v", report.Outcome, report.Failure, report.CleanupErrors)
		}
		root := filepath.Join(r.Dir(), worktreeDir)
		requireMissing(t, filepath.Join(root, "checkout"))
		requireMissing(t, filepath.Join(root, "repository.git", "worktrees", "checkout"))
		if got := f.gitIn(root, "--git-dir="+filepath.Join(root, "repository.git"), "rev-parse", "refs/remotes/origin/main"); got != f.head {
			t.Fatalf("repository history lost: %s", got)
		}
		if _, err := os.Stat(input.Path); err != nil {
			t.Fatalf("input record removed: %v", err)
		}
	})
}

func waitBlocked(t *testing.T, path string, done <-chan engine.Report) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case report := <-done:
			t.Fatalf("workflow ended before the barrier: %s %v", report.Outcome, report.Failure)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("command did not reach the barrier")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Cancellation reaches the running command; a partial worktree is removed.
func TestCancellationDuringExternalCommands(t *testing.T) {
	for _, tc := range []string{"get", "fetch", "worktree"} {
		t.Run(tc, func(t *testing.T) {
			f := newFixture(t)
			if tc == "get" {
				f.write(filepath.Join(f.dir, "gh-block"), "")
			} else {
				f.write(filepath.Join(f.dir, "git-block"), tc)
			}
			s := f.start(f.definition(f.prepareOnly), prompt)
			waitBlocked(t, filepath.Join(f.dir, "blocked"), s.done)
			cancelled := time.Now()
			s.run.Cancel(engine.OriginControllerUser)
			report := s.wait(t)
			// The blocked command sleeps a minute unless the cancellation kills it.
			if elapsed := time.Since(cancelled); elapsed > 30*time.Second {
				t.Fatalf("cancellation did not reach the command: returned after %s", elapsed)
			}
			if report.Outcome != engine.CancelledState || report.ExitCode != 130 || report.Final != nil || len(report.CleanupErrors) != 0 {
				t.Fatalf("outcome %s exit %d final %+v cleanup %v", report.Outcome, report.ExitCode, report.Final, report.CleanupErrors)
			}
			if tc == "get" {
				f.requireUntouched(t, s.run, report)
				return
			}
			root := filepath.Join(s.run.Dir(), worktreeDir)
			requireMissing(t, filepath.Join(root, "checkout"))
			requireMissing(t, filepath.Join(root, "repository.git", "worktrees", "checkout"))
			if calls := f.gitCalls(); subcommand(calls[len(calls)-1].Args) != tc {
				t.Fatalf("git stopped after %q, want %s", calls, tc)
			}
			if len(report.Snapshot.Attempts) != 0 {
				t.Fatalf("attempts: %d", len(report.Snapshot.Attempts))
			}
		})
	}
}
