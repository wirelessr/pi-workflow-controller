package github

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pi-workflow-controller/internal/testutil/pathexec"
)

// TestGHHelper is the GitHub API boundary: the gh PATH wrapper runs it in a
// real subprocess that records the endpoint it asked for, then fails when
// gh-fail exists and otherwise writes gh-body.
func TestGHHelper(t *testing.T) {
	dir := os.Getenv("PWC_GITHUB_GH")
	if dir == "" {
		return
	}
	args, ok := pathexec.Args()
	if !ok || len(args) == 0 {
		return
	}
	calls, err := os.OpenFile(filepath.Join(dir, "gh-calls"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		os.Exit(2)
	}
	_, _ = fmt.Fprintln(calls, args[len(args)-1])
	_ = calls.Close()
	if _, err := os.Stat(filepath.Join(dir, "gh-fail")); err == nil {
		os.Exit(1)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "gh-body"))
	if err != nil {
		os.Exit(2)
	}
	_, _ = os.Stdout.Write(raw)
	os.Exit(0)
}

func TestAncestor(t *testing.T) {
	commit, head, other := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	compare := func(status, base string) string {
		return `{"status":"` + status + `","merge_base_commit":{"sha":"` + base + `"},"commits":[]}`
	}
	for _, tc := range []struct {
		name, commit, head string
		body               string // "" makes gh fail
		cancelled          bool
		ancestor           bool
		status             string
		err                string // a prefix of the error, "" for none
	}{
		{name: "identical-without-query", commit: commit, head: commit, ancestor: true, status: "identical"},
		{name: "ahead-from-commit", commit: commit, head: head, body: compare("ahead", commit), ancestor: true, status: "ahead"},
		{name: "ahead-with-another-merge-base", commit: commit, head: head, body: compare("ahead", other), status: "ahead"},
		{name: "ahead-without-merge-base", commit: commit, head: head, body: `{"status":"ahead"}`, status: "ahead"},
		{name: "behind", commit: commit, head: head, body: compare("behind", head), status: "behind"},
		{name: "diverged", commit: commit, head: head, body: compare("diverged", other), status: "diverged"},
		{name: "identical-status-for-different-SHAs", commit: commit, head: head, body: compare("identical", commit), status: "identical"},
		{name: "unknown-status", commit: commit, head: head, body: compare("unknown", commit), err: `compare GET: status "unknown" is not ahead, behind, diverged or identical`},
		{name: "malformed-JSON", commit: commit, head: head, body: `{`, err: "compare GET: not a compare object"},
		{name: "not-an-object", commit: commit, head: head, body: `[]`, err: "compare GET: not a compare object"},
		// The runtime's message for a failed gh varies with how its process
		// group ends; only the wrapping is this package's.
		{name: "gh-failure", commit: commit, head: head, err: "compare GET: "},
		{name: "cancelled", commit: commit, head: head, body: compare("ahead", commit), cancelled: true, err: "compare GET: "},
		{name: "uppercase-commit", commit: strings.ToUpper(commit), head: head, err: fmt.Sprintf("compare: %q is not a full lowercase commit SHA", strings.ToUpper(commit))},
		{name: "path-in-head", commit: commit, head: "../" + head[:37], err: fmt.Sprintf("compare: %q is not a full lowercase commit SHA", "../"+head[:37])},
		{name: "equal-but-invalid", commit: "HEAD", head: "HEAD", err: `compare: "HEAD" is not a full lowercase commit SHA`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			pathexec.Install(t, "gh", "TestGHHelper")
			t.Setenv("PWC_GITHUB_GH", dir)
			if tc.body == "" {
				if err := os.WriteFile(filepath.Join(dir, "gh-fail"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(filepath.Join(dir, "gh-body"), []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			ancestor, status, err := Ancestor(ctx, dir, "owner/repo", tc.commit, tc.head)
			if (err == nil) != (tc.err == "") || err != nil && !strings.HasPrefix(err.Error(), tc.err) || ancestor != tc.ancestor || status != tc.status {
				t.Fatalf("Ancestor = %t, %q, %v; want %t, %q, %q", ancestor, status, err, tc.ancestor, tc.status, tc.err)
			}
			if tc.cancelled && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation cause lost: %v", err)
			}
			calls := ghCalls(t, dir)
			if (tc.commit == tc.head || tc.cancelled) && calls != nil {
				t.Fatalf("gh started: %q", calls)
			}
			if len(calls) > 1 || len(calls) == 1 && calls[0] != "repos/owner/repo/compare/"+tc.commit+"..."+tc.head {
				t.Fatalf("gh calls = %q; want at most the compare of %s...%s", calls, tc.commit, tc.head)
			}
		})
	}
}

func ghCalls(t *testing.T, dir string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "gh-calls"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
}
