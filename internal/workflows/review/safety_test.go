package review

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestPreflightDiscoveryReadOnly(t *testing.T) {
	// Wait reaps an actual child; no guessed PID or polling is needed.
	child := exec.Command("/usr/bin/true")
	if err := child.Run(); err != nil {
		t.Fatal(err)
	}
	deadPID := child.ProcessState.Pid()
	for _, tc := range []struct {
		name, filename, data string
		blocked              bool
	}{
		{"empty", "", "", false},
		{"live-parent-dead-pi", "live.json", fmt.Sprintf(`{"pid":%d,"piPid":%d}`, os.Getpid(), deadPID), false},
		{"dead-parent-live-pi", "dead.json", fmt.Sprintf(`{"pid":%d,"piPid":%d}`, deadPID, os.Getpid()), true},
		{"recovering", "session.json.recovering", "claimed", true},
		{"malformed", "broken.json", `{`, false},
		{"hub-state", "hub-state.json", `{"sessions":[]}`, false},
		{"missing-parent", "missing.json", fmt.Sprintf(`{"piPid":%d}`, os.Getpid()), false},
		{"zero-parent", "zero.json", `{"pid":0}`, false},
		{"negative-parent", "negative.json", `{"pid":-1}`, true},
		{"ignored-non-discovery", "notes.txt", "keep", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bridge := t.TempDir()
			t.Setenv("PI_BRIDGE_DIR", bridge)
			if tc.filename != "" {
				fixtureWrite(t, filepath.Join(bridge, tc.filename), []byte(tc.data))
			}
			before, err := os.ReadDir(bridge)
			if err != nil {
				t.Fatal(err)
			}
			var info os.FileInfo
			if tc.filename != "" {
				info, err = os.Lstat(filepath.Join(bridge, tc.filename))
				if err != nil {
					t.Fatal(err)
				}
			}
			err = preflightDiscovery()
			if (err != nil) != tc.blocked {
				t.Fatalf("blocked=%v: %v", tc.blocked, err)
			}
			after, err := os.ReadDir(bridge)
			if err != nil || len(after) != len(before) {
				t.Fatalf("discovery entries changed: %v", err)
			}
			if tc.filename != "" {
				path := filepath.Join(bridge, tc.filename)
				raw, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(raw, []byte(tc.data)) {
					t.Fatalf("discovery bytes changed: %q %v", raw, err)
				}
				got, err := os.Lstat(path)
				if err != nil || !os.SameFile(info, got) || info.Mode() != got.Mode() || !info.ModTime().Equal(got.ModTime()) {
					t.Fatalf("discovery inode/mode/mtime changed: %v", err)
				}
			}
		})
	}
	for _, kind := range []string{"missing-directory", "symlink", "directory", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			bridge := t.TempDir()
			t.Setenv("PI_BRIDGE_DIR", bridge)
			path := filepath.Join(bridge, "session.json")
			switch kind {
			case "missing-directory":
				t.Setenv("PI_BRIDGE_DIR", filepath.Join(bridge, "absent"))
			case "symlink":
				target := filepath.Join(t.TempDir(), "target")
				fixtureWrite(t, target, []byte(fmt.Sprintf(`{"pid":%d}`, os.Getpid())))
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				fixtureWrite(t, path, bytes.Repeat([]byte(" "), (1<<20)+1))
			}
			before, _ := os.Lstat(path)
			if err := preflightDiscovery(); err == nil {
				t.Fatal("unsafe discovery accepted")
			}
			after, err := os.Lstat(path)
			if before != nil && (err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime())) {
				t.Fatalf("invalid discovery changed: %v", err)
			}
		})
	}
}

func TestCheckoutVerifyRealGit(t *testing.T) {
	for _, scenario := range []string{"clean", "tracked", "staged", "untracked", "ignored directory", "snapshot", "deleted", "head"} {
		t.Run(scenario, func(t *testing.T) {
			f := newAcquisitionFixture(t)
			if scenario == "ignored directory" {
				fixtureWrite(t, filepath.Join(f.sourceRepo, ".gitignore"), []byte("generated/\n"))
				fixtureGit(t, f.sourceRepo, "add", ".gitignore")
				fixtureGit(t, f.sourceRepo, "commit", "-m", "ignore generated files")
				f.head = fixtureGit(t, f.sourceRepo, "rev-parse", "HEAD")
				fixtureGit(t, f.sourceRepo, "push", f.headRepo, "topic")
				f.metadata["head"].(map[string]any)["sha"] = f.head
				f.saveMetadata(t)
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
			checkError(t, c.Verify(context.Background()), "")
			want := "modified"
			switch scenario {
			case "clean":
				want = ""
			case "tracked", "staged":
				fixtureWrite(t, filepath.Join(c.Worktree, "shared.txt"), []byte("changed\n"))
				if scenario == "staged" {
					fixtureGit(t, c.Worktree, "add", "shared.txt")
				}
			case "untracked":
				fixtureWrite(t, filepath.Join(c.Worktree, "new.txt"), []byte("changed\n"))
			case "ignored directory":
				if err := os.Mkdir(filepath.Join(c.Worktree, "generated"), 0700); err != nil {
					t.Fatal(err)
				}
				fixtureWrite(t, filepath.Join(c.Worktree, "generated", "new.txt"), []byte("ignored mutation\n"))
				if got := fixtureGit(t, c.Worktree, "status", "--porcelain=v1", "--untracked-files=all"); got != "" {
					t.Fatalf("fixture mutation is not ignored: %s", got)
				}
			case "snapshot":
				fixtureWrite(t, c.Snapshots["diff"], []byte("tampered diff\n"))
				want = "acquisition snapshot diff changed"
			case "deleted":
				if err := os.Remove(filepath.Join(c.Worktree, "shared.txt")); err != nil {
					t.Fatal(err)
				}
			case "head":
				fixtureGit(t, c.Worktree, "checkout", "--detach", c.BaseSHA)
				want = "HEAD changed"
			}
			before := fixtureGit(t, c.Worktree, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching")
			head := fixtureGit(t, c.Worktree, "rev-parse", "HEAD")
			checkError(t, c.Verify(context.Background()), want)
			if got := fixtureGit(t, c.Worktree, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching"); got != before {
				t.Fatalf("Verify changed checkout: %q vs %q", got, before)
			}
			if got := fixtureGit(t, c.Worktree, "rev-parse", "HEAD"); got != head {
				t.Fatal("Verify changed HEAD")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := c.Verify(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled Verify: %v", err)
			}
		})
	}
}
