package review

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

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
