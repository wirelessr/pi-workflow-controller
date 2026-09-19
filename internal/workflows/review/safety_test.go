package review

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"pi-workflow-controller/internal/contract"
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

func TestReviewFileReaderBoundaries(t *testing.T) {
	for _, reader := range []string{"checkout", "published"} {
		t.Run(reader, func(t *testing.T) {
			for _, scenario := range []string{"local", "missing", "directory", "outside relative", "outside absolute", "escaping symlink", "inside symlink", "FIFO", "pre cancel", "sparse over limit"} {
				t.Run(scenario, func(t *testing.T) {
					dir := reportTempDir(t)
					fixtureWrite(t, filepath.Join(dir, "local.txt"), []byte("local contents\n"))
					root, err := os.OpenRoot(dir)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := root.Close(); err != nil {
							t.Error(err)
						}
					})
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					path, want := "local.txt", "local contents\n"
					var wantErr error
					var wantExact, wantContains string
					switch scenario {
					case "missing":
						path, wantErr = "missing.txt", os.ErrNotExist
					case "directory":
						path = "."
						wantExact = `not a regular file: "."`
					case "outside relative":
						path = "../outside.txt"
						wantExact = fmt.Sprintf("path is not relative and rooted: %q", path)
					case "outside absolute":
						path = filepath.Join(reportTempDir(t), "outside.txt")
						fixtureWrite(t, path, []byte("outside"))
						wantExact = fmt.Sprintf("path is not relative and rooted: %q", path)
					case "escaping symlink", "inside symlink":
						target := "local.txt"
						if scenario == "escaping symlink" {
							target = filepath.Join(reportTempDir(t), "outside.txt")
							fixtureWrite(t, target, []byte("outside"))
							wantContains = "path escapes from parent"
						}
						path = "link.txt"
						if err := os.Symlink(target, filepath.Join(dir, path)); err != nil {
							t.Fatal(err)
						}
					case "FIFO":
						path = "pipe"
						if err := syscall.Mkfifo(filepath.Join(dir, path), 0600); err != nil {
							t.Fatal(err)
						}
						wantExact = `not a regular file: "pipe"`
					case "pre cancel":
						cancel()
						wantErr = context.Canceled
					case "sparse over limit":
						path = "sparse.bin"
						f, err := os.Create(filepath.Join(dir, path))
						if err != nil {
							t.Fatal(err)
						}
						if err := errors.Join(f.Truncate((64<<20)+1), f.Close()); err != nil {
							t.Fatal(err)
						}
						wantExact = `file exceeds review read limit: "sparse.bin"`
					}
					read := func() ([]byte, error) {
						if reader == "published" {
							return readPublishedFile(ctx, contract.Ref{Path: filepath.Join(dir, "envelope.json")}, checkFile{ID: "file", Kind: "artifact", Path: path})
						}
						return readCheckFile(ctx, root, path)
					}
					raw, err := read()
					if wantErr != nil || wantExact != "" || wantContains != "" {
						if err == nil || raw != nil {
							t.Fatalf("error must return nil bytes: len=%d nil=%t err=%v", len(raw), raw == nil, err)
						}
						if wantErr != nil && !errors.Is(err, wantErr) {
							t.Fatalf("got %v, want %v", err, wantErr)
						}
						if wantExact != "" && err.Error() != wantExact {
							t.Fatalf("got %q, want %q", err, wantExact)
						}
						if wantContains != "" && !strings.Contains(err.Error(), wantContains) {
							t.Fatalf("got %q, want substring %q", err, wantContains)
						}
					} else if err != nil || string(raw) != want {
						t.Fatalf("got bytes=%q err=%v, want %q", raw, err, want)
					}
				})
			}
		})
	}
}
