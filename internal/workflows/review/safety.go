package review

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Verify never executes repository code. A tracked/untracked mutation or a
// different HEAD invalidates this review instead of silently changing revision.
func (c *Checkout) Verify(ctx context.Context) error {
	for key, hash := range c.snapshotHashes {
		root, err := os.OpenRoot(filepath.Dir(c.Snapshots[key]))
		if err != nil {
			return err
		}
		raw, err := readCheckFile(ctx, root, filepath.Base(c.Snapshots[key]))
		_ = root.Close()
		if err != nil {
			return err
		}
		if sha256.Sum256(raw) != hash {
			return fmt.Errorf("acquisition snapshot %s changed", key)
		}
	}
	gitdir := filepath.Join(c.repository, "worktrees", "checkout")
	args := []string{"--git-dir=" + gitdir, "--work-tree=" + c.worktree}
	head, err := c.git(ctx, append(args, "rev-parse", "HEAD")...)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(head)) != c.HeadSHA {
		return fmt.Errorf("review checkout HEAD changed")
	}
	status, err := c.git(ctx, append(args, "status", "--porcelain=v1", "--untracked-files=all", "--ignored=matching", "--ignore-submodules=none")...)
	if err != nil {
		return err
	}
	if len(status) != 0 {
		return fmt.Errorf("review checkout was modified; static review requires unchanged code")
	}
	return nil
}
