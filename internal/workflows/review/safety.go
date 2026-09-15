package review

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Deployed WebUI recovery checks the discovery's parent pid, not piPid.
// This conservative read-only preflight is not a lock against concurrent starts.
func preflightDiscovery() error {
	bridge := os.Getenv("PI_BRIDGE_DIR")
	if bridge == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		bridge = filepath.Join(home, ".pi/agent/extensions/pi-webui-extension/data")
	}
	entries, err := os.ReadDir(bridge)
	if err != nil {
		return fmt.Errorf("shared discovery preflight blocked: %w", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".recovering") {
			return fmt.Errorf("shared discovery preflight blocked: recovery claim exists")
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(bridge, entry.Name())
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return fmt.Errorf("shared discovery preflight blocked: invalid discovery file")
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("shared discovery preflight blocked: unreadable discovery")
		}
		var d map[string]any
		if json.Unmarshal(raw, &d) != nil || d == nil {
			continue
		}
		value := d["pid"]
		// WebUI skips entries with no truthy pid, including hub-state.json.
		if value == nil || value == false || value == "" || value == float64(0) {
			continue
		}
		pid, ok := value.(float64)
		if !ok || pid <= 0 || pid > 2147483647 || pid != float64(int(pid)) {
			return fmt.Errorf("shared discovery preflight blocked: invalid parent pid")
		}
		if syscall.Kill(int(pid), 0) != nil {
			return fmt.Errorf("shared discovery preflight blocked: dead/unverifiable parent pid permits recovery")
		}
	}
	return nil
}

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
