package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Deployed WebUI recovery checks the discovery's parent pid, not piPid.
// This read-only preflight is not a lock against concurrent starts or writes.
func preflightDiscovery(ctx context.Context, bridge string) error {
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	entries, err := os.ReadDir(bridge)
	if err != nil {
		return fmt.Errorf("shared discovery preflight blocked: %w", err)
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		if strings.HasSuffix(entry.Name(), ".recovering") {
			return fmt.Errorf("shared discovery preflight blocked: recovery claim exists")
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(bridge, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("shared discovery preflight blocked: %w", err)
		}
		if !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return fmt.Errorf("shared discovery preflight blocked: invalid discovery file")
		}
		// Keep a replacement FIFO/symlink or a growing file from bypassing the limit.
		f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return fmt.Errorf("shared discovery preflight blocked: %w", err)
		}
		opened, err := f.Stat()
		if err == nil && (!opened.Mode().IsRegular() || opened.Size() > 1<<20) {
			err = fmt.Errorf("invalid discovery file")
		}
		var raw []byte
		if err == nil {
			raw, err = io.ReadAll(io.LimitReader(f, (1<<20)+1))
		}
		err = errors.Join(err, f.Close())
		if err != nil {
			return fmt.Errorf("shared discovery preflight blocked: %w", err)
		}
		if len(raw) > 1<<20 {
			return fmt.Errorf("shared discovery preflight blocked: invalid discovery file")
		}
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
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
		if err := syscall.Kill(int(pid), 0); err != nil {
			return fmt.Errorf("shared discovery preflight blocked: dead/unverifiable parent pid permits recovery: %w", err)
		}
	}
	return context.Cause(ctx)
}
