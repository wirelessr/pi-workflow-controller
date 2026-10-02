package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

var errSize = errors.New("file exceeds byte limit")
var errUnstable = errors.New("file changed while taking snapshot")
var errUnsafe = errors.New("path must contain only directories and a regular file, without symlinks")

func digest(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }

func noSymlinks(root *os.Root, path string) (err error) {
	if !filepath.IsLocal(path) || filepath.Clean(path) != path {
		return errUnsafe
	}
	// Native single-open proof: O_NOFOLLOW_ANY rejects symlinks anywhere in
	// the path in one syscall instead of one Lstat per prefix from the root.
	var cleanupErr error
	defer func() {
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	if dir, openErr := root.Open("."); openErr == nil {
		fd, nativeErr := unix.Openat(int(dir.Fd()), path, unix.O_EVTONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW_ANY|unix.O_CLOEXEC, 0)
		parentOK := false
		if nativeErr == nil {
			// Native lookup only needs search permission; retain Go's directory read requirement.
			if parent, parentErr := root.OpenFile(filepath.Dir(path), os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0); parentErr == nil {
				parentOK = true
				cleanupErr = parent.Close()
			}
			cleanupErr = errors.Join(cleanupErr, unix.Close(fd))
		}
		cleanupErr = errors.Join(cleanupErr, dir.Close())
		if nativeErr == nil && parentOK && cleanupErr == nil {
			return nil
		}
	}
	// Preserve the original prefix result on proof failure, retaining visible Close errors.
	current := ""
	parts := strings.Split(path, string(filepath.Separator))
	for i, part := range parts {
		current = filepath.Join(current, part)
		info, err := root.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (i < len(parts)-1 && !info.IsDir()) {
			return errUnsafe
		}
	}
	return nil
}

func openRegular(root *os.Root, path string) (*os.File, os.FileInfo, error) {
	if err := noSymlinks(root, path); err != nil {
		return nil, nil, err
	}
	listed, err := root.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !listed.Mode().IsRegular() {
		return nil, nil, errUnsafe
	}
	// NONBLOCK prevents a path replacement with a FIFO from hanging Open.
	f, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = errUnsafe
	}
	if err == nil && !os.SameFile(listed, info) {
		err = errUnstable
	}
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return f, info, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := context.Cause(r.ctx); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// copyStable checks both metadata and a second full digest on the same open
// file. This detects observed in-place writes and replacements, not malicious
// same-UID writers which can restore bytes between observations.
func copyStable(ctx context.Context, root *os.Root, path string, max int64, dst io.Writer, afterCopy func(string)) (string, int64, os.FileInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return "", 0, nil, err
	}
	f, before, err := openRegular(root, path)
	if err != nil {
		return "", 0, nil, err
	}
	defer func(f *os.File) { _ = f.Close() }(f)
	if before.Size() > max {
		return "", 0, nil, errSize
	}
	h := sha256.New()
	// LimitReader(max) plus an explicit one-byte probe avoids max+1 overflow.
	n, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(contextReader{ctx, f}, max))
	if err != nil {
		return "", 0, nil, err
	}
	var extra [1]byte
	count, end := contextReader{ctx, f}.Read(extra[:])
	if count != 0 {
		return "", 0, nil, errSize
	}
	if end != io.EOF {
		return "", 0, nil, end
	}
	first := hex.EncodeToString(h.Sum(nil))
	if afterCopy != nil {
		afterCopy(path)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return "", 0, nil, err
	}
	h.Reset()
	second, err := io.Copy(h, io.LimitReader(contextReader{ctx, f}, max))
	if err != nil {
		return "", 0, nil, err
	}
	count, end = contextReader{ctx, f}.Read(extra[:])
	if count != 0 {
		return "", 0, nil, errUnstable
	}
	if end != io.EOF {
		return "", 0, nil, end
	}
	after, err := f.Stat()
	if err != nil {
		return "", 0, nil, err
	}
	if err = noSymlinks(root, path); err != nil {
		return "", 0, nil, err
	}
	current, err := root.Lstat(path)
	if err != nil {
		return "", 0, nil, err
	}
	if n != before.Size() || n != second || first != hex.EncodeToString(h.Sum(nil)) || !sameVersion(before, after) || !sameVersion(after, current) {
		return "", 0, nil, errUnstable
	}
	if err = context.Cause(ctx); err != nil {
		return "", 0, nil, err
	}
	return first, n, before, nil
}
func sameVersion(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}
func readStable(ctx context.Context, root *os.Root, path string, max int64, barrier func(string)) ([]byte, error) {
	var buf bytes.Buffer
	_, _, _, err := copyStable(ctx, root, path, max, &buf, barrier)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeExclusive(root *os.Root, path string, raw []byte, syncFile func(*os.File) error) error {
	f, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	if err == nil {
		err = syncFile(f)
	}
	return errors.Join(err, f.Close())
}

func writeAtomic(root *os.Root, path string, raw []byte, syncFile func(*os.File) error) (err error) {
	temp := filepath.Join(filepath.Dir(path), ".tmp-"+NewID())
	f, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if cleanup := root.Remove(temp); !errors.Is(cleanup, os.ErrNotExist) {
			err = errors.Join(err, cleanup)
		}
	}()
	_, err = f.Write(raw)
	if err == nil {
		err = syncFile(f)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	return root.Rename(temp, path)
}

// Both parents are opened under os.Root before the platform exclusive rename;
// only basenames reach renameat, so concurrent path swaps cannot escape root.
func renameExclusive(root *os.Root, from, to string) error {
	for _, p := range []string{from, filepath.Dir(to)} {
		if err := noSymlinks(root, p); err != nil {
			return err
		}
	}
	source, err := root.Open(filepath.Dir(from))
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(source)
	target, err := root.Open(filepath.Dir(to))
	if err != nil {
		return err
	}
	defer func(f *os.File) { _ = f.Close() }(target)
	if err = renameNoReplace(int(source.Fd()), filepath.Base(from), int(target.Fd()), filepath.Base(to)); err != nil {
		return fmt.Errorf("publish %s: %w", to, err)
	}
	return nil
}

// ReadStable reads a regular file under root with no symlink anywhere in its
// path, at most max bytes, and verifies the bytes and metadata did not change
// during the read. Cancellation is checked between reads; it cannot interrupt
// a blocked open or read.
func ReadStable(ctx context.Context, root *os.Root, path string, max int64) ([]byte, error) {
	return readStable(ctx, root, path, max, nil)
}
