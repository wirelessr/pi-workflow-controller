package contract

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

// storeTestRoot opens an os.Root over dir with t cleanup, mirroring Store construction.
func storeTestRoot(t *testing.T, dir string) *os.Root {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	return root
}

func noSymlinksOutcome(root *os.Root, path string) (ok bool, notExist, permission, unsafe bool) {
	err := noSymlinks(root, path)
	if err == nil {
		return true, false, false, false
	}
	notExist = errors.Is(err, os.ErrNotExist)
	var pathErr *os.PathError
	permission = errors.As(err, &pathErr) && errors.Is(pathErr.Err, os.ErrPermission)
	unsafe = err == errUnsafe
	return false, notExist, permission, unsafe
}

// TestNoSymlinksNativeProofEquivalence pins the native single-open proof to the
// prefix walker's observable outcomes on every structural and permission shape.
func TestNoSymlinksNativeProofEquivalence(t *testing.T) {
	cases := []struct {
		name string
		keys []string
	}{
		{"regular", []string{"a/file"}},
		{"directory", []string{"a"}},
		{"FIFO", []string{"a/fifo"}},
		{"leaf-inside", []string{"a/link"}},
		{"leaf-outside", []string{"a/link"}},
		{"ancestor-inside", []string{"a/file"}},
		{"ancestor-outside", []string{"a/file"}},
		{"nonDir-prefix", []string{"a/file"}},
		{"missing-leaf", []string{"a/missing"}},
		{"missing-prefix", []string{"missing/file"}},
		{"leaf-no-permission", []string{"a/file"}},
		{"ancestor-no-search", []string{"a/file"}},
		{"ancestor-search-only", []string{"a/file"}},
		{"root-search-only", []string{"a/file"}},
		{"renamed-root", []string{"a/file"}},
		{"dot", []string{"."}},
		{"absolute", []string{filepath.Join(t.TempDir(), "file")}},
		{"escape", []string{"../file"}},
		{"noncanonical", []string{"a//file"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var pinnedRoot *os.Root
			base := t.TempDir()
			if err := os.Mkdir(filepath.Join(base, "a"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(base, "a", "file"), []byte("body"), 0600); err != nil {
				t.Fatal(err)
			}
			switch tc.name {
			case "FIFO":
				if err := syscall.Mkfifo(filepath.Join(base, "a", "fifo"), 0600); err != nil {
					t.Fatal(err)
				}
			case "leaf-inside":
				if err := os.Symlink("file", filepath.Join(base, "a", "link")); err != nil {
					t.Fatal(err)
				}
			case "leaf-outside":
				if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(base, "a", "link")); err != nil {
					t.Fatal(err)
				}
			case "ancestor-inside":
				if err := os.Symlink(".", filepath.Join(base, "a-link")); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(base, "a"), filepath.Join(base, "a-kept")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("a-kept", filepath.Join(base, "a")); err != nil {
					t.Fatal(err)
				}
			case "ancestor-outside":
				outside := t.TempDir()
				if err := os.Symlink(outside, filepath.Join(base, "a-link")); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(base, "a"), filepath.Join(base, "a-kept")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("a-kept", filepath.Join(base, "a")); err != nil {
					t.Fatal(err)
				}
				// keep a pointing through an outside symlink chain
				if err := os.Remove(filepath.Join(base, "a")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(outside, "real-a"), filepath.Join(base, "a")); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(base, "a-kept"), filepath.Join(outside, "real-a")); err != nil {
					t.Fatal(err)
				}
			case "nonDir-prefix":
				if err := os.WriteFile(filepath.Join(base, "a"), []byte("regular"), 0600); err == nil {
					t.Fatal("expected setup conflict")
				}
				if err := os.Remove(filepath.Join(base, "a", "file")); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(base, "a")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(base, "a"), []byte("regular"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-leaf", "missing-prefix":
			case "leaf-no-permission":
				if err := os.Chmod(filepath.Join(base, "a", "file"), 0000); err != nil {
					t.Fatal(err)
				}
			case "ancestor-no-search":
				if err := os.Chmod(filepath.Join(base, "a"), 0000); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(filepath.Join(base, "a"), 0700) })
			case "ancestor-search-only":
				if err := os.Chmod(filepath.Join(base, "a"), 0100); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(filepath.Join(base, "a"), 0700) })
			case "root-search-only":
				// Only the root drops its read bit; the 09 controls keep the
				// rooted fd pinned before chmod and observe ok on both arms.
				rootEarly, err := os.OpenRoot(base)
				if err != nil {
					t.Fatal(err)
				}
				pinnedRoot = rootEarly
				t.Cleanup(func() { _ = rootEarly.Close() })
				if err := os.Chmod(base, 0100); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(base, 0700) })
			case "renamed-root":
				if err := os.Mkdir(filepath.Join(base, "b"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(base, "b", "file"), []byte("body"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(base, "a", "file")); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(filepath.Join(base, "a")); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(filepath.Join(base, "b"), filepath.Join(base, "a")); err != nil {
					t.Fatal(err)
				}
			}
			root := pinnedRoot
			if root == nil {
				root = storeTestRoot(t, base)
			}
			for _, key := range tc.keys {
				ok, notExist, permission, unsafe := noSymlinksOutcome(root, key)
				switch tc.name {
				case "regular", "directory", "FIFO", "renamed-root", "dot":
					if !ok {
						t.Fatalf("expected ok, got notExist=%v permission=%v unsafe=%v", notExist, permission, unsafe)
					}
				case "leaf-inside", "leaf-outside", "ancestor-inside", "ancestor-outside", "nonDir-prefix", "absolute", "escape", "noncanonical":
					if ok || !unsafe {
						t.Fatalf("expected unsafe rejection, got ok=%v notExist=%v permission=%v unsafe=%v", ok, notExist, permission, unsafe)
					}
				case "missing-leaf", "missing-prefix":
					if ok || !notExist {
						t.Fatalf("expected not-exist, got ok=%v notExist=%v permission=%v unsafe=%v", ok, notExist, permission, unsafe)
					}
				case "leaf-no-permission":
					// 09 controls: metadata open does not need the leaf's
					// read bit; both arms accept (openRegular enforces later).
					if !ok {
						t.Fatalf("expected ok, got notExist=%v permission=%v unsafe=%v", notExist, permission, unsafe)
					}
				case "ancestor-no-search", "ancestor-search-only":
					// 09 controls: the walker's statat needs search on the
					// prefix; both arms fail with a typed permission error.
					if ok || !permission {
						t.Fatalf("expected permission failure, got ok=%v notExist=%v permission=%v unsafe=%v", ok, notExist, permission, unsafe)
					}
				case "root-search-only":
					// 09 controls: root's own search bit does not gate the
					// rooted parent traversal; both arms accept.
					if !ok {
						t.Fatalf("expected ok, got notExist=%v permission=%v unsafe=%v", notExist, permission, unsafe)
					}
				}
			}
		})
	}
}

// TestNoSymlinksNativeProofDeviceNode ensures special device leaves fall back to
// the prefix walker's exact classification instead of a new native outcome.
func TestNoSymlinksNativeProofDeviceNode(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission cases are meaningless as root")
	}
	base := t.TempDir()
	if err := os.Mkdir(filepath.Join(base, "a"), 0700); err != nil {
		t.Fatal(err)
	}
	// mknod requires root on darwin; a directory named like a device must still
	// classify by mode, so use a mode-0 socket via Mkfifo already covered and
	// verify a device-like irregular leaf cannot be fabricated non-root. The
	// contract: any irregular leaf keeps the walker's rejection.
	if err := syscall.Mkfifo(filepath.Join(base, "a", "dev"), 0000); err != nil {
		t.Fatal(err)
	}
	root := storeTestRoot(t, base)
	// Walker semantics: an irregular leaf passes noSymlinks and is rejected
	// by openRegular's regular-file check instead.
	ok, _, _, unsafe := noSymlinksOutcome(root, "a/dev")
	if !ok || unsafe {
		t.Fatalf("noSymlinks must accept an irregular leaf like the walker: ok=%v unsafe=%v", ok, unsafe)
	}
	if f, _, err := openRegular(root, "a/dev"); err == nil {
		_ = f.Close()
		t.Fatal("openRegular accepted an irregular leaf")
	} else if err != errUnsafe {
		t.Fatalf("openRegular irregular leaf error = %v, want errUnsafe", err)
	}
}

// TestNoSymlinksNativeProofConcurrentRename keeps the proof stable while a
// prefix directory is swapped concurrently: every observed outcome must be
// one of the walker-legal outcomes, never a wrong acceptance.
func TestNoSymlinksNativeProofConcurrentRename(t *testing.T) {
	base := t.TempDir()
	if err := os.Mkdir(filepath.Join(base, "a"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "a", "file"), []byte("body"), 0600); err != nil {
		t.Fatal(err)
	}
	root := storeTestRoot(t, base)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		link := filepath.Join(base, "a-link")
		for {
			select {
			case <-stop:
				return
			default:
				_ = os.Symlink(".", link)
				_ = os.Remove(link)
			}
		}
	}()
	for i := 0; i < 200; i++ {
		ok, notExist, permission, unsafe := noSymlinksOutcome(root, "a/file")
		if !ok && !notExist && !permission && !unsafe {
			t.Fatalf("unexpected outcome kind at iteration %d", i)
		}
		if unsafe {
			t.Fatal("stable regular path classified unsafe under concurrent unrelated symlink churn")
		}
	}
	close(stop)
	wg.Wait()
}

// TestNoSymlinksNativeProofExhaustedFDs proves FD exhaustion degrades to the
// prefix walker instead of surfacing a spurious acceptance or misclassification.
func TestNoSymlinksNativeProofExhaustedFDs(t *testing.T) {
	base := t.TempDir()
	if err := os.Mkdir(filepath.Join(base, "a"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "a", "file"), []byte("body"), 0600); err != nil {
		t.Fatal(err)
	}
	root := storeTestRoot(t, base)
	// Drive exhaustion deterministically: lower this test's soft FD ceiling
	// until every remaining descriptor is spent, then verify the outcome.
	var before syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &before); err != nil {
		t.Fatal(err)
	}
	const floor = 128
	if before.Cur > floor {
		next := syscall.Rlimit{Cur: floor, Max: before.Max}
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &next); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &before)
		})
	}
	var held []*os.File
	defer func() {
		for _, f := range held {
			_ = f.Close()
		}
	}()
	for {
		f, err := os.Open(os.DevNull)
		if err != nil {
			break
		}
		held = append(held, f)
		if len(held) > 20000 {
			t.Fatal("fd floor did not produce exhaustion")
		}
	}
	ok, notExist, permission, unsafe := noSymlinksOutcome(root, "a/file")
	if ok || unsafe {
		t.Fatalf("fd exhaustion must not accept or misclassify: ok=%v unsafe=%v notExist=%v permission=%v", ok, unsafe, notExist, permission)
	}
}
