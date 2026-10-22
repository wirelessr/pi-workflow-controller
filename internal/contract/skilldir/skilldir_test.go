package skilldir

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func sha(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

// newSource returns an absolute source directory whose only subtree is tools.
func newSource(t *testing.T) string {
	t.Helper()
	source := filepath.Join(t.TempDir(), "skills")
	write(t, filepath.Join(source, "tools", "SKILL.md"), []byte("# tools\n"))
	return source
}

// placeholder returns a path whose open blocks until a writer opens it, as a
// cloud placeholder can. Cleanup opens the writer so the abandoned reader
// returns.
func placeholder(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placeholder")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		opened := make(chan error, 1)
		go func() {
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err == nil {
				err = f.Close()
			}
			opened <- err
		}()
		select {
		case err := <-opened:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("the abandoned reader never opened the placeholder")
		}
	})
	return path
}

func TestReadAndExpand(t *testing.T) {
	source := newSource(t)
	contents := map[string]string{
		"tools/SKILL.md":   "# tools\n",
		"tools/B.md":       "upper\n",
		"tools/a.md":       "lower\n",
		"tools/sub/x.md":   "nested\n",
		"tools/\u00e9.md":  "non-ascii\n",
		"tools/.hidden.md": "editor state\n",
		"base/SKILL.md":    "# base\n",
		"other/SKILL.md":   "# not requested\n",
		"README.md":        "sync notes\n",
	}
	for path, data := range contents {
		write(t, filepath.Join(source, filepath.FromSlash(path)), []byte(data))
	}
	// A dot directory is skipped whole, including entries that would be rejected.
	if err := os.MkdirAll(filepath.Join(source, "tools", ".cache"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(source, "tools", ".cache", "pipe"), 0600); err != nil {
		t.Fatal(err)
	}
	// tools is walked before base, so only sorting yields byte order.
	src, err := Read(context.Background(), source, []string{"tools", "base"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if src.Sidecar("README.md") != nil {
		t.Fatal("a file that was not named as a sidecar was returned")
	}
	runDir := t.TempDir()
	expansion, err := src.Expand(runDir, "neutral-skills")
	if err != nil {
		t.Fatal(err)
	}
	if expansion.Dir != filepath.Join(runDir, "neutral-skills") || !filepath.IsAbs(expansion.Dir) {
		t.Fatalf("expansion dir = %s", expansion.Dir)
	}
	// Byte order, not case-folded or locale order.
	want := []string{"base/SKILL.md", "tools/B.md", "tools/SKILL.md", "tools/a.md", "tools/sub/x.md", "tools/\u00e9.md"}
	// sha256 of the "<sha256>  <path>\n" lines of want, computed outside Go.
	const digest = "2aad1941a643f4ff04690f5989b5b401cc832e3e7ea52b4de7c93885fab1e888"
	var paths []string
	for _, file := range expansion.Files {
		paths = append(paths, file.Path)
		path := filepath.Join(expansion.Dir, filepath.FromSlash(file.Path))
		raw, err := os.ReadFile(path)
		if err != nil || string(raw) != contents[file.Path] || file.SHA256 != sha(raw) {
			t.Fatalf("expanded %s = %q (%v), recorded %s", file.Path, raw, err, file.SHA256)
		}
		if info, err := os.Lstat(path); err != nil || info.Mode() != 0600 {
			t.Fatalf("expanded %s mode = %v (%v)", file.Path, info.Mode(), err)
		}
	}
	if strings.Join(paths, " ") != strings.Join(want, " ") || expansion.Digest != digest {
		t.Fatalf("files = %v digest = %s", paths, expansion.Digest)
	}
	for _, dir := range []string{"", "base", "tools", "tools/sub"} {
		if info, err := os.Lstat(filepath.Join(expansion.Dir, dir)); err != nil || info.Mode() != fs.ModeDir|0700 {
			t.Fatalf("expanded directory %q mode = %v (%v)", dir, info.Mode(), err)
		}
	}
	for _, excluded := range []string{"other", "README.md", "tools/.hidden.md", "tools/.cache"} {
		if _, err := os.Lstat(filepath.Join(expansion.Dir, excluded)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s was expanded: %v", excluded, err)
		}
	}
}

// The source does not exist, so an error from any filesystem access would
// differ from the declaration error.
func TestReadRejectsDeclarationsBeforeAccess(t *testing.T) {
	source := filepath.Join(t.TempDir(), "absent")
	for _, tc := range []struct {
		name           string
		dirs, sidecars []string
		want           string
	}{
		{"no subtrees", nil, nil, "no skill subtrees declared"},
		{"empty subtree list", []string{}, []string{"meta.json"}, "no skill subtrees declared"},
		{"empty subtree name", []string{""}, nil, `skill subtree "" must be a single path segment`},
		{"dot subtree", []string{"tools", "."}, nil, `skill subtree "." must be a single path segment`},
		{"parent subtree", []string{".."}, nil, `skill subtree ".." must be a single path segment`},
		{"nested subtree", []string{"tools/sub"}, nil, `skill subtree "tools/sub" must be a single path segment`},
		{"absolute subtree", []string{"/tools"}, nil, `skill subtree "/tools" must be a single path segment`},
		{"duplicate subtree", []string{"tools", "base", "tools"}, nil, `skill subtree "tools" is listed twice`},
		{"sidecar inside a subtree", []string{"base", "tools"}, []string{"meta.json", "tools/meta.json"}, `skill sidecar "tools/meta.json" lies inside subtree "tools"`},
		{"sidecar naming a subtree", []string{"tools"}, []string{"tools"}, `skill sidecar "tools" lies inside subtree "tools"`},
		{"sidecar inside a subtree after cleaning", []string{"tools"}, []string{"./tools//meta.json"}, `skill sidecar "./tools//meta.json" lies inside subtree "tools"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Read(context.Background(), source, tc.dirs, tc.sidecars)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestExpandRejectsName(t *testing.T) {
	src, err := Read(context.Background(), newSource(t), []string{"tools"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "nested/skills", "../skills", "/skills"} {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			runDir := filepath.Join(parent, "run")
			if err := os.MkdirAll(filepath.Join(runDir, "nested"), 0700); err != nil {
				t.Fatal(err)
			}
			_, err := src.Expand(runDir, name)
			if want := fmt.Sprintf("skill expansion name %q must be a single path segment", name); err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %q", err, want)
			}
			for dir, want := range map[string]int{parent: 1, runDir: 1, filepath.Join(runDir, "nested"): 0} {
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != want {
					t.Fatalf("%s holds %d entries (%v), want %d", dir, len(entries), err, want)
				}
			}
		})
	}
}

// The limits are written as literals so that changing a constant fails here.
func TestReadLimits(t *testing.T) {
	skill := len("# tools\n")
	for _, tc := range []struct {
		name  string
		build func(t *testing.T, source string)
		want  string
		files int
	}{
		{"file at the size limit", func(t *testing.T, source string) {
			write(t, filepath.Join(source, "tools", "big.md"), make([]byte, 1<<20))
		}, "", 2},
		{"file over the size limit", func(t *testing.T, source string) {
			write(t, filepath.Join(source, "tools", "big.md"), make([]byte, 1<<20+1))
		}, "skill file tools/big.md: file exceeds byte limit", 0},
		{"total at the limit, sidecar not counted", func(t *testing.T, source string) {
			for i := range 7 {
				write(t, filepath.Join(source, "tools", fmt.Sprintf("big-%d.md", i)), make([]byte, 1<<20))
			}
			write(t, filepath.Join(source, "tools", "filler.md"), make([]byte, 1<<20-skill))
			write(t, filepath.Join(source, "meta.json"), make([]byte, 1<<20))
		}, "", 9},
		{"total over the limit", func(t *testing.T, source string) {
			for i := range 7 {
				write(t, filepath.Join(source, "tools", fmt.Sprintf("big-%d.md", i)), make([]byte, 1<<20))
			}
			write(t, filepath.Join(source, "tools", "filler.md"), make([]byte, 1<<20-skill+1))
		}, "skill files exceed 8388608 bytes", 0},
		{"file count at the limit, sidecar not counted", func(t *testing.T, source string) {
			for i := range 127 {
				write(t, filepath.Join(source, "tools", fmt.Sprintf("note-%03d.md", i)), []byte("x"))
			}
			write(t, filepath.Join(source, "meta.json"), []byte("{}"))
		}, "", 128},
		{"file count over the limit", func(t *testing.T, source string) {
			for i := range 128 {
				write(t, filepath.Join(source, "tools", fmt.Sprintf("note-%03d.md", i)), []byte("x"))
			}
		}, "skill directory has more than 128 files", 0},
		{"sidecar over the size limit", func(t *testing.T, source string) {
			write(t, filepath.Join(source, "meta.json"), make([]byte, 1<<20+1))
		}, "read skill meta.json: file exceeds byte limit", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := newSource(t)
			write(t, filepath.Join(source, "meta.json"), []byte("{}"))
			tc.build(t, source)
			src, err := Read(context.Background(), source, []string{"tools"}, []string{"meta.json"})
			if tc.want != "" {
				if err == nil || err.Error() != tc.want {
					t.Fatalf("error = %v, want %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			sidecar, err := os.ReadFile(filepath.Join(source, "meta.json"))
			if err != nil || !bytes.Equal(src.Sidecar("meta.json"), sidecar) {
				t.Fatalf("sidecar = %d bytes, want %d (%v)", len(src.Sidecar("meta.json")), len(sidecar), err)
			}
			expansion, err := src.Expand(t.TempDir(), "neutral-skills")
			if err != nil || len(expansion.Files) != tc.files {
				t.Fatalf("files = %d (%v), want %d", len(expansion.Files), err, tc.files)
			}
			if _, err := os.Lstat(filepath.Join(expansion.Dir, "meta.json")); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("sidecar was expanded: %v", err)
			}
		})
	}
}

func TestReadRejects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source func(t *testing.T, source string) string
		want   string
	}{
		{"empty path", func(*testing.T, string) string { return "" }, `skill directory must be an absolute path, got ""`},
		{"relative path to a valid directory", func(t *testing.T, source string) string {
			t.Chdir(filepath.Dir(source))
			return filepath.Base(source)
		}, `skill directory must be an absolute path, got "skills"`},
		{"missing directory", func(_ *testing.T, source string) string { return filepath.Join(source, "absent") }, "open skill directory: "},
		{"missing subtree", func(t *testing.T, source string) string {
			if err := os.RemoveAll(filepath.Join(source, "tools")); err != nil {
				t.Fatal(err)
			}
			return source
		}, "skill directory tools: "},
		{"symlinked subtree", func(t *testing.T, source string) string {
			if err := os.Rename(filepath.Join(source, "tools"), filepath.Join(source, "tools-real")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("tools-real", filepath.Join(source, "tools")); err != nil {
				t.Fatal(err)
			}
			return source
		}, "skill entry tools is not a directory"},
		{"subtree without SKILL.md", func(t *testing.T, source string) string {
			if err := os.Rename(filepath.Join(source, "tools", "SKILL.md"), filepath.Join(source, "tools", "skill.md")); err != nil {
				t.Fatal(err)
			}
			return source
		}, "skill directory tools has no SKILL.md"},
		{"SKILL.md only in a nested directory", func(t *testing.T, source string) string {
			if err := os.Remove(filepath.Join(source, "tools", "SKILL.md")); err != nil {
				t.Fatal(err)
			}
			write(t, filepath.Join(source, "tools", "sub", "SKILL.md"), []byte("# nested\n"))
			return source
		}, "skill directory tools has no SKILL.md"},
		{"symlinked file", func(t *testing.T, source string) string {
			write(t, filepath.Join(source, "outside.md"), []byte("outside\n"))
			if err := os.Symlink(filepath.Join(source, "outside.md"), filepath.Join(source, "tools", "linked.md")); err != nil {
				t.Fatal(err)
			}
			return source
		}, "skill entry tools/linked.md is not a regular file or directory"},
		{"fifo", func(t *testing.T, source string) string {
			if err := syscall.Mkfifo(filepath.Join(source, "tools", "pipe"), 0600); err != nil {
				t.Fatal(err)
			}
			return source
		}, "skill entry tools/pipe is not a regular file or directory"},
		{"name with an invisible format character", func(t *testing.T, source string) string {
			write(t, filepath.Join(source, "tools", "a\u202emd.txt"), []byte("x"))
			return source
		}, "not printable UTF-8"},
		{"name with a control character", func(t *testing.T, source string) string {
			write(t, filepath.Join(source, "tools", "a\nb.md"), []byte("x"))
			return source
		}, "not printable UTF-8"},
		{"missing sidecar", func(t *testing.T, source string) string {
			if err := os.Remove(filepath.Join(source, "meta.json")); err != nil {
				t.Fatal(err)
			}
			return source
		}, "read skill meta.json: "},
		{"symlinked sidecar", func(t *testing.T, source string) string {
			if err := os.Rename(filepath.Join(source, "meta.json"), filepath.Join(source, "meta-real.json")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("meta-real.json", filepath.Join(source, "meta.json")); err != nil {
				t.Fatal(err)
			}
			return source
		}, "read skill meta.json: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := newSource(t)
			write(t, filepath.Join(source, "meta.json"), []byte("{}"))
			_, err := Read(context.Background(), tc.source(t, source), []string{"tools"}, []string{"meta.json"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestReadDeadline(t *testing.T) {
	if readTimeout != time.Minute {
		t.Fatalf("default read deadline = %s, want 1m", readTimeout)
	}
	source := placeholder(t)
	old := readTimeout
	readTimeout = 50 * time.Millisecond
	t.Cleanup(func() { readTimeout = old })
	_, err := Read(context.Background(), source, []string{"tools"}, nil)
	want := "reading the skill directory took longer than 50ms (a cloud placeholder may not be downloaded; use a local copy)"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestReadOuterCancellation(t *testing.T) {
	source := placeholder(t)
	stop := errors.New("operator stopped the run")
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel(stop) }()
	if _, err := Read(ctx, source, []string{"tools"}, nil); !errors.Is(err, stop) {
		t.Fatalf("error = %v, want the outer cause", err)
	}
}
