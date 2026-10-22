// Package skilldir copies the declared subtrees of a private skill directory
// outside the repository into a run, with a recorded digest. The caller
// names the subtrees, the expansion directory and any record it commits.
//
// A read abandoned at its deadline keeps its open file descriptor and the
// bytes it has read, up to the read limits, until the blocked call returns.
package skilldir

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing/fstest"
	"time"
	"unicode"
	"unicode/utf8"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/reportresource"
)

const (
	fileLimit  = 1 << 20
	totalLimit = 8 << 20
	fileCount  = 128
)

// readTimeout is a variable only so tests can shorten it.
var readTimeout = time.Minute

// Source is the in-memory copy of the declared parts of a skill directory.
type Source struct {
	files    map[string][]byte
	sidecars map[string][]byte
}

// File is one expanded file; Path is slash-separated and relative to the
// expansion directory.
type File struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// Expansion is the run-owned copy. Files are in byte order of Path and Digest
// is the sha256 of the lines "<sha256>  <path>\n" in that order.
type Expansion struct {
	Dir    string
	Digest string
	Files  []File
}

// Read copies each subtree in dirs and each file in sidecars from the
// absolute directory source, under a deadline. Before any filesystem access
// it rejects a relative source, an empty dirs, a dir that is not a single
// path segment or is listed twice, and a sidecar inside a declared subtree.
// Every subtree must hold a SKILL.md. Entries whose name starts with "." are
// skipped; anything else that is not a regular file or directory, or has a
// name that is not printable UTF-8, rejects the source. Subtree files share
// the 128-file and 8 MiB budgets and each is at most 1 MiB; sidecars have the
// 1 MiB bound only, are not counted in the budgets and, lying outside every
// subtree, are never expanded. Each file is read with contract.ReadStable;
// files are not a consistent snapshot of each other.
func Read(ctx context.Context, source string, dirs, sidecars []string) (Source, error) {
	if !filepath.IsAbs(source) {
		return Source{}, fmt.Errorf("skill directory must be an absolute path, got %q", source)
	}
	if len(dirs) == 0 {
		return Source{}, errors.New("no skill subtrees declared")
	}
	seen := map[string]bool{}
	for _, dir := range dirs {
		if !segment(dir) {
			return Source{}, fmt.Errorf("skill subtree %q must be a single path segment", dir)
		}
		if seen[dir] {
			return Source{}, fmt.Errorf("skill subtree %q is listed twice", dir)
		}
		seen[dir] = true
	}
	for _, name := range sidecars {
		first, _, _ := strings.Cut(filepath.ToSlash(filepath.Clean(name)), "/")
		if seen[first] {
			return Source{}, fmt.Errorf("skill sidecar %q lies inside subtree %q", name, first)
		}
	}
	return Abandon(ctx, readTimeout, fmt.Errorf("reading the skill directory took longer than %s (a cloud placeholder may not be downloaded; use a local copy)", readTimeout), func(ctx context.Context) (Source, error) {
		return read(ctx, source, dirs, sidecars)
	})
}

// segment reports whether name is one path element that stays where it is.
func segment(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsRune(name, '/') && !strings.ContainsRune(name, filepath.Separator)
}

// Sidecar returns the bytes of a sidecar named to Read.
func (s Source) Sidecar(name string) []byte { return s.sidecars[name] }

// Expand writes the subtree files into a fresh runDir/name with
// reportresource.ExtractFresh. Empty directories are not reproduced. name
// must be a single path segment.
func (s Source) Expand(runDir, name string) (Expansion, error) {
	if !segment(name) {
		return Expansion{}, fmt.Errorf("skill expansion name %q must be a single path segment", name)
	}
	tree := fstest.MapFS{}
	files := []File{}
	var lines strings.Builder
	names := make([]string, 0, len(s.files))
	for path := range s.files {
		names = append(names, path)
	}
	sort.Strings(names)
	for _, path := range names {
		sum := sha256.Sum256(s.files[path])
		file := File{Path: path, SHA256: hex.EncodeToString(sum[:])}
		files = append(files, file)
		fmt.Fprintf(&lines, "%s  %s\n", file.SHA256, file.Path)
		tree[path] = &fstest.MapFile{Data: s.files[path]}
	}
	sum := sha256.Sum256([]byte(lines.String()))
	dir, err := reportresource.ExtractFresh(runDir, name, tree)
	if err != nil {
		return Expansion{}, err
	}
	return Expansion{Dir: dir, Digest: hex.EncodeToString(sum[:]), Files: files}, nil
}

// Abandon bounds read. A directory on a synced cloud drive can block in
// open or read on a placeholder that is not downloaded, which no context
// check interrupts, so read runs on its own goroutine and is abandoned at the
// deadline. The goroutine exits when its blocked call returns and may
// outlive the run, so read must only read and never touch the run; that is
// the caller's obligation, which Abandon does not enforce. On abandonment the
// error is the cause of ctx: the outer cause, or cause at the deadline.
func Abandon[T any](ctx context.Context, timeout time.Duration, cause error, read func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, timeout, cause)
	defer cancel()
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() {
		v, err := read(ctx)
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.value, r.err
	case <-ctx.Done():
		var zero T
		return zero, context.Cause(ctx)
	}
}

func read(ctx context.Context, source string, dirs, sidecars []string) (Source, error) {
	root, err := os.OpenRoot(source)
	if err != nil {
		return Source{}, fmt.Errorf("open skill directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	files := map[string][]byte{}
	var total int64
	for _, dir := range dirs {
		// WalkDir follows a symlinked start directory, so check it first.
		info, err := root.Lstat(dir)
		if err != nil {
			return Source{}, fmt.Errorf("skill directory %s: %w", dir, err)
		}
		if !info.IsDir() {
			return Source{}, fmt.Errorf("skill entry %s is not a directory", dir)
		}
		err = fs.WalkDir(root.FS(), dir, func(name string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			// Editor and sync clients leave dot files; they are not skill content.
			if name != dir && strings.HasPrefix(entry.Name(), ".") {
				if entry.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			if !utf8.ValidString(name) || strings.IndexFunc(name, func(r rune) bool { return !unicode.IsGraphic(r) }) >= 0 {
				return fmt.Errorf("skill entry %q has a name that is not printable UTF-8", name)
			}
			switch {
			case entry.IsDir():
				return nil
			case !entry.Type().IsRegular():
				return fmt.Errorf("skill entry %s is not a regular file or directory (%s)", name, entry.Type())
			case len(files) >= fileCount:
				return fmt.Errorf("skill directory has more than %d files", fileCount)
			}
			raw, err := contract.ReadStable(ctx, root, filepath.FromSlash(name), fileLimit)
			if err != nil {
				return fmt.Errorf("skill file %s: %w", name, err)
			}
			if total += int64(len(raw)); total > totalLimit {
				return fmt.Errorf("skill files exceed %d bytes", totalLimit)
			}
			files[name] = raw
			return nil
		})
		if err != nil {
			return Source{}, err
		}
		if _, ok := files[dir+"/SKILL.md"]; !ok {
			return Source{}, fmt.Errorf("skill directory %s has no SKILL.md", dir)
		}
	}
	extra := map[string][]byte{}
	for _, name := range sidecars {
		raw, err := contract.ReadStable(ctx, root, name, fileLimit)
		if err != nil {
			return Source{}, fmt.Errorf("read skill %s: %w", name, err)
		}
		extra[name] = raw
	}
	return Source{files, extra}, nil
}
