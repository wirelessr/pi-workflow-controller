package triagev2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing/fstest"
	"time"
	"unicode"
	"unicode/utf8"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/reportresource"
	"pi-workflow-controller/internal/engine"
)

const SkillsSchema = "triage.skills.v1"

// skillDirs are the only subtrees Agents read. Sync material, the manifest,
// the internal-term list and the README stay outside the run: they hold
// upstream text that tells an Agent to delegate or write back.
var skillDirs = []string{"core", "intake", "identity", "investigator", "steward", "validator"}

const (
	skillFileLimit    = 1 << 20
	skillTotalLimit   = 8 << 20
	skillFileCount    = 128
	upstreamFileLimit = 4 << 20
	manifestSchema    = "pwc-triage-skills/manifest/v1"
	skillsExtractName = "triage-skills"
	staleGapID        = "derived-skills-stale"
	uncheckedGapID    = "derived-skills-staleness-unchecked"
	gapPathLimit      = 20
)

// Deadlines and the directory opener are variables only so tests can
// shorten them and simulate a blocking filesystem.
var (
	skillReadTimeout    = time.Minute
	upstreamReadTimeout = 30 * time.Second
	openRoot            = os.OpenRoot
)

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Skills is the run-owned expansion of the private skill directory. Record
// is the committed Controller record; branch on it, not on local state.
type Skills struct {
	Dir    string
	Record contract.Ref
}

// Entry is the absolute SKILL.md path a Step request names for a role.
func (s Skills) Entry(role string) string { return filepath.Join(s.Dir, role, "SKILL.md") }

// SkillsRecord lists the expanded files and their digest, so a run's skill
// version can be compared with another run's. Staleness appears only as gaps.
type SkillsRecord struct {
	Digest string      `json:"digest"`
	Files  []SkillFile `json:"files"`
	Gaps   []Gap       `json:"gaps"`
}

type SkillFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// PrepareSkills copies the allow-listed skill subtrees of source into the
// run, compares the manifest's upstream hashes with the current upstream
// files, and commits a Controller record of the expanded set and any
// staleness gap. Staleness never blocks the run; an unreadable, slow,
// malformed or incomplete source does. The Controller copies and digests
// bytes only; it never interprets skill text.
func PrepareSkills(ctx context.Context, r *engine.Run, scope *engine.Scope, source string) (Skills, error) {
	if source == "" {
		return Skills{}, errors.New("PWC_TRIAGE_SKILLS_DIR is not set: the triage workflow needs its private skill directory")
	}
	if !filepath.IsAbs(source) {
		return Skills{}, fmt.Errorf("PWC_TRIAGE_SKILLS_DIR must be an absolute path, got %q", source)
	}
	read, err := abandonable(ctx, skillReadTimeout, fmt.Errorf("reading the skill directory took longer than %s (a cloud placeholder may not be downloaded; use a local copy)", skillReadTimeout), func(ctx context.Context) (skillSource, error) {
		return readSkillSource(ctx, source)
	})
	if err != nil {
		return Skills{}, err
	}
	files := read.files
	upstream, err := parseManifest(read.manifest)
	if err != nil {
		return Skills{}, err
	}
	gaps, err := checkUpstream(ctx, upstream)
	if err != nil {
		return Skills{}, err
	}
	tree := fstest.MapFS{}
	record := SkillsRecord{Files: []SkillFile{}, Gaps: gaps}
	var lines strings.Builder
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		sum := sha256.Sum256(files[name])
		file := SkillFile{Path: name, SHA256: hex.EncodeToString(sum[:])}
		record.Files = append(record.Files, file)
		fmt.Fprintf(&lines, "%s  %s\n", file.SHA256, file.Path)
		tree[name] = &fstest.MapFile{Data: files[name]}
	}
	sum := sha256.Sum256([]byte(lines.String()))
	record.Digest = hex.EncodeToString(sum[:])
	dir, err := reportresource.ExtractFresh(r.Dir(), skillsExtractName, tree)
	if err != nil {
		return Skills{}, err
	}
	ref, err := scope.Attach(ctx, engine.AttachSpec{Key: "skills", Output: contract.Spec{SchemaID: SkillsSchema}, Data: record})
	if err != nil {
		return Skills{}, err
	}
	return Skills{Dir: dir, Record: ref}, nil
}

type skillSource struct {
	files    map[string][]byte
	manifest []byte
}

// abandonable bounds read. A directory on a synced cloud drive can block in
// open or read on a placeholder that is not downloaded, which no context
// check interrupts, so read runs on its own goroutine and is abandoned at the
// deadline. That goroutine only reads, never touches the run, and exits when
// its blocked call returns; it may outlive the run.
func abandonable[T any](ctx context.Context, timeout time.Duration, cause error, read func(context.Context) (T, error)) (T, error) {
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

func readSkillSource(ctx context.Context, source string) (skillSource, error) {
	root, err := openRoot(source)
	if err != nil {
		return skillSource{}, fmt.Errorf("open skill directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	files := map[string][]byte{}
	var total int64
	for _, dir := range skillDirs {
		// WalkDir follows a symlinked start directory, so check it first.
		info, err := root.Lstat(dir)
		if err != nil {
			return skillSource{}, fmt.Errorf("skill directory %s: %w", dir, err)
		}
		if !info.IsDir() {
			return skillSource{}, fmt.Errorf("skill entry %s is not a directory", dir)
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
			case len(files) >= skillFileCount:
				return fmt.Errorf("skill directory has more than %d files", skillFileCount)
			}
			raw, err := contract.ReadStable(ctx, root, filepath.FromSlash(name), skillFileLimit)
			if err != nil {
				return fmt.Errorf("skill file %s: %w", name, err)
			}
			if total += int64(len(raw)); total > skillTotalLimit {
				return fmt.Errorf("skill files exceed %d bytes", skillTotalLimit)
			}
			files[name] = raw
			return nil
		})
		if err != nil {
			return skillSource{}, err
		}
		if _, ok := files[dir+"/SKILL.md"]; !ok {
			return skillSource{}, fmt.Errorf("skill directory %s has no SKILL.md", dir)
		}
	}
	manifest, err := contract.ReadStable(ctx, root, "manifest.json", skillFileLimit)
	if err != nil {
		return skillSource{}, fmt.Errorf("read skill manifest.json: %w", err)
	}
	return skillSource{files, manifest}, nil
}

type upstreamFiles struct {
	root  string
	paths []string
	sums  []string
}

// parseManifest reads only the fields the Controller contract names, by
// exact key: schema, upstream_root, upstream[].path and upstream[].sha256.
func parseManifest(raw []byte) (upstreamFiles, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return upstreamFiles{}, fmt.Errorf("skill manifest.json: %w", err)
	}
	var schema, root string
	var entries []map[string]json.RawMessage
	for _, field := range []struct {
		key    string
		target any
	}{{"schema", &schema}, {"upstream_root", &root}, {"upstream", &entries}} {
		if err := json.Unmarshal(top[field.key], field.target); err != nil {
			return upstreamFiles{}, fmt.Errorf("skill manifest.json: field %s: %w", field.key, err)
		}
	}
	if schema != manifestSchema {
		return upstreamFiles{}, fmt.Errorf("skill manifest.json: schema %q, want %q", schema, manifestSchema)
	}
	if len(entries) == 0 {
		return upstreamFiles{}, errors.New("skill manifest.json: upstream is empty")
	}
	u := upstreamFiles{root: root}
	seen := map[string]bool{}
	for i, entry := range entries {
		var path, sum string
		if json.Unmarshal(entry["path"], &path) != nil || !fs.ValidPath(path) || path == "." {
			return upstreamFiles{}, fmt.Errorf("skill manifest.json: upstream[%d].path must be a slash-separated relative path without .. segments", i)
		}
		if json.Unmarshal(entry["sha256"], &sum) != nil || !sha256Hex.MatchString(sum) {
			return upstreamFiles{}, fmt.Errorf("skill manifest.json: upstream[%d].sha256 must be 64 lowercase hex characters", i)
		}
		if seen[path] {
			return upstreamFiles{}, fmt.Errorf("skill manifest.json: upstream[%d].path %q is listed twice", i, path)
		}
		seen[path] = true
		u.paths, u.sums = append(u.paths, path), append(u.sums, sum)
	}
	if !strings.HasPrefix(root, "~/") && !filepath.IsAbs(root) {
		return upstreamFiles{}, fmt.Errorf("skill manifest.json: upstream_root %q is not absolute or ~/-relative", root)
	}
	return u, nil
}

// checkUpstream compares hashes and existence only. Anything that keeps the
// comparison from running (another machine, no HOME, unreadable or slow
// files) is recorded as an unchecked gap; it never fails the run.
func checkUpstream(ctx context.Context, u upstreamFiles) ([]Gap, error) {
	unchecked := func(reason string) []Gap {
		return []Gap{{ID: uncheckedGapID, Text: "The derived skills were not checked for staleness: " + reason}}
	}
	root := u.root
	if rest, ok := strings.CutPrefix(root, "~/"); ok {
		home, err := os.UserHomeDir()
		if err != nil {
			return unchecked("the home directory is unknown, so the ~/ upstream root cannot be resolved."), nil
		}
		root = filepath.Join(home, rest)
	}
	type result struct{ changed, unreadable []string }
	r, err := abandonable(ctx, upstreamReadTimeout, errors.New("upstream read timed out"), func(ctx context.Context) (result, error) {
		var r result
		dir, err := openRoot(root)
		if err != nil {
			return r, errUpstreamRoot
		}
		defer func() { _ = dir.Close() }()
		for i, path := range u.paths {
			current, err := contract.ReadStable(ctx, dir, filepath.FromSlash(path), upstreamFileLimit)
			if cause := context.Cause(ctx); cause != nil {
				return r, cause
			}
			switch {
			case errors.Is(err, fs.ErrNotExist):
				r.changed = append(r.changed, path+" (removed)")
			case err != nil:
				r.unreadable = append(r.unreadable, path+" ("+err.Error()+")")
			case sha256Sum(current) != u.sums[i]:
				r.changed = append(r.changed, path)
			}
		}
		return r, nil
	})
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	switch {
	case errors.Is(err, errUpstreamRoot):
		return unchecked("the upstream skill directory is not readable on this machine."), nil
	case err != nil:
		return unchecked(fmt.Sprintf("reading the upstream skill files took longer than %s.", upstreamReadTimeout)), nil
	}
	gaps := []Gap{}
	if len(r.unreadable) > 0 {
		gaps = append(gaps, unchecked("these upstream skill files could not be read: " + listPaths(r.unreadable))[0])
	}
	if len(r.changed) > 0 {
		gaps = append(gaps, Gap{ID: staleGapID, Text: "The derived skills may be stale: these upstream skill files changed since they were derived: " + listPaths(r.changed)})
	}
	return gaps, nil
}

var errUpstreamRoot = errors.New("upstream root not readable")

func sha256Sum(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }

func listPaths(paths []string) string {
	if len(paths) > gapPathLimit {
		return strings.Join(paths[:gapPathLimit], ", ") + fmt.Sprintf(" and %d more", len(paths)-gapPathLimit)
	}
	return strings.Join(paths, ", ")
}
