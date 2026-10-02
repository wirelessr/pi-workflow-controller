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
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing/fstest"
	"time"

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
	skillFileLimit     = 1 << 20
	skillTotalLimit    = 8 << 20
	skillFileCount     = 128
	upstreamFileLimit  = 4 << 20
	manifestLimit      = 1 << 20
	manifestSchema     = "pwc-triage-skills/manifest/v1"
	skillReadTimeout   = time.Minute
	skillsExtractName  = "triage-skills"
	staleGapID         = "derived-skills-stale"
	uncheckedGapID     = "derived-skills-staleness-unchecked"
	stalenessCurrent   = "current"
	stalenessStale     = "stale"
	stalenessUnchecked = "unchecked"
)

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Skills is the run-owned expansion of the private skill directory.
type Skills struct {
	Dir       string
	Record    contract.Ref
	Staleness string
}

// Entry is the absolute SKILL.md path a Step request names for a role.
func (s Skills) Entry(role string) string { return filepath.Join(s.Dir, role, "SKILL.md") }

type SkillsRecord struct {
	Digest    string      `json:"digest"`
	Files     []SkillFile `json:"files"`
	Staleness string      `json:"staleness"`
	Gaps      []Gap       `json:"gaps"`
}

type SkillFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// PrepareSkills copies the allow-listed skill subtrees of source into the
// run, compares the manifest's upstream hashes with the current upstream
// files, and commits a Controller record of the expanded set and the result.
// Staleness never blocks the run; an unreadable or malformed source does.
// The Controller copies files and digests bytes only; it never interprets
// skill text.
func PrepareSkills(ctx context.Context, r *engine.Run, scope *engine.Scope, source string) (Skills, error) {
	if source == "" {
		return Skills{}, errors.New("PWC_TRIAGE_SKILLS_DIR is not set: the triage workflow needs its private skill directory")
	}
	if !filepath.IsAbs(source) {
		return Skills{}, fmt.Errorf("PWC_TRIAGE_SKILLS_DIR must be an absolute path, got %q", source)
	}
	read, err := readSkills(ctx, source)
	if err != nil {
		return Skills{}, err
	}
	tree := fstest.MapFS{}
	record := SkillsRecord{Files: []SkillFile{}, Staleness: read.staleness, Gaps: read.gaps}
	var lines strings.Builder
	names := make([]string, 0, len(read.files))
	for name := range read.files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		sum := sha256.Sum256(read.files[name])
		file := SkillFile{Path: name, SHA256: hex.EncodeToString(sum[:])}
		record.Files = append(record.Files, file)
		fmt.Fprintf(&lines, "%s  %s\n", file.SHA256, file.Path)
		tree[name] = &fstest.MapFile{Data: read.files[name], Mode: 0600}
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
	return Skills{Dir: dir, Record: ref, Staleness: read.staleness}, nil
}

type skillSource struct {
	files     map[string][]byte
	staleness string
	gaps      []Gap
}

// readSkills bounds the whole read. A directory on a synced cloud drive can
// block in open or read on a placeholder that is not downloaded, which no
// context check interrupts, so the read runs on its own goroutine and is
// abandoned at the deadline; that goroutine may outlive the run.
func readSkills(ctx context.Context, source string) (skillSource, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, skillReadTimeout, fmt.Errorf("reading the skill directory took longer than %s (a cloud placeholder may not be downloaded; use a local copy)", skillReadTimeout))
	defer cancel()
	type result struct {
		source skillSource
		err    error
	}
	done := make(chan result, 1)
	go func() {
		s, err := readSkillSource(ctx, source)
		done <- result{s, err}
	}()
	select {
	case r := <-done:
		return r.source, r.err
	case <-ctx.Done():
		return skillSource{}, context.Cause(ctx)
	}
}

func readSkillSource(ctx context.Context, source string) (skillSource, error) {
	root, err := os.OpenRoot(source)
	if err != nil {
		return skillSource{}, fmt.Errorf("open skill directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	s := skillSource{files: map[string][]byte{}}
	var total int64
	for _, dir := range skillDirs {
		if err := readSkillTree(ctx, root, dir, s.files, &total); err != nil {
			return skillSource{}, err
		}
	}
	raw, err := contract.ReadStable(ctx, root, "manifest.json", manifestLimit)
	if err != nil {
		return skillSource{}, fmt.Errorf("read skill manifest.json: %w", err)
	}
	s.staleness, s.gaps, err = checkUpstream(ctx, raw)
	return s, err
}

func readSkillTree(ctx context.Context, root *os.Root, dir string, files map[string][]byte, total *int64) error {
	info, err := root.Lstat(dir)
	if err != nil {
		return fmt.Errorf("skill directory %s: %w", dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("skill entry %s is not a directory", dir)
	}
	f, err := root.Open(dir)
	if err != nil {
		return fmt.Errorf("skill directory %s: %w", dir, err)
	}
	entries, err := f.ReadDir(-1)
	err = errors.Join(err, f.Close())
	if err != nil {
		return fmt.Errorf("skill directory %s: %w", dir, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		name := path.Join(dir, entry.Name())
		// Editor and sync clients leave dot files; they are not skill content.
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		switch {
		case entry.IsDir():
			if err := readSkillTree(ctx, root, name, files, total); err != nil {
				return err
			}
		case entry.Type().IsRegular():
			if len(files) >= skillFileCount {
				return fmt.Errorf("skill directory has more than %d files", skillFileCount)
			}
			raw, err := contract.ReadStable(ctx, root, filepath.FromSlash(name), skillFileLimit)
			if err != nil {
				return fmt.Errorf("skill file %s: %w", name, err)
			}
			if *total += int64(len(raw)); *total > skillTotalLimit {
				return fmt.Errorf("skill files exceed %d bytes", skillTotalLimit)
			}
			files[name] = raw
		default:
			return fmt.Errorf("skill entry %s is not a regular file or directory (%s)", name, entry.Type())
		}
	}
	return nil
}

// checkUpstream reads only the manifest fields the Controller contract
// names: schema, upstream_root, upstream[].path and upstream[].sha256.
func checkUpstream(ctx context.Context, raw []byte) (string, []Gap, error) {
	var m struct {
		Schema       string `json:"schema"`
		UpstreamRoot string `json:"upstream_root"`
		Upstream     []struct {
			Path   string `json:"path"`
			SHA256 string `json:"sha256"`
		} `json:"upstream"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", nil, fmt.Errorf("skill manifest.json: %w", err)
	}
	if m.Schema != manifestSchema {
		return "", nil, fmt.Errorf("skill manifest.json: schema %q, want %q", m.Schema, manifestSchema)
	}
	upstream := m.UpstreamRoot
	if rest, ok := strings.CutPrefix(upstream, "~/"); ok {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", nil, fmt.Errorf("skill manifest.json upstream_root: %w", err)
		}
		upstream = filepath.Join(home, rest)
	}
	if !filepath.IsAbs(upstream) {
		return "", nil, fmt.Errorf("skill manifest.json: upstream_root %q is not absolute or ~/-relative", m.UpstreamRoot)
	}
	if len(m.Upstream) == 0 {
		return "", nil, errors.New("skill manifest.json: upstream is empty")
	}
	for i, u := range m.Upstream {
		if !fs.ValidPath(u.Path) || u.Path == "." {
			return "", nil, fmt.Errorf("skill manifest.json: upstream[%d].path %q must be a slash-separated relative path without .. segments", i, u.Path)
		}
		if !sha256Hex.MatchString(u.SHA256) {
			return "", nil, fmt.Errorf("skill manifest.json: upstream[%d].sha256 is not 64 lowercase hex characters", i)
		}
	}
	root, err := os.OpenRoot(upstream)
	if err != nil {
		return stalenessUnchecked, []Gap{{ID: uncheckedGapID, Text: "The upstream skill directory is not readable on this machine, so the derived skills were not checked for staleness."}}, nil
	}
	defer func() { _ = root.Close() }()
	var changed, unreadable []string
	for _, u := range m.Upstream {
		current, err := contract.ReadStable(ctx, root, filepath.FromSlash(u.Path), upstreamFileLimit)
		if err := context.Cause(ctx); err != nil {
			return "", nil, err
		}
		if err != nil {
			unreadable = append(unreadable, u.Path)
			continue
		}
		if sum := sha256.Sum256(current); hex.EncodeToString(sum[:]) != u.SHA256 {
			changed = append(changed, u.Path)
		}
	}
	var gaps []Gap
	staleness := stalenessCurrent
	if len(unreadable) > 0 {
		staleness = stalenessUnchecked
		gaps = append(gaps, Gap{ID: uncheckedGapID, Text: "These upstream skill files could not be read, so the derived skills were not checked against them: " + strings.Join(unreadable, ", ")})
	}
	if len(changed) > 0 {
		staleness = stalenessStale
		gaps = append(gaps, Gap{ID: staleGapID, Text: "The derived skills may be stale: these upstream skill files changed since they were derived: " + strings.Join(changed, ", ")})
	}
	if gaps == nil {
		gaps = []Gap{}
	}
	return staleness, gaps, nil
}
