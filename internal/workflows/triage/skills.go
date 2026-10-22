package triage

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
	"strings"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/skilldir"
	"pi-workflow-controller/internal/engine"
)

const SkillsSchema = "triage.skills.v1"

// skillDirs are the only subtrees Agents read. Sync material, the manifest,
// the internal-term list and the README stay outside the run: they hold
// upstream text that tells an Agent to delegate or write back.
var skillDirs = []string{"core", "intake", "identity", "investigator", "steward", "validator"}

const (
	upstreamFileLimit = 4 << 20
	manifestSchema    = "pwc-triage-skills/manifest/v1"
	skillsExtractName = "triage-skills"
	staleGapID        = "derived-skills-stale"
	uncheckedGapID    = "derived-skills-staleness-unchecked"
	gapPathLimit      = 20
)

// The upstream deadline and the directory opener are variables only so tests
// can shorten it and simulate a blocking filesystem.
var (
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
	Digest string          `json:"digest"`
	Files  []skilldir.File `json:"files"`
	Gaps   []Gap           `json:"gaps"`
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
	src, err := skilldir.Read(ctx, source, skillDirs, []string{"manifest.json"})
	if err != nil {
		return Skills{}, err
	}
	upstream, err := parseManifest(src.Sidecar("manifest.json"))
	if err != nil {
		return Skills{}, err
	}
	gaps, err := checkUpstream(ctx, upstream)
	if err != nil {
		return Skills{}, err
	}
	expansion, err := src.Expand(r.Dir(), skillsExtractName)
	if err != nil {
		return Skills{}, err
	}
	record := SkillsRecord{Digest: expansion.Digest, Files: expansion.Files, Gaps: gaps}
	ref, err := scope.Attach(ctx, engine.AttachSpec{Key: "skills", Output: contract.Spec{SchemaID: SkillsSchema}, Data: record})
	if err != nil {
		return Skills{}, err
	}
	return Skills{Dir: expansion.Dir, Record: ref}, nil
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
	r, err := skilldir.Abandon(ctx, upstreamReadTimeout, errors.New("upstream read timed out"), func(ctx context.Context) (result, error) {
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
