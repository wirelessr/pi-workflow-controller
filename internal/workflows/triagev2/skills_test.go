package triagev2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// noRuntime stands in for Pi: skill preparation never opens a session.
type noRuntime struct{}

func (noRuntime) Start(context.Context, runtime.SessionSpec) (runtime.Session, error) {
	return nil, errors.New("skill preparation must not start Pi")
}

type skillFixture struct {
	source, upstream string
	upstreamFile     []byte
}

func newSkillFixture(t *testing.T) skillFixture {
	t.Helper()
	dir := t.TempDir()
	f := skillFixture{source: filepath.Join(dir, "skills"), upstream: filepath.Join(dir, "upstream"), upstreamFile: []byte("upstream rules v1\n")}
	write := func(path string, data []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, role := range skillDirs {
		write(filepath.Join(f.source, role, "SKILL.md"), []byte("# "+role+"\n"))
	}
	write(filepath.Join(f.source, "core", "references", "methods.md"), []byte("methods\n"))
	write(filepath.Join(f.source, "core", ".DS_Store"), []byte("finder"))
	write(filepath.Join(f.source, "sync", "upstream-snapshot", "rules.md"), []byte("delegate to a helper agent\n"))
	write(filepath.Join(f.source, "README.md"), []byte("readme\n"))
	write(filepath.Join(f.source, "denylist.txt"), []byte("[hard]\nsecret\n"))
	write(filepath.Join(f.upstream, "work", "rules", "SKILL.md"), f.upstreamFile)
	f.writeManifest(t, f.upstream, sha(f.upstreamFile))
	return f
}

func (f skillFixture) writeManifest(t *testing.T, root, sum string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"schema": manifestSchema, "upstream_root": root, "upstream": []any{map[string]any{"path": "work/rules/SKILL.md", "sha256": sum}}, "informational": true})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.source, "manifest.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func sha(raw []byte) string { s := sha256.Sum256(raw); return hex.EncodeToString(s[:]) }

func runSkills(t *testing.T, prepare func(context.Context, *engine.Run) error) engine.Report {
	t.Helper()
	return runSkillsPolicy(t, engine.DefaultRunPolicy(), prepare)
}

func runSkillsPolicy(t *testing.T, policy engine.RunPolicy, prepare func(context.Context, *engine.Run) error) engine.Report {
	t.Helper()
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	r, err := engine.New(context.Background(), engine.Definition{Name: "skills-fixture", Version: "1", Policy: policy, Execute: func(ctx context.Context, run *engine.Run, _ engine.Input) (engine.Result, error) {
		return engine.Result{}, prepare(ctx, run)
	}}, engine.Input{Prompt: "skills fixture", LaunchCWD: base}, engine.Options{BaseDir: base, Schemas: registry, Runtime: noRuntime{}})
	if err != nil {
		t.Fatal(err)
	}
	return r.Execute()
}

func TestPrepareSkills(t *testing.T) {
	f := newSkillFixture(t)
	var skills Skills
	var record SkillsRecord
	var attached bool
	var runDir string
	report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
		var err error
		runDir = r.Dir()
		if skills, err = PrepareSkills(ctx, r, r.Root(), f.source); err != nil {
			return err
		}
		attached = r.ControllerAttached(skills.Record)
		record, err = engine.Decode[SkillsRecord](ctx, r, skills.Record)
		return err
	})
	if report.Outcome != engine.Succeeded {
		t.Fatalf("outcome = %s: %v", report.Outcome, report.Failure)
	}
	if !attached || len(record.Gaps) != 0 {
		t.Fatalf("skills = %+v record = %+v attached=%t", skills, record, attached)
	}
	if skills.Dir != filepath.Join(runDir, skillsExtractName) || skills.Entry("steward") != filepath.Join(skills.Dir, "steward", "SKILL.md") {
		t.Fatalf("expansion dir = %s", skills.Dir)
	}
	var paths []string
	var lines strings.Builder
	for _, file := range record.Files {
		paths = append(paths, file.Path)
		raw, err := os.ReadFile(filepath.Join(skills.Dir, filepath.FromSlash(file.Path)))
		if err != nil || sha(raw) != file.SHA256 {
			t.Fatalf("expanded %s differs from its recorded digest: %v", file.Path, err)
		}
		lines.WriteString(file.SHA256 + "  " + file.Path + "\n")
	}
	want := "core/SKILL.md core/references/methods.md identity/SKILL.md intake/SKILL.md investigator/SKILL.md steward/SKILL.md validator/SKILL.md"
	if strings.Join(paths, " ") != want || record.Digest != sha([]byte(lines.String())) {
		t.Fatalf("recorded files = %v digest = %s", paths, record.Digest)
	}
	for _, excluded := range []string{"sync", "README.md", "denylist.txt", "manifest.json", "core/.DS_Store"} {
		if _, err := os.Lstat(filepath.Join(skills.Dir, excluded)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was expanded into the run: %v", excluded, err)
		}
	}
}

func TestPrepareSkillsStaleness(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*testing.T, skillFixture)
		gapIDs string
		text   string
	}{
		{"upstream changed", func(t *testing.T, f skillFixture) {
			if err := os.WriteFile(filepath.Join(f.upstream, "work", "rules", "SKILL.md"), []byte("upstream rules v2\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}, staleGapID, "work/rules/SKILL.md"},
		{"upstream file removed", func(t *testing.T, f skillFixture) {
			if err := os.Remove(filepath.Join(f.upstream, "work", "rules", "SKILL.md")); err != nil {
				t.Fatal(err)
			}
		}, staleGapID, "work/rules/SKILL.md (removed)"},
		{"upstream file unreadable", func(t *testing.T, f skillFixture) {
			path := filepath.Join(f.upstream, "work", "rules", "SKILL.md")
			if err := os.Rename(path, path+".real"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(path+".real", path); err != nil {
				t.Fatal(err)
			}
		}, uncheckedGapID, "work/rules/SKILL.md ("},
		{"one upstream file changed and another unreadable", func(t *testing.T, f skillFixture) {
			other := filepath.Join(f.upstream, "work", "other", "SKILL.md")
			if err := os.MkdirAll(filepath.Dir(other), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(other, make([]byte, upstreamFileLimit+1), 0600); err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(map[string]any{"schema": manifestSchema, "upstream_root": f.upstream, "upstream": []any{
				map[string]any{"path": "work/rules/SKILL.md", "sha256": sha([]byte("older"))},
				map[string]any{"path": "work/other/SKILL.md", "sha256": sha(nil)}}})
			if err := os.WriteFile(filepath.Join(f.source, "manifest.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}, uncheckedGapID + "," + staleGapID, "work/other/SKILL.md ("},
		{"upstream root absent on this machine", func(t *testing.T, f skillFixture) {
			f.writeManifest(t, filepath.Join(f.upstream, "elsewhere"), sha(f.upstreamFile))
		}, uncheckedGapID, "not readable on this machine"},
		{"home-relative upstream root", func(t *testing.T, f skillFixture) {
			t.Setenv("HOME", filepath.Dir(f.upstream))
			f.writeManifest(t, "~/"+filepath.Base(f.upstream), sha(f.upstreamFile))
		}, "", ""},
		{"home unknown", func(t *testing.T, f skillFixture) {
			t.Setenv("HOME", "")
			f.writeManifest(t, "~/"+filepath.Base(f.upstream), sha(f.upstreamFile))
		}, uncheckedGapID, "home directory is unknown"},
		{"upstream read too slow", func(t *testing.T, f skillFixture) {
			blockOpen(t, f.upstream, &upstreamReadTimeout)
		}, uncheckedGapID, "took longer than"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSkillFixture(t)
			tc.change(t, f)
			var record SkillsRecord
			report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
				skills, err := PrepareSkills(ctx, r, r.Root(), f.source)
				if err != nil {
					return err
				}
				record, err = engine.Decode[SkillsRecord](ctx, r, skills.Record)
				return err
			})
			if report.Outcome != engine.Succeeded {
				t.Fatalf("staleness blocked the run: %v", report.Failure)
			}
			var ids, texts []string
			for _, g := range record.Gaps {
				ids, texts = append(ids, g.ID), append(texts, g.Text)
			}
			if strings.Join(ids, ",") != tc.gapIDs || !strings.Contains(strings.Join(texts, "\n"), tc.text) {
				t.Fatalf("gaps = %+v, want ids %q mentioning %q", record.Gaps, tc.gapIDs, tc.text)
			}
		})
	}
}

// blockOpen makes opening dir block until the test ends, as a cloud
// placeholder can, and shortens the deadline that bounds it.
func blockOpen(t *testing.T, dir string, timeout *time.Duration) {
	t.Helper()
	release, returned := make(chan struct{}), make(chan struct{})
	var opened atomic.Bool
	open, old := openRoot, *timeout
	openRoot = func(name string) (*os.Root, error) {
		if name == dir && opened.CompareAndSwap(false, true) {
			defer close(returned)
			<-release
		}
		return open(name)
	}
	*timeout = 50 * time.Millisecond
	// The abandoned reader holds the replaced opener until released.
	t.Cleanup(func() {
		close(release)
		if opened.Load() {
			select {
			case <-returned:
			case <-time.After(5 * time.Second):
				t.Error("blocked open never returned")
			}
		}
		openRoot, *timeout = open, old
	})
}

func TestPrepareSkillsRejectsUnsafeSources(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source func(*testing.T, skillFixture) string
		want   string
	}{
		{"unset", func(*testing.T, skillFixture) string { return "" }, "PWC_TRIAGE_SKILLS_DIR is not set"},
		{"relative", func(*testing.T, skillFixture) string { return "skills" }, "must be an absolute path"},
		{"missing", func(_ *testing.T, f skillFixture) string { return filepath.Join(f.source, "absent") }, "open skill directory"},
		{"role directory missing", func(t *testing.T, f skillFixture) string {
			if err := os.RemoveAll(filepath.Join(f.source, "validator")); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "skill directory validator"},
		{"symlinked role directory", func(t *testing.T, f skillFixture) string {
			if err := os.Rename(filepath.Join(f.source, "steward"), filepath.Join(f.source, "steward-real")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("steward-real", filepath.Join(f.source, "steward")); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "skill entry steward is not a directory"},
		{"symlinked skill file", func(t *testing.T, f skillFixture) string {
			if err := os.Symlink(filepath.Join(f.source, "README.md"), filepath.Join(f.source, "core", "linked.md")); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "skill entry core/linked.md is not a regular file or directory"},
		{"fifo", func(t *testing.T, f skillFixture) string {
			if err := syscall.Mkfifo(filepath.Join(f.source, "core", "pipe"), 0600); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "skill entry core/pipe is not a regular file or directory"},
		{"oversized skill file", func(t *testing.T, f skillFixture) string {
			if err := os.WriteFile(filepath.Join(f.source, "core", "huge.md"), make([]byte, skillFileLimit+1), 0600); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "skill file core/huge.md"},
		{"manifest missing", func(t *testing.T, f skillFixture) string {
			if err := os.Remove(filepath.Join(f.source, "manifest.json")); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "read skill manifest.json"},
		{"manifest with an unknown schema", func(t *testing.T, f skillFixture) string {
			if err := os.WriteFile(filepath.Join(f.source, "manifest.json"), []byte(`{"schema":"other","upstream_root":"/x","upstream":[]}`), 0600); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, `schema "other"`},
		{"manifest path escaping the upstream root", func(t *testing.T, f skillFixture) string {
			raw, _ := json.Marshal(map[string]any{"schema": manifestSchema, "upstream_root": f.upstream, "upstream": []any{map[string]any{"path": "../outside.md", "sha256": sha(nil)}}})
			if err := os.WriteFile(filepath.Join(f.source, "manifest.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "must be a slash-separated relative path"},
		{"role without SKILL.md", func(t *testing.T, f skillFixture) string {
			if err := os.Rename(filepath.Join(f.source, "intake", "SKILL.md"), filepath.Join(f.source, "intake", "skill.txt")); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "skill directory intake has no SKILL.md"},
		{"too many skill files", func(t *testing.T, f skillFixture) string {
			for i := range skillFileCount {
				if err := os.WriteFile(filepath.Join(f.source, "core", fmt.Sprintf("extra-%03d.md", i)), []byte("x"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			return f.source
		}, "more than 128 files"},
		{"skill files over the total limit", func(t *testing.T, f skillFixture) string {
			for i := range skillTotalLimit/skillFileLimit + 1 {
				if err := os.WriteFile(filepath.Join(f.source, "core", fmt.Sprintf("big-%d.md", i)), make([]byte, skillFileLimit), 0600); err != nil {
					t.Fatal(err)
				}
			}
			return f.source
		}, "skill files exceed"},
		{"name with an invisible format character", func(t *testing.T, f skillFixture) string {
			if err := os.WriteFile(filepath.Join(f.source, "core", "a\u202emd.txt"), []byte("x"), 0600); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "not printable UTF-8"},
		{"name with a control character", func(t *testing.T, f skillFixture) string {
			if err := os.WriteFile(filepath.Join(f.source, "core", "a\nb.md"), []byte("x"), 0600); err != nil {
				t.Fatal(err)
			}
			return f.source
		}, "not printable UTF-8"},
		{"manifest with null upstream", manifestCase(`{"schema":"` + manifestSchema + `","upstream_root":"/x","upstream":null}`), "upstream is empty"},
		{"manifest with a non-string digest", manifestCase(`{"schema":"` + manifestSchema + `","upstream_root":"/x","upstream":[{"path":"a.md","sha256":1}]}`), "upstream[0].sha256"},
		{"manifest listing a path twice", manifestCase(`{"schema":"` + manifestSchema + `","upstream_root":"/x","upstream":[{"path":"a.md","sha256":"` + strings.Repeat("a", 64) + `"},{"path":"a.md","sha256":"` + strings.Repeat("b", 64) + `"}]}`), "listed twice"},
		{"manifest keys differing in case", manifestCase(`{"schema":"` + manifestSchema + `","upstream_root":"/x","Upstream":[{"path":"a.md","sha256":"` + strings.Repeat("a", 64) + `"}]}`), "field upstream"},
		{"relative upstream root", func(t *testing.T, f skillFixture) string {
			f.writeManifest(t, "upstream", sha(f.upstreamFile))
			return f.source
		}, "is not absolute"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSkillFixture(t)
			source := tc.source(t, f)
			var prepareErr error
			var runDir string
			report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
				runDir = r.Dir()
				_, prepareErr = PrepareSkills(ctx, r, r.Root(), source)
				return prepareErr
			})
			if prepareErr == nil || !strings.Contains(prepareErr.Error(), tc.want) || report.Outcome != engine.Failed {
				t.Fatalf("error = %v outcome = %s, want %q", prepareErr, report.Outcome, tc.want)
			}
			if _, err := os.Lstat(filepath.Join(runDir, skillsExtractName)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("rejected source was expanded: %v", err)
			}
			if len(report.Snapshot.Attempts) != 0 {
				t.Errorf("rejected source committed a record: %+v", report.Snapshot.Attempts)
			}
		})
	}
}

func TestPrepareSkillsKeepsExistingExpansion(t *testing.T) {
	f := newSkillFixture(t)
	var prepareErr error
	var kept []byte
	report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
		dir := filepath.Join(r.Dir(), skillsExtractName)
		if err := os.Mkdir(dir, 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, "owned.md"), []byte("other owner"), 0600); err != nil {
			return err
		}
		_, prepareErr = PrepareSkills(ctx, r, r.Root(), f.source)
		kept, _ = os.ReadFile(filepath.Join(dir, "owned.md"))
		return prepareErr
	})
	if prepareErr == nil || !strings.Contains(prepareErr.Error(), "reserve triage skills") || string(kept) != "other owner" || report.Outcome != engine.Failed {
		t.Fatalf("error = %v kept = %q", prepareErr, kept)
	}
}

func TestPrepareSkillsSourceDeadline(t *testing.T) {
	f := newSkillFixture(t)
	blockOpen(t, f.source, &skillReadTimeout)
	var prepareErr error
	var runDir string
	report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
		runDir = r.Dir()
		_, prepareErr = PrepareSkills(ctx, r, r.Root(), f.source)
		return prepareErr
	})
	if prepareErr == nil || !strings.Contains(prepareErr.Error(), "took longer than") || report.Outcome != engine.Failed {
		t.Fatalf("error = %v outcome = %s, want the source read deadline", prepareErr, report.Outcome)
	}
	if _, err := os.Lstat(filepath.Join(runDir, skillsExtractName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("timed-out source was expanded: %v", err)
	}
}

func manifestCase(raw string) func(*testing.T, skillFixture) string {
	return func(t *testing.T, f skillFixture) string {
		if err := os.WriteFile(filepath.Join(f.source, "manifest.json"), []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		return f.source
	}
}

func TestPrepareSkillsSourceBoundaries(t *testing.T) {
	f := newSkillFixture(t)
	// A dot directory is skipped whole, including entries that would be rejected.
	if err := os.MkdirAll(filepath.Join(f.source, "core", ".git"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(f.source, "core", ".git", "pipe"), 0600); err != nil {
		t.Fatal(err)
	}
	// The fixture has 7 skill files; fill up to exactly the file limit.
	for i := range skillFileCount - 7 {
		if err := os.WriteFile(filepath.Join(f.source, "steward", fmt.Sprintf("note-%03d.md", i)), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var record SkillsRecord
	report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
		skills, err := PrepareSkills(ctx, r, r.Root(), f.source)
		if err != nil {
			return err
		}
		record, err = engine.Decode[SkillsRecord](ctx, r, skills.Record)
		return err
	})
	if report.Outcome != engine.Succeeded || len(record.Files) != skillFileCount {
		t.Fatalf("outcome = %s (%v), files = %d, want exactly %d accepted", report.Outcome, report.Failure, len(record.Files), skillFileCount)
	}
}

func TestPrepareSkillsOuterCancellation(t *testing.T) {
	for _, dir := range []string{"source", "upstream"} {
		t.Run(dir, func(t *testing.T) {
			f := newSkillFixture(t)
			target := f.source
			if dir == "upstream" {
				target = f.upstream
			}
			blockOpen(t, target, &skillReadTimeout)
			oldUpstream := upstreamReadTimeout
			t.Cleanup(func() { upstreamReadTimeout = oldUpstream })
			skillReadTimeout, upstreamReadTimeout = time.Minute, time.Minute
			var prepareErr error
			stop := errors.New("operator stopped the run")
			report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
				ctx, cancel := context.WithCancelCause(ctx)
				go func() { time.Sleep(50 * time.Millisecond); cancel(stop) }()
				_, prepareErr = PrepareSkills(ctx, r, r.Root(), f.source)
				return prepareErr
			})
			if !errors.Is(prepareErr, stop) || report.Outcome != engine.Failed || len(report.Snapshot.Attempts) != 0 {
				t.Fatalf("error = %v outcome = %s attempts = %d, want the cancellation as an error with no record", prepareErr, report.Outcome, len(report.Snapshot.Attempts))
			}
		})
	}
}

func TestListPaths(t *testing.T) {
	var paths []string
	for i := range gapPathLimit + 1 {
		paths = append(paths, fmt.Sprintf("p%d", i))
	}
	if got := listPaths(paths[:gapPathLimit]); got != strings.Join(paths[:gapPathLimit], ", ") {
		t.Fatalf("listPaths at the limit = %q", got)
	}
	if got := listPaths(paths); got != strings.Join(paths[:gapPathLimit], ", ")+" and 1 more" {
		t.Fatalf("listPaths over the limit = %q", got)
	}
}
