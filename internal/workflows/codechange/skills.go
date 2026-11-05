package codechange

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/skilldir"
	"pi-workflow-controller/internal/engine"
)

// SkillsDirEnv names the private skill directory; the CLI reads it.
const SkillsDirEnv = "PWC_CODE_CHANGE_SKILLS_DIR"

const (
	// convergeSkill is the only subtree of the private skill directory a
	// run expands; the converger of every round follows it.
	convergeSkill   = "converge"
	skillsExpansion = "code-change-skills"
)

// errSkillsExpansion is a round's copy of the private skill that could not
// be written into the run.
var errSkillsExpansion = errors.New("code-change cannot expand the private skill")

// runSkills is the private skill as the run read it, kept in memory, and
// its committed record.
type runSkills struct {
	source skilldir.Source
	record contract.Ref
}

// skillsRecord lists the files read and their digest.
type skillsRecord struct {
	Digest string          `json:"digest"`
	Files  []skilldir.File `json:"files"`
}

// readSkills reads the converge subtree of dir into memory. It touches no
// network, so a missing or unreadable skill fails the run before the
// repository GET.
func readSkills(ctx context.Context, dir string) (skilldir.Source, error) {
	src, err := skilldir.Read(ctx, dir, []string{convergeSkill}, nil)
	if err != nil {
		return skilldir.Source{}, fmt.Errorf("code-change %s: %w", SkillsDirEnv, err)
	}
	return src, nil
}

// recordSkills attaches the digest and files of src as skills. Only Expand
// computes them, so src is expanded once into skillsExpansion for the
// record; no Step is given that copy. The Controller copies and digests the
// bytes; it never reads the skill.
func recordSkills(ctx context.Context, r *engine.Run, src skilldir.Source) (runSkills, error) {
	expansion, err := src.Expand(r.Dir(), skillsExpansion)
	if err != nil {
		return runSkills{}, err
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "skills", Output: contract.Spec{SchemaID: SkillsSchema}, Data: skillsRecord{Digest: expansion.Digest, Files: expansion.Files}})
	if err != nil {
		return runSkills{}, err
	}
	return runSkills{source: src, record: ref}, nil
}

// expandRound writes a fresh copy of the skill for round n from memory, so
// what an earlier Agent changed in its own copy never reaches this round,
// and returns the absolute path of its SKILL.md. The round's Agent can still
// change this copy: the skills record's digest is of the bytes as read, not
// of what the Agent reads.
func (s runSkills) expandRound(r *engine.Run, n int) (string, error) {
	expansion, err := s.source.Expand(r.Dir(), fmt.Sprintf("%s-%03d", skillsExpansion, n))
	if err != nil {
		return "", fmt.Errorf("%w for round %d: %w", errSkillsExpansion, n, err)
	}
	return filepath.Join(expansion.Dir, convergeSkill, "SKILL.md"), nil
}
