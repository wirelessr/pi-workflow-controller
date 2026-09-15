package review

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"unicode/utf8"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

func pin(c *Checkout) Pin {
	return Pin{URL: c.URL, Repository: c.Repository, Number: c.Number, BaseSHA: c.BaseSHA, HeadSHA: c.HeadSHA, MergeBase: c.MergeBase, DiffRange: c.DiffRange, ContextID: c.ContextID}
}

type checkFile struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Path string `json:"path"`
}

func readReviewContract[T any](ctx context.Context, r *engine.Run, ref contract.Ref, schema string) (T, map[string]checkFile, error) {
	var envelope struct {
		Data  T           `json:"data"`
		Files []checkFile `json:"files"`
	}
	if ref.SchemaID != schema {
		return envelope.Data, nil, fmt.Errorf("expected schema %s, got %s", schema, ref.SchemaID)
	}
	raw, err := engine.ReadContract(ctx, r, ref)
	if err != nil {
		return envelope.Data, nil, err
	}
	if err = json.Unmarshal(raw, &envelope); err != nil {
		return envelope.Data, nil, err
	}
	files := make(map[string]checkFile, len(envelope.Files))
	for _, f := range envelope.Files {
		if !nonblank(f.ID) || files[f.ID].ID != "" {
			return envelope.Data, nil, fmt.Errorf("duplicate or empty file ID %q", f.ID)
		}
		files[f.ID] = f
	}
	return envelope.Data, files, nil
}

// Bound secondary reads as well as Store's initial digest validation.
const checkReadLimit int64 = 64 << 20

func readCheckFile(ctx context.Context, root *os.Root, path string) ([]byte, error) {
	if !filepath.IsLocal(path) {
		return nil, fmt.Errorf("path is not relative and rooted: %q", path)
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	info, err := root.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file: %q", path)
	}
	// A replacement FIFO must not block between the path stat and fd stat.
	f, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) { _ = f.Close() }(f)
	opened, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return nil, fmt.Errorf("file changed while opening: %q", path)
	}
	var out bytes.Buffer
	block := make([]byte, 32<<10)
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		n, err := f.Read(block)
		if int64(out.Len()+n) > checkReadLimit {
			return nil, fmt.Errorf("file exceeds review read limit: %q", path)
		}
		out.Write(block[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func readPublishedFile(ctx context.Context, ref contract.Ref, f checkFile) ([]byte, error) {
	root, err := os.OpenRoot(filepath.Dir(ref.Path))
	if err != nil {
		return nil, err
	}
	defer func(root *os.Root) { _ = root.Close() }(root)
	return readCheckFile(ctx, root, f.Path)
}
func checkArtifact(ctx context.Context, ref contract.Ref, files map[string]checkFile, id string) error {
	f, ok := files[id]
	if !ok || f.Kind != "artifact" {
		return fmt.Errorf("%q is not an artifact file ID", id)
	}
	raw, err := readPublishedFile(ctx, ref, f)
	if err != nil {
		return err
	}
	if !utf8.Valid(raw) || !nonblank(string(raw)) {
		return fmt.Errorf("artifact %q must be nonempty UTF-8", id)
	}
	return nil
}
func nonblank(s string) bool { return strings.TrimSpace(s) != "" }
func checkStrings(name string, values []string) error {
	for _, s := range values {
		if !nonblank(s) {
			return fmt.Errorf("%s contains blank text", name)
		}
	}
	return nil
}
func reviewRole(role string) bool { return role == "code" || role == "scale" || role == "simplicity" }

func checkPrepared(ctx context.Context, r *engine.Run, ref contract.Ref, c *Checkout) (Prepared, error) {
	p, files, err := readReviewContract[Prepared](ctx, r, ref, PrepareSchema)
	if err == nil {
		err = preparedSemantics(ctx, ref, c, p, files)
	}
	if err != nil {
		return Prepared{}, fmt.Errorf("prepare acceptance: %w", err)
	}
	return p, nil
}
func preparedSemantics(ctx context.Context, ref contract.Ref, c *Checkout, p Prepared, files map[string]checkFile) error {
	if p.Pin != pin(c) {
		return fmt.Errorf("pin mismatch")
	}
	if err := checkStrings("open questions", p.OpenQuestions); err != nil {
		return err
	}
	if len(p.Requirements) == 0 && len(p.OpenQuestions) == 0 {
		return fmt.Errorf("empty requirements require an open question")
	}
	sources := map[string]Source{}
	for _, s := range p.Sources {
		if !nonblank(s.ID) || sources[s.ID].ID != "" {
			return fmt.Errorf("duplicate or empty source ID %q", s.ID)
		}
		if !nonblank(s.Kind) || !nonblank(s.Status) {
			return fmt.Errorf("source %q has blank kind/status", s.ID)
		}
		if s.Status != "available" && !nonblank(s.Note) {
			return fmt.Errorf("source %q needs an uncertainty note", s.ID)
		}
		if s.Status != "missing" && (!nonblank(s.FileID) || !nonblank(s.URL)) {
			return fmt.Errorf("source %q has no snapshot/URL", s.ID)
		}
		if s.FileID != "" {
			if _, ok := files[s.FileID]; !ok {
				return fmt.Errorf("source %q has unknown file ID %q", s.ID, s.FileID)
			}
		}
		sources[s.ID] = s
	}
	for _, key := range []string{"metadata", "diff", "changed-files"} {
		if c.Snapshots[key] == "" {
			return fmt.Errorf("controller snapshot %q missing", key)
		}
	}
	for key, path := range c.Snapshots {
		originalRoot, err := os.OpenRoot(filepath.Dir(path))
		if err != nil {
			return err
		}
		original, err := readCheckFile(ctx, originalRoot, filepath.Base(path))
		_ = originalRoot.Close()
		if err != nil {
			return err
		}
		matched := false
		for _, s := range p.Sources {
			u, e := url.Parse(s.URL)
			byURL := e == nil && u.Scheme == "file" && u.Host == "" && u.Path == path && u.RawQuery == "" && u.Fragment == ""
			if s.ID != key && !byURL {
				continue
			}
			if s.Status != "available" {
				return fmt.Errorf("snapshot %q is not available", key)
			}
			f, ok := files[s.FileID]
			if !ok {
				return fmt.Errorf("snapshot %q has unknown file ID", key)
			}
			copied, err := readPublishedFile(ctx, ref, f)
			if err != nil {
				return err
			}
			if sha256.Sum256(original) != sha256.Sum256(copied) {
				return fmt.Errorf("snapshot %q bytes differ", key)
			}
			matched = true
		}
		if !matched {
			return fmt.Errorf("snapshot %q absent from sources", key)
		}
	}
	for _, key := range c.Missing {
		s, ok := sources[key]
		if !ok || s.Status == "available" || !nonblank(s.Note) {
			return fmt.Errorf("missing snapshot %q must have unavailable source and note", key)
		}
	}
	requirements := map[string]bool{}
	for _, q := range p.Requirements {
		if !nonblank(q.ID) || requirements[q.ID] {
			return fmt.Errorf("duplicate or empty requirement ID %q", q.ID)
		}
		requirements[q.ID] = true
		if !nonblank(q.Statement) || len(q.SourceIDs) == 0 {
			return fmt.Errorf("requirement %q needs statement and sources", q.ID)
		}
		seen := map[string]bool{}
		for _, id := range q.SourceIDs {
			if _, ok := sources[id]; !ok || seen[id] {
				return fmt.Errorf("requirement %q has unknown/duplicate source %q", q.ID, id)
			}
			seen[id] = true
		}
	}
	return checkArtifact(ctx, ref, files, p.ContextFile)
}

func checkReviewed(ctx context.Context, r *engine.Run, ref contract.Ref, c *Checkout, prepared Prepared, contextRef contract.Ref, role string) (Reviewed, error) {
	v, _, err := readReviewContract[Reviewed](ctx, r, ref, ReviewerSchema)
	if err == nil {
		err = reviewedSemantics(ctx, c, prepared, contextRef, role, v)
	}
	if err != nil {
		return Reviewed{}, fmt.Errorf("%s reviewer acceptance: %w", role, err)
	}
	return v, nil
}
func reviewedSemantics(ctx context.Context, c *Checkout, p Prepared, contextRef contract.Ref, role string, v Reviewed) error {
	if v.Pin != pin(c) || p.Pin != pin(c) {
		return fmt.Errorf("pin mismatch")
	}
	if v.Context != contextRef {
		return fmt.Errorf("context Ref mismatch")
	}
	if !reviewRole(role) || v.Role != role {
		return fmt.Errorf("role mismatch: %q", v.Role)
	}
	if len(v.Coverage) == 0 {
		return fmt.Errorf("coverage is empty")
	}
	if err := checkStrings("coverage", v.Coverage); err != nil {
		return err
	}
	if err := checkStrings("limitations", v.Limitations); err != nil {
		return err
	}
	if err := checkFindings(ctx, c, v.Findings, role); err != nil {
		return err
	}
	return checkAssessments(ctx, c, p.Requirements, v.Requirements, role == "code")
}
func checkEvidence(ctx context.Context, c *Checkout, e Evidence) error {
	if !nonblank(e.Detail) || e.Line < 1 || e.EndLine < e.Line {
		return fmt.Errorf("invalid evidence detail/range at %q", e.Path)
	}
	root, err := os.OpenRoot(c.Worktree)
	if err != nil {
		return err
	}
	defer func(root *os.Root) { _ = root.Close() }(root)
	raw, err := readCheckFile(ctx, root, e.Path)
	if err != nil {
		return fmt.Errorf("head evidence %q: %w", e.Path, err)
	}
	lines := bytes.Count(raw, []byte{'\n'})
	if len(raw) > 0 && raw[len(raw)-1] != '\n' {
		lines++
	}
	if e.EndLine > lines {
		return fmt.Errorf("head evidence %q end line %d exceeds %d", e.Path, e.EndLine, lines)
	}
	return nil
}
func checkFindings(ctx context.Context, c *Checkout, findings []Finding, role string) error {
	seen := map[string]bool{}
	for _, f := range findings {
		prefix, _, _ := strings.Cut(f.ID, "-")
		if !reviewRole(prefix) || (role != "" && prefix != role) || len(f.ID) <= len(prefix)+1 || seen[f.ID] {
			return fmt.Errorf("invalid/duplicate finding ID %q", f.ID)
		}
		seen[f.ID] = true
		if !nonblank(f.Title) || !nonblank(f.Severity) || !nonblank(f.Impact) || len(f.Evidence) == 0 {
			return fmt.Errorf("finding %q lacks concrete title/severity/impact/evidence", f.ID)
		}
		if err := checkEvidence(ctx, c, f.Location); err != nil {
			return err
		}
		for _, e := range f.Evidence {
			if err := checkEvidence(ctx, c, e); err != nil {
				return err
			}
		}
	}
	return nil
}
func checkAssessments(ctx context.Context, c *Checkout, baseline []Requirement, assessments []Assessment, all bool) error {
	ids := map[string]bool{}
	for _, q := range baseline {
		if !nonblank(q.ID) || ids[q.ID] {
			return fmt.Errorf("invalid baseline ID %q", q.ID)
		}
		ids[q.ID] = true
	}
	seen := map[string]bool{}
	for _, a := range assessments {
		if !ids[a.RequirementID] || seen[a.RequirementID] {
			return fmt.Errorf("unknown/duplicate assessment ID %q", a.RequirementID)
		}
		seen[a.RequirementID] = true
		if !nonblank(a.Reason) {
			return fmt.Errorf("assessment %q needs reason", a.RequirementID)
		}
		switch a.Status {
		case "satisfied", "not_satisfied":
			if len(a.Evidence) == 0 {
				return fmt.Errorf("assessment %q needs head evidence", a.RequirementID)
			}
		case "unconfirmed":
		default:
			return fmt.Errorf("invalid assessment status %q", a.Status)
		}
		for _, e := range a.Evidence {
			if err := checkEvidence(ctx, c, e); err != nil {
				return err
			}
		}
	}
	if all && len(seen) != len(ids) {
		return fmt.Errorf("baseline requirements not assessed exactly once")
	}
	return nil
}

func checkValidated(ctx context.Context, r *engine.Run, ref contract.Ref, c *Checkout, prepared Prepared, contextRef contract.Ref, expected []ReviewerResult, reviewed map[string]Reviewed) (Validated, error) {
	v, files, err := readReviewContract[Validated](ctx, r, ref, ValidationSchema)
	if err == nil {
		err = validatedSemantics(ctx, c, prepared, contextRef, expected, reviewed, v)
	}
	if err == nil {
		err = checkReport(ctx, ref, files, prepared, v)
	}
	if err != nil {
		return Validated{}, fmt.Errorf("validation acceptance: %w", err)
	}
	return v, nil
}
func checkReport(ctx context.Context, ref contract.Ref, files map[string]checkFile, p Prepared, v Validated) error {
	f, ok := files[v.ReportFile]
	if !ok || f.Kind != "artifact" {
		return fmt.Errorf("report is not an artifact file ID")
	}
	raw, err := readPublishedFile(ctx, ref, f)
	if err != nil {
		return err
	}
	if !utf8.Valid(raw) || !nonblank(string(raw)) {
		return fmt.Errorf("report must be nonempty UTF-8")
	}
	const marker = "<!-- pwc-review-data -->\n"
	_, tail, ok := strings.Cut(string(raw), marker)
	if !ok {
		return fmt.Errorf("report lacks the structured traceability appendix; run the embedded renderer")
	}
	header, body, ok := strings.Cut(tail, "\n")
	fence := strings.TrimSuffix(header, "json")
	if !ok || !strings.HasSuffix(header, "json") || len(fence) < 3 || strings.Trim(fence, "`") != "" {
		return fmt.Errorf("invalid report appendix fence")
	}
	body = strings.TrimSpace(body)
	if !strings.HasSuffix(body, "\n"+fence) {
		return fmt.Errorf("report appendix is not closed")
	}
	body = strings.TrimSuffix(body, "\n"+fence)
	var appendix struct {
		Prepared   Prepared  `json:"prepared"`
		Validation Validated `json:"validation"`
	}
	if err := json.Unmarshal([]byte(body), &appendix); err != nil {
		return fmt.Errorf("invalid report appendix: %w", err)
	}
	if !reflect.DeepEqual(appendix.Prepared, p) || !reflect.DeepEqual(appendix.Validation, v) {
		return fmt.Errorf("report appendix differs from accepted requirements/validation")
	}
	return nil
}

func validatedSemantics(ctx context.Context, c *Checkout, p Prepared, contextRef contract.Ref, expected []ReviewerResult, reviewed map[string]Reviewed, v Validated) error {
	if v.Pin != pin(c) || p.Pin != pin(c) {
		return fmt.Errorf("pin mismatch")
	}
	if v.Context != contextRef {
		return fmt.Errorf("context Ref mismatch")
	}
	if err := checkStrings("validation limitations", v.Limitations); err != nil {
		return err
	}
	if len(expected) != 3 || len(v.Reviewers) != 3 {
		return fmt.Errorf("expected exactly three reviewers")
	}
	rows := map[string]ReviewerResult{}
	incomplete, limited := false, len(p.Requirements) == 0 || len(p.OpenQuestions) > 0 || len(c.Missing) > 0 || len(v.Limitations) > 0
	for _, s := range p.Sources {
		if s.Status != "available" {
			limited = true
		}
	}
	origins := map[string]bool{}
	for _, row := range expected {
		if !reviewRole(row.Role) || rows[row.Role].Role != "" {
			return fmt.Errorf("invalid/duplicate expected role %q", row.Role)
		}
		rows[row.Role] = row
		rv, exists := reviewed[row.Role]
		switch row.Status {
		case "succeeded":
			if row.Ref == nil || row.Ref.SchemaID != ReviewerSchema || !exists {
				return fmt.Errorf("succeeded reviewer %q lacks Ref/result", row.Role)
			}
			if err := reviewedSemantics(ctx, c, p, contextRef, row.Role, rv); err != nil {
				return fmt.Errorf("source reviewer %s: %w", row.Role, err)
			}
			if len(rv.Limitations) > 0 {
				limited = true
			}
			for _, a := range rv.Requirements {
				if a.Status == "unconfirmed" {
					limited = true
				}
			}
			for _, f := range rv.Findings {
				if origins[f.ID] {
					return fmt.Errorf("duplicate source finding %q", f.ID)
				}
				origins[f.ID] = true
			}
		case "failed", "missing":
			incomplete = true
			if row.Ref != nil || exists {
				return fmt.Errorf("unsuccessful reviewer %q has Ref/result", row.Role)
			}
		default:
			return fmt.Errorf("invalid reviewer status %q", row.Status)
		}
	}
	for role := range reviewed {
		if !reviewRole(role) || rows[role].Status != "succeeded" {
			return fmt.Errorf("unexpected reviewed role %q", role)
		}
	}
	seenRoles := map[string]bool{}
	for _, row := range v.Reviewers {
		want, ok := rows[row.Role]
		if !ok || seenRoles[row.Role] || !reflect.DeepEqual(row, want) {
			return fmt.Errorf("reviewer result mismatch for %q", row.Role)
		}
		seenRoles[row.Role] = true
	}
	if err := checkFindings(ctx, c, v.Findings, ""); err != nil {
		return err
	}
	targets := map[string]int{}
	for _, f := range v.Findings {
		if !origins[f.ID] {
			return fmt.Errorf("confirmed finding %q has no source", f.ID)
		}
		targets[f.ID] = 0
	}
	dispositions := map[string]bool{}
	confirmed := map[string]bool{}
	for _, d := range v.Dispositions {
		if !origins[d.FindingID] || dispositions[d.FindingID] || !nonblank(d.Reason) {
			return fmt.Errorf("invalid/duplicate disposition %q", d.FindingID)
		}
		dispositions[d.FindingID] = true
		switch d.Action {
		case "confirmed", "merged":
			if d.Action == "confirmed" {
				if d.FindingID != d.TargetID {
					return fmt.Errorf("confirmed disposition must retain its own ID")
				}
				confirmed[d.FindingID] = true
			} else if d.FindingID == d.TargetID {
				return fmt.Errorf("merged disposition must target another finding")
			}
			if _, ok := targets[d.TargetID]; !ok {
				return fmt.Errorf("disposition target %q not confirmed", d.TargetID)
			}
			targets[d.TargetID]++
		case "excluded", "unconfirmed":
			if d.TargetID != "" {
				return fmt.Errorf("%s disposition has target", d.Action)
			}
			if d.Action == "unconfirmed" {
				limited = true
			}
		default:
			return fmt.Errorf("unknown disposition action %q", d.Action)
		}
	}
	if len(dispositions) != len(origins) {
		return fmt.Errorf("source findings not disposed exactly once")
	}
	for id, n := range targets {
		if n == 0 || !confirmed[id] {
			return fmt.Errorf("confirmed target %q has no self-confirmed source", id)
		}
	}
	if err := checkAssessments(ctx, c, p.Requirements, v.Requirements, true); err != nil {
		return err
	}
	for _, a := range v.Requirements {
		if a.Status == "unconfirmed" {
			limited = true
		}
	}
	switch v.Completeness {
	case "incomplete":
		if !incomplete {
			return fmt.Errorf("incomplete requires missing/failed reviewer")
		}
	case "complete":
		if incomplete || limited {
			return fmt.Errorf("complete overstates available review evidence")
		}
	case "limited":
		if incomplete {
			return fmt.Errorf("missing/failed reviewer requires incomplete")
		}
	default:
		return fmt.Errorf("unknown completeness %q", v.Completeness)
	}
	if v.Completeness != "complete" && len(v.Limitations) == 0 {
		return fmt.Errorf("non-complete validation needs explicit limitations")
	}
	conclusion := "undetermined"
	if len(v.Findings) > 0 {
		conclusion = "findings"
	} else if v.Completeness == "complete" {
		conclusion = "no_confirmed_findings"
	}
	if v.Conclusion != conclusion {
		return fmt.Errorf("conclusion must be %q", conclusion)
	}
	return nil
}
