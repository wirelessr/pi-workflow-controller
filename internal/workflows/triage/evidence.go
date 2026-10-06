package triage

import (
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"pi-workflow-controller/internal/contract"
)

// Evidence cites a kind=evidence file. A nil Ref means this contract's own
// file; otherwise Ref must be one of the Step's citable inputs.
type Evidence struct {
	Ref     *contract.Ref `json:"ref"`
	FileID  string        `json:"file_id"`
	Locator *Locator      `json:"locator,omitempty"`
}

type Gap struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

// maxViolations bounds how many independent violations one diagnostic lists.
const maxViolations = 20

// violations joins the independent violations of one contract so a single
// repair can fix them all; nil when there are none.
func violations(errs []error) error {
	if len(errs) > maxViolations {
		errs = append(errs[:maxViolations:maxViolations], fmt.Errorf("and %d more violations not listed", len(errs)-maxViolations))
	}
	return errors.Join(errs...)
}

// checkGaps requires unique ids and text that is not only whitespace.
func checkGaps(field string, gaps []Gap) error {
	for i, g := range gaps {
		if !nonblank(g.Text) {
			return fmt.Errorf("%s[%d].text: got only whitespace; want what remains undone or failed", field, i)
		}
	}
	return CheckGapIDs(field, gaps)
}

// CheckGapIDs enforces id uniqueness within one contract. JSON Schema
// uniqueItems compares whole items, so two gaps with the same id and
// different text would pass the schema.
func CheckGapIDs(field string, gaps []Gap) error {
	seen := map[string]int{}
	for i, g := range gaps {
		if j, ok := seen[g.ID]; ok {
			return fmt.Errorf("duplicate gap id %q at %s[%d] and %s[%d]; ids must be unique within this contract", g.ID, field, j, field, i)
		}
		seen[g.ID] = i
	}
	return nil
}

// Inputs holds a Step's citable inputs: exact committed Refs whose
// kind=evidence files may be cited, with their files.
type Inputs struct {
	Citable map[contract.Ref][]contract.FileEntry
}

// CheckEvidence reports exactly which part of a citation is wrong, what was
// received and what is expected, so a repair can fix it without guessing.
func (in Inputs) CheckEvidence(field string, e Evidence, own []contract.FileEntry) error {
	if e.Ref == nil {
		err := checkFile(field, e.FileID, own, "this contract", "this contract's files[] (a null ref cites only this contract's own files; to cite a committed input, pair its exact ref with its file_id)")
		if err == nil || slices.ContainsFunc(own, func(f contract.FileEntry) bool { return f.ID == e.FileID }) {
			return err
		}
		// Judgment contracts are left out: a candidate may not cite them, so
		// pointing there would trade one rejection for another.
		var owners []string
		for ref, files := range in.Citable {
			if !slices.Contains(judgmentSchemas, ref.SchemaID) && slices.ContainsFunc(files, func(f contract.FileEntry) bool { return f.ID == e.FileID && f.Kind == "evidence" }) {
				owners = append(owners, describeRef(ref))
			}
		}
		slices.Sort(owners)
		switch len(owners) {
		case 0:
			return err
		case 1:
			return fmt.Errorf("%w (input %s declares %q: set ref to that input's ref copied byte-exact from the request)", err, owners[0], e.FileID)
		default:
			return fmt.Errorf("%w (inputs %s declare %q: set ref to the ref of the input you read, copied byte-exact from the request)", err, strings.Join(owners, ", "), e.FileID)
		}
	}
	ref := *e.Ref
	if files, ok := in.Citable[ref]; ok {
		return checkFile(field, e.FileID, files, "input "+describeRef(ref), "that input's files[]")
	}
	for known := range in.Citable {
		if known.AttemptID == ref.AttemptID {
			return fmt.Errorf("%s.ref: got attempt %s with %s; want the ref copied byte-exact from the request", field, ref.AttemptID, differingFields(ref, known))
		}
	}
	return fmt.Errorf("%s.ref: got %s, which is not an input of this Step; want one of this Step's citable inputs copied byte-exact from the request", field, describeRef(ref))
}

func checkFile(field, id string, files []contract.FileEntry, owner, want string) error {
	for _, f := range files {
		if f.ID == id {
			if f.Kind != "evidence" {
				return fmt.Errorf("%s.file_id: got %q, which %s declares as kind=%s; want a kind=evidence file", field, id, owner, f.Kind)
			}
			return nil
		}
	}
	hint := ""
	base := path.Base(id)
	for _, f := range files {
		if f.Kind == "evidence" && (f.Path == id || f.ID == base || f.ID == strings.TrimSuffix(base, path.Ext(base))) {
			hint = fmt.Sprintf(" (files[] declares %q at %s: cite that bare id, never a path or an added extension)", f.ID, f.Path)
		}
	}
	return fmt.Errorf("%s.file_id: got %q, which %s does not declare in files[]; want a kind=evidence id from %s%s", field, id, owner, want, hint)
}

func describeRef(ref contract.Ref) string {
	return fmt.Sprintf("attempt %s (%s)", ref.AttemptID, ref.SchemaID)
}

func differingFields(got, want contract.Ref) string {
	var fields []string
	for _, f := range []struct {
		name      string
		got, want string
	}{
		{"run_id", got.RunID, want.RunID},
		{"path", got.Path, want.Path},
		{"schema_id", got.SchemaID, want.SchemaID},
		{"sha256", got.SHA256, want.SHA256},
		{"manifest_sha256", got.ManifestSHA256, want.ManifestSHA256},
	} {
		if f.got != f.want {
			fields = append(fields, fmt.Sprintf("%s %q (committed %q)", f.name, f.got, f.want))
		}
	}
	return strings.Join(fields, ", ")
}
