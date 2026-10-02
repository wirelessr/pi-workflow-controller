package triagev2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

const IntakeSchema = "triage.intake.v1"

var ticketKey = regexp.MustCompile(`^[A-Z][A-Z0-9_]*-[1-9][0-9]*$`)

type Source struct {
	Status string `json:"status"`
	FileID string `json:"file_id"`
	Reason string `json:"reason"`
}

type CommentPage struct {
	Start  int    `json:"start"`
	Source Source `json:"source"`
}

type LinkedIssue struct {
	Key    string `json:"key"`
	Source Source `json:"source"`
}

type Attachment struct {
	ID       string `json:"id"`
	Content  Source `json:"content"`
	Analysis Source `json:"analysis"`
}

type Intake struct {
	Ticket      string        `json:"ticket"`
	URL         string        `json:"url"`
	FetchedAt   string        `json:"fetched_at"`
	Issue       Source        `json:"issue"`
	Fields      Source        `json:"fields"`
	Comments    []CommentPage `json:"comments"`
	Linked      []LinkedIssue `json:"linked"`
	Attachments []Attachment  `json:"attachments"`
	Complete    bool          `json:"complete"`
	Gaps        []Gap         `json:"gaps"`
}

// Raw Jira issue projections, used only to cross-check an intake inventory
// against the raw issue the Agent saved.
type jiraIssue struct {
	Key    string                     `json:"key"`
	Fields map[string]json.RawMessage `json:"fields"`
}
type jiraAttachment struct {
	ID   string `json:"id"`
	Size *int64 `json:"size"`
}
type jiraLink struct {
	In *struct {
		Key string `json:"key"`
	} `json:"inwardIssue"`
	Out *struct {
		Key string `json:"key"`
	} `json:"outwardIssue"`
}

func nonblank(s string) bool { return strings.TrimSpace(s) != "" }

func utc(value string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || !strings.HasSuffix(value, "Z") {
		return t, fmt.Errorf("explicit UTC required: %q", value)
	}
	return t, nil
}

func evidenceFile(files []contract.FileEntry, id string) bool {
	for _, f := range files {
		if f.ID == id && f.Kind == "evidence" {
			return true
		}
	}
	return false
}

// rawFile reads a committed file a second time, after the Store verified its
// containment and digest, bounded and without blocking on a FIFO.
func rawFile(ctx context.Context, ref contract.Ref, files []contract.FileEntry, id string) ([]byte, error) {
	var path string
	for _, f := range files {
		if f.ID == id {
			path = f.Path
		}
	}
	if !filepath.IsLocal(path) {
		return nil, fmt.Errorf("unknown/local file required: %s", id)
	}
	root, err := os.OpenRoot(filepath.Dir(ref.Path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	f, err := root.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	const limit = 64 << 20
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("invalid evidence file")
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	raw, err := contract.ReadBounded(context.Background(), f, limit)
	if errors.Is(err, contract.ErrReadLimit) {
		return nil, fmt.Errorf("evidence exceeds read limit")
	}
	if raw == nil {
		raw = []byte{}
	}
	if err == nil {
		err = context.Cause(ctx)
	}
	return raw, err
}

func rawJSON(ctx context.Context, ref contract.Ref, files []contract.FileEntry, id string, value any) error {
	raw, err := rawFile(ctx, ref, files, id)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}

// readAccepted reads a committed publication through the engine resolver and
// decodes it anew for each caller.
func readAccepted[T any](ctx context.Context, r *engine.Run, ref contract.Ref, schema string) (contract.Publication[T], error) {
	var p contract.Publication[T]
	if ref.SchemaID != schema {
		return p, fmt.Errorf("expected %s, got %s", schema, ref.SchemaID)
	}
	raw, err := engine.ReadContract(ctx, r, ref)
	if err != nil {
		return p, err
	}
	return contract.DecodePublication[T](raw)
}

func checkIntake(ctx context.Context, r *engine.Run, ref contract.Ref, ticket string) (Intake, error) {
	p, err := readAccepted[Intake](ctx, r, ref, IntakeSchema)
	if err != nil {
		return Intake{}, err
	}
	return checkIntakePublication(ctx, ref, p, ticket)
}

// checkSource reports whether s is available and rejects citations of files
// this intake does not declare as evidence.
func checkSource(field string, s Source, files []contract.FileEntry) (bool, error) {
	if s.Status == "available" {
		if !evidenceFile(files, s.FileID) {
			return false, fmt.Errorf("%s.file_id: got %q; want a kind=evidence files[] id of this contract for an available source", field, s.FileID)
		}
		return true, nil
	}
	if !nonblank(s.Reason) {
		return false, fmt.Errorf("%s.reason: got an empty reason for status %s; want why the source is not available", field, s.Status)
	}
	if s.FileID != "" && !evidenceFile(files, s.FileID) {
		return false, fmt.Errorf("%s.file_id: got %q; want empty or a kind=evidence files[] id of this contract holding the partial data", field, s.FileID)
	}
	return false, nil
}

// checkIntakePublication cross-checks the inventory against the raw issue:
// every comment page, formal link and attachment must be accounted for, and
// complete must equal what the raw sources prove.
func checkIntakePublication(ctx context.Context, ref contract.Ref, p contract.Publication[Intake], ticket string) (Intake, error) {
	v := p.Data
	u, err := url.Parse(v.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/browse/"+ticket || v.Ticket != ticket {
		return v, fmt.Errorf("ticket/url: got ticket %q url %q; want ticket %s and its canonical browse URL https://<host>/browse/%s", v.Ticket, v.URL, ticket, ticket)
	}
	if _, err := utc(v.FetchedAt); err != nil {
		return v, fmt.Errorf("fetched_at: %w", err)
	}
	read := func(s Source) ([]byte, error) { return rawFile(ctx, ref, p.Files, s.FileID) }
	decode := func(s Source, value any) error { return rawJSON(ctx, ref, p.Files, s.FileID, value) }
	complete := true
	check := func(field string, s Source) error {
		ok, err := checkSource(field, s, p.Files)
		complete = complete && ok
		return err
	}
	if err := check("issue", v.Issue); err != nil {
		return v, err
	}
	if err := check("fields", v.Fields); err != nil {
		return v, err
	}
	if v.Fields.Status == "available" {
		var fields []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		if err := decode(v.Fields, &fields); err != nil {
			return v, fmt.Errorf("fields: the file is not a JSON array of field objects: %w", err)
		}
		if len(fields) == 0 {
			return v, errors.New("fields: got an empty field metadata array")
		}
		seen := map[string]bool{}
		for i, f := range fields {
			if !nonblank(f.ID) || !nonblank(f.Name) || seen[f.ID] {
				return v, fmt.Errorf("fields: element %d needs a unique nonblank id and a name", i)
			}
			seen[f.ID] = true
		}
	}
	var issue jiraIssue
	if v.Issue.Status == "available" {
		if err := decode(v.Issue, &issue); err != nil {
			return v, fmt.Errorf("issue: the file is not a raw issue JSON object: %w", err)
		}
		if issue.Key != ticket {
			return v, fmt.Errorf("issue: raw issue key is %q, want %s", issue.Key, ticket)
		}
		for _, name := range []string{"description", "comment", "issuelinks", "attachment"} {
			if _, ok := issue.Fields[name]; !ok {
				return v, fmt.Errorf("issue: the raw issue omits fields.%s; save the complete issue with all fields", name)
			}
		}
	}
	var embedded struct {
		Total *int `json:"total"`
	}
	if issue.Fields != nil {
		if err := json.Unmarshal(issue.Fields["comment"], &embedded); err != nil {
			return v, fmt.Errorf("issue: fields.comment is not an object: %w", err)
		}
		if embedded.Total == nil || *embedded.Total < 0 {
			return v, errors.New("issue: fields.comment.total is missing")
		}
	}
	next, total := 0, -1
	ids := map[string]bool{}
	starts := map[int]bool{}
	for i, page := range v.Comments {
		field := fmt.Sprintf("comments[%d]", i)
		if starts[page.Start] {
			return v, fmt.Errorf("%s.start: page %d is listed twice", field, page.Start)
		}
		starts[page.Start] = true
		if err := check(field+".source", page.Source); err != nil {
			return v, err
		}
		if page.Source.Status != "available" {
			continue
		}
		var raw struct {
			Start    *int `json:"startAt"`
			Total    *int `json:"total"`
			Comments []struct {
				ID   string          `json:"id"`
				Body json.RawMessage `json:"body"`
			} `json:"comments"`
		}
		if err := decode(page.Source, &raw); err != nil {
			return v, fmt.Errorf("%s: the file is not a raw comment page: %w", field, err)
		}
		if raw.Start == nil || raw.Total == nil || raw.Comments == nil || *raw.Total < 0 || *raw.Start != page.Start {
			return v, fmt.Errorf("%s: the raw page needs startAt equal to start %d, a total and a comments array", field, page.Start)
		}
		if total >= 0 && total != *raw.Total {
			complete = false
		}
		total = *raw.Total
		if page.Start != next {
			complete = false
		}
		next = page.Start + len(raw.Comments)
		if next > total {
			return v, fmt.Errorf("%s: comments extend past the total %d", field, total)
		}
		for _, c := range raw.Comments {
			if !nonblank(c.ID) || ids[c.ID] || len(c.Body) == 0 || strings.TrimSpace(string(c.Body)) == "null" {
				return v, fmt.Errorf("%s: comment %q is duplicate or has no body", field, c.ID)
			}
			ids[c.ID] = true
		}
	}
	complete = complete && total >= 0 && next == total && len(ids) == total && embedded.Total != nil && total == *embedded.Total
	var links []jiraLink
	var attachments []jiraAttachment
	if issue.Fields != nil {
		if err := json.Unmarshal(issue.Fields["issuelinks"], &links); err != nil {
			return v, fmt.Errorf("issue: fields.issuelinks is not an array: %w", err)
		}
		if err := json.Unmarshal(issue.Fields["attachment"], &attachments); err != nil {
			return v, fmt.Errorf("issue: fields.attachment is not an array: %w", err)
		}
	}
	expected := map[string]bool{}
	for _, l := range links {
		if l.In != nil {
			expected[l.In.Key] = true
		}
		if l.Out != nil {
			expected[l.Out.Key] = true
		}
	}
	seen := map[string]bool{}
	for i, l := range v.Linked {
		field := fmt.Sprintf("linked[%d]", i)
		if !expected[l.Key] || seen[l.Key] {
			return v, fmt.Errorf("%s.key: got %q; want each formal issue link of the raw issue exactly once", field, l.Key)
		}
		seen[l.Key] = true
		if err := check(field+".source", l.Source); err != nil {
			return v, err
		}
		if l.Source.Status == "available" {
			var raw jiraIssue
			if err := decode(l.Source, &raw); err != nil {
				return v, fmt.Errorf("%s: the file is not a raw issue: %w", field, err)
			}
			if raw.Key != l.Key || len(raw.Fields) == 0 {
				return v, fmt.Errorf("%s: the snapshot is of %q with no fields; want %s", field, raw.Key, l.Key)
			}
		}
	}
	if len(seen) != len(expected) {
		return v, fmt.Errorf("linked: got %d entries; the raw issue has %d formal links", len(seen), len(expected))
	}
	sizes := map[string]int64{}
	for _, a := range attachments {
		_, duplicate := sizes[a.ID]
		if !nonblank(a.ID) || a.Size == nil || *a.Size < 0 || duplicate {
			return v, fmt.Errorf("issue: raw attachment %q needs a unique id and a size", a.ID)
		}
		sizes[a.ID] = *a.Size
	}
	seen = map[string]bool{}
	for i, a := range v.Attachments {
		field := fmt.Sprintf("attachments[%d]", i)
		size, ok := sizes[a.ID]
		if !ok || seen[a.ID] {
			return v, fmt.Errorf("%s.id: got %q; want each attachment of the raw issue exactly once", field, a.ID)
		}
		seen[a.ID] = true
		if err := check(field+".content", a.Content); err != nil {
			return v, err
		}
		if err := check(field+".analysis", a.Analysis); err != nil {
			return v, err
		}
		if a.Content.Status == "available" {
			raw, err := read(a.Content)
			if err != nil {
				return v, fmt.Errorf("%s.content: %w", field, err)
			}
			if int64(len(raw)) != size {
				return v, fmt.Errorf("%s.content: got %d bytes; the raw issue says %d", field, len(raw), size)
			}
		} else if a.Analysis.Status == "available" {
			return v, fmt.Errorf("%s.analysis: available analysis needs available content", field)
		}
	}
	if len(seen) != len(sizes) {
		return v, fmt.Errorf("attachments: got %d entries; the raw issue has %d attachments", len(seen), len(sizes))
	}
	if err := CheckGapIDs("gaps", v.Gaps); err != nil {
		return v, err
	}
	if v.Complete != complete || complete != (len(v.Gaps) == 0) {
		return v, fmt.Errorf("complete/gaps: got complete=%t with %d gaps; the sources prove complete=%t, and complete requires no gaps while incomplete requires at least one", v.Complete, len(v.Gaps), complete)
	}
	return v, nil
}
