package triage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

type file = contract.FileEntry

type publication[T any] = contract.Publication[T]

func nonblank(s string) bool { return strings.TrimSpace(s) != "" }
func texts(values []string) bool {
	for _, s := range values {
		if !nonblank(s) {
			return false
		}
	}
	return true
}
func hasFile(files []file, id string) bool {
	for _, f := range files {
		if f.ID == id && f.Kind == "evidence" {
			return true
		}
	}
	return false
}

// fileDiagnostic explains why an evidence citation of id failed: a wrong-kind
// declaration is the repairable case the feedback loop must distinguish from a
// genuinely unknown id, so the repairing agent fixes the kind instead of
// hunting for a missing file.
func fileDiagnostic(files []file, id string) string {
	for _, f := range files {
		if f.ID == id && f.Kind != "evidence" {
			return fmt.Sprintf("unknown worker evidence file %s (the file exists but is declared kind=%s; evidence citations require kind=evidence)", id, f.Kind)
		}
	}
	return fmt.Sprintf("unknown worker evidence file %s (no files[] entry declares this id)", id)
}
func checkSource(s Source, files []file) (bool, error) {
	if s.Ref != nil {
		return false, fmt.Errorf("retained source requires intake lineage")
	}
	if s.Status == "available" {
		if !hasFile(files, s.FileID) {
			return false, fmt.Errorf("missing evidence file %q", s.FileID)
		}
		return true, nil
	}
	if !nonblank(s.Reason) {
		return false, fmt.Errorf("unavailable source needs reason")
	}
	if s.FileID != "" && !hasFile(files, s.FileID) {
		return false, fmt.Errorf("unknown partial file")
	}
	return false, nil
}

// Store has already verified containment and digests. Bound the second read as
// well, and avoid blocking on a replaced FIFO while inspecting raw API pages.
func rawFile(ctx context.Context, ref contract.Ref, files []file, id string) ([]byte, error) {
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
	// Keep this adapter's before/after cancellation and I/O-error precedence.
	raw, err := contract.ReadBounded(context.Background(), f, limit)
	if errors.Is(err, contract.ErrReadLimit) {
		return nil, fmt.Errorf("evidence exceeds read limit")
	}
	// Preserve io.ReadAll's non-nil empty result, including zero-byte I/O errors.
	if raw == nil {
		raw = []byte{}
	}
	if err == nil {
		err = context.Cause(ctx)
	}
	return raw, err
}
func rawJSON(ctx context.Context, ref contract.Ref, files []file, id string, value any) error {
	raw, err := rawFile(ctx, ref, files, id)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, value)
}

func checkIntake(ctx context.Context, r *engine.Run, ref contract.Ref, ticket string) (Intake, error) {
	return newAcceptance(ctx, r).checkIntake(ref, ticket)
}

func (a *acceptance) checkIntake(ref contract.Ref, ticket string) (Intake, error) {
	h, err := a.loadIntakeHistory(ref, ticket)
	return h.value, err
}

func checkIntakePublication(ctx context.Context, ref contract.Ref, p publication[Intake], ticket string, sources map[contract.Ref][]file, priorInventory *Source) (Intake, error) {
	v := p.Data
	owner := func(s Source) (contract.Ref, []file) {
		if s.Ref != nil {
			return *s.Ref, sources[*s.Ref]
		}
		return ref, p.Files
	}
	readSource := func(s Source) ([]byte, error) {
		ref, files := owner(s)
		return rawFile(ctx, ref, files, s.FileID)
	}
	decodeSource := func(s Source, value any) error {
		ref, files := owner(s)
		return rawJSON(ctx, ref, files, s.FileID, value)
	}
	u, err := url.Parse(v.URL)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/browse/"+ticket || v.Ticket != ticket {
		return v, fmt.Errorf("ticket/source mismatch")
	}
	if _, err := utc(v.FetchedAt); err != nil {
		return v, err
	}
	complete := true
	check := func(s Source) error {
		_, files := owner(s)
		s.Ref = nil
		ok, err := checkSource(s, files)
		complete = complete && ok
		return err
	}
	if v.Acquisition != nil {
		if err := check(*v.Acquisition); err != nil {
			return v, err
		}
	}
	if err := check(v.Issue); err != nil {
		return v, err
	}
	if err := check(v.Fields); err != nil {
		return v, err
	}
	if v.Fields.Status == "available" {
		var fields []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		if err := decodeSource(v.Fields, &fields); err != nil {
			return v, err
		}
		seen := map[string]bool{}
		if len(fields) == 0 {
			return v, fmt.Errorf("empty field metadata")
		}
		for _, f := range fields {
			if !nonblank(f.ID) || !nonblank(f.Name) || seen[f.ID] {
				return v, fmt.Errorf("invalid field metadata")
			}
			seen[f.ID] = true
		}
	}
	var issue jiraIssue
	if v.Issue.Status == "available" {
		if err := decodeSource(v.Issue, &issue); err != nil {
			return v, err
		}
		if issue.Key != ticket {
			return v, fmt.Errorf("raw issue key mismatch")
		}
		for _, name := range []string{"description", "comment", "issuelinks", "attachment"} {
			if _, ok := issue.Fields[name]; !ok {
				return v, fmt.Errorf("raw issue omitted %s", name)
			}
		}
	}
	var embedded struct {
		Total *int `json:"total"`
	}
	if issue.Fields != nil {
		if err := json.Unmarshal(issue.Fields["comment"], &embedded); err != nil {
			return v, err
		}
		if embedded.Total == nil || *embedded.Total < 0 {
			return v, fmt.Errorf("missing comment total")
		}
	}
	next, total := 0, -1
	ids := map[string]bool{}
	starts := map[int]bool{}
	for _, page := range v.Comments {
		if starts[page.Start] {
			return v, fmt.Errorf("duplicate comment page")
		}
		starts[page.Start] = true
		if err := check(page.Source); err != nil {
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
		if err := decodeSource(page.Source, &raw); err != nil {
			return v, err
		}
		if raw.Start == nil || raw.Total == nil || raw.Comments == nil || *raw.Total < 0 || *raw.Start != page.Start {
			return v, fmt.Errorf("invalid comment page metadata")
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
			return v, fmt.Errorf("comment page exceeds total")
		}
		for _, c := range raw.Comments {
			if !nonblank(c.ID) || ids[c.ID] || len(c.Body) == 0 || strings.TrimSpace(string(c.Body)) == "null" {
				return v, fmt.Errorf("duplicate/incomplete comment")
			}
			ids[c.ID] = true
		}
	}
	complete = complete && total >= 0 && next == total && len(ids) == total && embedded.Total != nil && total == *embedded.Total
	// A failed replacement does not erase the historical inventory. Use its
	// exact raw owner for structural checks only; the new issue remains a gap.
	inventory := issue
	if v.Issue.Status != "available" && priorInventory != nil {
		if err := decodeSource(*priorInventory, &inventory); err != nil {
			return v, err
		}
	}
	var links []jiraLink
	var attachments []jiraAttachment
	if inventory.Fields != nil {
		if err := json.Unmarshal(inventory.Fields["issuelinks"], &links); err != nil {
			return v, err
		}
		if err := json.Unmarshal(inventory.Fields["attachment"], &attachments); err != nil {
			return v, err
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
	for _, l := range v.Linked {
		if !expected[l.Key] || seen[l.Key] {
			return v, fmt.Errorf("unexpected/duplicate linked issue")
		}
		seen[l.Key] = true
		if err := check(l.Source); err != nil {
			return v, err
		}
		if l.Source.Status == "available" {
			var raw jiraIssue
			if err := decodeSource(l.Source, &raw); err != nil {
				return v, err
			}
			if raw.Key != l.Key || len(raw.Fields) == 0 {
				return v, fmt.Errorf("linked issue snapshot mismatch")
			}
		}
	}
	if len(seen) != len(expected) {
		return v, fmt.Errorf("linked issue inventory omitted entries")
	}
	sizes := map[string]int64{}
	for _, a := range attachments {
		_, duplicate := sizes[a.ID]
		if !nonblank(a.ID) || a.Size == nil || *a.Size < 0 || duplicate {
			return v, fmt.Errorf("invalid raw attachment")
		}
		sizes[a.ID] = *a.Size
	}
	seen = map[string]bool{}
	for _, a := range v.Attachments {
		size, ok := sizes[a.ID]
		if !ok || seen[a.ID] {
			return v, fmt.Errorf("unexpected/duplicate attachment")
		}
		seen[a.ID] = true
		if err := check(a.Content); err != nil {
			return v, err
		}
		if err := check(a.Analysis); err != nil {
			return v, err
		}
		// Retained bytes were checked against their owning intake's inventory
		// earlier in the lineage, not metadata from a later issue snapshot.
		if a.Content.Status == "available" && a.Content.Ref == nil {
			raw, err := readSource(a.Content)
			if err != nil {
				return v, err
			}
			if int64(len(raw)) != size {
				return v, fmt.Errorf("truncated attachment")
			}
		} else if a.Content.Status != "available" && a.Analysis.Status == "available" && a.Analysis.Ref == nil {
			return v, fmt.Errorf("analysis without attachment content")
		}
	}
	if len(seen) != len(sizes) {
		return v, fmt.Errorf("attachment inventory omitted entries")
	}
	if v.Complete != complete || !texts(v.Gaps) || (!complete && len(v.Gaps) == 0) || (complete && len(v.Gaps) != 0) {
		return v, fmt.Errorf("intake completeness/gaps mismatch")
	}
	return v, nil
}

func wikiComplete(v WikiSearch) bool {
	return v.Status == "completed-with-matches" || v.Status == "completed-no-matches"
}
func checkWiki(ctx context.Context, r *engine.Run, ref, intake contract.Ref) (WikiSearch, error) {
	return newAcceptance(ctx, r).checkWiki(ref, intake)
}

func (a *acceptance) checkWiki(ref, intake contract.Ref) (WikiSearch, error) {
	p, err := readAccepted[WikiSearch](a, ref, WikiSchema)
	if err != nil {
		return p.Data, err
	}
	return checkWikiPublication(p, intake)
}

func checkWikiPublication(p publication[WikiSearch], intake contract.Ref) (WikiSearch, error) {
	v := p.Data
	if v.Intake != intake || v.Scope != "wiki-only" || len(v.Queries) == 0 || !texts(v.Queries) || !texts(v.Gaps) {
		return v, fmt.Errorf("wiki search provenance missing")
	}
	available, err := checkSource(v.Search, p.Files)
	if err != nil {
		return v, err
	}
	for _, page := range v.Pages {
		ok, err := checkSource(page, p.Files)
		if err != nil {
			return v, err
		}
		available = available && ok
	}
	if wikiComplete(v) {
		if !available || len(v.Gaps) != 0 || (v.Status == "completed-no-matches") != (len(v.Pages) == 0) {
			return v, fmt.Errorf("wiki completion inconsistent")
		}
	} else if len(v.Gaps) == 0 {
		return v, fmt.Errorf("wiki incomplete requires gaps")
	}
	return v, nil
}

func utc(value string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || !strings.HasSuffix(value, "Z") {
		return t, fmt.Errorf("explicit UTC required: %q", value)
	}
	return t, nil
}

func checkTime(v TimeResolution, evidence func(Evidence) error) error {
	var first, last time.Time
	for _, a := range v.Anchors {
		if !nonblank(a.Event) || !nonblank(a.SourceTZ) {
			return fmt.Errorf("time anchor lacks event/time basis")
		}
		if err := evidence(a.Evidence); err != nil {
			return err
		}
		want, err := utc(a.UTC)
		if err != nil {
			return err
		}
		var actual time.Time
		switch a.Format {
		case "rfc3339":
			actual, err = time.Parse(time.RFC3339Nano, a.Original)
			_, offset := actual.Zone()
			if err == nil && offset != a.OffsetSeconds {
				return fmt.Errorf("timestamp offset mismatch")
			}
		case "epoch-seconds", "epoch-millis":
			n, e := strconv.ParseInt(a.Original, 10, 64)
			err = e
			if a.Format == "epoch-seconds" {
				actual = time.Unix(n, 0)
			} else {
				actual = time.UnixMilli(n)
			}
			if a.OffsetSeconds != 0 || a.SourceTZ != "UTC" {
				return fmt.Errorf("epoch basis must be UTC")
			}
		case "local-paired":
			if a.PairedEpochMillis == nil || a.PairedEvidence == nil {
				return fmt.Errorf("local timestamp needs same-event absolute evidence")
			}
			if reflect.DeepEqual(a.Evidence, *a.PairedEvidence) {
				return fmt.Errorf("local timestamp cannot be its own absolute evidence")
			}
			if err := evidence(*a.PairedEvidence); err != nil {
				return err
			}
			local, e := time.Parse("2006-01-02T15:04:05.999999999", a.Original)
			err = e
			actual = time.UnixMilli(*a.PairedEpochMillis)
			if !local.Add(-time.Duration(a.OffsetSeconds) * time.Second).Equal(actual) {
				return fmt.Errorf("calculated local/epoch offset mismatch")
			}
		default:
			return fmt.Errorf("unsupported time format")
		}
		if a.Format != "local-paired" && (a.PairedEpochMillis != nil || a.PairedEvidence != nil) {
			return fmt.Errorf("unexpected paired anchor")
		}
		if err != nil || !actual.Equal(want) {
			return fmt.Errorf("UTC conversion mismatch")
		}
		if first.IsZero() || want.Before(first) {
			first = want
		}
		if last.IsZero() || want.After(last) {
			last = want
		}
	}
	if v.Status == "resolved" {
		from, e1 := utc(v.From)
		to, e2 := utc(v.To)
		// Observed incident bounds are separate from query windows, which the
		// agent selects and adjusts autonomously within the authorized task.
		if e1 != nil || e2 != nil || first.IsZero() || !from.Equal(first) || !to.Equal(last) {
			return fmt.Errorf("UTC window must match observed anchors")
		}
	} else if v.From != "" || v.To != "" {
		return fmt.Errorf("unresolved/conflicting time cannot authorize a window")
	}
	return nil
}

func checkSupportingQuery(q SupportingQuery, evidence func(Evidence) error) error {
	from, e1 := utc(q.From)
	to, e2 := utc(q.To)
	if e1 != nil || e2 != nil || !from.Before(to) {
		return fmt.Errorf("supporting query requires a nonzero UTC window")
	}
	if !nonblank(q.Source) || !nonblank(q.Filter) || !nonblank(q.Outcome) || len(q.Basis) == 0 || len(q.Evidence) == 0 {
		return fmt.Errorf("supporting query lacks conditions, basis or result evidence")
	}
	for _, e := range append(slices.Clone(q.Basis), q.Evidence...) {
		if err := evidence(e); err != nil {
			return err
		}
	}
	return nil
}

func checkContext(ctx context.Context, r *engine.Run, ref contract.Ref, scope Scope, intakeRef, wikiRef contract.Ref, intake Intake, wiki WikiSearch, history ...contextHistory) (Context, error) {
	return newAcceptance(ctx, r).checkContext(ref, scope, intakeRef, wikiRef, intake, wiki, history...)
}

func (a *acceptance) checkContext(ref contract.Ref, scope Scope, intakeRef, wikiRef contract.Ref, intake Intake, wiki WikiSearch, history ...contextHistory) (Context, error) {
	p, err := readAccepted[Context](a, ref, ContextSchema)
	if err != nil {
		return p.Data, err
	}
	v := p.Data
	if v.Intake != intakeRef || v.Wiki != wikiRef || !reflect.DeepEqual(v.Scope, scope) || !nonblank(v.Problem) {
		return v, fmt.Errorf("context source/scope mismatch")
	}
	ip, err := readAccepted[Intake](a, intakeRef, IntakeSchema)
	if err != nil {
		return v, err
	}
	wp, err := readAccepted[WikiSearch](a, wikiRef, WikiSchema)
	if err != nil {
		return v, err
	}
	ih, err := a.loadIntakeHistory(intakeRef, scope.Ticket)
	if err != nil {
		return v, err
	}
	sources := ih.sources
	sources[intakeRef], sources[wikiRef] = ip.Files, wp.Files
	return checkContextPublication(a.ctx, ref, p, scope, intakeRef, wikiRef, intake, wiki, sources, history...)
}

func checkContextPublication(ctx context.Context, ref contract.Ref, p publication[Context], scope Scope, intakeRef, wikiRef contract.Ref, intake Intake, wiki WikiSearch, sources map[contract.Ref][]file, history ...contextHistory) (Context, error) {
	v := p.Data
	if v.Intake != intakeRef || v.Wiki != wikiRef || !reflect.DeepEqual(v.Scope, scope) || !nonblank(v.Problem) {
		return v, fmt.Errorf("context source/scope mismatch")
	}
	if len(history) == 0 {
		if v.Previous != nil || len(v.ResolvedGaps) != 0 {
			return v, fmt.Errorf("initial context cannot invent resolution lineage")
		}
	} else {
		if v.Intake != history[0].value.Intake && (intake.Previous == nil || *intake.Previous != history[0].value.Intake || wikiRef == history[0].value.Wiki) {
			return v, fmt.Errorf("context intake revision must extend prior intake and bind new wiki")
		}
		if v.Previous == nil || *v.Previous != history[0].ref {
			return v, fmt.Errorf("context revision must bind exact previous input")
		}
		for input, files := range history[0].sources {
			sources[input] = files
		}
	}
	evidence := func(e Evidence) error {
		files := p.Files
		if e.Ref != nil {
			var ok bool
			files, ok = sources[*e.Ref]
			if !ok {
				return fmt.Errorf("evidence is not an exact committed input")
			}
		}
		if !hasFile(files, e.FileID) {
			return fmt.Errorf("%s", fileDiagnostic(files, e.FileID))
		}
		return nil
	}
	fact := func(f Fact) error {
		if nonblank(f.Value) && len(f.Evidence) == 0 {
			return fmt.Errorf("fact lacks evidence")
		}
		for _, e := range f.Evidence {
			if err := evidence(e); err != nil {
				return err
			}
		}
		return nil
	}
	i := v.Identity
	for _, f := range append([]Fact{i.Stack, i.Pop, i.Binding, i.TenantID, i.OrgKey, i.UserKey, i.Release}, v.Observations...) {
		if err := fact(f); err != nil {
			return v, err
		}
	}
	if i.Lookup != nil {
		if err := evidence(*i.Lookup); err != nil {
			return v, err
		}
	}
	if i.Status == "resolved" {
		if !nonblank(i.Stack.Value) || !nonblank(i.Pop.Value) || !nonblank(i.Binding.Value) || !nonblank(i.TenantID.Value) {
			return v, fmt.Errorf("resolved identity requires complete target facts")
		}
		if i.Lookup == nil {
			return v, fmt.Errorf("resolved identity requires target/DB resolution receipt")
		}
		lookupRef, lookupFiles := ref, p.Files
		if i.Lookup.Ref != nil {
			lookupRef = *i.Lookup.Ref
			lookupFiles = sources[lookupRef]
		}
		var lookup IdentityLookup
		if err := rawJSON(ctx, lookupRef, lookupFiles, i.Lookup.FileID, &lookup); err != nil {
			return v, err
		}
		if len(lookup.Matches) != 1 || lookup.Matches[0].TenantID != i.TenantID.Value || lookup.Matches[0].OrgKey != i.OrgKey.Value || lookup.Stack != i.Stack.Value || lookup.Pop != i.Pop.Value || lookup.Binding != i.Binding.Value || lookup.Release != i.Release.Value {
			return v, fmt.Errorf("identity conflicts with target/DB resolution receipt")
		}
		if i.Stack.Value != scope.Stack || i.Pop.Value != scope.Pop || i.Binding.Value != scope.Binding || !slices.Contains(scope.TenantIDs, i.TenantID.Value) || !nonblank(i.OrgKey.Value) || !nonblank(i.Release.Value) {
			return v, fmt.Errorf("resolved identity outside scope or missing required facts")
		}
	}
	if err := checkTime(v.Time, evidence); err != nil {
		return v, err
	}
	kinds := map[string]bool{}
	for _, a := range v.Attempts {
		if !nonblank(a.Source) || !nonblank(a.Outcome) || len(a.Evidence) == 0 {
			return v, fmt.Errorf("resolution attempt lacks outcome/evidence")
		}
		for _, e := range a.Evidence {
			if err := evidence(e); err != nil {
				return v, err
			}
		}
		for _, q := range a.Queries {
			if err := checkSupportingQuery(q, evidence); err != nil {
				return v, err
			}
		}
		kinds[a.Kind] = true
	}
	if !kinds["identity"] || !kinds["time"] {
		return v, fmt.Errorf("active identity and time resolution required")
	}
	attachmentsComplete := true
	for _, a := range intake.Attachments {
		attachmentsComplete = attachmentsComplete && a.Content.Status == "available" && a.Analysis.Status == "available"
	}
	if v.AttachmentComplete != attachmentsComplete || v.WikiStatus != wiki.Status {
		return v, fmt.Errorf("context completeness differs from inputs")
	}
	ready := intake.Complete && wikiComplete(wiki) && i.Status == "resolved" && v.Time.Status == "resolved" && len(v.Gaps) == 0
	if !texts(v.Gaps) || (v.Readiness == "ready") != ready || (!ready && len(v.Gaps) == 0) {
		return v, fmt.Errorf("context readiness/gaps mismatch")
	}
	for _, gap := range append(slices.Clone(intake.Gaps), wiki.Gaps...) {
		if !slices.Contains(v.Gaps, gap) {
			return v, fmt.Errorf("context dropped upstream gap")
		}
	}
	if len(history) > 0 {
		if err := checkRevision(v, history[0], fact, wikiRef); err != nil {
			return v, err
		}
	}
	return v, nil
}
