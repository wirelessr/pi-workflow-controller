package triage

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

const RoundSchema = "triage.round.v1"

type Round struct {
	Status          string           `json:"status"`
	Summary         string           `json:"summary"`
	FactsUpdate     []Fact           `json:"facts_update"`
	TimeAnchors     []TimeAnchor     `json:"time_anchors"`
	Identity        *Identity        `json:"identity"`
	Receipts        []Receipt        `json:"receipts"`
	DeployedBuilds  []Build          `json:"deployed_builds"`
	VisionRequests  []VisionRequest  `json:"vision_requests"`
	Candidate       *Candidate       `json:"candidate"`
	Unblock         *string          `json:"unblock"`
	Gaps            []Gap            `json:"gaps"`
	GapDispositions []GapDisposition `json:"gap_dispositions"`
}

type Identity struct {
	Lookups   []Lookup   `json:"lookups"`
	Decisions []Decision `json:"decisions"`
}

type Lookup struct {
	ID         string     `json:"id"`
	Stack      string     `json:"stack"`
	Kubeconfig *string    `json:"kubeconfig"`
	Context    *string    `json:"context"`
	Status     string     `json:"status"`
	Rows       []Row      `json:"rows"`
	Evidence   []Evidence `json:"evidence"`
	Note       string     `json:"note"`
}

type Row struct {
	TenantID   *string `json:"tenant_id"`
	Orgkey     *string `json:"orgkey"`
	UIHostname *string `json:"ui_hostname"`
	Name       *string `json:"name"`
}

func (r Row) column(name string) *string {
	return map[string]*string{"tenant_id": r.TenantID, "orgkey": r.Orgkey, "ui_hostname": r.UIHostname, "name": r.Name}[name]
}

type Decision struct {
	ID          string   `json:"id"`
	Fact        string   `json:"fact"`
	Value       *string  `json:"value"`
	Status      string   `json:"status"`
	Lookup      *string  `json:"lookup"`
	Row         *int     `json:"row"`
	Identifiers []string `json:"identifiers"`
	Reason      string   `json:"reason"`
}

type Receipt struct {
	ID          string     `json:"id"`
	Target      string     `json:"target"`
	TargetBasis *string    `json:"target_basis"`
	Source      string     `json:"source"`
	Condition   string     `json:"condition"`
	Kind        string     `json:"kind"`
	From        *string    `json:"from"`
	To          *string    `json:"to"`
	At          *string    `json:"at"`
	TimeBasis   []Evidence `json:"time_basis"`
	Status      string     `json:"status"`
	Outcome     string     `json:"outcome"`
	Evidence    []Evidence `json:"evidence"`
}

type Build struct {
	ID        string     `json:"id"`
	Stack     string     `json:"stack"`
	Component string     `json:"component"`
	Build     string     `json:"build"`
	Evidence  []Evidence `json:"evidence"`
}

type Candidate struct {
	Statement       string     `json:"statement"`
	Premises        []string   `json:"premises"`
	AllowedEvidence []Evidence `json:"allowed_evidence"`
	CodeRefs        []CodeRef  `json:"code_refs"`
	NoCodeBasis     bool       `json:"no_code_basis"`
	Verification    string     `json:"verification"`
}

// CodeRef attributes code the claim relies on to the revision it was read
// at; Evidence is the committed copy of the excerpt.
type CodeRef struct {
	Repo     string     `json:"repo"`
	Ref      string     `json:"ref"`
	Path     string     `json:"path"`
	Relation string     `json:"relation"`
	Basis    *BuildRef  `json:"basis"`
	Evidence []Evidence `json:"evidence"`
}

// BuildRef names a deployed build: in this round (nil Ref) or in an input.
type BuildRef struct {
	Ref *contract.Ref `json:"ref"`
	ID  string        `json:"id"`
}

type GapRef struct {
	Ref contract.Ref `json:"ref"`
	ID  string       `json:"id"`
}

type GapDisposition struct {
	Gap         GapRef     `json:"gap"`
	Disposition string     `json:"disposition"`
	Reason      string     `json:"reason"`
	Evidence    []Evidence `json:"evidence"`
}

// roundCheck is what a round's acceptance needs from the Controller: the
// request's inputs and the items already recorded absent, which may not be
// declared again.
type roundCheck struct {
	inputs []contract.Ref
	absent map[string]string
}

// checkRound is the structural acceptance of a round: unique ids,
// resolvable citations, recomputed anchors, identity decisions that echo
// their lookup row, nonzero UTC receipt windows and dispositions naming a
// real earlier gap. Whether evidence supports a decision is judged later.
func checkRound(ctx context.Context, r *engine.Run, ref contract.Ref, c roundCheck) (Round, error) {
	p, err := readAccepted[Round](ctx, r, ref, RoundSchema)
	if err != nil {
		return Round{}, err
	}
	v := p.Data
	in, err := citable(ctx, r, c.inputs...)
	if err != nil {
		return v, err
	}
	cite := citations{ctx, in, ref, p.Files}.check
	for i, f := range v.FactsUpdate {
		if gap, ok := c.absent[factKey(f)]; ok {
			return v, fmt.Errorf("facts_update[%d]: %s %q was not accepted too often and is recorded absent (%s); do not declare it again", i, f.Kind, f.Value, gap)
		}
	}
	for i, a := range v.TimeAnchors {
		if gap, ok := c.absent[anchorKey(a)]; ok {
			return v, fmt.Errorf("time_anchors[%d]: the anchor at %s was not accepted too often and is recorded absent (%s); do not declare it again", i, a.UTC, gap)
		}
	}
	visionRef := func(ref *contract.Ref) bool {
		if ref == nil {
			return true
		}
		_, ok := in.Citable[*ref]
		return ok
	}
	ids := idSet{}
	if err := checkDeclared(ids, cite, "facts_update", v.FactsUpdate, v.TimeAnchors, v.VisionRequests, visionRef, "an input of this request holding the image, or null for an image this contract fetched itself"); err != nil {
		return v, err
	}
	if v.Identity != nil {
		if err := checkIdentity(ids, cite, *v.Identity, c.absent); err != nil {
			return v, err
		}
	}
	for i, q := range v.Receipts {
		field := fmt.Sprintf("receipts[%d]", i)
		if err := ids.add(field, q.ID); err != nil {
			return v, err
		}
		if err := checkReceipt(field, q, cite); err != nil {
			return v, err
		}
	}
	for i, b := range v.DeployedBuilds {
		field := fmt.Sprintf("deployed_builds[%d]", i)
		if err := ids.add(field, b.ID); err != nil {
			return v, err
		}
		if err := citeAll(cite, field+".evidence", b.Evidence); err != nil {
			return v, err
		}
	}
	if v.Candidate != nil {
		if err := checkCandidate(ctx, r, *v.Candidate, v.DeployedBuilds, in, cite); err != nil {
			return v, err
		}
	}
	for i, d := range v.GapDispositions {
		field := fmt.Sprintf("gap_dispositions[%d]", i)
		gaps, err := inputItems(ctx, r, field+".gap.ref", d.Gap.Ref, in, "gaps")
		if err != nil {
			return v, err
		}
		if !slices.Contains(gaps, d.Gap.ID) {
			return v, fmt.Errorf("%s.gap.id: %s has no gap %q", field, describeRef(d.Gap.Ref), d.Gap.ID)
		}
		if err := citeAll(cite, field+".evidence", d.Evidence); err != nil {
			return v, err
		}
	}
	return v, checkGaps("gaps", v.Gaps)
}

func citeAll(cite func(string, Evidence) error, field string, evidence []Evidence) error {
	for i, e := range evidence {
		if err := cite(fmt.Sprintf("%s[%d]", field, i), e); err != nil {
			return err
		}
	}
	return nil
}

// checkIdentity keeps the approved receipt echo: a confirmed value equals
// the cited row's column (home_pop equals the lookup's stack), and the
// identifiers it rests on are present in that row. Which rows to trust and
// whether the identifiers suffice stay the Agent's and validator's call.
func checkIdentity(ids idSet, cite func(string, Evidence) error, v Identity, absent map[string]string) error {
	lookups := map[string]Lookup{}
	for i, l := range v.Lookups {
		field := fmt.Sprintf("identity.lookups[%d]", i)
		if err := ids.add(field, l.ID); err != nil {
			return err
		}
		if err := citeAll(cite, field+".evidence", l.Evidence); err != nil {
			return err
		}
		lookups[l.ID] = l
	}
	confirmed := map[string]string{}
	for i, d := range v.Decisions {
		field := fmt.Sprintf("identity.decisions[%d]", i)
		if err := ids.add(field, d.ID); err != nil {
			return err
		}
		if d.Status != "confirmed" {
			continue
		}
		if gap, ok := absent[decisionKey(d)]; ok {
			return fmt.Errorf("%s: confirming %s %q was not accepted too often and is recorded absent (%s); do not declare it again", field, d.Fact, *d.Value, gap)
		}
		if prior, ok := confirmed[d.Fact]; ok {
			return fmt.Errorf("%s: %s is already confirmed at %s; want at most one confirmed decision per fact", field, d.Fact, prior)
		}
		confirmed[d.Fact] = field
		l, ok := lookups[*d.Lookup]
		if !ok {
			return fmt.Errorf("%s.lookup: got %q; want the id of a lookup in identity.lookups", field, *d.Lookup)
		}
		if l.Status != "ok" {
			return fmt.Errorf("%s.lookup: lookup %q has status %s; a confirmed decision needs a lookup that ran (ok)", field, l.ID, l.Status)
		}
		if *d.Row >= len(l.Rows) {
			return fmt.Errorf("%s.row: got %d; lookup %q has %d rows", field, *d.Row, l.ID, len(l.Rows))
		}
		row := l.Rows[*d.Row]
		want, column := l.Stack, "stack"
		if d.Fact != "home_pop" {
			got := row.column(d.Fact)
			if got == nil {
				return fmt.Errorf("%s.value: lookup %q row %d has no %s; a confirmed value must equal the row", field, l.ID, *d.Row, d.Fact)
			}
			want, column = *got, d.Fact
		}
		if *d.Value != want {
			return fmt.Errorf("%s.value: got %q; want %q, the %s of lookup %q row %d", field, *d.Value, want, column, l.ID, *d.Row)
		}
		for _, name := range d.Identifiers {
			if row.column(name) == nil {
				return fmt.Errorf("%s.identifiers: %s is null in lookup %q row %d; list only columns the row has", field, name, l.ID, *d.Row)
			}
		}
	}
	return nil
}

// checkReceipt carries the approved supporting-query rules for a
// time-ranged search: a nonzero explicit UTC window. A snapshot of current
// state has no window, only the explicit UTC time it was read (a decided
// relaxation: the old rule forced a made-up window onto database reads).
// Both cite their result evidence.
func checkReceipt(field string, q Receipt, cite func(string, Evidence) error) error {
	switch q.Kind {
	case "window":
		if q.From == nil || q.To == nil {
			return fmt.Errorf("%s: a window receipt needs from and to", field)
		}
		from, err := utc(*q.From)
		if err != nil {
			return fmt.Errorf("%s.from: %w", field, err)
		}
		to, err := utc(*q.To)
		if err != nil {
			return fmt.Errorf("%s.to: %w", field, err)
		}
		if !from.Before(to) {
			return fmt.Errorf("%s: from %q is not strictly before to %q; want a nonzero UTC window, or kind snapshot with at when the query read current state", field, *q.From, *q.To)
		}
	case "snapshot":
		if q.At == nil {
			return fmt.Errorf("%s: a snapshot receipt needs at", field)
		}
		if _, err := utc(*q.At); err != nil {
			return fmt.Errorf("%s.at: %w", field, err)
		}
	default:
		return fmt.Errorf("%s.kind: got %q; want window or snapshot", field, q.Kind)
	}
	if !nonblank(q.Source) || !nonblank(q.Condition) || !nonblank(q.Outcome) {
		return fmt.Errorf("%s: source, condition and outcome must not be blank", field)
	}
	if err := citeAll(cite, field+".time_basis", q.TimeBasis); err != nil {
		return err
	}
	return citeAll(cite, field+".evidence", q.Evidence)
}

// checkCandidate keeps the claim checkable from evidence alone: allowed
// evidence resolves, every code excerpt is allowed evidence, and code read
// at the deployed revision names the deployed build it rests on.
func checkCandidate(ctx context.Context, r *engine.Run, c Candidate, builds []Build, in Inputs, cite func(string, Evidence) error) error {
	if err := citeAll(cite, "candidate.allowed_evidence", c.AllowedEvidence); err != nil {
		return err
	}
	// Verifiers must not read other roles' judgments or session logs.
	judged := func(field string, e Evidence) error {
		if e.Ref != nil && slices.Contains(judgmentSchemas, e.Ref.SchemaID) {
			return fmt.Errorf("%s.ref: got %s, a %s; verifiers may not read it, so copy what the claim needs into this round's own evidence files", field, describeRef(*e.Ref), e.Ref.SchemaID)
		}
		return nil
	}
	for i, e := range c.AllowedEvidence {
		if err := judged(fmt.Sprintf("candidate.allowed_evidence[%d]", i), e); err != nil {
			return err
		}
	}
	allowed := map[citedFile]bool{}
	for _, e := range c.AllowedEvidence {
		allowed[fileOf(e)] = true
	}
	own := []string{}
	for _, b := range builds {
		own = append(own, b.ID)
	}
	for i, code := range c.CodeRefs {
		field := fmt.Sprintf("candidate.code_refs[%d]", i)
		for j, e := range code.Evidence {
			if err := cite(fmt.Sprintf("%s.evidence[%d]", field, j), e); err != nil {
				return err
			}
			if err := judged(fmt.Sprintf("%s.evidence[%d]", field, j), e); err != nil {
				return err
			}
			if !allowed[fileOf(e)] {
				return fmt.Errorf("%s.evidence[%d]: file %q is not in candidate.allowed_evidence; list every code excerpt there so a verifier may read it", field, j, e.FileID)
			}
		}
		if code.Basis == nil {
			continue
		}
		ids, where := own, "this round's deployed_builds"
		if code.Basis.Ref != nil {
			var err error
			if ids, err = inputItems(ctx, r, field+".basis.ref", *code.Basis.Ref, in, "deployed_builds"); err != nil {
				return err
			}
			where = "the deployed_builds of " + describeRef(*code.Basis.Ref)
		}
		if !slices.Contains(ids, code.Basis.ID) {
			return fmt.Errorf("%s.basis.id: got %q; want the id of a build in %s", field, code.Basis.ID, where)
		}
	}
	return nil
}

// judgmentSchemas are contracts holding other roles' judgments or session
// logs; a claim may not hand them to verifiers as evidence.
var judgmentSchemas = []string{StewardSchema, AuditSchema, FactCheckSchema, ClaimSchema, VerificationSchema, DeliverySchema, ObservationSchema}

type citedFile struct {
	ref    contract.Ref
	fileID string
}

func fileOf(e Evidence) citedFile {
	f := citedFile{fileID: e.FileID}
	if e.Ref != nil {
		f.ref = *e.Ref
	}
	return f
}

// inputItems reads the ids of a list in an input of this request, such as
// its gaps or deployed builds.
func inputItems(ctx context.Context, r *engine.Run, field string, ref contract.Ref, in Inputs, list string) ([]string, error) {
	if _, ok := in.Citable[ref]; !ok {
		return nil, fmt.Errorf("%s: got %s; want an input of this request", field, describeRef(ref))
	}
	raw, err := engine.ReadContract(ctx, r, ref)
	if err != nil {
		return nil, err
	}
	p, err := contract.DecodePublication[map[string]json.RawMessage](raw)
	if err != nil {
		return nil, err
	}
	var items []struct {
		ID string `json:"id"`
	}
	if data, ok := p.Data[list]; ok {
		if err := json.Unmarshal(data, &items); err != nil {
			return nil, fmt.Errorf("%s: %s: %w", field, list, err)
		}
	}
	ids := []string{}
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids, nil
}

// factKey, decisionKey and anchorKey identify a declared item across
// rounds for the rejection limit; item ids are only unique within one
// contract.
func factKey(f Fact) string { return "fact:" + f.Kind + "=" + f.Value }

func decisionKey(d Decision) string { return "identity:" + d.Fact + "=" + *d.Value }

func anchorKey(a TimeAnchor) string { return "anchor:" + a.Event + "@" + a.UTC }

// judged is an item of a round the fact check must judge, with its key.
type judged struct{ id, key string }

// roundJudged lists the declared facts, anchors and confirmed identity
// decisions; an unconfirmed decision unlocks nothing and is not judged.
func roundJudged(v Round) []judged {
	var out []judged
	for _, f := range v.FactsUpdate {
		out = append(out, judged{f.ID, factKey(f)})
	}
	for _, a := range v.TimeAnchors {
		out = append(out, judged{a.ID, anchorKey(a)})
	}
	if v.Identity != nil {
		for _, d := range v.Identity.Decisions {
			if d.Status == "confirmed" {
				out = append(out, judged{d.ID, decisionKey(d)})
			}
		}
	}
	return out
}
