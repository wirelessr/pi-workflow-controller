package triagev2

import (
	"context"
	"fmt"

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
	From        string     `json:"from"`
	To          string     `json:"to"`
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
	Basis           string     `json:"basis"`
}

type CodeRef struct {
	Repo   string `json:"repo"`
	Commit string `json:"commit"`
	Path   string `json:"path"`
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
	if err := checkDeclared(ids, cite, "facts_update", v.FactsUpdate, v.TimeAnchors, v.VisionRequests, visionRef); err != nil {
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
		if err := citeAll(cite, "candidate.allowed_evidence", v.Candidate.AllowedEvidence); err != nil {
			return v, err
		}
	}
	for i, d := range v.GapDispositions {
		field := fmt.Sprintf("gap_dispositions[%d]", i)
		if err := checkGapRef(ctx, r, field+".gap", d.Gap, in); err != nil {
			return v, err
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
		if gap, ok := absent[decisionKey(d)]; ok {
			return fmt.Errorf("%s: this %s decision was not accepted too often and is recorded absent (%s); do not declare it again", field, d.Fact, gap)
		}
		if d.Status != "confirmed" {
			continue
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
		want := l.Stack
		if d.Fact != "home_pop" {
			got := row.column(d.Fact)
			if got == nil {
				return fmt.Errorf("%s.value: lookup %q row %d has no %s; a confirmed value must equal the row", field, l.ID, *d.Row, d.Fact)
			}
			want = *got
		}
		if *d.Value != want {
			return fmt.Errorf("%s.value: got %q; want %q, the %s of lookup %q row %d", field, *d.Value, want, map[bool]string{true: "stack", false: d.Fact}[d.Fact == "home_pop"], l.ID, *d.Row)
		}
		for _, name := range d.Identifiers {
			if row.column(name) == nil {
				return fmt.Errorf("%s.identifiers: %s is null in lookup %q row %d; list only columns the row has", field, name, l.ID, *d.Row)
			}
		}
	}
	return nil
}

// checkReceipt carries the approved supporting-query rules: a nonzero
// explicit UTC window and cited basis and result evidence.
func checkReceipt(field string, q Receipt, cite func(string, Evidence) error) error {
	from, err := utc(q.From)
	if err != nil {
		return fmt.Errorf("%s.from: %w", field, err)
	}
	to, err := utc(q.To)
	if err != nil {
		return fmt.Errorf("%s.to: %w", field, err)
	}
	if !from.Before(to) {
		return fmt.Errorf("%s: from %q is not strictly before to %q; want a nonzero UTC window", field, q.From, q.To)
	}
	if err := citeAll(cite, field+".time_basis", q.TimeBasis); err != nil {
		return err
	}
	return citeAll(cite, field+".evidence", q.Evidence)
}

// checkGapRef requires the named gap to exist in the named input.
func checkGapRef(ctx context.Context, r *engine.Run, field string, g GapRef, in Inputs) error {
	if _, ok := in.Citable[g.Ref]; !ok {
		return fmt.Errorf("%s.ref: got %s; want an input of this request that owns the gap", field, describeRef(g.Ref))
	}
	raw, err := engine.ReadContract(ctx, r, g.Ref)
	if err != nil {
		return err
	}
	owner, err := contract.DecodePublication[struct {
		Gaps []Gap `json:"gaps"`
	}](raw)
	if err != nil {
		return err
	}
	for _, gap := range owner.Data.Gaps {
		if gap.ID == g.ID {
			return nil
		}
	}
	return fmt.Errorf("%s.id: %s has no gap %q", field, describeRef(g.Ref), g.ID)
}

// factKey and decisionKey identify a declared fact across rounds for the
// rejection limit; item ids are only unique within one contract.
func factKey(f Fact) string { return "fact:" + f.Kind + "=" + f.Value }

func decisionKey(d Decision) string {
	value := ""
	if d.Value != nil {
		value = *d.Value
	}
	return "identity:" + d.Fact + "=" + value + ":" + d.Status
}

func anchorKey(a TimeAnchor) string { return "anchor:" + a.UTC }

// roundJudged lists the items of a round the fact check must judge.
func roundJudged(v Round) []string {
	var ids []string
	for _, f := range v.FactsUpdate {
		ids = append(ids, f.ID)
	}
	for _, a := range v.TimeAnchors {
		ids = append(ids, a.ID)
	}
	if v.Identity != nil {
		for _, d := range v.Identity.Decisions {
			ids = append(ids, d.ID)
		}
	}
	return ids
}
