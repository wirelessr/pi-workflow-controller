package triagev2

import (
	"context"
	"fmt"
	"strings"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

const (
	PromptSchema     = "triage.prompt.v1"
	FactsSchema      = "triage.facts.v1"
	FactCheckSchema  = "triage.factcheck.v1"
	FactStatusSchema = "triage.factstatus.v1"
)

// CallerPrompt is the one-line prompt split into the ticket it names and
// free-text hints. Hints are candidate sources, never authorization.
type CallerPrompt struct {
	Ticket string `json:"ticket"`
	Hints  string `json:"hints"`
}

// ParseCallerPrompt requires the prompt to start with a ticket key.
func ParseCallerPrompt(prompt string) (CallerPrompt, error) {
	key, rest, _ := strings.Cut(strings.TrimSpace(prompt), " ")
	if !ticketKey.MatchString(key) {
		return CallerPrompt{}, fmt.Errorf("the prompt must start with a ticket key such as CASE-123, got %q", key)
	}
	return CallerPrompt{Ticket: key, Hints: strings.TrimSpace(rest)}, nil
}

type Fact struct {
	ID       string     `json:"id"`
	Kind     string     `json:"kind"`
	Value    string     `json:"value"`
	Evidence []Evidence `json:"evidence"`
	Note     string     `json:"note,omitempty"`
}

type VisionRequest struct {
	ID         string   `json:"id"`
	Attachment Evidence `json:"attachment"`
	Question   string   `json:"question"`
}

type Facts struct {
	Intake         contract.Ref    `json:"intake"`
	Prompt         contract.Ref    `json:"prompt"`
	Facts          []Fact          `json:"facts"`
	TimeAnchors    []TimeAnchor    `json:"time_anchors"`
	VisionRequests []VisionRequest `json:"vision_requests"`
	Gaps           []Gap           `json:"gaps"`
}

type FactVerdict struct {
	ID      string     `json:"id"`
	Verdict string     `json:"verdict"`
	Reason  string     `json:"reason"`
	Basis   []Evidence `json:"basis"`
}

type Warning struct {
	Text  string     `json:"text"`
	Basis []Evidence `json:"basis"`
}

type FactCheck struct {
	Subject  contract.Ref  `json:"subject"`
	Items    []FactVerdict `json:"items"`
	Warnings []Warning     `json:"warnings"`
	Gaps     []Gap         `json:"gaps"`
}

// FactStatus binds the final facts and check; each item the check did not
// judge supported is absent and has a gap. Verdicts stay in the check.
type FactStatus struct {
	Facts contract.Ref `json:"facts"`
	Check contract.Ref `json:"check"`
	Gaps  []Gap        `json:"gaps"`
}

// citable reads the files of each exact input for citation checks.
func citable(ctx context.Context, r *engine.Run, refs ...contract.Ref) (Inputs, error) {
	in := Inputs{Citable: map[contract.Ref][]contract.FileEntry{}}
	for _, ref := range refs {
		raw, err := engine.ReadContract(ctx, r, ref)
		if err != nil {
			return in, err
		}
		p, err := contract.DecodePublication[struct{}](raw)
		if err != nil {
			return in, err
		}
		in.Citable[ref] = p.Files
	}
	return in, nil
}

func sameRef(field string, got, want contract.Ref) error {
	if got != want {
		if got.AttemptID == want.AttemptID {
			return fmt.Errorf("%s: got attempt %s with %s; want the ref copied byte-exact from the request", field, got.AttemptID, differingFields(got, want))
		}
		return fmt.Errorf("%s: got %s; want %s copied byte-exact from the request", field, describeRef(got), describeRef(want))
	}
	return nil
}

// checkFacts is the structural acceptance of a facts contract: exact input
// Refs, unique ids, resolvable citations and recomputed time anchors. It
// does not judge whether a source supports a fact.
func checkFacts(ctx context.Context, r *engine.Run, ref contract.Ref, intake, prompt contract.Ref) (Facts, error) {
	p, err := readAccepted[Facts](ctx, r, ref, FactsSchema)
	if err != nil {
		return Facts{}, err
	}
	v := p.Data
	if err := sameRef("intake", v.Intake, intake); err != nil {
		return v, err
	}
	if err := sameRef("prompt", v.Prompt, prompt); err != nil {
		return v, err
	}
	in, err := citable(ctx, r, intake, prompt)
	if err != nil {
		return v, err
	}
	cite := citations{ctx, in, ref, p.Files}.check
	visionRef := func(r *contract.Ref) bool { return r == nil || *r == intake }
	if err := checkDeclared(idSet{}, cite, "facts", v.Facts, v.TimeAnchors, v.VisionRequests, visionRef); err != nil {
		return v, err
	}
	return v, checkGaps("gaps", v.Gaps)
}

// checkFactCheck requires exactly one verdict per fact and anchor of the
// subject, each with resolvable basis citations. The verdicts are the
// validator's judgment; Go does not second-guess them.
func checkFactCheck(ctx context.Context, r *engine.Run, ref, subject contract.Ref, inputs []contract.Ref, ids []string) (FactCheck, error) {
	p, err := readAccepted[FactCheck](ctx, r, ref, FactCheckSchema)
	if err != nil {
		return FactCheck{}, err
	}
	v := p.Data
	if err := sameRef("subject", v.Subject, subject); err != nil {
		return v, err
	}
	in, err := citable(ctx, r, inputs...)
	if err != nil {
		return v, err
	}
	cite := citations{ctx, in, ref, p.Files}.check
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	seen := map[string]bool{}
	for i, item := range v.Items {
		field := fmt.Sprintf("items[%d]", i)
		if !want[item.ID] {
			return v, fmt.Errorf("%s.id: got %q; want an id of an item the subject declares for judgment", field, item.ID)
		}
		if seen[item.ID] {
			return v, fmt.Errorf("%s.id: %q already has a verdict; want exactly one per judged item", field, item.ID)
		}
		seen[item.ID] = true
		for j, e := range item.Basis {
			if err := cite(fmt.Sprintf("%s.basis[%d]", field, j), e); err != nil {
				return v, err
			}
		}
	}
	for i, w := range v.Warnings {
		for j, e := range w.Basis {
			if err := cite(fmt.Sprintf("warnings[%d].basis[%d]", i, j), e); err != nil {
				return v, err
			}
		}
	}
	var missing []string
	for _, id := range ids {
		if !seen[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return v, fmt.Errorf("items: no verdict for %s; want exactly one per judged item", strings.Join(missing, ", "))
	}
	return v, checkGaps("gaps", v.Gaps)
}

// judgedIDs are the facts and time anchors the check must judge, in order.
func judgedIDs(f Facts) []string {
	var ids []string
	for _, x := range f.Facts {
		ids = append(ids, x.ID)
	}
	for _, a := range f.TimeAnchors {
		ids = append(ids, a.ID)
	}
	return ids
}

// idSet tracks item ids across one contract, for a diagnostic naming the
// first use.
type idSet map[string]string

func (s idSet) add(field, id string) error {
	if prior, ok := s[id]; ok {
		return fmt.Errorf("%s.id: %q is already used at %s; item ids must be unique within this contract", field, id, prior)
	}
	s[id] = field
	return nil
}

// checkDeclared checks the facts, time anchors and vision requests a facts
// or round contract declares: unique ids, resolvable citations, recomputed
// anchors, and vision attachments from an allowed owner.
func checkDeclared(ids idSet, cite func(string, Evidence) error, factsField string, facts []Fact, anchors []TimeAnchor, vision []VisionRequest, visionRef func(*contract.Ref) bool) error {
	for i, f := range facts {
		field := fmt.Sprintf("%s[%d]", factsField, i)
		if err := ids.add(field, f.ID); err != nil {
			return err
		}
		for j, e := range f.Evidence {
			if err := cite(fmt.Sprintf("%s.evidence[%d]", field, j), e); err != nil {
				return err
			}
		}
	}
	for i, a := range anchors {
		field := fmt.Sprintf("time_anchors[%d]", i)
		if err := ids.add(field, a.ID); err != nil {
			return err
		}
		if !nonblank(a.Event) || !nonblank(a.SourceTZ) {
			return fmt.Errorf("%s: event and source_tz must not be blank", field)
		}
		if err := checkAnchor(field, a, cite); err != nil {
			return err
		}
	}
	for i, q := range vision {
		field := fmt.Sprintf("vision_requests[%d]", i)
		if err := ids.add(field, q.ID); err != nil {
			return err
		}
		if !visionRef(q.Attachment.Ref) {
			return fmt.Errorf("%s.attachment.ref: got %s; want the intake or another input of this request holding the image, or null for an image this contract fetched itself", field, describeRef(*q.Attachment.Ref))
		}
		if err := cite(field+".attachment", q.Attachment); err != nil {
			return err
		}
	}
	return nil
}
