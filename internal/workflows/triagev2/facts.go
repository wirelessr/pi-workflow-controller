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

type FactCheck struct {
	Subject contract.Ref  `json:"subject"`
	Items   []FactVerdict `json:"items"`
	Gaps    []Gap         `json:"gaps"`
}

type DegradedFact struct {
	ID      string `json:"id"`
	Verdict string `json:"verdict"`
}

type FactStatus struct {
	Facts    contract.Ref   `json:"facts"`
	Check    contract.Ref   `json:"check"`
	Accepted []string       `json:"accepted"`
	Degraded []DegradedFact `json:"degraded"`
	Gaps     []Gap          `json:"gaps"`
}

// classify reads the files of each exact input for citation checks.
func classify(ctx context.Context, r *engine.Run, citable, background []LabeledRef) (Inputs, error) {
	in := Inputs{Citable: map[contract.Ref][]contract.FileEntry{}, Background: map[contract.Ref][]contract.FileEntry{}}
	for _, group := range []struct {
		refs []LabeledRef
		into map[contract.Ref][]contract.FileEntry
	}{{citable, in.Citable}, {background, in.Background}} {
		for _, l := range group.refs {
			raw, err := engine.ReadContract(ctx, r, l.Ref)
			if err != nil {
				return in, err
			}
			p, err := contract.DecodePublication[struct{}](raw)
			if err != nil {
				return in, err
			}
			group.into[l.Ref] = p.Files
		}
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
	in, err := classify(ctx, r, []LabeledRef{{"intake", intake}, {"caller prompt", prompt}}, nil)
	if err != nil {
		return v, err
	}
	cite := func(field string, e Evidence) error { return in.CheckEvidence(field, e, p.Files) }
	ids := map[string]string{}
	unique := func(field, id string) error {
		if prior, ok := ids[id]; ok {
			return fmt.Errorf("%s.id: %q is already used at %s; ids must be unique across facts, time_anchors and vision_requests", field, id, prior)
		}
		ids[id] = field
		return nil
	}
	for i, f := range v.Facts {
		field := fmt.Sprintf("facts[%d]", i)
		if err := unique(field, f.ID); err != nil {
			return v, err
		}
		for j, e := range f.Evidence {
			if err := cite(fmt.Sprintf("%s.evidence[%d]", field, j), e); err != nil {
				return v, err
			}
		}
	}
	for i, a := range v.TimeAnchors {
		field := fmt.Sprintf("time_anchors[%d]", i)
		if err := unique(field, a.ID); err != nil {
			return v, err
		}
		if err := checkAnchor(field, a, cite); err != nil {
			return v, err
		}
	}
	for i, q := range v.VisionRequests {
		field := fmt.Sprintf("vision_requests[%d]", i)
		if err := unique(field, q.ID); err != nil {
			return v, err
		}
		if q.Attachment.Ref == nil || *q.Attachment.Ref != intake {
			return v, fmt.Errorf("%s.attachment.ref: want the intake input, whose files hold the attachments", field)
		}
		if err := cite(field+".attachment", q.Attachment); err != nil {
			return v, err
		}
	}
	return v, CheckGapIDs("gaps", v.Gaps)
}

// checkFactCheck requires exactly one verdict per fact and anchor of the
// subject, each with resolvable basis citations. The verdicts are the
// validator's judgment; Go does not second-guess them.
func checkFactCheck(ctx context.Context, r *engine.Run, ref, subject, intake, prompt contract.Ref, facts Facts) (FactCheck, error) {
	p, err := readAccepted[FactCheck](ctx, r, ref, FactCheckSchema)
	if err != nil {
		return FactCheck{}, err
	}
	v := p.Data
	if err := sameRef("subject", v.Subject, subject); err != nil {
		return v, err
	}
	in, err := classify(ctx, r, []LabeledRef{{"intake", intake}, {"caller prompt", prompt}, {"facts under review", subject}}, nil)
	if err != nil {
		return v, err
	}
	want := map[string]bool{}
	for _, f := range facts.Facts {
		want[f.ID] = true
	}
	for _, a := range facts.TimeAnchors {
		want[a.ID] = true
	}
	seen := map[string]bool{}
	for i, item := range v.Items {
		field := fmt.Sprintf("items[%d]", i)
		if !want[item.ID] {
			return v, fmt.Errorf("%s.id: got %q; want an id of a fact or time anchor of the subject", field, item.ID)
		}
		if seen[item.ID] {
			return v, fmt.Errorf("%s.id: %q already has a verdict; want exactly one per fact and time anchor", field, item.ID)
		}
		seen[item.ID] = true
		for j, e := range item.Basis {
			if err := in.CheckEvidence(fmt.Sprintf("%s.basis[%d]", field, j), e, p.Files); err != nil {
				return v, err
			}
		}
	}
	var missing []string
	for _, f := range facts.Facts {
		if !seen[f.ID] {
			missing = append(missing, f.ID)
		}
	}
	for _, a := range facts.TimeAnchors {
		if !seen[a.ID] {
			missing = append(missing, a.ID)
		}
	}
	if len(missing) > 0 {
		return v, fmt.Errorf("items: no verdict for %s; want exactly one per fact and time anchor", strings.Join(missing, ", "))
	}
	return v, CheckGapIDs("gaps", v.Gaps)
}
