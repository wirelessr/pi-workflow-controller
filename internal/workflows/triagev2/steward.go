package triagev2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const StewardSchema = "triage.steward.v1"

const stewardRequirements = `You are the steward at trigger %s (the steward rules, Triggers). Read the committed results in the inputs and return a pass or one challenge; you do not investigate, query production systems or decide what happens next. You may search and read the wiki: copy each page you rely on into this contract's own evidence files.`

const stewardUnconfirmed = `The home stack is not confirmed: flag what that means for the work ahead.`

type StewardTarget struct {
	Ref contract.Ref `json:"ref"`
	ID  *string      `json:"id"`
}

type StewardItem struct {
	PatternID string        `json:"pattern_id"`
	Target    StewardTarget `json:"target"`
	Question  string        `json:"question"`
}

type StewardWiki struct {
	Query    string     `json:"query"`
	Evidence []Evidence `json:"evidence"`
}

type Steward struct {
	Trigger string        `json:"trigger"`
	Verdict string        `json:"verdict"`
	Notes   []string      `json:"notes"`
	Items   []StewardItem `json:"items"`
	Wiki    []StewardWiki `json:"wiki"`
	Gaps    []Gap         `json:"gaps"`
}

// checkSteward requires the trigger named in the request, items that
// target an input (and an item id inside it, when named) and wiki copies
// in its own files. Whether a challenge is right is not Go's call.
func checkSteward(ctx context.Context, r *engine.Run, ref contract.Ref, trigger string, inputs []contract.Ref) error {
	p, err := readAccepted[Steward](ctx, r, ref, StewardSchema)
	if err != nil {
		return err
	}
	v := p.Data
	if v.Trigger != trigger {
		return fmt.Errorf("trigger: got %q; want %q from the request", v.Trigger, trigger)
	}
	in, err := citable(ctx, r, inputs...)
	if err != nil {
		return err
	}
	for i, item := range v.Items {
		field := fmt.Sprintf("items[%d].target", i)
		if _, ok := in.Citable[item.Target.Ref]; !ok {
			return fmt.Errorf("%s.ref: got %s; want an input of this request", field, describeRef(item.Target.Ref))
		}
		if item.Target.ID == nil {
			continue
		}
		raw, err := engine.ReadContract(ctx, r, item.Target.Ref)
		if err != nil {
			return err
		}
		p, err := contract.DecodePublication[json.RawMessage](raw)
		if err != nil {
			return err
		}
		if !hasItemID(p.Data, *item.Target.ID) {
			return fmt.Errorf("%s.id: %s has no item with id %q", field, describeRef(item.Target.Ref), *item.Target.ID)
		}
	}
	cite := citations{ctx, Inputs{}, ref, p.Files}.check
	for i, w := range v.Wiki {
		for j, e := range w.Evidence {
			if e.Ref != nil {
				return fmt.Errorf("wiki[%d].evidence[%d].ref: want null, a copy in this contract's own evidence files", i, j)
			}
			if err := cite(fmt.Sprintf("wiki[%d].evidence[%d]", i, j), e); err != nil {
				return err
			}
		}
	}
	return checkGaps("gaps", v.Gaps)
}

// hasItemID reports whether any object inside data has the given id.
func hasItemID(data json.RawMessage, id string) bool {
	var v any
	if json.Unmarshal(data, &v) != nil {
		return false
	}
	var walk func(any) bool
	walk = func(v any) bool {
		switch x := v.(type) {
		case map[string]any:
			if x["id"] == id {
				return true
			}
			for _, child := range x {
				if walk(child) {
					return true
				}
			}
		case []any:
			for _, child := range x {
				if walk(child) {
					return true
				}
			}
		}
		return false
	}
	return walk(v)
}

// steward is one steward call at a trigger.
type steward struct {
	Model       runtime.ModelSpec
	Ticket      string
	Trigger     string
	Key         string
	Round       int
	Citable     []LabeledRef
	Unconfirmed bool
	Retries     int
	Timeout     time.Duration
}

// runSteward runs the steward in a fresh session; a timed-out call reruns.
func runSteward(ctx context.Context, r *engine.Run, skills Skills, s steward) (contract.Ref, Steward, []RecoveryFailure, error) {
	requirements := []string{fmt.Sprintf(stewardRequirements, s.Trigger)}
	if s.Unconfirmed {
		requirements = append(requirements, stewardUnconfirmed)
	}
	t := newTask(r, "steward", s.Ticket, []string{skills.Entry("steward"), skills.Entry("core")}, append(requirements, citationRequirements)...)
	t.Round, t.Citable, t.Trigger = s.Round, s.Citable, s.Trigger
	ref, failures, err := RetryInputs(ctx, r, r.Root(), s.Key, "steward", s.Retries, func(ctx context.Context, scope *engine.Scope, retry *engine.Feedback) (contract.Ref, error) {
		return RunTaskStep(ctx, r, TaskStep{Scope: scope, Model: s.Model, Stage: "steward", Key: "steward", Task: t, Schema: StewardSchema, Inputs: t.inputs(), Recovery: true, Timeout: s.Timeout, Feedback: retry,
			Validate: func(ctx context.Context, ref contract.Ref) error {
				return checkSteward(ctx, r, ref, s.Trigger, t.inputs())
			}})
	}, nil)
	if err != nil {
		return ref, Steward{}, failures, err
	}
	p, err := readAccepted[Steward](ctx, r, ref, StewardSchema)
	return ref, p.Data, failures, err
}

// challengeFeedback turns a challenge into the next round's feedback.
func challengeFeedback(ref contract.Ref, v Steward) *engine.Feedback {
	var items []string
	for _, item := range v.Items {
		target := describeRef(item.Target.Ref)
		if item.Target.ID != nil {
			target += " item " + *item.Target.ID
		}
		items = append(items, fmt.Sprintf("[%s] %s (on %s)", item.PatternID, item.Question, target))
	}
	return &engine.Feedback{Message: fmt.Sprintf("The steward (%s) challenged: %s. Act on each item in this round.", v.Trigger, strings.Join(items, "; ")), Refs: []contract.Ref{ref}}
}
