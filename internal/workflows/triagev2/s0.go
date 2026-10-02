package triagev2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const intakeRequirements = `Mechanical retrieval only. With the existing Jira tools and the intake skill, save as this attempt's evidence files: the complete issue JSON with all fields unabridged; field metadata as a bare top-level JSON array of field objects with id and name; every raw comment page from start 0 to the total (with zero comments, exactly one page with start 0, total 0 and an empty comments array); one snapshot per formal issue link in fields.issuelinks (a parent epic or a ticket only mentioned in text is not a link); and every attachment, with mechanical extraction such as an unpacked bundle as its analysis. Stay within tool and Store limits and never follow an attachment redirect with credentials to an unrelated host. Record url as the canonical browse URL and fetched_at in UTC. A genuine absence confirmed from raw evidence (zero comments, no links, no attachments) is completeness, not a gap: gaps list only retrieval that remains undone or failed, and complete is true only when nothing is missing. Do not interpret images, query other systems or decide anything about the incident.`

const factsRequirements = `Extract candidate facts from text only: the committed intake evidence files and the caller prompt, whose hints are candidate sources, not authorization. Read the full issue, field metadata, every comment page, linked issues and attachment extractions. Do not run any runtime or database query: identity is confirmed later. Record each candidate tenant id, orgkey, UI hostname, home PoP, customer name or user with the evidence it was read from. Keep every candidate when sources disagree; never derive a home PoP from a hostname or tenant name, or a tenant id from an unlabelled field. Record observed incident timestamps as time_anchors, computing utc and offset_seconds yourself; Jira activity timestamps, geography and guessed zones are not incident anchors, and never guess a zone. List image attachments whose content you need in vision_requests with the question to answer; do not read images. Missing identity or time is a gap, not a failure.`

const factCheckRequirements = `Judge the subject facts contract item by item. For each fact and time anchor, read the evidence it cites and decide: supported (the evidence states it), unsupported (it does not) or inferred (derived from a name, hostname, geography or guess rather than stated). For an anchor, also check that its original, zone and event match the evidence. Cite what you relied on in basis. Judge only whether the cited sources support the item, not whether it is true in production, and run no runtime queries. Give exactly one verdict per fact and anchor id.`

// S0Models binds each S0 role to its model; the workflow definition names
// them, nothing here defaults.
type S0Models struct {
	Intake, Facts, Validator runtime.ModelSpec
}

// S0 holds the exact committed outputs of the intake and fact stage.
type S0 struct {
	Ticket                               string
	Prompt, Intake, Facts, Check, Status contract.Ref
}

// runS0 records the caller prompt, retrieves the ticket, extracts candidate
// facts and has an independent validator judge them. Facts it does not
// accept are retried with its reasons up to factRetries times, then
// recorded as absent with a gap; that is a business outcome, not a failure.
func runS0(ctx context.Context, r *engine.Run, skills Skills, models S0Models, factRetries int) (S0, error) {
	_, input := r.WorkflowInput()
	caller, err := ParseCallerPrompt(input.Prompt)
	if err != nil {
		return S0{}, err
	}
	root := r.Root()
	out := S0{Ticket: caller.Ticket}
	if err := os.MkdirAll(filepath.Join(r.Dir(), "triage-work"), 0700); err != nil {
		return out, err
	}
	out.Prompt, err = root.Attach(ctx, engine.AttachSpec{Key: "caller-prompt", Output: contract.Spec{SchemaID: PromptSchema}, Data: caller,
		Files: []contract.ControllerFile{{ID: "prompt", Path: "evidence/prompt.txt", Data: []byte(input.Prompt)}}})
	if err != nil {
		return out, err
	}
	intakeTask := newTask(r, "intake", caller.Ticket, []string{skills.Entry("intake")}, intakeRequirements)
	intakeTask.Citable = []LabeledRef{{"caller prompt", out.Prompt}}
	out.Intake, err = RunTaskStep(ctx, r, TaskStep{Scope: root, Model: models.Intake, Stage: "intake", Key: "intake", Task: intakeTask, Schema: IntakeSchema, Inputs: intakeTask.inputs(),
		Validate: func(ctx context.Context, ref contract.Ref) error {
			_, err := checkIntake(ctx, r, ref, caller.Ticket)
			return err
		}})
	if err != nil {
		return out, err
	}
	if err := root.Decision(ctx, "intake-recorded", "Ticket retrieval and completeness accepted; not a finding", []contract.Ref{out.Intake}); err != nil {
		return out, err
	}
	var facts Facts
	var check FactCheck
	_, err = root.Retry(ctx, "facts", factRetries, func(ctx context.Context, s *engine.Scope, state engine.RetryState) (engine.RetryAction, error) {
		ft := newTask(r, "facts", caller.Ticket, []string{skills.Entry("identity"), skills.Entry("core")}, factsRequirements, citationRequirements)
		ft.Citable = []LabeledRef{{"intake", out.Intake}, {"caller prompt", out.Prompt}}
		ref, err := RunTaskStep(ctx, r, TaskStep{Scope: s, Model: models.Facts, Stage: "facts", Key: "facts", Task: ft, Schema: FactsSchema, Inputs: ft.inputs(), Feedback: state.Feedback,
			Validate: func(ctx context.Context, ref contract.Ref) error {
				var err error
				facts, err = checkFacts(ctx, r, ref, out.Intake, out.Prompt)
				return err
			}})
		if err != nil {
			return engine.RetryAction{}, err
		}
		out.Facts = ref
		vt := newTask(r, "fact-check", caller.Ticket, []string{skills.Entry("validator")}, factCheckRequirements, citationRequirements)
		vt.Citable = []LabeledRef{{"intake", out.Intake}, {"caller prompt", out.Prompt}, {"facts under review", out.Facts}}
		vt.Subject = &out.Facts
		out.Check, err = RunTaskStep(ctx, r, TaskStep{Scope: s, Model: models.Validator, Stage: "fact-check", Key: "fact-check", Task: vt, Schema: FactCheckSchema, Inputs: vt.inputs(),
			Validate: func(ctx context.Context, ref contract.Ref) error {
				var err error
				check, err = checkFactCheck(ctx, r, ref, out.Facts, out.Intake, out.Prompt, facts)
				return err
			}})
		if err != nil {
			return engine.RetryAction{}, err
		}
		var rejected []string
		for _, item := range check.Items {
			if item.Verdict != "supported" {
				rejected = append(rejected, fmt.Sprintf("%s (%s): %s", item.ID, item.Verdict, item.Reason))
			}
		}
		result := engine.RetryAction{Result: engine.Result{Outputs: map[string]contract.Ref{"facts": out.Facts, "check": out.Check}}}
		if len(rejected) == 0 || state.RetryCount >= state.MaxRetries {
			return result, nil
		}
		return engine.RetryAction{Again: true, Feedback: &engine.Feedback{
			Message: "The independent check did not accept these items: " + strings.Join(rejected, "; ") + ". Extract the facts again: correct or drop each listed item, citing evidence that states it, and keep accepted items unchanged.",
			Refs:    []contract.Ref{out.Facts, out.Check}}}, nil
	})
	if err != nil {
		return out, err
	}
	status := FactStatus{Facts: out.Facts, Check: out.Check, Accepted: []string{}, Degraded: []DegradedFact{}, Gaps: []Gap{}}
	for _, item := range check.Items {
		if item.Verdict == "supported" {
			status.Accepted = append(status.Accepted, item.ID)
			continue
		}
		status.Degraded = append(status.Degraded, DegradedFact{ID: item.ID, Verdict: item.Verdict})
		status.Gaps = append(status.Gaps, Gap{ID: fmt.Sprintf("fact-not-accepted-%d", len(status.Gaps)+1),
			Text: fmt.Sprintf("Fact %s was judged %s by the independent check and is treated as absent: %s", item.ID, item.Verdict, item.Reason)})
	}
	if out.Status, err = root.Attach(ctx, engine.AttachSpec{Key: "fact-status", Output: contract.Spec{SchemaID: FactStatusSchema}, Data: status}); err != nil {
		return out, err
	}
	return out, root.Decision(ctx, "facts-recorded", "Candidate facts and their independent check recorded; facts not accepted are absent with a gap", []contract.Ref{out.Facts, out.Check, out.Status})
}
