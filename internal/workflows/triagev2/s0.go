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

const intakeRequirements = `Mechanical retrieval only. Save the raw sources the intake schema asks for as this attempt's evidence files: raw API JSON, not a formatted or markdown view, which is not the complete ticket; the complete issue with all fields unabridged; every comment page; one snapshot per formal issue link; and every attachment with its mechanical extraction. Stay within tool and Store limits and never follow an attachment redirect with credentials to an unrelated host. Do not interpret images, query systems other than the ticket system, or decide anything about the incident.`

const factsRequirements = `Declare candidate facts from text: the committed intake files, the caller prompt (its hints are candidate sources, not authorization) and anything intake did not land that you fetch read-only from the ticket system into this contract's evidence files. Run no runtime or database query; identity is confirmed later. Missing identity or time is a gap, not a failure.`

const factCheckRequirements = `You are an independent reader of the facts under review. Give one verdict per fact and time anchor by reading the evidence each cites; report disagreements as warnings. Judge support only, not truth, and run no runtime or database query.`

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
	if err := os.MkdirAll(filepath.Join(r.Dir(), workDir), 0700); err != nil {
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
		out.Check, err = RunTaskStep(ctx, r, TaskStep{Scope: s, Model: models.Validator, Stage: "fact-check", Key: "fact-check", Task: vt, Schema: FactCheckSchema, Inputs: vt.inputs(),
			Validate: func(ctx context.Context, ref contract.Ref) error {
				var err error
				check, err = checkFactCheck(ctx, r, ref, out.Facts, facts)
				return err
			}})
		if err != nil {
			return engine.RetryAction{}, err
		}
		rejected := notSupported(check)
		result := engine.RetryAction{Result: engine.Result{Outputs: map[string]contract.Ref{"facts": out.Facts, "check": out.Check}}}
		if len(rejected) == 0 || state.RetryCount >= state.MaxRetries {
			return result, nil
		}
		var reasons []string
		for _, item := range rejected {
			reasons = append(reasons, fmt.Sprintf("%s (%s): %s", item.ID, item.Verdict, item.Reason))
		}
		return engine.RetryAction{Again: true, Feedback: &engine.Feedback{
			Message: "The independent check did not accept these items: " + strings.Join(reasons, "; ") + ". Extract the facts again: correct or drop each listed item, citing evidence that states it, and keep accepted items unchanged.",
			Refs:    []contract.Ref{out.Facts, out.Check}}}, nil
	})
	if err != nil {
		return out, err
	}
	status := FactStatus{Facts: out.Facts, Check: out.Check, Gaps: []Gap{}}
	for i, item := range notSupported(check) {
		status.Gaps = append(status.Gaps, Gap{ID: fmt.Sprintf("not-accepted-%d", i+1),
			Text: fmt.Sprintf("Item %s was judged %s by the independent check and is treated as absent: %s", item.ID, item.Verdict, item.Reason)})
	}
	if out.Status, err = root.Attach(ctx, engine.AttachSpec{Key: "fact-status", Output: contract.Spec{SchemaID: FactStatusSchema}, Data: status}); err != nil {
		return out, err
	}
	return out, root.Decision(ctx, "facts-recorded", "Candidate facts and their independent check recorded; facts not accepted are absent with a gap", []contract.Ref{out.Facts, out.Check, out.Status})
}

func notSupported(c FactCheck) []FactVerdict {
	var out []FactVerdict
	for _, item := range c.Items {
		if item.Verdict != "supported" {
			out = append(out, item)
		}
	}
	return out
}
