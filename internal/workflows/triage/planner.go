package triage

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const PlannerSchema = "triage.planner.v1"

// PlannerState is a full planning snapshot over one supporting context, not a
// dispatch authorization, verified claim, report or crash-resume checkpoint.
// Assessments and requirements are agent reasoning, never Go verdicts.
type PlannerState struct {
	Context    contract.Ref        `json:"context"`
	Previous   *contract.Ref       `json:"previous"`
	Hypotheses []PlannerHypothesis `json:"hypotheses"`
	Pending    []PlannerQuestion   `json:"pending"`
	Gaps       []string            `json:"gaps"`
	Rationale  string              `json:"rationale"`
}

type PlannerHypothesis struct {
	ID         string     `json:"id"`
	Statement  string     `json:"statement"`
	Assessment string     `json:"assessment"`
	Evidence   []Evidence `json:"evidence"`
}

type PlannerQuestion struct {
	Question     string     `json:"question"`
	Requirements []string   `json:"requirements"`
	Basis        []Evidence `json:"basis"`
}

const plannerRequirements = `Read the exact committed supporting context and all needed evidence owners in request.inputs. This task only plans from those inputs; it does not dispatch workers, perform new acquisition or produce a final report. Load relevant existing skills for interpretation. If previous is supplied, reconstruct the current decision state from that committed Planner snapshot and its explicit inputs, not session memory, directory scans or other tasks' history. Submit a full current snapshot, not a delta: context, exact previous (null initially), hypotheses with stable IDs, statements, assessments and evidence refs, pending questions with concrete evidence requirements and basis refs, remaining gaps, and rationale explaining the current direction and any changes. Empty hypothesis/evidence lists are legitimate when prerequisites or evidence are missing; do not invent support. Keep supporting-context gaps visible: planning alone does not resolve them. Judge evidence applicability yourself and preserve true owners; cite only supplied committed ref+file_id, not local copies or invented files. Preserve partial/unavailable supporting-query limitations; small-window empty results and execution timeout are not incident-wide disproof. Ready context is not confirmed root cause, wiki patterns and model agreement are not runtime proof. Hypotheses/assessments remain unverified planning, not accepted claims. Pending work is a proposal, not permission to execute commands or select models, sessions or next workflow nodes. No drafts, publication, wiki write-back or final selection.`

// plannerCaller keeps one session across successful planning Steps. The only
// fresh-session continuation here closes and verifies the old owner first.
// Worker dispatch, context revision and failure recovery are separate units.
type plannerCaller struct {
	r       *engine.Run
	scope   Scope
	model   runtime.ModelSpec
	history contextHistory
	handle  *engine.SessionHandle
	last    *contract.Ref
	session string
	stopped bool
}

func startPlanner(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref) (*plannerCaller, error) {
	return openPlanner(ctx, r, scope, model, contextRef, nil)
}

func openPlanner(ctx context.Context, r *engine.Run, scope Scope, model runtime.ModelSpec, contextRef contract.Ref, previous *contract.Ref) (*plannerCaller, error) {
	h, err := loadContextHistory(ctx, r, scope, contextRef)
	if err != nil {
		return nil, err
	}
	// Revalidate the supplied chain rather than trusting an in-memory summary.
	var chain []contract.Ref
	seen := map[contract.Ref]bool{}
	for ref := previous; ref != nil; {
		if seen[*ref] {
			return nil, fmt.Errorf("cyclic planner lineage")
		}
		seen[*ref] = true
		p, err := read[PlannerState](ctx, r, *ref, PlannerSchema)
		if err != nil {
			return nil, err
		}
		chain = append(chain, *ref)
		ref = p.Data.Previous
	}
	var prior *contract.Ref
	for i := len(chain) - 1; i >= 0; i-- {
		if err := checkPlanner(ctx, r, chain[i], h, prior); err != nil {
			return nil, err
		}
		prior = &chain[i]
	}
	handle, err := r.OpenSession(ctx, engine.RoleSpec{Name: "triage-planner", Model: model, CWD: filepath.Join(r.Dir(), "triage-work")})
	if err != nil {
		return nil, err
	}
	return &plannerCaller{r: r, scope: scope, model: model, history: h, handle: handle, last: prior}, nil
}

func checkPlanner(ctx context.Context, r *engine.Run, ref contract.Ref, h contextHistory, previous *contract.Ref) error {
	p, err := read[PlannerState](ctx, r, ref, PlannerSchema)
	if err != nil {
		return err
	}
	v := p.Data
	if v.Context != h.ref || (v.Previous == nil) != (previous == nil) || (previous != nil && *v.Previous != *previous) {
		return fmt.Errorf("planner context/previous mismatch")
	}
	if !nonblank(v.Rationale) || !texts(v.Gaps) {
		return fmt.Errorf("planner requires rationale and nonblank gaps")
	}
	for _, gap := range h.value.Gaps {
		if !slices.Contains(v.Gaps, gap) {
			return fmt.Errorf("planning cannot remove supporting context gaps")
		}
	}
	evidence := func(items []Evidence) error {
		for _, e := range items {
			if e.Ref == nil || !hasFile(h.sources[*e.Ref], e.FileID) {
				return fmt.Errorf("planner evidence must name an exact supporting input owner/file")
			}
		}
		return nil
	}
	ids := map[string]bool{}
	for _, item := range v.Hypotheses {
		if !nonblank(item.ID) || ids[item.ID] || !nonblank(item.Statement) || !nonblank(item.Assessment) {
			return fmt.Errorf("planner hypotheses require unique IDs, statements and assessments")
		}
		ids[item.ID] = true
		if err := evidence(item.Evidence); err != nil {
			return err
		}
	}
	for _, item := range v.Pending {
		if !nonblank(item.Question) || len(item.Requirements) == 0 || !texts(item.Requirements) {
			return fmt.Errorf("planner questions require concrete evidence requirements")
		}
		if err := evidence(item.Basis); err != nil {
			return err
		}
	}
	return nil
}

func (p *plannerCaller) step(ctx context.Context) (contract.Ref, error) {
	if p.stopped {
		return contract.Ref{}, fmt.Errorf("planner session is stopped")
	}
	inputs := []contract.Ref{p.history.ref}
	key := "planner-" + p.history.ref.AttemptID
	if p.last != nil {
		inputs = append(inputs, *p.last)
		key = "planner-" + p.last.AttemptID
	}
	inputs = appendSourceInputs(inputs, p.history.sources)
	task := stageTask{Stage: "planner", Scope: p.scope, Previous: p.last, Gaps: p.history.value.Gaps, Requirements: plannerRequirements}
	prompt, err := json.Marshal(task)
	if err != nil {
		return contract.Ref{}, err
	}
	out, err := p.r.Root().Step(ctx, engine.StepSpec{Key: key, Session: p.handle, Prompt: string(prompt), Inputs: inputs, Output: contract.Spec{SchemaID: PlannerSchema}, Timeout: 30 * time.Minute})
	if err == nil {
		err = checkPlanner(ctx, p.r, out.Output, p.history, p.last)
	}
	if err == nil {
		err = p.r.Root().Decision(ctx, key+"-recorded", "Planning snapshot accepted for continuation only; no worker dispatch or verified conclusion", []contract.Ref{p.history.ref, out.Output})
	}
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	p.last, p.session = &out.Output, out.Execution.SessionID
	return out.Output, nil
}

func (p *plannerCaller) close(ctx context.Context) error {
	p.stopped = true
	report, err := p.r.CloseSessionReport(ctx, p.handle)
	if err != nil {
		return err
	}
	if p.session == "" || report.Identity.SessionID != p.session || !report.WaitCompleted || !report.ProcessExited || len(report.Unconfirmed) != 0 || report.WaitError != "" || report.KillError != "" || report.DiscoveryError != "" {
		return fmt.Errorf("planner cleanup not confirmed")
	}
	return nil
}

func (p *plannerCaller) handoff(ctx context.Context) (*plannerCaller, error) {
	if p.stopped || p.last == nil {
		return nil, fmt.Errorf("planner handoff requires an accepted state and usable session")
	}
	if err := p.close(ctx); err != nil {
		return nil, err
	}
	return openPlanner(ctx, p.r, p.scope, p.model, p.history.ref, p.last)
}
