package triage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/reportresource"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/workflows/triagev2"
)

const ReportSchema = "triage.report.v1"
const ReportFileID = "triage-report"

// ReportClaim selects an existing assessment, never a rewritten claim or verdict.
type ReportClaim struct {
	Claim           contract.Ref `json:"claim"`
	DeliveryID      string       `json:"delivery_id"`
	AssessmentOwner contract.Ref `json:"assessment_owner"`
}

type InvestigationReport struct {
	Request      string          `json:"request,omitempty"`
	State        contract.Ref    `json:"state"`
	Context      contract.Ref    `json:"context"`
	Claims       []ReportClaim   `json:"claims"`
	Completeness string          `json:"completeness"`
	Closure      string          `json:"closure"`
	Gaps         []string        `json:"gaps"`
	NextSteps    []string        `json:"next_steps"`
	ReportFile   string          `json:"report_file"`
	M6           *ReportMetadata `json:"m6,omitempty"`
}

type reportOwner struct {
	Ref      contract.Ref      `json:"ref"`
	HandleID string            `json:"handle_id"`
	Scope    string            `json:"scope"`
	Step     string            `json:"step"`
	Role     string            `json:"role"`
	Model    runtime.ModelSpec `json:"model"`
}

type reportTask struct {
	Workspace    string              `json:"workspace"`
	Request      string              `json:"request,omitempty"`
	Stage        string              `json:"stage"`
	State        contract.Ref        `json:"state"`
	Context      contract.Ref        `json:"context"`
	Assessments  []ReportClaim       `json:"assessments"`
	Owners       []reportOwner       `json:"owners"`
	Renderer     string              `json:"renderer"`
	Requirements string              `json:"requirements"`
	M6           *reportContinuation `json:"m6,omitempty"`
}

// reportInputs follows committed lineage, not directory order or session memory.
func (p *plannerCaller) reportInputs(a *acceptance) (reportTask, []contract.Ref, error) {
	task := reportTask{Workspace: filepath.Join(p.r.Dir(), "triage-work"), Request: p.request, Stage: "planner-report", State: *p.last, Context: p.history.ref, Assessments: []ReportClaim{}, Owners: []reportOwner{}}
	inputs := []contract.Ref{*p.last, p.history.ref}
	seen := map[contract.Ref]bool{}
	for ref := p.last; ref != nil; {
		if seen[*ref] {
			return task, nil, fmt.Errorf("cyclic report state lineage")
		}
		seen[*ref] = true
		state, err := readAccepted[PlannerState](a, *ref, PlannerSchema)
		if err != nil {
			return task, nil, err
		}
		h, err := a.loadContextHistory(p.scope, state.Data.Context)
		if err != nil {
			return task, nil, err
		}
		records, err := a.loadWorkerResults(p.scope, state.Data.WorkerResults)
		if err != nil {
			return task, nil, err
		}
		if err := a.checkPlannerWithWorkers(*ref, h, state.Data.Previous, records); err != nil {
			return task, nil, err
		}
		inputs = triagev2.AppendUniqueRefs(inputs, *ref, h.ref)
		inputs = appendSourceInputs(inputs, h.sources)
		inputs = triagev2.AppendUniqueRefs(inputs, state.Data.WorkerResults...)
		inputs = triagev2.AppendUniqueRefs(inputs, state.Data.WikiResults...)
		if p.reporting != nil {
			sources, err := a.recoverySources(p.scope, h.sources, state.Data.Recovery)
			if err != nil {
				return task, nil, err
			}
			inputs = appendSourceInputs(inputs, sources)
			if state.Data.Recovery != nil {
				for _, delivery := range state.Data.Recovery.Deliveries {
					inputs = triagev2.AppendUniqueRefs(inputs, delivery.Proposal, delivery.Context)
					if delivery.Support != nil {
						inputs = triagev2.AppendUniqueRefs(inputs, delivery.Support.refs()...)
					}
				}
			}
		}
		if review := state.Data.VerificationReview; review != nil {
			task.Assessments = append(task.Assessments, ReportClaim{review.Claim, review.DeliveryID, *ref})
		}
		if state.Data.Verification != nil {
			for _, claimRef := range state.Data.Verification.Claims {
				claim, err := a.loadClaim(p.scope, claimRef)
				if err != nil {
					return task, nil, err
				}
				inputs = triagev2.AppendUniqueRefs(inputs, claimInputs(claimRef, claim)...)
				inputs = triagev2.AppendUniqueRefs(inputs, claim.ParentState, claim.Context)
			}
			for _, delivery := range state.Data.Verification.Deliveries {
				inputs = triagev2.AppendUniqueRefs(inputs, delivery.Proposal)
				for _, role := range delivery.Roles {
					if role.Result != nil {
						inputs = triagev2.AppendUniqueRefs(inputs, *role.Result)
					}
				}
			}
		}
		ref = state.Data.Previous
	}
	var err error
	task.M6, err = p.reportContinuation(a)
	if err != nil {
		return task, nil, err
	}
	snapshot := p.r.Snapshot()
	for _, ref := range inputs {
		if _, err := readAccepted[json.RawMessage](a, ref, ref.SchemaID); err != nil {
			return task, nil, err
		}
		attempt := snapshot.Attempts[ref.AttemptID]
		owner := snapshot.Sessions[attempt.HandleID]
		task.Owners = append(task.Owners, reportOwner{ref, attempt.HandleID, attempt.Scope, attempt.Key, owner.Role.Name, owner.Role.Model})
	}
	return task, inputs, nil
}

// report is a normal Step on the existing Planner. It does not yield, reserve
// budget, retry, close the session, or change the state-only M5 continuation.
func (p *plannerCaller) report(ctx context.Context, renderer string) (contract.Ref, error) {
	return p.reportInScope(ctx, p.r.Root(), renderer)
}

func (p *plannerCaller) reportInScope(ctx context.Context, executionScope *engine.Scope, renderer string) (contract.Ref, error) {
	if p.stopped || p.last == nil {
		return contract.Ref{}, fmt.Errorf("report requires an accepted state and usable Planner")
	}
	if renderer != filepath.Join(p.r.Dir(), "triage-report", "render_report.py") {
		return contract.Ref{}, fmt.Errorf("report requires extracted run resources")
	}
	a := newAcceptance(ctx, p.r)
	task, inputs, err := p.reportInputs(a)
	if err != nil {
		return contract.Ref{}, err
	}
	task.Renderer = renderer
	task.Requirements = "You are the existing investigation Planner, producing a report, not a new claim. Read the supplied exact accepted state/context, historical assessment owners, claims, deliveries, allowed evidence and original producers. Select claims only by exact entries in assessments. Do not rewrite statement, premises, support, disputes or measurement conditions; a new or changed claim must return to independent verification. Declare completeness (complete/incomplete), closure reason, retained gaps and concrete next_steps. Retained gaps are copied verbatim from the accepted state: every gap string in the supplied planner state gaps[] must appear byte-exact in report gaps[] (no rewording, merging, splitting or dropping); you may add new gaps, never alter or omit an accepted one. No claim is legitimate only as incomplete. Missing evidence is not disproof; preserve execution failures and unavailable roles. Write triage.report.v1 candidate with state/context exactly supplied and report_file triage-report, then run python3 -B with renderer and the absolute request.json and candidate.json paths. The fixed renderer appends the artifact entry; do not create that entry beforehand. No acquisition, external publication, wiki write-back or final selection."
	if task.Request != "" {
		task.Requirements += "\nThe request is the immutable original caller instruction, not additional scope or evidence. Copy it exactly into data.request, preserving all whitespace and Unicode. Address that request without rewriting it."
	}
	if task.M6 != nil {
		task.Requirements += "\nM6 finalization: reconstruct every supplied historical owner and pending recovery/verification/controller feedback, including failures for claims you do not select. Supply m6.dispositions for every exact item in m6.failures once, without changing owner, delivery, claim, role, index or failure. Declare action unresolved (retain incomplete and no results), handled (cite subsequent supplied results of the same failed work: the actual Planner/claim retry key and claim parent; the original worker task or wiki task through an explicit resume proposal; or the successful support delivery retaining original proposal/context/work, accepted phases and resume authorization; verification retains the exact claim version and role. A receiving Planner snapshot or an unrelated result is not a worker/wiki/support completion. If this continuation cannot be shown, use unresolved or evidence-backed redirect), or redirect (explain why no longer needed using nonempty supplied evidence basis, without claiming success). Include reason, results and basis arrays for each. Necessity and applicability are your judgments; do not infer confirmation from support text, source schemas or votes. Echo m6.report_failures and m6.budget exactly. These report retry failures remain history even when this report succeeds. A nonnull budget requires resource-limited incomplete closure using the last accepted state, not a new ledger yield or a new claim. A handled disposition does not alter any historical diagnostic or assessment. No extra planning, acquisition or verification is authorized by this report task."
		p.reporting.pending.Inputs = slices.Clone(inputs)
	}
	task.Requirements += "\n" + triageWorkspaceRequirements
	prompt, err := json.Marshal(task)
	if err != nil {
		return contract.Ref{}, err
	}
	identity, err := p.r.SessionIdentity(ctx, p.handle)
	if err != nil {
		return contract.Ref{}, err
	}
	key := "report-" + p.last.AttemptID
	out, err := executionScope.Step(ctx, engine.StepSpec{Key: key, Session: p.handle, Prompt: string(prompt), Inputs: inputs, Output: contract.Spec{SchemaID: ReportSchema}, Timeout: 30 * time.Minute})
	if p.reporting != nil {
		if out.Output != (contract.Ref{}) {
			p.reporting.pending.Report = &out.Output
			p.reporting.pending.Boundary = "step-committed"
		} else if err != nil {
			p.stopped = true
			return contract.Ref{}, &triagev2.TaskFailure{Cause: err, Handle: p.handle, Identity: identity, Stage: "report", Attempt: out.AttemptID}
		}
	}
	if err == nil && out.Execution.SessionID != identity.SessionID {
		err = fmt.Errorf("report execution identity mismatch")
	}
	if err == nil {
		err = p.checkReport(ctx, out.Output, task, inputs)
	}
	if err == nil {
		if p.reporting != nil {
			p.reporting.pending.Boundary = "validated"
		}
		err = executionScope.Decision(ctx, key+"-recorded", "Report projection and exact bindings accepted; support and closure remain Planner judgments", triagev2.AppendUniqueRefs(inputs, out.Output))
	}
	if err != nil {
		p.stopped = true
		return contract.Ref{}, err
	}
	if p.reporting != nil {
		p.reporting.pending.Boundary = "decision-recorded"
	}
	p.session = out.Execution.SessionID
	return out.Output, nil
}

func checkReportBinding(v InvestigationReport, task reportTask) error {
	if v.State != task.State || v.Context != task.Context || v.ReportFile != ReportFileID {
		return fmt.Errorf("report state/context/file binding mismatch")
	}
	return nil
}

func (p *plannerCaller) checkReport(ctx context.Context, ref contract.Ref, task reportTask, inputs []contract.Ref) error {
	a := newAcceptance(ctx, p.r)
	accepted, err := readAccepted[InvestigationReport](a, ref, ReportSchema)
	if err != nil {
		return err
	}
	identity, err := p.r.SessionIdentity(ctx, p.handle)
	if err != nil {
		return err
	}
	snapshot := p.r.Snapshot()
	attempt := snapshot.Attempts[ref.AttemptID]
	owner := snapshot.Sessions[attempt.HandleID]
	if attempt.Key != "report-"+task.State.AttemptID || attempt.HandleID != identity.HandleID || owner.Identity.SessionID != identity.SessionID || owner.Role.Name != "triage-planner" || owner.Role.Model != p.model {
		return fmt.Errorf("report must retain its exact Planner producer")
	}
	v := accepted.Data
	if v.Request != p.request || task.Request != p.request {
		return fmt.Errorf("report request differs from immutable caller instruction")
	}
	if err := checkReportBinding(v, task); err != nil {
		return err
	}
	if len(accepted.Files) != 1 || accepted.Files[0].ID != ReportFileID || accepted.Files[0].Kind != "artifact" || accepted.Files[0].Path != "artifacts/triage-report.md" {
		return fmt.Errorf("report requires its exclusive artifact")
	}
	if !nonblank(v.Closure) || !texts(v.Gaps) || !texts(v.NextSteps) || (v.Completeness != "complete" && v.Completeness != "incomplete") || len(v.Claims) == 0 && v.Completeness != "incomplete" {
		return fmt.Errorf("report requires declared completeness, closure and limitations")
	}
	state, err := readAccepted[PlannerState](a, task.State, PlannerSchema)
	if err != nil {
		return err
	}
	if state.Data.Context != task.Context {
		return fmt.Errorf("report context differs from accepted state")
	}
	for _, gap := range state.Data.Gaps {
		if !slices.Contains(v.Gaps, gap) {
			return fmt.Errorf("report cannot drop accepted gaps")
		}
	}
	seen := map[contract.Ref]bool{}
	for _, selected := range v.Claims {
		if !slices.Contains(task.Assessments, selected) || seen[selected.Claim] {
			return fmt.Errorf("report requires an exact distinct claim/delivery/assessment owner")
		}
		seen[selected.Claim] = true
		owner, err := readAccepted[PlannerState](a, selected.AssessmentOwner, PlannerSchema)
		if err != nil {
			return err
		}
		review := owner.Data.VerificationReview
		if review == nil || review.Claim != selected.Claim || review.DeliveryID != selected.DeliveryID {
			return fmt.Errorf("report assessment owner/version mismatch")
		}
	}
	if err := p.checkReportMetadata(a, v, task, inputs); err != nil {
		return err
	}
	// Reconstruct after the Step: a cached pre-dispatch projection is not authority.
	current, currentInputs, err := p.reportInputs(a)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current.M6, task.M6) || !reflect.DeepEqual(current.Assessments, task.Assessments) || !reflect.DeepEqual(current.Owners, task.Owners) || !slices.Equal(currentInputs, inputs) {
		return fmt.Errorf("report input owners changed")
	}
	var meta struct {
		Meta json.RawMessage `json:"meta"`
	}
	if err := json.Unmarshal(a.publications[ref], &meta); err != nil {
		return err
	}
	documents := make([]reportDocument, 0, len(inputs))
	for _, input := range inputs {
		doc, err := readAccepted[json.RawMessage](a, input, input.SchemaID)
		if err != nil {
			return err
		}
		documents = append(documents, reportDocument{input, doc.Data})
	}
	want, err := renderReport(ctx, reportProjection{meta.Meta, v, documents})
	if err != nil {
		return err
	}
	got, err := rawFile(ctx, ref, accepted.Files, ReportFileID)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("report differs from deterministic accepted projection")
	}
	return nil
}

type reportDocument struct {
	Ref  contract.Ref    `json:"ref"`
	Data json.RawMessage `json:"data"`
}
type reportProjection struct {
	Meta      json.RawMessage     `json:"meta"`
	Data      InvestigationReport `json:"data"`
	Documents []reportDocument    `json:"documents"`
}

type reportBuffer struct {
	buffer bytes.Buffer
	limit  int
}

func (b *reportBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.buffer.Len() {
		return 0, fmt.Errorf("report renderer output limit exceeded")
	}
	return b.buffer.Write(p)
}

// Only embedded code executes here. Neither extracted files nor agent-supplied
// programs/digests determine the expected bytes. Run waits for our one process.
func renderReport(ctx context.Context, projection reportProjection) ([]byte, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	common, err := reportresource.Common.ReadFile("pwc_report_io.py")
	if err != nil {
		return nil, err
	}
	template, err := resources.ReadFile("report/render_report.py")
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(struct {
		Common     string           `json:"common"`
		Template   string           `json:"template"`
		Projection reportProjection `json:"projection"`
	}{string(common), string(template), projection})
	if err != nil {
		return nil, err
	}
	const program = "import json,sys,types\np=json.load(sys.stdin)\nm=types.ModuleType('pwc_report_io')\nexec(p['common'],m.__dict__)\nsys.modules[m.__name__]=m\nn={'__name__':'triage_report'}\nexec(p['template'],n)\nsys.stdout.buffer.write(n['render'](**p['projection']))\n"
	cmd := exec.CommandContext(ctx, "python3", "-I", "-S", "-B", "-c", program)
	cmd.Stdin = bytes.NewReader(input)
	stdout, stderr := &reportBuffer{limit: 64 << 20}, &reportBuffer{limit: 64 << 10}
	cmd.Stdout, cmd.Stderr, cmd.WaitDelay = stdout, stderr, time.Second
	if err := cmd.Run(); err != nil {
		return nil, errors.Join(context.Cause(ctx), fmt.Errorf("report renderer verification: %w: %s", err, stderr.buffer.String()))
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return stdout.buffer.Bytes(), nil
}
