package triage

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/workflows/triagev2"
)

const WorkerSchema = "triage.worker.v1"

// A task belongs to its committed Planner proposal and context. Dependencies
// name stable task IDs; only the Controller resolves them to accepted results.
type WorkerTask struct {
	ID             string        `json:"id"`
	SourceKind     string        `json:"source_kind"`
	Responsibility string        `json:"responsibility"`
	Question       string        `json:"question"`
	Requirements   []string      `json:"requirements"`
	Basis          []Evidence    `json:"basis"`
	DependsOn      []string      `json:"depends_on"`
	Search         *WorkerSearch `json:"search"`
}

type WorkerSearch struct {
	Source string     `json:"source"`
	Filter string     `json:"filter"`
	From   string     `json:"from"`
	To     string     `json:"to"`
	Basis  []Evidence `json:"basis"`
}

// Status describes delivery, never whether a hypothesis is true. Local evidence
// retains this result as its owner when a later Planner or worker cites it.
type WorkerResult struct {
	Proposal contract.Ref      `json:"proposal"`
	Context  contract.Ref      `json:"context"`
	TaskID   string            `json:"task_id"`
	Work     string            `json:"work"`
	Status   string            `json:"status"`
	Inputs   []contract.Ref    `json:"inputs"`
	Evidence []Evidence        `json:"evidence"`
	Queries  []SupportingQuery `json:"queries"`
	Analysis []Fact            `json:"analysis"`
	Gaps     []string          `json:"gaps"`
	Next     []PlannerQuestion `json:"next"`
}

type workerRequest struct {
	Workspace    string         `json:"workspace"`
	Stage        string         `json:"stage"`
	Scope        Scope          `json:"scope"`
	Proposal     contract.Ref   `json:"proposal"`
	Context      contract.Ref   `json:"context"`
	Task         WorkerTask     `json:"task"`
	Dependencies []contract.Ref `json:"dependencies"`
	Requirements string         `json:"requirements"`
}

const workerRequirements = `Load the relevant existing skills and use their normal tools for this complete authorized task. Do not create agents, select models or dispatch other work. Respect source_kind, responsibility, scope and task completion requirements. Evidence-only work acquires/extracts evidence and diagnostics, with an empty analysis array; analysis work may interpret supplied and newly acquired evidence. Evidence truth, applicability, hypotheses and next questions are your reasoning, not Controller verdicts. Do not expand production scope from discovered sources. Read the exact proposal/context/dependencies and true evidence owners in request.inputs; a ref and its file_id must come from the same owner: the file_id must appear in that exact ref's files[] list (pair a context ref with context-owned file ids, a worker ref with worker-owned file ids), never a file id from a different publication under another ref. Return proposal, context, task_id and inputs exactly as dispatched. Record work actually performed, evidence, all actual supporting queries, analysis, gaps and next questions. Every recorded query uses the exact schema fields: source, filter, from, to, basis, status, outcome and evidence (never a result field: the schema has no result key), with a strictly nonzero UTC window (from/to follow the schema's Z-suffix pattern) and a nonempty basis array (the inputs available before querying that motivated it, e.g. the issue/context evidence, prior queries or the dispatched task basis; a query with empty basis fails acceptance); for snapshot or point-in-time commands (git, gh, kubectl, file reads) record the actual execution start and a strictly later end, never from == to. Complete/incomplete means delivery status only, never root cause confirmation. Incomplete requires concrete gaps; execution failure or timeout is not a successful incomplete result. Preserve raw requests, results, receipts, failure/partial diagnostics and your own analysis narratives under this attempt's evidence, declaring every file you cite as evidence (including analysis notes) as kind=evidence; kind artifact is reserved exclusively for final deliverable outputs never cited as evidence. An evidence citation of a file declared kind=artifact fails acceptance with unknown file: when in doubt, declare kind=evidence. Null evidence ref means only this result's own declared file; retained evidence uses its exact input owner ref and file_id, and that owner's files[] list must actually declare the named file_id; never invent or assume a file under any other ref (a planner or batch ref that declares no files owns nothing). A later consumer qualifies your local evidence with your result ref. For logs/metrics, search supplies an evidence-backed initial finite UTC window and source/filter, not per-query approval. Autonomously narrow, shift, split, expand or aggregate within the authorized task and tool limits, retaining actual query conditions, UTC basis, complete/partial/unavailable status, outcomes, raw results and diagnostics even after later success. Do not guess timezones or overwrite observed incident anchors with query windows. Empty small windows, partial data and timeouts do not prove incident-wide absence. Preserve existing target/DB receipts and UTC calculations. Do not write back, publish drafts or produce a final report. Next questions are not dispatch authorization.`

// These records exist only within one acceptance pass. The caller retains just
// accepted Refs, not a second publication registry or cross-Step byte cache.
type workerRecord struct {
	value   WorkerResult
	sources map[contract.Ref][]file
}

func workerSources(base map[contract.Ref][]file, refs []contract.Ref, records map[contract.Ref]workerRecord) (map[contract.Ref][]file, error) {
	sources := maps.Clone(base)
	seen := map[contract.Ref]bool{}
	for _, ref := range refs {
		record, ok := records[ref]
		if !ok || seen[ref] {
			return nil, fmt.Errorf("worker results require distinct workflow-accepted refs")
		}
		seen[ref] = true
		maps.Copy(sources, record.sources)
	}
	return sources, nil
}

func checkWorkerTasks(v PlannerState, h contextHistory, sources map[contract.Ref][]file, records map[contract.Ref]workerRecord) error {
	if len(v.WorkerTasks) > 0 && v.SupportingWork != nil {
		return fmt.Errorf("worker_tasks and supporting_work are mutually exclusive")
	}
	completed := map[string]bool{}
	for _, ref := range v.WorkerResults {
		completed[records[ref].value.TaskID] = true
	}
	ids := map[string]bool{}
	for _, task := range v.WorkerTasks {
		if !nonblank(task.ID) || ids[task.ID] || completed[task.ID] {
			return fmt.Errorf("worker tasks require unique, uncompleted IDs")
		}
		ids[task.ID] = true
	}
	evidence := func(e Evidence) error {
		if e.Ref == nil || !hasFile(sources[*e.Ref], e.FileID) {
			return fmt.Errorf("worker task basis must name an exact input owner/file")
		}
		return nil
	}
	for _, task := range v.WorkerTasks {
		if !nonblank(task.Question) || len(task.Requirements) == 0 || !texts(task.Requirements) {
			return fmt.Errorf("worker task requires question and completion requirements")
		}
		switch task.SourceKind {
		case "code", "db", "logs", "metrics", "attachments", "vision":
		default:
			return fmt.Errorf("unsupported worker source_kind %q", task.SourceKind)
		}
		if task.Responsibility != "evidence-only" && task.Responsibility != "analysis" {
			return fmt.Errorf("unsupported worker responsibility %q", task.Responsibility)
		}
		for _, e := range task.Basis {
			if err := evidence(e); err != nil {
				return err
			}
		}
		deps := map[string]bool{}
		for _, id := range task.DependsOn {
			if !nonblank(id) || id == task.ID || deps[id] {
				return fmt.Errorf("worker dependency must name a distinct task ID")
			}
			deps[id] = true
		}
		if (task.SourceKind == "logs" || task.SourceKind == "metrics") && task.Search == nil {
			return fmt.Errorf("log/metric task requires initial search basis")
		}
		if task.Search != nil {
			search := task.Search
			from, e1 := utc(search.From)
			to, e2 := utc(search.To)
			if e1 != nil || e2 != nil || !from.Before(to) {
				return fmt.Errorf("worker search requires a nonzero UTC window")
			}
			if !nonblank(search.Source) || !nonblank(search.Filter) || len(search.Basis) == 0 {
				return fmt.Errorf("worker search requires source, filter and basis")
			}
			for _, e := range search.Basis {
				if err := evidence(e); err != nil {
					return err
				}
			}
		}
		if task.SourceKind == "db" || task.SourceKind == "logs" || task.SourceKind == "metrics" {
			scope := h.value.Scope
			if !nonblank(scope.Stack) || !nonblank(scope.Pop) || !nonblank(scope.Binding) || len(scope.TenantIDs) == 0 || !texts(scope.TenantIDs) {
				return fmt.Errorf("runtime worker requires authorized target scope: task %q (source_kind %s) needs nonblank stack/pop/binding and tenant IDs in the request scope (got stack=%q pop=%q binding=%q tenants=%v); do not propose db/logs/metrics worker tasks on an unauthorized scope, plan code/attachment/vision tasks or supporting_work instead", task.ID, task.SourceKind, scope.Stack, scope.Pop, scope.Binding, scope.TenantIDs)
			}
		}
	}
	return nil
}

func (a *acceptance) workerDispatch(scope Scope, proposal contract.Ref, taskID string, records map[contract.Ref]workerRecord) (workerRequest, []contract.Ref, map[contract.Ref][]file, error) {
	var request workerRequest
	state, err := readAccepted[PlannerState](a, proposal, PlannerSchema)
	if err != nil {
		return request, nil, nil, err
	}
	h, err := a.loadContextHistory(scope, state.Data.Context)
	if err != nil {
		return request, nil, nil, err
	}
	if err := a.checkPlannerWithWorkers(proposal, h, state.Data.Previous, records); err != nil {
		return request, nil, nil, err
	}
	index := slices.IndexFunc(state.Data.WorkerTasks, func(task WorkerTask) bool { return task.ID == taskID })
	if index < 0 {
		return request, nil, nil, fmt.Errorf("worker dispatch requires an explicit proposed task ID")
	}
	task := state.Data.WorkerTasks[index]
	for _, record := range records {
		if record.value.TaskID == taskID {
			return request, nil, nil, fmt.Errorf("worker task ID already completed")
		}
	}
	refs := slices.Clone(state.Data.WorkerResults)
	dependencies := []contract.Ref{}
	for _, id := range task.DependsOn {
		var found *contract.Ref
		for ref, record := range records {
			if record.value.TaskID == id {
				found = &ref
				break
			}
		}
		if found == nil {
			return request, nil, nil, fmt.Errorf("worker dependency %q has no accepted result", id)
		}
		dependencies = append(dependencies, *found)
		if !slices.Contains(refs, *found) {
			refs = append(refs, *found)
		}
	}
	sources, err := workerSources(h.sources, refs, records)
	if err != nil {
		return request, nil, nil, err
	}
	sources, err = a.wikiSources(scope, sources, state.Data.WikiResults, records)
	if err != nil {
		return request, nil, nil, err
	}
	sources, err = a.recoverySources(scope, sources, state.Data.Recovery)
	if err != nil {
		return request, nil, nil, err
	}
	if task.SourceKind == "db" || task.SourceKind == "logs" || task.SourceKind == "metrics" {
		intake, err := a.checkIntake(h.value.Intake, scope.Ticket)
		if err != nil {
			return request, nil, nil, err
		}
		wiki, err := a.checkWiki(h.value.Wiki, h.value.Intake)
		if err != nil {
			return request, nil, nil, err
		}
		if !intake.Complete || !wikiComplete(wiki) {
			return request, nil, nil, fmt.Errorf("runtime worker requires completed intake/wiki prerequisites")
		}
		if len(state.Data.WikiResults) > 0 {
			latest, err := readAccepted[WikiSearch](a, state.Data.WikiResults[len(state.Data.WikiResults)-1], WikiSchema)
			if err != nil {
				return request, nil, nil, err
			}
			if !wikiComplete(latest.Data) {
				return request, nil, nil, fmt.Errorf("runtime worker requires completed investigation wiki prerequisite")
			}
		}
	}
	request = workerRequest{Stage: "worker-" + task.SourceKind + "-" + task.Responsibility, Scope: scope, Proposal: proposal, Context: h.ref, Task: task, Dependencies: dependencies, Requirements: workerRequirements}
	inputs := append([]contract.Ref{proposal, h.ref}, dependencies...)
	inputs = appendSourceInputs(inputs, sources)
	return request, inputs, sources, nil
}

func checkWorkerResult(p publication[WorkerResult], request workerRequest, inputs []contract.Ref, sources map[contract.Ref][]file) error {
	v := p.Data
	// Inputs are an ownership set, not an ordering: the agent must return the
	// exact same refs with no additions or omissions, but their order in the
	// echoed array carries no semantic meaning and is not verified.
	sameInputs := len(v.Inputs) == len(inputs) && contract.RefSetEqual(v.Inputs, inputs)
	if v.Proposal != request.Proposal || v.Context != request.Context || v.TaskID != request.Task.ID || !sameInputs {
		if os.Getenv("PWC_DEBUG_ACCEPT") != "" {
			fmt.Fprintf(os.Stderr, "PWC_DEBUG_ACCEPT worker mismatch: proposal=%v context=%v taskID=%q vs %q sameInputs=%v inputs=%d vs %d\\n", v.Proposal != request.Proposal, v.Context != request.Context, v.TaskID, request.Task.ID, sameInputs, len(v.Inputs), len(inputs))
		}
		return fmt.Errorf("worker proposal/context/task/inputs mismatch")
	}
	if !nonblank(v.Work) || !texts(v.Gaps) || (v.Status != "complete" && v.Status != "incomplete") || (v.Status == "incomplete" && len(v.Gaps) == 0) {
		return fmt.Errorf("worker requires actual work and complete/incomplete delivery with gaps")
	}
	if request.Task.Responsibility == "evidence-only" && len(v.Analysis) != 0 {
		return fmt.Errorf("evidence-only worker cannot supply analysis")
	}
	evidence := func(e Evidence) error {
		files := p.Files
		if e.Ref != nil {
			var ok bool
			files, ok = sources[*e.Ref]
			if !ok {
				return fmt.Errorf("worker evidence is not an exact committed input")
			}
		}
		if !hasFile(files, e.FileID) {
			return fmt.Errorf("%s", fileDiagnostic(files, e.FileID))
		}
		return nil
	}
	for _, e := range v.Evidence {
		if err := evidence(e); err != nil {
			return err
		}
	}
	for _, q := range v.Queries {
		if err := checkSupportingQuery(q, evidence); err != nil {
			return err
		}
	}
	for _, fact := range v.Analysis {
		if !nonblank(fact.Value) || len(fact.Evidence) == 0 {
			return fmt.Errorf("worker analysis requires text and evidence")
		}
		for _, e := range fact.Evidence {
			if err := evidence(e); err != nil {
				return err
			}
		}
	}
	for _, next := range v.Next {
		if !nonblank(next.Question) || len(next.Requirements) == 0 || !texts(next.Requirements) {
			return fmt.Errorf("worker next question requires completion requirements")
		}
		for _, e := range next.Basis {
			if err := evidence(e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *acceptance) loadWorkerResults(scope Scope, refs []contract.Ref) (map[contract.Ref]workerRecord, error) {
	records := map[contract.Ref]workerRecord{}
	for _, ref := range refs {
		if _, ok := records[ref]; ok {
			return nil, fmt.Errorf("duplicate worker result ref")
		}
		p, err := readAccepted[WorkerResult](a, ref, WorkerSchema)
		if err != nil {
			return nil, err
		}
		request, inputs, sources, err := a.workerDispatch(scope, p.Data.Proposal, p.Data.TaskID, records)
		if err != nil {
			return nil, err
		}
		if err := checkWorkerResult(p, request, inputs, sources); err != nil {
			return nil, err
		}
		// Keep the original proposal and every delivered owner explicit on a
		// fresh handoff. A Planner publication is not a raw evidence owner.
		sources[p.Data.Proposal] = nil
		sources[ref] = p.Files
		records[ref] = workerRecord{value: p.Data, sources: sources}
	}
	return records, nil
}

// work dispatches exactly one named task. It is not a ready queue or retry loop.
func (p *plannerCaller) work(ctx context.Context, models sliceModels, taskID string) (contract.Ref, error) {
	if p.stopped || p.last == nil {
		return contract.Ref{}, fmt.Errorf("worker dispatch requires an accepted state and usable planner")
	}
	p.stopped = true
	prepared, err := prepareWorker(ctx, p.r, p.scope, p.history.ref, *p.last, p.workerResults, taskID)
	if err != nil {
		return contract.Ref{}, err
	}
	ref, err := runWorker(ctx, p.r, p.r.Root(), models, prepared)
	if err != nil {
		return contract.Ref{}, err
	}
	if err := acceptWorker(ctx, p.r, p.scope, *p.last, p.workerResults, prepared, ref); err != nil {
		return contract.Ref{}, err
	}
	p.workerResults, p.stopped = append(slices.Clone(p.workerResults), ref), false
	return ref, nil
}

type preparedWorker struct {
	recovery bool
	request  workerRequest
	inputs   []contract.Ref
	sources  map[contract.Ref][]file
	key      string
}

func prepareWorker(ctx context.Context, r *engine.Run, scope Scope, contextRef, proposal contract.Ref, accepted []contract.Ref, taskID string) (preparedWorker, error) {
	var prepared preparedWorker
	a := newAcceptance(ctx, r)
	records, err := a.loadWorkerResults(scope, accepted)
	if err != nil {
		return prepared, err
	}
	request, inputs, sources, err := a.workerDispatch(scope, proposal, taskID, records)
	if err != nil {
		return prepared, err
	}
	prepared.sources = sources
	if request.Context != contextRef {
		return prepared, fmt.Errorf("worker proposal differs from current context")
	}
	state, err := readAccepted[PlannerState](a, proposal, PlannerSchema)
	if err != nil {
		return prepared, err
	}
	index := slices.IndexFunc(state.Data.WorkerTasks, func(task WorkerTask) bool { return task.ID == taskID })
	key := fmt.Sprintf("worker-%s-%d", proposal.AttemptID, index)
	return preparedWorker{request: request, inputs: inputs, sources: sources, key: key}, nil
}

func runWorker(ctx context.Context, r *engine.Run, s *engine.Scope, models sliceModels, prepared preparedWorker) (contract.Ref, error) {
	request, inputs, key := prepared.request, prepared.inputs, prepared.key
	request.Workspace = filepath.Join(r.Dir(), "triage-work")
	if err := s.Decision(ctx, key+"-dispatch", "Dispatch one explicit worker task: "+request.Task.ID, inputs); err != nil {
		if prepared.recovery {
			return contract.Ref{}, &triagev2.TaskFailure{Cause: err, Stage: request.Stage}
		}
		return contract.Ref{}, err
	}
	model := models.Analysis
	if request.Task.Responsibility == "evidence-only" {
		model = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: models.FetchThinking}
	}
	if prepared.recovery {
		state, err := readAccepted[PlannerState](newAcceptance(ctx, r), request.Proposal, PlannerSchema)
		if err != nil {
			return contract.Ref{}, err
		}
		for _, choice := range state.Data.RecoveryChoices {
			if choice.Action == "inspect" {
				request.Requirements += "\nRecovery inspection only: inspect authorized read-only status/evidence for the uncertain remote work in the exact proposal recovery metadata. Do not create, resubmit or restart that work. Record job identity, actual status, limitations and evidence-backed safety assessment."
			}
		}
	}
	request.Requirements += "\n" + triageWorkspaceRequirements
	return triagev2.RunTaskStep(ctx, r, triagev2.TaskStep{Scope: s, Model: model, Stage: request.Stage, Key: key, Task: request, Schema: WorkerSchema, Inputs: inputs, Recovery: prepared.recovery, Validate: func(ctx context.Context, ref contract.Ref) error {
		// Full acceptance on every (re)published worker result; a rejected
		// contract returns to the session as repair feedback, and the repaired
		// one must pass these same gates again.
		after := newAcceptance(ctx, r)
		result, err := readAccepted[WorkerResult](after, ref, WorkerSchema)
		if err != nil {
			return err
		}
		return checkWorkerResult(result, prepared.request, prepared.inputs, prepared.sources)
	}})
}

func acceptWorker(ctx context.Context, r *engine.Run, scope Scope, proposal contract.Ref, accepted []contract.Ref, prepared preparedWorker, ref contract.Ref) error {
	after := newAcceptance(ctx, r)
	records, err := after.loadWorkerResults(scope, accepted)
	if err != nil {
		return err
	}
	// The dispatch-time snapshot is authoritative for the expected request,
	// inputs and sources: parallel siblings in the same batch may have
	// completed since dispatch, and their sources must not leak into this
	// worker's expected inputs. The recomputed dispatch still validates that
	// the task and its dependencies are accepted.
	if _, _, _, err := after.workerDispatch(scope, proposal, prepared.request.Task.ID, records); err != nil {
		return err
	}
	result, err := readAccepted[WorkerResult](after, ref, WorkerSchema)
	if err != nil {
		return err
	}
	if err := checkWorkerResult(result, prepared.request, prepared.inputs, prepared.sources); err != nil {
		return fmt.Errorf("worker result acceptance: %w", err)
	}
	return r.Root().Decision(ctx, prepared.key+"-recorded", "Worker delivery accepted for Planner interpretation, not a verified conclusion", append(slices.Clone(prepared.inputs), ref))
}
