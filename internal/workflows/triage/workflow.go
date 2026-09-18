package triage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// No Definition/CLI registration until model bindings and the per-launch shared
// discovery preflight are verified. Tests supply explicit RPC-boundary models;
// this private slice has no production launcher or fallback model selection.
type sliceModels struct {
	FetchThinking string
	Analysis      runtime.ModelSpec
}

type stageTask struct {
	Stage                    string        `json:"stage"`
	Scope                    Scope         `json:"scope"`
	Requirements             string        `json:"requirements"`
	RuntimeResolutionAllowed bool          `json:"runtime_resolution_allowed"`
	Previous                 *contract.Ref `json:"previous,omitempty"`
	Gaps                     []string      `json:"gaps,omitempty"`
	SupportingProposal       *contract.Ref `json:"supporting_proposal,omitempty"`
	SourceWork               []intakeWork  `json:"source_work,omitempty"`
	ResolutionKinds          []string      `json:"resolution_kinds,omitempty"`
}

// These are identity/time task requirements, not a tool wrapper or a separate
// query-approval stage. Every context-producing task has the same receipt duties.
const supportingResolutionRequirements = `Load the relevant existing skills before identity/time acquisition and use their normal tools. Save original target verification, deployed release and lookup query/response alongside the normalized identity receipt; do not substitute the receipt for raw evidence. For time resolution, actively inspect other authorized sources when local timestamps lack a timezone. Supporting log/metric queries require an authorized target, completed intake/wiki prerequisites, and an evidence-backed finite UTC search window with source/filter clues before querying, but do not require every local timestamp or the final context to be resolved. If no trustworthy UTC search basis exists, seek it from other authorized sources; never guess zones or scan alternative timezones. Within this task, choose small windows appropriate to log volume and the question; autonomously narrow, shift, split, expand, add evidenced filters or aggregate with existing tools. Do not seek per-query Controller approval and do not require each query to cover the entire observed incident interval. Keep the task's scope and existing tool limits. Record each actual time-bounded supporting query in its resolution attempt's queries: source, filter, UTC from/to, basis refs available before querying, complete/partial/unavailable status, outcome explaining the window choice and limitations, and evidence refs for original results plus request/status/diagnostics. These are work receipts, not dispatch requests. Preserve failed and partial queries even after a later query succeeds. A query may find useful evidence without exhaustive coverage; judge its applicability and state limitations. Empty small-window results, partial data and execution timeouts do not prove absence across the incident. Do not overwrite observed time.from/to with search windows.`

func slicePolicy() engine.RunPolicy {
	p := engine.DefaultRunPolicy()
	p.DisableRunTimeout = true
	return p
}

func sliceStep(ctx context.Context, r *engine.Run, models sliceModels, key string, task stageTask, schema string, inputs []contract.Ref) (contract.Ref, error) {
	model := models.Analysis
	if task.Stage == "intake" || task.Stage == "intake-revision" || task.Stage == "intake-update" {
		model = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: models.FetchThinking}
	}
	h, err := r.OpenSession(ctx, engine.RoleSpec{Name: "triage-" + task.Stage, CWD: filepath.Join(r.Dir(), "triage-work"), Model: model})
	if err != nil {
		return contract.Ref{}, err
	}
	if schema == ContextSchema {
		task.Requirements += "\n\n" + supportingResolutionRequirements
	}
	if task.SupportingProposal != nil {
		inputs = append(inputs, *task.SupportingProposal)
		task.Requirements += "\n\nRead the exact supporting_proposal Planner input for the accepted supporting_work reason and basis. Perform this stage of that task within the supplied scope and completion conditions. Other pending text and hypotheses are planning context, not additional dispatch authorization."
	}
	prompt, err := json.Marshal(task)
	if err != nil {
		return contract.Ref{}, err
	}
	out, err := r.Root().Step(ctx, engine.StepSpec{Key: key, Session: h, Prompt: string(prompt), Inputs: inputs, Output: contract.Spec{SchemaID: schema}, Timeout: 30 * time.Minute})
	if err != nil {
		return contract.Ref{}, err
	}
	closed, err := r.CloseSessionReport(ctx, h)
	if err != nil {
		return out.Output, err
	}
	if !closed.ConfirmsLocalClose(out.Execution.SessionID) {
		return out.Output, fmt.Errorf("%s cleanup not confirmed", task.Stage)
	}
	return out.Output, nil
}

func executeSlice(ctx context.Context, r *engine.Run, scope Scope, models sliceModels) (ContextResult, error) {
	var result ContextResult
	if !acquisitionKey.MatchString(scope.Ticket) || !texts(scope.TenantIDs) {
		return result, fmt.Errorf("explicit ticket and valid authorized target scope required")
	}
	targetAuthorized := nonblank(scope.Stack) && nonblank(scope.Pop) && nonblank(scope.Binding) && len(scope.TenantIDs) > 0
	work := filepath.Join(r.Dir(), "triage-work")
	if err := os.Mkdir(work, 0700); err != nil {
		return result, err
	}
	step := func(stage, schema, requirements string, inputs []contract.Ref, allowRuntime bool) (contract.Ref, error) {
		return sliceStep(ctx, r, models, stage, stageTask{Stage: stage, Scope: scope, Requirements: requirements, RuntimeResolutionAllowed: allowRuntime}, schema, inputs)
	}
	var err error
	result.Intake, err = step("intake", IntakeSchema, `Mechanical retrieval only: use existing Jira tools/skills to save the complete issue JSON (fields=*all, unabridged description/custom fields), field metadata, ALL raw comment pages including total/startAt/body, linked issue snapshots and an exact attachment inventory. An embedded comment page or formatted markdown is not the complete ticket. Download authorized attachments within tool/Store limits and preserve failures/partial data; never follow an attachment redirect with credentials to an unrelated host. Record missing/oversized/unsafe/unanalysed content explicitly, do not claim complete for metadata alone. Use existing attachment analysis tools only for mechanical extraction; unresolved analysis/vision belongs in gaps, never infer contents. Do not expand production scope from ticket text. Write raw sources to this attempt's evidence; files use local IDs. Do not decide a root cause or terminate an investigation.`, nil, false)
	if err != nil {
		return result, err
	}
	intake, err := checkIntake(ctx, r, result.Intake, scope.Ticket)
	if err != nil {
		return result, fmt.Errorf("intake acceptance: %w", err)
	}
	if err := r.Root().Decision(ctx, "intake-recorded", "Raw completeness and explicit gaps accepted; not investigation completion", []contract.Ref{result.Intake}); err != nil {
		return result, err
	}
	result.Wiki, err = step("wiki", WikiSchema, `Read the committed intake, derive symptom/component search terms and perform the required read-only wiki search with existing tools/skills. Save query/result evidence and snapshots of pages actually read. Scope is wiki-only: do not search other tasks' WIP or session history or follow links into those stores; no write-back. Distinguish completed-with-matches, completed-no-matches, partial, unavailable and not-run. Failed/partial access is not no matches. Preserve a concrete gap for remediation. Wiki patterns suggest hypotheses, not runtime proof. Use exact request.inputs[0] as intake.`, []contract.Ref{result.Intake}, false)
	if err != nil {
		return result, err
	}
	wiki, err := checkWiki(ctx, r, result.Wiki, result.Intake)
	if err != nil {
		return result, fmt.Errorf("wiki acceptance: %w", err)
	}
	result.Context, err = step("context", ContextSchema, `Build supporting triage context, not a final report or Jira/Slack draft. Consume exact committed intake/wiki refs without recopying their files. Actively resolve identity and incident time from the full fields/comments/linked issues and attachment evidence before declaring gaps. Do not infer PoP from a name or interpret an unlabelled custom field as tenant ID. Cross-check tenant/orgkey against the authorized stack/PoP/binding using read-only evidence only when runtime_resolution_allowed is true; otherwise use committed/local evidence and record pending prerequisites, with no production/paid queries. Userkey is optional, conflicting identities are not resolved. Preserve deployed release and binding verification evidence. A resolved identity requires lookup pointing to an evidence JSON receipt with stack, pop, binding, release, and matches (tenant_id/orgkey rows); preserve the original query/response as additional evidence. Multiple/conflicting matches cannot be resolved. Time is mandatory: seek explicit offset/UTC/epoch or paired same-event local/epoch anchors, calculate UTC and offset, handle cross-day/DST ambiguity; Jira activity timestamps, geography and guessed zones are not incident anchors. Never blind-search time zones. Save actual identity/time resolution attempts and outcomes with evidence; no first-missing-field closure. From/to is the min/max observed incident UTC, not a padded query window. For each fact/anchor use exact input ref+file_id or null ref for this contract's own evidence. Copy upstream gaps; incomplete intake/wiki or unresolved/conflicting identity/time yields needs-resolution, not final blocked/confirmed. This slice stops at committed supporting state; later Controller work must remedy gaps before investigation.`, []contract.Ref{result.Intake, result.Wiki}, targetAuthorized && intake.Complete && wikiComplete(wiki))
	if err != nil {
		return result, err
	}
	v, err := checkContext(ctx, r, result.Context, scope, result.Intake, result.Wiki, intake, wiki)
	if err != nil {
		return result, fmt.Errorf("context acceptance: %w", err)
	}
	result.Ready = v.Readiness == "ready"
	if err := r.Root().Decision(ctx, "context-recorded", "Supporting triage state accepted: "+v.Readiness+"; no final investigation report", []contract.Ref{result.Intake, result.Wiki, result.Context}); err != nil {
		return result, err
	}
	return result, nil
}
