package triage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/workflows/triagev2"
)

type sliceModels struct {
	FetchThinking string
	Analysis      runtime.ModelSpec
}

type stageTask struct {
	Workspace                string        `json:"workspace"`
	Request                  string        `json:"request,omitempty"`
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

// This is triage task guidance, not an enforced filesystem sandbox.
const triageWorkspaceRequirements = `Follow the existing AGENTS.md and relevant skills from the Pi working directory. Where this task permits repository inspection, read source context in that directory and sibling repositories without modifying them; this does not expand the task's allowed evidence or acquisition scope. Do not write to those repositories. Use the absolute workspace for scratch and downloads, and the explicit Step request/candidate/evidence paths for outputs. Do not resolve output paths relative to the Pi working directory. Scratch files are not committed evidence; downstream work consumes only supplied exact committed Refs.`

// These are identity/time task requirements, not a tool wrapper or a separate
// query-approval stage. Every context-producing task has the same receipt duties.
const supportingResolutionRequirements = `Load the relevant existing skills before identity/time acquisition and use their normal tools. Save original target verification, deployed release and lookup query/response alongside the normalized identity receipt; do not substitute the receipt for raw evidence. For time resolution, actively inspect other authorized sources when local timestamps lack a timezone. Supporting log/metric queries require an authorized target, completed intake/wiki prerequisites, and an evidence-backed finite UTC search window with source/filter clues before querying, but do not require every local timestamp or the final context to be resolved. If no trustworthy UTC search basis exists, seek it from other authorized sources; never guess zones or scan alternative timezones. Within this task, choose small windows appropriate to log volume and the question; autonomously narrow, shift, split, expand, add evidenced filters or aggregate with existing tools. Do not seek per-query Controller approval and do not require each query to cover the entire observed incident interval. Keep the task's scope and existing tool limits. Record each actual time-bounded supporting query in its resolution attempt's queries: source, filter, UTC from/to (a strictly nonzero window: from must be strictly before to; from == to is always rejected; for snapshot, point-in-time or single-timestamp lookups such as git rev-parse, gh api or kubectl get, record the actual command execution start and a strictly later end timestamp, never from == to), basis refs available before querying, complete/partial/unavailable status, outcome explaining the window choice and limitations, and evidence refs for original results plus request/status/diagnostics. These are work receipts, not dispatch requests. Preserve failed and partial queries even after a later query succeeds. A query may find useful evidence without exhaustive coverage; judge its applicability and state limitations. Empty small-window results, partial data and execution timeouts do not prove absence across the incident. Do not overwrite observed time.from/to with search windows.`

func slicePolicy() engine.RunPolicy {
	p := engine.DefaultRunPolicy()
	p.DisableRunTimeout = true
	return p
}

func sliceStep(ctx context.Context, r *engine.Run, models sliceModels, key string, task stageTask, schema string, inputs []contract.Ref) (contract.Ref, error) {
	return sliceStepRecovery(ctx, r, models, key, task, schema, inputs, false)
}

func sliceStepRecovery(ctx context.Context, r *engine.Run, models sliceModels, key string, task stageTask, schema string, inputs []contract.Ref, recovery bool) (contract.Ref, error) {
	request, err := callerRequest(r)
	if err != nil {
		return contract.Ref{}, err
	}
	task.Request = request
	task.Workspace = filepath.Join(r.Dir(), "triage-work")
	task.Requirements += "\n" + triageWorkspaceRequirements
	if request != "" {
		task.Requirements += "\nThe request is the original caller instruction. Address it within the supplied scope; it does not add target authorization or constitute evidence."
	}
	model := models.Analysis
	if task.Stage == "intake" || task.Stage == "intake-revision" || task.Stage == "intake-update" {
		model = runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/deepseek-v4p1-flash", Thinking: models.FetchThinking}
	}
	if schema == ContextSchema {
		task.Requirements += "\n\n" + supportingResolutionRequirements
	}
	if task.SupportingProposal != nil {
		inputs = append(inputs, *task.SupportingProposal)
		task.Requirements += "\n\nRead the exact supporting_proposal Planner input for the accepted supporting_work reason and basis. Perform this stage of that task within the supplied scope and completion conditions. Other pending text and hypotheses are planning context, not additional dispatch authorization."
	}
	return triagev2.RunTaskStep(ctx, r, triagev2.TaskStep{Scope: r.Root(), Model: model, Stage: task.Stage, Key: key, Task: task, Schema: schema, Inputs: inputs, Recovery: recovery, Validate: sliceAcceptance(ctx, r, schema, task.Stage, task.Scope, inputs)})
}

// sliceAcceptance returns the semantic acceptance gate for one slice stage, or
// nil when the schema has no standalone publication check. Acceptance runs on
// every (re)published contract; a rejected one returns to the session as
// repair feedback.
func sliceAcceptance(ctx context.Context, r *engine.Run, schema, stage string, scope Scope, inputs []contract.Ref) func(context.Context, contract.Ref) error {
	// Only the initial acquisition stages carry a standalone publication
	// gate here; revision/update/resume stages keep their dedicated
	// post-hoc acceptance contracts (different inputs and lineage rules).
	switch schema {
	case IntakeSchema:
		if stage != "intake" {
			return nil
		}
		return func(ctx context.Context, ref contract.Ref) error {
			if _, err := checkIntake(ctx, r, ref, scope.Ticket); err != nil {
				return fmt.Errorf("intake acceptance: %w", err)
			}
			return nil
		}
	case WikiSchema:
		if stage != "wiki" {
			return nil
		}
		return func(ctx context.Context, ref contract.Ref) error {
			intakeRef := contract.Ref{}
			for _, in := range inputs {
				if in.SchemaID == IntakeSchema {
					intakeRef = in
					break
				}
			}
			if intakeRef == (contract.Ref{}) {
				return nil
			}
			if _, err := checkWiki(ctx, r, ref, intakeRef); err != nil {
				return fmt.Errorf("wiki acceptance: %w", err)
			}
			return nil
		}
	case ContextSchema:
		if stage != "context" {
			return nil
		}
		return func(ctx context.Context, ref contract.Ref) error {
			var intakeRef, wikiRef contract.Ref
			for _, in := range inputs {
				switch in.SchemaID {
				case IntakeSchema:
					intakeRef = in
				case WikiSchema:
					wikiRef = in
				}
			}
			if intakeRef == (contract.Ref{}) || wikiRef == (contract.Ref{}) {
				return nil
			}
			intake, err := checkIntake(ctx, r, intakeRef, scope.Ticket)
			if err != nil {
				return fmt.Errorf("intake prerequisite: %w", err)
			}
			wiki, err := checkWiki(ctx, r, wikiRef, intakeRef)
			if err != nil {
				return fmt.Errorf("wiki prerequisite: %w", err)
			}
			if _, err := checkContext(ctx, r, ref, scope, intakeRef, wikiRef, intake, wiki); err != nil {
				return fmt.Errorf("context acceptance: %w", err)
			}
			return nil
		}
	default:
		return nil
	}
}

func executeSlice(ctx context.Context, r *engine.Run, scope Scope, models sliceModels) (ContextResult, error) {
	var result ContextResult
	if !jiraKey.MatchString(scope.Ticket) || !texts(scope.TenantIDs) {
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
	result.Intake, err = step("intake", IntakeSchema, `Mechanical retrieval only: use existing Jira tools/skills to save the complete issue JSON (fields=*all, unabridged description/custom fields), field metadata saved as a bare top-level JSON array of field objects (no wrapper object: the file content starts with [ and each element has id/name), ALL raw comment pages including total/startAt/body, linked issue snapshots and an exact attachment inventory. comments[] must cover every page from start 0 to total even when total is 0: then include exactly one page with start 0, the fetched total 0 and an empty comments body, saved as evidence. linked[] mirrors the ticket's formal Jira issue links (fields.issuelinks) exactly: one entry per linked key, no others; the parent epic and tickets merely referenced in description text or comments are not linked[] entries (record them in gaps or observations if relevant). An embedded comment page or formatted markdown is not the complete ticket. Record url as the canonical browse URL (https://host/browse/TICKET), never a REST API endpoint, with ticket and a fetched_at UTC timestamp. Download authorized attachments within tool/Store limits and preserve failures/partial data; never follow an attachment redirect with credentials to an unrelated host. Record missing/oversized/unsafe/unanalysed content explicitly, do not claim complete for metadata alone. A genuine absence confirmed from raw evidence (zero comments, empty issuelinks, no attachments) is completeness, not a gap: gaps list only retrieval work that remains undone or failed; complete=true requires gaps to stay empty, and such absence observations go in gaps nowhere. Use existing attachment analysis tools only for mechanical extraction; unresolved analysis/vision belongs in gaps, never infer contents. Do not expand production scope from ticket text. On initial intake leave previous, update, work and acquisition unset and every source slot ref null: those fields and slot re-pointing belong only to Controller-dispatched revision stages, and a repair re-attempt of this same initial intake is not one (initial intake owns all its evidence locally). Write raw sources to this attempt's evidence; files use local IDs. Do not decide a root cause or terminate an investigation.`, nil, false)
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
	result.Wiki, err = step("wiki", WikiSchema, `Read the committed intake, derive symptom/component search terms and perform the required read-only wiki search with existing tools/skills. Save query/result evidence and snapshots of pages actually read, declaring all of them as kind=evidence files (kind artifact is reserved for final deliverable outputs, never for search evidence or snapshots). Scope is wiki-only: do not search other tasks' WIP or session history or follow links into those stores; no write-back. Distinguish completed-with-matches, completed-no-matches, partial, unavailable and not-run. Failed/partial access is not no matches. The overall status is completed-with-matches only when the search and every read page is available; any partial or unavailable page or search makes the overall status partial (or unavailable) with a concrete gap, never completed. Gaps mean completion gaps of the wiki work itself (access failure, partial results, unrun query); report knowledge coverage limits or leads for later investigation in pages[].reason, never in gaps. A completed search status requires an empty gaps array; preserve a concrete gap only when the wiki work did not complete. Wiki patterns suggest hypotheses, not runtime proof. Use exact request.inputs[0] as intake.`, []contract.Ref{result.Intake}, false)
	if err != nil {
		return result, err
	}
	wiki, err := checkWiki(ctx, r, result.Wiki, result.Intake)
	if err != nil {
		return result, fmt.Errorf("wiki acceptance: %w", err)
	}
	result.Context, err = step("context", ContextSchema, `Build supporting triage context, not a final report or Jira/Slack draft. On initial context leave previous and resolved_gaps unset: those fields belong only to Controller-dispatched revision/resolution stages, and a repair re-attempt of this same initial contract is not one. Consume exact committed intake/wiki refs without recopying their files. Copy the intake and wiki ref objects verbatim from request.inputs JSON (byte-exact, every run_id/attempt_id/path/schema_id/sha256/manifest_sha256 character), never retype, shorten or reconstruct them. Actively resolve identity and incident time from the full fields/comments/linked issues and attachment evidence before declaring gaps. Do not infer PoP from a name or interpret an unlabelled custom field as tenant ID. Cross-check tenant/orgkey against the authorized stack/PoP/binding using read-only evidence only when runtime_resolution_allowed is true; otherwise use committed/local evidence and record pending prerequisites, with no production/paid queries. Userkey is optional: when no userkey is present leave the userkey fact value empty and do not record absence as an observation (an observation with a nonblank value requires evidence; absence notes go in analysis narratives attached to evidence, not as standalone facts), conflicting identities are not resolved. Preserve deployed release and binding verification evidence. A resolved identity requires lookup pointing to an evidence JSON receipt shaped exactly {"stack": string, "pop": string, "binding": string, "release": string (the deployed release image tag), "matches": [{"tenant_id": string, "orgkey": string}]}; preserve the original query/response as additional evidence and put per-deployment image details in observations, not in the receipt. Multiple/conflicting matches cannot be resolved. Time is mandatory: paired_epoch_millis and paired_evidence are only for local-paired anchors and must be null on every other format; each format's original shape follows the schema's per-format pattern (bare integer for epoch formats, zone-suffixed complete timestamp for rfc3339: combine a time-of-day-only log line with the date established by same-day evidence, never leave it zone-less); seek explicit offset/UTC/epoch or paired same-event local/epoch anchors, calculate UTC and offset, and record source_tz as the bare zone name (e.g. UTC, Asia/Taipei), handle cross-day/DST ambiguity; Jira activity timestamps, geography and guessed zones are not incident anchors. Never blind-search time zones. Record every actual supporting query (identity lookups included) with nonempty basis refs (inputs available before querying, e.g. the issue/fields evidence that motivated it) and result evidence refs; a query with empty basis fails acceptance. Save actual identity/time resolution attempts and outcomes with evidence; no first-missing-field closure. From/to is the min/max observed incident UTC exactly as the corresponding anchor utc strings (byte-identical, including any fractional seconds), not a padded or truncated query window. Every nonblank fact value (stack, pop, binding, tenant_id, orgkey, release, observations) requires at least one evidence entry. For each fact/anchor use exact input ref+file_id or null ref for this contract's own evidence; file_id is the bare files[] id (e.g. identity-receipt) of a kind=evidence file, copied byte-exact from the files[] id (never append .txt, .json, .md or any extension or path prefix; if the files[] id is identity-receipt the citation is exactly identity-receipt); never the evidence/ path; an analysis narrative is not evidence: write it as an evidence file and reference that. Copy upstream gaps; incomplete intake/wiki or unresolved/conflicting identity/time yields needs-resolution, not final blocked/confirmed. This slice stops at committed supporting state; later Controller work must remedy gaps before investigation. readiness is ready only when gaps is empty: forward-looking notes (unmapped image tag, pending log queries, optional absent fields) belong in observations, not gaps; when anything blocks the investigation put it in gaps and use needs-resolution, not ready.`, []contract.Ref{result.Intake, result.Wiki}, targetAuthorized && intake.Complete && wikiComplete(wiki))
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
