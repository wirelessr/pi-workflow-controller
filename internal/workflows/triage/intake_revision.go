package triage

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

type intakeHistory struct {
	value     Intake
	inventory *Source
	sources   map[contract.Ref][]file
}

func intakeSlots(v Intake) map[string]Source {
	slots := map[string]Source{"issue": v.Issue, "fields": v.Fields}
	for _, p := range v.Comments {
		slots["comment:"+strconv.Itoa(p.Start)] = p.Source
	}
	for _, l := range v.Linked {
		slots["linked:"+l.Key] = l.Source
	}
	for _, a := range v.Attachments {
		slots["attachment-content:"+a.ID] = a.Content
		slots["attachment-analysis:"+a.ID] = a.Analysis
	}
	return slots
}

func checkIntakeWork(v Intake, work []intakeWork) error {
	if v.Complete || len(work) == 0 {
		return fmt.Errorf("local intake revision requires incomplete intake and explicit work")
	}
	slots, seen := intakeSlots(v), map[string]bool{}
	for _, item := range work {
		if !nonblank(item.Reason) || seen[item.Source] {
			return fmt.Errorf("source work requires unique selectors and reasons")
		}
		seen[item.Source] = true
		if _, ok := slots[item.Source]; ok {
			continue
		}
		// A missing page may not yet have a manifest entry. Other inventory
		// changes require a separate, explicitly scoped acquisition cycle.
		n, err := strconv.Atoi(strings.TrimPrefix(item.Source, "comment:"))
		if err != nil || n < 0 || item.Source != "comment:"+strconv.Itoa(n) {
			return fmt.Errorf("unknown source work selector %q", item.Source)
		}
	}
	return nil
}

func retainedSource(s Source, ref contract.Ref) Source {
	if s.FileID != "" && s.Ref == nil {
		s.Ref = &ref
	}
	return s
}

func checkIntakeRevision(v, old Intake, previous contract.Ref) error {
	if v.Previous == nil || *v.Previous != previous || v.Ticket != old.Ticket || v.URL != old.URL {
		return fmt.Errorf("intake revision must bind exact previous ticket/source")
	}
	if !v.Update {
		if err := checkIntakeWork(old, v.Work); err != nil {
			return err
		}
	}
	if v.Acquisition == nil || v.Acquisition.Ref != nil || v.Acquisition.FileID == "" {
		return fmt.Errorf("intake revision requires own acquisition metadata/diagnostics")
	}
	before, after := intakeSlots(old), intakeSlots(v)
	selected := map[string]bool{}
	for _, item := range v.Work {
		if !nonblank(item.Reason) || selected[item.Source] {
			return fmt.Errorf("source work requires unique selectors and reasons")
		}
		selected[item.Source] = true
		s, ok := after[item.Source]
		if !ok && v.Update {
			if _, existed := before[item.Source]; existed {
				continue
			}
		}
		if !ok || s.Ref != nil {
			return fmt.Errorf("source work needs a new local result or an update removal: %s", item.Source)
		}
	}
	if v.Update && !selected["issue"] {
		return fmt.Errorf("intake update requires a new issue result, including failures")
	}
	for key, prior := range before {
		next, ok := after[key]
		if !ok {
			if v.Update && selected[key] {
				continue
			}
			return fmt.Errorf("intake revision dropped unrecorded source %s", key)
		}
		if !selected[key] && !reflect.DeepEqual(next, retainedSource(prior, previous)) {
			return fmt.Errorf("unselected source must retain exact prior provenance: %s", key)
		}
	}
	for key := range after {
		if _, exists := before[key]; !exists && !selected[key] {
			return fmt.Errorf("unrequested source added: %s", key)
		}
	}
	return nil
}

func loadIntakeHistory(ctx context.Context, r *engine.Run, ref contract.Ref, ticket string) (intakeHistory, error) {
	return newAcceptance(ctx, r).loadIntakeHistory(ref, ticket)
}

func (a *acceptance) loadIntakeHistory(ref contract.Ref, ticket string) (intakeHistory, error) {
	h := intakeHistory{sources: map[contract.Ref][]file{}}
	var chain []contract.Ref
	var publications []publication[Intake]
	seen := map[contract.Ref]bool{}
	for {
		if seen[ref] {
			return h, fmt.Errorf("cyclic intake lineage")
		}
		seen[ref] = true
		p, err := readAccepted[Intake](a, ref, IntakeSchema)
		if err != nil {
			return h, err
		}
		chain, publications = append(chain, ref), append(publications, p)
		if p.Data.Previous == nil {
			break
		}
		ref = *p.Data.Previous
	}
	for i := len(chain) - 1; i >= 0; i-- {
		p, ref := publications[i], chain[i]
		if i == len(chain)-1 {
			if p.Data.Update || len(p.Data.Work) != 0 {
				return h, fmt.Errorf("initial intake cannot declare revision work")
			}
			for _, s := range intakeSlots(p.Data) {
				if s.Ref != nil {
					return h, fmt.Errorf("initial intake cannot retain foreign evidence")
				}
			}
			if p.Data.Acquisition != nil && p.Data.Acquisition.Ref != nil {
				return h, fmt.Errorf("initial acquisition metadata must be local")
			}
		} else if err := checkIntakeRevision(p.Data, h.value, chain[i+1]); err != nil {
			return h, err
		}
		v, err := checkIntakePublication(a.ctx, ref, p, ticket, h.sources, h.inventory)
		if err != nil {
			return h, err
		}
		h.value, h.sources[ref] = v, p.Files
		if v.Issue.Status == "available" {
			source := retainedSource(v.Issue, ref)
			h.inventory = &source
		}
	}
	return h, nil
}

func appendSourceInputs(inputs []contract.Ref, sources map[contract.Ref][]file) []contract.Ref {
	var retained []contract.Ref
	for ref := range sources {
		if !slices.Contains(inputs, ref) {
			retained = append(retained, ref)
		}
	}
	sort.Slice(retained, func(i, j int) bool { return retained[i].AttemptID < retained[j].AttemptID })
	return append(inputs, retained...)
}

// refreshSlice performs one specified acquisition revision and dependency
// handoff, not an adaptive planner or an acquisition tool implementation.
func refreshSlice(ctx context.Context, r *engine.Run, scope Scope, models sliceModels, previous ContextResult, work []intakeWork) (ContextResult, error) {
	return reviseSlice(ctx, r, scope, models, previous, work, false, nil)
}

// updateSlice lets the agent update the ticket and its inventory within one
// intake Step. Work is a record of results, not a per-source approval request.
func updateSlice(ctx context.Context, r *engine.Run, scope Scope, models sliceModels, previous ContextResult) (ContextResult, error) {
	return reviseSlice(ctx, r, scope, models, previous, nil, true, nil)
}

func reviseSlice(ctx context.Context, r *engine.Run, scope Scope, models sliceModels, previous ContextResult, work []intakeWork, update bool, proposal *contract.Ref) (ContextResult, error) {
	result := previous
	h, err := loadContextHistory(ctx, r, scope, previous.Context)
	if err != nil {
		return result, err
	}
	if h.value.Intake != previous.Intake || h.value.Wiki != previous.Wiki {
		return result, fmt.Errorf("refresh inputs do not match committed context")
	}
	intake, err := checkIntake(ctx, r, previous.Intake, scope.Ticket)
	if err != nil {
		return result, err
	}
	if !update {
		if err := checkIntakeWork(intake, work); err != nil {
			return result, err
		}
	}
	key := "refresh-" + previous.Context.AttemptID
	decision := "Perform only specified source work; retain other exact committed sources and hand off new intake dependencies"
	if update {
		key = "update-" + previous.Context.AttemptID
		decision = "Update the intake within authorized scope; the agent handles inventory changes and records work before dependency handoff"
	}
	inputs := appendSourceInputs([]contract.Ref{previous.Intake, previous.Wiki, previous.Context}, h.sources)
	if err := r.Root().Decision(ctx, key+"-dispatch", decision, inputs); err != nil {
		return result, err
	}
	task := stageTask{Stage: "intake-revision", SupportingProposal: proposal, Scope: scope, Previous: &previous.Intake, SourceWork: work, Requirements: `Load the existing Jira/attachment skills first and use their normal scripts, CLI, REST or shell tools. Perform only source_work, not full acquisition. Selectors are issue, fields, comment:<startAt>, linked:<key>, attachment-content:<id>, attachment-analysis:<id>. Reasons describe missing or explicitly invalidated sources; do not choose additional work. Save new raw/partial responses, extraction, metadata, failures and diagnostics directly to this attempt's evidence. Produce an intake with previous=request.inputs[0], update=false and exact source_work as work. Each selected slot must contain its new local result, including missing/partial outcomes. Retain every unselected source unchanged, qualifying prior local file_id with the exact owning intake ref; do not copy old evidence into this attempt. Acquisition must reference this attempt's metadata/diagnostics. Preserve raw completeness and all remaining gaps, never treat failure as empty results. Mechanical extraction only; unperformed analysis remains a gap. Do not infer identity/time or root cause. Inventory removal/replacement beyond these slots belongs to an intake update task.`}
	if update {
		task.Stage = "intake-update"
		task.Requirements = `Load existing Jira/attachment skills and update this ticket's intake using their normal tools within the authorized scope. Obtain a new raw issue result, including partial/failure diagnostics. Handle discovered linked issues, attachments and comment pagination within this task without asking for per-source approval; do not refetch unaffected sources just to make them local. Use mechanical extraction where supported; pending analysis/vision or unavailable sources remain explicit gaps. Produce previous=request.inputs[0] and update=true. Record the acquired/changed/removed source selectors and reasons in work after doing the work; work is not a request for another dispatch. Selectors are issue, fields, comment:<startAt>, linked:<key>, attachment-content:<id>, attachment-analysis:<id>. Every acquired/changed slot has a local result including failures; removed slots leave the active inventory, not committed history. Retained slots preserve their exact source and true owner ref, not copied evidence. Save this attempt's acquisition metadata, raw/partial results and failures. Reconcile the active inventory with the raw issue; if issue retrieval fails, keep the last known inventory and the issue gap. Different IDs need no declared replacement relationship. Historical analysis remains historical, not a claim of new analysis; source updates do not automatically require new analysis. You judge applicability and record remaining work in gaps/diagnostics. Do not expand production scope, infer root cause, or dispatch other agents. Source removal does not by itself resolve an investigation gap; downstream context must explain any removed gap with evidence.`
	}
	intakeRef, err := sliceStep(ctx, r, models, key+"-intake", task, IntakeSchema, inputs)
	if err != nil {
		return result, err
	}
	ih, err := loadIntakeHistory(ctx, r, intakeRef, scope.Ticket)
	if err != nil {
		return result, err
	}
	intake = ih.value
	if intake.Previous == nil || *intake.Previous != previous.Intake || intake.Update != update || (!update && !reflect.DeepEqual(intake.Work, work)) {
		return result, fmt.Errorf("intake revision changed dispatched task/work/previous")
	}
	inputs = appendSourceInputs([]contract.Ref{intakeRef, previous.Wiki, previous.Context}, h.sources)
	inputs = appendSourceInputs(inputs, ih.sources)
	wikiRef, err := sliceStep(ctx, r, models, key+"-wiki", stageTask{Stage: "wiki-revision", SupportingProposal: proposal, Scope: scope, Previous: &previous.Context, Requirements: `Load the existing wiki skill and perform the required read-only wiki-only search for the revised intake=request.inputs[0]. Previous wiki/context and historical evidence are explicit inputs for context, not a search bound to this new intake. Choose terms and judge applicability yourself. Save this search's results and pages actually read as local evidence. Retain partial/unavailable/not-run status and gaps when incomplete; never relabel an unfinished search as no matches. Do not search other tasks' WIP/session history or write back. Do not repeat Jira acquisition.`}, WikiSchema, inputs)
	if err != nil {
		return result, err
	}
	wiki, err := checkWiki(ctx, r, wikiRef, intakeRef)
	if err != nil {
		return result, err
	}
	inputs[1] = wikiRef
	inputs = appendSourceInputs(inputs, h.sources)
	allowed := nonblank(scope.Stack) && nonblank(scope.Pop) && nonblank(scope.Binding) && len(scope.TenantIDs) > 0 && intake.Complete && wikiComplete(wiki)
	contextRef, err := sliceStep(ctx, r, models, key+"-context", stageTask{Stage: "context-revision", SupportingProposal: proposal, Scope: scope, Previous: &previous.Context, RuntimeResolutionAllowed: allowed, Requirements: `Produce supporting context with previous=request.inputs[2], intake=request.inputs[0], wiki=request.inputs[1]. Load relevant existing skills. You judge which prior information remains applicable to the revised sources; preserve valid information and exact evidence provenance, including historical wiki refs where applicable. Exact historical evidence is not a new search or new acquisition. Reassess affected identity/time/observations yourself, do not repeat unaffected acquisition/lookups. Preserve resolution attempt history with exact owner refs and record new attempts/diagnostics. Keep the problem and authorized scope unchanged. Preserve upstream gaps; each removed previous gap requires resolved_gaps evidence including a new local file, this revision's intake evidence or new wiki evidence. Keep the existing receipt and UTC calculation requirements. No production/paid resolution unless runtime_resolution_allowed. An incomplete intake/search remains needs-resolution, not no matches or final closure. No report, root cause, drafts, write-back or final selection.`}, ContextSchema, inputs)
	if err != nil {
		return result, err
	}
	v, err := checkContext(ctx, r, contextRef, scope, intakeRef, wikiRef, intake, wiki, h)
	if err != nil {
		return result, err
	}
	if err := r.Root().Decision(ctx, key+"-recorded", "Revised intake/wiki/context supporting state accepted; remaining work is not final closure", []contract.Ref{previous.Context, intakeRef, wikiRef, contextRef}); err != nil {
		return result, err
	}
	return ContextResult{Intake: intakeRef, Wiki: wikiRef, Context: contextRef, Ready: v.Readiness == "ready"}, nil
}
