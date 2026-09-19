package triage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

type contextHistory struct {
	ref     contract.Ref
	value   Context
	sources map[contract.Ref][]file
}

// Revalidate committed lineage, never discover state by scanning candidates or
// publications. Intake revisions remain explicit links, not discovered files.
func loadContextHistory(ctx context.Context, r *engine.Run, scope Scope, ref contract.Ref) (contextHistory, error) {
	return newAcceptance(ctx, r).loadContextHistory(scope, ref)
}

func (a *acceptance) loadContextHistory(scope Scope, ref contract.Ref) (contextHistory, error) {
	h := contextHistory{sources: map[contract.Ref][]file{}}
	chain := []contract.Ref{}
	seen := map[contract.Ref]bool{}
	for {
		if seen[ref] {
			return h, fmt.Errorf("cyclic context lineage")
		}
		seen[ref] = true
		p, err := readAccepted[Context](a, ref, ContextSchema)
		if err != nil {
			return h, err
		}
		chain = append(chain, ref)
		if p.Data.Previous == nil {
			break
		}
		ref = *p.Data.Previous
	}
	for i := len(chain) - 1; i >= 0; i-- {
		ref := chain[i]
		p, err := readAccepted[Context](a, ref, ContextSchema)
		if err != nil {
			return h, err
		}
		v := p.Data
		intake, err := a.checkIntake(v.Intake, scope.Ticket)
		if err != nil {
			return h, err
		}
		wiki, err := a.checkWiki(v.Wiki, v.Intake)
		if err != nil {
			return h, err
		}
		var history []contextHistory
		if i < len(chain)-1 {
			history = append(history, h)
		}
		if _, err := a.checkContext(ref, scope, v.Intake, v.Wiki, intake, wiki, history...); err != nil {
			return h, err
		}
		for _, input := range []contract.Ref{v.Intake, v.Wiki} {
			inputFiles, err := readAccepted[json.RawMessage](a, input, input.SchemaID)
			if err != nil {
				return h, err
			}
			h.sources[input] = inputFiles.Files
		}
		ih, err := a.loadIntakeHistory(v.Intake, scope.Ticket)
		if err != nil {
			return h, err
		}
		for input, files := range ih.sources {
			h.sources[input] = files
		}
		h.sources[ref] = p.Files
		h.ref, h.value = ref, v
	}
	return h, nil
}

// Bind self-owned evidence before carrying a prior fact to a fresh contract.
// The JSON copy prevents qualification from mutating the accepted input slices.
func qualifiedContext(v Context, ref contract.Ref) Context {
	raw, _ := json.Marshal(v)
	var out Context
	_ = json.Unmarshal(raw, &out)
	bind := func(e *Evidence) {
		if e != nil && e.Ref == nil {
			e.Ref = &ref
		}
	}
	fact := func(f *Fact) {
		for i := range f.Evidence {
			bind(&f.Evidence[i])
		}
	}
	i := &out.Identity
	bind(i.Lookup)
	for _, f := range []*Fact{&i.Stack, &i.Pop, &i.Binding, &i.TenantID, &i.OrgKey, &i.UserKey, &i.Release} {
		fact(f)
	}
	for n := range out.Time.Anchors {
		bind(&out.Time.Anchors[n].Evidence)
		bind(out.Time.Anchors[n].PairedEvidence)
	}
	for n := range out.Observations {
		fact(&out.Observations[n])
	}
	for n := range out.ResolvedGaps {
		fact(&out.ResolvedGaps[n])
	}
	for n := range out.Attempts {
		for j := range out.Attempts[n].Evidence {
			bind(&out.Attempts[n].Evidence[j])
		}
		for j := range out.Attempts[n].Queries {
			q := &out.Attempts[n].Queries[j]
			for k := range q.Basis {
				bind(&q.Basis[k])
			}
			for k := range q.Evidence {
				bind(&q.Evidence[k])
			}
		}
	}
	return out
}

func checkRevision(v Context, h contextHistory, fact func(Fact) error, wikiRef contract.Ref) error {
	old := qualifiedContext(h.value, h.ref)
	if v.Problem != old.Problem {
		return fmt.Errorf("local resolution changed problem")
	}
	// Cross-intake applicability is the agent's work. The prior contract stays
	// committed history; same-intake remediation retains its existing rules.
	if v.Intake == old.Intake {
		if old.Identity.Status == "resolved" && !reflect.DeepEqual(v.Identity, old.Identity) {
			return fmt.Errorf("local resolution replaced valid identity")
		}
		if old.Time.Status == "resolved" && !reflect.DeepEqual(v.Time, old.Time) {
			return fmt.Errorf("local resolution replaced valid incident anchors")
		}
		for _, observation := range old.Observations {
			if !slices.ContainsFunc(v.Observations, func(f Fact) bool { return reflect.DeepEqual(f, observation) }) {
				return fmt.Errorf("context revision dropped prior observation")
			}
		}
	}
	for _, attempt := range old.Attempts {
		if !slices.ContainsFunc(v.Attempts, func(a ResolutionAttempt) bool {
			// The qualified JSON copy omits empty optional queries. An agent's
			// explicit [] represents the same history as an omitted field.
			if len(a.Queries) == 0 {
				a.Queries = nil
			}
			return reflect.DeepEqual(a, attempt)
		}) {
			return fmt.Errorf("context revision dropped resolution history")
		}
	}
	resolved := map[string]bool{}
	for _, f := range v.ResolvedGaps {
		if !slices.Contains(old.Gaps, f.Value) || slices.Contains(v.Gaps, f.Value) || resolved[f.Value] {
			return fmt.Errorf("invalid resolved gap")
		}
		if err := fact(f); err != nil {
			return err
		}
		fresh := false
		for _, e := range f.Evidence {
			fresh = fresh || e.Ref == nil || (wikiRef != old.Wiki && e.Ref != nil && *e.Ref == wikiRef) || (v.Intake != old.Intake && e.Ref != nil && *e.Ref == v.Intake)
		}
		if !fresh {
			return fmt.Errorf("gap resolution requires new evidence")
		}
		resolved[f.Value] = true
	}
	for _, gap := range old.Gaps {
		if !slices.Contains(v.Gaps, gap) && !resolved[gap] {
			return fmt.Errorf("context revision dropped gap without resolution evidence")
		}
	}
	return nil
}

// resolveSlice performs one Controller-designated local remediation cycle. It
// returns supporting state even if gaps remain; it is not a finalization or a
// full adaptive planner. Further calls consume the same run's budgets.
func resolveSlice(ctx context.Context, r *engine.Run, scope Scope, models sliceModels, previous ContextResult) (ContextResult, error) {
	return resolveWithProposal(ctx, r, scope, models, previous, nil, nil)
}

func resolveWithProposal(ctx context.Context, r *engine.Run, scope Scope, models sliceModels, previous ContextResult, proposal *contract.Ref, supportingSources map[contract.Ref][]file) (ContextResult, error) {
	result := previous
	h, err := loadContextHistory(ctx, r, scope, previous.Context)
	if err != nil {
		return result, err
	}
	if h.value.Intake != previous.Intake || h.value.Wiki != previous.Wiki {
		return result, fmt.Errorf("resolution inputs do not match committed context")
	}
	result.Ready = h.value.Readiness == "ready"
	if result.Ready {
		return result, nil
	}
	intake, err := checkIntake(ctx, r, previous.Intake, scope.Ticket)
	if err != nil {
		return result, err
	}
	wiki, err := checkWiki(ctx, r, previous.Wiki, previous.Intake)
	if err != nil {
		return result, err
	}
	key := "resolution-" + previous.Context.AttemptID
	if err := r.Root().Decision(ctx, key+"-dispatch", "Remedy required wiki search and unresolved local identity/time only; reuse committed intake and preserve remaining acquisition gaps", []contract.Ref{previous.Context, previous.Intake, previous.Wiki}); err != nil {
		return result, err
	}
	wikiRef := previous.Wiki
	if !wikiComplete(wiki) {
		wikiInputs := appendSourceInputs([]contract.Ref{previous.Intake, previous.Wiki, previous.Context}, h.sources)
		wikiInputs = appendSourceInputs(wikiInputs, supportingSources)
		wikiRef, err = sliceStep(ctx, r, models, key+"-wiki", stageTask{Stage: "wiki-resolution", SupportingProposal: proposal, Scope: scope, Gaps: wiki.Gaps, Previous: &previous.Context, Requirements: `Remedy the required wiki search using committed intake and previous wiki/context inputs. Perform a new read-only wiki-only search with existing tools; save raw search results and page evidence. Do not read other WIP/session history or write back. Preserve failed/partial status and diagnostics; unavailable/not-run/partial is never no matches. Bind intake to request.inputs[0]. Do not repeat Jira acquisition or expand production scope.`}, WikiSchema, wikiInputs)
		if err != nil {
			return result, err
		}
		wiki, err = checkWiki(ctx, r, wikiRef, previous.Intake)
		if err != nil {
			return result, err
		}
	}
	inputs := []contract.Ref{previous.Intake, wikiRef, previous.Context}
	// Include every retained evidence source explicitly; no implicit cross-Step
	// file access or copied ownership of another attempt's evidence.
	inputs = appendSourceInputs(inputs, h.sources)
	inputs = appendSourceInputs(inputs, supportingSources)
	kinds := []string{}
	if h.value.Identity.Status != "resolved" {
		kinds = append(kinds, "identity")
	}
	if h.value.Time.Status != "resolved" {
		kinds = append(kinds, "time")
	}
	allowed := nonblank(scope.Stack) && nonblank(scope.Pop) && nonblank(scope.Binding) && len(scope.TenantIDs) > 0 && intake.Complete && wikiComplete(wiki)
	contextRef, err := sliceStep(ctx, r, models, key+"-context", stageTask{Stage: "context-resolution", SupportingProposal: proposal, Scope: scope, Previous: &previous.Context, Gaps: h.value.Gaps, ResolutionKinds: kinds, RuntimeResolutionAllowed: allowed, Requirements: `Produce a new supporting context bound to previous=request.inputs[2], intake=request.inputs[0], wiki=request.inputs[1]. Work only on the Controller's unresolved resolution_kinds and required wiki remediation. Reuse valid committed intake, observations, identity and incident UTC anchors; do not refetch Jira or repeat resolved lookups. Retain prior observations and resolution attempts with exact input refs; qualify previous self-owned evidence with its previous contract ref, never recopy attachments. Actively seek missing identity/time evidence with existing tools and save new raw sources, metadata and outcomes to this attempt. Production/paid queries are forbidden unless runtime_resolution_allowed; ticket-only scope can still inspect committed/local evidence. Keep unresolved acquisition/wiki prerequisites in gaps, not final blocked. Every removed previous gap requires a resolved_gaps fact whose value is the exact gap and whose evidence includes a new local file or new wiki evidence; no unsupported gap deletion. Preserve original timestamp/offset/epoch calculations and target/DB receipt rules; from/to remains observed anchors, not a query window. Do not change already resolved identity/time or problem scope in this local cycle; new contradictions require the future invalidation/reframe path, not silent replacement. New evidence does not authorize a wider target. No final report, root cause, drafts or final selection.`}, ContextSchema, inputs)
	if err != nil {
		return result, err
	}
	v, err := checkContext(ctx, r, contextRef, scope, previous.Intake, wikiRef, intake, wiki, h)
	if err != nil {
		return result, err
	}
	if err := r.Root().Decision(ctx, key+"-recorded", "Local remediation recorded: "+v.Readiness+"; remaining gaps require further Controller work, not final closure", []contract.Ref{previous.Context, previous.Intake, wikiRef, contextRef}); err != nil {
		return result, err
	}
	result.Wiki, result.Context, result.Ready = wikiRef, contextRef, v.Readiness == "ready"
	return result, nil
}
