package codechange

import (
	"cmp"
	"context"
	"errors"
	"fmt"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

// finish follows the rounds d ended: the final snapshot and the summary,
// which is the Final.
//
// Nothing follows rounds the run can no longer act on, cancelled, past its
// deadline or stopped by a fatal error, or rounds that ended on a receipt or
// identity check the worktree or GitHub did not confirm: d's result is
// returned with d's error, or with the run's cause when d has none, so that
// rounds that converged do not return as a success without the final
// snapshot and the summary. Otherwise the final snapshot follows; a
// snapshot that cannot be taken is recorded in the summary as unavailable.
// A summary that cannot be attached leaves d's result as it was.
//
// The error is d's, with the final snapshot's added to its message, or the
// final snapshot's alone when d has none; then the summary's.
func (p prepared) finish(ctx context.Context, r *engine.Run, d roundsEnd, sp statePolicy) (engine.Result, error) {
	if ctx.Err() != nil || identityFailure(d.err) {
		return d.result, cmp.Or(d.err, context.Cause(ctx))
	}
	result, unavailable := p.finalState(ctx, r, d.result, sp)
	err := d.err
	if unavailable != nil {
		if err == nil {
			err = unavailable
		} else {
			err = fmt.Errorf("%w; the final snapshot: %v", err, unavailable)
		}
		if ctx.Err() != nil {
			return result, err
		}
	}
	ref, sErr := p.summarize(ctx, r, d, result.Outputs, unavailable, err)
	if sErr != nil {
		if err != nil {
			return result, fmt.Errorf("%w; %w", err, sErr)
		}
		return result, sErr
	}
	result.Outputs["summary"] = ref
	return engine.Result{Outputs: result.Outputs, Final: &engine.FinalSelection{Output: "summary"}}, err
}

// identityFailure reports whether err is a receipt or identity check that
// the worktree or GitHub did not confirm: the pull request is no longer
// written to.
func identityFailure(err error) bool {
	return errors.Is(err, errReceipt) || errors.Is(err, errUnverifiable)
}

// summaryRecord is the final summary: what the Controller observed and what
// the Agents reported, side by side, every value copied from a committed
// Ref. Nothing in it is judged, counted or reconciled.
type summaryRecord struct {
	Failure       string             `json:"failure,omitempty"`
	RoundsEnd     string             `json:"rounds_end"`
	PullRequest   pullRequestSummary `json:"pull_request"`
	Reports       []reportSummary    `json:"reports"`
	FinalHead     string             `json:"final_head"`
	Statements    []string           `json:"statements"`
	Rounds        []roundSummary     `json:"rounds"`
	Claim         *claimSummary      `json:"claim"`
	FinalSnapshot finalSummary       `json:"final_snapshot"`
}

type pullRequestSummary struct {
	Ref          contract.Ref `json:"ref"`
	URL          string       `json:"url"`
	Number       int          `json:"number"`
	Branch       string       `json:"branch"`
	ReportedHead string       `json:"reported_head"`
	HeadSHA      string       `json:"head_sha"`
}

// reportSummary is an accepted report and the head it is for.
type reportSummary struct {
	Output string       `json:"output"`
	Ref    contract.Ref `json:"ref"`
	Head   string       `json:"head"`
}

type roundSummary struct {
	Round           int           `json:"round"`
	Snapshot        *contract.Ref `json:"snapshot"`
	Report          *contract.Ref `json:"report"`
	ExternalObjects *contract.Ref `json:"external_objects"`
}

type claimSummary struct {
	Output      string      `json:"output"`
	Converged   bool        `json:"converged"`
	Unconverged []string    `json:"unconverged,omitempty"`
	Items       []itemClaim `json:"items"`
}

type finalSummary struct {
	Status            string            `json:"status"`
	Detail            string            `json:"detail,omitempty"`
	Ref               *contract.Ref     `json:"ref,omitempty"`
	HeadSHA           string            `json:"head_sha,omitempty"`
	CheckRuns         []checkRunSummary `json:"check_runs"`
	Statuses          []statusSummary   `json:"statuses"`
	UnresolvedThreads []string          `json:"unresolved_threads"`
}

type checkRunSummary struct {
	Name       string  `json:"name"`
	Status     string  `json:"status"`
	Conclusion *string `json:"conclusion"`
}

type statusSummary struct {
	Context string `json:"context"`
	State   string `json:"state"`
}

// summarize attaches the summary of the run that d's rounds leave in
// outputs, with failure, the error the run fails with, and unavailable, why
// the final snapshot is missing.
//
// It decodes each committed Ref it cites. The reports are the last of each
// stage that was accepted, in order, each with the head it is for: the
// implement, correction and fix contracts, the deep review report with the
// pull request record's reported head, the head the review pinned it to,
// and the last accepted converge contract, whose claim the summary copies.
// The final head is the last report's head, the one the Controller verified
// last. Each round's own Refs are in its round entry. Exactly one fact gets
// a statement: a final snapshot showing another head.
func (p prepared) summarize(ctx context.Context, r *engine.Run, d roundsEnd, outputs map[string]contract.Ref, unavailable, failure error) (contract.Ref, error) {
	record := summaryRecord{RoundsEnd: d.kind, Reports: []reportSummary{}, Statements: []string{}, Rounds: []roundSummary{}}
	if failure != nil {
		record.Failure = failure.Error()
	}
	pr, err := engine.Decode[pullRequestRecord](ctx, r, outputs["pull-request"])
	if err != nil {
		return contract.Ref{}, err
	}
	record.PullRequest = pullRequestSummary{Ref: outputs["pull-request"], URL: pr.URL, Number: pr.Number, Branch: pr.Branch, ReportedHead: pr.ReportedHead, HeadSHA: pr.HeadSHA}
	add := func(output, head string) {
		record.Reports = append(record.Reports, reportSummary{Output: output, Ref: outputs[output], Head: head})
	}
	implemented, err := engine.Decode[implementation](ctx, r, outputs["implement"])
	if err != nil {
		return contract.Ref{}, err
	}
	add("implement", implemented.LocalHead)
	if ref, ok := outputs["publish"]; ok {
		v, err := engine.Decode[published](ctx, r, ref)
		if err != nil {
			return contract.Ref{}, err
		}
		add("publish", v.Head)
	}
	add(deepReviewName+"-report", pr.ReportedHead)
	verified, err := engine.Decode[fixed](ctx, r, outputs["fix"])
	if err != nil {
		return contract.Ref{}, err
	}
	add("fix", verified.Head)
	for n := 1; n <= d.started; n++ {
		name := roundName(n)
		round := roundSummary{Round: n, Snapshot: refOf(outputs, name+"-snapshot"), ExternalObjects: refOf(outputs, name+"-external-objects")}
		if n <= d.accepted {
			round.Report = refOf(outputs, name+"-converge")
		}
		record.Rounds = append(record.Rounds, round)
	}
	if d.accepted > 0 {
		key := reportKey(d.accepted)
		claim, err := engine.Decode[convergeReport](ctx, r, outputs[key])
		if err != nil {
			return contract.Ref{}, err
		}
		add(key, claim.Head)
		record.Claim = &claimSummary{Output: key, Converged: claim.Converged, Unconverged: claim.Unconverged, Items: claim.Items}
	}
	record.FinalHead = record.Reports[len(record.Reports)-1].Head
	record.FinalSnapshot = finalSummary{CheckRuns: []checkRunSummary{}, Statuses: []statusSummary{}, UnresolvedThreads: []string{}}
	if ref := refOf(outputs, "final-snapshot"); ref == nil {
		record.FinalSnapshot.Status, record.FinalSnapshot.Detail = "unavailable", unavailable.Error()
	} else {
		state, err := engine.Decode[prStateRecord](ctx, r, *ref)
		if err != nil {
			return contract.Ref{}, err
		}
		record.FinalSnapshot.Status, record.FinalSnapshot.Ref, record.FinalSnapshot.HeadSHA = "recorded", ref, state.PullRequest.HeadSHA
		for _, run := range state.CheckRuns {
			record.FinalSnapshot.CheckRuns = append(record.FinalSnapshot.CheckRuns, checkRunSummary{Name: run.Name, Status: run.Status, Conclusion: run.Conclusion})
		}
		for _, s := range state.Statuses {
			record.FinalSnapshot.Statuses = append(record.FinalSnapshot.Statuses, statusSummary{Context: s.Context, State: s.State})
		}
		for _, t := range state.UnresolvedThreads {
			record.FinalSnapshot.UnresolvedThreads = append(record.FinalSnapshot.UnresolvedThreads, t.ID)
		}
		if state.PullRequest.HeadSHA != record.FinalHead {
			record.Statements = append(record.Statements, fmt.Sprintf("The final snapshot shows the pull request at head %s, not the final head %s", state.PullRequest.HeadSHA, record.FinalHead))
		}
	}
	return r.Root().Attach(ctx, engine.AttachSpec{Key: "summary", Output: contract.Spec{SchemaID: SummarySchema}, Data: record})
}

// refOf is the Ref outputs names key, nil when there is none.
func refOf(outputs map[string]contract.Ref, key string) *contract.Ref {
	if ref, ok := outputs[key]; ok {
		return &ref
	}
	return nil
}
