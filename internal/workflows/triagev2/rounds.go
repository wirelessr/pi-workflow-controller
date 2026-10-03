package triagev2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const roundRequirements = `You are the investigator for one round: report it through the round contract and end it at a decision point. Rebuild state from the inputs, not memory. Facts, time anchors and identity decisions you declare are judged by an independent check before later rounds may rely on them; the inputs hold each earlier check and the items recorded absent.`

const identityOnlyRequirements = `Runtime access this round: identity-only. The home stack is not confirmed, so the only runtime action allowed is a read-only identity lookup on every known stack; run no other runtime, log or metric query. Code, documents and the ticket may still be read.`

const openRequirements = `Runtime access this round: open, read-only, with home stack %q as confirmed by an earlier round and its check. Do not change anything in any system.`

const budgetRequirements = `Round time budget: %s. End the round before it; work after the deadline is lost.`

// RoundModels binds the investigator and the independent fact check.
type RoundModels struct {
	Investigator, Validator runtime.ModelSpec
}

// RoundPolicy bounds the rounds; the workflow definition names every value.
type RoundPolicy struct {
	// MaxRounds is Rmax and includes the identity round.
	MaxRounds int
	// MaxRejections is Fmax: an item the check does not accept this many
	// times is recorded absent with a gap.
	MaxRejections int
	// TimeoutRetries reruns a round that timed out, in a fresh session from
	// the same committed inputs.
	TimeoutRetries int
	RoundTimeout   time.Duration
	// HandoffPercent is the context usage at which the session is strictly
	// closed and the next round starts fresh.
	HandoffPercent float64
}

func (p RoundPolicy) check() error {
	if p.MaxRounds < 1 || p.MaxRejections < 1 || p.TimeoutRetries < 0 || p.RoundTimeout <= 0 {
		return fmt.Errorf("round policy needs MaxRounds and MaxRejections of at least 1, TimeoutRetries of at least 0 and a positive RoundTimeout")
	}
	if math.IsNaN(p.HandoffPercent) || p.HandoffPercent <= 0 || p.HandoffPercent > 100 {
		return fmt.Errorf("round policy needs a HandoffPercent in (0,100]")
	}
	return nil
}

// RoundRecord is one accepted round with its check and fact status; Check
// and Status are zero when the round declared nothing to judge.
type RoundRecord struct {
	Round, Check, Status contract.Ref
}

// Rounds is the outcome of the round loop. Until the steward and
// verification stages exist, a candidate, stuck or blocked round ends it.
type Rounds struct {
	Records    []RoundRecord
	Last       Round
	Exhausted  bool
	HomeStack  string
	Recoveries []RecoveryFailure
}

// runRounds runs investigation rounds from the S0 outputs. A session is
// reused while its context usage stays below the handoff threshold; a
// timed-out round reruns in a fresh session from the same inputs.
func runRounds(ctx context.Context, r *engine.Run, skills Skills, s0 S0, models RoundModels, policy RoundPolicy) (Rounds, error) {
	var out Rounds
	if err := policy.check(); err != nil {
		return out, err
	}
	root := r.Root()
	inputs := []LabeledRef{{"caller prompt", s0.Prompt}, {"intake", s0.Intake}, {"facts", s0.Facts}, {"fact check", s0.Check}, {"fact status", s0.Status}}
	rejections, absent := map[string]int{}, map[string]string{}
	var ts *TaskSession
	var feedback *engine.Feedback
	for n := 1; n <= policy.MaxRounds; n++ {
		names := []string{skills.Entry("investigator"), skills.Entry("core"), skills.Entry("identity")}
		if n == 1 {
			names = []string{skills.Entry("identity"), skills.Entry("core")}
		}
		runtimeAccess, gate := "identity-only", identityOnlyRequirements
		if out.HomeStack != "" {
			runtimeAccess, gate = "open", fmt.Sprintf(openRequirements, out.HomeStack)
		}
		t := newTask(r, "investigator", s0.Ticket, names, roundRequirements, gate, fmt.Sprintf(budgetRequirements, policy.RoundTimeout), citationRequirements)
		t.Round, t.Runtime, t.Citable = n, runtimeAccess, slices.Clone(inputs)
		var round Round
		step := TaskStep{Model: models.Investigator, Stage: "investigator", Key: "round", Task: t, Schema: RoundSchema, Inputs: t.inputs(), Recovery: true, Timeout: policy.RoundTimeout,
			Validate: func(ctx context.Context, ref contract.Ref) error {
				var err error
				round, err = checkRound(ctx, r, ref, roundCheck{inputs: t.inputs(), absent: absent})
				return err
			}}
		ref, failures, err := RetryInputs(ctx, r, root, fmt.Sprintf("round-%d", n), "round", policy.TimeoutRetries, func(ctx context.Context, s *engine.Scope, retry *engine.Feedback) (contract.Ref, error) {
			if ts == nil {
				opened, err := OpenTaskSession(ctx, r, step)
				if err != nil {
					return contract.Ref{}, err
				}
				ts = &opened
			}
			step.Scope, step.Feedback = s, joinFeedback(retry, feedback)
			res, err := ts.Run(ctx, r, step)
			var failure *TaskFailure
			if errors.As(err, &failure) {
				// Recovery closes this session; a retry opens a fresh one.
				ts = nil
			}
			return res.Output, err
		}, nil)
		out.Recoveries = append(out.Recoveries, failures...)
		if err != nil {
			return out, err
		}
		record := RoundRecord{Round: ref}
		inputs = append(inputs, LabeledRef{fmt.Sprintf("round %d", n), ref})
		feedback = nil
		if ids := roundJudged(round); len(ids) > 0 {
			var notes []string
			record.Check, record.Status, notes, err = judgeRound(ctx, r, s0, skills, models.Validator, n, ref, round, ids, policy.MaxRejections, rejections, absent, &out.HomeStack)
			if err != nil {
				return out, err
			}
			inputs = append(inputs, LabeledRef{fmt.Sprintf("round %d fact check", n), record.Check}, LabeledRef{fmt.Sprintf("round %d fact status", n), record.Status})
			if len(notes) > 0 {
				feedback = &engine.Feedback{Message: strings.Join(notes, " "), Refs: []contract.Ref{ref, record.Check, record.Status}}
			}
		}
		out.Records, out.Last = append(out.Records, record), round
		if err := root.Decision(ctx, fmt.Sprintf("round-%d-recorded", n), "Investigation round accepted with status "+round.Status+"; not a verified finding", []contract.Ref{ref}); err != nil {
			return out, err
		}
		if round.Status != "continue" {
			break
		}
		if n == policy.MaxRounds {
			out.Exhausted = true
			break
		}
		if ts != nil {
			handoff, note, err := capacityHandoff(ctx, r, *ts, policy.HandoffPercent)
			if err != nil {
				return out, err
			}
			if handoff {
				ts = nil
			}
			feedback = joinFeedback(&engine.Feedback{Message: note}, feedback)
		}
	}
	if ts != nil {
		if err := closeTaskSession(ctx, r, *ts, "investigator"); err != nil {
			return out, err
		}
	}
	return out, nil
}

// judgeRound has an independent check judge the items a round declares,
// counts rejections per item across rounds and records an item absent with
// a gap once it reaches the limit. A supported home-stack decision sets or
// clears the runtime gate.
func judgeRound(ctx context.Context, r *engine.Run, s0 S0, skills Skills, model runtime.ModelSpec, n int, ref contract.Ref, round Round, ids []string, limit int, rejections map[string]int, absent map[string]string, home *string) (check, status contract.Ref, notes []string, err error) {
	inputs := []contract.Ref{s0.Intake, s0.Prompt, ref}
	vt := newTask(r, "fact-check", s0.Ticket, []string{skills.Entry("validator")}, factCheckRequirements, citationRequirements)
	vt.Round = n
	vt.Citable = []LabeledRef{{"intake", s0.Intake}, {"caller prompt", s0.Prompt}, {"facts under review", ref}}
	for _, cited := range roundCitedInputs(round) {
		if !slices.Contains(inputs, cited) {
			inputs = append(inputs, cited)
			vt.Citable = append(vt.Citable, LabeledRef{"cited by the facts under review", cited})
		}
	}
	var fc FactCheck
	check, err = RunTaskStep(ctx, r, TaskStep{Scope: r.Root(), Model: model, Stage: "fact-check", Key: fmt.Sprintf("round-%d-fact-check", n), Task: vt, Schema: FactCheckSchema, Inputs: vt.inputs(),
		Validate: func(ctx context.Context, cref contract.Ref) error {
			var err error
			fc, err = checkFactCheck(ctx, r, cref, ref, inputs, ids)
			return err
		}})
	if err != nil {
		return check, status, nil, err
	}
	verdicts := map[string]FactVerdict{}
	for _, item := range fc.Items {
		verdicts[item.ID] = item
	}
	keys := map[string]string{}
	for _, f := range round.FactsUpdate {
		keys[f.ID] = factKey(f)
	}
	for _, a := range round.TimeAnchors {
		keys[a.ID] = anchorKey(a)
	}
	var decisions []Decision
	if round.Identity != nil {
		decisions = round.Identity.Decisions
	}
	for _, d := range decisions {
		keys[d.ID] = decisionKey(d)
		if d.Fact == "home_pop" && verdicts[d.ID].Verdict == "supported" {
			*home = ""
			if d.Status == "confirmed" {
				*home = *d.Value
			}
		}
	}
	fs := FactStatus{Facts: ref, Check: check, Gaps: []Gap{}}
	var rejected []string
	for _, id := range ids {
		item := verdicts[id]
		if item.Verdict == "supported" {
			continue
		}
		rejected = append(rejected, fmt.Sprintf("%s (%s): %s", id, item.Verdict, item.Reason))
		key := keys[id]
		if rejections[key]++; rejections[key] < limit {
			continue
		}
		gap := fmt.Sprintf("not-accepted-r%d-%s", n, id)
		if len(gap) > 128 {
			gap = fmt.Sprintf("not-accepted-r%d-%d", n, len(fs.Gaps)+1)
		}
		absent[key] = gap
		fs.Gaps = append(fs.Gaps, Gap{ID: gap, Text: fmt.Sprintf("Item %s of round %d was not accepted by the independent check %d times and is treated as absent; last verdict %s: %s", id, n, rejections[key], item.Verdict, item.Reason)})
	}
	if status, err = r.Root().Attach(ctx, engine.AttachSpec{Key: fmt.Sprintf("round-%d-fact-status", n), Output: contract.Spec{SchemaID: FactStatusSchema}, Data: fs}); err != nil {
		return check, status, nil, err
	}
	if len(rejected) > 0 {
		notes = append(notes, "The independent check of round "+fmt.Sprint(n)+" did not accept: "+strings.Join(rejected, "; ")+". Do not rely on these items; correct them with evidence that states them, or leave them.")
	}
	if len(fs.Gaps) > 0 {
		notes = append(notes, fmt.Sprintf("%d of them reached the rejection limit and are recorded absent in the round %d fact status.", len(fs.Gaps), n))
	}
	return check, status, notes, nil
}

// roundCitedInputs lists the inputs the judged items of a round cite, so
// the check can read the evidence each item rests on.
func roundCitedInputs(v Round) []contract.Ref {
	var refs []contract.Ref
	add := func(evidence ...Evidence) {
		for _, e := range evidence {
			if e.Ref != nil && !slices.Contains(refs, *e.Ref) {
				refs = append(refs, *e.Ref)
			}
		}
	}
	for _, f := range v.FactsUpdate {
		add(f.Evidence...)
	}
	for _, a := range v.TimeAnchors {
		add(a.Evidence)
		if a.PairedEvidence != nil {
			add(*a.PairedEvidence)
		}
	}
	if v.Identity != nil {
		for _, l := range v.Identity.Lookups {
			add(l.Evidence...)
		}
	}
	return refs
}

// capacityHandoff samples the session's context usage after a round. At or
// above the threshold it closes the session strictly, so the next round
// starts fresh from committed inputs; accounting is never reset. An unknown
// usage keeps the session and says so.
func capacityHandoff(ctx context.Context, r *engine.Run, ts TaskSession, percent float64) (bool, string, error) {
	usage, err := r.SessionContextUsage(ctx, ts.Handle)
	if err != nil {
		// SessionContextUsage marks the handle unusable and closes it.
		return false, "", err
	}
	if usage.Percent == nil {
		return false, "Context usage is unknown; this round continues in the same session without treating it as low.", nil
	}
	if *usage.Percent < percent {
		return false, "", nil
	}
	if err := closeTaskSession(ctx, r, ts, "investigator"); err != nil {
		return false, "", err
	}
	return true, fmt.Sprintf("The previous session reached %.0f%% context usage and was closed; this round starts in a fresh session with only the inputs.", *usage.Percent), nil
}

func closeTaskSession(ctx context.Context, r *engine.Run, ts TaskSession, stage string) error {
	closed, err := r.CloseSessionReport(ctx, ts.Handle)
	if err != nil {
		return err
	}
	if !SameIdentity(closed.Identity, ts.Identity) || !closed.ConfirmsLocalClose(ts.Identity.SessionID) {
		return fmt.Errorf("%s cleanup not confirmed", stage)
	}
	return nil
}

// joinFeedback puts the newest note first and keeps the earlier refs.
func joinFeedback(first, second *engine.Feedback) *engine.Feedback {
	switch {
	case first == nil || first.Message == "":
		return second
	case second == nil:
		return first
	}
	return &engine.Feedback{Message: first.Message + "\n\n" + second.Message, Refs: second.Refs, SourceAttemptID: first.SourceAttemptID, SourceCode: first.SourceCode}
}
