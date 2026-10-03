package triagev2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
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
	// times across rounds is recorded absent with a gap.
	MaxRejections int
	// TimeoutRetries reruns a round or its fact check after a recoverable
	// failure (attempt timeout or failed compaction), in a fresh session from
	// the same committed inputs.
	TimeoutRetries int
	RoundTimeout   time.Duration
	CheckTimeout   time.Duration
	// HandoffPercent is the context usage at which the session is strictly
	// closed and the next round starts fresh.
	HandoffPercent float64
}

func (p RoundPolicy) check() error {
	if p.MaxRounds < 1 || p.MaxRejections < 1 || p.TimeoutRetries < 0 || p.RoundTimeout <= 0 || p.CheckTimeout <= 0 {
		return fmt.Errorf("round policy needs MaxRounds and MaxRejections of at least 1, TimeoutRetries of at least 0 and positive RoundTimeout and CheckTimeout")
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
// Limit says what ended a round that still wanted to continue.
type Rounds struct {
	Records    []RoundRecord
	Last       Round
	Limit      string
	HomeStack  string
	Recoveries []RecoveryFailure
}

const (
	LimitRounds = "rounds"
	LimitRun    = "run budget"
)

// ledger is the state the rounds carry: rejections per item key, items
// recorded absent with their gap, and the confirmed home stack.
type ledger struct {
	rejections map[string]int
	absent     map[string]string
	home       string
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
	book := &ledger{rejections: map[string]int{}, absent: map[string]string{}}
	var ts *TaskSession
	var feedback *engine.Feedback
	var last engine.StepResult
	for n := 1; ; n++ {
		if !roundFits(r.Snapshot(), policy) {
			out.Limit = LimitRun
			break
		}
		names := []string{skills.Entry("investigator"), skills.Entry("core"), skills.Entry("identity")}
		if n == 1 {
			names = []string{skills.Entry("identity"), skills.Entry("core")}
		}
		gate := identityOnlyRequirements
		if book.home != "" {
			gate = fmt.Sprintf(openRequirements, book.home)
		}
		t := newTask(r, "investigator", s0.Ticket, names, roundRequirements, gate, fmt.Sprintf(budgetRequirements, policy.RoundTimeout), citationRequirements)
		t.Round, t.Citable = n, slices.Clone(inputs)
		var round Round
		step := TaskStep{Model: models.Investigator, Stage: "investigator", Key: "round", Task: t, Schema: RoundSchema, Inputs: t.inputs(), Recovery: true, Timeout: policy.RoundTimeout,
			Validate: func(ctx context.Context, ref contract.Ref) error {
				var err error
				round, err = checkRound(ctx, r, ref, roundCheck{inputs: t.inputs(), absent: book.absent})
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
			if retry != nil {
				retry = &engine.Feedback{Message: fmt.Sprintf(rerunNote, retry.SourceCode) + retry.Message, SourceAttemptID: retry.SourceAttemptID, SourceCode: retry.SourceCode}
			}
			step.Scope, step.Feedback = s, joinFeedback(retry, feedback)
			res, err := ts.Run(ctx, r, step)
			var failure *TaskFailure
			if errors.As(err, &failure) {
				// Recovery closes this session before a retry; any other
				// failure ends the run, whose cleanup closes it.
				ts = nil
				return res.Output, err
			}
			if err == nil && res.Execution.SessionID != ts.Identity.SessionID {
				return res.Output, fmt.Errorf("investigator execution identity mismatch")
			}
			last = res
			return res.Output, err
		}, nil)
		out.Recoveries = append(out.Recoveries, failures...)
		if err != nil {
			return out, err
		}
		record := RoundRecord{Round: ref}
		inputs = append(inputs, LabeledRef{fmt.Sprintf("round %d", n), ref})
		feedback = nil
		var supported map[string]bool
		if len(roundJudged(round)) > 0 {
			var fails []RecoveryFailure
			record.Check, record.Status, supported, feedback, fails, err = judgeRound(ctx, r, skills, s0, models.Validator, policy, n, ref, round, book)
			out.Recoveries = append(out.Recoveries, fails...)
			if err != nil {
				return out, err
			}
			inputs = append(inputs, LabeledRef{fmt.Sprintf("round %d fact check", n), record.Check}, LabeledRef{fmt.Sprintf("round %d fact status", n), record.Status})
		}
		updateHome(book, round, supported)
		out.Records, out.Last, out.HomeStack = append(out.Records, record), round, book.home
		if err := root.Decision(ctx, fmt.Sprintf("round-%d-recorded", n), "Investigation round accepted with status "+round.Status+"; not a verified finding", []contract.Ref{ref}); err != nil {
			return out, err
		}
		if round.Status != "continue" {
			break
		}
		if n == policy.MaxRounds {
			out.Limit = LimitRounds
			break
		}
		if ts != nil {
			handoff, note, err := capacityHandoff(ctx, r, *ts, last, policy.HandoffPercent)
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
		if err := ts.Close(ctx, r, "investigator", last, true); err != nil {
			return out, err
		}
	}
	return out, nil
}

const rerunNote = "The previous attempt of this round failed (%s) and its work was not committed; queries it started may still be running remotely, so reuse what the inputs already hold and narrow expensive queries. "

// roundFits reports whether the run's session and attempt budgets still
// cover one more round in the worst case: every Step of it retried after a
// timeout and repaired once, plus its fact status record.
func roundFits(s engine.Snapshot, p RoundPolicy) bool {
	steps := 2 * (p.TimeoutRetries + 1)
	return s.Policy.MaxTotalSessions-len(s.Sessions) >= steps && s.Policy.MaxTotalAttempts-len(s.Attempts) >= 2*steps+1
}

// judgeRound has an independent check judge what a round declares, counts
// rejections per item across rounds and records an item absent with a gap
// once it reaches the limit. It returns feedback for the next round.
func judgeRound(ctx context.Context, r *engine.Run, skills Skills, s0 S0, model runtime.ModelSpec, policy RoundPolicy, n int, ref contract.Ref, round Round, book *ledger) (check, status contract.Ref, supported map[string]bool, feedback *engine.Feedback, failures []RecoveryFailure, err error) {
	items := roundJudged(round)
	citable := []LabeledRef{{"intake", s0.Intake}, {"caller prompt", s0.Prompt}, {"facts under review", ref}}
	for _, cited := range roundCitedInputs(round) {
		if !slices.Contains(task{Citable: citable}.inputs(), cited) {
			citable = append(citable, LabeledRef{"cited by the facts under review", cited})
		}
	}
	var ids []string
	for _, item := range items {
		ids = append(ids, item.id)
	}
	var fc FactCheck
	check, failures, err = RetryInputs(ctx, r, r.Root(), fmt.Sprintf("round-%d-fact-check", n), "check", policy.TimeoutRetries, func(ctx context.Context, s *engine.Scope, retry *engine.Feedback) (contract.Ref, error) {
		var cref contract.Ref
		var err error
		cref, fc, err = runFactCheck(ctx, r, skills, factCheck{Scope: s, Model: model, Ticket: s0.Ticket, Key: "fact-check", Round: n, Subject: ref, Citable: citable, IDs: ids, Recovery: true, Timeout: policy.CheckTimeout, Feedback: retry})
		return cref, err
	}, nil)
	if err != nil {
		return check, status, nil, nil, failures, err
	}
	supported = map[string]bool{}
	for _, item := range fc.Items {
		supported[item.ID] = item.Verdict == "supported"
	}
	fs := FactStatus{Facts: ref, Check: check, Gaps: []Gap{}}
	rejected := notSupported(fc)
	keys := map[string]string{}
	for _, j := range items {
		keys[j.id] = j.key
	}
	counted := map[string]bool{}
	for _, item := range rejected {
		// Items repeating a key count once per round.
		key := keys[item.ID]
		if counted[key] {
			continue
		}
		counted[key] = true
		if book.rejections[key]++; book.rejections[key] < policy.MaxRejections {
			continue
		}
		gap := absentGapID(fmt.Sprintf("not-accepted-r%d-", n), item.ID, len(fs.Gaps)+1)
		book.absent[key] = gap
		fs.Gaps = append(fs.Gaps, Gap{ID: gap, Text: fmt.Sprintf("Item %s of round %d was not accepted by the independent check %d times and is treated as absent; last verdict %s: %s", item.ID, n, book.rejections[key], item.Verdict, item.Reason)})
	}
	if status, err = r.Root().Attach(ctx, engine.AttachSpec{Key: fmt.Sprintf("round-%d-fact-status", n), Output: contract.Spec{SchemaID: FactStatusSchema}, Data: fs}); err != nil {
		return check, status, supported, nil, failures, err
	}
	if len(rejected) == 0 {
		return check, status, supported, nil, failures, nil
	}
	message := fmt.Sprintf("The independent check of round %d did not accept: %s. Do not rely on these items; correct them with evidence that states them, or leave them.", n, rejectedReasons(rejected))
	if len(fs.Gaps) > 0 {
		message += fmt.Sprintf(" %d of them reached the rejection limit and are recorded absent in the round %d fact status.", len(fs.Gaps), n)
	}
	return check, status, supported, &engine.Feedback{Message: message, Refs: []contract.Ref{ref, check, status}}, failures, nil
}

// updateHome applies a round's home stack decisions to the runtime gate. A
// supported confirmed decision opens runtime, or keeps it open, even beside
// an unconfirmed or conflict note. Without one, any unconfirmed or conflict
// home stack decision closes runtime; that needs no judgment. A rejected
// confirmation alone changes nothing.
func updateHome(book *ledger, round Round, supported map[string]bool) {
	if round.Identity == nil {
		return
	}
	confirmed, other := "", false
	for _, d := range round.Identity.Decisions {
		switch {
		case d.Fact != "home_pop":
		case d.Status == "confirmed" && supported[d.ID]:
			confirmed = *d.Value
		case d.Status != "confirmed":
			other = true
		}
	}
	switch {
	case confirmed != "":
		book.home = confirmed
	case other:
		book.home = ""
	}
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
func capacityHandoff(ctx context.Context, r *engine.Run, ts TaskSession, last engine.StepResult, percent float64) (bool, string, error) {
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
	if err := ts.Close(ctx, r, "investigator", last, true); err != nil {
		return false, "", err
	}
	return true, fmt.Sprintf("The previous session reached %.0f%% context usage and was closed; this round starts in a fresh session with only the inputs.", *usage.Percent), nil
}

// joinFeedback puts the newer note first and keeps both sets of refs.
func joinFeedback(first, second *engine.Feedback) *engine.Feedback {
	switch {
	case first == nil || first.Message == "":
		return second
	case second == nil:
		return first
	}
	return &engine.Feedback{Message: first.Message + "\n\n" + second.Message, Refs: append(slices.Clone(first.Refs), second.Refs...), SourceAttemptID: first.SourceAttemptID, SourceCode: first.SourceCode}
}
