package triage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"

	"pi-workflow-controller/internal/contract"
)

// A nil policy preserves the pre-capacity caller; there is no live default.
type PlannerCapacityPolicy struct {
	HandoffPercent float64 `json:"handoff_percent"`
}

func (p PlannerCapacityPolicy) check() error {
	if math.IsNaN(p.HandoffPercent) || math.IsInf(p.HandoffPercent, 0) || p.HandoffPercent <= 0 || p.HandoffPercent > 100 {
		return fmt.Errorf("planner capacity requires an explicit finite handoff percentage in (0,100]")
	}
	return nil
}

type PlannerFeedback struct {
	After contract.Ref `json:"after"`
	Note  string       `json:"note"`
}

// Checkpoint metadata is copied into the same full snapshot as the evidence and
// planning state. It does not authorize a publication or claim crash recovery.
type PlannerCheckpoint struct {
	Policy             PlannerCapacityPolicy `json:"policy"`
	DispatchCycle      int                   `json:"dispatch_cycle"`
	CheckpointCycle    int                   `json:"checkpoint_cycle"`
	FullCheckpoint     bool                  `json:"full_checkpoint"`
	AdaptiveNote       string                `json:"adaptive_note"`
	ControllerFeedback []PlannerFeedback     `json:"controller_feedback"`
}

func checkpointCounters(v, prior PlannerState) (cycle, checkpoint int, full bool, err error) {
	if prior.Checkpoint != nil {
		cycle = prior.Checkpoint.DispatchCycle
	} else if prior.Recovery != nil {
		cycle = prior.Recovery.DispatchCycle
	}
	deliveries := 0
	if len(v.WorkerResults) > len(prior.WorkerResults) {
		deliveries++
	}
	if len(v.WikiResults) > len(prior.WikiResults) {
		deliveries++
	}
	if v.Previous != nil && v.Context != prior.Context {
		deliveries++
	}
	if v.Recovery != nil {
		suffix, e := recoverySuffix(v, prior)
		if e != nil {
			return 0, 0, false, e
		}
		deliveries = len(suffix)
	}
	verificationBatch, e := verificationSuffix(v, prior)
	if e != nil {
		return 0, 0, false, e
	}
	deliveries += len(verificationBatch)
	if cycle < 0 || deliveries > 1 || (deliveries == 1 && cycle == int(^uint(0)>>1)) {
		return 0, 0, false, fmt.Errorf("invalid planner dispatch cycle transition")
	}
	cycle += deliveries
	return cycle, cycle / 3 * 3, deliveries == 1 && cycle%3 == 0, nil
}

func checkPlannerCheckpoint(v, prior PlannerState) error {
	c := v.Checkpoint
	if c == nil && prior.Checkpoint == nil {
		return nil
	}
	if c == nil || v.Ledger == nil {
		return fmt.Errorf("planner must retain checkpoint metadata and adaptive ledger")
	}
	if err := c.Policy.check(); err != nil {
		return err
	}
	var retained []PlannerFeedback
	if prior.Checkpoint != nil {
		if c.Policy != prior.Checkpoint.Policy {
			return fmt.Errorf("planner cannot change checkpoint capacity policy")
		}
		retained = prior.Checkpoint.ControllerFeedback
	}
	cycle, checkpoint, full, err := checkpointCounters(v, prior)
	if err != nil {
		return err
	}
	if c.DispatchCycle != cycle || c.CheckpointCycle != checkpoint || c.FullCheckpoint != full {
		return fmt.Errorf("planner dispatch/checkpoint counter echo mismatch")
	}
	if len(c.ControllerFeedback) < len(retained) || !slices.Equal(c.ControllerFeedback[:len(retained)], retained) {
		return fmt.Errorf("planner cannot drop or change controller feedback")
	}
	for _, feedback := range c.ControllerFeedback[len(retained):] {
		if v.Previous == nil || feedback.After != *v.Previous || !nonblank(feedback.Note) {
			return fmt.Errorf("planner feedback must bind the preceding accepted snapshot")
		}
	}
	return nil
}

func samePlannerCheckpoint(a, b *PlannerCheckpoint) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Policy == b.Policy && a.DispatchCycle == b.DispatchCycle && a.CheckpointCycle == b.CheckpointCycle &&
		a.FullCheckpoint == b.FullCheckpoint && a.AdaptiveNote == b.AdaptiveNote && slices.Equal(a.ControllerFeedback, b.ControllerFeedback)
}

func (p *plannerCaller) checkpointTask(a *acceptance) (*PlannerCheckpoint, error) {
	if p.capacity == nil {
		return nil, nil
	}
	var prior PlannerState
	if p.last != nil {
		state, err := readAccepted[PlannerState](a, *p.last, PlannerSchema)
		if err != nil {
			return nil, err
		}
		prior = state.Data
	}
	v := PlannerState{Context: p.history.ref, Previous: p.last, WorkerResults: p.workerResults, WikiResults: p.wikiResults, Recovery: p.recovery, Verification: p.verification}
	cycle, checkpoint, full, err := checkpointCounters(v, prior)
	if err != nil {
		return nil, err
	}
	feedback := []PlannerFeedback{}
	if prior.Checkpoint != nil {
		feedback = append(feedback, prior.Checkpoint.ControllerFeedback...)
	}
	feedback = append(feedback, p.pendingFeedback...)
	return &PlannerCheckpoint{Policy: *p.capacity, DispatchCycle: cycle, CheckpointCycle: checkpoint, FullCheckpoint: full, AdaptiveNote: p.adaptiveNote, ControllerFeedback: feedback}, nil
}

// Called only after strict close by handoff/support. Pending diagnostics have
// not yet reached a Planner Step and must not disappear at this session boundary.
func (p *plannerCaller) reopen(ctx context.Context, contextRef contract.Ref) (*plannerCaller, error) {
	if err := p.checkPendingClaims(ctx); err != nil {
		return nil, err
	}
	next, err := openPlannerWithEvidence(ctx, p.r, p.scope, p.model, contextRef, p.last, p.workerResults, p.wikiResults)
	if err != nil {
		return nil, err
	}
	if p.capacity != nil {
		policy := *p.capacity
		next.capacity = &policy
		next.adaptiveNote = p.adaptiveNote
		next.pendingFeedback = slices.Clone(p.pendingFeedback)
	}
	next.adaptive = p.adaptive
	next.adaptiveNote = p.adaptiveNote
	if p.recovery != nil && next.identity.SessionID == "" {
		next.identity, err = p.r.SessionIdentity(ctx, next.handle)
		if err != nil {
			return nil, err
		}
	}
	next.recovery = p.recoveryTask()
	next.verification = p.verificationTask()
	next.recoveryErrors = slices.Clone(p.recoveryErrors)
	return next, nil
}

// The accepted action still runs in this iteration, even after a handoff. This
// is not an extra Planner Step, a polling loop or a reusable capacity sample.
func (p *plannerCaller) capacityHandoff(ctx context.Context) (*plannerCaller, error) {
	if p.capacity == nil {
		return p, nil
	}
	usage, err := p.r.SessionContextUsage(ctx, p.handle)
	if err != nil {
		// SessionContextUsage already marks the handle unusable and closes it.
		p.stopped = true
		return nil, err
	}
	high := usage.Percent != nil && *usage.Percent >= p.capacity.HandoffPercent
	if usage.Percent != nil && !high {
		return p, nil
	}
	sample, err := json.Marshal(usage)
	if err != nil {
		p.stopped = true
		return nil, err
	}
	note := "Context usage percent is unknown; continue without treating it as zero. On-demand sample: "
	if high {
		note = "Context usage reached the caller's handoff threshold; continue from committed state after strict close. On-demand sample: "
	}
	p.pendingFeedback = append(p.pendingFeedback, PlannerFeedback{After: *p.last, Note: note + string(sample)})
	if high {
		return p.handoff(ctx)
	}
	return p, nil
}
