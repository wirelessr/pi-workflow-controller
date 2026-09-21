package triage

import (
	"context"
	"fmt"
	"path/filepath"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// ReportPolicy has no product defaults. The first report uses the live Planner;
// each finite retry needs a fresh session after confirmed cleanup.
type ReportPolicy struct {
	ReportRetries   int `json:"report_retries"`
	ReserveSessions int `json:"reserve_sessions"`
	ReserveAttempts int `json:"reserve_attempts"`
}

func (p ReportPolicy) check() error {
	if p.ReportRetries < 0 || p.ReportRetries == int(^uint(0)>>1) || p.ReserveSessions < p.ReportRetries || p.ReserveAttempts < p.ReportRetries+1 {
		return fmt.Errorf("explicit report reserves must cover the report and all finite fresh retries")
	}
	return nil
}

type investigationCost struct {
	Sessions int `json:"sessions"`
	Attempts int `json:"attempts"`
	Live     int `json:"additional_live"`
}

func (c *investigationCost) add(sessions, attempts int) error {
	max := int(^uint(0) >> 1)
	if sessions < 0 || attempts < 0 || sessions > max-c.Sessions || attempts > max-c.Attempts {
		return fmt.Errorf("investigation admission cost overflow")
	}
	c.Sessions += sessions
	c.Attempts += attempts
	return nil
}

type ReportBudget struct {
	Policy       ReportPolicy      `json:"policy"`
	MaxSessions  int               `json:"max_sessions"`
	MaxAttempts  int               `json:"max_attempts"`
	MaxLive      int               `json:"max_live"`
	UsedSessions int               `json:"used_sessions"`
	UsedAttempts int               `json:"used_attempts"`
	LiveSessions int               `json:"live_sessions"`
	Action       string            `json:"action"`
	Rejected     investigationCost `json:"rejected"`
	Reason       string            `json:"reason"`
}

// This entry is the Run's sole investigation dispatch coordinator. Snapshot is
// not an atomic reservation against other dispatchers. Each admitted segment
// includes all branches/replacements and the following accepted Planner update;
// another check occurs only after their join. Bytes, disk and time are not reserved.
func executeInvestigationReport(ctx context.Context, r *engine.Run, scope Scope, plannerModel runtime.ModelSpec, models sliceModels, contextRef contract.Ref, capacity *PlannerCapacityPolicy, recovery *RecoveryPolicy, verification *VerificationPolicy, policy ReportPolicy, renderer string) (engine.Result, error) {
	if err := policy.check(); err != nil {
		return engine.Result{}, err
	}
	if recovery == nil || recovery.PlannerRetries < 0 || recovery.PlannerRetries == int(^uint(0)>>1) {
		return engine.Result{}, fmt.Errorf("report investigation requires an explicit finite Planner recovery policy")
	}
	if renderer != filepath.Join(r.Dir(), "triage-report", "render_report.py") {
		return engine.Result{}, fmt.Errorf("report requires extracted run resources")
	}
	bootstrap := investigationCost{Sessions: 1 + recovery.PlannerRetries, Attempts: 1 + recovery.PlannerRetries, Live: 1}
	budget, err := reportAdmission(ctx, r, policy, "bootstrap", bootstrap)
	if err != nil {
		return engine.Result{}, err
	}
	if budget != nil {
		return engine.Result{}, fmt.Errorf("resource-limited bootstrap: no accepted investigation state for a report")
	}
	p, err := initializeInvestigation(ctx, r, scope, plannerModel, contextRef, capacity, recovery, verification)
	if err != nil {
		return engine.Result{}, err
	}
	reporting := &investigationReporting{policy: policy, renderer: renderer}
	p.reporting = reporting
	_, err = p.adapt(ctx, models)
	return reporting.result, err
}

func reportAdmission(ctx context.Context, r *engine.Run, policy ReportPolicy, action string, cost investigationCost) (*ReportBudget, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	snapshot := r.Snapshot()
	live := 0
	for _, session := range snapshot.Sessions {
		if session.State != "Closed" {
			live++
		}
	}
	fits := func(max, used, reserve, need int) bool {
		return used <= max && reserve <= max-used && need <= max-used-reserve
	}
	caps := snapshot.Policy
	if fits(caps.MaxTotalSessions, len(snapshot.Sessions), policy.ReserveSessions, cost.Sessions) && fits(caps.MaxTotalAttempts, len(snapshot.Attempts), policy.ReserveAttempts, cost.Attempts) && fits(caps.MaxLiveSessions, live, 0, cost.Live) {
		return nil, nil
	}
	return &ReportBudget{Policy: policy, MaxSessions: caps.MaxTotalSessions, MaxAttempts: caps.MaxTotalAttempts, MaxLive: caps.MaxLiveSessions, UsedSessions: len(snapshot.Sessions), UsedAttempts: len(snapshot.Attempts), LiveSessions: live, Action: action, Rejected: cost, Reason: "resource-limited: ordinary segment cannot preserve final report capacity"}, nil
}

func (p *plannerCaller) investigationCost(ctx context.Context, state PlannerState) (investigationCost, error) {
	var cost investigationCost
	retries := p.recovery.Policy.PlannerRetries
	if err := cost.add(retries, 1); err != nil {
		return cost, err
	}
	if err := cost.add(0, retries); err != nil {
		return cost, err
	}
	action := state.Ledger.Action
	if p.capacity != nil && action != "support" {
		if err := cost.add(1, 0); err != nil {
			return cost, err
		}
	}
	switch action {
	case "workers":
		records, err := newAcceptance(ctx, p.r).loadWorkerResults(p.scope, p.workerResults)
		if err != nil {
			return cost, err
		}
		n := len(readyWorkerTasks(state, records))
		cost.Live = n
		err = cost.add(n, n)
		return cost, err
	case "wiki", "reframe":
		cost.Live = 1
		err := cost.add(1, 1)
		return cost, err
	case "support":
		// The old Planner closes first. Resolve has at most wiki/context;
		// revision has intake/wiki/context. Resume may use fewer phases.
		phases := 3
		if state.SupportingWork.Kind == "resolve" {
			phases = 2
		}
		err := cost.add(phases+1, phases)
		return cost, err
	case "verify":
		request := state.VerificationRequest
		if request.Candidate != nil {
			if err := cost.add(retries, 1); err != nil {
				return cost, err
			}
			if err := cost.add(0, retries); err != nil {
				return cost, err
			}
		}
		var retained *VerificationDelivery
		if request.Claim != nil {
			for _, d := range p.verification.Deliveries {
				if d.Claim == *request.Claim {
					copy := d
					retained = &copy
				}
			}
		}
		for i, role := range p.verification.Policy.roles() {
			if retained != nil && retained.Roles[i].Result != nil {
				continue
			}
			cost.Live++
			if err := cost.add(1, 1); err != nil {
				return cost, err
			}
			if err := cost.add(role.policy.Retries, role.policy.Retries); err != nil {
				return cost, err
			}
		}
	case "plan":
	default:
		return cost, fmt.Errorf("unsupported admission action %q", action)
	}
	return cost, nil
}
