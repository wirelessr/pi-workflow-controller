package triage

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

// Native causes stay in workflow memory, never reconstructed from diagnostics
// or Snapshot.FailureInfo. Parallel consumers merge these only after joining.
type nativeRecoveryFailure struct {
	failure RecoveryFailure
	cause   error
}

func sameRecoveryFailure(a, b RecoveryFailure) bool {
	if !sameIdentity(a.Identity, b.Identity) {
		return false
	}
	a.Identity = b.Identity
	if a.Cleanup != nil && b.Cleanup != nil {
		if !sameIdentity(a.Cleanup.Identity, b.Cleanup.Identity) {
			return false
		}
		cleanup := *a.Cleanup
		cleanup.Identity = b.Cleanup.Identity
		a.Cleanup = &cleanup
	}
	return reflect.DeepEqual(a, b)
}

type investigationReporting struct {
	policy   ReportPolicy
	renderer string
	budget   *ReportBudget
	failures []RecoveryFailure
	result   engine.Result
	pending  *PendingReportError
}

// PendingReportError is not a final selection. Boundary describes how far the
// real report reached; even a committed output can fail validation or cleanup.
type PendingReportError struct {
	State              contract.Ref
	Report             *contract.Ref
	Boundary           string
	Inputs             []contract.Ref
	Recovery           *PlannerRecovery
	Verification       *PlannerVerification
	ReportFailures     []RecoveryFailure
	ControllerFeedback []PlannerFeedback
	cause              error
}

func (e *PendingReportError) Error() string { return e.cause.Error() }
func (e *PendingReportError) Unwrap() error { return e.cause }

func (p *plannerCaller) finishReport(ctx context.Context) (retErr error) {
	reporting := p.reporting
	reporting.pending = &PendingReportError{State: *p.last, Boundary: "prepared"}
	closeAttempted := false
	defer func() {
		if retErr == nil || reporting.result.Final != nil {
			return
		}
		if !closeAttempted {
			if err := p.close(ctx); err != nil {
				retErr = recoveryError(err, retErr)
			}
		}
		pending := reporting.pending
		pending.Recovery = p.recoveryTask()
		pending.Verification = p.verificationTask()
		pending.ReportFailures = slices.Clone(reporting.failures)
		pending.ControllerFeedback = slices.Clone(p.pendingFeedback)
		pending.cause = retErr
		retErr = pending
	}()
	run := func(ctx context.Context, scope *engine.Scope) (contract.Ref, error) {
		return p.reportInScope(ctx, scope, reporting.renderer)
	}
	recovered := func(ctx context.Context, failure RecoveryFailure, cause error, again bool) error {
		reporting.failures = append(reporting.failures, failure)
		p.nativeFailures = append(p.nativeFailures, nativeRecoveryFailure{failure, cause})
		if again {
			next, err := p.reopen(ctx, p.history.ref)
			if err != nil {
				return err
			}
			*p = *next
		}
		return nil
	}
	ref, _, err := retryPlannerInputs(ctx, p.r, p.r.Root(), "report-recovery-"+p.last.AttemptID, "report", reporting.policy.ReportRetries, run, recovered)
	if err != nil {
		return err
	}
	reporting.pending.Boundary = "retry-finished"
	accepted, err := readAccepted[InvestigationReport](newAcceptance(ctx, p.r), ref, ReportSchema)
	if err != nil {
		return err
	}
	var unresolved []error
	for _, disposition := range accepted.Data.M6.Dispositions {
		if disposition.Action != "unresolved" {
			continue
		}
		for _, native := range p.nativeFailures {
			if sameRecoveryFailure(native.failure, disposition.Item.Failure) {
				unresolved = append(unresolved, native.cause)
			}
		}
	}
	closeAttempted = true
	if err := p.close(ctx); err != nil {
		return recoveryError(err, unresolved...)
	}
	reporting.pending.Boundary = "closed"
	reporting.result = engine.Result{Outputs: map[string]contract.Ref{"state": *p.last, "report": ref}, Final: &engine.FinalSelection{Output: "report", FileID: ReportFileID}}
	if reporting.budget != nil {
		return recoveryError(fmt.Errorf("resource-limited investigation: %s", reporting.budget.Reason), unresolved...)
	}
	if len(unresolved) > 0 {
		return recoveryError(fmt.Errorf("investigation report retains unresolved execution failures"), unresolved...)
	}
	return nil
}
