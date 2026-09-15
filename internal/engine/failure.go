package engine

import (
	"context"
	"errors"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

type Failure = runtime.Failure
type Code = runtime.Code
type Origin = runtime.Origin
type LimitScope = runtime.LimitScope
type DispatchAccepted = runtime.DispatchAccepted

const (
	WorkflowFailed       Code = "WorkflowFailed"
	Cancelled                 = runtime.Cancelled
	TimedOut                  = runtime.TimedOut
	ProviderFailed            = runtime.ProviderFailed
	OutputTruncated           = runtime.OutputTruncated
	CompactionFailed          = runtime.CompactionFailed
	InteractionRequired       = runtime.InteractionRequired
	ExtensionFailed           = runtime.ExtensionFailed
	ModelChanged              = runtime.ModelChanged
	ThinkingChanged           = runtime.ThinkingChanged
	SessionChanged            = runtime.SessionChanged
	PromptNotObserved         = runtime.PromptNotObserved
	AmbiguousExecution        = runtime.AmbiguousExecution
	ProtocolFailed            = runtime.ProtocolFailed
	RPCUnresponsive           = runtime.RPCUnresponsive
	ProcessExited             = runtime.ProcessExited
	DispatchRejected          = runtime.DispatchRejected
	ContractMissing           = runtime.ContractMissing
	ContractInvalid           = runtime.ContractInvalid
	IdentityMismatch          = runtime.IdentityMismatch
	ReferenceInvalid          = runtime.ReferenceInvalid
	InvalidDefinition         = runtime.InvalidDefinition
	SessionBusy               = runtime.SessionBusy
	StartFailed               = runtime.StartFailed
	BridgeUnavailable         = runtime.BridgeUnavailable
	UnsupportedPiVersion      = runtime.UnsupportedPiVersion
	RetryExhausted            = runtime.RetryExhausted
	StorageFailed             = runtime.StorageFailed
	JournalFailed             = runtime.JournalFailed
	LimitExceeded             = runtime.LimitExceeded
	CleanupFailed             = runtime.CleanupFailed
	FinalizationFailed        = runtime.FinalizationFailed

	AcceptedUnknown = runtime.AcceptedUnknown
	AcceptedYes     = runtime.AcceptedYes
	AcceptedNo      = runtime.AcceptedNo

	OriginControllerUser     = runtime.ControllerUser
	OriginSignalINT          = runtime.SignalINT
	OriginSignalTERM         = runtime.SignalTERM
	OriginExternalAgentAbort = runtime.ExternalAgentAbort
	OriginRunDeadline        = runtime.RunDeadline
	OriginAttemptDeadline    = runtime.AttemptDeadline
	OriginFailFastSibling    = runtime.FailFastSibling
	OriginProtocol           = runtime.Protocol
	OriginProvider           = runtime.Provider
	OriginCompaction         = runtime.Compaction
	OriginContract           = runtime.Contract
	OriginStorage            = runtime.Storage
	OriginDefinition         = runtime.Definition
)

func newFailure(code Code, phase, message string) *Failure {
	return &Failure{
		Code:             code,
		Message:          message,
		Phase:            phase,
		Origin:           OriginDefinition,
		DispatchAccepted: AcceptedNo,
	}
}

// Select the outermost classified boundary, not the first type anywhere in
// the chain. A storage wrapper must not be downgraded by its timeout cause.
func classified(err error) error {
	switch e := err.(type) {
	case *Failure, *contract.Error:
		return err
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			if found := classified(child); found != nil {
				return found
			}
		}
	case interface{ Unwrap() error }:
		return classified(e.Unwrap())
	}
	return nil
}
func normalize(err error, phase string) *Failure {
	if err == nil {
		return nil
	}
	if f, ok := classified(err).(*Failure); ok {
		// Copy before retaining the original chain, which may already contain f.
		copy := *f
		copy.Cause = err
		if copy.Phase == "" {
			copy.Phase = phase
		}
		return &copy
	}
	if c, ok := classified(err).(*contract.Error); ok {
		f := newFailure(Code(c.Code), c.Phase, c.Message)
		if f.Phase == "" {
			f.Phase = phase
		}
		f.RunID = c.Identity.RunID
		f.StepID = c.Identity.InvocationID
		f.AttemptID = c.Identity.AttemptID
		f.Cause = err
		f.Origin = OriginContract
		switch c.Code {
		case contract.InvalidDefinition:
			f.Origin = OriginDefinition
		case contract.StorageFailed:
			f.Origin = OriginStorage
		case contract.LimitExceeded:
			f.LimitScope = "attempt"
		}
		return f
	}
	f := newFailure(WorkflowFailed, phase, err.Error())
	f.Cause = err
	return f
}

type attemptContextKey struct{}
type attemptOwner struct {
	Run      *Run
	Identity contract.Identity
	HandleID string
}

// Root failure provenance is explicit. Dispatch evidence belongs to the affected
// operation; the originating failure remains reachable through Cause.
func attemptError(ctx context.Context, err error) error {
	owner, ok := ctx.Value(attemptContextKey{}).(attemptOwner)
	if !ok || err == nil {
		return err
	}
	root := owner.Run.rootCause()
	cause := context.Cause(ctx)
	if !fatal(root) || cause == nil || !errors.Is(err, cause) {
		return err
	}
	origin := normalize(root, "")
	if origin.AttemptID == owner.Identity.AttemptID || (origin.AttemptID == "" && origin.HandleID == owner.HandleID) {
		return err
	}
	// A newly returned Store wrapper is its own I/O failure, not cancellation.
	if _, own := classified(err).(*contract.Error); own {
		return err
	}
	f := normalize(err, "attempt")
	f.Code = Cancelled
	f.Origin = origin.Origin
	f.Message = "attempt cancelled by run failure"
	f.Cause = err
	return f
}

func fatal(err error) bool {
	f := normalize(err, "")
	return f != nil && (f.Code == StorageFailed || f.Code == JournalFailed ||
		(f.Code == LimitExceeded && f.LimitScope == "run"))
}

func outcome(err error) (State, int) {
	f := normalize(err, "")
	if f == nil {
		return Succeeded, 0
	}
	switch f.Code {
	case Cancelled:
		if f.Origin == OriginSignalTERM {
			return CancelledState, 143
		}
		return CancelledState, 130
	case TimedOut:
		return TimedOutState, 1
	default:
		return Failed, 1
	}
}
