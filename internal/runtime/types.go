package runtime

import (
	"context"
	"fmt"
	"time"
)

type ModelSpec struct{ Provider, ID, Thinking string }
type SessionSpec struct {
	HandleID, Name                string
	Model                         ModelSpec
	CWD, SessionDir, AppendPrompt string
}
type Dispatch struct{ Token, Message string }
type Execution struct {
	SessionID, Token                                        string
	StartSeq, SettledSeq, ActivityEpoch                     uint64
	PromptEntryID, LastEntryID, LastAssistantID, StopReason string
	ExtraUserInputs                                         int
}
type Runtime interface {
	Start(context.Context, SessionSpec) (Session, error)
}
type Session interface {
	Identity() Identity
	Snapshot(context.Context) (SessionState, error)
	Execute(context.Context, Dispatch) (Execution, error)
	Confirm(context.Context, Execution) (Confirmation, error)
	Close(context.Context) (CleanupReport, error)
}
type Identity struct {
	HandleID, SessionID, SessionFile string
	PID, PGID, ParentPID             int
	SpawnTime                        time.Time
	Executable                       string
}
type SessionState struct {
	Identity                          Identity
	Health                            string
	Model                             ModelSpec
	Streaming, Compacting, HubVisible bool
	PendingCount                      int
	LastResponseTime                  time.Time
	Seq, ActivityEpoch                uint64
}
type Confirmation struct{ Seq, ActivityEpoch uint64 }
type CleanupReport struct {
	Identity                                                         Identity
	AbortAcknowledged, AbortBashAcknowledged, SIGKILL, WaitCompleted bool
	ProcessExited                                                    bool
	WaitError, KillError, DiscoveryError                             string
	DiscoveryRemoved                                                 []string
	Unconfirmed                                                      []string
	StderrTruncated                                                  bool
	StderrTail                                                       string
}
type Observation struct {
	DispatchToken      string
	HandleID, Kind     string
	Seq, ActivityEpoch uint64
	Time               time.Time
	Failure            *Failure
}

// Observer must return when ctx is cancelled. It runs outside the stdout reader.
type Observer func(context.Context, Observation) error

type Code string
type Origin string
type DispatchAccepted string
type LimitScope string

const (
	AcceptedUnknown    DispatchAccepted = "unknown"
	AcceptedYes        DispatchAccepted = "yes"
	AcceptedNo         DispatchAccepted = "no"
	ControllerUser     Origin           = "ControllerUser"
	SignalINT          Origin           = "SignalINT"
	SignalTERM         Origin           = "SignalTERM"
	ExternalAgentAbort Origin           = "ExternalAgentAbort"
	RunDeadline        Origin           = "RunDeadline"
	AttemptDeadline    Origin           = "AttemptDeadline"
	FailFastSibling    Origin           = "FailFastSibling"
	Protocol           Origin           = "Protocol"
	Provider           Origin           = "Provider"
	Compaction         Origin           = "Compaction"
	Contract           Origin           = "Contract"
	Storage            Origin           = "Storage"
	Definition         Origin           = "Definition"
)
const (
	Cancelled            Code = "Cancelled"
	TimedOut             Code = "TimedOut"
	ProviderFailed       Code = "ProviderFailed"
	OutputTruncated      Code = "OutputTruncated"
	CompactionFailed     Code = "CompactionFailed"
	InteractionRequired  Code = "InteractionRequired"
	ExtensionFailed      Code = "ExtensionFailed"
	ModelChanged         Code = "ModelChanged"
	ThinkingChanged      Code = "ThinkingChanged"
	SessionChanged       Code = "SessionChanged"
	PromptNotObserved    Code = "PromptNotObserved"
	AmbiguousExecution   Code = "AmbiguousExecution"
	ProtocolFailed       Code = "ProtocolFailed"
	RPCUnresponsive      Code = "RPCUnresponsive"
	ProcessExited        Code = "ProcessExited"
	DispatchRejected     Code = "DispatchRejected"
	ContractMissing      Code = "ContractMissing"
	ContractInvalid      Code = "ContractInvalid"
	IdentityMismatch     Code = "IdentityMismatch"
	ReferenceInvalid     Code = "ReferenceInvalid"
	InvalidDefinition    Code = "InvalidDefinition"
	SessionBusy          Code = "SessionBusy"
	StartFailed          Code = "StartFailed"
	BridgeUnavailable    Code = "BridgeUnavailable"
	UnsupportedPiVersion Code = "UnsupportedPiVersion"
	RetryExhausted       Code = "RetryExhausted"
	StorageFailed        Code = "StorageFailed"
	JournalFailed        Code = "JournalFailed"
	LimitExceeded        Code = "LimitExceeded"
	CleanupFailed        Code = "CleanupFailed"
	FinalizationFailed   Code = "FinalizationFailed"
)

type Failure struct {
	Code                                               Code
	Message, Phase, RunID, StepID, AttemptID, HandleID string
	Cause                                              error
	StderrTail                                         string
	Cleanup                                            *CleanupReport
	Origin                                             Origin
	LimitScope                                         LimitScope
	DispatchAccepted                                   DispatchAccepted
}

func (f *Failure) Error() string { return fmt.Sprintf("%s: %s", f.Code, f.Message) }
func (f *Failure) Unwrap() error { return f.Cause }
func failure(code Code, message string) *Failure {
	return &Failure{Code: code, Message: message, Origin: Protocol, DispatchAccepted: AcceptedUnknown}
}

type Policy struct {
	StartupTimeout, RPCTimeout, PromptAckTimeout, PromptObservationTimeout time.Duration
	AbortGrace, CleanupTimeout, HealthInterval                             time.Duration
	MaxFrameBytes, ObservationQueue, MaxStderrBytes, StderrTailBytes       int
}

func DefaultPolicy() Policy {
	return Policy{
		StartupTimeout: 120 * time.Second, RPCTimeout: 10 * time.Second,
		PromptAckTimeout: 120 * time.Second, PromptObservationTimeout: 30 * time.Second,
		AbortGrace: 5 * time.Second, CleanupTimeout: 15 * time.Second, HealthInterval: 5 * time.Second,
		MaxFrameBytes: 32 << 20, ObservationQueue: 1024, MaxStderrBytes: 8 << 20, StderrTailBytes: 64 << 10,
	}
}
func (p Policy) valid() bool {
	return p.StartupTimeout > 0 && p.RPCTimeout > 0 && p.PromptAckTimeout > 0 && p.PromptObservationTimeout > 0 && p.AbortGrace > 0 && p.CleanupTimeout > 0 && p.HealthInterval > 0 && p.MaxFrameBytes > 0 && p.ObservationQueue > 0 && p.MaxStderrBytes > 0 && p.StderrTailBytes > 0
}
