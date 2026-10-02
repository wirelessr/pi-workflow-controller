package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type ModelSpec struct{ Provider, ID, Thinking string }
type SessionSpec struct {
	HandleID, Name                string
	Model                         ModelSpec
	CWD, SessionDir, AppendPrompt string
}
type Dispatch struct {
	Token, Message string
	// Entries, when set, receives this dispatch's session entries for audit.
	// It never affects completion: the runtime does not wait on it, and a
	// sink that drops entries only loses audit coverage.
	Entries EntrySink
}

// EntrySink receives this dispatch's raw session entries in session order.
// The same entry can arrive again from a later source; deduplicate by id.
type EntrySink interface {
	Entries(EntryBatch)
}

type EntryBatch struct {
	Entries []json.RawMessage
	Source  EntrySource
	// TailUncertain reports that later entries may be missing because the
	// session's process exit was not confirmed or its file was unreadable.
	TailUncertain bool
}

type EntrySource string

const (
	// EntriesVerified entries passed the append-lineage checks used for
	// completion.
	EntriesVerified EntrySource = "rpc"
	// EntriesUnverified entries came from one best-effort read after a
	// failure, without lineage checks.
	EntriesUnverified EntrySource = "rpc-unverified"
	// EntriesSessionFile entries were read from the session file after the
	// process exit was confirmed.
	EntriesSessionFile EntrySource = "session-file"
)

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
	ContextUsage(context.Context) (ContextUsage, error)
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

// ContextUsage is an on-demand estimate, not provider admission evidence.
// Nil values mean unknown, including immediately after compaction, not zero.
type ContextUsage struct {
	Identity           Identity
	SampledAt          time.Time
	Seq, ActivityEpoch uint64
	Tokens             *int64
	ContextWindow      *int64
	Percent            *float64
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

// ConfirmsLocalClose compares the report with the caller's expected session ID.
// The caller owns ID validity and continuation policy; this is not evidence that
// remote jobs have stopped, and does not replace handling the Close error.
func (r CleanupReport) ConfirmsLocalClose(sessionID string) bool {
	return r.Identity.SessionID == sessionID && r.WaitCompleted && r.ProcessExited &&
		len(r.Unconfirmed) == 0 && r.WaitError == "" && r.KillError == "" && r.DiscoveryError == ""
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
