package engine

import (
	"encoding/json"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

type State string

const (
	Created        State = "Created"
	Running        State = "Running"
	Preparing      State = "Preparing"
	WaitingSession State = "WaitingSession"
	Dispatching    State = "Dispatching"
	Validating     State = "Validating"
	AwaitingScope  State = "AwaitingScope"
	Finalizing     State = "Finalizing"
	Succeeded      State = "Succeeded"
	Failed         State = "Failed"
	CancelledState State = "Cancelled"
	TimedOutState  State = "TimedOut"
)

type FailureInfo struct {
	Code             Code             `json:"code"`
	Message          string           `json:"message"`
	Phase            string           `json:"phase"`
	Origin           Origin           `json:"origin"`
	LimitScope       LimitScope       `json:"limit_scope,omitempty"`
	DispatchAccepted DispatchAccepted `json:"dispatch_accepted"`
	RunID            string           `json:"run_id,omitempty"`
	StepID           string           `json:"invocation_id,omitempty"`
	AttemptID        string           `json:"attempt_id,omitempty"`
	HandleID         string           `json:"handle_id,omitempty"`
	Cause            string           `json:"cause,omitempty"`
}

func failureInfo(err error) *FailureInfo {
	if err == nil {
		return nil
	}
	f := normalize(err, "")
	info := &FailureInfo{Code: f.Code, Message: f.Message, Phase: f.Phase, Origin: f.Origin, LimitScope: f.LimitScope, DispatchAccepted: f.DispatchAccepted, RunID: f.RunID, StepID: f.StepID, AttemptID: f.AttemptID, HandleID: f.HandleID}
	if f.Cause != nil {
		info.Cause = f.Cause.Error()
	}
	return info
}

type AttemptState struct {
	Identity         contract.Identity  `json:"identity"`
	Scope            string             `json:"scope"`
	Key              string             `json:"key"`
	Epoch            string             `json:"epoch"`
	HandleID         string             `json:"handle_id"`
	Number           int                `json:"number"`
	State            State              `json:"state"`
	StartedAt        time.Time          `json:"started_at"`
	FinishedAt       time.Time          `json:"finished_at,omitempty"`
	LastSeq          uint64             `json:"last_seq"`
	DispatchAccepted DispatchAccepted   `json:"dispatch_accepted"`
	Execution        *runtime.Execution `json:"execution,omitempty"`
	Output           *contract.Ref      `json:"output,omitempty"`
	Feedback         *Feedback          `json:"feedback,omitempty"`
	Failure          *FailureInfo       `json:"failure,omitempty"`
}
type InvocationState struct {
	ID                 string   `json:"id"`
	Scope              string   `json:"scope"`
	Key                string   `json:"key"`
	Epoch              string   `json:"epoch"`
	State              State    `json:"state"`
	Provisional        State    `json:"provisional,omitempty"`
	LastAttemptID      string   `json:"last_attempt_id"`
	RetryActivationIDs []string `json:"retry_activation_ids,omitempty"`
	Attempts           int      `json:"attempts"`
	LastSeq            uint64   `json:"last_seq"`
}
type SessionStatus struct {
	ID              string           `json:"id"`
	Role            RoleSpec         `json:"role"`
	Identity        runtime.Identity `json:"identity"`
	Health          string           `json:"health"`
	State           string           `json:"state"`
	RuntimeSeq      uint64           `json:"runtime_seq"`
	ProviderRetries int              `json:"provider_retries"`
}
type RetryStatus struct {
	Scope string `json:"scope"`
	RetryState
	Active bool `json:"active"`
}
type Snapshot struct {
	Version            int                        `json:"version"`
	TaskID             string                     `json:"task_id"`
	RunID              string                     `json:"run_id"`
	Workflow           string                     `json:"workflow"`
	WorkflowVersion    string                     `json:"workflow_version"`
	ControllerVersion  string                     `json:"controller_version"`
	PiVersion          string                     `json:"pi_version"`
	Input              Input                      `json:"input"`
	Policy             RunPolicy                  `json:"policy"`
	CreatedAt          time.Time                  `json:"created_at"`
	FinishedAt         time.Time                  `json:"finished_at,omitempty"`
	State              State                      `json:"state"`
	WorkflowOutcome    State                      `json:"workflow_outcome,omitempty"`
	StatePersisted     bool                       `json:"state_persisted"`
	LastSeq            uint64                     `json:"last_seq"`
	Failure            *FailureInfo               `json:"failure,omitempty"`
	Attempts           map[string]AttemptState    `json:"attempts"`
	Invocations        map[string]InvocationState `json:"invocations"`
	Sessions           map[string]SessionStatus   `json:"sessions"`
	Retries            map[string]RetryStatus     `json:"retries"`
	FinalizationErrors []*FailureInfo             `json:"finalization_errors,omitempty"`
	CleanupErrors      []*FailureInfo             `json:"cleanup_errors,omitempty"`
}
type Event struct {
	Version int       `json:"version"`
	Seq     uint64    `json:"seq"`
	Time    time.Time `json:"time"`
	RunID   string    `json:"run_id"`
	Kind    string    `json:"kind"`
	Details any       `json:"details,omitempty"`
}

// FinalDelivery is display metadata resolved before Store closure, not another
// publication capability. Session state is read from the final cleanup snapshot.
type FinalDelivery struct {
	Output       string       `json:"output"`
	Ref          contract.Ref `json:"ref"`
	ArtifactPath string       `json:"artifact_path,omitempty"`
	HandleID     string       `json:"handle_id"`
	Scope        string       `json:"scope"`
	Step         string       `json:"step"`
}

type Report struct {
	Final              *FinalDelivery
	Outcome            State
	ExitCode           int
	Result             Result
	Failure            error
	Cleanup            []runtime.CleanupReport
	CleanupErrors      []error
	FinalizationErrors []error
	Snapshot           Snapshot
}

// WorkflowInput returns the immutable workflow name and input by value, without
// copying run history. Input contains only strings; mutable fields added later
// must be copied here rather than exposing run-owned data.
func (r *Run) WorkflowInput() (string, Input) {
	if r == nil {
		return "", Input{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state.Workflow, r.state.Input
}

// Snapshot returns an owned copy, including emergency status after persistence failure.
func (r *Run) Snapshot() Snapshot {
	if r == nil {
		return Snapshot{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}
func (r *Run) snapshotLocked() Snapshot {
	raw, _ := json.Marshal(r.state)
	var copy Snapshot
	_ = json.Unmarshal(raw, &copy)
	return copy
}

// Changes coalesces notifications; consumers reread Snapshot rather than replaying them.
func (r *Run) Changes() <-chan struct{} { return r.changed }
func (r *Run) notifyLocked() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}
