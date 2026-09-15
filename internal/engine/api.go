// Package engine executes code-defined workflows over owned Pi sessions.
package engine

import (
	"context"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

type Input struct {
	Prompt    string
	LaunchCWD string
}

type Result struct {
	Outputs map[string]contract.Ref
	Final   *FinalSelection
}

// FinalSelection names an output explicitly; map order or session finish time
// never determines which producer the user should resume. FileID is optional
// for workflows whose final deliverable is the contract itself.
type FinalSelection struct {
	Output string `json:"output"`
	FileID string `json:"file_id,omitempty"`
}

type Workflow func(context.Context, *Run, Input) (Result, error)

type Definition struct {
	Name        string
	Description string
	Version     string
	Policy      RunPolicy
	Execute     Workflow
}

type RoleSpec struct {
	Name         string
	Model        runtime.ModelSpec
	CWD          string
	AppendPrompt string
}

type Feedback = contract.Feedback

type RetryState struct {
	ActivationID string
	RetryCount   int
	MaxRetries   int
	Feedback     *Feedback
}

type StepSpec struct {
	Key      string
	Session  *SessionHandle
	Prompt   string
	Inputs   []contract.Ref
	Feedback *Feedback
	Output   contract.Spec
	Timeout  time.Duration
}

type StepResult struct {
	Output    contract.Ref
	AttemptID string
	Execution runtime.Execution
}

type Branch struct {
	Name string
	Do   func(context.Context, *Scope) (Result, error)
}

type BranchResult struct {
	Name   string
	Result Result
	Err    error
}

type JoinMode string

const (
	FailFast   JoinMode = "fail_fast"
	CollectAll JoinMode = "collect_all"
)

type RetryAction struct {
	Again    bool
	Feedback *Feedback
	Result   Result
}
