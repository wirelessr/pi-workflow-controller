package engine

import (
	"time"

	"pi-workflow-controller/internal/runtime"
)

// RunPolicy requires explicit positive limits; use DefaultRunPolicy for defaults.
type RunPolicy struct {
	Runtime             runtime.Policy
	AttemptTimeout      time.Duration
	RunTimeout          time.Duration
	DisableRunTimeout   bool // Opt out of the Controller deadline; durations must still be positive.
	MaxLiveSessions     int
	MaxTotalSessions    int
	MaxTotalAttempts    int
	MaxPromptBytes      int64
	MaxCandidateBytes   int64
	MaxJSONDepth        int
	MaxFileBytes        int64
	MaxAttemptFileBytes int64
	MaxAttemptFiles     int
	MaxJournalBytes     int64
}

func (p RunPolicy) validate() error {
	values := []struct {
		name  string
		value int64
	}{
		{"Runtime.StartupTimeout", int64(p.Runtime.StartupTimeout)},
		{"Runtime.RPCTimeout", int64(p.Runtime.RPCTimeout)},
		{"Runtime.PromptAckTimeout", int64(p.Runtime.PromptAckTimeout)},
		{"Runtime.PromptObservationTimeout", int64(p.Runtime.PromptObservationTimeout)},
		{"Runtime.AbortGrace", int64(p.Runtime.AbortGrace)},
		{"Runtime.CleanupTimeout", int64(p.Runtime.CleanupTimeout)},
		{"Runtime.HealthInterval", int64(p.Runtime.HealthInterval)},
		{"Runtime.MaxFrameBytes", int64(p.Runtime.MaxFrameBytes)},
		{"Runtime.ObservationQueue", int64(p.Runtime.ObservationQueue)},
		{"Runtime.MaxStderrBytes", int64(p.Runtime.MaxStderrBytes)},
		{"Runtime.StderrTailBytes", int64(p.Runtime.StderrTailBytes)},
		{"AttemptTimeout", int64(p.AttemptTimeout)},
		{"RunTimeout", int64(p.RunTimeout)},
		{"MaxLiveSessions", int64(p.MaxLiveSessions)},
		{"MaxTotalSessions", int64(p.MaxTotalSessions)},
		{"MaxTotalAttempts", int64(p.MaxTotalAttempts)},
		{"MaxPromptBytes", p.MaxPromptBytes},
		{"MaxCandidateBytes", p.MaxCandidateBytes},
		{"MaxJSONDepth", int64(p.MaxJSONDepth)},
		{"MaxFileBytes", p.MaxFileBytes},
		{"MaxAttemptFileBytes", p.MaxAttemptFileBytes},
		{"MaxAttemptFiles", int64(p.MaxAttemptFiles)},
		{"MaxJournalBytes", p.MaxJournalBytes},
	}
	for _, value := range values {
		if value.value <= 0 {
			return newFailure(InvalidDefinition, "policy", value.name+" must be positive")
		}
	}
	return nil
}

func DefaultRunPolicy() RunPolicy {
	return RunPolicy{
		Runtime:             runtime.DefaultPolicy(),
		AttemptTimeout:      30 * time.Minute,
		RunTimeout:          4 * time.Hour,
		MaxLiveSessions:     8,
		MaxTotalSessions:    64,
		MaxTotalAttempts:    256,
		MaxPromptBytes:      64 << 10,
		MaxCandidateBytes:   1 << 20,
		MaxJSONDepth:        64,
		MaxFileBytes:        64 << 20,
		MaxAttemptFileBytes: 256 << 20,
		MaxAttemptFiles:     128,
		MaxJournalBytes:     100 << 20,
	}
}
