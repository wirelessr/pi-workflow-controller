// Package options adapts the cycle-free bundled harness to runtime construction.
package options

import (
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/bundled"
	"time"
)

func New(h *bundled.Harness) runtime.Options {
	p := runtime.DefaultPolicy()
	p.StartupTimeout = 20 * time.Second
	p.RPCTimeout = 3 * time.Second
	p.PromptAckTimeout = 5 * time.Second
	p.PromptObservationTimeout = 2 * time.Second
	p.CleanupTimeout = 8 * time.Second
	p.AbortGrace = time.Second
	return runtime.Options{Executable: h.Launch.Executable, Args: h.Launch.Args, Env: h.Launch.Env, BridgeDir: h.BridgeDir, Policy: p}
}
