// jira-triage-acquire produces an attempt-local intake candidate, not a workflow run.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"pi-workflow-controller/internal/workflows/triage"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: jira-triage-acquire /absolute/attempt/request.json < configuration.json")
		os.Exit(2)
	}
	// Keep default signal termination while waiting for stdin EOF. No acquisition
	// resources exist yet, and a pipe read must not mask SIGINT/SIGTERM.
	config, err := io.ReadAll(io.LimitReader(os.Stdin, triage.AcquisitionConfigLimit+1))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot read acquisition configuration")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = triage.RunAcquisition(ctx, os.Args[1], config)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
