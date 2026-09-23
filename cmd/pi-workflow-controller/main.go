package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/charmbracelet/x/term"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/tui"
	"pi-workflow-controller/internal/workflows"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, cliOptions{definitions: workflows.Definitions(), resources: workflows.Resources(), schemas: workflows.Schemas()}))
}

// Construction-time dependencies, never flags or workflow configuration.
type cliOptions struct {
	definitions []engine.Definition
	resources   []contract.Resource
	schemas     []contract.SchemaDefinition
	engine      engine.Options
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer, opts cliOptions) int {
	// Go otherwise exits immediately on a broken stdout pipe, bypassing cleanup.
	brokenPipe := make(chan os.Signal, 1)
	signal.Notify(brokenPipe, syscall.SIGPIPE)
	defer signal.Stop(brokenPipe)
	fail := func(err error) int {
		_, _ = fmt.Fprintln(stderr, tui.SafeText(err.Error()))
		return 2
	}
	if (len(args) != 1 || args[0] != "list") && (len(args) != 3 || args[0] != "run") {
		return fail(fmt.Errorf("Usage: pi-workflow-controller list | run <workflow> \"Prompt\"")) //nolint:staticcheck // ST1005: Usage capitalization is part of the CLI output contract.
	}
	registry, err := engine.NewRegistry(opts.definitions)
	if err != nil {
		return fail(err)
	}
	schemas, err := contract.NewRegistry(opts.resources, opts.schemas)
	if err != nil {
		return fail(err)
	}
	if args[0] == "list" {
		definitions := registry.Definitions()
		if len(definitions) == 0 {
			_, err = fmt.Fprintln(stdout, "No predefined workflows registered.")
		}
		for _, definition := range definitions {
			if err != nil {
				break
			}
			_, err = fmt.Fprintf(stdout, "%s\t%s\n", tui.SafeText(definition.Name), tui.SafeText(definition.Description))
		}
		if err != nil {
			_, _ = fmt.Fprintln(stderr, tui.SafeText(err.Error()))
			return 1
		}
		return 0
	}
	definition, err := registry.Lookup(args[1])
	if err != nil {
		return fail(err)
	}
	if err = contract.ValidatePrompt(args[2], definition.Policy.MaxPromptBytes); err != nil {
		return fail(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return fail(err)
	}

	signals := make(chan os.Signal, 8)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	opts.engine.PiDefaultCWD = os.Getenv("PWC_PI_CWD")
	opts.engine.Schemas = schemas
	opts.engine.ControllerVersion = "M6"
	opts.engine.PiVersion = "0.84.3"
	r, err := engine.New(context.Background(), definition, engine.Input{Prompt: args[2], LaunchCWD: cwd}, opts.engine)
	if err != nil {
		return fail(err)
	}
	watchDone, watchStopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watchStopped)
		for {
			select {
			case sig := <-signals:
				origin := engine.OriginSignalINT
				if sig == syscall.SIGTERM {
					origin = engine.OriginSignalTERM
				}
				r.Cancel(origin)
			case <-watchDone:
				return
			}
		}
	}()
	defer func() { close(watchDone); <-watchStopped }()

	// Execute and signal forwarding are independent of terminal/pipe speed.
	reports := make(chan engine.Report, 1)
	done := make(chan struct{})
	var report engine.Report
	go func() { report = r.Execute(); reports <- report; close(done) }()
	var displayErr error
	if terminal(stdin) && terminal(stdout) {
		displayErr = terminalDisplay(r, reports, stdin.(*os.File), stdout.(*os.File))
		if displayErr != nil {
			r.Cancel(engine.OriginControllerUser)
		}
		<-done
	} else {
		displayErr = plain(r, done, stdout)
		if displayErr != nil {
			r.Cancel(engine.OriginControllerUser)
		}
		<-done
	}
	// Terminal input has joined and raw mode is restored before the report.
	_, finalErr := io.WriteString(stdout, tui.FormatReport(report, r.Dir()))
	if displayErr != nil {
		_, _ = fmt.Fprintln(stderr, "Display error:", tui.SafeText(displayErr.Error()))
	}
	if finalErr != nil {
		_, _ = fmt.Fprintln(stderr, "Report output error:", tui.SafeText(finalErr.Error()))
		_, _ = io.WriteString(stderr, tui.FormatReport(report, r.Dir()))
	}
	if report.ExitCode == 0 && (displayErr != nil || finalErr != nil) {
		return 1
	}
	return report.ExitCode
}

func terminal(stream any) bool {
	file, ok := stream.(*os.File)
	return ok && term.IsTerminal(file.Fd())
}

func plain(r *engine.Run, done <-chan struct{}, output io.Writer) error {
	if _, err := fmt.Fprintf(output, "Run path: %s\n", tui.SafeText(r.Dir())); err != nil {
		return err
	}
	for {
		if _, err := io.WriteString(output, tui.FormatSnapshot(r.Snapshot(), time.Now())); err != nil {
			return err
		}
		select {
		case <-done:
			return nil
		case <-r.Changes():
		}
	}
}
