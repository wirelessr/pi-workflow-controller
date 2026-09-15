package tui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

func updateModel(m Model, msg tea.Msg) (Model, tea.Cmd) {
	next, cmd := m.Update(msg)
	return next.(Model), cmd
}

func TestModelWaitsForReportNotTerminalSnapshot(t *testing.T) {
	m := New(nil, nil)
	m, cmd := updateModel(m, tea.KeyPressMsg{Code: 'q', Text: "q"})
	if cmd != nil {
		t.Fatal("cancel requested quit before report")
	}
	requireText(t, m.View().Content, "waiting for final report and cleanup")
	m, cmd = updateModel(m, SnapshotMsg{State: engine.CancelledState, LastSeq: 10, StatePersisted: true})
	if cmd != nil || m.Report != nil {
		t.Fatal("terminal snapshot treated as completed Execute")
	}
	m, _ = updateModel(m, SnapshotMsg{State: engine.Running, LastSeq: 9, StatePersisted: true})
	requireText(t, m.View().Content, "State: Cancelled")
	report := engine.Report{Outcome: engine.CancelledState, ExitCode: 130, Snapshot: engine.Snapshot{State: engine.CancelledState}}
	m, cmd = updateModel(m, ReportMsg(report))
	if cmd == nil {
		t.Fatal("final report did not request quit")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatal("final report command is not Quit")
	}
	if m.Report == nil || m.Report.ExitCode != 130 {
		t.Fatal("final report not retained")
	}
	m, cmd = updateModel(m, SnapshotMsg{State: engine.Running, LastSeq: 99})
	if cmd != nil || m.snapshot.State != engine.CancelledState {
		t.Fatal("late snapshot changed completed model")
	}
}

func TestModelActiveInvocationSpinnersShareClock(t *testing.T) {
	now := time.Now()
	s := displaySnapshot(now)
	s.LastSeq = 1
	for i, state := range []engine.State{engine.Preparing, engine.WaitingSession, engine.Dispatching, engine.Running, engine.Validating} {
		id := fmt.Sprintf("parallel-%d", i)
		s.Invocations[id] = engine.InvocationState{Scope: "root/parallel/" + id, Key: "work", State: engine.Running, LastAttemptID: id}
		s.Attempts[id] = engine.AttemptState{State: state, StartedAt: now}
	}
	for _, state := range []engine.State{engine.AwaitingScope, engine.Succeeded, engine.Failed, engine.CancelledState, engine.TimedOutState} {
		id := string(state)
		s.Invocations[id] = engine.InvocationState{Scope: "root/parallel/" + id, Key: "done", State: state}
	}
	m := New(nil, nil)
	m, _ = updateModel(m, SnapshotMsg(s))
	checkRows := func() string {
		t.Helper()
		view := ansi.Strip(m.View().Content)
		frame := strings.Fields(view)[0]
		for id, inv := range s.Invocations {
			prefix := "  "
			if inv.State == engine.Running && s.FinishedAt.IsZero() && m.Report == nil {
				prefix += frame + " "
			}
			want := fmt.Sprintf("%s%s/%s  invocation=%s", prefix, inv.Scope, inv.Key, id)
			found := false
			for _, line := range strings.Split(view, "\n") {
				if strings.HasPrefix(line, want) {
					found = true
				}
			}
			if !found {
				t.Errorf("missing invocation row %q in:\n%s", want, view)
			}
		}
		return frame
	}
	before := checkRows()
	m, cmd := updateModel(m, m.spinner.Tick())
	if cmd == nil || checkRows() == before {
		t.Fatal("shared tick did not animate active rows")
	}
	v := s.Invocations["inv-a"]
	v.State = engine.AwaitingScope
	s.Invocations["inv-a"] = v
	s.LastSeq++
	m, _ = updateModel(m, SnapshotMsg(s))
	checkRows()
	m, _ = updateModel(m, cmd())
	checkRows()
	v.State = engine.Running
	s.Invocations["inv-a"] = v
	s.LastSeq++
	m, _ = updateModel(m, SnapshotMsg(s))
	checkRows()
	s.FinishedAt = now.Add(time.Second)
	s.LastSeq++
	m, _ = updateModel(m, SnapshotMsg(s))
	checkRows()
	m, _ = updateModel(m, ReportMsg{Snapshot: s})
	checkRows()
}

func TestModelMonotonicAnchorsSurviveJSONSnapshots(t *testing.T) {
	base := time.Now()
	s := displaySnapshot(base)
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	m := New(nil, nil)
	m.receive(s, base)
	before := m.spinner.View()
	tick := m.spinner.Tick().(spinner.TickMsg)
	tick.Time = base.Add(2 * time.Second)
	m, cmd := updateModel(m, tick)
	if cmd == nil || m.spinner.View() == before || m.spinner.Spinner.FPS != 100*time.Millisecond {
		t.Fatal("spinner did not advance at the configured interval")
	}
	requireText(t, m.View().Content, "Elapsed: 12s", "elapsed=4.3s")
	// Re-reading timestamps cannot reset an already observed attempt's clock.
	s.CreatedAt = s.CreatedAt.Add(time.Hour)
	a := s.Attempts["a3"]
	a.StartedAt = a.StartedAt.Add(time.Hour)
	s.Attempts["a3"] = a
	m.receive(s, base.Add(3*time.Second))
	requireText(t, m.View().Content, "Elapsed: 13s", "elapsed=5.3s")
	s.FinishedAt = base.Add(4 * time.Second).Round(0)
	a.FinishedAt = s.FinishedAt
	s.Attempts["a3"] = a
	m.receive(s, base.Add(4*time.Second))
	tick = m.spinner.Tick().(spinner.TickMsg)
	tick.Time = base.Add(time.Hour)
	m, _ = updateModel(m, tick)
	requireText(t, m.View().Content, "Elapsed: 14s", "elapsed=6.3s")
}

func TestModelDelayedCompletionDoesNotCountRenderingDelay(t *testing.T) {
	base := time.Now()
	s := displaySnapshot(base)
	m := New(nil, nil)
	m.receive(s, base)
	s.FinishedAt = base.Add(3 * time.Second).Round(0)
	a := s.Attempts["a3"]
	a.FinishedAt = s.FinishedAt
	s.Attempts["a3"] = a
	m.receive(s, base.Add(time.Minute))
	requireText(t, m.View().Content, "Elapsed: 13s", "elapsed=5.3s")
}

func TestModelScrollAndResize(t *testing.T) {
	now := time.Now()
	m := New(nil, nil)
	s := displaySnapshot(now)
	s.FinalizationErrors = []*engine.FailureInfo{{Code: engine.FinalizationFailed, Message: "bottom warning"}}
	m.receive(s, now)
	m, _ = updateModel(m, tea.WindowSizeMsg{Width: 60, Height: 8})
	view := m.View()
	if !view.AltScreen || len(strings.Split(view.Content, "\n")) > 8 {
		t.Fatal("view does not fit terminal")
	}
	for _, line := range strings.Split(view.Content, "\n") {
		if ansi.StringWidth(line) > 60 {
			t.Fatalf("line exceeds terminal width: %q", line)
		}
	}
	for range 100 {
		m, _ = updateModel(m, tea.KeyPressMsg{Code: tea.KeyPgDown})
	}
	requireText(t, ansi.Strip(m.View().Content), "bottom warning")
	m, _ = updateModel(m, tea.WindowSizeMsg{Width: 120, Height: 200})
	requireText(t, m.View().Content, "Workflow controller", "bottom warning")
}

func TestReportChannelClosedIsNotCompletion(t *testing.T) {
	reports := make(chan engine.Report)
	close(reports)
	m := New(nil, reports)
	msg := m.wait()()
	m, cmd := updateModel(m, msg)
	if cmd != nil || m.Report != nil {
		t.Fatal("closed channel must not produce a report or quit")
	}
	requireText(t, m.View().Content, "closed without a final Report", "completion is unconfirmed")
}

// This boundary replaces only the external Pi session. Cleanup remains gated
// so the real Run can prove that cancelling the view never detaches resources.
type viewRuntime struct {
	closeStarted chan struct{}
	allowClose   chan struct{}
}

type viewSession struct {
	runtime *viewRuntime
	spec    runtime.SessionSpec
}

func (r *viewRuntime) Start(_ context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	return &viewSession{runtime: r, spec: spec}, nil
}
func (s *viewSession) Identity() runtime.Identity {
	return runtime.Identity{HandleID: s.spec.HandleID, SessionID: "session"}
}
func (s *viewSession) Snapshot(context.Context) (runtime.SessionState, error) {
	return runtime.SessionState{Identity: s.Identity(), Health: "Online", Model: s.spec.Model}, nil
}
func (s *viewSession) Execute(context.Context, runtime.Dispatch) (runtime.Execution, error) {
	return runtime.Execution{}, errors.New("unexpected dispatch in cancellation-only fixture")
}
func (s *viewSession) Confirm(context.Context, runtime.Execution) (runtime.Confirmation, error) {
	return runtime.Confirmation{}, errors.New("unexpected confirmation in cancellation-only fixture")
}
func (s *viewSession) Close(ctx context.Context) (runtime.CleanupReport, error) {
	close(s.runtime.closeStarted)
	select {
	case <-s.runtime.allowClose:
		return runtime.CleanupReport{Identity: s.Identity(), ProcessExited: true, WaitCompleted: true}, nil
	case <-ctx.Done():
		return runtime.CleanupReport{}, context.Cause(ctx)
	}
}

func newViewRun(t *testing.T, rt runtime.Runtime, workflow engine.Workflow) *engine.Run {
	t.Helper()
	schemas, err := contract.NewRegistry(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	run, err := engine.New(ctx, engine.Definition{Name: "view-test", Version: "1", Policy: engine.DefaultRunPolicy(), Execute: workflow}, engine.Input{Prompt: "test", LaunchCWD: dir}, engine.Options{Schemas: schemas, BaseDir: dir, Runtime: rt})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestRealRunCancellationWaitsForCleanup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		key    tea.KeyPressMsg
		origin engine.Origin
	}{
		{"q", tea.KeyPressMsg{Code: 'q', Text: "q"}, engine.OriginControllerUser},
		{"ctrl+c", tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}, engine.OriginSignalINT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := &viewRuntime{closeStarted: make(chan struct{}), allowClose: make(chan struct{})}
			releaseCleanup := sync.OnceFunc(func() { close(rt.allowClose) })
			defer releaseCleanup()
			ready := make(chan struct{})
			run := newViewRun(t, rt, func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
				_, err := r.OpenSession(ctx, engine.RoleSpec{Name: "worker", Model: runtime.ModelSpec{Provider: "test", ID: "test", Thinking: "off"}})
				if err != nil {
					return engine.Result{}, err
				}
				close(ready)
				<-ctx.Done()
				return engine.Result{}, context.Cause(ctx)
			})
			reports := make(chan engine.Report, 1)
			m := New(run, reports)
			go func() { reports <- run.Execute() }()
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("workflow did not open session")
			}
			m, cmd := updateModel(m, tc.key)
			if cmd != nil {
				t.Fatal("key requested quit")
			}
			select {
			case <-rt.closeStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("cancellation did not start cleanup")
			}
			select {
			case <-reports:
				t.Fatal("Execute returned before cleanup")
			default:
			}
			// Cancel remains first-wins when repeated with another origin.
			run.Cancel(engine.OriginSignalTERM)
			releaseCleanup()
			select {
			case report := <-reports:
				var f *engine.Failure
				if report.Outcome != engine.CancelledState || report.ExitCode != 130 || !errors.As(report.Failure, &f) || f.Origin != tc.origin {
					t.Fatalf("unexpected cancellation report: %+v, failure=%+v", report, f)
				}
				m, cmd = updateModel(m, ReportMsg(report))
				if cmd == nil || m.Report == nil || !report.Cleanup[0].ProcessExited {
					t.Fatal("missing final report / cleanup")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup did not produce final report")
			}
		})
	}
}

func TestStoppedConsumerDoesNotBlockRealEngine(t *testing.T) {
	run := newViewRun(t, &viewRuntime{}, func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
		for i := range 32 {
			if err := r.Root().Decision(ctx, fmt.Sprintf("decision-%d", i), "progress", nil); err != nil {
				return engine.Result{}, err
			}
		}
		return engine.Result{}, nil
	})
	reports := make(chan engine.Report, 1)
	m := New(run, reports)
	go func() { reports <- run.Execute() }()
	// Do not call Init or consume Changes until Execute is finished.
	select {
	case report := <-reports:
		if report.Outcome != engine.Succeeded || report.ExitCode != 0 {
			t.Fatalf("stopped consumer blocked or failed engine: %+v", report)
		}
		m, cmd := updateModel(m, ReportMsg(report))
		if cmd == nil || m.Report == nil {
			t.Fatal("report not accepted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stopped consumer blocked engine")
	}
}

func TestRealTeaProgramConsumesFinalReport(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	run := newViewRun(t, &viewRuntime{}, func(ctx context.Context, _ *engine.Run, _ engine.Input) (engine.Result, error) {
		select {
		case <-release:
			return engine.Result{}, nil
		case <-ctx.Done():
			return engine.Result{}, context.Cause(ctx)
		}
	})
	reports := make(chan engine.Report, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := tea.NewProgram(New(run, reports), tea.WithInput(nil), tea.WithOutput(io.Discard), tea.WithoutRenderer(), tea.WithoutSignalHandler(), tea.WithContext(ctx))
	finished := make(chan struct{})
	var result tea.Model
	var runErr error
	go func() {
		result, runErr = p.Run()
		close(finished)
	}()
	go func() { reports <- run.Execute() }()
	p.Send(tea.KeyPressMsg{Code: 'q', Text: "q"})
	select {
	case <-finished:
		if runErr != nil {
			t.Fatal(runErr)
		}
		m := result.(Model)
		if m.Report == nil || m.Report.Outcome != engine.CancelledState {
			t.Fatalf("Tea exited without final cancellation report: %+v", m.Report)
		}
		if strings.Contains(m.View().Content, "q / ctrl+c") {
			t.Fatal("finished view still offers cancellation")
		}
	case <-ctx.Done():
		t.Fatal("Tea did not consume final report")
	}
}
