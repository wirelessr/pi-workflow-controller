package tui

import (
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"pi-workflow-controller/internal/engine"
)

// ReportMsg is the sole normal completion signal. A terminal Snapshot is not
// sufficient: Execute may still be closing persistence or collecting errors.
type ReportMsg engine.Report
type SnapshotMsg engine.Snapshot

type reportsClosedMsg struct{}

type clockAnchor struct {
	start    time.Time
	finished bool
	duration time.Duration
}

// Model observes one Run. The caller owns Execute and must send exactly one
// final Report, preferably on a buffered channel. It must disable Tea's default
// signal handler and forward OS signals to Run.Cancel instead of quitting Tea.
// Report is populated before the model requests tea.Quit.
type Model struct {
	Report *engine.Report

	run        *engine.Run
	reports    <-chan engine.Report
	changes    <-chan struct{}
	snapshot   engine.Snapshot
	spinner    spinner.Model
	now        time.Time
	anchors    map[string]clockAnchor
	cancelling bool
	feedClosed bool
	width      int
	height     int
	offset     int
}

func New(run *engine.Run, reports <-chan engine.Report) Model {
	m := Model{run: run, reports: reports, spinner: spinner.New(spinner.WithSpinner(spinner.Line)), anchors: make(map[string]clockAnchor)}
	if run != nil {
		m.changes = run.Changes()
		m.receive(run.Snapshot(), time.Now())
	}
	return m
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(m.spinner.Tick, m.wait())
}

// A single subscription waits for both final reports and coalesced changes.
// No goroutine sends directly into Tea from the engine's notification path.
func (m Model) wait() tea.Cmd {
	if m.reports == nil && m.changes == nil {
		return nil
	}
	return func() tea.Msg {
		select {
		case report, ok := <-m.reports:
			if !ok {
				return reportsClosedMsg{}
			}
			return ReportMsg(report)
		case <-m.changes:
			return SnapshotMsg(m.run.Snapshot())
		}
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if m.Report != nil {
		return m, nil
	}
	switch msg := msg.(type) {
	case ReportMsg:
		report := engine.Report(msg)
		m.receive(report.Snapshot, time.Now())
		m.Report = &report
		return m, tea.Quit
	case SnapshotMsg:
		s := engine.Snapshot(msg)
		if s.LastSeq >= m.snapshot.LastSeq {
			m.receive(s, time.Now())
		}
		return m, m.wait()
	case spinner.TickMsg:
		m.now = maxTime(m.now, msg.Time)
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.scroll(0)
	case tea.KeyPressMsg:
		switch msg.String() {
		case "q":
			m.cancel(engine.OriginControllerUser)
		case "ctrl+c":
			m.cancel(engine.OriginSignalINT)
		case "up", "k":
			m.scroll(-1)
		case "down", "j":
			m.scroll(1)
		case "pgup":
			m.scroll(-max(m.height-1, 1))
		case "pgdown":
			m.scroll(max(m.height-1, 1))
		}
	case reportsClosedMsg:
		// Closing the channel is not proof of cleanup. Fail closed and keep
		// waiting for an explicit ReportMsg rather than detaching resources.
		m.feedClosed = true
		m.reports = nil
		m.cancel(engine.OriginControllerUser)
		return m, m.wait()
	}
	return m, nil
}

func (m *Model) cancel(origin engine.Origin) {
	m.cancelling = true
	if m.run != nil {
		m.run.Cancel(origin)
	}
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func (m *Model) receive(snapshot engine.Snapshot, now time.Time) {
	m.snapshot = snapshot
	m.now = maxTime(m.now, now)
	if m.anchors == nil {
		m.anchors = make(map[string]clockAnchor)
	}
	m.anchor("run:"+snapshot.RunID, snapshot.CreatedAt, snapshot.FinishedAt)
	for id, a := range snapshot.Attempts {
		m.anchor("attempt:"+id, a.StartedAt, a.FinishedAt)
	}
}

func (m *Model) anchor(id string, start, end time.Time) {
	if start.IsZero() {
		return
	}
	a, ok := m.anchors[id]
	if !ok {
		// JSON snapshots have no monotonic component. Seed once from their
		// historical age, then advance only using the local monotonic clock.
		a.start = m.now.Add(-elapsed(start, time.Time{}, m.now))
		if !end.IsZero() {
			a.duration = elapsed(start, end, m.now)
			a.finished = true
		}
	}
	if !a.finished && !end.IsZero() {
		// Backdate the local completion anchor by notification latency. A slow
		// renderer must not count time after the engine finished the attempt.
		finished := m.now.Add(-max(m.now.Sub(end), 0))
		a.duration = max(finished.Sub(a.start), 0)
		a.finished = true
	}
	m.anchors[id] = a
}

func (m Model) elapsed(id string, start, end time.Time) time.Duration {
	if a, ok := m.anchors[id]; ok {
		if a.finished {
			return a.duration
		}
		return max(m.now.Sub(a.start), 0)
	}
	return elapsed(start, end, m.now)
}

func (m Model) content() string {
	w := &textWriter{}
	if m.Report == nil {
		w.line("%v Workflow controller", m.spinner.View())
	} else {
		w.line("Workflow controller")
	}
	if m.run != nil {
		w.line("Run path: %v", m.run.Dir())
	}
	frame := ""
	if m.Report == nil {
		frame = m.spinner.View()
	}
	w.WriteString(formatSnapshot(m.snapshot, m.elapsed, true, frame))
	if m.Report == nil {
		if m.cancelling {
			w.line("Cancelling; waiting for final report and cleanup. No detach.")
		} else {
			w.line("q / ctrl+c: cancel and wait for cleanup")
		}
	}
	if m.feedClosed {
		w.line("Warning: report channel closed without a final Report; completion is unconfirmed.")
	}
	return w.String()
}

func (m Model) lines() []string {
	content := m.content()
	if m.width > 0 {
		content = ansi.Hardwrap(content, m.width, true)
	}
	return strings.Split(strings.TrimSuffix(content, "\n"), "\n")
}

func (m *Model) scroll(delta int) {
	visible := max(m.height-1, 1)
	m.offset = min(max(m.offset+delta, 0), max(len(m.lines())-visible, 0))
}

func (m Model) View() tea.View {
	lines := m.lines()
	if m.height > 0 {
		visible := max(m.height-1, 1)
		offset := min(m.offset, max(len(lines)-visible, 0))
		end := min(offset+visible, len(lines))
		page := append([]string(nil), lines[offset:end]...)
		if m.height > 1 {
			footer := fmt.Sprintf("%d-%d/%d | up/down pgup/pgdown | q: cancel", offset+1, end, len(lines))
			if m.Report != nil {
				footer = "Final report received"
			} else if m.cancelling {
				footer = "Cancelling; waiting for cleanup and final report"
			}
			if m.width > 0 {
				footer = ansi.Truncate(footer, m.width, "")
			}
			page = append(page, footer)
		}
		lines = page
	}
	view := tea.NewView(strings.Join(lines, "\n"))
	view.AltScreen = true
	return view
}
