package tui

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// FormatSnapshot renders metadata only. Callers supplying deserialized times
// get wall-clock durations; the live Model maintains its own monotonic anchors.
// Newlines belong to the layout; all external fields pass through SafeText.
func FormatSnapshot(snapshot engine.Snapshot, now time.Time) string {
	return formatSnapshot(snapshot, func(_ string, start, end time.Time) time.Duration {
		return elapsed(start, end, now)
	}, false, "")
}

type elapsedFunc func(string, time.Time, time.Time) time.Duration

type textWriter struct {
	strings.Builder
	styled bool
}

func (w *textWriter) line(format string, args ...any) {
	w.coloredLine("", format, args...)
}

func (w *textWriter) coloredLine(color, format string, args ...any) {
	// Sanitize fields separately: an unterminated OSC must not swallow the
	// next field or any controller-authored labels.
	clean := make([]any, len(args))
	for i, arg := range args {
		clean[i] = SafeText(fmt.Sprint(arg))
	}
	line := fmt.Sprintf(format, clean...)
	if w.styled && color != "" {
		line = lipgloss.NewStyle().Foreground(lipgloss.Color(color)).Render(line)
	}
	w.WriteString(line)
	w.WriteByte('\n')
}

func elapsed(start, end, now time.Time) time.Duration {
	if start.IsZero() {
		return 0
	}
	if end.IsZero() {
		end = now
	}
	return max(end.Sub(start), 0)
}

func duration(d time.Duration) string { return max(d, 0).Truncate(100 * time.Millisecond).String() }

func keys[T any](values map[string]T) []string {
	result := make([]string, 0, len(values))
	for k := range values {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

func formatSnapshot(s engine.Snapshot, timing elapsedFunc, styled bool, frame string) string {
	w := &textWriter{styled: styled}
	w.line("Workflow: %v  Run: %v  Task: %v", s.Workflow, s.RunID, s.TaskID)
	w.line("State: %v  Outcome: %v  Elapsed: %v", s.State, s.WorkflowOutcome, duration(timing("run:"+s.RunID, s.CreatedAt, s.FinishedAt)))
	persistence(w, s.StatePersisted)
	failureInfo(w, "Failure", s.Failure)
	w.line("Sessions:")
	for _, id := range keys(s.Sessions) {
		v := s.Sessions[id]
		color := ""
		switch v.Health {
		case "Online":
			color = "2"
		case "Offline":
			color = "1"
		case "Unresponsive":
			color = "3"
		}
		w.coloredLine(color, "  %v  role=%v  health=%v  state=%v  model=%v/%v  thinking=%v  provider retries=%v", id, v.Role.Name, v.Health, v.State, v.Role.Model.Provider, v.Role.Model.ID, v.Role.Model.Thinking, v.ProviderRetries)
	}
	w.line("Invocations:")
	ids := keys(s.Invocations)
	sort.SliceStable(ids, func(i, j int) bool {
		a, b := s.Invocations[ids[i]], s.Invocations[ids[j]]
		if a.Scope != b.Scope {
			return a.Scope < b.Scope
		}
		return a.Key < b.Key
	})
	for _, id := range ids {
		v := s.Invocations[id]
		indicator := ""
		if frame != "" && v.State == engine.Running && s.FinishedAt.IsZero() {
			indicator = frame + " "
		}
		w.line("  %v%v/%v  invocation=%v  epoch=%v  state=%v  provisional=%v", indicator, v.Scope, v.Key, id, v.Epoch, v.State, v.Provisional)
		if a, ok := s.Attempts[v.LastAttemptID]; ok {
			h := s.Sessions[a.HandleID]
			w.line("    latest attempt=%v (#%v)  state=%v  elapsed=%v  session=%v  model=%v/%v  accepted=%v", v.LastAttemptID, a.Number, a.State, duration(timing("attempt:"+v.LastAttemptID, a.StartedAt, a.FinishedAt)), a.HandleID, h.Role.Model.Provider, h.Role.Model.ID, a.DispatchAccepted)
			feedbackDetails(w, "    ", a.Feedback)
			failureInfo(w, "    Failure", a.Failure)
		} else {
			w.line("    latest attempt=%v (metadata unavailable)", v.LastAttemptID)
		}
		for _, activationID := range v.RetryActivationIDs {
			if retry, ok := s.Retries[activationID]; ok {
				retryDetails(w, "    ", activationID, retry)
			} else {
				w.line("    activation=%v (metadata unavailable)", activationID)
			}
		}
	}
	if len(s.Retries) > 0 {
		// Retry callbacks can run without invoking a Step.
		w.line("Retry activations (scope budgets):")
		rids := keys(s.Retries)
		sort.SliceStable(rids, func(i, j int) bool { return s.Retries[rids[i]].Scope < s.Retries[rids[j]].Scope })
		for _, id := range rids {
			retryDetails(w, "  ", id, s.Retries[id])
		}
	}
	for _, f := range s.CleanupErrors {
		failureInfo(w, "Cleanup warning", f)
	}
	for _, f := range s.FinalizationErrors {
		failureInfo(w, "Finalization error", f)
	}
	return w.String()
}

func retryDetails(w *textWriter, indent, id string, v engine.RetryStatus) {
	w.line("%v%v  activation=%v  retry=%v/%v  active=%v", indent, v.Scope, id, v.RetryCount, v.MaxRetries, v.Active)
	feedbackDetails(w, indent, v.Feedback)
}

func feedbackDetails(w *textWriter, indent string, f *engine.Feedback) {
	if f != nil {
		w.coloredLine("3", "%v  Latest feedback: %v  source attempt=%v  code=%v", indent, feedbackSummary(f.Message), f.SourceAttemptID, f.SourceCode)
	}
}

func feedbackSummary(message string) string {
	const limit = 240
	const suffix = "… [full text in request/events]"
	text := []rune(SafeText(message))
	if len(text) > limit {
		return string(text[:limit-len([]rune(suffix))]) + suffix
	}
	return string(text)
}

func persistence(w *textWriter, persisted bool) {
	if persisted {
		w.line("state_persisted=true")
	} else {
		w.coloredLine("1", "EMERGENCY: state_persisted=false; in-memory status is not a committed snapshot")
	}
}

func failureInfo(w *textWriter, label string, f *engine.FailureInfo) {
	if f == nil {
		return
	}
	w.line("%v: %v: %v  phase=%v  origin=%v  accepted=%v  limit=%v", label, f.Code, f.Message, f.Phase, f.Origin, f.DispatchAccepted, f.LimitScope)
	if f.RunID != "" || f.StepID != "" || f.AttemptID != "" || f.HandleID != "" {
		w.line("  run=%v  invocation=%v  attempt=%v  handle=%v", f.RunID, f.StepID, f.AttemptID, f.HandleID)
	}
	if f.Cause != "" {
		w.line("  Cause: %v", f.Cause)
	}
}

// FormatReport prints the final outcome and diagnostic metadata without reading
// the Store. Result references on a failed run are not proof of publication.
func FormatReport(report engine.Report, dir string) string {
	w := &textWriter{}
	w.line("Outcome: %v  Exit code: %v", report.Outcome, report.ExitCode)
	w.line("Run path: %v", dir)
	w.line("Run: %v  Workflow: %v", report.Snapshot.RunID, report.Snapshot.Workflow)
	persistence(w, report.Snapshot.StatePersisted)
	w.line("Result index path: %v", filepath.Join(dir, "result.json"))
	if final := report.Final; final != nil {
		w.line("Final output (verified before cleanup): %v", final.Output)
		w.line("  Contract: %v", final.Ref.Path)
		if final.ArtifactPath != "" {
			w.line("  Readable artifact: %v", final.ArtifactPath)
		}
		w.line("Final node: %v/%v", final.Scope, final.Step)
		if session, ok := report.Snapshot.Sessions[final.HandleID]; ok {
			w.line("  Role: %v", session.Role.Name)
			w.line("  Session ID: %v", session.Identity.SessionID)
			w.line("  Session file: %v", session.Identity.SessionFile)
			closed := false
			for _, cleanup := range report.Cleanup {
				if cleanup.Identity.HandleID == final.HandleID && cleanup.ProcessExited && cleanup.WaitCompleted {
					closed = true
				}
			}
			if closed {
				w.line("  Session state: Closed (process exit confirmed; use the session file to resume after safety preflight)")
			} else {
				w.line("  Session state: %v (process exit unconfirmed; do not resume until cleanup is verified)", session.State)
			}
		} else {
			w.line("  Session metadata unavailable")
		}
	} else {
		w.line("Final output/node: unavailable (no verified final contract selected; no session inferred)")
	}
	w.line("Output Ref paths (metadata only; not a commit assertion):")
	for _, name := range keys(report.Result.Outputs) {
		ref := report.Result.Outputs[name]
		w.line("  %v: %v  run=%v  attempt=%v  schema=%v", name, ref.Path, ref.RunID, ref.AttemptID, ref.SchemaID)
	}
	errorDetails(w, "Failure", report.Failure)
	if report.Failure == nil {
		failureInfo(w, "Failure", report.Snapshot.Failure)
	}
	for i, c := range report.Cleanup {
		w.line("Cleanup report %v:", i+1)
		cleanupDetails(w, c)
	}
	for _, e := range report.CleanupErrors {
		errorDetails(w, "Cleanup warning", e)
	}
	for _, f := range report.Snapshot.CleanupErrors {
		failureInfo(w, "Snapshot cleanup warning", f)
	}
	for _, e := range report.FinalizationErrors {
		errorDetails(w, "Finalization error", e)
	}
	for _, f := range report.Snapshot.FinalizationErrors {
		failureInfo(w, "Snapshot finalization error", f)
	}
	return w.String()
}

func errorDetails(w *textWriter, label string, err error) {
	if err == nil {
		return
	}
	if f, ok := err.(*engine.Failure); ok {
		failureInfo(w, label, &engine.FailureInfo{Code: f.Code, Message: f.Message, Phase: f.Phase, Origin: f.Origin, DispatchAccepted: f.DispatchAccepted, LimitScope: f.LimitScope, RunID: f.RunID, StepID: f.StepID, AttemptID: f.AttemptID, HandleID: f.HandleID})
		if f.StderrTail != "" {
			w.line("  Stderr tail: %v", f.StderrTail)
		}
		if f.Cleanup != nil {
			cleanupDetails(w, *f.Cleanup)
		}
	} else {
		w.line("%v: %v", label, err)
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			errorDetails(w, "  Cause", child)
		}
	case interface{ Unwrap() error }:
		errorDetails(w, "  Cause", e.Unwrap())
	}
}

func cleanupDetails(w *textWriter, c runtime.CleanupReport) {
	w.line("  handle=%v  session=%v  pid=%v  pgid=%v", c.Identity.HandleID, c.Identity.SessionID, c.Identity.PID, c.Identity.PGID)
	w.line("  Abort acknowledged=%v  AbortBash acknowledged=%v  SIGKILL=%v  Wait completed=%v  Process exited=%v", c.AbortAcknowledged, c.AbortBashAcknowledged, c.SIGKILL, c.WaitCompleted, c.ProcessExited)
	if !c.ProcessExited || !c.WaitCompleted {
		w.line("  Warning: process exit / Wait not fully confirmed")
	}
	for _, item := range []struct{ name, value string }{{"Wait", c.WaitError}, {"Kill", c.KillError}, {"Discovery", c.DiscoveryError}} {
		if item.value != "" {
			w.line("  %v warning: %v", item.name, item.value)
		}
	}
	for _, item := range c.Unconfirmed {
		w.line("  Unconfirmed: %v", item)
	}
	for _, path := range c.DiscoveryRemoved {
		w.line("  Discovery removed: %v", path)
	}
	if c.StderrTruncated {
		w.line("  Warning: stderr truncated")
	}
	if c.StderrTail != "" {
		w.line("  Stderr tail: %v", c.StderrTail)
	}
}
