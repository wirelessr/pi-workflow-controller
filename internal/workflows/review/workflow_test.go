package review

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

type workflowScenario struct {
	missing, empty, unknown, renderer            bool
	findings                                     string
	prepareError, reviewerError, validationError string
	stop                                         string
}

type workflowControl struct {
	protocol.Event
	request contract.Request
	task    task
}

// Wrap only the runtime ownership boundary. All children still speak real RPC,
// including the child whose partial Start is deliberately rejected.
type workflowRuntime struct {
	runtime.Runtime
	mu              sync.Mutex
	sessions        map[string]*workflowSession
	worktree        string
	partial         bool
	attemptDeadline bool
	violations      []error
}
type workflowSession struct {
	runtime.Session
	owner  *workflowRuntime
	name   string
	exited bool
}

func (w *workflowRuntime) Start(ctx context.Context, spec runtime.SessionSpec) (runtime.Session, error) {
	w.mu.Lock()
	if spec.Name != "review-prepare" {
		if p := w.sessions["review-prepare"]; p == nil || !p.exited {
			w.violations = append(w.violations, errors.New("reviewer started before prepare exit"))
		}
	}
	if spec.Name == "review-validate" {
		for _, role := range []string{"code", "scale", "simplicity"} {
			if s := w.sessions["review-"+role]; s == nil || !s.exited {
				w.violations = append(w.violations, fmt.Errorf("validation started before %s exit/join", role))
			}
		}
	}
	w.mu.Unlock()
	s, err := w.Runtime.Start(ctx, spec)
	if err != nil {
		return nil, err
	}
	wrapped := &workflowSession{Session: s, owner: w, name: spec.Name}
	w.mu.Lock()
	w.sessions[spec.Name] = wrapped
	w.mu.Unlock()
	if w.partial && spec.Name == "review-simplicity" {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		report, err := wrapped.Close(cleanupCtx)
		return nil, &runtime.Failure{Code: runtime.StartFailed, Origin: runtime.Protocol, Message: "fixture partial Start failure", Cause: err, Cleanup: &report}
	}
	return wrapped, nil
}

// The production Step has a fixed long timeout. Shorten only the external
// runtime execution context to exercise a genuine deadline and RPC abort,
// without replacing Step or waiting for the production review time budget.
func (s *workflowSession) Execute(ctx context.Context, dispatch runtime.Dispatch) (runtime.Execution, error) {
	if s.owner.attemptDeadline && s.name == "review-code" {
		timed, cancel := context.WithTimeoutCause(ctx, 2*time.Second, &runtime.Failure{Code: runtime.TimedOut, Origin: runtime.AttemptDeadline, Message: "fixture runtime attempt deadline"})
		defer cancel()
		return s.Session.Execute(timed, dispatch)
	}
	return s.Session.Execute(ctx, dispatch)
}

func (s *workflowSession) Close(ctx context.Context) (runtime.CleanupReport, error) {
	w := s.owner
	w.mu.Lock()
	path := w.worktree
	w.mu.Unlock()
	if path != "" {
		if _, err := os.Stat(path); err != nil {
			w.mu.Lock()
			w.violations = append(w.violations, fmt.Errorf("checkout removed before %s Close: %w", s.name, err))
			w.mu.Unlock()
		}
	}
	report, err := s.Session.Close(ctx)
	w.mu.Lock()
	defer w.mu.Unlock()
	if path != "" {
		if _, e := os.Stat(path); e != nil {
			w.violations = append(w.violations, fmt.Errorf("checkout removed before %s confirmed exit returned: %w", s.name, e))
		}
	}
	s.exited = report.ProcessExited && report.WaitCompleted
	return report, err
}

type workflowFixture struct {
	t        *testing.T
	ctx      context.Context
	run      *engine.Run
	runtime  *workflowRuntime
	host     *protocol.Host
	done     chan engine.Report
	joined   chan struct{}
	scenario workflowScenario
	hellos   map[string]protocol.Control
}

func newWorkflowFixture(t *testing.T, scenario workflowScenario) *workflowFixture {
	t.Helper()
	return newWorkflowFixtureFromSeed(t, scenario, nil)
}

func newWorkflowFixtureFromSeed(t *testing.T, scenario workflowScenario, seed *acquisitionGit) *workflowFixture {
	t.Helper()
	t.Setenv("NODE_TLS_REJECT_UNAUTHORIZED", "1")
	// Only the runtime's explicit directory controls discovery, not this setting.
	t.Setenv("PI_BRIDGE_DIR", filepath.Join(t.TempDir(), "not-the-runtime-bridge"))
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	source := newAcquisitionFixtureFromSeed(t, seed)
	if scenario.missing {
		if err := os.Remove(filepath.Join(source.dir, "issues.json")); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	t.Cleanup(cancel)
	host, err := protocol.NewHost(ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	f := &workflowFixture{t: t, ctx: ctx, host: host, done: make(chan engine.Report, 1), joined: make(chan struct{}), scenario: scenario, hellos: map[string]protocol.Control{}}
	// Install the lifeline before any Start, so even a failed assertion releases children.
	protocol.RegisterCleanup(t, host, func() <-chan struct{} {
		if f.run != nil {
			f.run.Cancel(engine.OriginControllerUser)
			return f.joined
		}
		return nil
	}, 10*time.Second, "", "workflow cleanup did not join")
	definition := Definition()
	definition.Policy.RunTimeout = 30 * time.Second
	if scenario.stop == "deadline" {
		definition.Policy.RunTimeout = 8 * time.Second
	}
	definition.Policy.Runtime.HealthInterval = time.Hour
	definition.Policy.Runtime.CleanupTimeout = 5 * time.Second
	definition.Policy.Runtime.AbortGrace = 100 * time.Millisecond
	definition.Execute = func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
		return executeSource(ctx, r, in, source.source)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	baseDir := dir
	if scenario.renderer {
		baseDir = filepath.Join(t.TempDir(), "aliased-store-base")
		if err := os.Symlink(dir, baseDir); err != nil {
			t.Fatal(err)
		}
	}
	bridge := filepath.Join(dir, "bridge")
	if err := os.Mkdir(bridge, 0700); err != nil {
		t.Fatal(err)
	}
	if scenario.stop == "preflight" {
		if err := os.WriteFile(filepath.Join(bridge, "other.json.recovering"), []byte("keep"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	pi, err := runtime.New(runtime.Options{Executable: executable, Args: []string{"-test.run=^TestCheckProtocolSubprocess$", "--"}, Env: []string{"PWC_CHECK_PROTOCOL=1", "PWC_ENGINE_MANUAL_CANDIDATE=1", "PWC_ENGINE_CONTROL=" + host.Addr().String(), "PI_CODING_AGENT_DIR=" + filepath.Join(dir, "agent"), "GORACE=atexit_sleep_ms=0"}, BridgeDir: bridge, Policy: definition.Policy.Runtime, Observe: func(ctx context.Context, o runtime.Observation) error { return f.run.Observe(ctx, o) }})
	if err != nil {
		t.Fatal(err)
	}
	f.runtime = &workflowRuntime{Runtime: pi, sessions: map[string]*workflowSession{}, partial: scenario.stop == "partial-start", attemptDeadline: scenario.stop == "attempt-deadline"}
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	f.run, err = engine.New(ctx, definition, engine.Input{Prompt: "https://github.com/owner/repo/pull/17/", LaunchCWD: dir}, engine.Options{BaseDir: baseDir, Schemas: registry, Runtime: f.runtime, PiVersion: "0.84.3"})
	if err != nil {
		t.Fatal(err)
	}
	go func() { defer close(f.joined); f.done <- f.run.Execute() }()
	return f
}
func (f *workflowFixture) next(kind string) workflowControl {
	f.t.Helper()
	for {
		select {
		case received, ok := <-f.host.Events():
			if !ok {
				f.t.Fatal("control host closed")
			}
			event := workflowControl{Event: received}
			if event.Err != nil {
				f.t.Fatal(event.Err)
			}
			if event.Message.Type == "hello" {
				hello := event.Message
				if f.hellos[hello.SessionID].SessionID != "" {
					f.t.Fatal("session reused")
				}
				for _, other := range f.hellos {
					if hello.History == other.History || hello.PID == other.PID {
						f.t.Fatal("review stages share process/history")
					}
				}
				f.hellos[hello.SessionID] = hello
				continue
			}
			if event.Message.Type != kind {
				f.t.Fatalf("expected %s barrier, got %+v", kind, event.Message)
			}
			if kind == "prompt" {
				if err := protocol.ReadJSON(event.Message.RequestPath, &event.request); err != nil {
					f.t.Fatal(err)
				}
				prompt := event.request.Prompt
				if err := json.Unmarshal([]byte(prompt[strings.LastIndex(prompt, "\n")+1:]), &event.task); err != nil {
					f.t.Fatal(err)
				}
				if _, err := os.Stat(event.task.Skill); err != nil {
					f.t.Fatal(err)
				}
				if head := fixtureGit(f.t, event.task.Worktree, "rev-parse", "HEAD"); head != event.task.Pin.HeadSHA {
					f.t.Fatal("task does not use acquired git HEAD")
				}
				f.runtime.mu.Lock()
				f.runtime.worktree = event.task.Worktree
				f.runtime.mu.Unlock()
			}
			return event
		case report := <-f.done:
			f.t.Fatalf("workflow ended before %s barrier: outcome=%s failure=%v cleanup=%v", kind, report.Outcome, report.Failure, report.CleanupErrors)
		case <-f.ctx.Done():
			f.t.Fatalf("waiting for %s: %v", kind, context.Cause(f.ctx))
		}
	}
}
func (f *workflowFixture) ack(event workflowControl, kind string) {
	f.t.Helper()
	if err := event.Reply(protocol.Control{Type: kind}); err != nil {
		f.t.Fatal(err)
	}
}
func (f *workflowFixture) candidate(event workflowControl, data any, files map[string][]byte) {
	f.t.Helper()
	entries := []contract.FileEntry{}
	for id, raw := range files {
		kind, prefix := "evidence", "evidence"
		if id == "context" || id == "report" {
			kind, prefix = "artifact", "artifacts"
		}
		path := filepath.Join(prefix, id)
		fixtureWrite(f.t, filepath.Join(filepath.Dir(event.Message.CandidatePath), path), raw)
		entries = append(entries, contract.FileEntry{ID: id, Kind: kind, Path: path})
	}
	if err := protocol.WriteEnvelope(event.Message.CandidatePath, event.request, data, entries); err != nil {
		f.t.Fatal(err)
	}
}
func workflowReadData[T any](t *testing.T, ref contract.Ref) T {
	t.Helper()
	var envelope struct {
		Data T `json:"data"`
	}
	if err := protocol.ReadJSON(ref.Path, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope.Data
}
func (f *workflowFixture) prepare(event workflowControl) {
	f.t.Helper()
	if event.task.Stage != "prepare" || len(event.request.Inputs) != 0 {
		f.t.Fatalf("unexpected prepare: %+v", event)
	}
	if len(event.task.RequiredSources) == 0 || len(event.task.RequiredSources) != len(event.task.Snapshots)+len(event.task.Missing) {
		f.t.Fatalf("required_sources must cover snapshot and missing keys: %+v", event.task)
	}
	missing := map[string]bool{}
	for _, id := range event.task.Missing {
		missing[id] = true
	}
	seen := map[string]bool{}
	p := Prepared{Pin: event.task.Pin, Sources: append([]Source(nil), event.task.RequiredSources...), Requirements: []Requirement{{ID: "req-head", Kind: "requirement", Statement: "The pinned shared file contains the requested head content", SourceIDs: []string{"metadata"}}}, OpenQuestions: []string{}, ContextFile: "context"}
	files := map[string][]byte{"context": []byte("Pinned acquisition context and requirements")}
	for _, source := range event.task.RequiredSources {
		if seen[source.ID] || source.Kind == "" || source.Note == "" {
			f.t.Fatalf("duplicate or incomplete required source: %+v", source)
		}
		seen[source.ID] = true
		if path, ok := event.task.Snapshots[source.ID]; ok {
			if missing[source.ID] || source.Status != "available" || source.FileID != source.ID || source.URL != (&url.URL{Scheme: "file", Path: path}).String() {
				f.t.Fatalf("required snapshot template differs: %+v", source)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				f.t.Fatal(err)
			}
			files[source.FileID] = raw
		} else if !missing[source.ID] || source.Status != "missing" || source.FileID != "" || source.URL != "" {
			f.t.Fatalf("required missing template differs: %+v", source)
		}
	}
	if f.scenario.renderer {
		p.Requirements[0].Statement += reportSpecial
	}
	if f.scenario.missing && !reflect.DeepEqual(event.task.Missing, []string{"issues"}) {
		f.t.Fatalf("missing transport not reflected: %+v", event.task)
	}
	if f.scenario.empty {
		p.Requirements = []Requirement{}
		p.OpenQuestions = []string{"No authoritative requirement was available"}
	}
	switch f.scenario.prepareError {
	case "empty":
		p.Requirements = []Requirement{}
		p.OpenQuestions = []string{}
	case "source":
		for i, source := range p.Sources {
			if source.ID == "metadata" {
				p.Sources = append(p.Sources[:i], p.Sources[i+1:]...)
				break
			}
		}
	case "pin":
		p.Pin.HeadSHA = p.Pin.BaseSHA
	}
	f.candidate(event, p, files)
	f.ack(event, "settle")
}
func workflowEvidence() Evidence {
	return Evidence{Path: "shared.txt", Line: 1, EndLine: 1, Detail: "Inspected pinned head content"}
}
func (f *workflowFixture) assessments(p Prepared) []Assessment {
	out := []Assessment{}
	for _, q := range p.Requirements {
		a := Assessment{RequirementID: q.ID, Status: "satisfied", Evidence: []Evidence{workflowEvidence()}, Reason: "Pinned head checked against this requirement"}
		if f.scenario.unknown {
			a.Status = "unconfirmed"
			a.Evidence = []Evidence{}
			a.Reason = "External acceptance criteria remain unavailable"
		}
		out = append(out, a)
	}
	return out
}
func (f *workflowFixture) review(event workflowControl) {
	f.t.Helper()
	role := event.task.Stage
	if len(event.request.Inputs) != 1 || event.request.Inputs[0].SchemaID != PrepareSchema {
		f.t.Fatalf("reviewer inputs: %+v", event.request.Inputs)
	}
	p := workflowReadData[Prepared](f.t, event.request.Inputs[0])
	v := Reviewed{Pin: event.task.Pin, Role: role, Context: event.request.Inputs[0], Coverage: []string{"Pinned changed files and shared.txt callers"}, Limitations: []string{}, Findings: []Finding{}, Requirements: []Assessment{}}
	if role == "code" {
		v.Requirements = f.assessments(p)
	}
	if f.scenario.findings != "" && (role == "code" || f.scenario.findings == "dedup") {
		e := workflowEvidence()
		v.Findings = []Finding{{ID: role + "-001", Title: "Pinned content violates downstream expectation", Severity: "high", Location: e, Evidence: []Evidence{e}, Impact: "Downstream consumer cannot use the changed value"}}
	}
	if role == "code" {
		switch f.scenario.reviewerError {
		case "role":
			v.Role = "scale"
		case "sha":
			v.Pin.HeadSHA = v.Pin.BaseSHA
		case "context":
			sum := sha256.Sum256([]byte(v.Context.ManifestSHA256))
			v.Context.ManifestSHA256 = hex.EncodeToString(sum[:])
		case "requirements":
			v.Requirements = []Assessment{}
		case "schema":
			v.Coverage = []string{}
		}
	}
	f.candidate(event, v, nil)
	ack := "settle"
	if role == "code" && f.scenario.reviewerError == "provider" {
		ack = "provider-error"
	}
	f.ack(event, ack)
}
func (f *workflowFixture) validate(event workflowControl) Validated {
	f.t.Helper()
	journal, err := os.Open(filepath.Join(f.run.Dir(), "events.jsonl"))
	if err != nil {
		f.t.Fatal(err)
	}
	defer func(journal *os.File) { _ = journal.Close() }(journal)
	decoder := json.NewDecoder(journal)
	for {
		var event engine.Event
		if err := decoder.Decode(&event); err != nil {
			f.t.Fatalf("validation dispatched without durable reviewer join: %v", err)
		}
		if event.Kind == "GroupJoined" {
			break
		}
	}
	if event.task.Stage != "validate" || len(event.task.Expected) != 3 {
		f.t.Fatalf("required reviewers dropped: %+v", event.task)
	}
	p := workflowReadData[Prepared](f.t, event.request.Inputs[0])
	v := Validated{Pin: event.task.Pin, Context: event.request.Inputs[0], Reviewers: event.task.Expected, Findings: []Finding{}, Dispositions: []Disposition{}, Requirements: f.assessments(p), Completeness: "complete", Conclusion: "no_confirmed_findings", Limitations: []string{}, ReportFile: "report"}
	if f.scenario.missing || f.scenario.empty || f.scenario.unknown {
		v.Completeness = "limited"
		v.Conclusion = "undetermined"
		v.Limitations = []string{"Source or requirement coverage remains uncertain"}
	}
	inputRefs := map[contract.Ref]bool{}
	for _, ref := range event.request.Inputs[1:] {
		inputRefs[ref] = true
	}
	success := 0
	for _, row := range event.task.Expected {
		if row.Status != "succeeded" {
			if row.Role != "code" || (f.scenario.reviewerError == "" && f.scenario.stop != "attempt-deadline") || row.Status != "failed" || row.Ref != nil {
				f.t.Fatalf("unexpected failed reviewer: %+v", row)
			}
			v.Completeness = "incomplete"
			v.Conclusion = "undetermined"
			v.Limitations = []string{"Required code reviewer failed; no clean-review claim is possible"}
			continue
		}
		success++
		if row.Ref == nil || !inputRefs[*row.Ref] {
			f.t.Fatalf("accepted reviewer missing from published inputs: %+v", row)
		}
		reviewer := workflowReadData[Reviewed](f.t, *row.Ref)
		for _, finding := range reviewer.Findings {
			d := Disposition{FindingID: finding.ID, Action: "confirmed", TargetID: finding.ID, Reason: "Independently checked pinned head"}
			if f.scenario.findings == "excluded" || (f.scenario.findings == "dedup" && row.Role == "simplicity") {
				d.Action = "excluded"
				d.TargetID = ""
				d.Reason = "Caller handles the case; source claim is a false positive"
			} else if f.scenario.findings == "dedup" && row.Role == "scale" {
				d.Action = "merged"
				d.TargetID = "code-001"
				d.Reason = "Same root cause and evidence as code-001"
			} else {
				v.Findings = append(v.Findings, finding)
			}
			v.Dispositions = append(v.Dispositions, d)
		}
	}
	if len(event.request.Inputs) != success+1 {
		f.t.Fatal("validation inputs contain missing or extra refs")
	}
	if len(v.Findings) > 0 {
		v.Conclusion = "findings"
	}
	switch f.scenario.validationError {
	case "overstated":
		v.Completeness = "complete"
		v.Conclusion = "no_confirmed_findings"
		v.Limitations = []string{}
	case "sha":
		v.Pin.HeadSHA = v.Pin.BaseSHA
	case "context":
		sum := sha256.Sum256([]byte(v.Context.ManifestSHA256))
		v.Context.ManifestSHA256 = hex.EncodeToString(sum[:])
	case "role":
		v.Reviewers = append([]ReviewerResult(nil), v.Reviewers...)
		v.Reviewers[0].Role = "scale"
	}
	if f.scenario.renderer {
		f.candidate(event, v, nil)
		script := filepath.Join(filepath.Dir(event.task.Skill), "scripts", "render_report.py")
		if output, err := runReport(f.t, script, event.Message.RequestPath, event.Message.CandidatePath); err != nil {
			f.t.Fatalf("renderer at Pi candidate boundary: %v\n%s", err, output)
		}
		v = workflowReadData[Validated](f.t, contract.Ref{Path: event.Message.CandidatePath})
	} else {
		f.candidate(event, v, map[string][]byte{"report": reportTestArtifact(f.t, p, v)})
	}
	f.ack(event, "settle")
	return v
}
func (f *workflowFixture) finish() engine.Report {
	f.t.Helper()
	select {
	case report := <-f.done:
		if len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 {
			f.t.Fatalf("cleanup/finalization: %v / %v", report.CleanupErrors, report.FinalizationErrors)
		}
		f.runtime.mu.Lock()
		violations := append([]error(nil), f.runtime.violations...)
		for name, s := range f.runtime.sessions {
			if !s.exited {
				violations = append(violations, fmt.Errorf("%s exit not confirmed", name))
			}
		}
		f.runtime.mu.Unlock()
		if len(violations) != 0 {
			f.t.Fatalf("ownership/order violations: %v", violations)
		}
		root := filepath.Join(f.run.Dir(), "review")
		for _, path := range []string{"checkout", "repository.git/worktrees/checkout"} {
			if _, err := os.Stat(filepath.Join(root, path)); !errors.Is(err, os.ErrNotExist) {
				f.t.Fatalf("checkout cleanup: %s: %v", path, err)
			}
		}
		for _, path := range []string{"snapshots/metadata.json", "snapshots/diff.diff", "repository.git/objects"} {
			if _, err := os.Stat(filepath.Join(root, path)); err != nil {
				f.t.Fatalf("acquisition history deleted: %s: %v", path, err)
			}
		}
		for _, cleanup := range report.Cleanup {
			if f.scenario.stop == "preflight" {
				if cleanup.ProcessExited || cleanup.WaitCompleted || cleanup.Identity.PID != 0 || len(cleanup.Unconfirmed) != 0 {
					f.t.Fatalf("preflight invented a process cleanup: %+v", cleanup)
				}
				continue
			}
			if !cleanup.ProcessExited || !cleanup.WaitCompleted {
				f.t.Fatalf("owned child exit unconfirmed: %+v", cleanup)
			}
			if _, err := os.Stat(cleanup.Identity.SessionFile); err != nil {
				f.t.Fatalf("Pi history removed: %v", err)
			}
		}
		for _, name := range []string{"events.jsonl", "run.json", "result.json", "cleanup.json"} {
			if _, err := os.Stat(filepath.Join(f.run.Dir(), name)); err != nil {
				f.t.Fatalf("engine history missing: %s: %v", name, err)
			}
		}
		if ref, ok := report.Result.Outputs["report"]; ok {
			var envelope struct {
				Data  Validated   `json:"data"`
				Files []checkFile `json:"files"`
			}
			if err := protocol.ReadJSON(ref.Path, &envelope); err != nil {
				f.t.Fatal(err)
			}
			selection := &engine.FinalSelection{Output: "report", FileID: envelope.Data.ReportFile}
			attempt := report.Snapshot.Attempts[ref.AttemptID]
			want := &engine.FinalDelivery{Output: "report", Ref: ref, HandleID: attempt.HandleID, Scope: attempt.Scope, Step: "validate"}
			for _, file := range envelope.Files {
				if file.ID == envelope.Data.ReportFile && file.Kind == "artifact" {
					want.ArtifactPath = filepath.Join(filepath.Dir(ref.Path), file.Path)
				}
			}
			if want.ArtifactPath == "" || !reflect.DeepEqual(report.Result.Final, selection) || !reflect.DeepEqual(report.Final, want) {
				f.t.Fatalf("accepted report lost explicit final selection/delivery: selection=%+v final=%+v want=%+v", report.Result.Final, report.Final, want)
			}
			if report.Snapshot.Sessions[report.Final.HandleID].Role.Name != "review-validate" {
				f.t.Fatal("final session is not the validation producer")
			}
			if _, err := os.Stat(report.Final.ArtifactPath); err != nil {
				f.t.Fatalf("final report file unavailable: %v", err)
			}
		} else if report.Final != nil || report.Result.Final != nil {
			f.t.Fatalf("rejected/cancelled review fabricated final: selection=%+v final=%+v", report.Result.Final, report.Final)
		}
		var result struct {
			Final *engine.FinalDelivery `json:"final"`
		}
		if err := protocol.ReadJSON(filepath.Join(f.run.Dir(), "result.json"), &result); err != nil {
			f.t.Fatal(err)
		}
		if !reflect.DeepEqual(result.Final, report.Final) {
			f.t.Fatalf("persisted final differs from Report: %+v / %+v", result.Final, report.Final)
		}
		return report
	case <-f.ctx.Done():
		f.t.Fatal("workflow failed to join")
		return engine.Report{}
	}
}

func TestWorkflowProductionReports(t *testing.T) {
	seed := newAcquisitionGit(t, t.TempDir(), t.TempDir())
	for _, tc := range []struct {
		name                     string
		scenario                 workflowScenario
		completeness, conclusion string
		findings, dispositions   int
	}{
		{"renderer-output-committed-through-Step", workflowScenario{renderer: true}, "complete", "no_confirmed_findings", 0, 0},
		{"confirmed-finding-is-execution-success", workflowScenario{findings: "confirmed"}, "complete", "findings", 1, 1},
		{"dedup-and-excluded-keep-dispositions", workflowScenario{findings: "dedup"}, "complete", "findings", 1, 3},
		{"false-positive-excluded", workflowScenario{findings: "excluded"}, "complete", "no_confirmed_findings", 0, 1},
		{"missing-source", workflowScenario{missing: true}, "limited", "undetermined", 0, 0},
		{"empty-requirements-with-question", workflowScenario{empty: true}, "limited", "undetermined", 0, 0},
		{"unconfirmed-requirement", workflowScenario{unknown: true}, "limited", "undetermined", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWorkflowFixtureFromSeed(t, tc.scenario, &seed)
			f.prepare(f.next("prompt"))
			reviewers := f.reviewerBarrier()
			for _, event := range reviewers {
				f.review(event)
			}
			expected := f.validate(f.next("prompt"))
			report := f.finish()
			if report.Outcome != engine.Succeeded || report.ExitCode != 0 {
				t.Fatalf("execution should succeed independently of findings: %s %v", report.Outcome, report.Failure)
			}
			if len(f.hellos) != 5 || len(report.Snapshot.Sessions) != 5 || len(report.Snapshot.Attempts) != 5 {
				t.Fatalf("stages did not use five independent sessions/attempts: %+v", report.Snapshot)
			}
			got := workflowReadData[Validated](t, report.Result.Outputs["report"])
			if !reflect.DeepEqual(got, expected) || got.Completeness != tc.completeness || got.Conclusion != tc.conclusion || len(got.Findings) != tc.findings || len(got.Dispositions) != tc.dispositions {
				t.Fatalf("published report differs: %+v", got)
			}
			if tc.scenario.findings == "dedup" {
				actions := map[string]string{}
				for _, d := range got.Dispositions {
					actions[d.FindingID] = d.Action
				}
				if !reflect.DeepEqual(actions, map[string]string{"code-001": "confirmed", "scale-001": "merged", "simplicity-001": "excluded"}) {
					t.Fatalf("dispositions lost: %v", actions)
				}
			}
		})
	}
}

// No reviewer is released until three distinct RPC prompts have arrived. This
// would deadlock a serial workflow rather than falsely passing a timing check.
func (f *workflowFixture) reviewerBarrier() []workflowControl {
	f.t.Helper()
	events := []workflowControl{}
	seen := map[string]bool{}
	for range 3 {
		event := f.next("prompt")
		if !reviewRole(event.task.Stage) || seen[event.task.Stage] {
			f.t.Fatalf("not three required reviewers: %s", event.task.Stage)
		}
		seen[event.task.Stage] = true
		if len(events) > 0 && (event.task.Pin != events[0].task.Pin || !reflect.DeepEqual(event.request.Inputs, events[0].request.Inputs)) {
			f.t.Fatal("reviewers did not receive same pinned context")
		}
		events = append(events, event)
	}
	snapshot := f.run.Snapshot()
	for _, s := range snapshot.Sessions {
		if s.Role.Name == "review-prepare" && s.State != "Closed" {
			f.t.Fatalf("prepare still live at reviewer barrier: %+v", s)
		}
		if s.Role.Name == "review-validate" {
			f.t.Fatal("validation started before reviewer join")
		}
	}
	return events
}

func TestWorkflowProductionRejectsInvalidResults(t *testing.T) {
	seed := newAcquisitionGit(t, t.TempDir(), t.TempDir())
	for _, tc := range []struct {
		name     string
		scenario workflowScenario
		want     string
	}{
		{"prepare-empty", workflowScenario{prepareError: "empty"}, "empty requirements"},
		{"prepare-source-omitted", workflowScenario{prepareError: "source"}, "absent from sources"},
		{"prepare-pin", workflowScenario{prepareError: "pin"}, "pin mismatch"},
		{"reviewer-role", workflowScenario{reviewerError: "role"}, "role mismatch"},
		{"reviewer-SHA", workflowScenario{reviewerError: "sha"}, "pin mismatch"},
		{"reviewer-context-ref", workflowScenario{reviewerError: "context"}, "context Ref mismatch"},
		{"code-missing-requirement", workflowScenario{reviewerError: "requirements"}, "exactly once"},
		{"provider-failure", workflowScenario{reviewerError: "provider"}, "ProviderFailed"},
		{"schema-invalid-releases-slot", workflowScenario{reviewerError: "schema"}, "ContractInvalid"},
		{"validation-SHA", workflowScenario{validationError: "sha"}, "pin mismatch"},
		{"validation-context-ref", workflowScenario{validationError: "context"}, "context Ref mismatch"},
		{"validation-role", workflowScenario{validationError: "role"}, "reviewer result mismatch"},
		{"missing-source-cannot-claim-complete", workflowScenario{missing: true, validationError: "overstated"}, "overstates"},
		{"empty-baseline-cannot-claim-complete", workflowScenario{empty: true, validationError: "overstated"}, "overstates"},
		{"unknown-coverage-cannot-claim-complete", workflowScenario{unknown: true, validationError: "overstated"}, "overstates"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWorkflowFixtureFromSeed(t, tc.scenario, &seed)
			f.prepare(f.next("prompt"))
			if tc.scenario.prepareError == "" {
				reviewers := f.reviewerBarrier()
				for _, event := range reviewers {
					f.review(event)
				}
				f.validate(f.next("prompt"))
			}
			report := f.finish()
			if report.Outcome != engine.Failed || report.ExitCode == 0 {
				t.Fatalf("invalid result accepted: %s %v", report.Outcome, report.Failure)
			}
			checkError(t, report.Failure, tc.want)
			if tc.scenario.reviewerError != "" {
				if len(report.Snapshot.Sessions) != 5 || len(report.Snapshot.Attempts) != 5 {
					t.Fatal("required reviewer failure silently skipped validation or reviewer")
				}
				got := workflowReadData[Validated](t, report.Result.Outputs["report"])
				if got.Completeness != "incomplete" || len(got.Reviewers) != 3 || got.Conclusion != "undetermined" || len(got.Limitations) == 0 {
					t.Fatalf("failure hidden: %+v", got)
				}
				if _, ok := report.Result.Outputs["code"]; ok {
					t.Fatal("rejected code result exposed as accepted")
				}
			} else if _, ok := report.Result.Outputs["report"]; ok {
				t.Fatal("rejected prepare/validation exposed report")
			}
		})
	}
}

func TestWorkflowProductionSharedPreflight(t *testing.T) {
	f := newWorkflowFixture(t, workflowScenario{stop: "preflight"})
	report := f.finish()
	var failure *runtime.Failure
	if report.Outcome != engine.Failed || report.ExitCode != 1 || !errors.As(report.Failure, &failure) || failure.Code != runtime.BridgeUnavailable || failure.Phase != "preflight" || failure.HandleID == "" || failure.DispatchAccepted != runtime.AcceptedNo {
		t.Fatalf("review bypassed common preflight: %+v", report)
	}
	if len(report.Snapshot.Sessions) != 1 || report.Snapshot.Sessions[failure.HandleID].State != "Closed" || len(report.Snapshot.Attempts) != 0 || report.Final != nil {
		t.Fatalf("review preflight lost startup accounting: %+v", report)
	}
	if _, err := os.Stat(filepath.Join(f.run.Dir(), "sessions", failure.HandleID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review preflight created persistent resources: %v", err)
	}
	claim := filepath.Join(report.Snapshot.Input.LaunchCWD, "bridge", "other.json.recovering")
	if raw, err := os.ReadFile(claim); err != nil || string(raw) != "keep" {
		t.Fatalf("review modified foreign discovery: %v", err)
	}
}

func TestWorkflowProductionCancellationAndPartialStartCleanup(t *testing.T) {
	for _, stop := range []string{"cancel", "deadline", "partial-start"} {
		t.Run(stop, func(t *testing.T) {
			f := newWorkflowFixture(t, workflowScenario{stop: stop})
			f.prepare(f.next("prompt"))
			if stop != "partial-start" {
				reviewers := f.reviewerBarrier()
				for _, event := range reviewers {
					f.ack(event, "hold")
				}
				for range 3 {
					f.next("held")
				}
				if stop == "cancel" {
					f.run.Cancel(engine.OriginControllerUser)
				}
			}
			report := f.finish()
			want := engine.Failed
			if stop == "cancel" {
				want = engine.CancelledState
			}
			if stop == "deadline" {
				want = engine.TimedOutState
			}
			if report.Outcome != want || report.ExitCode == 0 {
				t.Fatalf("stop outcome=%s failure=%v, want %s", report.Outcome, report.Failure, want)
			}
			if stop == "partial-start" {
				checkError(t, report.Failure, "partial Start failure")
			} else {
				failed := 0
				for _, attempt := range report.Snapshot.Attempts {
					if attempt.Key == "review" {
						failed++
						if attempt.State != want {
							t.Fatalf("reviewer did not settle at stop: %+v", attempt)
						}
					}
				}
				if failed != 3 {
					t.Fatalf("required attempts lost at cancellation: %d", failed)
				}
			}
			if _, ok := report.Result.Outputs["report"]; ok {
				t.Fatal("cancelled/partial workflow published a report")
			}
		})
	}
}

func TestWorkflowProductionRuntimeAttemptDeadline(t *testing.T) {
	f := newWorkflowFixture(t, workflowScenario{stop: "attempt-deadline"})
	f.prepare(f.next("prompt"))
	reviewers := f.reviewerBarrier()
	for _, event := range reviewers {
		if event.task.Stage == "code" {
			f.ack(event, "hold")
		} else {
			f.review(event)
		}
	}
	f.next("held")
	f.validate(f.next("prompt"))
	report := f.finish()
	if report.Outcome != engine.TimedOutState || report.ExitCode == 0 {
		t.Fatalf("attempt deadline outcome: %s %v", report.Outcome, report.Failure)
	}
	var failure *runtime.Failure
	if !errors.As(report.Failure, &failure) || failure.Code != runtime.TimedOut || failure.Origin != runtime.AttemptDeadline {
		t.Fatalf("attempt deadline cause lost: %v", report.Failure)
	}
	got := workflowReadData[Validated](t, report.Result.Outputs["report"])
	if got.Completeness != "incomplete" || len(got.Reviewers) != 3 {
		t.Fatalf("timed out reviewer silently omitted: %+v", got)
	}
	if len(report.Snapshot.Sessions) != 5 {
		t.Fatal("attempt timeout did not release the validation slot")
	}
}

func TestWorkflowProductionCleanupJoinsUnfinishedRun(t *testing.T) {
	var f *workflowFixture
	if !t.Run("active-run", func(t *testing.T) {
		f = newWorkflowFixture(t, workflowScenario{})
		f.prepare(f.next("prompt"))
		for _, event := range f.reviewerBarrier() {
			f.ack(event, "hold")
		}
		for range 3 {
			f.next("held")
		}
	}) {
		return
	}
	select {
	case <-f.joined:
	default:
		t.Fatal("registered cleanup returned without joining the active engine")
	}
	report := <-f.done
	if report.Outcome != engine.CancelledState || report.ExitCode == 0 {
		t.Fatalf("cleanup did not cancel active run: outcome=%s failure=%v", report.Outcome, report.Failure)
	}
	if len(report.Cleanup) != len(report.Snapshot.Sessions) {
		t.Fatal("cleanup lost owned sessions")
	}
	for _, cleanup := range report.Cleanup {
		owner := report.Snapshot.Sessions[cleanup.Identity.HandleID]
		if owner.State != "Closed" || cleanup.Identity.SessionID != owner.Identity.SessionID || !cleanup.WaitCompleted || !cleanup.ProcessExited {
			t.Fatalf("registered cleanup did not retain real close/Wait: %+v", cleanup)
		}
		t.Logf("ordinary cleanup retained diagnostics: WaitError=%q KillError=%q DiscoveryError=%q Unconfirmed=%v", cleanup.WaitError, cleanup.KillError, cleanup.DiscoveryError, cleanup.Unconfirmed)
	}
}
