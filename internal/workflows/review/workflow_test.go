package review

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
	failClose       string
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
	if err == nil && s.name == w.failClose {
		err = &runtime.Failure{Code: runtime.CleanupFailed, Origin: runtime.Protocol, Message: "fixture close failure"}
	}
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
	source   *acquisitionFixture
	// name and scope are the unit's run-directory prefix and scope path.
	name, scope string
}

// workflowOptions composes the unit inside a test-owned outer workflow. The
// zero value runs the code-review Definition unchanged.
type workflowOptions struct {
	name, scope, prompt string
	tlsDisabled         bool
	failClose           string
	policy              func(*engine.RunPolicy)
	execute             func(context.Context, *engine.Run, engine.Input, acquisitionSource) (engine.Result, error)
}

const outerSchema = "outer.v1"

func newWorkflowFixture(t *testing.T, scenario workflowScenario) *workflowFixture {
	t.Helper()
	source := newAcquisitionFixture(t)
	if source.source.gitOutput != nil || source.sourceRepo == "" {
		t.Fatal("default workflow fixture must use real Git")
	}
	return newWorkflowFixtureWith(t, scenario, source, workflowOptions{})
}

func newWorkflowFixtureWith(t *testing.T, scenario workflowScenario, source *acquisitionFixture, opts workflowOptions) *workflowFixture {
	t.Helper()
	if opts.name == "" {
		opts.name, opts.scope = "review", "root"
	}
	if opts.prompt == "" {
		opts.prompt = "https://github.com/owner/repo/pull/17/"
	}
	if opts.tlsDisabled {
		t.Setenv("NODE_TLS_REJECT_UNAUTHORIZED", "0")
	} else {
		t.Setenv("NODE_TLS_REJECT_UNAUTHORIZED", "1")
	}
	// Only the runtime's explicit directory controls discovery, not this setting.
	t.Setenv("PI_BRIDGE_DIR", filepath.Join(t.TempDir(), "not-the-runtime-bridge"))
	t.Setenv("GORACE", "atexit_sleep_ms=0")
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
	f := &workflowFixture{t: t, ctx: ctx, host: host, done: make(chan engine.Report, 1), joined: make(chan struct{}), scenario: scenario, hellos: map[string]protocol.Control{}, source: source, name: opts.name, scope: opts.scope}
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
	if opts.policy != nil {
		opts.policy(&definition.Policy)
	}
	definition.Execute = func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
		if opts.execute != nil {
			return opts.execute(ctx, r, in, source.source)
		}
		result, err := run(ctx, r, r.Root(), "review", in.Prompt, source.source)
		if scenario.prepareError == "source" {
			if _, readErr := engine.ReadContract(ctx, r, result.Outputs["prepare"]); readErr != nil {
				return result, fmt.Errorf("rejected prepare lost committed authorization: %w", readErr)
			}
		}
		return result, err
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
	f.runtime = &workflowRuntime{Runtime: pi, sessions: map[string]*workflowSession{}, partial: scenario.stop == "partial-start", attemptDeadline: scenario.stop == "attempt-deadline", failClose: opts.failClose}
	resources, schemas := Resources(), Schemas()
	if opts.execute != nil {
		uri := "https://fixture.local/outer.v1.json"
		resources = append(resources, contract.Resource{URI: uri, JSON: json.RawMessage(`{"type":"object","required":["value"],"additionalProperties":false,"properties":{"value":{"type":"string"}}}`)})
		schemas = append(schemas, contract.SchemaDefinition{ID: outerSchema, URI: uri})
	}
	registry, err := contract.NewRegistry(resources, schemas)
	if err != nil {
		t.Fatal(err)
	}
	f.run, err = engine.New(ctx, definition, engine.Input{Prompt: opts.prompt, LaunchCWD: dir}, engine.Options{BaseDir: baseDir, Schemas: registry, Runtime: f.runtime, PiVersion: "0.84.3", PiDefaultCWD: filepath.Join(dir, "unused-missing-default")})
	if err != nil {
		t.Fatal(err)
	}
	if source.source.gitOutput != nil {
		// Bind the fixture directory before Execute or any callback can run.
		source.root = filepath.Join(f.run.Dir(), f.name)
		if source.sourceRepo != "" || source.baseRepo != "" || source.headRepo != "" || len(source.gitCalls(t)) != 0 {
			t.Fatal("external Git fixture reused a repository or command history")
		}
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
				if hello.CWD != filepath.Join(f.run.Dir(), f.name+"-work") {
					f.t.Fatal("review explicit cwd was replaced by default")
				}
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
				snapshot := f.run.Snapshot()
				handle := snapshot.Attempts[event.request.Identity.AttemptID].HandleID
				if snapshot.Sessions[handle].Role.CWD != filepath.Join(f.run.Dir(), f.name+"-work") {
					f.t.Fatal("review persisted Role.CWD differs from explicit workspace")
				}
				prompt := event.request.Prompt
				if err := json.Unmarshal([]byte(prompt[strings.LastIndex(prompt, "\n")+1:]), &event.task); err != nil {
					f.t.Fatal(err)
				}
				if _, err := os.Stat(event.task.Skill); err != nil {
					f.t.Fatal(err)
				}
				if info, err := os.Stat(filepath.Join(f.run.Dir(), f.name, "repository.git", "worktrees", "checkout")); err != nil || !info.IsDir() {
					f.t.Fatalf("checkout registration must exist before cleanup: %v", err)
				}
				var head string
				if output := f.source.source.gitOutput; output != nil {
					raw, err := output(f.ctx, event.task.Worktree, []string{"-c", "core.hooksPath=/dev/null", "rev-parse", "HEAD"})
					if err != nil {
						f.t.Fatal(err)
					}
					head = strings.TrimSpace(string(raw))
				} else {
					head = fixtureGit(f.t, event.task.Worktree, "rev-parse", "HEAD")
				}
				if head != event.task.Pin.HeadSHA {
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
	if f.scenario.prepareError == "source" {
		for i, source := range p.Sources {
			if source.ID == "metadata" {
				p.Sources = append(p.Sources[:i], p.Sources[i+1:]...)
				break
			}
		}
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
	joined := false
	for _, e := range workflowJournal(f.t, f.run.Dir()) {
		joined = joined || (e.Kind == "GroupJoined" && eventDetail(e, "scope") == f.scope+"/reviewers")
	}
	if !joined {
		f.t.Fatal("validation dispatched without durable reviewer join")
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
	report := f.report()
	if len(report.CleanupErrors) != 0 || len(report.FinalizationErrors) != 0 {
		f.t.Fatalf("cleanup/finalization: %v / %v", report.CleanupErrors, report.FinalizationErrors)
	}
	f.ownedCleanup(report)
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
}

// ownedCleanup checks session order and exit, external calls, checkout
// removal and retained history, whatever the outcome.
func (f *workflowFixture) ownedCleanup(report engine.Report) {
	f.t.Helper()
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
	if f.source.source.gitOutput != nil {
		counts := map[string]int{}
		calls := f.source.gitCalls(f.t)
		for i, key := range calls {
			if i < 16 && key != fmt.Sprintf("acquire-%d", i) {
				f.t.Fatalf("acquisition command order changed: %v", calls)
			}
			counts[key]++
		}
		verifies := 3
		if f.scenario.validationError != "" {
			verifies = 2
		}
		if len(calls) != 16+5+2*verifies || counts["next-head"] != 5 || counts["verify-head"] != verifies || counts["verify-status"] != verifies {
			f.t.Fatalf("external fixture bypassed acquisition/HEAD/Verify: %v", calls)
		}
	} else {
		if len(f.source.gitCalls(f.t)) != 0 || len(f.source.requests) == 0 {
			f.t.Fatal("real Git workflow used a fake or did not fetch")
		}
	}
	root := filepath.Join(f.run.Dir(), f.name)
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
}

func TestWorkflowProductionReports(t *testing.T) {
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
			var f *workflowFixture
			if tc.name == "renderer-output-committed-through-Step" {
				f = newWorkflowFixture(t, tc.scenario)
			} else {
				f = newWorkflowFixtureWith(t, tc.scenario, newExternalGitFixture(t), workflowOptions{})
			}
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
			if failure := validateClosedByUnit(t, report, f.run.Dir(), "root"); failure != nil {
				t.Fatalf("validation close failed: %v", failure)
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
	for _, tc := range []struct {
		name     string
		scenario workflowScenario
		want     string
	}{
		{"prepare-source-omitted", workflowScenario{prepareError: "source"}, "absent from sources"},
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
			var f *workflowFixture
			if tc.name == "prepare-source-omitted" {
				f = newWorkflowFixture(t, tc.scenario)
			} else {
				f = newWorkflowFixtureWith(t, tc.scenario, newExternalGitFixture(t), workflowOptions{})
			}
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
			if tc.scenario.prepareError == "source" {
				cause := errors.Unwrap(report.Failure)
				if report.Failure.Error() != "prepare acceptance: snapshot \"metadata\" absent from sources" || cause == nil || cause.Error() != "snapshot \"metadata\" absent from sources" {
					t.Fatalf("prepare semantic rejection lost its acceptance wrapper/cause: %v", report.Failure)
				}
				failure := report.Snapshot.Failure
				if failure == nil || failure.Code != engine.WorkflowFailed || failure.Origin != engine.OriginDefinition || failure.Phase != "" {
					t.Fatalf("prepare semantic rejection became an execution/schema failure: %+v", failure)
				}
				if len(f.hellos) != 1 || len(report.Snapshot.Sessions) != 1 || len(report.Snapshot.Attempts) != 1 || len(report.Snapshot.Invocations) != 1 || len(report.Snapshot.Retries) != 0 || len(report.Cleanup) != 1 {
					t.Fatalf("prepare rejection dispatched extra work or lost accounting: %+v", report)
				}
				ref, ok := report.Result.Outputs["prepare"]
				attempt := report.Snapshot.Attempts[ref.AttemptID]
				if !ok || len(report.Result.Outputs) != 1 || ref.RunID != f.run.ID() || ref.SchemaID != PrepareSchema || attempt.Key != "prepare" || attempt.State != engine.Succeeded || attempt.Failure != nil || attempt.Output == nil || *attempt.Output != ref {
					t.Fatalf("rejected prepare must retain its exact successful Step publication: %+v / %+v", ref, attempt)
				}
				invocation := report.Snapshot.Invocations[attempt.Identity.InvocationID]
				if invocation.Key != "prepare" || invocation.State != engine.Succeeded || invocation.Attempts != 1 || invocation.LastAttemptID != ref.AttemptID {
					t.Fatalf("prepare invocation lost committed history: %+v", invocation)
				}
				owner := report.Snapshot.Sessions[attempt.HandleID]
				hello := f.hellos[owner.Identity.SessionID]
				cleanup := report.Cleanup[0]
				cleanupIdentity, ownerIdentity := cleanup.Identity, owner.Identity
				// Snapshot's JSON copy drops monotonic clock data.
				cleanupIdentity.SpawnTime, ownerIdentity.SpawnTime = time.Time{}, time.Time{}
				if owner.Role.Name != "review-prepare" || owner.State != "Closed" || owner.Identity.HandleID != attempt.HandleID || hello.SessionID != owner.Identity.SessionID || hello.PID != owner.Identity.PID || hello.History != owner.Identity.SessionFile || cleanupIdentity != ownerIdentity || !cleanup.Identity.SpawnTime.Equal(owner.Identity.SpawnTime) || !cleanup.ConfirmsLocalClose(owner.Identity.SessionID) {
					t.Fatalf("prepare rejection lost process/history/cleanup ownership: owner=%+v hello=%+v cleanup=%+v", owner, hello, cleanup)
				}
				var persisted struct {
					Outputs map[string]contract.Ref `json:"outputs"`
				}
				if err := protocol.ReadJSON(filepath.Join(f.run.Dir(), "result.json"), &persisted); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(persisted.Outputs, report.Result.Outputs) {
					t.Fatalf("prepare publication lost from failed result history: %+v", persisted.Outputs)
				}
				for _, event := range workflowJournal(t, f.run.Dir()) {
					if event.Kind == "Decision" {
						t.Fatalf("semantic rejection recorded a downstream Decision: %+v", event)
					}
				}
			}
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
				if failure := validateClosedByUnit(t, report, f.run.Dir(), "root"); failure != nil {
					t.Fatalf("validation close failed: %v", failure)
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

func workflowJournal(t *testing.T, dir string) []engine.Event {
	t.Helper()
	journal, err := os.Open(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	decoder := json.NewDecoder(journal)
	var events []engine.Event
	for {
		var event engine.Event
		if err := decoder.Decode(&event); errors.Is(err, io.EOF) {
			return events
		} else if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
}

func eventDetail(event engine.Event, key string) any {
	details, _ := event.Details.(map[string]any)
	return details[key]
}

// The unit, not run finalization, closes the validation session after its
// report-accepted Decision; it returns the close failure, if any.
func validateClosedByUnit(t *testing.T, report engine.Report, dir, scope string) any {
	t.Helper()
	handle := ""
	for id, s := range report.Snapshot.Sessions {
		if s.Role.Name == "review-validate" {
			handle = id
		}
	}
	decision, closed, finalizing := -1, -1, -1
	var failure any
	for i, event := range workflowJournal(t, dir) {
		switch {
		case event.Kind == "Decision" && eventDetail(event, "name") == "report-accepted" && eventDetail(event, "scope") == scope:
			decision = i
		case event.Kind == "SessionClosed" && eventDetail(event, "handle_id") == handle:
			closed, failure = i, eventDetail(event, "failure")
		case event.Kind == "RunFinalizing":
			finalizing = i
		}
	}
	if handle == "" || decision < 0 || closed < decision || finalizing < closed {
		t.Fatalf("validation session not closed by the unit after report acceptance: handle=%q decision=%d closed=%d finalizing=%d", handle, decision, closed, finalizing)
	}
	return failure
}

func (f *workflowFixture) report() engine.Report {
	f.t.Helper()
	select {
	case report := <-f.done:
		return report
	case <-f.ctx.Done():
		f.t.Fatal("workflow failed to join")
		return engine.Report{}
	}
}

// Closing the validation session fails after report acceptance. The failure
// classification stays the incomplete path's reviewer failure.
func TestWorkflowProductionValidateCloseFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		scenario workflowScenario
		code     engine.Code
	}{
		{"complete", workflowScenario{}, engine.CleanupFailed},
		{"incomplete-keeps-reviewer-failure", workflowScenario{reviewerError: "provider"}, engine.ProviderFailed},
		{"incomplete-semantic", workflowScenario{reviewerError: "sha"}, engine.WorkflowFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newWorkflowFixtureWith(t, tc.scenario, newExternalGitFixture(t), workflowOptions{failClose: "review-validate"})
			f.prepare(f.next("prompt"))
			for _, event := range f.reviewerBarrier() {
				f.review(event)
			}
			f.validate(f.next("prompt"))
			report := f.report()
			f.ownedCleanup(report)
			// An unclassified chain normalizes to WorkflowFailed.
			chainCode := engine.WorkflowFailed
			var failure *engine.Failure
			if errors.As(report.Failure, &failure) {
				chainCode = failure.Code
			}
			if report.Outcome != engine.Failed || report.ExitCode != 1 || chainCode != tc.code || report.Snapshot.Failure == nil || report.Snapshot.Failure.Code != tc.code || !strings.Contains(report.Failure.Error(), "fixture close failure") {
				t.Fatalf("validate close failure: outcome=%s exit=%d failure=%v snapshot=%+v, want %s", report.Outcome, report.ExitCode, report.Failure, report.Snapshot.Failure, tc.code)
			}
			if report.Final != nil || report.Result.Final != nil {
				t.Fatalf("close failure delivered a final: %+v / %+v", report.Result.Final, report.Final)
			}
			if _, ok := report.Result.Outputs["report"]; !ok {
				t.Fatal("accepted report Ref lost from failed result history")
			}
			if failure := validateClosedByUnit(t, report, f.run.Dir(), "root"); failure == nil {
				t.Fatal("SessionClosed did not record the close failure")
			}
			// Finalization re-reports the handle's stored close outcome.
			if len(report.CleanupErrors) != 1 || !strings.Contains(fmt.Sprint(errors.Unwrap(report.CleanupErrors[0])), "fixture close failure") || len(report.FinalizationErrors) != 0 {
				t.Fatalf("cleanup/finalization errors: %v / %v", report.CleanupErrors, report.FinalizationErrors)
			}
		})
	}
}

// outerOccupied holds the code-review literal names in root, in both scope
// name sets and in the run directory and cleanup namespace.
type outerOccupied struct {
	cleaned   bool
	afterUnit engine.Snapshot
}

func (o *outerOccupied) occupy(ctx context.Context, r *engine.Run) error {
	for _, dir := range []string{"review", "review-work", "review-skills"} {
		if err := os.Mkdir(filepath.Join(r.Dir(), dir), 0700); err != nil {
			return err
		}
	}
	if err := r.AddCleanup(ctx, "review-checkout", func(context.Context) error { o.cleaned = true; return nil }); err != nil {
		return err
	}
	root := r.Root()
	for _, key := range []string{"prepare", "validate"} {
		if _, err := root.Attach(ctx, engine.AttachSpec{Key: key, Output: contract.Spec{SchemaID: outerSchema}, Data: map[string]string{"value": "outer " + key}}); err != nil {
			return err
		}
	}
	for _, name := range []string{"context-accepted", "report-accepted"} {
		if err := root.Decision(ctx, name, "outer workflow decision", nil); err != nil {
			return err
		}
	}
	_, err := root.Parallel(ctx, "reviewers", engine.CollectAll, []engine.Branch{{Name: "code", Do: func(ctx context.Context, s *engine.Scope) (engine.Result, error) {
		ref, err := s.Attach(ctx, engine.AttachSpec{Key: "review", Output: contract.Spec{SchemaID: outerSchema}, Data: map[string]string{"value": "outer review"}})
		return engine.Result{Outputs: map[string]contract.Ref{"review": ref}}, err
	}}})
	return err
}

// nestedReview runs the unit as "deep-review" in a Child of root after the
// outer workflow occupies the code-review literals.
func nestedReview(o *outerOccupied) func(context.Context, *engine.Run, engine.Input, acquisitionSource) (engine.Result, error) {
	return func(ctx context.Context, r *engine.Run, in engine.Input, source acquisitionSource) (engine.Result, error) {
		if err := o.occupy(ctx, r); err != nil {
			return engine.Result{}, err
		}
		scope, err := r.Root().Child("deep-review")
		if err != nil {
			return engine.Result{}, err
		}
		result, err := run(ctx, r, scope, "deep-review", in.Prompt, source)
		o.afterUnit = r.Snapshot()
		return result, err
	}
}

// Outer occupation uses 3 Attach attempts; the unit adds exactly 5 sessions,
// at most 3 live, and 5 attempts.
func nestedPolicy(live, sessions, attempts int) func(*engine.RunPolicy) {
	return func(p *engine.RunPolicy) {
		p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = live, sessions, attempts
	}
}

func TestRunComposesInsideOuterWorkflow(t *testing.T) {
	const scope = "root/deep-review"
	outer := &outerOccupied{}
	f := newWorkflowFixtureWith(t, workflowScenario{}, newAcquisitionFixture(t), workflowOptions{name: "deep-review", scope: scope, policy: nestedPolicy(3, 5, 8), execute: nestedReview(outer)})
	f.prepare(f.next("prompt"))
	for _, event := range f.reviewerBarrier() {
		f.review(event)
	}
	f.validate(f.next("prompt"))
	report := f.finish()
	if report.Outcome != engine.Succeeded || report.ExitCode != 0 {
		t.Fatalf("nested review failed: %s %v", report.Outcome, report.Failure)
	}
	if len(f.hellos) != 5 || len(report.Snapshot.Sessions) != 5 || len(report.Snapshot.Attempts) != 8 {
		t.Fatalf("unit accounting differs from 5 sessions and 5 attempts plus 3 outer attempts: %+v", report.Snapshot)
	}
	want := map[string]bool{
		"root prepare": true, "root validate": true, "root/reviewers/code review": true,
		scope + " prepare": false, scope + " validate": false,
		scope + "/reviewers/code review": false, scope + "/reviewers/scale review": false, scope + "/reviewers/simplicity review": false,
	}
	got := map[string]bool{}
	for _, attempt := range report.Snapshot.Attempts {
		if attempt.State != engine.Succeeded {
			t.Fatalf("attempt did not succeed: %+v", attempt)
		}
		got[attempt.Scope+" "+attempt.Key] = attempt.Controller
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("attempt scopes/keys: %v", got)
	}
	for id, s := range outer.afterUnit.Sessions {
		if s.State != "Closed" {
			t.Fatalf("unit returned holding a live slot: %s %+v", id, s)
		}
	}
	decisions := map[string]int{}
	groups := map[string]int{}
	for _, event := range workflowJournal(t, f.run.Dir()) {
		switch event.Kind {
		case "Decision":
			decisions[fmt.Sprint(eventDetail(event, "scope"), " ", eventDetail(event, "name"))]++
		case "GroupJoined":
			groups[fmt.Sprint(eventDetail(event, "scope"))]++
		}
	}
	if !reflect.DeepEqual(decisions, map[string]int{"root context-accepted": 1, "root report-accepted": 1, scope + " context-accepted": 1, scope + " report-accepted": 1}) || !reflect.DeepEqual(groups, map[string]int{"root/reviewers": 1, scope + "/reviewers": 1}) {
		t.Fatalf("same-named outer and unit names did not coexist: %v %v", decisions, groups)
	}
	if report.Final.Scope != scope || report.Final.Step != "validate" {
		t.Fatalf("final not delivered from the unit's scope: %+v", report.Final)
	}
	validateClosedByUnit(t, report, f.run.Dir(), scope)
	for _, dir := range []string{"review", "review-work", "review-skills"} {
		entries, err := os.ReadDir(filepath.Join(f.run.Dir(), dir))
		if err != nil || len(entries) != 0 {
			t.Fatalf("unit wrote into outer %s: %v %v", dir, entries, err)
		}
	}
	if _, err := os.Stat(filepath.Join(f.run.Dir(), "deep-review-skills", "pwc-review-validate", "SKILL.md")); err != nil {
		t.Fatalf("unit skills not under its prefix: %v", err)
	}
	if !outer.cleaned {
		t.Fatal("outer cleanup with the code-review name did not run")
	}
}

func TestRunChargesOuterRunLimits(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		live, sessions, attempts int
		reviewers, validate      bool
		gotSessions, gotAttempts int
		message                  string
	}{
		{"three-live-required", 2, 5, 8, false, false, 3, 4, "session limit exceeded"},
		{"five-sessions-required", 3, 4, 8, true, false, 4, 7, "session limit exceeded"},
		{"five-attempts-required", 3, 5, 7, true, false, 5, 7, "run attempt limit exceeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outer := &outerOccupied{}
			f := newWorkflowFixtureWith(t, workflowScenario{}, newAcquisitionFixture(t), workflowOptions{name: "deep-review", scope: "root/deep-review", policy: nestedPolicy(tc.live, tc.sessions, tc.attempts), execute: nestedReview(outer)})
			f.prepare(f.next("prompt"))
			if tc.reviewers {
				for _, event := range f.reviewerBarrier() {
					f.review(event)
				}
			}
			report := f.finish()
			var failure *engine.Failure
			if report.Outcome != engine.Failed || report.ExitCode != 1 || !errors.As(report.Failure, &failure) || failure.Code != engine.LimitExceeded || failure.LimitScope != "run" || failure.Message != tc.message {
				t.Fatalf("too small outer policy: outcome=%s failure=%v", report.Outcome, report.Failure)
			}
			if len(report.Snapshot.Sessions) != tc.gotSessions || len(report.Snapshot.Attempts) != tc.gotAttempts {
				t.Fatalf("limit accounting: sessions=%d attempts=%d", len(report.Snapshot.Sessions), len(report.Snapshot.Attempts))
			}
			if _, ok := report.Result.Outputs["report"]; ok {
				t.Fatal("limit failure published a report")
			}
			if !outer.cleaned {
				t.Fatal("outer cleanup did not run")
			}
		})
	}
}

// One run tries every rejection in turn; none may leave a run-directory entry,
// session, attempt or external call behind.
func TestRunRejectsBeforeSideEffects(t *testing.T) {
	const prURL = "https://github.com/owner/repo/pull/17/"
	// An empty want is the TLS preflight rejection.
	cases := []struct {
		name, prURL string
		tlsDisabled bool
		want        string
	}{
		{"", prURL, false, `invalid review name ""`},
		{"../x", prURL, false, `invalid review name "../x"`},
		{strings.Repeat("a", 64), prURL, false, fmt.Sprintf("invalid review name %q", strings.Repeat("a", 64))},
		{strings.Repeat("a", 63), prURL, true, ""},
		{"Bad", "https://example.com/x", false, `invalid review name "Bad"`},
		{"a-work", prURL, false, `invalid review name "a-work"`},
		{"a-skills", prURL, false, `invalid review name "a-skills"`},
		{"deep-review", "https://example.com/x", false, "invalid GitHub PR URL"},
		{"deep-review", prURL, true, ""},
	}
	var failures []string
	f := newWorkflowFixtureWith(t, workflowScenario{}, newExternalGitFixture(t), workflowOptions{name: "deep-review", scope: "root", execute: func(ctx context.Context, r *engine.Run, _ engine.Input, source acquisitionSource) (engine.Result, error) {
		for _, tc := range cases {
			want := tc.want
			t.Setenv("NODE_TLS_REJECT_UNAUTHORIZED", "1")
			if tc.tlsDisabled {
				t.Setenv("NODE_TLS_REJECT_UNAUTHORIZED", "0")
			}
			if err := runtime.CheckNodeTLS(); want == "" && err != nil {
				want = err.Error()
			}
			before := workflowDirNames(t, r.Dir())
			_, err := run(ctx, r, r.Root(), tc.name, tc.prURL, source)
			after := workflowDirNames(t, r.Dir())
			if err == nil || err.Error() != want || !reflect.DeepEqual(before, after) {
				failures = append(failures, fmt.Sprintf("name=%q url=%q: err=%v want %q, entries %v -> %v", tc.name, tc.prURL, err, want, before, after))
			}
		}
		return engine.Result{}, nil
	}})
	report := f.report()
	if len(failures) != 0 || report.Outcome != engine.Succeeded {
		t.Fatalf("rejections: %v, outcome=%s failure=%v", failures, report.Outcome, report.Failure)
	}
	workflowNoSideEffects(t, f, report)
}

// The production execute keeps the original order: URL validation, then TLS,
// both before any run-directory entry, session or GitHub call.
func TestWorkflowProductionRejectsBeforeSideEffects(t *testing.T) {
	for _, tc := range []struct {
		name, prompt string
		want         string
	}{
		{"URL-before-TLS", "https://example.com/x", "invalid GitHub PR URL"},
		{"TLS-disabled", "https://github.com/owner/repo/pull/17/", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockProxyNetwork(t)
			f := newWorkflowFixtureWith(t, workflowScenario{}, newExternalGitFixture(t), workflowOptions{tlsDisabled: true, prompt: tc.prompt, execute: func(ctx context.Context, r *engine.Run, in engine.Input, _ acquisitionSource) (engine.Result, error) {
				return execute(ctx, r, in)
			}})
			report := f.report()
			want := tc.want
			if err := runtime.CheckNodeTLS(); want == "" && err != nil {
				want = err.Error()
			}
			if report.Outcome != engine.Failed || report.ExitCode != 1 || report.Failure == nil || report.Failure.Error() != want {
				t.Fatalf("code-review rejection: outcome=%s failure=%v, want %q", report.Outcome, report.Failure, want)
			}
			for _, name := range []string{"review", "review-work", "review-skills"} {
				if _, err := os.Lstat(filepath.Join(f.run.Dir(), name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("rejection created %s: %v", name, err)
				}
			}
			workflowNoSideEffects(t, f, report)
		})
	}
}

// The production execute derives its run-directory entries from "review": an
// occupied review-skills fails the skills reservation before any acquisition.
func TestWorkflowProductionReviewNameOwnsRunDirectory(t *testing.T) {
	blockProxyNetwork(t)
	f := newWorkflowFixtureWith(t, workflowScenario{}, newExternalGitFixture(t), workflowOptions{execute: func(ctx context.Context, r *engine.Run, in engine.Input, _ acquisitionSource) (engine.Result, error) {
		if err := os.Mkdir(filepath.Join(r.Dir(), "review-skills"), 0700); err != nil {
			return engine.Result{}, err
		}
		return execute(ctx, r, in)
	}})
	report := f.report()
	failure := report.Snapshot.Failure
	if report.Outcome != engine.Failed || report.ExitCode == 0 || failure == nil || failure.Code != engine.StorageFailed || failure.Phase != "review-skills" {
		t.Fatalf("occupied review-skills: outcome=%s failure=%+v", report.Outcome, failure)
	}
	if _, err := os.Lstat(filepath.Join(f.run.Dir(), "review")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkout root created after the reservation failure: %v", err)
	}
	if entries, err := os.ReadDir(filepath.Join(f.run.Dir(), "review-skills")); err != nil || len(entries) != 0 {
		t.Fatalf("occupied review-skills changed: %v %v", entries, err)
	}
	workflowNoSideEffects(t, f, report)
}

// Production acquisition reaches GitHub through Git; route it to a closed
// loopback port so a regression past the expected rejection fails fast.
func blockProxyNetwork(t *testing.T) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := "http://" + l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"} {
		t.Setenv(key, proxy)
	}
	for _, key := range []string{"NO_PROXY", "no_proxy"} {
		t.Setenv(key, "")
	}
}

func workflowDirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Error(err)
		return nil
	}
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	return names
}

func workflowNoSideEffects(t *testing.T, f *workflowFixture, report engine.Report) {
	t.Helper()
	if len(report.Snapshot.Sessions) != 0 || len(report.Snapshot.Attempts) != 0 || len(f.hellos) != 0 || report.Final != nil || len(report.Result.Outputs) != 0 {
		t.Fatalf("rejection dispatched work: %+v", report.Snapshot)
	}
	if _, err := os.Stat(filepath.Join(f.source.dir, "calls")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejection called GitHub: %v", err)
	}
	if len(f.source.gitCalls(t)) != 0 {
		t.Fatal("rejection ran git")
	}
}
