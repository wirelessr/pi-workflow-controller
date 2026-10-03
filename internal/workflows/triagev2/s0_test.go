package triagev2

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

// TestTriageV2Subprocess is the fake Pi process: the real RPC protocol with
// candidates written by the test host, not by a model.
func TestTriageV2Subprocess(t *testing.T) {
	if os.Getenv("PWC_TRIAGEV2_PROTOCOL") != "1" {
		return
	}
	for _, arg := range os.Args {
		if arg == "--version" {
			fmt.Println("0.84.3")
			os.Exit(0)
		}
	}
	if err := protocol.Serve(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

// agentCall is one dispatch the fake agent answers.
type agentCall struct {
	Role      string
	Request   contract.Request
	Task      task
	Candidate string
}

// writeFiles writes evidence files into the attempt and returns their entries.
func (c agentCall) writeFiles(t *testing.T, files map[string][]byte) []contract.FileEntry {
	t.Helper()
	entries := []contract.FileEntry{}
	for id, data := range files {
		path := filepath.Join("evidence", id+".txt")
		if err := os.WriteFile(filepath.Join(filepath.Dir(c.Candidate), path), data, 0600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, contract.FileEntry{ID: id, Kind: "evidence", Path: path})
	}
	return entries
}

func (c agentCall) reply(t *testing.T, data any, files map[string][]byte) {
	t.Helper()
	if err := protocol.WriteEnvelope(c.Candidate, c.Request, data, c.writeFiles(t, files)); err != nil {
		t.Fatal(err)
	}
}

func (c agentCall) citable(label string) contract.Ref {
	for _, in := range c.Task.Citable {
		if in.Label == label {
			return in.Ref
		}
	}
	panic("no citable input " + label)
}

// harnessPercent is the context usage the fake Pi reports; nil reports
// none. Tests that change it restore it.
var harnessPercent = ptr(10.0)

// harnessPolicy adjusts the run policy; nil keeps the default.
var harnessPolicy func(*engine.RunPolicy)

type harnessResult struct {
	Report engine.Report
	Run    *engine.Run
	Roles  []string
}

// runHarness executes a workflow over the real engine, runtime and RPC
// protocol. agent writes each candidate; the fake Pi then settles.
// agent returns the fake Pi's answer to the prompt: "" settles, "hold"
// leaves the prompt running until the attempt times out.
func runHarness(t *testing.T, prompt string, execute engine.Workflow, agent func(*testing.T, agentCall) string) harnessResult {
	t.Helper()
	dir := t.TempDir()
	bridge := filepath.Join(dir, "bridge")
	if err := os.Mkdir(bridge, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	host, err := protocol.NewHost(ctx, 16)
	if err != nil {
		t.Fatal(err)
	}
	var r *engine.Run
	done := make(chan struct{})
	protocol.RegisterCleanup(t, host, func() <-chan struct{} {
		cancel()
		if r != nil {
			r.Cancel(engine.OriginControllerUser)
			return done
		}
		return nil
	}, 8*time.Second, "fixture host close: ", "fixture run did not join")
	policy := engine.DefaultRunPolicy()
	if harnessPolicy != nil {
		harnessPolicy(&policy)
	}
	policy.Runtime.StartupTimeout = 5 * time.Second
	policy.Runtime.CleanupTimeout = 3 * time.Second
	policy.Runtime.AbortGrace = 50 * time.Millisecond
	policy.Runtime.HealthInterval = time.Hour
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pi, err := runtime.New(runtime.Options{Executable: exe, Args: []string{"-test.run=^TestTriageV2Subprocess$", "--"},
		Env:       []string{"PWC_TRIAGEV2_PROTOCOL=1", "PWC_ENGINE_MANUAL_CANDIDATE=1", "PWC_ENGINE_CONTROL_STATS=1", "PWC_ENGINE_CONTROL=" + host.Addr().String(), "GORACE=atexit_sleep_ms=0"},
		BridgeDir: bridge, Policy: policy.Runtime, Observe: func(ctx context.Context, o runtime.Observation) error { return r.Observe(ctx, o) }})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := contract.NewRegistry(Resources(), Schemas())
	if err != nil {
		t.Fatal(err)
	}
	r, err = engine.New(ctx, engine.Definition{Name: "triagev2-fixture", Version: "1", Policy: policy, Execute: execute},
		engine.Input{Prompt: prompt, LaunchCWD: dir}, engine.Options{BaseDir: dir, Schemas: registry, Runtime: pi, PiVersion: "0.84.3"})
	if err != nil {
		t.Fatal(err)
	}
	var report engine.Report
	go func() { report = r.Execute(); close(done) }()
	var mu sync.Mutex
	var roles []string
	for {
		select {
		case <-done:
			return harnessResult{Report: report, Run: r, Roles: roles}
		case <-ctx.Done():
			t.Fatal("workflow exceeded the fixture deadline")
		case e, ok := <-host.Events():
			if !ok {
				t.Fatal("fixture host events closed")
			}
			if e.Err != nil {
				t.Fatalf("fixture host event: %v", e.Err)
			}
			if e.Message.Type == "stats" {
				usage := map[string]any{"tokens": nil, "contextWindow": 100000, "percent": nil}
				if harnessPercent != nil {
					usage["tokens"], usage["percent"] = 1000, *harnessPercent
				}
				data, err := json.Marshal(map[string]any{"sessionId": e.Message.SessionID, "sessionFile": e.Message.History, "contextUsage": usage})
				if err != nil {
					t.Fatal(err)
				}
				if err := e.Reply(protocol.Control{Type: "stats", Data: data}); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if e.Message.Type != "prompt" {
				continue
			}
			var req contract.Request
			if err := protocol.ReadJSON(e.Message.RequestPath, &req); err != nil {
				t.Fatal(err)
			}
			var tk task
			if err := json.Unmarshal([]byte(req.Prompt), &tk); err != nil {
				t.Fatalf("request prompt is not a task: %v", err)
			}
			mu.Lock()
			roles = append(roles, tk.Role)
			mu.Unlock()
			ack := agent(t, agentCall{Role: tk.Role, Request: req, Task: tk, Candidate: e.Message.CandidatePath})
			if ack == "" {
				ack = "settle"
			}
			if err := e.Reply(protocol.Control{Type: ack}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// intakeFiles is an anonymous complete ticket: two comment pages, one linked
// issue and one attachment with an extraction.
func intakeFiles() (Intake, map[string][]byte) {
	files := map[string][]byte{
		"fields": []byte(`[{"id":"customfield_1","name":"Ambiguous"},{"id":"description","name":"Description"}]`),
		"page-0": []byte(`{"startAt":0,"total":2,"comments":[{"id":"c1","body":"Tenant 17 (orgkey org-17) reports failures at 2025-01-02T00:30:00+02:00"}]}`),
		"page-1": []byte(`{"startAt":1,"total":2,"comments":[{"id":"c2","body":"Host acme.example.invalid, possibly on pop-a"}]}`),
		"linked": []byte(`{"key":"CASE-18","fields":{"summary":"Same incident"}}`),
		"bundle": []byte("event=sample at 2025-01-02T00:30:00+02:00\n"),
		"unpack": []byte("extracted bundle listing\n"),
	}
	issue, _ := json.Marshal(map[string]any{"key": "CASE-17", "fields": map[string]any{
		"description": "Anonymous incident", "customfield_1": 17,
		"comment":    map[string]any{"total": 2, "comments": []any{}},
		"issuelinks": []any{map[string]any{"outwardIssue": map[string]any{"key": "CASE-18"}}},
		"attachment": []any{map[string]any{"id": "a1", "size": len(files["bundle"])}},
	}})
	files["issue"] = issue
	available := func(id string) Source { return Source{Status: "available", FileID: id} }
	return Intake{Ticket: "CASE-17", URL: "https://jira.example.invalid/browse/CASE-17", FetchedAt: "2025-01-03T00:00:00Z",
		Issue: available("issue"), Fields: available("fields"),
		Comments:    []CommentPage{{0, available("page-0")}, {1, available("page-1")}},
		Linked:      []LinkedIssue{{"CASE-18", available("linked")}},
		Attachments: []Attachment{{"a1", available("bundle"), available("unpack")}},
		Complete:    true, Gaps: []Gap{}}, files
}

// cite cites a whole JSON file; citeRange cites bytes of any file.
func cite(ref contract.Ref, id string) Evidence {
	whole := ""
	return Evidence{Ref: &ref, FileID: id, Locator: &Locator{Pointer: &whole}}
}

func citeRange(ref contract.Ref, id string, offset, length int64) Evidence {
	return Evidence{Ref: &ref, FileID: id, Locator: &Locator{Offset: &offset, Length: &length}}
}

func factsFor(call agentCall) Facts {
	intake, prompt := call.citable("intake"), call.citable("caller prompt")
	return Facts{Intake: intake, Prompt: prompt,
		Facts: []Fact{
			{ID: "tenant", Kind: "tenant_id", Value: "17", Evidence: []Evidence{cite(intake, "page-0")}},
			{ID: "orgkey", Kind: "orgkey", Value: "org-17", Evidence: []Evidence{cite(intake, "page-0")}},
			{ID: "pop", Kind: "home_pop", Value: "pop-a", Evidence: []Evidence{cite(intake, "page-1")}},
		},
		TimeAnchors:    []TimeAnchor{{ID: "event", Event: "reported failure", Original: "2025-01-02T00:30:00+02:00", Format: "rfc3339", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 7200, Evidence: cite(intake, "page-0")}},
		VisionRequests: []VisionRequest{},
		Gaps:           []Gap{}}
}

// checkFor is the fake validator: one verdict per id the request lists in
// judge, so the tests exercise the channel the real validator reads.
func checkFor(t *testing.T, call agentCall, verdict func(id string) string) FactCheck {
	t.Helper()
	intake := call.citable("intake")
	out := FactCheck{Subject: call.citable("facts under review"), Items: []FactVerdict{}, Warnings: []Warning{}, Gaps: []Gap{}}
	if call.Task.Judge == nil {
		t.Fatal("fact check request has no judge list")
	}
	for _, id := range *call.Task.Judge {
		out.Items = append(out.Items, FactVerdict{ID: id, Verdict: verdict(id), Reason: "checked against the cited source", Basis: []Evidence{cite(intake, "page-0")}})
	}
	return out
}

func s0Workflow(t *testing.T, source string, out *S0, retries int) engine.Workflow {
	return func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
		skills, err := PrepareSkills(ctx, r, r.Root(), source)
		if err != nil {
			return engine.Result{}, err
		}
		model := runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}
		*out, err = runS0(ctx, r, skills, S0Models{Intake: model, Facts: model, Validator: model}, retries)
		if err != nil {
			return engine.Result{}, err
		}
		return engine.Result{Outputs: map[string]contract.Ref{"status": out.Status}, Final: &engine.FinalSelection{Output: "status"}}, nil
	}
}

func decodeRef[T any](t *testing.T, r *engine.Run, ref contract.Ref) T {
	t.Helper()
	raw, err := os.ReadFile(ref.Path)
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Data T `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	return env.Data
}

// firstRound holds round 1's facts Ref, captured from the fact-check that
// judged it.
var firstRound struct{ facts contract.Ref }

func TestS0(t *testing.T) {
	inferredPop := func(id string) string {
		if id == "pop" {
			return "unsupported"
		}
		return "supported"
	}
	allSupported := func(string) string { return "supported" }
	for _, tc := range []struct {
		name    string
		retries int
		verdict func(id string) string
		agent   func(t *testing.T, call agentCall, round int) bool
		roles   string
		gaps    string
		failure string
	}{
		{name: "all facts accepted", retries: 1, verdict: allSupported, roles: "intake facts fact-check"},
		{name: "a rejected fact is corrected on retry", retries: 1, verdict: inferredPop, roles: "intake facts fact-check facts fact-check",
			agent: func(t *testing.T, call agentCall, round int) bool {
				if call.Role != "facts" || round != 2 {
					return false
				}
				fb := call.Request.Feedback
				if fb == nil || !strings.Contains(fb.Message, "pop (unsupported)") || len(fb.Refs) != 2 || fb.Refs[0] != firstRound.facts || fb.Refs[1].SchemaID != FactCheckSchema {
					t.Fatalf("retry feedback = %+v, want the rejected item and round 1's exact facts and check Refs %+v", fb, firstRound)
				}
				if check := decodeRef[FactCheck](t, nil, fb.Refs[1]); check.Subject != firstRound.facts {
					t.Errorf("retry feedback check judged %+v, not round 1's facts", check.Subject)
				}
				f := factsFor(call)
				f.Facts = f.Facts[:2]
				call.reply(t, f, nil)
				return true
			}},
		{name: "a fact still rejected after the retries is absent with a gap", retries: 1, verdict: inferredPop, roles: "intake facts fact-check facts fact-check", gaps: "not-accepted-pop:Item pop was judged unsupported "},
		{name: "an undecidable fact is absent with a gap", retries: 0, verdict: func(id string) string {
			if id == "orgkey" {
				return "insufficient"
			}
			return "supported"
		}, roles: "intake facts fact-check", gaps: "not-accepted-orgkey:Item orgkey was judged insuffici"},
		{name: "missing identity and time are gaps, not failures", retries: 0, verdict: allSupported, roles: "intake facts fact-check",
			agent: func(t *testing.T, call agentCall, round int) bool {
				if call.Role != "facts" {
					return false
				}
				f := factsFor(call)
				f.Facts, f.TimeAnchors = []Fact{}, []TimeAnchor{}
				f.Gaps = []Gap{{ID: "no-pop", Text: "No PoP candidate in any text source"}, {ID: "no-time", Text: "No incident timestamp with a zone"}}
				call.reply(t, f, nil)
				return true
			}},
		{name: "a bad citation is repaired in the same session", retries: 0, verdict: allSupported, roles: "intake facts facts fact-check",
			agent: func(t *testing.T, call agentCall, round int) bool {
				if call.Role != "facts" || call.Request.Feedback != nil {
					return false
				}
				f := factsFor(call)
				f.Facts[0].Evidence = []Evidence{{FileID: "page-0"}}
				call.reply(t, f, nil)
				return true
			}},
		{name: "an intake whose completeness the raw sources contradict fails after repair", retries: 0, roles: "intake intake", failure: "complete/gaps",
			agent: func(t *testing.T, call agentCall, round int) bool {
				if call.Role != "intake" {
					return false
				}
				v, files := intakeFiles()
				delete(files, "page-1")
				v.Comments = v.Comments[:1]
				call.reply(t, v, files)
				return true
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			skills := newSkillFixture(t)
			var out S0
			rounds := map[string]int{}
			sessions := map[string][]string{}
			defer func() { firstRound.facts = contract.Ref{} }()
			res := runHarness(t, "CASE-17 pop=pop-a please check", s0Workflow(t, skills.source, &out, tc.retries), func(t *testing.T, call agentCall) string {
				repair := call.Request.Feedback != nil && strings.HasPrefix(call.Request.Feedback.Message, "Previous contract")
				if !repair {
					rounds[call.Role]++
				}
				if !strings.HasPrefix(call.Task.Requirements, baselineRequirements) {
					t.Errorf("%s request does not start with the environment overrides", call.Role)
				}
				sessions[call.Role] = append(sessions[call.Role], call.Request.Identity.InvocationID)
				if call.Role == "fact-check" {
					var labels []string
					for _, in := range call.Task.Citable {
						labels = append(labels, in.Label)
					}
					if strings.Join(labels, ",") != "intake,caller prompt,facts under review" || len(call.Request.Inputs) != 3 || call.Request.Feedback != nil {
						t.Errorf("validator sees %v, %d inputs, feedback %v; want only the evidence and the facts", labels, len(call.Request.Inputs), call.Request.Feedback)
					}
					if want := judgedIDs(decodeRef[Facts](t, nil, call.citable("facts under review"))); call.Task.Judge == nil || strings.Join(*call.Task.Judge, ",") != strings.Join(want, ",") {
						t.Errorf("validator is asked to judge %v, want %v", call.Task.Judge, want)
					}
				}
				if call.Role == "fact-check" && rounds["fact-check"] == 1 {
					firstRound.facts = call.citable("facts under review")
				}
				if tc.agent != nil && tc.agent(t, call, rounds[call.Role]) {
					return ""
				}
				switch call.Role {
				case "intake":
					v, files := intakeFiles()
					call.reply(t, v, files)
				case "facts":
					call.reply(t, factsFor(call), nil)
				case "fact-check":
					call.reply(t, checkFor(t, call, tc.verdict), nil)
				}
				return ""
			})
			if got := strings.Join(res.Roles, " "); got != tc.roles {
				t.Fatalf("dispatched roles = %q, want %q", got, tc.roles)
			}
			if tc.failure != "" {
				if res.Report.Outcome != engine.Failed || !strings.Contains(fmt.Sprint(res.Report.Failure), tc.failure) {
					t.Fatalf("outcome = %s failure = %v, want failure mentioning %q", res.Report.Outcome, res.Report.Failure, tc.failure)
				}
				return
			}
			if res.Report.Outcome != engine.Succeeded {
				t.Fatalf("outcome = %s: %v", res.Report.Outcome, res.Report.Failure)
			}
			status := decodeRef[FactStatus](t, res.Run, out.Status)
			var gaps []string
			for _, g := range status.Gaps {
				gaps = append(gaps, g.ID+":"+g.Text[:min(len(g.Text), 32)])
			}
			if strings.Join(gaps, " ") != tc.gaps {
				t.Fatalf("status gaps = %v, want %q", gaps, tc.gaps)
			}
			if status.Facts != out.Facts || status.Check != out.Check || !res.Run.ControllerAttached(out.Status) || !res.Run.ControllerAttached(out.Prompt) {
				t.Fatalf("status record does not bind the final facts and check: %+v", status)
			}
			if tc.name == "a bad citation is repaired in the same session" {
				if ids := sessions["facts"]; len(ids) != 2 {
					t.Fatalf("facts dispatches = %v", ids)
				}
				snapshot := res.Report.Snapshot
				var factsSessions []string
				for _, a := range snapshot.Attempts {
					if a.Key == "facts" || strings.HasSuffix(a.Scope, "contract-repair-facts") {
						factsSessions = append(factsSessions, a.HandleID)
					}
				}
				if len(factsSessions) != 2 || factsSessions[0] != factsSessions[1] {
					t.Fatalf("repair did not reuse the session: %v", factsSessions)
				}
			}
			prompt := decodeRef[CallerPrompt](t, res.Run, out.Prompt)
			if prompt.Ticket != "CASE-17" || prompt.Hints != "pop=pop-a please check" {
				t.Fatalf("caller prompt record = %+v", prompt)
			}
		})
	}
}

func TestS0RejectsAPromptWithoutTicket(t *testing.T) {
	skills := newSkillFixture(t)
	var out S0
	res := runHarness(t, "please check the incident", s0Workflow(t, skills.source, &out, 0), func(t *testing.T, call agentCall) string {
		t.Errorf("dispatched %s for a prompt without a ticket", call.Role)
		return ""
	})
	if res.Report.Outcome != engine.Failed || !strings.Contains(fmt.Sprint(res.Report.Failure), "must start with a ticket key") || len(res.Report.Snapshot.Sessions) != 0 {
		t.Fatalf("outcome = %s failure = %v sessions = %d", res.Report.Outcome, res.Report.Failure, len(res.Report.Snapshot.Sessions))
	}
}
