package triagev2

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// investigatorCall is one investigator dispatch: the round it serves and
// whether it repairs a rejected contract.
type investigatorCall struct {
	agentCall
	round  int
	repair bool
}

func (c investigatorCall) round0(t *testing.T, change func(*Round)) {
	t.Helper()
	v := baseRound(c.citable("intake"))
	if change != nil {
		change(&v)
	}
	c.reply(t, v, roundFiles())
}

func stuck(v *Round) { v.Status = "stuck" }

func TestRounds(t *testing.T) {
	model := runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}
	policy := RoundPolicy{MaxRounds: 3, MaxRejections: 2, TimeoutRetries: 1, RoundTimeout: 30 * time.Second, CheckTimeout: 30 * time.Second, HandoffPercent: 80,
		Vision: VisionPolicy{Model: model, MaxSteps: 2, Parallel: 2, Timeout: 30 * time.Second}}
	allSupported := func(string) string { return "supported" }
	for _, tc := range []struct {
		name    string
		policy  func(*RoundPolicy)
		percent *float64
		unknown bool
		verdict func(id string) string
		agent   func(t *testing.T, c investigatorCall) string
		checker func(t *testing.T, call agentCall, n int) string
		facts   func(*Facts, agentCall)
		vision  func(t *testing.T, call agentCall) string
		run     func(*engine.RunPolicy)
		within  time.Duration
		roles   string
		check   func(t *testing.T, out Rounds, calls []investigatorCall, handles []string)
		visions func(t *testing.T, out Rounds, calls []investigatorCall, visions []agentCall)
	}{
		{name: "a confirmed home stack opens runtime and a candidate ends the rounds", verdict: allSupported,
			roles: "intake facts fact-check investigator fact-check investigator",
			agent: func(t *testing.T, c investigatorCall) string {
				if c.round == 1 {
					c.round0(t, nil)
					return ""
				}
				c.round0(t, func(v *Round) {
					v.Status, v.FactsUpdate, v.Identity, v.Receipts = "candidate", []Fact{}, nil, []Receipt{}
					v.DeployedBuilds, v.Candidate = deployedBuild(), codeClaim("b1", nil)
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, handles []string) {
				first, second := calls[0].Task, calls[1].Task
				if !strings.Contains(first.Requirements, identityOnlyRequirements) || len(first.Skills) != 2 || !strings.HasSuffix(first.Skills[0], "identity/SKILL.md") || first.Round != 1 {
					t.Errorf("identity round task = skills %v round %d", first.Skills, first.Round)
				}
				if strings.Contains(second.Requirements, identityOnlyRequirements) || !strings.Contains(second.Requirements, `home stack "pop-a"`) || !strings.HasSuffix(second.Skills[0], "investigator/SKILL.md") || second.Round != 2 {
					t.Errorf("second round task = skills %v round %d", second.Skills, second.Round)
				}
				var labels []string
				for _, in := range second.Citable {
					labels = append(labels, in.Label)
				}
				if strings.Join(labels, ",") != "caller prompt,intake,facts,fact check,fact status,round 1,round 1 fact check,round 1 fact status" {
					t.Errorf("second round inputs = %v", labels)
				}
				if calls[1].Request.Feedback != nil {
					t.Errorf("second round feedback = %+v, want none", calls[1].Request.Feedback)
				}
				if out.HomeStack != "pop-a" || len(out.Records) != 2 || out.Records[1].Check != (contract.Ref{}) || out.Last.Status != "candidate" || out.Limit != "" {
					t.Errorf("rounds = %+v", out)
				}
				if len(handles) != 2 || handles[0] != handles[1] {
					t.Errorf("round sessions = %v, want one reused session below the threshold", handles)
				}
			}},
		{name: "a rejected home stack keeps runtime closed until it is recorded absent", verdict: func(id string) string {
			if id == "d-pop" {
				return "unsupported"
			}
			return "supported"
		}, roles: "intake facts fact-check investigator fact-check investigator fact-check investigator investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				switch {
				case c.round == 3 && c.repair:
					if !strings.Contains(c.Request.Feedback.Message, `confirming home_pop "pop-a" was not accepted too often`) {
						t.Errorf("repair feedback = %q", c.Request.Feedback.Message)
					}
					c.round0(t, func(v *Round) {
						stuck(v)
						v.Identity.Decisions = v.Identity.Decisions[1:]
						v.FactsUpdate = []Fact{}
					})
				default:
					c.round0(t, nil)
				}
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, handles []string) {
				for _, c := range calls[1:] {
					if !strings.Contains(c.Task.Requirements, identityOnlyRequirements) {
						t.Errorf("round %d is not identity-only", c.round)
					}
				}
				if fb := calls[1].Request.Feedback; fb == nil || !strings.Contains(fb.Message, "d-pop (unsupported)") || len(fb.Refs) != 3 || fb.Refs[0] != out.Records[0].Round || fb.Refs[1] != out.Records[0].Check {
					t.Errorf("round 2 feedback = %+v", fb)
				}
				if fb := calls[2].Request.Feedback; fb == nil || !strings.Contains(fb.Message, "recorded absent in the round 2 fact status") {
					t.Errorf("round 3 feedback = %+v", fb)
				}
				status := decodeRef[FactStatus](t, nil, out.Records[1].Status)
				if len(status.Gaps) != 1 || status.Gaps[0].ID != "not-accepted-r2-d-pop" || !strings.Contains(status.Gaps[0].Text, "2 times") {
					t.Errorf("round 2 status gaps = %+v", status.Gaps)
				}
				if first := decodeRef[FactStatus](t, nil, out.Records[0].Status); len(first.Gaps) != 0 || first.Facts != out.Records[0].Round {
					t.Errorf("round 1 status = %+v", first)
				}
				if out.HomeStack != "" || len(out.Records) != 3 || out.Last.Status != "stuck" {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "a conflict note beside the confirmation does not close runtime, in either order", verdict: func(id string) string {
			if id == "d-note" {
				return "unsupported"
			}
			return "supported"
		},
			roles: "intake facts fact-check investigator fact-check investigator fact-check investigator",
			agent: func(t *testing.T, c investigatorCall) string {
				conflict := Decision{ID: "d-note", Fact: "home_pop", Value: ptr("pop-b"), Status: "conflict", Identifiers: []string{}, Reason: "the ticket says pop-b; the DB wins"}
				c.round0(t, func(v *Round) {
					switch c.round {
					case 1:
						v.Identity.Decisions = append(v.Identity.Decisions, conflict)
					case 2:
						v.Identity.Decisions = append([]Decision{conflict}, v.Identity.Decisions...)
					default:
						stuck(v)
						v.Identity, v.FactsUpdate = nil, []Fact{}
					}
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				for _, c := range calls[1:] {
					if !strings.Contains(c.Task.Requirements, `home stack "pop-a"`) {
						t.Errorf("round %d runtime is not open for pop-a", c.round)
					}
					if c.Request.Feedback != nil {
						t.Errorf("round %d feedback = %q; the unconfirmed note must not be judged", c.round, c.Request.Feedback.Message)
					}
				}
			}},
		{name: "a timed-out fact check reruns within its own timeout", verdict: allSupported, policy: func(p *RoundPolicy) { p.CheckTimeout = time.Second }, within: 20 * time.Second,
			roles: "intake facts fact-check investigator fact-check fact-check",
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, stuck); return "" },
			checker: func(t *testing.T, call agentCall, n int) string {
				if n == 1 {
					return "hold"
				}
				if call.Request.Feedback == nil || call.Request.Feedback.SourceCode != string(engine.TimedOut) {
					t.Errorf("fact check rerun feedback = %+v", call.Request.Feedback)
				}
				call.reply(t, checkFor(t, call, func(string) string { return "supported" }), nil)
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if len(out.Recoveries) != 1 || out.Recoveries[0].Stage != "fact-check" || out.Recoveries[0].Code != engine.TimedOut || out.Records[0].Check == (contract.Ref{}) {
					t.Errorf("recoveries = %+v", out.Recoveries)
				}
			}},
		{name: "a run budget that cannot cover the first round runs none", verdict: allSupported, run: func(p *engine.RunPolicy) { p.MaxTotalAttempts = 14 },
			roles: "intake facts fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				t.Error("dispatched a round past the budget")
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if out.Limit != LimitRun || len(out.Records) != 0 {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "two rejected items with one key count one rejection", verdict: func(id string) string {
			if strings.HasPrefix(id, "host") {
				return "unsupported"
			}
			return "supported"
		}, policy: func(p *RoundPolicy) { p.MaxRounds = 1 }, roles: "intake facts fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					twin := v.FactsUpdate[0]
					twin.ID = "host-2"
					v.FactsUpdate = append(v.FactsUpdate, twin)
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if status := decodeRef[FactStatus](t, nil, out.Records[0].Status); len(status.Gaps) != 0 {
					t.Errorf("one round reached the limit of 2 with a repeated key: %+v", status.Gaps)
				}
			}},
		{name: "a run budget that cannot cover another round ends the rounds", verdict: allSupported, run: func(p *engine.RunPolicy) { p.MaxTotalAttempts = 17 },
			roles: "intake facts fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, nil); return "" },
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if out.Limit != LimitRun || len(out.Records) != 1 || out.Last.Status != "continue" {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "a later unconfirmed home stack closes runtime again", verdict: allSupported,
			roles: "intake facts fact-check investigator fact-check investigator fact-check investigator",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					switch c.round {
					case 2:
						v.Identity.Decisions[0] = Decision{ID: "d-pop", Fact: "home_pop", Status: "unconfirmed", Identifiers: []string{}, Reason: "a second stack now has the same row"}
					case 3:
						stuck(v)
						v.Identity, v.FactsUpdate = nil, []Fact{}
					}
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				if !strings.Contains(calls[1].Task.Requirements, `home stack "pop-a"`) || !strings.Contains(calls[2].Task.Requirements, identityOnlyRequirements) || out.HomeStack != "" {
					t.Errorf("runtime did not close after the unconfirmed decision: home %q", out.HomeStack)
				}
			}},
		{name: "a rejected confirmation keeps the earlier home stack", verdict: func(id string) string {
			if id == "d-pop-b" {
				return "unsupported"
			}
			return "supported"
		}, roles: "intake facts fact-check investigator fact-check investigator fact-check investigator",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					switch c.round {
					case 2:
						v.Identity.Lookups[1].Rows = []Row{{TenantID: ptr("17"), Orgkey: ptr("org-17")}}
						v.Identity.Decisions[0] = Decision{ID: "d-pop-b", Fact: "home_pop", Value: ptr("pop-b"), Status: "confirmed", Lookup: ptr("l-b"), Row: ptr(0), Identifiers: []string{"orgkey"}, Reason: "row on pop-b"}
					case 3:
						stuck(v)
						v.Identity, v.FactsUpdate = nil, []Fact{}
					}
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				if !strings.Contains(calls[2].Task.Requirements, `home stack "pop-a"`) || out.HomeStack != "pop-a" {
					t.Errorf("home = %q after a rejected confirmation of another stack", out.HomeStack)
				}
			}},
		{name: "a blocked round ends the rounds", verdict: allSupported, roles: "intake facts fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) { v.Status, v.Unblock = "blocked", ptr("a kubeconfig for pop-c") })
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if out.Last.Status != "blocked" || out.Limit != "" || len(out.Records) != 1 {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "an image the facts ask about is read before the identity round", verdict: allSupported,
			roles: "intake facts fact-check vision investigator fact-check",
			facts: func(f *Facts, call agentCall) {
				f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: ptr(call.citable("intake")), FileID: "bundle"}, Question: "which host does the screenshot show"}}
			},
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, stuck); return "" },
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {},
			visions: func(t *testing.T, out Rounds, calls []investigatorCall, visions []agentCall) {
				v := visions[0].Task
				if v.Vision == nil || v.Vision.Question != "which host does the screenshot show" || v.Vision.Image.FileID != "bundle" || len(v.Skills) != 0 || !strings.Contains(v.Requirements, visionRequirements) || !strings.HasPrefix(v.Requirements, baselineRequirements) {
					t.Errorf("vision task = %+v", v)
				}
				if labels := labelsOf(calls[0].Task); !strings.Contains(labels, "vision status of facts,vision shot of facts") {
					t.Errorf("identity round inputs = %s", labels)
				}
			}},
		{name: "vision requests beyond the budget become gaps", verdict: allSupported,
			roles: "intake facts fact-check investigator fact-check vision vision investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					if c.round == 1 {
						for _, id := range []string{"a", "b", "c"} {
							v.VisionRequests = append(v.VisionRequests, VisionRequest{ID: "shot-" + id, Attachment: Evidence{Ref: ptr(c.citable("intake")), FileID: "bundle"}, Question: "what does it show"})
						}
						return
					}
					stuck(v)
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				var status contract.Ref
				for _, in := range calls[1].Task.Citable {
					if in.Label == "vision status of round 1" {
						status = in.Ref
					}
				}
				batch := decodeRef[VisionBatch](t, nil, status)
				if len(batch.Results) != 2 || len(batch.Gaps) != 1 || batch.Gaps[0].ID != "vision-not-run-shot-c" || batch.Owner != out.Records[0].Round {
					t.Errorf("vision batch = %+v", batch)
				}
			}},
		{name: "a vision transcript that is not its own file is repaired", verdict: allSupported,
			roles: "intake facts fact-check vision vision investigator fact-check",
			facts: func(f *Facts, call agentCall) {
				f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: ptr(call.citable("intake")), FileID: "bundle"}, Question: "what does it show"}}
			},
			vision: func(t *testing.T, call agentCall) string {
				if call.Request.Feedback == nil {
					transcribe(t, call, func(v *Vision) { v.Transcript.Ref = ptr(call.citable("request owner")) })
					return ""
				}
				if !strings.Contains(call.Request.Feedback.Message, "transcript.ref") {
					t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
				}
				transcribe(t, call, nil)
				return ""
			},
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, stuck); return "" },
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {}},
		{name: "a timed-out round reruns in a fresh session from the same inputs", verdict: allSupported, policy: func(p *RoundPolicy) { p.RoundTimeout = time.Second },
			roles: "intake facts fact-check investigator investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				if c.Request.Feedback == nil {
					return "hold"
				}
				if c.Request.Feedback.SourceCode != string(engine.TimedOut) || !strings.HasPrefix(c.Request.Feedback.Message, fmt.Sprintf(rerunNote, engine.TimedOut)) {
					t.Errorf("rerun feedback = %+v, want the timeout diagnostic", c.Request.Feedback)
				}
				c.round0(t, stuck)
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, handles []string) {
				if len(out.Recoveries) != 1 || out.Recoveries[0].Code != engine.TimedOut || len(handles) != 2 || handles[0] == handles[1] {
					t.Errorf("recoveries = %+v handles = %v", out.Recoveries, handles)
				}
				if a, b := calls[0].Task.Citable, calls[1].Task.Citable; len(a) != len(b) || a[len(a)-1] != b[len(b)-1] {
					t.Errorf("rerun inputs differ: %v vs %v", a, b)
				}
			}},
		{name: "context usage at the threshold hands off to a fresh session", verdict: allSupported, percent: ptr(95.0),
			roles: "intake facts fact-check investigator fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				if c.round == 2 {
					c.round0(t, stuck)
					return ""
				}
				c.round0(t, nil)
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, handles []string) {
				if fb := calls[1].Request.Feedback; fb == nil || !strings.Contains(fb.Message, "reached 95% context usage") {
					t.Errorf("round 2 feedback = %+v", fb)
				}
				if len(handles) != 2 || handles[0] == handles[1] {
					t.Errorf("round sessions = %v, want a fresh session after the handoff", handles)
				}
			}},
		{name: "unknown context usage keeps the session and says so", verdict: allSupported, unknown: true,
			roles: "intake facts fact-check investigator fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				if c.round == 2 {
					c.round0(t, stuck)
					return ""
				}
				c.round0(t, nil)
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, handles []string) {
				if fb := calls[1].Request.Feedback; fb == nil || !strings.Contains(fb.Message, "Context usage is unknown") {
					t.Errorf("round 2 feedback = %+v", fb)
				}
				if len(handles) != 2 || handles[0] != handles[1] {
					t.Errorf("round sessions = %v", handles)
				}
			}},
		{name: "the round limit ends a continuing investigation", verdict: allSupported, policy: func(p *RoundPolicy) { p.MaxRounds = 1 },
			roles: "intake facts fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, nil); return "" },
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if out.Limit != LimitRounds || len(out.Records) != 1 || out.Last.Status != "continue" {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "an identity value that does not echo its row is repaired in the same session", verdict: allSupported,
			roles: "intake facts fact-check investigator investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				if !c.repair {
					c.round0(t, func(v *Round) { stuck(v); v.Identity.Decisions[0].Value = ptr("pop-b") })
					return ""
				}
				if !strings.Contains(c.Request.Feedback.Message, `want "pop-a", the stack of lookup "l-a" row 0`) {
					t.Errorf("repair feedback = %q", c.Request.Feedback.Message)
				}
				c.round0(t, stuck)
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, handles []string) {
				if len(handles) != 2 || handles[0] != handles[1] || out.HomeStack != "pop-a" {
					t.Errorf("handles = %v home = %q", handles, out.HomeStack)
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saved := harnessPercent
			defer func() { harnessPercent = saved }()
			if tc.percent != nil {
				harnessPercent = tc.percent
			}
			if tc.unknown {
				harnessPercent = nil
			}
			p := policy
			if tc.policy != nil {
				tc.policy(&p)
			}
			skills := newSkillFixture(t)
			var out Rounds
			var calls []investigatorCall
			checks := 0
			var visions []agentCall
			started := time.Now()
			harnessPolicy = tc.run
			defer func() { harnessPolicy = nil }()
			res := runHarness(t, "CASE-17 pop=pop-a", func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
				sk, err := PrepareSkills(ctx, r, r.Root(), skills.source)
				if err != nil {
					return engine.Result{}, err
				}
				s0, err := runS0(ctx, r, sk, S0Models{Intake: model, Facts: model, Validator: model}, 0)
				if err != nil {
					return engine.Result{}, err
				}
				out, err = runRounds(ctx, r, sk, s0, RoundModels{Investigator: model, Validator: model}, p)
				if err != nil {
					return engine.Result{}, err
				}
				// Every session is closed by the rounds themselves, not
				// by the run's final cleanup.
				for id, s := range r.Snapshot().Sessions {
					if s.State != "Closed" {
						t.Errorf("session %s is %s when the rounds return", id, s.State)
					}
				}
				if len(out.Records) == 0 {
					return engine.Result{}, nil
				}
				last := out.Records[len(out.Records)-1].Round
				return engine.Result{Outputs: map[string]contract.Ref{"round": last}, Final: &engine.FinalSelection{Output: "round"}}, nil
			}, func(t *testing.T, call agentCall) string {
				switch call.Role {
				case "intake":
					v, files := intakeFiles()
					call.reply(t, v, files)
				case "facts":
					f := factsFor(call)
					if tc.facts != nil {
						tc.facts(&f, call)
					}
					call.reply(t, f, nil)
				case "vision":
					visions = append(visions, call)
					if tc.vision != nil {
						return tc.vision(t, call)
					}
					transcribe(t, call, nil)
				case "fact-check":
					if call.Task.Round > 0 {
						checks++
					}
					if tc.checker != nil && call.Task.Round > 0 {
						return tc.checker(t, call, checks)
					}
					call.reply(t, checkFor(t, call, tc.verdict), nil)
				case "investigator":
					c := investigatorCall{agentCall: call, round: call.Task.Round, repair: call.Request.Feedback != nil && strings.HasPrefix(call.Request.Feedback.Message, "Previous contract")}
					calls = append(calls, c)
					return tc.agent(t, c)
				}
				return ""
			})
			if res.Report.Outcome != engine.Succeeded {
				t.Fatalf("outcome = %s: %v", res.Report.Outcome, res.Report.Failure)
			}
			if tc.within > 0 && time.Since(started) > tc.within {
				t.Errorf("took %s, want under %s", time.Since(started), tc.within)
			}
			if got := strings.Join(res.Roles, " "); got != tc.roles {
				t.Fatalf("dispatched roles = %q, want %q", got, tc.roles)
			}
			var handles []string
			var rounds []engine.AttemptState
			for _, a := range res.Report.Snapshot.Attempts {
				if a.Key == "round" {
					rounds = append(rounds, a)
				}
			}
			slices.SortFunc(rounds, func(a, b engine.AttemptState) int { return a.StartedAt.Compare(b.StartedAt) })
			for _, a := range rounds {
				handles = append(handles, a.HandleID)
			}
			for _, s := range res.Report.Snapshot.Sessions {
				if s.State != "Closed" {
					t.Errorf("session %s left %s", s.ID, s.State)
				}
			}
			tc.check(t, out, calls, handles)
			if tc.visions != nil {
				tc.visions(t, out, calls, visions)
			}
		})
	}
}

func TestRoundFits(t *testing.T) {
	p := RoundPolicy{TimeoutRetries: 1}
	snapshot := func(sessions, attempts int) engine.Snapshot {
		s := engine.Snapshot{Policy: engine.RunPolicy{MaxTotalSessions: 10, MaxTotalAttempts: 20}, Sessions: map[string]engine.SessionStatus{}, Attempts: map[string]engine.AttemptState{}}
		for i := range sessions {
			s.Sessions[fmt.Sprint(i)] = engine.SessionStatus{}
		}
		for i := range attempts {
			s.Attempts[fmt.Sprint(i)] = engine.AttemptState{}
		}
		return s
	}
	for _, tc := range []struct {
		sessions, attempts int
		fits               bool
	}{{6, 11, true}, {7, 11, false}, {6, 12, false}} {
		if got := roundFits(snapshot(tc.sessions, tc.attempts), p); got != tc.fits {
			t.Errorf("roundFits(%d sessions, %d attempts) = %v, want %v", tc.sessions, tc.attempts, got, tc.fits)
		}
	}
}

func TestCloseFailure(t *testing.T) {
	classified := &engine.Failure{Code: engine.TimedOut, Origin: engine.OriginRunDeadline}
	for _, tc := range []struct {
		name string
		err  error
		code engine.Code
	}{
		{"a classified failure stays", classified, engine.TimedOut},
		{"a bare cancellation is a cancellation", context.Canceled, engine.Cancelled},
		{"running out of cleanup time is a cleanup failure", context.DeadlineExceeded, engine.CleanupFailed},
		{"anything else is a cleanup failure", fmt.Errorf("close: broken pipe"), engine.CleanupFailed},
	} {
		var f *engine.Failure
		err := closeFailure(tc.err, "triage-probe", "h1")
		if !errors.As(err, &f) || f.Code != tc.code || f.Code != engine.TimedOut && f.HandleID != "h1" {
			t.Errorf("%s: closeFailure = %+v", tc.name, f)
		}
	}
}

// transcribe is the fake vision agent: it answers the request it was given.
func transcribe(t *testing.T, call agentCall, change func(*Vision)) {
	t.Helper()
	v := Vision{Request: call.Task.Vision.Request, Image: call.Task.Vision.Image, Transcript: Evidence{FileID: "transcript"}, Answer: "a host name", Gaps: []Gap{}}
	if change != nil {
		change(&v)
	}
	call.reply(t, v, map[string][]byte{"transcript": []byte("host acme.example.invalid\n")})
}

func labelsOf(t task) string {
	var labels []string
	for _, in := range t.Citable {
		labels = append(labels, in.Label)
	}
	return strings.Join(labels, ",")
}
