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

// ends makes a round end the investigation: blocked, with what would
// unblock it.
func ends(v *Round) { v.Status, v.Unblock = "blocked", ptr("a kubeconfig for pop-c") }

func TestRounds(t *testing.T) {
	model := runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}
	policy := RoundPolicy{MaxRounds: 3, MaxRejections: 2, MaxChallenges: 2, TimeoutRetries: 1, RoundTimeout: 30 * time.Second, CheckTimeout: 30 * time.Second, StewardTimeout: 30 * time.Second, HandoffPercent: 80,
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
		auditor func(t *testing.T, call agentCall) string
		steward func(t *testing.T, call agentCall) string
		facts   func(*Facts, agentCall)
		vision  func(t *testing.T, call agentCall) string
		run     func(*engine.RunPolicy)
		within  time.Duration
		fails   string
		roles   string
		// stewards lists the steward triggers in order; empty means T1.
		stewards string
		check    func(t *testing.T, out Rounds, calls []investigatorCall, handles []string)
	}{
		{name: "a confirmed home stack opens runtime and a candidate the steward passes ends the rounds", verdict: allSupported, stewards: "T1 T2a",
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
				if strings.Join(labels, ",") != "caller prompt,intake,facts,fact check,fact status,round 1,round 1 fact check,round 1 fact status,round 1 audit,steward T1 after round 1" {
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
			steward: func(t *testing.T, call agentCall) string {
				if !strings.Contains(call.Task.Requirements, stewardUnconfirmed) {
					t.Errorf("T1 requirements do not flag the unconfirmed home stack")
				}
				call.reply(t, stewardVerdict(call, "pass"), nil)
				return ""
			},
			agent: func(t *testing.T, c investigatorCall) string {
				switch {
				case c.round == 3 && c.repair:
					if !strings.Contains(c.Request.Feedback.Message, `confirming home_pop "pop-a" was not accepted too often`) {
						t.Errorf("repair feedback = %q", c.Request.Feedback.Message)
					}
					c.round0(t, func(v *Round) {
						ends(v)
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
				if out.HomeStack != "" || len(out.Records) != 3 || out.Last.Status != "blocked" {
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
						ends(v)
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
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, ends); return "" },
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
		{name: "a run budget that cannot cover the first round runs none", verdict: allSupported, run: func(p *engine.RunPolicy) { p.MaxTotalAttempts = 23 },
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
		{name: "a run budget that cannot cover another round ends the rounds", verdict: allSupported, run: func(p *engine.RunPolicy) { p.MaxTotalAttempts = 24 },
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
						ends(v)
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
						ends(v)
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
		{name: "an image the facts ask about is read before the identity round and its transcript is judged", verdict: allSupported,
			roles: "intake facts fact-check vision investigator fact-check",
			facts: func(f *Facts, call agentCall) {
				f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: ptr(call.citable("intake")), FileID: "bundle"}, Question: "which host does the screenshot show"}}
			},
			vision: func(t *testing.T, call agentCall) string {
				v := call.Task
				if v.Vision == nil || v.Vision.Question != "which host does the screenshot show" || v.Vision.Image.FileID != "bundle" || *v.Vision.Image.Ref != call.citable("image owner") || len(v.Skills) != 0 || !strings.Contains(v.Requirements, visionRequirements) || !strings.HasPrefix(v.Requirements, baselineRequirements) {
					t.Errorf("vision task = %+v", v)
				}
				transcribe(t, call, nil)
				return ""
			},
			agent: func(t *testing.T, c investigatorCall) string {
				shot := c.citable("vision shot of facts")
				c.round0(t, func(v *Round) {
					ends(v)
					v.FactsUpdate[0].Evidence = []Evidence{citeRange(shot, "transcript", 5, 20)}
				})
				return ""
			},
			checker: func(t *testing.T, call agentCall, _ int) string {
				if labels := labelsOf(call.Task); !strings.Contains(labels, "cited by the facts under review") {
					t.Errorf("fact check inputs = %s; want the cited transcript", labels)
				}
				call.reply(t, checkFor(t, call, func(string) string { return "supported" }), nil)
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				if labels := labelsOf(calls[0].Task); !strings.Contains(labels, "vision status of facts,vision shot of facts") || len(out.Vision) != 1 {
					t.Errorf("identity round inputs = %s, batches %v", labels, out.Vision)
				}
			}},
		{name: "vision requests beyond the budget become gaps", verdict: allSupported,
			roles: "intake facts fact-check investigator fact-check vision vision investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					if c.round == 1 {
						for _, id := range []string{"a", "b", "c"} {
							v.VisionRequests = append(v.VisionRequests, VisionRequest{ID: "shot-" + id, Attachment: Evidence{Ref: ptr(c.citable("intake")), FileID: "bundle"}, Question: "what is shot " + id})
						}
						return
					}
					ends(v)
				})
				return ""
			},
			vision: func(t *testing.T, call agentCall) string {
				transcribe(t, call, func(v *Vision) { v.Answer = call.Task.Vision.Question })
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				batch := decodeRef[VisionBatch](t, nil, calls[1].citable("vision status of round 1"))
				if len(batch.Results) != 2 || len(batch.Gaps) != 1 || batch.Gaps[0].ID != "vision-not-run-shot-c" || batch.Owner != out.Records[0].Round {
					t.Fatalf("vision batch = %+v", batch)
				}
				for _, res := range batch.Results {
					if v := decodeRef[Vision](t, nil, res.Vision); v.Request.ID != res.ID || v.Answer != "what is shot "+strings.TrimPrefix(res.ID, "shot-") {
						t.Errorf("result %s binds vision for %s", res.ID, v.Request.ID)
					}
				}
			}},
		{name: "vision requests of the last round become gaps", verdict: allSupported, policy: func(p *RoundPolicy) { p.MaxRounds = 1 },
			roles: "intake facts fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					v.VisionRequests = []VisionRequest{{ID: "late", Attachment: Evidence{Ref: ptr(c.citable("intake")), FileID: "bundle"}, Question: "what does it show"}}
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if len(out.Vision) != 1 {
					t.Fatalf("batches = %v", out.Vision)
				}
				if batch := decodeRef[VisionBatch](t, nil, out.Vision[0]); len(batch.Results) != 0 || len(batch.Gaps) != 1 || out.Limit != LimitRounds {
					t.Errorf("vision batch = %+v", batch)
				}
			}},
		{name: "a vision Step that copies the image wrongly or not as its own transcript is repaired", verdict: allSupported,
			roles: "intake facts fact-check vision vision investigator fact-check",
			facts: func(f *Facts, call agentCall) {
				f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: ptr(call.citable("intake")), FileID: "bundle"}, Question: "which host does the screenshot show"}}
			},
			vision: func(t *testing.T, call agentCall) string {
				if call.Request.Feedback == nil {
					transcribe(t, call, func(v *Vision) { v.Image.Ref = nil })
					return ""
				}
				if !strings.Contains(call.Request.Feedback.Message, "image.ref: got null") {
					t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
				}
				transcribe(t, call, nil)
				return ""
			},
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, ends); return "" }},
		{name: "a vision Step that answers another request is repaired", verdict: allSupported,
			roles: "intake facts fact-check vision vision investigator fact-check",
			facts: func(f *Facts, call agentCall) {
				f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: ptr(call.citable("intake")), FileID: "bundle"}, Question: "what does it show"}}
			},
			vision: func(t *testing.T, call agentCall) string {
				if call.Request.Feedback == nil {
					transcribe(t, call, func(v *Vision) { v.Request.ID = "other" })
					return ""
				}
				if !strings.Contains(call.Request.Feedback.Message, `request.id: got "other"; want "shot"`) {
					t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
				}
				transcribe(t, call, nil)
				return ""
			},
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, ends); return "" }},
		{name: "a timed-out vision Step reruns", verdict: allSupported, roles: "intake facts fact-check vision vision investigator fact-check",
			facts: func(f *Facts, call agentCall) {
				f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: ptr(call.citable("intake")), FileID: "bundle"}, Question: "which host does the screenshot show"}}
			},
			policy: func(p *RoundPolicy) { p.Vision.Timeout = time.Second },
			vision: func(t *testing.T, call agentCall) string {
				if call.Request.Feedback == nil {
					return "hold"
				}
				transcribe(t, call, nil)
				return ""
			},
			agent: func(t *testing.T, c investigatorCall) string { c.round0(t, ends); return "" },
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if len(out.Recoveries) != 1 || out.Recoveries[0].Stage != "vision" || out.Recoveries[0].Code != engine.TimedOut {
					t.Errorf("recoveries = %+v", out.Recoveries)
				}
			}},
		{name: "vision requests beside an exactly covered round become gaps", verdict: allSupported, run: func(p *engine.RunPolicy) { p.MaxTotalAttempts = 24 },
			roles: "intake facts fact-check",
			facts: func(f *Facts, call agentCall) {
				f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: ptr(call.citable("intake")), FileID: "bundle"}, Question: "what does it show"}}
			},
			agent: func(t *testing.T, c investigatorCall) string {
				t.Error("dispatched a round past the budget")
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if len(out.Vision) != 1 || out.Limit != LimitRun {
					t.Fatalf("rounds = %+v", out)
				}
				if batch := decodeRef[VisionBatch](t, nil, out.Vision[0]); len(batch.Results) != 0 || len(batch.Gaps) != 1 {
					t.Errorf("vision batch = %+v", batch)
				}
			}},
		{name: "vision requests of a last round with little budget left become gaps", verdict: allSupported, policy: func(p *RoundPolicy) { p.MaxRounds = 1 }, run: func(p *engine.RunPolicy) { p.MaxTotalAttempts = 24 },
			roles: "intake facts fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					v.VisionRequests = []VisionRequest{{ID: "late", Attachment: Evidence{Ref: ptr(c.citable("intake")), FileID: "bundle"}, Question: "what does it show"}}
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if len(out.Vision) != 1 || out.Limit != LimitRounds {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "vision requests the run budget cannot cover leave the rounds to the budget check", verdict: allSupported, run: func(p *engine.RunPolicy) { p.MaxTotalAttempts = 6 },
			roles: "intake facts fact-check",
			facts: func(f *Facts, call agentCall) {
				f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: ptr(call.citable("intake")), FileID: "bundle"}, Question: "which host does the screenshot show"}}
			},
			agent: func(t *testing.T, c investigatorCall) string {
				t.Error("dispatched a round past the budget")
				return ""
			},
			check: func(t *testing.T, out Rounds, _ []investigatorCall, _ []string) {
				if out.Limit != LimitRun || len(out.Records) != 0 || len(out.Vision) != 0 {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "vision parallelism must leave a live session for the investigator", verdict: allSupported, run: func(p *engine.RunPolicy) { p.MaxLiveSessions = 2 },
			roles: "intake facts fact-check", fails: "leaves no live session for the investigator",
			agent: func(t *testing.T, c investigatorCall) string { return "" }},
		{name: "a T1 challenge on a receipt reaches the next round", verdict: allSupported,
			roles: "intake facts fact-check investigator fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					if c.round == 2 {
						ends(v)
					}
				})
				return ""
			},
			steward: func(t *testing.T, call agentCall) string {
				v := stewardVerdict(call, "challenge")
				switch {
				case call.Request.Feedback == nil:
					v.Items[0].Target.ID = ptr("nope")
				case !strings.Contains(call.Request.Feedback.Message, `has no item with id "nope"`):
					t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
				default:
					v.Items[0].Target.ID = ptr("q1")
				}
				call.reply(t, v, nil)
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				fb := calls[1].Request.Feedback
				if fb == nil || !strings.Contains(fb.Message, "The steward (T1) challenged: [ORCH-CB4]") || !strings.Contains(fb.Message, "item q1") || len(out.Stewards) != 1 || out.Stewards[0].Verdict != "challenge" || !slices.Contains(fb.Refs, out.Stewards[0].Ref) {
					t.Errorf("round 2 feedback = %+v", fb)
				}
			}},
		{name: "a T2a challenge sends the candidate back and the challenge limit ends the rounds", verdict: allSupported, stewards: "T1 T2a",
			policy: func(p *RoundPolicy) { p.MaxChallenges = 1 },
			roles:  "intake facts fact-check investigator investigator",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					v.Status, v.FactsUpdate, v.Identity = "candidate", []Fact{}, nil
					v.DeployedBuilds, v.Candidate = deployedBuild(), codeClaim("b1", nil)
				})
				return ""
			},
			steward: func(t *testing.T, call agentCall) string {
				verdict := "pass"
				if call.Task.Trigger == "T2a" {
					verdict = "challenge"
				}
				call.reply(t, stewardVerdict(call, verdict), nil)
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				if out.Limit != LimitChallenges || out.Pass != (contract.Ref{}) || !strings.Contains(calls[1].Request.Feedback.Message, "The steward (T2a) challenged") {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "a stuck round gets a T3 redirection that does not count as a challenge", verdict: allSupported, stewards: "T1 T3 T2a",
			policy: func(p *RoundPolicy) { p.MaxChallenges = 1 },
			roles:  "intake facts fact-check investigator investigator",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					v.FactsUpdate, v.Identity = []Fact{}, nil
					v.Status = "stuck"
					if c.round == 2 {
						v.Status, v.DeployedBuilds, v.Candidate = "candidate", deployedBuild(), codeClaim("b1", nil)
					}
				})
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				if out.Pass == (contract.Ref{}) || !strings.Contains(calls[1].Request.Feedback.Message, "The steward (T3) challenged") {
					t.Errorf("rounds = %+v", out)
				}
			}},
		{name: "audit findings reach the next round and a finding without a recorded entry is repaired", verdict: allSupported,
			roles: "intake facts fact-check investigator fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				c.round0(t, func(v *Round) {
					if c.round == 2 {
						ends(v)
					}
				})
				return ""
			},
			auditor: func(t *testing.T, call agentCall) string {
				finding := Finding{Category: "unreceipted-query", Entry: ptr("nope"), Evidence: []Evidence{}, Reason: "a log search without a receipt", Effect: "undermines"}
				if call.Request.Feedback != nil {
					if !strings.Contains(call.Request.Feedback.Message, `findings[0].entry: got "nope"`) {
						t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
					}
					finding.Entry, finding.Receipt = nil, ptr("q1")
				}
				if !strings.Contains(call.Task.Requirements, "The confirmed home stack is") {
					t.Errorf("audit requirements = %q", call.Task.Requirements)
				}
				findings := []Finding{finding}
				if call.Task.Round == 2 {
					findings = []Finding{}
				}
				call.reply(t, Audit{Round: call.citable("round under audit"), Findings: findings, Gaps: []Gap{}}, nil)
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, _ []string) {
				if fb := calls[1].Request.Feedback; fb == nil || !strings.Contains(fb.Message, "The audit of round 1 reported 1 findings") || !slices.Contains(fb.Refs, out.Records[0].Audit) {
					t.Errorf("round 2 feedback = %+v", fb)
				}
			}},
		{name: "a timed-out round reruns in a fresh session from the same inputs", verdict: allSupported, policy: func(p *RoundPolicy) { p.RoundTimeout = time.Second },
			roles: "intake facts fact-check investigator investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				if c.Request.Feedback == nil {
					return "hold"
				}
				if c.Request.Feedback.SourceCode != string(engine.TimedOut) || !strings.HasPrefix(c.Request.Feedback.Message, fmt.Sprintf(rerunNote, engine.TimedOut)) {
					t.Errorf("rerun feedback = %+v, want the timeout diagnostic", c.Request.Feedback)
				}
				c.round0(t, ends)
				return ""
			},
			check: func(t *testing.T, out Rounds, calls []investigatorCall, handles []string) {
				if len(out.Recoveries) != 1 || out.Recoveries[0].Code != engine.TimedOut || len(handles) != 2 || handles[0] == handles[1] {
					t.Errorf("recoveries = %+v handles = %v", out.Recoveries, handles)
				}
				record := decodeRef[ObservationRecord](t, nil, out.Records[0].Observation)
				if len(record.Attempts) != 2 || record.Attempts[0].Committed || !record.Attempts[1].Committed || record.Round != out.Records[0].Round {
					t.Errorf("observation record = %+v, want the timed-out attempt and the committed one", record)
				}
				if a, b := calls[0].Task.Citable, calls[1].Task.Citable; len(a) != len(b) || a[len(a)-1] != b[len(b)-1] {
					t.Errorf("rerun inputs differ: %v vs %v", a, b)
				}
			}},
		{name: "context usage at the threshold hands off to a fresh session", verdict: allSupported, percent: ptr(95.0),
			roles: "intake facts fact-check investigator fact-check investigator fact-check",
			agent: func(t *testing.T, c investigatorCall) string {
				if c.round == 2 {
					c.round0(t, ends)
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
					c.round0(t, ends)
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
					c.round0(t, func(v *Round) { ends(v); v.Identity.Decisions[0].Value = ptr("pop-b") })
					return ""
				}
				if !strings.Contains(c.Request.Feedback.Message, `want "pop-a", the stack of lookup "l-a" row 0`) {
					t.Errorf("repair feedback = %q", c.Request.Feedback.Message)
				}
				c.round0(t, ends)
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
			var stewards []agentCall
			checks := 0
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
				out, err = runRounds(ctx, r, sk, s0, RoundModels{Investigator: model, Validator: model, Steward: model}, p)
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
				case "audit":
					if tc.auditor != nil {
						return tc.auditor(t, call)
					}
					call.reply(t, Audit{Round: call.citable("round under audit"), Findings: []Finding{}, Gaps: []Gap{}}, nil)
				case "steward":
					stewards = append(stewards, call)
					if tc.steward != nil {
						return tc.steward(t, call)
					}
					call.reply(t, stewardVerdict(call, "pass"), nil)
				case "investigator":
					c := investigatorCall{agentCall: call, round: call.Task.Round, repair: call.Request.Feedback != nil && strings.HasPrefix(call.Request.Feedback.Message, "Previous contract")}
					calls = append(calls, c)
					return tc.agent(t, c)
				}
				return ""
			})
			if tc.fails != "" {
				if res.Report.Outcome != engine.Failed || !strings.Contains(fmt.Sprint(res.Report.Failure), tc.fails) {
					t.Fatalf("outcome = %s failure = %v, want %q", res.Report.Outcome, res.Report.Failure, tc.fails)
				}
				return
			}
			if res.Report.Outcome != engine.Succeeded {
				t.Fatalf("outcome = %s: %v", res.Report.Outcome, res.Report.Failure)
			}
			if tc.within > 0 && time.Since(started) > tc.within {
				t.Errorf("took %s, want under %s", time.Since(started), tc.within)
			}
			var roles []string
			audits := 0
			for _, role := range res.Roles {
				switch role {
				case "audit":
					audits++
				case "steward":
				default:
					roles = append(roles, role)
				}
			}
			if got := strings.Join(roles, " "); got != tc.roles {
				t.Fatalf("dispatched roles without audits and stewards = %q, want %q", got, tc.roles)
			}
			if tc.fails == "" && (audits < len(out.Records) || len(out.Records) > 0 && out.Records[len(out.Records)-1].Audit == (contract.Ref{})) {
				t.Errorf("audits = %d for %d rounds", audits, len(out.Records))
			}
			var triggers []string
			for _, st := range out.Stewards {
				triggers = append(triggers, st.Trigger)
			}
			want := tc.stewards
			if want == "" && len(out.Records) > 0 {
				want = "T1"
			}
			if got := strings.Join(triggers, " "); tc.fails == "" && got != want {
				t.Errorf("steward triggers = %q, want %q", got, want)
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
			if tc.check != nil {
				tc.check(t, out, calls, handles)
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
	}{{2, 2, true}, {3, 2, false}, {2, 3, false}} {
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

func TestVisionAllowed(t *testing.T) {
	p := RoundPolicy{TimeoutRetries: 1, Vision: VisionPolicy{MaxSteps: 5}}
	snapshot := func(sessions, attempts int) engine.Snapshot {
		s := engine.Snapshot{Policy: engine.RunPolicy{MaxTotalSessions: 20, MaxTotalAttempts: 40}, Sessions: map[string]engine.SessionStatus{}, Attempts: map[string]engine.AttemptState{}}
		for i := range sessions {
			s.Sessions[fmt.Sprint(i)] = engine.SessionStatus{}
		}
		for i := range attempts {
			s.Attempts[fmt.Sprint(i)] = engine.AttemptState{}
		}
		return s
	}
	// A round reserves 8 sessions and 18 attempts; a vision Step takes up to
	// 2 sessions and 4 attempts; the batch record takes 1 attempt.
	for _, tc := range []struct {
		sessions, attempts, used, want int
	}{
		{0, 0, 0, 5},
		{0, 0, 3, 2},
		{10, 0, 0, 1},
		{14, 0, 0, 0},
		{0, 9, 0, 3},
		{0, 13, 0, 2},
		{0, 17, 0, 1},
		{0, 18, 0, 0},
		{0, 21, 0, 0},
	} {
		if got := visionAllowed(snapshot(tc.sessions, tc.attempts), p, tc.used); got != tc.want {
			t.Errorf("visionAllowed(%d sessions, %d attempts, %d used) = %d, want %d", tc.sessions, tc.attempts, tc.used, got, tc.want)
		}
	}
}

func TestSameEvidence(t *testing.T) {
	owner, other := contract.Ref{AttemptID: "owner"}, contract.Ref{AttemptID: "other"}
	whole := ""
	want := Evidence{Ref: &owner, FileID: "bundle", Locator: &Locator{Pointer: &whole}}
	for _, tc := range []struct {
		name string
		got  Evidence
		err  string
	}{
		{"same", Evidence{Ref: &owner, FileID: "bundle", Locator: &Locator{Pointer: &whole}}, ""},
		{"null ref", Evidence{FileID: "bundle", Locator: want.Locator}, "image.ref: got null"},
		{"other ref", Evidence{Ref: &other, FileID: "bundle", Locator: want.Locator}, "image.ref: got attempt other"},
		{"other file", Evidence{Ref: &owner, FileID: "shot", Locator: want.Locator}, `image.file_id: got "shot"; want "bundle"`},
		{"other locator", Evidence{Ref: &owner, FileID: "bundle"}, "image.locator"},
	} {
		if got := errText(sameEvidence("image", tc.got, want)); tc.err == "" && got != "" || tc.err != "" && !strings.Contains(got, tc.err) {
			t.Errorf("%s: sameEvidence = %q, want %q", tc.name, got, tc.err)
		}
	}
}

func TestAbsentGapID(t *testing.T) {
	long := strings.Repeat("x", 127)
	for _, tc := range []struct {
		name string
		id   string
		gaps []Gap
		want string
	}{
		{"short id", "pop", nil, "p-pop"},
		{"long id falls back to a position", long, []Gap{{ID: "p-a"}}, "p-2"},
		{"a fallback colliding with a short id gets a suffix", long, []Gap{{ID: "p-2"}}, "p-2-2"},
		{"suffixes chain", long, []Gap{{ID: "p-3"}, {ID: "p-3-2"}}, "p-3-3"},
	} {
		if got := absentGapID("p-", tc.id, tc.gaps); got != tc.want {
			t.Errorf("%s: absentGapID = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestVisionPolicyCheck(t *testing.T) {
	model := runtime.ModelSpec{Provider: "fixture", ID: "model"}
	for _, tc := range []struct {
		name string
		p    VisionPolicy
		ok   bool
	}{
		{"valid", VisionPolicy{Model: model, MaxSteps: 1, Parallel: 1, Timeout: time.Second}, true},
		{"no model", VisionPolicy{MaxSteps: 1, Parallel: 1, Timeout: time.Second}, false},
		{"no parallelism", VisionPolicy{Model: model, MaxSteps: 1, Timeout: time.Second}, false},
		{"no timeout", VisionPolicy{Model: model, MaxSteps: 1, Parallel: 1}, false},
	} {
		if err := tc.p.check(); (err == nil) != tc.ok {
			t.Errorf("%s: check = %v", tc.name, err)
		}
	}
}

// stewardVerdict is the fake steward: a pass, or a challenge on the latest
// round; T3 always challenges.
func stewardVerdict(call agentCall, verdict string) Steward {
	if call.Task.Trigger == "T3" {
		verdict = "challenge"
	}
	v := Steward{Trigger: call.Task.Trigger, Verdict: verdict, Notes: []string{}, Items: []StewardItem{}, Wiki: []StewardWiki{}, Gaps: []Gap{}}
	if verdict == "challenge" {
		round := call.citable(fmt.Sprintf("round %d", call.Task.Round))
		v.Items = []StewardItem{{PatternID: "ORCH-CB4", Target: StewardTarget{Ref: round}, Question: "where is the runtime evidence for this?"}}
	}
	return v
}
