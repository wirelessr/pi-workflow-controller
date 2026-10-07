package triage

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
	"pi-workflow-controller/internal/testutil/protocol"
)

// writeReport is the fake report agent: it writes the candidate from the
// request, then runs the extracted renderer as the requirements say.
func writeReport(t *testing.T, call agentCall, change func(*Report), render bool) {
	t.Helper()
	writeReportFiles(t, call, change, render, nil)
}

// writeReportFiles is writeReport with evidence files of the report's own.
func writeReportFiles(t *testing.T, call agentCall, change func(*Report), render bool, files map[string][]byte) {
	t.Helper()
	want := call.Task.Report
	ticket := []Evidence{cite(call.citable("intake"), "page-0")}
	v := Report{Request: want.Request, Claim: want.Claim, Question: ReportQuestion{Text: "why does svc fail?", Evidence: ticket}, Answer: "the error comes from svc",
		Chain: []ChainStep{{Statement: "svc rejects the flag", Basis: "evidence", Evidence: ticket}}, Certainty: "from the ticket text only",
		Actions:      []ReportAction{{Audience: "svc owners", Action: "accept the flag", Reason: "the rejection is the error", Evidence: ticket}},
		Completeness: "incomplete", Limit: want.Limit, Gaps: []ReportGap{}, NewGaps: []Gap{}, NextSteps: []string{"confirm on a second stack"}, ReportFile: ReportFileID}
	for _, g := range want.Gaps {
		v.Gaps = append(v.Gaps, ReportGap{Ref: g.Ref, ID: g.ID, Disposition: "resolved", Note: "covered by the verified claim", Evidence: []Evidence{cite(call.citable("intake"), "page-0")}})
	}
	if change != nil {
		change(&v)
	}
	if err := protocol.WriteEnvelope(call.Candidate, call.Request, v, call.writeFiles(t, files)); err != nil {
		t.Fatal(err)
	}
	if !render {
		return
	}
	request := filepath.Join(filepath.Dir(call.Candidate), "request.json")
	if out, err := exec.Command("python3", "-B", want.Renderer, request, call.Candidate).CombinedOutput(); err != nil {
		t.Fatalf("renderer: %v: %s", err, out)
	}
}

func TestReport(t *testing.T) {
	model := runtime.ModelSpec{Provider: "fixture", ID: "model", Thinking: "high"}
	policy := RoundPolicy{MaxRounds: 2, MaxRejections: 2, MaxChallenges: 2, TimeoutRetries: 1, RoundTimeout: 30 * time.Second, CheckTimeout: 30 * time.Second, AuditTimeout: 30 * time.Second, StewardTimeout: 30 * time.Second, HandoffPercent: 80,
		Vision:       VisionPolicy{Model: model, MaxSteps: 2, Parallel: 2, Timeout: 30 * time.Second},
		Verification: VerificationPolicy{Pro: model, Con: model, Cross: model, Timeout: 30 * time.Second, MaxRuns: 2},
		Report:       ReportPolicy{Model: model, Timeout: 30 * time.Second}}
	for _, tc := range []struct {
		name     string
		blocked  bool
		report   func(t *testing.T, call agentCall)
		timeout  time.Duration
		fails    string
		complete bool
	}{
		{name: "a passed claim with every gap resolved may be complete", complete: true,
			report: func(t *testing.T, call agentCall) {
				writeReport(t, call, func(v *Report) { v.Completeness, v.Chain[0].Basis = "complete", "verified-claim" }, true)
			}},
		{name: "a blocked investigation reported complete is repaired", blocked: true,
			report: func(t *testing.T, call agentCall) {
				if call.Request.Feedback == nil {
					writeReport(t, call, func(v *Report) { v.Completeness = "complete" }, true)
					return
				}
				if !strings.Contains(call.Request.Feedback.Message, "completeness: got complete; want incomplete") {
					t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
				}
				writeReport(t, call, nil, true)
			}},
		{name: "a report that drops a gap is repaired",
			report: func(t *testing.T, call agentCall) {
				if call.Request.Feedback == nil {
					writeReport(t, call, func(v *Report) { v.Gaps = v.Gaps[1:] }, true)
					return
				}
				if !strings.Contains(call.Request.Feedback.Message, "has no disposition") {
					t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
				}
				writeReport(t, call, nil, true)
			}},
		{name: "a rewritten request is repaired", report: repairedReport(func(v *Report) { v.Request += " please" }, "request: want the caller's prompt copied exactly")},
		{name: "a changed claim object is repaired", report: repairedReport(func(v *Report) { v.Claim.Outcome = "none" }, "claim: want the claim object copied exactly")},
		{name: "a changed limit is repaired", report: repairedReport(func(v *Report) { v.Limit = "rounds" }, `limit: got "rounds"; want ""`)},
		{name: "a gap listed twice is repaired", report: repairedReport(func(v *Report) { v.Gaps = append(v.Gaps, v.Gaps[0]) }, "is listed twice")},
		{name: "a gap from nowhere is repaired", report: func(t *testing.T, call agentCall) {
			// The renderer refuses it too; the Controller says why.
			if call.Request.Feedback == nil {
				writeReport(t, call, func(v *Report) {
					g := v.Gaps[0]
					g.ID = "invented"
					v.Gaps = append(v.Gaps, g)
				}, false)
				return
			}
			if !strings.Contains(call.Request.Feedback.Message, `gap "invented" is not in the request's gap list`) {
				t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
			}
			writeReport(t, call, nil, true)
		}},
		{name: "an open gap reported complete is repaired", report: repairedReport(func(v *Report) {
			v.Completeness, v.Gaps[0].Disposition, v.Gaps[0].Evidence = "complete", "open", []Evidence{}
		}, "completeness: got complete")},
		{name: "a new gap reported complete is repaired", report: repairedReport(func(v *Report) {
			v.Completeness, v.NewGaps = "complete", []Gap{{ID: "late", Text: "found while writing"}}
		}, "completeness: got complete")},
		{name: "a verified chain step without a passed claim is repaired", blocked: true, report: repairedReport(func(v *Report) { v.Chain[0].Basis = "verified-claim" }, `chain[0].basis: got verified-claim, but the claim outcome is "none"`)},
		{name: "a chain step citing an unknown file is repaired", report: repairedReport(func(v *Report) {
			v.Chain[0].Evidence = []Evidence{{FileID: "nowhere", Locator: &Locator{Pointer: ptr("")}}}
		}, "chain[0].evidence[0].file_id")},
		{name: "a report with nothing to do is repaired", report: repairedReport(func(v *Report) { v.Actions, v.NextSteps = []ReportAction{}, []string{} }, "actions, next_steps: both are empty")},
		{name: "a chain step without evidence is repaired by the schema", report: repairedReport(func(v *Report) { v.Chain[0].Evidence = []Evidence{} }, "/chain/0/evidence")},
		{name: "an action citing an unknown file is repaired", report: repairedReport(func(v *Report) {
			v.Actions[0].Evidence = []Evidence{{FileID: "nowhere", Locator: &Locator{Pointer: ptr("")}}}
		}, "actions[0].evidence[0].file_id")},
		{name: "a question citing a missing pointer is repaired", report: repairedReport(func(v *Report) {
			v.Question.Evidence[0].Locator = &Locator{Pointer: ptr("/no/such/field")}
		}, "question.evidence[0]")},
		{name: "a resolved gap without evidence is repaired by the schema", report: repairedReport(func(v *Report) { v.Gaps[0].Evidence = []Evidence{} }, "evidence")},
		{name: "a report edited after rendering is repaired", report: func(t *testing.T, call agentCall) {
			writeReport(t, call, nil, true)
			if call.Request.Feedback == nil {
				path := filepath.Join(filepath.Dir(call.Candidate), "artifacts", "triage-report.md")
				if err := os.WriteFile(path, []byte("# edited\n"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if !strings.Contains(call.Request.Feedback.Message, "differs from the deterministic rendering") {
				t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
			}
		}},
		{name: "a timed-out report reruns in a fresh attempt", timeout: 3 * time.Second, report: func(t *testing.T, call agentCall) {
			if call.Request.Feedback == nil {
				writeReport(t, call, nil, true)
				panic(holdReport{})
			}
			writeReport(t, call, nil, true)
		}},
		{name: "a report that declares its own evidence file is repaired", report: func(t *testing.T, call agentCall) {
			// Live: a report wrote an inventory file of its own to cite.
			if call.Request.Feedback == nil {
				writeReportFiles(t, call, func(v *Report) {
					v.Answer += " (no images attached)"
					v.Question.Evidence = append(v.Question.Evidence, Evidence{FileID: "inventory", Locator: &Locator{Offset: ptr(int64(0)), Length: ptr(int64(1))}})
				}, true, map[string][]byte{"inventory": []byte("no images\n")})
				return
			}
			if !strings.Contains(call.Request.Feedback.Message, "files: got [inventory (evidence, evidence/inventory.txt), triage-report (artifact, artifacts/triage-report.md)]; want exactly the renderer's artifact") ||
				!strings.Contains(call.Request.Feedback.Message, "cite a citable input's file with that input's exact ref instead") {
				t.Errorf("repair feedback = %q", call.Request.Feedback.Message)
			}
			writeReport(t, call, nil, true)
		}},
		{name: "a report without the rendered file fails after its repairs", fails: "want exactly the renderer's artifact",
			report: func(t *testing.T, call agentCall) { writeReport(t, call, nil, false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			skills := newSkillFixture(t)
			var reportRef contract.Ref
			var rounds Rounds
			res := runHarness(t, "CASE-17 pop=pop-a", func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
				sk, err := PrepareSkills(ctx, r, r.Root(), skills.source)
				if err != nil {
					return engine.Result{}, err
				}
				s0, err := runS0(ctx, r, sk, S0Models{Intake: model, Facts: model, Validator: model}, 0)
				if err != nil {
					return engine.Result{}, err
				}
				if rounds, err = runRounds(ctx, r, sk, s0, RoundModels{Investigator: model, Validator: model, Steward: model}, policy); err != nil {
					return engine.Result{}, err
				}
				if reportRef, _, err = runReport(ctx, r, s0, sk, rounds, withReportTimeout(policy, reportTimeout(tc.timeout))); err != nil {
					return engine.Result{}, err
				}
				return engine.Result{Outputs: map[string]contract.Ref{"report": reportRef}, Final: &engine.FinalSelection{Output: "report"}}, nil
			}, func(t *testing.T, call agentCall) string {
				switch call.Role {
				case "intake":
					v, files := intakeFiles()
					call.reply(t, v, files)
				case "facts":
					call.reply(t, factsFor(call), nil)
				case "fact-check":
					call.reply(t, checkFor(t, call, func(string) string { return "supported" }), nil)
				case "audit":
					call.reply(t, Audit{Round: call.citable("round under audit"), Findings: []Finding{}, Gaps: []Gap{}}, nil)
				case "steward":
					call.reply(t, stewardVerdict(call, "pass"), nil)
				case "verify-pro", "verify-con", "verify-cross":
					call.reply(t, verdictFor(call, nil), nil)
				case "investigator":
					inv := investigatorCall{agentCall: call, round: call.Task.Round}
					inv.round0(t, func(v *Round) {
						if tc.blocked {
							ends(v)
							return
						}
						v.Status, v.FactsUpdate, v.Identity = "candidate", []Fact{}, nil
						v.DeployedBuilds, v.Candidate = deployedBuild(), codeClaim("b1", nil)
					})
				case "report":
					if !strings.HasSuffix(call.Task.Report.Renderer, "triage-report/render_report.py") || len(call.Task.Skills) != 0 {
						t.Errorf("report task = %+v", call.Task.Report)
					}
					if strings.Contains(call.Task.Requirements, citationRequirements) || !strings.Contains(call.Task.Requirements, reportCitationRequirements) {
						t.Errorf("report requirements offer own evidence files: %q", call.Task.Requirements)
					}
					held := false
					func() {
						defer func() {
							if v := recover(); v != nil {
								if _, ok := v.(holdReport); !ok {
									panic(v)
								}
								held = true
							}
						}()
						tc.report(t, call)
					}()
					if held {
						return "hold"
					}
				}
				return ""
			})
			if tc.fails != "" {
				if res.Report.Outcome != engine.Failed || !strings.Contains(res.Report.Failure.Error(), tc.fails) {
					t.Fatalf("outcome = %s failure = %v, want %q", res.Report.Outcome, res.Report.Failure, tc.fails)
				}
				return
			}
			if res.Report.Outcome != engine.Succeeded {
				t.Fatalf("outcome = %s: %v", res.Report.Outcome, res.Report.Failure)
			}
			report := decodeRef[Report](t, res.Run, reportRef)
			if (report.Completeness == "complete") != tc.complete || (report.Claim.Outcome == "passed") == tc.blocked || len(report.Gaps) == 0 {
				t.Errorf("report = %+v", report)
			}
			body, err := os.ReadFile(filepath.Join(filepath.Dir(reportRef.Path), "artifacts", "triage-report.md"))
			if err != nil || !strings.HasPrefix(string(body), "# 調查報告") || tc.blocked == strings.Contains(string(body), "## 附錄：候選結論與獨立驗證") {
				t.Errorf("report file = %q, %v", body, err)
			}
		})
	}
}

// holdReport makes the fake report agent leave its prompt running.
type holdReport struct{}

func reportTimeout(d time.Duration) time.Duration {
	if d == 0 {
		return 30 * time.Second
	}
	return d
}

// repairedReport writes a report with change, then the correct one after
// the repair feedback names want.
func repairedReport(change func(*Report), want string) func(*testing.T, agentCall) {
	return func(t *testing.T, call agentCall) {
		if call.Request.Feedback == nil {
			writeReport(t, call, change, true)
			return
		}
		if !strings.Contains(call.Request.Feedback.Message, want) {
			t.Errorf("repair feedback = %q, want %q", call.Request.Feedback.Message, want)
		}
		writeReport(t, call, nil, true)
	}
}

func withReportTimeout(p RoundPolicy, d time.Duration) RoundPolicy {
	p.Report.Timeout = d
	return p
}
