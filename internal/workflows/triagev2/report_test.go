package triagev2

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
	want := call.Task.Report
	v := Report{Request: want.Request, Claim: want.Claim, Conclusion: "the error comes from svc", Completeness: "incomplete", Gaps: []ReportGap{}, NewGaps: []Gap{}, NextSteps: []string{"confirm on a second stack"}, ReportFile: ReportFileID}
	for _, g := range want.Gaps {
		v.Gaps = append(v.Gaps, ReportGap{Ref: g.Ref, ID: g.ID, Disposition: "resolved", Note: "covered by the verified claim"})
	}
	if change != nil {
		change(&v)
	}
	if err := protocol.WriteEnvelope(call.Candidate, call.Request, v, []contract.FileEntry{}); err != nil {
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
		Verification: VerificationPolicy{Pro: model, Con: model, Cross: model, Timeout: 30 * time.Second, MaxRuns: 2}}
	for _, tc := range []struct {
		name     string
		blocked  bool
		report   func(t *testing.T, call agentCall)
		fails    string
		complete bool
	}{
		{name: "a passed claim with every gap resolved may be complete", complete: true,
			report: func(t *testing.T, call agentCall) {
				writeReport(t, call, func(v *Report) { v.Completeness = "complete" }, true)
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
		{name: "a report without the rendered file fails after its repair", fails: "want exactly the renderer's artifact",
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
				inputs := append([]LabeledRef{{"skills", sk.Record}}, rounds.Inputs...)
				if reportRef, _, err = runReport(ctx, r, s0, rounds, inputs, ReportPolicy{Model: model, Timeout: 30 * time.Second}, 1); err != nil {
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
					tc.report(t, call)
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
			if err != nil || !strings.HasPrefix(string(body), "# 調查報告") || tc.blocked == strings.Contains(string(body), "## 候選結論與獨立驗證") {
				t.Errorf("report file = %q, %v", body, err)
			}
		})
	}
}
