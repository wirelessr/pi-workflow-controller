package triage

import (
	"context"
	"strings"
	"testing"

	"pi-workflow-controller/internal/engine"
)

// TestExecute runs the whole workflow with its default configuration over
// the real engine, runtime and RPC protocol.
func TestExecute(t *testing.T) {
	// Run with the shipped run limits; the harness keeps its own runtime
	// timeouts.
	harnessPolicy = func(p *engine.RunPolicy) {
		runtime := p.Runtime
		*p = RunPolicy()
		p.Runtime = runtime
	}
	defer func() { harnessPolicy = nil }()
	skills := newSkillFixture(t)
	res := runHarness(t, "CASE-17 pop=pop-a", func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
		return execute(ctx, r, skills.source, DefaultConfig())
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
				if call.Task.Round == 2 {
					v.Status, v.FactsUpdate, v.Identity = "candidate", []Fact{}, nil
					v.DeployedBuilds, v.Candidate = deployedBuild(), codeClaim("b1", nil)
				}
			})
		case "report":
			writeReport(t, call, nil, true)
		}
		return ""
	})
	if res.Report.Outcome != engine.Succeeded {
		t.Fatalf("outcome = %s: %v", res.Report.Outcome, res.Report.Failure)
	}
	if final := res.Report.Final; final == nil || final.Output != "report" || !strings.HasSuffix(final.ArtifactPath, "triage-report.md") {
		t.Fatalf("final = %+v", res.Report.Final)
	}
	if got := strings.Join(res.Roles, " "); !strings.HasSuffix(got, " steward report") || !strings.Contains(got, "verify-pro") {
		t.Errorf("roles = %s", got)
	}
	// Every role runs on the model its configuration names.
	c := DefaultConfig()
	want := map[string]string{"triage-intake": c.S0.Intake.ID, "triage-facts": c.S0.Facts.ID, "triage-fact-check": c.S0.Validator.ID, "triage-investigator": c.Rounds.Investigator.ID,
		"triage-audit": c.Rounds.Validator.ID, "triage-steward": c.Rounds.Steward.ID, "triage-verify-pro": c.Policy.Verification.Pro.ID, "triage-verify-con": c.Policy.Verification.Con.ID,
		"triage-verify-cross": c.Policy.Verification.Cross.ID, "triage-report": c.Policy.Report.Model.ID}
	seen := map[string]bool{}
	for _, s := range res.Report.Snapshot.Sessions {
		if id, ok := want[s.Role.Name]; !ok || s.Role.Model.ID != id {
			t.Errorf("session %s runs %s", s.Role.Name, s.Role.Model.ID)
		}
		seen[s.Role.Name] = true
	}
	if len(seen) != len(want) {
		t.Errorf("roles run = %v, want all of %v", seen, want)
	}
}

func TestExecuteNeedsTheSkillsDir(t *testing.T) {
	res := runHarness(t, "CASE-17", func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
		return execute(ctx, r, "", DefaultConfig())
	}, func(t *testing.T, call agentCall) string {
		t.Errorf("dispatched %s without skills", call.Role)
		return ""
	})
	if res.Report.Outcome != engine.Failed || !strings.Contains(res.Report.Failure.Error(), SkillsDirEnv) {
		t.Fatalf("outcome = %s failure = %v", res.Report.Outcome, res.Report.Failure)
	}
}

func TestDefinition(t *testing.T) {
	d := Definition("/skills")
	if d.Name != "jira-triage" || d.Policy.MaxLiveSessions < len(verifierRoles)+1 {
		t.Fatalf("definition = %+v", d)
	}
	c := DefaultConfig()
	if err := c.Policy.checkRun(RunPolicy()); err != nil {
		t.Fatal(err)
	}
	// The run budget covers every round at its worst case; the rounds' own
	// checks end them before the engine would refuse a Step.
	sessions, attempts := roundCost(c.Policy)
	if p := RunPolicy(); c.Policy.MaxRounds*sessions > p.MaxTotalSessions || c.Policy.MaxRounds*attempts > p.MaxTotalAttempts {
		t.Errorf("run policy %d sessions %d attempts is below %d rounds of %d and %d", p.MaxTotalSessions, p.MaxTotalAttempts, c.Policy.MaxRounds, sessions, attempts)
	}
}
