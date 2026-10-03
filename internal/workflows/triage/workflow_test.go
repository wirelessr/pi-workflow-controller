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
	if err := c.Policy.check(); err != nil {
		t.Fatal(err)
	}
	if err := c.Report.check(); err != nil {
		t.Fatal(err)
	}
	// The run budget covers every round and both verifications before the
	// rounds' own budget checks have to end them.
	sessions, attempts := roundCost(c.Policy)
	if p := RunPolicy(); c.Policy.MaxRounds*sessions > p.MaxTotalSessions || c.Policy.MaxRounds*attempts > p.MaxTotalAttempts {
		t.Errorf("run policy %d sessions %d attempts is below %d rounds of %d and %d", p.MaxTotalSessions, p.MaxTotalAttempts, c.Policy.MaxRounds, sessions, attempts)
	}
}
