package triage

import (
	"context"
	"fmt"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// SkillsDirEnv names the private skill directory; the CLI reads it.
const SkillsDirEnv = "PWC_TRIAGE_SKILLS_DIR"

// Config is every model and limit of one triage run.
type Config struct {
	S0          S0Models
	FactRetries int
	Rounds      RoundModels
	Policy      RoundPolicy
}

func fireworks(id, thinking string) runtime.ModelSpec {
	return runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/" + id, Thinking: thinking}
}

// DefaultConfig binds the models and limits. The limits are conservative
// starting values, not measured ones; live runs are expected to tune them.
func DefaultConfig() Config {
	flash, strong := fireworks("glm-5p3-flash", "high"), fireworks("glm-5p3", "high")
	return Config{
		S0:          S0Models{Intake: fireworks("deepseek-v4p1-flash", "off"), Facts: flash, Validator: flash},
		FactRetries: 1,
		Rounds:      RoundModels{Investigator: strong, Validator: flash, Steward: strong},
		Policy: RoundPolicy{MaxRounds: 8, MaxRejections: 2, MaxChallenges: 3, TimeoutRetries: 1,
			RoundTimeout: 45 * time.Minute, CheckTimeout: 15 * time.Minute, AuditTimeout: 15 * time.Minute, StewardTimeout: 20 * time.Minute, HandoffPercent: 80,
			Vision:       VisionPolicy{Model: flash, MaxSteps: 6, Parallel: 2, Timeout: 10 * time.Minute},
			Verification: VerificationPolicy{Pro: strong, Con: strong, Cross: strong, Timeout: 30 * time.Minute, MaxRuns: 2},
			Report:       ReportPolicy{Model: strong, Timeout: 30 * time.Minute}},
	}
}

// RunPolicy gives the default limits room. The rounds check sessions,
// attempts and remaining time against their worst case before each round
// and verification, so a budget ends them with a report rather than the
// engine refusing a Step or the deadline failing the run.
func RunPolicy() engine.RunPolicy {
	p := engine.DefaultRunPolicy()
	p.RunTimeout = 16 * time.Hour
	p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = 6, 160, 400
	return p
}

// Definition is the jira-triage workflow over the private skills in
// skillsDir.
func Definition(skillsDir string) engine.Definition {
	config := DefaultConfig()
	return engine.Definition{Name: "jira-triage", Description: "Investigate a Jira ticket: facts, identity, investigation rounds, independent verification, report", Version: "2", Policy: RunPolicy(),
		Execute: func(ctx context.Context, r *engine.Run, _ engine.Input) (engine.Result, error) {
			return execute(ctx, r, skillsDir, config)
		}}
}

func execute(ctx context.Context, r *engine.Run, skillsDir string, c Config) (engine.Result, error) {
	if skillsDir == "" {
		return engine.Result{}, fmt.Errorf("%s must name the private triage skill directory", SkillsDirEnv)
	}
	if err := c.Policy.checkRun(r.Snapshot().Policy); err != nil {
		return engine.Result{}, err
	}
	skills, err := PrepareSkills(ctx, r, r.Root(), skillsDir)
	if err != nil {
		return engine.Result{}, err
	}
	s0, err := runS0(ctx, r, skills, c.S0, c.FactRetries)
	if err != nil {
		return engine.Result{}, err
	}
	rounds, err := runRounds(ctx, r, skills, s0, c.Rounds, c.Policy)
	if err != nil {
		return engine.Result{}, err
	}
	report, _, err := runReport(ctx, r, s0, skills, rounds, c.Policy)
	if err != nil {
		return engine.Result{}, err
	}
	return engine.Result{Outputs: map[string]contract.Ref{"report": report}, Final: &engine.FinalSelection{Output: "report", FileID: ReportFileID}}, nil
}
