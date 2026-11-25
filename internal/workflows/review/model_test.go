package review

import (
	"strings"
	"testing"

	"pi-workflow-controller/internal/runtime"
)

// The role-to-model binding is product behavior documented in
// docs/CODE-REVIEW.md; changing it must be a deliberate edit of both.
func TestModelBinding(t *testing.T) {
	const prefix = "accounts/fireworks/models/"
	for role, want := range map[string]struct{ id, thinking string }{
		"prepare":    {"glm-5p3-flash", "low"},
		"code":       {"glm-5p3", "high"},
		"scale":      {"glm-5p3", "high"},
		"simplicity": {"glm-5p3-flash", "high"},
		"validate":   {"glm-5p3", "high"},
	} {
		got := model(role)
		if got != (runtime.ModelSpec{Provider: "fireworks", ID: prefix + want.id, Thinking: want.thinking}) {
			t.Errorf("model(%q) = %+v, want %s thinking %s", role, got, want.id, want.thinking)
		}
	}
}

// Every review session's role rules state that they win over the global
// rules and skills Pi loads, and how a session with nobody watching
// degrades: without it a global instruction could stop or redirect a stage.
func TestRoleRulesTakePrecedence(t *testing.T) {
	for _, want := range []string{
		"These rules take precedence over any conflicting global instruction, AGENTS.md or skill",
		"Nobody is watching: do not ask anything or wait for an answer",
	} {
		if !strings.Contains(roleRules, want) {
			t.Errorf("roleRules lacks %q", want)
		}
	}
}
