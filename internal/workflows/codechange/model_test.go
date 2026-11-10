package codechange

import (
	"testing"

	"pi-workflow-controller/internal/runtime"
)

// The role-to-model binding is product behavior documented in
// docs/CODE-CHANGE.md; changing it must be a deliberate edit of both.
func TestModelBinding(t *testing.T) {
	const prefix = "accounts/fireworks/models/"
	got := map[string]runtime.ModelSpec{
		"implement": implementModel,
		"converge":  convergeModel,
		"cleanup":   cleanupModel,
	}
	for _, rv := range reviewers {
		got["review/"+rv.name] = rv.model
	}
	want := map[string]struct{ id, thinking string }{
		"implement":          {"glm-5p3", "high"},
		"review/basic":       {"glm-5p3-flash", "high"},
		"review/consistency": {"glm-5p3", "high"},
		"converge":           {"glm-5p3", "high"},
		"cleanup":            {"deepseek-v4p1-flash", "high"},
	}
	if len(got) != len(want) {
		t.Fatalf("roles = %d, want %d", len(got), len(want))
	}
	for role, w := range want {
		if got[role] != (runtime.ModelSpec{Provider: "fireworks", ID: prefix + w.id, Thinking: w.thinking}) {
			t.Errorf("%s model = %+v, want %s thinking %s", role, got[role], w.id, w.thinking)
		}
	}
}
