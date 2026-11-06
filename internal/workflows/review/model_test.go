package review

import (
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
