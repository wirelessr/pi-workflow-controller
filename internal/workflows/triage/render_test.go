package triage

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRenderReportInline checks the renderer's one-line Refs and evidence:
// backticks stay inside their code span and a line break in an agent-chosen
// file id cannot start a Markdown block.
func TestRenderReportInline(t *testing.T) {
	script := `
import sys
sys.path[:0] = sys.argv[1:3]
from render_report import render
ref = {"run_id": "r", "attempt_id": "a1", "path": "/run/steps/s/attempts/0001-a1/published/contract.json", "schema_id": "triage.facts.v1", "sha256": "x", "manifest_sha256": "y"}
data = {"request": "T-1", "conclusion": "c", "completeness": "incomplete", "limit": None,
        "claim": {"claim": None, "delivery": None, "t2a": None, "t2b": None, "outcome": "none"},
        "gaps": [{"ref": ref, "id": "g1", "disposition": "open", "note": "n",
                  "evidence": [{"ref": None, "file_id": "x\n\n# Injected\n<script>"}, {"ref": ref, "file_id": "` + "a`b" + `"}]}],
        "new_gaps": [], "next_steps": []}
docs = [{"ref": ref, "data": {"gaps": [{"id": "g1", "text": "t"}]}}]
sys.stdout.write(render({}, data, docs).decode())
`
	out, err := exec.Command("python3", "-B", "-c", script, "report", filepath.Join("..", "..", "contract", "reportresource")).CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v: %s", err, out)
	}
	got := string(out)
	for _, want := range []string{
		"缺口來源：`triage.facts.v1` attempt `a1` `/run/steps/s/attempts/0001-a1/published/contract.json`\n",
		"處置依據：`\"x\\n\\n# Injected\\n<script>\"` in 本 attempt\n",
		"處置依據：``a`b`` in `triage.facts.v1` attempt `a1`",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report lacks %q:\n%s", want, got)
		}
	}
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "# Injected") || strings.HasPrefix(line, "<script>") {
			t.Errorf("an evidence file id started a Markdown block: %q", line)
		}
	}
}
