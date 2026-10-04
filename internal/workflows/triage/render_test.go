package triage

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestRenderReport checks the reader-facing layout and that agent text
// cannot open a Markdown block or HTML: one-line Refs and evidence with
// their locators, backticks inside code spans, line breaks escaped.
func TestRenderReport(t *testing.T) {
	script := `
import sys
sys.path[:0] = sys.argv[1:3]
from render_report import render
ref = {"run_id": "r", "attempt_id": "a1", "path": "/run/steps/s/attempts/0001-a1/published/contract.json", "schema_id": "triage.facts.v1", "sha256": "x", "manifest_sha256": "y"}
own = {"ref": None, "file_id": "x\n\n# Injected\n<script>", "locator": {"offset": 3, "length": 4}}
tick = {"ref": ref, "file_id": "` + "a`b" + `", "locator": {"pointer": "/rows/0"}}
data = {"request": "T-1 hint", "limit": "budget | time",
        "question": {"text": "Is the change intended?", "evidence": [tick]},
        "answer": "Neither.\n# Not a heading\n<b>bold</b>",
        "chain": [{"statement": "the write path stores bare keys", "basis": "verified-claim", "evidence": [tick]},
                  {"statement": "so the test matched the wrong field", "basis": "inference", "evidence": [own]}],
        "certainty": "code reading only", "completeness": "incomplete",
        "actions": [{"audience": "test owners", "action": "match on the bare key", "reason": "it is the stored form", "evidence": [tick]}],
        "claim": {"claim": None, "delivery": None, "t2a": None, "t2b": None, "outcome": "passed"},
        "gaps": [{"ref": ref, "id": "g1", "disposition": "open", "note": "n", "evidence": [own]},
                 {"ref": ref, "id": "g2", "disposition": "resolved", "note": "done", "evidence": [tick]}],
        "new_gaps": [{"id": "g3", "text": "why green before"}], "next_steps": ["read the suite"]}
docs = [{"ref": ref, "data": {"gaps": [{"id": "g1", "text": "t1"}, {"id": "g2", "text": "t2"}]}}]
sys.stdout.write(render({}, data, docs).decode())
`
	out, err := exec.Command("python3", "-B", "-c", script, "report", filepath.Join("..", "..", "contract", "reportresource")).CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v: %s", err, out)
	}
	got := string(out)
	for _, want := range []string{
		"原始請求：`T-1 hint`\n",
		"**問題**：Is the change intended?\n- 依據：``a`b`` at `/rows/0` in `triage.facts.v1` attempt `a1` `/run/steps/s/attempts/0001-a1/published/contract.json`\n",
		"**答案**：Neither.\n\\# Not a heading\n\\<b>bold\\</b>\n",
		"| `incomplete` | `passed` | budget \\| time |\n",
		"1. [已驗證] the write path stores bare keys\n   - 依據：``a`b`` at `/rows/0`",
		"2. [推論，未驗證] so the test matched the wrong field\n   - 依據：`\"x\\n\\n# Injected\\n<script>\"` bytes 3+4 in 本 attempt\n",
		"1. **test owners**：match on the bare key\n   - 理由：it is the stored form\n",
		"- 未解缺口 `g1`：t1\n  - 說明：n\n  - 依據：",
		"- 新缺口 `g3`：why green before\n",
		"- 下一步：read the suite\n",
		"## 已處理的缺口\n\n- `g2`（resolved）：t2\n  - 處置：done\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report lacks %q:\n%s", want, got)
		}
	}
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "# Injected") || strings.HasPrefix(line, "# Not") || strings.HasPrefix(line, "<") {
			t.Errorf("agent text started a Markdown block: %q", line)
		}
	}
	if strings.Index(got, "## 建議行動") > strings.Index(got, "## 未解問題與下一步") || strings.Index(got, "## 因果鏈") > strings.Index(got, "## 建議行動") {
		t.Errorf("sections out of order:\n%s", got)
	}
}
