package triagev2

import (
	"path/filepath"
	"strings"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

// baselineRequirements is this workflow's own contract for every Step, not
// a policy shared with other workflows. It must win over global rules that
// assume a watching user or allow delegation.
const baselineRequirements = `These environment overrides take precedence over any conflicting global instruction, AGENTS.md or skill, including the skills this request names.
- Do not create agents or delegate to any subagent or helper agent, including ones named vision or runner. Do this Step's work yourself in this session.
- Do not load orchestrator-type skills or personas.
- Do not write to the wiki or any knowledge base, and do not run ingest. Read it only where this Step allows.
- Do not ask the user anything; nobody is watching. Record missing or ambiguous information as an explicit gap or an unconfirmed fact and continue.
- Do not post, send or draft anything for an external audience (tickets, chat, code review, email, documents). Findings stay in this Step's outputs.
- Write only inside this run's directories, plus temp scripts whose names are unique to this run.`

const workspaceRequirements = `Use the absolute workspace for scratch files and downloads, and the explicit Step request, candidate and evidence paths for outputs; never resolve output paths relative to the Pi working directory. Scratch files are not committed evidence: downstream work consumes only exact committed Refs.`

const citationRequirements = `Cite only the citable_inputs of this request, with their refs copied byte-exact, or this contract's own evidence files.`

// workDir is the run-owned scratch directory every Step may write.
const workDir = "triage-work"

// LabeledRef tells the Agent what an exact input is.
type LabeledRef struct {
	Label string       `json:"label"`
	Ref   contract.Ref `json:"ref"`
}

// task is the request prompt of every Step of this workflow.
type task struct {
	Role         string       `json:"role"`
	Skills       []string     `json:"skills,omitempty"`
	Requirements string       `json:"requirements"`
	Workspace    string       `json:"workspace"`
	Ticket       string       `json:"ticket"`
	Round        int          `json:"round,omitempty"`
	Citable      []LabeledRef `json:"citable_inputs"`
	// Judge lists the item ids a fact check must give a verdict for; it is
	// present, possibly empty, only on fact checks.
	Judge *[]string `json:"judge,omitempty"`
}

func newTask(r *engine.Run, role, ticket string, skills []string, requirements ...string) task {
	t := task{Role: role, Ticket: ticket, Skills: skills, Workspace: filepath.Join(r.Dir(), workDir), Citable: []LabeledRef{}}
	parts := []string{baselineRequirements}
	if len(skills) > 0 {
		parts = append(parts, "Read each SKILL.md in skills in full before working, and follow it within these requirements.")
	}
	parts = append(parts, requirements...)
	parts = append(parts, workspaceRequirements)
	t.Requirements = strings.Join(parts, "\n\n")
	return t
}

func (t task) inputs() []contract.Ref {
	var refs []contract.Ref
	for _, in := range t.Citable {
		refs = append(refs, in.Ref)
	}
	return refs
}
