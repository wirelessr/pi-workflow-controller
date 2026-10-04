package triage

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/reportresource"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const (
	ReportSchema = "triage.report.v1"
	ReportFileID = "triage-report"
	reportDir    = "triage-report"
)

//go:embed report/render_report.py
var reportResources embed.FS

const reportRequirements = `You write the report of this investigation from its committed results, for a reader who did not follow it and has to act on it; you state no new claim or verdict. Write its prose in Traditional Chinese; identifiers, code, quotes and technical terms stay as they are. State the ticket's question in the ticket's own words, quoted, not translated, and answer it directly first; if the investigation could not answer it, or the claim did not pass verification, say so in the answer and what is missing. Then give the chain from root cause or established mechanism to the observed symptom, in order, marking each step as part of the verified claim, shown directly by its evidence, or your own inference; never present more than the verified claim as verified. Say how sure the answer is and why. Name the concrete actions for the people who own the next move (a fix, a decision, a reply, a test change), apart from further investigation, which goes in next steps. Copy request and claim exactly as this request gives them. Give every gap the request lists one disposition with a note, add gaps you find, and say how complete the investigation is: incomplete whenever a limit ended it, the claim did not pass verification, or a gap stays open. Write the candidate, then run python3 -B with the renderer and the absolute request.json and candidate.json paths; the renderer adds the report file, so do not create it yourself.`

type ReportClaim struct {
	Claim    *contract.Ref `json:"claim"`
	Delivery *contract.Ref `json:"delivery"`
	T2a      *contract.Ref `json:"t2a"`
	T2b      *contract.Ref `json:"t2b"`
	Outcome  string        `json:"outcome"`
}

type ReportQuestion struct {
	Text     string     `json:"text"`
	Evidence []Evidence `json:"evidence"`
}

type ChainStep struct {
	Statement string     `json:"statement"`
	Basis     string     `json:"basis"`
	Evidence  []Evidence `json:"evidence"`
}

type ReportAction struct {
	Audience string     `json:"audience"`
	Action   string     `json:"action"`
	Reason   string     `json:"reason"`
	Evidence []Evidence `json:"evidence"`
}

type ReportGap struct {
	Ref         contract.Ref `json:"ref"`
	ID          string       `json:"id"`
	Disposition string       `json:"disposition"`
	Note        string       `json:"note"`
	Evidence    []Evidence   `json:"evidence"`
}

type Report struct {
	Request      string         `json:"request"`
	Claim        ReportClaim    `json:"claim"`
	Question     ReportQuestion `json:"question"`
	Answer       string         `json:"answer"`
	Chain        []ChainStep    `json:"chain"`
	Certainty    string         `json:"certainty"`
	Actions      []ReportAction `json:"actions"`
	Completeness string         `json:"completeness"`
	Limit        string         `json:"limit"`
	Gaps         []ReportGap    `json:"gaps"`
	NewGaps      []Gap          `json:"new_gaps"`
	NextSteps    []string       `json:"next_steps"`
	ReportFile   string         `json:"report_file"`
}

// reportTask is what the report Step is given beyond its inputs.
type reportTask struct {
	Request  string      `json:"request"`
	Claim    ReportClaim `json:"claim"`
	Gaps     []GapRef    `json:"gaps"`
	Limit    string      `json:"limit"`
	Renderer string      `json:"renderer"`
}

// ReportPolicy binds the report Step.
type ReportPolicy struct {
	Model   runtime.ModelSpec
	Timeout time.Duration
}

func (p ReportPolicy) check() error {
	if p.Model.Provider == "" || p.Model.ID == "" || p.Timeout <= 0 {
		return fmt.Errorf("report policy needs a model and a positive Timeout")
	}
	return nil
}

// reportClaim is the last verified claim as the Controller knows it.
func reportClaim(out Rounds) ReportClaim {
	if len(out.Claims) == 0 {
		return ReportClaim{Outcome: "none"}
	}
	last := out.Claims[len(out.Claims)-1]
	c := ReportClaim{Claim: &last.Claim, Outcome: "not-passed"}
	if last.T2a != (contract.Ref{}) {
		c.T2a = &last.T2a
	}
	if last.Delivery != (contract.Ref{}) {
		c.Delivery = &last.Delivery
	}
	if last.T2b != (contract.Ref{}) {
		c.T2b = &last.T2b
	}
	if last.Passed {
		c.Outcome = "passed"
	}
	return c
}

// reportGaps lists every gap the committed inputs record, in input order.
func reportGaps(ctx context.Context, r *engine.Run, inputs []contract.Ref) ([]GapRef, error) {
	var gaps []GapRef
	for _, ref := range inputs {
		raw, err := engine.ReadContract(ctx, r, ref)
		if err != nil {
			return nil, err
		}
		p, err := contract.DecodePublication[struct {
			Gaps []Gap `json:"gaps"`
		}](raw)
		if err != nil {
			return nil, err
		}
		for _, g := range p.Data.Gaps {
			gaps = append(gaps, GapRef{Ref: ref, ID: g.ID})
		}
	}
	return gaps, nil
}

// checkReport keeps the approved report rules: the exact caller request
// and claim, every recorded gap kept, declared completeness that is
// incomplete whenever the investigation is, and an exclusive report file
// equal to the deterministic rendering of the committed inputs. The
// answer, chain and actions must cite resolvable evidence, a chain step
// may claim verification only when the claim passed, and the report must
// leave the reader something to do; what they say is the report's own.
func checkReport(ctx context.Context, r *engine.Run, ref contract.Ref, want reportTask, inputs []contract.Ref) error {
	p, err := readAccepted[Report](ctx, r, ref, ReportSchema)
	if err != nil {
		return err
	}
	v := p.Data
	if v.Request != want.Request {
		return fmt.Errorf("request: want the caller's prompt copied exactly")
	}
	if !reflect.DeepEqual(v.Claim, want.Claim) {
		return fmt.Errorf("claim: want the claim object copied exactly from the request")
	}
	if v.Limit != want.Limit {
		return fmt.Errorf("limit: got %q; want %q copied exactly from the request", v.Limit, want.Limit)
	}
	in, err := citable(ctx, r, inputs...)
	if err != nil {
		return err
	}
	cite := citations{ctx, in, ref, p.Files}.check
	if err := citeAll(cite, "question.evidence", v.Question.Evidence); err != nil {
		return err
	}
	for i, step := range v.Chain {
		if step.Basis == "verified-claim" && want.Claim.Outcome != "passed" {
			return fmt.Errorf("chain[%d].basis: got verified-claim, but the claim outcome is %q; mark the step evidence or inference", i, want.Claim.Outcome)
		}
		if err := citeAll(cite, fmt.Sprintf("chain[%d].evidence", i), step.Evidence); err != nil {
			return err
		}
	}
	for i, a := range v.Actions {
		if err := citeAll(cite, fmt.Sprintf("actions[%d].evidence", i), a.Evidence); err != nil {
			return err
		}
	}
	if len(v.Actions) == 0 && len(v.NextSteps) == 0 {
		return fmt.Errorf("actions, next_steps: both are empty; name at least one action for the owners of the next move or one further investigation step")
	}
	listed := map[GapRef]bool{}
	for i, g := range v.Gaps {
		key := GapRef{Ref: g.Ref, ID: g.ID}
		if !slices.Contains(want.Gaps, key) {
			return fmt.Errorf("gaps[%d]: %s gap %q is not in the request's gap list", i, describeRef(g.Ref), g.ID)
		}
		if listed[key] {
			return fmt.Errorf("gaps[%d]: %s gap %q is listed twice", i, describeRef(g.Ref), g.ID)
		}
		listed[key] = true
		if err := citeAll(cite, fmt.Sprintf("gaps[%d].evidence", i), g.Evidence); err != nil {
			return err
		}
	}
	for _, g := range want.Gaps {
		if !listed[g] {
			return fmt.Errorf("gaps: %s gap %q has no disposition; want every gap of the request once", describeRef(g.Ref), g.ID)
		}
	}
	if err := checkGaps("new_gaps", v.NewGaps); err != nil {
		return err
	}
	open := len(v.NewGaps) > 0 || want.Limit != "" || want.Claim.Outcome != "passed"
	for _, g := range v.Gaps {
		open = open || g.Disposition == "open"
	}
	if open && v.Completeness != "incomplete" {
		return fmt.Errorf("completeness: got complete; want incomplete, since a limit ended the investigation, the claim did not pass verification or a gap is open")
	}
	if len(p.Files) != 1 || p.Files[0].ID != ReportFileID || p.Files[0].Kind != "artifact" || p.Files[0].Path != "artifacts/triage-report.md" {
		return fmt.Errorf("files: want exactly the renderer's artifact %s at artifacts/triage-report.md; run the renderer once", ReportFileID)
	}
	var meta struct {
		Meta json.RawMessage `json:"meta"`
	}
	raw, err := engine.ReadContract(ctx, r, ref)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return err
	}
	documents := []reportDocument{}
	for _, input := range inputs {
		doc, err := engine.ReadContract(ctx, r, input)
		if err != nil {
			return err
		}
		d, err := contract.DecodePublication[json.RawMessage](doc)
		if err != nil {
			return err
		}
		documents = append(documents, reportDocument{input, d.Data})
	}
	rendered, err := renderReport(ctx, reportProjection{meta.Meta, v, documents})
	if err != nil {
		return err
	}
	got, err := rawFile(ctx, ref, p.Files, ReportFileID)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, rendered) {
		return fmt.Errorf("files: the report differs from the deterministic rendering of the committed inputs; run the renderer once on the final candidate")
	}
	return nil
}

// checkReportOwner requires the report to come from a closed fresh report
// session of its model.
func checkReportOwner(r *engine.Run, attemptID string, model runtime.ModelSpec) error {
	snapshot := r.Snapshot()
	attempt, ok := snapshot.Attempts[attemptID]
	owner, owned := snapshot.Sessions[attempt.HandleID]
	if !ok || !owned || owner.Role.Name != "triage-report" || owner.Role.Model != model || owner.State != "Closed" || attempt.Key != "report" {
		return fmt.Errorf("report %s must come from a closed fresh report session of its model", attemptID)
	}
	return nil
}

type reportDocument struct {
	Ref  contract.Ref    `json:"ref"`
	Data json.RawMessage `json:"data"`
}

type reportProjection struct {
	Meta      json.RawMessage  `json:"meta"`
	Data      Report           `json:"data"`
	Documents []reportDocument `json:"documents"`
}

type reportBuffer struct {
	buffer bytes.Buffer
	limit  int
}

func (b *reportBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.buffer.Len() {
		return 0, fmt.Errorf("report renderer output limit exceeded")
	}
	return b.buffer.Write(p)
}

// renderReport runs only the embedded renderer, so neither extracted files
// nor agent-supplied programs determine the expected bytes.
func renderReport(ctx context.Context, projection reportProjection) ([]byte, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	common, err := reportresource.Common.ReadFile("pwc_report_io.py")
	if err != nil {
		return nil, err
	}
	template, err := reportResources.ReadFile("report/render_report.py")
	if err != nil {
		return nil, err
	}
	input, err := json.Marshal(struct {
		Common     string           `json:"common"`
		Template   string           `json:"template"`
		Projection reportProjection `json:"projection"`
	}{string(common), string(template), projection})
	if err != nil {
		return nil, err
	}
	const program = "import json,sys,types\np=json.load(sys.stdin)\nm=types.ModuleType('pwc_report_io')\nexec(p['common'],m.__dict__)\nsys.modules[m.__name__]=m\nn={'__name__':'triage_report'}\nexec(p['template'],n)\nsys.stdout.buffer.write(n['render'](**p['projection']))\n"
	cmd := exec.CommandContext(ctx, "python3", "-I", "-S", "-B", "-c", program)
	cmd.Stdin = bytes.NewReader(input)
	stdout, stderr := &reportBuffer{limit: 64 << 20}, &reportBuffer{limit: 64 << 10}
	cmd.Stdout, cmd.Stderr, cmd.WaitDelay = stdout, stderr, time.Second
	if err := cmd.Run(); err != nil {
		return nil, errors.Join(context.Cause(ctx), fmt.Errorf("report renderer verification: %w: %s", err, stderr.buffer.String()))
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return stdout.buffer.Bytes(), nil
}

// runReport extracts the renderer into the run and has a fresh report
// Step write the report over every committed result of the investigation;
// a timed-out report reruns.
func runReport(ctx context.Context, r *engine.Run, s0 S0, skills Skills, out Rounds, rounds RoundPolicy) (contract.Ref, []RecoveryFailure, error) {
	policy, retries := rounds.Report, rounds.TimeoutRetries
	if err := policy.check(); err != nil {
		return contract.Ref{}, nil, err
	}
	sub, err := fs.Sub(reportResources, "report")
	if err != nil {
		return contract.Ref{}, nil, err
	}
	root, err := reportresource.ExtractFresh(r.Dir(), reportDir, sub, reportresource.Common)
	if err != nil {
		return contract.Ref{}, nil, err
	}
	_, input := r.WorkflowInput()
	// Every committed result with recorded gaps: the skills record and
	// what the rounds handed on, session observations included.
	t := newTask(r, "report", s0.Ticket, nil, reportRequirements, citationRequirements)
	t.Citable = append([]LabeledRef{{"skills", skills.Record}}, out.Inputs...)
	refs := t.inputs()
	gaps, err := reportGaps(ctx, r, refs)
	if err != nil {
		return contract.Ref{}, nil, err
	}
	want := reportTask{Request: input.Prompt, Claim: reportClaim(out), Gaps: gaps, Limit: out.Limit, Renderer: filepath.Join(root, "render_report.py")}
	for _, ref := range []*contract.Ref{want.Claim.Claim, want.Claim.Delivery, want.Claim.T2a, want.Claim.T2b} {
		if ref != nil && !slices.Contains(refs, *ref) {
			return contract.Ref{}, nil, fmt.Errorf("report inputs lack %s named by the claim", describeRef(*ref))
		}
	}
	if want.Gaps == nil {
		want.Gaps = []GapRef{}
	}
	t.Report = &want
	ref, failures, err := RetryInputs(ctx, r, r.Root(), "report-recovery", "report", retries, func(ctx context.Context, s *engine.Scope, retry *engine.Feedback) (contract.Ref, error) {
		ref, err := RunTaskStep(ctx, r, TaskStep{Scope: s, Model: policy.Model, Stage: "report", Key: "report", Task: t, Schema: ReportSchema, Inputs: refs, Recovery: true, Timeout: policy.Timeout, Feedback: retry,
			Validate: func(ctx context.Context, ref contract.Ref) error { return checkReport(ctx, r, ref, want, refs) }})
		if err != nil {
			return ref, err
		}
		return ref, checkReportOwner(r, ref.AttemptID, policy.Model)
	}, nil)
	if err != nil {
		return ref, failures, err
	}
	return ref, failures, r.Root().Decision(ctx, "report-recorded", "Report coverage, completeness and rendering accepted; its answer is the report's judgment", append(slices.Clone(refs), ref))
}
