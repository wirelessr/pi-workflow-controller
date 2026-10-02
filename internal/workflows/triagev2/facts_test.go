package triagev2

import (
	"context"
	"strings"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

// Contracts are committed with Attach only to get exact committed Refs for
// the checks; the checks never look at who produced them.
func TestCheckFactsAndFactCheck(t *testing.T) {
	type refs struct{ intake, prompt contract.Ref }
	attach := func(ctx context.Context, r *engine.Run, key, schema string, data any, files map[string][]byte) contract.Ref {
		var cf []contract.ControllerFile
		for id, b := range files {
			cf = append(cf, contract.ControllerFile{ID: id, Path: "evidence/" + id + ".txt", Data: b})
		}
		ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: key, Output: contract.Spec{SchemaID: schema}, Data: data, Files: cf})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	inputs := func(ctx context.Context, r *engine.Run) refs {
		v, files := intakeFiles()
		return refs{
			intake: attach(ctx, r, "intake", IntakeSchema, v, files),
			prompt: attach(ctx, r, "prompt", PromptSchema, CallerPrompt{Ticket: "CASE-17"}, map[string][]byte{"prompt": []byte("CASE-17")}),
		}
	}
	baseFacts := func(in refs) Facts {
		return factsFor(agentCall{Task: task{Citable: []LabeledRef{{"intake", in.intake}, {"caller prompt", in.prompt}}}})
	}
	for _, tc := range []struct {
		name   string
		change func(*Facts, refs)
		want   string
	}{
		{"valid", func(*Facts, refs) {}, ""},
		{"mistyped intake ref", func(f *Facts, in refs) { f.Intake.SHA256 = strings.Repeat("0", 64) }, "intake: got attempt"},
		{"citing the prompt file", func(f *Facts, in refs) { f.Facts[0].Evidence = []Evidence{cite(in.prompt, "prompt")} }, ""},
		{"citing a file the intake does not declare", func(f *Facts, in refs) { f.Facts[0].Evidence = []Evidence{cite(in.intake, "issue.json")} }, "facts[0].evidence[0].file_id"},
		{"id reused by an anchor", func(f *Facts, _ refs) { f.TimeAnchors[0].ID = "tenant" }, `"tenant" is already used at facts[0]`},
		{"anchor conversion wrong", func(f *Facts, _ refs) { f.TimeAnchors[0].UTC = "2025-01-02T00:30:00Z" }, "time_anchors[0].utc"},
		{"vision request not citing the intake", func(f *Facts, in refs) {
			f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: cite(in.prompt, "prompt"), Question: "what does it show"}}
		}, "vision_requests[0].attachment.ref"},
		{"vision request for an intake file", func(f *Facts, in refs) {
			f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: cite(in.intake, "bundle"), Question: "what does it show"}}
		}, ""},
	} {
		t.Run("facts/"+tc.name, func(t *testing.T) {
			var err error
			report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
				in := inputs(ctx, r)
				f := baseFacts(in)
				tc.change(&f, in)
				ref := attach(ctx, r, "facts", FactsSchema, f, nil)
				_, err = checkFacts(ctx, r, ref, in.intake, in.prompt)
				return nil
			})
			if report.Outcome != engine.Succeeded {
				t.Fatalf("fixture run failed: %v", report.Failure)
			}
			if got := errText(err); tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("checkFacts = %q, want %q", got, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*FactCheck)
		want   string
	}{
		{"valid", func(*FactCheck) {}, ""},
		{"missing a verdict", func(c *FactCheck) { c.Items = c.Items[1:] }, "no verdict for tenant"},
		{"verdict for an unknown id", func(c *FactCheck) { c.Items[0].ID = "other" }, `got "other"`},
		{"two verdicts for one id", func(c *FactCheck) { c.Items[1].ID = c.Items[0].ID }, "already has a verdict"},
		{"basis outside the inputs", func(c *FactCheck) { c.Items[0].Basis = []Evidence{{FileID: "nothing"}} }, "items[0].basis[0].file_id"},
	} {
		t.Run("factcheck/"+tc.name, func(t *testing.T) {
			var err error
			report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
				in := inputs(ctx, r)
				f := baseFacts(in)
				factsRef := attach(ctx, r, "facts", FactsSchema, f, nil)
				c := FactCheck{Subject: factsRef, Gaps: []Gap{}}
				for _, id := range append(factIDs(f), anchorIDs(f)...) {
					c.Items = append(c.Items, FactVerdict{ID: id, Verdict: "supported", Reason: "stated", Basis: []Evidence{cite(in.intake, "page-0")}})
				}
				tc.change(&c)
				ref := attach(ctx, r, "check", FactCheckSchema, c, nil)
				_, err = checkFactCheck(ctx, r, ref, factsRef, in.intake, in.prompt, f)
				return nil
			})
			if report.Outcome != engine.Succeeded {
				t.Fatalf("fixture run failed: %v", report.Failure)
			}
			if got := errText(err); tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("checkFactCheck = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseCallerPrompt(t *testing.T) {
	for _, tc := range []struct{ prompt, ticket, hints, err string }{
		{"CASE-17", "CASE-17", "", ""},
		{"  CASE-17   pop=a  check this ", "CASE-17", "pop=a  check this", ""},
		{"case-17 lowercase", "", "", "ticket key"},
		{"please check CASE-17", "", "", "ticket key"},
		{"CASE-0", "", "", "ticket key"},
	} {
		got, err := ParseCallerPrompt(tc.prompt)
		if got.Ticket != tc.ticket || got.Hints != tc.hints || tc.err == "" && err != nil || tc.err != "" && (err == nil || !strings.Contains(err.Error(), tc.err)) {
			t.Errorf("ParseCallerPrompt(%q) = %+v, %v", tc.prompt, got, err)
		}
	}
}
