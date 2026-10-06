package triage

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
		ref, err := attachFixture(ctx, r, key, schema, data, files)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	inputs := func(ctx context.Context, r *engine.Run) refs {
		v, files := intakeFiles()
		files["lines"] = []byte("{\"a\":1}\n{\"a\":2}\n")
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
		{"citing the prompt file", func(f *Facts, in refs) { f.Facts[0].Evidence = []Evidence{citeRange(in.prompt, "prompt", 0, 7)} }, ""},
		{"citing a file the intake does not declare", func(f *Facts, in refs) { f.Facts[0].Evidence = []Evidence{cite(in.intake, "issue.json")} }, "facts[0].evidence[0].file_id"},
		{"id reused by an anchor", func(f *Facts, _ refs) { f.TimeAnchors[0].ID = "tenant" }, `"tenant" is already used at facts[0]`},
		{"anchor conversion wrong", func(f *Facts, _ refs) { f.TimeAnchors[0].UTC = "2025-01-02T00:30:00Z" }, "time_anchors[0].utc"},
		{"vision request not citing the intake", func(f *Facts, in refs) {
			f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: &in.prompt, FileID: "prompt"}, Question: "what does it show"}}
		}, "vision_requests[0].attachment.ref"},
		{"vision request for an intake file", func(f *Facts, in refs) {
			f.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: &in.intake, FileID: "bundle"}, Question: "what does it show"}}
		}, ""},
		{"locator into the raw issue", func(f *Facts, in refs) {
			p := "/fields/customfield_1"
			f.Facts[0].Evidence = []Evidence{{Ref: &in.intake, FileID: "issue", Locator: &Locator{Pointer: &p}}}
		}, ""},
		{"locator to a missing JSON member", func(f *Facts, in refs) {
			p := "/fields/customfield_9"
			f.Facts[0].Evidence = []Evidence{{Ref: &in.intake, FileID: "issue", Locator: &Locator{Pointer: &p}}}
		}, `"/fields/customfield_9" does not exist`},
		{"byte range within the file", func(f *Facts, in refs) {
			o, l := int64(0), int64(5)
			f.Facts[0].Evidence = []Evidence{{Ref: &in.intake, FileID: "bundle", Locator: &Locator{Offset: &o, Length: &l}}}
		}, ""},
		{"byte range past the end", func(f *Facts, in refs) {
			o, l := int64(10), int64(1000)
			f.Facts[0].Evidence = []Evidence{{Ref: &in.intake, FileID: "bundle", Locator: &Locator{Offset: &o, Length: &l}}}
		}, "the cited file has"},
		{"pointer into a text file", func(f *Facts, in refs) {
			p := "/x"
			f.Facts[0].Evidence = []Evidence{{Ref: &in.intake, FileID: "bundle", Locator: &Locator{Pointer: &p}}}
		}, "not a single JSON document"},
		{"blank anchor event", func(f *Facts, _ refs) { f.TimeAnchors[0].Event = "\u00a0" }, "event and source_tz"},
		{"violations of one fact count toward the cap one by one", func(f *Facts, in refs) {
			f.Facts[0].Evidence = nil
			for range maxViolations + 5 {
				f.Facts[0].Evidence = append(f.Facts[0].Evidence, cite(in.intake, "issue.json"))
			}
			f.TimeAnchors[0].UTC = "2025-01-02T00:30:00Z"
		}, "facts[0].evidence[19].file_id*and 6 more violations not listed"},
		{"independent violations are all reported", func(f *Facts, in refs) {
			p := "/fields/customfield_9"
			f.Facts[0].Evidence = []Evidence{cite(in.intake, "issue.json"), {Ref: &in.intake, FileID: "issue", Locator: &Locator{Pointer: &p}}}
			f.TimeAnchors[0].UTC = "2025-01-02T00:30:00Z"
		}, "facts[0].evidence[0].file_id: got \"issue.json\"*facts[0].evidence[1].locator.pointer: \"/fields/customfield_9\" does not exist*time_anchors[0].utc"},
		{"own fetched file with a locator", func(f *Facts, _ refs) {
			p := "/fetched"
			f.Facts[0].Evidence = []Evidence{{FileID: "fetched", Locator: &Locator{Pointer: &p}}}
		}, ""},
		{"own fetched file with a missing member", func(f *Facts, _ refs) {
			p := "/other"
			f.Facts[0].Evidence = []Evidence{{FileID: "fetched", Locator: &Locator{Pointer: &p}}}
		}, `"/other" does not exist`},
		{"byte range that overflows", func(f *Facts, in refs) {
			f.Facts[0].Evidence = []Evidence{citeRange(in.intake, "bundle", 1<<62, 1<<62)}
		}, "the cited file has"},
		{"JSON lines cited by pointer", func(f *Facts, in refs) {
			p := "/a"
			f.Facts[0].Evidence = []Evidence{{Ref: &in.intake, FileID: "lines", Locator: &Locator{Pointer: &p}}}
		}, "not a single JSON document"},
		{"pointer with an invalid escape", func(f *Facts, in refs) {
			p := "/fields/a~2b"
			f.Facts[0].Evidence = []Evidence{{Ref: &in.intake, FileID: "issue", Locator: &Locator{Pointer: &p}}}
		}, "not ~0 or ~1"},
	} {
		t.Run("facts/"+tc.name, func(t *testing.T) {
			var err error
			report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
				in := inputs(ctx, r)
				f := baseFacts(in)
				tc.change(&f, in)
				ref := attach(ctx, r, "facts", FactsSchema, f, map[string][]byte{"fetched": []byte(`{"fetched":true}`)})
				_, err = checkFacts(ctx, r, ref, in.intake, in.prompt)
				return nil
			})
			if report.Outcome != engine.Succeeded {
				t.Fatalf("fixture run failed: %v", report.Failure)
			}
			if got := errText(err); tc.want == "" && got != "" || tc.want != "" && !containsInOrder(got, strings.Split(tc.want, "*")) {
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
		{"basis outside the inputs", func(c *FactCheck) {
			whole := ""
			c.Items[0].Basis = []Evidence{{FileID: "nothing", Locator: &Locator{Pointer: &whole}}}
		}, "items[0].basis[0].file_id"},
		{"insufficient verdict", func(c *FactCheck) { c.Items[0].Verdict = "insufficient" }, ""},
		{"warning citing the facts file under review", func(c *FactCheck) {
			c.Warnings = []Warning{{Text: "the caller prompt and the ticket disagree", Basis: []Evidence{cite(c.Subject, "fetched")}}}
		}, ""},
		{"warning citing a file the facts do not declare", func(c *FactCheck) {
			c.Warnings = []Warning{{Text: "the caller prompt and the ticket disagree", Basis: []Evidence{cite(c.Subject, "nothing")}}}
		}, "warnings[0].basis[0].file_id"},
		{"blank gap text", func(c *FactCheck) { c.Gaps = []Gap{{ID: "g", Text: "\u00a0"}} }, "gaps[0].text"},
	} {
		t.Run("factcheck/"+tc.name, func(t *testing.T) {
			var err error
			report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
				in := inputs(ctx, r)
				f := baseFacts(in)
				factsRef := attach(ctx, r, "facts", FactsSchema, f, map[string][]byte{"fetched": []byte(`{"fetched":true}`)})
				c := FactCheck{Subject: factsRef, Warnings: []Warning{}, Gaps: []Gap{}}
				for _, id := range judgedIDs(f) {
					c.Items = append(c.Items, FactVerdict{ID: id, Verdict: "supported", Reason: "stated", Basis: []Evidence{cite(in.intake, "page-0")}})
				}
				tc.change(&c)
				ref := attach(ctx, r, "check", FactCheckSchema, c, nil)
				_, err = checkFactCheck(ctx, r, ref, factsRef, []contract.Ref{f.Intake, f.Prompt, factsRef}, judgedIDs(f))
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

// containsInOrder reports whether s contains each part, in order.
func containsInOrder(s string, parts []string) bool {
	for _, p := range parts {
		i := strings.Index(s, p)
		if i < 0 {
			return false
		}
		s = s[i+len(p):]
	}
	return true
}
