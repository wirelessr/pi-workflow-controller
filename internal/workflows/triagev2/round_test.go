package triagev2

import (
	"context"
	"errors"
	"strings"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

func attachFixture(ctx context.Context, r *engine.Run, key, schema string, data any, files map[string][]byte) (contract.Ref, error) {
	var cf []contract.ControllerFile
	for id, b := range files {
		cf = append(cf, contract.ControllerFile{ID: id, Path: "evidence/" + id + ".txt", Data: b})
	}
	return r.Root().Attach(ctx, engine.AttachSpec{Key: key, Output: contract.Spec{SchemaID: schema}, Data: data, Files: cf})
}

func ptr[T any](v T) *T { return &v }

// roundFiles are a round's own evidence: two identity lookups and a query.
func roundFiles() map[string][]byte {
	return map[string][]byte{
		"lookup-a": []byte(`[{"tenant_id":"17","orgkey":"org-17","ui_hostname":"acme.example.invalid"}]`),
		"lookup-b": []byte(`[]`),
		"query-1":  []byte("3 errors between 22:00 and 23:00\n"),
		"excerpt":  []byte("svc@abc123 main.go:10-12\nif !flag { return errFailed }\n"),
	}
}

// baseRound is a valid round over the given intake: it confirms the home
// stack from one of two lookups and records one runtime query.
func baseRound(intake contract.Ref) Round {
	whole := ""
	own := func(id string) Evidence { return Evidence{FileID: id, Locator: &Locator{Pointer: &whole}} }
	return Round{Status: "continue", Summary: "Identity searched on both stacks; one query ran.",
		FactsUpdate: []Fact{{ID: "host", Kind: "ui_hostname", Value: "acme.example.invalid", Evidence: []Evidence{cite(intake, "page-1")}}},
		TimeAnchors: []TimeAnchor{},
		Identity: &Identity{
			Lookups: []Lookup{
				{ID: "l-a", Stack: "pop-a", Kubeconfig: ptr("kc-a"), Context: ptr("ctx-a"), Status: "ok", Rows: []Row{{TenantID: ptr("17"), Orgkey: ptr("org-17"), UIHostname: ptr("acme.example.invalid")}}, Evidence: []Evidence{own("lookup-a")}},
				{ID: "l-b", Stack: "pop-b", Kubeconfig: ptr("kc-b"), Context: ptr("ctx-b"), Status: "ok", Rows: []Row{}, Evidence: []Evidence{own("lookup-b")}},
			},
			Decisions: []Decision{
				{ID: "d-pop", Fact: "home_pop", Value: ptr("pop-a"), Status: "confirmed", Lookup: ptr("l-a"), Row: ptr(0), Identifiers: []string{"tenant_id", "orgkey"}, Reason: "only stack whose row matches two identifiers"},
				{ID: "d-tenant", Fact: "tenant_id", Value: ptr("17"), Status: "confirmed", Lookup: ptr("l-a"), Row: ptr(0), Identifiers: []string{"orgkey"}, Reason: "row matches the orgkey"},
			},
		},
		Receipts: []Receipt{{ID: "q1", Target: "pop-a", Source: "logs", Condition: "level=error", From: "2025-01-01T22:00:00Z", To: "2025-01-01T23:00:00Z",
			TimeBasis: []Evidence{cite(intake, "page-0")}, Status: "ok", Outcome: "3 errors", Evidence: []Evidence{{FileID: "query-1"}}}},
		DeployedBuilds:  []Build{},
		VisionRequests:  []VisionRequest{},
		Gaps:            []Gap{{ID: "g1", Text: "metrics not yet read"}},
		GapDispositions: []GapDisposition{},
	}
}

func deployedBuild() []Build {
	return []Build{{ID: "b1", Stack: "pop-a", Component: "svc", Build: "1.2.3", Evidence: []Evidence{{FileID: "query-1", Locator: &Locator{Offset: ptr(int64(0)), Length: ptr(int64(8))}}}}}
}

// codeClaim is a candidate that read deployed code of build id in this
// round (nil owner) or in owner.
func codeClaim(id string, owner *contract.Ref) *Candidate {
	excerpt := Evidence{FileID: "excerpt"}
	return &Candidate{Statement: "svc returns the error when the flag is off", Premises: []string{"the flag is off on pop-a"},
		AllowedEvidence: []Evidence{{FileID: "query-1"}, excerpt},
		CodeRefs:        []CodeRef{{Repo: "svc", Ref: "abc123", Path: "main.go", Relation: "deployed", Basis: &BuildRef{Ref: owner, ID: id}, Evidence: []Evidence{excerpt}}},
		Verification:    "runtime-verified"}
}

// Contracts are committed with Attach only to get exact committed Refs for
// the checks; the checks never look at who produced them.
func TestCheckRound(t *testing.T) {
	type refs struct{ intake, prompt, status, other contract.Ref }
	for _, tc := range []struct {
		name   string
		change func(*Round, refs)
		absent map[string]string
		want   string
		schema bool
	}{
		{name: "valid", change: func(*Round, refs) {}},
		{name: "confirmed stack differs from the lookup", change: func(v *Round, _ refs) { v.Identity.Decisions[0].Value = ptr("pop-b") },
			want: `identity.decisions[0].value: got "pop-b"; want "pop-a", the stack of lookup "l-a" row 0`},
		{name: "confirmed tenant differs from the row", change: func(v *Round, _ refs) { v.Identity.Decisions[1].Value = ptr("71") },
			want: `identity.decisions[1].value: got "71"; want "17", the tenant_id of lookup "l-a" row 0`},
		{name: "row outside the lookup", change: func(v *Round, _ refs) { v.Identity.Decisions[0].Row = ptr(1) }, want: `identity.decisions[0].row: got 1; lookup "l-a" has 1 rows`},
		{name: "unknown lookup", change: func(v *Round, _ refs) { v.Identity.Decisions[0].Lookup = ptr("l-c") }, want: `identity.decisions[0].lookup: got "l-c"`},
		{name: "confirmed from a failed lookup", change: func(v *Round, _ refs) { v.Identity.Lookups[0].Status = "failed" }, want: "has status failed"},
		{name: "identifier the row lacks", change: func(v *Round, _ refs) { v.Identity.Decisions[0].Identifiers = []string{"name"} }, want: "identity.decisions[0].identifiers: name is null"},
		{name: "two confirmed home stacks", change: func(v *Round, _ refs) {
			d := v.Identity.Decisions[0]
			d.ID = "d-pop-2"
			v.Identity.Decisions = append(v.Identity.Decisions, d)
		}, want: "home_pop is already confirmed at identity.decisions[0]"},
		{name: "unconfirmed decision needs no row", change: func(v *Round, _ refs) {
			v.Identity.Decisions[0] = Decision{ID: "d-pop", Fact: "home_pop", Status: "unconfirmed", Identifiers: []string{}, Reason: "same row on two stacks"}
		}},
		{name: "confirmed decision without a row", change: func(v *Round, _ refs) { v.Identity.Decisions[0].Row = nil }, schema: true},
		{name: "ok lookup without evidence", change: func(v *Round, _ refs) { v.Identity.Lookups[1].Evidence = []Evidence{} }, schema: true},
		{name: "unavailable stack without a kubeconfig", change: func(v *Round, _ refs) {
			v.Identity.Lookups[1] = Lookup{ID: "l-b", Stack: "pop-b", Status: "unavailable", Rows: []Row{}, Evidence: []Evidence{}, Note: "no kubeconfig"}
		}},
		{name: "duplicate id across sections", change: func(v *Round, _ refs) { v.Receipts[0].ID = "host" }, want: `receipts[0].id: "host" is already used at facts_update[0]`},
		{name: "receipt window reversed", change: func(v *Round, _ refs) { v.Receipts[0].From, v.Receipts[0].To = v.Receipts[0].To, v.Receipts[0].From }, want: "receipts[0]: from \"2025-01-01T23:00:00Z\" is not strictly before"},
		{name: "receipt window not RFC 3339", change: func(v *Round, _ refs) { v.Receipts[0].From = "2025-01-01 22:00Z" }, want: "receipts[0].from"},
		{name: "receipt without result evidence", change: func(v *Round, _ refs) { v.Receipts[0].Evidence = []Evidence{} }, schema: true},
		{name: "receipt citing a file it does not have", change: func(v *Round, _ refs) { v.Receipts[0].Evidence = []Evidence{{FileID: "query-2"}} }, want: "receipts[0].evidence[0].file_id"},
		{name: "build citation resolves", change: func(v *Round, _ refs) { v.DeployedBuilds = deployedBuild() }},
		{name: "candidate with evidence outside the inputs", change: func(v *Round, in refs) {
			v.Status = "candidate"
			v.Candidate = &Candidate{Statement: "x", Premises: []string{}, AllowedEvidence: []Evidence{{Ref: &in.other, FileID: "prompt"}}, CodeRefs: []CodeRef{}, NoCodeBasis: true, Verification: "inference"}
		}, want: "candidate.allowed_evidence[0].ref"},
		{name: "claim reading the deployed code of a build in this round", change: func(v *Round, _ refs) {
			v.Status, v.DeployedBuilds, v.Candidate = "candidate", deployedBuild(), codeClaim("b1", nil)
		}},
		{name: "claim citing a build this round does not have", change: func(v *Round, _ refs) {
			v.Status, v.DeployedBuilds, v.Candidate = "candidate", deployedBuild(), codeClaim("b2", nil)
		},
			want: `candidate.code_refs[0].basis.id: got "b2"; want the id of a build in this round's deployed_builds`},
		{name: "claim citing a build of an input without builds", change: func(v *Round, in refs) {
			v.Status, v.DeployedBuilds, v.Candidate = "candidate", deployedBuild(), codeClaim("b1", &in.status)
		}, want: "candidate.code_refs[0].basis.id: got \"b1\"; want the id of a build in the deployed_builds of attempt"},
		{name: "claim citing a build of a contract outside the inputs", change: func(v *Round, in refs) {
			v.Status, v.DeployedBuilds, v.Candidate = "candidate", deployedBuild(), codeClaim("b1", &in.other)
		}, want: "candidate.code_refs[0].basis.ref"},
		{name: "code excerpt the verifier may not read", change: func(v *Round, _ refs) {
			v.Status, v.DeployedBuilds, v.Candidate = "candidate", deployedBuild(), codeClaim("b1", nil)
			v.Candidate.AllowedEvidence = []Evidence{{FileID: "query-1"}}
		}, want: `candidate.code_refs[0].evidence[0]: file "excerpt" is not in candidate.allowed_evidence`},
		{name: "deployed code without its build", change: func(v *Round, _ refs) {
			v.Status, v.Candidate = "candidate", codeClaim("b1", nil)
			v.Candidate.CodeRefs[0].Basis = nil
		}, schema: true},
		{name: "claim without code and without saying so", change: func(v *Round, _ refs) {
			v.Status, v.Candidate = "candidate", codeClaim("b1", nil)
			v.Candidate.CodeRefs = []CodeRef{}
		}, schema: true},
		{name: "claim with code that says it has none", change: func(v *Round, _ refs) {
			v.Status, v.DeployedBuilds, v.Candidate = "candidate", deployedBuild(), codeClaim("b1", nil)
			v.Candidate.NoCodeBasis = true
		}, schema: true},
		{name: "receipt with only a non-breaking space as outcome", change: func(v *Round, _ refs) { v.Receipts[0].Outcome = "\u00a0" }, want: "receipts[0]: source, condition and outcome must not be blank"},
		{name: "candidate without a claim", change: func(v *Round, _ refs) { v.Status = "candidate" }, schema: true},
		{name: "claim on a continue round", change: func(v *Round, in refs) {
			v.Candidate = &Candidate{Statement: "x", Premises: []string{}, AllowedEvidence: []Evidence{{FileID: "query-1"}}, CodeRefs: []CodeRef{}, NoCodeBasis: true, Verification: "inference"}
		}, schema: true},
		{name: "blocked without what would unblock it", change: func(v *Round, _ refs) { v.Status = "blocked" }, schema: true},
		{name: "blocked with what would unblock it", change: func(v *Round, _ refs) { v.Status, v.Unblock = "blocked", ptr("a kubeconfig for pop-c") }},
		{name: "vision request on a stuck round", change: func(v *Round, in refs) {
			v.Status = "stuck"
			v.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: &in.intake, FileID: "bundle"}, Question: "what does it show"}}
		}, schema: true},
		{name: "vision request on a continue round", change: func(v *Round, in refs) {
			v.VisionRequests = []VisionRequest{{ID: "shot", Attachment: Evidence{Ref: &in.intake, FileID: "bundle"}, Question: "what does it show"}}
		}},
		{name: "disposition of an earlier gap", change: func(v *Round, in refs) {
			v.GapDispositions = []GapDisposition{{Gap: GapRef{Ref: in.status, ID: "not-accepted-pop"}, Disposition: "resolved", Reason: "confirmed by the lookup", Evidence: []Evidence{}}}
		}},
		{name: "disposition of a gap the input lacks", change: func(v *Round, in refs) {
			v.GapDispositions = []GapDisposition{{Gap: GapRef{Ref: in.status, ID: "g9"}, Disposition: "resolved", Reason: "x", Evidence: []Evidence{}}}
		}, want: `gap_dispositions[0].gap.id: attempt`},
		{name: "unconfirmed decision is never recorded absent", change: func(v *Round, _ refs) {
			v.Identity.Decisions[0] = Decision{ID: "d-pop", Fact: "home_pop", Status: "unconfirmed", Identifiers: []string{}, Reason: "same row on two stacks"}
		}, absent: map[string]string{"identity:home_pop=pop-a": "not-accepted-r1-d-pop"}},
		{name: "disposition naming a contract outside the inputs", change: func(v *Round, in refs) {
			v.GapDispositions = []GapDisposition{{Gap: GapRef{Ref: in.other, ID: "g1"}, Disposition: "not-applicable", Reason: "x", Evidence: []Evidence{}}}
		}, want: "gap_dispositions[0].gap.ref"},
		{name: "fact recorded absent declared again", change: func(*Round, refs) {}, absent: map[string]string{"fact:ui_hostname=acme.example.invalid": "not-accepted-r1-host"},
			want: `facts_update[0]: ui_hostname "acme.example.invalid" was not accepted too often`},
		{name: "decision recorded absent declared again", change: func(*Round, refs) {}, absent: map[string]string{"identity:home_pop=pop-a": "not-accepted-r1-d-pop"},
			want: `identity.decisions[0]: confirming home_pop "pop-a" was not accepted too often`},
		{name: "anchor recorded absent declared again", change: func(v *Round, in refs) {
			v.TimeAnchors = []TimeAnchor{{ID: "event", Event: "reported failure", Original: "2025-01-02T00:30:00+02:00", Format: "rfc3339", SourceTZ: "+02:00", UTC: "2025-01-01T22:30:00Z", OffsetSeconds: 7200, Evidence: cite(in.intake, "page-0")}}
		}, absent: map[string]string{"anchor:reported failure@2025-01-01T22:30:00Z": "g"}, want: "time_anchors[0]: the anchor at 2025-01-01T22:30:00Z was not accepted too often"},
		{name: "blank gap text", change: func(v *Round, _ refs) { v.Gaps[0].Text = "\u00a0" }, want: "gaps[0].text"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			report := runSkills(t, func(ctx context.Context, r *engine.Run) error {
				v, files := intakeFiles()
				var in refs
				var e error
				if in.intake, e = attachFixture(ctx, r, "intake", IntakeSchema, v, files); e != nil {
					return e
				}
				if in.prompt, e = attachFixture(ctx, r, "prompt", PromptSchema, CallerPrompt{Ticket: "CASE-17"}, map[string][]byte{"prompt": []byte("CASE-17")}); e != nil {
					return e
				}
				if in.status, e = attachFixture(ctx, r, "status", FactStatusSchema, FactStatus{Facts: in.prompt, Check: in.prompt, Gaps: []Gap{{ID: "not-accepted-pop", Text: "pop not accepted"}}}, nil); e != nil {
					return e
				}
				if in.other, e = attachFixture(ctx, r, "other", PromptSchema, CallerPrompt{Ticket: "CASE-17"}, map[string][]byte{"prompt": []byte("CASE-17")}); e != nil {
					return e
				}
				round := baseRound(in.intake)
				tc.change(&round, in)
				ref, e := attachFixture(ctx, r, "round", RoundSchema, round, roundFiles())
				if tc.schema {
					var f *engine.Failure
					if !errors.As(e, &f) || f.Code != engine.ContractInvalid {
						t.Errorf("schema accepted the round: %v", e)
					}
					return nil
				}
				if e != nil {
					return e
				}
				_, err = checkRound(ctx, r, ref, roundCheck{inputs: []contract.Ref{in.intake, in.prompt, in.status}, absent: tc.absent})
				return nil
			})
			if report.Outcome != engine.Succeeded {
				t.Fatalf("fixture run failed: %v", report.Failure)
			}
			if got := errText(err); tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("checkRound = %q, want %q", got, tc.want)
			}
		})
	}
}
