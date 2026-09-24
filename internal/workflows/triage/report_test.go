package triage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/contract/reportresource"
	"pi-workflow-controller/internal/engine"
)

// This is a pure projection test, not evidence of engine commit authority.
func TestTriageReportProjection(t *testing.T) {
	const special = "原始問題 | <script> & [link] \\\n````````\n# still JSON\n"
	ref := func(id string) contract.Ref {
		return contract.Ref{RunID: "run", AttemptID: id, Path: "/run/" + id + "/output.json", SchemaID: id, SHA256: strings.Repeat("a", 64)}
	}
	stateRef, contextRef, claimRef, assessmentRef, resultRef := ref("state"), ref("context"), ref("claim"), ref("assessment"), ref("verifier")
	basis := []Evidence{{Ref: &contextRef, FileID: "raw"}}
	assessment := VerificationAssessment{Support: special, Reason: special, Basis: basis, RuntimeBasis: basis, Measurement: special, Window: special, Filter: special, Environment: special, Release: special, Counterexamples: []VerificationIssue{{Statement: special, Disposition: special, Reason: special, Basis: basis}}, Gaps: []string{special}}
	claim := PureClaim{ParentState: assessmentRef, Context: contextRef, Candidate: ClaimCandidate{ID: "candidate", Statement: special, Premises: []string{special}, AllowedEvidence: basis}}
	delivery := VerificationDelivery{ID: "delivery", Proposal: assessmentRef, Claim: claimRef, Roles: []VerificationRoleDelivery{{Role: "pro", Result: &resultRef}, {Role: "con", Unavailable: true}, {Role: "cross", Unavailable: true}}}
	review := PlannerVerificationReview{DeliveryID: delivery.ID, Claim: claimRef, Assessment: assessment, Disputes: assessment.Counterexamples, NextAction: "yield"}
	owner := PlannerState{Verification: &PlannerVerification{Claims: []contract.Ref{claimRef}, Deliveries: []VerificationDelivery{delivery}}, VerificationReview: &review}
	state := PlannerState{Context: contextRef, Previous: &assessmentRef, Gaps: []string{special}, Rationale: special}
	contextData := Context{Problem: special, Identity: Identity{Status: special, Stack: Fact{Value: special, Evidence: basis}}, Time: TimeResolution{Status: special, From: special, To: special, Anchors: []TimeAnchor{{Event: special, Original: special, Format: special, SourceTZ: special, UTC: special, Evidence: basis[0]}}}, Observations: []Fact{{Value: special, Evidence: basis}}}
	result := VerificationResult{Claim: claimRef, Role: "pro", AllowedEvidence: basis, Assessment: assessment}
	data := InvestigationReport{State: stateRef, Context: contextRef, Claims: []ReportClaim{{claimRef, delivery.ID, assessmentRef}}, Completeness: "incomplete", Closure: special, Gaps: []string{special}, NextSteps: []string{special}, ReportFile: ReportFileID}
	projection := reportProjection{Meta: testJSON(map[string]any{"version": 1, "run_id": "run"}), Data: data, Documents: []reportDocument{{stateRef, testJSON(state)}, {contextRef, testJSON(contextData)}, {claimRef, testJSON(claim)}, {assessmentRef, testJSON(owner)}, {resultRef, testJSON(result)}}}
	before := testJSON(projection)
	first, err := renderReport(context.Background(), projection)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(projection.Documents)
	second, err := renderReport(context.Background(), projection)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("nondeterministic projection: %v", err)
	}
	slices.Reverse(projection.Documents)
	if !bytes.Equal(before, testJSON(projection)) {
		t.Fatal("renderer mutated input")
	}
	var blocks []any
	tail := string(first)
	for {
		start := strings.Index(tail, "\n```")
		if start < 0 {
			break
		}
		header, body, ok := strings.Cut(tail[start+1:], "\n")
		if !ok || (!strings.HasSuffix(header, "json") && !strings.HasSuffix(header, "text")) {
			t.Fatal("invalid fence")
		}
		fence := strings.TrimSuffix(strings.TrimSuffix(header, "json"), "text")
		encoded, next, ok := strings.Cut(body, "\n"+fence+"\n")
		if !ok {
			t.Fatal("unclosed fence")
		}
		var value any = encoded
		if strings.HasSuffix(header, "json") {
			if err := json.Unmarshal([]byte(encoded), &value); err != nil {
				t.Fatal(err)
			}
		}
		blocks = append(blocks, value)
		tail = next
	}
	for _, expected := range []any{special, stateRef, contextRef, claimRef, assessmentRef, resultRef, "raw", "candidate", "delivery", "yield"} {
		var want any
		if err := json.Unmarshal(testJSON(expected), &want); err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(blocks, func(got any) bool { return reflect.DeepEqual(got, want) }) {
			t.Fatalf("report omitted original value or exact reference: %v", expected)
		}
	}
	for _, label := range []string{"問題", "身份解析狀態", "已解析時間線", "Claim 原文", "前提原文", "支持程度", "支持理由", "量測條件", "查詢時間窗", "查詢條件", "環境", "版本", "反例", "分歧", "Runtime 依據", "Unavailable", "缺口", "下一步"} {
		if !bytes.Contains(first, []byte(label)) {
			t.Errorf("report omitted field %s", label)
		}
	}
	var reference map[string]any
	if err := json.Unmarshal(testJSON(stateRef), &reference); err != nil {
		t.Fatal(err)
	}
	canonical, err := json.MarshalIndent(reference, "", "  ")
	if err != nil || !bytes.Contains(first, canonical) {
		t.Fatalf("exact reference ordering is not canonical: %v", err)
	}
	if !bytes.Contains(first, []byte("`````````text")) {
		t.Fatal("embedded fences were not escaped")
	}
	for _, action := range []string{"unresolved", "handled", "redirect", "budget"} {
		t.Run("M6-"+action, func(t *testing.T) {
			p := projection
			failure := RecoveryFailure{Stage: "verify-con", Diagnostic: "preserved original diagnostic", StepID: "RETAINED_FAILURE_STEP", AttemptID: "RETAINED_FAILURE_ATTEMPT"}
			failure.Identity.HandleID = "RETAINED_FAILURE_HANDLE"
			disposition := ReportDisposition{Item: ReportFailure{Owner: assessmentRef, Kind: "verification", DeliveryID: "old-delivery", Claim: &claimRef, Role: "con", Failure: failure}, Action: action, Reason: special, Results: []contract.Ref{}, Basis: basis}
			if action == "handled" {
				disposition.Results = []contract.Ref{resultRef}
			}
			p.Data.M6 = &ReportMetadata{Dispositions: []ReportDisposition{disposition}, ReportFailures: []RecoveryFailure{{Stage: "report", Diagnostic: "preserved report retry diagnostic"}}}
			if action == "budget" {
				p.Data.M6.Dispositions[0].Action = "unresolved"
				p.Data.M6.Budget = &ReportBudget{Policy: ReportPolicy{ReportRetries: 2, ReserveSessions: 2, ReserveAttempts: 3}, MaxSessions: 19, MaxAttempts: 29, MaxLive: 4, UsedSessions: 13, UsedAttempts: 23, LiveSessions: 1, Action: "verify", Rejected: investigationCost{Sessions: 6, Attempts: 8, Live: 3}, Reason: "resource-limited projection fixture"}
			}
			before := testJSON(p)
			got, err := renderReport(context.Background(), p)
			if err != nil {
				t.Fatal(err)
			}
			again, err := renderReport(context.Background(), p)
			if err != nil || !bytes.Equal(got, again) || !bytes.Equal(before, testJSON(p)) {
				t.Fatalf("M6 projection changed input or output: %v", err)
			}
			for _, value := range []string{"RETAINED_FAILURE_STEP", "RETAINED_FAILURE_ATTEMPT", "RETAINED_FAILURE_HANDLE", "preserved original diagnostic", "preserved report retry diagnostic", "old-delivery", "Planner 處置", "處置依據", "最初接收此項目的歷史 owner"} {
				if !bytes.Contains(got, []byte(value)) {
					t.Errorf("M6 projection omitted %s", value)
				}
			}
			if action == "handled" && !bytes.Contains(got, []byte("後續成功結果")) {
				t.Fatal("handled projection omitted result Ref")
			}
			if action == "budget" {
				for _, value := range []string{"調查容量限制", "reserve_sessions", "reserve_attempts", "additional_live", "非原子 reservation"} {
					if !bytes.Contains(got, []byte(value)) {
						t.Errorf("budget projection omitted %s", value)
					}
				}
			}
		})
	}
}

func TestTriageReportOutputBoundary(t *testing.T) {
	stateRef := contract.Ref{AttemptID: "state"}
	contextRef := contract.Ref{AttemptID: "context"}
	projection := reportProjection{
		Meta: testJSON(map[string]any{"run_id": "run"}),
		Data: InvestigationReport{State: stateRef, Context: contextRef, Claims: []ReportClaim{}, Completeness: "incomplete", Closure: "closure retained", Gaps: []string{"gap retained"}, NextSteps: []string{"next retained"}},
		Documents: []reportDocument{
			{stateRef, testJSON(map[string]any{"rationale": "rationale retained", "hypotheses": []any{}, "ledger": "PRIVATE_LEDGER", "checkpoint": "PRIVATE_CHECKPOINT", "pending": "PRIVATE_PENDING", "recovery": "PRIVATE_RECOVERY", "handle_id": "PRIVATE_HANDLE", "step": "PRIVATE_STEP"})},
			{contextRef, testJSON(map[string]any{"problem": "problem retained", "identity": Identity{}, "time": TimeResolution{}, "attempts": "PRIVATE_ATTEMPTS", "previous": "PRIVATE_CONTEXT_CHAIN"})},
		},
	}
	out, err := renderReport(context.Background(), projection)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"PRIVATE_LEDGER", "PRIVATE_CHECKPOINT", "PRIVATE_PENDING", "PRIVATE_RECOVERY", "PRIVATE_ATTEMPTS", "PRIVATE_CONTEXT_CHAIN", "PRIVATE_HANDLE", "PRIVATE_STEP"} {
		if bytes.Contains(out, []byte(private)) {
			t.Errorf("report leaked internal state: %s", private)
		}
	}
	for _, retained := range []string{"problem retained", "rationale retained", "closure retained", "gap retained", "next retained"} {
		if !bytes.Contains(out, []byte(retained)) {
			t.Errorf("report omitted %s", retained)
		}
	}
}

func TestTriageReportBufferCopyLimit(t *testing.T) {
	for _, size := range []int{7, 8, 9} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			buffer := &reportBuffer{limit: 8}
			n, err := io.Copy(buffer, io.LimitReader(strings.NewReader(strings.Repeat("x", size)), int64(size)))
			if size > 8 {
				if err == nil || err.Error() != "report renderer output limit exceeded" || n != 0 || buffer.buffer.Len() != 0 {
					t.Fatalf("io.Copy bypassed limit: copied=%d error=%v", n, err)
				}
			} else if err != nil || n != int64(size) {
				t.Fatalf("bounded copy: copied=%d error=%v", n, err)
			}
		})
	}
}

// Real producer I/O only; engine authorization is covered by the RPC cases.
func TestTriageReportProducerManyRefs(t *testing.T) {
	root := t.TempDir()
	renderer, err := ExtractReport(root)
	if err != nil {
		t.Fatal(err)
	}
	const count, fdLimit = 96, 64
	var refs []contract.Ref
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("input-%d", i)
		path := filepath.Join(root, "inputs", id, "published", "output.json")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		data := any(Context{Problem: "many refs retained"})
		if i == 0 {
			data = PlannerState{Rationale: "producer retained"}
		}
		raw := testJSON(map[string]any{"meta": map[string]any{"version": 1, "run_id": "run", "attempt_id": id, "schema_id": "fixture"}, "data": data, "files": []any{}})
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, contract.Ref{RunID: "run", AttemptID: id, SchemaID: "fixture", Path: path, SHA256: fmt.Sprintf("%x", sha256.Sum256(raw))})
	}
	identity := map[string]any{"run_id": "run", "attempt_id": "report"}
	data := InvestigationReport{State: refs[0], Context: refs[1], Claims: []ReportClaim{}, Completeness: "incomplete", Closure: "bounded descriptors", Gaps: []string{}, NextSteps: []string{}}
	request := map[string]any{"identity": identity, "output": map[string]any{"schema_id": ReportSchema}, "inputs": refs, "prompt": string(testJSON(reportTask{Owners: []reportOwner{}}))}
	candidate := map[string]any{"meta": map[string]any{"version": 1, "run_id": "run", "attempt_id": "report", "schema_id": ReportSchema}, "data": data, "files": []any{}}
	for name, value := range map[string]any{"request.json": request, "candidate.json": candidate} {
		if err := os.WriteFile(filepath.Join(root, name), testJSON(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const program = "import resource,runpy,sys\nresource.setrlimit(resource.RLIMIT_NOFILE,(64,64))\nsys.argv=sys.argv[1:]\nrunpy.run_path(sys.argv[0],run_name='__main__')\n"
	cmd := exec.CommandContext(ctx, "python3", "-B", "-c", program, renderer, filepath.Join(root, "request.json"), filepath.Join(root, "candidate.json"))
	cmd.WaitDelay = time.Second
	output, err := cmd.CombinedOutput()
	t.Logf("inputs=%d child RLIMIT_NOFILE=%d input parent depth=%d", count, fdLimit, len(strings.Split(filepath.Dir(refs[0].Path), string(os.PathSeparator)))-1)
	if err != nil {
		t.Fatalf("real producer: %v: %s", err, output)
	}
	out, err := os.ReadFile(filepath.Join(root, "artifacts", "triage-report.md"))
	if err != nil || !bytes.Contains(out, []byte("many refs retained")) {
		t.Fatalf("missing producer artifact: %v", err)
	}
	var envelope struct {
		Data  InvestigationReport
		Files []contract.FileEntry
	}
	raw, err := os.ReadFile(filepath.Join(root, "candidate.json"))
	if err != nil || json.Unmarshal(raw, &envelope) != nil || envelope.Data.ReportFile != ReportFileID || len(envelope.Files) != 1 {
		t.Fatalf("candidate registration failed: %v", err)
	}
}

func TestTriageReportHostOutputLimit(t *testing.T) {
	stateRef, contextRef := contract.Ref{AttemptID: "state"}, contract.Ref{AttemptID: "context"}
	projection := reportProjection{Meta: testJSON(map[string]any{}), Data: InvestigationReport{State: stateRef, Context: contextRef, Closure: strings.Repeat("x", 64<<20), Claims: []ReportClaim{}, Gaps: []string{}, NextSteps: []string{}}, Documents: []reportDocument{{stateRef, testJSON(PlannerState{})}, {contextRef, testJSON(Context{})}}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := renderReport(ctx, projection)
	if out != nil || err == nil || !strings.Contains(err.Error(), "report renderer output limit exceeded") || context.Cause(ctx) != nil {
		t.Fatalf("host renderer did not return its output boundary error: bytes=%d error=%v context=%v", len(out), err, context.Cause(ctx))
	}
}

func TestTriageReportRendererFailures(t *testing.T) {
	for _, name := range []string{"cancel", "missing-executable", "missing-document", "native-deadline"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("report cancelled")
			if name == "cancel" {
				cancel(cause)
			}
			if name == "missing-executable" {
				t.Setenv("PATH", t.TempDir())
			}
			if name == "native-deadline" {
				deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stop()
				ctx = deadline
			}
			out, err := renderReport(ctx, reportProjection{})
			if err == nil || out != nil {
				t.Fatal("renderer failure became a report")
			}
			if name == "native-deadline" && (!errors.Is(err, context.DeadlineExceeded) || err != context.Cause(ctx)) {
				t.Fatalf("lost original standard deadline cause: %v", err)
			}
			if name == "cancel" && !errors.Is(err, cause) {
				t.Fatalf("lost cancellation: %v", err)
			}
			if name == "missing-executable" && !errors.Is(err, exec.ErrNotFound) {
				t.Fatalf("lost executable error: %v", err)
			}
			if name == "missing-document" {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || !exit.Exited() {
					t.Fatalf("lost waited renderer exit: %v", err)
				}
				projection := reportProjection{Meta: testJSON(map[string]any{}), Data: InvestigationReport{State: contract.Ref{AttemptID: "missing-state"}, Context: contract.Ref{AttemptID: "missing-context"}}, Documents: []reportDocument{}}
				out, err = renderReport(ctx, projection)
				if out != nil || !errors.As(err, &exit) || !exit.Exited() || !strings.Contains(err.Error(), "report requires one exact input owner") {
					t.Fatalf("missing owner did not reach document boundary: %v", err)
				}
			}
		})
	}
}

// These routes test existing mechanical predicates, not the authority of synthetic Refs.
func pureValidationStage(name string) string {
	switch name {
	case "m5-policy-pro-model", "m5-policy-con-model", "m5-policy-cross-model", "m5-policy-pro-retries", "m5-policy-con-retries", "m5-policy-cross-retries":
		return "verification-policy"
	case "m3-policy-invalid-zero", "m3-policy-invalid-negative", "m3-policy-invalid-above", "m3-policy-invalid-nan", "m3-policy-invalid-infinite":
		return "capacity-policy"
	case "m6-policy-negative-retry", "m6-policy-negative-session", "m6-policy-negative-attempt", "m6-policy-overflow", "m6-policy-retry-session-short", "m6-policy-retry-attempt-short":
		return "policy"
	case "m6-metadata-missing", "m6-history-missing-item", "m6-budget-echo":
		return "header"
	case "m6-history-duplicate-item", "m6-history-wrong-version":
		return "tuple"
	case "m6-history-no-basis":
		return "action"
	case "r5-state-binding", "r5-context-binding":
		return "binding"
	case "m5-claim-projection", "m5-claim-context":
		return "projection"
	}
	return ""
}

func TestTriagePureValidation(t *testing.T) {
	var names []string
	for _, tc := range triageCases {
		if pureValidationStage(tc.name) != "" {
			names = append(names, tc.name)
		}
	}
	var reportNames []string
	for name := range m6Cases {
		if pureValidationStage(name) != "" {
			reportNames = append(reportNames, name)
		}
	}
	slices.Sort(reportNames)
	names = append(names, reportNames...)
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			stage := pureValidationStage(name)
			if stage == "verification-policy" || stage == "capacity-policy" {
				var err error
				if stage == "verification-policy" {
					if err := verificationPolicyFixture("").check(); err != nil {
						t.Fatal("invalid verification prerequisite: ", err)
					}
					err = verificationPolicyFixture(name).check()
				} else {
					if err := capacityPolicyFixture("").check(); err != nil {
						t.Fatal("invalid capacity prerequisite: ", err)
					}
					// NaN/Inf must reach the policy, not a JSON encoder.
					err = capacityPolicyFixture(name).check()
				}
				var execution *engine.Failure
				if errors.As(err, &execution) || err == nil || err.Error() != validationError(name) {
					t.Fatalf("wrong policy rejection: %v", err)
				}
				if err := validationRejection(name, err); err != nil {
					t.Fatal(err)
				}
				return
			}
			if stage == "policy" {
				policy := ReportPolicy{ReportRetries: 1, ReserveSessions: 1, ReserveAttempts: 2}
				if err := policy.check(); err != nil {
					t.Fatal("invalid policy prerequisite: ", err)
				}
				switch name {
				case "m6-policy-negative-retry":
					policy.ReportRetries = -1
				case "m6-policy-negative-session":
					policy.ReserveSessions = -1
				case "m6-policy-negative-attempt":
					policy.ReserveAttempts = -1
				case "m6-policy-overflow":
					policy.ReportRetries = int(^uint(0) >> 1)
				case "m6-policy-retry-session-short":
					policy.ReserveSessions = 0
				case "m6-policy-retry-attempt-short":
					policy.ReserveAttempts = 1
				}
				if err := policy.check(); err == nil || err.Error() != "explicit report reserves must cover the report and all finite fresh retries" {
					t.Fatalf("wrong policy rejection: %v", err)
				}
				return
			}
			state, contextRef := contract.Ref{AttemptID: "state"}, contract.Ref{AttemptID: "context"}
			if stage == "projection" {
				candidate := ClaimCandidate{ID: "claim", Statement: "The proposed candidate", Premises: []string{"first", "second"}, AllowedEvidence: []Evidence{{Ref: &contextRef, FileID: "raw"}}}
				parent := PlannerState{Context: contextRef, Ledger: &InvestigationLedger{Action: "verify"}, VerificationRequest: &VerificationRequest{Candidate: &candidate}}
				projection := PureClaim{ParentState: state, Context: contextRef, Candidate: candidate}
				if err := checkClaimProjection(projection, parent, nil); err != nil {
					t.Fatal("invalid claim prerequisite: ", err)
				}
				if name == "m5-claim-projection" {
					projection.Candidate.Statement = "A different candidate"
				} else {
					projection.Context = projection.ParentState
				}
				if err := checkClaimProjection(projection, parent, nil); err == nil || err.Error() != "pure claim must equal its parent state's candidate projection" {
					t.Fatalf("wrong projection rejection: %v", err)
				}
				return
			}
			task := reportTask{State: state, Context: contextRef}
			v := InvestigationReport{State: state, Context: contextRef, ReportFile: ReportFileID, Completeness: "incomplete"}
			if stage == "binding" {
				if err := checkReportBinding(v, task); err != nil {
					t.Fatal("invalid binding prerequisite: ", err)
				}
				if name == "r5-state-binding" {
					v.State = task.Context
				} else {
					v.Context = task.State
				}
				if err := checkReportBinding(v, task); err == nil || err.Error() != "report state/context/file binding mismatch" {
					t.Fatalf("wrong binding rejection: %v", err)
				}
				return
			}
			claim := contract.Ref{AttemptID: "claim"}
			task.M6 = &reportContinuation{Failures: []ReportFailure{}, ReportFailures: []RecoveryFailure{}}
			v.M6 = &ReportMetadata{Dispositions: []ReportDisposition{}, ReportFailures: []RecoveryFailure{}}
			for i := 0; i < 2; i++ {
				item := ReportFailure{Owner: contract.Ref{AttemptID: fmt.Sprintf("owner-%d", i)}, Kind: "verification", DeliveryID: "delivery", Claim: &claim, Role: "con", Index: i, Failure: RecoveryFailure{Stage: "verify-con", AttemptID: fmt.Sprintf("failed-%d", i)}}
				task.M6.Failures = append(task.M6.Failures, item)
				v.M6.Dispositions = append(v.M6.Dispositions, ReportDisposition{Item: item, Action: "redirect", Reason: "Evidence supports redirect", Results: []contract.Ref{}, Basis: []Evidence{{Ref: &contextRef, FileID: "raw"}}})
			}
			if name == "m6-budget-echo" {
				task.M6.Budget = &ReportBudget{UsedAttempts: 4}
				budget := *task.M6.Budget
				v.M6.Budget = &budget
			}
			if err := checkReportMetadataHeader(v, task); err != nil {
				t.Fatal("invalid header prerequisite: ", err)
			}
			seen := make([]bool, len(task.M6.Failures))
			for i, disposition := range v.M6.Dispositions {
				index, err := matchReportDisposition(disposition, task.M6.Failures, seen)
				if err != nil || index != i || seen[index] {
					t.Fatalf("invalid tuple prerequisite or helper mutated seen: %d, %v", index, err)
				}
				seen[index] = true
				if err := checkInvestigationBasis(disposition.Basis, map[contract.Ref][]file{contextRef: {{ID: "raw", Kind: "evidence"}}}); err != nil {
					t.Fatal("invalid basis prerequisite: ", err)
				}
				if err := checkReportDispositionAction(disposition, v.Completeness); err != nil {
					t.Fatal("invalid action prerequisite: ", err)
				}
			}
			switch name {
			case "m6-metadata-missing":
				v.M6 = nil
			case "m6-history-missing-item":
				v.M6.Dispositions = v.M6.Dispositions[1:]
			case "m6-budget-echo":
				v.M6.Budget.UsedAttempts++
			case "m6-history-duplicate-item":
				v.M6.Dispositions[1] = v.M6.Dispositions[0]
			case "m6-history-wrong-version":
				v.M6.Dispositions[0].Item.Claim = &task.Context
			case "m6-history-no-basis":
				v.M6.Dispositions[0].Basis = []Evidence{}
			}
			if stage == "header" {
				if err := checkReportMetadataHeader(v, task); err == nil || err.Error() != "report requires complete M6 history and exact budget metadata" {
					t.Fatalf("wrong header rejection: %v", err)
				}
				return
			}
			if err := checkReportMetadataHeader(v, task); err != nil {
				t.Fatal("mutation rejected before intended tuple/action: ", err)
			}
			if stage == "tuple" {
				seen = make([]bool, len(task.M6.Failures))
				rejected := 0
				for i, disposition := range v.M6.Dispositions {
					index, err := matchReportDisposition(disposition, task.M6.Failures, seen)
					bad := i == 0
					if name == "m6-history-duplicate-item" {
						bad = i == 1
					}
					if !bad {
						if err != nil || index != i || seen[index] {
							t.Fatalf("unexpected earlier tuple rejection: %v", err)
						}
						seen[index] = true
						continue
					}
					if err == nil || err.Error() != "report disposition requires an exact distinct historical owner/delivery/role/item" {
						t.Fatalf("wrong tuple rejection: %v", err)
					}
					rejected++
					break
				}
				if rejected != 1 {
					t.Fatal("mutation never reached tuple rejection")
				}
				return
			}
			if stage != "action" {
				t.Fatal("unknown pure validation stage")
			}
			d := v.M6.Dispositions[0]
			if _, err := matchReportDisposition(d, task.M6.Failures, make([]bool, len(task.M6.Failures))); err != nil {
				t.Fatal("no-basis rejected before action: ", err)
			}
			if err := checkInvestigationBasis(d.Basis, map[contract.Ref][]file{contextRef: {{ID: "raw", Kind: "evidence"}}}); err != nil {
				t.Fatal("empty basis rejected before action: ", err)
			}
			if err := checkReportDispositionAction(d, v.Completeness); err == nil || err.Error() != "redirect requires evidence basis, not fabricated success" {
				t.Fatalf("wrong action rejection: %v", err)
			}
		})
	}
}

func TestTriageReportMetadataPredicates(t *testing.T) {
	for _, name := range []string{"valid", "no-m6", "unexpected-m6", "nil-empty", "retry-echo", "retry-order", "retry-multiplicity", "limited-complete", "header-before-completeness"} {
		t.Run("header/"+name, func(t *testing.T) {
			task := reportTask{M6: &reportContinuation{ReportFailures: []RecoveryFailure{{Stage: "report", AttemptID: "first"}, {Stage: "report", AttemptID: "second"}}}}
			v := InvestigationReport{Completeness: "incomplete", M6: &ReportMetadata{ReportFailures: slices.Clone(task.M6.ReportFailures)}}
			if err := checkReportMetadataHeader(v, task); err != nil {
				t.Fatal("invalid prerequisite: ", err)
			}
			want := ""
			switch name {
			case "no-m6":
				task.M6, v.M6 = nil, nil
			case "unexpected-m6":
				task.M6 = nil
				want = "report continuation metadata requires the M6 caller"
			case "nil-empty":
				task.M6.ReportFailures, v.M6.ReportFailures = nil, []RecoveryFailure{}
			case "retry-echo":
				v.M6.ReportFailures = []RecoveryFailure{}
			case "retry-order":
				slices.Reverse(v.M6.ReportFailures)
			case "retry-multiplicity":
				v.M6.ReportFailures[1] = v.M6.ReportFailures[0]
			case "limited-complete", "header-before-completeness":
				task.M6.Budget, v.M6.Budget = &ReportBudget{}, &ReportBudget{}
				v.Completeness = "complete"
				want = "resource-limited report must remain incomplete"
				if name == "header-before-completeness" {
					v.M6.ReportFailures = nil
				}
			}
			if strings.HasPrefix(name, "retry-") || name == "header-before-completeness" {
				want = "report requires complete M6 history and exact budget metadata"
			}
			err := checkReportMetadataHeader(v, task)
			if want == "" && err != nil || want != "" && (err == nil || err.Error() != want) {
				t.Fatalf("header: %v, want %q", err, want)
			}
		})
	}
	for _, name := range []string{"valid", "wrong-owner", "blank-reason", "seen", "reverse-order"} {
		t.Run("tuple/"+name, func(t *testing.T) {
			failures := []ReportFailure{{Owner: contract.Ref{AttemptID: "owner-1"}, Index: 0}, {Owner: contract.Ref{AttemptID: "owner-2"}, Index: 1}}
			d := ReportDisposition{Item: failures[0], Reason: "retained reason"}
			seen := []bool{false, false}
			if _, err := matchReportDisposition(d, failures, seen); err != nil {
				t.Fatal("invalid prerequisite: ", err)
			}
			wantIndex := 0
			switch name {
			case "wrong-owner":
				d.Item.Owner = contract.Ref{AttemptID: "context"}
			case "blank-reason":
				d.Reason = " "
			case "seen":
				seen[0] = true
			case "reverse-order":
				slices.Reverse(failures)
				wantIndex = 1
			}
			before := slices.Clone(seen)
			index, err := matchReportDisposition(d, failures, seen)
			if !slices.Equal(before, seen) {
				t.Fatal("tuple helper changed caller-owned seen state")
			}
			if name == "valid" || name == "reverse-order" {
				if err != nil || index != wantIndex {
					t.Fatalf("exact tuple failed: %d %v", index, err)
				}
			} else if err == nil || err.Error() != "report disposition requires an exact distinct historical owner/delivery/role/item" {
				t.Fatalf("wrong tuple rejection: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name, action, completeness, want string
		results                          []contract.Ref
		basis                            []Evidence
	}{
		{"unresolved", "unresolved", "incomplete", "", nil, nil},
		{"unresolved-complete", "unresolved", "complete", "unresolved execution requires incomplete report without success results", nil, nil},
		{"unresolved-result", "unresolved", "incomplete", "unresolved execution requires incomplete report without success results", []contract.Ref{{AttemptID: "result"}}, nil},
		{"redirect", "redirect", "incomplete", "", nil, []Evidence{{FileID: "raw"}}},
		{"redirect-result", "redirect", "incomplete", "redirect requires evidence basis, not fabricated success", []contract.Ref{{AttemptID: "result"}}, []Evidence{{FileID: "raw"}}},
		{"handled", "handled", "complete", "", []contract.Ref{{AttemptID: "result"}}, nil},
		{"handled-no-result", "handled", "incomplete", "handled failure requires subsequent committed results", nil, nil},
		{"unsupported", "other", "incomplete", "unsupported report disposition", nil, nil},
	} {
		t.Run("action/"+tc.name, func(t *testing.T) {
			err := checkReportDispositionAction(ReportDisposition{Action: tc.action, Results: tc.results, Basis: tc.basis}, tc.completeness)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || err.Error() != tc.want) {
				t.Fatalf("action shape: %v, want %q", err, tc.want)
			}
		})
	}
}

func TestTriageClaimProjectionPredicates(t *testing.T) {
	for _, name := range []string{"valid", "files", "ledger-nil", "action", "request-nil", "candidate-nil", "existing-claim", "premise-order", "evidence-multiplicity", "file-binding"} {
		t.Run(name, func(t *testing.T) {
			ctx := contract.Ref{AttemptID: "context"}
			candidate := ClaimCandidate{ID: "claim", Statement: "candidate", Premises: []string{"first", "second"}, AllowedEvidence: []Evidence{{Ref: &ctx, FileID: "one"}, {Ref: &ctx, FileID: "two"}}}
			parent := PlannerState{Context: ctx, Ledger: &InvestigationLedger{Action: "verify"}, VerificationRequest: &VerificationRequest{Candidate: &candidate}}
			claim := PureClaim{Context: ctx, Candidate: candidate}
			var files []file
			if err := checkClaimProjection(claim, parent, files); err != nil {
				t.Fatal("invalid prerequisite: ", err)
			}
			switch name {
			case "files":
				files = []file{{ID: "unexpected"}}
			case "ledger-nil":
				parent.Ledger = nil
			case "action":
				parent.Ledger.Action = "yield"
			case "request-nil":
				parent.VerificationRequest = nil
			case "candidate-nil":
				parent.VerificationRequest.Candidate = nil
			case "existing-claim":
				parent.VerificationRequest.Claim = &ctx
			case "premise-order":
				claim.Candidate.Premises = []string{"second", "first"}
			case "evidence-multiplicity":
				claim.Candidate.AllowedEvidence = []Evidence{candidate.AllowedEvidence[0], candidate.AllowedEvidence[0]}
			case "file-binding":
				v := InvestigationReport{State: ctx, Context: ctx, ReportFile: ReportFileID}
				task := reportTask{State: ctx, Context: ctx}
				if err := checkReportBinding(v, task); err != nil {
					t.Fatal(err)
				}
				v.ReportFile = "other"
				if err := checkReportBinding(v, task); err == nil || err.Error() != "report state/context/file binding mismatch" {
					t.Fatalf("wrong file binding rejection: %v", err)
				}
				return
			}
			err := checkClaimProjection(claim, parent, files)
			if name == "valid" && err != nil || name != "valid" && (err == nil || err.Error() != "pure claim must equal its parent state's candidate projection") {
				t.Fatalf("projection: %v", err)
			}
		})
	}
}

func TestTriageReportPolicyBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy ReportPolicy
		valid  bool
	}{
		{"zero-invalid", ReportPolicy{}, false},
		{"zero-retry-valid", ReportPolicy{ReserveAttempts: 1}, true},
		{"finite-retry-valid", ReportPolicy{ReportRetries: 1, ReserveSessions: 1, ReserveAttempts: 2}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.check()
			if tc.valid && err != nil || !tc.valid && (err == nil || err.Error() != "explicit report reserves must cover the report and all finite fresh retries") {
				t.Fatalf("policy boundary: %v", err)
			}
		})
	}
}

// These arithmetic checks do not claim engine admission or verification authority.
func TestM6SupplementRetryArithmetic(t *testing.T) {
	max := int(^uint(0) >> 1)
	for _, role := range []string{"pro", "con", "cross"} {
		for _, delta := range []int{0, 1, 5} {
			t.Run(fmt.Sprintf("%s-max-minus-%d", role, delta), func(t *testing.T) {
				p := &plannerCaller{recovery: &PlannerRecovery{}, verification: &PlannerVerification{}}
				roles := map[string]*VerifierPolicy{"pro": &p.verification.Policy.Pro, "con": &p.verification.Policy.Con, "cross": &p.verification.Policy.Cross}
				roles[role].Retries = max - delta
				state := PlannerState{Ledger: &InvestigationLedger{Action: "verify"}, VerificationRequest: &VerificationRequest{Candidate: &ClaimCandidate{}}}
				cost, err := p.investigationCost(context.Background(), state)
				if delta < 5 {
					if err == nil || !strings.Contains(err.Error(), "cost overflow") {
						t.Fatalf("wrapped retry arithmetic: %+v %v", cost, err)
					}
				} else if err != nil || cost != (investigationCost{Sessions: max - 2, Attempts: max, Live: 3}) {
					t.Fatalf("exact int boundary: %+v %v", cost, err)
				}
			})
		}
	}
	for mask := 0; mask < 8; mask++ {
		t.Run(fmt.Sprintf("retained-%03b", mask), func(t *testing.T) {
			claim := contract.Ref{AttemptID: "claim"}
			p := &plannerCaller{recovery: &PlannerRecovery{Policy: RecoveryPolicy{PlannerRetries: 2}}, verification: &PlannerVerification{}}
			d := VerificationDelivery{Claim: claim}
			want := investigationCost{Sessions: 2, Attempts: 3}
			for i, role := range []string{"pro", "con", "cross"} {
				r := VerificationRoleDelivery{Role: role}
				roles := []*VerifierPolicy{&p.verification.Policy.Pro, &p.verification.Policy.Con, &p.verification.Policy.Cross}
				if mask&(1<<i) != 0 {
					ref := contract.Ref{AttemptID: role}
					r.Result = &ref
					roles[i].Retries = max
				} else {
					roles[i].Retries = i + 1
					want.Sessions += i + 2
					want.Attempts += i + 2
					want.Live++
				}
				d.Roles = append(d.Roles, r)
			}
			p.verification.Deliveries = []VerificationDelivery{d}
			state := PlannerState{Ledger: &InvestigationLedger{Action: "verify"}, VerificationRequest: &VerificationRequest{Claim: &claim}}
			got, err := p.investigationCost(context.Background(), state)
			if err != nil || got != want {
				t.Fatalf("retained roles charged or pending role omitted: %+v want %+v: %v", got, want, err)
			}
		})
	}
}

func TestTriageReportResources(t *testing.T) {
	for _, name := range []string{"fresh", "file", "directory", "symlink", "partial"} {
		t.Run(name, func(t *testing.T) {
			run := t.TempDir()
			root := filepath.Join(run, "triage-report")
			switch name {
			case "file":
				if err := os.WriteFile(root, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(t.TempDir(), root); err != nil {
					t.Fatal(err)
				}
			case "partial":
				_, err := reportresource.ExtractFresh(run, "triage-report", reportresource.Common, reportresource.Common)
				if !errors.Is(err, os.ErrExist) {
					t.Fatalf("duplicate source did not preserve exclusive creation: %v", err)
				}
			}
			if name != "fresh" {
				before, err := os.Lstat(root)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := ExtractReport(run); !errors.Is(err, os.ErrExist) {
					t.Fatalf("existing directory was not reserved: %v", err)
				}
				after, err := os.Lstat(root)
				if err != nil || !os.SameFile(before, after) {
					t.Fatalf("reserved inode changed: %v", err)
				}
				if name == "partial" {
					got, err := os.ReadFile(filepath.Join(root, "pwc_report_io.py"))
					want, _ := reportresource.Common.ReadFile("pwc_report_io.py")
					if err != nil || !bytes.Equal(got, want) {
						t.Fatalf("partial extraction was removed: %v", err)
					}
				}
				return
			}
			script, err := ExtractReport(run)
			if err != nil || script != filepath.Join(root, "render_report.py") {
				t.Fatalf("report extraction: %v", err)
			}
			for _, path := range []string{"render_report.py", "pwc_report_io.py"} {
				got, err := os.ReadFile(filepath.Join(root, path))
				if err != nil || len(got) == 0 {
					t.Fatalf("missing extracted resource: %v", err)
				}
				info, err := os.Stat(filepath.Join(root, path))
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatalf("resource mode: %v", err)
				}
			}
		})
	}
}
