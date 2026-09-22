package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/runtime"
)

func engPersistEnvelope(id contract.Identity, value string, files []map[string]string) ([]byte, error) {
	if files == nil {
		files = []map[string]string{}
	}
	return json.Marshal(map[string]any{
		"meta": map[string]any{"version": 1, "run_id": id.RunID, "invocation_id": id.InvocationID,
			"attempt_id": id.AttemptID, "dispatch_token": id.DispatchToken, "schema_id": engTestSchema},
		"data": engTestData{Value: value}, "files": files,
	})
}

func engPersistClosed(t *testing.T, fake *engTestRuntime, report Report, want int) {
	t.Helper()
	sessions := fake.allSessions()
	if len(sessions) != want || len(report.Cleanup) != want || len(report.CleanupErrors) != 0 {
		t.Errorf("cleanup sessions/reports/errors = %d/%d/%v, want %d/%d/none", len(sessions), len(report.Cleanup), report.CleanupErrors, want, want)
	}
	for _, session := range sessions {
		_, closes, _ := session.history()
		if closes != 1 {
			t.Errorf("session %s Close calls = %d, want 1", session.id.HandleID, closes)
		}
		found := false
		for _, cleanup := range report.Cleanup {
			if cleanup.Identity == session.id {
				found = true
				if !cleanup.WaitCompleted || !cleanup.ProcessExited || !cleanup.AbortAcknowledged || !cleanup.AbortBashAcknowledged || len(cleanup.Unconfirmed) != 0 {
					t.Errorf("incomplete cleanup: %+v", cleanup)
				}
			}
		}
		if !found {
			t.Errorf("missing cleanup for %s", session.id.HandleID)
		}
	}
}

// Store publication alone is deliberately insufficient authority. This creates a
// fully valid same-run snapshot without calling any engine commit internals.
func engPersistUnregistered(ctx context.Context, run *Run) (contract.Ref, error) {
	id := contract.Identity{RunID: run.ID(), InvocationID: contract.NewID(), AttemptID: contract.NewID(), DispatchToken: contract.NewID()}
	a, err := run.store.BeginAttempt(id, contract.Request{Identity: id, Prompt: "unregistered", Output: contract.OutputSpec{SchemaID: engTestSchema}})
	if err != nil {
		return contract.Ref{}, err
	}
	raw, err := engPersistEnvelope(id, "unregistered", nil)
	if err != nil {
		return contract.Ref{}, err
	}
	if err := os.WriteFile(a.CandidatePath(), raw, 0600); err != nil {
		return contract.Ref{}, err
	}
	staged, err := a.Stage(ctx, contract.Spec{SchemaID: engTestSchema})
	if err != nil {
		return contract.Ref{}, err
	}
	defer func(staged *contract.Staged) { _ = staged.Discard() }(staged)
	ref, err := a.Publish(ctx, staged)
	if err != nil {
		return contract.Ref{}, err
	}
	if _, err := run.store.Read(ctx, ref); err != nil {
		return contract.Ref{}, fmt.Errorf("unregistered fixture is not otherwise valid: %w", err)
	}
	return ref, nil
}

func TestEnginePersistenceRefEntrypoints(t *testing.T) {
	foreignRun, _ := engTestNew(t, "", &engTestRuntime{}, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("foreign"))
		if err != nil {
			return Result{}, err
		}
		step, err := engTestStep(ctx, run.Root(), h, "foreign")
		return engTestResult(step), err
	})
	foreignReport := foreignRun.Execute()
	engTestReport(t, foreignReport, Succeeded, 0)
	foreign := foreignReport.Result.Outputs["output"]

	entries := []string{"Inputs", "Feedback", "RetryFeedback", "Decision", "Decode", "RetryResult", "ParallelResult", "FinalResult"}
	mutations := []string{"cross-run", "forged-run", "forged-attempt", "forged-path", "forged-schema", "forged-contract-hash", "forged-manifest-hash", "unregistered-correct-hashes", "tampered-contract", "tampered-manifest", "tampered-evidence"}
	for _, entry := range entries {
		for _, mutation := range mutations {
			t.Run(entry+"/"+mutation, func(t *testing.T) {
				fake := &engTestRuntime{execute: func(_ context.Context, call engTestCall) engTestReply {
					path := filepath.Join(filepath.Dir(call.CandidatePath), "evidence", "source.txt")
					if err := os.WriteFile(path, []byte("original evidence"), 0600); err != nil {
						return engTestReply{Err: err}
					}
					raw, err := engPersistEnvelope(call.Request.Identity, "original", []map[string]string{{"id": "source", "kind": "evidence", "path": "evidence/source.txt"}})
					return engTestReply{Raw: raw, Err: err}
				}}
				var observed error
				var bad contract.Ref
				var attempted StepResult
				callbacks, earlyCloses := 0, -1
				r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
					h, err := run.OpenSession(ctx, engTestRole("worker"))
					if err != nil {
						return Result{}, err
					}
					producer, err := engTestStep(ctx, run.Root(), h, "producer")
					if err != nil {
						return Result{}, err
					}
					if value, err := Decode[engTestData](ctx, run, producer.Output); err != nil || value.Value != "original" {
						return Result{}, fmt.Errorf("valid control Ref rejected: %+v, %v", value, err)
					}
					bad = producer.Output
					switch mutation {
					case "cross-run":
						bad = foreign
					case "forged-run":
						bad.RunID = contract.NewID()
					case "forged-attempt":
						bad.AttemptID = contract.NewID()
					case "forged-path":
						bad.Path = filepath.Join(filepath.Dir(filepath.Dir(bad.Path)), "candidate.json")
					case "forged-schema":
						bad.SchemaID = "forged.schema"
					case "forged-contract-hash":
						bad.SHA256 = strings.Repeat("0", 64)
					case "forged-manifest-hash":
						bad.ManifestSHA256 = strings.Repeat("0", 64)
					case "unregistered-correct-hashes":
						bad, err = engPersistUnregistered(ctx, run)
					case "tampered-contract", "tampered-manifest", "tampered-evidence":
						path := bad.Path
						switch mutation {
						case "tampered-manifest":
							path = filepath.Join(filepath.Dir(bad.Path), "manifest.json")
						case "tampered-evidence":
							path = filepath.Join(filepath.Dir(bad.Path), "evidence", "source.txt")
						}
						var raw []byte
						raw, err = os.ReadFile(path)
						if err == nil {
							// Whitespace leaves JSON valid while invalidating its digest.
							err = os.WriteFile(path, append(raw, ' '), 0600)
						}
					}
					if err != nil {
						return Result{}, err
					}
					result := Result{Outputs: map[string]contract.Ref{"bad": bad}}
					switch entry {
					case "Inputs":
						attempted, observed = engTestStep(ctx, run.Root(), h, "consumer", bad)
					case "Feedback":
						attempted, observed = run.Root().Step(ctx, StepSpec{Key: "consumer", Session: h, Prompt: "consumer", Output: contract.Spec{SchemaID: engTestSchema}, Feedback: &Feedback{Message: "review", SourceAttemptID: producer.AttemptID, Refs: []contract.Ref{bad}}})
					case "Decision":
						observed = run.Root().Decision(ctx, "decision", "reject invalid Ref", []contract.Ref{bad})
					case "Decode":
						var decoded engTestData
						decoded, observed = Decode[engTestData](ctx, run, bad)
						if decoded != (engTestData{}) {
							return Result{}, fmt.Errorf("invalid Decode returned data: %+v", decoded)
						}
					case "RetryFeedback", "RetryResult":
						var accepted Result
						accepted, observed = run.Root().Retry(ctx, "retry", 1, func(context.Context, *Scope, RetryState) (RetryAction, error) {
							callbacks++
							if entry == "RetryFeedback" {
								return RetryAction{Again: true, Feedback: &Feedback{Message: "review", SourceAttemptID: producer.AttemptID, Refs: []contract.Ref{bad}}}, nil
							}
							return RetryAction{Result: result}, nil
						})
						if len(accepted.Outputs) != 0 {
							return Result{}, fmt.Errorf("Retry returned invalid outputs: %+v", accepted)
						}
					case "ParallelResult":
						var joined []BranchResult
						joined, err = run.Root().Parallel(ctx, "parallel", CollectAll, []Branch{{Name: "invalid", Do: func(context.Context, *Scope) (Result, error) {
							callbacks++
							return result, nil
						}}})
						if err != nil || len(joined) != 1 {
							return Result{}, fmt.Errorf("parallel failed to collect: %+v, %v", joined, err)
						}
						observed = joined[0].Err
					case "FinalResult":
						return result, nil
					}
					_, earlyCloses, _ = fake.allSessions()[0].history()
					if !engTestCode(observed, ReferenceInvalid) {
						return Result{}, fmt.Errorf("%s accepted %s: %v", entry, mutation, observed)
					}
					// ReferenceInvalid is recoverable; swallowing it must not cancel the run.
					return Result{}, nil
				})
				report := r.Execute()
				if entry == "FinalResult" {
					engTestReport(t, report, Failed, 1)
					if !engTestCode(report.Failure, ReferenceInvalid) {
						t.Errorf("final Ref failure = %v", report.Failure)
					}
				} else {
					engTestReport(t, report, Succeeded, 0)
					if !engTestCode(observed, ReferenceInvalid) || earlyCloses != 0 {
						t.Errorf("reference rejection/handle disposition = %v/%d", observed, earlyCloses)
					}
				}
				engTestPersisted(t, r, report)
				engPersistClosed(t, fake, report, 1)
				calls, _, _ := fake.allSessions()[0].history()
				if len(calls) != 1 {
					t.Errorf("invalid Ref reached downstream: calls=%d", len(calls))
				}
				wantAttempts := 1
				if entry == "Inputs" || entry == "Feedback" {
					wantAttempts++
					state, ok := report.Snapshot.Attempts[attempted.AttemptID]
					if !ok || state.State != Failed || state.DispatchAccepted != AcceptedNo || state.Output != nil || state.Failure == nil || state.Failure.Code != ReferenceInvalid || attempted.Output != (contract.Ref{}) {
						t.Errorf("Preparing reference refusal lost attempt: result=%+v state=%+v", attempted, state)
					}
					if entry == "Feedback" && (state.Feedback == nil || state.Feedback.Message != "review" || !reflect.DeepEqual(state.Feedback.Refs, []contract.Ref{bad})) {
						t.Errorf("rejected feedback missing from display metadata: %+v", state.Feedback)
					}
				}
				if len(report.Snapshot.Attempts) != wantAttempts {
					t.Errorf("attempts=%d, want %d", len(report.Snapshot.Attempts), wantAttempts)
				}
				if (entry == "RetryFeedback" || entry == "RetryResult" || entry == "ParallelResult") && callbacks != 1 {
					t.Errorf("callback count=%d, want 1", callbacks)
				}
				for _, event := range engTestEvents(t, r) {
					if event.Kind == "Decision" || event.Kind == "RetryScheduled" {
						t.Errorf("invalid Ref committed %s", event.Kind)
					}
				}
			})
		}
	}
}

// Preserve the original journal so assertions can distinguish durable events
// from emergency in-memory state after a real append failure.
func engPersistDirectory(path string) error {
	if _, err := os.Lstat(path); err == nil {
		if err := os.Rename(path, path+".before-fault"); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Mkdir(path, 0700)
}

func engPersistSavedEvents(t *testing.T, r *Run) []Event {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(r.Dir(), "events.jsonl.before-fault"))
	if err != nil {
		t.Fatal(err)
	}
	var events []Event
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if event.Seq != uint64(len(events)+1) || event.RunID != r.ID() {
			t.Errorf("invalid saved journal sequence/identity: %+v", event)
		}
		events = append(events, event)
	}
	return events
}

func TestEnginePersistenceRenameWithoutJournalCommit(t *testing.T) {
	fake := &engTestRuntime{}
	var published contract.Ref
	var producer, downstream StepResult
	var stepErr, downstreamErr, decodeErr, injectionErr error
	var swallowed bool
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		producer, stepErr = engTestStep(ctx, run.Root(), h, "producer")
		_, decodeErr = Decode[engTestData](ctx, run, published)
		downstream, downstreamErr = engTestStep(ctx, run.Root(), h, "downstream", published)
		swallowed = true
		return Result{}, nil
	})
	r.afterPublish = func(ref contract.Ref) {
		published = ref
		injectionErr = engPersistDirectory(filepath.Join(r.Dir(), "events.jsonl"))
	}
	report := r.Execute()
	if injectionErr != nil {
		t.Fatal(injectionErr)
	}
	if !swallowed || !engTestCode(stepErr, JournalFailed) || !engTestCode(report.Failure, JournalFailed) || report.Outcome != Failed || report.ExitCode != 1 || report.Snapshot.StatePersisted {
		t.Errorf("journal failure was swallowed: step=%v report=%+v", stepErr, report)
	}
	if len(report.FinalizationErrors) == 0 || len(report.Snapshot.FinalizationErrors) != len(report.FinalizationErrors) {
		t.Errorf("unavailable RunFinalizing/RunFinished journal lacked FinalizationFailed diagnostics: report=%v snapshot=%+v", report.FinalizationErrors, report.Snapshot.FinalizationErrors)
	}
	if producer.Output != (contract.Ref{}) || downstream != (StepResult{}) || !engTestCode(downstreamErr, JournalFailed) || !engTestCode(decodeErr, JournalFailed) {
		t.Errorf("uncommitted publication escaped: producer=%+v downstream=%+v errors=%v/%v", producer, downstream, downstreamErr, decodeErr)
	}
	r.mu.Lock()
	_, registered := r.publications[published.AttemptID]
	r.mu.Unlock()
	if registered {
		t.Error("rename-only Ref entered committed registry")
	}
	raw, err := os.ReadFile(published.Path)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != published.SHA256 {
		t.Error("test did not reach a real, valid Store.Publish")
	}
	if len(report.Snapshot.Attempts) != 1 {
		t.Errorf("downstream created an attempt: %+v", report.Snapshot.Attempts)
	}
	for _, attempt := range report.Snapshot.Attempts {
		if attempt.State != Failed || attempt.Failure == nil || attempt.Failure.Code != JournalFailed {
			t.Errorf("failed publication retained successful attempt state: %+v", attempt)
		}
	}
	for _, event := range engPersistSavedEvents(t, r) {
		if event.Kind == "AttemptSucceeded" || event.Kind == "RunFinished" {
			t.Errorf("failed journal fabricated durable %s", event.Kind)
		}
	}
	calls, closes, confirms := fake.allSessions()[0].history()
	if len(calls) != 1 || confirms != 1 || closes != 1 {
		t.Errorf("publication lifecycle calls/confirms/closes=%d/%d/%d", len(calls), confirms, closes)
	}
	engPersistClosed(t, fake, report, 1)
}

func TestEnginePersistenceFinalizationIOFailures(t *testing.T) {
	cases := []struct {
		name, path, phase string
		outcome           State
		finishedEvent     bool
	}{
		{"result", "result.json", "result", Failed, true},
		{"RunFinalizing", "events.jsonl", "RunFinalizing", Succeeded, false},
		{"RunFinalizing-snapshot", "run.json", "RunFinalizing", Succeeded, true},
		{"cleanup", "cleanup.json", "cleanup", Succeeded, true},
		{"RunFinished", "events.jsonl", "RunFinished", Succeeded, false},
		{"final-run-snapshot", "run.json", "RunFinished", Succeeded, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &engTestRuntime{}
			workflowCalls, hits := 0, 0
			var injectionErr error
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				workflowCalls++
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				step, err := engTestStep(ctx, run.Root(), h, "producer")
				return engTestResult(step), err
			})
			r.beforeIO = func(path, phase string) {
				if path == tc.path && phase == tc.phase {
					hits++
					injectionErr = engPersistDirectory(filepath.Join(r.Dir(), path))
				}
			}
			report := r.Execute()
			if hits != 1 || injectionErr != nil || workflowCalls != 1 {
				t.Fatalf("barrier/workflow calls = %d/%d; injection=%v", hits, workflowCalls, injectionErr)
			}
			if report.Outcome != tc.outcome || report.Snapshot.WorkflowOutcome != tc.outcome || report.Snapshot.State != tc.outcome || report.ExitCode != 1 || report.Snapshot.StatePersisted {
				t.Errorf("wrong locked outcome/emergency status: %+v", report)
			}
			if tc.outcome == Failed {
				if !engTestCode(report.Failure, StorageFailed) || report.Snapshot.Failure == nil || report.Snapshot.Failure.Phase != "result" {
					t.Errorf("pre-lock failure did not prevent success: %+v", report)
				}
			} else if report.Failure != nil || report.Snapshot.Failure != nil {
				t.Errorf("post-lock I/O rewrote workflow success: %v", report.Failure)
			}
			if len(report.FinalizationErrors) == 0 || len(report.Snapshot.FinalizationErrors) != len(report.FinalizationErrors) {
				t.Errorf("missing independent finalization channel: %+v", report)
			}
			wantPhase := tc.phase
			if tc.name == "result" {
				wantPhase = "failed_result"
			}
			found := false
			for _, err := range report.FinalizationErrors {
				var f *Failure
				if !errors.As(err, &f) || f.Code != FinalizationFailed || f.Origin != OriginStorage || f.Cause == nil {
					t.Errorf("lost typed finalization cause: %v", err)
					continue
				}
				found = found || f.Phase == wantPhase
			}
			if !found {
				t.Errorf("missing finalization phase %s: %v", wantPhase, report.FinalizationErrors)
			}
			engPersistClosed(t, fake, report, 1)
			for _, attempt := range report.Snapshot.Attempts {
				if attempt.State != Succeeded || attempt.Output == nil || attempt.Failure != nil {
					t.Errorf("finalization rewrote committed attempt: %+v", attempt)
				}
			}
			var events []Event
			if tc.path == "events.jsonl" {
				events = engPersistSavedEvents(t, r)
			} else {
				events = engTestEvents(t, r)
			}
			finished := 0
			for _, event := range events {
				if event.Kind == "RunFinished" {
					finished++
				}
				if event.Kind == "RunFinalizing" || event.Kind == "RunFinished" {
					raw, err := json.Marshal(event.Details)
					if err != nil {
						t.Fatal(err)
					}
					var details struct {
						Outcome State `json:"outcome"`
					}
					if err := json.Unmarshal(raw, &details); err != nil {
						t.Fatal(err)
					}
					if details.Outcome != tc.outcome {
						t.Errorf("journal outcome changed: %+v", event)
					}
				}
			}
			wantFinished := 0
			if tc.finishedEvent {
				wantFinished = 1
			}
			if finished != wantFinished || report.Snapshot.LastSeq != events[len(events)-1].Seq {
				t.Errorf("fabricated/lost commit: finished=%d want=%d, last seq=%d journal=%d", finished, wantFinished, report.Snapshot.LastSeq, events[len(events)-1].Seq)
			}
			if tc.path == "run.json" {
				var previous Snapshot
				if err := engTestReadJSON(filepath.Join(r.Dir(), "run.json.before-fault"), &previous); err != nil {
					t.Fatal(err)
				}
				if previous.LastSeq >= report.Snapshot.LastSeq || previous.State == Succeeded || !previous.FinishedAt.IsZero() {
					t.Errorf("failed snapshot appeared durable: %+v", previous)
				}
			} else {
				var disk Snapshot
				if err := engTestReadJSON(filepath.Join(r.Dir(), "run.json"), &disk); err != nil {
					t.Fatal(err)
				}
				if tc.finishedEvent && !reflect.DeepEqual(disk, report.Snapshot) {
					t.Error("persistable emergency snapshot differs from report")
				}
				if !tc.finishedEvent && (disk.State == Succeeded || !disk.FinishedAt.IsZero()) {
					t.Errorf("journal failure fabricated final disk snapshot: %+v", disk)
				}
			}
			if tc.path != "result.json" {
				var result struct {
					RunID   string                  `json:"run_id"`
					Outputs map[string]contract.Ref `json:"outputs"`
				}
				if err := engTestReadJSON(filepath.Join(r.Dir(), "result.json"), &result); err != nil {
					t.Fatal(err)
				}
				if result.RunID != r.ID() || !reflect.DeepEqual(result.Outputs, report.Result.Outputs) {
					t.Errorf("result inventory lost: %+v", result)
				}
			}
			if tc.path != "cleanup.json" {
				var cleanup struct {
					Reports []runtime.CleanupReport `json:"reports"`
				}
				if err := engTestReadJSON(filepath.Join(r.Dir(), "cleanup.json"), &cleanup); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(cleanup.Reports, report.Cleanup) {
					t.Error("cleanup report did not persist actual Close results")
				}
			}
		})
	}
}

// Preserve the shared runtime's request/candidate I/O, changing only its receipt.
type engPersistReceiptSession struct{ runtime.Session }

func (s engPersistReceiptSession) Execute(ctx context.Context, dispatch runtime.Dispatch) (runtime.Execution, error) {
	receipt, err := s.Session.Execute(ctx, dispatch)
	receipt.Token = "wrong-token"
	return receipt, err
}

func TestEnginePersistenceStageConfirmBoundary(t *testing.T) {
	cases := []struct {
		name     string
		code     Code
		confirms int
		origin   Origin
		accepted DispatchAccepted
	}{
		{"stage-invalid-json", ContractInvalid, 0, OriginContract, AcceptedYes},
		{"stage-invalid-schema", ContractInvalid, 0, OriginContract, AcceptedYes},
		{"execute-invalid-receipt", AmbiguousExecution, 0, OriginProtocol, AcceptedUnknown},
		// Settled execution evidence takes precedence over the fixture's empty acceptance.
		{"confirm-rejected", AmbiguousExecution, 1, OriginProtocol, AcceptedYes},
		{"confirm-epoch-mismatch", AmbiguousExecution, 1, OriginProtocol, AcceptedYes},
		{"confirm-stale-seq", AmbiguousExecution, 1, OriginProtocol, AcceptedYes},
		{"candidate-changed-during-confirm", "", 1, "", AcceptedYes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &engTestRuntime{}
			var original, changed []byte
			var candidatePath string
			fake.execute = func(_ context.Context, call engTestCall) engTestReply {
				candidatePath = call.CandidatePath
				switch tc.name {
				case "stage-invalid-json":
					return engTestReply{Raw: []byte(`{"broken":`)}
				case "stage-invalid-schema":
					return engTestReply{Data: engTestData{Value: ""}}
				}
				return engTestReply{Data: engTestData{Value: "staged bytes"}}
			}
			rejected := &Failure{Code: AmbiguousExecution, Origin: OriginProtocol, Phase: "Confirm", Message: "runtime rejected receipt"}
			fake.confirm = func(_ context.Context, call engTestCall, receipt runtime.Execution) (runtime.Confirmation, error) {
				confirmation := runtime.Confirmation{Seq: receipt.SettledSeq + 1, ActivityEpoch: receipt.ActivityEpoch}
				switch tc.name {
				case "confirm-rejected":
					return runtime.Confirmation{}, rejected
				case "confirm-epoch-mismatch":
					confirmation.ActivityEpoch++
				case "confirm-stale-seq":
					confirmation.Seq = receipt.SettledSeq - 1
				case "candidate-changed-during-confirm":
					var err error
					original, err = os.ReadFile(call.CandidatePath)
					if err != nil {
						return runtime.Confirmation{}, err
					}
					changed, err = engPersistEnvelope(call.Request.Identity, "changed after Stage", nil)
					if err != nil {
						return runtime.Confirmation{}, err
					}
					if err := os.WriteFile(call.CandidatePath, changed, 0600); err != nil {
						return runtime.Confirmation{}, err
					}
				}
				return confirmation, nil
			}
			var step StepResult
			var stepErr error
			published, earlyCloses := 0, -1
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				if tc.name == "execute-invalid-receipt" {
					h.session = engPersistReceiptSession{Session: h.session}
				}
				step, stepErr = engTestStep(ctx, run.Root(), h, "producer")
				_, earlyCloses, _ = fake.allSessions()[0].history()
				if stepErr != nil {
					return Result{}, stepErr
				}
				value, err := Decode[engTestData](ctx, run, step.Output)
				if err != nil || value.Value != "staged bytes" {
					return Result{}, fmt.Errorf("published mutable candidate rather than staged bytes: %+v, %v", value, err)
				}
				return engTestResult(step), nil
			})
			r.afterPublish = func(contract.Ref) { published++ }
			report := r.Execute()
			wantState, wantExit, wantPublished, wantEarlyCloses := Failed, 1, 0, 0
			if tc.code == "" {
				wantState, wantExit, wantPublished = Succeeded, 0, 1
			} else if strings.HasPrefix(tc.name, "confirm-") || tc.name == "execute-invalid-receipt" {
				wantEarlyCloses = 1
			}
			engTestReport(t, report, wantState, wantExit)
			engTestPersisted(t, r, report)
			engPersistClosed(t, fake, report, 1)
			if published != wantPublished || earlyCloses != wantEarlyCloses {
				t.Errorf("publication/early close=%d/%d, want %d/%d", published, earlyCloses, wantPublished, wantEarlyCloses)
			}
			calls, _, confirms := fake.allSessions()[0].history()
			if len(calls) != 1 || confirms != tc.confirms || len(report.Snapshot.Attempts) != 1 {
				t.Errorf("unexpected retry/Confirm: calls=%d confirms=%d attempts=%d", len(calls), confirms, len(report.Snapshot.Attempts))
			}
			staging, err := os.ReadDir(filepath.Join(r.Dir(), ".staging"))
			if err != nil || len(staging) != 0 {
				t.Errorf("staged snapshot leaked after refusal/publication: %v, %v", staging, err)
			}
			if tc.code != "" {
				if !engTestCode(stepErr, tc.code) || !engTestCode(report.Failure, tc.code) || step.Output != (contract.Ref{}) || len(r.publications) != 0 {
					t.Errorf("rejected output became consumable: step=%+v failure=%v publications=%+v", step, report.Failure, r.publications)
				}
				for _, err := range []error{stepErr, report.Failure} {
					var failure *Failure
					if !errors.As(err, &failure) || failure.Origin != tc.origin || failure.DispatchAccepted != tc.accepted {
						t.Errorf("boundary failure classification = %+v, want origin=%s accepted=%q", failure, tc.origin, tc.accepted)
					}
				}
				if tc.name == "confirm-rejected" && !errors.Is(report.Failure, rejected) {
					t.Error("Confirm error cause lost")
				}
				if _, err := os.Lstat(filepath.Join(filepath.Dir(candidatePath), "published")); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("Stage/Confirm refusal published on disk: %v", err)
				}
				for _, attempt := range report.Snapshot.Attempts {
					if attempt.State != Failed || attempt.Output != nil || attempt.Failure == nil || attempt.Failure.Code != tc.code || attempt.Failure.Origin != tc.origin || attempt.DispatchAccepted != tc.accepted || attempt.Failure.DispatchAccepted != tc.accepted {
						t.Errorf("wrong rejection state: %+v", attempt)
					}
				}
			} else {
				raw, err := os.ReadFile(step.Output.Path)
				if err != nil {
					t.Fatal(err)
				}
				candidate, err := os.ReadFile(candidatePath)
				if err != nil {
					t.Fatal(err)
				}
				if len(original) == 0 || bytes.Equal(original, changed) || !bytes.Equal(raw, original) || !bytes.Equal(candidate, changed) {
					t.Errorf("Stage byte isolation failed: published=%s original=%s candidate=%s", raw, original, candidate)
				}
				if publication, ok := r.publications[step.AttemptID]; !ok || publication.Ref != step.Output || publication.Seq == 0 {
					t.Errorf("valid staged bytes lack committed membership: %+v", publication)
				}
			}
		})
	}
}

func TestEnginePersistenceRefReadFailureCancelsStagedSibling(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("read permission fault requires an unprivileged user")
	}
	for _, entry := range []string{"Decode", "Inputs"} {
		t.Run(entry, func(t *testing.T) {
			staged := make(chan struct{})
			var run *Run
			var producer StepResult
			var joined []BranchResult
			var injectionErr, confirmCause error
			faultHits, confirms := 0, 0
			fake := &engTestRuntime{}
			fake.execute = func(ctx context.Context, call engTestCall) engTestReply {
				if call.Spec.Name == "source" {
					_, err := Decode[engTestData](ctx, run, producer.Output)
					return engTestReply{Err: err}
				}
				return engTestReply{Data: engTestData{Value: call.Spec.Name}}
			}
			fake.confirm = func(ctx context.Context, call engTestCall, receipt runtime.Execution) (runtime.Confirmation, error) {
				if call.Spec.Name == "sibling" {
					confirms++
					close(staged)
					<-ctx.Done()
					confirmCause = context.Cause(ctx)
				}
				// Return valid evidence so Step's plain context check, not a new
				// runtime or Store error, determines the sibling disposition.
				return runtime.Confirmation{Seq: receipt.SettledSeq + 1, ActivityEpoch: receipt.ActivityEpoch}, nil
			}
			run, _ = engTestNew(t, "", fake, func(ctx context.Context, r *Run, _ Input) (Result, error) {
				p, err := r.OpenSession(ctx, engTestRole("producer"))
				if err != nil {
					return Result{}, err
				}
				producer, err = engTestStep(ctx, r.Root(), p, "publish")
				if err != nil {
					return Result{}, err
				}
				var branches []Branch
				for _, name := range []string{"source", "sibling"} {
					h, err := r.OpenSession(ctx, engTestRole(name))
					if err != nil {
						return Result{}, err
					}
					branches = append(branches, Branch{Name: name, Do: func(ctx context.Context, scope *Scope) (Result, error) {
						var inputs []contract.Ref
						if name == "source" {
							select {
							case <-staged:
							case <-ctx.Done():
								return Result{}, context.Cause(ctx)
							}
							if entry == "Inputs" {
								inputs = []contract.Ref{producer.Output}
							}
						}
						_, err := engTestStep(ctx, scope, h, "work", inputs...)
						return Result{}, err
					}})
				}
				joined, err = r.Root().Parallel(ctx, "read-failure", CollectAll, branches)
				return Result{}, err
			})
			run.beforeIO = func(path, phase string) {
				if phase != "step" || faultHits != 0 {
					return
				}
				select {
				case <-staged:
				default:
					return
				}
				faultHits++
				// Keep the canonical path and bytes intact so only the real
				// read I/O fails, rather than reference integrity validation.
				injectionErr = os.Chmod(producer.Output.Path, 0000)
			}
			report := run.Execute()
			if faultHits != 1 || injectionErr != nil || confirms != 1 {
				t.Fatalf("read fault/staged barrier not exercised: hits=%d injection=%v confirms=%d", faultHits, injectionErr, confirms)
			}
			engTestReport(t, report, Failed, 1)
			engTestPersisted(t, run, report)
			engPersistClosed(t, fake, report, 3)
			if len(joined) != 2 || len(report.Snapshot.Attempts) != 3 {
				t.Fatalf("missing read source/staged sibling: branches=%+v attempts=%+v", joined, report.Snapshot.Attempts)
			}
			var storeErr *contract.Error
			var pathErr *os.PathError
			if !errors.As(joined[0].Err, &storeErr) || storeErr.Code != contract.StorageFailed || storeErr.Phase != "read" || storeErr.Identity.AttemptID != producer.AttemptID || !errors.As(storeErr, &pathErr) || !errors.Is(storeErr, os.ErrPermission) {
				t.Fatalf("source did not exercise real Store read failure: %v", joined[0].Err)
			}
			var source *Failure
			if !errors.As(joined[0].Err, &source) {
				t.Fatal("source missing classified failure")
			}
			for name, err := range map[string]error{"source": joined[0].Err, "root": report.Failure, "Confirm context": confirmCause} {
				var f *Failure
				if !errors.As(err, &f) || f.Code != StorageFailed || f.Origin != OriginStorage || f.Phase != "read" || f.AttemptID != source.AttemptID || f.StepID != source.StepID || f.HandleID != source.HandleID || !errors.Is(err, storeErr) {
					t.Errorf("%s lost source identity/root chain: %+v", name, f)
				}
			}
			for i, branch := range joined {
				wantCode, wantState := StorageFailed, Failed
				if i == 1 {
					wantCode, wantState = Cancelled, CancelledState
				}
				var f *Failure
				if !errors.As(branch.Err, &f) || f.Code != wantCode || f.Origin != OriginStorage || !errors.Is(branch.Err, storeErr) {
					t.Errorf("%s disposition/root chain: %+v", branch.Name, f)
					continue
				}
				attempt, ok := report.Snapshot.Attempts[f.AttemptID]
				if !ok || attempt.Identity.AttemptID == producer.AttemptID || attempt.State != wantState || attempt.Failure == nil || attempt.Failure.Code != wantCode || attempt.Output != nil || f.HandleID != attempt.HandleID || f.StepID != attempt.Identity.InvocationID {
					t.Errorf("%s terminal attributed to wrong operation: %+v", branch.Name, attempt)
				}
				if i == 1 && (attempt.Execution == nil || attempt.DispatchAccepted != AcceptedYes || attempt.Failure == nil || attempt.Failure.DispatchAccepted != AcceptedYes || f.DispatchAccepted != AcceptedYes) {
					t.Errorf("staged sibling lost settled acceptance: %+v", attempt)
				}
			}
			if attempt := report.Snapshot.Attempts[producer.AttemptID]; attempt.State != Succeeded || attempt.Failure != nil {
				t.Errorf("read failure rewrote producer: %+v", attempt)
			}
			staging, err := os.ReadDir(filepath.Join(run.Dir(), ".staging"))
			if err != nil || len(staging) != 0 {
				t.Errorf("cancelled sibling leaked Stage: %v, %v", staging, err)
			}
			for _, session := range fake.allSessions() {
				calls, _, confirmCalls := session.history()
				wantCalls, wantConfirms := 1, 1
				if session.spec.Name == "source" {
					wantConfirms = 0
					if entry == "Inputs" {
						wantCalls = 0
					}
				}
				if len(calls) != wantCalls || confirmCalls != wantConfirms {
					t.Errorf("%s lifecycle calls/confirms=%d/%d, want %d/%d", session.spec.Name, len(calls), confirmCalls, wantCalls, wantConfirms)
				}
			}
		})
	}
}

func TestEnginePersistenceStageDeadlineAndReportWriteFailure(t *testing.T) {
	fake := &engTestRuntime{}
	var attemptCtx context.Context
	var validationPath string
	fake.execute = func(ctx context.Context, call engTestCall) engTestReply {
		attemptCtx = ctx
		validationPath = filepath.Join(filepath.Dir(call.CandidatePath), "validation.json")
		return engTestReply{Data: engTestData{Value: "valid candidate"}}
	}
	var first, downstream StepResult
	var firstErr, downstreamErr, timeoutCause, injectionErr error
	hits := 0
	swallowed := false
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		first, firstErr = run.Root().Step(ctx, StepSpec{Key: "deadline", Session: h, Prompt: "deadline", Output: contract.Spec{SchemaID: engTestSchema}, Timeout: time.Second})
		downstream, downstreamErr = engTestStep(ctx, run.Root(), h, "must-not-dispatch")
		swallowed = true
		return Result{}, nil
	})
	r.beforeIO = func(path, phase string) {
		if path != "run.json" || phase != "AttemptSettled" {
			return
		}
		hits++
		if attemptCtx == nil {
			injectionErr = errors.New("AttemptSettled preceded runtime Execute context capture")
			return
		}
		<-attemptCtx.Done()
		timeoutCause = context.Cause(attemptCtx)
		injectionErr = engPersistDirectory(validationPath)
	}
	report := r.Execute()
	if hits != 1 || injectionErr != nil {
		t.Fatalf("settled barrier hits/error=%d/%v", hits, injectionErr)
	}
	var deadline *Failure
	if !errors.As(timeoutCause, &deadline) || deadline.Code != TimedOut || deadline.Origin != OriginAttemptDeadline {
		t.Fatalf("Stage did not receive typed attempt deadline: %v", timeoutCause)
	}
	info, err := os.Stat(validationPath)
	if err != nil || !info.IsDir() {
		t.Fatalf("validation report fault is not a real directory: %v, %v", info, err)
	}
	var storeErr *contract.Error
	if !errors.As(firstErr, &storeErr) || storeErr.Code != contract.StorageFailed || storeErr.Phase != "validation-report" || !errors.Is(storeErr, timeoutCause) {
		t.Errorf("real Stage did not retain report failure and deadline: %+v", storeErr)
	}
	for _, err := range []error{firstErr, downstreamErr, report.Failure} {
		var failure *Failure
		if !errors.As(err, &failure) || failure.Code != StorageFailed || failure.Origin != OriginStorage || !errors.Is(err, timeoutCause) {
			t.Errorf("outer StorageFailed downgraded or deadline cause lost: %+v", failure)
		}
	}
	engTestReport(t, report, Failed, 1)
	engTestPersisted(t, r, report)
	engPersistClosed(t, fake, report, 1)
	if !swallowed || first.AttemptID == "" || first.Output != (contract.Ref{}) || downstream != (StepResult{}) || len(report.Snapshot.Attempts) != 1 || len(r.publications) != 0 {
		t.Errorf("swallowed storage failure allowed publication/dispatch: first=%+v downstream=%+v report=%+v", first, downstream, report)
	}
	attempt := report.Snapshot.Attempts[first.AttemptID]
	if attempt.State != Failed || attempt.DispatchAccepted != AcceptedYes || attempt.Failure == nil || attempt.Failure.Code != StorageFailed || attempt.Failure.Origin != OriginStorage || attempt.Failure.Phase != "validation-report" || attempt.Output != nil {
		t.Errorf("settled storage failure lost attempt disposition: %+v", attempt)
	}
	calls, _, confirms := fake.allSessions()[0].history()
	if len(calls) != 1 || confirms != 0 {
		t.Errorf("failed Stage reached Confirm or dispatched again: calls/confirms=%d/%d", len(calls), confirms)
	}
}

func TestEnginePersistenceObservationAtOutcomeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, path, phase string
		overflow          bool
		outcome           State
	}{
		{"result-fatal", "result.json", "result", false, Failed},
		{"finalizing-fatal", "events.jsonl", "RunFinalizing", false, Succeeded},
		{"finalizing-overflow", "events.jsonl", "RunFinalizing", true, Succeeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			guard, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			fake := &engTestRuntime{}
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var handleID string
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				handleID = h.id
				step, err := engTestStep(ctx, run.Root(), h, "producer")
				return engTestResult(step), err
			})
			r.beforeIO = func(path, phase string) {
				if path == tc.path && phase == tc.phase {
					close(entered)
					select {
					case <-release:
					case <-guard.Done():
					}
				}
			}
			done := engTestExecuteAsync(t, r)
			select {
			case <-entered:
			case <-guard.Done():
				t.Fatal("engine did not reach outcome I/O barrier")
			}
			payload := &Failure{Code: StorageFailed, Origin: OriginStorage, Phase: "runtime-observation", RunID: r.ID(), HandleID: handleID, Message: "runtime storage failure"}
			ingress := make(chan error, 1)
			go func() {
				if !tc.overflow {
					if len(r.observations) != 0 {
						ingress <- errors.New("fatal observation queue was not empty")
						return
					}
					ingress <- r.Observe(guard, runtime.Observation{HandleID: handleID, Seq: 100, Kind: "RuntimeFailed", Failure: payload})
					return
				}
				// The consumer can hold one event while waiting for the I/O lock.
				// All remaining accepted events must fit in the bounded queue.
				for i := 0; i < cap(r.observations)+2; i++ {
					if err := r.Observe(guard, runtime.Observation{HandleID: handleID, Seq: uint64(100 + i), Kind: "auto_retry_start"}); err != nil {
						if i < cap(r.observations) {
							ingress <- fmt.Errorf("observation rejected available capacity at %d: %w", i, err)
							return
						}
						ingress <- err
						return
					}
				}
				ingress <- errors.New("bounded observation queue never overflowed")
			}()
			var observed error
			select {
			case observed = <-ingress:
			case <-guard.Done():
				t.Fatal("Observe blocked behind outcome persistence")
			}
			if tc.overflow {
				var failure *Failure
				if !errors.As(observed, &failure) || failure.Code != LimitExceeded || failure.LimitScope != "run" || failure.Phase != "observation" {
					t.Errorf("overflow not rejected as run-fatal: %v", observed)
				}
			} else if observed != nil {
				t.Errorf("fatal payload ingress failed despite available queue slot: %v", observed)
			}
			unblock()
			var report Report
			select {
			case report = <-done:
			case <-guard.Done():
				t.Fatal("engine did not drain observations and finish cleanup")
			}
			if report.Outcome != tc.outcome || report.Snapshot.WorkflowOutcome != tc.outcome || report.Snapshot.State != tc.outcome || report.ExitCode != 1 {
				t.Errorf("observation crossed outcome lock incorrectly: %+v", report)
			}
			if tc.outcome == Failed {
				engTestReport(t, report, Failed, 1)
				if !engTestCode(report.Failure, StorageFailed) || !errors.Is(report.Failure, payload) {
					t.Errorf("accepted pre-lock fatal payload lost: %v", report.Failure)
				}
			} else {
				if report.Failure != nil || report.Snapshot.Failure != nil || report.Snapshot.StatePersisted || len(report.FinalizationErrors) == 0 || len(report.FinalizationErrors) != len(report.Snapshot.FinalizationErrors) {
					t.Errorf("post-lock fatal rewrote success or lost finalization diagnostics: %+v", report)
				}
				cause := error(payload)
				if tc.overflow {
					cause = observed
				}
				found := false
				for _, err := range report.FinalizationErrors {
					found = found || (engTestCode(err, FinalizationFailed) && errors.Is(err, cause))
				}
				if !found {
					t.Errorf("finalization did not retain observed fatal cause %v: %v", cause, report.FinalizationErrors)
				}
			}
			engTestPersisted(t, r, report)
			engPersistClosed(t, fake, report, 1)
			calls, _, confirms := fake.allSessions()[0].history()
			if len(calls) != 1 || confirms != 1 || len(report.Snapshot.Attempts) != 1 {
				t.Errorf("finalization redispatched: calls/confirms/attempts=%d/%d/%d", len(calls), confirms, len(report.Snapshot.Attempts))
			}
			for _, attempt := range report.Snapshot.Attempts {
				if attempt.State != Succeeded || attempt.Output == nil || attempt.Failure != nil {
					t.Errorf("observation rewrote committed attempt: %+v", attempt)
				}
			}
		})
	}
}

func TestEnginePersistenceRequestWriteFailureConsumesAttempt(t *testing.T) {
	fake := &engTestRuntime{}
	var first, second StepResult
	var firstErr, secondErr, injectionErr error
	hits := 0
	swallowed := false
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		first, firstErr = engTestStep(ctx, run.Root(), h, "request-fails")
		second, secondErr = engTestStep(ctx, run.Root(), h, "must-not-dispatch")
		swallowed = true
		return Result{}, nil
	})
	r.beforeIO = func(path, phase string) {
		if path != "events.jsonl" || phase != "AttemptStarted" {
			return
		}
		hits++
		parent := filepath.Join(r.Dir(), "steps")
		injectionErr = os.Rename(parent, parent+".before-fault")
		if injectionErr == nil {
			injectionErr = os.WriteFile(parent, []byte("not a directory"), 0600)
		}
	}
	report := r.Execute()
	if hits != 1 || injectionErr != nil {
		t.Fatalf("request barrier hits/error=%d/%v", hits, injectionErr)
	}
	engTestReport(t, report, Failed, 1)
	engPersistClosed(t, fake, report, 1)
	if !swallowed || !engTestCode(firstErr, StorageFailed) || !engTestCode(secondErr, StorageFailed) || !engTestCode(report.Failure, StorageFailed) {
		t.Errorf("request StorageFailed was not sticky: first=%v second=%v report=%v", firstErr, secondErr, report.Failure)
	}
	if first.AttemptID == "" || first.Output != (contract.Ref{}) || second != (StepResult{}) || r.totalAttempts != 1 || len(report.Snapshot.Attempts) != 1 || len(report.Snapshot.Invocations) != 1 {
		t.Errorf("request failure lost/duplicated attempt allocation: first=%+v second=%+v count=%d snapshot=%+v", first, second, r.totalAttempts, report.Snapshot)
	}
	attempt, ok := report.Snapshot.Attempts[first.AttemptID]
	if !ok || attempt.Number != 1 || attempt.State != Failed || attempt.DispatchAccepted != AcceptedNo || attempt.Output != nil || attempt.Failure == nil || attempt.Failure.Code != StorageFailed || attempt.Failure.Phase != "begin" {
		t.Errorf("request failure not retained as Preparing failure: %+v", attempt)
	}
	for _, invocation := range report.Snapshot.Invocations {
		if invocation.Attempts != 1 || invocation.LastAttemptID != first.AttemptID || invocation.State != Failed {
			t.Errorf("request failure did not consume invocation number: %+v", invocation)
		}
	}
	calls, _, confirms := fake.allSessions()[0].history()
	if len(calls) != 0 || confirms != 0 {
		t.Errorf("request failure dispatched: calls=%d confirms=%d", len(calls), confirms)
	}
	started, failed := 0, 0
	for _, event := range engTestEvents(t, r) {
		switch event.Kind {
		case "AttemptStarted":
			started++
		case "AttemptFailed":
			failed++
		case "AttemptSettled", "AttemptSucceeded":
			t.Errorf("request failure emitted %s", event.Kind)
		}
	}
	if started != 1 || failed != 1 {
		t.Errorf("request failure journal lost attempt: started/failed=%d/%d", started, failed)
	}
	var disk Snapshot
	if err := engTestReadJSON(filepath.Join(r.Dir(), "run.json"), &disk); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disk, report.Snapshot) {
		t.Error("request failure emergency attempt was not persisted in final snapshot")
	}
}

func TestEnginePersistenceResourceLimitsAreSticky(t *testing.T) {
	for _, resource := range []string{"attempts", "live-sessions", "total-sessions"} {
		t.Run(resource, func(t *testing.T) {
			fake := &engTestRuntime{}
			var observed, afterStop error
			var rejectedStep StepResult
			var rejectedHandle *SessionHandle
			callbacks := 0
			swallowed := false
			r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
				if resource == "attempts" {
					h, err := run.OpenSession(ctx, engTestRole("worker"))
					if err != nil {
						return Result{}, err
					}
					_, observed = run.Root().Retry(ctx, "limited", 5, func(ctx context.Context, scope *Scope, _ RetryState) (RetryAction, error) {
						callbacks++
						step, err := engTestStep(ctx, scope, h, "work")
						if err != nil {
							rejectedStep = step
							return RetryAction{}, err
						}
						return RetryAction{Again: true, Feedback: &Feedback{Message: "explicit retry", SourceAttemptID: step.AttemptID}}, nil
					})
					_, afterStop = engTestStep(ctx, run.Root(), h, "after-stop")
				} else {
					for i := 0; i < 2; i++ {
						h, err := run.OpenSession(ctx, engTestRole("worker"))
						if err != nil {
							return Result{}, err
						}
						if resource == "total-sessions" {
							if err := run.CloseSession(ctx, h); err != nil {
								return Result{}, err
							}
						}
					}
					rejectedHandle, observed = run.OpenSession(ctx, engTestRole("over-limit"))
					_, afterStop = run.OpenSession(ctx, engTestRole("after-stop"))
				}
				swallowed = true
				return Result{}, nil
			})
			// Configure before Execute, preserving the shared real-engine fixture.
			switch resource {
			case "attempts":
				r.definition.Policy.MaxTotalAttempts = 2
			case "live-sessions":
				r.definition.Policy.MaxLiveSessions = 2
			case "total-sessions":
				r.definition.Policy.MaxTotalSessions = 2
			}
			r.state.Policy = r.definition.Policy
			report := r.Execute()
			engTestReport(t, report, Failed, 1)
			engTestPersisted(t, r, report)
			if !swallowed || !engTestCode(observed, LimitExceeded) || !engTestCode(afterStop, LimitExceeded) || !engTestCode(report.Failure, LimitExceeded) || report.Snapshot.Failure == nil || report.Snapshot.Failure.LimitScope != "run" {
				t.Errorf("run limit was swallowed or lost classification: observed=%v after=%v report=%+v", observed, afterStop, report)
			}
			wantSessions, wantAttempts := 2, 0
			if resource == "attempts" {
				wantSessions, wantAttempts = 1, 2
				if callbacks != 3 || rejectedStep != (StepResult{}) {
					t.Errorf("attempt limit dispatched/allocated extra work: callbacks=%d step=%+v", callbacks, rejectedStep)
				}
				if len(report.Snapshot.Invocations) != 1 {
					t.Errorf("retry invocation changed across budget: %+v", report.Snapshot.Invocations)
				}
				for _, inv := range report.Snapshot.Invocations {
					if inv.Attempts != 2 {
						t.Errorf("over-limit retry consumed number: %+v", inv)
					}
				}
			} else if rejectedHandle != nil {
				t.Errorf("session limit returned usable handle: %+v", rejectedHandle)
			}
			if r.totalAttempts != wantAttempts || len(report.Snapshot.Attempts) != wantAttempts || len(r.ownedHandles()) != wantSessions || len(report.Snapshot.Sessions) != wantSessions {
				t.Errorf("resource accounting drift: attempts=%d/%d sessions=%d/%d", r.totalAttempts, len(report.Snapshot.Attempts), len(r.ownedHandles()), len(report.Snapshot.Sessions))
			}
			engPersistClosed(t, fake, report, wantSessions)
			calls := 0
			for _, session := range fake.allSessions() {
				history, _, _ := session.history()
				calls += len(history)
			}
			if calls != wantAttempts {
				t.Errorf("limit automatically dispatched extra work: calls=%d want=%d", calls, wantAttempts)
			}
		})
	}
}

func TestEnginePersistenceObservationQueueSaturation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		queue, capacity int
	}{
		{"default1024", 0, 1024},
		{"small2", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Drain every accepted core event through fsync, including under -race.
			// This is only a watchdog; all ordering is controlled by the I/O barrier.
			guard, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			fake := &engTestRuntime{}
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			overflowed := make(chan error, 1)
			var observed, downstreamErr error
			var downstream StepResult
			var handleID string
			accepted := 0
			swallowed, barrierHit := false, false
			policy := DefaultRunPolicy()
			if tc.queue != 0 {
				policy.Runtime.ObservationQueue = tc.queue
			}
			ctx, cancelRun := context.WithTimeout(context.Background(), 15*time.Second)
			t.Cleanup(cancelRun)
			base := t.TempDir()
			r, err := New(ctx, Definition{Name: "integration", Version: "v1", Policy: policy, Execute: func(ctx context.Context, run *Run, _ Input) (Result, error) {
				h, err := run.OpenSession(ctx, engTestRole("worker"))
				if err != nil {
					return Result{}, err
				}
				handleID = h.id
				if err := run.Observe(ctx, runtime.Observation{HandleID: h.id, Seq: 1, Kind: "auto_retry_start"}); err != nil {
					return Result{}, err
				}
				accepted++
				select {
				case <-entered:
				case <-ctx.Done():
					return Result{}, context.Cause(ctx)
				}
				// The first observation is inside real journal I/O, so the consumer
				// cannot free another slot until the test releases the barrier.
				for i := 0; i < cap(run.observations); i++ {
					if err := run.Observe(ctx, runtime.Observation{HandleID: h.id, Seq: uint64(i + 2), Kind: "auto_retry_start"}); err != nil {
						overflowed <- fmt.Errorf("queue rejected available slot %d: %w", i, err)
						return Result{}, err
					}
					accepted++
				}
				observed = run.Observe(ctx, runtime.Observation{HandleID: h.id, Seq: uint64(accepted + 1), Kind: "auto_retry_start"})
				overflowed <- observed
				<-release
				downstream, downstreamErr = engTestStep(ctx, run.Root(), h, "after-overflow")
				swallowed = true
				return Result{}, nil
			}}, Input{Prompt: `原始 prompt "quotes" $(not-a-command)`, LaunchCWD: base},
				Options{Schemas: engTestSchemas(t), Runtime: fake, BaseDir: base})
			if err != nil {
				t.Fatal(err)
			}
			r.beforeIO = func(path, phase string) {
				if path == "events.jsonl" && phase == "SessionObservation" && !barrierHit {
					barrierHit = true
					close(entered)
					select {
					case <-release:
					case <-guard.Done():
					}
				}
			}
			done := engTestExecuteAsync(t, r)
			select {
			case err := <-overflowed:
				if !engTestCode(err, LimitExceeded) {
					t.Errorf("overflow did not explicitly reject observation: %v", err)
				}
			case <-guard.Done():
				t.Error("workflow did not reach observation overflow barrier")
			}
			unblock()
			var report Report
			select {
			case report = <-done:
			case <-guard.Done():
				t.Fatal("engine did not drain accepted observations and join after queue saturation")
			}
			engTestReport(t, report, Failed, 1)
			engTestPersisted(t, r, report)
			engPersistClosed(t, fake, report, 1)
			if !barrierHit || !swallowed || !engTestCode(observed, LimitExceeded) || !engTestCode(downstreamErr, LimitExceeded) || !engTestCode(report.Failure, LimitExceeded) || report.Snapshot.Failure == nil || report.Snapshot.Failure.LimitScope != "run" || report.Snapshot.Failure.Phase != "observation" {
				t.Errorf("queue saturation was swallowed or misclassified: observed=%v downstream=%v report=%+v", observed, downstreamErr, report)
			}
			if downstream != (StepResult{}) || len(report.Snapshot.Attempts) != 0 || accepted != cap(r.observations)+1 || accepted != tc.capacity+1 {
				t.Errorf("overflow allocated downstream or failed to fill queue: downstream=%+v attempts=%d accepted=%d", downstream, len(report.Snapshot.Attempts), accepted)
			}
			seen := make(map[uint64]int)
			for _, event := range engTestEvents(t, r) {
				if event.Kind != "SessionObservation" {
					continue
				}
				raw, err := json.Marshal(event.Details)
				if err != nil {
					t.Fatal(err)
				}
				var observation runtime.Observation
				if err := json.Unmarshal(raw, &observation); err != nil {
					t.Fatal(err)
				}
				if observation.HandleID != handleID || observation.Kind != "auto_retry_start" {
					t.Errorf("observation identity changed: %+v", observation)
				}
				seen[observation.Seq]++
			}
			if len(seen) != accepted {
				t.Errorf("accepted observations dropped: journal=%d accepted=%d", len(seen), accepted)
			}
			for seq := 1; seq <= accepted; seq++ {
				if seen[uint64(seq)] != 1 {
					t.Errorf("accepted observation %d journal count=%d, want 1", seq, seen[uint64(seq)])
				}
			}
			if seen[uint64(accepted+1)] != 0 {
				t.Error("rejected overflow observation was falsely committed")
			}
			if session := report.Snapshot.Sessions[handleID]; session.RuntimeSeq != uint64(accepted) || session.State != "Closed" {
				t.Errorf("drain lost final runtime seq or reopened closed handle: %+v", session)
			}
			calls, _, _ := fake.allSessions()[0].history()
			if len(calls) != 0 {
				t.Errorf("queue fatal still dispatched %d prompts", len(calls))
			}
		})
	}
}
