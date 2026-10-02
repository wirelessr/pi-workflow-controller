package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"pi-workflow-controller/internal/contract"
)

func engAttachSpec(key, value string) AttachSpec {
	return AttachSpec{Key: key, Output: contract.Spec{SchemaID: engTestSchema}, Data: engTestData{Value: value},
		Files: []contract.ControllerFile{{ID: "prompt", Path: "evidence/prompt.txt", Data: []byte(value)}, {ID: "note", Path: "artifacts/note.md", Data: []byte("note")}}}
}

func TestEngineAttachCommitsControllerArtifact(t *testing.T) {
	fake := &engTestRuntime{}
	var attached contract.Ref
	var agent StepResult
	var decoded engTestData
	var raw json.RawMessage
	r, _ := engTestNew(t, "", fake, func(ctx context.Context, run *Run, _ Input) (Result, error) {
		var err error
		if attached, err = run.Root().Attach(ctx, engAttachSpec("caller-prompt", "original prompt")); err != nil {
			return Result{}, err
		}
		if decoded, err = Decode[engTestData](ctx, run, attached); err != nil {
			return Result{}, err
		}
		if raw, err = ReadContract(ctx, run, attached); err != nil {
			return Result{}, err
		}
		h, err := run.OpenSession(ctx, engTestRole("worker"))
		if err != nil {
			return Result{}, err
		}
		// An Agent publishing the same schema, file id and path is not a Controller artifact.
		fake.execute = func(_ context.Context, call engTestCall) engTestReply {
			if err := os.WriteFile(filepath.Join(filepath.Dir(call.CandidatePath), "evidence", "prompt.txt"), []byte("original prompt"), 0600); err != nil {
				return engTestReply{Err: err}
			}
			raw, err := json.Marshal(map[string]any{
				"meta":  map[string]any{"run_id": call.Request.Identity.RunID, "invocation_id": call.Request.Identity.InvocationID, "attempt_id": call.Request.Identity.AttemptID, "dispatch_token": call.Request.Identity.DispatchToken, "version": 1, "schema_id": engTestSchema},
				"data":  engTestData{Value: "original prompt"},
				"files": []contract.FileEntry{{ID: "prompt", Kind: "evidence", Path: "evidence/prompt.txt"}},
			})
			return engTestReply{Raw: raw, Err: err}
		}
		if agent, err = engTestStep(ctx, run.Root(), h, "consumer", attached); err != nil {
			return Result{}, err
		}
		if err := run.Root().Decision(ctx, "prompt-recorded", "Controller artifact consumed", []contract.Ref{attached, agent.Output}); err != nil {
			return Result{}, err
		}
		return Result{Outputs: map[string]contract.Ref{"prompt": attached, "agent": agent.Output}}, nil
	})
	report := r.Execute()
	engTestReport(t, report, Succeeded, 0)
	engTestPersisted(t, r, report)
	if decoded.Value != "original prompt" || !r.ControllerAttached(attached) || r.ControllerAttached(agent.Output) || r.ControllerAttached(contract.Ref{}) {
		t.Fatalf("decoded=%+v attached owner=%t agent owner=%t", decoded, r.ControllerAttached(attached), r.ControllerAttached(agent.Output))
	}
	tampered := attached
	tampered.SHA256 = strings.Repeat("0", 64)
	if r.ControllerAttached(tampered) {
		t.Fatal("inexact Ref accepted as Controller artifact")
	}
	var envelope struct {
		Files []contract.FileEntry `json:"files"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope.Files) != 2 || envelope.Files[0] != (contract.FileEntry{ID: "prompt", Kind: "evidence", Path: "evidence/prompt.txt"}) || envelope.Files[1] != (contract.FileEntry{ID: "note", Kind: "artifact", Path: "artifacts/note.md"}) {
		t.Fatalf("attached files = %+v, %v", envelope.Files, err)
	}
	body, err := os.ReadFile(filepath.Join(filepath.Dir(attached.Path), "evidence", "prompt.txt"))
	if err != nil || string(body) != "original prompt" {
		t.Fatalf("published Controller file = %q, %v", body, err)
	}
	calls, _, _ := fake.allSessions()[0].history()
	if len(calls) != 1 || len(calls[0].Request.Inputs) != 1 || calls[0].Request.Inputs[0] != attached {
		t.Fatalf("Agent did not receive the attached Ref as input: %+v", calls)
	}
	a := report.Snapshot.Attempts[attached.AttemptID]
	if !a.Controller || a.HandleID != "" || a.DispatchAccepted != AcceptedNo || a.Execution != nil || a.State != Succeeded || a.Output == nil || *a.Output != attached || a.Scope != "root" || a.Key != "caller-prompt" || a.Number != 1 {
		t.Fatalf("attach attempt state = %+v", a)
	}
	if s := report.Snapshot.Attempts[agent.Output.AttemptID]; s.Controller || s.HandleID == "" {
		t.Fatalf("agent attempt marked as Controller: %+v", s)
	}
	var kinds []string
	for _, event := range engTestEvents(t, r) {
		if strings.HasPrefix(event.Kind, "Attempt") {
			raw, _ := json.Marshal(event.Details)
			if strings.Contains(string(raw), attached.AttemptID) {
				kinds = append(kinds, event.Kind)
			}
		}
	}
	if strings.Join(kinds, ",") != "AttemptStarted,AttemptSucceeded" {
		t.Fatalf("attach journal events = %v", kinds)
	}
	var onDisk AttemptState
	if err := engTestReadJSON(filepath.Join(r.Dir(), "steps", a.Identity.InvocationID, "attempts", "0001-"+attached.AttemptID, "attempt.json"), &onDisk); err != nil || !onDisk.Controller || onDisk.State != Succeeded {
		t.Fatalf("attempt snapshot = %+v, %v", onDisk, err)
	}
}

func TestEngineAttachRejections(t *testing.T) {
	withFile := func(mutate func(*AttachSpec)) func(context.Context, *Run) (contract.Ref, error) {
		return func(ctx context.Context, r *Run) (contract.Ref, error) {
			spec := engAttachSpec("prompt", "x")
			mutate(&spec)
			return r.Root().Attach(ctx, spec)
		}
	}
	for _, tc := range []struct {
		name     string
		policy   func(*RunPolicy)
		attach   func(context.Context, *Run) (contract.Ref, error)
		code     Code
		cause    string
		attempts int
		outcome  State
	}{
		{"invalid key", nil, withFile(func(s *AttachSpec) { s.Key = "bad key" }), InvalidDefinition, "", 0, Succeeded},
		{"key used by a Step in the same scope", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			h, err := r.OpenSession(ctx, engTestRole("worker"))
			if err != nil {
				return contract.Ref{}, err
			}
			if _, err := engTestStep(ctx, r.Root(), h, "shared"); err != nil {
				return contract.Ref{}, err
			}
			return r.Root().Attach(ctx, engAttachSpec("shared", "x"))
		}, InvalidDefinition, "", 1, Succeeded},
		{"unregistered schema", nil, withFile(func(s *AttachSpec) { s.Output.SchemaID = "engine.unknown.v1" }), InvalidDefinition, "", 0, Succeeded},
		{"data that is not JSON", nil, withFile(func(s *AttachSpec) { s.Data = make(chan int) }), InvalidDefinition, "", 0, Succeeded},
		{"file outside evidence and artifacts", nil, withFile(func(s *AttachSpec) { s.Files[0].Path = "evidence/../request.json" }), InvalidDefinition, "", 0, Succeeded},
		{"nested file path", nil, withFile(func(s *AttachSpec) { s.Files[0].Path = "evidence/a/b.txt" }), InvalidDefinition, "", 0, Succeeded},
		{"duplicate file path", nil, withFile(func(s *AttachSpec) { s.Files[1].Path = s.Files[0].Path }), InvalidDefinition, "", 0, Succeeded},
		{"file paths differing only in case", nil, withFile(func(s *AttachSpec) { s.Files[1].Path = "evidence/PROMPT.txt" }), InvalidDefinition, "", 0, Succeeded},
		{"duplicate file id", nil, withFile(func(s *AttachSpec) { s.Files[1].ID = s.Files[0].ID }), InvalidDefinition, "", 0, Succeeded},
		{"file over the file byte limit", func(p *RunPolicy) { p.MaxFileBytes = 4 }, withFile(func(s *AttachSpec) { s.Files[0].Data = []byte("12345") }), LimitExceeded, "", 0, Succeeded},
		{"more files than the attempt allows", func(p *RunPolicy) { p.MaxAttemptFiles = 1 }, withFile(func(*AttachSpec) {}), LimitExceeded, "", 0, Succeeded},
		{"data violating the schema", nil, withFile(func(s *AttachSpec) { s.Data = engTestData{} }), ContractInvalid, "", 1, Succeeded},
		{"cancelled context", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			cancelled, cancel := context.WithCancelCause(ctx)
			cancel(errors.New("caller stopped"))
			return r.Root().Attach(cancelled, engAttachSpec("prompt", "x"))
		}, "", "caller stopped", 0, Succeeded},
		{"run attempt limit", func(p *RunPolicy) { p.MaxTotalAttempts = 1 }, func(ctx context.Context, r *Run) (contract.Ref, error) {
			if _, err := r.Root().Attach(ctx, engAttachSpec("first", "x")); err != nil {
				return contract.Ref{}, err
			}
			return r.Root().Attach(ctx, engAttachSpec("second", "x"))
		}, LimitExceeded, "", 1, Failed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := t.TempDir()
			policy := DefaultRunPolicy()
			if tc.policy != nil {
				tc.policy(&policy)
			}
			var ref contract.Ref
			var attachErr error
			var attached bool
			r, err := New(context.Background(), Definition{Name: "attach-rejection", Version: "1", Policy: policy, Execute: func(ctx context.Context, run *Run, _ Input) (Result, error) {
				ref, attachErr = tc.attach(ctx, run)
				attached = run.ControllerAttached(ref)
				return Result{}, nil
			}}, Input{Prompt: "attach rejection", LaunchCWD: base}, Options{BaseDir: base, Schemas: engTestSchemas(t), Runtime: &engTestRuntime{}})
			if err != nil {
				t.Fatal(err)
			}
			report := r.Execute()
			if tc.cause != "" && !strings.Contains(fmt.Sprint(attachErr), tc.cause) || tc.code != "" && !engTestCode(attachErr, tc.code) {
				t.Fatalf("Attach error = %v, want code %q cause %q", attachErr, tc.code, tc.cause)
			}
			if ref != (contract.Ref{}) || attached {
				t.Fatalf("rejected Attach returned an authorized Ref: %+v", ref)
			}
			if report.Outcome != tc.outcome {
				t.Fatalf("outcome = %s, want %s: %v", report.Outcome, tc.outcome, report.Failure)
			}
			if len(report.Snapshot.Attempts) != tc.attempts {
				t.Fatalf("attempts = %d, want %d: %+v", len(report.Snapshot.Attempts), tc.attempts, report.Snapshot.Attempts)
			}
			for _, a := range report.Snapshot.Attempts {
				if a.Controller && a.Key != "first" && (a.State != Failed || a.Output != nil) {
					t.Fatalf("rejected attach attempt = %+v", a)
				}
			}
		})
	}
}

// A run failure raised elsewhere while an Attach is in flight is collateral
// cancellation of that attempt, not the attempt's own failure.
func TestEngineAttachCollateralRunFailure(t *testing.T) {
	base := t.TempDir()
	root := newFailure(LimitExceeded, "probe", "run limit raised by another operation")
	root.LimitScope = "run"
	var attachErr error
	r, err := New(context.Background(), Definition{Name: "attach-collateral", Version: "1", Policy: DefaultRunPolicy(), Execute: func(ctx context.Context, run *Run, _ Input) (Result, error) {
		_, attachErr = run.Root().Attach(ctx, engAttachSpec("prompt", "x"))
		return Result{}, nil
	}}, Input{Prompt: "attach collateral", LaunchCWD: base}, Options{BaseDir: base, Schemas: engTestSchemas(t), Runtime: &engTestRuntime{}})
	if err != nil {
		t.Fatal(err)
	}
	r.afterPublish = func(contract.Ref) {
		r.mu.Lock()
		r.stopLocked(root)
		r.mu.Unlock()
	}
	report := r.Execute()
	if !engTestCode(attachErr, Cancelled) || !errors.Is(attachErr, root) {
		t.Fatalf("Attach error = %v, want collateral cancellation caused by the run failure", attachErr)
	}
	if !engTestCode(report.Failure, LimitExceeded) || len(r.publications) != 0 {
		t.Fatalf("root failure = %v, registry = %d", report.Failure, len(r.publications))
	}
	for _, a := range report.Snapshot.Attempts {
		if a.State != CancelledState || a.Failure == nil || a.Failure.Message != "attempt cancelled by run failure" || a.Output != nil {
			t.Fatalf("attach attempt = %+v failure=%+v", a, a.Failure)
		}
	}
}

func TestEngineAttachPersistenceFaults(t *testing.T) {
	for _, site := range []string{"controller-file", "journal", "snapshot"} {
		t.Run(site, func(t *testing.T) {
			base := t.TempDir()
			injected := errors.New("synthetic attach Sync failure")
			var armed atomic.Bool
			callback := func(f *os.File) error {
				name := filepath.Base(f.Name())
				switch {
				case site == "controller-file" && name == "prompt.txt",
					armed.Load() && site == "journal" && name == "events.jsonl",
					armed.Load() && site == "snapshot" && strings.HasPrefix(name, ".engine-"):
					return injected
				}
				return f.Sync()
			}
			var published, ref contract.Ref
			var attachErr, decodeErr, laterErr error
			var attached bool
			r, err := New(context.Background(), Definition{Name: "attach-fault", Version: "1", Policy: DefaultRunPolicy(), Execute: func(ctx context.Context, run *Run, _ Input) (Result, error) {
				ref, attachErr = run.Root().Attach(ctx, engAttachSpec("prompt", "x"))
				attached = run.ControllerAttached(published)
				if published != (contract.Ref{}) {
					_, decodeErr = Decode[engTestData](ctx, run, published)
				}
				_, laterErr = run.Root().Attach(ctx, engAttachSpec("later", "y"))
				return Result{}, nil
			}}, Input{Prompt: "attach fault", LaunchCWD: base}, Options{BaseDir: base, Schemas: engTestSchemas(t), Runtime: &engTestRuntime{}, SyncFile: callback})
			if err != nil {
				t.Fatal(err)
			}
			r.afterPublish = func(p contract.Ref) { published = p; armed.Store(true) }
			report := r.Execute()
			want := StorageFailed
			if site == "journal" {
				want = JournalFailed
			}
			for _, e := range []error{attachErr, laterErr, report.Failure} {
				if !engTestCode(e, want) || !errors.Is(e, injected) {
					t.Errorf("lost sticky %s failure: %v", want, e)
				}
			}
			if ref != (contract.Ref{}) || attached || len(r.publications) != 0 {
				t.Fatalf("uncommitted attach authorized: ref=%+v owner=%t registry=%d", ref, attached, len(r.publications))
			}
			if site != "controller-file" {
				if published == (contract.Ref{}) || !engTestCode(decodeErr, want) {
					t.Fatalf("rename-only publication readable: published=%+v decode=%v", published, decodeErr)
				}
				raw, err := os.ReadFile(published.Path)
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(raw)
				if hex.EncodeToString(sum[:]) != published.SHA256 {
					t.Fatal("fixture did not reach real Store.Publish")
				}
			} else if published != (contract.Ref{}) {
				t.Fatal("failed Controller file write still published")
			}
			// A Store file failure leaves the journal and snapshots intact.
			if report.Outcome != Failed || report.Snapshot.StatePersisted == (site != "controller-file") {
				t.Fatalf("fault reported wrong persistence: %+v", report)
			}
			// After a durable AttemptSucceeded only the snapshot failed, so the
			// attempt may read Succeeded; its Ref still never entered the registry.
			for _, a := range report.Snapshot.Attempts {
				if a.State == Succeeded && site != "snapshot" || a.State != Succeeded && a.Output != nil {
					t.Fatalf("failed attach left a successful attempt or an uncommitted Ref: %+v", a)
				}
			}
		})
	}
}
