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
		Files: []contract.ControllerFile{{ID: "prompt", Kind: "evidence", Path: "evidence/prompt.txt", Data: []byte(value)}}}
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
		// An Agent publishing the same schema and file id is not a Controller artifact.
		fake.execute = func(_ context.Context, call engTestCall) engTestReply {
			return engTestReply{Data: engTestData{Value: "impersonated"}}
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
	if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope.Files) != 1 || envelope.Files[0] != (contract.FileEntry{ID: "prompt", Kind: "evidence", Path: "evidence/prompt.txt"}) {
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
	for _, tc := range []struct {
		name     string
		policy   func(*RunPolicy)
		attach   func(context.Context, *Run) (contract.Ref, error)
		code     Code
		attempts int
		outcome  State
	}{
		{"invalid key", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			return r.Root().Attach(ctx, engAttachSpec("bad key", "x"))
		}, InvalidDefinition, 0, Succeeded},
		{"key used by a Step in the same scope", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			h, err := r.OpenSession(ctx, engTestRole("worker"))
			if err != nil {
				return contract.Ref{}, err
			}
			if _, err := engTestStep(ctx, r.Root(), h, "shared"); err != nil {
				return contract.Ref{}, err
			}
			return r.Root().Attach(ctx, engAttachSpec("shared", "x"))
		}, InvalidDefinition, 1, Succeeded},
		{"unregistered schema", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			spec := engAttachSpec("prompt", "x")
			spec.Output.SchemaID = "engine.unknown.v1"
			return r.Root().Attach(ctx, spec)
		}, InvalidDefinition, 0, Succeeded},
		{"data that is not JSON", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			spec := engAttachSpec("prompt", "x")
			spec.Data = make(chan int)
			return r.Root().Attach(ctx, spec)
		}, InvalidDefinition, 0, Succeeded},
		{"data violating the schema", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			return r.Root().Attach(ctx, engAttachSpec("prompt", ""))
		}, ContractInvalid, 1, Succeeded},
		{"file outside evidence and artifacts", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			spec := engAttachSpec("prompt", "x")
			spec.Files[0].Path = "evidence/../request.json"
			return r.Root().Attach(ctx, spec)
		}, InvalidDefinition, 1, Succeeded},
		{"artifact declared under evidence", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			spec := engAttachSpec("prompt", "x")
			spec.Files[0].Kind = "artifact"
			return r.Root().Attach(ctx, spec)
		}, ContractInvalid, 1, Succeeded},
		{"uncommitted input", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			spec := engAttachSpec("prompt", "x")
			spec.Inputs = []contract.Ref{{RunID: r.ID(), AttemptID: strings.Repeat("a", 32), SchemaID: engTestSchema}}
			return r.Root().Attach(ctx, spec)
		}, ReferenceInvalid, 1, Succeeded},
		{"cancelled context", nil, func(ctx context.Context, r *Run) (contract.Ref, error) {
			cancelled, cancel := context.WithCancelCause(ctx)
			cancel(errors.New("caller stopped"))
			return r.Root().Attach(cancelled, engAttachSpec("prompt", "x"))
		}, WorkflowFailed, 0, Succeeded},
		{"run attempt limit", func(p *RunPolicy) { p.MaxTotalAttempts = 1 }, func(ctx context.Context, r *Run) (contract.Ref, error) {
			if _, err := r.Root().Attach(ctx, engAttachSpec("first", "x")); err != nil {
				return contract.Ref{}, err
			}
			return r.Root().Attach(ctx, engAttachSpec("second", "x"))
		}, LimitExceeded, 1, Failed},
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
			if tc.code == WorkflowFailed {
				if !strings.Contains(fmt.Sprint(attachErr), "caller stopped") {
					t.Fatalf("Attach error = %v, want the caller's cancellation cause", attachErr)
				}
			} else if !engTestCode(attachErr, tc.code) {
				t.Fatalf("Attach error = %v, want %s", attachErr, tc.code)
			}
			if ref != (contract.Ref{}) || attached {
				t.Fatalf("rejected Attach returned an authorized Ref: %+v", ref)
			}
			if report.Outcome != tc.outcome {
				t.Fatalf("outcome = %s, want %s: %v", report.Outcome, tc.outcome, report.Failure)
			}
			controller := 0
			for _, a := range report.Snapshot.Attempts {
				if a.Controller {
					controller++
					if a.Key != "first" && (a.State != Failed || a.Output != nil) {
						t.Fatalf("rejected attach attempt = %+v", a)
					}
				}
			}
			if want := tc.attempts; tc.name == "key used by a Step in the same scope" {
				if controller != 0 || len(report.Snapshot.Attempts) != want {
					t.Fatalf("attempts = %+v", report.Snapshot.Attempts)
				}
			} else if controller != want {
				t.Fatalf("controller attempts = %d, want %d: %+v", controller, want, report.Snapshot.Attempts)
			}
		})
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
				if a.State == Succeeded && site != "snapshot" {
					t.Fatalf("failed attach left a successful attempt: %+v", a)
				}
			}
		})
	}
}
