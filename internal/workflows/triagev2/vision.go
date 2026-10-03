package triagev2

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const (
	VisionSchema      = "triage.vision.v1"
	VisionBatchSchema = "triage.visionbatch.v1"
)

const visionRequirements = `Read the image file yourself with the read tool: you can see images. Transcribe the visible text that bears on the question into this contract's own evidence file and answer the question from what is visible only: no analysis, no conclusions about the incident, no guesses about text that is cut off or unreadable (record those as gaps).`

// VisionPolicy bounds the vision Steps; the workflow definition names every
// value.
type VisionPolicy struct {
	Model runtime.ModelSpec
	// MaxSteps is Vmax, the vision Steps of the whole run.
	MaxSteps int
	// Parallel is how many run at once.
	Parallel int
	Timeout  time.Duration
}

func (p VisionPolicy) check() error {
	if p.MaxSteps < 0 || p.Parallel < 1 || p.Timeout <= 0 || p.Model.Provider == "" || p.Model.ID == "" {
		return fmt.Errorf("vision policy needs a model, MaxSteps of at least 0, Parallel of at least 1 and a positive Timeout")
	}
	return nil
}

type Vision struct {
	Request VisionRef `json:"request"`
	Image   Evidence  `json:"image"`
	// Transcript is this contract's own evidence file.
	Transcript Evidence `json:"transcript"`
	Answer     string   `json:"answer"`
	Gaps       []Gap    `json:"gaps"`
}

type VisionRef struct {
	Ref contract.Ref `json:"ref"`
	ID  string       `json:"id"`
}

type VisionResult struct {
	ID     string       `json:"id"`
	Vision contract.Ref `json:"vision"`
}

// VisionBatch is the Controller's record of one contract's vision requests.
type VisionBatch struct {
	Owner   contract.Ref   `json:"owner"`
	Results []VisionResult `json:"results"`
	Gaps    []Gap          `json:"gaps"`
}

// visionTask is what a vision Step answers.
type visionTask struct {
	Request  VisionRef `json:"request"`
	Image    Evidence  `json:"image"`
	Question string    `json:"question"`
}

// visionImage is the request's attachment as the vision Step must copy it:
// an image the requesting contract holds itself is cited through that
// contract's Ref.
func visionImage(owner contract.Ref, q VisionRequest) Evidence {
	image := q.Attachment
	if image.Ref == nil {
		image.Ref = &owner
	}
	return image
}

// checkVision requires the exact request and image and an own transcript
// file. The image citation was resolved when the requesting contract was
// accepted; whether the transcription is right is for the Agent that cites
// it.
func checkVision(ctx context.Context, r *engine.Run, ref contract.Ref, want visionTask) error {
	p, err := readAccepted[Vision](ctx, r, ref, VisionSchema)
	if err != nil {
		return err
	}
	v := p.Data
	if err := sameRef("request.ref", v.Request.Ref, want.Request.Ref); err != nil {
		return err
	}
	if v.Request.ID != want.Request.ID {
		return fmt.Errorf("request.id: got %q; want %q from the request", v.Request.ID, want.Request.ID)
	}
	if err := sameEvidence("image", v.Image, want.Image); err != nil {
		return err
	}
	if v.Transcript.Ref != nil {
		return fmt.Errorf("transcript.ref: got %s; want null, this contract's own evidence file", describeRef(*v.Transcript.Ref))
	}
	if err := (citations{ctx, Inputs{}, ref, p.Files}).check("transcript", v.Transcript); err != nil {
		return err
	}
	return checkGaps("gaps", v.Gaps)
}

// sameEvidence names the part of a copied citation that differs.
func sameEvidence(field string, got, want Evidence) error {
	switch {
	case got.Ref == nil:
		return fmt.Errorf("%s.ref: got null; want %s copied byte-exact from the request", field, describeRef(*want.Ref))
	case got.FileID != want.FileID:
		return fmt.Errorf("%s.file_id: got %q; want %q from the request", field, got.FileID, want.FileID)
	case !reflect.DeepEqual(got.Locator, want.Locator):
		return fmt.Errorf("%s.locator: want the locator copied byte-exact from the request", field)
	}
	return sameRef(field+".ref", *got.Ref, *want.Ref)
}

// runVision dispatches the vision requests of one contract, at most
// Parallel at a time and only the first allowed; each request not run gets
// a gap in the Controller's batch record. A timed-out vision Step reruns
// like a round. Any other failure fails the run.
func runVision(ctx context.Context, r *engine.Run, ticket string, policy RoundPolicy, key string, owner contract.Ref, requests []VisionRequest, allowed int) (contract.Ref, VisionBatch, []RecoveryFailure, error) {
	batch := VisionBatch{Owner: owner, Results: []VisionResult{}, Gaps: []Gap{}}
	n := min(allowed, len(requests))
	for _, q := range requests[n:] {
		batch.Gaps = append(batch.Gaps, Gap{ID: absentGapID("vision-not-run-", q.ID, batch.Gaps), Text: fmt.Sprintf("Vision request %s was not run: the vision budget, the round limit or the run's session and attempt budget is used up", q.ID)})
	}
	var failures []RecoveryFailure
	var mu sync.Mutex
	for start := 0; start < n; start += policy.Vision.Parallel {
		chunk := requests[start:min(start+policy.Vision.Parallel, n)]
		var branches []engine.Branch
		for _, q := range chunk {
			branches = append(branches, engine.Branch{Name: "vision-" + q.ID, Do: func(ctx context.Context, s *engine.Scope) (engine.Result, error) {
				ref, fails, err := visionStep(ctx, r, s, ticket, policy, owner, q)
				mu.Lock()
				failures = append(failures, fails...)
				mu.Unlock()
				if err != nil {
					return engine.Result{}, err
				}
				return engine.Result{Outputs: map[string]contract.Ref{"vision": ref}}, nil
			}})
		}
		results, err := r.Root().Parallel(ctx, fmt.Sprintf("%s-%d", key, start), engine.CollectAll, branches)
		if err != nil {
			return contract.Ref{}, batch, failures, err
		}
		var errs []error
		for i, res := range results {
			if res.Err != nil {
				errs = append(errs, res.Err)
				continue
			}
			batch.Results = append(batch.Results, VisionResult{ID: chunk[i].ID, Vision: res.Result.Outputs["vision"]})
		}
		if len(errs) > 0 {
			return contract.Ref{}, batch, failures, errors.Join(errs...)
		}
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: key, Output: contract.Spec{SchemaID: VisionBatchSchema}, Data: batch})
	return ref, batch, failures, err
}

func visionStep(ctx context.Context, r *engine.Run, s *engine.Scope, ticket string, policy RoundPolicy, owner contract.Ref, q VisionRequest) (contract.Ref, []RecoveryFailure, error) {
	want := visionTask{Request: VisionRef{Ref: owner, ID: q.ID}, Image: visionImage(owner, q), Question: q.Question}
	t := newTask(r, "vision", ticket, nil, visionRequirements)
	t.Citable = []LabeledRef{{"request owner", owner}}
	if *want.Image.Ref != owner {
		t.Citable = append(t.Citable, LabeledRef{"image owner", *want.Image.Ref})
	}
	t.Vision = &want
	return RetryInputs(ctx, r, s, "vision", "vision", policy.TimeoutRetries, func(ctx context.Context, s *engine.Scope, retry *engine.Feedback) (contract.Ref, error) {
		return RunTaskStep(ctx, r, TaskStep{Scope: s, Model: policy.Vision.Model, Stage: "vision", Key: "vision", Task: t, Schema: VisionSchema, Inputs: t.inputs(), Recovery: true, Timeout: policy.Vision.Timeout, Feedback: retry,
			Validate: func(ctx context.Context, ref contract.Ref) error { return checkVision(ctx, r, ref, want) }})
	}, nil)
}

// visionAllowed is how many vision Steps may still run: within Vmax, and
// leaving the run budget for the batch record and one more round. Each
// vision Step may be retried after a timeout and repaired once.
func visionAllowed(s engine.Snapshot, p RoundPolicy, used int) int {
	per := p.TimeoutRetries + 1
	sessions, attempts := roundCost(p)
	bySessions := (s.Policy.MaxTotalSessions - len(s.Sessions) - sessions) / per
	byAttempts := (s.Policy.MaxTotalAttempts - len(s.Attempts) - attempts - batchAttach) / (2 * per)
	return max(0, min(p.Vision.MaxSteps-used, bySessions, byAttempts))
}
