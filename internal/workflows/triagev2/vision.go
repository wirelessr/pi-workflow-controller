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

const visionRequirements = `Read the image file yourself with the read tool: you can see images. Do not create agents or delegate, including to any agent named vision. Transcribe the visible text that bears on the question into this contract's own evidence file and answer the question from what is visible only: no analysis, no conclusions about the incident, no guesses about text that is cut off or unreadable (record those as gaps).`

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
	if p.MaxSteps < 0 || p.Parallel < 1 || p.Timeout <= 0 {
		return fmt.Errorf("vision policy needs MaxSteps of at least 0, Parallel of at least 1 and a positive Timeout")
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

// checkVision requires the exact request and image, an own transcript file
// and resolvable citations. Whether the transcription is right is for the
// Agent that cites it.
func checkVision(ctx context.Context, r *engine.Run, ref, owner contract.Ref, q VisionRequest) (Vision, error) {
	p, err := readAccepted[Vision](ctx, r, ref, VisionSchema)
	if err != nil {
		return Vision{}, err
	}
	v := p.Data
	if err := sameRef("request.ref", v.Request.Ref, owner); err != nil {
		return v, err
	}
	if v.Request.ID != q.ID {
		return v, fmt.Errorf("request.id: got %q; want %q from the request", v.Request.ID, q.ID)
	}
	image := visionImage(owner, q)
	if !reflect.DeepEqual(v.Image, image) {
		return v, fmt.Errorf("image: got file %q; want the image evidence copied byte-exact from the request", v.Image.FileID)
	}
	if v.Transcript.Ref != nil {
		return v, fmt.Errorf("transcript.ref: got %s; want null, this contract's own evidence file", describeRef(*v.Transcript.Ref))
	}
	in, err := citable(ctx, r, visionInputs(owner, image)...)
	if err != nil {
		return v, err
	}
	cite := citations{ctx, in, ref, p.Files}.check
	if err := cite("image", v.Image); err != nil {
		return v, err
	}
	if err := cite("transcript", v.Transcript); err != nil {
		return v, err
	}
	return v, checkGaps("gaps", v.Gaps)
}

func visionInputs(owner contract.Ref, image Evidence) []contract.Ref {
	if *image.Ref == owner {
		return []contract.Ref{owner}
	}
	return []contract.Ref{owner, *image.Ref}
}

// runVision dispatches the vision requests of one contract, at most
// Parallel at a time and only as many as the remaining budget allows; each
// request not run gets a gap in the Controller's batch record. A timed-out
// vision Step reruns like a round. Any other failure fails the run.
func runVision(ctx context.Context, r *engine.Run, s0 S0, policy VisionPolicy, retries int, key string, owner contract.Ref, requests []VisionRequest, allowed int) (contract.Ref, []RecoveryFailure, error) {
	batch := VisionBatch{Owner: owner, Results: []VisionResult{}, Gaps: []Gap{}}
	var failures []RecoveryFailure
	var mu sync.Mutex
	for start := 0; start < len(requests); start += policy.Parallel {
		chunk := requests[start:min(start+policy.Parallel, len(requests))]
		var branches []engine.Branch
		var run []VisionRequest
		for _, q := range chunk {
			if len(batch.Results)+len(run) >= allowed {
				batch.Gaps = append(batch.Gaps, Gap{ID: absentGapID("vision-not-run-", q.ID, len(batch.Gaps)+1), Text: fmt.Sprintf("Vision request %s was not run: the vision budget or the run's session and attempt budget is used up", q.ID)})
				continue
			}
			run = append(run, q)
			branches = append(branches, engine.Branch{Name: "vision-" + q.ID, Do: func(ctx context.Context, s *engine.Scope) (engine.Result, error) {
				ref, fails, err := visionStep(ctx, r, s, s0, policy, retries, owner, q)
				mu.Lock()
				failures = append(failures, fails...)
				mu.Unlock()
				if err != nil {
					return engine.Result{}, err
				}
				return engine.Result{Outputs: map[string]contract.Ref{"vision": ref}}, nil
			}})
		}
		if len(branches) == 0 {
			continue
		}
		results, err := r.Root().Parallel(ctx, fmt.Sprintf("%s-%d", key, start), engine.CollectAll, branches)
		if err != nil {
			return contract.Ref{}, failures, err
		}
		var errs []error
		for i, res := range results {
			if res.Err != nil {
				errs = append(errs, res.Err)
				continue
			}
			batch.Results = append(batch.Results, VisionResult{ID: run[i].ID, Vision: res.Result.Outputs["vision"]})
		}
		if len(errs) > 0 {
			return contract.Ref{}, failures, errors.Join(errs...)
		}
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: key, Output: contract.Spec{SchemaID: VisionBatchSchema}, Data: batch})
	return ref, failures, err
}

func visionStep(ctx context.Context, r *engine.Run, s *engine.Scope, s0 S0, policy VisionPolicy, retries int, owner contract.Ref, q VisionRequest) (contract.Ref, []RecoveryFailure, error) {
	image := visionImage(owner, q)
	t := newTask(r, "vision", s0.Ticket, nil, visionRequirements)
	t.Citable = []LabeledRef{{"request owner", owner}}
	if *image.Ref != owner {
		t.Citable = append(t.Citable, LabeledRef{"image owner", *image.Ref})
	}
	t.Vision = &visionTask{Request: VisionRef{Ref: owner, ID: q.ID}, Image: image, Question: q.Question}
	return RetryInputs(ctx, r, s, "vision", "vision", retries, func(ctx context.Context, s *engine.Scope, retry *engine.Feedback) (contract.Ref, error) {
		return RunTaskStep(ctx, r, TaskStep{Scope: s, Model: policy.Model, Stage: "vision", Key: "vision", Task: t, Schema: VisionSchema, Inputs: t.inputs(), Recovery: true, Timeout: policy.Timeout, Feedback: retry,
			Validate: func(ctx context.Context, ref contract.Ref) error {
				_, err := checkVision(ctx, r, ref, owner, q)
				return err
			}})
	}, nil)
}

// visionAllowed is how many vision Steps may still run: within Vmax, and
// leaving the run budget for one more worst-case round.
func visionAllowed(s engine.Snapshot, p RoundPolicy, used int) int {
	per := p.TimeoutRetries + 1
	round := 2 * per
	bySessions := (s.Policy.MaxTotalSessions - len(s.Sessions) - round) / per
	byAttempts := (s.Policy.MaxTotalAttempts - len(s.Attempts) - 1 - (2*round + 1)) / (2 * per)
	return max(0, min(p.Vision.MaxSteps-used, bySessions, byAttempts))
}
