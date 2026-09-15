package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	"pi-workflow-controller/internal/contract"
)

func (r *Run) resolve(ctx context.Context, ref contract.Ref) (json.RawMessage, error) {
	if r == nil || r.store == nil {
		return nil, newFailure(ReferenceInvalid, "resolve", "run is not initialized")
	}
	r.mu.Lock()
	p, ok := r.publications[ref.AttemptID]
	r.mu.Unlock()
	if !ok || ref.RunID != r.ID() || ref != p.Ref {
		return nil, newFailure(ReferenceInvalid, "resolve", "reference is not an exact committed publication of this run")
	}
	// Store verifies the canonical path and the identity it reserved for this attempt.
	raw, err := r.store.Read(ctx, ref)
	if err != nil {
		if err == context.Cause(ctx) {
			return nil, err
		}
		if owner, ok := ctx.Value(attemptContextKey{}).(attemptOwner); ok {
			f := sourceFailure(err, []contract.Identity{owner.Identity})
			f.HandleID = owner.HandleID
			return nil, r.recordError(f)
		}
		return nil, r.recordError(err)
	}
	return raw, nil
}

// ReadContract returns the verified envelope bytes through the same committed
// resolver as Decode. File IDs can be checked without reopening mutable JSON.
func ReadContract(ctx context.Context, r *Run, ref contract.Ref) (json.RawMessage, error) {
	if r == nil {
		return nil, newFailure(InvalidDefinition, "ReadContract", "run is not initialized")
	}
	r.mu.Lock()
	err := r.checkLocked(ctx)
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	op, done := r.operationContext(ctx)
	defer done()
	return r.resolve(op, ref)
}

func Decode[T any](ctx context.Context, r *Run, ref contract.Ref) (T, error) {
	var zero T
	raw, err := ReadContract(ctx, r, ref)
	if err != nil {
		return zero, err
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err = json.Unmarshal(raw, &envelope); err != nil {
		return zero, normalize(err, "Decode")
	}
	dec := json.NewDecoder(bytes.NewReader(envelope.Data))
	dec.UseNumber()
	var value T
	if err = dec.Decode(&value); err != nil {
		return zero, normalize(err, "Decode")
	}
	return value, nil
}
func (r *Run) resolveFinal(ctx context.Context, result Result) (*FinalDelivery, error) {
	if result.Final == nil {
		return nil, nil
	}
	selected := *result.Final
	ref, ok := result.Outputs[selected.Output]
	if selected.Output == "" || !ok {
		return nil, newFailure(ReferenceInvalid, "final-output", "selected final output is missing")
	}
	raw, err := r.resolve(ctx, ref)
	if err != nil {
		return nil, err
	}
	final := &FinalDelivery{Output: selected.Output, Ref: ref}
	if selected.FileID != "" {
		var envelope struct {
			Files []struct{ ID, Kind, Path string }
		}
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return nil, err
		}
		for _, file := range envelope.Files {
			if file.ID == selected.FileID && file.Kind == "artifact" {
				final.ArtifactPath = filepath.Join(filepath.Dir(ref.Path), file.Path)
				break
			}
		}
		if final.ArtifactPath == "" {
			return nil, newFailure(ReferenceInvalid, "final-output", "selected final artifact ID is missing or not an artifact")
		}
	}
	r.mu.Lock()
	attempt := r.state.Attempts[ref.AttemptID]
	final.HandleID, final.Scope, final.Step = attempt.HandleID, attempt.Scope, attempt.Key
	r.mu.Unlock()
	return final, nil
}

func (r *Run) validateResult(ctx context.Context, result Result) (*FinalDelivery, error) {
	for name, ref := range result.Outputs {
		if result.Final != nil && name == result.Final.Output {
			continue
		}
		if _, err := r.resolve(ctx, ref); err != nil {
			return nil, err
		}
	}
	final, err := r.resolveFinal(ctx, result)
	if err != nil {
		return nil, err
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	return final, nil
}
func (r *Run) validateFeedback(ctx context.Context, feedback *Feedback) error {
	if feedback == nil || strings.TrimSpace(feedback.Message) == "" {
		return newFailure(InvalidDefinition, "feedback", "nonempty feedback required")
	}
	if feedback.SourceAttemptID != "" {
		r.mu.Lock()
		_, known := r.state.Attempts[feedback.SourceAttemptID]
		r.mu.Unlock()
		if !known {
			return newFailure(ReferenceInvalid, "feedback", "unknown feedback source attempt")
		}
	}
	for _, ref := range feedback.Refs {
		if _, err := r.resolve(ctx, ref); err != nil {
			return err
		}
	}
	return context.Cause(ctx)
}
