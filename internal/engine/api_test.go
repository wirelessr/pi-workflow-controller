package engine_test

import (
	"context"
	"errors"
	"testing"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

// Zero-value receivers cannot start work or authorize references.
func TestUninitializedOperationsFailClosed(t *testing.T) {
	ctx := context.Background()
	var run engine.Run
	scope := run.Root()
	cases := []struct {
		name string
		call func(t *testing.T) error
	}{
		{"OpenSession", func(t *testing.T) error {
			h, err := run.OpenSession(ctx, engine.RoleSpec{})
			if h != nil {
				t.Fatal("unimplemented operation returned a handle")
			}
			return err
		}},
		{"CloseSession", func(t *testing.T) error { return run.CloseSession(ctx, nil) }},
		{"Child", func(t *testing.T) error {
			child, err := scope.Child("child")
			if child != nil {
				t.Fatal("unimplemented operation returned a child")
			}
			return err
		}},
		{"Step", func(t *testing.T) error {
			result, err := scope.Step(ctx, engine.StepSpec{Key: "step"})
			if result != (engine.StepResult{}) {
				t.Fatal("unimplemented operation returned an attempt or output")
			}
			return err
		}},
		{"Parallel", func(t *testing.T) error {
			results, err := scope.Parallel(ctx, "group", engine.FailFast, []engine.Branch{{
				Name: "branch", Do: func(context.Context, *engine.Scope) (engine.Result, error) {
					t.Fatal("unimplemented Parallel invoked a branch")
					return engine.Result{}, nil
				},
			}})
			if results != nil {
				t.Fatal("unimplemented operation returned branch results")
			}
			return err
		}},
		{"Retry", func(t *testing.T) error {
			result, err := scope.Retry(ctx, "retry", 3, func(context.Context, *engine.Scope, engine.RetryState) (engine.RetryAction, error) {
				t.Fatal("unimplemented Retry invoked its callback")
				return engine.RetryAction{}, nil
			})
			if result.Outputs != nil {
				t.Fatal("unimplemented operation returned outputs")
			}
			return err
		}},
		{"Decision", func(t *testing.T) error { return scope.Decision(ctx, "decision", "reason", nil) }},
		{"Decode", func(t *testing.T) error {
			value, err := engine.Decode[string](ctx, &run, contract.Ref{Path: "/uncommitted/contract.json"})
			if value != "" {
				t.Fatal("unimplemented Decode returned data")
			}
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(t)
			var failure *engine.Failure
			if !errors.As(err, &failure) {
				t.Fatalf("got %v, want typed InvalidDefinition", err)
			}
			if failure.Code != engine.InvalidDefinition || failure.Origin != engine.OriginDefinition || failure.DispatchAccepted != engine.AcceptedNo {
				t.Fatalf("incorrect unimplemented failure: %+v", failure)
			}
		})
	}
}
