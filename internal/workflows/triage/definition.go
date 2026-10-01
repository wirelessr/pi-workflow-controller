package triage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

const workflowName = "jira-triage"

type workflowInput struct {
	Scope   Scope  `json:"scope"`
	Request string `json:"request"`
}

// Definition keeps the initial product choices together, separate from the
// explicit policies accepted by the private investigation entry points.
func Definition() engine.Definition {
	planner := runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/glm-5p3", Thinking: "high"}
	models := sliceModels{FetchThinking: "off", Analysis: runtime.ModelSpec{Provider: "fireworks", ID: "accounts/fireworks/models/glm-5p3-flash", Thinking: "high"}}
	capacity := PlannerCapacityPolicy{HandoffPercent: 80}
	recovery := RecoveryPolicy{PlannerRetries: 1}
	verification := VerificationPolicy{
		Pro:   VerifierPolicy{Model: planner, Retries: 1},
		Con:   VerifierPolicy{Model: planner, Retries: 1},
		Cross: VerifierPolicy{Model: planner, Retries: 1},
	}
	report := ReportPolicy{ReportRetries: 1, ReserveSessions: 1, ReserveAttempts: 2}
	return engine.Definition{Name: workflowName, Description: "Investigate an explicitly scoped Jira request and deliver a verified report artifact", Version: "1", Policy: slicePolicy(), Execute: func(ctx context.Context, r *engine.Run, input engine.Input) (engine.Result, error) {
		v, err := decodeWorkflowInput(input.Prompt)
		if err != nil {
			return engine.Result{}, err
		}
		contextResult, err := executeSlice(ctx, r, v.Scope, models)
		if err != nil {
			return engine.Result{}, err
		}
		renderer, err := ExtractReport(r.Dir())
		if err != nil {
			return engine.Result{}, err
		}
		return executeInvestigationReport(ctx, r, v.Scope, planner, models, contextResult.Context, &capacity, &recovery, &verification, report, renderer)
	}}
}

func decodeWorkflowInput(prompt string) (workflowInput, error) {
	// An omitted tenant set authorizes nobody. Keep the empty array shape
	// required by Context, without requiring a runtime target at intake.
	v := workflowInput{Scope: Scope{TenantIDs: []string{}}}
	var scope json.RawMessage
	if err := decodeInputObject([]byte(prompt), map[string]any{"scope": &scope, "request": &v.Request}); err != nil {
		return v, fmt.Errorf("triage input: %w", err)
	}
	if err := decodeInputObject(scope, map[string]any{"ticket": &v.Scope.Ticket, "stack": &v.Scope.Stack, "pop": &v.Scope.Pop, "binding": &v.Scope.Binding, "tenant_ids": &v.Scope.TenantIDs}); err != nil {
		return v, fmt.Errorf("triage scope: %w", err)
	}
	if !nonblank(v.Request) || !jiraKey.MatchString(v.Scope.Ticket) || !texts(v.Scope.TenantIDs) {
		return v, fmt.Errorf("triage requires a nonblank request, explicit ticket and valid authorized target scope")
	}
	return v, nil
}

// The envelope and scope are fixed objects, not an instruction or domain parser.
// encoding/json alone accepts duplicate keys and case-insensitive field names.
func decodeInputObject(raw []byte, fields map[string]any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return fmt.Errorf("expected JSON object")
	}
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		target, exists := fields[key]
		if !ok || !exists || seen[key] {
			return fmt.Errorf("unknown or duplicate input field %q", key)
		}
		seen[key] = true
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return err
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("null input field %q", key)
		}
		if err := json.Unmarshal(value, target); err != nil {
			return err
		}
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("expected a single JSON object")
	}
	return nil
}

// Run.Input is already persisted by the engine. Only the registered product
// envelope has this contract; legacy slice callers keep their original inputs.
func callerRequest(r *engine.Run) (string, error) {
	workflow, input := r.WorkflowInput()
	if workflow != workflowName {
		return "", nil
	}
	v, err := decodeWorkflowInput(input.Prompt)
	return v.Request, err
}
