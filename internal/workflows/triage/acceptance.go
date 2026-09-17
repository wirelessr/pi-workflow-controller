package triage

import (
	"context"
	"encoding/json"
	"fmt"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
)

// acceptance shares verified envelopes within one synchronous validation pass.
// Do not reuse it after a Step, session operation or Decision. Each new boundary
// reads committed inputs again, including every file's metadata and digest.
// Semantic checks still run for each use; only redundant Store reads are shared.
type acceptance struct {
	ctx          context.Context
	run          *engine.Run
	publications map[contract.Ref]json.RawMessage
}

func newAcceptance(ctx context.Context, r *engine.Run) *acceptance {
	return &acceptance{ctx: ctx, run: r, publications: map[contract.Ref]json.RawMessage{}}
}

func readAccepted[T any](a *acceptance, ref contract.Ref, schema string) (publication[T], error) {
	var p publication[T]
	if ref.SchemaID != schema {
		return p, fmt.Errorf("expected %s", schema)
	}
	if err := context.Cause(a.ctx); err != nil {
		return p, err
	}
	raw, ok := a.publications[ref]
	if !ok {
		var err error
		raw, err = engine.ReadContract(a.ctx, a.run, ref)
		if err != nil {
			return p, err
		}
		a.publications[ref] = raw
	}
	// Decode anew so callers cannot mutate another check's accepted inputs.
	err := json.Unmarshal(raw, &p)
	return p, err
}
