package engine

import (
	"context"
	"fmt"
)

type resourceCleanup struct {
	name  string
	close func(context.Context) error
}

// AddCleanup registers run-owned resources. Call before exposing the resource
// to Pi. Cleanup waits for workflow return and confirmed Pi exit; history is
// never implicitly deleted. A failed registration leaves ownership with caller.
func (r *Run) AddCleanup(ctx context.Context, name string, close func(context.Context) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkLocked(ctx); err != nil {
		return err
	}
	if !validName(name) || close == nil {
		return newFailure(InvalidDefinition, "AddCleanup", "name and cleanup required")
	}
	r.ownerMu.Lock()
	defer r.ownerMu.Unlock()
	if r.resourcesStopped {
		return context.Cause(r.ctx)
	}
	for _, c := range r.resourceCleanups {
		if c.name == name {
			return newFailure(InvalidDefinition, "AddCleanup", "duplicate resource name")
		}
	}
	r.resourceCleanups = append(r.resourceCleanups, resourceCleanup{name, close})
	return nil
}

func (r *Run) cleanupResources(ctx context.Context, handles []*SessionHandle) []error {
	r.ownerMu.Lock()
	cleanups := append([]resourceCleanup(nil), r.resourceCleanups...)
	r.ownerMu.Unlock()
	if len(cleanups) == 0 {
		return nil
	}
	select {
	case <-r.workflowDone:
	case <-ctx.Done():
		return []error{fmt.Errorf("resource cleanup awaits workflow join: %w", context.Cause(ctx))}
	}
	for _, h := range handles {
		if !h.closed.Load() {
			return []error{fmt.Errorf("resources retained: process exit unconfirmed for %s", h.id)}
		}
	}
	var errs []error
	for i := len(cleanups) - 1; i >= 0; i-- {
		c := cleanups[i]
		if err := c.close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("resource %s: %w", c.name, err))
		}
	}
	return errs
}
