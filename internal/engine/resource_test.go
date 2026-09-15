package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pi-workflow-controller/internal/runtime"
)

type resourceRuntime struct {
	*engTestRuntime
	unconfirmed bool
}
type resourceSession struct {
	runtime.Session
	unconfirmed bool
}

func (r resourceRuntime) Start(ctx context.Context, s runtime.SessionSpec) (runtime.Session, error) {
	v, e := r.engTestRuntime.Start(ctx, s)
	if v == nil {
		return nil, e
	}
	return resourceSession{v, r.unconfirmed}, e
}
func (s resourceSession) Close(ctx context.Context) (runtime.CleanupReport, error) {
	v, e := s.Session.Close(ctx)
	if s.unconfirmed {
		v.ProcessExited = false
	}
	return v, e
}

func TestOwnedResourceCleanup(t *testing.T) {
	for _, name := range []string{"success", "cancel", "unconfirmed-exit", "cleanup-error", "blocked-journal"} {
		t.Run(name, func(t *testing.T) {
			pi := &engTestRuntime{}
			ready := make(chan struct{})
			removed := make(chan struct{})
			joined := make(chan struct{})
			var dir string
			def := Definition{Name: "resources", Version: "1", Policy: DefaultRunPolicy(), Execute: func(ctx context.Context, r *Run, _ Input) (Result, error) {
				dir = filepath.Join(r.Dir(), "owned-worktree")
				if e := os.Mkdir(dir, 0700); e != nil {
					return Result{}, e
				}
				e := r.AddCleanup(ctx, "worktree", func(ctx context.Context) error {
					select {
					case <-joined:
					default:
						t.Error("resource cleanup preceded workflow join")
					}
					for _, s := range pi.sessions {
						s.mu.Lock()
						n := s.closes
						s.mu.Unlock()
						if n != 1 {
							t.Errorf("resource cleanup preceded Pi close: %d", n)
						}
					}
					defer close(removed)
					if name == "cleanup-error" {
						return os.ErrPermission
					}
					return os.Remove(dir)
				})
				if e != nil {
					return Result{}, e
				}
				if e := r.AddCleanup(ctx, "worktree", func(context.Context) error { return nil }); !engTestCode(e, InvalidDefinition) {
					t.Errorf("duplicate cleanup: %v", e)
				}
				_, e = r.OpenSession(ctx, engTestRole("worker"))
				if e != nil {
					return Result{}, e
				}
				close(ready)
				if name == "cancel" {
					<-ctx.Done()
					close(joined)
					return Result{}, context.Cause(ctx)
				}
				close(joined)
				return Result{}, nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			r, e := New(ctx, def, Input{Prompt: "resources", LaunchCWD: t.TempDir()}, Options{BaseDir: t.TempDir(), Schemas: engTestSchemas(t), Runtime: resourceRuntime{pi, name == "unconfirmed-exit"}})
			if e != nil {
				t.Fatal(e)
			}
			if name == "blocked-journal" {
				r.beforeIO = func(path, phase string) {
					if phase == "RunFinalizing" {
						select {
						case <-removed:
						case <-ctx.Done():
							t.Error("resource cleanup waited for journal")
						}
					}
				}
			}
			done := engTestExecuteAsync(t, r)
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal("start barrier")
			}
			if name == "cancel" {
				r.Cancel(OriginControllerUser)
			}
			var report Report
			select {
			case report = <-done:
			case <-ctx.Done():
				t.Fatal("cleanup join")
			}
			retained := name == "unconfirmed-exit" || name == "cleanup-error"
			_, statErr := os.Stat(dir)
			if retained && statErr != nil || !retained && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("resource retention: %v", statErr)
			}
			if retained {
				if report.ExitCode != 1 || len(report.CleanupErrors) == 0 || report.Outcome != Succeeded {
					t.Fatalf("cleanup failure lost: %+v", report)
				}
			} else if name == "cancel" {
				if report.ExitCode != 130 || report.Outcome != CancelledState {
					t.Fatalf("cancel lost: %+v", report)
				}
			} else if report.ExitCode != 0 {
				t.Fatalf("cleanup: %+v", report)
			}
		})
	}
}
