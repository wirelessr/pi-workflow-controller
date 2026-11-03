// Package codechange implements a workflow that turns an approved design doc
// into changes in a Controller-owned worktree of the target repository.
package codechange

import (
	"context"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// Definition is the code-change workflow. piCWD is the raw PWC_PI_CWD value;
// it is checked before any side effect and is every Agent's CWD.
func Definition(piCWD string) engine.Definition {
	p := engine.DefaultRunPolicy()
	p.RunTimeout = 8 * time.Hour
	p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = 3, 22, 80
	// The deadline, shared with closing sessions, for the run's cleanup to
	// remove a successful run's worktree; a removal under way is not
	// interrupted. The value awaits live data.
	p.Runtime.CleanupTimeout = 2 * time.Minute
	return engine.Definition{Name: "code-change", Description: "Implement an approved design doc as local commits in a Controller-owned worktree, have two reviewers check them, then correct the work, push one branch and open a pull request; this version stops after the pull request is opened, with no post-PR review, triage, convergence or final cleanup", Version: "1", Policy: p,
		Execute: func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
			return execute(ctx, r, in, piCWD, gitSource{remote: githubRemote})
		}}
}

func githubRemote(repository string) string { return "https://github.com/" + repository + ".git" }

// prepared is what the Controller hands to the first Agent Step.
type prepared struct {
	input         contract.Ref
	repository    string
	defaultBranch string
	worktree      *worktree
}

func execute(ctx context.Context, r *engine.Run, in engine.Input, piCWD string, source gitSource) (engine.Result, error) {
	p, err := prepare(ctx, r, in, piCWD, source)
	if err != nil {
		return engine.Result{}, err
	}
	result, hand, err := implement(ctx, r, p, piCWD)
	if err != nil {
		return result, err
	}
	if result, err = preReview(ctx, r, p, piCWD, result, hand.head); err != nil {
		return result, err
	}
	if result, err = publish(ctx, r, p, hand, result); err != nil {
		return result, err
	}
	// Only here, with the implementer's session confirmed closed and the
	// HEAD still the head verified on the pull request, may the run's
	// cleanup remove the worktree.
	p.worktree.remove.Store(true)
	return result, nil
}

// prepare checks the prompt, PWC_PI_CWD, Node TLS and the design doc before
// any network call, then the repository with one GET, and writes nothing
// before that GET succeeds. It then checks git, creates and registers the
// worktree and records the input.
func prepare(ctx context.Context, r *engine.Run, in engine.Input, piCWD string, source gitSource) (prepared, error) {
	repository, docPath, err := parsePrompt(in.Prompt)
	if err != nil {
		return prepared{}, err
	}
	if err := checkPiCWD(piCWD); err != nil {
		return prepared{}, err
	}
	if err := runtime.CheckNodeTLS(); err != nil {
		return prepared{}, err
	}
	doc, err := readDesignDoc(ctx, in.LaunchCWD, docPath)
	if err != nil {
		return prepared{}, err
	}
	meta, err := repositoryMetadata(ctx, r.Dir(), repository)
	if err != nil {
		return prepared{}, err
	}
	if err := checkGit(ctx, r.Dir()); err != nil {
		return prepared{}, err
	}
	wt, err := createWorktree(ctx, r, source, repository, meta.DefaultBranch)
	if err != nil {
		return prepared{}, err
	}
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "input", Output: contract.Spec{SchemaID: InputSchema},
		Data: inputRecord{Repository: repository, DefaultBranch: meta.DefaultBranch, BaseSHA: wt.base, Worktree: wt.repo.Worktree(),
			DesignDoc: designDocRecord{Path: docPath, Resolved: doc.resolved}},
		Files: []contract.ControllerFile{{ID: "design-doc", Path: "evidence/design-doc", Data: doc.data}}})
	if err != nil {
		return prepared{}, err
	}
	return prepared{input: ref, repository: repository, defaultBranch: meta.DefaultBranch, worktree: wt}, nil
}

type inputRecord struct {
	Repository    string          `json:"repository"`
	DefaultBranch string          `json:"default_branch"`
	BaseSHA       string          `json:"base_sha"`
	Worktree      string          `json:"worktree"`
	DesignDoc     designDocRecord `json:"design_doc"`
}

type designDocRecord struct {
	Path     string `json:"path"`
	Resolved string `json:"resolved"`
}
