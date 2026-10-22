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
// it is checked before any side effect.
func Definition(piCWD string) engine.Definition {
	p := engine.DefaultRunPolicy()
	p.RunTimeout = 8 * time.Hour
	p.MaxLiveSessions, p.MaxTotalSessions, p.MaxTotalAttempts = 3, 22, 80
	return engine.Definition{Name: "code-change", Description: "Implement an approved design doc in a Controller-owned worktree; currently prepares the worktree and records the input only", Version: "1", Policy: p,
		Execute: func(ctx context.Context, r *engine.Run, in engine.Input) (engine.Result, error) {
			return execute(ctx, r, in, piCWD, gitSource{remote: githubRemote})
		}}
}

func githubRemote(repository string) string { return "https://github.com/" + repository + ".git" }

// prepared is what the Controller hands to the first Agent Step.
type prepared struct {
	input    contract.Ref
	worktree *worktree
}

func execute(ctx context.Context, r *engine.Run, in engine.Input, piCWD string, source gitSource) (engine.Result, error) {
	p, err := prepare(ctx, r, in, piCWD, source)
	if err != nil {
		return engine.Result{}, err
	}
	return engine.Result{Outputs: map[string]contract.Ref{"input": p.input}, Final: &engine.FinalSelection{Output: "input"}}, nil
}

// prepare checks the prompt, PWC_PI_CWD, Node TLS and the design doc before
// any network call, then the repository with one GET, and writes nothing
// before that GET succeeds. It then creates and registers the worktree and
// records the input.
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
	return prepared{input: ref, worktree: wt}, nil
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
