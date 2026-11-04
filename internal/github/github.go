// Package github interprets read-only GitHub REST responses that more than
// one workflow needs. Commands run through runtime.GitHubGET; policy on what a
// result means for a workflow stays with the caller.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"pi-workflow-controller/internal/runtime"
)

// commitSHA is a full commit SHA as git and GitHub write it.
var commitSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// Ancestor reports whether commit is head or one of its ancestors in
// repository on GitHub, with the compare status GitHub reported. Equal
// commits are "identical" without a query; otherwise one compare GET in dir
// decides: commit is an ancestor when head is ahead of it and it is the merge
// base. commit and head must be full lowercase SHAs, since they go into the
// endpoint path; anything else is rejected before gh starts. The caller
// guarantees that repository is a valid owner/name.
func Ancestor(ctx context.Context, dir, repository, commit, head string) (bool, string, error) {
	for _, sha := range []string{commit, head} {
		if !commitSHA.MatchString(sha) {
			return false, "", fmt.Errorf("compare: %q is not a full lowercase commit SHA", sha)
		}
	}
	if commit == head {
		return true, "identical", nil
	}
	raw, err := runtime.GitHubGET(ctx, dir, "repos/"+repository+"/compare/"+commit+"..."+head, false)
	if err != nil {
		return false, "", fmt.Errorf("compare GET: %w", err)
	}
	var c struct {
		Status    string `json:"status"`
		MergeBase struct {
			SHA string `json:"sha"`
		} `json:"merge_base_commit"`
	}
	if json.Unmarshal(raw, &c) != nil {
		return false, "", errors.New("compare GET: not a compare object")
	}
	switch c.Status {
	case "ahead", "behind", "diverged", "identical":
	default:
		return false, "", fmt.Errorf("compare GET: status %q is not ahead, behind, diverged or identical", c.Status)
	}
	return c.Status == "ahead" && c.MergeBase.SHA == commit, c.Status, nil
}
