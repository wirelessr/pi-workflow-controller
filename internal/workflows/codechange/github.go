package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"pi-workflow-controller/internal/runtime"
)

// pullRequest is the part of a GitHub pull request the receipt check and the
// records read. Pointers tell a missing field from its zero value.
type pullRequest struct {
	Number  int    `json:"number"`
	HTMLURL string `json:"html_url"`
	State   string `json:"state"`
	Draft   *bool  `json:"draft"`
	Merged  *bool  `json:"merged"`
	Base    prSide `json:"base"`
	Head    prSide `json:"head"`
}

type prSide struct {
	Ref  string `json:"ref"`
	SHA  string `json:"sha"`
	Repo *struct {
		FullName string `json:"full_name"`
	} `json:"repo"`
}

// repository is the side's repository, "" when GitHub reports none.
func (s prSide) repository() string {
	if s.Repo == nil {
		return ""
	}
	return s.Repo.FullName
}

// commitSHA is a full commit SHA as git and GitHub write it.
var commitSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// getPullRequest reads pull request number of repository. Its head SHA is
// checked before it is used in a compare path or recorded.
func getPullRequest(ctx context.Context, dir, repository string, number int) (pullRequest, error) {
	raw, err := runtime.GitHubGET(ctx, dir, fmt.Sprintf("repos/%s/pulls/%d", repository, number), false)
	if err != nil {
		return pullRequest{}, fmt.Errorf("pull request GET: %w", err)
	}
	var pr pullRequest
	if json.Unmarshal(raw, &pr) != nil {
		return pullRequest{}, errors.New("pull request GET: not a pull request object")
	}
	if pr.Draft == nil || pr.Merged == nil {
		return pullRequest{}, errors.New("pull request GET: no draft or merged field")
	}
	if !commitSHA.MatchString(pr.Head.SHA) {
		return pullRequest{}, fmt.Errorf("pull request GET: head.sha %q is not a full commit SHA", pr.Head.SHA)
	}
	return pr, nil
}

// githubAncestor reports whether commit is head or one of its ancestors in
// repository on GitHub, with the compare status GitHub reported. Equal
// commits are "identical" without a query; otherwise one compare GET decides:
// commit is an ancestor when head is ahead of it and it is the merge base.
func githubAncestor(ctx context.Context, dir, repository, commit, head string) (bool, string, error) {
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

// remoteBranch reads branch of repository on GitHub: its tip, or "" when the
// branch does not exist.
func remoteBranch(ctx context.Context, dir, repository, branch string) (string, error) {
	segments := strings.Split(branch, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	raw, err := runtime.GitHubGET(ctx, dir, "repos/"+repository+"/git/matching-refs/heads/"+strings.Join(segments, "/"), false)
	if err != nil {
		return "", fmt.Errorf("branch GET: %w", err)
	}
	var refs []struct {
		Ref    string `json:"ref"`
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if json.Unmarshal(raw, &refs) != nil || refs == nil {
		return "", errors.New("branch GET: not a list of refs")
	}
	// matching-refs is a prefix match; only the exact ref is the branch.
	for _, ref := range refs {
		if ref.Ref == "refs/heads/"+branch {
			if ref.Object.SHA == "" {
				return "", errors.New("branch GET: the branch ref has no object")
			}
			return ref.Object.SHA, nil
		}
	}
	return "", nil
}

// branchPulls lists the pull requests in any state whose head is branch of
// repository, one page of at most 100.
func branchPulls(ctx context.Context, dir, repository, branch string) ([]pullRequest, error) {
	owner, _, _ := strings.Cut(repository, "/")
	query := url.Values{"head": {owner + ":" + branch}, "state": {"all"}, "per_page": {"100"}}
	raw, err := runtime.GitHubGET(ctx, dir, "repos/"+repository+"/pulls?"+query.Encode(), false)
	if err != nil {
		return nil, fmt.Errorf("pull request list GET: %w", err)
	}
	var pulls []pullRequest
	if json.Unmarshal(raw, &pulls) != nil || pulls == nil {
		return nil, errors.New("pull request list GET: not a list of pull requests")
	}
	return pulls, nil
}
