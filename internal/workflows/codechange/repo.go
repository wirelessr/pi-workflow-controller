package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"pi-workflow-controller/internal/runtime"
)

type repositoryInfo struct {
	FullName      string `json:"full_name"`
	DefaultBranch string `json:"default_branch"`
	Permissions   *struct {
		Push *bool `json:"push"`
	} `json:"permissions"`
}

// repositoryMetadata confirms with one GET that the repository is reachable
// under the requested name and that the authenticated identity may push to
// it, and returns its default branch. A renamed or transferred repository is
// rejected rather than followed, and a missing permission field is never
// read as permission.
func repositoryMetadata(ctx context.Context, dir, repository string) (repositoryInfo, error) {
	raw, err := runtime.GitHubGET(ctx, dir, "repos/"+repository, false)
	if err != nil {
		return repositoryInfo{}, fmt.Errorf("repository metadata: %w", err)
	}
	var info repositoryInfo
	if json.Unmarshal(raw, &info) != nil {
		return repositoryInfo{}, errors.New("repository metadata is not a JSON object")
	}
	if !strings.EqualFold(info.FullName, repository) {
		return repositoryInfo{}, fmt.Errorf("repository metadata full_name is %q, want %q", info.FullName, repository)
	}
	if info.Permissions == nil || info.Permissions.Push == nil {
		return repositoryInfo{}, fmt.Errorf("repository metadata for %q has no permissions.push field", repository)
	}
	if !*info.Permissions.Push {
		return repositoryInfo{}, fmt.Errorf("the authenticated identity has no push permission on %q", repository)
	}
	if info.DefaultBranch == "" {
		return repositoryInfo{}, fmt.Errorf("repository metadata for %q has no default_branch", repository)
	}
	return info, nil
}
