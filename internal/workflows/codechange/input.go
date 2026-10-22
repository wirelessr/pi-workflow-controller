package codechange

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	"pi-workflow-controller/internal/contract"
)

// designDocLimit bounds the snapshot; the doc is read whole into memory and
// committed as evidence.
const designDocLimit = 1 << 20

var repositoryPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9_.-]+$`)

// parsePrompt splits "<owner/name> <design doc path>": the first token is the
// repository, the rest after the separating whitespace is the path, which may
// itself contain spaces.
func parsePrompt(prompt string) (repository, path string, err error) {
	prompt = strings.TrimSpace(prompt)
	i := strings.IndexFunc(prompt, unicode.IsSpace)
	if i < 0 {
		return "", "", errors.New(`code-change prompt must be "<owner/name> <design doc path>"`)
	}
	repository, path = prompt[:i], strings.TrimLeftFunc(prompt[i:], unicode.IsSpace)
	name := repository[strings.IndexByte(repository, '/')+1:]
	if !repositoryPattern.MatchString(repository) || name == "." || name == ".." {
		return "", "", fmt.Errorf("code-change prompt must start with a GitHub repository owner/name, got %q", repository)
	}
	return repository, path, nil
}

// An unset PWC_PI_CWD makes Pi fall back to the launch directory, which may be
// a real checkout where relative edits would land instead of the worktree.
func checkPiCWD(piCWD string) error {
	if !filepath.IsAbs(piCWD) {
		return fmt.Errorf("code-change requires PWC_PI_CWD to be an absolute path, got %q", piCWD)
	}
	info, err := os.Stat(piCWD)
	if err != nil {
		return fmt.Errorf("code-change PWC_PI_CWD: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("code-change requires PWC_PI_CWD to be a directory, got %q", piCWD)
	}
	return nil
}

type designDoc struct {
	resolved string
	data     []byte
}

// readDesignDoc reads the doc at path, relative to launchCWD unless absolute,
// without expanding "~". Directories above it may be symlinks; the doc itself
// must be a regular file, not a symlink, of at most designDocLimit bytes, and
// must not change while it is read.
func readDesignDoc(ctx context.Context, launchCWD, path string) (designDoc, error) {
	resolved := path
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(launchCWD, resolved)
	}
	resolved = filepath.Clean(resolved)
	dir, name := filepath.Split(resolved)
	if name == "" {
		return designDoc{}, fmt.Errorf("design doc %q: not a file path", path)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return designDoc{}, fmt.Errorf("design doc %q: %w", path, err)
	}
	defer func(root *os.Root) { _ = root.Close() }(root)
	data, err := contract.ReadStable(ctx, root, name, designDocLimit)
	if err != nil {
		return designDoc{}, fmt.Errorf("design doc %q: %w", path, err)
	}
	return designDoc{resolved: resolved, data: data}, nil
}
