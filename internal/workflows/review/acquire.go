package review

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"pi-workflow-controller/internal/gitrepo"
	"pi-workflow-controller/internal/runtime"
)

// Checkout is a pinned PR acquisition, not a view of a caller's local repository.
// Missing contains unavailable source/content keys, including external gitlink
// content. Missing content is never represented by an empty successful snapshot.
type Checkout struct {
	URL, Repository                                             string
	Number                                                      int
	BaseSHA, HeadSHA, MergeBase, DiffRange, ContextID, Worktree string
	Snapshots                                                   map[string]string
	Missing                                                     []string

	repo           *gitrepo.Repo
	snapshotHashes map[string][32]byte
	gitOutput      func(context.Context, string, []string) ([]byte, error)
}

var (
	prPath     = regexp.MustCompile(`^/([A-Za-z0-9][A-Za-z0-9-]*)/([A-Za-z0-9_.-]+)/pull/([1-9][0-9]*)/?$`)
	shaPattern = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
)

// ParsePRURL accepts only a literal github.com HTTPS pull-request URL.
func ParsePRURL(raw string) (repository string, number int, err error) {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(raw, "#%") {
		return "", 0, errors.New("invalid GitHub PR URL")
	}
	m := prPath.FindStringSubmatch(u.Path)
	if m == nil || m[2] == "." || m[2] == ".." {
		return "", 0, errors.New("invalid GitHub PR path")
	}
	n, e := strconv.Atoi(m[3])
	if e != nil || n <= 0 {
		return "", 0, errors.New("invalid GitHub PR number")
	}
	return m[1] + "/" + m[2], n, nil
}

// acquisitionSource selects the Git remotes and an optional external Git boundary.
// A nil gitOutput preserves real Git; acquisition, parsing, ownership and Verify
// remain production paths. Configure callbacks before use, not concurrently.
type acquisitionSource struct {
	remote    func(string) string
	gitOutput func(context.Context, string, []string) ([]byte, error)
}

func Acquire(ctx context.Context, root, prURL string) (*Checkout, error) {
	return acquire(ctx, root, prURL, acquisitionSource{
		remote: func(repository string) string { return "https://github.com/" + repository + ".git" },
	})
}

type prMetadata struct {
	Number int        `json:"number"`
	URL    string     `json:"html_url"`
	Base   prRevision `json:"base"`
	Head   prRevision `json:"head"`
}
type prRevision struct {
	SHA  string `json:"sha"`
	Ref  string `json:"ref"`
	Repo struct {
		FullName string `json:"full_name"`
	} `json:"repo"`
}

func acquire(ctx context.Context, root, prURL string, source acquisitionSource) (result *Checkout, err error) {
	repository, number, err := ParsePRURL(prURL)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	// Reserve all paths before starting children, so an existing repository or
	// another run can never become an implicit acquisition target.
	repo, err := gitrepo.Reserve(root)
	if err != nil {
		return nil, err
	}
	root = repo.Root()
	c := &Checkout{URL: fmt.Sprintf("https://github.com/%s/pull/%d", repository, number), Repository: repository, Number: number,
		repo: repo, Snapshots: make(map[string]string), snapshotHashes: make(map[string][32]byte),
		gitOutput: source.gitOutput}
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if cleanupErr := c.Cleanup(cleanupCtx); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("acquisition cleanup: %w", cleanupErr))
			}
		}
	}()
	if err = os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("repos/%s/pulls/%d", repository, number)
	metadata, err := api(ctx, root, endpoint, false)
	if err != nil {
		return nil, fmt.Errorf("PR metadata: %w", err)
	}
	var pr prMetadata
	if json.Unmarshal(metadata, &pr) != nil {
		return nil, errors.New("invalid PR metadata JSON")
	}
	metaRepo, metaNumber, parseErr := ParsePRURL(pr.URL)
	if parseErr != nil || !strings.EqualFold(metaRepo, repository) || metaNumber != number || pr.Number != number || !strings.EqualFold(pr.Base.Repo.FullName, repository) {
		return nil, errors.New("PR metadata identity mismatch")
	}
	for _, revision := range []prRevision{pr.Base, pr.Head} {
		validated, _, e := ParsePRURL("https://github.com/" + revision.Repo.FullName + "/pull/1")
		if e != nil || validated != revision.Repo.FullName || revision.Ref == "" || !shaPattern.MatchString(revision.SHA) {
			return nil, errors.New("invalid PR revision metadata")
		}
	}
	c.BaseSHA, c.HeadSHA = strings.ToLower(pr.Base.SHA), strings.ToLower(pr.Head.SHA)
	if err = c.snapshot("metadata", "json", metadata); err != nil {
		return nil, err
	}

	if err = repo.Init(ctx, c.git); err != nil {
		return nil, err
	}
	for _, pin := range []struct{ repository, sha, ref string }{
		{pr.Base.Repo.FullName, c.BaseSHA, "refs/review/base"},
		{pr.Head.Repo.FullName, c.HeadSHA, "refs/review/head"},
	} {
		// Keep a promisor remote for lazy blob reads during the full checkout;
		// fetch only pinned commits, never mutable branches or refs/pull/*.
		remote := strings.TrimPrefix(pin.ref, "refs/review/")
		if err = repo.AddRemote(ctx, c.git, remote, source.remote(pin.repository)); err != nil {
			return nil, err
		}
		if err = repo.FetchPinned(ctx, c.git, remote, pin.sha, pin.ref); err != nil {
			return nil, err
		}
	}
	mb, err := c.git(ctx, "merge-base", "--all", c.BaseSHA, c.HeadSHA)
	if err != nil {
		return nil, fmt.Errorf("merge-base: %w", err)
	}
	bases := strings.Fields(string(mb))
	if len(bases) != 1 || !shaPattern.MatchString(bases[0]) {
		return nil, errors.New("missing or ambiguous merge-base")
	}
	c.MergeBase = strings.ToLower(bases[0])
	c.DiffRange = c.MergeBase + ".." + c.HeadSHA
	id := sha256.Sum256([]byte(fmt.Sprintf("%s\n%d\n%s\n%s\n%s", strings.ToLower(repository), number, c.BaseSHA, c.HeadSHA, c.MergeBase)))
	c.ContextID = hex.EncodeToString(id[:])

	// Preserve gitlinks as pinned evidence, but never execute their transports.
	// Missing external content is explicit review uncertainty, not empty code.
	tree, err := c.git(ctx, "ls-tree", "-r", "-z", c.HeadSHA)
	if err != nil {
		return nil, err
	}
	var submodules []string
	for _, entry := range bytes.Split(tree, []byte{0}) {
		if bytes.HasPrefix(entry, []byte("160000 ")) {
			submodules = append(submodules, string(entry))
		}
	}
	if len(submodules) > 0 {
		data, e := json.Marshal(submodules)
		if e != nil {
			return nil, e
		}
		if err = c.snapshot("submodules", "json", data); err != nil {
			return nil, err
		}
		c.Missing = append(c.Missing, "submodule-content")
	}
	if _, err = c.git(ctx, "worktree", "add", "--detach", repo.Worktree(), c.HeadSHA); err != nil {
		return nil, err
	}
	c.Worktree = repo.Worktree()
	if diff, e := c.git(ctx, "diff", "--binary", "--no-ext-diff", "--no-textconv", c.MergeBase, c.HeadSHA, "--"); e != nil {
		return nil, e
	} else if err = c.snapshot("diff", "diff", diff); err != nil {
		return nil, err
	}
	names, err := c.git(ctx, "diff", "--name-only", "-z", "--no-ext-diff", "--no-textconv", c.MergeBase, c.HeadSHA, "--")
	if err != nil {
		return nil, err
	}
	files := []string{}
	if len(names) > 0 {
		if names[len(names)-1] != 0 {
			return nil, errors.New("changed-files is not NUL terminated")
		}
		for _, name := range bytes.Split(names[:len(names)-1], []byte{0}) {
			if !utf8.Valid(name) {
				return nil, errors.New("changed-files contains a non-UTF-8 path")
			}
			files = append(files, string(name))
		}
	}
	encoded, err := json.Marshal(files)
	if err != nil {
		return nil, err
	}
	if err = c.snapshot("changed-files", "json", encoded); err != nil {
		return nil, err
	}

	for _, comments := range []struct{ key, endpoint string }{
		{"issues", fmt.Sprintf("repos/%s/issues/%d/comments?per_page=100", repository, number)},
		{"inline", endpoint + "/comments?per_page=100"},
		{"reviews", endpoint + "/reviews?per_page=100"},
	} {
		data, e := api(ctx, root, comments.endpoint, true)
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if e != nil {
			c.Missing = append(c.Missing, comments.key)
			continue
		}
		if err = c.snapshot(comments.key, "json", data); err != nil {
			return nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	return c, nil
}

func api(ctx context.Context, root, endpoint string, paginate bool) ([]byte, error) {
	data, err := runtime.GitHubGET(ctx, root, endpoint, paginate)
	if err != nil || !paginate {
		return data, err
	}
	var pages []json.RawMessage
	if json.Unmarshal(data, &pages) != nil || pages == nil || len(pages) == 0 {
		return nil, errors.New("invalid comment pagination")
	}
	items := []json.RawMessage{}
	for _, page := range pages {
		var values []json.RawMessage
		if json.Unmarshal(page, &values) != nil || values == nil {
			return nil, errors.New("invalid comments page")
		}
		for _, value := range values {
			if len(value) == 0 || value[0] != '{' {
				return nil, errors.New("invalid comment object")
			}
		}
		items = append(items, values...)
	}
	return json.Marshal(items)
}

func (c *Checkout) snapshot(key, extension string, data []byte) error {
	path := filepath.Join(c.repo.Root(), "snapshots", key+"."+extension)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if err = errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	c.Snapshots[key] = path
	c.snapshotHashes[key] = sha256.Sum256(data)
	return nil
}

// Cleanup removes only the originally reserved worktree and its registration.
// Public fields are descriptive: changing Worktree never changes deletion scope.
// The private repository and snapshots remain available as acquisition history.
func (c *Checkout) Cleanup(ctx context.Context) error {
	if c.repo == nil {
		return nil
	}
	return c.repo.Cleanup(ctx)
}

func (c *Checkout) git(ctx context.Context, args ...string) ([]byte, error) {
	fixed := []string{
		"--no-pager", "--git-dir=" + c.repo.Dir(),
		"-c", "core.hooksPath=/dev/null", "-c", "core.attributesFile=/dev/null",
		"-c", "core.autocrlf=false", "-c", "core.sparseCheckout=false",
		"-c", "http.sslVerify=true", "-c", "http.followRedirects=false",
		"-c", "protocol.allow=never", "-c", "protocol.https.allow=always", "-c", "protocol.http.allow=always",
		"-c", "submodule.recurse=false",
	}
	command := append(fixed, args...)
	var data []byte
	var err error
	if c.gitOutput == nil {
		data, err = runtime.Git(ctx, c.repo.Root(), runtime.GitConfigIsolation(), command...)
	} else {
		data, err = c.gitOutput(ctx, c.repo.Root(), command)
	}
	if err != nil {
		return nil, fmt.Errorf("git %s: %w", args[0], err)
	}
	return data, nil
}
