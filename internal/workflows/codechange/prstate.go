package codechange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"pi-workflow-controller/internal/contract"
	"pi-workflow-controller/internal/engine"
	"pi-workflow-controller/internal/runtime"
)

// statePolicy bounds the pull request state wait and reads: wait caps the
// whole wait, settle included; settle is how long every poll must find all
// check runs and commit statuses terminal on the same head; poll is the
// interval between polls; reads bounds the snapshot's reads, not its Attach.
type statePolicy struct {
	wait, settle, poll, reads time.Duration
}

// prStatePolicy is the production policy; every value awaits live data.
var prStatePolicy = statePolicy{wait: 30 * time.Minute, settle: 3 * time.Minute, poll: 30 * time.Second, reads: 10 * time.Minute}

// errStateRead is a pull request state read the snapshot needs that failed
// or returned something the Controller cannot read in full. No partial
// snapshot is recorded.
var errStateRead = errors.New("code-change cannot read the pull request state")

var errReadsCap = errors.New("the pull request state reads exceeded their bound")

// reviewThreadsQuery is a Controller constant and a query operation, which
// is what keeps the GraphQL read read-only.
const reviewThreadsQuery = `query($owner: String!, $name: String!, $number: Int!, $endCursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviewThreads(first: 100, after: $endCursor) {
        totalCount
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          comments(first: 1) { nodes { url } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`

// prStateRecord is the pull request state as read-only reads showed it:
// identities, states, locations and links, copied as GitHub gave them.
// A field the record cannot hold as null that GitHub's answer lacks is a
// read failure.
type prStateRecord struct {
	Wait              waitObserved       `json:"wait"`
	ObservedAt        string             `json:"observed_at"`
	PullRequest       pullStateObserved  `json:"pull_request"`
	CheckRuns         []checkRunObserved `json:"check_runs"`
	Statuses          []statusObserved   `json:"statuses"`
	UnresolvedThreads []threadObserved   `json:"unresolved_threads"`
}

type waitObserved struct {
	Outcome       string `json:"outcome"`
	StartedAt     string `json:"started_at"`
	EndedAt       string `json:"ended_at"`
	Polls         int    `json:"polls"`
	FailedPolls   int    `json:"failed_polls"`
	LastPollError string `json:"last_poll_error,omitempty"`
	// LastPolledHead is the head of the last poll that read the state in
	// full; a settled wait settled on it.
	LastPolledHead string `json:"last_polled_head,omitempty"`
}

func (w waitObserved) summary() string {
	s := fmt.Sprintf("the wait before it %s after %d polls, %d failed", w.Outcome, w.Polls, w.FailedPolls)
	if w.LastPollError != "" {
		s += ", the last with: " + w.LastPollError
	}
	return s
}

type pullStateObserved struct {
	Number  int    `json:"number"`
	URL     string `json:"url"`
	State   string `json:"state"`
	Draft   bool   `json:"draft"`
	Merged  bool   `json:"merged"`
	HeadSHA string `json:"head_sha"`
	HeadRef string `json:"head_ref"`
	BaseRef string `json:"base_ref"`
}

type checkRunObserved struct {
	ID         int64   `json:"id"`
	Name       string  `json:"name"`
	App        *string `json:"app"`
	Status     string  `json:"status"`
	Conclusion *string `json:"conclusion"`
	HTMLURL    *string `json:"html_url"`
	DetailsURL *string `json:"details_url"`
}

type statusObserved struct {
	Context   string  `json:"context"`
	State     string  `json:"state"`
	TargetURL *string `json:"target_url"`
}

type threadObserved struct {
	ID       string  `json:"id"`
	Path     string  `json:"path"`
	Line     *int    `json:"line"`
	Outdated bool    `json:"outdated"`
	URL      *string `json:"url"`
}

// terminal is the wait's only test of the state: every check run completed
// and every commit status success, failure or error. A conclusion is never
// read; any other value, an unknown one included, is not terminal.
func terminal(runs []checkRunObserved, statuses []statusObserved) bool {
	for _, run := range runs {
		if run.Status != "completed" {
			return false
		}
	}
	for _, s := range statuses {
		if s.State != "success" && s.State != "failure" && s.State != "error" {
			return false
		}
	}
	return true
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// finalState waits for the state of the pull request of the committed
// pull-request record, then reads it once and attaches it as final-snapshot,
// keeping result's Final. A wait that reached its cap is recorded, not an
// error. A failed read wraps errStateRead, adds a summary of the wait so a
// long failed wait is not lost, and attaches nothing; the run's
// cancellation, deadline or fatal error is returned as its cause.
func (p prepared) finalState(ctx context.Context, r *engine.Run, result engine.Result, sp statePolicy) (engine.Result, error) {
	pr, err := engine.Decode[pullRequestRecord](ctx, r, result.Outputs["pull-request"])
	if err != nil {
		return result, err
	}
	wait, err := p.awaitState(ctx, r.Dir(), pr.Number, sp)
	if err != nil {
		return result, err
	}
	record, err := p.readState(ctx, r.Dir(), pr.Number, sp)
	if errors.Is(err, errStateRead) {
		return result, fmt.Errorf("%w; %s", err, wait.summary())
	}
	if err != nil {
		return result, err
	}
	record.Wait = wait
	ref, err := r.Root().Attach(ctx, engine.AttachSpec{Key: "final-snapshot", Output: contract.Spec{SchemaID: PRStateSchema}, Data: record})
	if err != nil {
		return result, err
	}
	result.Outputs["final-snapshot"] = ref
	return result, nil
}

// awaitState polls the pull request's head, check runs and commit statuses
// until every poll for settle found them terminal on the same head, or until
// the wait cap. A failed poll restarts the settle window and is counted with
// its error kept; it does not end the wait.
func (p prepared) awaitState(ctx context.Context, dir string, number int, sp statePolicy) (waitObserved, error) {
	start := time.Now()
	end := start.Add(sp.wait)
	w := waitObserved{StartedAt: stamp(start), Outcome: "timed_out"}
	capped, cancel := context.WithDeadline(ctx, end)
	defer cancel()
	var stableSince time.Time
	var stableHead string
	for {
		head, done, err := p.pollState(capped, dir, number)
		if cause := context.Cause(ctx); cause != nil {
			return w, cause
		}
		// A poll the cap cut short is not a failed read.
		if capped.Err() != nil {
			break
		}
		now := time.Now()
		w.Polls++
		if err == nil {
			w.LastPolledHead = head
		}
		switch {
		case err != nil:
			w.FailedPolls++
			w.LastPollError = err.Error()
			stableSince = time.Time{}
		case !done:
			stableSince = time.Time{}
		case stableSince.IsZero() || head != stableHead:
			stableSince, stableHead = now, head
		}
		if !stableSince.IsZero() && now.Sub(stableSince) >= sp.settle {
			w.Outcome = "settled"
			break
		}
		if !now.Before(end) {
			break
		}
		timer := time.NewTimer(min(sp.poll, end.Sub(now)))
		select {
		case <-timer.C:
		case <-capped.Done():
			timer.Stop()
		}
	}
	w.EndedAt = stamp(time.Now())
	return w, nil
}

// pollState reads the pull request's head and whether its check runs and
// commit statuses are all terminal.
func (p prepared) pollState(ctx context.Context, dir string, number int) (string, bool, error) {
	pr, err := getPullRequest(ctx, dir, p.repository, number)
	if err != nil {
		return "", false, err
	}
	runs, err := checkRuns(ctx, dir, p.repository, pr.Head.SHA)
	if err != nil {
		return "", false, err
	}
	statuses, err := commitStatuses(ctx, dir, p.repository, pr.Head.SHA)
	if err != nil {
		return "", false, err
	}
	return pr.Head.SHA, terminal(runs, statuses), nil
}

// readState reads the snapshot once, within sp.reads: the pull request, then
// the check runs and commit statuses of the head it shows and its unresolved
// review threads.
func (p prepared) readState(ctx context.Context, dir string, number int, sp statePolicy) (prStateRecord, error) {
	reads, cancel := context.WithTimeoutCause(ctx, sp.reads, errReadsCap)
	defer cancel()
	record := prStateRecord{ObservedAt: stamp(time.Now())}
	pr, err := getPullRequest(reads, dir, p.repository, number)
	if err == nil {
		record.PullRequest, err = pullState(pr, number)
	}
	if err == nil {
		record.CheckRuns, err = checkRuns(reads, dir, p.repository, pr.Head.SHA)
	}
	if err == nil {
		record.Statuses, err = commitStatuses(reads, dir, p.repository, pr.Head.SHA)
	}
	if err == nil {
		record.UnresolvedThreads, err = unresolvedThreads(reads, dir, p.repository, number)
	}
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return prStateRecord{}, cause
		}
		return prStateRecord{}, fmt.Errorf("%w: %w", errStateRead, err)
	}
	return record, nil
}

// pullState is the record of pull request number from its GET, which
// getPullRequest checked for draft, merged and the head SHA.
func pullState(pr pullRequest, number int) (pullStateObserved, error) {
	if pr.Number != number {
		return pullStateObserved{}, fmt.Errorf("pull request GET: number %d, want %d", pr.Number, number)
	}
	if pr.HTMLURL == "" || pr.State == "" || pr.Head.Ref == "" || pr.Base.Ref == "" {
		return pullStateObserved{}, errors.New("pull request GET: no html_url, state, head ref or base ref")
	}
	return pullStateObserved{Number: pr.Number, URL: pr.HTMLURL, State: pr.State, Draft: *pr.Draft, Merged: *pr.Merged, HeadSHA: pr.Head.SHA, HeadRef: pr.Head.Ref, BaseRef: pr.Base.Ref}, nil
}

// checkRuns lists the check runs of commit sha, every page.
func checkRuns(ctx context.Context, dir, repository, sha string) ([]checkRunObserved, error) {
	raw, err := runtime.GitHubGET(ctx, dir, "repos/"+repository+"/commits/"+sha+"/check-runs?per_page=100", true)
	if err != nil {
		return nil, fmt.Errorf("check runs GET: %w", err)
	}
	runs, err := decodeCheckRuns(raw)
	if err != nil {
		return nil, fmt.Errorf("check runs GET: %w", err)
	}
	return runs, nil
}

func decodeCheckRuns(raw []byte) ([]checkRunObserved, error) {
	type githubCheckRun struct {
		ID   *int64  `json:"id"`
		Name *string `json:"name"`
		App  *struct {
			Slug *string `json:"slug"`
		} `json:"app"`
		Status     *string `json:"status"`
		Conclusion *string `json:"conclusion"`
		HTMLURL    *string `json:"html_url"`
		DetailsURL *string `json:"details_url"`
	}
	runs, err := restPages(raw, "check_runs", func(run githubCheckRun) (string, error) {
		if run.ID == nil || run.Name == nil || run.Status == nil {
			return "", errors.New("a check run without id, name or status")
		}
		return strconv.FormatInt(*run.ID, 10), nil
	})
	if err != nil {
		return nil, err
	}
	out := []checkRunObserved{}
	for _, run := range runs {
		o := checkRunObserved{ID: *run.ID, Name: *run.Name, Status: *run.Status, Conclusion: run.Conclusion, HTMLURL: run.HTMLURL, DetailsURL: run.DetailsURL}
		if run.App != nil {
			o.App = run.App.Slug
		}
		out = append(out, o)
	}
	return out, nil
}

// commitStatuses lists the latest commit status of each context of commit
// sha, every page.
func commitStatuses(ctx context.Context, dir, repository, sha string) ([]statusObserved, error) {
	raw, err := runtime.GitHubGET(ctx, dir, "repos/"+repository+"/commits/"+sha+"/status?per_page=100", true)
	if err != nil {
		return nil, fmt.Errorf("commit statuses GET: %w", err)
	}
	statuses, err := decodeStatuses(raw)
	if err != nil {
		return nil, fmt.Errorf("commit statuses GET: %w", err)
	}
	return statuses, nil
}

func decodeStatuses(raw []byte) ([]statusObserved, error) {
	type githubStatus struct {
		Context   *string `json:"context"`
		State     *string `json:"state"`
		TargetURL *string `json:"target_url"`
	}
	statuses, err := restPages(raw, "statuses", func(s githubStatus) (string, error) {
		if s.Context == nil || s.State == nil {
			return "", errors.New("a status without context or state")
		}
		return *s.Context, nil
	})
	if err != nil {
		return nil, err
	}
	out := []statusObserved{}
	for _, s := range statuses {
		out = append(out, statusObserved{Context: *s.Context, State: *s.State, TargetURL: s.TargetURL})
	}
	return out, nil
}

// restPages decodes a slurped paginated read whose pages are objects with
// total_count and the list field. Every page must report the same
// total_count, the merged list must have that many items, and the keys key
// returns must not repeat; key rejects an item that lacks a field.
func restPages[T any](raw []byte, field string, key func(T) (string, error)) ([]T, error) {
	var pages []map[string]json.RawMessage
	if json.Unmarshal(raw, &pages) != nil || len(pages) == 0 {
		return nil, errors.New("not a list of page objects")
	}
	out := []T{}
	seen := map[string]bool{}
	total := 0
	for i, page := range pages {
		var n int
		var items []T
		if json.Unmarshal(page["total_count"], &n) != nil {
			return nil, fmt.Errorf("page %d: no integer total_count", i+1)
		}
		if json.Unmarshal(page[field], &items) != nil || items == nil {
			return nil, fmt.Errorf("page %d: no %s list", i+1, field)
		}
		if i > 0 && n != total {
			return nil, fmt.Errorf("page %d: total_count %d; page 1 reported %d", i+1, n, total)
		}
		total = n
		for _, item := range items {
			k, err := key(item)
			if err != nil {
				return nil, fmt.Errorf("page %d: %w", i+1, err)
			}
			if seen[k] {
				return nil, fmt.Errorf("page %d: %s key %q repeats an earlier item", i+1, field, k)
			}
			seen[k] = true
		}
		out = append(out, items...)
	}
	if len(out) != total {
		return nil, fmt.Errorf("%d %s across %d pages; total_count is %d", len(out), field, len(pages), total)
	}
	return out, nil
}

// unresolvedThreads lists the review threads of pull request number, every
// page, and keeps those GitHub shows as not resolved.
func unresolvedThreads(ctx context.Context, dir, repository string, number int) ([]threadObserved, error) {
	owner, name, _ := strings.Cut(repository, "/")
	raw, err := runtime.GitHubGraphQL(ctx, dir, reviewThreadsQuery, map[string]string{"owner": owner, "name": name}, map[string]int{"number": number})
	if err != nil {
		return nil, fmt.Errorf("review threads query: %w", err)
	}
	threads, err := decodeThreads(raw)
	if err != nil {
		return nil, fmt.Errorf("review threads query: %w", err)
	}
	return threads, nil
}

// decodeThreads checks the slurped pages of reviewThreadsQuery like
// restPages, with thread IDs as the keys, and keeps the unresolved threads.
func decodeThreads(raw []byte) ([]threadObserved, error) {
	var pages []struct {
		Errors []json.RawMessage `json:"errors"`
		Data   *struct {
			Repository *struct {
				PullRequest *struct {
					ReviewThreads *struct {
						TotalCount *int `json:"totalCount"`
						Nodes      []struct {
							ID         string  `json:"id"`
							IsResolved *bool   `json:"isResolved"`
							IsOutdated *bool   `json:"isOutdated"`
							Path       *string `json:"path"`
							Line       *int    `json:"line"`
							Comments   struct {
								Nodes []struct {
									URL *string `json:"url"`
								} `json:"nodes"`
							} `json:"comments"`
						} `json:"nodes"`
					} `json:"reviewThreads"`
				} `json:"pullRequest"`
			} `json:"repository"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &pages) != nil || len(pages) == 0 {
		return nil, errors.New("not a list of page objects")
	}
	out := []threadObserved{}
	seen := map[string]bool{}
	total := 0
	for i, page := range pages {
		if len(page.Errors) > 0 {
			return nil, fmt.Errorf("page %d has errors", i+1)
		}
		if page.Data == nil || page.Data.Repository == nil || page.Data.Repository.PullRequest == nil || page.Data.Repository.PullRequest.ReviewThreads == nil {
			return nil, fmt.Errorf("page %d has no repository pull request review threads", i+1)
		}
		threads := page.Data.Repository.PullRequest.ReviewThreads
		if threads.TotalCount == nil || threads.Nodes == nil {
			return nil, fmt.Errorf("page %d has no totalCount or nodes", i+1)
		}
		if i > 0 && *threads.TotalCount != total {
			return nil, fmt.Errorf("page %d: totalCount %d; page 1 reported %d", i+1, *threads.TotalCount, total)
		}
		total = *threads.TotalCount
		for _, node := range threads.Nodes {
			if node.ID == "" || node.IsResolved == nil || node.IsOutdated == nil || node.Path == nil {
				return nil, fmt.Errorf("page %d: a thread without id, isResolved, isOutdated or path", i+1)
			}
			if seen[node.ID] {
				return nil, fmt.Errorf("page %d: thread %q repeats an earlier one", i+1, node.ID)
			}
			seen[node.ID] = true
			if *node.IsResolved {
				continue
			}
			t := threadObserved{ID: node.ID, Path: *node.Path, Line: node.Line, Outdated: *node.IsOutdated}
			if len(node.Comments.Nodes) > 0 {
				t.URL = node.Comments.Nodes[0].URL
			}
			out = append(out, t)
		}
	}
	if len(seen) != total {
		return nil, fmt.Errorf("%d threads across %d pages; totalCount is %d", len(seen), len(pages), total)
	}
	return out, nil
}
