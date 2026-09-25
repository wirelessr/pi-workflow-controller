package contract

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

type Limits struct {
	MaxPromptBytes      int64 `json:"max_prompt_bytes"`
	MaxCandidateBytes   int64 `json:"max_candidate_bytes"`
	MaxJSONDepth        int   `json:"max_json_depth"`
	MaxFileBytes        int64 `json:"max_file_bytes"`
	MaxAttemptFileBytes int64 `json:"max_attempt_file_bytes"`
	MaxAttemptFiles     int   `json:"max_attempt_files"`
}

func DefaultLimits() Limits {
	return Limits{64 << 10, 1 << 20, 64, 64 << 20, 256 << 20, 128}
}

// Options are Controller construction inputs, not workflow/CLI configuration.
// BaseDir defaults to ~/WIP. Limits must be entirely positive or entirely zero.
type Options struct {
	BaseDir           string
	Prompt            string
	LaunchCWD         string
	Workflow          string
	WorkflowVersion   string
	ControllerVersion string
	PiVersion         string
	Limits            Limits
	// SyncFile is fixed at construction. Nil uses os.File.Sync.
	// Callers must support concurrent file syncs from independent attempts.
	SyncFile func(*os.File) error
}

type runMetadata struct {
	TaskID            string    `json:"task_id"`
	RunID             string    `json:"run_id"`
	Workflow          string    `json:"workflow"`
	WorkflowVersion   string    `json:"workflow_version"`
	ControllerVersion string    `json:"controller_version"`
	PiVersion         string    `json:"pi_version"`
	LaunchCWD         string    `json:"launch_cwd"`
	CreatedAt         time.Time `json:"created_at"`
	ContractPolicy    Limits    `json:"contract_policy"`
}

func NewID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
func validID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) >= 16 && len(id) <= 128 && strings.ToLower(id) == id
}

func ValidatePrompt(prompt string, maxBytes int64) error {
	if maxBytes <= 0 || strings.TrimSpace(prompt) == "" || !utf8.ValidString(prompt) || strings.ContainsAny(prompt, "\r\n\x00") {
		return failure(InvalidDefinition, "prompt", Identity{}, errors.New("prompt must be nonempty, single-line UTF-8 without NUL"))
	}
	if int64(len(prompt)) > maxBytes {
		return failure(InvalidDefinition, "prompt", Identity{}, errors.New("prompt exceeds byte limit"))
	}
	return nil
}

func NewStore(registry *Registry, opts Options) (_ *Store, err error) {
	if opts.Limits == (Limits{}) {
		opts.Limits = DefaultLimits()
	}
	if opts.SyncFile == nil {
		opts.SyncFile = (*os.File).Sync
	}
	l := opts.Limits
	if registry == nil || registry.envelope == nil || l.MaxPromptBytes <= 0 || l.MaxCandidateBytes <= 0 || l.MaxJSONDepth <= 0 || l.MaxFileBytes <= 0 || l.MaxAttemptFileBytes <= 0 || l.MaxAttemptFiles <= 0 {
		return nil, failure(InvalidDefinition, "create", Identity{}, errors.New("compiled registry and positive limits required"))
	}
	if err := ValidatePrompt(opts.Prompt, l.MaxPromptBytes); err != nil {
		return nil, err
	}
	if opts.BaseDir == "" {
		home, e := os.UserHomeDir()
		if e != nil {
			return nil, failure(StorageFailed, "create", Identity{}, e)
		}
		opts.BaseDir = filepath.Join(home, "WIP")
	}
	base, e := filepath.Abs(opts.BaseDir)
	if e != nil {
		return nil, failure(StorageFailed, "create", Identity{}, e)
	}
	if e = os.MkdirAll(base, 0700); e != nil {
		return nil, failure(StorageFailed, "create", Identity{}, e)
	}
	// Pin the trusted base spelling once so all requests and Refs agree even
	// when HOME/WIP or the platform temp directory has a symlink ancestor.
	base, e = filepath.EvalSymlinks(base)
	if e != nil {
		return nil, failure(StorageFailed, "create", Identity{}, e)
	}
	baseRoot, e := os.OpenRoot(base)
	if e != nil {
		return nil, failure(StorageFailed, "create", Identity{}, e)
	}
	defer func(root *os.Root) { _ = root.Close() }(baseRoot)
	now := time.Now().UTC()
	var taskID string
	for {
		nonce := NewID()
		taskID = "pw-" + now.Format("20060102T150405Z") + "-" + nonce[:12]
		e = baseRoot.Mkdir(taskID, 0700)
		if errors.Is(e, os.ErrExist) {
			continue
		}
		if e != nil {
			return nil, failure(StorageFailed, "create", Identity{}, e)
		}
		break
	}
	// A failed constructor has never yielded an owned run; remove only its new task.
	defer func() {
		if err != nil {
			err = errors.Join(err, baseRoot.RemoveAll(taskID))
		}
	}()
	runID := NewID()
	rel := filepath.Join(taskID, "runs", runID)
	if e = baseRoot.MkdirAll(rel, 0700); e != nil {
		return nil, failure(StorageFailed, "create", Identity{}, e)
	}
	root, e := baseRoot.OpenRoot(rel)
	if e != nil {
		return nil, failure(StorageFailed, "create", Identity{}, e)
	}
	defer func() {
		if err != nil {
			_ = root.Close()
		}
	}()
	s := &Store{root: root, syncFile: opts.SyncFile, dir: filepath.Join(base, rel), taskID: taskID, runID: runID, registry: registry, limits: l,
		resources: make(map[string]SchemaResource), attempts: make(map[string]*Attempt), numbers: make(map[string]int), tokens: make(map[string]bool)}
	for _, dir := range []string{"steps", "sessions", "schemas", ".staging"} {
		if e = root.Mkdir(dir, 0700); e != nil {
			return nil, failure(StorageFailed, "create", Identity{RunID: runID}, e)
		}
	}
	for uri, raw := range registry.resources {
		path := filepath.Join("schemas", digest([]byte(uri))+".json")
		if e = writeExclusive(root, path, raw, s.syncFile); e != nil {
			return nil, failure(StorageFailed, "schemas", Identity{RunID: runID}, e)
		}
		s.resources[uri] = SchemaResource{Path: filepath.Join(s.dir, path), SHA256: digest(raw)}
	}
	meta := runMetadata{taskID, runID, opts.Workflow, opts.WorkflowVersion, opts.ControllerVersion, opts.PiVersion, opts.LaunchCWD, now, l}
	input := struct {
		Prompt    string `json:"prompt"`
		LaunchCWD string `json:"launch_cwd"`
	}{opts.Prompt, opts.LaunchCWD}
	for path, value := range map[string]any{"run.json": meta, "input.json": input} {
		raw, e := json.MarshalIndent(value, "", "  ")
		if e == nil {
			e = writeExclusive(root, path, raw, s.syncFile)
		}
		if e != nil {
			return nil, failure(StorageFailed, "create", Identity{RunID: runID}, e)
		}
	}
	return s, nil
}

func (s *Store) Dir() string    { return s.dir }
func (s *Store) TaskID() string { return s.taskID }
func (s *Store) RunID() string  { return s.runID }
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.life.Lock()
	defer s.life.Unlock()
	if s.root == nil {
		return nil
	}
	err := s.root.Close()
	s.root = nil
	return err
}

func (s *Store) BeginAttempt(id Identity, request Request) (*Attempt, error) {
	if s == nil {
		return nil, failure(InvalidDefinition, "begin", id, errors.New("store not initialized"))
	}
	s.life.RLock()
	defer s.life.RUnlock()
	if s.root == nil {
		return nil, failure(InvalidDefinition, "begin", id, errors.New("store closed or uninitialized"))
	}
	if id.RunID != s.runID || !validID(id.InvocationID) || !validID(id.AttemptID) || !validID(id.DispatchToken) || request.Identity != id || !s.registry.Has(request.Output.SchemaID) {
		return nil, failure(InvalidDefinition, "begin", id, errors.New("invalid identity or unregistered output schema"))
	}
	s.mu.Lock()
	if s.attempts[id.AttemptID] != nil || s.tokens[id.DispatchToken] {
		s.mu.Unlock()
		return nil, failure(InvalidDefinition, "begin", id, errors.New("attempt ID and dispatch token cannot be reused"))
	}
	s.numbers[id.InvocationID]++
	number := s.numbers[id.InvocationID]
	a := &Attempt{store: s, id: id, number: number, schemaID: request.Output.SchemaID, rel: filepath.Join("steps", id.InvocationID, "attempts", fmt.Sprintf("%04d-%s", number, id.AttemptID))}
	s.attempts[id.AttemptID] = a
	s.tokens[id.DispatchToken] = true
	parent := filepath.Dir(a.rel)
	// Go 1.25 Root.MkdirAll can return EEXIST for concurrent shared parents.
	// Serialize only parent creation; candidate I/O stays attempt-local.
	parentErr := s.root.MkdirAll(parent, 0700)
	s.mu.Unlock()
	if parentErr != nil {
		return nil, failure(StorageFailed, "begin", id, parentErr)
	}
	if err := noSymlinks(s.root, parent); err != nil {
		return nil, failure(StorageFailed, "begin", id, err)
	}
	if err := s.root.Mkdir(a.rel, 0700); err != nil {
		return nil, failure(StorageFailed, "begin", id, err)
	}
	for _, dir := range []string{"evidence", "artifacts"} {
		if err := s.root.Mkdir(filepath.Join(a.rel, dir), 0700); err != nil {
			return nil, failure(StorageFailed, "begin", id, err)
		}
	}
	if _, err := s.root.Lstat(filepath.Join(a.rel, "candidate.json")); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = errors.New("candidate already exists")
		}
		return nil, failure(StorageFailed, "begin", id, err)
	}
	request.Output = OutputSpec{SchemaID: a.schemaID, Schema: s.resources[s.registry.uris[a.schemaID]], Envelope: s.resources[EnvelopeURI], Resources: s.resources}
	raw, err := json.MarshalIndent(request, "", "  ")
	if err == nil {
		err = writeExclusive(s.root, filepath.Join(a.rel, "request.json"), raw, s.syncFile)
	}
	if err != nil {
		return nil, failure(StorageFailed, "begin", id, err)
	}
	return a, nil
}

func (a *Attempt) Dir() string           { return filepath.Join(a.store.dir, a.rel) }
func (a *Attempt) RequestPath() string   { return filepath.Join(a.Dir(), "request.json") }
func (a *Attempt) CandidatePath() string { return filepath.Join(a.Dir(), "candidate.json") }
func (a *Attempt) Number() int           { return a.number }
