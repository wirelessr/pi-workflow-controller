// Package contract validates and publishes attempt-local JSON snapshots.
// The engine owns the Store and separately authorizes committed publications.
package contract

import (
	"fmt"
	"os"
	"sync"
)

type Identity struct {
	RunID         string `json:"run_id"`
	InvocationID  string `json:"invocation_id"`
	AttemptID     string `json:"attempt_id"`
	DispatchToken string `json:"dispatch_token"`
}

type Spec struct {
	SchemaID string `json:"schema_id"`
}

type Ref struct {
	RunID          string `json:"run_id"`
	AttemptID      string `json:"attempt_id"`
	Path           string `json:"path"`
	SchemaID       string `json:"schema_id"`
	SHA256         string `json:"sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

// Request is input, not an authorization to consume its Refs. BeginAttempt
// replaces Output's resource locations with the binary-owned registry's copy.
type Request struct {
	Identity Identity   `json:"identity"`
	Prompt   string     `json:"prompt"`
	Inputs   []Ref      `json:"inputs"`
	Feedback *Feedback  `json:"feedback,omitempty"`
	Output   OutputSpec `json:"output"`
}

type Feedback struct {
	Message         string `json:"message"`
	SourceAttemptID string `json:"source_attempt_id,omitempty"`
	SourceCode      string `json:"source_code,omitempty"`
	Refs            []Ref  `json:"refs,omitempty"`
}

type SchemaResource struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

type OutputSpec struct {
	SchemaID  string                    `json:"schema_id"`
	Schema    SchemaResource            `json:"schema"`
	Envelope  SchemaResource            `json:"envelope"`
	Resources map[string]SchemaResource `json:"resources"`
}

type Code string

const (
	InvalidDefinition Code = "InvalidDefinition"
	ContractMissing   Code = "ContractMissing"
	ContractInvalid   Code = "ContractInvalid"
	IdentityMismatch  Code = "IdentityMismatch"
	ReferenceInvalid  Code = "ReferenceInvalid"
	LimitExceeded     Code = "LimitExceeded"
	StorageFailed     Code = "StorageFailed"
)

// Error retains the filesystem/validator cause for the engine's disposition
// mapping. Context cancellation is returned unchanged, including typed causes.
type Error struct {
	Code     Code     `json:"code"`
	Phase    string   `json:"phase"`
	Identity Identity `json:"identity"`
	Message  string   `json:"message"`
	Cause    error    `json:"-"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s (%s): %s", e.Code, e.Phase, e.Message) }
func (e *Error) Unwrap() error { return e.Cause }
func failure(code Code, phase string, id Identity, cause error) error {
	return &Error{Code: code, Phase: phase, Identity: id, Message: cause.Error(), Cause: cause}
}

// Store cannot be reopened from disk. Known attempts are not the engine's
// committed registry, and Read alone does not authorize downstream dispatch.
type Store struct {
	life               sync.RWMutex
	root               *os.Root
	dir, taskID, runID string
	registry           *Registry
	limits             Limits
	resources          map[string]SchemaResource
	mu                 sync.Mutex
	attempts           map[string]*Attempt
	numbers            map[string]int
	tokens             map[string]bool
	// Test-only filesystem barrier, configured before any operations start.
	afterCopy func(string)
}

type Attempt struct {
	store        *Store
	id           Identity
	number       int
	rel          string
	schemaID     string
	mu           sync.Mutex
	staged       *Staged
	published    bool
	manifestSize int64
}

// Staged is an opaque single-use snapshot. Discard it if runtime Confirm fails.
type Staged struct {
	attempt      *Attempt
	rel          string
	ref          Ref
	manifestSize int64
	used         bool
}

type fileEntry struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Path string `json:"path"`
}

type envelope struct {
	Meta struct {
		Identity
		SchemaID string `json:"schema_id"`
	} `json:"meta"`
	Files []fileEntry `json:"files"`
}

type manifestFile struct {
	fileEntry
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type manifest struct {
	Version        int            `json:"version"`
	Identity       Identity       `json:"identity"`
	SchemaID       string         `json:"schema_id"`
	ContractSHA256 string         `json:"contract_sha256"`
	Files          []manifestFile `json:"files"`
}
