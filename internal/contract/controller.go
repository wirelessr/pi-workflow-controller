package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// ControllerFile is a file the Controller itself places in an attempt. Path
// is "evidence/<name>" or "artifacts/<name>" without further nesting.
type ControllerFile struct {
	ID   string
	Kind string
	Path string
	Data []byte
}

// WriteControllerCandidate writes a Controller-produced candidate envelope and
// its files into this attempt, each created exclusively and synced. It exists
// for attempts that have no Agent; Stage and Publish validate the result
// exactly as they validate an Agent candidate.
func (a *Attempt) WriteControllerCandidate(data json.RawMessage, files []ControllerFile) error {
	if a == nil || a.store == nil {
		return failure(InvalidDefinition, "controller-candidate", Identity{}, errors.New("attempt not initialized"))
	}
	s := a.store
	s.life.RLock()
	defer s.life.RUnlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.root == nil || a.published || a.staged != nil {
		return failure(InvalidDefinition, "controller-candidate", a.id, errors.New("closed store or used attempt"))
	}
	entries := make([]FileEntry, 0, len(files))
	for _, f := range files {
		dir, name := filepath.Split(f.Path)
		if dir != "evidence/" && dir != "artifacts/" || name == "" || strings.ContainsAny(name, "/\\\x00") || name == "." || name == ".." {
			return failure(InvalidDefinition, "controller-candidate", a.id, fmt.Errorf("invalid controller file path %q", f.Path))
		}
		if err := writeExclusive(s.root, filepath.Join(a.rel, f.Path), f.Data, s.syncFile); err != nil {
			return failure(StorageFailed, "controller-candidate", a.id, err)
		}
		entries = append(entries, FileEntry{ID: f.ID, Kind: f.Kind, Path: f.Path})
	}
	type meta struct {
		Identity
		Version  int    `json:"version"`
		SchemaID string `json:"schema_id"`
	}
	raw, err := json.Marshal(struct {
		Meta  meta            `json:"meta"`
		Data  json.RawMessage `json:"data"`
		Files []FileEntry     `json:"files"`
	}{meta{a.id, 1, a.schemaID}, data, entries})
	if err != nil {
		return failure(InvalidDefinition, "controller-candidate", a.id, err)
	}
	if err := writeExclusive(s.root, filepath.Join(a.rel, "candidate.json"), raw, s.syncFile); err != nil {
		return failure(StorageFailed, "controller-candidate", a.id, err)
	}
	return nil
}
