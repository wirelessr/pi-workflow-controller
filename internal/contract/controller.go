package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// ControllerFile is a file the Controller itself places in an attempt. Path
// is "evidence/<name>" or "artifacts/<name>" without further nesting; the
// directory determines the file kind.
type ControllerFile struct {
	ID   string
	Path string
	Data []byte
}

// CheckControllerFiles rejects Controller-produced data and files that this
// Store would refuse, before any attempt is created or byte is written. Stage
// still validates the written candidate in full.
func (s *Store) CheckControllerFiles(data json.RawMessage, files []ControllerFile) error {
	_, err := s.controllerEntries(Identity{}, data, files)
	return err
}

func (s *Store) controllerEntries(id Identity, data json.RawMessage, files []ControllerFile) ([]FileEntry, error) {
	l := s.limits
	if int64(len(data)) > l.MaxCandidateBytes {
		return nil, failure(LimitExceeded, "controller-candidate", id, errors.New("controller data exceeds candidate byte limit"))
	}
	if len(files) > l.MaxAttemptFiles {
		return nil, failure(LimitExceeded, "controller-candidate", id, errors.New("too many controller files"))
	}
	entries := make([]FileEntry, 0, len(files))
	ids, paths := map[string]bool{}, map[string]bool{}
	var total int64
	for _, f := range files {
		dir, name := filepath.Split(f.Path)
		kind := map[string]string{"evidence/": "evidence", "artifacts/": "artifact"}[dir]
		if kind == "" || name == "" || name == "." || name == ".." || len(name) > 255 || !utf8.ValidString(name) || strings.ContainsAny(name, "/\\\x00") {
			return nil, failure(InvalidDefinition, "controller-candidate", id, fmt.Errorf("invalid controller file path %q", f.Path))
		}
		// The default macOS filesystem folds case, so case variants collide.
		folded := strings.ToLower(f.Path)
		if f.ID == "" || ids[f.ID] || paths[folded] {
			return nil, failure(InvalidDefinition, "controller-candidate", id, fmt.Errorf("empty or duplicate controller file id %q or path %q", f.ID, f.Path))
		}
		ids[f.ID], paths[folded] = true, true
		if int64(len(f.Data)) > l.MaxFileBytes {
			return nil, failure(LimitExceeded, "controller-candidate", id, fmt.Errorf("controller file %q exceeds file byte limit", f.ID))
		}
		if total += int64(len(f.Data)); total > l.MaxAttemptFileBytes {
			return nil, failure(LimitExceeded, "controller-candidate", id, errors.New("controller files exceed attempt byte limit"))
		}
		entries = append(entries, FileEntry{ID: f.ID, Kind: kind, Path: f.Path})
	}
	return entries, nil
}

// WriteControllerCandidate writes a Controller-produced candidate envelope and
// its files into this attempt, each created exclusively and synced. It exists
// for attempts that have no Agent; Stage and Publish validate the result
// exactly as they validate an Agent candidate. Files written by a failed call
// are removed.
func (a *Attempt) WriteControllerCandidate(data json.RawMessage, files []ControllerFile) (err error) {
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
	entries, err := s.controllerEntries(a.id, data, files)
	if err != nil {
		return err
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
	if int64(len(raw)) > s.limits.MaxCandidateBytes {
		return failure(LimitExceeded, "controller-candidate", a.id, errors.New("controller candidate exceeds byte limit"))
	}
	var written []string
	defer func() {
		if err != nil {
			for _, path := range written {
				if cleanup := s.root.Remove(path); cleanup != nil && !errors.Is(cleanup, os.ErrNotExist) {
					err = errors.Join(err, failure(StorageFailed, "controller-candidate", a.id, cleanup))
				}
			}
		}
	}()
	for _, f := range files {
		path := filepath.Join(a.rel, f.Path)
		e := writeExclusive(s.root, path, f.Data, s.syncFile)
		if e == nil || !errors.Is(e, os.ErrExist) {
			written = append(written, path)
		}
		if e != nil {
			return failure(StorageFailed, "controller-candidate", a.id, e)
		}
	}
	path := filepath.Join(a.rel, "candidate.json")
	if e := writeExclusive(s.root, path, raw, s.syncFile); e != nil {
		if !errors.Is(e, os.ErrExist) {
			written = append(written, path)
		}
		return failure(StorageFailed, "controller-candidate", a.id, e)
	}
	return nil
}
