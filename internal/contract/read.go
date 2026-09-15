package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func (s *Store) Read(ctx context.Context, ref Ref) (json.RawMessage, error) {
	if s == nil {
		return nil, failure(ReferenceInvalid, "read", Identity{}, errors.New("store not initialized"))
	}
	s.life.RLock()
	defer s.life.RUnlock()
	if s.root == nil {
		return nil, failure(ReferenceInvalid, "read", Identity{}, errors.New("store closed or uninitialized"))
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	a := s.attempts[ref.AttemptID]
	s.mu.Unlock()
	if ref.RunID != s.runID || a == nil {
		return nil, failure(ReferenceInvalid, "resolve", Identity{}, errors.New("reference does not identify a known attempt in this run"))
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.published || ref.SchemaID != a.schemaID || ref.Path != filepath.Join(a.Dir(), "published", "contract.json") {
		return nil, failure(ReferenceInvalid, "resolve", a.id, errors.New("reference is not the canonical publication path/schema"))
	}
	return s.readSnapshot(ctx, a, filepath.Join(a.rel, "published"), ref, a.manifestSize)
}

func (s *Store) readSnapshot(ctx context.Context, a *Attempt, rel string, ref Ref, manifestSize int64) (json.RawMessage, error) {
	invalid := func(err error) (json.RawMessage, error) {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		var pathErr *os.PathError
		if errors.As(err, &pathErr) && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, os.ErrInvalid) {
			return nil, failure(StorageFailed, "read", a.id, err)
		}
		return nil, failure(ReferenceInvalid, "read", a.id, err)
	}
	raw, err := readStable(ctx, s.root, filepath.Join(rel, "contract.json"), s.limits.MaxCandidateBytes, nil)
	if err != nil {
		return invalid(err)
	}
	if digest(raw) != ref.SHA256 {
		return invalid(errors.New("contract digest mismatch"))
	}
	env, err := s.validate(raw, a)
	if err != nil {
		return invalid(err)
	}
	// The Controller knows the exact generated size. A policy-derived estimate
	// would mishandle JSON escaping expansion or overflow for large limits.
	manifestRaw, err := readStable(ctx, s.root, filepath.Join(rel, "manifest.json"), manifestSize, nil)
	if err != nil {
		return invalid(err)
	}
	if digest(manifestRaw) != ref.ManifestSHA256 {
		return invalid(errors.New("manifest digest mismatch"))
	}
	if _, err = parseJSON(manifestRaw, s.limits.MaxJSONDepth); err != nil {
		return invalid(err)
	}
	var m manifest
	decoder := json.NewDecoder(bytes.NewReader(manifestRaw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&m); err != nil {
		return invalid(err)
	}
	if m.Version != 1 || m.Identity != a.id || m.SchemaID != a.schemaID || m.ContractSHA256 != ref.SHA256 || len(m.Files) != len(env.Files) {
		return invalid(errors.New("manifest identity or file list mismatch"))
	}
	var total int64
	var infos []os.FileInfo
	for i, file := range m.Files {
		if file.fileEntry != env.Files[i] || file.Size < 0 {
			return invalid(errors.New("manifest file reference mismatch"))
		}
		limit := min(s.limits.MaxFileBytes, s.limits.MaxAttemptFileBytes-total)
		hash, n, info, err := copyStable(ctx, s.root, filepath.Join(rel, file.Path), limit, io.Discard, nil)
		if err != nil {
			return invalid(err)
		}
		if hash != file.SHA256 || n != file.Size {
			return invalid(fmt.Errorf("file digest/size mismatch: %s", file.Path))
		}
		for _, previous := range infos {
			if os.SameFile(previous, info) {
				return invalid(errors.New("duplicate file target"))
			}
		}
		infos = append(infos, info)
		total += n
	}
	if err = context.Cause(ctx); err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}
