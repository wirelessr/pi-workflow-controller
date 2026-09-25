package contract

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func (a *Attempt) Stage(ctx context.Context, spec Spec) (*Staged, error) {
	if a == nil || a.store == nil {
		return nil, failure(InvalidDefinition, "stage", Identity{}, errors.New("attempt not initialized"))
	}
	s := a.store
	s.life.RLock()
	defer s.life.RUnlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.root == nil || a.published || a.staged != nil || spec.SchemaID != a.schemaID {
		return nil, failure(InvalidDefinition, "stage", a.id, errors.New("closed store, used attempt, or output schema mismatch"))
	}
	staged, err := a.stage(ctx)
	// Preserve cancellation diagnostics too, since ordinary errors marshal as {}.
	var diagnostic any
	if err != nil {
		diagnostic = struct {
			Message string `json:"message"`
			Detail  error  `json:"detail"`
		}{err.Error(), err}
	}
	raw, reportErr := json.MarshalIndent(struct {
		Identity Identity `json:"identity"`
		Valid    bool     `json:"valid"`
		Error    any      `json:"error,omitempty"`
	}{a.id, err == nil, diagnostic}, "", "  ")
	if reportErr == nil {
		reportErr = writeAtomic(s.root, filepath.Join(a.rel, "validation.json"), raw, s.syncFile)
	}
	if reportErr != nil {
		if staged != nil {
			reportErr = errors.Join(reportErr, s.root.RemoveAll(staged.rel))
		}
		return nil, failure(StorageFailed, "validation-report", a.id, errors.Join(reportErr, err))
	}
	if err == nil {
		a.staged = staged
	}
	return staged, err
}

func (a *Attempt) stage(ctx context.Context) (_ *Staged, err error) {
	s := a.store
	raw, e := readStable(ctx, s.root, filepath.Join(a.rel, "candidate.json"), s.limits.MaxCandidateBytes, s.afterCopy)
	if e != nil {
		return nil, a.sourceError(ctx, "candidate", e, true)
	}
	env, e := s.validate(raw, a)
	if e != nil {
		return nil, e
	}
	nonce := NewID()
	rel := filepath.Join(".staging", nonce)
	if e = s.root.Mkdir(rel, 0700); e != nil {
		return nil, failure(StorageFailed, "stage", a.id, e)
	}
	defer func() {
		if err != nil {
			if cleanup := s.root.RemoveAll(rel); cleanup != nil {
				err = failure(StorageFailed, "discard", a.id, errors.Join(cleanup, err))
			}
		}
	}()
	if e = writeExclusive(s.root, filepath.Join(rel, "contract.json"), raw, s.syncFile); e != nil {
		return nil, failure(StorageFailed, "stage", a.id, e)
	}
	m := manifest{Version: 1, Identity: a.id, SchemaID: a.schemaID, ContractSHA256: digest(raw), Files: make([]manifestFile, 0, len(env.Files))}
	var infos []os.FileInfo
	var total int64
	for _, entry := range env.Files {
		if e = context.Cause(ctx); e != nil {
			return nil, e
		}
		sourcePath := filepath.Join(a.rel, entry.Path)
		source, before, e := openRegular(s.root, sourcePath)
		if e != nil {
			return nil, a.sourceError(ctx, "files", e, false)
		}
		if e = source.Close(); e != nil {
			return nil, failure(StorageFailed, "files", a.id, e)
		}
		for _, previous := range infos {
			if os.SameFile(previous, before) {
				return nil, failure(ContractInvalid, "files", a.id, errors.New("duplicate file target"))
			}
		}
		dst := filepath.Join(rel, entry.Path)
		if e = s.root.MkdirAll(filepath.Dir(dst), 0700); e != nil {
			return nil, failure(StorageFailed, "stage", a.id, e)
		}
		f, e := s.root.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return nil, failure(StorageFailed, "stage", a.id, e)
		}
		limit := min(s.limits.MaxFileBytes, s.limits.MaxAttemptFileBytes-total)
		hash, n, info, copyErr := copyStable(ctx, s.root, sourcePath, limit, storageWriter{f, a.id}, s.afterCopy)
		syncErr := s.syncFile(f)
		closeErr := f.Close()
		if syncErr != nil || closeErr != nil {
			return nil, failure(StorageFailed, "stage", a.id, errors.Join(syncErr, closeErr, copyErr))
		}
		if copyErr != nil {
			return nil, a.sourceError(ctx, "files", copyErr, false)
		}
		if !sameVersion(before, info) {
			return nil, failure(ContractInvalid, "files", a.id, errUnstable)
		}
		infos = append(infos, info)
		copied, _, _, e := copyStable(ctx, s.root, dst, limit, io.Discard, s.afterCopy)
		if e != nil {
			if cause := context.Cause(ctx); cause != nil {
				return nil, cause
			}
			return nil, failure(StorageFailed, "stage-copy", a.id, e)
		}
		if copied != hash {
			return nil, failure(StorageFailed, "stage-copy", a.id, errUnstable)
		}
		total += n
		m.Files = append(m.Files, manifestFile{fileEntry: entry, SHA256: hash, Size: n})
	}
	manifestRaw, e := json.MarshalIndent(m, "", "  ")
	if e != nil {
		return nil, failure(StorageFailed, "manifest", a.id, e)
	}
	if e = writeExclusive(s.root, filepath.Join(rel, "manifest.json"), manifestRaw, s.syncFile); e != nil {
		return nil, failure(StorageFailed, "manifest", a.id, e)
	}
	if e = context.Cause(ctx); e != nil {
		return nil, e
	}
	ref := Ref{RunID: a.id.RunID, AttemptID: a.id.AttemptID, Path: filepath.Join(a.Dir(), "published", "contract.json"), SchemaID: a.schemaID, SHA256: m.ContractSHA256, ManifestSHA256: digest(manifestRaw)}
	return &Staged{attempt: a, rel: rel, ref: ref, manifestSize: int64(len(manifestRaw))}, nil
}

type storageWriter struct {
	io.Writer
	id Identity
}

func (w storageWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if err != nil {
		err = failure(StorageFailed, "stage-write", w.id, err)
	}
	return n, err
}

func (a *Attempt) sourceError(ctx context.Context, phase string, err error, candidate bool) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	var typed *Error
	if errors.As(err, &typed) {
		return err
	}
	code := StorageFailed
	switch {
	case errors.Is(err, errSize):
		code = LimitExceeded
	case errors.Is(err, os.ErrNotExist):
		code = ContractInvalid
		if candidate {
			code = ContractMissing
		}
	case errors.Is(err, errUnsafe), errors.Is(err, errUnstable):
		code = ContractInvalid
	}
	return failure(code, phase, a.id, err)
}

func (s *Store) validate(raw []byte, a *Attempt) (*envelope, error) {
	value, err := parseJSON(raw, s.limits.MaxJSONDepth)
	if err != nil {
		return nil, failure(ContractInvalid, "json", a.id, err)
	}
	if object, ok := value.(map[string]any); ok {
		if files, ok := object["files"].([]any); ok && len(files) > s.limits.MaxAttemptFiles {
			return nil, failure(LimitExceeded, "files", a.id, errors.New("too many referenced files"))
		}
	}
	if err = validateSchema(s.registry.envelope, value); err != nil {
		return nil, failure(ContractInvalid, "envelope", a.id, err)
	}
	var env envelope
	if err = json.Unmarshal(raw, &env); err != nil {
		return nil, failure(ContractInvalid, "envelope", a.id, err)
	}
	if env.Meta.Identity != a.id || env.Meta.SchemaID != a.schemaID {
		return nil, failure(IdentityMismatch, "identity", a.id, errors.New("contract identity/schema differs from request"))
	}
	if err = s.registry.validateData(a.schemaID, value.(map[string]any)["data"]); err != nil {
		return nil, failure(ContractInvalid, "schema", a.id, err)
	}
	ids, paths := make(map[string]bool), make(map[string]bool)
	for _, file := range env.Files {
		prefix := "evidence/"
		if file.Kind == "artifact" {
			prefix = "artifacts/"
		}
		if !filepath.IsLocal(file.Path) || filepath.ToSlash(filepath.Clean(file.Path)) != file.Path || !strings.HasPrefix(file.Path, prefix) || strings.ContainsAny(file.Path, "\\\x00") || ids[file.ID] || paths[file.Path] {
			return nil, failure(ContractInvalid, "files", a.id, fmt.Errorf("invalid or duplicate file reference %q", file.Path))
		}
		ids[file.ID] = true
		paths[file.Path] = true
	}
	for path := range paths {
		for parent := filepath.Dir(path); parent != "."; parent = filepath.Dir(parent) {
			if paths[parent] {
				return nil, failure(ContractInvalid, "files", a.id, fmt.Errorf("file path %q is an ancestor of %q", parent, path))
			}
		}
	}
	return &env, nil
}

func (a *Attempt) Publish(ctx context.Context, staged *Staged) (Ref, error) {
	if a == nil || a.store == nil {
		return Ref{}, failure(InvalidDefinition, "publish", Identity{}, errors.New("attempt not initialized"))
	}
	s := a.store
	s.life.RLock()
	defer s.life.RUnlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.root == nil || staged == nil || staged.attempt != a || staged != a.staged || staged.used || a.published {
		return Ref{}, failure(InvalidDefinition, "publish", a.id, errors.New("staged snapshot is not available for this attempt"))
	}
	if err := context.Cause(ctx); err != nil {
		return Ref{}, err
	}
	// Validate only the private snapshot, never the mutable candidate.
	if _, err := s.readSnapshot(ctx, a, staged.rel, staged.ref, staged.manifestSize); err != nil {
		return Ref{}, err
	}
	if err := context.Cause(ctx); err != nil {
		return Ref{}, err
	}
	if err := renameExclusive(s.root, staged.rel, filepath.Join(a.rel, "published")); err != nil {
		return Ref{}, failure(StorageFailed, "publish", a.id, err)
	}
	staged.used = true
	a.published = true
	a.manifestSize = staged.manifestSize
	a.staged = nil
	return staged.ref, nil
}

func (staged *Staged) Discard() error {
	if staged == nil || staged.attempt == nil {
		return nil
	}
	a := staged.attempt
	s := a.store
	s.life.RLock()
	defer s.life.RUnlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	if staged.used {
		return nil
	}
	if s.root == nil {
		return failure(StorageFailed, "discard", a.id, errors.New("store closed"))
	}
	if err := s.root.RemoveAll(staged.rel); err != nil {
		return failure(StorageFailed, "discard", a.id, err)
	}
	staged.used = true
	if a.staged == staged {
		a.staged = nil
	}
	return nil
}
