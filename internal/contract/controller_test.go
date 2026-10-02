package contract

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteControllerCandidate(t *testing.T) {
	files := []ControllerFile{{ID: "prompt", Path: "evidence/prompt.txt", Data: []byte("original")}, {ID: "note", Path: "artifacts/note.md", Data: []byte("note")}}
	t.Run("published like an Agent candidate", func(t *testing.T) {
		s := storeTestNew(t, Limits{})
		a, _ := storeTestAttempt(t, s, "")
		if err := a.WriteControllerCandidate(json.RawMessage(`{"answer":"yes"}`), files); err != nil {
			t.Fatal(err)
		}
		staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
		if err != nil {
			t.Fatal(err)
		}
		ref, err := a.Publish(context.Background(), staged)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := s.Read(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		p, err := DecodePublication[struct {
			Answer string `json:"answer"`
		}](raw)
		if err != nil || p.Data.Answer != "yes" || len(p.Files) != 2 || p.Files[0] != (FileEntry{ID: "prompt", Kind: "evidence", Path: "evidence/prompt.txt"}) || p.Files[1] != (FileEntry{ID: "note", Kind: "artifact", Path: "artifacts/note.md"}) {
			t.Fatalf("publication = %+v, %v", p, err)
		}
	})
	t.Run("a failed write removes the files it created", func(t *testing.T) {
		injected := errors.New("synthetic candidate Sync failure")
		s, err := NewStore(schemaTestRegistry(t, storeTestSchema), Options{BaseDir: t.TempDir(), Prompt: "驗收測試", SyncFile: func(f *os.File) error {
			if filepath.Base(f.Name()) == "candidate.json" {
				return injected
			}
			return f.Sync()
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		a, _ := storeTestAttempt(t, s, "")
		err = a.WriteControllerCandidate(json.RawMessage(`{"answer":"yes"}`), files)
		var typed *Error
		if !errors.As(err, &typed) || typed.Code != StorageFailed || !errors.Is(err, injected) {
			t.Fatalf("error = %v, want StorageFailed from the injected Sync", err)
		}
		for _, name := range []string{"candidate.json", "evidence/prompt.txt", "artifacts/note.md"} {
			if _, err := os.Lstat(filepath.Join(a.Dir(), name)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("%s left behind: %v", name, err)
			}
		}
	})
	t.Run("an envelope over the candidate limit writes nothing", func(t *testing.T) {
		s := storeTestNew(t, Limits{MaxPromptBytes: 64 << 10, MaxCandidateBytes: 300, MaxJSONDepth: 64, MaxFileBytes: 1 << 20, MaxAttemptFileBytes: 1 << 20, MaxAttemptFiles: 8})
		a, _ := storeTestAttempt(t, s, "")
		err := a.WriteControllerCandidate(json.RawMessage(`{"answer":"`+strings.Repeat("x", 200)+`"}`), files)
		var typed *Error
		if !errors.As(err, &typed) || typed.Code != LimitExceeded {
			t.Fatalf("error = %v, want LimitExceeded", err)
		}
		if _, err := os.Lstat(filepath.Join(a.Dir(), "evidence", "prompt.txt")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("file written before the size check: %v", err)
		}
	})
}
