package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const storeTestSchemaID = "test.output.v1"
const storeTestSchema = `{"type":"object","required":["answer"],"additionalProperties":false,"properties":{"answer":{"type":"string"}}}`

func storeTestHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func storeTestID(t *testing.T) string {
	t.Helper()
	return NewID()
}

func storeTestNew(t *testing.T, limits Limits) *Store {
	t.Helper()
	s, err := NewStore(schemaTestRegistry(t, storeTestSchema), Options{BaseDir: t.TempDir(), Prompt: "驗收測試", Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func storeTestAttempt(t *testing.T, s *Store, invocation string) (*Attempt, Identity) {
	t.Helper()
	if invocation == "" {
		invocation = storeTestID(t)
	}
	id := Identity{s.RunID(), invocation, storeTestID(t), storeTestID(t)}
	a, err := s.BeginAttempt(id, Request{Identity: id, Prompt: "驗收測試", Output: OutputSpec{SchemaID: storeTestSchemaID}})
	if err != nil {
		t.Fatal(err)
	}
	return a, id
}

func storeTestJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func storeTestCandidate(t *testing.T, id Identity, files ...fileEntry) []byte {
	t.Helper()
	if files == nil {
		files = []fileEntry{}
	}
	return storeTestJSON(t, map[string]any{
		"meta": map[string]any{"version": 1, "run_id": id.RunID, "invocation_id": id.InvocationID, "attempt_id": id.AttemptID, "dispatch_token": id.DispatchToken, "schema_id": storeTestSchemaID},
		"data": map[string]any{"answer": "原始資料"}, "files": files,
	})
}

func storeTestWrite(t *testing.T, path string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func storeTestReadFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func storeTestCode(t *testing.T, err error, want Code) {
	t.Helper()
	var got *Error
	if !errors.As(err, &got) || got.Code != want {
		t.Fatalf("error = %v, want typed %s", err, want)
	}
}

func storeTestAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s exists or cannot be checked: %v", path, err)
	}
}

func storeTestUnpublished(t *testing.T, a *Attempt) {
	t.Helper()
	storeTestAbsent(t, filepath.Join(a.Dir(), "published"))
}

func storeTestStage(t *testing.T, a *Attempt) *Staged {
	t.Helper()
	staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
	if err != nil || staged == nil {
		t.Fatalf("Stage = %v, %v", staged, err)
	}
	return staged
}

func storeTestPublish(t *testing.T, a *Attempt) Ref {
	t.Helper()
	ref, err := a.Publish(context.Background(), storeTestStage(t, a))
	if err != nil || ref == (Ref{}) {
		t.Fatalf("Publish = %+v, %v", ref, err)
	}
	return ref
}

func storeTestReport(t *testing.T, a *Attempt, id Identity, valid bool) {
	t.Helper()
	var report struct {
		Identity Identity        `json:"identity"`
		Valid    bool            `json:"valid"`
		Error    json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(storeTestReadFile(t, filepath.Join(a.Dir(), "validation.json")), &report); err != nil {
		t.Fatal(err)
	}
	if report.Identity != id || report.Valid != valid || (!valid && len(report.Error) == 0) {
		t.Fatalf("validation report = %+v, want identity %+v valid=%v", report, id, valid)
	}
}

func TestStoreFailsClosed(t *testing.T) {
	ctx := context.Background()
	for _, s := range []*Store{nil, {}} {
		var a Attempt
		cases := []struct {
			name string
			code Code
			call func() error
		}{
			{"BeginAttempt", InvalidDefinition, func() error {
				got, err := s.BeginAttempt(Identity{}, Request{})
				if got != nil {
					t.Fatal("uninitialized store returned an attempt")
				}
				return err
			}},
			{"Stage", InvalidDefinition, func() error {
				got, err := a.Stage(ctx, Spec{})
				if got != nil {
					t.Fatal("uninitialized attempt returned staged data")
				}
				return err
			}},
			{"Publish", InvalidDefinition, func() error {
				got, err := a.Publish(ctx, nil)
				if got != (Ref{}) {
					t.Fatal("uninitialized attempt returned a Ref")
				}
				return err
			}},
			{"Read", ReferenceInvalid, func() error {
				got, err := s.Read(ctx, Ref{Path: "/uncommitted/contract.json"})
				if got != nil {
					t.Fatal("uninitialized store returned data")
				}
				return err
			}},
		}
		for _, tc := range cases {
			t.Run(fmt.Sprintf("nil=%v/%s", s == nil, tc.name), func(t *testing.T) { storeTestCode(t, tc.call(), tc.code) })
		}
	}
}

func TestStoreCandidateValidation(t *testing.T) {
	cases := []struct {
		name   string
		change func([]byte) []byte
		code   Code
	}{
		{"valid", func(b []byte) []byte { return b }, ""},
		{"integer version notation", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1.0e0`), 1)
		}, ""},
		{"whitespace preserved", func(b []byte) []byte { return append(append([]byte(" \n\t"), b...), '\n') }, ""},
		{"missing", func([]byte) []byte { return nil }, ContractMissing},
		{"empty", func([]byte) []byte { return []byte{} }, ContractInvalid},
		{"invalid JSON", func([]byte) []byte { return []byte(`{"data":`) }, ContractInvalid},
		{"invalid UTF8", func(b []byte) []byte { return bytes.ReplaceAll(b, []byte("原始資料"), []byte{0xff}) }, ContractInvalid},
		{"duplicate key", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"answer":`), []byte(`"answer":"first","answer":`), 1)
		}, ContractInvalid},
		{"escaped duplicate key", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"answer":`), []byte(`"\u0061nswer":"first","answer":`), 1)
		}, ContractInvalid},
		{"trailing JSON", func(b []byte) []byte { return append(b, []byte(` {}`)...) }, ContractInvalid},
		{"trailing garbage", func(b []byte) []byte { return append(b, 'x') }, ContractInvalid},
		{"depth", func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"原始資料"`), []byte(strings.Repeat("[", 65)+"0"+strings.Repeat("]", 65)), 1)
		}, ContractInvalid},
		{"size", func(b []byte) []byte { return append(b, bytes.Repeat([]byte(" "), 1<<20)...) }, LimitExceeded},
		{"schema mismatch", func(b []byte) []byte { return bytes.Replace(b, []byte(`"原始資料"`), []byte(`42`), 1) }, ContractInvalid},
		{"missing envelope field", func(b []byte) []byte { return bytes.Replace(b, []byte(`"files":[],`), nil, 1) }, ContractInvalid},
		{"wrong version", func(b []byte) []byte { return bytes.Replace(b, []byte(`"version":1`), []byte(`"version":2`), 1) }, ContractInvalid},
		{"wrong schema identity", func(b []byte) []byte {
			return bytes.ReplaceAll(b, []byte(storeTestSchemaID), []byte("other.output.v1"))
		}, IdentityMismatch},
		{"identity precedes data validation", func(b []byte) []byte {
			b = bytes.ReplaceAll(b, []byte(storeTestSchemaID), []byte("other.output.v1"))
			return bytes.Replace(b, []byte(`"原始資料"`), []byte(`42`), 1)
		}, IdentityMismatch},
	}
	for _, field := range []string{"run_id", "invocation_id", "attempt_id", "dispatch_token"} {
		cases = append(cases, struct {
			name   string
			change func([]byte) []byte
			code   Code
		}{
			"wrong " + field, func(b []byte) []byte {
				var v map[string]any
				if err := json.Unmarshal(b, &v); err != nil {
					t.Fatal(err)
				}
				v["meta"].(map[string]any)[field] = strings.Repeat("f", 32)
				return storeTestJSON(t, v)
			}, IdentityMismatch,
		})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			raw := tc.change(storeTestCandidate(t, id))
			if raw != nil {
				storeTestWrite(t, a.CandidatePath(), raw)
			}
			staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			if tc.code != "" {
				storeTestCode(t, err, tc.code)
				if staged != nil {
					t.Fatal("invalid candidate yielded a snapshot")
				}
				ref, publishErr := a.Publish(context.Background(), staged)
				if publishErr == nil || ref != (Ref{}) {
					t.Fatalf("invalid candidate published: %+v %v", ref, publishErr)
				}
			} else {
				if err != nil || staged == nil {
					t.Fatalf("Stage = %v, %v", staged, err)
				}
				ref, err := a.Publish(context.Background(), staged)
				if err != nil {
					t.Fatal(err)
				}
				got, err := s.Read(context.Background(), ref)
				if err != nil || !bytes.Equal(got, raw) {
					t.Fatalf("Read did not preserve raw candidate: %s %v", got, err)
				}
			}
			storeTestReport(t, a, id, tc.code == "")
			if tc.code != "" {
				storeTestUnpublished(t, a)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		schema string
		number string
		valid  bool
	}{
		{"unsupported positive exponent", `{"type":"number","minimum":0}`, "1e1000001", false},
		{"unsupported negative number", `{"type":"number","minimum":0}`, "-1e1000001", false},
		{"unsupported negative exponent", `{"type":"number","minimum":0}`, "1e-1000001", false},
		{"large exact number", `{"type":"number","minimum":1e400,"maximum":1e400}`, "1e400", true},
		{"small exact number", `{"type":"number","minimum":1e-400,"maximum":1e-400}`, "1e-400", true},
		{"exact integer beyond float64", `{"type":"number","minimum":9007199254740993,"maximum":9007199254740993}`, "9007199254740993", true},
		{"adjacent integer below bound", `{"type":"number","minimum":9007199254740993}`, "9007199254740992", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if caught := recover(); caught != nil {
					t.Errorf("Store panicked on numeric candidate %s: %v", tc.number, caught)
				}
			}()
			s, err := NewStore(schemaTestRegistry(t, tc.schema), Options{BaseDir: t.TempDir(), Prompt: "numeric bounds"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			a, id := storeTestAttempt(t, s, "")
			raw := bytes.Replace(storeTestCandidate(t, id), []byte(`{"answer":"原始資料"}`), []byte(tc.number), 1)
			storeTestWrite(t, a.CandidatePath(), raw)
			staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			if !tc.valid {
				storeTestCode(t, err, ContractInvalid)
				if staged != nil {
					t.Fatal("invalid numeric candidate yielded a snapshot")
				}
				if ref, publishErr := a.Publish(context.Background(), staged); publishErr == nil || ref != (Ref{}) {
					t.Fatalf("invalid numeric candidate published: %+v %v", ref, publishErr)
				}
				storeTestUnpublished(t, a)
			} else {
				if err != nil || staged == nil {
					t.Fatalf("Stage = %v, %v", staged, err)
				}
				ref, err := a.Publish(context.Background(), staged)
				if err != nil || ref == (Ref{}) {
					t.Fatalf("Publish = %+v, %v", ref, err)
				}
				got, err := s.Read(context.Background(), ref)
				if err != nil || !bytes.Equal(got, raw) {
					t.Fatalf("numeric precision/raw bytes changed: %s %v", got, err)
				}
			}
			storeTestReport(t, a, id, tc.valid)
		})
	}
}

func TestStoreRejectsNonregularCandidates(t *testing.T) {
	for _, kind := range []string{"directory", "FIFO", "symlink inside", "symlink outside"} {
		t.Run(kind, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			var err error
			switch kind {
			case "directory":
				err = os.Mkdir(a.CandidatePath(), 0700)
			case "FIFO":
				err = syscall.Mkfifo(a.CandidatePath(), 0600)
			default:
				target := filepath.Join(a.Dir(), "evidence", "candidate.json")
				if kind == "symlink outside" {
					target = filepath.Join(t.TempDir(), "candidate.json")
				}
				storeTestWrite(t, target, storeTestCandidate(t, id))
				err = os.Symlink(target, a.CandidatePath())
			}
			if err != nil {
				t.Fatal(err)
			}
			staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			storeTestCode(t, err, ContractInvalid)
			if staged != nil {
				t.Fatal("nonregular candidate staged")
			}
			storeTestUnpublished(t, a)
		})
	}
}

func TestStoreFileReferences(t *testing.T) {
	cases := []struct {
		name  string
		paths []string
		setup string
		code  Code
	}{
		{"valid evidence and artifact", []string{"evidence/a", "artifacts/b"}, "", ""},
		{"absolute", []string{"/tmp/outside"}, "", ContractInvalid},
		{"dotdot", []string{"evidence/../artifacts/a"}, "", ContractInvalid},
		{"escape", []string{"../outside"}, "", ContractInvalid},
		{"noncanonical dot", []string{"evidence/./a"}, "", ContractInvalid},
		{"double separator", []string{"evidence//a"}, "", ContractInvalid},
		{"wrong root", []string{"request.json"}, "", ContractInvalid},
		{"backslash", []string{`evidence/..\a`}, "", ContractInvalid},
		{"missing", []string{"evidence/a"}, "missing", ContractInvalid},
		{"directory", []string{"evidence/a"}, "directory", ContractInvalid},
		{"FIFO", []string{"evidence/a"}, "FIFO", ContractInvalid},
		{"leaf symlink inside", []string{"evidence/a"}, "leaf inside", ContractInvalid},
		{"leaf symlink outside", []string{"evidence/a"}, "leaf outside", ContractInvalid},
		{"intermediate symlink inside", []string{"evidence/link/a"}, "intermediate inside", ContractInvalid},
		{"intermediate symlink outside", []string{"evidence/link/a"}, "intermediate outside", ContractInvalid},
		{"duplicate IDs", []string{"evidence/a", "artifacts/b"}, "duplicate IDs", ContractInvalid},
		{"duplicate path", []string{"evidence/a", "evidence/a"}, "", ContractInvalid},
		{"ancestor before descendant", []string{"evidence/a", "evidence/a/b"}, "", ContractInvalid},
		{"descendant before ancestor", []string{"evidence/a/b", "evidence/a"}, "", ContractInvalid},
		{"case aliases", []string{"evidence/a", "evidence/A"}, "case aliases", ContractInvalid},
		{"hardlink aliases", []string{"evidence/a", "artifacts/b"}, "hardlink", ContractInvalid},
		{"kind does not match path", []string{"artifacts/a"}, "wrong kind", ContractInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			entries := make([]fileEntry, 0, len(tc.paths))
			for i, path := range tc.paths {
				kind := "evidence"
				if strings.HasPrefix(path, "artifacts/") && tc.setup != "wrong kind" {
					kind = "artifact"
				}
				fileID := fmt.Sprintf("file-%d", i)
				if tc.setup == "duplicate IDs" {
					fileID = "same"
				}
				entries = append(entries, fileEntry{ID: fileID, Kind: kind, Path: path})
			}
			first := filepath.Join(a.Dir(), "evidence", "a")
			switch tc.setup {
			case "missing":
			case "directory":
				if err := os.Mkdir(first, 0700); err != nil {
					t.Fatal(err)
				}
			case "FIFO":
				if err := syscall.Mkfifo(first, 0600); err != nil {
					t.Fatal(err)
				}
			case "leaf inside", "leaf outside", "intermediate inside", "intermediate outside":
				targetDir := filepath.Join(a.Dir(), "artifacts")
				if strings.HasSuffix(tc.setup, "outside") {
					targetDir = t.TempDir()
				}
				target := filepath.Join(targetDir, "a")
				storeTestWrite(t, target, []byte("external sentinel"))
				link := first
				if strings.HasPrefix(tc.setup, "intermediate") {
					target = targetDir
					link = filepath.Join(a.Dir(), "evidence", "link")
				}
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
			case "case aliases":
				storeTestWrite(t, first, []byte("source"))
				alias := filepath.Join(a.Dir(), "evidence", "A")
				original, err := os.Stat(first)
				if err != nil {
					t.Fatal(err)
				}
				aliased, err := os.Stat(alias)
				if errors.Is(err, os.ErrNotExist) {
					if err := os.Link(first, alias); err != nil {
						t.Fatal(err)
					}
					aliased, err = os.Stat(alias)
					t.Log("case-sensitive volume: exercising real hardlink alias")
				} else {
					t.Log("case-insensitive volume: exercising native case alias")
				}
				if err != nil || !os.SameFile(original, aliased) {
					t.Fatalf("case alias does not identify the same source: %v", err)
				}
			case "hardlink":
				storeTestWrite(t, first, []byte("source"))
				if err := os.Link(first, filepath.Join(a.Dir(), "artifacts", "b")); err != nil {
					t.Fatal(err)
				}
			default:
				storeTestWrite(t, first, []byte("source"))
				storeTestWrite(t, filepath.Join(a.Dir(), "artifacts", "a"), []byte("artifact a"))
				storeTestWrite(t, filepath.Join(a.Dir(), "artifacts", "b"), []byte("artifact b"))
			}
			storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id, entries...))
			staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			if tc.code != "" {
				storeTestCode(t, err, tc.code)
				if staged != nil {
					t.Fatal("unsafe files staged")
				}
				storeTestUnpublished(t, a)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			ref, err := a.Publish(context.Background(), staged)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Read(context.Background(), ref); err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !bytes.Equal(storeTestReadFile(t, filepath.Join(a.Dir(), entry.Path)), storeTestReadFile(t, filepath.Join(filepath.Dir(ref.Path), entry.Path))) {
					t.Fatal("published file differs")
				}
			}
		})
	}
}

func TestStoreLimits(t *testing.T) {
	cases := []struct {
		name      string
		sizes     []int
		configure func(*Limits)
		code      Code
	}{
		{"file exact", []int{4}, func(l *Limits) { l.MaxFileBytes = 4 }, ""},
		{"file excess", []int{5}, func(l *Limits) { l.MaxFileBytes = 4 }, LimitExceeded},
		{"aggregate exact", []int{3, 3}, func(l *Limits) { l.MaxAttemptFileBytes = 6 }, ""},
		{"aggregate excess", []int{3, 4}, func(l *Limits) { l.MaxAttemptFileBytes = 6 }, LimitExceeded},
		{"count exact", []int{0, 0}, func(l *Limits) { l.MaxAttemptFiles = 2 }, ""},
		{"count excess", []int{0, 0, 0}, func(l *Limits) { l.MaxAttemptFiles = 2 }, LimitExceeded},
		{"empty file at aggregate limit", []int{4, 0}, func(l *Limits) { l.MaxAttemptFileBytes = 4 }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limits := DefaultLimits()
			tc.configure(&limits)
			s := storeTestNew(t, limits)
			a, id := storeTestAttempt(t, s, "")
			var entries []fileEntry
			for i, n := range tc.sizes {
				entry := fileEntry{ID: fmt.Sprint(i), Kind: "evidence", Path: fmt.Sprintf("evidence/%d", i)}
				entries = append(entries, entry)
				storeTestWrite(t, filepath.Join(a.Dir(), entry.Path), bytes.Repeat([]byte("x"), n))
			}
			storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id, entries...))
			staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			if tc.code != "" {
				storeTestCode(t, err, tc.code)
				if staged != nil {
					t.Fatal("quota violation staged")
				}
				storeTestUnpublished(t, a)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				ref, err := a.Publish(context.Background(), staged)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.Read(context.Background(), ref); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	for _, count := range []int{256, 4096, 500000} {
		t.Run(fmt.Sprintf("invalid items count=%d", count), func(t *testing.T) {
			limits := DefaultLimits()
			limits.MaxAttemptFiles = 2
			s := storeTestNew(t, limits)
			a, id := storeTestAttempt(t, s, "")
			raw := bytes.Replace(storeTestCandidate(t, id), []byte(`"files":[]`), []byte(`"files":[`+strings.Repeat("0,", count-1)+`0]`), 1)
			if int64(len(raw)) > limits.MaxCandidateBytes {
				t.Fatal("invalid items fixture exceeds candidate byte limit")
			}
			storeTestWrite(t, a.CandidatePath(), raw)
			staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			storeTestCode(t, err, LimitExceeded)
			if staged != nil {
				t.Fatal("excess invalid file items yielded snapshot")
			}
			if ref, publishErr := a.Publish(context.Background(), staged); publishErr == nil || ref != (Ref{}) {
				t.Fatalf("excess files published: %+v %v", ref, publishErr)
			}
			report := storeTestReadFile(t, filepath.Join(a.Dir(), "validation.json"))
			if len(report) > 4096 || bytes.Contains(report, []byte("/files/0")) {
				t.Fatalf("file count guard expanded item errors: report bytes=%d", len(report))
			}
			if !bytes.Equal(storeTestReadFile(t, a.CandidatePath()), raw) {
				t.Fatal("file count rejection changed candidate metadata/bytes")
			}
			storeTestReport(t, a, id, false)
			storeTestUnpublished(t, a)
		})
	}
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprintf("candidate byte limit extra=%d", extra), func(t *testing.T) {
			limits := DefaultLimits()
			limits.MaxCandidateBytes = 1024
			s := storeTestNew(t, limits)
			a, id := storeTestAttempt(t, s, "")
			raw := storeTestCandidate(t, id)
			if len(raw) > 1024 {
				t.Fatal("fixture exceeds candidate limit")
			}
			raw = append(raw, bytes.Repeat([]byte(" "), 1024-len(raw)+extra)...)
			storeTestWrite(t, a.CandidatePath(), raw)
			staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			if extra != 0 {
				storeTestCode(t, err, LimitExceeded)
				if staged != nil {
					t.Fatal("oversize candidate staged")
				}
				storeTestUnpublished(t, a)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				ref, err := a.Publish(context.Background(), staged)
				if err != nil {
					t.Fatal(err)
				}
				got, err := s.Read(context.Background(), ref)
				if err != nil || !bytes.Equal(got, raw) {
					t.Fatalf("exact limit failed: %v", err)
				}
			}
		})
	}
}

func TestStoreManifestCapacityUsesGeneratedBytes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		idLength int
		limits   func(*Limits)
	}{
		{"default escaping expansion", 190000, func(*Limits) {}},
		{"small policy escaping expansion", 1500, func(l *Limits) { l.MaxCandidateBytes = 2048; l.MaxAttemptFiles = 1 }},
		{"large candidate limit", 8, func(l *Limits) { l.MaxCandidateBytes = math.MaxInt64 }},
		{"large file count limit", 8, func(l *Limits) { l.MaxAttemptFiles = math.MaxInt }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := DefaultLimits()
			tc.limits(&limits)
			s := storeTestNew(t, limits)
			a, id := storeTestAttempt(t, s, "")
			entry := fileEntry{ID: strings.Repeat("<", tc.idLength), Kind: "evidence", Path: "evidence/x"}
			raw := bytes.ReplaceAll(storeTestCandidate(t, id, entry), []byte(`\u003c`), []byte("<"))
			if int64(len(raw)) > limits.MaxCandidateBytes {
				t.Fatal("fixture exceeds candidate limit")
			}
			storeTestWrite(t, a.CandidatePath(), raw)
			storeTestWrite(t, filepath.Join(a.Dir(), entry.Path), nil)
			ref := storeTestPublish(t, a)
			got, err := s.Read(context.Background(), ref)
			if err != nil || !bytes.Equal(got, raw) {
				t.Fatalf("legal manifest expansion failed: %v", err)
			}
		})
	}
}

func TestStoreCandidateDepthBoundary(t *testing.T) {
	for _, depth := range []int{64, 65} {
		t.Run(fmt.Sprintf("depth=%d", depth), func(t *testing.T) {
			s, err := NewStore(schemaTestRegistry(t, `true`), Options{BaseDir: t.TempDir(), Prompt: "depth boundary"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			a, id := storeTestAttempt(t, s, "")
			var env map[string]any
			if err := json.Unmarshal(storeTestCandidate(t, id), &env); err != nil {
				t.Fatal(err)
			}
			// The envelope root contributes one container to the total depth.
			env["data"] = json.RawMessage(strings.Repeat("[", depth-1) + "0" + strings.Repeat("]", depth-1))
			storeTestWrite(t, a.CandidatePath(), storeTestJSON(t, env))
			staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			if depth > 64 {
				storeTestCode(t, err, ContractInvalid)
				if staged != nil {
					t.Fatal("over-depth candidate staged")
				}
				storeTestUnpublished(t, a)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			ref, err := a.Publish(context.Background(), staged)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Read(context.Background(), ref); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStorePublishesValidatedSnapshot(t *testing.T) {
	s := storeTestNew(t, Limits{})
	a, id := storeTestAttempt(t, s, "")
	entry := fileEntry{ID: "source", Kind: "evidence", Path: "evidence/source"}
	raw := append([]byte(" \n"), storeTestCandidate(t, id, entry)...)
	evidence := []byte("已驗證的 evidence\x00\xff")
	storeTestWrite(t, a.CandidatePath(), raw)
	storeTestWrite(t, filepath.Join(a.Dir(), entry.Path), evidence)
	staged := storeTestStage(t, a)
	storeTestUnpublished(t, a)
	storeTestReport(t, a, id, true)
	storeTestWrite(t, a.CandidatePath(), []byte("not JSON anymore"))
	storeTestWrite(t, filepath.Join(a.Dir(), entry.Path), []byte("changed after Stage"))
	ref, err := a.Publish(context.Background(), staged)
	if err != nil {
		t.Fatal(err)
	}
	if ref.RunID != id.RunID || ref.AttemptID != id.AttemptID || ref.SchemaID != storeTestSchemaID || ref.Path != filepath.Join(a.Dir(), "published", "contract.json") || ref.SHA256 != storeTestHash(raw) {
		t.Fatalf("incorrect publication Ref: %+v", ref)
	}
	got, err := s.Read(context.Background(), ref)
	if err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("snapshot changed: %s %v", got, err)
	}
	if !bytes.Equal(storeTestReadFile(t, filepath.Join(filepath.Dir(ref.Path), entry.Path)), evidence) {
		t.Fatal("published mutable source rather than staged evidence")
	}
	manifestRaw := storeTestReadFile(t, filepath.Join(filepath.Dir(ref.Path), "manifest.json"))
	if ref.ManifestSHA256 != storeTestHash(manifestRaw) {
		t.Fatal("manifest digest not anchored in Ref")
	}
	var m manifest
	if err := json.Unmarshal(manifestRaw, &m); err != nil {
		t.Fatal(err)
	}
	if m.Identity != id || m.SchemaID != storeTestSchemaID || m.ContractSHA256 != ref.SHA256 || len(m.Files) != 1 || m.Files[0].fileEntry != entry || m.Files[0].Size != int64(len(evidence)) || m.Files[0].SHA256 != storeTestHash(evidence) {
		t.Fatalf("incorrect manifest: %+v", m)
	}
	got[0] = 'x'
	again, err := s.Read(context.Background(), ref)
	if err != nil || !bytes.Equal(again, raw) {
		t.Fatalf("returned bytes alias persisted snapshot: %v", err)
	}
	if err := staged.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(context.Background(), ref); err != nil {
		t.Fatalf("Discard removed publication: %v", err)
	}
}

func TestStoreDiscardAndSnapshotOwnership(t *testing.T) {
	s := storeTestNew(t, Limits{})
	a, id := storeTestAttempt(t, s, "")
	b, otherID := storeTestAttempt(t, s, "")
	storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
	storeTestWrite(t, b.CandidatePath(), storeTestCandidate(t, otherID))
	staged := storeTestStage(t, a)
	if ref, err := b.Publish(context.Background(), staged); err == nil || ref != (Ref{}) {
		t.Fatalf("cross-attempt snapshot accepted: %+v %v", ref, err)
	}
	if duplicate, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID}); err == nil || duplicate != nil {
		t.Fatal("second outstanding snapshot accepted")
	}
	for range 2 {
		if err := staged.Discard(); err != nil {
			t.Fatal(err)
		}
	}
	if ref, err := a.Publish(context.Background(), staged); err == nil || ref != (Ref{}) {
		t.Fatalf("discarded snapshot published: %+v %v", ref, err)
	}
	storeTestUnpublished(t, a)
	storeTestUnpublished(t, b)
	ref := storeTestPublish(t, a)
	if _, err := s.Read(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if next, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID}); err == nil || next != nil {
		t.Fatal("published attempt staged again")
	}
}

func TestStoreRejectsStagingTampering(t *testing.T) {
	for _, target := range []string{"contract", "manifest", "artifact", "artifact and manifest", "missing artifact", "directory symlink"} {
		t.Run(target, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			entry := fileEntry{ID: "source", Kind: "artifact", Path: "artifacts/result"}
			storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id, entry))
			storeTestWrite(t, filepath.Join(a.Dir(), entry.Path), []byte("original"))
			staged := storeTestStage(t, a)
			dir := filepath.Join(s.Dir(), staged.rel)
			manifestPath := filepath.Join(dir, "manifest.json")
			artifactPath := filepath.Join(dir, entry.Path)
			var external string
			switch target {
			case "contract":
				storeTestWrite(t, filepath.Join(dir, "contract.json"), []byte(`{}`))
			case "manifest":
				storeTestWrite(t, manifestPath, []byte(`{}`))
			case "artifact":
				storeTestWrite(t, artifactPath, []byte("changed"))
			case "missing artifact":
				if err := os.Remove(artifactPath); err != nil {
					t.Fatal(err)
				}
			case "artifact and manifest":
				var m manifest
				if err := json.Unmarshal(storeTestReadFile(t, manifestPath), &m); err != nil {
					t.Fatal(err)
				}
				changed := []byte("changed")
				storeTestWrite(t, artifactPath, changed)
				m.Files[0].SHA256 = storeTestHash(changed)
				m.Files[0].Size = int64(len(changed))
				storeTestWrite(t, manifestPath, storeTestJSON(t, m))
			case "directory symlink":
				external = filepath.Join(t.TempDir(), "snapshot")
				if err := os.Rename(dir, external); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, dir); err != nil {
					t.Fatal(err)
				}
			}
			ref, err := a.Publish(context.Background(), staged)
			storeTestCode(t, err, ReferenceInvalid)
			if ref != (Ref{}) {
				t.Fatal("tampered staging returned Ref")
			}
			storeTestUnpublished(t, a)
			if err := staged.Discard(); err != nil {
				t.Fatal(err)
			}
			if external != "" && string(storeTestReadFile(t, filepath.Join(external, entry.Path))) != "original" {
				t.Fatal("discard touched external directory")
			}
		})
	}
}

func TestStoreNeverOverwritesPublication(t *testing.T) {
	for _, existing := range []string{"empty directory", "nonempty directory", "file", "symlink", "already published"} {
		t.Run(existing, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
			staged := storeTestStage(t, a)
			path := filepath.Join(a.Dir(), "published")
			var original Ref
			switch existing {
			case "empty directory", "nonempty directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				if existing == "nonempty directory" {
					storeTestWrite(t, filepath.Join(path, "sentinel"), []byte("keep"))
				}
			case "file":
				storeTestWrite(t, path, []byte("keep"))
			case "symlink":
				target := t.TempDir()
				storeTestWrite(t, filepath.Join(target, "sentinel"), []byte("keep"))
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "already published":
				var err error
				original, err = a.Publish(context.Background(), staged)
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := a.Publish(context.Background(), staged)
			if err == nil || ref != (Ref{}) {
				t.Fatalf("existing publication overwritten: %+v %v", ref, err)
			}
			after, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			if !os.SameFile(before, after) || before.Mode() != after.Mode() {
				t.Fatal("existing destination replaced")
			}
			switch existing {
			case "empty directory":
				entries, err := os.ReadDir(path)
				if err != nil || len(entries) != 0 {
					t.Fatalf("empty destination modified: %v %v", entries, err)
				}
			case "nonempty directory", "symlink":
				if string(storeTestReadFile(t, filepath.Join(path, "sentinel"))) != "keep" {
					t.Fatal("sentinel modified")
				}
			case "file":
				if string(storeTestReadFile(t, path)) != "keep" {
					t.Fatal("destination file modified")
				}
			case "already published":
				if _, err := s.Read(context.Background(), original); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestStoreRejectsTamperingAndForgedRefs(t *testing.T) {
	cases := []string{
		"contract", "artifact", "manifest", "artifact and manifest with original Ref",
		"candidate path", "noncanonical path", "relative path", "external path", "cross run", "unknown attempt", "other known attempt", "wrong schema Ref",
		"rehashed run identity", "rehashed invocation identity", "rehashed attempt identity", "rehashed token identity", "rehashed schema identity", "rehashed invalid data",
		"rehashed manifest identity", "rehashed manifest schema", "rehashed manifest file ID", "rehashed manifest size", "rehashed manifest contract digest",
		"artifact symlink", "contract symlink", "manifest symlink",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			entry := fileEntry{ID: "result", Kind: "artifact", Path: "artifacts/result"}
			raw := storeTestCandidate(t, id, entry)
			storeTestWrite(t, a.CandidatePath(), raw)
			storeTestWrite(t, filepath.Join(a.Dir(), entry.Path), []byte("original artifact"))
			ref := storeTestPublish(t, a)
			dir := filepath.Dir(ref.Path)
			manifestPath := filepath.Join(dir, "manifest.json")
			artifactPath := filepath.Join(dir, entry.Path)
			var m manifest
			if err := json.Unmarshal(storeTestReadFile(t, manifestPath), &m); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "contract":
				storeTestWrite(t, ref.Path, append(raw, ' '))
			case "artifact":
				storeTestWrite(t, artifactPath, []byte("tampered artifact"))
			case "manifest":
				storeTestWrite(t, manifestPath, append(storeTestReadFile(t, manifestPath), ' '))
			case "artifact and manifest with original Ref":
				changed := []byte("tampered artifact")
				storeTestWrite(t, artifactPath, changed)
				m.Files[0].SHA256 = storeTestHash(changed)
				m.Files[0].Size = int64(len(changed))
				storeTestWrite(t, manifestPath, storeTestJSON(t, m))
			case "candidate path":
				ref.Path = a.CandidatePath()
			case "noncanonical path":
				ref.Path = dir + "/../published/contract.json"
			case "relative path":
				var err error
				ref.Path, err = filepath.Rel(s.Dir(), ref.Path)
				if err != nil {
					t.Fatal(err)
				}
			case "external path":
				ref.Path = filepath.Join(t.TempDir(), "contract.json")
				storeTestWrite(t, ref.Path, raw)
				storeTestWrite(t, filepath.Join(filepath.Dir(ref.Path), "manifest.json"), storeTestJSON(t, m))
			case "cross run":
				other := storeTestNew(t, Limits{})
				if got, err := other.Read(context.Background(), ref); err == nil || got != nil {
					t.Fatal("other store accepted cross-run Ref")
				}
				ref.RunID = other.RunID()
			case "unknown attempt":
				ref.AttemptID = storeTestID(t)
			case "other known attempt":
				_, other := storeTestAttempt(t, s, "")
				ref.AttemptID = other.AttemptID
			case "wrong schema Ref":
				ref.SchemaID = "other.output.v1"
			case "artifact symlink", "contract symlink", "manifest symlink":
				path := artifactPath
				if name == "contract symlink" {
					path = ref.Path
				}
				if name == "manifest symlink" {
					path = manifestPath
				}
				target := filepath.Join(t.TempDir(), "same-bytes")
				storeTestWrite(t, target, storeTestReadFile(t, path))
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			default:
				var env map[string]any
				if err := json.Unmarshal(raw, &env); err != nil {
					t.Fatal(err)
				}
				meta := env["meta"].(map[string]any)
				switch name {
				case "rehashed run identity":
					meta["run_id"] = storeTestID(t)
					m.Identity.RunID = meta["run_id"].(string)
				case "rehashed invocation identity":
					meta["invocation_id"] = storeTestID(t)
					m.Identity.InvocationID = meta["invocation_id"].(string)
				case "rehashed attempt identity":
					meta["attempt_id"] = storeTestID(t)
					m.Identity.AttemptID = meta["attempt_id"].(string)
				case "rehashed token identity":
					meta["dispatch_token"] = storeTestID(t)
					m.Identity.DispatchToken = meta["dispatch_token"].(string)
				case "rehashed schema identity":
					meta["schema_id"] = "other.output.v1"
					m.SchemaID = "other.output.v1"
					ref.SchemaID = "other.output.v1"
				case "rehashed invalid data":
					env["data"] = map[string]any{"answer": 42}
				case "rehashed manifest identity":
					m.Identity.DispatchToken = storeTestID(t)
				case "rehashed manifest schema":
					m.SchemaID = "other.output.v1"
				case "rehashed manifest file ID":
					m.Files[0].ID = "forged"
				case "rehashed manifest size":
					m.Files[0].Size++
				case "rehashed manifest contract digest":
					m.ContractSHA256 = strings.Repeat("0", 64)
				}
				raw = storeTestJSON(t, env)
				storeTestWrite(t, ref.Path, raw)
				ref.SHA256 = storeTestHash(raw)
				if name != "rehashed manifest contract digest" {
					m.ContractSHA256 = ref.SHA256
				}
				manifestRaw := storeTestJSON(t, m)
				storeTestWrite(t, manifestPath, manifestRaw)
				ref.ManifestSHA256 = storeTestHash(manifestRaw)
			}
			got, err := s.Read(context.Background(), ref)
			storeTestCode(t, err, ReferenceInvalid)
			if got != nil {
				t.Fatal("invalid reference returned consumable bytes")
			}
		})
	}
}

func TestStoreSchemasAndPromptAreExact(t *testing.T) {
	const commonURI = "https://workflow.example/schemas/common.json"
	resources := []Resource{
		{URI: schemaTestURI, JSON: json.RawMessage(" \n" + `{"$ref":"common.json"}` + "\n")},
		{URI: commonURI, JSON: json.RawMessage(storeTestSchema + " \n")},
	}
	registry, err := NewRegistry(resources, []SchemaDefinition{{ID: storeTestSchemaID, URI: schemaTestURI}})
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	marker := filepath.Join(base, "shell-must-not-run")
	prompt := `  中文 '引號' "雙引號" $HOME $(touch ` + marker + ") `touch " + marker + "` ; | & < > * ? \\  "
	s, err := NewStore(registry, Options{BaseDir: base, Prompt: prompt, LaunchCWD: base})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	var input struct {
		Prompt    string `json:"prompt"`
		LaunchCWD string `json:"launch_cwd"`
	}
	if err := json.Unmarshal(storeTestReadFile(t, filepath.Join(s.Dir(), "input.json")), &input); err != nil {
		t.Fatal(err)
	}
	if input.Prompt != prompt || input.LaunchCWD != base {
		t.Fatalf("input changed: %+v", input)
	}
	id := Identity{s.RunID(), storeTestID(t), storeTestID(t), storeTestID(t)}
	a, err := s.BeginAttempt(id, Request{Identity: id, Prompt: prompt, Output: OutputSpec{SchemaID: storeTestSchemaID, Schema: SchemaResource{Path: "/agent/spec", SHA256: "untrusted"}}})
	if err != nil {
		t.Fatal(err)
	}
	var request Request
	if err := json.Unmarshal(storeTestReadFile(t, a.RequestPath()), &request); err != nil {
		t.Fatal(err)
	}
	if request.Identity != id || request.Prompt != prompt || request.Output.SchemaID != storeTestSchemaID {
		t.Fatalf("request changed: %+v", request)
	}
	want := map[string][]byte{schemaTestURI: resources[0].JSON, commonURI: resources[1].JSON, EnvelopeURI: []byte(envelopeJSON)}
	if len(request.Output.Resources) != len(want) {
		t.Fatalf("resource count = %d, want %d", len(request.Output.Resources), len(want))
	}
	for uri, raw := range want {
		resource, exists := request.Output.Resources[uri]
		if !exists {
			t.Fatalf("missing resource %s", uri)
		}
		rel, err := filepath.Rel(filepath.Join(s.Dir(), "schemas"), resource.Path)
		if err != nil || !filepath.IsAbs(resource.Path) || !filepath.IsLocal(rel) {
			t.Fatalf("resource outside run.schemas: %+v %v", resource, err)
		}
		if !bytes.Equal(storeTestReadFile(t, resource.Path), raw) || resource.SHA256 != storeTestHash(raw) {
			t.Fatalf("resource bytes/digest differ: %s", uri)
		}
	}
	if request.Output.Schema != request.Output.Resources[schemaTestURI] || request.Output.Envelope != request.Output.Resources[EnvelopeURI] {
		t.Fatal("output resource selection incorrect")
	}
	for _, resource := range request.Output.Resources {
		storeTestWrite(t, resource.Path, []byte(`false`))
	}
	storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
	ref := storeTestPublish(t, a)
	if _, err := s.Read(context.Background(), ref); err != nil {
		t.Fatalf("disk schema overrode compiled registry: %v", err)
	}
	for _, resource := range request.Output.Resources {
		storeTestWrite(t, resource.Path, []byte(`true`))
	}
	b, otherID := storeTestAttempt(t, s, "")
	storeTestWrite(t, b.CandidatePath(), bytes.Replace(storeTestCandidate(t, otherID), []byte(`"原始資料"`), []byte(`42`), 1))
	if staged, err := b.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID}); err == nil || staged != nil {
		t.Fatal("tampered schema relaxed binary validation")
	}
	storeTestUnpublished(t, b)
	storeTestAbsent(t, marker)
	for _, path := range []string{s.Dir(), filepath.Join(s.Dir(), "schemas"), a.Dir(), filepath.Join(a.Dir(), "evidence"), filepath.Dir(ref.Path)} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() || info.Mode().Perm() != 0700 {
			t.Fatalf("directory permissions %s: %v", path, info.Mode())
		}
	}
	for _, path := range []string{filepath.Join(s.Dir(), "input.json"), filepath.Join(s.Dir(), "run.json"), a.RequestPath(), filepath.Join(a.Dir(), "validation.json"), ref.Path, filepath.Join(filepath.Dir(ref.Path), "manifest.json"), request.Output.Schema.Path} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			t.Fatalf("file permissions %s: %v", path, info.Mode())
		}
	}
}

func TestStoreCrossBundleSchemas(t *testing.T) {
	resources := []Resource{
		{URI: schemaTestURI, JSON: json.RawMessage(`{"$ref":"common.json"}`)},
		{URI: "https://workflow.example/schemas/bundle.json", JSON: json.RawMessage(`{"$defs":{"common":{"$id":"common.json",` + storeTestSchema[1:] + `}}`)},
	}
	registry, err := NewRegistry(resources, []SchemaDefinition{{ID: storeTestSchemaID, URI: schemaTestURI}})
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewStore(registry, Options{BaseDir: t.TempDir(), Prompt: "embedded schema"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	a, id := storeTestAttempt(t, s, "")
	var request Request
	if err := json.Unmarshal(storeTestReadFile(t, a.RequestPath()), &request); err != nil {
		t.Fatal(err)
	}
	if len(request.Output.Resources) != len(resources)+1 {
		t.Fatal("request does not contain the original resources plus envelope")
	}
	for _, resource := range resources {
		spec, ok := request.Output.Resources[resource.URI]
		if !ok || spec.SHA256 != storeTestHash(resource.JSON) || !bytes.Equal(storeTestReadFile(t, spec.Path), resource.JSON) {
			t.Fatalf("resource drift: %s", resource.URI)
		}
	}
	storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
	ref := storeTestPublish(t, a)
	if _, err := s.Read(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	b, otherID := storeTestAttempt(t, s, "")
	storeTestWrite(t, b.CandidatePath(), bytes.Replace(storeTestCandidate(t, otherID), []byte(`"原始資料"`), []byte(`42`), 1))
	staged, err := b.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
	storeTestCode(t, err, ContractInvalid)
	if staged != nil {
		t.Fatal("embedded schema did not reject invalid data")
	}
}

func TestStoreCreatesIndependentRuns(t *testing.T) {
	realBase := t.TempDir()
	base := filepath.Join(t.TempDir(), "linked-base")
	if err := os.Symlink(realBase, base); err != nil {
		t.Fatal(err)
	}
	canonicalBase, err := filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	registry := schemaTestRegistry(t, storeTestSchema)
	seenTasks, seenRuns, seenIDs := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, prompt := range []string{"第一個 task", "第二個 task"} {
		s, err := NewStore(registry, Options{BaseDir: base, Prompt: prompt, Workflow: "acceptance", WorkflowVersion: "v1", LaunchCWD: base})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		if s.TaskID() == "" || s.RunID() == "" || seenTasks[s.TaskID()] || seenRuns[s.RunID()] {
			t.Fatal("automatic task/run IDs are empty or reused")
		}
		seenTasks[s.TaskID()] = true
		seenRuns[s.RunID()] = true
		if s.Dir() != filepath.Join(canonicalBase, s.TaskID(), "runs", s.RunID()) {
			t.Fatalf("unexpected run layout: %s", s.Dir())
		}
		var metadata struct {
			TaskID          string `json:"task_id"`
			RunID           string `json:"run_id"`
			Workflow        string `json:"workflow"`
			WorkflowVersion string `json:"workflow_version"`
			LaunchCWD       string `json:"launch_cwd"`
		}
		if err := json.Unmarshal(storeTestReadFile(t, filepath.Join(s.Dir(), "run.json")), &metadata); err != nil {
			t.Fatal(err)
		}
		if metadata.TaskID != s.TaskID() || metadata.RunID != s.RunID() || metadata.Workflow != "acceptance" || metadata.WorkflowVersion != "v1" || metadata.LaunchCWD != base {
			t.Fatalf("run identity/config not persisted: %+v", metadata)
		}
		a, id := storeTestAttempt(t, s, "")
		for _, value := range []string{id.InvocationID, id.AttemptID, id.DispatchToken} {
			if value == "" || seenIDs[value] {
				t.Fatal("NewID reused an identity across calls")
			}
			seenIDs[value] = true
		}
		storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
		ref := storeTestPublish(t, a)
		if _, err := s.Read(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(base)
	if err != nil || len(entries) != len(seenTasks) {
		t.Fatalf("tasks not independently retained: %v %v", entries, err)
	}
}

func TestStorePreflightDoesNotCreateDirectories(t *testing.T) {
	cases := []struct {
		name     string
		prompt   string
		registry string
		limits   Limits
	}{
		{"empty prompt", "", "valid", Limits{}},
		{"whitespace prompt", " \t ", "valid", Limits{}},
		{"CR", "a\rb", "valid", Limits{}},
		{"LF", "a\nb", "valid", Limits{}},
		{"NUL", "a\x00b", "valid", Limits{}},
		{"invalid UTF8", "\xff", "valid", Limits{}},
		{"oversize prompt", strings.Repeat("中", (64<<10)/3+1), "valid", Limits{}},
		{"nil registry", "ok", "nil", Limits{}},
		{"uncompiled registry", "ok", "zero", Limits{}},
		{"partial limits", "ok", "valid", Limits{MaxPromptBytes: 64}},
		{"negative limits", "ok", "valid", Limits{MaxPromptBytes: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parent := t.TempDir()
			base := filepath.Join(parent, "must-not-exist")
			registry := schemaTestRegistry(t, storeTestSchema)
			if tc.registry == "nil" {
				registry = nil
			}
			if tc.registry == "zero" {
				registry = &Registry{}
			}
			s, err := NewStore(registry, Options{BaseDir: base, Prompt: tc.prompt, Limits: tc.limits})
			if s != nil {
				_ = s.Close()
				t.Fatal("invalid preflight returned a store")
			}
			storeTestCode(t, err, InvalidDefinition)
			storeTestAbsent(t, base)
			entries, err := os.ReadDir(parent)
			if err != nil || len(entries) != 0 {
				t.Fatalf("preflight created files: %v %v", entries, err)
			}
		})
	}
	for _, prompt := range []string{strings.Repeat("x", 64<<10), strings.Repeat("中", (64<<10)/3) + "x"} {
		t.Run(fmt.Sprintf("exact byte limit %d", len(prompt)), func(t *testing.T) {
			s, err := NewStore(schemaTestRegistry(t, storeTestSchema), Options{BaseDir: t.TempDir(), Prompt: prompt})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStoreAttemptHistoryAndIdentityReuse(t *testing.T) {
	s := storeTestNew(t, Limits{})
	a, first := storeTestAttempt(t, s, "")
	storeTestAbsent(t, a.CandidatePath())
	request := storeTestReadFile(t, a.RequestPath())
	original := storeTestCandidate(t, first)
	storeTestWrite(t, a.CandidatePath(), original)
	firstRef := storeTestPublish(t, a)
	b, second := storeTestAttempt(t, s, first.InvocationID)
	if a.Number() != 1 || b.Number() != 2 || first.AttemptID == second.AttemptID || first.DispatchToken == second.DispatchToken || a.Dir() == b.Dir() {
		t.Fatalf("attempt identity/number reused: %+v %+v", first, second)
	}
	if filepath.Base(a.Dir()) != "0001-"+first.AttemptID || filepath.Base(b.Dir()) != "0002-"+second.AttemptID {
		t.Fatal("attempt paths do not preserve numbered history")
	}
	storeTestWrite(t, b.CandidatePath(), original)
	if staged, err := b.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID}); staged != nil || err == nil {
		t.Fatal("previous complete valid contract accepted for new attempt")
	} else {
		storeTestCode(t, err, IdentityMismatch)
	}
	storeTestUnpublished(t, b)
	secondRaw := storeTestCandidate(t, second)
	storeTestWrite(t, b.CandidatePath(), secondRaw)
	secondRef := storeTestPublish(t, b)
	c, _ := storeTestAttempt(t, s, "")
	if c.Number() != 1 {
		t.Fatal("new invocation did not start independent numbering")
	}
	for _, tc := range []struct {
		name string
		id   Identity
	}{
		{"attempt ID", Identity{s.RunID(), storeTestID(t), first.AttemptID, storeTestID(t)}},
		{"dispatch token", Identity{s.RunID(), storeTestID(t), storeTestID(t), first.DispatchToken}},
		{"entire identity", first},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := s.BeginAttempt(tc.id, Request{Identity: tc.id, Output: OutputSpec{SchemaID: storeTestSchemaID}})
			storeTestCode(t, err, InvalidDefinition)
			if got != nil {
				t.Fatal("reused identity returned attempt")
			}
		})
	}
	for _, tc := range []struct {
		ref Ref
		raw []byte
	}{{firstRef, original}, {secondRef, secondRaw}} {
		got, err := s.Read(context.Background(), tc.ref)
		if err != nil || !bytes.Equal(got, tc.raw) {
			t.Fatalf("history lost: %+v %v", tc.ref, err)
		}
	}
	if !bytes.Equal(storeTestReadFile(t, a.RequestPath()), request) || !bytes.Equal(storeTestReadFile(t, a.CandidatePath()), original) {
		t.Fatal("earlier attempt history modified")
	}
}

func TestStoreFailedAndDiscardedAttemptsReserveIdentity(t *testing.T) {
	for _, state := range []string{"invalid", "discarded"} {
		t.Run(state, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			if state == "invalid" {
				staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
				storeTestCode(t, err, ContractMissing)
				if staged != nil {
					t.Fatal("missing candidate staged")
				}
			} else {
				storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
				if err := storeTestStage(t, a).Discard(); err != nil {
					t.Fatal(err)
				}
			}
			for _, identity := range []Identity{
				{s.RunID(), id.InvocationID, id.AttemptID, storeTestID(t)},
				{s.RunID(), id.InvocationID, storeTestID(t), id.DispatchToken},
			} {
				got, err := s.BeginAttempt(identity, Request{Identity: identity, Output: OutputSpec{SchemaID: storeTestSchemaID}})
				storeTestCode(t, err, InvalidDefinition)
				if got != nil {
					t.Fatal("failed/discarded identity reused")
				}
			}
			b, _ := storeTestAttempt(t, s, id.InvocationID)
			if b.Number() != 2 {
				t.Fatalf("failed/discarded attempt not counted: %d", b.Number())
			}
			storeTestUnpublished(t, a)
		})
	}
}

func TestStoreAttemptPreflightAndWrongSpec(t *testing.T) {
	for _, name := range []string{"wrong run", "bad invocation", "bad attempt", "bad token", "request identity", "unknown schema"} {
		t.Run(name, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			id := Identity{s.RunID(), storeTestID(t), storeTestID(t), storeTestID(t)}
			request := Request{Identity: id, Output: OutputSpec{SchemaID: storeTestSchemaID}}
			switch name {
			case "wrong run":
				id.RunID = storeTestID(t)
				request.Identity = id
			case "bad invocation":
				id.InvocationID = "../escape"
				request.Identity = id
			case "bad attempt":
				id.AttemptID = "short"
				request.Identity = id
			case "bad token":
				id.DispatchToken = ""
				request.Identity = id
			case "request identity":
				request.Identity.DispatchToken = storeTestID(t)
			case "unknown schema":
				request.Output.SchemaID = "unknown"
			}
			a, err := s.BeginAttempt(id, request)
			storeTestCode(t, err, InvalidDefinition)
			if a != nil {
				t.Fatal("invalid request created attempt")
			}
			entries, err := os.ReadDir(filepath.Join(s.Dir(), "steps"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("invalid attempt created directories: %v %v", entries, err)
			}
		})
	}
	s := storeTestNew(t, Limits{})
	a, id := storeTestAttempt(t, s, "")
	storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
	if staged, err := a.Stage(context.Background(), Spec{SchemaID: "other"}); err == nil || staged != nil {
		t.Fatal("wrong Stage spec accepted")
	}
	storeTestUnpublished(t, a)
}

func TestStoreParallelAttempts(t *testing.T) {
	s := storeTestNew(t, Limits{})
	invocation := storeTestID(t)
	const workers = 12
	type result struct {
		a   *Attempt
		id  Identity
		ref Ref
		raw []byte
		err error
	}
	results := make(chan result, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		id := Identity{s.RunID(), invocation, storeTestID(t), storeTestID(t)}
		raw := storeTestCandidate(t, id)
		wg.Go(func() {
			<-start
			got := result{id: id, raw: raw}
			defer func() { results <- got }()
			got.a, got.err = s.BeginAttempt(id, Request{Identity: id, Output: OutputSpec{SchemaID: storeTestSchemaID}})
			if got.err != nil {
				return
			}
			got.err = os.WriteFile(got.a.CandidatePath(), raw, 0600)
			if got.err != nil {
				return
			}
			var staged *Staged
			staged, got.err = got.a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
			if got.err != nil {
				return
			}
			got.ref, got.err = got.a.Publish(context.Background(), staged)
		})
	}
	close(start)
	wg.Wait()
	close(results)
	numbers, ids, tokens := map[int]bool{}, map[string]bool{}, map[string]bool{}
	for got := range results {
		if got.err != nil {
			t.Errorf("parallel attempt %s: %v", got.id.AttemptID, got.err)
			continue
		}
		if numbers[got.a.Number()] || ids[got.id.AttemptID] || tokens[got.id.DispatchToken] {
			t.Fatal("parallel attempts reused identity/number")
		}
		numbers[got.a.Number()] = true
		ids[got.id.AttemptID] = true
		tokens[got.id.DispatchToken] = true
		raw, err := s.Read(context.Background(), got.ref)
		if err != nil || !bytes.Equal(raw, got.raw) {
			t.Fatalf("parallel publication corrupted: %v", err)
		}
	}
	for number := 1; number <= workers; number++ {
		if !numbers[number] {
			t.Errorf("missing attempt number %d", number)
		}
	}
}

func TestStorePublishRace(t *testing.T) {
	s := storeTestNew(t, Limits{})
	const workers = 12
	var wg sync.WaitGroup
	a, id := storeTestAttempt(t, s, "")
	storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
	staged := storeTestStage(t, a)
	type publication struct {
		ref Ref
		err error
	}
	published := make(chan publication, workers)
	start := make(chan struct{})
	for range workers {
		wg.Go(func() {
			<-start
			ref, err := a.Publish(context.Background(), staged)
			published <- publication{ref, err}
		})
	}
	close(start)
	wg.Wait()
	close(published)
	succeeded := 0
	for got := range published {
		if got.err != nil {
			if got.ref != (Ref{}) {
				t.Fatal("failed publication returned Ref")
			}
			continue
		}
		succeeded++
		if _, err := s.Read(context.Background(), got.ref); err != nil {
			t.Fatal(err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("publish race succeeded %d times, want exactly once", succeeded)
	}
}

func TestStoreConcurrentIdentityReservation(t *testing.T) {
	for _, reuse := range []string{"attempt ID", "token"} {
		t.Run(reuse, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			id := Identity{s.RunID(), storeTestID(t), storeTestID(t), storeTestID(t)}
			const workers = 12
			type result struct {
				a   *Attempt
				err error
			}
			results := make(chan result, workers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for range workers {
				identity := id
				if reuse == "token" {
					identity.AttemptID = storeTestID(t)
				} else {
					identity.DispatchToken = storeTestID(t)
				}
				wg.Go(func() {
					<-start
					a, err := s.BeginAttempt(identity, Request{Identity: identity, Output: OutputSpec{SchemaID: storeTestSchemaID}})
					results <- result{a, err}
				})
			}
			close(start)
			wg.Wait()
			close(results)
			succeeded := 0
			for got := range results {
				if got.err != nil {
					storeTestCode(t, got.err, InvalidDefinition)
					if got.a != nil {
						t.Fatal("duplicate identity returned attempt")
					}
				} else {
					succeeded++
					if got.a == nil || got.a.Number() != 1 {
						t.Fatal("identity reservation did not create first attempt")
					}
				}
			}
			if succeeded != 1 {
				t.Fatalf("identity reservation succeeded %d times", succeeded)
			}
		})
	}
}

func TestStoreCloseFailsClosed(t *testing.T) {
	for _, phase := range []string{"begin", "stage", "publish", "read"} {
		t.Run(phase, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
			var staged *Staged
			var ref Ref
			if phase == "publish" {
				staged = storeTestStage(t, a)
			}
			if phase == "read" {
				ref = storeTestPublish(t, a)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			switch phase {
			case "begin":
				got, err := s.BeginAttempt(id, Request{Identity: id, Output: OutputSpec{SchemaID: storeTestSchemaID}})
				storeTestCode(t, err, InvalidDefinition)
				if got != nil {
					t.Fatal("closed store created attempt")
				}
			case "stage":
				got, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
				storeTestCode(t, err, InvalidDefinition)
				if got != nil {
					t.Fatal("closed store staged data")
				}
			case "publish":
				got, err := a.Publish(context.Background(), staged)
				storeTestCode(t, err, InvalidDefinition)
				if got != (Ref{}) {
					t.Fatal("closed store published Ref")
				}
			case "read":
				got, err := s.Read(context.Background(), ref)
				storeTestCode(t, err, ReferenceInvalid)
				if got != nil {
					t.Fatal("closed store returned data")
				}
			}
		})
	}
}

func TestStoreCancellationPreservesTypedCause(t *testing.T) {
	for _, phase := range []string{"Stage before read", "Stage during copy", "Stage during evidence destination copy", "Stage during artifact destination copy", "Publish", "Read"} {
		t.Run(phase, func(t *testing.T) {
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
			cause := &Error{Code: LimitExceeded, Phase: "run-deadline", Message: "typed controller stop"}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			var err error
			switch phase {
			case "Stage before read", "Stage during copy", "Stage during evidence destination copy", "Stage during artifact destination copy":
				if phase == "Stage before read" {
					cancel(cause)
				} else {
					entry := fileEntry{ID: "source", Kind: "evidence", Path: "evidence/source"}
					if phase == "Stage during artifact destination copy" {
						entry.Kind, entry.Path = "artifact", "artifacts/source"
					}
					storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id, entry))
					storeTestWrite(t, filepath.Join(a.Dir(), entry.Path), []byte("referenced source"))
					calls := 0
					s.afterCopy = func(path string) {
						if phase == "Stage during copy" {
							if path != filepath.Join(a.rel, "candidate.json") {
								return
							}
						} else if !strings.HasPrefix(path, ".staging"+string(filepath.Separator)) || !strings.HasSuffix(path, string(filepath.Separator)+filepath.FromSlash(entry.Path)) {
							return
						}
						calls++
						cancel(cause)
					}
					defer func() {
						if calls != 1 {
							t.Errorf("cancel barrier calls=%d", calls)
						}
					}()
				}
				var staged *Staged
				staged, err = a.Stage(ctx, Spec{SchemaID: storeTestSchemaID})
				if staged != nil {
					t.Fatal("cancelled Stage returned snapshot")
				}
				storeTestReport(t, a, id, false)
				if ref, publishErr := a.Publish(context.Background(), staged); publishErr == nil || ref != (Ref{}) {
					t.Fatalf("cancelled snapshot published: %+v %v", ref, publishErr)
				}
			case "Publish":
				staged := storeTestStage(t, a)
				cancel(cause)
				var ref Ref
				ref, err = a.Publish(ctx, staged)
				if ref != (Ref{}) {
					t.Fatal("cancelled Publish returned Ref")
				}
				if discardErr := staged.Discard(); discardErr != nil {
					t.Fatal(discardErr)
				}
			case "Read":
				ref := storeTestPublish(t, a)
				cancel(cause)
				var raw json.RawMessage
				raw, err = s.Read(ctx, ref)
				if raw != nil {
					t.Fatal("cancelled Read returned bytes")
				}
			}
			var typed *Error
			if err != context.Cause(ctx) || !errors.Is(err, cause) || !errors.As(err, &typed) || typed != cause {
				t.Fatalf("typed cancellation lost: %T %v", err, err)
			}
			if phase != "Read" {
				storeTestUnpublished(t, a)
			}
		})
	}
}

func TestStoreFilesystemFailuresYieldNoRef(t *testing.T) {
	for _, phase := range []string{"validation report", "rename parent", "atomic report partial write"} {
		t.Run(phase, func(t *testing.T) {
			if phase == "atomic report partial write" && os.Getenv("CONTRACT_TEST_FSIZE_CHILD") != "1" {
				executable, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				var before, after syscall.Rlimit
				if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &before); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run=^TestStoreFilesystemFailuresYieldNoRef$/^atomic_report_partial_write$", "-test.v")
				cmd.Env = append(os.Environ(), "CONTRACT_TEST_FSIZE_CHILD=1")
				output, err := cmd.CombinedOutput()
				if err != nil {
					t.Errorf("isolated file-size fault test: %v\n%s", err, output)
				} else {
					t.Logf("isolated file-size fault test:\n%s", output)
				}
				if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &after); err != nil {
					t.Fatal(err)
				}
				if before != after {
					t.Fatalf("child changed parent file-size limit: %+v -> %+v", before, after)
				}
				return
			}
			s := storeTestNew(t, Limits{})
			a, id := storeTestAttempt(t, s, "")
			storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id))
			switch phase {
			case "atomic report partial write":
				report := filepath.Join(a.Dir(), "validation.json")
				sentinel := filepath.Join(a.Dir(), ".tmp-sentinel")
				for _, path := range []string{report, sentinel} {
					storeTestWrite(t, path, []byte("non-owned sentinel"))
				}
				// Restrict only the child after Store/candidate creation, so both
				// staging and the atomic report encounter real partial OS writes.
				signal.Ignore(syscall.SIGXFSZ)
				var limit syscall.Rlimit
				if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
					t.Fatal(err)
				}
				restricted := limit
				restricted.Cur = 64
				if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &restricted); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limit); err != nil {
						t.Error(err)
					}
				}()
				staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
				storeTestCode(t, err, StorageFailed)
				var reportErr, stageErr *Error
				if !errors.As(err, &reportErr) || reportErr.Phase != "validation-report" || !errors.Is(reportErr.Cause, syscall.EFBIG) {
					t.Errorf("expected actual report file-size write failure, got %v", err)
				} else {
					var writeErr *os.PathError
					if !errors.As(reportErr.Cause, &writeErr) || writeErr.Op != "write" || !strings.HasPrefix(filepath.Base(writeErr.Path), ".tmp-") {
						t.Errorf("report did not fail writing its owned temp: %v", reportErr.Cause)
					}
					if !errors.As(reportErr.Cause, &stageErr) || stageErr.Code != StorageFailed || stageErr.Phase != "stage" || !errors.Is(stageErr, syscall.EFBIG) {
						t.Errorf("expected actual staging file-size write failure, got %v", reportErr.Cause)
					}
				}
				if staged != nil {
					t.Error("partially written report yielded snapshot")
				}
				if ref, publishErr := a.Publish(context.Background(), staged); publishErr == nil || ref != (Ref{}) {
					t.Errorf("partially written snapshot published: %+v %v", ref, publishErr)
				}
				storeTestUnpublished(t, a)
				if err := filepath.WalkDir(a.Dir(), func(path string, entry os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if strings.HasPrefix(entry.Name(), ".tmp-") && path != sentinel {
						t.Errorf("failed atomic write left owned temp: %s", path)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{report, sentinel} {
					if string(storeTestReadFile(t, path)) != "non-owned sentinel" {
						t.Errorf("failed atomic write changed non-owned path: %s", path)
					}
				}
				entries, err := os.ReadDir(filepath.Join(s.Dir(), ".staging"))
				if err != nil || len(entries) != 0 {
					t.Errorf("failed Stage left private snapshot: %v %v", entries, err)
				}
			case "validation report":
				path := filepath.Join(a.Dir(), "validation.json")
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				storeTestWrite(t, filepath.Join(path, "sentinel"), []byte("keep"))
				staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
				storeTestCode(t, err, StorageFailed)
				if staged != nil {
					t.Fatal("unwritable report yielded snapshot")
				}
				if ref, err := a.Publish(context.Background(), staged); err == nil || ref != (Ref{}) {
					t.Fatal("unreported snapshot published")
				}
				storeTestUnpublished(t, a)
				if string(storeTestReadFile(t, filepath.Join(path, "sentinel"))) != "keep" {
					t.Fatal("report error destroyed existing path")
				}
			default:
				staged := storeTestStage(t, a)
				path := a.Dir()
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				storeTestWrite(t, path, []byte("parent is now a file"))
				ref, err := a.Publish(context.Background(), staged)
				storeTestCode(t, err, StorageFailed)
				if ref != (Ref{}) {
					t.Fatal("failed rename returned Ref")
				}
				storeTestAbsent(t, filepath.Join(path+".saved", "published"))
				if err := staged.Discard(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestStoreDetectsSourceChangesDuringCopy(t *testing.T) {
	for _, target := range []string{"candidate", "evidence"} {
		for _, mutation := range []string{"in-place", "replace", "append", "restored mtime digest", "replace with symlink"} {
			t.Run(target+"/"+mutation, func(t *testing.T) {
				s := storeTestNew(t, Limits{})
				a, id := storeTestAttempt(t, s, "")
				entry := fileEntry{ID: "source", Kind: "evidence", Path: "evidence/source"}
				storeTestWrite(t, a.CandidatePath(), storeTestCandidate(t, id, entry))
				evidencePath := filepath.Join(a.Dir(), entry.Path)
				storeTestWrite(t, evidencePath, []byte("original evidence"))
				path := a.CandidatePath()
				if target == "evidence" {
					path = evidencePath
				}
				rel, err := filepath.Rel(s.Dir(), path)
				if err != nil {
					t.Fatal(err)
				}
				before, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				original := storeTestReadFile(t, path)
				changed := bytes.Clone(original)
				changed[len(changed)-1] ^= 1
				calls := 0
				s.afterCopy = func(copied string) {
					if copied != rel {
						return
					}
					calls++
					switch mutation {
					case "in-place", "restored mtime digest":
						storeTestWrite(t, path, changed)
						if mutation == "restored mtime digest" {
							if err := os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
								t.Fatal(err)
							}
							after, err := os.Stat(path)
							if err != nil {
								t.Fatal(err)
							}
							if !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
								t.Fatal("fixture did not restore metadata")
							}
						}
					case "replace":
						storeTestWrite(t, path+".replacement", original)
						if err := os.Chtimes(path+".replacement", before.ModTime(), before.ModTime()); err != nil {
							t.Fatal(err)
						}
						if err := os.Rename(path+".replacement", path); err != nil {
							t.Fatal(err)
						}
					case "replace with symlink":
						target := filepath.Join(t.TempDir(), "outside")
						storeTestWrite(t, target, original)
						if err := os.Remove(path); err != nil {
							t.Fatal(err)
						}
						if err := os.Symlink(target, path); err != nil {
							t.Fatal(err)
						}
					case "append":
						f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
						if err != nil {
							t.Fatal(err)
						}
						_, writeErr := f.Write([]byte("append"))
						if err := errors.Join(writeErr, f.Close()); err != nil {
							t.Fatal(err)
						}
					}
				}
				staged, err := a.Stage(context.Background(), Spec{SchemaID: storeTestSchemaID})
				storeTestCode(t, err, ContractInvalid)
				if calls != 1 || staged != nil {
					t.Fatalf("barrier calls=%d staged=%v", calls, staged)
				}
				storeTestUnpublished(t, a)
				storeTestReport(t, a, id, false)
			})
		}
	}
}

type boundedTestReader func([]byte) (int, error)

func (r boundedTestReader) Read(p []byte) (int, error) { return r(p) }

func TestReadBoundedReaderBoundary(t *testing.T) {
	sentinelIO := errors.New("reader failure")
	for _, tc := range []struct {
		name, source, want string
		limit              int64
		finalErr, wantErr  error
		preCancel, cancel  bool
		chunk, calls, read int
		wantNil            bool
	}{
		{name: "empty", limit: 3, calls: 1},
		{name: "zero limit empty", calls: 1},
		{name: "exact", source: "abc", limit: 3, want: "abc", calls: 2, read: 3},
		{name: "over limit", source: "abcdef", limit: 3, wantErr: ErrReadLimit, calls: 1, read: 4, wantNil: true},
		{name: "zero limit nonempty", source: "abc", wantErr: ErrReadLimit, calls: 1, read: 1, wantNil: true},
		{name: "EOF with final bytes", source: "abc", limit: 3, finalErr: io.EOF, want: "abc", calls: 1, read: 3},
		{name: "partial with IO error", source: "abc", limit: 9, finalErr: sentinelIO, wantErr: sentinelIO, want: "abc", calls: 1, read: 3},
		{name: "pre cancel", source: "abc", limit: 3, preCancel: true, wantErr: context.Canceled, wantNil: true},
		{name: "cancel during read", source: "abcdef", limit: 9, chunk: 3, cancel: true, want: "abc", wantErr: context.Canceled, calls: 1, read: 3},
		{name: "limit before IO error", source: "abcd", limit: 3, finalErr: sentinelIO, wantErr: ErrReadLimit, calls: 1, read: 4, wantNil: true},
		{name: "limit before EOF", source: "abcd", limit: 3, finalErr: io.EOF, wantErr: ErrReadLimit, calls: 1, read: 4, wantNil: true},
		{name: "multi block bounded", source: strings.Repeat("x", 1<<16), limit: 1 << 15, wantErr: ErrReadLimit, calls: 2, read: (1 << 15) + 1, wantNil: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.preCancel {
				cancel()
			}
			source := strings.NewReader(tc.source)
			calls, total := 0, 0
			r := boundedTestReader(func(p []byte) (int, error) {
				calls++
				if int64(total+len(p)) > tc.limit+1 {
					t.Fatalf("reader requested %d bytes after %d, limit=%d", len(p), total, tc.limit)
				}
				if tc.chunk > 0 && len(p) > tc.chunk {
					p = p[:tc.chunk]
				}
				n, err := source.Read(p)
				total += n
				if tc.cancel {
					cancel()
				}
				if source.Len() == 0 && tc.finalErr != nil {
					err = tc.finalErr
				}
				return n, err
			})
			got, err := ReadBounded(ctx, r, tc.limit)
			if !errors.Is(err, tc.wantErr) || string(got) != tc.want || (tc.wantNil && got != nil) {
				t.Fatalf("got bytes=%q nil=%t err=%v; want bytes=%q nil=%t err=%v", got, got == nil, err, tc.want, tc.wantNil, tc.wantErr)
			}
			if calls != tc.calls || total != tc.read || int64(total) > tc.limit+1 {
				t.Fatalf("reader calls=%d bytes=%d; want calls=%d bytes=%d, limit=%d", calls, total, tc.calls, tc.read, tc.limit)
			}
		})
	}
}

func TestDecodePublicationFreshMutableData(t *testing.T) {
	type data struct {
		Nested map[string][]string   `json:"nested"`
		Groups []map[string][]string `json:"groups"`
	}
	raw := json.RawMessage(`{"data":{"nested":{"key":["original"]},"groups":[{"key":["original"]}]},"files":[{"id":"evidence","kind":"artifact","path":"original.txt"}]}`)
	original := bytes.Clone(raw)
	first, err := DecodePublication[data](raw)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DecodePublication[data](raw)
	if err != nil {
		t.Fatal(err)
	}
	first.Data.Nested["key"][0] = "changed"
	first.Data.Nested["added"] = []string{"changed"}
	first.Data.Groups[0]["key"][0] = "changed"
	first.Data.Groups[0]["added"] = []string{"changed"}
	first.Files[0].ID = "changed"
	first.Files[0].Path = "changed.txt"
	third, err := DecodePublication[data](raw)
	if err != nil {
		t.Fatal(err)
	}
	want := Publication[data]{
		Data:  data{Nested: map[string][]string{"key": {"original"}}, Groups: []map[string][]string{{"key": {"original"}}}},
		Files: []FileEntry{{ID: "evidence", Kind: "artifact", Path: "original.txt"}},
	}
	if !reflect.DeepEqual(second, want) || !reflect.DeepEqual(third, want) || !bytes.Equal(raw, original) {
		t.Fatalf("decode shared mutable state: second=%+v third=%+v rawChanged=%t", second, third, !bytes.Equal(raw, original))
	}
}

func TestDecodePublicationUnmarshalCompatibility(t *testing.T) {
	type data struct {
		Answer string `json:"answer"`
	}
	for _, tc := range []struct{ name, raw string }{
		{"unknown fields", `{"data":{"answer":"yes","unknown":true},"unknown":true}`},
		{"duplicate fields", `{"data":{"answer":"first","answer":"last"}}`},
		{"case insensitive fields", `{"DATA":{"ANSWER":"yes"},"FILES":[]}`},
		{"null", `null`},
		{"missing fields", `{}`},
		{"declarations without authorization", `{"files":[{"id":"","kind":"unknown","path":"../outside"},{"id":"","path":"/outside"}]}`},
		{"trailing JSON", `{"data":{"answer":"yes"}} {}`},
		{"type error preserves partial data", `{"data":{"answer":1},"files":[{"id":"retained"}]}`},
		{"malformed JSON", `{"data":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want Publication[data]
			wantErr := json.Unmarshal([]byte(tc.raw), &want)
			got, err := DecodePublication[data](json.RawMessage(tc.raw))
			if !reflect.DeepEqual(got, want) || reflect.TypeOf(err) != reflect.TypeOf(wantErr) || fmt.Sprint(err) != fmt.Sprint(wantErr) {
				t.Fatalf("got=%+v err=%v; Unmarshal=%+v err=%v", got, err, want, wantErr)
			}
		})
	}
}
