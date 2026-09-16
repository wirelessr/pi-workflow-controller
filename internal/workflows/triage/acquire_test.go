package triage

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func acquireRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "evidence"), 0700); err != nil {
		t.Fatal(err)
	}
	return root
}

func acquireFixture(t *testing.T, mode string, bundle []byte, mime string, observe ...func(*http.Request)) *httptest.Server {
	t.Helper()
	_, raw := intakeFixture("complete")
	raw["bundle"] = bundle
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, record := range observe {
			record(r)
		}
		if r.Header.Get("Authorization") != "" {
			t.Error("HTTP fixture received Authorization")
		}
		var body []byte
		switch r.URL.Path {
		case "/rest/api/3/issue/CASE-17":
			if r.URL.Query().Get("fields") != "*all" {
				t.Error("not all fields")
			}
			var issue map[string]any
			if err := json.Unmarshal(raw["issue"], &issue); err != nil {
				t.Error(err)
				return
			}
			issue["fields"].(map[string]any)["attachment"] = []any{map[string]any{"id": "a1", "size": len(bundle), "filename": "../../untrusted", "content": server.URL + "/attachment?signature=private", "mimeType": mime}}
			body = testJSON(issue)
			if mode == "malformed-issue" {
				body = []byte(`{"key":`)
			}
			if mode == "issue-failure" {
				w.WriteHeader(http.StatusServiceUnavailable)
				body = []byte("issue source unavailable")
			}
		case "/rest/api/3/field":
			body = raw["fields"]
			if mode == "malformed-fields" {
				body = []byte(`{}`)
			}
		case "/rest/api/3/issue/CASE-18":
			body = raw["linked"]
			if mode == "linked-failure" {
				w.WriteHeader(403)
			}
		case "/rest/api/3/issue/CASE-17/comment":
			start := r.URL.Query().Get("startAt")
			body = raw["page-"+start]
			if mode == "page-short" && start == "0" {
				body = []byte(`{"startAt":0,"total":2,"maxResults":100,"comments":[{"id":"c1","body":"first"}]}`)
			}
			if start == "1" {
				switch mode {
				case "page-failure":
					w.WriteHeader(503)
				case "page-malformed":
					body = []byte(`{"startAt":`)
				case "page-empty":
					body = []byte(`{"startAt":1,"total":2,"comments":[]}`)
				case "page-offset":
					body = []byte(`{"startAt":0,"total":2,"comments":[{"id":"c2","body":"second"}]}`)
				case "page-duplicate":
					body = []byte(`{"startAt":1,"total":2,"comments":[{"id":"c1","body":"duplicate"}]}`)
				case "page-total":
					body = []byte(`{"startAt":1,"total":3,"comments":[{"id":"c2","body":"second"}]}`)
				case "page-short":
					body = []byte(`{"startAt":1,"total":2,"maxResults":100,"comments":[]}`)
				case "page-null":
					body = []byte(`{"startAt":1,"total":2,"comments":[{"id":"c2","body":null}]}`)
				case "page-partial":
					w.Header().Set("Content-Length", "9999")
				}
			}
		case "/attachment":
			body = bundle
			if mode == "attachment-partial" {
				body = body[:len(body)-1]
			}
		default:
			w.WriteHeader(404)
			return
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	return server
}

func acquireMetadata(t *testing.T, root string) (metadata struct {
	Partial bool                 `json:"partial"`
	Limited bool                 `json:"limited"`
	Records []acquisitionReceipt `json:"records"`
}) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "evidence", "acquisition-metadata"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"signature=", "private", "must-not-send", "test-secret", "secret-query", "Authorization", "Cookie"} {
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("metadata leaked %q", secret)
		}
	}
	for _, record := range metadata.Records {
		if record.Kind != "http" {
			continue
		}
		u, err := url.Parse(record.Origin)
		if err != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			t.Fatalf("metadata origin contains more than scheme/host: %q", record.Origin)
		}
	}
	return
}

func TestAcquireIntake(t *testing.T) {
	for _, mode := range []string{"complete", "malformed-issue", "malformed-fields", "linked-failure", "page-failure", "page-malformed", "page-empty", "page-offset", "page-duplicate", "page-total", "page-short", "page-partial", "page-null", "attachment-partial", "page-limit", "file-limit", "byte-limit", "aggregate-limit"} {
		t.Run(mode, func(t *testing.T) {
			var issueRequests atomic.Int32
			server := acquireFixture(t, mode, []byte("event at 2025-01-02T00:30:00+02:00\n"), "text/plain", func(r *http.Request) {
				if r.URL.Path == "/rest/api/3/issue/CASE-17" {
					issueRequests.Add(1)
				}
			})
			options := acquisitionOptions{BaseURL: server.URL, Authorization: "must-not-send"}
			switch mode {
			case "page-limit":
				options.MaxPages = 1
			case "file-limit":
				options.MaxFiles = 3
			case "byte-limit":
				options.MaxBytes = 32
			case "aggregate-limit":
				options.MaxTotalBytes = 1500
			}
			root := acquireRoot(t)
			started := time.Now()
			v, files, err := acquireIntake(context.Background(), root, testScope(), options)
			if err != nil {
				t.Fatal(err)
			}
			if v.Complete != (mode == "complete") || v.Complete != (len(v.Gaps) == 0) {
				t.Fatalf("completeness: %+v", v)
			}
			if v.URL != "https://jira.example.invalid/browse/CASE-17" {
				t.Fatal(v.URL)
			}
			if err := options.limits(); err != nil {
				t.Fatal(err)
			}
			var total int64
			ids := map[string]bool{}
			for _, f := range files {
				if ids[f.ID] || f.Path != "evidence/"+f.ID {
					t.Fatalf("bad generated entry: %+v", f)
				}
				ids[f.ID] = true
				info, err := os.Stat(filepath.Join(root, f.Path))
				if err != nil {
					t.Fatal(err)
				}
				total += info.Size()
				if info.Size() > options.MaxBytes {
					t.Fatal("file cap exceeded")
				}
			}
			if len(files) > options.MaxFiles || total > options.MaxTotalBytes {
				t.Fatal("aggregate cap exceeded")
			}
			if issueRequests.Load() > 1 {
				t.Fatal("issue fetched more than once")
			}
			if mode == "complete" {
				if issueRequests.Load() != 1 {
					t.Fatal("issue not fetched exactly once")
				}
				for _, id := range []string{"acquisition-metadata", "issue", "fields", "page-0", "page-1", "linked", "bundle"} {
					if !ids[id] {
						t.Errorf("missing %s", id)
					}
				}
				if len(v.Linked) != 1 || len(v.Attachments) != 1 || len(v.Comments) != 2 {
					t.Fatalf("inventory: %+v", v)
				}
				raw, err := os.ReadFile(filepath.Join(root, "evidence", "issue"))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(raw, []byte("?signature=private")) {
					t.Fatal("raw content URL rewritten")
				}
				if !bytes.Contains(raw, []byte(strings.Repeat("unabridged ", 80))) {
					t.Fatal("raw issue abridged")
				}
			}
			if strings.HasPrefix(mode, "page-") && mode != "page-limit" {
				s := v.Comments[len(v.Comments)-1].Source
				wantID := "page-1"
				if mode == "page-short" {
					wantID = "page-0"
				}
				if s.Status == "available" || s.FileID != wantID || !ids[wantID] {
					t.Fatalf("bad partial page: %+v", s)
				}
			}
			if ids["acquisition-metadata"] {
				metadata := acquireMetadata(t, root)
				if metadata.Partial != !v.Complete {
					t.Fatal("metadata completeness mismatch")
				}
				if mode == "complete" && (metadata.Limited || len(metadata.Records) != 7) {
					t.Fatalf("successful receipts missing: %+v", metadata)
				}
				for _, r := range metadata.Records {
					if r.Source.FileID != "" && !ids[r.Source.FileID] {
						t.Fatalf("unknown metadata file: %+v", r)
					}
					if r.Kind != "http" {
						continue
					}
					fetched, err := time.Parse(time.RFC3339Nano, r.FetchedAt)
					if err != nil || fetched.Before(started) || fetched.After(time.Now()) {
						t.Fatalf("invalid fetched timestamp: %q", r.FetchedAt)
					}
					wantStatus := 200
					if mode == "page-failure" && r.SourceID == "page-1" {
						wantStatus = 503
					}
					if mode == "linked-failure" && r.SourceID == "linked" {
						wantStatus = 403
					}
					if r.HTTPStatus != wantStatus {
						t.Fatalf("HTTP receipt: %+v", r)
					}
					if strings.HasPrefix(r.SourceID, "page-") {
						wantOffset := 0
						if r.SourceID == "page-1" {
							wantOffset = 1
						}
						if r.RequestedOffset == nil || *r.RequestedOffset != wantOffset {
							t.Fatalf("offset receipt: %+v", r)
						}
						for _, p := range v.Comments {
							if p.Source.FileID == r.Source.FileID && p.Source != r.Source {
								t.Fatal("partial page diagnostic lost")
							}
						}
					}
					if r.SourceID == "issue" && r.Source != v.Issue {
						t.Fatal("invalid raw diagnostic lost")
					}
				}
			}
			if mode == "page-null" {
				body, err := os.ReadFile(filepath.Join(root, "evidence", "page-1"))
				if err != nil || !bytes.Contains(body, []byte(`"body":null`)) {
					t.Fatal("null-body raw evidence lost")
				}
			}
			encoded := string(testJSON(v))
			if strings.Contains(encoded, "private") || strings.Contains(encoded, "must-not-send") {
				t.Fatal("diagnostic leaked credentials")
			}
		})
	}
}

func TestAcquireRedirects(t *testing.T) {
	for _, mode := range []string{"same", "cross", "direct-cross", "return", "userinfo", "downgrade", "loop"} {
		t.Run(mode, func(t *testing.T) {
			var received []string
			var mu sync.Mutex
			record := func(r *http.Request) {
				mu.Lock()
				received = append(received, r.Header.Get("Authorization"))
				mu.Unlock()
			}
			var home, foreign *httptest.Server
			foreign = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record(r)
				if mode == "return" {
					http.Redirect(w, r, home.URL+"/end", 302)
				} else {
					_, _ = io.WriteString(w, "foreign")
				}
			}))
			defer foreign.Close()
			plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("downgrade followed") }))
			defer plain.Close()
			home = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record(r)
				if r.URL.Path == "/end" {
					_, _ = io.WriteString(w, "complete")
					return
				}
				target := home.URL + "/end"
				switch mode {
				case "cross", "return":
					target = foreign.URL
				case "userinfo":
					target = strings.Replace(home.URL, "https://", "https://user:secret@", 1)
				case "downgrade":
					target = plain.URL
				case "loop":
					target = home.URL
				}
				w.Header().Set("Location", target+"?signature=secret-query")
				w.WriteHeader(302)
				_, _ = io.WriteString(w, "redirect body")
			}))
			defer home.Close()
			base, _ := url.Parse(home.URL)
			client := acquisitionClient(base, "Bearer test-secret")
			defer client.CloseIdleConnections()
			pool := x509.NewCertPool()
			pool.AddCert(home.Certificate())
			pool.AddCert(foreign.Certificate())
			client.Transport.(*http.Transport).TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
			root := acquireRoot(t)
			dir, err := os.OpenRoot(filepath.Join(root, "evidence"))
			if err != nil {
				t.Fatal(err)
			}
			defer dir.Close()
			options := acquisitionOptions{Authorization: "Bearer test-secret"}
			if err := options.limits(); err != nil {
				t.Fatal(err)
			}
			a := intakeAcquirer{ctx: context.Background(), root: dir, base: base, client: client, options: options}
			if err := a.reserveMetadata(); err != nil {
				t.Fatal(err)
			}
			target := home.URL
			if mode == "direct-cross" {
				target = foreign.URL
			}
			source, err := a.fetch("body", target)
			if err != nil {
				t.Fatal(err)
			}
			wantAvailable := mode == "same" || mode == "cross" || mode == "direct-cross" || mode == "return"
			if (source.Status == "available") != wantAvailable {
				t.Fatalf("source: %+v", source)
			}
			mu.Lock()
			got := append([]string(nil), received...)
			mu.Unlock()
			for i, auth := range got {
				want := "Bearer test-secret"
				if mode == "direct-cross" || (mode == "cross" || mode == "return") && i > 0 {
					want = ""
				}
				if auth != want {
					t.Fatalf("request %d auth=%q want %q", i, auth, want)
				}
			}
			if mode == "return" && len(got) != 3 {
				t.Fatal("missing return redirect")
			}
			intake := Intake{Issue: source, Complete: source.Status == "available"}
			if err := a.finishMetadata(&intake); err != nil {
				t.Fatal(err)
			}
			metadata := acquireMetadata(t, root)
			if len(metadata.Records) != len(got) {
				t.Fatal("redirect receipt count mismatch")
			}
			for i, receipt := range metadata.Records {
				wantStatus := 302
				if wantAvailable && i == len(metadata.Records)-1 {
					wantStatus = 200
				}
				if receipt.HTTPStatus != wantStatus || receipt.Source.FileID == "" {
					t.Fatalf("redirect receipt: %+v", receipt)
				}
			}
			for _, entry := range a.files {
				raw, err := os.ReadFile(filepath.Join(root, entry.Path))
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(raw, []byte("test-secret")) || bytes.Contains(raw, []byte("secret-query")) {
					t.Fatal("leaked transport secrets")
				}
			}
			if len(a.files) == 0 {
				t.Fatal("redirect body not retained")
			}
		})
	}
}

func TestAcquireTextClassification(t *testing.T) {
	for _, tc := range []struct {
		name, body, mime, status string
		maxFiles                 int
	}{
		{name: "pk-text", body: "PKCS#12 import failed\n", mime: "text/plain", status: "available"},
		{name: "short-pk", body: "PK", mime: "text/plain", status: "available"},
		{name: "no-copy-at-file-cap", body: "event text", mime: "text/plain", status: "available", maxFiles: 7},
		{name: "broken-zip-mime", body: "not an archive", mime: "application/zip", status: "partial"},
		{name: "broken-zip-signature", body: "PK\x03\x04broken archive", mime: "application/octet-stream", status: "partial"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := acquireFixture(t, "complete", []byte(tc.body), tc.mime)
			root := acquireRoot(t)
			v, files, err := acquireIntake(context.Background(), root, testScope(), acquisitionOptions{BaseURL: server.URL, MaxFiles: tc.maxFiles})
			if err != nil {
				t.Fatal(err)
			}
			if len(v.Attachments) != 1 || v.Attachments[0].Analysis.Status != tc.status || v.Complete != (tc.status == "available") {
				t.Fatalf("classification: %+v", v)
			}
			if tc.status == "available" {
				if v.Attachments[0].Analysis != v.Attachments[0].Content || len(files) != 7 {
					t.Fatal("plain text consumed duplicate evidence quota")
				}
				metadata := acquireMetadata(t, root)
				if len(metadata.Records) != 7 || metadata.Records[6].Kind != "extraction" || metadata.Records[6].Source != v.Attachments[0].Content {
					t.Fatal("text classification receipt lost")
				}
			}
			raw, err := os.ReadFile(filepath.Join(root, "evidence", "bundle"))
			if err != nil || string(raw) != tc.body {
				t.Fatal("classification changed raw attachment")
			}
		})
	}
}

// Inject only the filesystem ReaderAt boundary; ZIP parsing/extraction is real.
type failingArchiveReader struct {
	io.ReaderAt
	header bool
	fault  error
}

func (r failingArchiveReader) ReadAt(p []byte, offset int64) (int, error) {
	if (offset == 0) == r.header {
		return 0, r.fault
	}
	return r.ReaderAt.ReadAt(p, offset)
}

func TestAcquireZIPReadFailure(t *testing.T) {
	for _, header := range []bool{false, true} {
		name := "directory"
		if header {
			name = "entry-header"
		}
		t.Run(name, func(t *testing.T) {
			var buffer bytes.Buffer
			writer := zip.NewWriter(&buffer)
			entry, err := writer.CreateHeader(&zip.FileHeader{Name: "event.txt", Method: zip.Store})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(entry, strings.Repeat("event", 1024)); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			root := acquireRoot(t)
			dir, err := os.OpenRoot(filepath.Join(root, "evidence"))
			if err != nil {
				t.Fatal(err)
			}
			defer dir.Close()
			options := acquisitionOptions{}
			if err := options.limits(); err != nil {
				t.Fatal(err)
			}
			a := intakeAcquirer{ctx: context.Background(), root: dir, options: options}
			if err := a.reserveMetadata(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := a.finishMetadata(&Intake{}); err != nil {
					t.Error(err)
				}
			}()
			content, err := a.save("bundle", bytes.NewReader(buffer.Bytes()))
			if err != nil {
				t.Fatal(err)
			}
			f, err := dir.Open(content.FileID)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			fault := &os.PathError{Op: "read", Path: "bundle", Err: syscall.EIO}
			s, err := a.extractZIP(failingArchiveReader{ReaderAt: f, header: header, fault: fault}, int64(buffer.Len()), content, "extracted")
			if !errors.Is(err, fault) || s.Status == "available" {
				t.Fatalf("filesystem failure became a data gap: source=%+v error=%v", s, err)
			}
		})
	}
}

func TestAcquireZIP(t *testing.T) {
	for _, tc := range []struct {
		name  string
		names []string
		mode  os.FileMode
		body  string
		want  string
		limit int64
		count int
	}{
		{name: "text", names: []string{"folder/a.txt", "b.txt"}, body: "event text\n", want: "available"},
		{name: "traversal", names: []string{"../escape"}, body: "text", want: "unsafe"},
		{name: "internal-traversal", names: []string{"a/../b"}, body: "text", want: "unsafe"},
		{name: "absolute", names: []string{"/escape"}, body: "text", want: "unsafe"},
		{name: "backslash", names: []string{"a\\b"}, body: "text", want: "unsafe"},
		{name: "drive", names: []string{"C:/escape"}, body: "text", want: "unsafe"},
		{name: "duplicate", names: []string{"a", "a"}, body: "text", want: "unsafe"},
		{name: "alias", names: []string{"a", "./a"}, body: "text", want: "unsafe"},
		{name: "symlink", names: []string{"link"}, mode: os.ModeSymlink | 0600, body: "target", want: "unsafe"},
		{name: "special", names: []string{"fifo"}, mode: os.ModeNamedPipe | 0600, body: "text", want: "unsafe"},
		{name: "binary", names: []string{"picture"}, body: "\x00\xff", want: "unsupported"},
		{name: "expanded", names: []string{"large"}, body: strings.Repeat("a", 20000), want: "too-large", limit: 4096},
		{name: "count", names: []string{"a", "b"}, body: "text", want: "too-large", count: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buffer bytes.Buffer
			writer := zip.NewWriter(&buffer)
			for _, name := range tc.names {
				h := &zip.FileHeader{Name: name, Method: zip.Deflate}
				if tc.mode != 0 {
					h.SetMode(tc.mode)
				}
				entry, err := writer.CreateHeader(h)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = io.WriteString(entry, tc.body); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			server := acquireFixture(t, "complete", buffer.Bytes(), "application/zip")
			root := acquireRoot(t)
			v, files, err := acquireIntake(context.Background(), root, testScope(), acquisitionOptions{BaseURL: server.URL, MaxBytes: tc.limit, MaxFiles: tc.count})
			if err != nil {
				t.Fatal(err)
			}
			if len(v.Attachments) != 1 || v.Attachments[0].Analysis.Status != tc.want {
				t.Fatalf("attachment: %+v", v.Attachments)
			}
			metadata := acquireMetadata(t, root)
			mapped := map[string]Source{}
			for _, r := range metadata.Records {
				if r.Kind != "zip-entry" {
					continue
				}
				if r.ArchiveFileID != "bundle" {
					t.Fatalf("archive mapping: %+v", r)
				}
				mapped[r.Filename] = r.Source
			}
			if tc.want == "available" {
				if len(mapped) != len(tc.names) || mapped[tc.names[0]].FileID != "extracted" || mapped[tc.names[1]].FileID != "extracted-entry-1" {
					t.Fatalf("ZIP mappings: %+v", mapped)
				}
			}
			if tc.name == "binary" && (mapped[tc.names[0]].Status != "unsupported" || mapped[tc.names[0]].FileID != "extracted") {
				t.Fatalf("partial ZIP mapping: %+v", mapped)
			}
			if tc.count != 0 && (len(files) > tc.count || len(mapped) != 0) {
				t.Fatal("ZIP file cap bypassed")
			}
			if v.Complete != (tc.want == "available") {
				t.Fatal("false completeness")
			}
			if tc.want == "available" {
				if v.Attachments[0].Analysis.FileID != "extracted" {
					t.Fatal("first extracted ID")
				}
				if len(files) != 9 {
					t.Fatalf("extracted inventory: %+v", files)
				}
			}
		})
	}
}

func TestAcquireMetadataQuota(t *testing.T) {
	for _, tc := range []struct {
		name     string
		maxFiles int
		maxTotal int64
		longName bool
	}{
		{name: "one-file", maxFiles: 1},
		{name: "two-files", maxFiles: 2},
		{name: "tiny-aggregate", maxTotal: 1},
		{name: "reserved-aggregate", maxTotal: 2048},
		{name: "partial-zip-mapping", longName: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buffer bytes.Buffer
			z := zip.NewWriter(&buffer)
			names := []string{"first.txt", "second.txt"}
			if tc.longName {
				names[1] = strings.Repeat("x", 5000)
			}
			for _, name := range names {
				w, err := z.Create(name)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.WriteString(w, "event text"); err != nil {
					t.Fatal(err)
				}
			}
			if err := z.Close(); err != nil {
				t.Fatal(err)
			}
			server := acquireFixture(t, "complete", buffer.Bytes(), "application/zip")
			options := acquisitionOptions{BaseURL: server.URL, MaxFiles: tc.maxFiles, MaxTotalBytes: tc.maxTotal}
			// The raw archive must fit, while its escaped filename exceeds metadata quota.
			if tc.longName {
				options.MaxTotalBytes = 20000
			}
			if err := options.limits(); err != nil {
				t.Fatal(err)
			}
			root := acquireRoot(t)
			v, files, err := acquireIntake(context.Background(), root, testScope(), options)
			if err != nil {
				t.Fatal(err)
			}
			if v.Complete || len(v.Gaps) == 0 {
				t.Fatal("quota claimed complete")
			}
			var total int64
			for _, f := range files {
				info, err := os.Stat(filepath.Join(root, f.Path))
				if err != nil {
					t.Fatal(err)
				}
				if info.Size() > options.MaxBytes {
					t.Fatal("per-file cap bypassed")
				}
				total += info.Size()
			}
			entries, err := os.ReadDir(filepath.Join(root, "evidence"))
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != len(files) || len(files) > options.MaxFiles || total > options.MaxTotalBytes {
				t.Fatal("metadata bypassed aggregate/file budget")
			}
			if tc.longName {
				m := acquireMetadata(t, root)
				if !m.Limited || !m.Partial {
					t.Fatal("metadata truncation not explicit")
				}
				var mappings []acquisitionReceipt
				for _, r := range m.Records {
					if r.Kind == "zip-entry" {
						mappings = append(mappings, r)
					}
				}
				if len(mappings) != 1 || mappings[0].Filename != "first.txt" || mappings[0].Source != acquired("extracted") {
					t.Fatalf("successful mapping lost: %+v", mappings)
				}
				if _, err := os.Stat(filepath.Join(root, "evidence", "extracted-entry-1")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("extracted entry without mapping")
				}
				if len(v.Attachments) != 1 || v.Attachments[0].Analysis.Status != "too-large" || v.Attachments[0].Analysis.FileID != "extracted" {
					t.Fatalf("partial extraction lost: %+v", v.Attachments)
				}
			}
		})
	}
}

func TestAcquireUnsafeURLs(t *testing.T) {
	for _, target := range []string{"", "http://example.invalid/a?signature=secret", "file:///etc/passwd", "https://user:secret@example.invalid/a", "https://example.invalid/a#fragment"} {
		t.Run(strings.SplitN(target, ":", 2)[0], func(t *testing.T) {
			root := acquireRoot(t)
			dir, err := os.OpenRoot(filepath.Join(root, "evidence"))
			if err != nil {
				t.Fatal(err)
			}
			defer dir.Close()
			base, _ := url.Parse("https://jira.example.invalid")
			options := acquisitionOptions{Authorization: "secret"}
			if err := options.limits(); err != nil {
				t.Fatal(err)
			}
			a := intakeAcquirer{ctx: context.Background(), root: dir, options: options, base: base, client: acquisitionClient(base, "secret")}
			defer a.client.CloseIdleConnections()
			source, err := a.fetch("attachment", target)
			if err != nil || source.Status != "unsafe" || len(a.files) != 0 {
				t.Fatalf("%+v %v", source, err)
			}
			if strings.Contains(source.Reason, "secret") {
				t.Fatal("secret leaked")
			}
		})
	}
}

func TestAcquireCancellationDuringBody(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("cancel streaming fixture")
	defer cancel(cause)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"key":`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	root := acquireRoot(t)
	done := make(chan error, 1)
	go func() {
		_, _, err := acquireIntake(ctx, root, testScope(), acquisitionOptions{BaseURL: server.URL})
		done <- err
	}()
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
waiting:
	for {
		select {
		case <-deadline:
			cancel(cause)
			t.Fatal("body did not reach evidence")
		case <-tick.C:
			info, err := os.Stat(filepath.Join(root, "evidence", "issue"))
			if err == nil && info.Size() > 0 {
				break waiting
			}
		}
	}
	cancel(cause)
	select {
	case err := <-done:
		if err != cause {
			t.Fatalf("cause changed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not join")
	}
	raw, err := os.ReadFile(filepath.Join(root, "evidence", "issue"))
	if err != nil || string(raw) != `{"key":` {
		t.Fatalf("partial raw lost: %q %v", raw, err)
	}
	metadata := acquireMetadata(t, root)
	if !metadata.Partial || len(metadata.Records) != 1 || metadata.Records[0].HTTPStatus != 200 || metadata.Records[0].Source.FileID != "issue" || metadata.Records[0].Source.Status != "partial" || metadata.Records[0].Source.Reason != "acquisition interrupted" {
		t.Fatalf("cancelled HTTP metadata lost: %+v", metadata)
	}
}

func TestAcquireErrors(t *testing.T) {
	for _, mode := range []string{"cancel", "collision", "evidence-symlink", "invalid-base", "invalid-limit", "unsafe-attachment", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			server := acquireFixture(t, "complete", []byte("\x00picture"), "image/png")
			options := acquisitionOptions{BaseURL: server.URL}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("fixture cancelled")
			root := acquireRoot(t)
			switch mode {
			case "cancel":
				cancel(cause)
			case "collision":
				if err := os.WriteFile(filepath.Join(root, "evidence", "issue"), []byte("owned"), 0600); err != nil {
					t.Fatal(err)
				}
			case "evidence-symlink":
				if err := os.Remove(filepath.Join(root, "evidence")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(t.TempDir(), filepath.Join(root, "evidence")); err != nil {
					t.Fatal(err)
				}
			case "invalid-base":
				options.BaseURL = "http://example.invalid"
			case "invalid-limit":
				options.MaxFiles = 128
			case "unsafe-attachment":
				options.BaseURL = "https://user:secret@example.invalid"
			}
			v, _, err := acquireIntake(ctx, root, testScope(), options)
			if mode == "unsupported" {
				if err != nil || v.Attachments[0].Analysis.Status != "unsupported" {
					t.Fatalf("%+v %v", v, err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if mode == "cancel" && err != cause {
				t.Fatalf("cancel cause lost: %v", err)
			}
			if mode == "collision" {
				raw, e := os.ReadFile(filepath.Join(root, "evidence", "issue"))
				if e != nil || string(raw) != "owned" {
					t.Fatal("overwrote evidence")
				}
			}
			if strings.Contains(fmt.Sprint(err), "secret") {
				t.Fatal("error leaked secret")
			}
		})
	}
}
