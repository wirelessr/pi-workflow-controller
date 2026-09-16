package triage

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type acquisitionOptions struct {
	BaseURL       string
	Authorization string
	MaxBytes      int64
	MaxFiles      int
	MaxTotalBytes int64
	MaxPages      int
}

var acquisitionKey = regexp.MustCompile(`^[A-Z][A-Z0-9_]*-[1-9][0-9]*$`)

// Zero limits select conservative defaults; callers may only lower these caps.
func (o *acquisitionOptions) limits() error {
	if o.MaxBytes == 0 {
		o.MaxBytes = 8 << 20
	}
	if o.MaxTotalBytes == 0 {
		o.MaxTotalBytes = 64 << 20
	}
	if o.MaxFiles == 0 {
		o.MaxFiles = 96
	}
	if o.MaxPages == 0 {
		o.MaxPages = 32
	}
	if o.MaxBytes < 1 || o.MaxBytes > 8<<20 || o.MaxTotalBytes < 1 || o.MaxTotalBytes > 64<<20 || o.MaxFiles < 1 || o.MaxFiles > 96 || o.MaxPages < 1 || o.MaxPages > 32 {
		return errors.New("invalid acquisition limits")
	}
	return nil
}

func acquisitionURL(u *url.URL) bool {
	if u == nil || u.User != nil || u.Host == "" || u.Opaque != "" || u.Fragment != "" {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	ip := net.ParseIP(u.Hostname())
	return u.Scheme == "http" && (u.Hostname() == "localhost" || ip != nil && ip.IsLoopback())
}
func acquisitionOrigin(a, b *url.URL) bool { return a.Scheme == b.Scheme && a.Host == b.Host }

func acquisitionClient(base *url.URL, authorization string) *http.Client {
	// Do not inherit a process proxy or mutate http.DefaultTransport.
	transport := &http.Transport{TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 15 * time.Second, DisableCompression: true}
	return &http.Client{Transport: transport, Timeout: 30 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		req.Header.Del("Authorization")
		if len(via) >= 10 || !acquisitionURL(req.URL) {
			return errors.New("redirect rejected")
		}
		trusted := req.URL.Scheme == "https" && acquisitionOrigin(base, req.URL)
		for _, previous := range via {
			if previous.URL.Scheme == "https" && req.URL.Scheme != "https" {
				return errors.New("redirect downgrade rejected")
			}
			trusted = trusted && acquisitionOrigin(base, previous.URL)
		}
		if trusted {
			req.Header.Set("Authorization", authorization)
		}
		return nil
	}}
}

type intakeAcquirer struct {
	ctx             context.Context
	root            *os.Root
	options         acquisitionOptions
	base            *url.URL
	client          *http.Client
	files           []file
	total           int64
	metadata        *os.File
	metadataQuota   int64
	metadataUsed    int64
	metadataLimited bool
	receipts        []*acquisitionReceipt
}

type acquisitionReceipt struct {
	Kind            string `json:"kind"`
	SourceID        string `json:"source_id"`
	FetchedAt       string `json:"fetched_at,omitempty"`
	HTTPStatus      int    `json:"http_status,omitempty"`
	Origin          string `json:"origin,omitempty"`
	RequestedOffset *int   `json:"requested_comment_offset,omitempty"`
	ArchiveFileID   string `json:"archive_file_id,omitempty"`
	Filename        string `json:"filename,omitempty"`
	Source          Source `json:"source"`
}

// Reserve one file and a bounded share of the aggregate budget before raw IO.
func (a *intakeAcquirer) reserveMetadata() error {
	a.metadataQuota = min(int64(64<<10), a.options.MaxBytes, a.options.MaxTotalBytes/4)
	if a.metadataQuota < 128 {
		a.metadataLimited = true
		return nil
	}
	f, err := a.root.OpenFile("acquisition-metadata", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	a.metadata = f
	a.files = append(a.files, file{ID: "acquisition-metadata", Kind: "evidence", Path: "evidence/acquisition-metadata"})
	a.total += a.metadataQuota
	a.metadataUsed = 128
	return nil
}

func (a *intakeAcquirer) receipt(r acquisitionReceipt) *acquisitionReceipt {
	// Allow room for the eventual timestamp, generated file ID and fixed diagnostic.
	raw, _ := json.Marshal(r)
	cost := int64(len(raw) + 256)
	if a.metadata == nil || a.metadataUsed+cost > a.metadataQuota {
		a.metadataLimited = true
		return nil
	}
	a.metadataUsed += cost
	a.receipts = append(a.receipts, &r)
	return &r
}

func (a *intakeAcquirer) finishMetadata(v *Intake) (err error) {
	v.Acquisition = &Source{Status: "available", FileID: "acquisition-metadata"}
	if a.metadataLimited {
		v.Complete = false
		v.Gaps = append(v.Gaps, "acquisition metadata limit")
		v.Acquisition.Status, v.Acquisition.Reason = "partial", "acquisition metadata limit"
	}
	if a.metadata == nil {
		v.Acquisition.FileID = ""
		return nil
	}
	defer func() { err = errors.Join(err, a.metadata.Close()) }()
	// Validation may qualify an otherwise successful HTTP response after fetch.
	sources := []Source{v.Issue, v.Fields}
	for _, p := range v.Comments {
		sources = append(sources, p.Source)
	}
	for _, l := range v.Linked {
		sources = append(sources, l.Source)
	}
	for _, at := range v.Attachments {
		sources = append(sources, at.Content)
	}
	for _, r := range a.receipts {
		if r.Kind != "http" {
			continue
		}
		for _, s := range sources {
			if s.FileID != "" && s.FileID == r.Source.FileID && (r.Source.Status == "available" || s.Status != "available") {
				r.Source = s
			}
		}
	}
	raw, err := json.Marshal(struct {
		Partial bool                  `json:"partial"`
		Limited bool                  `json:"limited"`
		Records []*acquisitionReceipt `json:"records"`
	}{!v.Complete, a.metadataLimited, a.receipts})
	if err != nil {
		return err
	}
	if int64(len(raw)) > a.metadataQuota {
		return errors.New("acquisition metadata quota exceeded")
	}
	n, err := a.metadata.Write(raw)
	a.total += int64(n) - a.metadataQuota
	if err == nil && n != len(raw) {
		err = io.ErrShortWrite
	}
	return err
}

func acquired(id string) Source { return Source{Status: "available", FileID: id} }
func acquisitionGap(s Source, status, reason string) Source {
	s.Status, s.Reason = status, reason
	return s
}
func acquisitionID(prefix string, i int) string {
	if i == 0 {
		return prefix
	}
	return fmt.Sprintf("%s-%d", prefix, i)
}

// save streams to an exclusively owned generated path, including failed bodies.
// Read/limit failures are data gaps; filesystem failures and cancellation are errors.
func (a *intakeAcquirer) save(id string, r io.Reader) (s Source, err error) {
	if err = context.Cause(a.ctx); err != nil {
		return s, err
	}
	if len(a.files) >= a.options.MaxFiles || a.total >= a.options.MaxTotalBytes {
		return Source{Status: "too-large", Reason: "acquisition file/aggregate limit"}, nil
	}
	f, err := a.root.OpenFile(id, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return s, err
	}
	s = acquired(id)
	a.files = append(a.files, file{ID: id, Kind: "evidence", Path: "evidence/" + id})
	defer func() { err = errors.Join(err, f.Close()) }()
	remaining := min(a.options.MaxBytes, a.options.MaxTotalBytes-a.total)
	buffer := make([]byte, 32<<10)
	for {
		if err = context.Cause(a.ctx); err != nil {
			return s, err
		}
		// One lookahead byte detects truncation without writing beyond the cap.
		n, readErr := r.Read(buffer[:min(int64(len(buffer)), remaining+1)])
		keep := min(int64(n), remaining)
		if keep > 0 {
			written, writeErr := f.Write(buffer[:keep])
			a.total += int64(written)
			remaining -= int64(written)
			if writeErr != nil {
				return s, writeErr
			}
			if int64(written) != keep {
				return s, io.ErrShortWrite
			}
		}
		if err = context.Cause(a.ctx); err != nil {
			return s, err
		}
		if int64(n) > keep {
			return acquisitionGap(s, "too-large", "acquisition byte limit"), nil
		}
		if readErr == io.EOF {
			return s, nil
		}
		if readErr != nil {
			var diskError *os.PathError
			if errors.As(readErr, &diskError) {
				return s, readErr
			}
			return acquisitionGap(s, "partial", "source body read failed"), nil
		}
	}
}

func (a *intakeAcquirer) fetch(id, rawURL string) (result Source, fetchErr error) {
	var receipt *acquisitionReceipt
	defer func() {
		if receipt != nil {
			receipt.Source = result
			if fetchErr != nil {
				receipt.Source = acquisitionGap(result, "partial", "acquisition interrupted")
			}
		}
	}()
	u, err := url.Parse(rawURL)
	if err != nil || !acquisitionURL(u) {
		return Source{Status: "unsafe", Reason: "source URL rejected"}, nil
	}
	req, err := http.NewRequestWithContext(a.ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Source{Status: "unsafe", Reason: "source request rejected"}, nil
	}
	if u.Scheme == "https" && acquisitionOrigin(a.base, u) {
		req.Header.Set("Authorization", a.options.Authorization)
	}
	// Disable automatic redirects so every response body, including redirect
	// failures, reaches evidence before the next request is attempted.
	client := *a.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	via := []*http.Request{}
	for redirects := 0; ; redirects++ {
		if err := context.Cause(a.ctx); err != nil {
			return Source{}, err
		}
		if len(a.files) >= a.options.MaxFiles || a.total >= a.options.MaxTotalBytes {
			return Source{Status: "too-large", Reason: "acquisition file/aggregate limit"}, nil
		}
		record := acquisitionReceipt{Kind: "http", SourceID: id, Origin: req.URL.Scheme + "://" + req.URL.Host}
		if strings.HasPrefix(id, "page-") {
			if offset, err := strconv.Atoi(u.Query().Get("startAt")); err == nil {
				record.RequestedOffset = &offset
			}
		}
		receipt = a.receipt(record)
		if receipt == nil {
			return Source{Status: "too-large", Reason: "acquisition metadata limit"}, nil
		}
		resp, err := client.Do(req)
		receipt.FetchedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err != nil {
			if cause := context.Cause(a.ctx); cause != nil {
				return Source{}, cause
			}
			return Source{Status: "missing", Reason: "HTTP transport failed"}, nil
		}
		receipt.HTTPStatus = resp.StatusCode
		redirect := resp.StatusCode == 301 || resp.StatusCode == 302 || resp.StatusCode == 303 || resp.StatusCode == 307 || resp.StatusCode == 308
		bodyID := id
		if redirect {
			bodyID = fmt.Sprintf("%s-redirect-%d", id, redirects)
		}
		s, err := a.save(bodyID, resp.Body)
		receipt.Source = s
		closeErr := resp.Body.Close()
		if err != nil {
			return s, err
		}
		if s.Status != "available" {
			return s, nil
		}
		if closeErr != nil {
			return acquisitionGap(s, "partial", "HTTP body close failed"), nil
		}
		if !redirect {
			if resp.StatusCode != http.StatusOK {
				return acquisitionGap(s, "partial", fmt.Sprintf("HTTP status %d", resp.StatusCode)), nil
			}
			return s, nil
		}
		target, err := resp.Location()
		if err != nil {
			return acquisitionGap(s, "partial", "invalid redirect location"), nil
		}
		next, err := http.NewRequestWithContext(a.ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			return acquisitionGap(s, "unsafe", "redirect request rejected"), nil
		}
		via = append(via, req)
		if err := a.client.CheckRedirect(next, via); err != nil {
			return acquisitionGap(s, "unsafe", "redirect rejected"), nil
		}
		req = next
		receipt = nil
	}
}
func (a *intakeAcquirer) decode(s Source, value any) (bool, error) {
	if s.Status != "available" {
		return false, nil
	}
	f, err := a.root.Open(s.FileID)
	if err != nil {
		return false, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, a.options.MaxBytes+1))
	if err != nil {
		return false, err
	}
	if err := context.Cause(a.ctx); err != nil {
		return false, err
	}
	return int64(len(raw)) <= a.options.MaxBytes && json.Unmarshal(raw, value) == nil, nil
}

type acquisitionIssue struct {
	Key    string                     `json:"key"`
	Fields map[string]json.RawMessage `json:"fields"`
}
type acquisitionAttachment struct {
	ID      string `json:"id"`
	Size    *int64 `json:"size"`
	Content string `json:"content"`
	Mime    string `json:"mimeType"`
}
type acquisitionLink struct {
	In *struct {
		Key string `json:"key"`
	} `json:"inwardIssue"`
	Out *struct {
		Key string `json:"key"`
	} `json:"outwardIssue"`
}
type acquisitionPage struct {
	Start    *int `json:"startAt"`
	Total    *int `json:"total"`
	Max      *int `json:"maxResults"`
	Comments []struct {
		ID   string          `json:"id"`
		Body json.RawMessage `json:"body"`
	} `json:"comments"`
}

func acquireIntake(ctx context.Context, root string, scope Scope, options acquisitionOptions) (v Intake, files []file, err error) {
	if err = context.Cause(ctx); err != nil {
		return
	}
	if err = options.limits(); err != nil {
		return
	}
	base, e := url.Parse(options.BaseURL)
	if e != nil || !acquisitionURL(base) || base.RawQuery != "" || base.ForceQuery || (base.Path != "" && base.Path != "/") || !acquisitionKey.MatchString(scope.Ticket) {
		return v, nil, errors.New("invalid acquisition base URL or ticket")
	}
	r, e := os.OpenRoot(root)
	if e != nil {
		return v, nil, e
	}
	defer r.Close()
	info, e := r.Lstat("evidence")
	if e != nil {
		return v, nil, e
	}
	if !info.IsDir() {
		return v, nil, errors.New("evidence must be an existing directory")
	}
	evidence, e := r.OpenRoot("evidence")
	if e != nil {
		return v, nil, e
	}
	defer evidence.Close()
	a := &intakeAcquirer{ctx: ctx, root: evidence, options: options, base: base, client: acquisitionClient(base, options.Authorization), files: []file{}}
	defer a.client.CloseIdleConnections()
	if err = a.reserveMetadata(); err != nil {
		return
	}
	defer func() {
		err = errors.Join(err, a.finishMetadata(&v))
		files = a.files
		if cause := context.Cause(ctx); cause != nil {
			err = cause
		}
	}()
	canonical := base.Scheme + "://" + base.Host
	if base.Scheme == "http" || base.Hostname() == "localhost" || net.ParseIP(base.Hostname()).IsLoopback() {
		canonical = "https://jira.example.invalid"
	}
	v = Intake{Ticket: scope.Ticket, URL: canonical + "/browse/" + scope.Ticket, FetchedAt: time.Now().UTC().Format(time.RFC3339Nano), Comments: []CommentPage{}, Linked: []LinkedIssue{}, Attachments: []Attachment{}, Gaps: []string{}}
	endpoint := strings.TrimRight(base.String(), "/") + "/rest/api/3"
	v.Issue, err = a.fetch("issue", endpoint+"/issue/"+scope.Ticket+"?fields=*all")
	if err != nil {
		return
	}
	var issue acquisitionIssue
	valid, e := a.decode(v.Issue, &issue)
	if e != nil {
		err = e
		return
	}
	var embedded struct {
		Total *int `json:"total"`
	}
	var links []acquisitionLink
	var attachments []acquisitionAttachment
	if valid {
		valid = issue.Key == scope.Ticket && issue.Fields != nil
		for _, key := range []string{"description", "comment", "issuelinks", "attachment"} {
			_, ok := issue.Fields[key]
			valid = valid && ok
		}
		valid = valid && json.Unmarshal(issue.Fields["comment"], &embedded) == nil && embedded.Total != nil && *embedded.Total >= 0
		valid = valid && json.Unmarshal(issue.Fields["issuelinks"], &links) == nil && json.Unmarshal(issue.Fields["attachment"], &attachments) == nil
		for _, link := range links {
			if link.In != nil {
				valid = valid && acquisitionKey.MatchString(link.In.Key)
			}
			if link.Out != nil {
				valid = valid && acquisitionKey.MatchString(link.Out.Key)
			}
		}
		seen := map[string]bool{}
		for _, attachment := range attachments {
			valid = valid && nonblank(attachment.ID) && !seen[attachment.ID] && attachment.Size != nil
			if attachment.Size != nil {
				valid = valid && *attachment.Size >= 0
			}
			seen[attachment.ID] = true
		}
	}
	if v.Issue.Status == "available" && !valid {
		v.Issue = acquisitionGap(v.Issue, "partial", "invalid raw issue/inventory")
	}
	v.Fields, err = a.fetch("fields", endpoint+"/field")
	if err != nil {
		return
	}
	var fieldsMeta []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	fieldsValid, e := a.decode(v.Fields, &fieldsMeta)
	if e != nil {
		err = e
		return
	}
	seenFields := map[string]bool{}
	fieldsValid = fieldsValid && len(fieldsMeta) > 0
	for _, f := range fieldsMeta {
		fieldsValid = fieldsValid && nonblank(f.ID) && nonblank(f.Name) && !seenFields[f.ID]
		seenFields[f.ID] = true
	}
	if v.Fields.Status == "available" && !fieldsValid {
		v.Fields = acquisitionGap(v.Fields, "partial", "invalid field metadata")
	}
	if valid {
		next := 0
		seenComments := map[string]bool{}
		for pageIndex := 0; pageIndex < options.MaxPages; pageIndex++ {
			s, e := a.fetch(fmt.Sprintf("page-%d", pageIndex), fmt.Sprintf("%s/issue/%s/comment?startAt=%d&maxResults=100", endpoint, scope.Ticket, next))
			if e != nil {
				err = e
				return
			}
			var page acquisitionPage
			ok, e := a.decode(s, &page)
			if e != nil {
				err = e
				return
			}
			ok = ok && page.Start != nil && page.Total != nil && page.Comments != nil
			if ok {
				ok = *page.Start == next && *page.Total == *embedded.Total && next <= *page.Total && len(page.Comments) <= *page.Total-next
				pageIDs := map[string]bool{}
				for _, c := range page.Comments {
					ok = ok && nonblank(c.ID) && len(c.Body) > 0 && strings.TrimSpace(string(c.Body)) != "null" && !seenComments[c.ID] && !pageIDs[c.ID]
					pageIDs[c.ID] = true
				}
				if next+len(page.Comments) < *page.Total {
					ok = ok && len(page.Comments) > 0
					if page.Max != nil {
						ok = ok && *page.Max > 0 && len(page.Comments) == *page.Max
					}
				}
			}
			if s.Status == "available" && !ok {
				s = acquisitionGap(s, "partial", "inconsistent or incomplete comment page")
			}
			v.Comments = append(v.Comments, CommentPage{Start: next, Source: s})
			if !ok {
				break
			}
			for _, c := range page.Comments {
				seenComments[c.ID] = true
			}
			next += len(page.Comments)
			if next == *embedded.Total {
				break
			}
			if pageIndex+1 == options.MaxPages {
				v.Comments = append(v.Comments, CommentPage{Start: next, Source: Source{Status: "missing", Reason: "comment page limit"}})
			}
		}
		seenLinks := map[string]bool{}
		for _, link := range links {
			keys := []string{}
			if link.In != nil {
				keys = append(keys, link.In.Key)
			}
			if link.Out != nil {
				keys = append(keys, link.Out.Key)
			}
			for _, key := range keys {
				if seenLinks[key] {
					continue
				}
				seenLinks[key] = true
				s, e := a.fetch(acquisitionID("linked", len(v.Linked)), endpoint+"/issue/"+key+"?fields=*all")
				if e != nil {
					err = e
					return
				}
				var snapshot acquisitionIssue
				ok, e := a.decode(s, &snapshot)
				if e != nil {
					err = e
					return
				}
				if s.Status == "available" && (!ok || snapshot.Key != key || len(snapshot.Fields) == 0) {
					s = acquisitionGap(s, "partial", "invalid linked snapshot")
				}
				v.Linked = append(v.Linked, LinkedIssue{Key: key, Source: s})
			}
		}
		for i, attachment := range attachments {
			s, e := a.fetch(acquisitionID("bundle", i), attachment.Content)
			if e != nil {
				err = e
				return
			}
			analysis := Source{Status: "missing", Reason: "attachment content incomplete"}
			if s.Status == "available" {
				f, e := a.root.Stat(s.FileID)
				if e != nil {
					err = e
					return
				}
				if f.Size() != *attachment.Size {
					s = acquisitionGap(s, "partial", "attachment size mismatch")
				} else {
					analysis, e = a.extract(s, acquisitionID("extracted", i), attachment.Mime)
					if e != nil {
						err = e
						return
					}
				}
			}
			v.Attachments = append(v.Attachments, Attachment{ID: attachment.ID, Content: s, Analysis: analysis})
		}
	}
	v.Complete = true
	record := func(label string, s Source) {
		if s.Status != "available" {
			v.Complete = false
			v.Gaps = append(v.Gaps, label+": "+s.Reason)
		}
	}
	record("issue", v.Issue)
	record("fields", v.Fields)
	if !valid {
		v.Complete = false
		v.Gaps = append(v.Gaps, "comment and linked/attachment inventory unavailable")
	}
	for i, p := range v.Comments {
		record(fmt.Sprintf("comment page %d", i), p.Source)
	}
	for i, l := range v.Linked {
		record(fmt.Sprintf("linked snapshot %d", i), l.Source)
	}
	for i, at := range v.Attachments {
		record(fmt.Sprintf("attachment %d content", i), at.Content)
		record(fmt.Sprintf("attachment %d analysis", i), at.Analysis)
	}
	return
}

func (a *intakeAcquirer) text(s Source) (bool, error) {
	f, err := a.root.Open(s.FileID)
	if err != nil {
		return false, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, a.options.MaxBytes+1))
	if err != nil {
		return false, err
	}
	if err := context.Cause(a.ctx); err != nil {
		return false, err
	}
	if int64(len(raw)) > a.options.MaxBytes || !utf8.Valid(raw) {
		return false, nil
	}
	for _, r := range string(raw) {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			return false, nil
		}
	}
	return true, nil
}

func (a *intakeAcquirer) extract(content Source, id, mime string) (result Source, extractErr error) {
	receipt := a.receipt(acquisitionReceipt{Kind: "extraction", SourceID: id, ArchiveFileID: content.FileID})
	if receipt == nil {
		return Source{Status: "too-large", Reason: "acquisition metadata limit"}, nil
	}
	defer func() {
		receipt.Source = result
		if extractErr != nil {
			receipt.Source = acquisitionGap(result, "partial", "extraction interrupted")
		}
	}()
	f, err := a.root.Open(content.FileID)
	if err != nil {
		return Source{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return Source{}, err
	}
	magic := make([]byte, 4)
	n, err := f.ReadAt(magic, 0)
	if err != nil && err != io.EOF {
		return Source{}, err
	}
	signature := string(magic[:n])
	isZIP := signature == "PK\x03\x04" || signature == "PK\x05\x06" || signature == "PK\x07\x08" || mime == "application/zip" || mime == "application/x-zip-compressed"
	if !isZIP {
		ok, e := a.text(content)
		if e != nil {
			return Source{}, e
		}
		if !ok || mime != "" && !strings.HasPrefix(mime, "text/") && mime != "application/json" && mime != "application/xml" && mime != "application/octet-stream" {
			return Source{Status: "unsupported", Reason: "attachment is not supported text"}, nil
		}
		return content, nil
	}
	return a.extractZIP(f, info.Size(), content, id)
}

func (a *intakeAcquirer) extractZIP(source io.ReaderAt, size int64, content Source, id string) (Source, error) {
	z, err := zip.NewReader(source, size)
	if err != nil {
		var diskError *os.PathError
		if errors.As(err, &diskError) {
			return Source{}, err
		}
		return Source{Status: "partial", Reason: "invalid ZIP archive"}, nil
	}
	if len(z.File) == 0 {
		return Source{Status: "unsupported", Reason: "ZIP has no text entries"}, nil
	}
	names := map[string]bool{}
	var expanded uint64
	for _, entry := range z.File {
		name := entry.Name
		clean := path.Clean(name)
		if name == "" || strings.ContainsAny(name, "\\:\x00") || strings.HasPrefix(name, "/") || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.Contains("/"+name+"/", "/../") || (!entry.Mode().IsRegular() && !entry.Mode().IsDir()) || names[clean] {
			return Source{Status: "unsafe", Reason: "unsafe or duplicate ZIP entry"}, nil
		}
		names[clean] = true
		if entry.Mode().IsDir() {
			continue
		}
		if entry.UncompressedSize64 > uint64(a.options.MaxBytes) || entry.UncompressedSize64 > uint64(a.options.MaxTotalBytes-a.total)-min(expanded, uint64(a.options.MaxTotalBytes-a.total)) {
			return Source{Status: "too-large", Reason: "ZIP expansion byte limit"}, nil
		}
		expanded += entry.UncompressedSize64
	}
	if len(z.File) > a.options.MaxFiles-len(a.files) {
		return Source{Status: "too-large", Reason: "ZIP expansion file limit"}, nil
	}
	first := Source{Status: "unsupported", Reason: "ZIP has no text entries"}
	count := 0
	for _, entry := range z.File {
		if err := context.Cause(a.ctx); err != nil {
			return Source{}, err
		}
		if entry.Mode().IsDir() {
			continue
		}
		entryID := id
		if count > 0 {
			entryID = fmt.Sprintf("%s-entry-%d", id, count)
		}
		mapping := a.receipt(acquisitionReceipt{Kind: "zip-entry", SourceID: entryID, ArchiveFileID: content.FileID, Filename: entry.Name})
		if mapping == nil {
			return acquisitionGap(first, "too-large", "acquisition metadata limit"), nil
		}
		reader, e := entry.Open()
		if e != nil {
			mapping.Source = Source{Status: "partial", Reason: "ZIP entry open failed"}
			var diskError *os.PathError
			if errors.As(e, &diskError) {
				return Source{}, e
			}
			return acquisitionGap(first, "partial", "ZIP entry open failed"), nil
		}
		s, e := a.save(entryID, reader)
		mapping.Source = s
		closeErr := reader.Close()
		count++
		if e != nil {
			mapping.Source = acquisitionGap(s, "partial", "extraction interrupted")
			return s, e
		}
		if count == 1 {
			first = s
		}
		if s.Status != "available" {
			return acquisitionGap(first, s.Status, s.Reason), nil
		}
		if closeErr != nil {
			mapping.Source = acquisitionGap(s, "partial", "ZIP entry close failed")
			return acquisitionGap(first, "partial", "ZIP entry close failed"), nil
		}
		ok, e := a.text(s)
		if e != nil {
			return s, e
		}
		if !ok {
			mapping.Source = acquisitionGap(s, "unsupported", "ZIP contains non-text content")
			return acquisitionGap(first, "unsupported", "ZIP contains non-text content"), nil
		}
	}
	return first, nil
}
