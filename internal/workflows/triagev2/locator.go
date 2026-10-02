package triagev2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"pi-workflow-controller/internal/contract"
)

// Locator narrows a citation to a place in the cited file: a JSON pointer
// for a JSON file, or a byte range for anything else. Resolving it proves
// only that the place exists; whether the text there supports the claim is
// the reader's judgment.
type Locator struct {
	Pointer *string `json:"pointer,omitempty"`
	Offset  *int64  `json:"offset,omitempty"`
	Length  *int64  `json:"length,omitempty"`
}

// citations checks every citation of one contract against its Step's
// citable inputs and resolves each locator.
type citations struct {
	ctx   context.Context
	in    Inputs
	own   contract.Ref
	files []contract.FileEntry
}

func (c citations) check(field string, e Evidence) error {
	if err := c.in.CheckEvidence(field, e, c.files); err != nil {
		return err
	}
	if e.Locator == nil {
		return nil
	}
	owner, files := c.own, c.files
	if e.Ref != nil {
		owner, files = *e.Ref, c.in.Citable[*e.Ref]
	}
	raw, err := rawFile(c.ctx, owner, files, e.FileID)
	if err != nil {
		return fmt.Errorf("%s.locator: file %q cannot be read: %w", field, e.FileID, err)
	}
	return resolveLocator(field+".locator", *e.Locator, raw)
}

func resolveLocator(field string, l Locator, raw []byte) error {
	switch {
	case l.Pointer != nil && l.Offset == nil && l.Length == nil:
		var doc any
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if dec.Decode(&doc) != nil || dec.Decode(&struct{}{}) != io.EOF {
			return fmt.Errorf("%s.pointer: the cited file is not a single JSON document, so cite a byte range instead", field)
		}
		return walkPointer(field+".pointer", *l.Pointer, doc)
	case l.Pointer == nil && l.Offset != nil && l.Length != nil:
		size := int64(len(raw))
		if *l.Offset < 0 || *l.Length <= 0 || *l.Offset > size || *l.Length > size-*l.Offset {
			return fmt.Errorf("%s: got bytes %d..%d; the cited file has %d bytes", field, *l.Offset, *l.Offset+*l.Length, len(raw))
		}
		return nil
	default:
		return fmt.Errorf("%s: want either pointer, or offset with length", field)
	}
}

// walkPointer resolves an RFC 6901 JSON pointer.
func walkPointer(field, pointer string, doc any) error {
	if pointer == "" {
		return nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return fmt.Errorf("%s: got %q; a JSON pointer starts with /", field, pointer)
	}
	at := ""
	for _, token := range strings.Split(pointer[1:], "/") {
		if strings.Contains(strings.NewReplacer("~0", "", "~1", "").Replace(token), "~") {
			return fmt.Errorf("%s: %q has a ~ that is not ~0 or ~1", field, pointer)
		}
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		at += "/" + token
		switch v := doc.(type) {
		case map[string]any:
			next, ok := v[token]
			if !ok {
				return fmt.Errorf("%s: %q does not exist in the cited file", field, at)
			}
			doc = next
		case []any:
			i, err := strconv.Atoi(token)
			if err != nil || i < 0 || i >= len(v) || strconv.Itoa(i) != token {
				return fmt.Errorf("%s: %q does not exist in the cited file (array of %d)", field, at, len(v))
			}
			doc = v[i]
		default:
			return fmt.Errorf("%s: %q goes below a scalar value", field, at)
		}
	}
	return nil
}
