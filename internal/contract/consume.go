package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

// FileEntry identifies a declared file, not permission to consume it.
type FileEntry struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Path string `json:"path"`
}

// Publication projects the data and file declarations of an envelope.
// It does not carry Store validation or engine committed authorization.
type Publication[T any] struct {
	Data  T           `json:"data"`
	Files []FileEntry `json:"files"`
}

// DecodePublication decodes anew so mutable data is not shared between callers.
// Workflows must obtain raw through the engine's committed resolver first.
func DecodePublication[T any](raw json.RawMessage) (Publication[T], error) {
	var p Publication[T]
	err := json.Unmarshal(raw, &p)
	return p, err
}

var ErrReadLimit = errors.New("read exceeds byte limit")

// ReadBounded consumes at most limit+1 bytes, checking cancellation before each
// read. The caller owns the reader, path/identity checks and error disposition.
// limit must be nonnegative. I/O errors retain any bytes already read.
func ReadBounded(ctx context.Context, r io.Reader, limit int64) ([]byte, error) {
	var out bytes.Buffer
	block := make([]byte, 32<<10)
	reader := contextReader{ctx, r}
	for {
		remaining := limit - int64(out.Len())
		chunk := block
		if remaining < int64(len(chunk)) {
			chunk = chunk[:remaining+1]
		}
		n, err := reader.Read(chunk)
		if int64(out.Len())+int64(n) > limit {
			return nil, ErrReadLimit
		}
		out.Write(chunk[:n])
		if err == io.EOF {
			return out.Bytes(), nil
		}
		if err != nil {
			return out.Bytes(), err
		}
	}
}
