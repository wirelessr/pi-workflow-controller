package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

type chunkReader struct {
	data  []byte
	chunk int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := min(len(p), r.chunk, len(r.data))
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}
func TestLFFraming(t *testing.T) {
	payload := []byte("{\"type\":\"test\",\"text\":\"a\u2028b\u2029" + strings.Repeat("x", 100<<10) + "\"}")
	if !bytes.Contains(payload, []byte{0xe2, 0x80, 0xa8}) || !bytes.Contains(payload, []byte{0xe2, 0x80, 0xa9}) {
		t.Fatal("fixture must contain raw Unicode separators, not JSON escapes")
	}
	wire := append(append(append([]byte{}, payload...), '\r', '\n'), []byte("{\"type\":\"second\"}\n")...)
	for _, chunk := range []int{1, 2, 7, 65535, len(wire)} {
		t.Run(fmt.Sprint(chunk), func(t *testing.T) {
			var frames [][]byte
			err := readFrames(&chunkReader{data: wire, chunk: chunk}, 128<<10, func(b []byte) error { frames = append(frames, bytes.Clone(b)); return nil })
			if !errors.Is(err, io.EOF) || len(frames) != 2 || !bytes.Equal(frames[0], payload) {
				t.Fatalf("frames=%d err=%v", len(frames), err)
			}
		})
	}
}
func TestInvalidFrames(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		limit     int
	}{{"invalid", "{no}\n", 100}, {"half", "{\"type\":\"valid\"}", 100}, {"oversize", strings.Repeat(" ", 101) + "\n", 100}, {"utf8", "{\"x\":\"\xff\"}\n", 100}, {"blank", "\n", 100}} {
		t.Run(tc.name, func(t *testing.T) {
			err := readFrames(strings.NewReader(tc.raw), tc.limit, func([]byte) error { t.Fatal("invalid frame delivered"); return nil })
			_ = requireCode(t, err, ProtocolFailed)
		})
	}
}

func TestAssistantContentValidation(t *testing.T) {
	for _, tc := range []struct {
		name, content string
		valid         bool
	}{
		{"empty", `[]`, true},
		{"text", `[{"type":"text","text":"result","textSignature":"signature"}]`, true},
		{"empty-text", `[{"type":"text","text":""}]`, true},
		{"thinking", `[{"type":"thinking","thinking":"reasoning"}]`, true},
		{"redacted-thinking", `[{"type":"thinking","thinking":"","redacted":true,"thinkingSignature":"opaque"}]`, true},
		{"tool", `[{"type":"toolCall","id":"call-1","name":"read","arguments":{"path":"file"}}]`, true},
		{"empty-arguments", `[{"type":"toolCall","id":"call-1","name":"read","arguments":{}}]`, true},
		{"missing", ``, false},
		{"null", `null`, false},
		{"number", `17`, false},
		{"string", `"result"`, false},
		{"object", `{}`, false},
		{"null-block", `[null]`, false},
		{"missing-type", `[{}]`, false},
		{"missing-text", `[{"type":"text"}]`, false},
		{"null-text", `[{"type":"text","text":null}]`, false},
		{"number-text", `[{"type":"text","text":17}]`, false},
		{"missing-thinking", `[{"type":"thinking"}]`, false},
		{"null-thinking", `[{"type":"thinking","thinking":null}]`, false},
		{"missing-tool-id", `[{"type":"toolCall","name":"read","arguments":{}}]`, false},
		{"missing-tool-name", `[{"type":"toolCall","id":"call-1","arguments":{}}]`, false},
		{"missing-arguments", `[{"type":"toolCall","id":"call-1","name":"read"}]`, false},
		{"null-arguments", `[{"type":"toolCall","id":"call-1","name":"read","arguments":null}]`, false},
		{"array-arguments", `[{"type":"toolCall","id":"call-1","name":"read","arguments":[]}]`, false},
		{"unknown-block", `[{"type":"unsupported"}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			content := ""
			if tc.content != "" {
				content = `,"content":` + tc.content
			}
			raw := []byte(`{"role":"assistant","provider":"fixture","model":"model","stopReason":"stop"` + content + `}`)
			_, err := parseMessage(raw)
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				_ = requireCode(t, err, ProtocolFailed)
			}
		})
	}
}
