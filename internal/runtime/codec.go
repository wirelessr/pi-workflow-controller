package runtime

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// readFrames splits only at LF; even a valid JSON fragment without LF is a protocol error.
func readFrames(r io.Reader, limit int, consume func([]byte) error) error {
	br := bufio.NewReaderSize(r, min(limit+1, 64<<10))
	var line []byte
	for {
		part, err := br.ReadSlice('\n')
		if len(line)+len(part) > limit+2 {
			return failure(ProtocolFailed, "RPC frame exceeds limit")
		}
		line = append(line, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if len(line) > 0 {
				f := failure(ProtocolFailed, "EOF in incomplete RPC frame")
				f.Cause = err
				return f
			}
			return err
		}
		line = line[:len(line)-1]
		line = bytes.TrimSuffix(line, []byte{'\r'})
		if len(line) > limit || !utf8.Valid(line) || !json.Valid(line) {
			return failure(ProtocolFailed, "invalid JSON RPC frame")
		}
		if err = consume(line); err != nil {
			return err
		}
		line = line[:0]
	}
}

type wireFrame struct {
	Type         string          `json:"type"`
	ID           string          `json:"id"`
	Command      string          `json:"command"`
	Success      *bool           `json:"success"`
	Data         json.RawMessage `json:"data"`
	Error        string          `json:"error"`
	Message      json.RawMessage `json:"message"`
	Method       string          `json:"method"`
	ToolCallID   string          `json:"toolCallId"`
	Level        string          `json:"level"`
	Aborted      *bool           `json:"aborted"`
	ErrorMessage string          `json:"errorMessage"`
	FinalError   string          `json:"finalError"`
	WillRetry    bool            `json:"willRetry"`
	Steering     *[]string       `json:"steering"`
	FollowUp     *[]string       `json:"followUp"`
}
type response struct {
	frame wireFrame
	seq   uint64
}
type pendingRequest struct {
	command string
	ch      chan response
}

type message struct {
	raw        json.RawMessage
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	Provider   string          `json:"provider"`
	Model      string          `json:"model"`
	StopReason string          `json:"stopReason"`
	Timestamp  json.Number     `json:"timestamp"`
}

func parseMessage(raw json.RawMessage) (message, error) {
	var m message
	if json.Unmarshal(raw, &m) != nil || m.Role == "" {
		return m, failure(ProtocolFailed, "missing message role")
	}
	if m.Role == "user" {
		var text string
		var blocks []struct {
			Type string  `json:"type"`
			Text *string `json:"text"`
		}
		if len(m.Content) == 0 || bytes.Equal(m.Content, []byte("null")) {
			return m, failure(ProtocolFailed, "missing user content")
		}
		if json.Unmarshal(m.Content, &text) != nil {
			if json.Unmarshal(m.Content, &blocks) != nil {
				return m, failure(ProtocolFailed, "invalid user content")
			}
			for _, b := range blocks {
				if b.Type == "" || (b.Type == "text" && b.Text == nil) {
					return m, failure(ProtocolFailed, "invalid user content block")
				}
			}
		}
	}
	if m.Role == "assistant" && (m.Provider == "" || m.Model == "" || m.StopReason == "") {
		return m, failure(ProtocolFailed, "missing assistant binding/outcome")
	}
	if m.Role == "assistant" {
		var blocks []struct {
			Type      string                     `json:"type"`
			Text      *string                    `json:"text"`
			Thinking  *string                    `json:"thinking"`
			ID        string                     `json:"id"`
			Name      string                     `json:"name"`
			Arguments map[string]json.RawMessage `json:"arguments"`
		}
		if json.Unmarshal(m.Content, &blocks) != nil || blocks == nil {
			return m, failure(ProtocolFailed, "assistant content requires an array")
		}
		for _, b := range blocks {
			switch b.Type {
			case "text":
				if b.Text == nil {
					return m, failure(ProtocolFailed, "assistant text block requires text")
				}
			case "thinking":
				if b.Thinking == nil {
					return m, failure(ProtocolFailed, "assistant thinking block requires thinking")
				}
			case "toolCall":
				if b.ID == "" || b.Name == "" || b.Arguments == nil {
					return m, failure(ProtocolFailed, "assistant toolCall requires id, name and arguments object")
				}
			default:
				return m, failure(ProtocolFailed, "invalid assistant content block")
			}
		}
		switch m.StopReason {
		case "stop", "toolUse", "length", "error", "aborted":
		default:
			return m, failure(ProtocolFailed, "invalid assistant stopReason")
		}
	}
	m.raw = raw
	return m, nil
}
func messageText(m message) string {
	var text string
	if json.Unmarshal(m.Content, &text) == nil {
		return text
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(m.Content, &blocks) != nil {
		return ""
	}
	for _, b := range blocks {
		if b.Type == "text" {
			text += b.Text
		}
	}
	return text
}
