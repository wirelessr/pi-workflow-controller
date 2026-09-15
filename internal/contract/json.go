package contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

// parseJSON counts object/array nesting; a scalar has depth zero.
func parseJSON(raw []byte, maxDepth int) (any, error) {
	if !utf8.Valid(raw) {
		return nil, fmt.Errorf("invalid UTF-8 in JSON")
	}
	if maxDepth < 0 {
		return nil, fmt.Errorf("JSON maximum depth must not be negative")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := parseJSONValue(decoder, 0, maxDepth)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("trailing JSON: %w", err)
		}
		return nil, fmt.Errorf("trailing JSON value")
	}
	return value, nil
}

func parseJSONValue(decoder *json.Decoder, depth, maxDepth int) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	delim, container := token.(json.Delim)
	if !container {
		return token, nil
	}
	if depth >= maxDepth {
		return nil, fmt.Errorf("JSON exceeds maximum depth %d", maxDepth)
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return nil, fmt.Errorf("invalid JSON object key: %w", err)
			}
			key, ok := token.(string)
			if !ok {
				return nil, fmt.Errorf("JSON object key is not a string")
			}
			if _, exists := object[key]; exists {
				return nil, fmt.Errorf("duplicate JSON object key %q", key)
			}
			value, err := parseJSONValue(decoder, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			object[key] = value
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
			return nil, fmt.Errorf("invalid JSON object ending: %v", err)
		}
		return object, nil
	case '[':
		array := make([]any, 0)
		for decoder.More() {
			value, err := parseJSONValue(decoder, depth+1, maxDepth)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		if token, err := decoder.Token(); err != nil || token != json.Delim(']') {
			return nil, fmt.Errorf("invalid JSON array ending: %v", err)
		}
		return array, nil
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
}
