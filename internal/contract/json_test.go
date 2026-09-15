package contract

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestParseJSON(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want any
	}{
		{"null", `null`, nil},
		{"boolean", `true`, true},
		{"string", `"繁體中文"`, "繁體中文"},
		{"integer", `9007199254740993`, json.Number("9007199254740993")},
		{"decimal", `0.123456789012345678901`, json.Number("0.123456789012345678901")},
		{"exponent", `1e400`, json.Number("1e400")},
		{"empty object", `{}`, map[string]any{}},
		{"empty array", `[]`, []any{}},
		{"whitespace", " \t\r\n[null,false,2]\r\n", []any{nil, false, json.Number("2")}},
		{"same key in different objects", `[{"x":1},{"x":2}]`, []any{map[string]any{"x": json.Number("1")}, map[string]any{"x": json.Number("2")}}},
		{"decoded keys", `{"\u0061":1}`, map[string]any{"a": json.Number("1")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseJSON([]byte(tc.raw), 64)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestParseJSONRejectsMalformedInput(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"empty", ""},
		{"whitespace only", " \n"},
		{"invalid UTF-8 string", "\"\xff\""},
		{"invalid UTF-8 key", "{\"\xff\":1}"},
		{"invalid UTF-8 outside string", "null\xff"},
		{"duplicate key", `{"a":1,"a":2}`},
		{"duplicate null key", `{"a":null,"a":2}`},
		{"escaped equivalent key", `{"a":1,"\u0061":2}`},
		{"surrogate pair equivalent key", `{"𝄞":1,"\ud834\udd1e":2}`},
		{"nested duplicate key", `{"a":[{"x":1,"x":2}]}`},
		{"trailing object", `{} {}`},
		{"trailing scalar", `null true`},
		{"trailing garbage", `{} xyz`},
		{"trailing closing bracket", `[]]`},
		{"unclosed object", `{"a":1`},
		{"unclosed array", `[1`},
		{"unclosed string", `"hello`},
		{"non-string key", `{1:2}`},
		{"missing colon", `{"a" 1}`},
		{"missing value", `{"a":}`},
		{"missing comma", `[1 2]`},
		{"trailing comma", `[1,]`},
		{"bad escape", `"\x41"`},
		{"control character", "\"hello\nworld\""},
		{"leading zero", `01`},
		{"non-finite number", `NaN`},
		{"incomplete exponent", `1e`},
		{"comment", `/* comment */ {}`},
		{"BOM", "\xef\xbb\xbf{}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if value, err := parseJSON([]byte(tc.raw), 64); err == nil {
				t.Fatalf("accepted malformed JSON: %#v", value)
			}
		})
	}
}

func TestParseJSONDepth(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		maxDepth int
		valid    bool
	}{
		{"negative limit", `null`, -1, false},
		{"scalar has no containers", `1`, 0, true},
		{"root object", `{}`, 1, true},
		{"root array", `[]`, 1, true},
		{"object at zero", `{}`, 0, false},
		{"array at zero", `[]`, 0, false},
		{"scalar child", `{"a":1}`, 1, true},
		{"object child", `{"a":{}}`, 1, false},
		{"array child", `[[]]`, 1, false},
		{"mixed nesting", `{"a":[{"b":true}]}`, 3, true},
		{"mixed nesting excess", `{"a":[{"b":true}]}`, 2, false},
		{"policy boundary", strings.Repeat("[", 64) + "0" + strings.Repeat("]", 64), 64, true},
		{"policy excess", strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65), 64, false},
		{"deep attack", strings.Repeat("[", 10000), 64, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseJSON([]byte(tc.raw), tc.maxDepth)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
}
