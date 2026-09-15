package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"pi-workflow-controller/internal/engine"
)

func TestCLIPreflightDoesNotCreateTaskOrStartPi(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		change func(*cliOptions)
		want   string
	}{
		{name: "no arguments", want: "Usage:"},
		{name: "unknown command", args: []string{"other"}, want: "Usage:"},
		{name: "list argument", args: []string{"list", "protocol"}, want: "Usage:"},
		{name: "missing workflow", args: []string{"run"}, want: "Usage:"},
		{name: "missing prompt", args: []string{"run", "protocol"}, want: "Usage:"},
		{name: "extra prompt", args: []string{"run", "protocol", "hello", "extra"}, want: "Usage:"},
		{name: "task flag", args: []string{"run", "protocol", "hello", "--task", "named"}, want: "Usage:"},
		{name: "input flag", args: []string{"run", "protocol", "--input", "task.json"}, want: "Usage:"},
		{name: "unknown workflow", args: []string{"run", "unknown", "hello"}, want: "unknown workflow"},
		{name: "empty registry", args: []string{"run", "protocol", "hello"}, change: func(o *cliOptions) { o.definitions = nil }, want: "unknown workflow"},
		{name: "empty prompt", args: []string{"run", "protocol", ""}, want: "nonempty"},
		{name: "whitespace prompt", args: []string{"run", "protocol", " \t\u3000 "}, want: "nonempty"},
		{name: "LF", args: []string{"run", "protocol", "one\ntwo"}, want: "single-line"},
		{name: "CR", args: []string{"run", "protocol", "one\rtwo"}, want: "single-line"},
		{name: "CRLF", args: []string{"run", "protocol", "one\r\ntwo"}, want: "single-line"},
		{name: "invalid UTF8", args: []string{"run", "protocol", "one\xff"}, want: "UTF-8"},
		{name: "NUL", args: []string{"run", "protocol", "one\x00two"}, want: "NUL"},
		{name: "oversize", args: []string{"run", "protocol", strings.Repeat("x", (64<<10)+1)}, want: "byte limit"},
		{name: "multibyte byte limit", args: []string{"run", "protocol", strings.Repeat("中", (64<<10)/3+1)}, want: "byte limit"},
		{name: "definition prompt limit", args: []string{"run", "protocol", "中文"}, change: func(o *cliOptions) { o.definitions[0].Policy.MaxPromptBytes = 5 }, want: "byte limit"},
		{name: "invalid definition name", args: []string{"run", "protocol", "hello"}, change: func(o *cliOptions) { o.definitions[0].Name = "../bad" }, want: "invalid workflow name"},
		{name: "duplicate definition", args: []string{"list"}, change: func(o *cliOptions) { o.definitions = append(o.definitions, o.definitions[0]) }, want: "duplicate workflow"},
		{name: "nil workflow", args: []string{"run", "protocol", "hello"}, change: func(o *cliOptions) { o.definitions[0].Execute = nil }, want: "requires Execute"},
		{name: "invalid policy", args: []string{"run", "protocol", "hello"}, change: func(o *cliOptions) { o.definitions[0].Policy.RunTimeout = 0 }, want: "InvalidDefinition"},
		{name: "invalid schema", args: []string{"run", "protocol", "hello"}, change: func(o *cliOptions) { o.resources[0].JSON = json.RawMessage(`{"type":"not-a-type"}`) }, want: "compile schema"},
		{name: "schema JSON malformed", args: []string{"run", "protocol", "hello"}, change: func(o *cliOptions) { o.resources[0].JSON = json.RawMessage(`{`) }, want: "invalid JSON"},
		{name: "duplicate schema", args: []string{"list"}, change: func(o *cliOptions) { o.schemas = append(o.schemas, o.schemas[0]) }, want: "duplicate schema ID"},
		{name: "unregistered schema resource", args: []string{"run", "protocol", "hello"}, change: func(o *cliOptions) { o.resources = nil }, want: "unregistered resource"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			marker := filepath.Join(home, "pi-started")
			opts := cliProtocolOptions(t, "", "", "")
			opts.engine.RuntimeOptions.Env = append(opts.engine.RuntimeOptions.Env, "PWC_CLI_PI_MARKER="+marker)
			called := false
			workflow := opts.definitions[0].Execute
			opts.definitions[0].Execute = func(ctx context.Context, r *engine.Run, input engine.Input) (engine.Result, error) {
				called = true
				return workflow(ctx, r, input)
			}
			if tc.change != nil {
				tc.change(&opts)
			}
			var stdout, stderr bytes.Buffer
			if code := run(tc.args, strings.NewReader("not a prompt source"), &stdout, &stderr, opts); code != 2 {
				t.Fatalf("exit=%d, want 2; stdout=%s stderr=%s", code, &stdout, &stderr)
			}
			if called || stdout.Len() != 0 || !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("called=%t stdout=%q stderr=%q, want %q", called, &stdout, &stderr, tc.want)
			}
			assertCLIPlain(t, stderr.String())
			for _, path := range []string{filepath.Join(home, "WIP"), marker} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("preflight side effect at %s: %v", path, err)
				}
			}
		})
	}
}

func TestCLIList(t *testing.T) {
	for _, empty := range []bool{true, false} {
		name := "sorted and safe"
		if empty {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HOME", home)
			opts := cliProtocolOptions(t, "", "", "")
			marker := filepath.Join(home, "pi-started")
			opts.engine.RuntimeOptions.Env = append(opts.engine.RuntimeOptions.Env, "PWC_CLI_PI_MARKER="+marker)
			want := "No predefined workflows registered.\n"
			if empty {
				opts.definitions = nil
			} else {
				alpha, zeta := opts.definitions[0], opts.definitions[0]
				alpha.Name, alpha.Description = "alpha", "中文\x1b[31mred\x1b[0m\x1b]52;c;SECRET\a\nline\tend"
				zeta.Name, zeta.Description = "zeta", "last"
				opts.definitions = []engine.Definition{zeta, alpha}
				want = "alpha\t中文red line end\nzeta\tlast\n"
			}
			var stdout, stderr bytes.Buffer
			if code := run([]string{"list"}, strings.NewReader("ignored"), &stdout, &stderr, opts); code != 0 || stdout.String() != want || stderr.Len() != 0 {
				t.Fatalf("exit=%d stdout=%q stderr=%q, want %q", code, &stdout, &stderr, want)
			}
			assertCLIPlain(t, stdout.String())
			for _, path := range []string{filepath.Join(home, "WIP"), marker} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("list side effect: %s: %v", path, err)
				}
			}
		})
	}
}

type cliErrorWriter struct{ err error }

func (w cliErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestCLIListOutputFailure(t *testing.T) {
	var stderr bytes.Buffer
	code := run([]string{"list"}, strings.NewReader(""), cliErrorWriter{errors.New("closed\x1b]52;c;SECRET\a")}, &stderr, cliOptions{})
	if code != 1 || stderr.String() != "closed\n" {
		t.Fatalf("exit=%d stderr=%q", code, &stderr)
	}
}

func assertCLIPlain(t *testing.T, text string) {
	t.Helper()
	if !utf8.ValidString(text) {
		t.Fatalf("invalid UTF8: %q", text)
	}
	for _, r := range text {
		if unicode.IsControl(r) && r != '\n' && r != '\t' || unicode.Is(unicode.Cf, r) {
			t.Fatalf("terminal control %U in %q", r, text)
		}
	}
	if strings.Contains(text, "SECRET") {
		t.Fatalf("terminal control payload leaked: %q", text)
	}
}

func cliReadJSON(t *testing.T, path string, value any) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func(f *os.File) { _ = f.Close() }(f)
	if err := json.NewDecoder(f).Decode(value); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}
