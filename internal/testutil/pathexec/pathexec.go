// Package pathexec shadows an external command on PATH with a wrapper that
// re-executes the current test binary at one named test, so a fixed entry
// point that resolves its executable on PATH still runs a real subprocess.
package pathexec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Install puts a wrapper named name first on PATH for the rest of t. The
// wrapper runs the test binary with -test.run='^test$', then "--" and the
// wrapper's own arguments, which Args returns inside that test. The test
// selects its behavior from environment it inherits, and must return early
// when that environment is unset so it passes in an ordinary run.
func Install(t *testing.T, name, test string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(exe, "'") {
		t.Fatalf("test binary path cannot be quoted: %s", exe)
	}
	bin := t.TempDir()
	script := "#!/bin/sh\nexec '" + exe + "' -test.run='^" + test + "$' -- \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// Args returns the arguments after the first "--" in os.Args; ok is false
// when there is none.
func Args() (args []string, ok bool) {
	args = os.Args
	for len(args) > 0 && args[0] != "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		return nil, false
	}
	return args[1:], true
}
