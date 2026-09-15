package tui

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

func TestSafeText(t *testing.T) {
	for _, tc := range []struct{ name, input, want string }{
		{"unicode", "中文 café", "中文 café"},
		{"sgr", "a\x1b[31mred\x1b[0mz", "aredz"},
		{"cursor", "a\x1b[2J\x1b[1;1Hz", "az"},
		{"osc bel", "a\x1b]52;c;SECRET\az", "az"},
		{"osc st", "a\x1b]0;SECRET\x1b\\z", "az"},
		{"hyperlink", "\x1b]8;;https://SECRET\x1b\\label\x1b]8;;\x1b\\", "label"},
		{"dcs", "a\x1bPSECRET\x1b\\z", "az"},
		{"sos", "a\x1bXSECRET\x1b\\z", "az"},
		{"pm", "a\x1b^SECRET\x1b\\z", "az"},
		{"apc", "a\x1b_SECRET\x1b\\z", "az"},
		{"raw c1", "a\x9d52;c;SECRET\x9cz\x9b31mx", "azx"},
		{"utf8 c1", "a\u009d52;c;SECRET\u009cz\u009b31mx", "azx"},
		{"raw dcs", "a\x90SECRET\x9cz", "az"},
		{"unterminated osc", "a\x1b]52;c;SECRET", "a"},
		{"unterminated csi", "a\x1b[31;", "a"},
		{"escape in string", "a\x1b]SECRET\x1b[31mSTILL_SECRET\az", "az"},
		{"intermediate", "a\x1b(Bz", "az"},
		{"controls", "a\x00\b\a\v\f\x7fz\n\r\t", "az   "},
		{"bidi", "a\u202eSECRET\u2069z\u2028\u2029", "aSECRETz  "},
		{"invalid utf8", "a\xffz", "a\ufffdz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SafeText(tc.input); got != tc.want {
				t.Fatalf("SafeText(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func assertPlain(t *testing.T, text string) {
	t.Helper()
	if !utf8.ValidString(text) {
		t.Fatal("invalid UTF-8 output")
	}
	for _, r := range text {
		if (unicode.IsControl(r) && r != '\n') || unicode.Is(unicode.Cf, r) {
			t.Fatalf("terminal control %U in %q", r, text)
		}
	}
}

func FuzzSafeText(f *testing.F) {
	for _, seed := range []string{"中文", "\x1b]52;c;SECRET\a", "\u009dSECRET\u009c", "\x1bPSECRET", "\x1b[31m", "\xff\x90x\x9c"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		got := SafeText(input)
		assertPlain(t, got)
		if strings.Contains(got, "\n") || SafeText(got) != got {
			t.Fatalf("not a stable safe line: %q", got)
		}
	})
}
