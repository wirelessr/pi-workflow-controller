package tui

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// SafeText converts external text to a single terminal-safe line. Control
// strings (including unterminated ones) are consumed, not merely stripped of
// their introducer. Both UTF-8 and raw eight-bit C1 controls are recognized.
func SafeText(s string) string {
	const (
		ground = iota
		escape
		intermediate
		csi
		controlString
		stringEscape
	)
	state := ground
	osc := false
	var out strings.Builder
	for len(s) > 0 {
		r, n := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && n == 1 && s[0] >= 0x80 && s[0] <= 0x9f {
			r = rune(s[0])
		}
		s = s[n:]
		if state == controlString || state == stringEscape {
			if r == 0x9c || (osc && r == '\a') || (state == stringEscape && r == '\\') {
				state = ground
			} else if r == 0x1b {
				state = stringEscape
			} else {
				state = controlString
			}
			continue
		}
		switch r {
		case 0x1b:
			state = escape
			continue
		case 0x9b:
			state = csi
			continue
		case 0x90, 0x98, 0x9d, 0x9e, 0x9f:
			state, osc = controlString, r == 0x9d
			continue
		}
		switch state {
		case escape:
			switch r {
			case '[':
				state = csi
			case 'P', 'X', ']', '^', '_':
				state, osc = controlString, r == ']'
			default:
				if r >= 0x20 && r <= 0x2f {
					state = intermediate
				} else if r >= 0x30 && r <= 0x7e {
					state = ground
				}
			}
		case intermediate:
			if r >= 0x30 && r <= 0x7e {
				state = ground
			}
		case csi:
			if r >= 0x40 && r <= 0x7e {
				state = ground
			}
		case ground:
			switch {
			case r == '\n' || r == '\r' || r == '\t' || r == '\u2028' || r == '\u2029':
				out.WriteByte(' ')
			case unicode.IsControl(r), unicode.Is(unicode.Cf, r):
			default:
				out.WriteRune(r)
			}
		}
	}
	return out.String()
}
