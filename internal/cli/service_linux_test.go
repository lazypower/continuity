//go:build linux

package cli

import (
	"strings"
	"testing"
)

// TestGenerateUnitContainsPATHStanza asserts the generated systemd user unit
// bakes in an Environment=PATH= line so the service can resolve the LLM provider
// binary (issue #41) — systemd does not inherit the login PATH.
func TestGenerateUnitContainsPATHStanza(t *testing.T) {
	t.Setenv("PATH", "/custom/tools:/opt/homebrew/bin")

	unit, err := generateUnit()
	if err != nil {
		t.Fatalf("generateUnit: %v", err)
	}

	if !strings.Contains(unit, `Environment="PATH=`) {
		t.Errorf("unit missing quoted Environment=\"PATH= line:\n%s", unit)
	}
	if !strings.Contains(unit, "/custom/tools") {
		t.Errorf("unit PATH stanza missing captured install-time dir:\n%s", unit)
	}
	if !strings.Contains(unit, "/usr/bin") {
		t.Errorf("unit PATH stanza missing common default dir:\n%s", unit)
	}
}

// envPATHLine returns the single `Environment="PATH=...` line from a generated
// systemd unit, or "" if none is present.
func envPATHLine(unit string) string {
	for _, line := range strings.Split(unit, "\n") {
		if strings.HasPrefix(line, `Environment="PATH=`) {
			return line
		}
	}
	return ""
}

// TestGenerateUnitPATHIsSingleLine asserts that a PATH entry carrying a newline
// (line-injection attempt) must not produce an extra/forged line in the systemd
// unit. The Environment="PATH= value must stay a single line and the injected
// content must be absent.
func TestGenerateUnitPATHIsSingleLine(t *testing.T) {
	// The middle entry tries to inject a unit directive via an embedded newline.
	t.Setenv("PATH", "/good/bin:/evil\nExecStartPre=/bin/rm -rf ~:/also/good")

	unit, err := generateUnit()
	if err != nil {
		t.Fatalf("generateUnit: %v", err)
	}

	// The injected directive must not appear anywhere in the unit.
	if strings.Contains(unit, "ExecStartPre=/bin/rm") {
		t.Errorf("line-injection via PATH newline was not neutralized:\n%s", unit)
	}

	pathLine := envPATHLine(unit)
	if pathLine == "" {
		t.Fatalf("no Environment=\"PATH= line found:\n%s", unit)
	}
	if !strings.Contains(pathLine, "/good/bin") || !strings.Contains(pathLine, "/also/good") {
		t.Errorf("well-formed PATH entries missing from the line: %q", pathLine)
	}
	if strings.ContainsAny(pathLine, "\r\t") {
		t.Errorf("Environment=\"PATH= line carries control chars: %q", pathLine)
	}
}

// TestGenerateUnitPATHIsQuoted asserts the Environment= assignment is double-
// quoted per systemd syntax. Without quoting, a value with a space would be
// split by systemd's Environment= parser.
func TestGenerateUnitPATHIsQuoted(t *testing.T) {
	t.Setenv("PATH", "/usr/bin")
	unit, err := generateUnit()
	if err != nil {
		t.Fatalf("generateUnit: %v", err)
	}
	pathLine := envPATHLine(unit)
	if pathLine == "" {
		t.Fatalf("no quoted Environment=\"PATH= line found:\n%s", unit)
	}
	// The value must be enclosed in double quotes: Environment="PATH=...".
	if !strings.HasSuffix(pathLine, `"`) {
		t.Errorf("Environment= assignment is not closed with a double quote: %q", pathLine)
	}
}

// TestGenerateUnitSpaceDirNotSplit confirms a space-containing PATH dir survives
// INSIDE the quotes — it must appear intact, not truncated at the space. With an
// unquoted assignment systemd would treat the post-space remainder as a second
// KEY=VALUE pair.
func TestGenerateUnitSpaceDirNotSplit(t *testing.T) {
	t.Setenv("PATH", "/opt/My Tools/bin")
	unit, err := generateUnit()
	if err != nil {
		t.Fatalf("generateUnit: %v", err)
	}
	pathLine := envPATHLine(unit)
	if pathLine == "" {
		t.Fatalf("no quoted Environment=\"PATH= line found:\n%s", unit)
	}
	// The full space-containing dir must be present, and it must sit BEFORE the
	// closing quote (i.e. inside the quoted value, not split out).
	if !strings.Contains(pathLine, "/opt/My Tools/bin") {
		t.Errorf("space-containing dir not preserved in unit line: %q", pathLine)
	}
	inner := strings.TrimPrefix(pathLine, `Environment="`)
	inner = strings.TrimSuffix(inner, `"`)
	if !strings.Contains(inner, "/opt/My Tools/bin") {
		t.Errorf("space-containing dir escaped the quoted value: %q", pathLine)
	}
}

// TestGenerateUnitNoSecondAssignmentViaSpace asserts a crafted PATH entry cannot
// produce a second systemd environment assignment. With the value quoted, an
// entry like " FOO=bar" stays part of the single PATH value and is never parsed
// as its own KEY=VALUE pair.
func TestGenerateUnitNoSecondAssignmentViaSpace(t *testing.T) {
	t.Setenv("PATH", "/good/bin: FOO=bar:/also/good")
	unit, err := generateUnit()
	if err != nil {
		t.Fatalf("generateUnit: %v", err)
	}
	pathLine := envPATHLine(unit)
	if pathLine == "" {
		t.Fatalf("no quoted Environment=\"PATH= line found:\n%s", unit)
	}
	// The crafted token must remain inside the quoted PATH value, never breaking
	// out as a standalone assignment on the line.
	inner := strings.TrimPrefix(pathLine, `Environment="`)
	inner = strings.TrimSuffix(inner, `"`)
	if !strings.Contains(inner, "FOO=bar") {
		t.Errorf("crafted token vanished instead of being contained: %q", pathLine)
	}
	// The line must still be a single Environment= directive (only one '=' worth
	// of directive name): it must start with Environment=" and not contain an
	// unescaped quote that would prematurely close the value.
	body := inner
	// A stray unescaped double-quote inside the value would let an attacker close
	// the quote and append a directive; ensure none survived unescaped.
	for i := 0; i < len(body); i++ {
		if body[i] == '"' && (i == 0 || body[i-1] != '\\') {
			t.Errorf("unescaped double-quote inside Environment value allows breakout: %q", pathLine)
			break
		}
	}
}

// TestGenerateUnitEscapesQuoteAndBackslash asserts a value carrying a double-
// quote or backslash is escaped so it cannot break out of the quoted assignment.
func TestGenerateUnitEscapesQuoteAndBackslash(t *testing.T) {
	// /x" + "\nEnvironment=EVIL=1 style breakout attempt (quote + backslash). The
	// newline itself is dropped upstream as a control char; the quote/backslash
	// must be escaped.
	t.Setenv("PATH", `/good/bin:/x"q\b:/also/good`)
	unit, err := generateUnit()
	if err != nil {
		t.Fatalf("generateUnit: %v", err)
	}
	if strings.Contains(unit, "Environment=EVIL=1") {
		t.Errorf("quote-breakout produced a forged assignment:\n%s", unit)
	}
	pathLine := envPATHLine(unit)
	if pathLine == "" {
		t.Fatalf("no quoted Environment=\"PATH= line found:\n%s", unit)
	}
	// The embedded double-quote must be backslash-escaped inside the value.
	if !strings.Contains(pathLine, `\"q`) {
		t.Errorf("embedded double-quote not escaped: %q", pathLine)
	}
	// The embedded backslash must be doubled.
	if !strings.Contains(pathLine, `q\\b`) {
		t.Errorf("embedded backslash not escaped: %q", pathLine)
	}
}

// TestGenerateUnitEscapesPercent asserts a '%' in a PATH entry is doubled to
// '%%' so systemd does not treat it as a specifier (e.g. %h, %i).
func TestGenerateUnitEscapesPercent(t *testing.T) {
	t.Setenv("PATH", "/good/bin:/opt/100%cool/bin")
	unit, err := generateUnit()
	if err != nil {
		t.Fatalf("generateUnit: %v", err)
	}
	pathLine := envPATHLine(unit)
	if pathLine == "" {
		t.Fatalf("no quoted Environment=\"PATH= line found:\n%s", unit)
	}
	if !strings.Contains(pathLine, "/opt/100%%cool/bin") {
		t.Errorf("percent not escaped to %%%% in Environment value: %q", pathLine)
	}
}
