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

	if !strings.Contains(unit, "Environment=PATH=") {
		t.Errorf("unit missing Environment=PATH= line:\n%s", unit)
	}
	if !strings.Contains(unit, "/custom/tools") {
		t.Errorf("unit PATH stanza missing captured install-time dir:\n%s", unit)
	}
	if !strings.Contains(unit, "/usr/bin") {
		t.Errorf("unit PATH stanza missing common default dir:\n%s", unit)
	}
}

// TestGenerateUnitPATHIsSingleLine asserts Fix 2: a PATH entry carrying a newline
// (line-injection attempt) must not produce an extra/forged line in the systemd
// unit. The Environment=PATH= value must stay a single line and the injected
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

	// Find the Environment=PATH= line and ensure it is a single, self-contained
	// line that still carries the well-formed entries.
	var pathLine string
	for _, line := range strings.Split(unit, "\n") {
		if strings.HasPrefix(line, "Environment=PATH=") {
			pathLine = line
			break
		}
	}
	if pathLine == "" {
		t.Fatalf("no Environment=PATH= line found:\n%s", unit)
	}
	if !strings.Contains(pathLine, "/good/bin") || !strings.Contains(pathLine, "/also/good") {
		t.Errorf("well-formed PATH entries missing from the line: %q", pathLine)
	}
	if strings.ContainsAny(pathLine, "\r\t") {
		t.Errorf("Environment=PATH= line carries control chars: %q", pathLine)
	}
}

// TestGenerateUnitContainsSpaceDir confirms a space-containing PATH dir survives
// into the systemd unit (spaces are valid in an Environment= value).
func TestGenerateUnitContainsSpaceDir(t *testing.T) {
	t.Setenv("PATH", "/opt/My Tools/bin")
	unit, err := generateUnit()
	if err != nil {
		t.Fatalf("generateUnit: %v", err)
	}
	if !strings.Contains(unit, "/opt/My Tools/bin") {
		t.Errorf("space-containing dir not preserved in unit:\n%s", unit)
	}
}
