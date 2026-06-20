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
