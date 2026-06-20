//go:build darwin

package cli

import (
	"strings"
	"testing"
)

// TestGeneratePlistContainsPATHStanza asserts the generated LaunchAgent plist
// bakes in an EnvironmentVariables/PATH stanza so the service can resolve the
// LLM provider binary (issue #41) — launchd does not inherit the login PATH.
func TestGeneratePlistContainsPATHStanza(t *testing.T) {
	t.Setenv("PATH", "/custom/tools:/opt/homebrew/bin")

	plist, err := generatePlist()
	if err != nil {
		t.Fatalf("generatePlist: %v", err)
	}

	if !strings.Contains(plist, "<key>EnvironmentVariables</key>") {
		t.Errorf("plist missing EnvironmentVariables dict:\n%s", plist)
	}
	if !strings.Contains(plist, "<key>PATH</key>") {
		t.Errorf("plist missing PATH key:\n%s", plist)
	}
	if !strings.Contains(plist, "/custom/tools") {
		t.Errorf("plist PATH stanza missing captured install-time dir:\n%s", plist)
	}
	if !strings.Contains(plist, "/usr/bin") {
		t.Errorf("plist PATH stanza missing common default dir:\n%s", plist)
	}
}
