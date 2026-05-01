//go:build darwin

package cli

import (
	"strings"
	"testing"
)

func TestGeneratePlistInjectsEnvironmentPATH(t *testing.T) {
	got, err := generatePlist()
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(got, "<key>EnvironmentVariables</key>") {
		t.Errorf("plist missing EnvironmentVariables key:\n%s", got)
	}
	if !strings.Contains(got, "<key>PATH</key>") {
		t.Errorf("plist missing PATH key inside EnvironmentVariables:\n%s", got)
	}
	if !strings.Contains(got, "/opt/homebrew/bin") {
		t.Errorf("plist PATH missing Apple-Silicon homebrew prefix:\n%s", got)
	}
	if !strings.Contains(got, "/usr/local/bin") {
		t.Errorf("plist PATH missing Intel homebrew prefix:\n%s", got)
	}

	// Homebrew prefixes must come before system stubs so `claude` resolves to the
	// brew install rather than any Apple-shipped binary of the same name.
	hbIdx := strings.Index(got, "/opt/homebrew/bin")
	sysIdx := strings.Index(got, "/usr/bin")
	if hbIdx < 0 || sysIdx < 0 || hbIdx > sysIdx {
		t.Errorf("homebrew prefix must precede /usr/bin in PATH (hb=%d sys=%d)", hbIdx, sysIdx)
	}
}
