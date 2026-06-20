//go:build darwin

package cli

import (
	"encoding/xml"
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

// TestGeneratePlistEscapesXMLSpecialChars asserts Fix 2: a PATH containing XML
// special chars (&, <, >) and spaces is escaped so the generated plist stays
// well-formed XML and the raw chars do not appear unescaped inside the value.
func TestGeneratePlistEscapesXMLSpecialChars(t *testing.T) {
	// Newline/CR entries are dropped by buildServicePATH; here we focus on the
	// XML-special chars that survive into the value and must be escaped.
	t.Setenv("PATH", "/opt/A&B/bin:/tools/<weird>:/My Tools/bin")

	plist, err := generatePlist()
	if err != nil {
		t.Fatalf("generatePlist: %v", err)
	}

	// Must parse as well-formed XML.
	if err := xml.Unmarshal([]byte(plist), new(struct {
		XMLName xml.Name `xml:"plist"`
	})); err != nil {
		t.Fatalf("generated plist is not well-formed XML: %v\n%s", err, plist)
	}

	// The escaped entities must be present, the raw special chars must NOT appear
	// inside the PATH value.
	if !strings.Contains(plist, "/opt/A&amp;B/bin") {
		t.Errorf("'&' in PATH was not XML-escaped to &amp;:\n%s", plist)
	}
	if strings.Contains(plist, "/opt/A&B/bin") {
		t.Errorf("raw unescaped '&' leaked into plist:\n%s", plist)
	}
	if strings.Contains(plist, "/tools/<weird>") {
		t.Errorf("raw unescaped '<weird>' leaked into plist:\n%s", plist)
	}
	// Spaces are legal in XML text and need no escaping, but must be preserved.
	if !strings.Contains(plist, "/My Tools/bin") {
		t.Errorf("space-containing dir not preserved in plist:\n%s", plist)
	}
}
