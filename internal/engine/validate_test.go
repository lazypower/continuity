package engine

import (
	"strings"
	"testing"
)

func TestSanitizeURIHint(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"coding-style", "coding-style"},
		{"coding_style", "coding_style"},
		{"CodingStyle", "codingstyle"},
		{"coding style", "coding-style"},
		{"coding.style", "coding-style"},
		{"coding/style", "coding-style"},
		{"  spaces  ", "spaces"},
		{"---leading", "leading"},
		{"trailing---", "trailing"},
		{"a--b", "a--b"}, // double hyphens OK, not worth overcomplicating
		{"valid123", "valid123"},
		{"café", "caf"},         // non-ascii dropped
		{"hello world!", "hello-world"},
		{"", ""},
		{"   ", ""},
		{"!!!!", ""},
		{"../../../etc/passwd", "etc-passwd"},
		{"'; DROP TABLE", "drop-table"},
	}

	for _, tt := range tests {
		got := sanitizeURIHint(tt.input)
		if got != tt.want {
			t.Errorf("sanitizeURIHint(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestValidateCandidate_Valid(t *testing.T) {
	c := memoryCandidate{
		Category: "profile",
		URIHint:  "coding-style",
		L0:       "User prefers Go with minimal dependencies",
		L1:       "Detailed overview of coding preferences and style choices.",
		L2:       "Full content about coding style...",
	}

	vc, err := validateCandidate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vc.URIHint != "coding-style" {
		t.Errorf("URIHint = %q, want %q", vc.URIHint, "coding-style")
	}
}

func TestValidateCandidate_InvalidCategory(t *testing.T) {
	c := memoryCandidate{Category: "bogus", URIHint: "x", L0: "something", L1: "something longer than 20 chars"}
	_, err := validateCandidate(c)
	if err == nil {
		t.Error("expected error for invalid category")
	}
}

func TestValidateCandidate_EmptyURIHint(t *testing.T) {
	c := memoryCandidate{Category: "profile", URIHint: "!!!", L0: "something", L1: "something longer than 20 chars"}
	_, err := validateCandidate(c)
	if err == nil {
		t.Error("expected error for URI hint that sanitizes to empty")
	}
}

func TestValidateCandidate_EmptyL0(t *testing.T) {
	c := memoryCandidate{Category: "profile", URIHint: "test", L0: "", L1: "something longer than 20 chars"}
	_, err := validateCandidate(c)
	if err == nil {
		t.Error("expected error for empty L0")
	}
}

func TestValidateCandidate_TrivialL1(t *testing.T) {
	c := memoryCandidate{Category: "profile", URIHint: "test", L0: "abstract", L1: "short"}
	_, err := validateCandidate(c)
	if err == nil {
		t.Error("expected error for trivial L1")
	}
}

func TestValidateCandidate_TruncatesOversizedL0(t *testing.T) {
	longL0 := strings.Repeat("word ", 200) // 1000 chars, over 800 limit
	c := memoryCandidate{
		Category: "profile",
		URIHint:  "test",
		L0:       longL0,
		L1:       "This is a valid L1 overview that is long enough.",
		L2:       "content",
	}

	vc, err := validateCandidate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(vc.L0) > maxL0Chars {
		t.Errorf("L0 length = %d, want ≤ %d", len(vc.L0), maxL0Chars)
	}
}

func TestValidateCandidate_TruncatesOversizedL1(t *testing.T) {
	longL1 := strings.Repeat("word ", 3000) // 15000 chars, over 12000 limit
	c := memoryCandidate{
		Category: "profile",
		URIHint:  "test",
		L0:       "abstract",
		L1:       longL1,
		L2:       "content",
	}

	vc, err := validateCandidate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(vc.L1) > maxL1Chars {
		t.Errorf("L1 length = %d, want ≤ %d", len(vc.L1), maxL1Chars)
	}
}

func TestValidateCandidate_SanitizesURIHint(t *testing.T) {
	c := memoryCandidate{
		Category: "profile",
		URIHint:  "Coding Style",
		L0:       "User prefers Go",
		L1:       "Detailed overview of coding preferences and style.",
	}

	vc, err := validateCandidate(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if vc.URIHint != "coding-style" {
		t.Errorf("URIHint = %q, want %q", vc.URIHint, "coding-style")
	}
}

func TestTruncateClean(t *testing.T) {
	s := "hello world this is a test string"
	result := truncateClean(s, 15)
	if len(result) > 15 {
		t.Errorf("truncateClean result too long: %d", len(result))
	}
	// Should cut at word boundary
	if strings.HasSuffix(result, " ") {
		t.Error("truncated result has trailing space")
	}
}
