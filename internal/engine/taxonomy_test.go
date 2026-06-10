package engine

import "testing"

// Issue #24 added `feedback` and `reference` as first-class categories. Pin
// owner routing so neither one ever silently flips to the agent-side tree.
func TestOwnerForCategory(t *testing.T) {
	tests := []struct {
		category string
		want     string
	}{
		{"profile", "user"},
		{"preferences", "user"},
		{"feedback", "user"},
		{"entities", "user"},
		{"events", "user"},
		{"reference", "user"},
		{"moments", "user"},
		{"patterns", "agent"},
		{"cases", "agent"},
	}
	for _, tt := range tests {
		if got := ownerForCategory(tt.category); got != tt.want {
			t.Errorf("ownerForCategory(%q) = %q, want %q", tt.category, got, tt.want)
		}
	}
}

// Pin merge rules for every category. feedback is mergeable (consolidate
// near-duplicate rules); reference is NOT (each pointer is a distinct entry,
// merging would corrupt the lookup).
func TestMergeableCategory(t *testing.T) {
	tests := []struct {
		category string
		want     bool
	}{
		{"profile", true},
		{"preferences", true},
		{"patterns", true},
		{"feedback", true},
		{"entities", false},
		{"events", false},
		{"cases", false},
		{"reference", false},
		{"moments", false},
		{"bogus", false},
	}
	for _, tt := range tests {
		if got := mergeableCategory(tt.category); got != tt.want {
			t.Errorf("mergeableCategory(%q) = %v, want %v", tt.category, got, tt.want)
		}
	}
}

// validCategories must include every category the system writes. If a new
// category is added to the migrations but not to the validator, extraction
// silently drops it.
func TestValidCategoriesContainsAll(t *testing.T) {
	required := []string{
		"profile", "preferences", "entities", "events",
		"patterns", "cases", "moments", "feedback", "reference",
	}
	for _, c := range required {
		if !validCategories[c] {
			t.Errorf("validCategories missing %q", c)
		}
	}
	if validCategories["session"] {
		t.Error("validCategories must not accept 'session' — it's a sentinel, not a writable category")
	}
	if validCategories["bogus"] {
		t.Error("validCategories accepted 'bogus'")
	}
}
