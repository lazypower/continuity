package cli

import "testing"

// validCategorySet is the client-side guard for `continuity remember`. It must
// stay in lockstep with the engine's validCategories map and the migration v9
// CHECK constraint; this test pins the membership so a drift surfaces here
// before it shows up as a 400 from the server.
func TestValidCategorySet(t *testing.T) {
	required := []string{
		"profile", "preferences", "feedback", "entities",
		"events", "patterns", "cases", "moments", "reference",
	}
	for _, c := range required {
		if !validCategorySet[c] {
			t.Errorf("validCategorySet missing %q", c)
		}
	}
	if validCategorySet["session"] {
		t.Error("validCategorySet must reject 'session' — it's a sentinel, not user-writable")
	}
	if validCategorySet["bogus"] {
		t.Error("validCategorySet accepted 'bogus'")
	}
}
