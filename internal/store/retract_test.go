package store

import (
	"strings"
	"testing"
)

// seedNode is a small helper to keep the test bodies focused on retraction behavior.
func seedNode(t *testing.T, db *DB, uri, category, l0 string) *MemNode {
	t.Helper()
	node := &MemNode{
		URI:        uri,
		NodeType:   "leaf",
		Category:   category,
		L0Abstract: l0,
		L1Overview: l0 + " — body content here.",
	}
	if err := db.CreateNode(node); err != nil {
		t.Fatalf("seed %s: %v", uri, err)
	}
	return node
}

func TestRetractNode_SetsFieldsAndReason(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/events/foo", "events", "foo summary")

	newly, err := db.RetractNode("mem://user/events/foo", "test repro, no ongoing value", "")
	if err != nil {
		t.Fatalf("RetractNode: %v", err)
	}
	if !newly {
		t.Errorf("newly = false, want true on first retraction")
	}

	got, err := db.GetNodeByURI("mem://user/events/foo")
	if err != nil || got == nil {
		t.Fatalf("GetNodeByURI returned nil/err: %v", err)
	}
	if !got.IsRetracted() {
		t.Errorf("IsRetracted = false, want true")
	}
	if got.TombstoneReason != "test repro, no ongoing value" {
		t.Errorf("TombstoneReason = %q, want exact match", got.TombstoneReason)
	}
	if got.SupersededBy != "" {
		t.Errorf("SupersededBy = %q, want empty (pure tombstone)", got.SupersededBy)
	}
	if got.TombstonedAt == nil || *got.TombstonedAt == 0 {
		t.Errorf("TombstonedAt = %v, want non-zero timestamp", got.TombstonedAt)
	}
}

func TestRetractNode_RequiresReason(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/events/foo", "events", "foo summary")

	_, err := db.RetractNode("mem://user/events/foo", "", "")
	if err == nil {
		t.Error("expected error when reason is empty")
	}
	if !strings.Contains(err.Error(), "reason required") {
		t.Errorf("error = %q, want substring %q", err.Error(), "reason required")
	}
}

func TestRetractNode_ErrorsOnMissingURI(t *testing.T) {
	db := testDB(t)

	_, err := db.RetractNode("mem://user/events/does-not-exist", "any reason", "")
	if err == nil {
		t.Error("expected error when URI does not exist")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want substring %q", err.Error(), "not found")
	}
}

func TestRetractNode_IdempotentReRetract(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/events/foo", "events", "foo summary")

	newly1, err := db.RetractNode("mem://user/events/foo", "first reason", "")
	if err != nil || !newly1 {
		t.Fatalf("first retraction failed: %v / newly=%v", err, newly1)
	}

	// Re-retract with a different reason. Should be a no-op; original reason wins.
	newly2, err := db.RetractNode("mem://user/events/foo", "second reason", "")
	if err != nil {
		t.Fatalf("re-retract: %v", err)
	}
	if newly2 {
		t.Error("re-retract: newly = true, want false (idempotent)")
	}

	got, _ := db.GetNodeByURI("mem://user/events/foo")
	if got.TombstoneReason != "first reason" {
		t.Errorf("TombstoneReason = %q after re-retract, want original %q", got.TombstoneReason, "first reason")
	}
}

func TestRetractNode_SupersessionLink(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/preferences/old-style", "preferences", "old style preference")
	seedNode(t, db, "mem://user/preferences/new-style", "preferences", "new style preference")

	_, err := db.RetractNode("mem://user/preferences/old-style", "preference changed",
		"mem://user/preferences/new-style")
	if err != nil {
		t.Fatalf("RetractNode with supersedes: %v", err)
	}

	old, _ := db.GetNodeByURI("mem://user/preferences/old-style")
	if old.SupersededBy != "mem://user/preferences/new-style" {
		t.Errorf("SupersededBy = %q, want link to successor", old.SupersededBy)
	}
}

func TestRetractNode_SupersedesNonexistentSuccessor(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/preferences/old-style", "preferences", "old style preference")

	_, err := db.RetractNode("mem://user/preferences/old-style", "preference changed",
		"mem://user/preferences/never-existed")
	if err == nil {
		t.Error("expected error when superseded_by URI does not exist")
	}
	if !strings.Contains(err.Error(), "successor not found") {
		t.Errorf("error = %q, want substring %q", err.Error(), "successor not found")
	}
}

func TestIsRetracted_Predicate(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/events/live", "events", "live memory")
	seedNode(t, db, "mem://user/events/dead", "events", "dead memory")
	if _, err := db.RetractNode("mem://user/events/dead", "test", ""); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		uri  string
		want bool
	}{
		{"mem://user/events/live", false},
		{"mem://user/events/dead", true},
		{"mem://user/events/missing", false}, // missing URI returns false, no error
	}
	for _, tt := range tests {
		got, err := db.IsRetracted(tt.uri)
		if err != nil {
			t.Errorf("IsRetracted(%q): unexpected error: %v", tt.uri, err)
		}
		if got != tt.want {
			t.Errorf("IsRetracted(%q) = %v, want %v", tt.uri, got, tt.want)
		}
	}
}

func TestReadFilters_DefaultExcludesRetracted(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/events/live-1", "events", "live one")
	seedNode(t, db, "mem://user/events/live-2", "events", "live two")
	seedNode(t, db, "mem://user/events/dead", "events", "dead memory")
	if _, err := db.RetractNode("mem://user/events/dead", "test", ""); err != nil {
		t.Fatal(err)
	}

	t.Run("FindByCategory", func(t *testing.T) {
		nodes, err := db.FindByCategory("events")
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) != 2 {
			t.Errorf("FindByCategory returned %d nodes, want 2 (retracted excluded)", len(nodes))
		}
		for _, n := range nodes {
			if n.IsRetracted() {
				t.Errorf("FindByCategory returned retracted node %s", n.URI)
			}
		}
	})

	t.Run("ListLeaves", func(t *testing.T) {
		nodes, err := db.ListLeaves()
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range nodes {
			if n.IsRetracted() {
				t.Errorf("ListLeaves returned retracted node %s", n.URI)
			}
		}
	})

	t.Run("GetChildren", func(t *testing.T) {
		children, err := db.GetChildren("mem://user/events")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 2 {
			t.Errorf("GetChildren returned %d, want 2 (retracted excluded)", len(children))
		}
	})
}

func TestReadFilters_IncludingRetractedReturnsAll(t *testing.T) {
	db := testDB(t)
	seedNode(t, db, "mem://user/events/live", "events", "live")
	seedNode(t, db, "mem://user/events/dead", "events", "dead")
	if _, err := db.RetractNode("mem://user/events/dead", "test", ""); err != nil {
		t.Fatal(err)
	}

	t.Run("FindByCategoryIncludingRetracted", func(t *testing.T) {
		nodes, err := db.FindByCategoryIncludingRetracted("events")
		if err != nil {
			t.Fatal(err)
		}
		if len(nodes) != 2 {
			t.Errorf("FindByCategoryIncludingRetracted returned %d, want 2 (live + retracted)", len(nodes))
		}
	})

	t.Run("ListLeavesIncludingRetracted", func(t *testing.T) {
		nodes, err := db.ListLeavesIncludingRetracted()
		if err != nil {
			t.Fatal(err)
		}
		hasRetracted := false
		for _, n := range nodes {
			if n.IsRetracted() {
				hasRetracted = true
			}
		}
		if !hasRetracted {
			t.Error("ListLeavesIncludingRetracted did not return retracted node")
		}
	})

	t.Run("GetChildrenIncludingRetracted", func(t *testing.T) {
		children, err := db.GetChildrenIncludingRetracted("mem://user/events")
		if err != nil {
			t.Fatal(err)
		}
		if len(children) != 2 {
			t.Errorf("GetChildrenIncludingRetracted returned %d, want 2 (live + retracted)", len(children))
		}
	})
}

func TestRetractNode_MultiHopSupersessionChain(t *testing.T) {
	// A → B → C → D: ensure chain integrity, no short-circuit.
	db := testDB(t)
	seedNode(t, db, "mem://user/preferences/A", "preferences", "version A")
	seedNode(t, db, "mem://user/preferences/B", "preferences", "version B")
	seedNode(t, db, "mem://user/preferences/C", "preferences", "version C")
	seedNode(t, db, "mem://user/preferences/D", "preferences", "version D")

	if _, err := db.RetractNode("mem://user/preferences/A", "evolved to B", "mem://user/preferences/B"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode("mem://user/preferences/B", "evolved to C", "mem://user/preferences/C"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetractNode("mem://user/preferences/C", "evolved to D", "mem://user/preferences/D"); err != nil {
		t.Fatal(err)
	}

	// Default reads see only D.
	live, err := db.FindByCategory("preferences")
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0].URI != "mem://user/preferences/D" {
		t.Errorf("default reads should return only D, got %d nodes (first=%v)", len(live), live)
	}

	// Each intermediate node links to its direct successor (no short-circuit to D).
	a, _ := db.GetNodeByURI("mem://user/preferences/A")
	if a.SupersededBy != "mem://user/preferences/B" {
		t.Errorf("A.SupersededBy = %q, want B (no short-circuit)", a.SupersededBy)
	}
	b, _ := db.GetNodeByURI("mem://user/preferences/B")
	if b.SupersededBy != "mem://user/preferences/C" {
		t.Errorf("B.SupersededBy = %q, want C", b.SupersededBy)
	}
	c, _ := db.GetNodeByURI("mem://user/preferences/C")
	if c.SupersededBy != "mem://user/preferences/D" {
		t.Errorf("C.SupersededBy = %q, want D", c.SupersededBy)
	}
}
