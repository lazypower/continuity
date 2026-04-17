package store

import (
	"testing"
)

func TestCreateNode(t *testing.T) {
	db := testDB(t)

	node := &MemNode{
		URI:           "mem://user/profile/coding-style",
		NodeType:      "leaf",
		Category:      "profile",
		L0Abstract:    "Prefers Go with minimal dependencies",
		L1Overview:    "Detailed coding style overview...",
		L2Content:     "Full content here...",
		SourceSession: "sess-001",
	}

	if err := db.CreateNode(node); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	if node.ID == 0 {
		t.Error("expected non-zero ID")
	}
	if !node.Mergeable {
		t.Error("profile category should be mergeable")
	}
	if node.Relevance != 1.0 {
		t.Errorf("relevance = %f, want 1.0", node.Relevance)
	}
}

func TestCreateNodeImmutable(t *testing.T) {
	db := testDB(t)

	node := &MemNode{
		URI:      "mem://user/events/deployed-v2",
		NodeType: "leaf",
		Category: "events",
	}
	if err := db.CreateNode(node); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}
	if node.Mergeable {
		t.Error("events category should not be mergeable")
	}
}

func TestGetNodeByURI(t *testing.T) {
	db := testDB(t)

	// Not found
	n, err := db.GetNodeByURI("mem://nonexistent")
	if err != nil {
		t.Fatalf("GetNodeByURI: %v", err)
	}
	if n != nil {
		t.Error("expected nil for nonexistent URI")
	}

	// Create and find
	node := &MemNode{
		URI:        "mem://user/profile/coding-style",
		NodeType:   "leaf",
		Category:   "profile",
		L0Abstract: "Go developer",
	}
	db.CreateNode(node)

	found, err := db.GetNodeByURI("mem://user/profile/coding-style")
	if err != nil {
		t.Fatalf("GetNodeByURI: %v", err)
	}
	if found == nil {
		t.Fatal("expected node, got nil")
	}
	if found.L0Abstract != "Go developer" {
		t.Errorf("l0_abstract = %q, want %q", found.L0Abstract, "Go developer")
	}
	if !found.Mergeable {
		t.Error("expected mergeable for profile")
	}
}

func TestUpdateNode(t *testing.T) {
	db := testDB(t)

	node := &MemNode{
		URI:        "mem://user/profile/coding-style",
		NodeType:   "leaf",
		Category:   "profile",
		L0Abstract: "Old abstract",
	}
	db.CreateNode(node)

	node.L0Abstract = "New abstract"
	node.L1Overview = "Updated overview"
	if err := db.UpdateNode(node); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	found, _ := db.GetNodeByURI("mem://user/profile/coding-style")
	if found.L0Abstract != "New abstract" {
		t.Errorf("l0_abstract = %q, want %q", found.L0Abstract, "New abstract")
	}
	if found.L1Overview != "Updated overview" {
		t.Errorf("l1_overview = %q, want %q", found.L1Overview, "Updated overview")
	}
}

func TestUpsertNodeMergeable(t *testing.T) {
	db := testDB(t)

	// First insert
	node := &MemNode{
		URI:        "mem://user/profile/coding-style",
		NodeType:   "leaf",
		Category:   "profile",
		L0Abstract: "Original",
	}
	if err := db.UpsertNode(node); err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}

	// Upsert (merge)
	node2 := &MemNode{
		URI:        "mem://user/profile/coding-style",
		NodeType:   "leaf",
		Category:   "profile",
		L0Abstract: "Updated",
	}
	if err := db.UpsertNode(node2); err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}

	// Should only have one node
	nodes, _ := db.FindByCategory("profile")
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].L0Abstract != "Updated" {
		t.Errorf("l0_abstract = %q, want %q", nodes[0].L0Abstract, "Updated")
	}
}

func TestUpsertNodeImmutable(t *testing.T) {
	db := testDB(t)

	// First insert
	node := &MemNode{
		URI:        "mem://user/events/deployed-v2",
		NodeType:   "leaf",
		Category:   "events",
		L0Abstract: "Deployed v2",
	}
	db.UpsertNode(node)

	// Second insert — should create new node with different URI
	node2 := &MemNode{
		URI:        "mem://user/events/deployed-v2",
		NodeType:   "leaf",
		Category:   "events",
		L0Abstract: "Deployed v2 again",
	}
	db.UpsertNode(node2)

	nodes, _ := db.FindByCategory("events")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes for immutable category, got %d", len(nodes))
	}
}

func TestFindByCategory(t *testing.T) {
	db := testDB(t)

	db.CreateNode(&MemNode{URI: "mem://user/profile/a", NodeType: "leaf", Category: "profile", L0Abstract: "a"})
	db.CreateNode(&MemNode{URI: "mem://user/profile/b", NodeType: "leaf", Category: "profile", L0Abstract: "b"})
	db.CreateNode(&MemNode{URI: "mem://user/events/c", NodeType: "leaf", Category: "events", L0Abstract: "c"})

	profiles, err := db.FindByCategory("profile")
	if err != nil {
		t.Fatalf("FindByCategory: %v", err)
	}
	if len(profiles) != 2 {
		t.Errorf("expected 2 profiles, got %d", len(profiles))
	}

	events, _ := db.FindByCategory("events")
	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

func TestTouchNode(t *testing.T) {
	db := testDB(t)

	db.CreateNode(&MemNode{URI: "mem://user/profile/coding-style", NodeType: "leaf", Category: "profile"})

	if err := db.TouchNode("mem://user/profile/coding-style"); err != nil {
		t.Fatalf("TouchNode: %v", err)
	}

	node, _ := db.GetNodeByURI("mem://user/profile/coding-style")
	if node.AccessCount != 1 {
		t.Errorf("access_count = %d, want 1", node.AccessCount)
	}
	if node.LastAccess == nil {
		t.Error("expected last_access to be set")
	}
	if node.Relevance != 1.0 {
		t.Errorf("relevance = %f, want 1.0 after touch", node.Relevance)
	}
}

func TestListLeaves(t *testing.T) {
	db := testDB(t)

	// CreateNode auto-creates parent dirs, so just create leaves
	db.CreateNode(&MemNode{URI: "mem://user/profile/a", NodeType: "leaf", Category: "profile"})
	db.CreateNode(&MemNode{URI: "mem://user/events/b", NodeType: "leaf", Category: "events"})

	leaves, err := db.ListLeaves()
	if err != nil {
		t.Fatalf("ListLeaves: %v", err)
	}
	if len(leaves) != 2 {
		t.Errorf("expected 2 leaves, got %d", len(leaves))
	}
}

func TestEnsureParentDirs(t *testing.T) {
	db := testDB(t)

	if err := db.EnsureParentDirs("mem://user/profile/coding-style", "profile"); err != nil {
		t.Fatalf("EnsureParentDirs: %v", err)
	}

	// Check that parent dirs were created
	user, _ := db.GetNodeByURI("mem://user")
	if user == nil {
		t.Error("expected mem://user dir to exist")
	} else if user.NodeType != "dir" {
		t.Errorf("mem://user node_type = %q, want dir", user.NodeType)
	}

	profile, _ := db.GetNodeByURI("mem://user/profile")
	if profile == nil {
		t.Error("expected mem://user/profile dir to exist")
	}

	// Idempotent
	if err := db.EnsureParentDirs("mem://user/profile/coding-style", "profile"); err != nil {
		t.Fatalf("second EnsureParentDirs: %v", err)
	}
}

func TestDecayAllNodes(t *testing.T) {
	db := testDB(t)

	// Create a node with old timestamps (simulate old data)
	db.CreateNode(&MemNode{URI: "mem://user/events/old", NodeType: "leaf", Category: "events"})

	// Newly created nodes have relevance 1.0 and last_access = now, so no decay should happen
	updated, err := db.DecayAllNodes()
	if err != nil {
		t.Fatalf("DecayAllNodes: %v", err)
	}
	// Fresh nodes shouldn't decay
	if updated != 0 {
		t.Errorf("expected 0 decayed nodes for fresh data, got %d", updated)
	}
}

func TestCreateMomentNode(t *testing.T) {
	db := testDB(t)

	node := &MemNode{
		URI:        "mem://user/moments/first-gift",
		NodeType:   "leaf",
		Category:   "moments",
		L0Abstract: "walked me through my own reflections, then presented a spec built from my ask",
		L1Overview: "Context about the moment and why it matters...",
	}
	if err := db.CreateNode(node); err != nil {
		t.Fatalf("CreateNode moments: %v", err)
	}
	if node.Mergeable {
		t.Error("moments category should not be mergeable")
	}
}

func TestDecaySkipsMoments(t *testing.T) {
	db := testDB(t)

	// Create a moment and an event
	db.CreateNode(&MemNode{URI: "mem://user/moments/anchor", NodeType: "leaf", Category: "moments"})
	db.CreateNode(&MemNode{URI: "mem://user/events/old-event", NodeType: "leaf", Category: "events"})

	// Manually backdate both nodes so decay would apply
	db.Exec(`UPDATE mem_nodes SET created_at = 0, last_access = NULL WHERE node_type = 'leaf'`)

	updated, err := db.DecayAllNodes()
	if err != nil {
		t.Fatalf("DecayAllNodes: %v", err)
	}

	// Only the event should have decayed, not the moment
	if updated != 1 {
		t.Errorf("expected 1 decayed node (event only), got %d", updated)
	}

	// Verify moment still has full relevance
	moment, _ := db.GetNodeByURI("mem://user/moments/anchor")
	if moment.Relevance != 1.0 {
		t.Errorf("moment relevance = %f, want 1.0 (exempt from decay)", moment.Relevance)
	}

	// Verify event did decay
	event, _ := db.GetNodeByURI("mem://user/events/old-event")
	if event.Relevance >= 1.0 {
		t.Errorf("event relevance = %f, expected < 1.0 (should have decayed)", event.Relevance)
	}
}

func TestFindByCategoryMoments(t *testing.T) {
	db := testDB(t)

	db.CreateNode(&MemNode{URI: "mem://user/moments/a", NodeType: "leaf", Category: "moments", L0Abstract: "moment a"})
	db.CreateNode(&MemNode{URI: "mem://user/moments/b", NodeType: "leaf", Category: "moments", L0Abstract: "moment b"})
	db.CreateNode(&MemNode{URI: "mem://user/events/c", NodeType: "leaf", Category: "events", L0Abstract: "event c"})

	moments, err := db.FindByCategory("moments")
	if err != nil {
		t.Fatalf("FindByCategory moments: %v", err)
	}
	if len(moments) != 2 {
		t.Errorf("expected 2 moments, got %d", len(moments))
	}
}

// testDB is a helper that creates an in-memory DB for testing.
func testDB(t *testing.T) *DB {
	t.Helper()
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
