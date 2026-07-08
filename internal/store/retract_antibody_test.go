package store

import "testing"

// TestRetractErasesContentKeepsReceipt pins antibody retraction: the bulky l1/l2
// content is erased (not merely hidden), while the receipt — the tombstone, reason,
// and the l0 label — survives, and the row remains (so the stored vector still
// guards against resurrection).
func TestRetractErasesContentKeepsReceipt(t *testing.T) {
	db := testDB(t)
	uri := "mem://user/events/secret"
	if err := db.CreateNode(&MemNode{
		URI: uri, NodeType: "leaf", Category: "events",
		L0Abstract: "operator home address",
		L1Overview: "123 Main Street, the full sensitive body text goes here",
		L2Content:  "even more sensitive detail retained in l2",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := db.RetractNode(uri, "PII captured by accident", ""); err != nil {
		t.Fatal(err)
	}

	n, err := db.GetNodeByURI(uri)
	if err != nil || n == nil {
		t.Fatalf("receipt row should still exist: %v", err)
	}
	if !n.IsRetracted() {
		t.Fatal("node should be retracted")
	}
	if n.L1Overview != "" || n.L2Content != "" {
		t.Errorf("content not erased: l1=%q l2=%q", n.L1Overview, n.L2Content)
	}
	if n.L0Abstract == "" {
		t.Error("default retract must keep l0 as the receipt label / antibody re-embed source")
	}
	if n.TombstoneReason != "PII captured by accident" {
		t.Errorf("receipt reason lost: %q", n.TombstoneReason)
	}
}

// TestReRetractErasesLegacyContent pins the upgrade path (codex P1): a node
// retracted by an older binary still has its content in the row. Re-retracting it
// under this binary erases the retained content WITHOUT disturbing the original
// receipt (reason/timestamp), and reports newly=false.
func TestReRetractErasesLegacyContent(t *testing.T) {
	db := testDB(t)
	uri := "mem://user/events/legacy"
	if err := db.CreateNode(&MemNode{
		URI: uri, NodeType: "leaf", Category: "events",
		L0Abstract: "legacy abstract",
		L1Overview: "legacy body still present in the row",
		L2Content:  "legacy l2 still present",
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate an OLD-binary tombstone: tombstoned, but content retained.
	if _, err := db.Exec(
		`UPDATE mem_nodes SET tombstoned_at = ?, tombstone_reason = ? WHERE uri = ?`,
		1, "old-binary retraction", uri,
	); err != nil {
		t.Fatal(err)
	}

	newly, err := db.RetractNode(uri, "ignored — already retracted", "")
	if err != nil {
		t.Fatal(err)
	}
	if newly {
		t.Error("re-retract of an already-retracted node must report newly=false")
	}

	n, err := db.GetNodeByURI(uri)
	if err != nil || n == nil {
		t.Fatalf("row should still exist: %v", err)
	}
	if !n.IsRetracted() {
		t.Fatal("still retracted")
	}
	if n.L1Overview != "" || n.L2Content != "" {
		t.Errorf("legacy content not erased on re-retract: l1=%q l2=%q", n.L1Overview, n.L2Content)
	}
	if n.TombstoneReason != "old-binary retraction" {
		t.Errorf("original receipt reason must be preserved, got %q", n.TombstoneReason)
	}
}
