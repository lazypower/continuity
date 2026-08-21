package store

import (
	"strings"
	"testing"
)

func TestTextNearIdentical_Exact(t *testing.T) {
	if !textNearIdentical("hello world", "hello world") {
		t.Error("identical strings should be near-identical")
	}
}

func TestTextNearIdentical_Empty(t *testing.T) {
	if textNearIdentical("", "hello") {
		t.Error("empty vs non-empty should not be near-identical")
	}
	if textNearIdentical("hello", "") {
		t.Error("non-empty vs empty should not be near-identical")
	}
	if !textNearIdentical("", "") {
		t.Error("both empty should be near-identical")
	}
}

func TestTextNearIdentical_MinorDiff(t *testing.T) {
	a := "User prefers Go with minimal dependencies and clean code style"
	b := "User prefers Go with minimal dependencies and clean code styles"
	if !textNearIdentical(a, b) {
		t.Error("strings differing by one char should be near-identical")
	}
}

func TestTextNearIdentical_MajorDiff(t *testing.T) {
	a := "User prefers Go with minimal dependencies"
	b := "System uses Python with maximum frameworks and heavy abstraction layers"
	if textNearIdentical(a, b) {
		t.Error("very different strings should not be near-identical")
	}
}

func TestTextNearIdentical_Whitespace(t *testing.T) {
	if !textNearIdentical("  hello world  ", "hello world") {
		t.Error("strings differing only by whitespace should be near-identical")
	}
}

func TestUpsertNode_SkipsNearIdentical(t *testing.T) {
	db := testDB(t)

	original := &MemNode{
		URI:           "mem://user/profile/coding-style",
		NodeType:      "leaf",
		Category:      "profile",
		L0Abstract:    "Prefers Go with minimal dependencies",
		L1Overview:    "Detailed coding style overview with important details about preferences.",
		L2Content:     "Full content here...",
		SourceSession: "sess-001",
	}
	if err := db.CreateNode(original); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	originalUpdatedAt := original.UpdatedAt

	// Upsert with near-identical content
	update := &MemNode{
		URI:           "mem://user/profile/coding-style",
		NodeType:      "leaf",
		Category:      "profile",
		L0Abstract:    "Prefers Go with minimal dependencies",
		L1Overview:    "Detailed coding style overview with important details about preferences.",
		L2Content:     "Full content here...",
		SourceSession: "sess-002",
	}
	if err := db.UpsertNode(update); err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}

	// Verify it was NOT updated
	node, err := db.GetNodeByURI("mem://user/profile/coding-style")
	if err != nil {
		t.Fatalf("GetNodeByURI: %v", err)
	}
	if node.SourceSession != "sess-001" {
		t.Error("near-identical upsert should have been skipped")
	}
	if node.UpdatedAt != originalUpdatedAt {
		t.Error("UpdatedAt should not have changed for skipped upsert")
	}
}

// TestUpsertNode_SystemOwnedBypassesNearIdenticalSkip (#78, H4 residue): the
// relational profile legitimately restates almost all of its prior content
// while adding one orthogonal observation — >95% bigram-similar, so the churn
// skip would silently discard the learning. System-owned URIs bypass the skip;
// their writers carry their own churn guards (see extractRelational).
func TestUpsertNode_SystemOwnedBypassesNearIdenticalSkip(t *testing.T) {
	db := testDB(t)

	base := "## 1. FEEDBACK CALIBRATION\n" +
		"User gives direct, specific feedback and expects the agent to act on it immediately.\n\n" +
		"## 2. WORKING DYNAMIC\n" +
		"Prefers high-level direction with autonomous execution; reviews results rather than steps.\n\n" +
		"## 3. CORRECTIONS RECEIVED\n" +
		"- Always use WAL mode for SQLite\n" +
		"- Prefer the standard library where possible\n"

	original := &MemNode{
		URI:           "mem://user/profile/communication",
		NodeType:      "leaf",
		Category:      "profile",
		L0Abstract:    "Relational profile: communication style, feedback patterns, working dynamic",
		L1Overview:    base,
		L2Content:     base,
		SourceSession: "sess-001",
	}
	if err := db.CreateNode(original); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	// Restated profile plus one new observation: identical L0 (it is a constant
	// on this node) and >95%-similar L1.
	restated := base + "- Fence identifiers in backticks\n"
	if !textNearIdentical(base, restated) {
		t.Fatal("test precondition: restated profile must be near-identical to the base")
	}
	update := &MemNode{
		URI:           "mem://user/profile/communication",
		NodeType:      "leaf",
		Category:      "profile",
		L0Abstract:    original.L0Abstract,
		L1Overview:    restated,
		L2Content:     restated,
		SourceSession: "sess-002",
	}
	if err := db.UpsertNode(update); err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}

	node, err := db.GetNodeByURI("mem://user/profile/communication")
	if err != nil {
		t.Fatalf("GetNodeByURI: %v", err)
	}
	if node.L1Overview != restated {
		t.Error("system-owned near-identical update must persist, not be skipped")
	}
	if node.SourceSession != "sess-002" {
		t.Errorf("source_session = %q, want sess-002", node.SourceSession)
	}
}

func TestUpsertNode_AllowsMeaningfulUpdate(t *testing.T) {
	db := testDB(t)

	original := &MemNode{
		URI:           "mem://user/profile/coding-style",
		NodeType:      "leaf",
		Category:      "profile",
		L0Abstract:    "Prefers Go with minimal dependencies",
		L1Overview:    "Detailed coding style overview with important details about preferences.",
		L2Content:     "Full content here...",
		SourceSession: "sess-001",
	}
	if err := db.CreateNode(original); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	// Upsert with meaningfully different content
	update := &MemNode{
		URI:           "mem://user/profile/coding-style",
		NodeType:      "leaf",
		Category:      "profile",
		L0Abstract:    "Prefers Rust with zero-cost abstractions and strong type safety",
		L1Overview:    strings.Repeat("Completely different content about Rust preferences. ", 5),
		L2Content:     "Totally new content...",
		SourceSession: "sess-002",
	}
	if err := db.UpsertNode(update); err != nil {
		t.Fatalf("UpsertNode: %v", err)
	}

	// Verify it WAS updated
	node, err := db.GetNodeByURI("mem://user/profile/coding-style")
	if err != nil {
		t.Fatalf("GetNodeByURI: %v", err)
	}
	if node.SourceSession != "sess-002" {
		t.Error("meaningful update should have been applied")
	}
}
