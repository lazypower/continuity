package store

import (
	"testing"
)

// TestInsertEvent_TableExists is the canary for migration v13: a fresh DB
// (OpenMemory replays all migrations) must accept a journal write. If this
// fails with "no such table", the migration never reached fresh DBs.
func TestInsertEvent_TableExists(t *testing.T) {
	db := testDB(t)

	err := db.InsertEvent(MemEvent{
		NodeURI:   "mem://user/feedback/example",
		Event:     "shown",
		Surface:   "tray",
		SessionID: "sess-1",
	})
	if err != nil {
		t.Fatalf("InsertEvent: %v", err)
	}

	n, err := db.CountEvents("shown")
	if err != nil {
		t.Fatalf("CountEvents: %v", err)
	}
	if n != 1 {
		t.Errorf("shown count = %d, want 1", n)
	}
}

// TestInsertEvent_VocabularyEnforced pins the CHECK constraint: the event
// vocabulary is ADR-governed; a typo'd writer must fail loudly, not corrupt
// the research data.
func TestInsertEvent_VocabularyEnforced(t *testing.T) {
	db := testDB(t)

	err := db.InsertEvent(MemEvent{NodeURI: "mem://x", Event: "viewed"})
	if err == nil {
		t.Fatal("expected CHECK violation for unknown event name 'viewed', got nil")
	}
}

// TestEventsByURI_NewestFirst verifies retrieval ordering and field fidelity.
func TestEventsByURI_NewestFirst(t *testing.T) {
	db := testDB(t)

	uri := "mem://agent/cases/scar"
	for i, e := range []MemEvent{
		{NodeURI: uri, Event: "shown", Surface: "search", SessionID: "s1", CreatedAt: 100},
		{NodeURI: uri, Event: "deepened", SessionID: "s1", CreatedAt: 200},
	} {
		if err := db.InsertEvent(e); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	events, err := db.EventsByURI(uri)
	if err != nil {
		t.Fatalf("EventsByURI: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].Event != "deepened" || events[1].Event != "shown" {
		t.Errorf("wrong order: got [%s, %s], want [deepened, shown]", events[0].Event, events[1].Event)
	}
	if events[1].Surface != "search" || events[1].SessionID != "s1" {
		t.Errorf("field fidelity: %+v", events[1])
	}
}

// TestContractRelevanceRestore pins migration v14 semantics at the store
// level: decay must never touch contract categories again, so their
// relevance stays wherever use puts it — and the migration restored eroded
// rows to 1.0 on upgrade. Here we verify the exemption boundary directly:
// DecayAllNodes touches episodic categories and leaves contract untouched.
func TestContractRelevanceRestore_DecayExemption(t *testing.T) {
	db := testDB(t)

	seed := []struct {
		uri, category string
	}{
		{"mem://user/preferences/devbox", "preferences"},
		{"mem://user/feedback/terse", "feedback"},
		{"mem://user/profile/style", "profile"},
		{"mem://agent/patterns/episodic", "patterns"},
	}
	for _, s := range seed {
		if err := db.CreateNode(&MemNode{URI: s.uri, NodeType: "leaf", Category: s.category}); err != nil {
			t.Fatalf("seed %s: %v", s.uri, err)
		}
		// Backdate last_access a year so decay would bite anything non-exempt.
		yearAgo := int64(1)
		if _, err := db.Exec(`UPDATE mem_nodes SET last_access = ?, created_at = ? WHERE uri = ?`, yearAgo, yearAgo, s.uri); err != nil {
			t.Fatalf("backdate %s: %v", s.uri, err)
		}
	}

	if _, err := db.DecayAllNodes(); err != nil {
		t.Fatalf("DecayAllNodes: %v", err)
	}

	for _, s := range seed[:3] {
		n, _ := db.GetNodeByURI(s.uri)
		if n.Relevance != 1.0 {
			t.Errorf("contract node %s decayed to %f — contract categories must be decay-exempt (ADR-001 §1)", s.uri, n.Relevance)
		}
	}
	episodic, _ := db.GetNodeByURI("mem://agent/patterns/episodic")
	if episodic.Relevance >= 1.0 {
		t.Errorf("episodic node did not decay (relevance=%f) — exemption is too broad", episodic.Relevance)
	}
}
