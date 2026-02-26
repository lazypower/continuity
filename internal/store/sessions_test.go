package store

import (
	"testing"
)

func TestInitSession(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	s, err := db.InitSession("sess-001", "myproject")
	if err != nil {
		t.Fatalf("InitSession: %v", err)
	}
	if s.SessionID != "sess-001" {
		t.Errorf("SessionID = %q, want sess-001", s.SessionID)
	}
	if s.Project != "myproject" {
		t.Errorf("Project = %q, want myproject", s.Project)
	}
	if s.Status != "active" {
		t.Errorf("Status = %q, want active", s.Status)
	}
	if s.ToolCount != 0 {
		t.Errorf("ToolCount = %d, want 0", s.ToolCount)
	}
}

func TestInitSessionResume(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	s1, err := db.InitSession("sess-001", "myproject")
	if err != nil {
		t.Fatalf("InitSession: %v", err)
	}

	s2, err := db.InitSession("sess-001", "myproject")
	if err != nil {
		t.Fatalf("InitSession resume: %v", err)
	}

	if s1.ID != s2.ID {
		t.Errorf("resumed session ID = %d, want %d", s2.ID, s1.ID)
	}
}

func TestInitSessionReactivatesCompleted(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	// Create and complete a session
	s1, err := db.InitSession("sess-001", "myproject")
	if err != nil {
		t.Fatalf("InitSession: %v", err)
	}
	if err := db.CompleteSession("sess-001"); err != nil {
		t.Fatalf("CompleteSession: %v", err)
	}

	// Re-init should reactivate, not error
	s2, err := db.InitSession("sess-001", "myproject")
	if err != nil {
		t.Fatalf("InitSession after complete: %v", err)
	}
	if s1.ID != s2.ID {
		t.Errorf("reactivated session ID = %d, want %d", s2.ID, s1.ID)
	}
	if s2.Status != "active" {
		t.Errorf("Status = %q, want active", s2.Status)
	}
}

func TestGetSession(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	// Not found returns nil
	s, err := db.GetSession("nonexistent")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if s != nil {
		t.Errorf("expected nil for nonexistent session, got %+v", s)
	}

	// Found
	db.InitSession("sess-001", "proj")
	s, err = db.GetSession("sess-001")
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if s == nil {
		t.Fatal("expected session, got nil")
	}
	if s.SessionID != "sess-001" {
		t.Errorf("SessionID = %q, want sess-001", s.SessionID)
	}
}

func TestCompleteSession(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	db.InitSession("sess-001", "proj")

	if err := db.CompleteSession("sess-001"); err != nil {
		t.Fatalf("CompleteSession: %v", err)
	}

	s, _ := db.GetSession("sess-001")
	if s.Status != "completed" {
		t.Errorf("Status = %q, want completed", s.Status)
	}
	if s.EndedAt == nil {
		t.Error("EndedAt should be set")
	}

	// Completing again should error (no active session)
	if err := db.CompleteSession("sess-001"); err == nil {
		t.Error("expected error completing already-completed session")
	}
}

func TestEndSession(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	db.InitSession("sess-001", "proj")

	// EndSession on active session should complete it
	if err := db.EndSession("sess-001"); err != nil {
		t.Fatalf("EndSession: %v", err)
	}

	s, _ := db.GetSession("sess-001")
	if s.Status != "completed" {
		t.Errorf("Status = %q, want completed", s.Status)
	}

	// EndSession on completed session is a no-op (not an error)
	if err := db.EndSession("sess-001"); err != nil {
		t.Fatalf("EndSession on completed: %v", err)
	}
}

func TestGetRecentSessions(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	db.InitSession("sess-001", "proj1")
	db.InitSession("sess-002", "proj2")
	db.InitSession("sess-003", "proj3")

	sessions, err := db.GetRecentSessions(2)
	if err != nil {
		t.Fatalf("GetRecentSessions: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("got %d sessions, want 2", len(sessions))
	}
	// Limit works — 3 inserted, 2 returned
}

func TestIncrementToolCount(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	db.InitSession("sess-001", "proj")

	for i := 0; i < 3; i++ {
		if err := db.IncrementToolCount("sess-001"); err != nil {
			t.Fatalf("IncrementToolCount: %v", err)
		}
	}

	s, _ := db.GetSession("sess-001")
	if s.ToolCount != 3 {
		t.Errorf("ToolCount = %d, want 3", s.ToolCount)
	}
}
