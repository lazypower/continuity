package store

import (
	"strings"
	"testing"
)

func TestAddObservation(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	err = db.AddObservation("sess-001", "Bash", `{"command":"ls"}`, "file1 file2")
	if err != nil {
		t.Fatalf("AddObservation: %v", err)
	}

	obs, err := db.GetObservations("sess-001")
	if err != nil {
		t.Fatalf("GetObservations: %v", err)
	}
	if len(obs) != 1 {
		t.Fatalf("got %d observations, want 1", len(obs))
	}
	if obs[0].ToolName != "Bash" {
		t.Errorf("ToolName = %q, want Bash", obs[0].ToolName)
	}
	if obs[0].ToolInput != `{"command":"ls"}` {
		t.Errorf("ToolInput = %q", obs[0].ToolInput)
	}
	if obs[0].ToolResponse != "file1 file2" {
		t.Errorf("ToolResponse = %q", obs[0].ToolResponse)
	}
}

func TestAddObservationTruncation(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	bigResponse := strings.Repeat("x", 20*1024) // 20KB
	err = db.AddObservation("sess-001", "Bash", "{}", bigResponse)
	if err != nil {
		t.Fatalf("AddObservation: %v", err)
	}

	obs, _ := db.GetObservations("sess-001")
	if len(obs[0].ToolResponse) != maxToolResponseSize {
		t.Errorf("ToolResponse length = %d, want %d", len(obs[0].ToolResponse), maxToolResponseSize)
	}
}

func TestGetObservationsEmpty(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	obs, err := db.GetObservations("nonexistent")
	if err != nil {
		t.Fatalf("GetObservations: %v", err)
	}
	if len(obs) != 0 {
		t.Errorf("got %d observations for nonexistent session, want 0", len(obs))
	}
}

func TestGetRecentObservations(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	db.AddObservation("sess-001", "Bash", "{}", "out1")
	db.AddObservation("sess-001", "Read", "{}", "out2")
	db.AddObservation("sess-002", "Edit", "{}", "out3")

	obs, err := db.GetRecentObservations(2)
	if err != nil {
		t.Fatalf("GetRecentObservations: %v", err)
	}
	if len(obs) != 2 {
		t.Fatalf("got %d observations, want 2", len(obs))
	}
	// Limit works — 3 inserted, 2 returned
	// (order is DESC by created_at, but within same millisecond it's by rowid DESC)
}

func TestGetSessionObservationCount(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	db.AddObservation("sess-001", "Bash", "{}", "out1")
	db.AddObservation("sess-001", "Read", "{}", "out2")
	db.AddObservation("sess-002", "Edit", "{}", "out3")

	count, err := db.GetSessionObservationCount("sess-001")
	if err != nil {
		t.Fatalf("GetSessionObservationCount: %v", err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}
