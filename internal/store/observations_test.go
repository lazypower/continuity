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

	bigInput := strings.Repeat("i", 20*1024)    // 20KB
	bigResponse := strings.Repeat("r", 20*1024) // 20KB
	err = db.AddObservation("sess-001", "Bash", bigInput, bigResponse)
	if err != nil {
		t.Fatalf("AddObservation: %v", err)
	}

	obs, _ := db.GetObservations("sess-001")
	if len(obs[0].ToolInput) != maxToolFieldSize {
		t.Errorf("ToolInput length = %d, want %d", len(obs[0].ToolInput), maxToolFieldSize)
	}
	if len(obs[0].ToolResponse) != maxToolFieldSize {
		t.Errorf("ToolResponse length = %d, want %d", len(obs[0].ToolResponse), maxToolFieldSize)
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

// --- retention -------------------------------------------------------------

// seedObservation inserts an observation at an explicit created_at so retention
// tests can place rows on either side of the grace horizon deterministically.
func seedObservation(t *testing.T, db *DB, sessionID string, createdAt int64) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO observations (session_id, tool_name, tool_input, tool_response, created_at)
		VALUES (?, 'Bash', '{}', 'out', ?)`, sessionID, createdAt)
	if err != nil {
		t.Fatalf("seed observation for %s: %v", sessionID, err)
	}
}

// seedSession inserts a session row with explicit status/started_at.
func seedSession(t *testing.T, db *DB, sessionID, status string, startedAt int64, extracted bool) {
	t.Helper()
	var extractedAt any
	if extracted {
		extractedAt = startedAt + 1000
	}
	_, err := db.Exec(`
		INSERT INTO sessions (session_id, project, started_at, status, extracted_at)
		VALUES (?, 'proj', ?, ?, ?)`, sessionID, startedAt, status, extractedAt)
	if err != nil {
		t.Fatalf("seed session %s: %v", sessionID, err)
	}
}

// TestPruneSpentObservationsBuckets is the regression test for issue #72. The
// original sketch keyed retention to extracted_at, which stranded roughly half
// of a real database: extraction's content gate skips thin sessions without
// ever marking them. Every bucket below must be reclaimed except the genuinely
// in-flight one.
func TestPruneSpentObservationsBuckets(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	const day = int64(24 * 60 * 60 * 1000)
	now := int64(1_700_000_000_000)
	graceCutoff := now - 14*day  // older than this is eligible
	zombieCutoff := now - 30*day // active but started before this is a zombie
	old := now - 20*day          // past grace
	recent := now - 1*day        // inside grace

	// Reclaimable: session finished and extraction ran — the classic spent case.
	seedSession(t, db, "completed-extracted", "completed", now-40*day, true)
	seedObservation(t, db, "completed-extracted", old)

	// Reclaimable: session finished but extraction never ran (content gate
	// skipped it). This is the 47% the extracted_at predicate would strand.
	seedSession(t, db, "completed-unextracted", "completed", now-40*day, false)
	seedObservation(t, db, "completed-unextracted", old)

	// Reclaimable: no session row at all.
	seedObservation(t, db, "orphaned", old)

	// Reclaimable: left 'active' by a crashed client and silent ever since.
	// Liveness is judged by last activity, so this needs an observation older
	// than the zombie horizon — an active session that recorded something 20
	// days ago is still live under a 30-day horizon, by design.
	seedSession(t, db, "zombie-active", "active", now-90*day, false)
	seedObservation(t, db, "zombie-active", now-60*day)

	// RETAINED: genuinely in flight — active and started inside the zombie
	// horizon. Its context header can still be read.
	seedSession(t, db, "live", "active", now-20*day, false)
	seedObservation(t, db, "live", old)

	// RETAINED: inside the grace window regardless of session state.
	seedSession(t, db, "recent-completed", "completed", now-2*day, true)
	seedObservation(t, db, "recent-completed", recent)

	wantCount := int64(4)
	got, err := db.CountSpentObservations(graceCutoff, zombieCutoff)
	if err != nil {
		t.Fatalf("CountSpentObservations: %v", err)
	}
	if got != wantCount {
		t.Errorf("CountSpentObservations = %d, want %d", got, wantCount)
	}

	deleted, err := db.PruneSpentObservations(graceCutoff, zombieCutoff)
	if err != nil {
		t.Fatalf("PruneSpentObservations: %v", err)
	}
	if deleted != wantCount {
		t.Errorf("PruneSpentObservations deleted %d, want %d", deleted, wantCount)
	}

	// The count and the delete must never diverge — they share one predicate.
	after, err := db.CountSpentObservations(graceCutoff, zombieCutoff)
	if err != nil {
		t.Fatalf("CountSpentObservations after prune: %v", err)
	}
	if after != 0 {
		t.Errorf("after prune, %d rows still spent — count and delete diverged", after)
	}

	for _, sess := range []string{"live", "recent-completed"} {
		n, err := db.GetSessionObservationCount(sess)
		if err != nil {
			t.Fatalf("GetSessionObservationCount(%s): %v", sess, err)
		}
		if n != 1 {
			t.Errorf("session %s retained %d observations, want 1", sess, n)
		}
	}
	for _, sess := range []string{"completed-extracted", "completed-unextracted", "orphaned", "zombie-active"} {
		n, err := db.GetSessionObservationCount(sess)
		if err != nil {
			t.Fatalf("GetSessionObservationCount(%s): %v", sess, err)
		}
		if n != 0 {
			t.Errorf("session %s retained %d observations, want 0", sess, n)
		}
	}
}

// TestPruneSpentObservationsIsIdempotent guards the sweep running on a timer.
func TestPruneSpentObservationsIsIdempotent(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	const day = int64(24 * 60 * 60 * 1000)
	now := int64(1_700_000_000_000)
	seedSession(t, db, "done", "completed", now-40*day, true)
	seedObservation(t, db, "done", now-20*day)

	first, err := db.PruneSpentObservations(now-14*day, now-30*day)
	if err != nil {
		t.Fatalf("first prune: %v", err)
	}
	second, err := db.PruneSpentObservations(now-14*day, now-30*day)
	if err != nil {
		t.Fatalf("second prune: %v", err)
	}
	if first != 1 || second != 0 {
		t.Errorf("prune not idempotent: first=%d second=%d, want 1 and 0", first, second)
	}
}

func TestVacuum(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	db.AddObservation("sess-001", "Bash", "{}", "out")
	if err := db.Vacuum(); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	n, err := db.GetSessionObservationCount("sess-001")
	if err != nil {
		t.Fatalf("GetSessionObservationCount: %v", err)
	}
	if n != 1 {
		t.Errorf("vacuum lost data: count = %d, want 1", n)
	}
}

// TestLongRunningSessionKeepsItsObservations is the regression test for the
// zombie-horizon bug. Liveness must be measured from the session's most recent
// activity, not its started_at: InitSession reactivates a resumed session
// without refreshing started_at, and a genuinely long-running session would
// otherwise cross the horizon and start shedding its own live history.
func TestLongRunningSessionKeepsItsObservations(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	const day = int64(24 * 60 * 60 * 1000)
	now := int64(1_700_000_000_000)
	graceCutoff := now - 14*day
	zombieCutoff := now - 30*day

	// Started 90 days ago — well past the zombie horizon — but still active and
	// still recording tool use as of an hour ago. This is a resumed or
	// long-lived session, not a crashed one.
	seedSession(t, db, "long-running", "active", now-90*day, false)
	seedObservation(t, db, "long-running", now-60*day) // old, but session is live
	seedObservation(t, db, "long-running", now-1*day)  // recent activity
	seedObservation(t, db, "long-running", now-(day/24))

	// A genuinely abandoned session: active, but nothing recorded in 60 days.
	seedSession(t, db, "crashed", "active", now-90*day, false)
	seedObservation(t, db, "crashed", now-60*day)

	deleted, err := db.PruneSpentObservations(graceCutoff, zombieCutoff)
	if err != nil {
		t.Fatalf("PruneSpentObservations: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted %d rows, want 1 (only the crashed session's)", deleted)
	}

	live, err := db.GetSessionObservationCount("long-running")
	if err != nil {
		t.Fatalf("count long-running: %v", err)
	}
	if live != 3 {
		t.Errorf("long-running session kept %d observations, want 3 — a live "+
			"session lost history to the zombie horizon", live)
	}

	dead, err := db.GetSessionObservationCount("crashed")
	if err != nil {
		t.Fatalf("count crashed: %v", err)
	}
	if dead != 0 {
		t.Errorf("crashed session kept %d observations, want 0", dead)
	}
}

// TestSessionWithNoObservationsFallsBackToStartedAt covers the COALESCE arm:
// an active session that has recorded nothing yet is judged by its start time.
func TestSessionWithNoObservationsFallsBackToStartedAt(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	const day = int64(24 * 60 * 60 * 1000)
	now := int64(1_700_000_000_000)

	// Freshly started, nothing recorded yet, but an old orphan row exists.
	seedSession(t, db, "fresh", "active", now-1*day, false)
	seedObservation(t, db, "orphan", now-20*day)

	deleted, err := db.PruneSpentObservations(now-14*day, now-30*day)
	if err != nil {
		t.Fatalf("PruneSpentObservations: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted %d, want 1 (the orphan only)", deleted)
	}
}
