package store

import (
	"testing"
)

func TestAddObservation(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	err = db.AddObservation("sess-001", "Bash")
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
}

// TestAddObservationNeverPersistsToolContent is the privacy guard.
//
// These rows used to carry the full tool call and its result — file contents,
// command output, and any credential that passed through a tool — written on
// every PostToolUse hook and read by nothing. The store must have no way to
// persist that material, so the API takes no argument that could carry it.
//
// If someone restores those parameters, this test fails. It exists so the
// justification has to be re-argued rather than reintroduced by habit.
func TestAddObservationNeverPersistsToolContent(t *testing.T) {
	db, err := OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	defer db.Close()

	if err := db.AddObservation("sess-001", "Bash"); err != nil {
		t.Fatalf("AddObservation: %v", err)
	}

	// Read the columns directly rather than through the struct: the schema
	// keeps them for compatibility with rows written by older builds, so the
	// contract is that NEW rows are empty, not that the columns are gone.
	var input, response string
	err = db.QueryRow(
		`SELECT tool_input, tool_response FROM observations WHERE session_id = ?`,
		"sess-001",
	).Scan(&input, &response)
	if err != nil {
		t.Fatalf("read columns: %v", err)
	}
	if input != "" {
		t.Errorf("tool_input = %q, want empty — tool arguments must never reach disk", input)
	}
	if response != "" {
		t.Errorf("tool_response = %q, want empty — tool output must never reach disk", response)
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

	db.AddObservation("sess-001", "Bash")
	db.AddObservation("sess-001", "Read")
	db.AddObservation("sess-002", "Edit")

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

	db.AddObservation("sess-001", "Bash")
	db.AddObservation("sess-001", "Read")
	db.AddObservation("sess-002", "Edit")

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

// seedSession inserts a session row whose last_active_at equals started_at —
// i.e. a session that has shown no sign of life since it began.
func seedSession(t *testing.T, db *DB, sessionID, status string, startedAt int64, extracted bool) {
	t.Helper()
	seedSessionActive(t, db, sessionID, status, startedAt, startedAt, extracted)
}

// seedSessionActive inserts a session with an explicit last_active_at, so tests
// can distinguish a long-running session from an abandoned one that merely
// started at the same time.
func seedSessionActive(t *testing.T, db *DB, sessionID, status string, startedAt, lastActiveAt int64, extracted bool) {
	t.Helper()
	var extractedAt any
	if extracted {
		extractedAt = startedAt + 1000
	}
	_, err := db.Exec(`
		INSERT INTO sessions (session_id, project, started_at, status, extracted_at, last_active_at)
		VALUES (?, 'proj', ?, ?, ?, ?)`, sessionID, startedAt, status, extractedAt, lastActiveAt)
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

	db.AddObservation("sess-001", "Bash")
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
	seedSessionActive(t, db, "long-running", "active", now-90*day, now-(day/24), false)
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
