package engine

import (
	"testing"
	"time"

	"github.com/lazypower/continuity/internal/store"
)

func TestObservationGraceDurationEnv(t *testing.T) {
	tests := []struct {
		name     string
		env      string
		wantDays int
		wantOK   bool
		setUnset bool
	}{
		{name: "default when unset", setUnset: true, wantDays: 14, wantOK: true},
		{name: "explicit day count", env: "3", wantDays: 3, wantOK: true},
		{name: "off disables", env: "off", wantOK: false},
		{name: "zero disables", env: "0", wantOK: false},
		{name: "false disables", env: "false", wantOK: false},
		{name: "garbage falls back to default", env: "banana", wantDays: 14, wantOK: true},
		{name: "negative falls back to default", env: "-5", wantDays: 14, wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setUnset {
				t.Setenv(retentionEnvVar, "")
			} else {
				t.Setenv(retentionEnvVar, tc.env)
			}
			got, ok := observationGraceDuration()
			if ok != tc.wantOK {
				t.Fatalf("enabled = %t, want %t", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if days := int(got.Hours() / 24); days != tc.wantDays {
				t.Errorf("grace = %dd, want %dd", days, tc.wantDays)
			}
		})
	}
}

// TestRetentionDisabledLeavesRowsAlone pins the operator escape hatch: with
// retention off, a row that would otherwise be spent must survive.
func TestRetentionDisabledLeavesRowsAlone(t *testing.T) {
	t.Setenv(retentionEnvVar, "off")

	db := newRetentionTestDB(t)
	seedSpentObservation(t, db, "old-session")

	n, err := PruneObservations(db)
	if err != nil {
		t.Fatalf("PruneObservations: %v", err)
	}
	if n != 0 {
		t.Errorf("pruned %d rows with retention off, want 0", n)
	}
	count, err := db.GetSessionObservationCount("old-session")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("observation count = %d, want 1 (retention was off)", count)
	}

	if got, err := CountSpentObservations(db); err != nil || got != 0 {
		t.Errorf("CountSpentObservations = %d, %v; want 0, nil when disabled", got, err)
	}
}

// TestStartDecayTimerRunsObservationRetention is the wiring test. The store
// package proves the predicate; this proves the sweep is actually connected to
// the timer, which is the part a refactor could silently drop.
func TestStartDecayTimerRunsObservationRetention(t *testing.T) {
	t.Setenv(retentionEnvVar, "14")

	db := newRetentionTestDB(t)
	seedSpentObservation(t, db, "old-session")

	e := &Engine{DB: db, stopCh: make(chan struct{})}
	e.StartDecayTimer()
	defer e.Stop()

	count, err := db.GetSessionObservationCount("old-session")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("observation count = %d after StartDecayTimer, want 0 — "+
			"observation retention is not wired into the decay pass", count)
	}
}

func newRetentionTestDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.OpenMemory()
	if err != nil {
		t.Fatalf("OpenMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// seedSpentObservation creates a completed session well outside the grace
// window with one observation of the same vintage.
func seedSpentObservation(t *testing.T, db *store.DB, sessionID string) {
	t.Helper()
	old := time.Now().Add(-90 * 24 * time.Hour).UnixMilli()
	if _, err := db.Exec(`
		INSERT INTO sessions (session_id, project, started_at, ended_at, status)
		VALUES (?, 'proj', ?, ?, 'completed')`, sessionID, old, old); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO observations (session_id, tool_name, tool_input, tool_response, created_at)
		VALUES (?, 'Bash', '{}', 'out', ?)`, sessionID, old); err != nil {
		t.Fatalf("seed observation: %v", err)
	}
}
