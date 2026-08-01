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
		// Unparseable input must fail CLOSED. An operator typing "of" while
		// reaching for "off" is trying to STOP deletion; answering with the
		// destructive default is the worst available reading, and the boot
		// sweep runs immediately so a log line arrives too late to intervene.
		{name: "garbage disables", env: "banana", wantOK: false},
		{name: "off typo disables", env: "of", wantOK: false},
		{name: "negative disables", env: "-5", wantOK: false},
		// A value meant to retain MORE must never delete more: an unclamped
		// day count overflows time.Duration into a negative grace, putting the
		// cutoff in the future and making everything immediately reclaimable.
		{name: "absurd day count clamps", env: "106752", wantDays: maxRetentionDays, wantOK: true},
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

// TestStartRetentionTimerSweepsAtBoot is the wiring test. The store package
// proves the predicate; this proves the sweep is actually connected to a timer,
// which is the part a refactor could silently drop.
//
// Critically, StartRetentionTimer takes a *store.DB and no Engine. serve starts
// the decay timer only when an LLM is configured; retention must not inherit
// that condition, because an install with no LLM records observations at
// exactly the same rate.
func TestStartRetentionTimerSweepsAtBoot(t *testing.T) {
	t.Setenv(retentionEnvVar, "14")

	db := newRetentionTestDB(t)
	seedSpentObservation(t, db, "old-session")

	stop := make(chan struct{})
	StartRetentionTimer(db, stop)
	defer close(stop)

	count, err := db.GetSessionObservationCount("old-session")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("observation count = %d after StartRetentionTimer, want 0 — "+
			"the boot sweep did not run", count)
	}
}

// TestRetentionGaugeIsRefreshedBySweep pins the /api/health contract: health
// reads a cached gauge rather than measuring, so the sweep must be what keeps
// it current. A health check whose cost scaled with table size would recreate
// the original failure and make `continuity prune` unreachable.
func TestRetentionGaugeIsRefreshedBySweep(t *testing.T) {
	t.Setenv(retentionEnvVar, "14")

	db := newRetentionTestDB(t)
	// Two spent rows on a live session's sibling, so something remains
	// reclaimable after we deliberately measure a non-zero state.
	seedSpentObservation(t, db, "old-a")

	spentObservationsGauge.Store(-1) // poison, so a stale read is detectable
	if _, err := PruneObservations(db); err != nil {
		t.Fatalf("PruneObservations: %v", err)
	}
	if got := SpentObservationsGauge(); got != 0 {
		t.Errorf("gauge = %d after a sweep that reclaimed everything, want 0", got)
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
