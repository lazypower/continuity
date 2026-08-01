package engine

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lazypower/continuity/internal/store"
)

// Observation retention is deliberately NOT modelled on the mem_nodes GC sweep.
// That sweep destroys memories, so it ships off and earns its way up through
// shadow mode behind a snapshot. Observations are the opposite kind of data:
// raw tool-use scaffolding whose only reader is the live session's own context
// header. Once a session is no longer in flight nothing can ask for them again,
// so retention ships ENABLED. Shipping it off by default would preserve exactly
// the unbounded growth this exists to fix (see issue #72).
const (
	// observationGrace is how long observations are kept regardless of session
	// state. Generous on purpose: it costs disk, and the alternative is racing
	// a session that is still being written to.
	observationGrace = 14 * 24 * time.Hour

	// sessionZombieAge is how long an 'active' session may go without recording
	// anything before we stop believing it is live. Clients that crash or get
	// killed never fire Stop/SessionEnd, so their sessions stay 'active' forever
	// and would otherwise pin their observations forever with them.
	sessionZombieAge = 30 * 24 * time.Hour

	// maxRetentionDays bounds the configured grace so a large value cannot
	// overflow time.Duration (~290 years) into a NEGATIVE grace — which would
	// put the cutoff in the future and make every completed session, including
	// one that just ended, immediately reclaimable. A config meant to retain
	// MORE must never delete more.
	maxRetentionDays = 3650

	// largeReclaimThreshold is where a sweep stops looking like routine daily
	// hygiene and starts looking like a backlog being cleared for the first
	// time — the upgrade case. Issue #72 measured ~2,400 observations/day on a
	// normal single-user install, so a daily sweep reclaims low thousands; an
	// order of magnitude past that is a catch-up, not a steady state.
	//
	// Keyed off the size of the sweep itself, deliberately. The alternative —
	// a persisted "have we explained this yet?" marker — is a schema change
	// that can be lost on a restored database and would explain the upgrade to
	// someone who never upgraded.
	largeReclaimThreshold = 50_000
)

// retentionEnvVar lets an operator widen, tighten, or disable retention without
// a rebuild. "0"/"off"/"false" disables pruning; a positive integer is a day
// count replacing observationGrace.
const retentionEnvVar = "CONTINUITY_OBSERVATION_RETENTION_DAYS"

// spentObservationsGauge caches the reclaimable count measured by the last
// sweep. /api/health reads this instead of running the query, because a health
// check whose cost scales with table size would recreate the exact failure this
// change exists to fix: the original bug was invisible precisely because health
// stayed fast while everything else timed out. A cached number that is a few
// hours stale is fine; a health endpoint that degrades with the table is not.
var spentObservationsGauge atomic.Int64

// SpentObservationsGauge returns the reclaimable count as of the last sweep.
func SpentObservationsGauge() int64 { return spentObservationsGauge.Load() }

// observationGraceDuration resolves the effective grace window. Returns ok=false
// when retention is disabled — either explicitly, or because the value could not
// be understood.
//
// Unparseable input fails CLOSED (disabled), not open. An operator typing "of"
// while reaching for "off" is trying to STOP deletion; answering that with the
// destructive default is the worst available reading, and the boot sweep runs
// immediately, so a log line arrives too late to intervene.
func observationGraceDuration() (d time.Duration, ok bool) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(retentionEnvVar)))
	if raw == "" {
		return observationGrace, true
	}
	if raw == "off" || raw == "false" {
		return 0, false
	}
	days, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("retention: cannot parse %s=%q — DISABLING observation retention. "+
			"Set a positive number of days, or 'off' to disable deliberately.", retentionEnvVar, raw)
		return 0, false
	}
	if days <= 0 {
		return 0, false
	}
	if days > maxRetentionDays {
		log.Printf("retention: %s=%d exceeds the %d-day maximum — clamping.", retentionEnvVar, days, maxRetentionDays)
		days = maxRetentionDays
	}
	return time.Duration(days) * 24 * time.Hour, true
}

// RetentionCutoffs returns (graceCutoff, zombieCutoff) in epoch millis, plus
// whether retention is enabled at all.
//
// Package-level on purpose. Retention is pure storage hygiene — it needs no LLM,
// no embedder, and no Engine. The Engine is legitimately nil when no LLM is
// configured, and those installs still accumulate observations, so binding this
// to a receiver would deny retention to exactly the users most likely to need it.
func RetentionCutoffs() (graceCutoff, zombieCutoff int64, enabled bool) {
	grace, ok := observationGraceDuration()
	if !ok {
		return 0, 0, false
	}
	now := time.Now().UnixMilli()
	return now - grace.Milliseconds(), now - sessionZombieAge.Milliseconds(), true
}

// CountSpentObservations measures how many observation rows are reclaimable
// right now. This runs the query — callers on a request path that must stay
// fast should read SpentObservationsGauge instead.
func CountSpentObservations(db *store.DB) (int64, error) {
	graceCutoff, zombieCutoff, enabled := RetentionCutoffs()
	if !enabled {
		return 0, nil
	}
	return db.CountSpentObservations(graceCutoff, zombieCutoff)
}

// PruneObservations reclaims spent observations and returns the row count. This
// is the shared path for the background sweep and `continuity prune`, so the two
// cannot drift onto different rules.
func PruneObservations(db *store.DB) (int64, error) {
	graceCutoff, zombieCutoff, enabled := RetentionCutoffs()
	if !enabled {
		return 0, nil
	}
	pruned, err := db.PruneSpentObservations(graceCutoff, zombieCutoff)
	if err != nil {
		return 0, err
	}
	refreshSpentGauge(db)
	return pruned, nil
}

// refreshSpentGauge re-measures the reclaimable count for /api/health. Failure
// is non-fatal — a stale gauge is a reporting inaccuracy, not a broken sweep.
func refreshSpentGauge(db *store.DB) {
	graceCutoff, zombieCutoff, enabled := RetentionCutoffs()
	if !enabled {
		graceCutoff, zombieCutoff = defaultCutoffs()
	}
	refreshSpentGaugeWith(db, graceCutoff, zombieCutoff)
}

func refreshSpentGaugeWith(db *store.DB, graceCutoff, zombieCutoff int64) {
	n, err := db.CountSpentObservations(graceCutoff, zombieCutoff)
	if err != nil {
		log.Printf("retention: gauge refresh failed: %v", err)
		return
	}
	spentObservationsGauge.Store(n)
}

// StartRetentionTimer runs the observation sweep at startup and then daily.
//
// Independent of the Engine and of StartDecayTimer: serve starts the decay timer
// only when an LLM is configured, and retention must not inherit that condition.
// An install with no LLM records observations at exactly the same rate.
func StartRetentionTimer(db *store.DB, stop <-chan struct{}) {
	runObservationRetention(db)

	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				runObservationRetention(db)
			case <-stop:
				return
			}
		}
	}()
}

// runObservationRetention performs one sweep. Unlike the GC sweep this is
// uncapped: observations accumulate at tool-call rate, and a cap low enough to
// be a safe blast radius for memories would never keep up.
func runObservationRetention(db *store.DB) {
	grace, ok := observationGraceDuration()
	if !ok {
		// Retention is off — deliberately, or because the config was
		// unparseable and we failed closed. This is exactly when the gauge
		// matters most: nothing is reclaiming, so the pile only grows, and
		// /api/health is the one place an operator will see it. Measure under
		// the DEFAULT policy so the number answers "what would retention
		// reclaim if it were on?".
		graceCutoff, zombieCutoff := defaultCutoffs()
		refreshSpentGaugeWith(db, graceCutoff, zombieCutoff)
		return
	}
	pruned, err := PruneObservations(db)
	if err != nil {
		log.Printf("retention: observation prune failed: %v", err)
		return
	}
	for _, line := range retentionSweepLog(pruned, int(grace.Hours()/24)) {
		log.Print(line)
	}
}

// retentionSweepLog builds the log lines for one sweep. Pure and table-tested:
// the interesting behaviour is a threshold, and proving it through the database
// would mean seeding largeReclaimThreshold rows to observe a string.
//
// Routine sweeps stay a single terse line — this runs daily, forever, and an
// explanation repeated every day is noise that trains people to skip the line.
// The backlog case is the one where a user is watching bulk deletion they did
// not ask for, so that is where the explanation, the issue, and the opt-out go.
func retentionSweepLog(pruned int64, graceDays int) []string {
	if pruned <= 0 {
		return nil
	}
	lines := []string{
		fmt.Sprintf("retention: pruned %d observation(s) from sessions no longer in flight (grace: %dd)", pruned, graceDays),
	}
	if pruned >= largeReclaimThreshold {
		lines = append(lines,
			fmt.Sprintf("retention: that was a one-time catch-up — these observations predate this build's "+
				"retention path (background: https://github.com/lazypower/continuity/issues/72). Routine sweeps "+
				"reclaim far less. Memories, vectors and the relational profile are untouched. Disk is not "+
				"returned to the filesystem until you run 'continuity prune'; set %s=off to disable retention, "+
				"or to a day count to widen the window.", retentionEnvVar))
	}
	return lines
}

// defaultCutoffs returns the cutoffs implied by the built-in policy, ignoring
// any operator override. Used to measure the gauge when retention is disabled.
func defaultCutoffs() (graceCutoff, zombieCutoff int64) {
	now := time.Now().UnixMilli()
	return now - observationGrace.Milliseconds(), now - sessionZombieAge.Milliseconds()
}
