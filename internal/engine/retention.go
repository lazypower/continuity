package engine

import (
	"log"
	"os"
	"strconv"
	"strings"
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

	// sessionZombieAge is how long a session may sit in 'active' before we stop
	// believing it. Clients that crash or get killed never fire Stop/SessionEnd,
	// so their sessions stay 'active' forever and would otherwise pin their
	// observations forever with them.
	sessionZombieAge = 30 * 24 * time.Hour
)

// retentionEnvVar lets an operator widen, tighten, or disable retention without
// a rebuild. "0" or "off" disables pruning entirely; any positive integer is a
// day count replacing observationGrace.
const retentionEnvVar = "CONTINUITY_OBSERVATION_RETENTION_DAYS"

// observationGraceDuration resolves the effective grace window. Returns ok=false
// when the operator has disabled retention outright.
func observationGraceDuration() (d time.Duration, ok bool) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(retentionEnvVar)))
	if raw == "" {
		return observationGrace, true
	}
	if raw == "off" || raw == "false" {
		return 0, false
	}
	days, err := strconv.Atoi(raw)
	if err != nil || days < 0 {
		log.Printf("retention: ignoring invalid %s=%q — using default %dd", retentionEnvVar, raw, int(observationGrace.Hours()/24))
		return observationGrace, true
	}
	if days == 0 {
		return 0, false
	}
	return time.Duration(days) * 24 * time.Hour, true
}

// RetentionCutoffs returns (graceCutoff, zombieCutoff) in epoch millis, plus
// whether retention is enabled at all.
//
// Package-level on purpose. Retention is pure storage hygiene — it needs no LLM,
// no embedder, and no Engine. s.engine is legitimately nil when no LLM is
// configured, and those installs still accumulate observations, so binding this
// to a receiver would deny `continuity prune` to exactly the users most likely
// to need it.
func RetentionCutoffs() (graceCutoff, zombieCutoff int64, enabled bool) {
	grace, ok := observationGraceDuration()
	if !ok {
		return 0, 0, false
	}
	now := time.Now().UnixMilli()
	return now - grace.Milliseconds(), now - sessionZombieAge.Milliseconds(), true
}

// CountSpentObservations reports how many observation rows are reclaimable right
// now. Returns 0 when retention is disabled.
func CountSpentObservations(db *store.DB) (int64, error) {
	graceCutoff, zombieCutoff, enabled := RetentionCutoffs()
	if !enabled {
		return 0, nil
	}
	return db.CountSpentObservations(graceCutoff, zombieCutoff)
}

// PruneObservations reclaims spent observations and returns the row count. This
// is the shared path for the background sweep, /api/health, and
// `continuity prune`, so none of the three can drift onto a different rule.
func PruneObservations(db *store.DB) (int64, error) {
	graceCutoff, zombieCutoff, enabled := RetentionCutoffs()
	if !enabled {
		return 0, nil
	}
	return db.PruneSpentObservations(graceCutoff, zombieCutoff)
}

// runObservationRetention is invoked alongside the decay pass. Unlike the GC
// sweep this is uncapped: observations accumulate at tool-call rate, and a cap
// low enough to be a safe blast radius for memories would never keep up.
func (e *Engine) runObservationRetention() {
	grace, ok := observationGraceDuration()
	if !ok {
		return
	}
	pruned, err := PruneObservations(e.DB)
	if err != nil {
		log.Printf("retention: observation prune failed: %v", err)
		return
	}
	if pruned > 0 {
		log.Printf("retention: pruned %d observation(s) from sessions no longer in flight (grace: %dd)", pruned, int(grace.Hours()/24))
	}
}
