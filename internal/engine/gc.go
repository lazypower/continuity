package engine

import (
	"log"
	"strings"
	"time"
)

// GCMode is the garbage-collection toggle. GC reclaims genuine dead weight —
// memories decayed to the floor AND unretrieved for a long stretch — but because
// it is the one operation that destroys real memories, it ships OFF and earns its
// way up: off (inert) → shadow (log what it WOULD reclaim, delete nothing) → on.
type GCMode int

const (
	GCOff    GCMode = iota // default: the sweep does nothing
	GCShadow               // compute + log candidates, delete NOTHING
	GCOn                   // snapshot, then reclaim (bounded by gcPerSweepCap)
)

// Conservative by design — I would rather stay a little bloated than reclaim a
// memory that turns out to have mattered.
const (
	gcFloorThreshold = 0.1                  // decayed to the decay floor
	gcMinUnusedAge   = 180 * 24 * time.Hour // and untouched at least this long
	gcPerSweepCap    = 50                   // bounded blast radius per sweep
)

// ParseGCMode maps CONTINUITY_GC to a mode. Anything but "shadow"/"on" is off.
func ParseGCMode(s string) GCMode {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "shadow":
		return GCShadow
	case "on":
		return GCOn
	default:
		return GCOff
	}
}

func (m GCMode) String() string {
	switch m {
	case GCShadow:
		return "shadow"
	case GCOn:
		return "on"
	default:
		return "off"
	}
}

// SetGCMode configures the sweep's mode (read from CONTINUITY_GC at serve start).
func (e *Engine) SetGCMode(m GCMode) { e.gcMode = m }

// GCMode reports the configured mode (for /api/health).
func (e *Engine) GCMode() GCMode { return e.gcMode }

// GCReclaimableCount returns how many memories the sweep would reclaim right now
// (same predicate as the sweep), for health surfacing.
func (e *Engine) GCReclaimableCount() (int, error) {
	return e.DB.CountGCCandidates(gcFloorThreshold, e.gcCutoff())
}

func (e *Engine) gcCutoff() int64 {
	return time.Now().UnixMilli() - gcMinUnusedAge.Milliseconds()
}

// runGCSweep is invoked after each decay pass. off: nothing. shadow: log what it
// would compost, delete nothing. on: snapshot first (never reclaim without a
// restore point), then hard-delete up to gcPerSweepCap oldest candidates.
func (e *Engine) runGCSweep() {
	if e.gcMode == GCOff {
		return
	}

	candidates, err := e.DB.GCCandidates(gcFloorThreshold, e.gcCutoff(), gcPerSweepCap)
	if err != nil {
		log.Printf("gc: candidate scan failed: %v", err)
		return
	}
	if len(candidates) == 0 {
		return
	}

	days := int(gcMinUnusedAge.Hours() / 24)
	if e.gcMode == GCShadow {
		log.Printf("gc [shadow]: would compost %d memory(ies) — decayed to floor and unretrieved >%dd; deleting NOTHING", len(candidates), days)
		for i := range candidates {
			log.Printf("gc [shadow]:   %s (relevance=%.3f)", candidates[i].URI, candidates[i].Relevance)
		}
		return
	}

	// GCOn: snapshot first. If we can't take a restore point, refuse to reclaim
	// this sweep — deletion without recovery is exactly what we won't do.
	snap, err := e.DB.SnapshotNow("pre-gc-compost")
	if err != nil {
		log.Printf("gc: snapshot failed — refusing to reclaim this sweep: %v", err)
		return
	}
	if snap == "" {
		// No restore point produced (in-memory / path-less DB). "snapshot first"
		// is the contract, so an empty path is a hard stop, not a silent pass.
		log.Printf("gc: no restore point available (in-memory / path-less DB) — refusing to reclaim")
		return
	}
	reclaimed := 0
	for i := range candidates {
		if err := e.DB.DeleteNode(candidates[i].ID); err != nil {
			log.Printf("gc: delete %s failed: %v", candidates[i].URI, err)
			continue
		}
		reclaimed++
	}
	log.Printf("gc: composted %d memory(ies) — decayed to floor and unretrieved >%dd (snapshot: %s)", reclaimed, days, snap)
}
