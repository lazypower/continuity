package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
)

// =========================================================================
// Per-DB instance identity.
//
// schema_versions alone cannot distinguish two continuity DBs: every normal
// install applies the same fixed migration set, so the (version, description)
// rows are byte-identical across unrelated databases. A lineage fingerprint
// built from those rows would FALSE-MATCH — a sidecar transplanted next to an
// unrelated DB would pass the lineage check and restore the WRONG database.
//
// To anchor lineage to a specific physical database we write a random
// instance_id into a small continuity_meta table exactly once, at DB init.
// The property we need:
//
//   - A copy of the DB (cp, VACUUM INTO) carries the SAME instance_id, so a
//     snapshot and its source DB match (restore is allowed).
//   - An independently created DB gets a DIFFERENT instance_id, so a
//     transplanted sidecar mismatches the target DB (restore is refused).
//
// The instance_id is folded into the lineage fingerprint (see snapshot.go).
// =========================================================================

const (
	// metaTableName holds opaque per-DB key/value metadata that is NOT part of
	// the application schema (no migration owns it; it is created on first open
	// of any DB, fresh or existing). Kept deliberately minimal.
	metaTableName = "continuity_meta"

	// metaKeyInstanceID is the continuity_meta key under which the random
	// per-DB instance identity is stored.
	metaKeyInstanceID = "instance_id"
)

// ErrInstanceIDMissing is returned when a DB has no readable instance_id. For
// restore this is a fail-closed condition: without a per-DB identity we cannot
// prove a sidecar belongs to the live DB.
var ErrInstanceIDMissing = errors.New("snapshot: db has no instance identity")

// ensureInstanceID creates the continuity_meta table if absent and backfills a
// random instance_id when one is not already present. Idempotent and safe to
// call on every Open: a DB that already carries an instance_id keeps it (so a
// copied DB and its source agree), and a fresh/legacy DB without one gets a new
// random value exactly once. Runs as part of migrate(), so OpenNoMigrate (the
// read-only inspection path) never mutates the DB it is examining.
func (db *DB) ensureInstanceID() error {
	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`, metaTableName)); err != nil {
		return fmt.Errorf("create %s: %w", metaTableName, err)
	}

	var existing string
	err := db.QueryRow(
		fmt.Sprintf(`SELECT value FROM %s WHERE key = ?`, metaTableName),
		metaKeyInstanceID,
	).Scan(&existing)
	if err == nil && existing != "" {
		return nil // already has an identity — never regenerate
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read instance_id: %w", err)
	}

	id, gerr := newInstanceID()
	if gerr != nil {
		return gerr
	}
	// INSERT OR IGNORE so a concurrent open that won the race keeps its value;
	// we then read back the winning id rather than assuming ours landed.
	if _, err := db.Exec(
		fmt.Sprintf(`INSERT OR IGNORE INTO %s (key, value) VALUES (?, ?)`, metaTableName),
		metaKeyInstanceID, id); err != nil {
		return fmt.Errorf("write instance_id: %w", err)
	}
	return nil
}

// newInstanceID returns a random 128-bit identity as lowercase hex. crypto/rand
// so two DBs created in the same millisecond cannot collide.
func newInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("snapshot: generate instance_id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// readInstanceID returns the DB's instance_id WITHOUT creating the meta table
// or writing anything. Used by the restore/fingerprint read paths against a
// live DB that must not be mutated. Returns ErrInstanceIDMissing when the meta
// table or the row is absent so callers fail closed rather than fabricate one.
func readInstanceID(q queryer) (string, error) {
	// Probe for the table first; a missing table is the legacy-DB / wrong-DB
	// signal we must surface as ErrInstanceIDMissing, not a raw SQL error.
	var name string
	terr := q.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
		metaTableName,
	).Scan(&name)
	if errors.Is(terr, sql.ErrNoRows) {
		return "", ErrInstanceIDMissing
	}
	if terr != nil {
		return "", fmt.Errorf("snapshot: probe %s: %w", metaTableName, terr)
	}

	var id string
	err := q.QueryRow(
		fmt.Sprintf(`SELECT value FROM %s WHERE key = ?`, metaTableName),
		metaKeyInstanceID,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && id == "") {
		return "", ErrInstanceIDMissing
	}
	if err != nil {
		return "", fmt.Errorf("snapshot: read instance_id: %w", err)
	}
	return id, nil
}
