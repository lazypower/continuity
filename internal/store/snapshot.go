package store

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// SnapshotRetentionBoots is how many successful boots pass before a retained
// migration safety snapshot is auto-deleted. Three is the smallest defensible
// value: enough for "ran the binary, the new schema works, ran it a couple
// more times to be sure," not so many that snapshots linger indefinitely.
const SnapshotRetentionBoots = 3

// EnvNoMigrationSnapshot is the opt-out: when set to any non-empty value,
// migrate() will skip the snapshot step for risky migrations. Intended for
// CI runs against ephemeral DBs, power users with their own backup story,
// and the rare case where the snapshot would not fit on disk.
//
// Setting this is a deliberate choice — the developer or operator is saying
// "I accept that a buggy table-rebuild migration would be unrecoverable in
// this run." It is NOT a default and we do not encourage it.
const EnvNoMigrationSnapshot = "CONTINUITY_NO_MIGRATION_SNAPSHOT"

// MigrationSnapshot describes a retained snapshot row. Returned by
// ListMigrationSnapshots for the `continuity snapshot list` command and used
// internally by the retention tick.
type MigrationSnapshot struct {
	Path          string
	PreVersion    int
	TargetVersion int
	CreatedAt     time.Time
	BootsSince    int
}

// ensureSnapshotStateTable creates the sidecar tracking table. Called from
// migrate() before the migration loop so the table is available whether or
// not any prior migration has touched it. Deliberately NOT part of the
// schema_versions migration system — this is metadata about migration runs,
// not user data.
func (db *DB) ensureSnapshotStateTable() error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS migration_snapshots (
			snapshot_path  TEXT PRIMARY KEY,
			pre_version    INTEGER NOT NULL,
			target_version INTEGER NOT NULL,
			created_at     INTEGER NOT NULL,
			boots_since    INTEGER NOT NULL DEFAULT 0
		)
	`)
	if err != nil {
		return fmt.Errorf("create migration_snapshots: %w", err)
	}
	return nil
}

// snapshotBeforeRiskyMigration is called from migrate() before applying each
// pending migration. If the migration is Risky and snapshots aren't opted
// out, it creates an atomic copy of the current DB via VACUUM INTO, returns
// the snapshot path, and leaves the recording of that path to the caller
// (after the migration successfully commits).
//
// Returns ("", nil) when no snapshot was taken (migration isn't risky, opt-out
// set, or DB is :memory:). Returns ("", err) when a snapshot SHOULD have been
// taken but couldn't be — the caller MUST treat this as fatal and abort the
// migration. Returns (path, nil) on success.
func (db *DB) snapshotBeforeRiskyMigration(m migration) (string, error) {
	if !m.Risky {
		return "", nil
	}
	if v := os.Getenv(EnvNoMigrationSnapshot); v != "" {
		// Opt-out path. We deliberately do not warn here — the operator
		// who set the env var knows what they chose.
		return "", nil
	}
	if db.Path == "" || db.Path == ":memory:" {
		// No on-disk file to snapshot. In-memory DBs are by definition
		// throwaway; tests that use them have other safety nets.
		return "", nil
	}

	dbDir := filepath.Dir(db.Path)
	snapDir := filepath.Join(dbDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o700); err != nil {
		return "", fmt.Errorf("create snapshot dir %s: %w", snapDir, err)
	}

	// Timestamp uses RFC3339 with dashes instead of colons so the filename
	// is safe on every filesystem (Windows in particular rejects colons).
	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	snapName := fmt.Sprintf("continuity-pre-v%d-%s.db", m.Version, timestamp)
	snapPath := filepath.Join(snapDir, snapName)

	// VACUUM INTO is the SQLite-blessed atomic copy. It produces a complete,
	// self-contained DB file in one statement, page-by-page from a consistent
	// snapshot, while the source DB stays open and readable.
	if _, err := db.Exec("VACUUM INTO ?", snapPath); err != nil {
		return "", fmt.Errorf("vacuum into %s: %w", snapPath, err)
	}

	if err := os.Chmod(snapPath, 0o600); err != nil {
		// Best-effort: a permission set failure isn't worth aborting the
		// migration over, but it IS worth telling the operator.
		fmt.Fprintf(os.Stderr, "warning: could not tighten permissions on snapshot %s: %v\n", snapPath, err)
	}

	return snapPath, nil
}

// recordSnapshotAndPruneOlder is called from migrate() AFTER a risky migration
// has successfully committed. It does two things atomically: enrolls the new
// snapshot in the tracking table, and removes any older snapshot records so
// only the most-recent risky migration's safety net is retained. The actual
// file deletion of older snapshots happens after the transaction commits so
// a failed DB write doesn't leave us with no record AND no file.
func (db *DB) recordSnapshotAndPruneOlder(snapPath string, preVersion, targetVersion int) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin snapshot record tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	rows, err := tx.Query(`SELECT snapshot_path FROM migration_snapshots WHERE snapshot_path != ?`, snapPath)
	if err != nil {
		return fmt.Errorf("scan old snapshots: %w", err)
	}
	var oldPaths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return fmt.Errorf("scan old snapshot path: %w", err)
		}
		oldPaths = append(oldPaths, p)
	}
	rows.Close()

	if _, err := tx.Exec(`DELETE FROM migration_snapshots WHERE snapshot_path != ?`, snapPath); err != nil {
		return fmt.Errorf("clear old snapshot rows: %w", err)
	}

	now := time.Now().UnixMilli()
	_, err = tx.Exec(
		`INSERT INTO migration_snapshots (snapshot_path, pre_version, target_version, created_at, boots_since) VALUES (?, ?, ?, ?, 0)`,
		snapPath, preVersion, targetVersion, now,
	)
	if err != nil {
		return fmt.Errorf("insert snapshot row: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit snapshot record: %w", err)
	}
	committed = true

	// DB commit succeeded — now best-effort delete the old files. A failure
	// here is a leaked file on disk; not fatal, and `continuity snapshot
	// prune` can clean it up later.
	for _, p := range oldPaths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "warning: could not delete superseded snapshot %s: %v\n", p, err)
		}
	}
	return nil
}

// TickSnapshotRetention is called once per successful `continuity serve`
// startup (from runServe, after Open). It increments boots_since on every
// retained snapshot row, then deletes any whose counter has reached the
// retention threshold.
//
// Deliberately NOT called from Open() itself: CLI subcommands that open the
// DB to inspect or prune snapshots should NOT advance the retention counter.
// Only the long-running serve process represents a real "the new schema
// works" boot.
func (db *DB) TickSnapshotRetention() error {
	if _, err := db.Exec(`UPDATE migration_snapshots SET boots_since = boots_since + 1`); err != nil {
		return fmt.Errorf("increment boots_since: %w", err)
	}

	rows, err := db.Query(`SELECT snapshot_path FROM migration_snapshots WHERE boots_since >= ?`, SnapshotRetentionBoots)
	if err != nil {
		return fmt.Errorf("scan expired snapshots: %w", err)
	}
	var expired []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			rows.Close()
			return fmt.Errorf("scan expired path: %w", err)
		}
		expired = append(expired, p)
	}
	rows.Close()

	for _, p := range expired {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "warning: could not delete expired snapshot %s: %v\n", p, err)
		}
		if _, err := db.Exec(`DELETE FROM migration_snapshots WHERE snapshot_path = ?`, p); err != nil {
			return fmt.Errorf("remove expired row: %w", err)
		}
	}
	return nil
}

// ListMigrationSnapshots returns all retained snapshot rows. Empty slice
// (not nil) when no snapshots are retained. Used by both the boot-time
// surfacing log and the `continuity snapshot list` command.
func (db *DB) ListMigrationSnapshots() ([]MigrationSnapshot, error) {
	rows, err := db.Query(`
		SELECT snapshot_path, pre_version, target_version, created_at, boots_since
		FROM migration_snapshots
		ORDER BY created_at DESC
	`)
	if err != nil {
		// migration_snapshots may not exist yet on a fresh install that
		// hasn't been through migrate() — treat that as empty.
		return []MigrationSnapshot{}, nil
	}
	defer rows.Close()

	var out []MigrationSnapshot
	for rows.Next() {
		var (
			s         MigrationSnapshot
			createdMs int64
		)
		if err := rows.Scan(&s.Path, &s.PreVersion, &s.TargetVersion, &createdMs, &s.BootsSince); err != nil {
			return nil, fmt.Errorf("scan snapshot: %w", err)
		}
		s.CreatedAt = time.UnixMilli(createdMs)
		out = append(out, s)
	}
	if out == nil {
		out = []MigrationSnapshot{}
	}
	return out, nil
}

// PruneMigrationSnapshots removes all retained snapshot files and clears
// the tracking table. Returns the number of snapshots removed.
//
// This is what `continuity snapshot prune` calls. It is destructive — the
// safety net is gone after this — so the CLI surface should make that clear
// to the operator.
func (db *DB) PruneMigrationSnapshots() (int, error) {
	snaps, err := db.ListMigrationSnapshots()
	if err != nil {
		return 0, err
	}
	for _, s := range snaps {
		if err := os.Remove(s.Path); err != nil && !os.IsNotExist(err) {
			return 0, fmt.Errorf("remove snapshot %s: %w", s.Path, err)
		}
	}
	if _, err := db.Exec(`DELETE FROM migration_snapshots`); err != nil {
		return 0, fmt.Errorf("clear migration_snapshots: %w", err)
	}
	return len(snaps), nil
}
