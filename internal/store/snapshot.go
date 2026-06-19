package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// =========================================================================
// Path-owned upgrade restore point.
//
// Before a destructive (Risky) migration rewrites mem_nodes, we capture ONE
// consistent image of the pre-upgrade DB into a sidecar directory next to the
// DB file. The artifact is owned by its on-disk path: there is no tracking
// table inside the DB, no absolute path persisted anywhere, and list/status/
// prune never open the DB. A copied/renamed DB does not inherit stale
// metadata unless its sidecar travels with it.
//
// Sidecar layout for DB /x/continuity.db:
//
//	/x/continuity.db.snapshot/
//	  snapshot.db     -- VACUUM INTO image of the pre-upgrade DB
//	  manifest.json   -- format_version 1 manifest (see Manifest)
//
// Core invariants (enforced here and exercised by snapshot_test.go):
//   - Snapshot is created at most once per upgrade run, before the FIRST
//     pending risky migration. A later risky migration must not replace it.
//   - A valid active manifest for this lineage is REUSED, never overwritten.
//   - A corrupt/partial sidecar fails closed (aborts the migration).
//   - Nothing outside the derived sidecar is ever written or deleted.
//   - We never delete/overwrite a file we cannot prove is ours.
// =========================================================================

const (
	// snapshotSidecarSuffix is appended to the canonical DB path to derive
	// the sidecar directory. Stable and path-derived: same real DB → same
	// sidecar, regardless of relative/absolute/symlinked spelling.
	snapshotSidecarSuffix = ".snapshot"

	// snapshotFileName is the ONLY permitted value of manifest.snapshot_file
	// in format_version 1. A plain filename — never absolute, never "..",
	// never containing a separator.
	snapshotFileName = "snapshot.db"

	// manifestFileName is the manifest within the sidecar.
	manifestFileName = "manifest.json"

	// manifestKind tags the manifest so a file that merely happens to be
	// named manifest.json cannot be mistaken for ours.
	manifestKind = "continuity.upgrade_restore_point"

	// manifestFormatVersion is the on-disk manifest schema version.
	manifestFormatVersion = 1

	// defaultExpiresAfterBoots is how many successful serve binds must occur
	// before the restore point auto-expires.
	defaultExpiresAfterBoots = 3

	// envDisableSnapshot, set to exactly "1", skips automatic snapshot
	// creation (the migration proceeds without a restore point, with a
	// warning). Any other value is ignored.
	envDisableSnapshot = "CONTINUITY_DISABLE_MIGRATION_SNAPSHOT"
)

// snapshotCreatedByVersion is overridden at link time by the CLI so the
// manifest records which binary produced the snapshot. Defaults to a static
// string so the store package has no dependency on cli.
var snapshotCreatedByVersion = "continuity dev"

// SetSnapshotCreatedByVersion lets the CLI record the running binary version
// in newly written manifests. Best-effort, informational only.
func SetSnapshotCreatedByVersion(v string) {
	if strings.TrimSpace(v) != "" {
		snapshotCreatedByVersion = v
	}
}

// Manifest is the format_version 1 sidecar manifest. All fields are persisted
// to manifest.json; no field holds an absolute path.
type Manifest struct {
	Kind          string `json:"kind"`
	FormatVersion int    `json:"format_version"`
	// SnapshotFile must equal snapshotFileName exactly (validated).
	SnapshotFile string `json:"snapshot_file"`

	CreatedAt        string `json:"created_at"`
	CreatedByVersion string `json:"created_by_version"`

	PreSchemaVersion        int `json:"pre_schema_version"`
	TargetSchemaVersion     int `json:"target_schema_version"`
	FirstRiskySchemaVersion int `json:"first_risky_schema_version"`

	// LineageFingerprint is sha256 over the schema_versions rows with
	// version <= pre_schema_version, formatted "sha256:<hex>". It lets
	// restore reject a sidecar transplanted next to an unrelated DB.
	LineageFingerprint string `json:"lineage_fingerprint"`

	// SnapshotSHA256 is "sha256:<hex>" over snapshot.db's bytes.
	SnapshotSHA256    string `json:"snapshot_sha256"`
	SnapshotSizeBytes int64  `json:"snapshot_size_bytes"`

	SuccessfulBoots             int     `json:"successful_boots"`
	ExpiresAfterSuccessfulBoots int     `json:"expires_after_successful_boots"`
	LastSuccessfulBootAt        *string `json:"last_successful_boot_at"`

	RestoreCount   int     `json:"restore_count"`
	LastRestoredAt *string `json:"last_restored_at"`
}

// SnapshotError wraps a snapshot/restore failure with a stable sentinel-style
// prefix so callers and tests can branch without string-fragility creep.
var (
	// ErrSnapshotUnsupportedPath is returned when the DB path is not eligible
	// for automatic snapshots (:memory: or a SQLite URI/DSN path).
	ErrSnapshotUnsupportedPath = errors.New("snapshot: db path does not support restore points")
	// ErrSnapshotSidecarCorrupt is returned when a sidecar exists but cannot
	// be proven valid. Risky migrations fail closed on this.
	ErrSnapshotSidecarCorrupt = errors.New("snapshot: sidecar exists but is corrupt or partial")
	// ErrNoRestorePoint is returned when no (valid) restore point exists.
	ErrNoRestorePoint = errors.New("snapshot: no restore point")
)

// snapshotEligiblePath reports whether path can host a path-owned sidecar.
// Rejects in-memory DBs and SQLite URI/DSN spellings (file:..., ?... ) whose
// real on-disk location is ambiguous to derive a sidecar from.
func snapshotEligiblePath(path string) bool {
	p := strings.TrimSpace(path)
	if p == "" || p == ":memory:" {
		return false
	}
	// SQLite URI ("file:..."), shared-cache and other DSN forms carry query
	// parameters; refuse rather than guess the underlying file.
	if strings.HasPrefix(p, "file:") {
		return false
	}
	if strings.ContainsAny(p, "?") {
		return false
	}
	return true
}

// sidecarPath derives the canonical sidecar directory for a DB path:
//
//	abs = Abs(path)
//	resolved = EvalSymlinks(abs)  (only if the DB file exists)
//	sidecar = resolved + ".snapshot"
//
// Relative and absolute spellings of the same real DB resolve identically.
// Returns ErrSnapshotUnsupportedPath for ineligible paths.
func sidecarPath(dbPath string) (string, error) {
	if !snapshotEligiblePath(dbPath) {
		return "", ErrSnapshotUnsupportedPath
	}
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return "", fmt.Errorf("snapshot: abs db path: %w", err)
	}
	resolved := abs
	if _, statErr := os.Stat(abs); statErr == nil {
		// Only resolve symlinks when the DB exists; a not-yet-created DB has
		// no link to resolve and EvalSymlinks would error.
		if r, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
			resolved = r
		}
	}
	return resolved + snapshotSidecarSuffix, nil
}

// SidecarPath is the exported derivation used by the CLI and tests. It does
// NOT open the DB.
func SidecarPath(dbPath string) (string, error) { return sidecarPath(dbPath) }

// manifestPath / snapshotDBPath are the two files inside a sidecar.
func manifestPathIn(sidecar string) string   { return filepath.Join(sidecar, manifestFileName) }
func snapshotDBPathIn(sidecar string) string { return filepath.Join(sidecar, snapshotFileName) }

// validateManifestShape checks the structural invariants of a manifest that
// do not require touching the snapshot file or the DB. Used by every caller
// before trusting any field.
func (m *Manifest) validateShape() error {
	if m.Kind != manifestKind {
		return fmt.Errorf("%w: kind %q != %q", ErrSnapshotSidecarCorrupt, m.Kind, manifestKind)
	}
	if m.FormatVersion != manifestFormatVersion {
		return fmt.Errorf("%w: format_version %d != %d", ErrSnapshotSidecarCorrupt, m.FormatVersion, manifestFormatVersion)
	}
	if err := validateSnapshotFileName(m.SnapshotFile); err != nil {
		return err
	}
	if m.PreSchemaVersion <= 0 {
		return fmt.Errorf("%w: pre_schema_version %d", ErrSnapshotSidecarCorrupt, m.PreSchemaVersion)
	}
	if m.TargetSchemaVersion < m.PreSchemaVersion {
		return fmt.Errorf("%w: target_schema_version %d < pre %d", ErrSnapshotSidecarCorrupt, m.TargetSchemaVersion, m.PreSchemaVersion)
	}
	if m.LineageFingerprint == "" || m.SnapshotSHA256 == "" {
		return fmt.Errorf("%w: empty fingerprint/hash", ErrSnapshotSidecarCorrupt)
	}
	return nil
}

// validateSnapshotFileName enforces that snapshot_file is exactly the
// permitted plain filename: no absolute path, no "..", no separator.
func validateSnapshotFileName(name string) error {
	if name != snapshotFileName {
		return fmt.Errorf("%w: snapshot_file %q != %q", ErrSnapshotSidecarCorrupt, name, snapshotFileName)
	}
	if filepath.IsAbs(name) ||
		strings.Contains(name, "..") ||
		strings.ContainsRune(name, '/') ||
		strings.ContainsRune(name, os.PathSeparator) ||
		filepath.Base(name) != name {
		return fmt.Errorf("%w: snapshot_file %q is not a plain filename", ErrSnapshotSidecarCorrupt, name)
	}
	return nil
}

// readManifest loads + JSON-decodes the manifest from a sidecar. It does not
// validate the snapshot file or DB lineage; callers layer that on.
func readManifest(sidecar string) (*Manifest, error) {
	raw, err := os.ReadFile(manifestPathIn(sidecar))
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSnapshotSidecarCorrupt, err)
	}
	return &m, nil
}

// loadValidManifest loads a manifest and validates its shape AND that the
// snapshot file exists, is a regular file (not a symlink), and matches the
// recorded hash and size. Returns ErrNoRestorePoint when no sidecar/manifest
// exists, ErrSnapshotSidecarCorrupt when present-but-unprovable.
func loadValidManifest(sidecar string) (*Manifest, error) {
	if err := assertNotSymlink(sidecar); err != nil {
		return nil, err
	}
	info, err := os.Stat(sidecar)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoRestorePoint
		}
		return nil, err
	}
	if !info.IsDir() {
		// A regular file where the sidecar dir should be is the "make
		// <db>.snapshot a regular file" sabotage case: fail closed.
		return nil, fmt.Errorf("%w: sidecar path is not a directory", ErrSnapshotSidecarCorrupt)
	}

	m, err := readManifest(sidecar)
	if err != nil {
		if os.IsNotExist(err) {
			// Sidecar dir exists but no manifest: partial/unknown. If a
			// snapshot.db is present this is a crash-after-rename remnant.
			// Either way, fail closed — never auto-delete.
			return nil, fmt.Errorf("%w: manifest missing", ErrSnapshotSidecarCorrupt)
		}
		return nil, err
	}
	if err := m.validateShape(); err != nil {
		return nil, err
	}

	snapPath := snapshotDBPathIn(sidecar)
	if err := assertRegularFile(snapPath); err != nil {
		return nil, err
	}
	if err := verifySnapshotHash(snapPath, m); err != nil {
		return nil, err
	}
	return m, nil
}

// assertNotSymlink fails closed if path is a symlink (sidecar must not be).
func assertNotSymlink(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s is a symlink", ErrSnapshotSidecarCorrupt, path)
	}
	return nil
}

// assertRegularFile fails closed unless path is a present, regular (non-link)
// file. Returns ErrSnapshotSidecarCorrupt (manifest present, snapshot absent
// is an invalid state).
func assertRegularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s missing", ErrSnapshotSidecarCorrupt, filepath.Base(path))
		}
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: %s is a symlink", ErrSnapshotSidecarCorrupt, filepath.Base(path))
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: %s is not a regular file", ErrSnapshotSidecarCorrupt, filepath.Base(path))
	}
	return nil
}

// verifySnapshotHash recomputes the snapshot's sha256 and size and compares
// them to the manifest. Mismatch is fail-closed.
func verifySnapshotHash(snapPath string, m *Manifest) error {
	sum, size, err := hashFile(snapPath)
	if err != nil {
		return err
	}
	if size != m.SnapshotSizeBytes {
		return fmt.Errorf("%w: snapshot size %d != manifest %d", ErrSnapshotSidecarCorrupt, size, m.SnapshotSizeBytes)
	}
	if sum != m.SnapshotSHA256 {
		return fmt.Errorf("%w: snapshot hash mismatch", ErrSnapshotSidecarCorrupt)
	}
	return nil
}

// hashFile returns ("sha256:<hex>", size, nil) for a file.
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), n, nil
}

// lineageFingerprint computes sha256 over the schema_versions rows with
// version <= upTo, ordered by version. Stable across the actual upgrade and
// across a copy of the DB; differs for an unrelated DB. Formatted
// "sha256:<hex>".
func lineageFingerprint(q queryer, upTo int) (string, error) {
	rows, err := q.Query(
		`SELECT version, description FROM schema_versions WHERE version <= ? ORDER BY version ASC`,
		upTo,
	)
	if err != nil {
		return "", fmt.Errorf("snapshot: read schema_versions for fingerprint: %w", err)
	}
	defer rows.Close()

	h := sha256.New()
	any := false
	for rows.Next() {
		var v int
		var desc string
		if err := rows.Scan(&v, &desc); err != nil {
			return "", err
		}
		// Length-prefix each field so row boundaries are unambiguous.
		fmt.Fprintf(h, "%d:%d:%s\n", v, len(desc), desc)
		any = true
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if !any {
		return "", fmt.Errorf("snapshot: no schema_versions rows <= %d", upTo)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// queryer is the read subset of *sql.DB / *DB we need for fingerprinting.
type queryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// =========================================================================
// Creation (runs inside migrate(), before any pending migration)
// =========================================================================

// ensureUpgradeRestorePoint takes (or reuses) the single pre-upgrade restore
// point when the pending migration set includes a risky migration. preVersion
// is the current on-disk schema version (== maxApplied).
//
//   - Fresh install (preVersion == 0): nothing to protect; no-op.
//   - No pending risky migration: no-op.
//   - Ineligible path (:memory:/URI/DSN): fail closed UNLESS the opt-out env
//     is set, matching the spec's "URI/DSN path risky migration fails closed
//     unless opt-out" failure-mode row.
//   - Opt-out env == "1": skip creation with a warning, proceed.
func (db *DB) ensureUpgradeRestorePoint(preVersion int) error {
	if preVersion <= 0 {
		return nil // fresh install — nothing to restore to
	}

	firstRisky, hasRisky := firstPendingRiskyVersion(preVersion)
	if !hasRisky {
		return nil
	}

	optOut := os.Getenv(envDisableSnapshot) == "1"

	if !snapshotEligiblePath(db.Path) {
		if optOut {
			fmt.Fprintf(os.Stderr,
				"warning: %s set; risky migration proceeding on path %q without a restore point\n",
				envDisableSnapshot, db.Path)
			return nil
		}
		return fmt.Errorf(
			"%w: %q (set %s=1 to proceed without a restore point)",
			ErrSnapshotUnsupportedPath, db.Path, envDisableSnapshot)
	}

	if optOut {
		fmt.Fprintf(os.Stderr,
			"warning: %s=1; skipping migration restore point before risky upgrade (pre v%d)\n",
			envDisableSnapshot, preVersion)
		return nil
	}

	target := headVersion()
	if err := db.createRestorePoint(preVersion, target, firstRisky); err != nil {
		return fmt.Errorf("create restore point before risky migration: %w", err)
	}
	return nil
}

// firstPendingRiskyVersion returns the version of the first risky migration
// strictly greater than preVersion (i.e. pending), and whether any exists.
func firstPendingRiskyVersion(preVersion int) (int, bool) {
	for _, m := range migrations {
		if m.Version > preVersion && m.Risky {
			return m.Version, true
		}
	}
	return 0, false
}

// createRestorePoint writes (or reuses) the sidecar restore point. The DB is
// expected to be open at preVersion and NOT yet migrated.
func (db *DB) createRestorePoint(preVersion, target, firstRisky int) error {
	sidecar, err := sidecarPath(db.Path)
	if err != nil {
		return err
	}

	// Reuse path: a fully valid manifest for THIS lineage already exists.
	// Never overwrite — this is what preserves the pre-v6 snapshot across a
	// later v9 migration in the same run, and across crash/retry.
	if existing, lerr := loadValidManifest(sidecar); lerr == nil {
		fp, fpErr := lineageFingerprint(db, existing.PreSchemaVersion)
		if fpErr != nil {
			return fpErr
		}
		if fp == existing.LineageFingerprint {
			return nil // reuse
		}
		// Valid-shaped manifest but different lineage: someone parked an
		// unrelated sidecar here. Fail closed rather than clobber it.
		return fmt.Errorf("%w: existing manifest belongs to a different DB lineage", ErrSnapshotSidecarCorrupt)
	} else if !errors.Is(lerr, ErrNoRestorePoint) {
		// Present-but-corrupt sidecar: fail closed, do not overwrite.
		return lerr
	}

	// No restore point yet — create one.
	return db.writeRestorePoint(sidecar, preVersion, target, firstRisky)
}

// writeRestorePoint performs the actual VACUUM INTO + validate + atomic
// publish. Only called when no valid sidecar exists.
func (db *DB) writeRestorePoint(sidecar string, preVersion, target, firstRisky int) error {
	// Sidecar path must not be a symlink even if we are about to create it.
	if err := assertNotSymlink(sidecar); err != nil {
		return err
	}
	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		return fmt.Errorf("snapshot: mkdir sidecar: %w", err)
	}
	_ = os.Chmod(sidecar, 0o700)

	// Compute lineage fingerprint from the live (pre-migration) DB.
	fingerprint, err := lineageFingerprint(db, preVersion)
	if err != nil {
		return err
	}

	// VACUUM INTO a temp file inside the sidecar. PID-tagged so a stale temp
	// from a crashed run is distinguishable and never mistaken for final.
	tmpSnap := filepath.Join(sidecar, fmt.Sprintf("snapshot.tmp.%d", os.Getpid()))
	_ = os.Remove(tmpSnap) // clear our own stale temp if present
	if _, err := db.Exec(`VACUUM INTO ?`, tmpSnap); err != nil {
		_ = os.Remove(tmpSnap)
		return fmt.Errorf("snapshot: VACUUM INTO: %w", err)
	}
	// From here on, remove tmpSnap on any failure.
	cleanupTmp := func() { _ = os.Remove(tmpSnap) }

	// Integrity-check the snapshot.
	if err := integrityCheck(tmpSnap); err != nil {
		cleanupTmp()
		return err
	}
	// The snapshot must reflect the pre-upgrade schema exactly.
	if sv, err := snapshotSchemaVersion(tmpSnap); err != nil {
		cleanupTmp()
		return err
	} else if sv != preVersion {
		cleanupTmp()
		return fmt.Errorf("snapshot: image schema v%d != pre-upgrade v%d", sv, preVersion)
	}

	_ = os.Chmod(tmpSnap, 0o600)

	sum, size, err := hashFile(tmpSnap)
	if err != nil {
		cleanupTmp()
		return err
	}

	// Atomic publish of the snapshot image.
	finalSnap := snapshotDBPathIn(sidecar)
	if err := os.Rename(tmpSnap, finalSnap); err != nil {
		cleanupTmp()
		return fmt.Errorf("snapshot: publish snapshot.db: %w", err)
	}
	_ = os.Chmod(finalSnap, 0o600)

	// Build + write the manifest (temp → fsync → rename).
	m := &Manifest{
		Kind:                        manifestKind,
		FormatVersion:               manifestFormatVersion,
		SnapshotFile:                snapshotFileName,
		CreatedAt:                   time.Now().UTC().Format(time.RFC3339),
		CreatedByVersion:            snapshotCreatedByVersion,
		PreSchemaVersion:            preVersion,
		TargetSchemaVersion:         target,
		FirstRiskySchemaVersion:     firstRisky,
		LineageFingerprint:          fingerprint,
		SnapshotSHA256:              sum,
		SnapshotSizeBytes:           size,
		SuccessfulBoots:             0,
		ExpiresAfterSuccessfulBoots: defaultExpiresAfterBoots,
		LastSuccessfulBootAt:        nil,
		RestoreCount:                0,
		LastRestoredAt:              nil,
	}
	if err := writeManifestAtomic(sidecar, m); err != nil {
		// Snapshot.db published but manifest write failed: sidecar is now
		// partial. We deliberately do NOT remove snapshot.db (we cannot
		// prove a concurrent process didn't just publish it). Subsequent
		// runs fail closed on the partial sidecar, which is the safe state.
		return err
	}

	fmt.Fprintf(os.Stderr,
		"  restore point: captured pre-v%d snapshot before risky migration → %s\n",
		preVersion, sidecar)
	return nil
}

// writeManifestAtomic writes the manifest via temp + fsync + rename, chmod 0600.
func writeManifestAtomic(sidecar string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: marshal manifest: %w", err)
	}
	tmp := filepath.Join(sidecar, fmt.Sprintf("manifest.tmp.%d", os.Getpid()))
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("snapshot: open manifest temp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: write manifest temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: fsync manifest temp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: close manifest temp: %w", err)
	}
	if err := os.Rename(tmp, manifestPathIn(sidecar)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: publish manifest: %w", err)
	}
	_ = os.Chmod(manifestPathIn(sidecar), 0o600)
	return nil
}

// integrityCheck opens path with OpenNoMigrate and runs PRAGMA integrity_check.
func integrityCheck(path string) error {
	sdb, err := OpenNoMigrate(path)
	if err != nil {
		return fmt.Errorf("snapshot: open for integrity_check: %w", err)
	}
	defer sdb.Close()
	var result string
	if err := sdb.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		return fmt.Errorf("snapshot: integrity_check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("snapshot: integrity_check returned %q", result)
	}
	return nil
}

// snapshotSchemaVersion opens path (no migrate) and reads MAX(schema_versions.version).
func snapshotSchemaVersion(path string) (int, error) {
	sdb, err := OpenNoMigrate(path)
	if err != nil {
		return 0, fmt.Errorf("snapshot: open for schema check: %w", err)
	}
	defer sdb.Close()
	return sdb.SchemaVersion()
}
