package store

import (
	"crypto/rand"
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

// canonicalDBPath resolves a DB path to its single real on-disk target, the
// ONE derivation every sidecar/lock/backup name is keyed to. It must return the
// same answer for every spelling of the same real DB AND survive the two states
// a mid-restore DB can be in:
//
//	abs           = Abs(path)
//	(a) DB present:        EvalSymlinks(abs)               — follows symlinks
//	(b) DB missing, abs is
//	    a dangling symlink: Readlink(abs) then resolve via parent dir
//	(c) DB missing, plain:  resolve via parent dir (e.g. /var → /private/var)
//
// (b) and (c) are why lock and sidecar resolution can no longer diverge
// (Finding 3): when a restore through a symlinked CONTINUITY_DB has moved the
// real DB aside, EvalSymlinks(abs) fails, and a naive fallback to abs would key
// the lock to the LINK while the sidecar was written under the REAL DB. Both
// dbLockPath and sidecarPath now route through this one helper, so the lock
// and the sidecar are always keyed to the same real DB.
func canonicalDBPath(dbPath string) (string, error) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return "", fmt.Errorf("snapshot: abs db path: %w", err)
	}
	// (a) Existing, resolvable path: follow symlinks to the real file.
	if _, statErr := os.Stat(abs); statErr == nil {
		if r, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
			return r, nil
		}
	}
	// (b) abs is itself a (possibly dangling) symlink: follow it via Readlink so
	// a crashed restore through a symlink still resolves to the REAL DB.
	if li, lerr := os.Lstat(abs); lerr == nil && li.Mode()&os.ModeSymlink != 0 {
		if target, rlErr := os.Readlink(abs); rlErr == nil {
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(abs), target)
			}
			return resolveViaParentDir(target), nil
		}
	}
	// (c) The DB itself is gone (or never existed) and abs is not a symlink.
	// Resolving the PARENT dir still canonicalizes platform symlinks so the
	// recomputed sidecar/lock/backup names match what production wrote.
	return resolveViaParentDir(abs), nil
}

// sidecarPath derives the canonical sidecar directory for a DB path:
//
//	sidecar = canonicalDBPath(path) + ".snapshot"
//
// Relative and absolute spellings of the same real DB resolve identically, and
// the derivation survives a dangling/missing target (see canonicalDBPath).
// Returns ErrSnapshotUnsupportedPath for ineligible paths.
func sidecarPath(dbPath string) (string, error) {
	if !snapshotEligiblePath(dbPath) {
		return "", ErrSnapshotUnsupportedPath
	}
	resolved, err := canonicalDBPath(dbPath)
	if err != nil {
		return "", err
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
	// Retention must be present and sane. A missing/zero
	// expires_after_successful_boots would otherwise default to 0 and make the
	// FIRST successful boot expire (delete) the restore point — so a corrupt or
	// hand-edited manifest that dropped this field must fail closed, not silently
	// self-destruct.
	if m.ExpiresAfterSuccessfulBoots < 1 {
		return fmt.Errorf("%w: expires_after_successful_boots %d < 1",
			ErrSnapshotSidecarCorrupt, m.ExpiresAfterSuccessfulBoots)
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

// fsyncDir fsyncs a directory so a rename INTO it (or a file creation) is
// durable across a power loss (Finding 5). An atomic rename updates a directory
// entry; without fsyncing the directory, a crash can lose that entry even though
// the file's own data was synced — leaving, e.g., a published snapshot.db with
// no manifest, or a moved-aside original that silently reverts. Best-effort by
// contract: a platform/filesystem that cannot fsync a directory handle (some
// network FS) returns an error the caller logs but does not treat as fatal, so
// durability hardening never breaks an otherwise-successful operation.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if serr := d.Sync(); serr != nil {
		d.Close()
		return serr
	}
	return d.Close()
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

// =========================================================================
// Owned temp files (creation proves ownership — Finding 7)
//
// Cleanup of PID-named temps is unsafe: a different process may legitimately
// hold a temp with the same PID-derived name (PIDs are reused), so removing a
// pre-existing temp could clobber a sibling's in-flight work. Instead we create
// every temp/staging file with O_CREATE|O_EXCL and a random token: the create
// SUCCEEDING proves we own that exact path, and we only ever remove paths we
// created. An O_EXCL collision yields a fresh random name rather than trusting
// or removing the existing file.
// =========================================================================

// tempTokenAttempts bounds the O_EXCL retry loop. With a 64-bit random token a
// single attempt practically never collides; the loop only guards against the
// astronomically unlikely case (or a hostile pre-creation), failing closed.
const tempTokenAttempts = 64

// randomToken returns a short random hex token for unique temp names.
func randomToken() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("snapshot: random token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// createOwnedTemp creates and opens a fresh file "<dir>/<prefix><token><suffix>"
// with O_CREATE|O_EXCL|O_WRONLY (0600). The returned path is proven owned by
// this process (the exclusive create succeeded). On the vanishingly rare O_EXCL
// collision it retries with a new token; it NEVER removes or truncates a
// pre-existing file. Caller is responsible for removing the path it gets back.
func createOwnedTemp(dir, prefix, suffix string) (*os.File, string, error) {
	for i := 0; i < tempTokenAttempts; i++ {
		tok, err := randomToken()
		if err != nil {
			return nil, "", err
		}
		path := filepath.Join(dir, fmt.Sprintf("%s%d.%s%s", prefix, os.Getpid(), tok, suffix))
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return f, path, nil
		}
		if !os.IsExist(err) {
			return nil, "", err
		}
		// Collision (someone holds that exact name): pick a fresh token, never
		// touch the existing file.
	}
	return nil, "", fmt.Errorf("snapshot: could not create a unique temp in %s", dir)
}

// reserveOwnedTempName returns a path that is proven free AND owned by this
// process for a primitive (like VACUUM INTO) that must create the file itself.
// It O_EXCL-creates a placeholder to prove ownership, then removes that
// placeholder (safe: we just created it) and returns the now-free path. A
// caller that hands this path to VACUUM INTO will fail closed if a racing
// process re-creates it in the meantime — never a clobber of foreign data.
func reserveOwnedTempName(dir, prefix, suffix string) (string, error) {
	f, path, err := createOwnedTemp(dir, prefix, suffix)
	if err != nil {
		return "", err
	}
	if cerr := f.Close(); cerr != nil {
		_ = os.Remove(path)
		return "", cerr
	}
	if rerr := os.Remove(path); rerr != nil {
		return "", rerr
	}
	return path, nil
}

// lineageFingerprint computes sha256 over the DB's per-instance identity AND
// the schema_versions rows with version <= upTo, ordered by version.
//
// schema_versions rows are identical across every normal continuity DB, so
// hashing them alone false-matches unrelated databases. We therefore fold in
// the random instance_id (see instance.go): a COPY of the DB (cp / VACUUM INTO)
// preserves the instance_id so a snapshot matches its source, while an
// independently created DB carries a different instance_id and mismatches.
//
// A DB with no readable instance identity yields ErrInstanceIDMissing so
// restore fails closed rather than fabricating a match. Formatted
// "sha256:<hex>".
func lineageFingerprint(q queryer, upTo int) (string, error) {
	instanceID, err := readInstanceID(q)
	if err != nil {
		return "", err
	}

	rows, err := q.Query(
		`SELECT version, description FROM schema_versions WHERE version <= ? ORDER BY version ASC`,
		upTo,
	)
	if err != nil {
		return "", fmt.Errorf("snapshot: read schema_versions for fingerprint: %w", err)
	}
	defer rows.Close()

	h := sha256.New()
	// Bind the fingerprint to this physical DB. Length-prefixed so the instance
	// component can never be confused with a following schema row.
	fmt.Fprintf(h, "instance:%d:%s\n", len(instanceID), instanceID)
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

// ensureUpgradeRestorePointLocked is ensureUpgradeRestorePoint's body for the
// case migrate() has ALREADY taken the operation lock (the risky-upgrade path).
// It performs the same eligibility/opt-out gating but calls the lock-free
// createRestorePointLocked so the single op-lock spans both restore-point
// creation and the subsequent migration DDL. Callers must hold the op-lock.
func (db *DB) ensureUpgradeRestorePointLocked(preVersion int) error {
	if preVersion <= 0 {
		return nil
	}
	firstRisky, hasRisky := firstPendingRiskyVersion(preVersion)
	if !hasRisky {
		return nil
	}
	// Eligibility was already checked by migrate() before taking the lock (this
	// path is only entered for an eligible risky upgrade), but re-assert
	// defensively rather than assume.
	if !snapshotEligiblePath(db.Path) {
		return fmt.Errorf("%w: %q", ErrSnapshotUnsupportedPath, db.Path)
	}
	// Opt-out skips ONLY the snapshot, never the serialization (Finding 4): migrate()
	// holds the op-lock around this call AND the subsequent DDL regardless, so two
	// opt-out upgrades still serialize. Here we simply decline to create the restore
	// point and let the (locked) migration proceed.
	if os.Getenv(envDisableSnapshot) == "1" {
		fmt.Fprintf(os.Stderr,
			"warning: %s=1; skipping migration restore point before risky upgrade (pre v%d)\n",
			envDisableSnapshot, preVersion)
		return nil
	}
	target := headVersion()
	if err := db.createRestorePointLocked(preVersion, target, firstRisky); err != nil {
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

// createRestorePoint writes (or reuses) the sidecar restore point, ACQUIRING
// the sidecar operation lock for the duration. The DB is expected to be open at
// preVersion and NOT yet migrated. Used by tests and any caller that does not
// already hold the op-lock; migrate()'s risky-upgrade path takes the lock once
// and uses createRestorePointLocked so the lock also spans the migration DDL.
func (db *DB) createRestorePoint(preVersion, target, firstRisky int) error {
	// Serialize concurrent restore-point creation: two migration opens racing
	// against the same DB could both pass the "no restore point" check and
	// double-publish. Take the EXCLUSIVE DB lock (the same lock a risky migration
	// and a restore take); the loser waits the bounded window then fails closed
	// (ErrDBLocked) rather than clobbering a sibling's work.
	//
	// This is the path used by tests and by any caller that does NOT already hold
	// the exclusive lock. migrate()'s risky-upgrade path takes exclusive once and
	// calls createRestorePointLocked so the lock also spans the migration DDL.
	release, lerr := acquireExclusiveLockForOwner(db)
	if lerr != nil {
		return lerr
	}
	defer release()

	return db.createRestorePointLocked(preVersion, target, firstRisky)
}

// createRestorePointLocked is createRestorePoint's body for callers that ALREADY
// hold the sidecar operation lock (migrate()'s risky-upgrade path). It must not
// acquire the lock again — the in-process op mutex is not re-entrant, so a
// second acquire from the same goroutine would deadlock.
func (db *DB) createRestorePointLocked(preVersion, target, firstRisky int) error {
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
		if fp != existing.LineageFingerprint {
			// Valid-shaped manifest but different lineage: someone parked an
			// unrelated sidecar here. Fail closed rather than clobber it.
			return fmt.Errorf("%w: existing manifest belongs to a different DB lineage", ErrSnapshotSidecarCorrupt)
		}
		// Same lineage. Reuse the existing restore point when it still COVERS this
		// upgrade run — i.e. it captures a pre-version at or below ours, its target
		// reaches at least as far as this run's target, and the upgrade it protects
		// is NOT yet complete (current schema below the existing target). This is
		// the interrupted-upgrade case: a crash after v6 commits but before v9
		// leaves preVersion=6 while the valid pre-v5 point (target>=9) still rolls
		// the whole v5→v9 window back. Continuing under it is correct — re-snapshot
		// at pre-v6 would lie about the rollback target and overwrite recovery
		// material; the point already covers us.
		//
		// preVersion is the current on-disk schema version (== maxApplied), so
		// "current schema" is preVersion here.
		covers := existing.PreSchemaVersion <= preVersion &&
			existing.TargetSchemaVersion >= target &&
			preVersion < existing.TargetSchemaVersion
		if covers {
			// REUSE INTEGRITY GATE (Finding 4, Round 6): loadValidManifest proved the
			// manifest's SHAPE + the snapshot.db's hash/size match what the manifest
			// records, but a self-consistent manifest can still sit beside a
			// snapshot.db that is NOT a usable SQLite database (garbage bytes whose
			// recorded hash happens to match a hand-edited/forged manifest). Reusing
			// it would let the risky migration proceed with a restore point that
			// `restore --confirm` later fails integrity_check on — defeating the whole
			// safety feature. Before trusting an EXISTING point to cover this run we
			// run the SAME PRAGMA integrity_check + snapshot-schema-version validation
			// that creation and restore do. On failure we DO NOT reuse and DO NOT
			// silently proceed: fail closed so the operator prunes/recreates.
			snapPath := snapshotDBPathIn(sidecar)
			if err := integrityCheck(snapPath); err != nil {
				return fmt.Errorf(
					"%w: existing restore point failed integrity_check (%v); "+
						"run 'continuity snapshot prune --confirm' to remove it and let a fresh restore point be created",
					ErrSnapshotSidecarCorrupt, err)
			}
			sv, svErr := snapshotSchemaVersion(snapPath)
			if svErr != nil {
				return fmt.Errorf(
					"%w: could not read existing restore point's schema version (%v); "+
						"run 'continuity snapshot prune --confirm' to remove it",
					ErrSnapshotSidecarCorrupt, svErr)
			}
			if sv != existing.PreSchemaVersion {
				return fmt.Errorf(
					"%w: existing restore point's snapshot is schema v%d but its manifest records pre-v%d; "+
						"run 'continuity snapshot prune --confirm' to remove it",
					ErrSnapshotSidecarCorrupt, sv, existing.PreSchemaVersion)
			}
			return nil // reuse — same lineage, covers us, AND the snapshot is a valid DB
		}
		// Otherwise the existing point does NOT protect this run: it is either for
		// an ALREADY-COMPLETED upgrade (current schema >= existing target) or a
		// different/older window that does not cover this target. Reusing it would
		// lie about what the restore point rolls back to; overwriting it would
		// destroy recovery material from the earlier window. Fail closed and make
		// the operator restore or prune explicitly.
		return fmt.Errorf(
			"%w: an existing restore point captures pre-v%d→v%d, but this upgrade is at v%d→v%d; "+
				"run 'continuity snapshot restore --confirm' or 'continuity snapshot prune --confirm' first",
			ErrSnapshotSidecarCorrupt,
			existing.PreSchemaVersion, existing.TargetSchemaVersion, preVersion, target)
	} else if !errors.Is(lerr, ErrNoRestorePoint) {
		// Present-but-corrupt sidecar: fail closed, do not overwrite.
		return lerr
	}

	// No restore point yet — create one.
	return db.writeRestorePoint(sidecar, preVersion, target, firstRisky)
}

// writeRestorePoint performs the actual VACUUM INTO + validate + atomic
// publish. Only called when no valid sidecar exists, and (for the risky-upgrade
// path) under the held op-lock — so the sidecar it creates and the temps within
// are this call's to clean up on failure.
func (db *DB) writeRestorePoint(sidecar string, preVersion, target, firstRisky int) error {
	// Sidecar path must not be a symlink even if we are about to create it.
	if err := assertNotSymlink(sidecar); err != nil {
		return err
	}
	// Track whether the sidecar dir already existed: if WE create it here and the
	// snapshot-creation then fails, we remove our partial sidecar so no
	// half-built restore point lingers (Finding 6). We never remove a sidecar dir
	// that pre-existed our call, and removeOwnedPartialSidecar only ever rmdir's
	// when the dir holds nothing but our own aborted artifacts.
	sidecarPreexisted := true
	if _, statErr := os.Lstat(sidecar); statErr != nil {
		if os.IsNotExist(statErr) {
			sidecarPreexisted = false
		} else {
			return statErr
		}
	}
	if err := os.MkdirAll(sidecar, 0o700); err != nil {
		return fmt.Errorf("snapshot: mkdir sidecar: %w", err)
	}
	_ = os.Chmod(sidecar, 0o700)

	// failClosed removes this call's partial artifacts (the named temp, if any)
	// and — only when WE created the sidecar dir this call — the now-empty sidecar
	// dir, so a failed snapshot creation leaves NO partial restore point behind
	// (Finding 6). It never touches a pre-existing sidecar or any file it cannot
	// prove this call created.
	failClosed := func(tmp string, e error) error {
		if tmp != "" {
			_ = os.Remove(tmp)
		}
		if !sidecarPreexisted {
			removeOwnedEmptySidecar(sidecar)
		}
		return e
	}

	// Establish the per-DB instance identity HERE — only after the sidecar is
	// proven usable (not a symlink, dir creatable), and BEFORE the VACUUM INTO so
	// the snapshot image captures the same instance_id. Doing it here (rather than
	// unconditionally in migrate()) keeps the DB UNMUTATED when the sidecar is
	// blocked/fail-closed: a v5 DB with a regular-file or symlinked sidecar gets
	// NO continuity_meta write, because we never reach this point. Idempotent: a
	// DB that already carries an instance_id keeps it (so a copy and its source
	// agree), which is exactly what the lineage fingerprint below relies on.
	//
	// IDENTITY vs TRACKING METADATA (Finding 6): instance_id is per-DB IDENTITY —
	// it is INTENTIONALLY written into the DB and INTENTIONALLY copy-preserved
	// (cp/VACUUM INTO carry it), which is categorically different from the
	// snapshot-TRACKING metadata the design keeps OUT of the DB (no absolute
	// paths, no manifest rows). A stray identity row left by a snapshot that
	// failed AFTER this write is BENIGN: it causes no data/schema loss, the DB
	// stays at its pre-version, and a later successful snapshot reuses the same
	// id. So we keep the identity write before VACUUM (the snapshot MUST capture
	// it for lineage) and accept that a post-write failure may leave the id — the
	// partial SIDECAR is what we scrub, not the benign identity row.
	if err := db.ensureInstanceID(); err != nil {
		return failClosed("", fmt.Errorf("snapshot: ensure instance identity: %w", err))
	}

	// Compute lineage fingerprint from the live (pre-migration) DB.
	fingerprint, err := lineageFingerprint(db, preVersion)
	if err != nil {
		return failClosed("", err)
	}

	// VACUUM INTO a temp file inside the sidecar. The name is reserved via an
	// O_EXCL placeholder (proves ownership, then removed) so we never clobber a
	// pre-existing snapshot.tmp from another process; VACUUM INTO then creates the
	// file at that proven-free path. A racing re-create makes VACUUM fail closed
	// rather than overwrite foreign data (Finding 7).
	tmpSnap, terr := reserveOwnedTempName(sidecar, "snapshot.tmp.", "")
	if terr != nil {
		return failClosed("", terr)
	}
	if _, err := db.Exec(`VACUUM INTO ?`, tmpSnap); err != nil {
		return failClosed(tmpSnap, fmt.Errorf("snapshot: VACUUM INTO: %w", err))
	}
	// From here on, remove tmpSnap (and our partial sidecar) on any failure.
	cleanupTmp := func() { _ = os.Remove(tmpSnap) }

	// Integrity-check the snapshot.
	if err := integrityCheck(tmpSnap); err != nil {
		return failClosed(tmpSnap, err)
	}
	// The snapshot must reflect the pre-upgrade schema exactly.
	if sv, err := snapshotSchemaVersion(tmpSnap); err != nil {
		return failClosed(tmpSnap, err)
	} else if sv != preVersion {
		return failClosed(tmpSnap, fmt.Errorf("snapshot: image schema v%d != pre-upgrade v%d", sv, preVersion))
	}

	_ = os.Chmod(tmpSnap, 0o600)

	sum, size, err := hashFile(tmpSnap)
	if err != nil {
		return failClosed(tmpSnap, err)
	}

	// Atomic publish of the snapshot image.
	finalSnap := snapshotDBPathIn(sidecar)
	if err := os.Rename(tmpSnap, finalSnap); err != nil {
		return failClosed(tmpSnap, fmt.Errorf("snapshot: publish snapshot.db: %w", err))
	}
	cleanupTmp() // tmpSnap consumed by the rename; nothing left to remove
	_ = os.Chmod(finalSnap, 0o600)
	// fsync the sidecar dir so the published snapshot.db entry survives power
	// loss BEFORE the manifest names it (Finding 5).
	if err := fsyncDir(sidecar); err != nil {
		fmt.Fprintf(os.Stderr, "warning: snapshot: fsync sidecar dir after publish: %v\n", err)
	}

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
		// Snapshot.db published but the manifest write failed: the sidecar is now
		// partial. We hold the op-lock and renamed OUR OWN proven-owned temp into
		// snapshot.db this call, so it is provably ours to scrub (no concurrent
		// process could have published it under the lock). Remove the partial
		// snapshot.db and our sidecar dir so no half-built restore point lingers
		// (Finding 6) — leaving a snapshot.db with no manifest would just fail
		// closed on every later run.
		if rmErr := os.Remove(finalSnap); rmErr != nil && !os.IsNotExist(rmErr) {
			fmt.Fprintf(os.Stderr, "warning: snapshot: remove partial snapshot.db after manifest failure: %v\n", rmErr)
		}
		return failClosed("", err)
	}

	fmt.Fprintf(os.Stderr,
		"  restore point: captured pre-v%d snapshot before risky migration → %s\n",
		preVersion, sidecar)
	return nil
}

// removeOwnedEmptySidecar removes the sidecar directory ONLY when it is empty —
// i.e. this call's aborted snapshot creation left nothing behind in it. If any
// entry remains (a foreign file, or material we could not prove is ours) the dir
// is left intact: we never rmdir a directory holding files we cannot prove are
// ours (Finding 6, consistent with the global bar). Best-effort.
func removeOwnedEmptySidecar(sidecar string) {
	entries, err := os.ReadDir(sidecar)
	if err != nil {
		return
	}
	if len(entries) != 0 {
		return // not empty — leave anything we did not prove ours
	}
	_ = os.Remove(sidecar)
}

// writeManifestAtomic writes the manifest via temp + fsync + rename, chmod 0600.
func writeManifestAtomic(sidecar string, m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: marshal manifest: %w", err)
	}
	// O_EXCL-create the temp (proves ownership) so we never truncate a
	// pre-existing manifest.tmp from another process (Finding 7).
	f, tmp, err := createOwnedTemp(sidecar, "manifest.tmp.", "")
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
	// fsync the sidecar dir so the manifest rename is durable across power loss
	// (Finding 5): a synced manifest file with an unsynced directory entry could
	// otherwise vanish on crash, leaving a snapshot.db with no manifest.
	if err := fsyncDir(sidecar); err != nil {
		fmt.Fprintf(os.Stderr, "warning: snapshot: fsync sidecar dir after manifest: %v\n", err)
	}
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
