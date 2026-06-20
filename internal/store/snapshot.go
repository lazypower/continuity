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
	// ErrSymlinkedDBUnsupported is returned when the database FILE itself (the
	// path's leaf) is a symlink. continuity does not support a symlinked database
	// file: every DB open (store.Open / OpenNoMigrate) and every path-derived
	// snapshot operation (Status / Restore / Prune) FAILS CLOSED with this error
	// BEFORE touching any file, rather than try to resolve a symlinked leaf (the
	// recurring complexity/bug source across review rounds). The operator must
	// point CONTINUITY_DB at the real file. Parent-DIRECTORY symlinks (real leaf)
	// remain fully supported — only a symlinked LEAF is refused.
	ErrSymlinkedDBUnsupported = errors.New("store: continuity does not support a symlinked database file")
	// ErrURIDSNUnsupported is returned when the DB path is a SQLite URI/DSN
	// spelling (a `file:` scheme, or a DSN query string carrying URI-reserved
	// bytes like '?'/'#'/'%') that references a REAL on-disk file. Such a path
	// opens the real DB but is INVISIBLE to the path-owned coordination the
	// snapshot/restore feature relies on: AcquireServeLock is a no-op for it,
	// store.Open takes no shared lock, and the interrupted-restore detector
	// canonicalizes the literal URI string (so it misses the real
	// `<db>.snapshot/restore.in-progress.json`). serve-via-URI and
	// restore-via-real-path would then NOT mutually exclude, and crash recovery
	// via a URI open would miss the marker. Rather than try to recover the real
	// file from a DSN (the symlinked-leaf bug class's sibling), we FAIL CLOSED at
	// every DB open and every path-derived snapshot operation BEFORE any
	// lock/marker/sql.Open. `:memory:` is NOT a URI/DSN in this sense (it has no
	// file to coordinate) and stays allowed.
	ErrURIDSNUnsupported = errors.New("store: continuity requires a plain database file path, not a SQLite URI/DSN")
)

// refuseSymlinkedDBLeaf fails closed with ErrSymlinkedDBUnsupported when the DB
// FILE itself (the path's final component) is a symlink. It lstats the ABSOLUTE
// path's leaf (canonicalDBPath only resolves the PARENT dir and keeps the leaf,
// so a symlinked leaf survives into the canonical path and would still lstat as a
// link). A non-existent leaf, or any lstat error other than "is a symlink",
// returns nil — eligibility / existence is decided by the other gates in that
// case; this gate ONLY refuses a present symlinked leaf.
//
// This is the SINGLE up-front refusal every DB open and every path-derived
// snapshot operation runs before any MkdirAll / lock acquire / sql.Open / file
// touch, so a symlinked-leaf DB never reaches migrations, the marker check, or a
// sidecar derivation.
func refuseSymlinkedDBLeaf(dbPath string) error {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return nil
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf(
			"%w %s; set CONTINUITY_DB to the real file",
			ErrSymlinkedDBUnsupported, dbPath)
	}
	return nil
}

// refuseURIDSNPath fails closed with ErrURIDSNUnsupported when dbPath is a SQLite
// URI/DSN spelling that references a real file: a `file:` scheme OR a `?` DSN
// query. BOTH were verified to make modernc/SQLite open the REAL db while the
// LITERAL path string (used for the lock/marker/sidecar derivation) no longer
// names that file's canonical path — `file:/abs/db?mode=ro` parses as a URI, and
// a bare `<path>?mode=rwc` opens the real file with the DSN query parsed off. So a
// serve-via-URI and a restore-via-real-path would not mutually exclude, and crash
// recovery via the URI would miss the real sidecar marker. We refuse both at the
// SAME up-front gate as the symlinked leaf, before any MkdirAll/lock/marker/sql.Open.
//
// We do NOT refuse a plain path that merely CONTAINS '#'/'%': those are ordinary
// filesystem bytes and modernc opens such a path as the LITERAL file (verified).
// snapshotEligiblePath now AGREES — a '#'/'%' path is snapshot-ELIGIBLE too (Round
// 12, Finding 4): OpenNoMigrate percent-escapes the path into the file: URI it
// builds (roFileURI), so the read-only inspection still opens the intended literal
// file, while the lock/sidecar are plain filenames derived from the path. Only a
// genuine URI/DSN shape (file: scheme / '?' query) is unsupported, and both gates
// agree on that. Aligning them is what gives a '#'/'%' DB its lock + sidecar +
// snapshots instead of leaving its risky DDL unguarded.
//
// `:memory:` is explicitly NOT a URI/DSN here: it has no on-disk file to
// coordinate and is the spelling OpenMemory / the whole test suite relies on, so
// it is allowed through. A plain filesystem path (even one with parent-directory
// symlinks, or '#'/'%' bytes) is likewise allowed — only `file:`/`?` shapes refuse.
func refuseURIDSNPath(dbPath string) error {
	p := strings.TrimSpace(dbPath)
	if p == "" || p == ":memory:" {
		return nil
	}
	if strings.HasPrefix(p, "file:") || strings.ContainsRune(p, '?') {
		return fmt.Errorf(
			"%w; set CONTINUITY_DB to the file path (got %q)",
			ErrURIDSNUnsupported, dbPath)
	}
	return nil
}

// refuseUnsupportedDBPath is the SINGLE up-front refusal every DB open
// (store.Open / OpenNoMigrate) and every path-derived snapshot operation
// (Status / Restore / Prune / AcquireServeLock) runs BEFORE any MkdirAll / lock
// acquire / marker check / sql.Open. It bundles the two unsupported-path classes
// that both open a real DB while defeating the path-owned coordination layer:
//
//	(1) a symlinked DB FILE (leaf)      → ErrSymlinkedDBUnsupported
//	(2) a SQLite URI/DSN spelling       → ErrURIDSNUnsupported
//
// `:memory:` passes both (it has no file to coordinate). Keeping both checks in
// one helper means a new entry point cannot add one refusal and forget the other.
func refuseUnsupportedDBPath(dbPath string) error {
	if err := refuseSymlinkedDBLeaf(dbPath); err != nil {
		return err
	}
	return refuseURIDSNPath(dbPath)
}

// snapshotEligiblePath reports whether path can host a path-owned sidecar /
// lock. Rejects in-memory DBs and SQLite URI/DSN spellings (file:..., ?... )
// whose real on-disk location is ambiguous to derive a sidecar from.
//
// NOTE: this is a PATH-SHAPE check only. A symlinked DB FILE (leaf) is refused
// up front by refuseSymlinkedDBLeaf at every DB open and path-derived snapshot
// operation (it never reaches this check), so this function only ever sees a real
// (non-symlinked) leaf — it concerns the path SHAPE (:memory:/URI/reserved-char),
// not symlink status.
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
	// A '?' makes a path a DSN spelling ("<path>?mode=rwc"): SQLite parses the
	// rest as the query string (so it could open a DIFFERENT file or drop mode=ro),
	// and the literal string used to derive the lock/sidecar would no longer name
	// the real file. Refuse '?' for eligibility (refuseURIDSNPath refuses it for
	// open too) — it is a genuine URI/DSN shape.
	if strings.ContainsRune(p, '?') {
		return false
	}
	// '#' and '%' are ordinary FILESYSTEM bytes: modernc/SQLite opens such a path
	// as the LITERAL file, refuseURIDSNPath ALLOWS it (it is a valid plain path),
	// and the lock/sidecar are plain filenames derived from it. Previously these
	// were rejected for snapshot eligibility, which split the contract: a '#'/'%'
	// path was open-allowed but lock/sidecar-INELIGIBLE, so its risky DDL ran with
	// a no-op lock and no restore point (Round 12, Finding 4). They are now ELIGIBLE
	// — the shared/exclusive lock + sidecar + snapshots apply normally. OpenNoMigrate
	// percent-escapes the path it builds into the file: URI (roFileURI), so the
	// read-only inspection opens exactly the intended literal file (Round 7 behavior,
	// pinned by the existing reserved-char read-only test). Only :memory: and genuine
	// URI/DSN shapes (file: scheme / '?' query) stay ineligible.
	return true
}

// canonicalDBPath resolves a DB path to its single real on-disk target, the
// ONE derivation every sidecar/lock/backup name is keyed to. It resolves the
// DIRECTORY's symlinks but KEEPS the real leaf:
//
//	canonical = filepath.Join(EvalSymlinks(filepath.Dir(abs)), filepath.Base(abs))
//
// Parent-dir symlinks are STABLE — continuity never moves directories — so this
// derivation never dangles and returns the same answer whether or not the leaf
// exists. (It also no longer needs to follow a leaf symlink: a symlinked DB FILE
// is excluded from the snapshot feature entirely, so there is no symlinked-leaf
// resolution to do here. The leaf is kept verbatim.)
//
// Both dbLockPath and sidecarPath route through this one helper, so the lock and
// the sidecar are always keyed to the same real DB directory + leaf name.
func canonicalDBPath(dbPath string) (string, error) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return "", fmt.Errorf("snapshot: abs db path: %w", err)
	}
	// Resolve only the PARENT directory's symlinks (stable; never moved) and
	// rejoin the leaf. EvalSymlinks of the parent canonicalizes platform symlinks
	// (e.g. macOS /var → /private/var) so the recomputed sidecar/lock/backup names
	// match across spellings. If the parent cannot be resolved (e.g. it does not
	// exist yet), fall back to a plain Clean of the absolute path.
	if rp, perr := filepath.EvalSymlinks(filepath.Dir(abs)); perr == nil {
		return filepath.Join(rp, filepath.Base(abs)), nil
	}
	return filepath.Clean(abs), nil
}

// sidecarPath derives the canonical sidecar directory for a DB path:
//
//	sidecar = canonicalDBPath(path) + ".snapshot"
//
// Relative and absolute spellings of the same real DB resolve identically
// (parent-dir symlinks resolved, leaf kept; see canonicalDBPath), and the
// derivation never dangles. Returns ErrSnapshotUnsupportedPath for ineligible
// paths. NOTE: a symlinked DB leaf still yields a sidecar path here (the leaf is
// kept) — the snapshot FEATURE is disabled for it upstream (ensureUpgradeRestorePoint),
// not by refusing to derive a path.
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

// =========================================================================
// THE consolidated managed-file gate (one helper; no call site may bypass it).
//
// EVERY open of a file continuity manages inside the sidecar / DB dir —
// snapshot.db, manifest.json, restore.in-progress.json, the .pre-restore.*
// backups, and the .restore.staged.* temps — goes through openManagedFileNoFollow.
// It is the SINGLE place that enforces both halves of the no-symlink invariant:
//
//	(1) O_NOFOLLOW  — a final-component symlink fails the open (ELOOP on unix; an
//	    open-reparse-point on windows) so a planted symlink is never traversed;
//	(2) fstat regular-file — the opened descriptor is proven a regular file, so a
//	    FIFO / device / socket / directory (or a race that swapped the path to one
//	    after the open) fails closed too.
//
// A symlink/FIFO/device/dir at a managed-file path => ErrSnapshotSidecarCorrupt
// (fail closed), regardless of the leaf-symlink rule for the DB FILE itself: a
// planted symlink in OUR OWN sidecar must ALWAYS be refused. Routing every reader
// (readControlFileNoFollow, hashFileNoFollow) through this one primitive means a
// future managed-file position cannot be added without the gate.
//
// openControlFileNoFollow (the per-OS primitive this builds on) additionally adds
// O_NONBLOCK on unix so a FIFO open returns immediately instead of hanging before
// the fstat can reject it. Callers own Close()ing the returned file.
// =========================================================================
func openManagedFileNoFollow(path string) (*os.File, error) {
	f, err := openControlFileNoFollow(path)
	if err != nil {
		// A MISSING file must stay os.IsNotExist so callers can distinguish absence
		// from corruption. Any OTHER open failure on a managed file is fail-closed
		// corruption: O_NOFOLLOW makes a final-component symlink fail with ELOOP, and
		// we report that (and any other non-NotExist open error) as a corrupt sidecar
		// rather than leaking a raw "too many levels of symbolic links" error or
		// following the redirection.
		if os.IsNotExist(err) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: managed file %s could not be opened (%v)", ErrSnapshotSidecarCorrupt, filepath.Base(path), err)
	}
	info, serr := f.Stat()
	if serr != nil {
		_ = f.Close()
		return nil, serr
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("%w: managed file %s is not a regular file", ErrSnapshotSidecarCorrupt, filepath.Base(path))
	}
	return f, nil
}

// readControlFileNoFollow reads a sidecar CONTROL FILE (manifest.json /
// restore.in-progress.json) through the consolidated managed-file gate
// (openManagedFileNoFollow): O_NOFOLLOW + fstat regular-file, so a symlink, FIFO,
// device, socket, or directory at the control-file path FAILS CLOSED as
// ErrSnapshotSidecarCorrupt rather than being followed (which could read a file
// outside the sidecar) or blocked on (a FIFO read can hang forever). A missing
// file propagates os.ErrNotExist so callers can distinguish absence.
func readControlFileNoFollow(path string) ([]byte, error) {
	f, err := openManagedFileNoFollow(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, rerr := io.ReadAll(f)
	if rerr != nil {
		return nil, rerr
	}
	return data, nil
}

// readManifest loads + JSON-decodes the manifest from a sidecar. It does not
// validate the snapshot file or DB lineage; callers layer that on. The manifest
// is read through readControlFileNoFollow so a symlink/FIFO planted at
// manifest.json fails closed (corrupt sidecar) instead of being followed/blocked
// (Round 9, Finding 6).
func readManifest(sidecar string) (*Manifest, error) {
	raw, err := readControlFileNoFollow(manifestPathIn(sidecar))
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
// no manifest, or a moved-aside original that silently reverts.
//
// PLATFORM-AWARE (Round 13, Finding 1): directory fsync is delegated to the
// per-OS platformFsyncDir hook. On unix it opens the directory and Sync()s the
// handle (real durability) — and several callers (restore-point publication, the
// restore move-aside/publish, recovery scrub) treat its failure as FATAL, so a
// genuine durability failure aborts the operation rather than reporting a
// non-durable success. On WINDOWS, File.Sync on a DIRECTORY handle returns an
// error (Windows does not support fsync of a directory handle), so platformFsyncDir
// is a NO-OP (returns nil): a fatal-on-failure caller would otherwise spuriously
// ABORT every restore-point publication and every restore/recovery on Windows.
// NTFS metadata durability is handled differently (the OS/filesystem orders
// metadata writes), and FILE fsync (fsyncFile) still runs on BOTH platforms so the
// snapshot/manifest BYTES are durable everywhere; only the directory-handle fsync
// is platform-specific.
func fsyncDir(dir string) error {
	return platformFsyncDir(dir)
}

// fsyncFile fsyncs a single regular file's contents to disk (Round 9, Finding 1A).
// Unlike fsyncDir (which makes a directory ENTRY durable) this makes the file's
// BYTES durable, so a hash committed to a manifest cannot name data still sitting
// in the page cache after a power loss. Opened read-write so Sync flushes file
// data; the caller passes a path it owns.
func fsyncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if serr := f.Sync(); serr != nil {
		f.Close()
		return serr
	}
	return f.Close()
}

// hookSnapshotDirFsync is a TEST-ONLY seam (nil in production). When set it
// REPLACES the sidecar-dir fsyncs that publish snapshot.db and manifest.json so a
// test can force the durability failure deterministically and assert that
// restore-point CREATION fails closed (which aborts the risky migration) and never
// leaves a published manifest naming non-durable snapshot bytes (Round 9, Finding 1A).
var hookSnapshotDirFsync func(sidecar string) error

// hookAfterManifestRename is a TEST-ONLY seam (nil in production). When set it
// fires inside writeManifestAtomic AFTER manifest.json has been renamed into the
// sidecar and BEFORE the function returns; a non-nil return makes the manifest
// publish FAIL despite the manifest file already being on disk. It models a
// transient fsync/publish failure that nonetheless left a manifest.json behind,
// so a test can assert the failed-creation cleanup removes BOTH snapshot.db AND
// the just-renamed manifest.json — leaving NO partial (manifest-only) sidecar
// (Change 2).
var hookAfterManifestRename func(sidecar string) error

// fsyncSnapshotDir makes the sidecar directory durable after a snapshot.db /
// manifest.json publish, routed through the hookSnapshotDirFsync test seam when
// set. FAIL-CLOSED by contract (Round 9, Finding 1A): the caller treats a non-nil
// return as an error that aborts restore-point creation, never a warning.
func fsyncSnapshotDir(sidecar string) error {
	if hookSnapshotDirFsync != nil {
		return hookSnapshotDirFsync(sidecar)
	}
	return fsyncDir(sidecar)
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

// hashFileNoFollow returns ("sha256:<hex>", size, nil) for a REGULAR managed file
// at path, opened through the consolidated managed-file gate
// (openManagedFileNoFollow): O_NOFOLLOW + fstat regular-file. It is the hash the
// destructive recovery paths MUST use instead of the symlink-following hashFile
// (Round 7, Findings 1 & 2): a forged/corrupt marker must never make recovery hash
// (and then trust) a file reached through a symlink the marker pointed at — that is
// how a backup_prefix symlinked to another directory's DB could be pulled over the
// live DB. The gate makes a symlink fail the open (ELOOP on unix) and rejects any
// non-regular file (FIFO/device/dir/socket), so even a race that swapped the path
// after the open cannot smuggle a non-regular file through.
func hashFileNoFollow(path string) (string, int64, error) {
	f, err := openManagedFileNoFollow(path)
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

	// Use the ACTUAL pending set (absent rows), matching the migrator and the
	// risk-detection gate (Round 10, Finding 3). A MAX-based firstPendingRiskyVersion(
	// preVersion) would miss a gapped risky migration (MAX=9, row 6 absent) and skip
	// the restore point even though runPendingMigrations runs the risky v6.
	firstRisky, hasRisky, ferr := db.firstPendingRiskyMigrationActual()
	if ferr != nil {
		return ferr
	}
	if !hasRisky {
		return nil
	}

	// A symlinked DB FILE (leaf) never reaches here: store.Open / OpenNoMigrate
	// refuse it up front with ErrSymlinkedDBUnsupported (refuseSymlinkedDBLeaf),
	// BEFORE any open/migration. db.Path therefore always names a real, non-symlinked
	// leaf at this point — there is no "proceed unprotected through a symlinked leaf"
	// case anymore.

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
	// ACTUAL pending set (absent rows), matching the migrator and risk gate (Round
	// 10, Finding 3) — a gapped risky migration must still get a restore point.
	firstRisky, hasRisky, ferr := db.firstPendingRiskyMigrationActual()
	if ferr != nil {
		return ferr
	}
	if !hasRisky {
		return nil
	}
	// A symlinked DB FILE (leaf) never reaches here: store.Open refused it up front
	// with ErrSymlinkedDBUnsupported before any open/migration, so db.Path always
	// names a real, non-symlinked leaf at this point.
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
//
// NOTE: this MAX-version heuristic is NOT a faithful model of which migrations
// the migrator will run. runPendingMigrations applies ANY migration whose
// schema_versions ROW IS ABSENT — gaps included — so a gapped/bogus bookkeeping
// table (e.g. MAX=9 but row 6 missing) would make this report "nothing risky
// pending" while the migrator still runs the risky v6 rebuild UNPROTECTED
// (Round 10, Finding 3). The risk-detection call sites therefore use
// db.firstPendingRiskyMigrationActual (the absent-row set, matching the
// migrator). This helper is retained only for the manifest `firstRisky` field
// where preVersion is the verified MAX of a contiguous bookkeeping table.
func firstPendingRiskyVersion(preVersion int) (int, bool) {
	for _, m := range migrations {
		if m.Version > preVersion && m.Risky {
			return m.Version, true
		}
	}
	return 0, false
}

// firstPendingRiskyMigrationActual computes risk detection from the ACTUAL pending
// set — exactly the migrations runPendingMigrations will apply: every migration
// whose schema_versions ROW IS ABSENT. It returns the version of the FIRST (lowest,
// in migration order) pending risky migration and whether any pending migration is
// risky.
//
// It models the migrator faithfully rather than via a MAX(version) heuristic, so a
// pending risky migration is never missed (the snapshot trigger matches what runs).
//
// GAPS ARE NOW REFUSED UPSTREAM (Round 12, Finding 3): a gapped bookkeeping table
// (a known migration below MAX(present) with no row) is rejected by
// detectSchemaVersionsGap in riskyUpgradePending/migrate BEFORE this runs, so in
// practice this only ever sees a CONTIGUOUS present set plus a trailing pending
// tail. The absent-row computation is retained because it remains the correct,
// migrator-faithful model for that contiguous-plus-tail case (Round 10, Finding 3,
// minus its gapped sub-case which now fails closed). A pure read; reads through the
// idempotent schema_versions create the caller has already ensured.
func (db *DB) firstPendingRiskyMigrationActual() (firstRisky int, hasRisky bool, err error) {
	for _, m := range migrations {
		var count int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM schema_versions WHERE version = ?", m.Version,
		).Scan(&count); err != nil {
			return 0, false, fmt.Errorf("check migration %d pending: %w", m.Version, err)
		}
		if count > 0 {
			continue // already applied — migrator skips it
		}
		// Absent row → the migrator WILL run this migration. If it is risky, it is
		// the first pending risky migration (migrations is in ascending order).
		if m.Risky {
			return m.Version, true, nil
		}
	}
	return 0, false, nil
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

	// failClosed removes this call's partial artifacts and — only when WE created
	// the sidecar dir this call — the now-empty sidecar dir, so a failed snapshot
	// creation leaves NO partial restore point behind (Finding 6). It removes BOTH
	// the named temp (if any) AND the published snapshot.db / manifest.json this
	// call created inside the sidecar, so a transient publish failure cannot leave a
	// MANIFEST-ONLY (or snapshot-only) sidecar that every later run treats as corrupt
	// and that prune refuses to remove (Change 2). All three names are provably ours:
	// this call holds the op-lock and O_EXCL-created/renamed every one of them, so no
	// concurrent process could have published them. It never touches a pre-existing
	// sidecar or any file it cannot prove this call created.
	failClosed := func(tmp string, e error) error {
		if tmp != "" {
			_ = os.Remove(tmp)
		}
		// Remove the whole sidecar CONTENT this call published (snapshot.db +
		// manifest.json), not just the temp — otherwise a manifest-publish failure
		// after the snapshot.db rename would wedge the sidecar with a partial
		// (manifest-only / snapshot-only) restore point that fails closed forever and
		// that prune will not delete (it is not a valid manifest). Best-effort, and
		// only on paths we created this call under the held lock.
		for _, p := range []string{snapshotDBPathIn(sidecar), manifestPathIn(sidecar)} {
			if rmErr := os.Remove(p); rmErr != nil && !os.IsNotExist(rmErr) {
				fmt.Fprintf(os.Stderr,
					"warning: snapshot: remove partial %s on fail-closed cleanup: %v\n",
					filepath.Base(p), rmErr)
			}
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

	// DURABILITY: fsync the snapshot BYTES before publishing the name (Round 9,
	// Finding 1A). VACUUM INTO wrote tmpSnap through SQLite's own fd, which is
	// already closed, so the page cache may still hold unflushed data; renaming it
	// to snapshot.db and then publishing a manifest that records its hash would let
	// a power loss leave a manifest naming a snapshot.db whose bytes never reached
	// disk (a published restore point describing non-durable data). fsync the file
	// first so the bytes the manifest commits to are durable. FAIL CLOSED: a sync
	// failure aborts the restore-point creation (and thus the risky migration).
	if err := fsyncFile(tmpSnap); err != nil {
		return failClosed(tmpSnap, fmt.Errorf("snapshot: fsync snapshot image before publish: %w", err))
	}

	// Atomic publish of the snapshot image.
	finalSnap := snapshotDBPathIn(sidecar)
	if err := os.Rename(tmpSnap, finalSnap); err != nil {
		return failClosed(tmpSnap, fmt.Errorf("snapshot: publish snapshot.db: %w", err))
	}
	cleanupTmp() // tmpSnap consumed by the rename; nothing left to remove
	_ = os.Chmod(finalSnap, 0o600)
	// fsync the sidecar dir so the published snapshot.db DIRECTORY ENTRY survives
	// power loss BEFORE the manifest names it. FAIL CLOSED (Round 9, Finding 1A):
	// this was previously a warning, so a published manifest could name a
	// snapshot.db whose directory entry never reached disk. The snapshot-dir fsync
	// is now an ERROR — it fails the restore-point creation, which aborts the risky
	// migration — so a published manifest never describes a non-durable snapshot.db.
	// We remove the just-published snapshot.db (provably ours: O_EXCL temp renamed
	// under the lock this call) so no half-built restore point lingers.
	if err := fsyncSnapshotDir(sidecar); err != nil {
		if rmErr := os.Remove(finalSnap); rmErr != nil && !os.IsNotExist(rmErr) {
			fmt.Fprintf(os.Stderr, "warning: snapshot: remove snapshot.db after fsync failure: %v\n", rmErr)
		}
		return failClosed("", fmt.Errorf("snapshot: fsync sidecar dir after publishing snapshot.db (must be durable before the manifest names it): %w", err))
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
		// Snapshot.db was published but the manifest publish failed (e.g. a transient
		// fsync/rename failure that nonetheless left a manifest.json renamed in). The
		// sidecar is now partial: it may hold snapshot.db, OR snapshot.db + a
		// manifest.json that does not describe a durable point, OR a manifest-only
		// remnant. failClosed removes BOTH snapshot.db AND manifest.json (provably
		// ours — op-lock held, every name O_EXCL-created/renamed this call) and the
		// sidecar dir if we created it, so NO partial restore point lingers. Leaving a
		// manifest-only sidecar would fail closed on every later run AND prune would
		// refuse to remove it (it is not a valid manifest), wedging the DB (Change 2).
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
	// TEST SEAM (Change 2): model a publish failure that occurs AFTER manifest.json
	// is already renamed in. Returning an error here leaves manifest.json on disk and
	// makes writeManifestAtomic fail, exercising writeRestorePoint's failed-creation
	// cleanup (which must remove BOTH snapshot.db and this manifest.json). nil in prod.
	if hookAfterManifestRename != nil {
		if err := hookAfterManifestRename(sidecar); err != nil {
			return fmt.Errorf("snapshot: publish manifest: %w", err)
		}
	}
	// fsync the sidecar dir so the manifest rename is durable across power loss.
	// FAIL CLOSED (Round 9, Finding 1A): this was previously a warning, so a
	// just-synced manifest file whose DIRECTORY ENTRY never reached disk could
	// vanish on a crash and leave a snapshot.db with no manifest — a restore point
	// that fails closed on every later run. The manifest-dir fsync is now an error
	// so a creation that cannot durably publish the manifest fails the restore-point
	// creation (and thus the risky migration) instead of reporting a non-durable
	// success. writeRestorePoint's caller cleans up the partial snapshot.db.
	if err := fsyncSnapshotDir(sidecar); err != nil {
		return fmt.Errorf("snapshot: fsync sidecar dir after publishing the manifest (must be durable): %w", err)
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
