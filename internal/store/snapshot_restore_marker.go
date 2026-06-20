package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// =========================================================================
// Restore marker (minimal crash-recovery journal for restore).
//
// Restore moves the live DB triplet aside and renames a staged snapshot into
// place. A crash between those renames could leave a MISSING DB next to a stale
// WAL with no automatic recovery. To make any crash recoverable we drop a small
// marker file in the sidecar BEFORE the first destructive rename. The marker
// records exactly the paths involved:
//
//	restoredDBPath  -- canonical (resolved) live DB path being replaced
//	stagedPath      -- staged snapshot copy in the DB dir
//	backupPrefix    -- pre-restore moved-aside name prefix ("<db>.pre-restore.<ts>")
//	movedSuffixes   -- which of {"","-wal","-shm"} were actually moved aside
//	dbPublished     -- whether the staged file was renamed into the DB path
//
// On the next Open/Restore we detect the marker and finish the job:
//   - If the staged file was already published (dbPublished), COMPLETE: scrub
//     any stale live -wal/-shm and clear the marker.
//   - Otherwise ROLL BACK: move the originals back from backupPrefix and remove
//     the staged file, restoring the pre-restore state.
//
// FAIL-CLOSED RECOVERY MODEL (the post-pivot contract):
//   - A routine Open()/OpenNoMigrate() NEVER acts on the marker. If a marker is
//     present (or the sidecar is unsafe, or the marker is corrupt) the open
//     FAILS CLOSED with ErrRestoreInterrupted and touches nothing. A marker a
//     crash, corruption, or an attacker can write therefore cannot drive
//     destructive file moves on an innocent open.
//   - Recovery runs ONLY from `snapshot restore --confirm` (recoverPendingRestore),
//     under the serve lock, AFTER a hard marker-schema gate (validateMarkerSchema)
//     AND the canonical-path gate (resolveCanonicalRestore).
//
// SECURITY: the marker is an on-disk file that an attacker (or a corrupt prior
// run) could plant with arbitrary path fields. Recovery therefore TRUSTS NOTHING
// in the marker except the dbPublished phase bit. Every path it acts on is
// RECOMPUTED from the canonical resolved DB path + sidecar (see
// recoverPendingRestore), and any marker field that names a path OUTSIDE that
// canonical set makes recovery fail closed rather than touch it. Because the
// originals are moved aside (never deleted) and all paths are canonical, even a
// flipped phase bit can only mis-sequence within the recoverable canonical set.
//
// This is deliberately a marker + recover/rollback, NOT a general journal.
// =========================================================================

// ErrRestoreInterrupted signals that a restore marker is PRESENT (or present
// but corrupt) in the DB's sidecar: a prior restore crashed mid-flight and the
// DB on disk may be missing, torn, or mid-swap. Open()/OpenNoMigrate() return
// this and refuse to touch the DB — recovery happens ONLY under explicit
// operator intent (`continuity snapshot restore --confirm`), never as a side
// effect of a routine open (Findings 1, 2, 4). A corrupt/partial marker is
// ALSO ErrRestoreInterrupted: we fail closed rather than erase it or fabricate
// a DB over it.
var ErrRestoreInterrupted = errors.New("store: an interrupted restore is pending")

// restoreMarkerName is the marker file inside the sidecar. The ".json" suffix
// is irrelevant to ownership — the sidecar is path-owned — but keeps it
// inspectable.
const restoreMarkerName = "restore.in-progress.json"

// preRestoreInfix is the fixed component of every pre-restore backup prefix
// ("<resolvedDB>.pre-restore.<ts>.<pid>[.<n>]"). Resume requires the marker's
// backup prefix to begin with "<resolvedDB>.pre-restore." so a planted marker
// cannot point the rollback rename at an arbitrary destination.
const preRestoreInfix = ".pre-restore."

// stagedInfix is the fixed component of every staged-snapshot temp name
// (".restore.staged.<pid>.db"). Resume requires the marker's staged path to be
// a plain file in the DB directory whose basename carries this infix.
const stagedInfix = ".restore.staged."

// safeTokenRune reports whether r may appear in the recovery-safe token that the
// canonical backup/staged names are built from (Round 7, Findings 1 & 2). The
// token is the variable tail after the fixed canonical prefix; constraining it
// to this charset means a reconstructed backup/staged path can NEVER contain a
// path separator, a ".." traversal, or any byte that lets the marker redirect
// recovery at a file outside this DB's own canonical set. Allowed: ASCII
// letters, digits, '.', '-', '_'. A '.' is permitted (timestamps + pid + suffix
// use it) but ".." is rejected separately by tokenIsSafe.
func safeTokenRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z':
		return true
	case r >= 'A' && r <= 'Z':
		return true
	case r >= '0' && r <= '9':
		return true
	case r == '.' || r == '-' || r == '_':
		return true
	default:
		return false
	}
}

// tokenIsSafe reports whether tok is a non-empty recovery-safe token: every rune
// is in the safe charset AND it contains no ".." traversal. A path separator can
// never appear (it is not in the charset), so a token that passes this can only
// name a sibling of the canonical prefix — never another directory, never a
// traversal (Round 7, Findings 1 & 2).
func tokenIsSafe(tok string) bool {
	if tok == "" {
		return false
	}
	if strings.Contains(tok, "..") {
		return false
	}
	for _, r := range tok {
		if !safeTokenRune(r) {
			return false
		}
	}
	return true
}

// restoreMarker is the on-disk recovery record. All paths are absolute/resolved
// so recovery does not depend on the process CWD. NONE of these fields is
// trusted as authority by resume — see resolveCanonicalRestore.
type restoreMarker struct {
	Version        int      `json:"version"`
	RestoredDBPath string   `json:"restored_db_path"`
	StagedPath     string   `json:"staged_path"`
	BackupPrefix   string   `json:"backup_prefix"`
	MovedSuffixes  []string `json:"moved_suffixes"`
	DBPublished    bool     `json:"db_published"`

	// OriginalDBSHA256 is "sha256:<hex>" over the ORIGINAL live DB's bytes,
	// recorded AT RESTORE START before the DB file is moved aside (Finding 1). On
	// rollback, recovery verifies the moved-aside backup file's hash equals this
	// recorded value before renaming it back over the live DB path — so a planted,
	// stale, or corrupt `<db>.pre-restore.*` file can never be pulled over the DB.
	// Empty only when the original DB was absent at restore start (no "" suffix in
	// MovedSuffixes), in which case there is no DB backup to provenance-check.
	OriginalDBSHA256 string `json:"original_db_sha256"`
}

func restoreMarkerPathIn(sidecar string) string {
	return filepath.Join(sidecar, restoreMarkerName)
}

// writeRestoreMarkerAtomic persists the marker via temp + fsync + rename so a
// crash never leaves a half-written marker that recovery would misread.
func writeRestoreMarkerAtomic(sidecar string, mk *restoreMarker) error {
	data, err := json.MarshalIndent(mk, "", "  ")
	if err != nil {
		return fmt.Errorf("restore: marshal marker: %w", err)
	}
	// O_EXCL-create the temp (proves ownership) so a foreign restore.marker.tmp
	// is never truncated (Finding 7).
	f, tmp, err := createOwnedTemp(sidecar, "restore.marker.tmp.", "")
	if err != nil {
		return fmt.Errorf("restore: open marker temp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("restore: write marker temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("restore: fsync marker temp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("restore: close marker temp: %w", err)
	}
	if err := os.Rename(tmp, restoreMarkerPathIn(sidecar)); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("restore: publish marker: %w", err)
	}
	// fsync the sidecar dir so the marker rename is DURABLE before the first
	// destructive rename relies on it. This is FAIL-CLOSED (Round 7, Finding 6):
	// the marker MUST be durable before Restore moves the live DB aside. A power
	// loss with a non-durable marker leaves a torn restore with NO marker — the
	// next Open would see no marker beside a missing DB and FABRICATE a fresh DB
	// instead of returning ErrRestoreInterrupted, silently destroying the data the
	// restore point existed to protect. So a marker-dir fsync failure here is a
	// hard error: the caller must NOT proceed to the destructive move-aside. The
	// test seam lets a test force the failure deterministically.
	if hookMarkerDirFsync != nil {
		if err := hookMarkerDirFsync(sidecar); err != nil {
			_ = removeRestoreMarker(sidecar)
			return fmt.Errorf("restore: fsync marker dir (marker must be durable before any destructive step): %w", err)
		}
		return nil
	}
	if err := fsyncDir(sidecar); err != nil {
		// Roll back the just-published marker so we do not leave a non-durable
		// marker the caller would (wrongly) treat as a committed recovery point.
		_ = removeRestoreMarker(sidecar)
		return fmt.Errorf("restore: fsync marker dir (marker must be durable before any destructive step): %w", err)
	}
	return nil
}

// hookMarkerDirFsync is a TEST-ONLY seam (nil in production). When set, it
// REPLACES the sidecar-dir fsync inside writeRestoreMarkerAtomic so a test can
// force the durability failure deterministically (no real FS that refuses dir
// fsync is needed) and assert Restore fails closed BEFORE the destructive
// move-aside (Round 7, Finding 6).
var hookMarkerDirFsync func(sidecar string) error

// restoreMarkerVersion is the only marker schema version this binary writes and
// accepts. A marker with a different (or zero) version is rejected as a hard
// gate by validateMarkerSchema — recovery never acts on a schema it cannot
// reason about.
const restoreMarkerVersion = 1

// readRestoreMarker loads the marker if present. Returns (nil, nil) when no
// marker exists. A present-but-unparseable marker is an error (fail closed:
// recovery cannot reason about it).
func readRestoreMarker(sidecar string) (*restoreMarker, error) {
	raw, err := os.ReadFile(restoreMarkerPathIn(sidecar))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var mk restoreMarker
	if err := json.Unmarshal(raw, &mk); err != nil {
		return nil, fmt.Errorf("%w: restore marker: %v", ErrSnapshotSidecarCorrupt, err)
	}
	return &mk, nil
}

// validateMarkerSchema is the hard schema gate the EXPLICIT recovery path runs
// before it will act on a marker: version must be exactly restoreMarkerVersion
// and the required fields must be present and well-formed. A corrupt `{}` or
// partial marker (e.g. version 0, no DB path, a backup prefix but no moved
// suffixes) fails closed here — recovery never trusts a marker it cannot fully
// reason about (Finding 4). Path-content constraints (canonical set membership)
// are still enforced separately by resolveCanonicalRestore; this gate is the
// SHAPE check that precedes it.
func validateMarkerSchema(mk *restoreMarker) error {
	if mk.Version != restoreMarkerVersion {
		return fmt.Errorf("%w: restore marker version %d != %d",
			ErrSnapshotSidecarCorrupt, mk.Version, restoreMarkerVersion)
	}
	if strings.TrimSpace(mk.RestoredDBPath) == "" {
		return fmt.Errorf("%w: restore marker missing restored_db_path", ErrSnapshotSidecarCorrupt)
	}
	// A not-yet-published marker that recorded moved suffixes MUST carry a backup
	// prefix to roll them back to; the inverse (a backup prefix but an empty
	// moved set) is an incoherent partial that we refuse rather than guess.
	if len(mk.MovedSuffixes) > 0 && strings.TrimSpace(mk.BackupPrefix) == "" {
		return fmt.Errorf("%w: restore marker moved suffixes without a backup prefix", ErrSnapshotSidecarCorrupt)
	}
	if strings.TrimSpace(mk.BackupPrefix) != "" && len(mk.MovedSuffixes) == 0 {
		return fmt.Errorf("%w: restore marker backup prefix without moved suffixes", ErrSnapshotSidecarCorrupt)
	}
	return nil
}

func removeRestoreMarker(sidecar string) error {
	if err := os.Remove(restoreMarkerPathIn(sidecar)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// canonicalRestore is the recomputed, trusted view of an in-progress restore.
// Every path here is derived from the resolved DB path + sidecar, NOT from the
// marker. resolveCanonicalRestore validates the marker's path fields against
// this canonical set and refuses if any field implies a path outside it.
type canonicalRestore struct {
	resolvedDB string   // canonical live DB path (survives a dangling symlink)
	sidecar    string   // <resolvedDB>.snapshot (already asserted not a symlink)
	staged     string   // canonical staged snapshot path inside the DB dir
	backup     string   // canonical pre-restore backup prefix
	moved      []string // subset of {"","-wal","-shm"} the marker claims it moved
	published  bool     // a phase HINT from the marker — reconciled against disk
	// originalDBSHA256 is the recorded hash of the pre-restore live DB, used to
	// provenance-check the moved-aside backup before a rollback rename (Finding 1).
	originalDBSHA256 string
	// snapshotSHA256 is the validated restore point's snapshot.db hash, used to
	// decide from DISK whether the live DB IS the restored image (Finding 1).
	snapshotSHA256 string
}

// resolveDBPathSurvivingDangling returns the canonical DB path surviving a
// dangling/missing target. It delegates to canonicalDBPath so recovery resolves
// the SAME real DB (and therefore the same sidecar, lock, and backup names) that
// sidecarPath/dbLockPath use — there is exactly one resolution rule now
// (Finding 3). The name is kept where recovery reads clearer for the intent.
func resolveDBPathSurvivingDangling(dbPath string) (string, error) {
	return canonicalDBPath(dbPath)
}

// resolveViaParentDir canonicalizes path by EvalSymlinks'ing its parent
// directory (which exists even when the file itself is missing) and rejoining
// the basename. Falls back to a plain Clean when the parent cannot be resolved.
func resolveViaParentDir(path string) string {
	if rp, perr := filepath.EvalSymlinks(filepath.Dir(path)); perr == nil {
		return filepath.Join(rp, filepath.Base(path))
	}
	return filepath.Clean(path)
}

// resolveCanonicalRestore derives the trusted canonical view for dbPath and
// validates the marker against it. It RECOMPUTES the sidecar (asserting it is
// not a symlink) and constrains every path the marker names to the canonical
// set: the live DB triplet, a staged file in the DB dir, and a pre-restore
// backup prefix beneath the resolved DB. A marker that points anywhere else
// fails closed (ErrSnapshotSidecarCorrupt) so a planted/corrupt marker can
// never drive resume to delete or rename a file outside this DB's own set.
func resolveCanonicalRestore(dbPath string, sidecar string, mk *restoreMarker) (*canonicalRestore, error) {
	resolvedDB, err := resolveDBPathSurvivingDangling(dbPath)
	if err != nil {
		return nil, fmt.Errorf("restore resume: resolve db path: %w", err)
	}
	dbDir := filepath.Dir(resolvedDB)

	// CANONICAL-DERIVED PATHS ONLY (Round 7, Findings 1 & 2). Every path recovery
	// will read/hash/rename/remove is RECONSTRUCTED here from the canonical
	// resolved DB path + a recovery-safe TOKEN extracted from the marker. The
	// marker's raw path fields are honoured ONLY when they EXACTLY equal that
	// reconstruction; a token carrying a path separator, a ".." traversal, or any
	// byte outside the safe charset fails closed. A marker can therefore never
	// name a backup that is a symlink to another directory's DB, nor a staged path
	// that is an unrelated file — the reconstructed path is always a sibling of
	// this DB under names only a real restore of THIS DB would have produced.

	// Validate the staged path: <dbDir>/.restore.staged.<token>, safe token, and
	// the marker's spelling must equal that exact reconstruction.
	staged := ""
	if mk.StagedPath != "" {
		clean := filepath.Clean(mk.StagedPath)
		if filepath.Dir(clean) != dbDir {
			return nil, fmt.Errorf("%w: restore marker staged path outside db dir", ErrSnapshotSidecarCorrupt)
		}
		base := filepath.Base(clean)
		if !strings.HasPrefix(base, stagedInfix) {
			return nil, fmt.Errorf("%w: restore marker staged name not canonical", ErrSnapshotSidecarCorrupt)
		}
		token := strings.TrimPrefix(base, stagedInfix)
		if !tokenIsSafe(token) {
			return nil, fmt.Errorf("%w: restore marker staged token not in safe charset", ErrSnapshotSidecarCorrupt)
		}
		// Reconstruct from canonical dir + safe basename and require an exact match
		// with the marker's own spelling — no trust placed in the raw field beyond
		// equality with the reconstruction.
		want := filepath.Join(dbDir, stagedInfix+token)
		if clean != want {
			return nil, fmt.Errorf("%w: restore marker staged path is not the canonical reconstruction", ErrSnapshotSidecarCorrupt)
		}
		staged = want
	}

	// Validate the backup prefix: "<resolvedDB>.pre-restore.<token>", safe token,
	// reconstructed and required to match exactly. Rollback can then only rename a
	// moved-aside original back into the live names, never pull an arbitrary file
	// (least of all a symlink to a foreign DB) into the DB path.
	backup := ""
	if mk.BackupPrefix != "" {
		wantPrefix := resolvedDB + preRestoreInfix
		if !strings.HasPrefix(mk.BackupPrefix, wantPrefix) {
			return nil, fmt.Errorf("%w: restore marker backup prefix outside canonical set", ErrSnapshotSidecarCorrupt)
		}
		token := strings.TrimPrefix(mk.BackupPrefix, wantPrefix)
		if !tokenIsSafe(token) {
			return nil, fmt.Errorf("%w: restore marker backup token not in safe charset", ErrSnapshotSidecarCorrupt)
		}
		// Exact canonical reconstruction (the safe token excludes separators, so
		// this is already a sibling of the DB; the equality check is belt-and-braces).
		want := wantPrefix + token
		if mk.BackupPrefix != want {
			return nil, fmt.Errorf("%w: restore marker backup prefix is not the canonical reconstruction", ErrSnapshotSidecarCorrupt)
		}
		backup = want
	}

	// Constrain moved suffixes to the known triplet set.
	var moved []string
	for _, suffix := range mk.MovedSuffixes {
		switch suffix {
		case "", "-wal", "-shm":
			moved = append(moved, suffix)
		default:
			return nil, fmt.Errorf("%w: restore marker moved suffix %q outside triplet", ErrSnapshotSidecarCorrupt, suffix)
		}
	}

	return &canonicalRestore{
		resolvedDB:       resolvedDB,
		sidecar:          sidecar,
		staged:           staged,
		backup:           backup,
		moved:            moved,
		published:        mk.DBPublished,
		originalDBSHA256: mk.OriginalDBSHA256,
	}, nil
}

// restoreMarkerPending reports whether a restore marker is PRESENT in the DB's
// sidecar (a torn restore is pending) WITHOUT acting on it. It is the read-only
// probe the fail-closed Open path uses.
//
// Returns:
//   - (false, nil)            no sidecar / no marker → routine open may proceed
//   - (true, nil)             a marker is present (parseable or not)
//   - (false, ErrSnapshot...) the sidecar itself is unsafe (e.g. a symlink)
//
// IMPORTANT: a present-but-unparseable marker is reported as PENDING (true), not
// as an error — Open must fail closed on it, never erase it. The only errors
// returned here are sidecar-shape problems that already mean "do not touch".
func restoreMarkerPending(dbPath string) (bool, error) {
	resolvedDB, err := canonicalDBPath(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return false, nil
		}
		return false, err
	}
	if !snapshotEligiblePath(resolvedDB) {
		return false, nil // :memory:/URI — no sidecar, nothing pending
	}
	sidecar := resolvedDB + snapshotSidecarSuffix

	// A planted symlinked sidecar could redirect marker reads elsewhere; refuse
	// to follow it (fail closed) rather than report "nothing pending".
	if err := assertNotSymlink(sidecar); err != nil {
		return false, err
	}
	info, statErr := os.Lstat(sidecar)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return false, nil // no sidecar dir → no marker
		}
		return false, statErr
	}
	if !info.IsDir() {
		// A regular file where the sidecar dir should be is a BLOCKED-snapshot
		// sabotage case, NOT a pending restore — there is no marker dir to hold a
		// marker. The migration path fails closed on it separately; a routine open
		// of the DB itself is unaffected (no restore is in flight).
		return false, nil
	}
	// We deliberately do NOT decode here: a corrupt/partial marker (`{}`, bad
	// JSON, version 0) must still count as PENDING so Open fails closed and the
	// operator runs explicit recovery — never silently erased or treated as
	// recovered (Finding 4). Existence of the marker FILE is sufficient.
	if _, mErr := os.Lstat(restoreMarkerPathIn(sidecar)); mErr != nil {
		if os.IsNotExist(mErr) {
			return false, nil // no marker → routine open
		}
		return false, mErr
	}
	return true, nil
}

// detectRestoreInterrupted is the FAIL-CLOSED gate Open()/OpenNoMigrate() run
// BEFORE any sql.Open or file creation. If a restore marker is present (or the
// sidecar is unsafe) it returns ErrRestoreInterrupted — the DB is NEVER opened,
// fabricated, or touched. Recovery is the operator's explicit job; a routine
// `continuity profile` must not drive destructive file moves off a marker a
// crash, corruption, or an attacker could have written (Findings 1, 2, 4).
func detectRestoreInterrupted(dbPath string) error {
	pending, err := restoreMarkerPending(dbPath)
	if err != nil {
		// A sidecar-shape problem (e.g. symlinked sidecar) is itself a reason to
		// refuse to open: surface it as an interrupted-restore condition so the
		// operator runs recovery, which fails closed on the same problem.
		return fmt.Errorf("%w: %v", ErrRestoreInterrupted, err)
	}
	if pending {
		resolvedDB, _ := canonicalDBPath(dbPath)
		return fmt.Errorf(
			"%w for %s; run `continuity snapshot restore --confirm` to complete recovery",
			ErrRestoreInterrupted, resolvedDB)
	}
	return nil
}

// recoverPendingRestore drives a torn restore to a clean terminal state
// (COMPLETE or ROLL BACK) under FULL validation. It runs ONLY from the explicit
// `snapshot restore --confirm` path, and ONLY while the caller already holds the
// serve lock for this DB — recovery is never a side effect of a routine open.
//
// dbPath is the (possibly symlinked, possibly dangling) path the operator knows.
// The canonical resolved path + sidecar are RECOMPUTED here and are the sole
// authority for every path recovery acts on; the marker's path fields are only
// validated against that canonical set (resolveCanonicalRestore) AFTER the
// marker passes the schema gate (validateMarkerSchema). A corrupt/partial marker
// fails closed — it is never erased and no DB is fabricated.
//
// Returns nil when there was nothing to recover (no marker), so the caller can
// proceed to a fresh restore on a clean DB.
func recoverPendingRestore(dbPath string) error {
	resolvedDB, err := canonicalDBPath(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return nil
		}
		return err
	}
	if !snapshotEligiblePath(resolvedDB) {
		return nil
	}
	sidecar := resolvedDB + snapshotSidecarSuffix

	// The sidecar must not be a symlink: a planted symlinked sidecar could
	// redirect marker reads/removes elsewhere. Fail closed.
	if err := assertNotSymlink(sidecar); err != nil {
		return err
	}
	info, statErr := os.Lstat(sidecar)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return nil // no sidecar dir → no marker
		}
		return statErr
	}
	if !info.IsDir() {
		return nil // regular-file sidecar holds no marker → nothing to recover
	}
	mk, err := readRestoreMarker(sidecar)
	if err != nil {
		return err // unparseable marker → fail closed (never erased)
	}
	if mk == nil {
		return nil // no marker → nothing to recover
	}

	// Hard schema gate: version==1 and required fields present/well-formed. A
	// corrupt `{}`/partial marker stops here, preserved and un-acted-on.
	if err := validateMarkerSchema(mk); err != nil {
		return err
	}

	// Path-content gate: every path the marker names is constrained to the
	// canonical set (live triplet, staged in the DB dir, backup beneath the
	// resolved DB). Anything outside fails closed without being touched.
	cr, err := resolveCanonicalRestore(dbPath, sidecar, mk)
	if err != nil {
		return err
	}

	// REALITY GATE (Finding 1): never act on the marker's claimed phase until the
	// restore point itself is PROVEN. Load + validate the restore point (manifest
	// shape + snapshot.db sha256 + schema). If there is NO valid restore point we
	// FAIL CLOSED and touch nothing — a planted/stale marker beside an absent or
	// corrupt sidecar can no longer drive a destructive rename/remove.
	vm, verr := loadValidManifest(sidecar)
	if verr != nil {
		// ErrNoRestorePoint or corrupt → cannot prove anything → fail closed. The
		// marker is preserved; the operator must inspect.
		if errors.Is(verr, ErrNoRestorePoint) {
			return fmt.Errorf("%w: restore marker present but no valid restore point to reconcile against", ErrSnapshotSidecarCorrupt)
		}
		return verr
	}
	cr.snapshotSHA256 = vm.SnapshotSHA256

	return reconcilePendingRestore(cr)
}

// reconcilePendingRestore drives a torn restore to a clean terminal state by
// RECONCILING the marker's claimed phase against on-disk REALITY (Finding 1),
// never by trusting the marker's db_published bit. The restore point has already
// been proven valid (cr.snapshotSHA256 is its snapshot.db hash). This is the
// recovery path for an UNTRUSTED, possibly planted/stale marker — distinct from
// the in-process finishPendingRestore used by a live Restore that just moved its
// own files.
//
// Decision table (all paths under the serve + op lock, marker already
// schema/path-gated):
//
//   - live DB present AND hash == snapshot hash → treat as PUBLISHED: complete
//     (scrub stale -wal/-shm, drop staged), remove the marker. NEVER roll back —
//     a stale pre-publish marker cannot clobber the already-restored DB.
//   - live DB absent AND DB backup present AND staged present → genuine
//     pre-publish torn state → roll back, but ONLY after the DB backup's hash
//     matches cr.originalDBSHA256 (provenance). A mismatch (planted/stale/corrupt
//     backup) → FAIL CLOSED, do not rename it over the DB.
//   - anything else (inconsistent: live DB present but != snapshot and no torn
//     evidence, or absent DB with no usable backups/staged) → FAIL CLOSED, touch
//     nothing. The operator inspects; we never guess.
func reconcilePendingRestore(cr *canonicalRestore) error {
	db := cr.resolvedDB

	liveSum, livePresent, herr := hashIfPresent(db)
	if herr != nil {
		return fmt.Errorf("restore recover: hash live db: %w", herr)
	}

	// CASE A — the live DB already IS the restored snapshot image. Complete.
	if livePresent && liveSum == cr.snapshotSHA256 {
		return completeReconciled(cr)
	}

	dbBackupPresent := cr.backup != "" && lstatExists(cr.backup) // "" suffix backup
	stagedPresent := cr.staged != "" && lstatExists(cr.staged)

	// CASE A2 — SAFE PRE-RENAME ABORT (Finding 2, Round 5): the crash landed AFTER
	// the marker was written but BEFORE the first move-aside rename. In that window
	// the live DB is still the UNTOUCHED ORIGINAL (its hash == the marker's
	// recorded original_db_sha256), no DB backup was created yet, and a staged copy
	// may or may not exist. NO destructive step happened, so there is nothing to
	// roll back: clear the marker, drop any orphaned staged temp we can PROVE is
	// ours, and leave the (original) DB intact. Without this case reconcile fell
	// through to the generic fail-closed below and the DB stayed permanently
	// ErrRestoreInterrupted with no recovery path — exactly the wedge this fixes.
	if livePresent && cr.originalDBSHA256 != "" && liveSum == cr.originalDBSHA256 && !dbBackupPresent {
		if derr := removeProvenStaged(cr); derr != nil {
			return derr
		}
		fmt.Fprintf(os.Stderr, "  restore reconciled: no destructive step occurred (live db is the untouched original); cleared marker for %s\n", db)
		return removeRestoreMarker(cr.sidecar)
	}

	// CASE B — genuine pre-publish torn state: the live DB is gone, the original
	// was moved aside (DB backup present), and a staged image is waiting. Roll
	// back to the moved-aside original after proving its provenance.
	if !livePresent && dbBackupPresent && stagedPresent {
		// Provenance: the DB backup must hash to the recorded original. A planted
		// or corrupt <db>.pre-restore.* can never be renamed over the DB.
		if cr.originalDBSHA256 == "" {
			return fmt.Errorf("%w: rollback requested but marker recorded no original db hash to verify against", ErrSnapshotSidecarCorrupt)
		}
		backupSum, _, berr := hashIfPresent(cr.backup)
		if berr != nil {
			return fmt.Errorf("restore recover: hash db backup: %w", berr)
		}
		if backupSum != cr.originalDBSHA256 {
			return fmt.Errorf("%w: pre-restore db backup hash does not match the recorded original; refusing to roll it back over the live db", ErrSnapshotSidecarCorrupt)
		}
		return rollbackReconciled(cr)
	}

	// Anything else is inconsistent — fail closed, touch nothing.
	return fmt.Errorf("%w: restore marker does not match on-disk state (live present=%v, db backup present=%v, staged present=%v); refusing to guess",
		ErrSnapshotSidecarCorrupt, livePresent, dbBackupPresent, stagedPresent)
}

// completeReconciled finishes a proven-published restore: scrub stale live
// -wal/-shm (they belong to the OLD DB), drop any orphaned staged temp we can
// PROVE is ours, remove the marker. The live DB is the restored image and is
// never touched. Every -wal/-shm we are about to remove is first symlink-gated:
// a forged marker must never make recovery os.Remove a symlink at the live
// -wal/-shm position (Round 7, Findings 1 & 2).
func completeReconciled(cr *canonicalRestore) error {
	db := cr.resolvedDB
	for _, suffix := range []string{"-wal", "-shm"} {
		side := db + suffix
		if lstatExists(side) {
			if err := assertRecoverableFile(side); err != nil {
				return fmt.Errorf("restore recover: scrub %s: %w", side, err)
			}
			if rmErr := os.Remove(side); rmErr != nil && !os.IsNotExist(rmErr) {
				return fmt.Errorf("restore recover: scrub %s: %w", side, rmErr)
			}
		}
	}
	// The live DB we just declared restored must not be a symlink (Round 7).
	if err := assertLiveDBNotSymlink(db); err != nil {
		return err
	}
	if derr := removeProvenStaged(cr); derr != nil {
		return derr
	}
	fmt.Fprintf(os.Stderr, "  restore reconciled: live db is the restored snapshot; completed %s\n", db)
	return removeRestoreMarker(cr.sidecar)
}

// rollbackReconciled moves the (provenance-verified) moved-aside originals back
// into the live names and drops the staged copy. Called only after the DB
// backup's hash was proven to match the recorded original. Every path it
// touches — each live name it clears, each backup it renames in — is
// symlink-gated first: a forged marker must never make recovery remove or
// publish a symlink (Round 7, Findings 1 & 2).
func rollbackReconciled(cr *canonicalRestore) error {
	db := cr.resolvedDB
	for _, suffix := range cr.moved {
		live := db + suffix
		backup := cr.backup + suffix
		if !lstatExists(backup) {
			continue // nothing to roll back for this suffix
		}
		// The moved-aside original we are about to rename back over the live name
		// must be a real regular file, never a symlink (the "" suffix was already
		// provenance-hashed via hashIfPresent; -wal/-shm get their guard here).
		if err := assertRecoverableFile(backup); err != nil {
			return fmt.Errorf("restore rollback: backup %s: %w", backup, err)
		}
		if lstatExists(live) {
			// Whatever currently sits at the live name must be a real file we may
			// remove — never a symlink pointing elsewhere.
			if err := assertRecoverableFile(live); err != nil {
				return fmt.Errorf("restore rollback: live %s: %w", live, err)
			}
			if rmErr := os.Remove(live); rmErr != nil && !os.IsNotExist(rmErr) {
				return fmt.Errorf("restore rollback: clear %s: %w", live, rmErr)
			}
		}
		if err := os.Rename(backup, live); err != nil {
			return fmt.Errorf("restore rollback: restore %s: %w", live, err)
		}
	}
	// After publishing the rolled-back original, the live DB must not be a symlink.
	if err := assertLiveDBNotSymlink(db); err != nil {
		return err
	}
	if derr := removeProvenStaged(cr); derr != nil {
		return derr
	}
	fmt.Fprintf(os.Stderr, "  restore rolled back: interrupted restore of %s reverted to pre-restore state\n", db)
	return removeRestoreMarker(cr.sidecar)
}

// removeProvenStaged deletes the staged file ONLY if we can prove it is our
// staged copy: a present, non-symlink regular file whose content hash equals the
// snapshot.db hash (Round 7, Finding 2). A staged path that is absent is a no-op.
// A staged path that exists but is a symlink, is non-regular, cannot be hashed,
// or whose hash does NOT match the snapshot is LEFT IN PLACE (not deleted) — a
// stray temp is harmless, but deleting an unproven file (e.g. an unrelated
// `.restore.staged.keep.db` a forged marker named) is exactly what the bar
// forbids. Leaving it is the fail-safe choice; we surface a note but do not error
// so a benign stray temp never blocks recovery from completing.
func removeProvenStaged(cr *canonicalRestore) error {
	if cr.staged == "" {
		return nil
	}
	if !lstatExists(cr.staged) {
		return nil
	}
	if cr.snapshotSHA256 == "" {
		// No proven snapshot hash to verify against → cannot prove ownership → leave it.
		fmt.Fprintf(os.Stderr, "  restore recover: leaving unproven staged file in place (no snapshot hash to verify): %s\n", cr.staged)
		return nil
	}
	sum, _, herr := hashIfPresent(cr.staged)
	if herr != nil {
		// A symlink / non-regular / unreadable staged path: do NOT delete it.
		fmt.Fprintf(os.Stderr, "  restore recover: leaving staged file we cannot verify in place: %s (%v)\n", cr.staged, herr)
		return nil
	}
	if sum != cr.snapshotSHA256 {
		fmt.Fprintf(os.Stderr, "  restore recover: leaving staged file in place — content does not match the snapshot (not provably ours): %s\n", cr.staged)
		return nil
	}
	if rmErr := os.Remove(cr.staged); rmErr != nil && !os.IsNotExist(rmErr) {
		return fmt.Errorf("restore recover: remove proven staged file %s: %w", cr.staged, rmErr)
	}
	return nil
}

// assertLiveDBNotSymlink fails closed if the live DB path is a symlink (Round 7,
// Findings 1 & 2). Recovery must NEVER leave a symlink as the live DB: after any
// publish/rollback that lands a file at the DB path, the path must be a real
// regular file (or absent), never a redirection. A symlink here means something
// outside the canonical set was published — fail closed and touch nothing more.
func assertLiveDBNotSymlink(db string) error {
	info, err := os.Lstat(db)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: live db path %s is a symlink after recovery; refusing to leave a symlink as the database", ErrSnapshotSidecarCorrupt, db)
	}
	return nil
}

// lstatExists reports whether path exists (does not follow the final symlink).
func lstatExists(path string) bool {
	_, err := os.Lstat(path)
	return err == nil
}

// assertRecoverableFile fails closed unless path is a present, NON-symlink,
// regular file (Round 7, Findings 1 & 2). Every position recovery is about to
// read/hash/rename/remove (the live DB, the pre-restore backup, the staged file,
// the published DB) is gated through this so a symlink a forged marker planted is
// rejected — recovery touches nothing it cannot prove is a real regular file of
// its own. A missing file is NOT an error here (callers distinguish absence via
// lstatExists); only a present-but-non-regular/symlink entry fails closed.
func assertRecoverableFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
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

// hashIfPresent returns ("sha256:<hex>", true, nil) for a present regular file,
// ("", false, nil) when it does not exist, and a non-nil error for any other
// stat/read failure. Used by reconciliation to learn the ACTUAL on-disk state of
// the live DB and the moved-aside backup rather than trusting the marker.
//
// NO-SYMLINK (Round 7, Findings 1 & 2): a present entry is lstat-rejected if it
// is a symlink (or otherwise non-regular) and the bytes are hashed via
// hashFileNoFollow — NEVER the symlink-following hashFile. A forged marker that
// points the live-DB or backup position at a symlink to a foreign file can
// therefore never make recovery hash that foreign file and then act on the
// match; it fails closed instead.
func hashIfPresent(path string) (string, bool, error) {
	if !lstatExists(path) {
		return "", false, nil
	}
	if err := assertRecoverableFile(path); err != nil {
		return "", false, err
	}
	sum, _, err := hashFileNoFollow(path)
	if err != nil {
		return "", false, err
	}
	return sum, true, nil
}

// finishPendingRestore completes or rolls back a torn restore described by the
// CANONICAL view cr, then removes the marker. Every path operated on comes from
// cr (recomputed from the resolved DB + sidecar), never from raw marker fields.
// Guarantees that on return NO stale -wal/-shm remain beside the restored DB and
// the DB path holds a coherent database.
func finishPendingRestore(cr *canonicalRestore) error {
	db := cr.resolvedDB

	if cr.published {
		// The staged snapshot already became the live DB. Just finish: scrub any
		// stale live -wal/-shm (they belong to the OLD DB, not the restored one)
		// and drop the now-orphaned staged temp if it somehow remains. Symlink-gate
		// every -wal/-shm before removing it (Round 7, Findings 1 & 2).
		for _, suffix := range []string{"-wal", "-shm"} {
			side := db + suffix
			if lstatExists(side) {
				if err := assertRecoverableFile(side); err != nil {
					return fmt.Errorf("restore resume: scrub %s: %w", side, err)
				}
				if rmErr := os.Remove(side); rmErr != nil && !os.IsNotExist(rmErr) {
					return fmt.Errorf("restore resume: scrub %s: %w", side, rmErr)
				}
			}
		}
		if err := assertLiveDBNotSymlink(db); err != nil {
			return err
		}
		if derr := removeProvenStaged(cr); derr != nil {
			return derr
		}
		fmt.Fprintf(os.Stderr, "  restore resumed: completed interrupted restore of %s\n", db)
		return removeRestoreMarker(cr.sidecar)
	}

	// Not yet published: roll back to the moved-aside originals so the operator
	// is left exactly where they were before restore began.
	//
	// Anything currently at the live names (a partial/foreign file from the
	// crash) is removed first, then each moved-aside original is moved back. Every
	// position is symlink-gated before remove/rename (Round 7, Findings 1 & 2).
	for _, suffix := range cr.moved {
		live := db + suffix
		backup := cr.backup + suffix
		if cr.backup == "" {
			break // no backup prefix recorded — nothing to roll back
		}
		if !lstatExists(backup) {
			// Backup not present — nothing to roll back for this suffix.
			continue
		}
		if err := assertRecoverableFile(backup); err != nil {
			return fmt.Errorf("restore rollback: backup %s: %w", backup, err)
		}
		// Clear whatever currently occupies the live name (best-effort): if the
		// rename below would fail because a partial file sits there, remove it —
		// but never remove a symlink at that position.
		if lstatExists(live) {
			if err := assertRecoverableFile(live); err != nil {
				return fmt.Errorf("restore rollback: live %s: %w", live, err)
			}
			if rmErr := os.Remove(live); rmErr != nil && !os.IsNotExist(rmErr) {
				return fmt.Errorf("restore rollback: clear %s: %w", live, rmErr)
			}
		}
		if err := os.Rename(backup, live); err != nil {
			return fmt.Errorf("restore rollback: restore %s: %w", live, err)
		}
	}
	if err := assertLiveDBNotSymlink(db); err != nil {
		return err
	}
	// Drop the staged snapshot copy that never got published, but only if proven ours.
	if derr := removeProvenStaged(cr); derr != nil {
		return derr
	}
	fmt.Fprintf(os.Stderr, "  restore rolled back: interrupted restore of %s reverted to pre-restore state\n", db)
	return removeRestoreMarker(cr.sidecar)
}
