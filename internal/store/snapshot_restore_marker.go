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
// SECURITY: the marker is an on-disk file that an attacker (or a corrupt prior
// run) could plant with arbitrary path fields. Resume therefore TRUSTS NOTHING
// in the marker except the dbPublished phase bit. Every path it acts on is
// RECOMPUTED from the canonical resolved DB path + sidecar (see
// resumeRestoreIfPending), and any marker field that names a path OUTSIDE that
// canonical set makes resume fail closed rather than touch it. Because the
// originals are moved aside (never deleted) and all paths are canonical, even a
// flipped phase bit can only mis-sequence within the recoverable canonical set.
//
// This is deliberately a marker + resume/rollback, NOT a general journal.
// =========================================================================

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
	return nil
}

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
	published  bool     // the ONLY field trusted verbatim from the marker
}

// resolveDBPathSurvivingDangling returns the canonical DB path the way
// sidecarPath/resolveDBPath do, but it ALSO survives a DANGLING symlink: if the
// real DB was moved aside mid-restore through a symlinked CONTINUITY_DB, the
// link target no longer exists, EvalSymlinks fails, and a naive fallback to the
// link's own ".snapshot" would miss the marker written under the REAL DB's
// sidecar. We therefore read the link target with os.Readlink (which returns
// the target even when dangling) and clean/abs it.
func resolveDBPathSurvivingDangling(dbPath string) (string, error) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return "", err
	}
	// Existing, resolvable path: behave exactly like resolveDBPath.
	if _, statErr := os.Stat(abs); statErr == nil {
		if r, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
			return r, nil
		}
	}
	// Stat failed (or EvalSymlinks failed). If abs is itself a (possibly
	// dangling) symlink, follow it via Readlink so a crashed restore through a
	// symlink still resolves to the REAL DB's sidecar.
	if li, lerr := os.Lstat(abs); lerr == nil && li.Mode()&os.ModeSymlink != 0 {
		target, rlErr := os.Readlink(abs)
		if rlErr == nil {
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(abs), target)
			}
			return resolveViaParentDir(target), nil
		}
	}
	// The DB itself is gone (mid-restore the real file was moved aside) and abs
	// is not a symlink. Resolving the PARENT dir still canonicalizes platform
	// symlinks (e.g. macOS /var → /private/var) so the recomputed sidecar and
	// backup names match what production wrote with the DB present.
	return resolveViaParentDir(abs), nil
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

	// Validate the staged path: it must be a plain file inside the DB dir whose
	// name carries the staged infix. Recompute it from the canonical dir + the
	// marker's BASENAME only (never trust the directory component).
	staged := ""
	if mk.StagedPath != "" {
		// The marker's directory component must equal the canonical DB dir; only
		// the basename is otherwise honoured, and only if it carries the staged
		// infix. Anything else (a staged path in another directory, a traversal,
		// a non-staged name) fails closed rather than being removed.
		if filepath.Dir(filepath.Clean(mk.StagedPath)) != dbDir {
			return nil, fmt.Errorf("%w: restore marker staged path outside db dir", ErrSnapshotSidecarCorrupt)
		}
		base := filepath.Base(mk.StagedPath)
		if !strings.HasPrefix(base, stagedInfix) || base != filepath.Clean(base) {
			return nil, fmt.Errorf("%w: restore marker staged name not canonical", ErrSnapshotSidecarCorrupt)
		}
		staged = filepath.Join(dbDir, base)
	}

	// Validate the backup prefix: it must begin with "<resolvedDB>.pre-restore."
	// so rollback can only rename moved-aside originals back into the live names,
	// never pull an arbitrary file into the DB path.
	backup := ""
	if mk.BackupPrefix != "" {
		wantPrefix := resolvedDB + preRestoreInfix
		if !strings.HasPrefix(mk.BackupPrefix, wantPrefix) {
			return nil, fmt.Errorf("%w: restore marker backup prefix outside canonical set", ErrSnapshotSidecarCorrupt)
		}
		// Defense in depth: no separators may appear after the canonical prefix
		// (a backup name is a sibling of the DB, never in a subdirectory).
		tail := strings.TrimPrefix(mk.BackupPrefix, wantPrefix)
		if strings.ContainsRune(tail, os.PathSeparator) {
			return nil, fmt.Errorf("%w: restore marker backup prefix escapes db dir", ErrSnapshotSidecarCorrupt)
		}
		backup = mk.BackupPrefix
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
		resolvedDB: resolvedDB,
		sidecar:    sidecar,
		staged:     staged,
		backup:     backup,
		moved:      moved,
		published:  mk.DBPublished,
	}, nil
}

// resumeRestoreIfPending detects an in-progress restore marker in the DB's
// sidecar and drives it to a clean terminal state (COMPLETE or ROLL BACK)
// before normal operation continues. It is invoked from Open() (so a crashed
// restore self-heals on the next boot) and from Restore() (so a fresh restore
// never starts on top of a torn one).
//
// dbPath is the (possibly symlinked, possibly dangling) path the caller knows.
// The canonical resolved path + sidecar are RECOMPUTED here and are the sole
// authority for every path resume acts on; the marker's path fields are only
// validated against that canonical set, never trusted as targets.
func resumeRestoreIfPending(dbPath string) error {
	// Resolve the canonical DB path FIRST (surviving a dangling symlink), then
	// derive the sidecar from it — never from a marker field. This is what makes
	// recovery find the marker under the REAL DB's sidecar even when the live DB
	// was moved aside through a symlink mid-restore.
	resolvedDB, err := resolveDBPathSurvivingDangling(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return nil
		}
		return err
	}
	if !snapshotEligiblePath(resolvedDB) {
		return nil // :memory:/URI — no sidecar, nothing to resume
	}
	sidecar := resolvedDB + snapshotSidecarSuffix

	// The sidecar on the RESUME path must not be a symlink: a planted symlinked
	// sidecar could redirect marker reads/removes elsewhere. Fail closed.
	if err := assertNotSymlink(sidecar); err != nil {
		return err
	}
	if _, statErr := os.Lstat(sidecar); statErr != nil {
		return nil // no sidecar dir → no marker
	}
	mk, err := readRestoreMarker(sidecar)
	if err != nil {
		return err
	}
	if mk == nil {
		return nil // routine open: no pending marker → behavior unchanged
	}

	// A marker is pending. Resume is mutually exclusive with an active restore or
	// serve: if ANOTHER live process holds the serve lock, it is actively
	// restoring/serving this DB — do NOT interfere; skip resume and let normal
	// open proceed (the holder owns recovery). Only when there is no live holder
	// (a stale/dead-PID lock = the crashed restorer, or no lock at all) do we
	// recover, holding the serve lock for the duration so nothing races in.
	owner, alive, lerr := serveLockOwnerFor(resolvedDB)
	if lerr != nil {
		return lerr
	}
	myPID := os.Getpid()
	if alive && owner != myPID {
		// Another live process owns the DB — leave the torn restore to it.
		return nil
	}

	// We either already hold the serve lock (owner == us, e.g. serve called Open
	// after acquiring) or no live holder exists. Acquire it for the resume
	// window unless we already hold it; release only what we acquired.
	release := func() {}
	if owner != myPID {
		rel, aerr := AcquireServeLock(resolvedDB)
		if aerr != nil {
			// Lost a race to another process that just acquired — skip resume; the
			// new holder will recover. Do not fail the Open.
			if errors.Is(aerr, ErrServeLockHeld) {
				return nil
			}
			return aerr
		}
		release = rel
	}
	defer release()

	// Re-read the marker now that we hold the lock: a concurrent recoverer may
	// have already cleared it between our first read and acquiring the lock.
	mk, err = readRestoreMarker(sidecar)
	if err != nil {
		return err
	}
	if mk == nil {
		return nil
	}

	cr, err := resolveCanonicalRestore(dbPath, sidecar, mk)
	if err != nil {
		return err
	}
	return finishPendingRestore(cr)
}

// serveLockOwnerFor returns (ownerPID, alive, err) for the serve lock derived
// from a resolved DB path. A missing lock is (0, false, nil). Used by resume to
// gate on whether a LIVE process (other than us) is actively holding the DB.
func serveLockOwnerFor(resolvedDB string) (int, bool, error) {
	path, err := serveLockPath(resolvedDB)
	if err != nil {
		return 0, false, err
	}
	return readServeLockOwner(path)
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
		// and drop the now-orphaned staged temp if it somehow remains.
		for _, suffix := range []string{"-wal", "-shm"} {
			if _, err := os.Lstat(db + suffix); err == nil {
				if rmErr := os.Remove(db + suffix); rmErr != nil && !os.IsNotExist(rmErr) {
					return fmt.Errorf("restore resume: scrub %s%s: %w", db, suffix, rmErr)
				}
			}
		}
		if cr.staged != "" {
			_ = os.Remove(cr.staged)
		}
		fmt.Fprintf(os.Stderr, "  restore resumed: completed interrupted restore of %s\n", db)
		return removeRestoreMarker(cr.sidecar)
	}

	// Not yet published: roll back to the moved-aside originals so the operator
	// is left exactly where they were before restore began.
	//
	// Anything currently at the live names (a partial/foreign file from the
	// crash) is removed first, then each moved-aside original is moved back.
	for _, suffix := range cr.moved {
		live := db + suffix
		backup := cr.backup + suffix
		if cr.backup == "" {
			break // no backup prefix recorded — nothing to roll back
		}
		if _, err := os.Lstat(backup); err != nil {
			// Backup not present — nothing to roll back for this suffix.
			continue
		}
		// Clear whatever currently occupies the live name (best-effort): if the
		// rename below would fail because a partial file sits there, remove it.
		if _, err := os.Lstat(live); err == nil {
			if rmErr := os.Remove(live); rmErr != nil && !os.IsNotExist(rmErr) {
				return fmt.Errorf("restore rollback: clear %s: %w", live, rmErr)
			}
		}
		if err := os.Rename(backup, live); err != nil {
			return fmt.Errorf("restore rollback: restore %s: %w", live, err)
		}
	}
	// Drop the staged snapshot copy that never got published.
	if cr.staged != "" {
		_ = os.Remove(cr.staged)
	}
	fmt.Fprintf(os.Stderr, "  restore rolled back: interrupted restore of %s reverted to pre-restore state\n", db)
	return removeRestoreMarker(cr.sidecar)
}
