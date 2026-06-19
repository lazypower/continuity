package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// =========================================================================
// Boot retention + expiry, restore, prune, status.
//
// All entry points here derive the sidecar purely from a DB path and (except
// restore, which must verify lineage) NEVER open the application DB. Nothing
// outside the derived sidecar is written or removed, and nothing is removed
// unless it has first been proven to be ours.
// =========================================================================

// RecordSuccessfulBoot is called by `serve` AFTER a successful TCP bind. It
// increments successful_boots on a valid active manifest whose
// target_schema_version <= currentSchemaVersion. When the count reaches the
// manifest's expiry threshold, the validated snapshot.db + manifest.json are
// removed (and only those two files).
//
// Best-effort: any error is returned for logging but must not crash serve. A
// missing/ineligible/absent restore point is not an error (returns nil).
func RecordSuccessfulBoot(dbPath string, currentSchemaVersion int) error {
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return nil // :memory:/URI — no restore point to tick
		}
		return err
	}

	m, err := loadValidManifest(sidecar)
	if err != nil {
		if errors.Is(err, ErrNoRestorePoint) {
			return nil
		}
		// Corrupt/partial sidecar: do NOT touch it, just report.
		return err
	}

	// Only tick a restore point whose upgrade has actually completed; if the
	// DB is still below target, this boot does not prove the upgrade good.
	if m.TargetSchemaVersion > currentSchemaVersion {
		return nil
	}

	m.SuccessfulBoots++
	now := time.Now().UTC().Format(time.RFC3339)
	m.LastSuccessfulBootAt = &now

	if m.SuccessfulBoots >= m.ExpiresAfterSuccessfulBoots {
		return expireRestorePoint(sidecar, m)
	}
	return writeManifestAtomic(sidecar, m)
}

// expireRestorePoint deletes ONLY the validated snapshot.db and manifest.json.
// The manifest m passed in has already been validated by loadValidManifest, so
// both files are proven ours. The sidecar directory itself is removed only if
// it is then empty (we never rmdir a dir that still holds unproven files).
func expireRestorePoint(sidecar string, m *Manifest) error {
	// Re-validate the snapshot one more time immediately before deletion so a
	// race that corrupted it since load leaves it untouched.
	snapPath := snapshotDBPathIn(sidecar)
	if err := assertRegularFile(snapPath); err != nil {
		return err
	}
	if err := verifySnapshotHash(snapPath, m); err != nil {
		return err
	}

	if err := os.Remove(manifestPathIn(sidecar)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("snapshot: remove manifest on expiry: %w", err)
	}
	if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("snapshot: remove snapshot on expiry: %w", err)
	}
	// Remove the sidecar dir only if empty — leave anything we did not prove.
	if entries, err := os.ReadDir(sidecar); err == nil && len(entries) == 0 {
		_ = os.Remove(sidecar)
	}
	fmt.Fprintf(os.Stderr,
		"  restore point expired after %d successful boots → removed %s\n",
		m.SuccessfulBoots, sidecar)
	return nil
}

// SnapshotStatus is the read-only view returned by `snapshot status`. It never
// opens the DB.
type SnapshotStatus struct {
	Present  bool
	Sidecar  string
	Manifest *Manifest
	// Problem is set when a sidecar is present but unprovable; the CLI shows
	// it and exits non-zero so an operator notices a fail-closed state.
	Problem string
}

// Status derives the sidecar from dbPath and reports the restore point state
// WITHOUT opening the DB. Ineligible paths report "not present".
func Status(dbPath string) (*SnapshotStatus, error) {
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return &SnapshotStatus{Present: false}, nil
		}
		return nil, err
	}
	st := &SnapshotStatus{Sidecar: sidecar}

	if _, statErr := os.Lstat(sidecar); statErr != nil {
		if os.IsNotExist(statErr) {
			return st, nil // not present
		}
		return nil, statErr
	}

	m, lerr := loadValidManifest(sidecar)
	if lerr != nil {
		if errors.Is(lerr, ErrNoRestorePoint) {
			return st, nil
		}
		// Present but corrupt — surface, do not touch.
		st.Present = true
		st.Problem = lerr.Error()
		return st, nil
	}
	st.Present = true
	st.Manifest = m
	return st, nil
}

// Prune removes a VALID restore point's snapshot.db + manifest.json. It
// refuses (fails closed) on a corrupt/partial sidecar — the CLI never deletes
// anything it cannot prove is ours. Never opens the DB.
func Prune(dbPath string) error {
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		return err
	}
	m, err := loadValidManifest(sidecar)
	if err != nil {
		if errors.Is(err, ErrNoRestorePoint) {
			return ErrNoRestorePoint
		}
		return err // corrupt → refuse
	}
	return expireRestorePoint(sidecar, m)
}

// =========================================================================
// Serve lockfile — lets restore refuse while a serve holds the DB.
// =========================================================================

// serveLockPath is the lockfile a running `serve` holds next to the DB.
func serveLockPath(dbPath string) (string, error) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return "", err
	}
	resolved := abs
	if _, statErr := os.Stat(abs); statErr == nil {
		if r, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
			resolved = r
		}
	}
	return resolved + ".serve.lock", nil
}

// ErrServeLockHeld is returned by AcquireServeLock when a LIVE serve already
// holds the lock for this DB. serve must refuse to start in that case so a
// second server cannot run against (and have the DB swapped under) the first.
var ErrServeLockHeld = errors.New("serve: another continuity serve is already running against this DB")

// AcquireServeLock takes an EXCLUSIVE advisory lock recording the serve PID.
//
// Semantics (O_CREATE|O_EXCL with stale reclaim):
//   - If no lock exists, create it atomically (O_EXCL) and own it.
//   - If a lock exists and its recorded PID is ALIVE, refuse with
//     ErrServeLockHeld — a live serve owns the DB.
//   - If a lock exists but its PID is dead (or the lock is unparseable in a way
//     that proves it stale), reclaim it: remove and recreate with our PID.
//
// The returned release func removes the lock ONLY if we still own it (our PID
// is recorded). This prevents a second serve that clobbered the first's lock
// from later deleting it on exit and leaving the first server unprotected.
func AcquireServeLock(dbPath string) (func(), error) {
	path, err := serveLockPath(dbPath)
	if err != nil {
		return func() {}, err
	}
	myPID := os.Getpid()

	for attempt := 0; attempt < 2; attempt++ {
		// Atomic create-if-absent: only one process wins the O_EXCL race.
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			_, werr := f.WriteString(fmt.Sprintf("%d\n", myPID))
			cerr := f.Close()
			if werr != nil {
				_ = os.Remove(path)
				return func() {}, werr
			}
			if cerr != nil {
				_ = os.Remove(path)
				return func() {}, cerr
			}
			return makeServeLockReleaser(path, myPID), nil
		}
		if !os.IsExist(err) {
			return func() {}, err
		}

		// A lock already exists. Decide: live (refuse) or stale (reclaim).
		owner, alive, perr := readServeLockOwner(path)
		if perr != nil {
			return func() {}, perr
		}
		if owner == myPID {
			// We already hold it (re-acquire in the same process).
			return makeServeLockReleaser(path, myPID), nil
		}
		if alive {
			return func() {}, ErrServeLockHeld
		}
		// Stale lock (dead PID): reclaim by removing, then retry the O_EXCL
		// create. If another process reclaims first, the retry's create fails
		// and we re-evaluate liveness (and may refuse).
		if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
			return func() {}, rmErr
		}
	}
	// Lost the reclaim race twice — treat as held to fail closed.
	return func() {}, ErrServeLockHeld
}

// makeServeLockReleaser returns a release func that removes the lockfile ONLY
// while it still records ownerPID. If another process has since reclaimed the
// lock (recorded a different PID), we leave it alone — never delete a lock we
// no longer own.
func makeServeLockReleaser(path string, ownerPID int) func() {
	return func() {
		owner, _, err := readServeLockOwner(path)
		if err != nil {
			return
		}
		if owner == ownerPID {
			_ = os.Remove(path)
		}
	}
}

// readServeLockOwner reads the lockfile and returns (pid, alive, err). A
// missing lock returns (0, false, nil). An unparseable lock is reported as
// (0, true, nil) — treated as a live lock so we fail closed rather than reclaim
// something we cannot understand.
func readServeLockOwner(path string) (pid int, alive bool, err error) {
	data, rerr := os.ReadFile(path)
	if rerr != nil {
		if os.IsNotExist(rerr) {
			return 0, false, nil
		}
		return 0, false, rerr
	}
	if _, serr := fmt.Sscanf(string(data), "%d", &pid); serr != nil || pid <= 0 {
		return 0, true, nil // unparseable → treat as held (fail closed)
	}
	return pid, processAlive(pid), nil
}

// serveLockHeld reports whether a live serve process holds dbPath's lock. A
// lock whose PID no longer exists is treated as stale (not held).
func serveLockHeld(dbPath string) (bool, error) {
	path, err := serveLockPath(dbPath)
	if err != nil {
		return false, err
	}
	_, alive, err := readServeLockOwner(path)
	if err != nil {
		return false, err
	}
	return alive, nil
}

// =========================================================================
// Restore
// =========================================================================

// Restore replaces the live DB with the sidecar snapshot, after validating
// manifest + hash + integrity + lineage and confirming no live serve holds the
// DB. The previous db / db-wal / db-shm triplet is renamed ASIDE to unique
// pre-restore names (never deleted, never overwritten).
//
// Crash-safety: a minimal restore marker is written into the sidecar BEFORE the
// first destructive rename, recording the resolved DB path, staged snapshot
// path, and the chosen pre-restore backup names. A crash at any point leaves a
// state the next Open/Restore can drive to a clean terminal state (COMPLETE the
// renames, or ROLL BACK to the moved-aside originals) with no stale -wal/-shm
// left beside the restored DB. The marker is removed only after success.
//
// Returns the directory-prefix of the moved-aside files so the CLI can report
// where the operator can find the prior DB.
func Restore(dbPath string) (movedAsidePrefix string, err error) {
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		return "", err
	}

	// Drive any torn restore from a previous crash to a clean state first; a
	// fresh restore must never start on top of an in-progress one.
	if rerr := resumeRestoreIfPending(dbPath); rerr != nil {
		return "", fmt.Errorf("restore: resume prior restore: %w", rerr)
	}

	m, err := loadValidManifest(sidecar)
	if err != nil {
		return "", err // ErrNoRestorePoint or corrupt — both refuse
	}

	// Refuse if a live serve holds the DB.
	if held, herr := serveLockHeld(dbPath); herr != nil {
		return "", fmt.Errorf("restore: check serve lock: %w", herr)
	} else if held {
		return "", errors.New("restore: a continuity serve appears to be running against this DB; stop it first")
	}

	snapPath := snapshotDBPathIn(sidecar)

	// Integrity-check the snapshot image before trusting it.
	if err := integrityCheck(snapPath); err != nil {
		return "", err
	}

	// Resolve the CANONICAL DB path once and use it for every staging,
	// backup-aside, rename-into-place, and WAL/SHM cleanup operation below.
	// With a symlinked CONTINUITY_DB, operating on the raw path would rename
	// the SYMLINK, not the real DB; resolving here renames the real file.
	resolvedDB, rerr := resolveDBPath(dbPath)
	if rerr != nil {
		return "", fmt.Errorf("restore: resolve db path: %w", rerr)
	}

	// Open the CURRENT DB without migrating and recompute the lineage
	// fingerprint over rows <= pre_schema_version. A MISSING live DB fails
	// closed here (OpenNoMigrate returns ErrDBMissing) — restore never
	// fabricates a DB. Refuse on lineage mismatch — this blocks a sidecar
	// transplanted next to an unrelated DB.
	cur, err := OpenNoMigrate(resolvedDB)
	if err != nil {
		return "", fmt.Errorf("restore: open current db: %w", err)
	}
	curVersion, verr := cur.SchemaVersion()
	if verr != nil {
		cur.Close()
		return "", fmt.Errorf("restore: read current schema version: %w", verr)
	}
	curFingerprint, ferr := lineageFingerprint(cur, m.PreSchemaVersion)
	cur.Close()
	if ferr != nil {
		return "", fmt.Errorf("restore: recompute lineage: %w", ferr)
	}
	if curFingerprint != m.LineageFingerprint {
		return "", errors.New("restore: lineage fingerprint mismatch; this restore point does not belong to this DB")
	}

	// Require pre <= current <= target.
	if curVersion < m.PreSchemaVersion || curVersion > m.TargetSchemaVersion {
		return "", fmt.Errorf(
			"restore: current schema v%d outside restore window [v%d, v%d]",
			curVersion, m.PreSchemaVersion, m.TargetSchemaVersion)
	}

	dbDir := filepath.Dir(resolvedDB)

	// Stage the snapshot to a temp file in the DB dir, then verify its hash
	// at the staged location before any destructive move.
	staged := filepath.Join(dbDir, fmt.Sprintf(".restore.staged.%d.db", os.Getpid()))
	_ = os.Remove(staged)
	if err := copyFile(snapPath, staged); err != nil {
		_ = os.Remove(staged)
		return "", fmt.Errorf("restore: stage snapshot: %w", err)
	}
	if err := verifySnapshotHash(staged, m); err != nil {
		_ = os.Remove(staged)
		return "", err
	}

	// Choose a UNIQUE pre-restore backup prefix that does not already exist for
	// any suffix. Second-resolution timestamps collide and os.Rename overwrites
	// on unix, which would clobber an earlier moved-aside DB; uniquify so we
	// never overwrite prior crash material.
	movedAsidePrefix, err = uniquePreRestorePrefix(resolvedDB)
	if err != nil {
		_ = os.Remove(staged)
		return "", err
	}

	// Determine which of the triplet are actually present so the marker records
	// exactly what we move (recovery moves back exactly these).
	var movedSuffixes []string
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if _, statErr := os.Lstat(resolvedDB + suffix); statErr == nil {
			movedSuffixes = append(movedSuffixes, suffix)
		}
	}

	// Write the restore marker BEFORE the first destructive rename. From here a
	// crash is recoverable from the sidecar marker.
	mk := &restoreMarker{
		Version:        1,
		RestoredDBPath: resolvedDB,
		StagedPath:     staged,
		BackupPrefix:   movedAsidePrefix,
		MovedSuffixes:  movedSuffixes,
		DBPublished:    false,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		_ = os.Remove(staged)
		return "", err
	}

	// Move the current triplet aside to the unique pre-restore names. We do NOT
	// delete them — they are crash material and an operator escape hatch.
	for _, suffix := range movedSuffixes {
		src := resolvedDB + suffix
		dst := movedAsidePrefix + suffix
		if err := os.Rename(src, dst); err != nil {
			// Roll back whatever we already moved, then clear the marker.
			_ = finishPendingRestore(sidecar, mk)
			_ = os.Remove(staged)
			return "", fmt.Errorf("restore: move %s aside: %w", src, err)
		}
	}

	// Rename the staged snapshot into the live DB path (atomic on same dir).
	if err := os.Rename(staged, resolvedDB); err != nil {
		// DB not yet published — roll back to the moved-aside originals.
		_ = finishPendingRestore(sidecar, mk)
		_ = os.Remove(staged)
		return "", fmt.Errorf("restore: publish restored db: %w", err)
	}
	_ = os.Chmod(resolvedDB, 0o600)

	// Mark the DB as published so a crash from here COMPLETES (never rolls back
	// over the freshly-restored DB).
	mk.DBPublished = true
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		// We could not record the published state. Scrub stale wal/shm and
		// surface the error; the DB is restored but the marker is stale, which
		// the next Open will resolve as a COMPLETE only if it reads published.
		fmt.Fprintf(os.Stderr, "warning: restore published but marker update failed: %v\n", err)
	}

	// Ensure no stale -wal/-shm remain at the LIVE names. They were moved aside
	// above, but a crash could have left a fresh one; remove any that match the
	// live names so the restored DB is not paired with a foreign WAL.
	for _, suffix := range []string{"-wal", "-shm"} {
		live := resolvedDB + suffix
		if _, statErr := os.Lstat(live); statErr == nil {
			_ = os.Remove(live)
		}
	}

	// Restore complete — clear the marker.
	if err := removeRestoreMarker(sidecar); err != nil {
		fmt.Fprintf(os.Stderr, "warning: restore succeeded but marker cleanup failed: %v\n", err)
	}

	// Record the restore in the manifest (best-effort).
	m.RestoreCount++
	nowStr := time.Now().UTC().Format(time.RFC3339)
	m.LastRestoredAt = &nowStr
	if werr := writeManifestAtomic(sidecar, m); werr != nil {
		fmt.Fprintf(os.Stderr, "warning: restore succeeded but manifest update failed: %v\n", werr)
	}

	return movedAsidePrefix, nil
}

// resolveDBPath returns the canonical (Abs + EvalSymlinks) path for an existing
// DB, matching the derivation sidecarPath uses. A missing file resolves to its
// absolute form (EvalSymlinks would error). This is the single resolved path
// every destructive restore operation works against.
func resolveDBPath(dbPath string) (string, error) {
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return "", err
	}
	if _, statErr := os.Stat(abs); statErr == nil {
		if r, evalErr := filepath.EvalSymlinks(abs); evalErr == nil {
			return r, nil
		}
	}
	return abs, nil
}

// uniquePreRestorePrefix returns a "<resolvedDB>.pre-restore.<ts>.<pid>[.<n>]"
// prefix for which NONE of the {"","-wal","-shm"} backup names already exist.
// This guarantees we never overwrite an earlier moved-aside DB: second-grained
// timestamps collide and os.Rename overwrites on unix.
func uniquePreRestorePrefix(resolvedDB string) (string, error) {
	stamp := time.Now().UTC().Format("20060102T150405Z")
	base := fmt.Sprintf("%s.pre-restore.%s.%d", resolvedDB, stamp, os.Getpid())
	for n := 0; n < 1000; n++ {
		prefix := base
		if n > 0 {
			prefix = fmt.Sprintf("%s.%d", base, n)
		}
		if prefixFree(prefix) {
			return prefix, nil
		}
	}
	return "", errors.New("restore: could not find a free pre-restore backup name")
}

// prefixFree reports whether none of prefix{,"-wal","-shm"} currently exist.
func prefixFree(prefix string) bool {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if _, err := os.Lstat(prefix + suffix); err == nil {
			return false
		}
	}
	return true
}

// copyFile copies src to dst (dst created 0600, truncated). Used to stage the
// snapshot into the DB dir so the final move is a same-filesystem rename.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
