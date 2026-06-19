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

// AcquireServeLock writes a lockfile recording the serve PID. It is advisory:
// restore checks for its presence (and liveness) and refuses while a serve
// owns the DB. Returns a release func. Best-effort; a failure to write the
// lock does not block serve (returns a no-op release + the error for logging).
func AcquireServeLock(dbPath string) (func(), error) {
	path, err := serveLockPath(dbPath)
	if err != nil {
		return func() {}, err
	}
	// Overwrite any stale lock; restore checks PID liveness, not exclusivity.
	if err := os.WriteFile(path, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o600); err != nil {
		return func() {}, err
	}
	return func() { _ = os.Remove(path) }, nil
}

// serveLockHeld reports whether a live serve process holds dbPath's lock. A
// lock whose PID no longer exists is treated as stale (not held).
func serveLockHeld(dbPath string) (bool, error) {
	path, err := serveLockPath(dbPath)
	if err != nil {
		return false, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	pid := 0
	if _, err := fmt.Sscanf(string(data), "%d", &pid); err != nil || pid <= 0 {
		// Unparseable lock: treat as held to fail closed.
		return true, nil
	}
	return processAlive(pid), nil
}

// =========================================================================
// Restore
// =========================================================================

// Restore replaces the live DB at dbPath with the sidecar snapshot, after
// validating manifest + hash + integrity + lineage and confirming no live
// serve holds the DB. The previous db / db-wal / db-shm triplet is renamed
// ASIDE to timestamped pre-restore names (never deleted). Crash-safety relies
// on atomic rename ordering only — there is no restore journal in v1.
//
// Returns the directory-prefix of the moved-aside files so the CLI can report
// where the operator can find the prior DB.
func Restore(dbPath string) (movedAsidePrefix string, err error) {
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		return "", err
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

	// Open the CURRENT DB without migrating and recompute the lineage
	// fingerprint over rows <= pre_schema_version. Refuse on mismatch — this
	// is what blocks a sidecar transplanted next to an unrelated DB.
	cur, err := OpenNoMigrate(dbPath)
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

	dbDir := filepath.Dir(dbPath)

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

	// Move the current triplet aside to timestamped pre-restore names. We do
	// NOT delete them — they are crash material and an operator escape hatch.
	stamp := time.Now().UTC().Format("20060102T150405Z")
	movedAsidePrefix = fmt.Sprintf("%s.pre-restore.%s", dbPath, stamp)
	for _, suffix := range []string{"", "-wal", "-shm"} {
		src := dbPath + suffix
		if _, statErr := os.Lstat(src); statErr != nil {
			continue // not present (e.g. no -wal/-shm) — skip
		}
		dst := movedAsidePrefix + suffix
		if err := os.Rename(src, dst); err != nil {
			_ = os.Remove(staged)
			return "", fmt.Errorf("restore: move %s aside: %w", src, err)
		}
	}

	// Rename the staged snapshot into the live DB path (atomic on same dir).
	if err := os.Rename(staged, dbPath); err != nil {
		_ = os.Remove(staged)
		return "", fmt.Errorf("restore: publish restored db: %w", err)
	}
	_ = os.Chmod(dbPath, 0o600)

	// Defensive: ensure no stale -wal/-shm remain at the LIVE names. They
	// were moved aside above, but a crash could have left a fresh one; remove
	// any that match the live names so the restored DB is not paired with a
	// foreign WAL.
	for _, suffix := range []string{"-wal", "-shm"} {
		live := dbPath + suffix
		if _, statErr := os.Lstat(live); statErr == nil {
			_ = os.Remove(live)
		}
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
