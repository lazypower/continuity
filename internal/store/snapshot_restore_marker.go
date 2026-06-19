package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
// This is deliberately a marker + resume/rollback, NOT a general journal.
// =========================================================================

// restoreMarkerName is the marker file inside the sidecar. The ".json" suffix
// is irrelevant to ownership — the sidecar is path-owned — but keeps it
// inspectable.
const restoreMarkerName = "restore.in-progress.json"

// restoreMarker is the on-disk recovery record. All paths are absolute/resolved
// so recovery does not depend on the process CWD.
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
	tmp := filepath.Join(sidecar, fmt.Sprintf("restore.marker.tmp.%d", os.Getpid()))
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
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

// resumeRestoreIfPending detects an in-progress restore marker in the DB's
// sidecar and drives it to a clean terminal state (COMPLETE or ROLL BACK)
// before normal operation continues. It is invoked from Open() (so a crashed
// restore self-heals on the next boot) and from Restore() (so a fresh restore
// never starts on top of a torn one).
//
// dbPath is the (possibly symlinked) path the caller knows; the marker carries
// the canonical resolved path it actually operated on, which is authoritative.
func resumeRestoreIfPending(dbPath string) error {
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return nil // :memory:/URI — no sidecar, nothing to resume
		}
		return err
	}
	// A sidecar that is a symlink/regular-file is handled by the normal
	// fail-closed paths; here we only care whether a marker is readable.
	if _, statErr := os.Lstat(sidecar); statErr != nil {
		return nil // no sidecar dir → no marker
	}
	mk, err := readRestoreMarker(sidecar)
	if err != nil {
		return err
	}
	if mk == nil {
		return nil
	}
	return finishPendingRestore(sidecar, mk)
}

// finishPendingRestore completes or rolls back a torn restore described by mk,
// then removes the marker. Guarantees that on return NO stale -wal/-shm remain
// beside the restored DB and the DB path holds a coherent database.
func finishPendingRestore(sidecar string, mk *restoreMarker) error {
	db := mk.RestoredDBPath

	if mk.DBPublished {
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
		if mk.StagedPath != "" {
			_ = os.Remove(mk.StagedPath)
		}
		fmt.Fprintf(os.Stderr, "  restore resumed: completed interrupted restore of %s\n", db)
		return removeRestoreMarker(sidecar)
	}

	// Not yet published: roll back to the moved-aside originals so the operator
	// is left exactly where they were before restore began.
	//
	// Anything currently at the live names (a partial/foreign file from the
	// crash) is removed first, then each moved-aside original is moved back.
	for _, suffix := range mk.MovedSuffixes {
		live := db + suffix
		backup := mk.BackupPrefix + suffix
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
	if mk.StagedPath != "" {
		_ = os.Remove(mk.StagedPath)
	}
	fmt.Fprintf(os.Stderr, "  restore rolled back: interrupted restore of %s reverted to pre-restore state\n", db)
	return removeRestoreMarker(sidecar)
}
