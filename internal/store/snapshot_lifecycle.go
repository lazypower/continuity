package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// hookAfterPublishBeforeWALScrub is a TEST-ONLY seam (nil in production) fired in
// Restore after the restored DB is published and before the stale -wal/-shm scrub.
// See TestRestore_StaleWALRemovalFailureIsError.
var hookAfterPublishBeforeWALScrub func(resolvedDB string)

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

	// LINEAGE GATE (Finding 8): recompute the lineage fingerprint from the LIVE
	// DB and only tick/expire a sidecar whose lineage MATCHES this DB. A sidecar
	// transplanted next to an unrelated DB (a foreign restore point) carries a
	// different instance_id, so its fingerprint cannot match — and boot expiry
	// must NEVER auto-delete unproven/foreign restore material. On mismatch we
	// leave the sidecar completely untouched (no tick, no expiry).
	//
	// A live DB we cannot open or fingerprint (missing/legacy/corrupt) is also a
	// fail-closed "do not touch" — we report the error but never delete the
	// sidecar off an unverifiable lineage.
	cur, oerr := OpenNoMigrate(dbPath)
	if oerr != nil {
		return fmt.Errorf("snapshot: boot lineage open: %w", oerr)
	}
	curFingerprint, ferr := lineageFingerprint(cur, m.PreSchemaVersion)
	cur.Close()
	if ferr != nil {
		return fmt.Errorf("snapshot: boot lineage recompute: %w", ferr)
	}
	if curFingerprint != m.LineageFingerprint {
		// Foreign/transplanted sidecar — never tick or auto-delete it.
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

	// DELETION ORDER (Round 12, Finding 1): remove manifest.json FIRST, then
	// snapshot.db. The manifest is what loadValidManifest keys the restore point on,
	// so once it is gone the point is logically void — and if the snapshot.db unlink
	// then fails, the residual snapshot-only sidecar is now PRUNABLE via
	// `prune --confirm` (pruneKnownSidecarFiles), never a wedge. (The reverse order
	// would leave a manifest naming an absent snapshot, which is also prunable, but
	// voiding the manifest first minimizes the window in which a partially-deleted
	// point still looks loadable.)
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
	// A symlinked DB FILE (leaf) OR a SQLite URI/DSN path is an UNSUPPORTED config,
	// not an absent restore point: refuse up front (ErrSymlinkedDBUnsupported /
	// ErrURIDSNUnsupported) rather than report "not present", matching
	// Open/OpenNoMigrate/Restore/Prune. The operator must point CONTINUITY_DB at the
	// real file path. (A URI path would otherwise fall through to sidecarPath, whose
	// ErrSnapshotUnsupportedPath we report as "not present" — masking the misconfig.)
	if err := refuseUnsupportedDBPath(dbPath); err != nil {
		return nil, err
	}
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

	// INTEGRITY CHECK ON STATUS (Round 12, Finding 5): loadValidManifest proves the
	// snapshot.db matches the manifest's recorded shape/size/HASH, but a hash-consistent
	// file can still be NON-SQLITE garbage (a forged/hand-edited manifest whose recorded
	// hash happens to match arbitrary bytes). Without opening the image, `status` would
	// report a clean "present" for a snapshot that `restore --confirm` later rejects on
	// integrity_check — a false "safe" signal. Run the SAME read-only PRAGMA
	// integrity_check restore/reuse run, but ONLY here in the status command, so routine
	// snapshot CREATION and the boot-tick path are not slowed. A failure is reported as a
	// present-but-corrupt Problem (the CLI exits non-zero); we never touch the sidecar.
	if err := integrityCheck(snapshotDBPathIn(sidecar)); err != nil {
		st.Problem = fmt.Errorf(
			"%w: snapshot present and hash-consistent but failed integrity_check (%v); "+
				"`continuity snapshot restore --confirm` would refuse it — run "+
				"`continuity snapshot prune --confirm` to remove it",
			ErrSnapshotSidecarCorrupt, err).Error()
		return st, nil
	}

	// SNAPSHOT-SCHEMA VERIFICATION ON STATUS (Round 13, Finding 3): a snapshot.db that
	// passes integrity_check (a valid SQLite DB) but is at a DIFFERENT schema version
	// than the manifest's pre_schema_version is a corrupt/tampered restore point that
	// `restore --confirm` now fails closed on. Without checking it here, `status` would
	// report a clean "present" for a point restore would refuse — the same false-"safe"
	// signal Finding 5 closed for integrity. The image is already opened read-only above,
	// so this is cheap; report present-but-corrupt (the CLI exits non-zero), touch nothing.
	if sv, svErr := snapshotSchemaVersion(snapshotDBPathIn(sidecar)); svErr != nil {
		st.Problem = fmt.Errorf(
			"%w: snapshot present but its schema version could not be read (%v); "+
				"`continuity snapshot restore --confirm` would refuse it — run "+
				"`continuity snapshot prune --confirm` to remove it",
			ErrSnapshotSidecarCorrupt, svErr).Error()
		return st, nil
	} else if sv != m.PreSchemaVersion {
		st.Problem = fmt.Errorf(
			"%w: snapshot is schema v%d but its manifest records pre-v%d; "+
				"`continuity snapshot restore --confirm` would refuse it — run "+
				"`continuity snapshot prune --confirm` to remove it",
			ErrSnapshotSidecarCorrupt, sv, m.PreSchemaVersion).Error()
		return st, nil
	}
	return st, nil
}

// Prune removes a restore point's snapshot.db + manifest.json. A VALID point is
// removed via expireRestorePoint (re-validated immediately before deletion). A
// PARTIAL/CORRUPT sidecar (e.g. snapshot.db present but manifest deleted, or vice
// versa) is ALSO removable here (Round 12, Finding 1) — but ONLY when NO restore
// marker is pending (that refusal is preserved). This is the explicit `--confirm`
// escape hatch so a torn sidecar can always be cleaned via the CLI and never wedges
// the operator. Never opens the DB.
//
// DECISIVE + HONEST (Round 13, Finding 2): the partial-sidecar prune now removes ALL
// of continuity's OWN sidecar contents via pruneKnownSidecarFiles — the canonical
// files snapshot.db / manifest.json / restore.in-progress.json (the marker only when
// no live restore is pending, already guaranteed by the marker refusal above) AND the
// owned temp patterns snapshot.tmp.* / manifest.tmp.* / restore.marker.tmp.* /
// .restore.staged.* — each through the regular-file/no-symlink gate. If the sidecar is
// then EMPTY it is rmdir'd and prune reports success. If FOREIGN/unrecognized entries
// remain, prune leaves them and returns a clear error NAMING the residuals (no false
// "pruned"), so the operator knows manual cleanup is needed. Net: prune fully unwedges
// any sidecar of our OWN making, never deletes a foreign file, never falsely reports
// success. (Deleting our own corrupt/partial canonical files on explicit --confirm is
// zero data loss: the live DB is untouched and a corrupt snapshot is an unrestorable
// backup copy — see .notes/restore-recovery-model.md.)
//
// LOCKED AGAINST MIGRATION/RESTORE (Finding 2, Round 6): Prune now acquires the
// EXCLUSIVE DB lock for the deletion. A risky migration holds EXCLUSIVE across
// restore-point creation + DDL, and a restore holds EXCLUSIVE for its whole span;
// without the lock, prune could delete manifest.json / snapshot.db out from under
// an in-flight migration (whose only rollback material is that restore point) or
// after a concurrent restore passed its own pre-marker checks. Acquiring EXCLUSIVE
// here (bounded wait, fail closed with ErrDBLocked) serializes prune against both
// — whoever holds the DB runs, prune waits then fails closed rather than racing.
//
// REFUSES WHILE A RESTORE MARKER IS PENDING (Finding 4, Round 5; re-checked UNDER
// the lock per Finding 2, Round 6): if a restore crashed, the manifest +
// snapshot.db are the ONLY material recovery can use. A Prune that deleted them
// whenever the manifest validates would leave the marker behind with no restore
// point — every Open would then fail ErrRestoreInterrupted AND `restore --confirm`
// would also fail (its restore point is gone). We check the marker BEFORE acquiring
// the lock (fast refusal) AND AGAIN under the lock (a restore that wrote its marker
// while we waited for the lock must still stop us), so a marker that appears at any
// point up to the deletion makes prune refuse.
func Prune(dbPath string) error {
	// REFUSE AN UNSUPPORTED DB PATH up front (before sidecarPath, which would mask a
	// URI/DSN as ErrSnapshotUnsupportedPath): a symlinked leaf → ErrSymlinkedDBUnsupported,
	// a SQLite URI/DSN → ErrURIDSNUnsupported. Matches Open/OpenNoMigrate/Status/Restore.
	if perr := refuseUnsupportedDBPath(dbPath); perr != nil {
		return perr
	}
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		return err
	}

	// ABSENCE PROBE BEFORE THE LOCK (Round 8, Finding 4): if there is provably no
	// restore point, return ErrNoRestorePoint cleanly WITHOUT creating <db>.lock or
	// contending for it. Previously prune O_CREATE'd the lock file and could return
	// "in use" / a lock error on a fresh install, missing dir, or running serve —
	// before ever checking whether there was anything to prune. A present-but-corrupt
	// sidecar is NOT absent: the probe returns nil and we still take the lock and fail
	// closed under it below.
	if perr := probeRestorePointAbsent(dbPath); perr != nil {
		return perr
	}

	// Fast pre-lock refusal: a marker that already exists blocks prune before we
	// even contend for the lock. We re-check under the lock below.
	if err := refuseIfRestoreMarkerPending(dbPath); err != nil {
		return err
	}

	// Acquire the EXCLUSIVE DB lock so prune serializes against a risky migration
	// and a restore (both EXCLUSIVE holders). Bounded wait, fail closed: if a
	// migration/restore holds the DB, prune does NOT delete the recovery material
	// out from under it — it waits the bounded window then returns ErrDBLocked.
	lockHandle, lockErr := acquireExclusiveLock(dbPath)
	if lockErr != nil {
		if errors.Is(lockErr, ErrDBLocked) {
			return fmt.Errorf(
				"%w: the database is in use (a migration or restore may be in progress); retry once it completes",
				ErrDBLocked)
		}
		return fmt.Errorf("prune: acquire exclusive db lock: %w", lockErr)
	}
	defer lockHandle.release()

	// RE-CHECK the marker UNDER the lock. A concurrent restore that passed its
	// pre-marker checks and wrote its marker while we were waiting for the lock
	// would otherwise have its recovery material deleted; this re-check makes prune
	// refuse on a marker that appeared after the pre-lock check.
	if err := refuseIfRestoreMarkerPending(dbPath); err != nil {
		return err
	}

	m, err := loadValidManifest(sidecar)
	if err != nil {
		if errors.Is(err, ErrNoRestorePoint) {
			return ErrNoRestorePoint
		}
		// PARTIAL/CORRUPT SIDECAR IS PRUNABLE (Round 12, Finding 1; made DECISIVE in
		// Round 13, Finding 2). Previously a corrupt or partial sidecar (e.g. snapshot.db
		// present but manifest deleted, or vice versa, or a leftover snapshot.tmp.* from an
		// aborted creation) made Prune REFUSE, so loadValidManifest failed forever and the
		// operator was WEDGED with no CLI path to clean it. Since we are here under the
		// EXCLUSIVE lock with NO restore marker pending (re-checked above), there is provably
		// no in-flight migration/restore whose recovery material this is — so an explicit
		// `prune --confirm` may remove ALL of continuity's OWN sidecar contents.
		// pruneKnownSidecarFiles removes the canonical files AND the owned temp patterns,
		// each only when a regular NON-SYMLINK file, then rmdir's the sidecar if empty —
		// or returns a clear error NAMING any foreign residual it refused to touch (no
		// false "pruned"). This makes any sidecar of our own making cleanable via the CLI;
		// no wedge, and no foreign deletion.
		return pruneKnownSidecarFiles(sidecar)
	}
	return expireRestorePoint(sidecar, m)
}

// ownedSidecarTempPrefixes are the fixed prefixes of the temp/staging files
// continuity itself creates (always O_EXCL-created with a random token, see
// createOwnedTemp). An entry whose name begins with one of these is OURS — an
// aborted creation/restore can leave one behind, and an explicit `prune --confirm`
// (under the lock, no marker pending) may remove it. snapshot.tmp.* and
// manifest.tmp.* are created in the sidecar by restore-point creation;
// restore.marker.tmp.* by the marker writer; .restore.staged.* is normally created
// in the DB dir but is listed so a stray one inside the sidecar is still recognized
// as ours rather than reported foreign. The trailing '.' is part of each prefix so a
// foreign file merely STARTING with e.g. "snapshot" (but not "snapshot.tmp.") is not
// mistaken for ours.
var ownedSidecarTempPrefixes = []string{
	"snapshot.tmp.",
	"manifest.tmp.",
	"restore.marker.tmp.",
	".restore.staged.",
}

// isOwnedSidecarTempName reports whether name is one of continuity's own
// temp/staging files (an O_EXCL-created leftover from an aborted operation).
func isOwnedSidecarTempName(name string) bool {
	for _, p := range ownedSidecarTempPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// isOwnedSidecarName reports whether name (a sidecar directory entry's basename) is
// one continuity OWNS: a canonical file (snapshot.db / manifest.json /
// restore.in-progress.json) or one of our owned temp patterns. Anything else is
// FOREIGN — prune must never delete it.
func isOwnedSidecarName(name string) bool {
	switch name {
	case snapshotFileName, manifestFileName, restoreMarkerName:
		return true
	}
	return isOwnedSidecarTempName(name)
}

// pruneKnownSidecarFiles makes the partial/corrupt-sidecar prune DECISIVE and HONEST
// (Round 12, Finding 1; Round 13, Finding 2). The caller (Prune) holds the EXCLUSIVE
// DB lock and has re-checked that NO restore marker is pending, so this only runs when
// there is provably no in-flight migration/restore that owns this material.
//
// It removes ALL of continuity's OWN sidecar contents — the canonical files
// (snapshot.db, manifest.json, restore.in-progress.json) AND the owned temp patterns
// (snapshot.tmp.* / manifest.tmp.* / restore.marker.tmp.* / .restore.staged.*) — so a
// sidecar wedged by an aborted creation (a leftover snapshot.tmp.*, a partial
// manifest-only/snapshot-only state) is fully cleaned. Then:
//   - if the sidecar is EMPTY, rmdir it and return success;
//   - if FOREIGN/unrecognized entries remain (not our canonical names or owned temp
//     patterns), LEAVE them and return a clear error NAMING the residuals — prune does
//     NOT claim success while a foreign file keeps the dir wedged, so the operator knows
//     manual cleanup is needed.
//
// SAFETY BARS (match the global no-symlink / prove-it-is-ours discipline):
//   - Each OWNED entry is removed ONLY when lstat shows a REGULAR, non-symlink file. A
//     symlink / FIFO / device / dir at an owned name is NOT unlinked through; it is left
//     and surfaced as a residual, matching the managed-file gate.
//   - A FOREIGN entry is NEVER touched — we only ever remove names we recognize as ours.
//   - The sidecar dir is rmdir'd ONLY if it is left empty afterward.
func pruneKnownSidecarFiles(sidecar string) error {
	entries, rerr := os.ReadDir(sidecar)
	if rerr != nil {
		return fmt.Errorf("prune: read sidecar dir: %w", rerr)
	}

	var residual []string // entries left in place (foreign, or owned-but-not-regular)
	for _, ent := range entries {
		name := ent.Name()
		p := filepath.Join(sidecar, name)

		if !isOwnedSidecarName(name) {
			// FOREIGN: never delete it. Record it so we can fail honestly below.
			residual = append(residual, name)
			continue
		}

		info, lerr := os.Lstat(p)
		if lerr != nil {
			if os.IsNotExist(lerr) {
				continue // raced away — nothing to remove
			}
			residual = append(residual, name)
			continue
		}
		// Only remove a REGULAR, non-symlink file at an owned name. A symlink / FIFO /
		// device / dir there is foreign-shaped — leave it (never unlink through it) and
		// surface it as a residual, keeping the dir.
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			residual = append(residual, name)
			continue
		}
		if rmErr := os.Remove(p); rmErr != nil && !os.IsNotExist(rmErr) {
			residual = append(residual, name)
		}
	}

	if len(residual) > 0 {
		// HONEST: foreign/non-removable entries remain — do NOT rmdir, do NOT claim
		// success. Name them so the operator can clean up by hand.
		return fmt.Errorf(
			"%w: sidecar %s has entries continuity does not own and did not remove: %v; remove them by hand",
			ErrSnapshotSidecarCorrupt, sidecar, residual)
	}

	// DECISIVE: every entry was ours and removed; the sidecar is now empty — rmdir it.
	if err := os.Remove(sidecar); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("prune: remove empty sidecar dir %s: %w", sidecar, err)
	}
	fmt.Fprintf(os.Stderr, "  pruned partial restore point → removed %s\n", sidecar)
	return nil
}

// probeRestorePointAbsent returns ErrNoRestorePoint when there is provably NO
// restore point for dbPath, WITHOUT opening the DB or creating the lock file
// (Round 8, Finding 4). It is the status-style absence probe that restore/prune
// run FIRST so a fresh install / missing dir / running serve reports
// ErrNoRestorePoint cleanly instead of "in use" or a lock-file error from
// O_CREATE-ing <db>.lock before they ever check whether there is anything to do.
//
// It derives the sidecar purely from the path (sidecarPath creates nothing) and
// calls loadValidManifest:
//   - ErrNoRestorePoint (no sidecar, no manifest, or no snapshot) → return it: the
//     caller short-circuits with NO lock file and no side effects.
//   - a VALID restore point → return nil: the caller proceeds to acquire the lock
//     and re-check under it.
//   - a present-but-CORRUPT sidecar → return nil too, so the caller takes the lock
//     and fails closed under it exactly as before (we never delete/refuse off an
//     unprovable sidecar without the lock, and corrupt is NOT "absent").
//
// Ineligible paths (:memory:/URI) have no sidecar → ErrNoRestorePoint.
func probeRestorePointAbsent(dbPath string) error {
	// A symlinked DB FILE (leaf) OR a SQLite URI/DSN path is an UNSUPPORTED config,
	// not an absent restore point: Restore/Prune call this FIRST, so refusing here
	// (ErrSymlinkedDBUnsupported / ErrURIDSNUnsupported) makes both fail closed up
	// front — before any lock file is created or contended — rather than report
	// ErrNoRestorePoint. The operator must point CONTINUITY_DB at the real file path.
	if err := refuseUnsupportedDBPath(dbPath); err != nil {
		return err
	}
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		if errors.Is(err, ErrSnapshotUnsupportedPath) {
			return ErrNoRestorePoint
		}
		return err
	}
	if _, lerr := loadValidManifest(sidecar); lerr != nil {
		if errors.Is(lerr, ErrNoRestorePoint) {
			return ErrNoRestorePoint
		}
		// Present-but-corrupt: NOT absent. Let the caller take the lock and fail
		// closed under it (unchanged refusal semantics).
		return nil
	}
	return nil
}

// refuseIfRestoreMarkerPending returns an ErrRestoreInterrupted error when a
// restore marker is present for dbPath, and nil otherwise. Prune calls it twice —
// once before taking the lock (fast refusal) and once under the lock (a restore
// that wrote its marker while prune waited for the lock must still stop the
// deletion).
func refuseIfRestoreMarkerPending(dbPath string) error {
	pending, perr := restoreMarkerPending(dbPath)
	if perr != nil {
		return fmt.Errorf("prune: check for pending restore: %w", perr)
	}
	if pending {
		return fmt.Errorf(
			"%w: a restore marker is pending for this DB; run `continuity snapshot restore --confirm` to complete recovery before pruning",
			ErrRestoreInterrupted)
	}
	return nil
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
	// REFUSE AN UNSUPPORTED DB PATH up front (before sidecarPath, which would mask a
	// URI/DSN as ErrSnapshotUnsupportedPath): a symlinked leaf → ErrSymlinkedDBUnsupported,
	// a SQLite URI/DSN → ErrURIDSNUnsupported. The operator must point CONTINUITY_DB
	// at the real file path. Matches Open/OpenNoMigrate/Status/Prune.
	if rerr := refuseUnsupportedDBPath(dbPath); rerr != nil {
		return "", rerr
	}
	sidecar, err := sidecarPath(dbPath)
	if err != nil {
		return "", err
	}

	// ABSENCE PROBE BEFORE ANY LOCK (Round 8, Finding 4): if there is provably no
	// restore point, return ErrNoRestorePoint cleanly WITHOUT creating <db>.lock /
	// <db>.serve.lock or contending for them. Previously Restore O_CREATE'd the lock
	// file and could report "in use" / a lock error on a fresh install, missing dir,
	// or running serve — before ever checking whether there was anything to restore.
	// A VALID restore point (or a present-but-corrupt sidecar, or a pending recovery
	// marker beside a corrupt manifest) is NOT absent: the probe returns nil and we
	// proceed to take the lock + run recovery / fail closed under it exactly as before.
	if perr := probeRestorePointAbsent(dbPath); perr != nil {
		return "", perr
	}

	// ACQUIRE the EXCLUSIVE DB lock and hold it for the ENTIRE restore — through
	// marker write, the moves, publish, cleanup, and marker removal (Findings 1 &
	// 5, Round 5). This is the SAME unified flock-based lock every writable open
	// takes SHARED and a risky migration takes EXCLUSIVE: holding it exclusively
	// here makes restore mutually exclusive with EVERY writable open (serve AND
	// the openDB() CLI commands: dedup/remember/retract/import/extract), with a
	// risky migration, and with another restore. Previously restore took only a
	// serve-lock + op-lock, which left ordinary writable opens unguarded — so a
	// `snapshot restore --confirm` could rename the DB triplet out from under an
	// active SQLite connection.
	//
	// NON-BLOCKING with a bounded wait: if a shared (writer) or exclusive holder
	// exists, exclusive acquisition waits the bounded window then FAILS CLOSED
	// (ErrDBLocked) rather than swapping the DB under a live writer. A crashed
	// holder's flock auto-releases (kernel), so a dead serve never wedges restore.
	lockHandle, lockErr := acquireExclusiveLock(dbPath)
	if lockErr != nil {
		if errors.Is(lockErr, ErrDBLocked) {
			return "", errors.New("restore: the database is in use; stop other continuity processes (serve and any running commands) and retry")
		}
		return "", fmt.Errorf("restore: acquire exclusive db lock: %w", lockErr)
	}
	defer lockHandle.release()

	// ACQUIRE THE DEDICATED SERVE LOCK for the whole restore (Round 8, Finding 2).
	// The DB exclusive lock above excludes ordinary writable opens, a risky
	// migration, and another restore — but it is SEPARATE from the dedicated serve
	// lock (<resolvedDB>.serve.lock) that `serve` takes BEFORE it opens the DB.
	// Without taking the serve lock here, a live `serve` could hold the serve lock,
	// briefly drop its DB SHARED lock (it never holds SHARED across the whole
	// session — only per-open), let this restore swap the pre-version DB into place,
	// then re-open and AUTO-MIGRATE the restored DB out from under the operator. We
	// FAIL CLOSED (ErrServeLockHeld) if a serve already holds it: restore is mutually
	// exclusive with serve. Released on return; the kernel also auto-releases it on
	// process death so a crashed serve never wedges restore.
	serveLock, slErr := AcquireServeLock(dbPath)
	if slErr != nil {
		if errors.Is(slErr, ErrServeLockHeld) {
			return "", errors.New("restore: a continuity serve is running for this database; stop `continuity serve` and retry")
		}
		return "", fmt.Errorf("restore: acquire serve lock: %w", slErr)
	}
	defer serveLock.Release()

	// Drive any torn restore from a previous crash to a clean terminal state
	// FIRST, under FULL validation: a fresh restore must never start on top of an
	// in-progress one. This is the ONE place recovery runs (explicit operator
	// intent + serve lock held), per the fail-closed pivot — Open never resumes.
	// recoverPendingRestore validates the marker schema and constrains every path
	// to the canonical set; a corrupt/partial marker fails closed here.
	if rerr := recoverPendingRestore(dbPath); rerr != nil {
		return "", fmt.Errorf("restore: recover prior restore: %w", rerr)
	}

	m, err := loadValidManifest(sidecar)
	if err != nil {
		return "", err // ErrNoRestorePoint or corrupt — both refuse
	}

	snapPath := snapshotDBPathIn(sidecar)

	// Integrity-check the snapshot image before trusting it.
	if err := integrityCheck(snapPath); err != nil {
		return "", err
	}

	// VERIFY THE SNAPSHOT IS ACTUALLY AT pre_schema_version (Round 13, Finding 3).
	// Creation and reuse both prove snapshot.db's schema == manifest.PreSchemaVersion,
	// but Restore previously trusted only manifest shape + hash + integrity_check — so
	// a snapshot.db swapped to a DIFFERENT schema version (with the manifest's hash/size
	// updated to match) would pass every gate and be published as the live DB at the
	// WRONG schema, defeating the "restore returns you to pre-vN" contract. The image is
	// already opened read-only for integrity_check just above, so this is cheap
	// defense-in-depth. FAIL CLOSED before any destructive swap on mismatch.
	if sv, svErr := snapshotSchemaVersion(snapPath); svErr != nil {
		return "", fmt.Errorf(
			"%w: could not read restore point snapshot schema version (%v)",
			ErrSnapshotSidecarCorrupt, svErr)
	} else if sv != m.PreSchemaVersion {
		return "", fmt.Errorf(
			"%w: restore point snapshot is schema v%d but its manifest records pre-v%d",
			ErrSnapshotSidecarCorrupt, sv, m.PreSchemaVersion)
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

	// Record the ORIGINAL live DB's sha256 BEFORE moving it aside (Finding 1).
	// Recovery uses this to provenance-check the moved-aside backup before ever
	// renaming it back over the live DB: a planted/stale/corrupt
	// <db>.pre-restore.* whose hash does not match this recorded value is refused.
	originalDBSHA256, _, ohErr := hashFile(resolvedDB)
	if ohErr != nil {
		return "", fmt.Errorf("restore: hash original db: %w", ohErr)
	}

	dbDir := filepath.Dir(resolvedDB)

	// Stage the snapshot to a temp file in the DB dir, then verify its hash
	// at the staged location before any destructive move. The staged name is
	// O_EXCL-created (proves ownership) so we never copy over a foreign
	// .restore.staged temp (Finding 7). The ".restore.staged." prefix is also what
	// resume validates a marker's staged path against.
	//
	// STAGED-TEMP OWNERSHIP (Finding 3, Round 5): we DO NOT close-then-reopen the
	// staged path by name. The prior code created the temp with O_EXCL, CLOSED it,
	// then copyFile reopened it with O_CREATE|O_TRUNC — a window in which a watcher
	// could swap the path with a SYMLINK so the copy wrote THROUGH the link and a
	// symlink could be published as the live DB. Instead we keep the proven-owned
	// fd OPEN and copy the snapshot bytes straight into it. A swapped symlink
	// cannot affect a write to an already-open fd, and we additionally assert the
	// staged path is a regular file (not a symlink) before publish.
	stagedFile, staged, terr := createOwnedTemp(dbDir, ".restore.staged.", ".db")
	if terr != nil {
		return "", fmt.Errorf("restore: reserve staged temp: %w", terr)
	}
	if err := copyFileToOpenFd(snapPath, stagedFile); err != nil {
		_ = stagedFile.Close()
		_ = os.Remove(staged)
		return "", fmt.Errorf("restore: stage snapshot: %w", err)
	}
	if err := stagedFile.Close(); err != nil {
		_ = os.Remove(staged)
		return "", fmt.Errorf("restore: close staged temp: %w", err)
	}
	// The staged path must be a REGULAR file (not a symlink someone swapped in):
	// fail closed so a symlink is never renamed into the live DB path.
	if err := assertRegularFile(staged); err != nil {
		_ = os.Remove(staged)
		return "", fmt.Errorf("restore: staged snapshot is not a regular file: %w", err)
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
	// exactly what we move (recovery moves back exactly these), and record a
	// per-suffix provenance hash for EACH so rollback can prove every moved-aside
	// backup is ours before renaming it back — not just the main DB (Round 8,
	// Finding 3). Each present suffix is symlink/regular-gated here: we refuse to
	// move (and later publish over) a triplet member that is a symlink someone
	// swapped in, matching the no-symlink recovery bar.
	var movedSuffixes []string
	var movedEntries []movedEntry
	movedHashes := map[string]string{}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		live := resolvedDB + suffix
		if !lstatExists(live) {
			continue
		}
		// No-symlink / regular-file gate on every moved triplet member (Round 8,
		// Finding 3): a symlinked -wal/-shm must never be moved aside (and thus never
		// published back over a live name on rollback).
		if err := assertRecoverableFile(live); err != nil {
			_ = os.Remove(staged)
			return "", fmt.Errorf("restore: live %s is not a regular file: %w", live, err)
		}
		sum, _, hErr := hashFileNoFollow(live)
		if hErr != nil {
			_ = os.Remove(staged)
			return "", fmt.Errorf("restore: hash live %s before move-aside: %w", live, hErr)
		}
		movedSuffixes = append(movedSuffixes, suffix)
		movedEntries = append(movedEntries, movedEntry{Suffix: suffix, SHA256: sum})
		movedHashes[suffix] = sum
	}

	// DEFENSE-IN-DEPTH durability of the staged temp before the marker (Round 10,
	// Finding 2). The staged BYTES are already fsync'd (copyFileToOpenFd → Sync),
	// but the staged DIRECTORY ENTRY is not durable until the DB dir is fsync'd —
	// the first dir-fsync today is AFTER the move-aside (below). fsync the DB dir
	// here so the `.restore.staged.*` entry is durable before the marker references
	// it, narrowing the torn window where a crash after the move-aside-rename loses
	// the staged entry while the backup survives. This is BEST-EFFORT (non-fatal):
	// recovery's rollback depends ONLY on the provenance-verified BACKUP, not on the
	// staged file (reconcile CASE B no longer requires stagedPresent), so a
	// dir-fsync failure here must not abort an otherwise-valid restore.
	if err := fsyncDir(dbDir); err != nil {
		fmt.Fprintf(os.Stderr, "warning: restore: fsync db dir after staging snapshot (defense-in-depth; rollback does not depend on it): %v\n", err)
	}

	// Write the restore marker BEFORE the first destructive rename. From here a
	// crash is recoverable from the sidecar marker.
	mk := &restoreMarker{
		Version:          1,
		RestoredDBPath:   resolvedDB,
		StagedPath:       staged,
		BackupPrefix:     movedAsidePrefix,
		MovedSuffixes:    movedSuffixes,
		DBPublished:      false,
		OriginalDBSHA256: originalDBSHA256,
		MovedEntries:     movedEntries,
	}
	if err := writeRestoreMarkerAtomic(sidecar, mk); err != nil {
		_ = os.Remove(staged)
		return "", err
	}

	// In-process canonical view for the rollback paths below. Every field here
	// was computed locally from the resolved DB (not read back from the marker),
	// so it is already trusted; finishPendingRestore consumes the same shape that
	// resume reconstructs from disk.
	cr := &canonicalRestore{
		resolvedDB:       resolvedDB,
		sidecar:          sidecar,
		staged:           staged,
		backup:           movedAsidePrefix,
		moved:            movedSuffixes,
		published:        false,
		originalDBSHA256: originalDBSHA256,
		movedHashes:      movedHashes,
		// The validated restore point's snapshot.db hash, so removeProvenStaged can
		// PROVE the staged temp is ours before deleting it on a rollback (Round 7).
		snapshotSHA256: m.SnapshotSHA256,
	}

	// Move the current triplet aside to the unique pre-restore names. We do NOT
	// delete them — they are crash material and an operator escape hatch.
	for _, suffix := range movedSuffixes {
		src := resolvedDB + suffix
		dst := movedAsidePrefix + suffix
		if err := os.Rename(src, dst); err != nil {
			// Roll back whatever we already moved, then clear the marker.
			_ = finishPendingRestore(cr)
			_ = os.Remove(staged)
			return "", fmt.Errorf("restore: move %s aside: %w", src, err)
		}
	}
	// fsync the DB dir so the moved-aside originals are DURABLE before we publish
	// over their old names. FAIL-CLOSED (Round 7, Finding 6): a power loss
	// mid-restore must leave the moved-aside originals findable for rollback, not
	// silently reverted to the live names. If the dir cannot be synced we roll the
	// move-aside back (the durable marker still describes it) and abort rather than
	// publish over a non-durable move.
	if err := fsyncDir(dbDir); err != nil {
		_ = finishPendingRestore(cr)
		_ = os.Remove(staged)
		return "", fmt.Errorf("restore: fsync db dir after move-aside (originals must be durable before publish): %w", err)
	}

	// Rename the staged snapshot into the live DB path (atomic on same dir).
	if err := os.Rename(staged, resolvedDB); err != nil {
		// DB not yet published — roll back to the moved-aside originals.
		_ = finishPendingRestore(cr)
		_ = os.Remove(staged)
		return "", fmt.Errorf("restore: publish restored db: %w", err)
	}
	// After publishing, the live DB path must be a real regular file, never a
	// symlink (Round 7, Findings 1 & 2): never leave a symlink as the live DB.
	if err := assertLiveDBNotSymlink(resolvedDB); err != nil {
		return "", fmt.Errorf("restore: %w", err)
	}
	_ = os.Chmod(resolvedDB, 0o600)
	// fsync the DB dir so the restored DB's directory entry is durable. FAIL-CLOSED
	// (Round 7, Finding 6): the published DB's directory entry must reach disk
	// before we report success and clear the marker — otherwise a power loss could
	// lose the publish while the marker is already gone. The DB IS published at
	// this point, so we surface the failure for the operator (the marker is still
	// present and recovery will complete against reality) rather than pretend the
	// restore durably succeeded.
	if err := fsyncDir(dbDir); err != nil {
		return "", fmt.Errorf("restore: the database was restored to %s but its directory entry could not be made durable (%v); the restore marker remains for recovery — re-run `continuity snapshot restore --confirm`", resolvedDB, err)
	}

	// PUBLISHED. From here, the staged image IS the live DB. The pre-publish
	// marker still says db_published:false, but a crash from this point is no
	// longer a rollback hazard: recovery reconciles against REALITY (Finding 1) —
	// it hashes the live DB, finds it equals the snapshot, and COMPLETES rather
	// than rolling back over the freshly-restored DB.

	// TEST SEAM (Finding 5): fires AFTER publish and BEFORE the stale-WAL scrub. A
	// test plants an unremovable -wal at the live name here to prove the scrub
	// failure becomes a restore error, not a false success. nil in production.
	if hookAfterPublishBeforeWALScrub != nil {
		hookAfterPublishBeforeWALScrub(resolvedDB)
	}

	// Ensure no stale -wal/-shm remain at the LIVE names. They were moved aside
	// above, but a crash could have left a fresh one; remove any that match the
	// live names so the restored DB is not paired with a foreign WAL.
	//
	// A removal FAILURE here is an ERROR, not a discardable best-effort (Finding 5,
	// Round 6): a stale -wal/-shm left beside the freshly-restored DB can corrupt or
	// silently mask the restored image when SQLite next opens it, so returning
	// success with it still present is a false success. The recovery paths
	// (completeReconciled / finishPendingRestore) already error on the same op;
	// normal restore now matches. The DB itself IS already published at this point,
	// so we surface the failure for the operator to clear the stale WAL by hand
	// rather than pretend the restore is clean.
	for _, suffix := range []string{"-wal", "-shm"} {
		live := resolvedDB + suffix
		if lstatExists(live) {
			// SYMLINK/REGULAR GATE (Round 8, Finding 3): match the recovery scrub paths
			// (completeReconciled / finishPendingRestore) — never os.Remove a symlink at
			// the live -wal/-shm position. A symlink here means something outside the
			// canonical set sits at that name; fail closed rather than unlink through it.
			if err := assertRecoverableFile(live); err != nil {
				return "", fmt.Errorf(
					"restore: the database was restored to %s but the stale %s is not a regular file (%v); "+
						"remove it by hand before opening the database", resolvedDB, live, err)
			}
			if rmErr := os.Remove(live); rmErr != nil && !os.IsNotExist(rmErr) {
				return "", fmt.Errorf(
					"restore: the database was restored to %s but a stale %s could not be removed (%v); "+
						"remove it by hand before opening the database", resolvedDB, live, rmErr)
			}
		}
	}

	// CRASH-SAFE POST-PUBLISH TRANSITION (Finding 2; durability ordering Round 9,
	// Finding 1B). The stale-WAL/-SHM unlinks just above are still only un-fsync'd
	// directory mutations; the marker MUST NOT be cleared until they are durable —
	// otherwise a power loss after marker-removal-durability but before
	// scrub-durability could resurrect a stale -wal beside the restored DB with NO
	// marker left to drive recovery. clearPublishedRestoreMarker therefore fsyncs the
	// DB dir (FAIL CLOSED) BEFORE removing the marker, then fsyncs the sidecar so the
	// removal itself is durable. If it cannot, FAIL LOUDLY rather than return success
	// with a marker still on disk.
	if err := clearPublishedRestoreMarker(sidecar, resolvedDB, dbDir); err != nil {
		return "", err
	}

	// Record the restore in the manifest (best-effort).
	//
	// RESET BOOT RETENTION (Round 12, Finding 2): the restored DB earns a FRESH
	// retention window. Without this, an old successful_boots>0 carried in the
	// manifest would let the point auto-expire after FEWER post-restore boots than
	// the operator expects (e.g. successful_boots=2, expires_after=3 ⇒ the point
	// vanishes after ONE post-restore boot, removing the only rollback material for
	// a re-restore). Zeroing the counter and clearing last_successful_boot_at means
	// the next RecordSuccessfulBoot starts counting from 0 against the full window.
	m.RestoreCount++
	m.SuccessfulBoots = 0
	m.LastSuccessfulBootAt = nil
	nowStr := time.Now().UTC().Format(time.RFC3339)
	m.LastRestoredAt = &nowStr
	if werr := writeManifestAtomic(sidecar, m); werr != nil {
		fmt.Fprintf(os.Stderr, "warning: restore succeeded but manifest update failed: %v\n", werr)
	}

	return movedAsidePrefix, nil
}

// clearPublishedRestoreMarker durably removes the restore marker AFTER a
// successful publish (Finding 2). The marker still records db_published:false at
// this point, but the live DB now IS the snapshot, so a crash here is no longer a
// rollback hazard (reconcilePendingRestore completes against reality). The marker
// must nonetheless be cleared before Restore returns success: a stale
// recovery-implying marker left behind a "success" return is exactly the state
// the bar forbids.
//
// DURABILITY ORDERING (Round 9, Finding 1B): the post-publish stale -wal/-shm
// scrub renames/unlinks in the DB dir must be DURABLE before the marker is removed.
// Previously this only fsync'd the SIDECAR (so the marker removal was durable) but
// NOT the DB dir, so a power loss after the marker was durably gone but before the
// scrub unlinks reached disk could resurrect a stale -wal beside the restored DB
// with no marker to drive recovery. We now fsync the DB DIR first — FAIL CLOSED,
// keeping the marker so recovery re-runs — and only then remove the marker and
// fsync the sidecar. On removal failure we FAIL LOUDLY (the operator is told the
// restore SUCCEEDED but must clear the marker by hand) rather than return success.
func clearPublishedRestoreMarker(sidecar, resolvedDB, dbDir string) error {
	// Make the stale-WAL/-SHM scrub (and the published DB rename) durable BEFORE the
	// marker is cleared. FAIL CLOSED: keep the marker so a crash here re-runs recovery.
	if err := fsyncRecoveryDBDir(dbDir); err != nil {
		return fmt.Errorf(
			"restore: the database was restored to %s but the db directory could not be made durable before clearing the restore marker (%v); "+
				"the restore marker remains for recovery — re-run `continuity snapshot restore --confirm`", resolvedDB, err)
	}
	if err := removeRestoreMarker(sidecar); err != nil {
		return fmt.Errorf(
			"restore: the database was restored successfully to %s, but the restore marker could not be cleared (%v); "+
				"remove %s by hand before the next restore", resolvedDB, err, restoreMarkerPathIn(sidecar))
	}
	if err := fsyncDir(sidecar); err != nil {
		// The marker file's unlink was issued; a dir-fsync failure cannot resurrect
		// it on a sane FS. Non-fatal, but surface it.
		fmt.Fprintf(os.Stderr, "warning: restore: fsync sidecar dir after marker removal: %v\n", err)
	}
	return nil
}

// resolveDBPath returns the canonical real path for a DB, matching the single
// derivation sidecarPath/dbLockPath use (canonicalDBPath). It is the resolved
// path every destructive restore operation works against. Retained as a named
// wrapper because tests and Restore read clearer with the intent in the name.
func resolveDBPath(dbPath string) (string, error) {
	return canonicalDBPath(dbPath)
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

// copyFileToOpenFd copies src into the ALREADY-OPEN, proven-owned destination
// fd (Finding 3). Writing into the open fd — rather than reopening the path by
// name — makes a mid-restore symlink swap of the staged path harmless: the write
// lands in the file the O_EXCL create proved we own, never through a substituted
// symlink. The caller owns closing dst.
func copyFileToOpenFd(src string, dst *os.File) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if _, err := io.Copy(dst, in); err != nil {
		return err
	}
	return dst.Sync()
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
