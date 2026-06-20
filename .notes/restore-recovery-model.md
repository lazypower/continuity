# Restore Recovery Model — fail-closed pivot (Round 3)

> **Round 12 update (Codex):** CORRECTNESS-EDGE pass — six non-critical edges (no
> P1), each with a regression test that fails pre-fix. No new threat classes; these
> close lifecycle/coordination gaps. See **"Round 12 — correctness edges"**
> immediately below; the bar is unchanged.

> **Round 10 update (Codex):** the symlinked-leaf refusal had a SIBLING plus a
> too-strict recovery case and a risk-detection gap. Four fixes, each with a
> regression test that fails pre-fix. See **"Round 10 — URI/DSN refusal, rollback
> needs the backup, pending-set risk detection"** immediately below; the bar is
> unchanged.

## Round 12 — correctness edges (prunable partials, boot reset, gap fail-close, reserved-char eligibility, status integrity, no-follow harden)

Six edges, each fixed + pinned by a regression test that is RED pre-fix:

### F1. A PARTIAL sidecar is always PRUNABLE (no wedge)

`expireRestorePoint`/`Prune` unlink `manifest.json` then `snapshot.db`. If the
SECOND unlink fails (or a crash lands between them), the residual is a partial
sidecar (snapshot-only, or manifest-only) that `loadValidManifest` rejects as
corrupt — and the OLD `Prune` REFUSED on corrupt, so the operator was WEDGED (no
CLI path to clean it; every later run fails closed). Fix: when `Prune` (explicit,
`--confirm`) finds a corrupt/partial sidecar AND **no restore marker is pending**
(the Round-5/8 pending-marker refusal is preserved, re-checked under the EXCLUSIVE
lock), it now removes the KNOWN sidecar files by their canonical names via
`pruneKnownSidecarFiles` — `snapshot.db` and `manifest.json` ONLY, each
lstat-gated to a REGULAR non-symlink file (never unlink through a symlink, never
touch a foreign/stray file), then rmdir the sidecar iff left empty. `expireRestorePoint`
now documents the manifest-first deletion order (the manifest is what keys the
point; voiding it first minimizes the still-loadable window, and either residual is
prunable). Net: any partial sidecar is cleanable via the CLI. Pinned by
`TestPrune_RemovesPartialSidecar` (snapshot-only AND manifest-only, no marker ⇒
`prune --confirm` removes it; pre-fix it refused-corrupt).

### F2. Boot retention RESET after a restore

A restore left the manifest's `successful_boots` at its pre-restore value (e.g. 2),
so the next `serve` incremented from there and the point could auto-expire after
ONE post-restore boot — deleting the only rollback material for a re-restore. Fix:
recording a successful restore now sets `successful_boots = 0` and
`last_successful_boot_at = nil` (the restored DB earns a FRESH retention window).
Pinned by `TestRestore_ResetsBootRetention` (set boots=2, restore ⇒ manifest back
to 0; re-migrate to head + one `RecordSuccessfulBoot` ⇒ boots=1, point survives;
pre-fix it expired on that single boot).

### F3. Gapped `schema_versions` FAILS CLOSED (was an unrestorable point)

`migrate()` applies+records migrations CONTIGUOUSLY, so a known migration `m` with
`m.Version < MAX(present)` that has NO `schema_versions` row is corrupt/tampered
bookkeeping — not a pending migration. The Round-10 behavior SNAPSHOTTED-AND-PROCEEDED
on such a gap, but the restore point's lineage fingerprint is computed pre-migration
WITHOUT the gapped row; once the gapped migration inserts the row, the fingerprint
recomputed at restore time mismatches and `restore --confirm` ALWAYS refuses — an
UNRESTORABLE point. Fix: a new `detectSchemaVersionsGap` (sentinel
`*ErrSchemaVersionsGap`, message *"schema_versions is inconsistent (missing version
N); database bookkeeping is corrupt — restore a backup"*) runs at the head of
`riskyUpgradePending` AND `migrate()`, so Open FAILS CLOSED — no migration runs, no
point is created. This REPLACES the Round-10 gapped snapshot-and-proceed (and
subsumes its "unprotected risky migration" sub-case: a gapped table runs nothing).
The absent-row `firstPendingRiskyMigrationActual` (Round-10 F3) is RETAINED for the
contiguous-plus-trailing-tail case, which it still models faithfully; only its
gapped sub-case now fails closed upstream. Pinned by
`TestMigrate_GappedSchemaVersions_FailsClosed` (v5-shaped DB + bogus rows 7/8/9,
MAX=9 row 6 absent ⇒ Open returns `*ErrSchemaVersionsGap{MissingVersion:6,
MaxPresent:9}`, no sidecar, v6 never ran). REPLACES the Round-10
`TestMigrate_GappedSchemaVersions_SnapshotsBeforeMissingRiskyV6`.

### F4. Reserved-char ('#'/'%') paths are snapshot-ELIGIBLE (lock + sidecar applied)

`refuseURIDSNPath` ALLOWS a plain path containing '#'/'%' (ordinary filesystem
bytes; modernc opens the literal file), but `snapshotEligiblePath` REJECTED it — so
`dbLockPath` returned `ErrSnapshotUnsupportedPath` and `acquireShared/ExclusiveLock`
returned a NO-OP handle. A risky upgrade on such a path therefore ran the destructive
DDL UNLOCKED and created NO restore point. Fix: `snapshotEligiblePath` now ACCEPTS
'#'/'%' (only `:memory:`, the `file:` scheme, and a `?` query stay ineligible — the
genuine URI/DSN shapes), so these paths get the real shared/exclusive lock + sidecar
+ snapshots. `OpenNoMigrate`/`roFileURI` already percent-escape the path into the
`file:` URI (Round-7 F7), so read-only inspection still opens the literal file (the
existing reserved-char read-only test is unaffected). Pinned by
`TestReservedCharPath_TakesLockAndSidecar` (v5 DB at a '#' path ⇒ a risky upgrade
creates a sidecar; a held foreign EXCLUSIVE flock makes `Prune` fail closed
`ErrDBLocked` — proving the lock is real, not a no-op).

### F5. `status` runs `integrity_check` (no false "safe" signal)

`loadValidManifest` proves `snapshot.db` matches the manifest's recorded
shape/size/HASH, but a hash-consistent file can still be NON-SQLITE garbage (a
forged/hand-edited manifest whose hash happens to match arbitrary bytes); integrity
was only checked on restore/reuse. So `status` emitted a clean "present" for a
snapshot `restore --confirm` would later refuse. Fix: `Status` now runs a read-only
`PRAGMA integrity_check` on `snapshot.db` and reports present-but-corrupt (the CLI
exits non-zero) — ONLY in the status command, so routine creation/boot-tick are not
slowed. Pinned by `TestStatus_HashConsistentNonSQLiteSnapshotReportsCorrupt`
(garbage snapshot + matching manifest hash ⇒ `status` reports corrupt; pre-fix clean
present).

### F6. `hardenPermissions` SKIPS symlinked -wal/-shm (never chmod a target)

`hardenPermissions` chmod'd `<db>`, `<db>-wal`, `<db>-shm` via `os.Stat`/`os.Chmod`,
which FOLLOW symlinks. The DB LEAF is refused up front (symlinked-leaf gate), but the
`-wal`/`-shm` siblings are not on that path — so a planted `continuity.db-wal ->
/victim` got the VICTIM chmod'd to 0600. Fix: `hardenPermissions` now `Lstat`s each
triplet member and SKIPS any symlink (never chmod a symlink target), while still
tightening a real loose-perm member. Pinned by
`TestHardenPermissions_SkipsSymlinkedWALSHM` (`-wal` symlinked to a 0644 victim ⇒
the victim's mode is unchanged AND a real loose DB leaf is still tightened to 0600;
pre-fix the victim became 0600).

### Minor: `TestDBClose_LockOutlivesSQLHandle`

Green in this sandbox (it probes raw same-process flock). Left as-is per the
review note — no robustness change was needed here.

## Round 10 — URI/DSN refusal, rollback needs the backup, pending-set risk detection

### F1 + F4. REFUSE SQLite URI/DSN DB PATHS (the symlinked-leaf SIBLING)

A SQLite URI/DSN path (`file:/abs/db?mode=rwc`, or any path carrying a URI-reserved
`?`/`#`/`%`) opens the REAL database but is INVISIBLE to the path-owned coordination
the snapshot/restore feature relies on: `AcquireServeLock` is a no-op for it,
`store.Open` takes no shared lock, and `detectRestoreInterrupted` canonicalizes the
LITERAL URI string (so it misses the real `<db>.snapshot/restore.in-progress.json`).
serve-via-URI and restore-via-real-path therefore did NOT mutually exclude, and
crash recovery via a URI open missed the marker — the exact sibling of the
symlinked-leaf bug. Fix: a new `refuseURIDSNPath` (sentinel `ErrURIDSNUnsupported`,
message *"continuity requires a plain database file path, not a SQLite URI/DSN; set
CONTINUITY_DB to the file path"*) is bundled with `refuseSymlinkedDBLeaf` into ONE
up-front gate `refuseUnsupportedDBPath`, run as the FIRST line of `store.Open` /
`store.OpenNoMigrate` and at the head of `Status` / `Restore` / `Prune` —
BEFORE any MkdirAll / lock / marker / `sql.Open`. So serve + every `openDB()` CLI
command + Status/Restore/Prune fail closed on a URI/DSN with no lock/sidecar/marker
touched. `:memory:` is explicitly NOT a URI/DSN (no file to coordinate) and stays
allowed — `OpenMemory` and the whole `:memory:` test suite are unaffected. F4: the
same gate is also the FIRST line of `AcquireServeLock` (which serve calls BEFORE
`store.Open`), so NO `<db>.serve.lock` is created for a symlinked-leaf OR URI/DSN
path — the serve-lock-before-refusal window is closed. Pinned by
`TestSnapshot_URIDSNPath_Unsupported` (Open/OpenNoMigrate/AcquireServeLock/Status/
Restore/Prune all fail closed with `ErrURIDSNUnsupported`, no lock/sidecar/serve-lock
created, `OpenMemory(:memory:)` still works) and the existing symlinked-leaf test
(now also covering the no-`.serve.lock` property through the shared gate).

### F2. ROLLBACK NEEDS THE BACKUP, NOT THE STAGED SNAPSHOT (reconcile CASE B)

A crash AFTER the live DB was renamed to `<db>.pre-restore.<token>` but BEFORE the
DB-dir fsync can leave the `.restore.staged.*` entry (never dir-synced) VANISHED
while the provenance-hash-verified backup SURVIVES — `livePresent=false`,
`dbBackupPresent=true`, `stagedPresent=FALSE`. CASE B required `stagedPresent`, so
reconcile fell through to the generic corrupt-state error and WEDGED the DB even
though the backup ALONE is sufficient to roll back. Fix: CASE B now fires on
`!livePresent && dbBackupPresent` (no `stagedPresent` requirement) — the rollback
restores the BACKUP (`rollbackReconciled` → `verifyMovedBackupProvenance`, all
provenance/symlink/canonical checks intact); the staged copy is only the forward
image we DROP, and `removeProvenStaged` no-ops a missing staged file. Defense in
depth: forward `Restore` now also best-effort `fsyncDir(dbDir)` after staging and
before the marker, so the staged entry is durable before the marker references it —
but the rollback does NOT depend on it (a fsync failure there is non-fatal). Pinned
by `TestReconcile_RollsBackWhenStagedMissingButBackupSurvives` (torn state, backup
present + verified, staged MISSING ⇒ reconcile rolls back and clears the marker;
fails pre-fix with the corrupt-state wedge).

### F3. RISK DETECTION USES THE ACTUAL PENDING SET, NOT MAX(version)

`runPendingMigrations` applies ANY migration whose `schema_versions` ROW IS ABSENT
(gaps included), but risk detection used `firstPendingRiskyVersion(maxApplied)` —
keyed to MAX(version). A gapped/bogus bookkeeping table (MAX=9 but row 6 absent)
made the MAX-based heuristic see nothing risky pending while the migrator still ran
the risky v6 mem_nodes rebuild UNPROTECTED (no restore point). Fix: a new
`db.firstPendingRiskyMigrationActual()` computes the pending set EXACTLY as the
migrator does (per-migration `COUNT(*) ... WHERE version = ?` → absent rows) and
reports the first pending RISKY migration. All four risk-detection call sites
(`riskyUpgradePending`, `migrate`'s `riskyUpgrade` gate, and BOTH
`ensureUpgradeRestorePoint`/`ensureUpgradeRestorePointLocked` — which also needed it
so the restore point isn't skipped for the gapped risky migration) now use it; the
`maxApplied > 0` fresh-install gate is unchanged. `firstPendingRiskyVersion` is
retained only for the manifest `first_risky` field where preVersion is a verified
contiguous MAX. Pinned by `TestMigrate_GappedSchemaVersions_SnapshotsBeforeMissingRiskyV6`
(v5-shaped DB + bogus rows 7/8/9 so MAX=9, row 6 absent ⇒ Open creates a restore
point recording first_risky=6 before running the missing risky v6; fails pre-fix
with "no restore point").

> **Round 9 update (Codex):** DURABILITY-AUDIT pass + a control-file read gap and
> a documented threat-model boundary. The ninth review found the durability
> ordering was applied per-spot but not UNIFORMLY: some fsyncs that a later
> irreversible step depends on were still warnings, one recovery abort-case could
> orphan a half-restored triplet, and the two on-disk CONTROL FILES were read with
> a symlink/FIFO-followable `os.ReadFile`. See **"Durability audit + control-file
> gate (Round 9)"** immediately below; the bar is unchanged.

> **Round 8 update (Codex):** COVERAGE-COMPLETION pass. The eighth review found the
> Round-7 structural invariants were applied to the FORWARD restore path but not
> fully to the RECOVERY paths, and the serve-lock pairing was incomplete. No new
> classes — four coverage-completion fixes. See **"Recovery coverage completion
> (Round 8)"** immediately below.

> **Round 7 update (Codex):** STRUCTURAL recovery-safety pass that closes the
> marker-trust class (no-symlink + content-verify + canonical-token), plus a
> Windows lock-downgrade fix, a dedicated serve lock, marker-durability-before-
> destructive-step, and a SQLite-URI fix. See **"Recovery safety invariants
> (Round 7)"** immediately below.

> **Round 6 update (Codex):** lock-LIFECYCLE hardening. The flock primitive itself
> was sound; the bugs were in WHEN locks are acquired/released relative to SQLite
> open/close and prune. See **"Lock-lifecycle invariants (Round 6)"** at the top.

## Durability audit + control-file gate (Round 9)

The ninth review treated durability as ONE invariant to enforce uniformly across
create / forward-restore / recovery, rather than spot-patching. The bar:

> **Durability ordering invariant.** A file whose durability a later IRREVERSIBLE
> step depends on must have BOTH the file's bytes fsync'd AND its containing
> directory fsync'd BEFORE that step runs. The restore marker is removed LAST —
> only after every rename/removal it describes is durable. Any required fsync that
> fails FAILS CLOSED: the dependent step does not proceed and the marker is not
> cleared, so a re-run resumes.

### 1A. CREATION fsyncs the snapshot bytes + dir as ERRORS, not warnings

`writeRestorePoint` (`snapshot.go`) now (a) `fsyncFile`s the VACUUM-INTO snapshot
image BEFORE it is renamed to `snapshot.db` and hashed into the manifest — so the
manifest never commits to a hash over bytes still in the page cache — and (b) makes
the sidecar-dir fsyncs after publishing `snapshot.db` and `manifest.json` HARD
ERRORS (`fsyncSnapshotDir`, previously warnings). A dir-fsync failure now ABORTS
restore-point creation, which aborts the risky migration (the DB stays at its
pre-version), and the just-published `snapshot.db` is removed so no half-built
point lingers. A published manifest therefore can never describe a non-durable
`snapshot.db`. Test seam `hookSnapshotDirFsync`. Pinned by
`TestCreate_SnapshotDirFsyncFailure_FailsClosedNoManifest` (forced dir-fsync
failure ⇒ Open fails closed, no manifest, DB still at v5).

### 1B. FORWARD RESTORE fsyncs the DB dir BEFORE clearing the marker

After publishing the restored DB, the forward `Restore` scrubs stale live
`-wal`/`-shm`. Those unlinks were not made durable before
`clearPublishedRestoreMarker` removed the marker — and that function fsync'd ONLY
the sidecar, not the DB dir. `clearPublishedRestoreMarker(sidecar, resolvedDB,
dbDir)` now `fsyncRecoveryDBDir(dbDir)` FIRST (FAIL CLOSED — keep the marker so a
crash re-runs recovery), then removes the marker, then fsyncs the sidecar. A power
loss can no longer land with the marker durably gone but a stale `-wal` resurrected
beside the restored DB. Pinned by
`TestRestore_PostPublishScrubDBDirFsyncFailure_KeepsMarker` (forced DB-dir fsync
failure during the scrub ⇒ Restore fails closed, marker survives).

### 1C. RECONCILE drives a half-finished rollback to completion (idempotent)

CASE A2 ("safe pre-rename abort") cleared the marker whenever the live DB equalled
the recorded original AND no main-DB backup remained. But a rollback that renamed
the MAIN DB back over the live path and then CRASHED before restoring `-wal`/`-shm`
lands in exactly that shape — with the recorded suffix backups still on disk.
Clearing there orphaned them (losing WAL-only commits). reconcile now computes
`anyMovedBackupPresent(cr)`: A2 may clear the marker ONLY when NO recorded
moved-suffix backup remains. If any remains, reconcile CONTINUES the rollback
(`rollbackReconciled` is idempotent — it skips suffixes whose backup is already
gone, provenance-checks and restores the ones that remain, then clears the marker
durably). A re-run after a crash mid-reconcile resumes and finishes; it never
abandons a half-restored triplet. Pinned by
`TestReconcile_ResumesHalfFinishedRollback_DoesNotOrphanWAL` (main DB restored, a
`-wal` backup remains ⇒ reconcile restores the `-wal`, THEN clears the marker).

### 6. CONTROL FILES are read no-follow + regular-file-gated

`manifest.json` and `restore.in-progress.json` were read with `os.ReadFile`, which
FOLLOWS a symlink and BLOCKS on a FIFO. A symlink there could read outside the
sidecar; a FIFO could hang status/restore/prune forever. Both are now read through
`readControlFileNoFollow` → `openControlFileNoFollow` (O_NOFOLLOW + O_NONBLOCK on
unix; reparse-point-open on Windows) plus an `fstat` regular-file check. A symlink
fails open (ELOOP) and a FIFO/device/dir/socket fails the regular-file check; both
map to `ErrSnapshotSidecarCorrupt` (fail closed) — never followed, never blocked. A
missing file stays `os.IsNotExist`. All status / migration-validation / prune /
restore reads route through `readManifest` / `readRestoreMarker`, so the gate
applies everywhere. (The Open fail-closed probe `restoreMarkerPending` only
`lstat`s the marker for PRESENCE and already reports a planted symlink/FIFO as
"pending" ⇒ Open fails closed regardless.) Pinned by
`TestControlFiles_SymlinkOrFIFO_RejectedAsCorrupt` (manifest + marker, each as a
symlink AND a FIFO; the FIFO subtests use a 5s watchdog that fails if the read
blocks).

### Threat model (recovery trusts the LOCAL sidecar)

Codex flagged that a FORGED marker plus an attacker-PLANTED backup file (with its
hash recorded in the forged marker) could make recovery rename that file into the
DB. This requires an actor with WRITE ACCESS to the DB/sidecar directory — who
could equally corrupt the live DB directly. There is no local trust anchor that
defeats directory-write tampering (the same boundary as the unencrypted DB file
itself). Recovery therefore TRUSTS THE LOCAL SIDECAR: an actor who already owns the
DB directory is OUT OF MODEL. The defenses in this document — content-verify
(per-suffix provenance hashes), canonical-path reconstruction from a safe token,
the no-symlink/no-FIFO gates, and the durability ordering above — defend the
REALISTIC cases: a crash mid-restore, on-disk corruption, a stale/partial marker, a
torn triplet, a power loss between any two steps. They are NOT claimed to stop a
local attacker who already has write access to the directory. We do NOT add code
for that boundary; it is unachievable without an external trust anchor and is
documented here as the explicit edge of the model.

## Recovery coverage completion (Round 8)

The eighth review confirmed the Round-7 bar but found its invariants under-applied
on the RECOVERY side. These four fixes complete the coverage; the bar is unchanged:
recovery only touches canonical, content-verified, non-symlink files; a durable
marker precedes any irreversible step; restore is mutually exclusive with serve.

### 1. RECOVERY DURABILITY ORDERING — fsync(dbDir) BEFORE marker removal (Finding 1)

The forward `Restore` path already treats the DB-dir fsyncs as mandatory (after
move-aside and after publish, `snapshot_lifecycle.go`). The RECOVERY terminal paths
(`rollbackReconciled`, `completeReconciled`, and the in-process `finishPendingRestore`
in `snapshot_restore_marker.go`) previously removed the marker with NO `fsyncDir(dbDir)`
first. The marker must NEVER be removed before the file moves it describes are durable:
a power loss after marker-removal-durability but before rename/scrub-durability would
leave NO marker and a torn/absent live DB, and the next `Open` would FABRICATE a fresh
DB over it — destroying the data the restore point existed to protect. Each recovery
terminal path now `fsyncRecoveryDBDir(dbDir)` (the rolled-back renames / the -wal/-shm
scrub) BEFORE `removeMarkerDurably` (remove marker → `fsyncDir(sidecar)`), mirroring the
forward path's `clearPublishedRestoreMarker`. A DB-dir fsync failure is FAIL-CLOSED: the
marker is NOT removed, so recovery can re-run. A test seam (`hookRecoveryDBDirFsync`)
forces that fsync to fail. Pinned by `TestRecover_RollbackDBDirFsyncFailure_KeepsMarker`
and `TestRecover_CompleteDBDirFsyncFailure_KeepsMarker` (marker survives a forced
failure ⇒ the fsync provably precedes marker removal).

### 2. RESTORE HOLDS THE DEDICATED SERVE LOCK (Finding 2)

`store.Restore` took only the DB EXCLUSIVE lock. `serve` takes the DEDICATED serve
lock (`AcquireServeLock`, `<resolvedDB>.serve.lock`) BEFORE opening the DB but does
NOT hold the DB SHARED lock across its whole session (only per-open). That left a
window: a serve holds the serve lock, a restore swaps the pre-version DB into place,
and the serve then re-opens and AUTO-MIGRATES the restored DB. `Restore` now ALSO
acquires `AcquireServeLock` for the whole operation and FAILS CLOSED ("stop
`continuity serve` and retry") if a serve already holds it — restore is mutually
exclusive with serve. The stale comment in `recoverPendingRestore` (which falsely
claimed the caller already held the serve lock) is corrected: the caller now holds
BOTH the DB EXCLUSIVE lock AND the serve lock. Pinned by
`TestRestore_FailsClosedWhileServeLockHeld`.

### 3. ALL-SUFFIX MOVED-BACKUP PROVENANCE (Finding 3)

Recovery provenance-checked only the main-DB backup (`original_db_sha256`) and gated
`-wal`/`-shm` backups with only a regular-file/symlink check before renaming them
back. The marker now records a hash PER moved suffix (`moved_entries: [{suffix,
sha256}]`, recorded at restore START before the move, generalizing
`original_db_sha256` to the whole triplet). On rollback, `verifyMovedBackupProvenance`
requires each moved-aside backup to be a regular, non-symlink file whose hash matches
its recorded value — a mismatch/symlink FAILS CLOSED, touching nothing. Rollback now
VERIFIES ALL suffixes BEFORE renaming ANY of them, so a bogus `-wal` aborts the whole
rollback with no partial revert of the main DB. The forward move-aside and the
post-publish `-wal`/`-shm` scrub also apply the symlink/regular gate. The schema gate
(`validateMarkerSchema`) requires a non-empty provenance hash for every moved suffix.
Pinned by `TestRecover_BogusWALBackup_FailsClosedNoRename`.

### 4. NO-RESTORE-POINT PROBE BEFORE ANY LOCK (Finding 4)

`Restore`/`Prune` opened (`O_CREATE`) the `<db>.lock` file before checking whether a
restore point existed, so a fresh install / missing dir / running serve reported "in
use" or a lock-file error instead of `ErrNoRestorePoint`. Both now run
`probeRestorePointAbsent` FIRST — a status-style probe (`sidecarPath` +
`loadValidManifest`, which creates nothing and never opens the DB). If there is
provably no restore point it returns `ErrNoRestorePoint` cleanly with NO lock file
created and no side effects (a missing parent dir → `ErrNoRestorePoint`, not a lock
error). A present-but-CORRUPT sidecar is NOT "absent": the probe returns nil and the
caller takes the lock and fails closed under it exactly as before. Pinned by
`TestRestorePrune_NoRestorePoint_BeforeLock`.

## Recovery safety invariants (Round 7)

The seventh review found the RECOVERY destructive path still trusted marker
fields enough to (1) follow symlinks while hashing/renaming and (2) delete a file
it could not prove it created. These are the load-bearing rules added to close
that as a CLASS, enforced uniformly across `snapshot_restore_marker.go`
(reconcile/complete/rollback) AND `snapshot_lifecycle.go` `Restore`.

### 1. NO SYMLINKS ANYWHERE IN RECOVERY (Findings 1 & 2)

Every path recovery reads / hashes / renames / removes — the live DB, the
`<db>.pre-restore.*` backup, the `.restore.staged.*` staged file, the published
DB after rename, the live `-wal`/`-shm` — is `lstat`-gated (`assertRecoverableFile`)
and REJECTED if it is a symlink (or otherwise non-regular). Hashing uses
`hashFileNoFollow` (O_NOFOLLOW open + fstat regular-file check), NEVER the
symlink-following `hashFile`. After publishing/rolling-back a file at the live DB
path, `assertLiveDBNotSymlink` fails closed if the live DB is a symlink — recovery
never leaves a symlink as the database. A symlink in ANY of these positions ⇒
fail closed, touch nothing. This is what defeats a forged marker whose
`backup_prefix` is a symlink to another directory's DB: the provenance hash opens
O_NOFOLLOW (ELOOP) and the rollback rename's `assertRecoverableFile` rejects it,
so no cross-path clobber occurs.

### 2. CANONICAL-DERIVED PATHS FROM A SAFE TOKEN (Findings 1 & 2)

`resolveCanonicalRestore` reconstructs the backup and staged paths from the
canonical resolved DB path + a marker TOKEN constrained to a safe charset
(`tokenIsSafe`: ASCII letters/digits/`.`/`-`/`_`, no path separator, no `..`). The
marker's `backup_prefix` must equal EXACTLY `<resolvedDB>.pre-restore.<token>` and
its `staged_path` EXACTLY `<dbDir>/.restore.staged.<token>` for a safe token;
anything else fails closed. A reconstructed path can therefore only ever name a
sibling of this DB under names a real restore of THIS DB would have produced —
never another directory, never a traversal.

### 3. CONTENT-VERIFY BEFORE ANY DESTRUCTIVE ACTION (Findings 1 & 2)

- A backup is renamed over the live DB ONLY if it is a regular, non-symlink file
  whose hash == the marker's recorded `original_db_sha256` (provenance, already
  present) AND its path is the canonical reconstruction.
- A staged file is DELETED ONLY if it is a regular, non-symlink file whose hash ==
  the validated snapshot.db hash (`removeProvenStaged`) — proving it is OUR staged
  copy. An unproven staged file (wrong hash, symlink, unreadable, or no snapshot
  hash to verify against) is LEFT IN PLACE, not deleted. A stray temp is safe;
  deleting an unproven file (e.g. a forged marker's `.restore.staged.keep.db`) is
  not. This is the Finding-2 fix: deletion was previously driven by the
  `.restore.staged.` prefix alone.

### 4. DEDICATED SERVE LOCK (Finding 4)

`serve` now takes a DEDICATED, serve-only EXCLUSIVE lock (`AcquireServeLock`,
`<resolvedDB>.serve.lock`) before opening the DB; a SECOND serve for the same DB
refuses to start (`ErrServeLockHeld`). This lock is SEPARATE from the DB
shared/exclusive lock, so it does NOT block ordinary CLI commands — only other
serves contend on it. serve still takes the DB SHARED lock (via `store.Open`) for
restore-exclusion. This restores single-serve exclusivity so boot retention
(`RecordSuccessfulBoot`) counts independent serve SESSIONS again, not concurrent
starts: previously N coexisting serves = N ticks and the restore point could
expire early.

### 5. MARKER DURABILITY BEFORE THE FIRST DESTRUCTIVE STEP (Finding 6)

`writeRestoreMarkerAtomic` now FAILS CLOSED (not warns) if the sidecar dir fsync
fails — the marker MUST be durable before `Restore` moves the live DB aside. The
post-move-aside and post-publish DB-dir fsyncs in `Restore` are likewise errors,
not warnings. A power loss with a non-durable marker would leave a torn restore
with NO marker, so the next `Open` would fabricate a fresh DB instead of returning
`ErrRestoreInterrupted` — the data the restore point existed to protect, silently
destroyed.

### 6. WINDOWS EX→SH DOWNGRADE HOLDS A LOCK CONTINUOUSLY (Finding 3)

Windows has no atomic flock EX→SH. `flockDowngradeToShared` (windows) now takes a
SHARED lock on a bridge sub-range (byte 1) on a SECOND handle BEFORE releasing the
EXCLUSIVE primary-range lock, then re-takes SHARED on the primary range. A foreign
EXCLUSIVE acquirer must lock the WHOLE range and so still conflicts with the bridge
byte during the unlock/relock window — no fully-unlocked cross-process gap while a
migrated SQLite conn is live. Unix keeps the atomic single-fd downgrade. Pinned by
`TestFlockDowngrade_NoForeignExclusiveInGap` (windows seam).

### 7. OpenNoMigrate BUILDS A SAFE URI (Finding 7)

`OpenNoMigrate` percent-escapes the path into the `file:` DSN (`roFileURI`) instead
of concatenating it raw, and `snapshotEligiblePath` rejects paths containing
URI-reserved bytes (`?`, `#`, `%`). A path with `#`/`%` now opens the intended file
read-only instead of being mis-parsed into a different filename or silently
dropping `mode=ro`.

> **Round 5 update (Codex):** the hand-rolled PID serve-lock / op-lock was
> REPLACED by an OS-flock shared/exclusive lock. See **"OS flock lock discipline
> (Round 5)"** below; the Round-3/4 "serve lockfile" / "op-lock" sections are
> historical context for what the flock lock replaces.

## Lock-lifecycle invariants (Round 6)

These are the load-bearing rules added after the sixth review. The flock primitive
is unchanged; these constrain its lifecycle.

### 1. NO OPEN *sql.DB HANDLE ACROSS A LOCK TRANSITION (Finding 1, centerpiece)

A SQLite connection is only ever open while the CORRECT lock level is HELD. The
dangerous shared→exclusive UPGRADE must NEVER happen with a live handle open.

Concretely, `store.Open()` of an existing on-disk DB:

- acquires SHARED, opens the conn, and probes (under SHARED) whether a RISKY
  migration is pending (`db.riskyUpgradePending()`).
- **No risky migration pending** → keep SHARED + conn and migrate normally
  (non-risky ALTER migrations do not rewrite tables; safe under the lifetime
  SHARED lock). Unchanged.
- **A risky migration IS pending** (`openRiskyUpgradeUnderExclusive`):
  1. CLOSE the conn and RELEASE shared — now NO `*sql.DB` handle to this path
     exists. (`DB.Close()` closes sql.DB FIRST then drops the lock, Finding 3, so
     the handle is provably gone.)
  2. acquire EXCLUSIVE (bounded wait, fail closed), then RE-CHECK the
     interrupted-restore marker under exclusive — a restore that won the gap left
     a marker; fail closed on it.
  3. open a FRESH conn AFTER exclusive is held and run the restore-point + DDL
     under EXCLUSIVE (`migratingUnderExclusive` set; `migrate()` does NOT
     re-acquire — the in-process RWMutex is not re-entrant).
  4. ATOMICALLY downgrade the flock EX→SH on the SAME fd
     (`flockDowngradeToShared`) so there is NO cross-process window in which the
     DB is unlocked, then hand the connection that lifetime SHARED hold.

The old `acquireMigrateExclusive` "release-shared-with-the-conn-still-open → take
exclusive → migrate the open conn → downgrade" dance is DELETED: it released SHARED
while a live conn was open, so a concurrent `restore --confirm` could rename the DB
triplet aside and the migration would then write to the MOVED-ASIDE inode (the open
fd kept it alive) while the live path held the restored DB. The invariant asserted
in code + test: between releasing shared and acquiring exclusive there is NO open
`*sql.DB` handle to this path; DDL + restore-point creation run only under EXCLUSIVE
on a conn opened AFTER exclusive is held.

**In-process RWMutex vs the EX→SH downgrade.** flock supports an atomic LOCK_EX→
LOCK_SH on one fd (no cross-process gap). The in-process RWMutex is NOT atomically
downgradable, so `downgradeExclusiveToShared` does the flock downgrade first (a real
second process is excluded throughout by the unbroken flock hold) and then flips the
in-process mutex Unlock()→RLock(). That leaves at most an IN-PROCESS window, which is
harmless: a same-process restore/migration would itself need both the in-process
write lock AND the flock, and the flock never goes to "unlocked / foreign-grantable"
across the call. A REAL second process can only act after the single in-kernel EX→SH
transition. Pinned by `TestOpen_RiskyUpgrade_NoHandleAcrossLockTransition` (in-
process race via a test seam) and `TestOpen_RiskyUpgrade_BlocksOnForeignExclusive`.

### 2. PRUNE IS LOCKED (Finding 2)

`Prune` acquires the EXCLUSIVE DB lock (bounded wait, fail closed with `ErrDBLocked`)
and re-checks `restoreMarkerPending` UNDER the lock before deleting anything. So
prune SERIALIZES against a risky migration (EXCLUSIVE across restore-point creation +
DDL) and a restore (EXCLUSIVE for its whole span): it can never delete
`manifest.json` / `snapshot.db` — the only recovery material — out from under an
in-flight migration/restore, nor after a concurrent restore passed its pre-marker
checks. The marker is checked BEFORE taking the lock (fast refusal) AND again under
the lock (a restore that wrote its marker while prune waited still stops it). Pinned
by `TestPrune_FailsClosedWhileExclusiveLockHeld` (and the existing
`TestPrune_RefusesWhileRestoreMarkerPending`).

### 3. THE LOCK OUTLIVES THE LAST LIVE HANDLE (Finding 3)

`DB.Close()` closes the underlying `sql.DB` FIRST (which can block until in-flight
queries drain and the SQLite file handles actually close), and only THEN releases
the flock / RWMutex. A restore's EXCLUSIVE acquire therefore cannot be granted while
any SQLite handle to the path is still alive. Pinned by
`TestDBClose_LockOutlivesSQLHandle`.

### 4. REUSE RE-VALIDATES THE RESTORE POINT (Finding 4)

Before REUSING an existing restore point to cover a risky migration,
`createRestorePointLocked` runs the SAME `PRAGMA integrity_check` + snapshot
schema-version validation that creation/restore do — `loadValidManifest` only proves
shape + hash + size, so a self-consistent manifest beside a non-SQLite `snapshot.db`
(matching recorded hash) would otherwise be reused and the risky migration would
proceed with an unusable restore point (restore later fails integrity_check). On
failure it does NOT reuse and does NOT silently proceed: it fails closed with a
prune/recreate message. Pinned by `TestReuse_NonSQLiteSnapshotFailsClosed`.

### 5. RESTORE WAL/SHM CLEANUP IS NOT BEST-EFFORT (Finding 5)

After publishing the restored DB, a failure to remove a stale live `-wal`/`-shm` is a
restore ERROR, not a discarded best-effort — returning success with a stale WAL
beside the restored DB is a false success (the recovery paths already error on the
same op; normal restore now matches). Pinned by
`TestRestore_StaleWALRemovalFailureIsError`.

---

This note records the recovery contract for the migration restore point after the
third cross-model adversarial review (Codex) found the prior crash-recovery model
itself unsafe. The operator approved a model pivot; this is the model now in code.

## The flaw the pivot fixes

Previously, **every `store.Open` auto-resumed a restore marker** — it completed or
rolled back an interrupted restore by acting on the marker's on-disk fields. A
marker is just a file. A crash, on-disk corruption, OR an attacker could write one,
and a routine open (e.g. `continuity profile`) would then trust it to drive
destructive file moves: renaming a `<db>.pre-restore.*`-prefixed file over the live
DB, or fabricating a fresh DB while another process was mid-restore. That is the
root flaw: **a forgeable marker drove destructive action on an innocent open.**

## The contract now (FAIL CLOSED)

1. **`Open()` and `OpenNoMigrate()` NEVER recover.** Before any `sql.Open` or file
   creation they call `detectRestoreInterrupted(path)`:
   - Derive the canonical sidecar from the path (parent-dir symlinks resolved,
     leaf kept — see `canonicalDBPath`).
   - If a restore marker is **present**, return the sentinel `ErrRestoreInterrupted`.
     Do not `sql.Open`, do not create a DB, do not touch any file.
   - A **corrupt / unparseable / partial** marker (`{}`, bad JSON, missing version
     or required fields) is **also** `ErrRestoreInterrupted` — fail closed; do not
     erase it, do not fabricate a DB over it.
   - A symlinked sidecar (a redirection attack) is likewise refused.
   - A regular-file-where-the-sidecar-dir-should-be is **not** a pending restore
     (no marker dir can exist); the migration path fails closed on it separately.

2. **Operator-facing message.** Non-server commands that reach `store.Open` via
   `openDB()` (profile, tree, dedup, …) and `serve` therefore fail closed with:
   `an interrupted restore is pending for <db>; run \`continuity snapshot restore
   --confirm\` to complete recovery.`

3. **Recovery runs ONLY under explicit operator intent.** In the
   `snapshot restore --confirm` path, `store.Restore`:
   - Acquires the serve lock and holds it for the entire restore.
   - Calls `recoverPendingRestore(dbPath)` BEFORE opening the DB, under the lock,
     with FULL validation, in this order:
     1. `assertNotSymlink(sidecar)` — refuse a redirected sidecar.
     2. `readRestoreMarker` — unparseable ⇒ fail closed.
     3. `validateMarkerSchema` — hard gate: `version == 1` and required fields
        present/well-formed (a `{}`/partial marker stops here, preserved).
     4. `resolveCanonicalRestore` — every path the marker names is constrained to
        the canonical set (live triplet, staged in the DB dir, backup prefixed
        `<resolvedDB>.pre-restore.`); anything outside fails closed untouched.
     5. `finishPendingRestore` — complete (if `db_published`) or roll back to the
        moved-aside originals; only files this recovery created/verified are moved.
   - Then proceeds with a fresh restore on the now-clean DB.

4. **Net effect:** a crash mid-restore no longer self-heals on the next innocent
   open. The operator re-runs `snapshot restore --confirm`, which recovers under
   the lock with full proof. Recovery never moves an unproven file over the live DB.

## Recovery RECONCILES against reality (Round 4, Findings 1 & 2)

The third-round model still drove recovery off the marker's *claimed* phase
(`db_published`) and its path fields, validating lineage only *after* it had
already begun. A planted or stale marker beside an absent/corrupt restore point
could therefore remove the live DB and rename a `<db>.pre-restore.*` file over it
before failing with "no restore point". The recovery contract is now:

1. **Schema + path gates first** (unchanged): `validateMarkerSchema`, then
   `resolveCanonicalRestore` constrains every marker path to this DB's canonical
   set. Anything outside fails closed untouched.

2. **REALITY GATE — prove the restore point BEFORE touching anything.** Recovery
   calls `loadValidManifest` (manifest shape + `snapshot.db` sha256 + schema).
   **If there is NO valid restore point, FAIL CLOSED — touch nothing.** A
   forged/stale marker can no longer trigger a destructive rename/remove.

3. **Determine the ACTUAL state from disk, never the `db_published` bit**
   (`reconcilePendingRestore`):
   - **live DB present AND its sha256 == the snapshot's sha256** → treat as
     PUBLISHED: complete (scrub stale `-wal`/`-shm`, drop staged), remove the
     marker. **Never roll back** — a stale pre-publish marker cannot clobber the
     already-restored DB.
   - **live DB absent AND the DB backup present AND staged present** → genuine
     pre-publish torn state → roll back, **but only after provenance**: the
     moved-aside backup's sha256 must equal `original_db_sha256`, which Restore
     records in the marker **at restore start, before moving the DB aside**. A
     mismatch (planted/stale/corrupt backup) → FAIL CLOSED; the unprovable file is
     never renamed over the DB.
   - **anything else (inconsistent)** → FAIL CLOSED, touch nothing.

4. **Crash-safe post-publish transition (Finding 2).** After a real publish the
   marker is durably removed (`clearPublishedRestoreMarker`: remove + `fsyncDir`).
   If it cannot be cleared, Restore **fails LOUDLY** — it returns an error telling
   the operator the restore SUCCEEDED but the marker must be cleared by hand —
   rather than returning success with a recovery-implying marker. Combined with
   the reality gate (live == snapshot ⇒ complete), a marker still saying
   `db_published:false` after a successful publish can no longer cause a future
   rollback: the next recovery hashes the live DB, sees it equals the snapshot,
   and COMPLETES.

   The intermediate `db_published:true` marker write was REMOVED — it was the
   stale-marker hazard, and the disk-truth reconcile makes the phase bit advisory.

### Threat model for the provenance check

The `original_db_sha256` provenance check defends the realistic **crash /
corruption / stale-marker** cases: a `<db>.pre-restore.*` left by a crash, a
truncated/partially-written backup, or a marker an attacker planted to point
rollback at a hostile file. It is **NOT** claimed to stop a local attacker who
already owns the DB directory — such an attacker can corrupt the live DB directly,
and could also recompute a matching hash into the marker. The guarantee is: a
mismatched/unprovable backup is **never** renamed over the live DB, and recovery
**never destroys before proving** a valid restore point exists.

## Restore serializes against migrating opens (Finding 3)

`Restore` now acquires the snapshot **operation lock** (`acquireSnapshotOpLock`)
in addition to the serve lock. Direct CLI commands (profile/tree/dedup) migrate
via `openDB()` → `store.Open` **without** the serve lock; a risky migration holds
the op-lock across its destructive DDL. Holding the op-lock in Restore makes
restore and any migrating Open serialize — the loser waits the bounded window then
fails closed (`ErrSnapshotOpLocked`), so a restore can never swap the DB out from
under SQLite handles a live migration holds. The serve lock is still held too
(serializes restore vs serve).

## Migration serialization is decoupled from the snapshot opt-out (Finding 4)

A risky on-disk upgrade against an eligible path acquires the op-lock **regardless
of** `CONTINUITY_DISABLE_MIGRATION_SNAPSHOT`. The env var suppresses only
*creating the restore point* (inside `ensureUpgradeRestorePoint*`), never the
lock/serialization boundary. Two opt-out processes can therefore no longer both
enter the destructive `mem_nodes` rebuild concurrently and tear the schema; one
upgrades, the other waits/fails closed.

## Lockfiles are atomic and PID-less files are reclaimable (Finding 5)

Serve/op lockfiles are now published **atomically** (`writeLockfileAtomic`):
an O_EXCL temp containing the PID is fsync'd and renamed into place, so the
lockfile is **never observably PID-less**. Correspondingly, an existing
**zero-length / unparseable** lockfile is treated as **STALE/reclaimable**, not as
a permanent live lock — closing the wedge where a crash between "create file" and
"write PID" left a PID-less lock that blocked serve/restore/migrations forever. A
well-formed live-PID lock still blocks.

## One path resolution rule — DIRECTORY symlinks resolved, LEAF kept

> **Scoping cut (operator-approved):** support for a symlinked DB **FILE** (leaf)
> was DROPPED. It was a recurring complexity/bug source across nine review rounds
> (leaf-symlink resolution, dangling-leaf recovery, lock/sidecar divergence). The
> retained, simpler rule below resolves only **parent-directory** symlinks — which
> are stable, since continuity never moves directories — and keeps the leaf
> verbatim, so the derivation can never dangle.

`canonicalDBPath` is the single derivation for the real DB path. Both
`sidecarPath` and `dbLockPath` route through it, so the lock and the sidecar are
always keyed to the **same** real DB. The rule is exactly:

```
canonical = filepath.Join(EvalSymlinks(filepath.Dir(abs)), filepath.Base(abs))
```

It resolves the **directory's** symlinks (e.g. macOS `/var → /private/var`) and
**keeps the real leaf**. Because the parent dir is stable, this returns the same
answer whether or not the DB file exists yet, with **no** `EvalSymlinks` on the
full path and **no** `os.Readlink` — the dangling-leaf machinery
(`resolveDBPathSurvivingDangling`, `resolveViaParentDir`, the Readlink fallbacks,
and all "survives dangling symlink" logic) is **deleted**.

This still closes the original hole (a serve-via-symlinked-dir and a
restore-via-real-path contend on **one** lock and share **one** sidecar) without
any leaf-symlink resolution.

### A symlinked DB FILE (leaf) = REFUSED for ALL operations (Round 11)

> **Round 11 scoping cut (operator-approved):** the Round-10 "keep the symlink leaf,
> skip snapshots + proceed" approach was a **broken middle**. It split lock / sidecar
> / marker ownership between the symlink and its target and let a risky migration run
> on a symlinked-leaf DB **unprotected**. The clean fix replaces it: a symlinked DB
> **file** is **REFUSED entirely** — every DB open and every path-derived snapshot
> operation **fails closed** before touching any file.

A symlinked DB **file** is detected by `refuseSymlinkedDBLeaf` (lstat the abs path,
`ModeSymlink` set → `ErrSymlinkedDBUnsupported`). The refusal is the SINGLE up-front
gate every entry point runs **before** any `MkdirAll` / lock acquire / `sql.Open` /
marker check / sidecar derivation:

- **`store.Open` and `store.OpenNoMigrate`** return `ErrSymlinkedDBUnsupported`
  immediately — BEFORE the interrupted-restore marker check, the lock, and
  `sql.Open`. The message is actionable: *"continuity does not support a symlinked
  database file `<path>`; set CONTINUITY_DB to the real file"*. Therefore `serve`
  and **every** `openDB()` CLI command (profile/tree/dedup/remember/retract/import/
  extract) fail closed with this message.
- **`Status` / `Restore` / `Prune`** (which derive the sidecar from the path) refuse
  with the **same** `ErrSymlinkedDBUnsupported` (NOT `ErrNoRestorePoint` — a
  symlinked leaf is an **unsupported configuration**, not an absent restore point).
  Restore/Prune refuse inside `probeRestorePointAbsent` (run first), so no lock file
  is ever created or contended.
- **No "proceed unprotected" case exists.** A symlinked-leaf DB never reaches
  migration (Open refuses first), so `ensureUpgradeRestorePoint*` always sees a real,
  non-symlinked leaf. The Round-10 `snapshotLeafIsSymlink`-based skip blocks,
  `warnSnapshotSymlinkedLeaf`, and the one-time warning were **DELETED**.

Parent-**directory** symlinks (real leaf) remain **fully supported**:
`canonicalDBPath` resolves only the parent dir (`filepath.Join(EvalSymlinks(Dir(abs)),
Base(abs))`) and keeps the verbatim leaf, so it now only ever sees a real
(non-symlinked) leaf. `snapshotEligiblePath` is unchanged (a path-SHAPE check:
`:memory:`/URI/reserved-char). Pinned by `TestSnapshot_SymlinkedDBLeaf_Unsupported`
(Open/OpenNoMigrate/Status/Restore/Prune all fail closed with
`ErrSymlinkedDBUnsupported`, NO sidecar/lock/marker created beside either the link or
the real DB, real DB byte-untouched) and the retained parent-dir-symlink tests
(`TestSidecarPath_ParentDirSymlinkResolves`,
`TestCanonicalDBPath_ParentDirSymlinkAgreesLockAndSidecar`,
`TestDBLock_ParentDirSymlinkUnifiedWithReal`).

### Managed files are NEVER opened through a symlink (the "keep half")

Independently of the leaf rule, **every** open of a file continuity manages inside
the sidecar / DB dir — `snapshot.db`, `manifest.json`, `restore.in-progress.json`,
the `.pre-restore.*` backups, the `.restore.staged.*` temps — goes through ONE
shared gate, `openManagedFileNoFollow` (built on `openControlFileNoFollow`):
`O_NOFOLLOW` + an `fstat` regular-file check. A symlink / FIFO / device / socket /
directory at a managed-file path fails closed as `ErrSnapshotSidecarCorrupt`. Both
the control-file reader (`readControlFileNoFollow` → `readManifest` /
`readRestoreMarker`) and the hash path (`hashFileNoFollow`, used for the backup /
staged / live provenance checks) route through this one primitive, so a planted
symlink in our **own** sidecar is **always** refused — regardless of the
leaf-symlink rule above. Pinned by `TestManagedFileGate_SymlinkOrFIFORejected` and
`TestControlFiles_SymlinkOrFIFO_RejectedAsCorrupt`.

## Durability (Finding 5)

After each durability-critical rename — `snapshot.db`, `manifest.json`, the restore
marker, and the moved-aside triplet — the containing directory is `fsync`'d
(`fsyncDir`). A power loss then leaves a durable restore point / durable marker /
durable moved-aside originals rather than losing a synced file whose directory
entry never reached disk. `fsyncDir` is best-effort (logged, non-fatal) on a
filesystem that cannot sync a directory handle.

## Boot expiry is lineage-gated (Finding 8)

`RecordSuccessfulBoot` recomputes the lineage fingerprint from the LIVE DB and only
ticks/expires a sidecar whose lineage MATCHES this DB. A transplanted/foreign
sidecar (different `instance_id`) is left entirely untouched — boot expiry never
auto-deletes unproven restore material.

## Serve-lock same-process reentry (Finding 9)

The serve lock is single-owner WITHIN a process via a path-keyed in-process
registry. A second same-PID acquire while the first is outstanding is treated as
CONTENTION (`ErrServeLockHeld`), not a silent share of one lock file. The file is
removed only when the in-process owner releases AND the file still records our PID;
releasers are idempotent, so a release after the owner already released cannot
strand a different acquirer or remove a lock we no longer own.

## instance_id is IDENTITY, not tracking metadata (Finding 6)

`instance_id` (in `continuity_meta`) is per-DB IDENTITY: intentionally written into
the DB and intentionally copy-preserved (`cp` / `VACUUM INTO` carry it), so a
snapshot matches its source. That is categorically different from the
snapshot-TRACKING metadata the design keeps OUT of the DB (no absolute paths, no
manifest rows in the DB). It is established inside `writeRestorePoint` only after
the sidecar is proven usable and before `VACUUM INTO` (so a blocked sidecar leaves
the DB unmutated). A stray identity row left by a snapshot that fails AFTER that
write is BENIGN (no data/schema loss, DB stays at its pre-version). On ANY
snapshot-creation failure the partial SIDECAR we created this call is removed
(never a pre-existing or foreign-populated one); the benign identity row is left.

### `instance_id` in the DB is ACCEPTED DESIGN (Round 11, Change 3)

Codex has repeatedly flagged `continuity_meta.instance_id` as "metadata inside the
DB," contrasting it with the design's stance that snapshot-tracking metadata stays
OUT of the DB. This is **accepted design, not a defect**, and is pinned here so it is
not re-litigated each round:

- `instance_id` is **per-DB IDENTITY**, not snapshot-tracking metadata. It records
  *which database this is*, not *whether/where a snapshot exists*. The "no metadata in
  the DB" rule is specifically about **snapshot tracking** (no absolute paths, no
  manifest rows, no "a restore point exists" flag) so that a copied/renamed DB does
  not inherit stale snapshot state — list/status/prune are path-owned and never open
  the DB. Identity is the opposite concern.
- It is **required to live in the DB** precisely so a faithful copy (`cp` /
  `VACUUM INTO`) **carries it** — that is what lets a snapshot's lineage fingerprint
  match its source DB and reject a sidecar transplanted next to an unrelated DB. An
  out-of-DB identity could not survive a copy and would defeat the lineage gate.
- A **benign identity row persisting after a failed snapshot is accepted**: it causes
  **no data or schema loss** (the DB stays at its pre-version), and a later successful
  snapshot reuses the same id. We scrub the partial SIDECAR on failure, never the
  identity row. (See "`instance_id` is IDENTITY, not tracking metadata" above.)

No code change for this item — it documents the accepted boundary.

## Failed snapshot-creation leaves NO partial sidecar (Round 11, Change 2)

`writeRestorePoint`'s `failClosed` cleanup removes the **whole sidecar content it
created this call** — BOTH `snapshot.db` AND any `manifest.json` this attempt
published — not just the named temp. Previously the manifest-failure branch removed
only `snapshot.db`, so a manifest publish that failed **after** `manifest.json` was
already renamed in (a transient fsync/publish failure) left a **manifest-only** (or
`snapshot.db`-only) sidecar. Such a partial sidecar fails closed on **every** later
`Open`/`Status` (`loadValidManifest` → corrupt/"snapshot missing") AND `Prune`
**refuses** to delete it (it is not a valid restore point) — wedging the DB. The
cleanup now scrubs both files (provably ours — the op-lock is held and every name was
`O_EXCL`-created/renamed this call), so a transient creation failure leaves the
sidecar with **neither** file (or removed entirely if we created the dir), and a
subsequent `Open` is never blocked by a corrupt sidecar. Test seam
`hookAfterManifestRename` (fires after the `manifest.json` rename); pinned by
`TestCreate_ManifestPublishFailureLeavesNoPartialSidecar`.

## KNOWN LIMITATION — fork ambiguity (Finding 7) — DO NOT cross-pollinate sidecars

A DB and a faithful COPY of it share `instance_id` **by design** (that is what lets
a snapshot match its source). Consequently:

> `cp A.db → B.db` makes B inherit A's identity. If you then diverge B and drop
> A's sidecar next to B, the lineage check PASSES and a restore will replace B's
> data with A's snapshot.

This is an inherent fork ambiguity we do **not** claim to defend against in v1. The
restore point protects **a database and faithful copies of it** — it cannot tell a
faithful copy apart from the original. **Operators must not move/copy a `<db>.snapshot`
sidecar between forked copies of one database.** This behavior is PINNED by
`TestRestore_ForkAmbiguityIsPinned` so it cannot change silently; if a future
version adds fork divergence detection, that test is the one to revisit.

## OS flock lock discipline (Round 5) — replaces the hand-rolled PID lock

The fifth cross-model adversarial review (Codex) found the hand-rolled PID
lockfile the recurring source of concurrency bugs:

- **(Round-5 Finding 1)** the "atomic" PID lockfile created a zero-length
  sentinel at the final path before the PID rename. A peer observing that window
  treated it as stale, removed it, and BOTH processes ended up "holding" the lock.
- **(Round-5 Finding 5)** restore only excluded serve + risky migrations; ordinary
  writable opens (dedup/remember/retract/import/extract via `openDB()` / `store.Open`)
  held NO lock, so `snapshot restore --confirm` could rename the DB triplet out from
  under an active SQLite connection.

### The lock

A proper advisory lock on a per-DB lock file `<resolvedDB>.lock`, keyed through the
single `canonicalDBPath` derivation (same real DB as the sidecar/backups):

- **Cross-process:** `flock(2)` — `LOCK_SH` / `LOCK_EX` (unix, `snapshot_proc_unix.go`),
  `LockFileEx`/`UnlockFileEx` (windows, `snapshot_proc_windows.go`). flock is
  kernel-managed (no zero-length window) and **auto-releases on close AND on
  process death**, so the PID-liveness / stale-reclaim / zero-length machinery the
  bug came from is DELETED.
- **In-process:** a process-local `RWMutex` registry keyed by the canonical lock
  path (flock across goroutines of one process is unreliable). `SHARED = RLock +
  LOCK_SH`; `EXCLUSIVE = Lock + LOCK_EX`. (`snapshot_lock.go`.)

### Discipline

- Every **WRITABLE open** (`store.Open`, used by serve AND `openDB()` CLI commands)
  takes a **SHARED** lock held for the connection's lifetime (released by `DB.Close`).
- **Restore** takes an **EXCLUSIVE** lock for the whole operation — non-blocking with
  a bounded wait: if shared/other holders exist it waits ~5s then **FAILS CLOSED**
  (`ErrDBLocked`, "database is in use; stop other continuity processes and retry").
  This is what makes restore exclude EVERY writable open, not just serve.
- A **risky migration** takes EXCLUSIVE across restore-point creation + the migration
  loop. Because the writable `Open` already holds SHARED, the risky path **downgrades**:
  release shared → take exclusive (bounded, fail-closed) → run → re-acquire shared.
  (flock is per-open-file-description: two fds of one process DO conflict, so the
  shared fd must be released before the exclusive acquire; the in-process RWMutex is
  not re-entrant either. See `acquireMigrateExclusive`.)
- A new writable open while EXCLUSIVE is held cannot reach `sql.Open` — `Open` checks
  the interrupted-restore marker AND acquires SHARED (which `LOCK_SH` blocks until the
  exclusive restore releases) BEFORE any `MkdirAll`/`hardenPermissions` (Round-5
  Finding 5: a pending-restore Open is truly no-touch, failing closed before any
  chmod/mkdir).
- **Reads** (`OpenNoMigrate`, status, prune inspection) do NOT take the lock —
  EXCEPT prune's deletion and restore's recovery, which run under the exclusive
  restore (or are gated by the pending-marker refusal, below).
- **serve** no longer takes a dedicated serve-lock: it relies on `store.Open`'s SHARED
  lock, and refuses to start if it cannot get its shared open (an exclusive restore in
  progress → `ErrRestoreInterrupted` / lock error). A crashed exclusive holder's flock
  auto-releases, so a dead serve never wedges the next process.

The marker-based `ErrRestoreInterrupted` fail-closed on `Open` is KEPT and is
orthogonal: it detects a CRASHED restore, while the flock lock excludes LIVE writers.

## Bounded recovery / safety edges (Round 5)

- **Finding 2 — safe pre-rename abort.** A crash AFTER the marker write but BEFORE
  the first move-aside rename leaves the live DB as the untouched ORIGINAL with no
  backup. `reconcilePendingRestore` now has CASE A2: live DB present AND
  `live_hash == original_db_sha256` AND no DB backup ⇒ no destructive step happened
  ⇒ clear the marker, drop any staged temp, leave the original intact. Without it
  the DB was wedged at `ErrRestoreInterrupted` forever.
- **Finding 3 — staged-temp ownership.** Restore no longer close-then-reopens the
  staged path by name. It copies the snapshot into the STILL-OPEN owned fd
  (`copyFileToOpenFd`), so a mid-restore symlink swap of the path cannot redirect the
  write, and it asserts the staged path is a regular file (`assertRegularFile`) before
  the publish rename — a swapped symlink is never published as the live DB.
- **Finding 4 — prune refuses while a marker is pending.** If a restore crashed, the
  manifest + `snapshot.db` are the only recovery material. `Prune` now checks
  `restoreMarkerPending` FIRST and refuses (`ErrRestoreInterrupted`) so it never
  deletes recovery material out from under a pending restore.
- **Finding 5 — Open no-touch ordering.** `Open` checks the interrupted-restore
  marker and acquires the SHARED lock BEFORE `MkdirAll`/`hardenPermissions`, so a
  pending-restore Open fails closed without chmod'ing/mkdir'ing anything.
