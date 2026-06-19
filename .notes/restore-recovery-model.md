# Restore Recovery Model — fail-closed pivot (Round 3)

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
   - Derive the canonical sidecar from the path (surviving dangling symlinks — see
     `canonicalDBPath`).
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

## One path resolution rule (Finding 3)

`canonicalDBPath` is the single derivation for the real DB path. Both
`sidecarPath` and `serveLockPath` route through it, so the lock and the sidecar
are always keyed to the **same** real DB. It survives:
- a symlinked DB (`EvalSymlinks` when present),
- a **dangling** symlink (`os.Readlink` of the link target), and
- a **missing** target (resolve the parent dir, e.g. macOS `/var → /private/var`).

This closes the hole where serve acquired `<link>.serve.lock` while the
sidecar/marker lived under `<real>.snapshot`, letting a serve-via-link and a
restore-via-real-path contend on different locks for one database.

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
