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
