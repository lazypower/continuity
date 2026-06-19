codex
I recommend the path-owned sidecar, with one hardening: store a lineage fingerprint in the manifest, derived from existing `schema_versions` rows, so restore can reject a sidecar manually transplanted next to an unrelated DB. No snapshot metadata goes into the DB.

I found two checkout mismatches: [migrations.go](/Users/chuck/Code/claude/continuity-restore-point/internal/store/migrations.go:7) has no `Risky` field yet, and [db.go](/Users/chuck/Code/claude/continuity-restore-point/internal/store/db.go:29) has no `OpenNoMigrate`. The spec below treats both as implementation work.

**1. Recommended Design**

Use a single path-owned upgrade restore point:

- DB `/x/continuity.db`
- sidecar `/x/continuity.db.snapshot/`
- final files: `snapshot.db`, `manifest.json`
- snapshot taken once before any pending migration runs if the pending set contains at least one `Risky` migration
- v6 and v9 must be marked `Risky: true`
- no `migration_snapshots` table, no absolute paths, no global snapshot tree, no broad scans
- list/prune derive the sidecar from the selected DB path and never open the DB
- restore is explicit: `continuity snapshot restore --confirm`

Keep `VACUUM INTO`, not file copying or manual `db/wal/shm` copying. `VACUUM INTO` is the right primitive because it asks SQLite for a consistent self-contained database image, including committed WAL content visible to SQLite.

Rejected alternatives:

- SQLite Online Backup API: good primitive, but harder through `database/sql`/modernc and not meaningfully safer than `VACUUM INTO` here.
- single `.bak` beside DB: simpler, but loses manifest, boot retention, hashes, and failure diagnostics.
- `ATTACH` snapshot DB: still external artifact complexity, worse operational clarity.
- copying `.db`, `-wal`, `-shm`: too easy to get wrong; do not build this feature on file-level SQLite copying.

Where sidecar can still bite:

- If an operator manually moves a sidecar next to the wrong DB, pure path ownership cannot prove logical ownership. Mitigation: manifest includes `lineage_fingerprint = sha256(schema_versions rows <= pre_schema_version)`. Restore opens the current DB read-only/no-migrate and refuses if the fingerprint does not match.
- If the current DB is so corrupt that `schema_versions` cannot be read, restore refuses by default. That is acceptable for this feature: it is an upgrade safety net, not physical corruption recovery.
- If an old active restore point still exists when a future risky migration arrives, do not overwrite it. Fail closed and tell the operator to restore or prune explicitly.

**2. On-Disk Layout + Manifest Schema**

For a normal filesystem DB path:

```text
/x/continuity.db
/x/continuity.db-wal
/x/continuity.db-shm
/x/continuity.db.snapshot/
  snapshot.db
  manifest.json
```

Canonical sidecar path:

1. reject `:memory:` for snapshot CLI; `OpenMemory` skips snapshots.
2. reject SQLite URI/DSN paths for automatic risky snapshots unless explicitly opted out.
3. `abs = filepath.Abs(path)`
4. if DB exists, `resolved = filepath.EvalSymlinks(abs)`; otherwise `resolved = abs`
5. sidecar = `resolved + ".snapshot"`

Relative and absolute references to the same real DB must resolve to the same sidecar. Sidecar path itself must not be a symlink. `snapshot.db` must be a regular file, not a symlink.

Manifest schema, version 1:

```json
{
  "kind": "continuity.upgrade_restore_point",
  "format_version": 1,
  "snapshot_file": "snapshot.db",
  "created_at": "2026-06-19T00:00:00Z",
  "created_by_version": "continuity <version>",
  "pre_schema_version": 5,
  "target_schema_version": 9,
  "first_risky_schema_version": 6,
  "lineage_fingerprint": "sha256:<hex>",
  "snapshot_sha256": "sha256:<hex>",
  "snapshot_size_bytes": 123456,
  "successful_boots": 0,
  "expires_after_successful_boots": 3,
  "last_successful_boot_at": null,
  "restore_count": 0,
  "last_restored_at": null
}
```

Only `snapshot_file` is a filename, and in v1 it must equal exactly `snapshot.db`. No absolute paths. No `..`. No path separators.

Missing DB directory:

- `serve`/`Open` keep existing behavior: create DB parent dir.
- fresh install skips snapshot creation.
- `snapshot list/status/restore/prune` do not create DB dirs; they report “no restore point”.

**3. Lifecycle**

Add:

```go
type migration struct {
    Version     int
    Description string
    SQL         string
    Risky       bool
}
```

Mark v6 and v9 risky because they rebuild `mem_nodes` via create/copy/drop/rename.

Migration flow:

1. `Open(path)` creates/configures DB as today.
2. `migrate()` creates `schema_versions` if needed, reads current version, and applies the forward-compat guard.
3. Build the pending migration list.
4. If no pending risky migration: migrate normally.
5. If pending risky migration exists and DB is an existing on-disk DB with `pre_schema_version > 0`: create or reuse one restore point before applying any pending migration.
6. If snapshot creation/reuse validation fails: abort before any pending migration runs.
7. If `CONTINUITY_DISABLE_MIGRATION_SNAPSHOT=1`: skip creation and proceed, with a clear warning. Exact value `1` only.

Snapshot creation:

- acquire sidecar operation lock
- if valid active manifest exists for this lineage, reuse it; never overwrite
- if corrupt/partial sidecar exists, fail closed
- create `snapshot.tmp.<pid>` via `VACUUM INTO`
- validate `PRAGMA integrity_check` on temp snapshot
- verify snapshot schema version equals `pre_schema_version`
- hash snapshot
- rename temp snapshot to `snapshot.db`
- write manifest temp, fsync, rename to `manifest.json`
- chmod sidecar `0700`, files `0600`

Successful boot:

- change [serve.go](/Users/chuck/Code/claude/continuity-restore-point/internal/cli/serve.go:171) to use explicit `net.Listen`.
- A successful boot is: DB opened/migrated, engine setup completed enough to construct HTTP server, and `net.Listen("tcp", addr)` succeeds.
- Only after bind succeeds, increment `successful_boots` for a valid active manifest whose `target_schema_version <= current_schema_version`.
- When `successful_boots >= expires_after_successful_boots`, delete only `snapshot.db` and `manifest.json` after manifest/hash validation. Leave unrelated files untouched. If validation fails, log and leave it.

Default retention: `3` successful serve boots.

**4. Restore Path**

Commands:

```text
continuity snapshot status
continuity snapshot prune --confirm
continuity snapshot restore --confirm
```

Restore behavior:

- resolve DB path with the same canonicalization as snapshot creation
- load manifest from derived sidecar only
- validate manifest schema, relative filename, regular files, snapshot hash, snapshot integrity
- open current DB read-only/no-migrate and recompute `lineage_fingerprint`
- require current schema version between `pre_schema_version` and `target_schema_version`, inclusive
- refuse if a live `serve` lock exists; operator must stop `continuity serve`
- copy `snapshot.db` to a temp file in the DB directory
- write a restore journal in the sidecar
- move current `db`, `db-wal`, and `db-shm` aside to timestamped pre-restore backup names
- rename temp file to the DB path
- ensure no old `db-wal` or `db-shm` remains at original names
- update manifest `restore_count` and `last_restored_at`

Restore is destructive to the selected DB path but should not delete the previous DB triplet; it renames it aside. This keeps WAL correctness and gives crash recovery material without pretending to be a backup system.

**5. Failure-Mode Table**

| Failure | Intended Behavior |
|---|---|
| snapshot create fails | Abort before pending migrations. DB remains at pre-upgrade version. |
| disk full during snapshot | Abort; remove only temp files created by this process; never touch existing manifest/snapshot. |
| partial snapshot temp | Not valid. Future creation ignores own temp names or fails if final names are ambiguous. |
| crash after `snapshot.db` rename before manifest | Sidecar is partial. Future migration fails closed; restore unavailable. |
| crash after manifest before migration | Future migration reuses valid restore point. |
| crash mid-migration | SQLite rolls back current transaction. Future run reuses existing pre-upgrade snapshot and continues. |
| crash after v6 before v9 | Existing pre-upgrade snapshot remains; v9 does not replace it. |
| crash mid-restore | Restore journal drives resume or rollback. Never leave stale original `-wal/-shm` beside restored DB. |
| copied DB without sidecar | New path has no restore point. No stale metadata exists. |
| copied DB with sidecar | Valid local restore point; lineage matches because DB was copied with it. |
| sidecar copied to unrelated DB | Restore refuses on lineage fingerprint mismatch. Prune only affects local sidecar if manifest is valid. |
| renamed DB without sidecar | No restore point. |
| renamed DB with sidecar renamed too | Restore point remains valid; no absolute paths break. |
| relative vs absolute path | Same canonical sidecar after `Abs` + `EvalSymlinks`. |
| two DBs same dir | `a.db.snapshot` and `b.db.snapshot`; no shared files. |
| concurrent serve same DB | Second serve refuses via live serve lock before boot tick. |
| concurrent migration opens | Operation lock serializes snapshot/migration; loser waits briefly then fails closed. |
| sidecar manually deleted | No restore point. Migration may create a new one only before risky migration. |
| manifest corrupt/partial | No restore/prune/delete. Risky migration fails closed. |
| snapshot present, manifest absent | Partial/unknown; fail closed; do not overwrite/delete automatically. |
| manifest present, snapshot absent | Invalid; fail closed; restore unavailable; no delete except explicit manual operator action outside CLI. |
| snapshot hash mismatch | Restore refuses; prune refuses. |
| sidecar is symlink | Refuse. |
| snapshot file is symlink | Refuse. |
| fresh install | No snapshot. Migrate to head normally. |
| `:memory:` | No snapshot support; memory tests continue to migrate. |
| SQLite URI/DSN path | Risky migration fails closed unless snapshot opt-out is set. |
| opt-out env set | Risky migration proceeds without restore point; warning is emitted. |

**6. Invariants**

- No snapshot metadata is stored in application DB tables.
- No manifest stores an absolute path.
- List/status/prune never open the DB.
- CLI never deletes anything outside the derived sidecar.
- CLI never deletes corrupt or unproven sidecar contents.
- Automatic snapshot is created at most once per upgrade run.
- A later risky migration in the same run never replaces the earlier restore point.
- If a risky snapshot is required and cannot be validated, no pending migration runs.
- Restore refuses if current DB lineage does not match the manifest.
- Restore removes or moves stale target `-wal`/`-shm` before the restored DB is used.
- Boot retention increments only after successful TCP bind.
- Fresh installs do not create meaningless empty restore points.

**7. Test Plan**

Unit tests:

- sidecar path canonicalization: relative, absolute, symlinked DB, sibling DBs.
- manifest validation rejects absolute paths, `..`, separators, wrong kind/version, symlink snapshot.
- lineage fingerprint stable across migration and DB copy; mismatch for unrelated DB.
- migration planning snapshots once when pending set includes v6/v9.
- v5 to v9 creates pre-v5 snapshot, not pre-v9.
- existing valid manifest is reused, not overwritten.
- corrupt/partial sidecar fails closed.
- expiry deletes only validated `snapshot.db` and `manifest.json`.
- restore staging moves `db`, `db-wal`, `db-shm` aside and leaves restored DB clean.
- restore journal resumes crash phases.

Subprocess tests using existing harness from [migration_e2e_test.go](/Users/chuck/Code/claude/continuity-restore-point/internal/store/migration_e2e_test.go:19):

- upgrade v5 to head creates sidecar and manifest with `pre_schema_version=5`, `first_risky_schema_version=6`.
- v5 to head preserves data and restore returns DB to v5 data.
- WAL-active committed data before migration appears in `snapshot.db`.
- make `<db>.snapshot` a regular file; serve fails and DB remains v5.
- copy migrated DB to `scratch.db` without sidecar; `snapshot status` reports none.
- copy DB plus sidecar; restore works locally.
- copy only sidecar to sibling DB; restore refuses lineage mismatch.
- run serve using relative `CONTINUITY_DB`, then status using absolute path; same sidecar.
- two DBs in same dir create/prune independent sidecars.
- start two `serve` processes for same DB; second exits, no boot tick.
- corrupt manifest and missing snapshot permutations block restore/prune.
- successful boot count increments only after health endpoint is reachable; failed bind does not increment.
- after 3 successful boots, sidecar final files expire and no DB file is touched.
