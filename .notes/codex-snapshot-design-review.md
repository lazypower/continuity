# Codex design review — migration safety snapshots (PR #31)
_Read-only `codex exec` (gpt-5.5), after 4 rounds of correctness fixes. Drawing-board pass: improve-in-place vs. core pivot._

## Verdict

The core idea earns its place: a `VACUUM INTO` snapshot before destructive table
rebuilds is exactly the right safety net for "migration committed but was
logically wrong."

The current lifecycle design does not fully earn its complexity. Four rounds of
fixes around cleanup, copied DBs, sibling DBs, stale paths, and prune behavior
are not just normal hardening. They point to accidental complexity from **split
ownership**: snapshot.go stores metadata inside a DB that can be copied or
renamed, while the recovery artifact is an external file whose ownership is
inferred from paths.

## Core Pivot

Pivot to a **single path-owned upgrade restore point**, not DB-row-owned snapshots.

**Keep:**
- `VACUUM INTO`
- explicit `Risky` migration marking
- fail-closed behavior if the snapshot cannot be created
- `snapshot list/prune`
- short retention after successful `serve` boots, if desired

**Throw away:**
- the `migration_snapshots` table
- absolute snapshot paths stored inside the DB
- pruning tracked rows from inside copied DBs
- broad untracked-file scans
- "snapshot before every risky migration, then retain only most recent"

**New shape:**
- For a DB at `/x/continuity.db`, use one derived sidecar location, e.g. `/x/continuity.db.snapshot/`.
- Store exactly one snapshot file there, plus a tiny external manifest: `snapshot.db`, `manifest.json`.
- Manifest stores relative file name, `pre_version`, `target_version`, `created_at`, `boots_since`; no absolute paths.
- `snapshot list/prune` derives the sidecar path from `CONTINUITY_DB` and does not open the database at all.
- Retention tick updates the sidecar manifest after a real successful serve bind.
- A copied DB does not inherit stale snapshot metadata unless the operator copies the sidecar too, in which case the snapshot remains local to the copied tree.

**Correctness fix (independent of the pivot):** take the snapshot once before the
**first pending risky migration in an upgrade run**, not before each risky
migration while retaining only the latest. In the current v5→v9 path, a pre-v6
snapshot can be replaced by a pre-v9 snapshot, which means a committed v6 bug may
no longer be recoverable. For the stated "one-shot upgrade window" goal, the
rollback point should be "before this upgrade began."

**Plus:** add a narrow `continuity snapshot restore --confirm` eventually. Manual
`cp` is a high-stakes recovery path, especially with WAL files. A restore verb
can replace the DB and remove stale `-wal`/`-shm` files correctly while still
being explicitly destructive and scoped only to the current safety snapshot.

## Bottom Line

The feature should exist, but I would not keep growing the current design. The
snapshot file is necessary; the in-DB tracking table and cross-database cleanup
rules are where the design is paying unnecessary interest. A single path-derived
sidecar restore point better matches the project's north star: transparent during
normal use, powerful when the operator reaches for the CLI, and clearly not a
general backup system.
