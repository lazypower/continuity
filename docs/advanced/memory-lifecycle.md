[Docs](../README.md) › Advanced › Memory lifecycle

# Memory lifecycle

What happens to a memory over time: how relevance decays, which categories the clock never touches, when the garbage collector is allowed to destroy anything, and how duplicates collapse.

**Audience:** engineer · **Read time:** ~9 min

Four separate mechanisms act on stored data on a schedule. They are easy to
conflate and they are not the same thing:

| Mechanism | Acts on | Destroys? | Default |
|---|---|---|---|
| Decay | `mem_nodes.relevance` | No | On |
| GC / compost | `mem_nodes` rows | **Yes**, snapshot-first | **Off** |
| Observation retention | `observations` rows | Yes | **On** |
| Dedup | `mem_nodes` rows | **Yes** | Manual only (`continuity dedup`) |

Decay and GC run from `Engine.StartDecayTimer` (startup, then every 24 h).
Retention runs from `engine.StartRetentionTimer` on its own 24 h ticker,
independent of the engine. Dedup never runs on a timer.

## Decay

The algorithm is documented in `internal/engine/decay.go` and implemented in
`DecayAllNodes` (`internal/store/nodes.go`) — the split exists because the
engine owns the schedule and the store owns the SQL.

- **90-day half-life.** `relevance = 0.5 ^ (elapsed / 90d)`.
- **Floor 0.1.** Memories fade but are never fully forgotten.
- **Reference time** is `last_access` when set, else `created_at`.
- **Sub-hour elapsed is skipped** — nothing decays in the first hour.
- **Monotonic.** If the computed value is `>=` the stored one, the row is
  skipped. Decay can only ever lower relevance; nothing but a deliberate use
  raises it.
- **Computed in Go, not SQL,** because `modernc.org/sqlite` has no `pow()`.
  `pow05` is `exp(-x·ln2)` with a hand-rolled Taylor-series `exp` — good enough
  for a decay curve, and it avoids a `math` import in the store layer.
- **One transaction for the whole sweep.** A mid-sweep failure leaves the
  corpus fully un-decayed rather than partially decayed; the `defer
  tx.Rollback()` makes every early return atomic.

### Relevance no longer scores search

Decay still runs and still accumulates state, but `relevance` is **not** a
search-ranking input. `engine.Find` scores `similarity × categoryBoost` and
nothing else.

The reason is in [ADR-001](../adr/ADR-001-signal-keyed-surfacing.md): with
decay running honestly, `similarity × relevance` does not preserve order across
cohorts. A floored node at similarity 0.9 scores 0.09 and loses top-k to a
fresh, barely-related node at similarity 0.15 — burial of exactly the scarred
memories the system exists to keep findable. LLM-assisted search
(`engine.Search`) drops the same term: its re-rank is
`(0.5·similarity + 0.2·parentScore) × categoryBoost`, with the former 0.3
relevance weight **removed, not redistributed**, so the two eras of scores
stay comparable during the research window.

Interim staleness authority is retraction, which search already enforces.
Relevance re-enters ranking only inside whatever formula the shown/used data
earns.

## Exposure is not use

This is the substrate's standing invariant, and it is the reason decay finally
means something.

Historically a single `TouchNode` fired whenever a node appeared in a result
list, and it did two things: incremented `access_count` **and reset
`relevance = 1.0`**. A node that rode many result lists therefore held
relevance 1.0 forever and outranked semantically closer, less-travelled nodes.
The 90-day half-life was dead letter for anything that kept appearing — "smart
decay" was, in practice, *time since last listed*, not time since last useful.
Reads wrote; every search mutated the ranking that shaped the next search.

The touch is now split into three distinct operations:

| Operation | Function | Mutates |
|---|---|---|
| **Use** — deliberate fetch by URI | `store.RecordUse` | `last_access`, `relevance = 1.0` |
| **Rotation** — diversity bookkeeping for moments | `store.AdvanceRotation` | `last_access` only |
| **Exposure** — appeared on any surface | nothing on the node | append-only journal row |

`RecordUse` deliberately leaves `access_count` alone. That column froze as
legacy the moment the `mem_events` journal became the authority on use counts.

The **only** path that calls `RecordUse` is `GET /api/memories`
(`handleGetMemory`). That is load-bearing by construction: search returns L0 +
URI only, so reaching an L1 or L2 payload requires a deliberate fetch. Search
returning L1 bodies was drift from the original tiering design; with payloads
behind the fetch, an agent cannot consume content invisibly to the accounting.

`AdvanceRotation` exists so moment-rotation fairness cannot smuggle a ranking
signal in through the back door the way the old `TouchNode` did.

### The shown / deepened journal

`mem_events` (schema v14) is append-only telemetry about exposure and use —
never node state. A schema `CHECK` pins the full vocabulary so a typo'd writer
cannot corrupt research data; adding a name takes a migration.

- `shown` — written for every injection and every result list, with a
  `surface` of `tray`, `moments`, `search`, or `gate`.
- `deepened` — the node's L1/L2 were fetched. This is the same act as "use".
- `attributed`, `re-taught`, `retrieval-miss` — reserved in the CHECK, written
  by a staged session-end pass, not by this slice.

Two accounting boundaries are enforced in code, not merely intended:

- **`deepened` is journaled only when `RecordUse` succeeds.** Journal and node
  state must agree on whether a use happened. The *read* stays fail-open — the
  user gets their memory even if the bookkeeping write hiccups; only the
  accounting fails closed.
- **`shown` for search is journaled only after the response write succeeds.**
  Recording before the write let a cancelled client inflate the
  used-given-shown denominator with results nobody saw. Smart-mode subquery
  candidates never reach the response, so they are never journaled either.

Writes go through a buffered fire-and-forget recorder
(`internal/server/events.go`, buffer 256). A full buffer **drops** the event
and counts the drop, logging at the first drop and every hundredth thereafter —
"by contract, not by accident". Inspection surfaces (`/api/profile`,
`/api/tree`, the pinned list) still return L1 and write **no** events: they are
operator observability, and counting UI browsing as use would inject exactly
the noise the deepened-vs-attributed control exists to measure.

## Decay-exempt categories

One SQL fragment names them, and it is deliberately the single authority:

```
decayExemptCategoriesSQL = `'moments', 'profile', 'preferences', 'feedback'`
```

Both `DecayAllNodes` and the GC candidate predicate (`gcCandidateWhere` in
`internal/store/gc.go`) derive from it, so a category made decay-exempt can
never silently become GC-eligible.

**Contract categories — `profile`, `preferences`, `feedback`.** These are the
user's standing stance: how they work, what they prefer, what corrections they
have given. After the touch split, *nothing refreshes them* — boot injection is
`shown`, not `used`, and they are consumed from the tray rather than fetched by
URI. Under decay plus a relevance cutoff they would silently fall off the tray
in about four months. That is the repeated-correction failure the whole system
exists to prevent, so: **decay is an episodic mechanism.** Contract lifecycle
authority is merge (supersede in place) and retraction — never the clock.

The exemption had to land with a data repair, and did: migration v15 sets
`relevance = 1.0` for live contract leaves. The exemption stops future erosion;
without the repair, contract memories already eroded by pre-exemption decay
would have shipped pre-hidden. Tombstoned rows are excluded — retraction is the
contract's lifecycle authority and a repair must not brighten what the operator
retracted.

**Moments** are exempt as permanent relational anchors, by their own category
contract. They also carry a `categoryBoost` of 1.3 in search, being high-signal
content that passed a triple qualification filter.

## GC (compost)

GC is the one operation that destroys real memories, so it ships **off** and
earns its way up:

```
off (inert) → shadow (log what it WOULD reclaim, delete nothing) → on
```

Set via `CONTINUITY_GC`; anything other than `shadow` or `on` is off.

The predicate for "genuine dead weight" (`gcCandidateWhere`) requires *all* of:

- `node_type = 'leaf'`
- `tombstoned_at IS NULL` — retracted receipts are never targeted. They are
  tiny, and their vector is the load-bearing resurrection antibody.
- `pinned_at IS NULL` — operator pins are declared contract.
- category not in the decay-exempt set.
- `relevance <= 0.1` — decayed all the way to the floor.
- `COALESCE(last_access, created_at) <= now − 180 days` — *and* unretrieved for
  half a year.

Both conditions, not either. Candidates are taken oldest-first and capped at
**50 per sweep** — a bounded blast radius. The constants carry their own
comment: "Conservative by design — I would rather stay a little bloated than
reclaim a memory that turns out to have mattered."

`/api/health` surfaces `gc_mode` and `gc_reclaimable` using the *same*
predicate, so the health count and the sweep can never diverge.

### Snapshot-first

`GCOn` takes a `VACUUM INTO` snapshot labelled `pre-gc-compost` before deleting
anything. Two refusal paths:

- Snapshot **errors** → "refusing to reclaim this sweep". Deletion without
  recovery is exactly what this will not do.
- Snapshot returns an **empty path** (in-memory or path-less database) → also a
  hard stop, not a silent pass. "Snapshot first" is the contract, so an empty
  path is treated as failure.

`VACUUM INTO` is not an implementation detail you may swap. `snapshot.go`
spells out why a file copy is wrong:

1. WAL mode means the main `.db` file is incomplete on its own — recent commits
   live in `-wal` until a checkpoint. A naïve copy of `<path>` alone silently
   drops the most recent writes, which is exactly the data most worth
   preserving.
2. Copying all three files does not fix it: another connection can be writing at
   any byte offset, so the copy is torn. OS file locks do not help — SQLite
   locks cooperatively through its own engine.
3. `VACUUM INTO` routes through SQLite: correct shared read lock, page-by-page
   walk from a consistent transaction view *including the WAL*, emitting one
   self-contained file with no separate WAL or SHM. Writers are not blocked.

`TestSnapshot_CapturesWALActiveData` pins this against regression by writing a
row that lives only in the WAL and asserting it lands in the snapshot.

The same `SnapshotNow` path backs `doctor --repair-vectors --apply` and
`embedder use`. Migration safety snapshots use the sibling
`snapshotBeforeRiskyMigration`, and are auto-deleted after
`SnapshotRetentionBoots = 3` successful `serve` boots — three being "ran the
binary, the new schema works, ran it a couple more times to be sure".

Snapshots live in `<dbdir>/snapshots/<db-filename>/`, namespaced by the
database's own basename. Deletion additionally checks `ownsSnapshotPath`: a
tracking row stores an absolute path, so a copied or renamed database inherits
rows pointing at the *original* database's snapshot files. Unlinking those
during prune or retention would destroy another database's rollback point, so
Continuity never unlinks a file it cannot prove is its own, no matter what a
stale row claims.

## Observation retention

Observations — raw `tool_input`/`tool_response` records from `PostToolUse` —
are **not** memories, and retention is deliberately not modelled on the GC
sweep. GC destroys memories, so it ships off; observations are raw scaffolding
whose only reader is the live session's own context header
(`GetSessionObservationCount`). Once a session is no longer in flight, nothing
can ask for them again, so retention ships **enabled**. Shipping it off by
default would preserve exactly the unbounded growth it exists to fix.

An observation is **spent** when its session is not in flight *and* the row is
older than the grace window:

- **In flight** = a `sessions` row still marked `active` whose
  `COALESCE(last_active_at, started_at)` is inside the zombie horizon.
- **Grace** = 14 days by default, regardless of session state. Generous on
  purpose: it costs disk, and the alternative is racing a session still being
  written to.
- **Zombie horizon** = 30 days. Clients that crash or get killed never fire
  Stop/SessionEnd, so their sessions stay `active` forever and would otherwise
  pin their observations forever with them.

An observation with no session row at all is orphaned, and therefore not in
flight.

`last_active_at` (migration v16) exists because neither older column answers
the question. `started_at` is not refreshed when `InitSession` reactivates a
resumed session; `status` alone cannot tell "in use" from "abandoned by a
client that crashed before firing Stop". Deriving liveness from
`MAX(observations.created_at)` was correct only *after* a resumed session's
first new observation landed — a sweep inside that window would delete the
history of a session the user had just reopened. It is stamped on init, on
resume, and on every recorded tool use.

Retention is deliberately **not** keyed to extraction. Extraction reads the
transcript file, not this table, and its content gate skips thin sessions
without ever marking them extracted — keying retention to `extracted_at` would
couple two unrelated things and strand every unextractable session's rows
forever.

### The retention env var fails closed

`CONTINUITY_OBSERVATION_RETENTION_DAYS` accepts a positive day count,
`off`/`false`, or nothing. **Unparseable input disables retention**, it does
not fall back to the default. An operator typing `of` while reaching for `off`
is trying to *stop* deletion; answering that with the destructive default is
the worst available reading, and the boot sweep runs immediately, so a log line
arrives too late to intervene. Values above `maxRetentionDays = 3650` are
clamped — a large value would otherwise overflow `time.Duration` into a
*negative* grace, putting the cutoff in the future and making every completed
session immediately reclaimable. A config meant to retain more must never
delete more.

When retention is disabled the gauge is still refreshed, under the **default**
policy, so `/api/health`'s `spent_observations` answers "what would retention
reclaim if it were on?" — precisely when nothing is reclaiming and the pile
only grows.

### The catch-up log

The sweep is uncapped, unlike GC: observations accumulate at tool-call rate,
and a cap low enough to be a safe blast radius for memories would never keep
up. A routine sweep logs one terse line — this runs daily forever, and an
explanation repeated every day trains people to skip the line.

Past `largeReclaimThreshold = 50 000` rows the sweep adds a second line
explaining that this was a one-time catch-up of observations predating the
retention path, that memories, vectors, and the relational profile are
untouched, that disk is not returned until `continuity prune` runs, and how to
widen or disable the window. The threshold is keyed off the size of the sweep
itself rather than a persisted "have we explained this yet?" marker —
deliberately, because such a marker is a schema change that can be lost on a
restored database and would explain the upgrade to someone who never upgraded.

### Reclaimed is not returned

`PruneSpentObservations` deletes rows; freed pages are reused but not returned
to the filesystem. `continuity prune` also runs `VACUUM`, which repacks the
file. A fragmented file is itself a performance problem: search scans all of
`mem_vectors` on every query, so vectors interleaved among millions of dead
observation pages turn a sequential read into scattered I/O.

`engine.PruneObservations` is the shared path for both the background sweep and
`continuity prune`, so the two cannot drift onto different rules.

## Dedup

`Engine.Dedup` is a manual, explicit reduction — `continuity dedup`. It never
runs on a timer.

**Threshold.** 0.65 for semantic embedders, 0.5 for the hashed lexical
fallback (`engine.MatchThreshold`). Keyword-overlap cosine for a genuine
paraphrase is inherently lower than semantic cosine, so a semantic-calibrated
0.65 would leave real duplicates behind. `continuity dedup` honors an explicit
`--threshold` and otherwise calibrates to the active embedder.

**It refuses when the vector identity is locked.** Dedup is destructive and it
embeds inline, so running it against an incompatible vector space could
cross-space-cluster and write foreign-identity vectors. It fails loud rather
than skipping quietly.

**Clustering** is a single greedy pass per category:

1. Embed any leaf missing a vector.
2. Build the vector map **filtered to the active identity only** — never delete
   a memory based on a cross-space cosine against a stale foreign-identity
   vector, which can linger even when active == declared (an interrupted
   repair, for instance).
3. Group leaves by category. Within a category, walk unclaimed nodes; each
   becomes a cluster seed, and every later unclaimed node with
   `cosine >= threshold` to the seed joins it.
4. The **keeper** is the cluster member with the highest `updated_at`. It
   survives with its URI, its vector, and its relevance intact.

**Merge-for-coverage.** Dedup is a reduction, but the survivor must not lose
detail a loser held — the keeper, chosen by recency, may be a terser
restatement. Before deleting, the keeper adopts the longest L1 and the longest
L2 found anywhere in the cluster. This is a heuristic (longest per tier), not a
semantic union: near-duplicates rarely carry divergent unique detail, and a
true merge would need an LLM. L0 is untouched, so the keeper's vector stays
valid. If persisting the adopted tiers **fails**, the whole cluster is left
intact this pass — deleting the losers then would drop exactly the detail the
step exists to preserve.

**Losers are hard-deleted, not tombstoned.** This is a deliberate
won't-fix. Dedup collapses near-identical restatements, so a per-loser
accountability receipt defeats its purpose — the tree would fill with dedup
tombstones. Detail is not lost, because merge-for-coverage runs first. A
tombstone-with-reason variant was rejected specifically because the retract API
takes an arbitrary reason, which would make a reason-prefix discriminator a
forgeable PII-resurrection bypass.

Orphaned directory nodes are cleaned up at the end.

### The one thing dedup does not do

The extraction path has its own inline similarity gate (`findSimilarNode`) that
redirects a new candidate onto an existing node's URI. `Engine.Remember` —
the direct write behind `continuity remember` and the MCP `remember` tool —
**skips it entirely**. A direct write is explicit user or agent intent, and
silently redirecting it onto a near-duplicate's URI causes silent data loss.
The caller-supplied slug is always honored; for immutable-category collisions
`UpsertNode` appends a timestamp suffix and the actual stored URI is reported
back.

### The moments pool cap

Moments are capped at 10. When the pool exceeds it, the most *redundant*
moment — highest average cosine to all other moments — is evicted. It is
**retracted, not deleted**: moments are the category the product treats as
emotionally load-bearing, so eviction leaves an accountable marker and drops
out of the live pool rather than shredding the row. Eviction is skipped
entirely while the vector identity is locked, since cross-space noise could
select a real moment instead of a redundant one.

---

**See also:** [Vector identity](vector-identity.md) · [Extraction](extraction.md) · [Architecture](architecture.md) · [ADR-001](../adr/ADR-001-signal-keyed-surfacing.md) · [Keeping it healthy](../guides/keeping-it-healthy.md) · [What gets remembered](../guides/what-gets-remembered.md) · [Configuration keys](../reference/configuration.md)
