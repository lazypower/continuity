[Docs](../README.md) › Advanced › Vector identity

# Vector identity

What binds a corpus to one embedding space, the state machine that reconciles it at every boot, and exactly what stops working when it locks.

**Audience:** engineer · **Read time:** ~7 min

## What a vector identity is

A vector identity is a string of the form `<model>:<dims>`, produced by
`canonicalIdentity` in `internal/engine/identity.go`:

```
hashtf:2048
ollama:nomic-embed-text:768
model2vec:potion-retrieval-32M:512
```

(The Ollama and model2vec forms have three segments because `Embedder.Model()`
already returns `ollama:<model>` / `model2vec:<model>`; the dimension is
appended by `canonicalIdentity`.)

The corpus stores exactly one such string in `mem_meta` under the key
`vector_identity` (schema v11, `store.MetaVectorIdentity`). That is the
corpus's **declared** identity. Every stored vector separately carries its own
`model` and `dimensions` columns in `mem_vectors`, which canonicalize into the
same form.

Two vectors are comparable only if they share an identity. Cosine similarity
across different embedders is meaningless — and worse, across *different
dimensions* it is exactly 0 (`CosineSimilarity` returns 0 on a length
mismatch), so a mismatched corpus does not look broken, it looks empty. On
matching dimensions with a different model, it looks like plausible noise,
which is worse still.

Every embedder binds — including the hashed lexical fallback. That fallback's
dimension is now fixed (`defaultHashDims = 2048`), so binding it no longer
self-locks as the corpus grows, the way the old corpus-derived TF-IDF would
have. One consequence is intentional: a corpus written by the legacy `tfidf`
embedder does not match the new `hashtf:2048` identity, so it locks on upgrade
and is repaired by re-embedding. That is the migration path, not a bug.

## Why the declaration exists at all

Before `mem_meta` held an identity, the active embedder was chosen by
environment alone. Change `CONTINUITY_EMBEDDER`, restart, and the daemon would
happily embed new content in a new space alongside the old — or, worse,
re-embed everything on startup as a silent side effect. `ReconcileVectorIdentity`'s
doc comment names that outcome directly: the silent re-embed migration.

The rule the declaration buys: **re-embedding an existing corpus into a new
vector space is a corpus migration, and must be explicit.** Never a side effect
of a boot.

## Selecting an embedder

`selectEmbedder` (`internal/cli/embedder_select.go`) runs *before*
reconciliation and is a pure function of gathered inputs. Precedence:

1. **`CONTINUITY_EMBEDDER`** — the one-off escape hatch. Wins over everything.
2. **`config.toml [embedder].backend`** — the persistent form of the same
   choice.
3. **The corpus's declared identity** — `ResolveEmbedderByIdentity` constructs
   *that* embedder, never a substitute. This is what grandfathers existing
   installs: a corpus declared `ollama:nomic-embed-text:768` gets Ollama, never
   model2vec, even though model2vec is the default for fresh corpora.
4. **Fresh corpus, no override** — model2vec. If its files cannot be ensured
   (offline first run), it falls back to `hashtf` for this session rather than
   leaving the corpus with no embedder at all — a documented trade, upgradable
   later with `continuity embedder use model2vec`.

Two properties matter downstream:

- An explicit override at tier 1 or 2 **wins over a mismatching declared
  identity**, and then locks. That is intentional: the override is honored, and
  the safety net catches the consequence.
- At tier 3, if the declared embedder cannot be constructed (Ollama
  unreachable, model2vec download fails), `selectEmbedder` returns **nil** — it
  never substitutes a different embedder on failure. A nil embedder means
  search reports "no embedder configured" and `EmbedNode` clears vectors rather
  than writing foreign ones.

Hand-editing `[embedder].backend` changes what the next `serve` selects; it
does **not** migrate stored vectors. Only `continuity embedder use <backend>`
does both — it writes the config *and* runs the snapshot-first re-embed.

## The reconciliation state machine

`Engine.ReconcileVectorIdentity` runs once at boot, immediately after embedder
selection and **before anything is embedded**. Its outer wrapper adds a
fail-closed guarantee: if the inner reconciliation returns an error, the engine
is locked anyway, because an unproven embedder must never serve search against
the stored corpus.

```
              ┌─ no active embedder ──────────────▶ NO-OP
              │   (search is already unavailable)
              │
              │                  ┌─ 0 stored identities ─▶ ADOPT
              │                  │
  declared? ──┼─ absent ─▶ buckets ┼─ exactly 1 ────────▶ BACKFILL
              │                  │
              │                  └─ 2 or more ──────────▶ LOCK (mixed)
              │
              └─ present ─┬─ active == declared ────────▶ MATCH
                          └─ active != declared ────────▶ LOCK (mismatch)

  any error along the way ─────────────────────────────▶ LOCK (fail closed)
```

**ADOPT** — fresh corpus with no vectors and no declaration. The active
embedder defines the identity; it is written to `mem_meta`. `Match = true`.

**BACKFILL** — no declaration, but every stored vector canonicalizes into a
single identity. That identity is written to `mem_meta`. Note what this binds
to: the *corpus's truth*, not whatever embedder happens to be up. Reconciliation
then falls through to the match check against the newly-declared value — so
backfilling to an identity the active embedder does not share still locks.

**MATCH** — active equals declared. The lock is cleared, and `serve` kicks off
a background `EmbedMissing` (5-minute budget) to fill vectors for leaves that
have none. `EmbedMissing` fills **only truly-missing** vectors: a vector under
a different model is *stale*, not missing, and is left for an explicit repair.

**LOCK (mixed)** — no declaration and *multiple* stored identities. The code
refuses to bless a majority, and says why: a prior interrupted re-embed could
have left a foreign identity in the lead. Requires explicit repair.

**LOCK (mismatch)** — active differs from declared. No re-embed happens; search
fails closed; the reason string names both identities and points at
`continuity doctor` then `continuity doctor --repair-vectors`.

**LOCK (error)** — the `mem_meta` read or write failed, or the vector-model
scan failed. The wrapper sets the lock with a reason saying reconciliation
could not complete. Never serve unproven.

### Every path that leads to a lock

1. Corpus declares identity A; active embedder is B (env override, config
   override, or a declared-embedder construction that produced a different
   backend).
2. No declaration, and stored vectors span two or more identities.
3. No declaration, exactly one stored identity, but the active embedder does
   not share it — BACKFILL writes the declaration, then the match check locks.
4. Any error inside reconciliation — `db.VectorIdentity()`,
   `db.VectorModelCounts()`, or `db.SetVectorIdentity()` failing.

An upgrade from the legacy `tfidf` embedder is case 1 or 3, depending on
whether an identity was ever declared.

Note what is *not* a lock: **no embedder at all**. That path is a no-op with
`Match = false` but the lock flag untouched, because search is already
unavailable through the "no embedder configured" branch.

The lock is process-local state on the `Engine` (`identityMismatch`,
`identityReason`). It is never persisted. It is cleared only by a boot where
reconciliation reaches MATCH — which is why every repair path ends with "run
`continuity restart`".

## What fails closed when locked

| Surface | Locked behavior | Where |
|---|---|---|
| `GET /api/search` (both modes) | **503** with the lock reason as the error body | `handleSearch` |
| `Engine.EmbedMissing` | Returns `(0, nil)` — no silent re-embed | `engine.go` |
| `Engine.EmbedNode` | **Deletes** any existing vector, leaves the node Pending | `engine.go` |
| `Engine.Remember` | **Refuses** unless `--acknowledge-retracted` — a validation error | `engine.go` |
| `Engine.Dedup` | **Errors** — "run `continuity doctor --repair-vectors` before dedup" | `engine.go` |
| `Engine.ExtractSignal` | Returns nil without writing — deferred, logged | `engine.go` |
| `Engine.extractSession` | Defers the whole session **without marking it extracted** | `engine.go` |
| Extraction worker drain | Skips the entire pass, logs, lets the ticker retry | `extraction_worker.go` |
| `findRetractedMatches` | Returns no matches (treated like "no embedder") | `retract.go` |
| Moment pool eviction | No-op | `engine.go` |
| `continuity dedup` (CLI) | Reconciles itself, then refuses | `commands.go` |
| `doctor --repair-vectors --apply` | Explicitly *allowed* against a locked server | `doctor.go` |

| Surface | Keeps working while locked |
|---|---|
| `GET /api/context` — cold-boot injection | Yes (profile, pins, moments, recent sessions) |
| `GET /api/memories` — show a memory | Yes, including `RecordUse` + `deepened` |
| `POST /api/memories/retract` | Yes — retraction needs no embedder |
| `POST /api/memories/pin` / `unpin` | Yes — pins are store-native |
| `GET /api/tree`, `/api/profile`, `/api/timeline`, `/api/metrics` | Yes |
| `POST /api/sessions/*` — init, observations, complete, end | Yes |
| `POST /api/prune` | Yes — storage hygiene, no vectors involved |
| `GET /api/health` | Yes, and reports `vector_identity_locked: true` |
| Decay, GC, observation retention | Yes — none of them read vectors |
| `remember --acknowledge-retracted` | Yes — the operator has taken the gate's job |

### Why the write paths defer rather than fail

The pattern behind most of the "locked" column is one gate: the
**retraction-resurrection gate**. Retracted memories stay in the corpus with
their vectors precisely so a future write that means the same thing gets
caught — otherwise "I retracted that because it was PII" is silently broken the
next time a session writes something similar.

That gate is a vector comparison. When the identity is locked it cannot run.
So every path that would create a memory refuses or defers rather than writing
unchecked. `Remember` says so in its error text; `extractSession` defers the
whole session *without* marking it extracted, so the next Stop or SessionEnd
re-extracts once the operator repairs; the extraction worker skips the drain
entirely rather than treating the engine's `nil` return as success and deleting
the queue row — which would lose the capture.

`EmbedNode` deleting a stale vector rather than skipping is the subtle one.
On a content **update**, leaving the old vector in place would make search
serve a vector describing the *previous* content the moment a compatible
embedder returned, because `EmbedMissing` only fills missing vectors.
`DeleteVector` is a no-op for a fresh node, so this simply leaves new nodes
Pending.

### Foreign vectors are skipped even when unlocked

The lock is a startup check; individual stale rows can survive it. `Find`,
`findSimilarNode`, `findRetractedMatchesIn`, `Dedup`, and moment eviction each
independently filter to `canonicalIdentity(v.Model, v.Dimensions) == activeID`.
`Find` counts the skips and logs "run `continuity doctor`" when any occur.

## Repair

```bash
continuity doctor                            # read-only diagnosis
continuity doctor --repair-vectors           # dry-run: print the plan
continuity doctor --repair-vectors --apply   # snapshot, then re-embed
continuity restart                           # clear the lock
```

`runDoctorRepair` (`internal/cli/doctor.go`) has four properties worth knowing:

- **It includes retracted leaves.** Their vectors are still used by the
  dedup-against-retracted gate; leaving them in an old vector space after
  rebinding would blind that gate (different dimension) or feed it cross-space
  noise (same dimension).
- **It refuses under a live, unlocked, disagreeing server.** Such a server
  would keep writing its own vectors right after the snapshot-first repair and
  re-mix the corpus. An *empty* reported identity counts as disagreement — that
  is a pre-vector-identity binary still running mid-upgrade.
- **Two phases.** Phase 1 embeds everything into memory, writing nothing: an
  embedding failure partway (Ollama drops) leaves the corpus completely
  untouched rather than half-migrated. Phase 2 commits the vectors, then
  rebinds the identity **last**, so a mid-write failure never leaves the
  identity pointing at a space the vectors do not fully occupy.
- **It snapshots first** even though it rewrites only derived data.

`continuity embedder use <backend>` is the same operation with config
persistence in front of it: validate that the backend can actually be
constructed, write `[embedder].backend`, then call `runDoctorRepair` with
`apply=true`. It reuses the repair path rather than reimplementing it.

`continuity embedder status` is the narrow read — active embedder, declared
identity, match/lock, and what the running server reports — for when you do not
need doctor's full vector-health report.

---

**See also:** [Architecture](architecture.md) · [Memory lifecycle](memory-lifecycle.md) · [Extraction](extraction.md) · [Embedding backends](../guides/embedders.md) · [Troubleshooting](../guides/troubleshooting.md) · [CLI reference](../reference/cli.md) · [Configuration keys](../reference/configuration.md)
