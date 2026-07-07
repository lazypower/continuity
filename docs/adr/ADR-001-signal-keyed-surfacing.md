# ADR-001: Signal-Keyed Surfacing — Episodes Arrive With the Query, Not the Boot

- **Status:** Accepted — ship-now slice ratified 2026-07-07 (call delegated to Claude
  post-pressure-test), including the contract decay exemption, which must land with the
  split. §3 (index) and §4 (gate) remain staged behind calibration per §MVP altitude;
  §5 is the research program that closes issue #50. Amended 2026-07-07
  after two adversarial rounds (Codex). Round 1, 9 findings: 8 folded in as
  precision/contract amendments, 1 split out as an open product call (Recent Sessions).
  Round 2, 6 findings: 5 folded in (all-surfaces invariant, best-effort telemetry,
  contract decay exemption, gate boundary mechanics, inspection-noise calibration),
  1 rejected as framed (the relevance "staleness brake" was the amplifier; trade named
  in §Consequences).
- **Date:** 2026-07-07
- **Deciders:** Chuck (product), Claude (analysis + structure). Review: **Fiona**
  (pending — the τ gate, the `re-taught` signal, whether the index earns the tray).

> **The cold-boot defect is temporal, not algorithmic.** At SessionStart the substrate
> holds zero bits about the session's task, so any episodic ranking at that moment is
> prediction from priors — and the prior it uses is a self-amplifying popularity count.
> Surfacing must be keyed to the signal available at each timepoint: **contract at boot,
> episodes at the first query, payloads on demand.** And the substrate's one standing
> invariant: **exposure is not use — only use may feed memory state.**

## Context

**The window is measured, not suspected.** Against the live corpus (n=288 leaves,
[mem://agent/cases/amplification-coefficient-measured], issue #50): the Recent Memories
window is neither recent nor important — it is the right tail of retrieval frequency.
Window median `access_count` = 12 vs corpus median = 1 (12× amplification); 36% of the
corpus has never been retrieved and can never enter the window; the top-20 nodes hold 37%
of all access events. Fifteen items × ~200 chars ride every launch
(`internal/server/context.go:205-233`, scored at `context.go:463-470` as
`relevance × (1 + log2(access_count))`, capped at `maxContextItems = 15`), with **no
project scoping anywhere in selection** — the ranking is global.

**The pollution is not confined to cold boot — it contaminates every search, by a
different vector.** Find-mode score is `similarity × relevance × categoryBoost`
(`internal/engine/search.go:123`). After scoring, **every node on the returned top-k list
is touched** (`search.go:149-152`), and `TouchNode` (`internal/store/nodes.go:331-336`)
does two things: increments `access_count` **and resets `relevance = 1.0`**. So:

- `access_count` never enters the search score directly — it pollutes **cold boot** via
  `nodeScore`.
- The **relevance reset** pollutes **search**: a node that rides many result lists holds
  `relevance = 1.0` forever and outranks semantically closer, less-travelled nodes. The
  90-day half-life (`nodes.go:398-410`) is dead letter for anything that keeps appearing
  in top-k — "smart decay" is, in practice, *time since last listed*, not time since last
  useful.
- Moment rotation is a third touch site — corpus max `access_count` = 209 is a
  session-injected moment. Raw access measures **injection frequency, not value**.

Two surfaces, two vectors, **one root cause: the substrate conflates
appeared-in-a-list with was-used.** Reads write. Every search mutates the ranking that
shapes the next search.

**Prior art this ADR executes rather than invents:**

- **L0-as-projection** ([mem://agent/patterns/l0-is-a-projection-not-a-tier]): *nothing
  is resident by recency or salience — only contract earns the tray.* Recent Memories
  violates the invariant by construction.
- **Fi's release criterion** ([mem://user/feedback/cold-injection-release-criterion]): a
  cold SessionStart must be enough to *behave correctly*, not to *remember everything*.
  Test: "would this be on the tray before knowing the operation?" No episodic window
  passes.
- **Telemetry never rides on retrieval**
  ([mem://user/preferences/memory-ops-intentional-not-ambient]): the agent's budget
  carries content; per-memory stats are intentional ops.
- Issue #50's constraint: research first. §5 is that research, made concrete.

## Decision

### 0. Name the failure precisely: the missing input is the query, not a better weight

Recent Memories asks "what will be relevant?" at the only moment in the session lifecycle
when the question is unanswerable. No replacement ranking function fixes this — swap
`access_count` for any other prior and it is still a prior. **The query arrives one event
later**, at the first `UserPromptSubmit`. Injection timing must move to where the signal
lives, and this session is the existence proof: a prompt mentioning "cold-boot" and
"recent memories" pulled the amplification measurement at 0.637 similarity — the exact
memory that mattered, selected by the query, not predicted by a prior.

### 1. Retire the ranked window — remove now, replacement not required

The Recent Memories section (`context.go:279-284` and the episodic half of the
ranked-item scan behind it) is **deleted from cold-boot assembly, unconditionally and
immediately.** Removal is not gated on the index (§3) or the prompt gate (§4) shipping:
~3000 of the 4000-char budget is speculative spend with a measured near-zero hit rate.
An empty slot is cheaper than a wrong guess.

Two precision points the first adversarial round forced:

- **The scan is shared; only the episodic half dies.** The loop at `context.go:205-223`
  scores profile/preferences/feedback (rendered as "Your Profile") in the same pass as
  the episodic categories (rendered as "Recent Memories"). §1 removes the episodic
  categories (patterns/events/cases/entities/reference) from the scan; the contract
  categories stay on the tray — they are exactly what §3 calls task-invariant. With the
  episodic half gone, `nodeScore()`'s popularity term has no remaining purpose and the
  function dies; contract-category ordering falls back to the existing category-priority
  iteration order, which the code already documents as the intended tiebreaker.
- **The interval is owned, not hidden.** Until §3/§4 ship, episodic surfacing is
  pull-only (search). §3's own argument — the agent cannot search for what it does not
  know exists — names a real gap, and this slice runs with that gap open. It is accepted
  because the measurement says the window never covered it: the window surfaced the
  already-most-retrieved (median access 12 vs corpus 1) and structurally excluded the
  36% of the corpus never retrieved — the unknown-unknowns the gap is about. Deleting
  the window removes ~zero coverage of the thing §3 exists to add.
- **The contract does not decay.** Post-split, nothing refreshes a contract node: boot
  injection is *shown*, not *used*, and profile/preferences/feedback are consumed from
  the tray, not fetched by URI. Under the current mechanics (decay exempts only
  `mem://user/profile/communication` and moments, `nodes.go:344-354`; the scan drops
  anything below relevance 0.3, `context.go:217`) the user's standing preferences would
  silently fall off the tray in ~4 months — the repeated-correction failure, rebuilt by
  this very ADR. So: **decay is an episodic mechanism.** Contract categories
  (profile/preferences/feedback) are decay-exempt; their lifecycle authority is merge
  (supersede in place) and retraction — one authority per question, and the relevance
  cutoff can never hide the contract.

### 2. Exposure is not use — split the touch

`TouchNode` conflates two facts that must have separate authorities:

- **Shown** (machine act): the node appeared in a result list, a tray, a rotation. Shown
  updates *bookkeeping only* — `last_access` where rotation fairness needs it
  (moments) — and **never** mutates `relevance` or a use counter.
- **Used** (deliberate act): the node was fetched by URI — L1/L2 deepening via
  `continuity show` / node GET — or attributed post-hoc (§5). **Only use refreshes
  relevance and increments the use counter.**

Concretely: the touch loop at `search.go:149-152` is removed — **search becomes a
read-idempotent operation**; running the same query twice returns the same scores.
Relevance-refresh relocates to the node-fetch path. This severs the amplification loop
structurally (rank → touch → rank has no edge left) and it is what makes decay honest:
relevance measures time-since-last-*useful*, which is the thing decay was designed to
measure. `access_count` freezes as a legacy column; the event log (§5) becomes the one
authority on exposure and use.

Two boundary conditions that make the split honest rather than nominal:

- **Search returns pointers, not payloads.** Today `/api/search` returns L1 bodies
  (`internal/server/routes.go:617`) and the CLI prints them — an agent can consume a
  hit's payload without ever fetching it, so its use would be invisible to the split.
  Search surfaces (API, CLI, gate) return **L0 + the `mem://` URI only**; L1/L2 live
  behind the node fetch. This is not new design — L0 was specified as the search surface
  in the original tiering; returning L1 from search was the drift. With payloads behind
  the fetch, the use event is load-bearing by construction.
- **Read-idempotent means node state, not the observation journal.** Search writes one
  thing: an append-only `shown` event (§5). It mutates no node column — not relevance,
  not counters — so scores are identical across repeated queries. Exposure reaches
  future ranking only as the *denominator* of used-given-shown, where riding many result
  lists without being fetched is **negative** evidence. The amplification loop does not
  just lose its edge; its sign flips.
- **One invariant, every search surface.** Find-mode is not the only path:
  LLM-assisted search (`mode=search`, `internal/engine/search.go:163-165`) decomposes
  into subqueries, rides `Find` per subquery, and re-ranks with its own
  relevance-weighted formula (`0.5·sim + 0.3·relevance + 0.2·parentScore`). The §2
  invariants bind it identically: no node-state mutation, relevance leaves its re-rank
  for the interregnum, and `shown` events are written **only for results actually
  returned to the caller** — never for intermediate subquery candidates the user never
  saw, or the used-given-shown denominator is corrupted at birth.

### 3. The tray at t=0: contract + pins + index — shape, not content

What cold boot *keeps* is everything task-invariant: Working With You, Pinned, safety,
Moments (existing sections, unchanged here). What replaces the window is not
better-ranked content but a **corpus index**: tree shape with counts, plus L0 one-liners
for nodes with affinity to the current project (derivable today:
`node → source_session → session.project`; sessions already record cwd at init,
`internal/hooks/submit.go:55-67`). Budget on the order of 600 chars — a fraction of what
the window spent.

One seam must move for this to work at t=0: project identity today lands at the *first
prompt* (`submit.go:55-67`), while the start hook posts only a session id
(`internal/hooks/start.go`). The SessionStart hook input already carries cwd; §3
requires the start hook to forward it. When the project is unknown at boot, or known
with zero affine nodes, the index degrades to shape-only — tree counts without L0
lines. Less content, never guessed content.

Why an index passes Fi's test when content cannot: **the agent cannot search for what it
does not know exists.** Pull-retrieval already works mechanically; it fails because the
agent lacks a reason to suspect memory holds something. The *shape* of the corpus is true
regardless of the operation — it is a map on the tray, not a guess about the route. The
index converts recall into recognition: push the pointers, pull the payloads.

**Recent Sessions: kept, shrunk to the project (decided at ratify).** The session log is
the one episodic tray element with a prior-free justification: booting into a project,
the likeliest continuation is that project's last session — resumption is tray-worthy
before knowing the operation. The justification is project-local, so the section follows
it: with the start hook forwarding cwd (above), Recent Sessions renders the **current
project's last 1–3 sessions only**; cross-project history moves behind the index as
counts. Project unknown at boot → one line, the most recent session overall. Fixed
recency, no ranking, no touch mechanics — the §2 invariants never applied to it and
still don't.

### 4. The prompt gate: threshold-gated JIT push at the first query

`continuity hook submit` already runs on every prompt and already round-trips to the
server (`submit.go:55-79`). Extend it:

1. Server runs **find-mode only** (pure vector — this path is synchronous ahead of the
   prompt; LLM-assisted search is banned here, permanently) against the prompt text,
   project-scoped.
2. Inject via `additionalContext` **only hits above a hard similarity threshold τ**,
   calibrated per-embedder so the **median prompt injects zero**.
3. Dedupe against a session ledger of already-surfaced URIs (tray + prior gate hits).
4. Inject **L0 + the `mem://` URI** — the pointer and the abstract; the agent deepens to
   L2 by choice, which is itself the use signal §2 counts.

The waste profile inverts: the old window pushed 15 items hoping one mattered; the gate
pushes ~nothing on most prompts and fires precisely when the prompt carries signal
("migration snapshots" → the PR #31 scar tissue, at the moment it is load-bearing).
Terse prompts ("yes", "continue") self-handle — they clear no τ. **Silence is the
default; precision is bought with an actual query.** Ships behind a config flag;
default-on only after τ calibration against real prompt traffic (§MVP).

**The gate fails closed as a contract, not a hope.** Every degraded state resolves to
*inject nothing and pass the prompt through*: server unreachable or slow (the gate gets
its own hard time budget, well under the hook client's 5s default — on expiry, silence),
vector store locked (`/api/search` already refuses rather than degrades), embedder
changed without recalibration (τ is embedder-specific; a mismatch auto-disables the gate
until recalibrated), τ absent (flag on but uncalibrated = gate off). The hook exits 0 in
all of these — a memory lookup is never allowed to block or delay the user's prompt
beyond its budget. Acceptance (§MVP) tests the degraded states, not just the happy path.

The mechanics are pinned at the boundary, because today's client semantics would leak:
the hook client turns any status ≥400 into an error (`internal/hooks/client.go:79-94`)
and `/api/search` returns 503 on vector lock (`internal/server/routes.go:559-574`). The
gate call is its own request with its own short timeout, and the hook **swallows every
gate-path failure** — non-200, timeout, transport error, malformed body — into silence
with exit 0. The gate never inherits the session-init call's error path; a failed init
stays a non-blocking exit-1 as today, and the gate still resolves to silence.

### 5. Instrument before you rank — the issue #50 research program

Issue #50's open question — what does *useful* mean operationally — becomes measurable
the moment §2's split exists. Operational definition: **a memory was useful if it was
shown and left fingerprints in the session.** The event log records, per surfacing:

- **`shown`** — {uri, surface: tray|gate|search|moments, session, timestamp}. Written by
  every injection and every result list.
- **`deepened`** — agent fetched L1/L2 after exposure. The cheap behavioral proxy;
  already §2's use event.
- **`attributed`** — the session-end extractor (an existing LLM pass over the condensed
  transcript) additionally judges which surfaced memories were load-bearing. Marginal
  cost ≈ zero; runs off the hot path, outside the agent's budget, honoring
  [mem://user/preferences/memory-ops-intentional-not-ambient].
- **`re-taught`** — extraction produced a memory duplicating one that was *shown* this
  session. **The strongest signal in the set and it is negative**: the substrate surfaced
  the fact and it failed to land. This is the exact failure a user feels when repeating
  a correction.
- **`retrieval-miss`** — extraction produced a duplicate of a node that exists but was
  *never shown* this session. The other half of the repeated-correction failure, and it
  indicts a different component: `re-taught` measures injection efficacy,
  `retrieval-miss` measures surfacing coverage. Conflating the two would make the
  strongest signal in the set unusable.

Ranking — wherever it still exists after the query does the selection work (tie-breaking
within τ, decay/curation) — is **re-derived from used-given-shown, never from raw
exposure counts**, and only after these events have accumulated against real sessions.
No formula is chosen in this ADR; that is deliberate. The instrumentation *is* the
investigation issue #50 asked for.

The log is bounded by design: append-only, off the hot path, with raw events compacting
into per-node aggregates (shown/used tallies per surface) once they age out of the
research window — exact retention is an open item (⚑). "Unbounded exposure table" is not
an acceptable end state; telemetry that can outgrow the corpus it measures has failed
the proportionality test.

"Off the hot path" is a write contract, not just a schedule: search and the gate are
synchronous surfaces, and SQLite is single-writer — a `shown` insert queued behind an
extraction write could otherwise spend the hook's budget (`busy_timeout=5000`,
`internal/store/db.go:118-125`). `shown` writes are therefore **best-effort and
buffered off the read path** (fire-and-forget on the server side); a failed or slow
telemetry write never fails or delays the surfacing that triggered it. Telemetry is
allowed to lose an event; the prompt is not allowed to wait for one.

## Consequences

- **Cold boot honors the keystone**: no ranked episodic *corpus content* is resident;
  ~3000 chars/launch stop being spent on a 12×-amplified greatest-hits reel. Two named
  exceptions remain by design, not by omission: Moments (a deliberate diversity channel,
  fate tracked in §Deferred) and Recent Sessions (a fixed-recency session log,
  `context.go:286-294`, untouched by ranking or touch mechanics — kept, shrunk to
  project scope; decided in §3).
- **Search reads stop writing.** Scores become stable and comparable across time; the
  popularity contamination Chuck named ends because its mechanism (relevance reset on
  listing) no longer exists.
- **Decay finally acts — and leaves the interim score.** Un-used nodes slide toward the
  0.1 floor as designed. But `similarity × relevance` (`search.go:123`) with a 10×
  spread between floor and fresh does not preserve order *across* cohorts: a floored
  node at similarity 0.9 scores 0.09 and loses top-k to a fresh, barely-related node at
  similarity 0.15 — burial of exactly the scarred memories the system exists to keep
  findable. So for the research interregnum, **relevance leaves the find-mode score**:
  interim ordering is `similarity × categoryBoost`, matching the gate (τ is already pure
  similarity). Decay keeps running and accumulating honest state; it re-enters ranking
  only inside whatever formula §5's data earns. One less clever term, one less way to be
  wrong — and it deletes the migration re-baseline question outright. Named trade: this
  also removes what looked like a staleness brake on live-but-wrong content — but
  reset-on-list meant it never braked anything that kept appearing (it *was* the
  amplifier). Interim staleness authority is retraction, which search already enforces
  (`search.go:115-120`); §5's shown-without-use is the earned replacement.
- **The moments confound dissolves**: rotation bookkeeping stops inflating use counts,
  so the 209-access outlier stops masquerading as importance.
- **One authority per question**: the event log owns exposure/use; `access_count`
  freezes as legacy; `nodeScore()` is deleted.
- **`/api/context` shrinks and `hook submit` grows** — the seam between them
  (session-init already posts project/cwd) is the load-bearing integration point.

## Deferred (do NOT reopen these here)

- **Working-set ambient push** — PostToolUse sees file paths; "agent entered
  `internal/store/` → surface the migration case" is real but rides *behind* shown/used
  telemetry, or it is just a new way to inject noise. Phase-2 at most.
- **Ranking formula replacement** — post-instrumentation, data in hand. Not before.
- **Moment rotation skew** (CV = 0.52, diversity slots re-picking outliers) — parked in
  the measurement memory; becomes live only if Moments remains default-injected.
- **LLM-assisted search in the gate path** — not deferred; **banned**. The path is
  synchronous and the agent's budget carries content.
- **Multi-project affinity for merged nodes** — mergeable categories accumulate across
  sessions; affinity becomes a set. Sketch exists (§3); resolve when the index ships.

## MVP altitude

**Ship-now slice (this ADR's ratify):** delete the Recent Memories window + `nodeScore()`
(§1 — contract categories stay on the tray); contract categories become decay-exempt
(§1 — must land with the split, or the split itself creates the silent-loss path); split
the touch — search idempotent on node state across *all* search modes, use recorded on
node-fetch (§2); search surfaces drop to L0 + URI, payloads behind the fetch (§2);
interim find score becomes `similarity × categoryBoost` (§Consequences); event log
writing `shown`/`deepened`, best-effort off the read path (§5 skeleton).

**Staged behind data:** corpus index on the tray (§3); prompt gate behind a flag, τ
calibrated per-embedder over ≥200 real prompts with median-injects-zero verified before
default-on (§4); `attributed`/`re-taught`/`retrieval-miss` added to the session-end
extractor (§5).

**Acceptance:** `continuity hook start` output contains no episodic ranked section and
still contains Your Profile; running the same search twice mutates no node state and
returns identical scores (the only write is one `shown` event per surfacing); search
output carries L0 + URI and no L1 body; `continuity show <uri>` records exactly one use
event; every injection writes a `shown` event with its surface; with the gate flagged
on, a terse prompt injects nothing and a prompt naming a scarred topic injects the scar
and only the scar; with the gate flagged on and the server stopped, locked, or past its
time budget, the prompt passes through unmodified with zero injection and exit 0;
smart-mode search likewise mutates no node state and writes `shown` only for results
returned to the caller; a contract node untouched for a year still renders in Your
Profile; a failed `shown` write never fails or delays the surfacing that triggered it.

## Open items for review (⚑)

- **τ methodology:** per-embedder calibration is mandatory — cosine distributions from
  the feature-hashed fallback embedder and a real embedder are not comparable. Propose:
  percentile-based (τ = Pk of max-similarity over a corpus of real prompts), recalibrated
  on embedder change. Fi to pressure-test.
- **What counts as `used`:** L1/L2 fetch only, or does `attributed` also refresh
  relevance? Lean: fetch refreshes now; attribution feeds ranking research only, until
  its precision is known. Known noise source, accepted with eyes open: L0-only search
  results (§2) increase disambiguation fetches, so some `deepened` events are
  inspection, not use. `deepened` is the cheap weak-positive by design; `attributed` is
  its calibration control — if deepened-vs-attributed divergence turns out large, fetch
  stops refreshing relevance and attribution takes over. The instrument measures its own
  noise floor.
- **Event-log retention:** raw `shown`/`used` events compact into per-node aggregates
  after the research window — pick the window and the aggregate shape. (Replaces the
  former decay-interregnum item, resolved by removing relevance from the interim find
  score.)
- **Contract ordering and cardinality under the decay exemption** (the Round-3 target,
  if a round runs): with contract categories decay-exempt and relevance refreshed only
  on *use* — which tray consumption is not — the contract's `ORDER BY relevance DESC`
  goes flat, and nothing bounds contract growth over years. When the contract outgrows
  its tray budget, truncation order is arbitrary and a load-bearing preference can
  silently drop: the quiet cousin of the loss path the exemption just closed. Lean:
  order contract categories by `updated_at DESC` (re-affirmation recency — moves on
  merge, i.e. on re-learning, never on exposure, so no popularity re-entry), and surface
  contract-size-vs-tray-budget in doctor/metrics as a curation prompt; merge and
  graduation remain the designed pressure valve.
- **Index shape:** tree-with-counts vs counts + top project-affine L0s; exact budget;
  whether it appears for a project with zero affine nodes.
- **`re-taught` detection:** extraction dedup already compares against existing nodes —
  confirm the duplicate-of-shown check can ride that pass without a new LLM call.
- **Does Moments stay on the tray?** Out of scope here, but §Deferred's skew item
  activates the day the answer is yes-permanently.
