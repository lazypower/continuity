[Docs](../README.md) › Advanced › Extraction

# Extraction

The durable queue that makes capture crash-safe, the gates a candidate must clear before it becomes a memory, and why automatic session-end extraction ships off.

**Audience:** engineer · **Read time:** ~8 min

## Three write paths, not one

"Extraction" covers three distinct things, and only one of them is off by
default:

| Path | Trigger | Default | Notes |
|---|---|---|---|
| **Session extraction** | Stop / SessionEnd hooks, `continuity extract` | **Off** | LLM infers memories from the whole transcript |
| **Signal extraction** | A trigger phrase in a user prompt | On | LLM turns one flagged message into a memory |
| **Direct write** | `continuity remember`, MCP `remember`, `POST /api/memories` | On | No LLM at all — structured input straight to `Engine.Remember` |

The queue described below carries the first two. The third bypasses it
entirely: `Remember` is synchronous and structured, so there is nothing to
infer and nothing to retry.

## The durable queue

Extraction used to be a fire-and-forget goroutine. A crash or restart
mid-extraction lost the work — and Claude Code transcripts are ephemeral, which
made the loss permanent. Schema v13 added `extraction_queue`, and the contract
is one sentence: **the row is deleted only after the extraction succeeds.**

### Enqueue

`POST /api/sessions/{id}/extract` and `POST /api/sessions/{id}/signal` both
call `store.EnqueueExtraction` and then `wakeExtractionWorker()`. A job row
carries `session_id`, `kind` (`session` or `signal`), `payload` (a transcript
path or the prompt text), `force`, `attempts`, and `queued_at`. Both endpoints
return **202 Accepted** — the HTTP call is the enqueue, never the work.

The wake channel is buffered at 1 and the send is non-blocking: a pending wake
already covers an in-flight drain, so a burst of enqueues cannot back up on it.

### One serial worker

`StartExtractionWorker` launches exactly one goroutine
(`internal/server/extraction_worker.go`). It is a no-op — and immediately
signals done — when extraction is disabled by a nil engine, so shutdown never
blocks waiting on a worker that never ran.

The loop is:

```
for {
    drainExtractionQueue()
    select {
    case <-stop:  return
    case <-wake:            // an enqueue arrived
    case <-ticker.C:        // every 30s
    }
}
```

The **first** thing the loop does is drain — before waiting on anything. That
is the crash-replay mechanism: whatever a prior process left in the queue is
processed on the next boot without needing a new enqueue to wake anything.

`extractionRetryInterval = 30s` is the safety net that covers both "a crash
left jobs behind" and "a transient failure needs retrying with no new enqueue
to trigger it".

### Drain semantics

Per job, `drainExtractionQueue`:

- **Success** → `DeleteExtraction(id)`. If the *delete* itself fails, the pass
  returns rather than looping on the same row.
- **Failure** → `BumpExtractionAttempts(id)`, log with the attempt count, and
  **end the pass**. Ending rather than continuing avoids a tight spin; the
  ticker or the next wake retries eligible jobs.
- **Unknown `kind`** → dropped with a log (returns nil, so the row is deleted)
  rather than retried forever.

`NextExtraction(maxAttempts)` orders by `attempts ASC, id ASC`, so a repeatedly
failing job cannot head-of-line-block fresh work.

### Max attempts, and parking

`maxExtractionAttempts = 20`, deliberately high. The comment states the trade:
a transient outage — LLM down, Ollama restarting — must not drop real memories.
Only a genuinely poison job, one failing roughly every retry for hours, is
abandoned. Losing a memory is worse than a lingering queue row.

And "abandoned" does not mean deleted. A job past the limit is **parked**:
`NextExtraction` excludes it, but the row stays in the table so the capture is
not silently lost. It surfaces in `/api/health`'s `pending_extractions`, and it
can be retried once the cause is fixed. The parking log line is explicit that
the job was "kept in queue for inspection/retry, NOT captured".

### The locked-identity deferral

Before draining anything, the worker checks whether the corpus vector identity
is locked. If it is, the **entire pass is skipped**.

This is subtler than it looks. While locked, `ExtractSignal` and
`extractSession` return `nil` — they defer rather than fail, because the
retraction-resurrection gate cannot run. A `nil` return means success to the
drain loop, which would delete the row and lose the capture. Skipping the pass
outright is what keeps the queued work alive until the operator repairs. See
[Vector identity](vector-identity.md).

### Shutdown

`StopExtractionWorker(10s)` closes the stop channel (guarded by a `sync.Once`
against a double close) and waits up to ten seconds for the current job. On
timeout it logs and moves on — the unfinished job's row is still in the queue
and replays next boot, so a slow shutdown never loses it. `serve` calls this
*after* `httpServer.Shutdown`, so no new work can arrive during the drain.

## The content gates

A transcript must clear all of these before anything is written.

**Pre-flight (`engine.hasEnoughContent`)** — the single source of truth:

- at least **3 user messages**, and
- at least **100 characters** after condensation.

Failing this returns nil **without marking the session extracted**, so a later
Stop or SessionEnd gets another chance once the conversation grows. This
matters: an earlier bug marked-on-skip, permanently locking thin sessions out
of extraction. `continuity extract --backfill-empty` exists to unmark sessions
that were caught by it.

The Stop hook mirrors the same gate client-side (`hooks.shouldExtract`) purely
to avoid an HTTP round-trip on turns that would be rejected anyway. Stop fires
per-turn, so most early turns skip here. The server applies the gate again as
belt-and-suspenders.

**Idempotency.** `extractSession` skips a session whose `extracted_at` is set,
unless forced. `ExtractSessionForce` bypasses only this guard — the content
gate still applies, so forcing extraction on a genuinely empty session is a
no-op.

**Post-LLM gates** in `extractMemories`:

- LLM response shorter than **20 characters** → skip.
- Response parsed by a **streaming JSON decoder** starting at the first `[`,
  not first-`[`/last-`]` slicing, so trailing model commentary — including a
  stray `]` — cannot break the parse. A *second decodable JSON value* is an
  error, not a fallback: the model emitted a corrected or multi-array response,
  and storing only the first array could persist a superseded candidate. Fail
  closed and let extraction retry rather than guess.
- **At most 3 candidates** are kept, even if the model returns more.

**Per-candidate validation** (`engine.validateCandidate`):

- category must be one of the nine valid names;
- URI hint sanitized to `[a-z0-9_-]` (spaces, dots, slashes collapse to
  hyphens; anything else is dropped) and rejected if empty afterwards;
- L0 required and non-empty;
- L1 at least **20** characters — it is the primary injection tier, so a
  one-word body is garbage;
- L0/L1/L2 truncated (not rejected) at **200 / 2 000 / 40 000** characters, at
  a word boundary, rune-safe so a multi-byte character is never split into
  invalid UTF-8. Every truncation is logged, because a firing ceiling means an
  upstream limit drifted.

**The similarity gate.** If an embedder is available, `findSimilarNode` looks
for a semantically equivalent **live** node in the same category above
`MatchThreshold` and redirects the candidate onto its URI. It deliberately
skips retracted nodes, so it can never merge *into* a tombstone.

**The retraction-resurrection gate.** Separately, `findRetractedMatchesIn`
checks the candidate against **retracted** nodes in the same category. A match
skips that candidate — otherwise retracted content (PII, most sharply) silently
resurfaces as a fresh live node. This is a distinct pass precisely because
`findSimilarNode` excludes tombstones and so cannot catch the
create-a-new-node path. On a gate *error* the candidate is skipped too: fail
closed. Only the offending candidate is dropped — one bad candidate must not
kill the rest of the batch.

**The exact-URI guard.** A constructed `uri_hint` can still collide with a
retracted canonical node that has no same-identity vector for the gate to find.
The candidate is skipped, and `UpsertNode` enforces the same thing atomically
via `ErrRetractedTarget`, closing the check-then-write race where a concurrent
retraction lands between the guard and the write.

### `merge_target` is not honored

The extraction prompt's candidate schema has **no** `merge_target` field, and
if a model emits one it is ignored as an unknown JSON key. The comment explains
why this is a security property rather than a simplification: an LLM-chosen
merge URI was a recurring retracted-PII gate-bypass surface — content landing
in a category or URI the gate had not checked. Dedup is owned by the system via
`findSimilarNode`, a path the gate can reason about. Ignoring `merge_target`
shrinks the trusted input to **zero LLM-controlled URIs**: a candidate always
lands in its own declared category, so the gate simply keys on `c.Category`.

### Vector sync after every write

After each upsert, the stored vector is reconciled with the (possibly merged)
content. When no usable embedder is available — locked, or `none` mode — any
existing vector is **deleted** rather than left alone, because on a content
update a stale vector would make search serve the previous content the moment
an embedder returned. `DeleteVector` is a no-op for a fresh node.

### What else a session extraction runs

Beyond memory candidates, `extractSession` also runs:

- **Relational extraction** (`internal/engine/relational.go`) — updates
  `mem://user/profile/communication`, capped at 1 200 characters, skipped if
  this session already produced it.
- **Tone extraction** — a short emotional-arc fragment stored on the session
  row. Rejected if empty or over 200 characters. Explicitly **non-fatal**: a
  tone failure logs and does not fail the extraction.

Only after all of these does `MarkExtracted` fire.

## Why auto-extraction is off by default

This is the single most surprising default in the product, and the rationale
is written down at the definition site. From `internal/config/config.go`:

> `Auto` enables automatic session-end extraction — the Stop/SessionEnd hooks
> asking an LLM to infer memories from the whole transcript. It defaults to OFF
> because **its usefulness is unmeasured** and **its writes are not
> provenance-distinguishable from authored ones** (nodes carry no
> `source_kind`): **an always-on, untrusted write path is not a safe default.**
> Retained for explicit opt-in and compatibility, and may be removed once there
> is evidence either way. Explicit `continuity remember`, the signal ("remember
> this") path, and `continuity extract --force` (the manual override) are all
> unaffected.

Unpack the three clauses:

**Usefulness is unmeasured.** Nobody has shown that memories inferred from a
whole transcript are retrieved and load-bearing at a rate that justifies
writing them. The instrumentation to answer that question — the `shown` /
`deepened` journal, with `attributed` and `re-taught` staged — exists now (see
[Memory lifecycle](memory-lifecycle.md#the-shown--deepened-journal)), but the
data has not accumulated. The default is what you choose *before* the evidence
arrives, and "off" is the reversible choice.

**Writes are not provenance-distinguishable.** `mem_nodes` has no `source_kind`
column. Once written, a memory an LLM guessed from a transcript is
indistinguishable from one the user explicitly asked to be remembered. There is
no query that separates them, no UI that flags them, and no cleanup that
targets only the inferred ones. That makes the write irreversible in the way
that matters: you cannot audit it after the fact.

**An always-on, untrusted write path is not a safe default.** The transcript is
untrusted input — it contains pasted logs, third-party content, error output,
and anything the user happened to copy into the session. Turning that into an
unattended writer, into a store that has no provenance and whose retraction
gate is the only thing standing between it and a resurfaced secret, is not a
default anyone should get without asking.

### What "off" actually does

The switch is enforced server-side, in `handleExtractSession`:

```
if !s.autoExtract && !req.Force { → 200 {"status":"extraction_disabled"} }
```

`StatusExtractionDisabled` is a stable part of the HTTP contract, asserted by
route tests and consumed by `continuity extract` — not merely a string the CLI
happens to recognize. The CLI surfaces it plainly rather than falsely reporting
a queued job:

```
automatic session extraction is off for <id> — re-run with --force to extract it anyway
```

The Stop and SessionEnd hooks POST with `force` absent, so they hit this branch
and nothing is enqueued. Unaffected:

- `POST /api/sessions/{id}/signal` — a **separate endpoint**, never gated by
  this flag.
- `continuity extract <id> --force` — the manual override.
- `continuity remember` / MCP `remember` / `POST /api/memories`.

Enabling it (`[extraction].auto = true` or `CONTINUITY_EXTRACTION_AUTO=1`)
makes `serve` print a startup warning that restates the rationale and names the
env var to unset. The `Server.autoExtract` field carries the same note.

Exhaustive key and flag details live in
[Configuration keys](../reference/configuration.md) and
[CLI reference](../reference/cli.md).

## The signal path

Signal extraction is the other side of the trade: it stays on because it fires
only on an explicit, up-front, plausibly-human request. Its gates live in the
hook, not the server — see
[Hook internals](hooks-internals.md#signal-phrase-gates) for the 2 000-character
prompt limit and the 500-character trigger offset that keep a trigger phrase
buried in a large paste from self-authoring a memory.

Server-side, `ExtractSignal` runs the same validation, retraction, and
exact-URI gates as session extraction, on a 60-second LLM budget.

## Recursion

Continuity's own LLM calls go through `claude -p`, which spawns a Claude Code
session, which fires hooks — including `UserPromptSubmit` — straight back into
Continuity. Every prompt `internal/llm/prompts.go` builds is therefore prefixed
with the sentinel `[continuity-internal]`, and the submit hook bails on any
prompt starting with it. The two constants (`llm.InternalSentinel` and
`hooks.internalSentinel`) carry matching comments demanding they stay
identical.

---

**See also:** [Hook internals](hooks-internals.md) · [Vector identity](vector-identity.md) · [Memory lifecycle](memory-lifecycle.md) · [What gets remembered](../guides/what-gets-remembered.md) · [Configuration](../guides/configuration.md) · [Configuration keys](../reference/configuration.md) · [HTTP API reference](../reference/http-api.md)
