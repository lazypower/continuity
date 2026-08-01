[Docs](../README.md) › Reference › HTTP API

# HTTP API

Every route the `continuity serve` daemon exposes, with request and response shapes, status codes, and the complete `/api/health` payload.

**Audience:** operator · **Read time:** ~9 min

## Base URL

The daemon listens on `http://127.0.0.1:37777` by default. `CONTINUITY_BIND` and `CONTINUITY_PORT` change the host and port; `CONTINUITY_URL` overrides both outright for clients. Every example below assumes the default.

There is no authentication. Access control is the loopback bind plus the localhost-only middleware described at the bottom of this page.

All API paths are prefixed `/api`. Any path that does not match an API route is served by the embedded web UI (single-page app fallback to `index.html`). If the binary was built without the embedded UI, those paths return `404` with the plain-text body `UI not embedded — build with 'make build'`.

---

## Route index

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/health` | Liveness, version, schema, and capacity gauges. Always fast. |
| GET | `/api/context` | The cold-boot context block injected at SessionStart. |
| POST | `/api/sessions/init` | Create or resume a session record. |
| POST | `/api/sessions/{id}/observations` | Append one raw tool-use observation. |
| POST | `/api/sessions/{id}/complete` | Mark a session complete (Stop hook). |
| POST | `/api/sessions/{id}/end` | Mark a session ended (SessionEnd hook). |
| POST | `/api/sessions/{id}/extract` | Queue transcript extraction. **Off by default** — see below. |
| POST | `/api/sessions/{id}/signal` | Queue a signal-keyword capture ("remember this"). |
| POST | `/api/sessions/unmark-empty-extractions` | Backfill: clear the extracted flag on sessions that produced nothing. |
| POST | `/api/prune` | Reclaim spent observations and compact the database file. |
| GET | `/api/search` | Search memory. **Returns pointers only.** |
| GET | `/api/memories` | Fetch one memory's full content. **Counts as a use.** |
| POST | `/api/memories` | Store a memory. |
| POST | `/api/memories/retract` | Retract (tombstone or supersede) a memory. |
| POST | `/api/memories/pin` | Pin a memory to the cold-boot tray. |
| POST | `/api/memories/unpin` | Remove a pin. |
| GET | `/api/memories/pinned` | List live pins. |
| GET | `/api/tree` | Browse the memory tree. |
| GET | `/api/profile` | Relational profile plus profile/preference nodes. |
| GET | `/api/timeline` | Recent sessions. |
| GET | `/api/metrics` | Memory Health dashboard payload. **Advanced** |
| GET | `/api/sessions` | **Not implemented** — always `501`. |
| GET | `/api/sessions/{id}` | **Not implemented** — always `501`. |

Every route returns `Content-Type: application/json` except the UI fallback. Error bodies are uniformly `{"error": "<message>"}`.

---

## GET /api/health

No parameters. Always returns `200` — including when the database ping fails, in which case `db` is `false`. This is deliberate: health is a liveness probe, and it is guaranteed to be O(1) no matter how large the database has grown, so it stays answerable on exactly the installs that are in trouble.

```json
{
  "active_embedder": "model2vec:potion-retrieval-32M:512",
  "api_version": 1,
  "db": true,
  "db_bytes": 47230288,
  "db_path": "/Users/chuck/.continuity/continuity.db",
  "exe": "/opt/homebrew/bin/continuity",
  "gc_mode": "off",
  "gc_reclaimable": 0,
  "pending_extractions": 11,
  "pid": 32221,
  "schema_current": 16,
  "schema_head": 16,
  "spent_observations": 0,
  "started_at": 1785609318,
  "status": "ok",
  "uptime": 1592.457156041,
  "vector_identity_locked": false,
  "version": "dev-4afa791 (4afa791)"
}
```

### Field by field

| Field | Type | Meaning |
|---|---|---|
| `status` | string | Always `"ok"` when the server answered at all. Not a summary of the fields below — check `db` and `vector_identity_locked` yourself. |
| `version` | string | Build version of the running binary. |
| `uptime` | number | Seconds since this process started serving. Fractional. |
| `db` | bool | `false` if the database ping failed. The one field that means "something is broken right now". |
| `api_version` | number | HTTP contract version of the running server. Compared against the client's build to detect a stale daemon after an upgrade. |
| `schema_head` | number | Highest schema migration the running binary knows about. |
| `schema_current` | number | Schema migration actually applied to the open database. Lower than `schema_head` means migrations have not run; `0` can also mean the version could not be read. |
| `pending_extractions` | number | Jobs sitting in the durable extraction queue. A number that only grows means the worker is wedged or no LLM is configured. |
| `gc_mode` | string | Memory garbage collection: `"off"` (default), `"shadow"` (log candidates, delete nothing), or `"on"`. Always `"off"` when no engine is configured. **Advanced** |
| `gc_reclaimable` | number | Memories GC would consider dead weight. Only measured when `gc_mode` is not `"off"`; otherwise `0`. **Advanced** |
| `spent_observations` | number | Raw tool-use records that observation retention could reclaim. Cached from the last retention sweep (which runs at boot and then every 24h), not measured per request — so it can be a few hours stale. When retention is disabled it still reports what the *default* policy would reclaim, so the pile stays visible. Feed this into `POST /api/prune`. |
| `db_bytes` | number | Size of the database file on disk, in bytes. Pair with `spent_observations` for a growth alert. |
| `pid` | number | Process ID of the daemon. |
| `started_at` | number | Unix timestamp (seconds) of process start. |
| `db_path` | string | Absolute path of the open database file. |
| `exe` | string | Absolute path of the running binary. Best-effort — may be empty. Use it to catch "I upgraded but the old binary is still serving". |
| `active_embedder` | string | Identity of the embedder the running server actually uses, as `backend:model:dimensions`. Empty when no engine is configured. |
| `vector_identity_locked` | bool | `true` when the active embedder is incompatible with the vectors already in the corpus. While true, `/api/search` returns `503` and writes that need the retracted-memory safety check are refused. |

A minimal scripted check:

```bash
curl -fsS http://127.0.0.1:37777/api/health \
  | jq -e '.db and (.vector_identity_locked | not) and (.schema_current == .schema_head)'
```

---

## GET /api/search

Returns **pointers, never payloads**: a URI, its category, its one-sentence L0 abstract, and scores. This is deliberate. Full bodies live only behind `GET /api/memories`, so reaching a memory's content is a distinct, countable act rather than something that happens invisibly as a side effect of searching.

| Query param | Required | Default | Notes |
|---|---|---|---|
| `q` | yes | — | The query text. |
| `limit` | no | `10` | Clamped to a maximum of `100`. Non-numeric or non-positive values fall back to the default. |
| `category` | no | all | Restrict to one category. |
| `mode` | no | `find` | `find` is pure vector search. `search` is the LLM-assisted ("smart") mode and needs a configured LLM. Any other value behaves as `find`. |
| `session_id` | no | — | Optional attribution recorded with the exposure telemetry. **Advanced** |

```bash
curl -s 'http://127.0.0.1:37777/api/search?q=observation+retention&limit=3'
```

```json
{
  "query": "observation retention",
  "mode": "find",
  "count": 3,
  "results": [
    {
      "uri": "mem://agent/patterns/retention-keys-on-readership",
      "category": "patterns",
      "l0_abstract": "Retention predicates must key on READERSHIP (who can still read this row?), not adjacent lifecycle state like extraction — coupling to an unrelated subsystem strands data.",
      "score": 0.2745213639057007,
      "similarity": 0.2745213639057007,
      "relevance": 0.9949264679468157
    }
  ]
}
```

`score` is what results are ranked by (similarity, adjusted by a category boost). `similarity` is the raw cosine similarity. `relevance` is the node's own decay state, carried as metadata — it does **not** feed the score. **Advanced**

| Status | When |
|---|---|
| `200` | Results returned (possibly `count: 0`). |
| `400` | `q` missing. |
| `503` | No embedder configured — `{"error":"search not available — no embedder configured"}`. |
| `503` | Vector identity is locked. The body carries the mismatch reason and the repair path. Search fails closed rather than scoring across incompatible vector spaces and returning noise. |
| `500` | Search itself failed. |

Retracted memories never appear in results.

---

## GET /api/memories

Fetch one memory's full content. **This request counts as a use.** It is the only path that refreshes a memory's relevance and increments its access count, which is why search deliberately withholds bodies. Do not poll this endpoint or script it in a loop — you will brighten memories nobody actually read.

| Query param | Required | Default | Notes |
|---|---|---|---|
| `uri` | yes | — | Full `mem://` URI. |
| `include_retracted` | no | `false` | `true` reveals a retracted memory's reason and original content. |
| `session_id` | no | — | Attribution for the use event. **Advanced** |

Normal response (`200`):

```json
{
  "uri": "mem://user/preferences/devbox",
  "category": "preferences",
  "node_type": "leaf",
  "summary": "Always use devbox for development tooling",
  "body": "The project uses devbox shell to provide Go, SQLite tools, and other dev dependencies.",
  "detail": "",
  "relevance": 0.94,
  "created_at": 1785609318000,
  "updated_at": 1785609318000,
  "access_count": 7
}
```

The values shown reflect the node's state *before* this fetch's use is recorded.

**Retracted memories return metadata only, and the content keys are absent — not empty strings, not `null`.** A consumer never has to disambiguate "this memory has no body" from "the body is being withheld". The absent keys are `summary`, `body`, `detail`, `relevance`, `access_count`, and `tombstone_reason`:

```json
{
  "uri": "mem://user/events/old-fact",
  "category": "events",
  "node_type": "leaf",
  "retracted": true,
  "tombstoned_at": 1785000000000,
  "created_at": 1784000000000,
  "updated_at": 1785000000000,
  "superseded_by": "mem://user/events/new-fact"
}
```

`superseded_by` itself is omitted when the retraction was not a supersession. Pass `include_retracted=true` to get the full object, which then also carries `retracted`, `tombstoned_at`, and `tombstone_reason`.

Fetching a retracted memory does **not** count as a use — a retraction is not undone by looking at it.

| Status | When |
|---|---|
| `200` | Found. |
| `400` | `uri` missing. |
| `404` | `{"error":"memory not found"}`. |
| `500` | Lookup failed. |

---

## POST /api/memories

Store a memory. Request body:

```json
{
  "category": "preferences",
  "name": "devbox-tooling",
  "summary": "Always use devbox for development tooling.",
  "body": "The project uses devbox shell to provide Go, SQLite tools, and other dev dependencies.",
  "detail": "",
  "session_id": "",
  "acknowledge_retracted": false
}
```

`category`, `name`, `summary`, and `body` are required. `name` is a slug and is sanitized to `[a-z0-9_-]`. The stored URI is `mem://<owner>/<category>/<name>`, where the owner is derived from the category.

Tier limits: `summary` ≤ 200 characters, `body` ≤ 2000 (and ≥ 20), `detail` ≤ 40000. **Over-limit content is truncated at a word boundary, not rejected** — the request still succeeds and the server logs the truncation. A `body` shorter than 20 characters *is* rejected.

| Status | Body | When |
|---|---|---|
| `201` | `{"status":"created","uri":"..."}` | New memory written. |
| `200` | `{"status":"updated","uri":"..."}` | Existing memory in a mergeable category updated in place. |
| `400` | `{"error":"..."}` | Invalid JSON, a missing required field, an invalid category, a body under the minimum, or a slug that collides with a retracted memory. Validation messages are surfaced verbatim. |
| `409` | see below | The candidate is semantically close to a memory that was retracted. |
| `503` | `{"error":"engine not configured"}` | No LLM/embedder engine is available. |

The `409` body names the matches but never their reasons:

```json
{
  "status": "matches_retracted",
  "matched_uris": ["mem://user/events/old-fact"],
  "hint": "inspect each with `continuity show <uri> --include-retracted` before proceeding; pass --acknowledge-retracted to override"
}
```

Re-send with `"acknowledge_retracted": true` to write anyway. This gate also fails closed while the vector identity is locked: without acknowledgement the write is refused, because the safety check cannot run.

---

## POST /api/memories/retract

```json
{ "uri": "mem://user/events/old-fact", "reason": "test repro, no ongoing value", "superseded_by": "" }
```

`uri` and `reason` are required. `superseded_by` is optional and turns the retraction into a supersession.

| Status | Body | When |
|---|---|---|
| `200` | `{"status":"retracted","uri":"...","superseded_by":"..."}` | Newly retracted. |
| `200` | `{"status":"already_retracted", ...}` | Idempotent repeat. |
| `400` | `{"error":"..."}` | Invalid JSON, missing `uri` or `reason`, or a validation failure. |
| `503` | `{"error":"engine not configured"}` | No engine. |

---

## Pins

Pins are the cold-boot tray: pinned memories are injected into every session regardless of relevance. At most **7** pins may be live at once, enforced at write time.

Pin routes work without an LLM or embedder — they only stamp a timestamp — so they stay available on installs where `/api/memories` returns `503`.

| Route | Body | Success |
|---|---|---|
| `POST /api/memories/pin` | `{"uri":"mem://..."}` | `200` `{"status":"pinned"\|"already_pinned","uri":"..."}` |
| `POST /api/memories/unpin` | `{"uri":"mem://..."}` | `200` `{"status":"unpinned"\|"not_pinned","uri":"..."}` |
| `GET /api/memories/pinned` | — | `200` `{"count":N,"pins":[...]}` |

Both write routes return `400` for invalid JSON, a missing `uri`, a URI without the `mem://` prefix, or a domain rejection (memory not found, target is a directory, target is retracted, pin cap reached). `GET /api/memories/pinned` returns `500` on a read failure; each pin carries `uri`, `category`, `l0_abstract`, `l1_overview` (omitted when empty), `relevance`, and `pinned_at`, oldest first. Retracted memories are excluded.

---

## GET /api/tree

| Query param | Required | Default | Notes |
|---|---|---|---|
| `uri` | no | roots | Omit to list root directories; pass a directory URI to list its children. |
| `include_retracted` | no | `false` | Include retracted leaves, and count them in `children`. |

```json
{
  "uri": "mem://user/preferences",
  "nodes": [
    { "uri": "mem://user/preferences/devbox", "node_type": "leaf", "category": "preferences", "l0_abstract": "Always use devbox…", "pinned": true }
  ]
}
```

`children` appears on directories only. `retracted` and `pinned` are omitted when false. Retracted nodes carry no `l0_abstract` or `l1_overview` — the same absence-not-empty contract as `GET /api/memories`. Returns `200`, or `500` on a read failure.

---

## GET /api/profile

No parameters. Returns the synthesized relational profile plus every `profile` and `preferences` node that has an L0 abstract.

```json
{
  "relational_profile": "Prefers terse responses…",
  "nodes": [
    { "uri": "mem://user/preferences/devbox", "category": "preferences", "l0_abstract": "…", "l1_overview": "…", "relevance": 0.94 }
  ]
}
```

`relational_profile` is an empty string when no profile has been synthesized yet. `nodes` may be `null`. Returns `200`, or `500` if the profile lookup fails.

---

## GET /api/context

Returns the markdown context block Continuity injects at SessionStart — the relational profile, pins, moments, recent sessions, and the current session's observation count — capped at a 4000-character budget.

| Query param | Required | Default | Notes |
|---|---|---|---|
| `session_id` | no | — | Excludes the current session from the "recent sessions" list and attributes exposure telemetry. |
| `preview` | no | `false` | `true` renders the block **without** side effects: moment rotation is not advanced and nothing is journaled as shown. Use this for any inspection; a plain call consumes rotation. |

Always `200`, body `{"context": "<context>…</context>"}`.

---

## Sessions and observations

These are the hook path. You normally do not call them by hand.

| Route | Request | Success | Errors |
|---|---|---|---|
| `POST /api/sessions/init` | `{"session_id":"…","project":"…"}` | `200` `{"session_id","status","tool_count"}` | `400` invalid JSON or missing `session_id`; `500` |
| `POST /api/sessions/{id}/observations` | `{"tool_name","tool_input","tool_response"}` | `201` `{"status":"ok"}` | `400` unreadable or invalid JSON; `500` |
| `POST /api/sessions/{id}/complete` | empty | `200` `{"status":"completed"}` | never fails — a session that was not active returns `200` `{"status":"ok"}` |
| `POST /api/sessions/{id}/end` | empty | `200` `{"status":"ended"}` | `500` |

`GET /api/sessions` and `GET /api/sessions/{id}` are **not implemented**. Both return `501` with `{"error":"sessions not yet implemented"}` and `{"error":"session detail not yet implemented"}` respectively. Use `GET /api/timeline` for session history.

---

## POST /api/sessions/{id}/extract

Queues transcript extraction for a session. Request body: `{"transcript_path":"…","force":false}`.

**Automatic extraction is off by default.** When it is off and `force` is not set, the route short-circuits:

```json
{ "status": "extraction_disabled" }
```

That response is `200`, not an error — it is a stable part of the contract, and it is what the Stop and SessionEnd hooks receive on a default install. `extraction_disabled` is the expected steady state, not a symptom. Send `"force": true` (what `continuity extract --force` does) to extract anyway.

| Status | Body | When |
|---|---|---|
| `202` | `{"status":"extracting"}` | Job durably enqueued; the worker drains it asynchronously. |
| `200` | `{"status":"extraction_disabled"}` | Auto-extraction off and `force` not set. |
| `400` | `{"error":"invalid json"}` | Malformed body. |
| `500` | `{"error":"internal error"}` | Enqueue failed. |
| `503` | `{"error":"engine not configured"}` | No engine. |

## POST /api/sessions/{id}/signal

The signal-keyword path ("remember this", "always use X"). Unaffected by the auto-extraction toggle. Request body `{"prompt":"…"}`; `prompt` is required.

Returns `202` `{"status":"processing"}`, or `400` / `500` / `503` on the same conditions as extract.

## POST /api/sessions/unmark-empty-extractions

No body. Clears the extracted marker on every session that was marked extracted but produced zero memories, making them eligible again. Returns `200` `{"status":"ok","unmarked":N}` or `500`. This is what `continuity extract --backfill-empty` calls.

---

## POST /api/prune

Reclaims spent observations — the raw tool-use records that only ever served their own session's live context — and by default compacts the file with `VACUUM`. Memories, vectors, and the relational profile are never touched.

This runs through the daemon because the daemon owns the write connection; running `VACUUM` from a second process would contend with it.

| Query param | Default | Notes |
|---|---|---|
| `dry_run` | `false` | `dry_run=true` reports what would be reclaimed and deletes nothing. |
| `vacuum` | `true` | `vacuum=false` prunes without compacting. Compaction is the slow half and needs free disk roughly equal to the current file size. |

Dry run (`200`):

```json
{ "status": "ok", "dry_run": true, "reclaimable": 0, "bytes_before": 47230288 }
```

Real run (`200`):

```json
{ "status": "ok", "pruned": 128394, "vacuumed": true, "bytes_before": 47230288, "bytes_after": 21118976 }
```

If the delete succeeds but `VACUUM` fails, the call still returns `200` with `"vacuumed": false` — the durable half already happened, and the failure is logged. `500` means the prune itself failed.

A real prune with vacuum extends its own write deadline to **30 minutes**, past the server's global 30-second write timeout, because `VACUUM` on a multi-gigabyte database is legitimately minutes of work. Set your client's timeout accordingly; `continuity prune` already does.

---

## GET /api/timeline

| Query param | Default | Notes |
|---|---|---|
| `since` | 90 days ago | Unix milliseconds. `0` or an unparseable value falls back to the default. |

Returns a bare JSON **array** (not an object), oldest boundary forward, of `{"project","started_at","tool_count","tone"}`. `tone` is omitted when unset. `200`, or `500`.

## GET /api/metrics — **Advanced**

The read-only Memory Health payload behind the dashboard. Computing it never mutates the store; relevance is derived live from timestamps rather than read from the stored column, so it does not depend on a decay sweep having run.

Top-level keys: `generated_at`, `summary` (`active_total`, `retracted_total`, `fresh`, `fading`, `stale`, `never_retrieved`, `retraction_rate`, `recent_retractions`), `categories`, `histogram`, `needs_attention` (`stale_high_retrieval`, `never_retrieved_old`, `near_decay_cliff`, `orphaned_tombstones`), `critical`, and `daily`. Returns `200`, or `500`.

---

## Middleware and transport

Four middleware layers wrap every route, in this order:

1. **Panic recovery.** A handler panic becomes a `500` instead of killing the daemon.
2. **Security headers.** Every response carries `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and `Referrer-Policy: no-referrer`.
3. **Localhost-only.** The `Host` header must normalize to `localhost`, `127.0.0.1`, or `::1` — ports, bracketed IPv6, casing, and a trailing dot are all handled. Anything else gets `403` `{"error":"forbidden"}` before the handler runs. This blocks DNS-rebinding attacks, where a page in your browser resolves an attacker-controlled name to `127.0.0.1` and talks to your daemon. It is a `Host`-header check, not a source-address check.
4. **1 MB request body limit.** Bodies are capped at 1,048,576 bytes. An oversized body makes the handler's JSON decode fail, so in practice you get `400` `{"error":"invalid json"}` — not a `413`. This matters for `POST /api/memories` with a large `detail`: 40,000 characters fits comfortably, but a multi-megabyte payload is rejected as malformed rather than truncated.

Header size is capped separately at 1 MB.

### Server timeouts

| Setting | Value | Effect |
|---|---|---|
| Read timeout | 10s | Time allowed to read the full request. |
| Write timeout | 30s | Time allowed to write the response. `POST /api/prune` raises its own deadline to 30 minutes when it is going to run `VACUUM`. |
| Idle timeout | 120s | Keep-alive connections are closed after 2 minutes of silence. |
| Max header bytes | 1 MB | Oversized headers are rejected. |

Client-side, `continuity`'s own timeouts differ by caller: hooks allow 5 seconds (they run inline in your editor's event loop), interactive CLI commands allow 30 seconds, and maintenance commands allow 30 minutes.

---

**See also:** [CLI](cli.md) · [MCP tools](mcp-tools.md) · [Configuration keys](configuration.md) · [Keeping it healthy](../guides/keeping-it-healthy.md)
