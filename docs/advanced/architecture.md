[Docs](../README.md) › Advanced › Architecture

# Architecture

How Continuity's processes divide up the work, how they reach the single SQLite database, and why the daemon is the only thing allowed to write to it.

**Audience:** engineer · **Read time:** ~8 min

## One binary, four roles

There is exactly one executable. Which role it plays is decided by the
subcommand, and each role runs as its own OS process:

| Role | Invocation | Lifetime |
|---|---|---|
| Daemon | `continuity serve` | Long-running |
| Hook handler | `continuity hook start\|submit\|tool\|stop\|end` | Milliseconds, once per Claude Code event |
| MCP server | `continuity mcp` | Lives as long as the agent's MCP client keeps the pipe open |
| CLI | `continuity search`, `continuity remember`, … | One command |

`cmd/continuity/main.go` is fifteen lines: it hands off to `internal/cli`,
which registers every verb on one cobra root. Everything else is a package
under `internal/`.

The daemon is the only role that holds the engine, the embedder, the LLM
client, and the background timers. Hooks and the MCP server are thin HTTP
clients of it — `internal/mcp/mcp.go` says so explicitly: "There is no engine
or store access here, only protocol + HTTP." Every MCP tool maps to an
`/api/*` endpoint, exactly as the CLI does.

### Why the MCP server is a client and not a second engine

The MCP path exists for ergonomics, not capability. Tool arguments are
structured JSON, so a multi-line 40 000-character L2 body never transits a
shell — no quoting, no heredocs. That is the friction the CLI path could not
shed. Giving it its own store handle would have bought nothing and cost the
single-writer property described below.

## Talking to the daemon

`hooks.ResolveServerURL` (`internal/hooks/client.go`) is the single source of
truth for the endpoint, and it must stay in lockstep with `serve`'s bind
resolution or restart and inspection would probe a different address than the
one `serve` listens on. Precedence:

1. `CONTINUITY_URL` — explicit, wins outright.
2. Otherwise `http://<CONTINUITY_BIND|127.0.0.1>:<CONTINUITY_PORT|37777>`.

Three client flavors exist, differing only in patience, and the difference is
deliberate:

- **Hook client — 5 s.** Hooks run inline in Claude Code's event loop, so a
  slow server must not become a slow editor. Five seconds is a latency budget,
  not a health signal.
- **CLI client — 30 s.** These block a human at a terminal, who can afford to
  wait out a slow query rather than have a perfectly healthy server reported
  as dead.
- **Maintenance client — 30 min.** `VACUUM` on a multi-gigabyte database is
  minutes of work. A 30 s budget would abandon the request while the daemon
  kept compacting, leaving the operator with a timeout error for an operation
  that actually succeeded.

### Slow is not dead

`Client.CheckHealth` and `Client.DescribeError` refuse to collapse every
transport error into "not running". A timeout plus a successful TCP dial
(`isListening`, 2 s) means the server *is* there and *is* answering — it just
did not finish in time, and the right advice is `continuity prune`, not "start
the daemon". A timeout with no dial is a black-holed address and reports as
not running.

This distinction is scar tissue. The comment on `CheckHealth` names it: a
healthy daemon answering correctly but slower than the client timeout was
reported as a dead one, sending the reporter after the port and the embedder
while the real cause was an unbounded `observations` table.

`Client.Healthy` — the boolean hooks use — deliberately skips the dial probe.
A boolean gains nothing from the distinction, and paying a second 2 s probe on
top of an elapsed 5 s timeout would cost the editor 7 s per hook instead of 5 s.

## Not everything goes over HTTP

Most CLI verbs are HTTP clients: `search`, `show`, `remember`, `retract`,
`pin`/`unpin`, `extract`, `prune`, `timeline`. A few open the database file
directly with `store.Open`: `profile`, `tree`, `dedup`, `doctor`, and
`embedder status`/`embedder use`.

The split is not arbitrary. Direct-open verbs are inspection or explicit,
operator-initiated repairs that are expected to run with the daemon stopped —
`doctor --repair-vectors --apply` in fact *refuses* to run against a reachable,
unlocked server embedding to a different identity, because that server would
keep writing its own vectors right after the snapshot-first repair.

`continuity snapshot list` / `snapshot prune` use `store.OpenNoMigrate`
instead. Opening with `Open()` would apply any pending risky migration, which
creates a safety snapshot — so a `prune` against a not-yet-migrated database
would manufacture a snapshot and immediately delete it, silently discarding the
only rollback point. Managing snapshot files is not a reason to upgrade the
operator's schema.

## The storage layer

`internal/store/db.go` opens `~/.continuity/continuity.db` through
`modernc.org/sqlite` — pure Go, no CGO, cross-compiles everywhere. On open it
creates the directory `0700`, then runs `hardenPermissions`, which chmods the
directory and the `.db`/`-wal`/`-shm` triple down to `0700`/`0600` if they are
looser. `MkdirAll` and `OpenFile` only set permissions at *creation*, so
pre-existing installs need the explicit pass.

### Pragmas

Five pragmas are set on every connection, in `configurePragmas`:

| Pragma | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | Concurrent reads while the daemon writes — hooks read context while extraction writes. |
| `synchronous` | `NORMAL` | The WAL-appropriate durability/throughput trade. |
| `foreign_keys` | `ON` | `mem_vectors` cascades from `mem_nodes`. |
| `mmap_size` | 256 MB | Memory-mapped reads for the vector scan. |
| `busy_timeout` | 5000 ms | Bounded wait rather than an immediate `SQLITE_BUSY`. |

WAL mode has a consequence that shows up in `internal/store/snapshot.go`: the
main `.db` file is **incomplete on its own**. Recent commits live in
`<path>-wal` until a checkpoint. Every snapshot therefore goes through
`VACUUM INTO`, never a file copy — see [Memory lifecycle](memory-lifecycle.md#snapshot-first)
for the full argument.

`store.OpenMemory` (tests only) pins `SetMaxOpenConns(1)`. A plain `:memory:`
DSN gives every pooled connection its own empty database, so migrations land on
one connection and the telemetry recorder draws a second and sees no tables.
Pinning the pool makes the in-memory database one database — matching the
single-writer reality of the on-disk path.

### Migrations

`internal/store/migrations.go` holds an ordered slice of migrations applied at
`Open`. Two mechanisms are worth knowing:

**The forward-compat guard.** Before anything else, `migrate()` compares
`MAX(schema_versions.version)` against this binary's head. A database stamped
higher fails fast with `ErrSchemaTooNew` rather than letting an older binary
operate against invariants (CHECK constraints, triggers, FK relationships) it
does not understand.

**Risky migrations pin a connection.** Full-table rebuilds
(`CREATE _new` → `INSERT SELECT *` → `DROP` → `RENAME`) are marked `Risky:
true`. `DROP TABLE mem_nodes` would cascade-delete every row in `mem_vectors`
via the v4 foreign key. SQLite's `PRAGMA foreign_keys` is a no-op inside a
transaction, and the pool may hand the transaction a different connection than
a pooled `Exec("PRAGMA …")` touched — so `applyMigration` acquires a single
`*sql.Conn`, toggles FK off on it *outside* any transaction, runs the migration
transaction on that same connection, and restores FK on in a `defer` so a
failed migration never poisons the pool. The migration bodies carry a comment
warning that re-adding an in-SQL `PRAGMA foreign_keys=OFF` is an inert trap.

Risky migrations also take a `VACUUM INTO` safety snapshot first. If the
snapshot cannot be taken, the migration **must not proceed** — that is the
contract. `CONTINUITY_NO_MIGRATION_SNAPSHOT` opts out; the comment on it is
blunt that this means accepting an unrecoverable buggy rebuild.

## Request flow

A hook event travels:

```
Claude Code ──stdin JSON──▶ continuity hook <evt>
                                │  HTTP (127.0.0.1:37777)
                                ▼
                          chi router  ─▶ middleware ─▶ handler ─▶ store / engine
                                │
                          ◀─ JSON ─┘
        ◀──stdout JSON (SessionStart only), stderr, exit 0
```

Router setup lives in `internal/server/server.go`. Middleware runs in this
order: `Recoverer` → `securityHeaders` → `localhostOnly` → `limitRequestBody`.
Everything below `/api` is a handler; every other path falls through to
`spaHandler()`, which serves the `go:embed`-ed Svelte UI with an
`index.html` fallback for client-side routes.

Two routes are still stubs returning 501: `GET /api/sessions` and
`GET /api/sessions/{id}`. The exhaustive route list and the full `/api/health`
payload live in [HTTP API reference](../reference/http-api.md).

### `/api/health` is O(1) on purpose

Health reports `spent_observations` by reading a cached gauge that the last
retention sweep measured (`engine.SpentObservationsGauge`) rather than running
the count. The comment in `server.go` states the reason directly: a health
check whose cost scales with table size would recreate the very failure it
exists to surface — and worse, `continuity prune` would become unreachable on
exactly the databases that need it. `gc_reclaimable` is likewise only computed
when GC is not `off`; off means dormant, no query.

Health also advertises `active_embedder` and `vector_identity_locked` from the
**live** engine, so `doctor` can compare against what the running server
actually embeds with instead of re-resolving a fresh embedder. That gap has a
name in the code: the fresh-resolve blind spot.

## Startup and graceful shutdown

`runServe` (`internal/cli/serve.go`) sequences boot carefully, and the ordering
carries meaning:

1. Load config; apply `CONTINUITY_*` overrides (invalid values are hard errors,
   never silently ignored).
2. Open the database (migrations run here).
3. Build the LLM client and engine. A missing LLM is not fatal — extraction is
   disabled and the engine stays nil.
4. Select the embedder (`selectEmbedder`), then reconcile it against the
   corpus's declared vector identity **before embedding anything**. See
   [Vector identity](vector-identity.md).
5. **`net.Listen` explicitly**, before anything else. A bind failure (port in
   use) surfaces synchronously, and a failed start must not count as a boot —
   otherwise three failed `serve` attempts in a row would tick the snapshot
   retention counter to zero and auto-delete the migration safety snapshot
   without the migrated schema ever having served a request.
6. Only after a successful bind: start the extraction worker, the decay+GC
   timer, and the observation retention timer. GC hard-deletes, so a
   failed-start process must never run a compost sweep.
7. Tick snapshot retention — the bound listener is the genuine "the new schema
   boots and serves" signal — and log any snapshot still retained.
8. Start the hourly metrics rollup.

Retention is deliberately **outside** the `engine != nil` guard. Decay and GC
need an engine because they act on memories; observation retention is pure
storage hygiene, and an install with no LLM records observations at exactly the
same rate. Gating it on the engine would leave those installs growing without
bound — the original failure.

Shutdown, on `SIGINT`/`SIGTERM`:

1. Close `rollupStop` and `retentionStop` (their tickers exit).
2. `httpServer.Shutdown(ctx)` with a 5 s deadline — stop accepting, drain
   in-flight requests.
3. `StopExtractionWorker(10 s)` — the worker gets a bounded window to finish
   its current job. Abandonment is safe: an unfinished job's queue row survives
   and replays on the next boot.
4. Deferred `eng.Stop()` and `db.Close()` unwind.

The event recorder's `Close()` flushes buffered telemetry with a 2 s cap.
Telemetry is droppable by contract, so shutdown never hangs on it.

HTTP server timeouts are `ReadTimeout` 10 s, `WriteTimeout` 30 s, `IdleTimeout`
120 s, `MaxHeaderBytes` 1 MB. `POST /api/prune` extends its own write deadline
to 30 minutes via `http.NewResponseController`, because otherwise a long
`VACUUM` would succeed while the response was discarded.

## Security posture

The threat model is a single-user localhost tool. The mitigations are
proportionate to accidental collision, not to a determined attacker on the box.

**Bound to loopback.** Default bind is `127.0.0.1:37777` (`config.Default()`).

**`localhostOnly` middleware.** Rejects any request whose `Host` header
normalizes to something other than `localhost`, `127.0.0.1`, or `::1`.
`normalizeHost` strips the port, unwraps bracketed IPv6, lowercases, and trims
a trailing dot. This is a DNS-rebinding defense: a page in the user's browser
resolving `evil.example` to `127.0.0.1` still sends `Host: evil.example`, and
gets a 403.

**Body limits.** `limitRequestBody` wraps every body in
`http.MaxBytesReader` at 1 MB. Hook stdin is separately bounded at 10 MB via
`io.LimitReader` in `hooks.Handle`. The MCP scanner caps one JSON-RPC line at
16 MB — a `remember` call can carry a 40 k L2 body plus JSON overhead, so the
default 64 k scanner token is too small.

**Security headers.** `X-Content-Type-Options: nosniff`,
`X-Frame-Options: DENY`, `Referrer-Policy: no-referrer` on every response.

**Filesystem.** Database directory `0700`; database, WAL, SHM, snapshots, and
`serve.log` all `0600`, tightened on existing installs.

**Prompt injection.** LLM prompts wrap untrusted transcript and prompt text in
nonce-delimited data fences (`llm.fencedData`, with the nonce from
`crypto/rand`) so a paste cannot forge a closing marker and escape into the
instruction region. The `claude -p` subprocess is pinned to a dedicated empty
directory so it has no surrounding project to scan.

### Why the daemon owns the write connection

SQLite is a single-writer store. Every write path of consequence —
`remember`, `retract`, extraction, decay, GC, retention, pins, telemetry —
lives behind the daemon, and the CLI verbs that perform them are HTTP clients.

Three concrete reasons the code names:

- **`VACUUM` contends.** `handlePrune`'s comment: prune is "routed through the
  server rather than run against the file directly because the daemon owns the
  write connection — VACUUM from a second process would contend with it."
- **Telemetry must never block a read.** `mem_events` inserts are buffered and
  fire-and-forget (`internal/server/events.go`). A `shown` insert queued behind
  an extraction write could otherwise spend a synchronous surface's whole
  latency budget against `busy_timeout=5000`. A full buffer drops the event and
  counts the drop: telemetry is allowed to lose an event; the surfacing that
  triggered it is never allowed to wait for one.
- **Repairs must not race a live writer.** `doctor --repair-vectors --apply`
  refuses when a reachable, unlocked server reports a different identity,
  precisely because that server would re-mix the corpus immediately after the
  repair.

The direct-open CLI verbs are the deliberate exception, and they are inspection
or explicit repair — never ambient writes.

---

**See also:** [Memory lifecycle](memory-lifecycle.md) · [Vector identity](vector-identity.md) · [Hook internals](hooks-internals.md) · [HTTP API reference](../reference/http-api.md) · [Files and paths](../reference/files-and-paths.md) · [Keeping it healthy](../guides/keeping-it-healthy.md)
