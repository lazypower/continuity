[Docs](../README.md) › Reference › CLI

# CLI reference

Every `continuity` command and subcommand, every flag with its default, and whether it changes your data.

**Audience:** operator · **Read time:** ~8 min

---

## How to read this page

- **Changes data** says whether running the command can write to your database,
  your config, or files in your home directory. Anything that *deletes* is
  called out as **destructive**.
- **Needs the server** says whether the command talks to a running
  `continuity serve` over HTTP. If it does and the server is down, you get
  `continuity server is not running — start it with: continuity serve`.
- The only flag available everywhere is `-h` / `--help`. Every other flag is
  per-command.
- Running `continuity` with no arguments prints help. It does not report status.
- `continuity help [command]` and `continuity completion <bash|zsh|fish|powershell>`
  are standard cobra commands, generated for free. `completion` writes a shell
  completion script to stdout and changes nothing.

Two commands share a name and are easy to confuse:

- `continuity prune` — deletes **spent observations** (raw tool-call records) and
  compacts the database.
- `continuity snapshot prune` — deletes **migration safety snapshots** (database
  backup copies taken before a risky upgrade).

---

## Every command at a glance

| Command | What it does | Changes data | Needs the server |
|---|---|---|---|
| [`version`](#version) | Print version, commit, build date | no | no |
| [`serve`](#serve) | Run the HTTP API and background workers | yes | — |
| [`init`](#init) | Write Continuity's directives into `~/.claude/CLAUDE.md` | yes | no |
| [`search <query>`](#search) | Search memories, print summaries + addresses | no | yes |
| [`show <uri>`](#show) | Print one memory's full content | yes (marks it used) | yes |
| [`tree [uri]`](#tree) | Browse the memory tree | no | no |
| [`profile`](#profile) | Print the relational profile | no | no |
| [`timeline`](#timeline) | Show session history clustered over time | no | yes |
| [`remember`](#remember) | Write a memory directly | yes | yes |
| [`retract <uri>`](#retract) | Mark a memory retracted | yes | yes |
| [`pin [uri]`](#pin--unpin) | Pin a memory to every session start, or list pins | yes (with a URI) | yes |
| [`unpin <uri>`](#pin--unpin) | Remove a pin | yes | yes |
| [`doctor`](#doctor) | Diagnose embedder/vector health | no (unless repairing) | no |
| [`embedder status`](#embedder-status) | Report the active embedder and whether it matches the corpus | no | no |
| [`embedder use <backend>`](#embedder-use) | Migrate the corpus to a different embedding backend | yes | no |
| [`prune`](#prune) | **Destructive.** Delete spent observations, compact the database | yes | yes |
| [`dedup`](#dedup) | **Destructive.** Merge semantically duplicate memories | yes | no |
| [`snapshot list`](#snapshot-list) | List retained migration safety snapshots | no | no |
| [`snapshot prune`](#snapshot-prune) | **Destructive.** Delete all migration safety snapshots | yes | no |
| [`extract [session-id]`](#extract) | Re-run memory extraction for a past session | yes | yes |
| [`restart`](#restart) | Bounce the running server onto the current binary | yes (applies migrations) | no |
| [`install-service`](#install-service--uninstall-service) | Install the launchd/systemd service | yes | no |
| [`uninstall-service`](#install-service--uninstall-service) | Remove the service | yes | no |
| [`mcp`](#mcp) | Run the MCP server on stdio | yes (via its tools) | yes |
| [`hook <event>`](#hook) | Handle a Claude Code hook event | yes | yes |

**Aliases:** `doctor` also answers to `diagnose`. `show` also answers to `get`
and `cat`.

### One thing that surprises people

Every command that reads the database **directly** — `tree`, `profile`, `dedup`,
`doctor`, `embedder status`, `embedder use` — opens it in a mode that applies any
pending schema migration. So a "read-only" inspection command can upgrade your
schema (and take a safety snapshot) the first time you run it after an upgrade.
`snapshot list` and `snapshot prune` are the exceptions: they deliberately open
without migrating.

---

## Commands

### `version`

```
continuity version
```

Prints `continuity <version> (commit: <sha>, built: <date>)`. No flags. Read-only.

---

### `serve`

```
continuity serve
```

Starts the HTTP API on `127.0.0.1:37777` (see
[Configuration](configuration.md)) and runs the background work: the extraction
worker, the relevance-decay timer, the daily observation-retention sweep, and an
hourly metrics rollup. Logs to stderr.

**No flags.** Everything is configured through `config.toml` and `CONTINUITY_*`
environment variables — see [Configuration](configuration.md).

**Changes data.** On boot it applies pending schema migrations (taking a safety
snapshot first for risky ones), fills in any missing embedding vectors, advances
the snapshot-retention counter, and runs an observation-retention sweep
immediately and then once every 24 hours.

If a schema migration snapshot is still retained, `serve` prints its path and how
many more successful boots remain before it auto-deletes.

---

### `init`

```
continuity init [--autostart]
```

Writes Continuity's memory directives into `~/.claude/CLAUDE.md`, inside a
managed block delimited by `<!-- continuity:managed -->` markers. Everything
outside the markers is preserved verbatim, and re-running refreshes the block in
place. Safe to run repeatedly.

| Flag | Default | Effect |
|---|---|---|
| `--autostart` | `false` | Creates `~/.continuity/autostart`, so the SessionStart hook launches the server when it finds it down. **Omitting the flag deletes that marker**, disabling autostart. |

**Changes data.** Writes `~/.claude/CLAUDE.md` and creates or removes
`~/.continuity/autostart`. Note that `--autostart` is not a toggle you set once:
a later plain `continuity init` turns autostart back off.

---

### `search`

```
continuity search <query...>
```

Searches the memory tree and prints one line per hit: score, address, then the
summary and category. **Search returns pointers, not payloads** — to read a
memory's body, run `continuity show` on the address.

| Flag | Default | Effect |
|---|---|---|
| `-n`, `--limit <n>` | `10` | Maximum results. The server caps this at **100**. |
| `-c`, `--category <name>` | *(none)* | Restrict to one category. |
| `--smart` | `false` | LLM-assisted search instead of plain vector similarity. Requires a configured LLM provider. |
| `--explain` | `false` | Print the score decomposition per result. **Advanced.** |

Read-only. Searching does not mark memories as used and does not affect their
relevance.

If the active embedder does not match what the corpus was built with, search
returns an error rather than nonsense results. See
[Embedding backends](../guides/embedders.md).

---

### `show`

```
continuity show <uri>
continuity get <uri>
continuity cat <uri>
```

Prints a memory's summary, body, and detail. The `mem://` prefix is optional —
it is added for you.

| Flag | Default | Effect |
|---|---|---|
| `--layer <tier>` | `all` | One of `all`, `summary`, `body`, `detail`. Anything else is rejected. |
| `--json` | `false` | Emit the server's JSON response instead of formatted text. |
| `--include-retracted` | `false` | Reveal a retracted memory's reason text and original content. |

**Changes data — mildly.** Opening a memory is the one action Continuity counts
as *use*: it resets that memory's relevance to full and records a `deepened`
event. Retracted memories are excluded from this.

For a retracted memory without `--include-retracted`, you get metadata only (the
address, that it is retracted, and what superseded it) — the reason and content
are omitted, not blanked.

---

### `tree`

```
continuity tree [uri]
```

With no argument, lists the root directories and how many children each has.
With an address, lists that node's children.

| Flag | Default | Effect |
|---|---|---|
| `--include-retracted` | `false` | Include retracted memories, marked `[retracted]`, and count them in child totals. |

Reads the database directly — does not need the server. May apply a pending
schema migration (see [above](#one-thing-that-surprises-people)).

---

### `profile`

```
continuity profile
```

Prints the relational profile — what Continuity has worked out about how you
work. If none exists yet it says so.

| Flag | Default | Effect |
|---|---|---|
| `--verbose` | `false` | Also list every `profile` and `preferences` node by address. |

Reads the database directly — does not need the server.

---

### `timeline`

```
continuity timeline
```

Groups your sessions into activity clusters per project, with gaps called out.
Sessions that recorded no tool calls are omitted.

| Flag | Default | Effect |
|---|---|---|
| `--days <n>` | `90` | How far back to look. |
| `--project <name>` | *(all)* | Only projects whose path ends with this string. |

Read-only.

---

### `remember`

```
continuity remember -c <category> -n <name> -s <summary> -b <body>
```

Writes a memory directly, with no LLM involved.

| Flag | Default | Effect |
|---|---|---|
| `-c`, `--category` | **required** | One of `profile`, `preferences`, `feedback`, `entities`, `events`, `patterns`, `cases`, `moments`, `reference`. Validated before anything is sent. |
| `-n`, `--name` | **required** | The address slug, e.g. `devbox-tooling`. |
| `-s`, `--summary` | **required** | The summary tier. One sentence, 200 characters. |
| `-b`, `--body` | **required** | The body tier. Up to 2,000 characters. |
| `-d`, `--detail` | *(empty)* | The detail tier. Up to 40,000 characters. |
| `--session <id>` | *(empty)* | Attribute the write to a session, for provenance. |
| `--acknowledge-retracted` | `false` | Proceed even though the write closely matches something you previously retracted. |

**Changes data.** Depending on the category, the write may merge into an existing
memory rather than creating a new one.

**Exit code 2** means the write was refused because it matches a *retracted*
memory. The matching addresses are printed to stderr. Inspect them with
`continuity show <uri> --include-retracted`, then either rephrase or re-run with
`--acknowledge-retracted`.

---

### `retract`

```
continuity retract <uri> --reason "<why>"
```

Marks a memory retracted. It stays in the tree as a marker but is excluded from
search, `tree`, and session injection.

| Flag | Default | Effect |
|---|---|---|
| `-r`, `--reason` | **required** | Why, in one sentence. |
| `--superseded-by <uri>` | *(none)* | Link to the memory that replaces this one, turning the retraction into a supersession. Must start with `mem://`. |

**Changes data.** This is not a delete — the marker is permanent and the original
content remains readable via `show --include-retracted`.

---

### `pin` / `unpin`

```
continuity pin                  # list current pins
continuity pin <uri>            # pin a memory
continuity unpin <uri>          # remove a pin
```

Pinned memories are injected into every cold session start regardless of
relevance. Retraction wins: a pinned memory that is later retracted goes silent.
There is a hard cap of **7 live pins**; pinning an eighth is refused.

No flags on either command. `continuity pin` with no argument is read-only;
with a URI it changes data.

---

### `doctor`

```
continuity doctor
continuity diagnose
```

Checks whether stored embedding vectors are coherent with the embedder actually
in use: the active embedder and expected dimension, the distribution of stored
vectors, missing vectors, mixed dimensions, stale vectors from an older model,
and a read-only retrieval smoke test that samples up to 10 memories and asks
whether each one finds itself. Ends with a `healthy` or `degraded` verdict.

| Flag | Default | Effect |
|---|---|---|
| `--json` | `false` | Emit the full report as JSON. **Advanced.** |
| `--repair-vectors` | `false` | Switch to repair mode: re-embed stale and missing vectors. **Dry-run unless `--apply` is also passed.** |
| `--apply` | `false` | With `--repair-vectors`: snapshot the database first, then actually re-embed. |

Plain `doctor` is strictly read-only — it never writes, re-embeds, or touches
access metrics.

`--repair-vectors --apply` **changes data**: it takes a snapshot, embeds
everything into memory first (so a failure part-way leaves the corpus untouched),
writes the new vectors, then rebinds the corpus's declared vector identity. It
**refuses to run** if a live server is reachable, unlocked, and embedding with a
different identity — stop the server first. Afterwards, run `continuity restart`.

---

### `embedder status`

```
continuity embedder status
```

Answers one question: what embedder am I actually using, and does it match my
corpus? Prints the configured backend, the resolved active embedder, the corpus's
declared identity, whether they match, and — if a server is running — what that
server is embedding with and whether it is locked.

No flags. Read-only. Run `continuity doctor` for the full vector-health picture.

---

### `embedder use`

```
continuity embedder use <backend>
```

`<backend>` is one of `model2vec`, `ollama`, or `hashtf` (`tfidf` is accepted as
a spelling of `hashtf`). Anything else is rejected.

This is the **only** command that migrates an existing corpus to a different
embedding backend. In order it:

1. Builds the target embedder and fails if it cannot — Ollama must be reachable
   and have the model pulled; model2vec's files are downloaded if absent.
2. Writes `[embedder].backend` into `~/.continuity/config.toml`.
3. Snapshots the database, re-embeds every memory, and rebinds the corpus's
   declared identity.
4. Tells you to run `continuity restart`.

No flags — there is no dry run. Use `embedder status` or `doctor` to see the
situation first.

**Changes data.** Editing `[embedder].backend` in `config.toml` by hand does
*not* do this: it changes what the next `serve` picks, leaves your vectors alone,
and search locks on the mismatch.

---

### `prune`

```
continuity prune
```

**Destructive.** Deletes spent observations — the raw tool-use records captured
during a session, which exist only to serve that session's live context header.
Memories, vectors, and the relational profile are never touched.

| Flag | Default | Effect |
|---|---|---|
| `--dry-run` | `false` | Report how many observations are reclaimable and the current database size. Deletes nothing. |
| `--skip-vacuum` | `false` | Delete the rows but skip compaction. Freed pages become reusable, but the file does not shrink. |

By default this also runs `VACUUM`, which is what actually returns space to the
filesystem. VACUUM needs free disk space roughly equal to the current database
size and can take minutes on a large file. The command allows up to **30 minutes**
for a real prune; `--dry-run` uses the ordinary 30-second budget.

Which observations count as spent is governed by
`CONTINUITY_OBSERVATION_RETENTION_DAYS` — see [Configuration](configuration.md).
Setting retention to `off` makes `prune` reclaim nothing.

Unlike other server-backed commands, `prune` does not health-check first. It is
the remedy for a database so large the server is slow, so gating it behind a
probe that can itself time out would put the fix out of reach.

---

### `dedup`

```
continuity dedup
```

**Destructive.** Finds memories whose vectors are near-identical and merges them,
deleting the duplicates.

| Flag | Default | Effect |
|---|---|---|
| `--threshold <0.0–1.0>` | see below | Cosine similarity above which two memories are considered the same. |
| `--dry-run` | `false` | Print the node count and the threshold that would be used, then stop without deleting. |

The `--threshold` default is **embedder-aware**. If you do not pass the flag, the
threshold is `0.65` for a semantic embedder and `0.50` for the lexical `hashtf`
fallback, matching the engine's own automatic dedup. If you *do* pass it, your
value is used exactly. (The value cobra reports in `--help` is the bare `0.65`;
the calibrated value is printed when the command runs.)

Note that `dedup` picks its own embedder — Ollama's `nomic-embed-text` if
reachable at `http://localhost:11434`, otherwise the lexical fallback. It does
**not** honor `CONTINUITY_EMBEDDER` or `[embedder].backend`. It then reconciles
against the corpus's declared vector identity and **refuses to run on a
mismatch**, because clustering across incompatible vector spaces would delete the
wrong things.

---

### `snapshot list`

```
continuity snapshot list
```

Lists retained migration safety snapshots with their path, the schema versions
they span, when they were created, and how many boots remain before auto-delete.
Also prints the exact `cp` command to restore one. No flags. Read-only, and
deliberately does **not** apply pending migrations.

---

### `snapshot prune`

```
continuity snapshot prune
```

**Destructive.** Deletes every retained migration safety snapshot file and clears
the tracking table. After this there is no automated way to roll back the most
recent risky migration. No flags, and **no confirmation prompt**.

Only snapshots belonging to this database are unlinked; a stale tracking row
pointing at another database's file is cleared but the file is left alone.

---

### `extract`

```
continuity extract <session-id>
continuity extract --backfill-empty
```

Re-runs memory extraction for a completed session. With a session id, the
transcript is auto-discovered at `~/.claude/projects/*/<session-id>.jsonl`.

| Flag | Default | Effect |
|---|---|---|
| `--force` | `false` | Bypass the idempotency guard and re-extract a session already marked done. **Also required to extract at all while automatic extraction is off** — which it is by default. |
| `--transcript <path>` | *(auto-discover)* | Point at a specific transcript file. |
| `--backfill-empty` | `false` | Unmark every session flagged as extracted that has no memories attributed to it, so they can be extracted again. Cannot be combined with a session id, `--force`, or `--transcript`. |

**Changes data.** Extraction is asynchronous — the command queues the job and
returns; progress goes to `~/.continuity/serve.log`.

Without `--force`, and with automatic extraction off, the server accepts the
request and skips it; the command says so plainly rather than claiming it queued.

---

### `restart`

```
continuity restart
```

Bounces the running server so it picks up an upgraded binary. Before stopping
anything it confirms via `/api/health` that the process really is Continuity, and
**refuses** rather than killing an unrelated process holding the port.

| Flag | Default | Effect |
|---|---|---|
| `-y`, `--yes` | `false` | Skip the confirmation prompt. |
| `--timeout <duration>` | `60s` | How long to wait for the server to come back healthy. Accepts Go durations, e.g. `5m`. |

**Changes data.** The restarted server applies any pending schema migration,
taking a safety snapshot first — which is why it prompts. On the first restart
after an upgrade this can legitimately exceed the timeout on a large database;
the command then reports that it may still be coming up rather than failing.

If a service is installed, the restart is routed through launchd or systemd. If
not, and the running server strongly identifies itself, it is stopped and
respawned directly. A reachable server that cannot be positively identified is
never killed.

---

### `install-service` / `uninstall-service`

```
continuity install-service
continuity uninstall-service
```

Installs Continuity as a platform service that starts at login and restarts on
crash — a LaunchAgent on macOS, a systemd user unit on Linux. Not supported on
Windows. No flags on either command; both are interactive and show what they will
do before asking to proceed.

**Changes data.** Writes or removes a service definition — see
[Files and paths](files-and-paths.md).

Install captures your current `PATH` and bakes it into the service definition, so
the service can find `claude` and `ollama`. If you move those binaries, re-run
`install-service`.

---

### `mcp`

```
continuity mcp
```

Runs a Model Context Protocol server on stdio, exposing six memory tools —
`remember`, `search`, `show`, `tree`, `profile`, `retract` — to an MCP client.
No flags. It is a thin client of the daemon, so `continuity serve` must be
running.

Register it in `.mcp.json`:

```json
{
  "mcpServers": {
    "continuity": { "command": "continuity", "args": ["mcp"] }
  }
}
```

**Changes data** through the tools it exposes. See [MCP tools](mcp-tools.md).

---

### `hook`

```
continuity hook start | submit | tool | stop | end
```

Handles a Claude Code hook event. Each subcommand reads the event JSON on stdin
and writes its response on stdout. You do not run these by hand — Claude Code
does, per the entries in `~/.claude/settings.json`.

| Subcommand | Claude Code event |
|---|---|
| `start` | `SessionStart` |
| `submit` | `UserPromptSubmit` |
| `tool` | `PostToolUse` |
| `stop` | `Stop` |
| `end` | `SessionEnd` |

No flags. **Changes data** — this is the path that records sessions and
observations and injects context.

**Hooks always exit 0**, including on error, so a Continuity failure can never
break your editing session. Problems surface as stderr messages instead. If the
server is unreachable, you get one warning per session — not one per tool call.

`hook start` is the only one that will start the server for you, and only when
`~/.continuity/autostart` exists.

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success. Also every `continuity hook` outcome, including failures. |
| `1` | Command failed. The reason is on stderr. |
| `2` | `continuity remember` only: refused because the write matches a retracted memory. |

---

**See also:** [Configuration](configuration.md) · [Files and paths](files-and-paths.md) · [HTTP API](http-api.md) · [Keeping it healthy](../guides/keeping-it-healthy.md) · [Embedding backends](../guides/embedders.md)
