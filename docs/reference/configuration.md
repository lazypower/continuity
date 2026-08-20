[Docs](../README.md) › Reference › Configuration keys

# Configuration keys

Every key Continuity reads from `~/.continuity/config.toml`, every `CONTINUITY_*` environment variable, and which one wins.

**Audience:** operator · **Read time:** ~6 min

---

## The short version

- **Environment variables beat `config.toml`.** Always.
- `config.toml` is **optional**. Every setting has a working default, and a
  missing file is not an error.
- Continuity only writes `config.toml` in one place: `continuity embedder use`,
  which rewrites `[embedder].backend` and leaves every other line untouched.
- **Hooks are not configured here.** They live in `~/.claude/settings.json`.
  There is no `[hooks]` section.

For *why* you would change any of this, see
[Configuration](../guides/configuration.md) and
[Embedding backends](../guides/embedders.md).

---

## `~/.continuity/config.toml`

### `[server]`

| Key | Type | Default | Notes |
|---|---|---|---|
| `bind` | string | `127.0.0.1` | Address `continuity serve` listens on. Loopback by default — Continuity has no authentication. |
| `port` | integer | `37777` | Port `continuity serve` listens on. |

**These do not affect the CLI.** `continuity search`, `restart`, and the hooks
work out which server to talk to from `CONTINUITY_URL` / `CONTINUITY_BIND` /
`CONTINUITY_PORT` only. If you change `[server].port` in `config.toml`, you must
also set `CONTINUITY_PORT` in your environment or the CLI will keep knocking on
37777.

### `[database]`

| Key | Type | Default | Notes |
|---|---|---|---|
| `path` | string | `~/.continuity/continuity.db` | Where the SQLite database lives. |

**This only affects `continuity serve`.** Commands that open the database
directly — `tree`, `profile`, `dedup`, `doctor`, `embedder status`,
`embedder use`, `snapshot list`, `snapshot prune` — use `CONTINUITY_DB` if set,
otherwise the default path. They never read `[database].path`. If you move the
database, set `CONTINUITY_DB` too or those commands will operate on a different
file than the server.

### `[llm]`

Governs the language model used for extraction and merge decisions. If no
provider can be constructed, `serve` prints a warning and runs with extraction
disabled — everything else, including `remember`, `search`, and retention, works
normally.

| Key | Type | Default | Accepted values |
|---|---|---|---|
| `provider` | string | `claude-cli` | `claude-cli`, `anthropic`, `ollama`. Anything else fails at startup with `unknown LLM provider`. |
| `model` | string | `haiku` | Model name for `claude-cli` and `anthropic`. Empty falls back to `haiku` for `claude-cli`, `claude-haiku-4-5-20251001` for `anthropic`. |
| `ollama_url` | string | `http://localhost:11434` | Base URL for Ollama, used for both chat and embeddings. |
| `ollama_model` | string | `llama3.2` | Chat model, used only when `provider = "ollama"`. |
| `embedding_model` | string | `nomic-embed-text` | Ollama embedding model. Only relevant when the Ollama embedding backend is active. |
| `anthropic_key` | string | *(empty)* | API key. Required when `provider = "anthropic"`. Prefer the `ANTHROPIC_API_KEY` environment variable. |

`provider = "claude-cli"` shells out to the `claude` binary. If that binary is
not on the server process's `PATH`, `serve` warns at startup that extraction will
fail — the usual cause is a service started by launchd or systemd, which does not
inherit your login `PATH`. Re-running `continuity install-service` bakes a usable
`PATH` into the service definition.

### `[extraction]`

| Key | Type | Default | Notes |
|---|---|---|---|
| `auto` | boolean | `false` | Automatic session-end extraction: the Stop/SessionEnd hooks asking an LLM to infer memories from the whole transcript. |
| `relational_auto` | boolean | `true` | Automatic relational profiling at session end: analyzing how you work and merging the result into the single profile node. Independent of `auto`. |

`auto` is off by default on purpose. When you turn it on, `serve` prints a
warning at startup saying so. Explicit `remember` calls, the signal-phrase path,
and `continuity extract --force` are unaffected either way. Booleans are read
loosely — `true`, `1`, and `yes` all mean on; anything else means off.

`relational_auto` is on by default: unlike transcript extraction it never
creates arbitrary memories — it only merges into the system-owned
`mem://user/profile/communication` node, and its provenance is unambiguous
(analysis of the session, not facts transiting it). Turning it off freezes the
relational profile; `serve` prints a warning at startup saying so.

### `[embedder]`

| Key | Type | Default | Accepted values |
|---|---|---|---|
| `backend` | string | `auto` | `auto`, `model2vec`, `ollama`, `hashtf` (`tfidf` is accepted as a spelling of `hashtf`), `none`. |

`auto` means identity-aware selection — see
[Embedder selection order](#embedder-selection-order) below. An unrecognized
value is not fatal: `serve` warns and falls back to `auto`.

`none` disables embedding entirely, which disables search and the
dedup-against-retracted gate. It exists for testing.

**Hand-editing this key does not migrate anything.** It changes which backend the
next `serve` selects; your stored vectors stay in the old space, and search fails
closed on the mismatch until you repair it. `continuity embedder use <backend>`
is the command that changes the key *and* re-embeds.

---

## Environment variables

| Variable | Overrides | Default when unset | Accepted values |
|---|---|---|---|
| `CONTINUITY_DB` | `[database].path` (for `serve`); the default path (for direct-DB commands) | `~/.continuity/continuity.db` | Any file path. |
| `CONTINUITY_BIND` | `[server].bind` | `127.0.0.1` | Any address. Read by both `serve` and the CLI/hooks. |
| `CONTINUITY_PORT` | `[server].port` | `37777` | Integer 0–65535. `serve` **refuses to start** on anything else. |
| `CONTINUITY_URL` | — | *(built from bind + port)* | Full base URL, e.g. `http://127.0.0.1:37777`. **Client-side only** — the CLI, hooks, and MCP server use it to find the server. It does not change what `serve` binds to. |
| `CONTINUITY_EMBEDDER` | `[embedder].backend` | *(no override)* | `auto`, `model2vec`, `ollama`, `tfidf`, `hashtf`, `none`, or empty. Unrecognized values warn and fall back to `auto`. |
| `CONTINUITY_EXTRACTION_AUTO` | `[extraction].auto` | `false` | Any Go boolean: `true`, `false`, `1`, `0`, `t`, `f`. `serve` **refuses to start** on anything else. |
| `CONTINUITY_RELATIONAL_AUTO` | `[extraction].relational_auto` | `true` | Any Go boolean. `serve` **refuses to start** on anything else. `false` freezes the relational profile. |
| `CONTINUITY_OBSERVATION_RETENTION_DAYS` | — | `14` days | A positive integer number of days, or `off` / `false` to disable pruning. See [below](#observation-retention). |
| `ANTHROPIC_API_KEY` | `[llm].provider` **and** `[llm].anthropic_key` | *(unset)* | An API key. Setting it forces `provider = "anthropic"`, overriding whatever `config.toml` says. |
| `CONTINUITY_GC` | — | `off` | `off`, `shadow`, `on`. Memory garbage collection. Anything unrecognized is treated as `off`. **Advanced.** |
| `CONTINUITY_NO_MIGRATION_SNAPSHOT` | — | *(unset)* | Any non-empty value skips the safety snapshot before a risky schema migration. **Advanced — this removes your rollback point.** |

`PATH` and `HOME` are read by `continuity install-service` to build the service
definition. They are not Continuity settings.

### Observation retention

`CONTINUITY_OBSERVATION_RETENTION_DAYS` controls how long raw tool-use
observations are kept before the daily sweep reclaims them. Memories, vectors,
and the relational profile are never affected.

| Value | Effect |
|---|---|
| unset | 14 days — the default. |
| a positive integer | That many days. |
| greater than `3650` | Clamped to 3650 days, with a log line. |
| `0`, or any negative number | Retention disabled; nothing is pruned. |
| `off` or `false` | Retention disabled; nothing is pruned. |
| anything unparseable | **Retention disabled**, with a log line. |

Unparseable input fails *closed* rather than falling back to the default: someone
typing `of` while reaching for `off` is trying to stop deletion, and the boot
sweep runs immediately, so a warning would arrive too late to intervene.

Note that a separate 30-day rule always applies: a session stuck in `active`
state — the client crashed, so `Stop` never fired — stops being treated as live
after 30 days, and its observations become reclaimable. This is not configurable.

Deleting rows does not shrink the database file. Run [`continuity prune`](cli.md#prune)
for that. See [Keeping it healthy](../guides/keeping-it-healthy.md).

---

## Precedence rules

### General

```
CONTINUITY_* environment variable   (wins)
  ↓
~/.continuity/config.toml
  ↓
built-in default
```

`ANTHROPIC_API_KEY` is applied *before* the `CONTINUITY_*` overrides, and it
rewrites both the provider and the key. There is no `CONTINUITY_*` variable for
the LLM provider, so setting `ANTHROPIC_API_KEY` is the only environment-level
way to change it.

### Embedder selection order

This is the one place with more than two tiers. `serve` picks its embedding
backend in this order, and stops at the first that applies:

1. **`CONTINUITY_EMBEDDER`**, if it names a concrete backend (not `auto` or
   empty).
2. **`[embedder].backend`** in `config.toml`, if it names a concrete backend.
3. **The corpus's declared identity** — whatever backend your existing memories
   were embedded with. This is what keeps an existing install on the backend it
   already has instead of silently moving it.
4. **model2vec**, for a fresh corpus with nothing declared yet. This is the
   default for new installs.

If step 4 fails because the model files cannot be downloaded (offline first run),
that session falls back to the lexical `hashtf` backend and says so. Run
`continuity embedder use model2vec` once you have a network to upgrade.

Steps 1 and 2 are *deliberate overrides* — they win even when they disagree with
what your memories were embedded with. That disagreement locks search, which
fails closed with an error rather than comparing incompatible vectors. Repair it
with `continuity embedder use <backend>` or
`continuity doctor --repair-vectors --apply`.

If step 3 fails to construct — Ollama is declared but not running, say — no
embedder is used at all. Continuity will not silently substitute a different one.

`continuity doctor` and `continuity embedder status` resolve the embedder by
exactly these rules, so they report what `serve` would actually do. **`continuity
dedup` does not** — it probes Ollama directly and otherwise uses the lexical
fallback, ignoring both the environment variable and the config key.

The identities you will see in `doctor` and `embedder status` output look like:

| Backend | Identity string |
|---|---|
| model2vec | `model2vec:potion-retrieval-32M:512` |
| Ollama | `ollama:nomic-embed-text:768` |
| lexical fallback | `hashtf:2048` |

### Client vs. server addressing

The CLI, the hooks, and the MCP server resolve the server URL like this:

```
CONTINUITY_URL                                     (wins outright)
  ↓
http://${CONTINUITY_BIND:-127.0.0.1}:${CONTINUITY_PORT:-37777}
```

`config.toml` plays no part in that. `serve` resolves its own listen address from
`config.toml` overlaid with `CONTINUITY_BIND` / `CONTINUITY_PORT`. Keeping the
two in agreement is on you.

---

## File format

The parser is deliberately small. It reads exactly the subset of TOML Continuity
writes:

- `# comment` lines and blank lines are ignored.
- `[section]` headers, one level deep only. Nested tables are not supported.
- `key = value` lines with string, boolean, or integer scalars. One layer of
  surrounding double quotes is stripped from strings. Arrays and multi-line
  strings are not supported.
- **Unknown sections and unknown keys are silently ignored.** A `config.toml`
  left over from an older version — one carrying a `[hooks]` section, or
  `[llm].merge_model` — still loads without error. Those keys simply do nothing.
- A malformed line is skipped, not treated as an error. A non-integer `port` is
  ignored and the default is kept.

Permissions: the file is written `0600` inside a `0700` directory when
`continuity embedder use` creates it.

### A complete example

Every key below is shown at its default value, so this file is equivalent to
having no `config.toml` at all.

```toml
[server]
bind = "127.0.0.1"
port = 37777

[database]
path = ""

[llm]
provider = "claude-cli"
model = "haiku"
ollama_url = "http://localhost:11434"
ollama_model = "llama3.2"
embedding_model = "nomic-embed-text"
anthropic_key = ""

[extraction]
auto = false
relational_auto = true

[embedder]
backend = "auto"
```

---

## Where hooks are configured

Not here. Claude Code reads its hooks from `~/.claude/settings.json`, and that is
the only place their commands and timeouts are set:

```json
{
  "hooks": {
    "SessionStart":     [{"hooks":[{"type":"command","command":"continuity hook start",  "timeout": 10}]}],
    "UserPromptSubmit": [{"hooks":[{"type":"command","command":"continuity hook submit", "timeout": 10}]}],
    "PostToolUse":      [{"hooks":[{"type":"command","command":"continuity hook tool",   "timeout": 10}]}],
    "Stop":             [{"hooks":[{"type":"command","command":"continuity hook stop",   "timeout": 120}]}],
    "SessionEnd":       [{"hooks":[{"type":"command","command":"continuity hook end",    "timeout": 10}]}]
  }
}
```

Installed as a Claude Code plugin, the equivalent definitions ship in the
plugin's own `hooks.json` and you do not edit `settings.json` yourself.

---

**See also:** [CLI reference](cli.md) · [Files and paths](files-and-paths.md) · [Configuration guide](../guides/configuration.md) · [Embedding backends](../guides/embedders.md) · [Keeping it healthy](../guides/keeping-it-healthy.md)
