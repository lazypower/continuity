[Docs](../README.md) › Reference › MCP tools

# MCP tools

The six memory tools Continuity exposes over the Model Context Protocol, every argument they take, and what an error looks like.

**Audience:** operator · **Read time:** ~7 min

## What the MCP server is

`continuity mcp` starts a Model Context Protocol server on stdio. It is a thin client of the running daemon: every tool is a call to an `/api/*` endpoint, exactly as the CLI does. It holds no database handle and no LLM of its own.

**The daemon must be running.** `continuity mcp` with no `continuity serve` behind it will start and answer `tools/list`, but every tool call fails with the "server is not running" guidance.

The reason to prefer MCP over the CLI is quoting. MCP tool arguments are structured JSON, so a multi-line memory body never passes through a shell — no escaping, no heredocs.

---

## Registering it with Claude Code

Add it to `.mcp.json` in your project root:

```json
{
  "mcpServers": {
    "continuity": { "command": "continuity", "args": ["mcp"] }
  }
}
```

Or from the command line, with `-s user` if you want it available in every project:

```bash
claude mcp add continuity -- continuity mcp
```

Once registered, the tools appear to the agent as `mcp__continuity__remember`, `mcp__continuity__search`, and so on. Confirm with `/mcp` inside Claude Code, or directly:

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | continuity mcp
```

`continuity mcp` speaks JSON-RPC 2.0 as newline-delimited messages on stdin/stdout. It implements `initialize`, `ping`, `tools/list`, and `tools/call`; messages without an `id` are notifications and get no reply. It advertises protocol revision `2025-06-18` but echoes the client's requested version when one is offered. A single message may be up to 16 MB, which is what lets a 40,000-character `detail` through. **Advanced**

---

## The six tools

Required arguments are marked in each table. Every tool returns **human-readable text**, not JSON — these are written for an agent to read, not for a script to parse. Script against the [HTTP API](http-api.md) instead.

### remember

Stores a memory directly. No LLM involved.

| Argument | Type | Required | Default | Notes |
|---|---|---|---|---|
| `category` | string (enum) | **yes** | — | One of `profile`, `preferences`, `feedback`, `entities`, `events`, `patterns`, `cases`, `moments`, `reference`. Rejected locally before any network call. |
| `name` | string | **yes** | — | Short kebab-case URI slug, e.g. `devbox-tooling`. Sanitized server-side to `[a-z0-9_-]`. |
| `summary` | string | **yes** | — | L0 abstract. One sentence, **max 200 characters**. |
| `body` | string | **yes** | — | L1 overview. **Max 2000 characters, minimum 20.** |
| `detail` | string | no | omitted | L2 full content. **Max 40000 characters.** |
| `session_id` | string | no | omitted | Provenance attribution. |
| `acknowledge_retracted` | boolean | no | `false` | Proceed past a match against a retracted memory. |

**The character limits truncate, they do not reject.** Content over the limit is cut at a word boundary and stored; the call still succeeds and the server logs the truncation. The one exception is `body` under 20 characters, which *is* rejected as a validation error. So a 5000-character `body` silently becomes a 2000-character body — compress before you write, do not rely on the ceiling.

Success returns `created: mem://user/preferences/devbox-tooling [preferences]`, or `updated: …` when an existing memory in a mergeable category was updated in place.

### search

| Argument | Type | Required | Default | Notes |
|---|---|---|---|---|
| `query` | string | **yes** | — | Whitespace-only is rejected locally. |
| `limit` | integer | no | `10` | Zero or negative becomes `10`. The server caps at `100`. |
| `category` | string | no | all | Restrict to one category. |
| `smart` | boolean | no | `false` | `true` selects LLM-assisted search mode. Needs a configured LLM. |

Returns **pointers, not bodies** — a numbered list of `[score] uri` with the L0 summary and category. Deepen with `show`. That split is deliberate: search costs nothing, reading a body is a countable act.

`No results found.` when there are no hits.

### show

| Argument | Type | Required | Default | Notes |
|---|---|---|---|---|
| `uri` | string | **yes** | — | The `mem://` prefix is optional — it is added automatically. |
| `include_retracted` | boolean | no | `false` | Reveal a retracted memory's reason and original content. |

Returns the URI and category, then `## Summary`, `## Body`, and `## Detail` sections. An empty body renders as `(empty)`; an empty detail section is omitted entirely.

A retracted memory without `include_retracted` returns metadata only, ending with `(reason and original content hidden — pass include_retracted to reveal)`. With the flag, a `## Retraction` section carrying the reason is prepended.

Reading a memory this way **counts as a use** and refreshes its relevance. Retracted memories are exempt.

### tree

| Argument | Type | Required | Default | Notes |
|---|---|---|---|---|
| `uri` | string | no | roots | Directory to list. Omit to list roots. `mem://` prefix optional. |
| `include_retracted` | boolean | no | `false` | Include retracted leaves. |

Directories render as `uri/ (n)` with a child count. Leaves render as the URI plus `[pinned]` / `[retracted]` flags and an indented L0 line. `(empty)` when the directory has no children.

### profile

No arguments. Returns the synthesized relational profile under `## Relational Profile`, then every profile and preference node under `## Profile & Preference Nodes`. `No relational profile yet.` when none has been synthesized.

### retract

| Argument | Type | Required | Default | Notes |
|---|---|---|---|---|
| `uri` | string | **yes** | — | **Must** start with `mem://` — unlike `show`, no prefix is added for you. |
| `reason` | string | **yes** | — | One sentence. Whitespace-only is rejected. |
| `superseded_by` | string | no | omitted | URI of the replacement. Must also start with `mem://`. Turns the retraction into a supersession. |

Returns `retracted: <uri>`, `retracted: <uri> → <superseded_by>`, or `already_retracted: <uri>` — retraction is idempotent.

Retraction is the agent's own curation verb. The memory stays in the tree as a marker but is excluded from search, tree listings, and context injection.

---

## Error shapes

There are two distinct failure modes, and they look different on the wire.

**Tool failures** — a bad argument, a rejected write, a daemon that is down — come back as a *successful* JSON-RPC result carrying `isError: true`, so the agent reads the message and can act on it:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "content": [{ "type": "text", "text": "category, name, summary, and body are required" }],
    "isError": true
  }
}
```

**Protocol failures** — a malformed message or a tool that does not exist — come back as JSON-RPC errors:

| Code | Meaning | Trigger |
|---|---|---|
| `-32700` | Parse error | The line was not valid JSON. |
| `-32601` | Method not found | A method other than `initialize`, `ping`, `tools/list`, `tools/call`. |
| `-32602` | Invalid params | Malformed `tools/call` params, or `unknown tool: <name>`. |

Messages you will actually see in the `isError` text:

| Text | Cause |
|---|---|
| `category, name, summary, and body are required` | A required `remember` argument was empty. Caught locally, before any network call. |
| `invalid category "..." (valid: profile, preferences, …)` | Category not in the taxonomy. Caught locally. |
| `query is required` / `reason is required` / `uri is required` | Empty required argument. Caught locally. |
| `invalid uri "...": must start with mem://` | `retract` given a bare slug. |
| `write blocked — candidate matches retracted memory:` followed by URIs and a hint | The `remember` dedup gate fired. Inspect each URI with `show` and `include_retracted`, then re-send with `acknowledge_retracted: true` if the new memory is genuinely different. |
| `uri ... is retracted; choose a different slug` | Direct slug collision with a tombstone. |
| `memory not found` | `show` on a URI that does not exist. |
| `search not available — no embedder configured` | No embedder. |
| A vector-identity mismatch message naming `continuity doctor` | Search is failing closed because the active embedder does not match the corpus. |
| `engine not configured` | `remember` or `retract` on an install with no LLM/embedder engine. |
| Guidance about starting the server | The daemon is not running, or is not reachable at the resolved URL. |

MCP tool calls use the 5-second hook timeout, not the 30-second interactive CLI timeout. A `smart` search against a slow LLM can hit it; the same query via `continuity search --smart` has six times the patience. **Advanced**

---

## MCP tool to CLI equivalent

| MCP tool | CLI equivalent |
|---|---|
| `remember` | `continuity remember -c <category> -n <name> -s <summary> -b <body> [-d <detail>] [--session <id>] [--acknowledge-retracted]` |
| `search` | `continuity search <query> [-n <limit>] [-c <category>] [--smart] [--explain]` |
| `show` | `continuity show <uri> [--include-retracted] [--layer summary\|body\|detail\|all] [--json]` |
| `tree` | `continuity tree [uri] [--include-retracted]` |
| `profile` | `continuity profile [--verbose]` |
| `retract` | `continuity retract <uri> --reason "<why>" [--superseded-by <uri>]` |

Two differences worth knowing:

- **`tree` and `profile` diverge.** The MCP tools call the daemon over HTTP and therefore need it running. The CLI commands of the same name open the database file directly and work with the daemon stopped. Every other pair goes through the API on both sides.
- **The CLI has options MCP does not:** `show --layer` and `--json`, and `search --explain` (per-result score decomposition). There is no MCP equivalent for any of them.

There is no MCP tool for pinning (`continuity pin` / `unpin`), pruning (`continuity prune`), session history (`continuity timeline`), extraction (`continuity extract`), or diagnostics (`continuity doctor`). Those are operator verbs and stay on the CLI.

---

**See also:** [HTTP API](http-api.md) · [CLI](cli.md) · [Connect it to Claude Code](../getting-started/claude-code-setup.md) · [Using your memory](../guides/using-memory.md)
