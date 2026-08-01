# Continuity documentation

Continuity gives Claude Code a memory that survives between sessions, so you stop
re-explaining your preferences, your project, and the bugs you already solved.

It runs as a background server on your machine. It keeps running after you close
your terminal, and it starts on login if you install it as a service. Claude Code
talks to it automatically at the start and end of each session, and it adds what
it has learned to Claude's instructions when a new session begins.

## What it does with your data

Worth knowing before you install it.

**Everything is stored in one file on your computer**, at
`~/.continuity/continuity.db`. The server only accepts connections from your own
machine and refuses anything else.

**Two kinds of things are stored:**

- **Memories** — short written notes: your preferences, decisions, techniques,
  and a profile of how you like to work. These are what get added to Claude's
  instructions.
- **Tool-call records** — the *name* of each tool your agent runs, and when.
  Arguments and output are **not** stored: nothing reads them, so keeping them
  would mean writing file contents and command output to disk for no reason.
  These records exist to count activity in the current session, and are
  [deleted automatically](guides/keeping-it-healthy.md#observation-retention)
  once their session finishes and they are 14 days old.

**What leaves your machine.** Searching, browsing, and storing memories are
entirely local. Continuity does use an AI model to write memory summaries and to
build your profile — by default that is the `claude` command you already have,
which sends that content to Anthropic the same way Claude Code does. You can
point it at a local model instead, or run without one; capture, search and
browsing all keep working. See [Configuration](guides/configuration.md).

**Continuity does not read your whole session and decide what to keep.** That
behavior exists but is **off by default**. Memories are created when your agent
deliberately writes one, or when you use a phrase like "remember this".

---

## Three ways to read this

### 1. Set it up — ~8 minutes

| | Page | Time |
|---|---|---|
| 1 | [Install](getting-started/install.md) | 2 min |
| 2 | [Connect it to Claude Code](getting-started/claude-code-setup.md) | 4 min |
| 3 | [Your first session](getting-started/first-session.md) | 2 min |

### 2. Run it day to day — ~15 minutes

| | Page | Time |
|---|---|---|
| 4 | [What gets remembered](guides/what-gets-remembered.md) | 3 min |
| 5 | [Using your memory](guides/using-memory.md) | 3 min |
| 6 | [Configuration](guides/configuration.md) | 2 min |
| 7 | [Embedding backends](guides/embedders.md) | 3 min |
| 8 | [Keeping it healthy](guides/keeping-it-healthy.md) | 4 min |

Read these when you need them, not in order:

- [Troubleshooting](guides/troubleshooting.md) — symptom, cause, fix
- [Upgrading](upgrading.md) — **read before every upgrade**; some delete data

### 3. Understand it deeply — engineers

- [Architecture](advanced/architecture.md) — how the pieces fit together
- [Memory lifecycle](advanced/memory-lifecycle.md) — how memories age, merge, and get removed
- [Vector identity](advanced/vector-identity.md) — why search refuses to run after a search-method change
- [Extraction](advanced/extraction.md) — turning sessions into memories, and why it is off by default
- [Hook internals](advanced/hooks-internals.md) — what runs at each point in a session

---

## Reference

Look things up here.

- [CLI](reference/cli.md) — every command and option
- [Configuration keys](reference/configuration.md) — every setting and environment variable
- [HTTP API](reference/http-api.md) — the URLs other programs can call
- [MCP tools](reference/mcp-tools.md) — the memory tools available to Claude Code
- [Files and paths](reference/files-and-paths.md) — everything Continuity puts on disk

---

## Three things that surprise people

1. **Memories are not captured automatically from your transcript.** See
   [What gets remembered](guides/what-gets-remembered.md).
2. **Old tool-call records are deleted on a schedule.** Memories are not affected.
   See [Keeping it healthy](guides/keeping-it-healthy.md#observation-retention).
3. **If you change how search works, search stops until you rebuild it.**
   Continuity returns an error rather than unreliable results. Nothing is deleted.
   See [Embedding backends](guides/embedders.md).

## Stopping it

```bash
launchctl unload ~/Library/LaunchAgents/com.continuity.server.plist   # macOS service
systemctl --user stop continuity                                      # Linux service
pkill -f 'continuity serve'                                           # started by hand
```
