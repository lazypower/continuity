[Docs](../README.md) › Guides › What gets remembered

# What gets remembered

The kinds of things Continuity stores, how much of each it keeps, and what it
deliberately does not capture.

**Audience:** operator · **Read time:** ~3 min

---

## What does *not* get captured

Continuity does **not** read your session transcript and decide on its own what
was important. That behavior exists, but it is **off by default** and has to be
turned on deliberately. See [Extraction](../advanced/extraction.md) for why.

By default, memories are created two ways:

1. **Your agent writes one on purpose**, using the `remember` tool.
2. **You say a trigger phrase** — "remember this", "always use", "never use",
   "the fix was", "root cause was", and a few others. That fires an immediate
   capture. The phrase has to appear near the start of a normal-length message,
   so a stray "remember this" buried in a giant paste will not trigger it.

Tool activity is recorded separately as **observations** — temporary records
used to assemble the current session's context, not memories. Each one holds
**only the tool's name and the time it ran.** The arguments it was given and the
output it returned are not stored.

That is deliberate. Those fields used to be written and were read by nothing,
which meant file contents and command output landed on disk for no reason. If
you are on a database written by a version before v0.11.0, older records may
still hold that material; it ages out under the same rule below, and
`continuity prune` clears it immediately.

Records are deleted automatically once the session that produced them has
finished and the record is at least **14 days** old. Records from sessions still
running are never touched. See [Keeping it
healthy](keeping-it-healthy.md#observation-retention) to change or disable that.

Your session transcript itself is not copied into the database. Continuity reads
the transcript file Claude Code already writes, and only when you explicitly ask
it to.

## The categories

Every memory lands in one of nine categories. Which one it is determines whether
it can be updated later and whether it fades over time.

| Category | Holds | Updated in place? | Fades? |
|---|---|---|---|
| `profile` | Who you are, how you work | yes | never |
| `preferences` | Tools, conventions, "always use X" | yes | never |
| `feedback` | Guidance you gave about how to approach work | yes | never |
| `patterns` | Reusable techniques the agent worked out | yes | yes |
| `entities` | People, services, systems | no | yes |
| `events` | Decisions, deployments, things that happened | no | yes |
| `cases` | A specific problem and how it was solved | no | yes |
| `reference` | Pointers to dashboards, tickets, docs | no | yes |
| `moments` | What working together was like | no | never |

**"Updated in place"** means a new memory on the same subject merges into the
existing one instead of creating a second copy. This is what stops your
preferences turning into forty near-duplicate entries.

**"Fades"** refers to relevance decay — a memory that is never used again slowly
drops down the search rankings. The first three categories and `moments` are
exempt: things you explicitly told the agent about how to work with you should
not quietly expire. Nothing is ever deleted by fading. See [Memory
lifecycle](../advanced/memory-lifecycle.md).

## Three sizes of every memory

Each memory is stored at three levels of detail, so the agent can get the gist
of a lot of things cheaply and pull the full text only when it needs it.

| Level | Limit | Used for |
|---|---|---|
| Summary | 200 characters | What gets injected when a memory is surfaced, and the only thing search results show |
| Body | 2,000 characters | Shown when a memory is opened |
| Detail | 40,000 characters | Full content, fetched on demand only |

These are hard limits, and **over-limit content is trimmed at a word boundary
rather than rejected**. If you write a 400-character summary you will silently
get the first 200 characters or so. Write short summaries on purpose.

The body has a minimum too — under 20 characters is rejected outright.

## How memories are addressed

Every memory has an address that looks like a file path:

```
mem://user/preferences/devbox-tooling
mem://agent/patterns/sqlite-wal-mode
```

`user` holds things about you. `agent` holds things the agent worked out for
itself — that is only `patterns` and `cases`. You can browse the whole thing:

```bash
continuity tree
continuity tree mem://user/preferences
```

## Pins and moments

**Pins** force a memory into every single session, regardless of relevance. Use
them for the handful of things that must never be missed. There is a **cap of
seven** — the point is that it stays small enough to be read.

```bash
continuity pin mem://user/preferences/devbox-tooling
continuity pin                      # list what is pinned
continuity unpin mem://user/preferences/devbox-tooling
```

**Moments** are a small pool of relational memories — what a stretch of work felt
like, not what was decided. A few are rotated into each session. They exist so a
returning agent has some sense of history, not just facts. The pool is capped at
ten and never fades.

---

**Next:** [Using your memory](using-memory.md)
**See also:** [Keeping it healthy](keeping-it-healthy.md) · [Memory lifecycle](../advanced/memory-lifecycle.md) · [CLI reference](../reference/cli.md)
