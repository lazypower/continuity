[Docs](../README.md) › Guides › Using your memory

# Using your memory

How to search, read, and browse what Continuity has stored — from the terminal
and from the web interface.

**Audience:** operator · **Read time:** ~3 min

---

Most of the time you never do this. Your agent reads and writes memory on its
own. These are the tools for when you want to look yourself — to check what it
knows, find something you told it months ago, or confirm it captured what you
meant.

All of these need the server running.

## Search

```bash
continuity search "sqlite configuration"
```

```
1. [0.286] mem://agent/patterns/observe-then-architect-then-implement
   Working method (validated 3+ times): observe → architect → implement...
2. [0.275] mem://agent/patterns/retention-keys-on-readership
   Retention predicates must key on READERSHIP (who can still read this row?)...
```

The number is the match score. Results show only the short summary and the
address — **never the full memory**. That is deliberate: search is for finding
things, and returning full bodies would flood your agent's context with material
it did not ask for.

Useful flags:

```bash
continuity search "auth" --limit 20
continuity search "auth" --category preferences
continuity search "auth" --explain          # show how the score was computed
```

## Read one memory

```bash
continuity show mem://user/preferences/devbox-tooling
```

You can drop the `mem://` prefix. To see only one level of detail, use `--layer
summary`, `--layer body`, or `--layer detail`.

> **Reading a memory counts as using it.** It resets that memory's relevance,
> which keeps it from fading. Search results do not — being listed is not the
> same as being read. See [Memory lifecycle](../advanced/memory-lifecycle.md).

## Browse the tree

```bash
continuity tree                          # top level
continuity tree mem://user/preferences   # inside a category
```

## See how you work

```bash
continuity profile
```

This prints the relational profile — what Continuity has worked out about how you
give feedback, how much autonomy you extend, and what corrections you have made.
It is the thing that makes a returning agent feel like it knows you. Add
`--verbose` to also list every stored preference.

## The web interface

```bash
open http://localhost:37777
```

Five tabs:

| Tab | What you can do |
|---|---|
| **Health** | Plain-language verdict on your memory: how much is fresh vs fading, what needs attention, what gets retrieved most |
| **Tree** | Browse and expand every memory; pin or unpin with one click |
| **Search** | Same search as the CLI, with a keyword-vs-meaning toggle |
| **Profile** | The relational profile, rendered |
| **Cold Boot** | Exactly what your agent wakes up with, verbatim, with a token count |

**Cold Boot is the one to look at first.** Everything else describes your memory;
that tab shows you the actual text injected into your agent at the start of a
session. If your agent is not behaving the way you expect, this is where you find
out what it was actually told. Viewing it does not consume anything — you can
open it as often as you like.

The interface is read-mostly. You can pin and unpin; you cannot create, edit, or
retract a memory from it.

## Removing something

If Continuity captured something wrong, stale, or private:

```bash
continuity retract mem://user/entities/old-server --reason "decommissioned"
```

The content is erased but the record stays, marked retracted and excluded from
normal reads. If something replaced it, link them so the history stays intact:

```bash
continuity retract mem://user/events/old-plan \
  --reason "superseded by the revised rollout" \
  --superseded-by mem://user/events/new-plan
```

A reason is required. This is a deliberate design choice — memory is meant to be
accountable rather than silently editable, so removals leave a trace of why.

---

**Next:** [Configuration](configuration.md)
**See also:** [What gets remembered](what-gets-remembered.md) · [CLI reference](../reference/cli.md) · [MCP tools](../reference/mcp-tools.md)
