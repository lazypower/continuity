[Docs](../README.md) › Guides › Using your memory

# Using your memory

How to search, read, and browse what Continuity has stored — from the terminal
and from the web interface.

**Audience:** operator · **Read time:** ~3 min

---

Your agent reads and writes memory on its own. These are the tools for when you
want to look yourself. All of them need the server running.

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

> **Opening a memory counts as using it**, which resets its relevance and keeps
> it from fading. That applies to `continuity show`, the MCP `show` tool, and
> expanding a card in the web interface. Being *listed* does not count — search
> results, `tree`, and session injection all leave relevance alone. See
> [Memory lifecycle](../advanced/memory-lifecycle.md).

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

Open <http://localhost:37777> in a browser (`open` on macOS, `xdg-open` on Linux).

Five tabs:

| Tab | What you can do |
|---|---|
| **Health** | Plain-language verdict on your memory: how much is fresh vs fading, what needs attention, what gets retrieved most |
| **Tree** | Browse and expand every memory; pin or unpin with one click |
| **Search** | Same search as the CLI, with a keyword-vs-meaning toggle |
| **Profile** | The relational profile, rendered |
| **Cold Boot** | Exactly what your agent wakes up with, verbatim, with a token count |

**Cold Boot is the one to look at first.** If your agent is not behaving the way
you expect, this is where you find out what it was actually told. Opening the tab
is free in both senses: it costs no model tokens, and it does not mark anything
as used or advance the rotation of which moments appear next.

The interface is read-mostly. You can pin and unpin; you cannot create, edit, or
retract a memory from it.

## Removing something

If Continuity captured something wrong, stale, or private:

```bash
continuity retract mem://user/entities/old-server --reason "decommissioned"
```

**This cannot be undone**, and it does not erase everything. The body and the
full detail are blanked. **The 200-character summary is kept** as the receipt's
label, along with the address, category, your reason, and a fingerprint used to
notice if the same content is written again. Retracted memories are excluded from
normal reads and never injected into a session, even if they were pinned.

That matters if you are retracting something sensitive: if the thing you want
gone is in the summary, retraction will not remove it.

Which raises the harder question — **you did not write these memories, so how do
you know what is in them?** The answer is to read them in bulk. `continuity tree`
prints every memory's summary, which is the same text that gets injected into
your sessions:

```bash
continuity tree mem://user            # everything stored about you
continuity tree mem://agent           # what the agent worked out for itself
```

The web interface **Tree** tab shows the same thing, and **Cold Boot** shows the
subset actually being injected right now.

Nothing notifies you at the moment a memory is written. Reviewing is something
you have to go and do — worth doing once after your first week, and any time you
have been working near credentials or customer data.

A **safety snapshot from a past upgrade may also still contain the original
text**. Check `continuity snapshot list` and prune what you no longer need.

If something replaced it, link them so the history stays intact:

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
