[Docs](../README.md) › Getting started › Your first session

# Your first session

Store something, prove it persisted, and see exactly what your agent wakes up
with.

**Audience:** operator · **Read time:** ~2 min

---

## Store something

In a Claude Code session, say something worth keeping, with a trigger phrase:

> Remember this: always use devbox for tooling in this project.

Phrases like **"remember this"**, **"always use"**, **"never use"**, **"the fix
was"** and **"root cause was"** save immediately, without waiting for the session
to end. The phrase needs to be near the start of a normal-length message — a
trigger buried in a long paste is ignored on purpose.

## Prove it persisted

In a terminal:

```bash
continuity search "devbox"
```

```
1. [0.412] mem://user/preferences/devbox-tooling
   Always use devbox for tooling in this project.
```

If it is there, capture is working end to end.

## See what your agent wakes up with

```bash
open http://localhost:37777
```

Go to the **Cold Boot** tab. This is the verbatim text added to your agent's
instructions at the start of a session, with an approximate token count. Nothing
else in Continuity answers "what does my agent actually know right now" as
directly.

You will see a few sections:

- **Working With You** — how you give feedback and how much autonomy you extend.
  Empty at first; it builds over several sessions.
- **Pinned** — memories you have forced into every session
- **Moments** — a rotating couple of relational memories
- **Recent Sessions** — where you have been working lately

Looking at this tab does not consume anything. Open it as often as you like.

## Start a new session

Close the session and start a fresh one. Ask your agent what it knows about your
tooling preferences. It should answer from memory, without you re-explaining.

That is the whole point of the product. Everything else is maintenance.

## Pin the things that must never be missed

Most memories surface when they are relevant. For the handful that must appear
every single time:

```bash
continuity pin mem://user/preferences/devbox-tooling
```

There is a cap of seven, deliberately — the list has to stay short enough to be
read. Check the Cold Boot tab afterwards to see it in place.

## What now

Memory compounds. The profile of how you work needs several real sessions before
it says anything useful, so the honest advice is to use Claude Code normally for
a week and then look again.

When you want to understand what is being stored and why, read
[What gets remembered](../guides/what-gets-remembered.md).

---

**Next:** [What gets remembered](../guides/what-gets-remembered.md)
**See also:** [Using your memory](../guides/using-memory.md) · [Troubleshooting](../guides/troubleshooting.md)
