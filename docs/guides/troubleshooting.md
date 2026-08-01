[Docs](../README.md) › Guides › Troubleshooting

# Troubleshooting

Symptoms you are likely to hit, what causes each one, and what to type.

**Audience:** operator · **Read time:** ~4 min

---

## "continuity server is not running" — but it is

```
continuity server is not running (start it with: continuity serve)
```

**If the server really is stopped**, start it, or install it as a service so it
starts on login:

```bash
continuity serve
continuity install-service      # starts on login, restarts on crash
```

**If the server is running**, the usual cause is a database that has grown large
enough that requests time out before they finish. Continuity distinguishes the
two cases, and a slow server says so:

```
continuity server at http://127.0.0.1:37777 is running but did not respond
within 30s — this usually means the database has grown large; try: continuity prune
```

Check the size, then reclaim it:

```bash
curl -s localhost:37777/api/health      # look at db_bytes
continuity prune
```

See [Keeping it healthy](keeping-it-healthy.md#reclaiming-disk-space).

## "server is running stale code"

```
⚠ continuity server is running dev-2b7c9a2 but this CLI is dev-bbe1a2b —
  schema/API mismatch; run `continuity restart` to pick up the new binary
```

You installed a new version but the background server is still running the old
one. Installing does not replace a running server.

```bash
continuity restart
```

## Search returns an error, or the web interface shows a red banner

```
vector identity mismatch: the corpus was embedded with ollama:nomic-embed-text:768
but the active embedder is model2vec:potion-retrieval-32M:512. Search is disabled
to avoid comparing across vector spaces.
```

Your memories were built for one search method and Continuity is now set to use a
different one. Results would be meaningless, so search turns itself off instead.

**Nothing is lost.** Memories are still captured; they just have no search entry
until you fix this.

```bash
continuity embedder status            # confirm the mismatch
continuity embedder use <backend>     # rebuild everything for the new method
continuity restart
```

Pick the backend you actually want — see [Embedding backends](embedders.md). If
you did not mean to change anything, set it back to what `corpus declared
identity` reports.

## My agent does not seem to have any memory

Work through these in order.

**1. Is the server running?**

```bash
curl -s localhost:37777/api/health
```

**2. Are the hooks configured?** Continuity only sees your session if Claude Code
is calling it. Check `~/.claude/settings.json` contains the five hook entries from
[Connect it to Claude Code](../getting-started/claude-code-setup.md).

**3. Is there anything to inject yet?** A brand new install has nothing stored.

```bash
continuity tree
```

**4. Look at what is actually being sent.** Open <http://localhost:37777> and go
to the **Cold Boot** tab. It shows the exact text your agent receives at session
start. If that looks right and your agent still ignores it, the problem is
downstream of Continuity.

If a session was not captured, Continuity says so once per session on stderr:

```
continuity: server unreachable — this session is not being captured
```

## "remember this" did not save anything

Trigger phrases only fire when they appear near the beginning of a reasonably
short message. A trigger buried inside a long paste is ignored on purpose — it is
usually quoting something, not asking you to save it. You will see:

```
continuity: memory cue found but the message is too long to fire as an
immediate signal — it will still be considered at session end
```

To store something directly, regardless of length, ask your agent to use its
`remember` tool, or run:

```bash
continuity remember -c preferences -n my-slug \
  -s "One-line summary" -b "The fuller explanation."
```

## Memories are not being created from my sessions

This is expected. Automatic end-of-session memory creation is **off by default** —
see [Extraction](../advanced/extraction.md) for the reasoning.

To extract one session by hand:

```bash
continuity extract <session-id> --force
```

To turn the automatic behavior on, add to `~/.continuity/config.toml`:

```toml
[extraction]
auto = true
```

Then `continuity restart`.

## Extraction stopped working after I installed it as a service

The service records where your `claude` and `ollama` programs were **at the time
you installed it**. Background services do not inherit your shell's `PATH`, so if
you moved or reinstalled those programs, the service can no longer find them.

```bash
continuity install-service      # re-detects and rewrites the service
```

## Port 37777 is already in use

```bash
CONTINUITY_PORT=37778 continuity serve
```

To make it permanent, add it to `~/.continuity/config.toml`:

```toml
[server]
port = 37778
```

Every command talks to `127.0.0.1:37777` unless told otherwise, so set
`CONTINUITY_PORT` in your shell profile too — the config file is only read by the
server. See [Configuration](configuration.md).

## A setting in config.toml is being ignored

Three common causes:

1. **Only the server reads it.** `continuity search`, `tree`, `show` and most
   other commands read environment variables only.
2. **You did not restart.** Run `continuity restart`.
3. **A typo.** Unknown keys are ignored silently. Check the spelling against the
   [configuration reference](../reference/configuration.md).

## Where are the logs?

```bash
tail -f ~/.continuity/serve.log
```

---

**See also:** [Keeping it healthy](keeping-it-healthy.md) · [Upgrading](../upgrading.md) · [Configuration](configuration.md)
