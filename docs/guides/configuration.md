[Docs](../README.md) › Guides › Configuration

# Configuration

Where settings live, which ones matter, and the one rule that explains why a
setting might not be taking effect.

**Audience:** operator · **Read time:** ~2 min

---

Continuity works with no configuration at all.

## Two places settings live

**`~/.continuity/config.toml`** — persistent settings. The file does not exist
until something writes it; create it yourself if you need it.

```toml
[server]
port = 37777

[llm]
provider = "claude-cli"
model = "haiku"

[embedder]
backend = "auto"

[extraction]
auto = false
```

**Environment variables** — the same knobs, for one-off overrides and for running
under a service manager.

```bash
CONTINUITY_PORT=37778
CONTINUITY_DB=~/continuity-scratch.db
CONTINUITY_OBSERVATION_RETENTION_DAYS=off
```

**Environment variables always win** over the config file.

A bare assignment only affects the shell you typed it in. For a background
service, put it in the service definition — the `EnvironmentVariables` block of
`~/Library/LaunchAgents/com.continuity.server.plist` on macOS, or an
`Environment=` line in `~/.config/systemd/user/continuity.service` on Linux —
then restart.

## The rule that trips people up

**Only `serve`, `doctor`, and `embedder` read `config.toml`.**

Every other command — `search`, `show`, `tree`, `remember`, `prune` — reads
environment variables only. So if you set a custom database path in
`config.toml`, the server will use it and `continuity tree` will not.

If you run a non-default setup, set the environment variable rather than the
config file, and set it somewhere every shell sees it.

> Settings in `config.toml` only take effect when the server next starts. Run
> `continuity restart` after editing it.

## Settings worth knowing

| Setting | Default | Why you would change it |
|---|---|---|
| `[server].port` / `CONTINUITY_PORT` | `37777` | Port already in use |
| `[llm].provider` | `claude-cli` | Use the Anthropic API or a local Ollama model instead |
| `[embedder].backend` | `auto` | Change how search works — **but see the warning below** |
| `[extraction].auto` | `false` | Turn on automatic memory creation at session end — see the warning below |
| `CONTINUITY_OBSERVATION_RETENTION_DAYS` | `14` | Keep raw tool-call records longer, or forever |

The full list is in the [configuration reference](../reference/configuration.md).

> **Turning on `[extraction].auto` has costs worth knowing.** Every qualifying
> session gets sent to your configured model to decide what is worth
> remembering. That means session-derived text goes to whichever provider you
> use — and if that is the Anthropic API rather than your Claude subscription,
> it is billed per token. It is off by default because its usefulness is
> unmeasured and its writes are indistinguishable from ones you authored. See
> [Extraction](../advanced/extraction.md).

> **Do not change `[embedder].backend` by hand.** It changes which search engine
> Continuity uses but leaves your existing memories built with the old one, which
> disables search until you fix it. Use `continuity embedder use <backend>`, which
> changes the setting *and* rebuilds your memories. See [Embedding
> backends](embedders.md).

## Choosing an LLM provider

Continuity uses an LLM for a few things — writing memory summaries, profiling how
you work, and assisted search. It is **not** used for ordinary search.

| Provider | Cost | Notes |
|---|---|---|
| `claude-cli` (default) | Free with a Claude Max subscription | Shells out to the `claude` command |
| `ollama` | Free, fully local | Needs Ollama running |
| `anthropic` | Billed per token, separately from Max | Needs an API key; for headless setups |

Setting `ANTHROPIC_API_KEY` **forces** the `anthropic` provider, overriding
whatever your config file says. If you have that key exported for other tools and
did not intend to use it here, that is worth knowing — check `continuity doctor`.

Continuity runs fine with no LLM at all. Capture, search, browsing and pins all
keep working; only the LLM-written summaries and profiling stop.

## A note on the file format

The config parser handles a deliberately small subset of TOML: `[section]`
headers and simple `key = value` lines. Arrays, nested tables and multi-line
strings are not supported, and **unknown keys are ignored silently** — so a typo
in a key name fails quietly. If a setting is not taking effect, check the
spelling against the [reference](../reference/configuration.md) first.

---

**Next:** [Embedding backends](embedders.md)
**See also:** [Configuration reference](../reference/configuration.md) · [Keeping it healthy](keeping-it-healthy.md)
