<p align="center">
  <img src=".github/continuity.jpg" alt="Continuity" width="720" />
</p>

<h1 align="center">Continuity</h1>

<p align="center">
  <strong>Persistent memory for AI coding agents.</strong><br/>
  Single binary. Zero dependencies. Your agent never starts cold again.
</p>

<p align="center">
  <a href="#install">Install</a> &bull;
  <a href="docs/README.md">Documentation</a> &bull;
  <a href="docs/getting-started/install.md">Getting started</a> &bull;
  <a href="RFC.md">Design</a>
</p>

---

Every time you start a Claude Code session, it forgets who you are. Your
preferences, your project context, the patterns you've established, the bugs
you've already solved — all gone. You re-explain yourself. Again.

Continuity fixes this. It captures what happened, what was learned, and how you
work, then injects that context into future sessions automatically.

## What it remembers

- **How you work** — feedback style, autonomy level, corrections given. A
  compounding relational profile that means your agent stops making the same
  mistakes.
- **What you prefer** — tools, workflows, conventions. "Always use devbox."
  Learned once, applied forever.
- **What happened** — decisions, deployments, architecture choices. Project
  history that doesn't vanish when a session ends.
- **How to solve things** — patterns, techniques, bug→fix pairs.
- **What it was like** — relational moments that capture the texture of working
  together.

## Install

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/lazypower/continuity/main/install.sh | sh

# Homebrew
brew install lazypower/tap/continuity

# Arch Linux (AUR) — community-maintained by @klrmngr
yay -S continuity-bin
```

One binary. 17MB. No Docker, no Node, no Python.

Then wire it into Claude Code — hooks, MCP tools, and directives — in about four
minutes: **[Getting started →](docs/getting-started/install.md)**

## Documentation

**[Full documentation →](docs/README.md)**

| | |
|---|---|
| [Getting started](docs/getting-started/install.md) | Install, connect to Claude Code, first session |
| [Operators guide](docs/README.md#2-run-it-day-to-day--15-minutes) | What gets remembered, configuration, search backends, maintenance |
| [Upgrading](docs/upgrading.md) | **Read before upgrading** — some releases delete data |
| [Troubleshooting](docs/guides/troubleshooting.md) | Symptom → cause → fix |
| [Reference](docs/README.md#reference) | Every command, setting, endpoint, and file |
| [Advanced](docs/README.md#3-understand-it-deeply--engineers) | Architecture and internals |

## How it works

Continuity runs as a background server on your machine. Claude Code calls it
through hooks at the start and end of each session and after each tool call. It
stores everything in one SQLite file at `~/.continuity/continuity.db`, and adds
what it has learned to your agent's instructions when a new session begins.

Memories are tiered — a ~200 character summary, a ~2,000 character body, and full
detail on demand — so an agent gets the shape of what it knows without paying for
the weight of it. Search is semantic and runs locally.

Nothing is captured automatically from your transcript by default; memories come
from deliberate writes and from phrases like "remember this". See
[what gets remembered](docs/guides/what-gets-remembered.md) for the full picture,
including what leaves your machine.

## Project structure

```
continuity-go/
├── cmd/continuity/            CLI entry + go:embed
├── internal/
│   ├── engine/                Memory extraction, relational profiling, decay, retrieval
│   ├── hooks/                 Claude Code hook handlers
│   ├── llm/                   LLM clients (claude-cli, anthropic, ollama)
│   ├── server/                HTTP API + embedded UI serving
│   ├── store/                 SQLite: migrations, nodes, vectors, sessions
│   └── transcript/            JSONL transcript parsing + condensation
├── ui/                        Svelte + Tailwind viewer SPA
├── docs/                      Operators manual
├── plugin/                    Claude Code plugin (manifest, .mcp.json, hooks/)
└── RFC.md                     Full design document
```

More test code than program. Three dependencies. One pure-Go static binary, no CGO.

## Why this exists

AI coding agents are stateless by default. Every session is a blank slate, so you
re-explain your preferences, the agent repeats mistakes you already corrected, and
no institutional knowledge ever accumulates.

Other tools bolt RAG onto your codebase. That is not memory, that is search.
Memory is knowing that *you* prefer minimal dependencies, that *you* give direct
feedback, that the last time someone touched the auth module it broke because of a
race condition.

Continuity captures the things that make working with an agent feel like working
with a colleague who actually remembers yesterday.

## License

MIT
