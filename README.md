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
  <a href="#how-it-works">How It Works</a> &bull;
  <a href="#quick-start">Quick Start</a> &bull;
  <a href="#architecture">Architecture</a> &bull;
  <a href="#building">Building</a>
</p>

---

Every time you start a Claude Code session, it forgets who you are. Your preferences, your project context, the patterns you've established, the bugs you've already solved — all gone. You re-explain yourself. Again.

Continuity fixes this. It captures what happened, what was learned, and how you work — then injects that context into future sessions automatically. No configuration beyond a single binary and five hook lines.

## What It Remembers

- **How you work** — feedback style, autonomy level, corrections given. A compounding relational profile that means your agent stops making the same mistakes.
- **What you prefer** — tools, workflows, conventions. "Always use devbox." "Never add comments unless asked." Learned once, applied forever.
- **What happened** — decisions, deployments, architecture choices. Project history that doesn't vanish when a session ends.
- **How to solve things** — patterns, techniques, bug→fix pairs. Your agent builds institutional knowledge.
- **What it was like** — relational moments that capture the texture of working together. Small anchors that never decay, so your agent wakes up knowing not just *who you are* but *what it's been like*.

## Install

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/lazypower/continuity/main/install.sh | sh

# Homebrew
brew install lazypower/tap/continuity

# From source
git clone https://github.com/lazypower/continuity.git
cd continuity && make build
```

One binary. 16MB. No runtime dependencies. No Docker. No node. No bun.

## Quick Start

**1. Start the server**

```bash
continuity serve
# continuity serving on 127.0.0.1:37777
#   db: ~/.continuity/continuity.db
#   llm: claude-cli (haiku)
#   embedder: tfidf (fallback)
```

**Or: enable autostart** so the server launches automatically when Claude Code needs it:

```bash
continuity init --autostart
```

> **Process lifecycle notice:** With `--autostart`, the SessionStart hook launches `continuity serve` as a detached background process when it detects the server isn't running. This process **persists after your Claude Code session ends** — it runs until explicitly stopped, the machine reboots, or you disable autostart. We never start background processes without your explicit opt-in.
>
> - Disable autostart: `continuity init` (without `--autostart`)
> - Stop the server: `pkill continuity` or `kill $(lsof -ti :37777)`
> - Logs: `~/.continuity/serve.log`
> - For proper process management (start on boot, auto-restart), use `continuity install-service` instead.

**Or: install as a system service** for proper lifecycle management:

```bash
continuity install-service
# Shows what will be installed, asks for confirmation
# macOS: LaunchAgent (start on login, restart on crash)
# Linux: systemd user unit (start on login, restart on failure)
```

Remove with `continuity uninstall-service`. Both commands are interactive and idempotent.

**2. Add hooks to Claude Code**

Drop this in `~/.claude/settings.json`:

```json
{
  "hooks": {
    "SessionStart": [
      { "hooks": [{ "type": "command", "command": "continuity hook start", "timeout": 10 }] }
    ],
    "UserPromptSubmit": [
      { "hooks": [{ "type": "command", "command": "continuity hook submit", "timeout": 10 }] }
    ],
    "PostToolUse": [
      { "hooks": [{ "type": "command", "command": "continuity hook tool", "timeout": 10 }] }
    ],
    "Stop": [
      { "hooks": [{ "type": "command", "command": "continuity hook stop", "timeout": 120 }] }
    ],
    "SessionEnd": [
      { "hooks": [{ "type": "command", "command": "continuity hook end", "timeout": 10 }] }
    ]
  }
}
```

**3. Initialize memory directives**

```bash
continuity init
# Initialized: /Users/you/.claude/CLAUDE.md
# Claude Code will now use continuity for memory in all sessions.
```

This writes behavioral directives to `~/.claude/CLAUDE.md` — the highest-priority instruction layer — telling Claude Code to use continuity instead of its built-in markdown memory system. Idempotent; safe to run again.

**4. Use Claude Code normally.** That's it. Continuity captures context in the background and injects it at session start. You'll see `## Continuity — Session Memory` appear in your agent's context.

**5. Say "remember this"** and Continuity captures it immediately. Signal phrases like "always use X", "never do Y", "the fix was" trigger instant memory extraction without waiting for session end.

**6. Browse your memories**

```bash
continuity search "sqlite configuration"
continuity profile
continuity tree
open http://localhost:37777    # Visual memory browser
```

## How It Works

```
┌─────────────┐    hooks     ┌──────────────┐    LLM     ┌──────────────┐
│  Claude Code │────────────▶│  continuity   │───────────▶│  Extraction  │
│   session    │◀────────────│  serve        │            │  Pipeline    │
│              │   context   │              │◀───────────│              │
└─────────────┘    inject    │  :37777       │  memories  └──────────────┘
                             │              │
                             │  ┌──────────┐│
                             │  │  SQLite   ││
                             │  │  + vectors││
                             │  └──────────┘│
                             └──────────────┘
```

**Session lifecycle:**

1. **SessionStart** — Continuity injects the current date, relational profile, moments, relevant memories, and recent sessions (with tone) into Claude's context. Flags gaps >7 days since last session.
2. **UserPromptSubmit** — Signal keywords ("remember this", "always use") trigger immediate memory capture
3. **PostToolUse** — Tool calls are buffered as observations (file edits, bash commands, etc.)
4. **Stop** — Session transcript is sent to the LLM for memory extraction, relational profiling, and tone classification
5. **SessionEnd** — Session finalized, ready for next startup

## Memory Tree

Memories aren't dumped in a flat vector store. They're organized as a browsable tree:

```
mem://
├── user/
│   ├── profile/
│   │   └── communication    → Relational profile (how you work)
│   ├── preferences/
│   │   ├── minimal-deps     → "Prefers standard library, minimal dependencies"
│   │   └── devbox-tooling   → "Always use devbox for development"
│   ├── entities/
│   │   └── continuity-go    → "Go CLI tool for AI agent memory"
│   ├── events/
│   │   └── v1-release       → "Released v1.0 with embedded UI"
│   └── moments/
│       └── first-gift       → "walked me through reflections, then presented a spec"
└── agent/
    ├── patterns/
    │   └── sqlite-wal-mode  → "Use WAL mode for concurrent SQLite access"
    └── cases/
        └── embed-gitignore  → "Fix: /binary pattern to avoid ignoring cmd/ dirs"
```

Every node has three tiers:
- **L0** (~100 tokens) — Abstract. Used for search and context injection.
- **L1** (~2K tokens) — Overview. Shown when you expand a memory.
- **L2** (full) — Complete content. On-demand.

Agents get shape without weight. The right memories surface at the right time.

## Architecture

**7 memory categories**, each with merge rules:

| Category | Owner | Mergeable | Decay | Example |
|----------|-------|-----------|-------|---------|
| `profile` | user | yes | yes | Coding style, skills, identity |
| `preferences` | user | yes | yes | Tools, workflows, conventions |
| `entities` | user | no | yes | Projects, people, services |
| `events` | user | no | yes | Decisions, deployments |
| `patterns` | agent | yes | yes | Reusable techniques, solutions |
| `cases` | agent | no | yes | Bug→fix pairs |
| `moments` | user | no | **no** | Relational anchors — texture, not facts |

**Smart decay**: 90-day half-life without access. Retrieval boosts relevance back to 1.0. Stale memories fade but never disappear — floor of 0.1. Moments and the relational profile are exempt.

**Relational profiling**: Extracts *how you work* — not what you work on. Feedback calibration, autonomy preferences, corrections given, trust earned. This is the compounding profile that makes your agent better over time.

**Session tone**: Each completed session gets a compressed emotional arc — a 10-20 token fragment like "flow state, sharp pivots" or "grind into breakthrough, late-night clarity." Displayed in session history so the agent reads narrative, not just logs.

**Moments**: Permanent relational anchors that capture *what it was like*, not what happened. Max 10 stored, 2-3 injected per session with diversity sampling (no two from the same emotional register). Pool eviction uses cosine similarity — the most semantically redundant moment gets displaced. Moments must pass a four-part qualification filter: relational, mutual, acknowledged, and counter-expected.

## LLM Providers

Continuity uses an LLM for memory extraction and semantic search. Three options:

| Provider | Config | Cost | Best For |
|----------|--------|------|----------|
| `claude-cli` | Default, zero config | Free with Max | Most users |
| `anthropic` | Set `ANTHROPIC_API_KEY` | API billing | Headless/CI |
| `ollama` | Run Ollama locally | Free | Privacy, offline |

Haiku handles bulk extraction. The Claude CLI provider (`claude -p`) is free with a Max subscription — no API key needed.

For embeddings: Ollama with `nomic-embed-text` if available, otherwise falls back to TF-IDF (zero external dependencies).

## CLI

```
continuity serve              Start the HTTP API server
continuity init [--autostart] Set up Claude Code integration + optional autostart
continuity timeline [--days N] [--project X]  Session clusters, gaps, and rhythm
continuity install-service    Install as system service (launchd/systemd)
continuity uninstall-service  Remove system service
continuity hook <evt>         Handle Claude Code hook events
continuity search             Search memories by query
continuity remember    Store a memory directly (no LLM needed)
continuity profile     Show relational profile
continuity tree        Browse the memory tree
continuity dedup       Deduplicate similar memory nodes
continuity version     Print version information
```

## API

All endpoints on `http://127.0.0.1:37777`:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Server health + uptime |
| `GET` | `/api/tree?uri=` | Browse memory tree |
| `GET` | `/api/search?q=&mode=find\|search` | Query memories |
| `GET` | `/api/profile` | Relational profile + preference nodes |
| `GET` | `/api/context?session_id=` | Get injection context |
| `POST` | `/api/sessions/init` | Initialize session |
| `POST` | `/api/sessions/{id}/signal` | Signal keyword extraction |
| `POST` | `/api/sessions/{id}/extract` | Full session extraction |
| `GET` | `/` | Embedded viewer UI |

## Building

Requires [devbox](https://www.jetpack.io/devbox/) (provides Go 1.24, Node 22, SQLite):

```bash
make build     # Build UI + Go binary
make test      # Run all tests
make dist      # Cross-compile for all platforms
make run       # Build and start server
make clean     # Remove build artifacts
```

Or without devbox:

```bash
cd ui && npm install && npm run build && cd ..
cp -r ui/dist cmd/continuity/ui
go build -o continuity ./cmd/continuity
```

## Project Structure

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
├── Formula/                   Homebrew formula
├── .github/workflows/         CI + release automation
├── plugin/hooks.json          Claude Code hook definitions
├── install.sh                 curl-pipe-sh installer
└── RFC.md                     Full design document
```

~7,500 lines of Go + Svelte. No generated code. No frameworks beyond cobra and chi.

## Why This Exists

AI coding agents are stateless by default. Every session is a blank slate. This means:

- You re-explain your preferences every time
- The agent makes the same mistakes you've already corrected
- Project context is lost between sessions
- There's no institutional knowledge — no learning curve

Other tools bolt on RAG over your codebase. That's not memory — that's search. Memory is knowing that *you* prefer minimal dependencies, that *you* give direct feedback, that the last time someone touched the auth module it broke because of a race condition.

Continuity captures the things that make working with an agent feel like working with a colleague who actually remembers yesterday.

## License

MIT
