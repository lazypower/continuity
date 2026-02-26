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

**2. Add hooks to Claude Code**

Drop this in `~/.claude/settings.json`:

```json
{
  "hooks": {
    "SessionStart": [
      { "type": "command", "command": "continuity hook start", "timeout": 10 }
    ],
    "UserPromptSubmit": [
      { "type": "command", "command": "continuity hook submit", "timeout": 10 }
    ],
    "PostToolUse": [
      { "type": "command", "command": "continuity hook tool", "timeout": 10 }
    ],
    "Stop": [
      { "type": "command", "command": "continuity hook stop --transcript=${CLAUDE_TRANSCRIPT}", "timeout": 120 }
    ],
    "SessionEnd": [
      { "type": "command", "command": "continuity hook end", "timeout": 10 }
    ]
  }
}
```

**3. Use Claude Code normally.** That's it. Continuity captures context in the background and injects it at session start. You'll see `## Continuity — Session Memory` appear in your agent's context.

**4. Say "remember this"** and Continuity captures it immediately. Signal phrases like "always use X", "the bug was", "we decided" trigger instant memory extraction without waiting for session end.

**5. Browse your memories**

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

1. **SessionStart** — Continuity injects relevant memories, recent session summaries, and the relational profile into Claude's context
2. **UserPromptSubmit** — Signal keywords ("remember this", "always use") trigger immediate memory capture
3. **PostToolUse** — Tool calls are buffered as observations (file edits, bash commands, etc.)
4. **Stop** — Session transcript is sent to the LLM for full memory extraction
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
│   └── events/
│       └── v1-release       → "Released v1.0 with embedded UI"
└── agent/
    ├── patterns/
    │   └── sqlite-wal-mode  → "Use WAL mode for concurrent SQLite access"
    └── cases/
        └── embed-gitignore  → "Fix: /binary pattern to avoid ignoring cmd/ dirs"
```

Every node has three tiers:
- **L0** (~100 tokens) — Abstract. Used for search and context injection.
- **L1** (~500 tokens) — Overview. Shown when you expand a memory.
- **L2** (full) — Complete content. On-demand.

Agents get shape without weight. The right memories surface at the right time.

## Architecture

**6 memory categories**, each with merge rules:

| Category | Owner | Mergeable | Example |
|----------|-------|-----------|---------|
| `profile` | user | yes | Coding style, skills, identity |
| `preferences` | user | yes | Tools, workflows, conventions |
| `entities` | user | no | Projects, people, services |
| `events` | user | no | Decisions, deployments |
| `patterns` | agent | yes | Reusable techniques, solutions |
| `cases` | agent | no | Bug→fix pairs |

**Smart decay**: 90-day half-life without access. Retrieval boosts relevance back to 1.0. Stale memories fade but never disappear — floor of 0.1.

**Relational profiling**: Extracts *how you work* — not what you work on. Feedback calibration, autonomy preferences, corrections given, trust earned. This is the compounding profile that makes your agent better over time.

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
continuity serve       Start the HTTP API server
continuity hook <evt>  Handle Claude Code hook events
continuity search      Search memories by query
continuity profile     Show relational profile
continuity tree        Browse the memory tree
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
