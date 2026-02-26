# Continuity

Persistent memory for AI coding agents. Single Go binary, zero dependencies.

## What This Is

Continuity gives Claude Code (and eventually any AI agent) memory that persists across sessions. It captures what happened, what was learned, and how you work — then injects that context into future sessions so the agent doesn't start cold every time.

**This is a clean-room Go rewrite** of [claude-mem](../continuity/) (TypeScript/Bun). The ideas are proven — the JS ecosystem was the problem. Read `RFC.md` for the full design rationale and architecture.

## Core Concepts

- **Memory Tree**: Hierarchical memory organized as a virtual filesystem with `mem://` URIs. Not a flat vector store — a browsable tree.
- **L0/L1/L2 Tiering**: Every memory has three representations: ~100 token abstract (search surface), ~2K token overview (context injection), full content (on-demand). Agents get shape without weight.
- **6-Category Taxonomy**: profile (mergeable), preferences (mergeable), entities (immutable), events (immutable), patterns (mergeable), cases (immutable). Merge rules prevent memory corruption.
- **Relational Profiling**: Extracts *how the user works* (feedback style, autonomy level, corrections given) as a compounding profile. No other tool does this.
- **Smart Decay**: 90-day half-life without access. Retrieval boosts relevance. Stale memories fade but never disappear.
- **Signal Keywords**: "remember this", "always use X", "bug was" trigger immediate capture at user-message time, not just session end.

## Architecture

```
continuity serve       → HTTP API + background worker (goroutines)
continuity hook <evt>  → Claude Code hook handler (reads stdin JSON, writes stdout)
continuity search      → CLI memory search
continuity profile     → Show relational profile
continuity tree        → Browse memory tree
continuity import      → Migrate from claude-mem JS database
```

Single binary. SQLite via `modernc.org/sqlite` (pure Go, no CGO). Embedded vector search. `go:embed` for viewer UI.

## Tech Stack

- **Language**: Go 1.22+
- **Database**: SQLite via `modernc.org/sqlite` — pure Go, cross-compiles everywhere
- **CLI**: `cobra` for commands
- **HTTP**: `net/http` + `chi` router
- **UI**: Svelte + Tailwind CSS + Vite → static assets embedded via `go:embed`
- **LLM**: Claude CLI `claude -p` (primary, free with Max), Ollama (local/free), Anthropic API (optional, separate billing)
- **Config**: TOML at `~/.continuity/config.toml`
- **Data**: `~/.continuity/continuity.db`

## Claude Code Integration

Continuity integrates via Claude Code's hook system. Hook scripts are one-liners:

```json
{
  "hooks": {
    "SessionStart": [{ "type": "command", "command": "continuity hook start" }],
    "UserPromptSubmit": [{ "type": "command", "command": "continuity hook submit" }],
    "PostToolUse": [{ "type": "command", "command": "continuity hook tool" }],
    "Stop": [{ "type": "command", "command": "continuity hook stop --transcript=${CLAUDE_TRANSCRIPT}" }],
    "SessionEnd": [{ "type": "command", "command": "continuity hook end" }]
  }
}
```

No bun-runner. No node. No wrapper scripts. The binary IS the hook handler.

## Project Structure

```
continuity-go/
├── cmd/continuity/main.go         # CLI entry (cobra)
├── internal/
│   ├── engine/                    # Memory engine: extraction, relational, decay, retrieval
│   ├── hooks/                     # Hook handlers: start, submit, tool, stop, end
│   ├── llm/                       # LLM clients: anthropic, claude-cli, ollama
│   ├── server/                    # HTTP API
│   ├── store/                     # SQLite: migrations, nodes, vectors, sessions
│   ├── tree/                      # Virtual filesystem: URI parsing, traversal
│   └── transcript/                # JSONL transcript parsing + condensation
├── ui/                            # Svelte + Tailwind SPA, embedded via go:embed
├── plugin/hooks.json              # Claude Code hook definitions
├── RFC.md                         # Full design document
├── go.mod
└── Makefile
```

## Build

```bash
go build -o continuity ./cmd/continuity
./continuity version
./continuity serve
```

## Implementation Phases

See `RFC.md` §16 for full breakdown. Summary:

0. **Skeleton**: cobra CLI + SQLite + health endpoint — prove it compiles and serves
1. **Hook Pipeline**: transcript parser, hook handlers, observation buffering, context injection
2. **Memory Engine**: mem_nodes, LLM client, extraction pipeline, L0/L1/L2, relational profiler
3. **Retrieval**: vector embeddings, find/search, smart decay
4. **UI + Polish**: embedded viewer, import tool, signal keywords, tree browsing
5. **Distribution**: cross-compilation, Homebrew, install script

## Legacy Migration

The old claude-mem JS database lives at `~/.claude-mem/claude-mem.db`. Run `continuity import --from=claude-mem` to migrate sessions, observations, and the relational profile into the new memory tree. Non-destructive — old DB is preserved.

## Development Notes

- **Always use devbox** for development tooling: `devbox shell` provides Go, SQLite tools, etc.
- The predecessor project lives at `../continuity/` — reference it for battle-tested prompt templates, transcript parsing logic, and hook contracts
- Claude CLI (`claude -p`) is the primary LLM provider — free with Max subscription. Use Haiku for bulk extraction, Sonnet for merge decisions. Anthropic API is optional (separate billing from Max).
- Exit codes matter: 0 = success, 1 = non-blocking error (stderr shown to user), 2 = blocking error (stderr fed to Claude)
- SQLite WAL mode for concurrent reads during hook handling
