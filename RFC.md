# RFC-001: Continuity

> *In fact, forget the park. And the blackjack.*

## Status: Draft
## Author: Chuck + Claude
## Date: 2026-02-25

---

## 1. Problem Statement

claude-mem works. The ideas are proven: hook-based lifecycle capture, AI-powered compression, relational profiling, context injection. But the implementation is buried under an ecosystem tax that makes it fragile and hostile to new environments:

- **5 runtimes** to do one job: Node.js, Bun, Python (for Chroma), uv, SQLite CLI
- **bun-runner.js** exists solely to find bun when it's not in PATH
- **smart-install.js** exists solely to install the runtimes that run the actual code
- **devbox** exists solely to make the runtimes reproducible
- **SessionStore has a duplicate migration chain** because two subsystems evolved independently
- Stop hooks don't reliably fire because the bun→node→worker relay chain has too many failure modes

The architecture is sound. The substrate is the problem.

## 2. Proposal

Replace the entire JS/Python stack with **Continuity** — a single statically-linked Go binary. Same concepts, clean-room implementation, zero runtime dependencies.

```
$ continuity
Usage:
  continuity serve                 # Start HTTP API + worker
  continuity hook <event>          # Handle Claude Code hook (reads stdin)
  continuity search <query>        # Search memories from CLI
  continuity profile               # Show relational profile
  continuity tree [path]           # Browse memory tree
  continuity import                # Migrate from claude-mem SQLite DB
  continuity version               # Print version
```

One binary. Ships as a GitHub release. `brew install continuity` or `curl | sh`. No npm, no bun, no devbox, no smart-install. Hook scripts become:

```json
{
  "type": "command",
  "command": "continuity hook stop --transcript=${CLAUDE_TRANSCRIPT}",
  "timeout": 120
}
```

## 3. Architecture

```
┌─────────────────────────────────────────────────────┐
│                  continuity binary                    │
├──────────┬──────────┬───────────┬───────────────────┤
│  CLI     │  Hooks   │  HTTP API │  Worker (goroutines)│
│  search  │  start   │  /context │  compress           │
│  profile │  submit  │  /search  │  extract            │
│  tree    │  tool    │  /health  │  decay              │
│  import  │  stop    │  /profile │  vectorize          │
│          │  end     │  /tree    │                     │
├──────────┴──────────┴───────────┴───────────────────┤
│                  Memory Engine                        │
│  ┌─────────┐ ┌──────────┐ ┌────────┐ ┌───────────┐ │
│  │MemTree  │ │Extractor │ │Relator │ │ Retriever │ │
│  │(L0/L1/L2)│ │(taxonomy)│ │(profile)│ │(hybrid)  │ │
│  └─────────┘ └──────────┘ └────────┘ └───────────┘ │
├─────────────────────────────────────────────────────┤
│                  Storage Layer                        │
│  ┌──────────────────┐  ┌──────────────────────────┐ │
│  │ SQLite            │  │ Embedded Vector Index     │ │
│  │ (modernc.org)     │  │ (pure Go, no CGO)        │ │
│  │ ~/.continuity/    │  │ HNSW in same .db file    │ │
│  └──────────────────┘  └──────────────────────────┘ │
├─────────────────────────────────────────────────────┤
│                  LLM Clients                         │
│  Claude CLI  │  Anthropic API  │  Gemini  │  Ollama │
└─────────────────────────────────────────────────────┘
```

## 4. The Memory Tree (stolen from OpenViking, made ours)

Flat vector databases treat all content as equal-weight chunks floating in embedding space. This sucks for two reasons: (1) you burn tokens retrieving irrelevant detail, and (2) you can't browse — you can only search.

We steal OpenViking's filesystem paradigm but implement it in SQLite, not on disk.

### 4.1 Virtual Filesystem in SQLite

```sql
CREATE TABLE mem_nodes (
  id          INTEGER PRIMARY KEY,
  uri         TEXT NOT NULL UNIQUE,  -- 'mem://user/profile/coding-style'
  parent_uri  TEXT,                  -- 'mem://user/profile/'
  node_type   TEXT NOT NULL,         -- 'dir' | 'leaf'
  category    TEXT NOT NULL,         -- taxonomy category (see §5)

  -- Three-tier content
  l0_abstract TEXT,     -- ~100 tokens. Vector search surface.
  l1_overview TEXT,     -- ~2K tokens. Structured summary.
  l2_content  TEXT,     -- Full content. Loaded on demand.

  -- Merge control
  mergeable   INTEGER NOT NULL DEFAULT 0,  -- Can this be updated in place?
  merged_from TEXT,     -- JSON array of source node IDs if merged

  -- Decay
  relevance   REAL NOT NULL DEFAULT 1.0,   -- Decays over time
  last_access INTEGER,  -- epoch ms, boosted on retrieval
  access_count INTEGER NOT NULL DEFAULT 0,

  -- Metadata
  source_session TEXT,  -- session ID that created this
  created_at  INTEGER NOT NULL,  -- epoch ms
  updated_at  INTEGER NOT NULL,  -- epoch ms

  FOREIGN KEY (parent_uri) REFERENCES mem_nodes(uri)
);

CREATE INDEX idx_nodes_parent ON mem_nodes(parent_uri);
CREATE INDEX idx_nodes_category ON mem_nodes(category);
CREATE INDEX idx_nodes_relevance ON mem_nodes(relevance DESC);
```

### 4.2 Three-Tier Progressive Loading

Every memory gets three representations, auto-generated by LLM:

| Tier | Size | Purpose | When loaded |
|------|------|---------|-------------|
| **L0** | ~100 tokens | One-line abstract. This is the vector search surface. | Always — it's what gets embedded and searched |
| **L1** | ~2K tokens | Structured overview. Enough to decide if L2 is needed. | Context injection, browsing |
| **L2** | Unlimited | Full original content. Raw observations, full transcripts. | On-demand deep retrieval only |

**Why this matters**: Current claude-mem injects compressed observations at full fidelity. With L0/L1/L2, Continuity injects L1s — the agent gets the shape of what happened without the weight. If it needs detail, it can pull specific L2s via the search skill.

### 4.3 URI Addressing

```
mem://user/profile/                    # User identity & preferences
mem://user/profile/coding-style        # Mergeable leaf
mem://user/profile/communication       # Mergeable leaf (relational)
mem://user/entities/                    # People, projects, services
mem://user/entities/acme-api           # Immutable leaf
mem://user/events/                     # Things that happened
mem://user/events/2026-02-25-deploy    # Immutable leaf

mem://agent/patterns/                  # Reusable techniques learned
mem://agent/patterns/error-handling    # Mergeable leaf
mem://agent/cases/                     # Problem→solution pairs
mem://agent/cases/sqlite-migration     # Immutable leaf

mem://sessions/                        # Session archive
mem://sessions/abc123/                 # Per-session directory
mem://sessions/abc123/summary          # L1 session summary
mem://sessions/abc123/observations     # L2 full observations
```

Deterministic access: `continuity tree mem://user/profile/` shows all profile nodes without search. Debugging is trivial — you can browse the tree.

## 5. Memory Taxonomy

Six categories with explicit merge rules. Stolen from OpenViking's insight that profile data should be updated but historical events should be preserved.

| Category | Owner | Mergeable | Description |
|----------|-------|-----------|-------------|
| **profile** | user | yes | Identity attributes: coding style, tool preferences, communication patterns. Updated as understanding deepens. |
| **preferences** | user | yes | Changeable choices: "uses bun not npm", "prefers Go over Rust". Overwritten when preferences change. |
| **entities** | user | no | People, projects, services, APIs the user works with. Each is a distinct node. Never merged — `acme-api` and `beta-service` stay separate. |
| **events** | user | no | Completed actions with timestamps: deployments, bug fixes, decisions. Immutable historical record. |
| **patterns** | agent | yes | Reusable techniques: "this codebase uses X pattern for Y". Merged as understanding refines. |
| **cases** | agent | no | Problem→solution pairs: "SQLite migration failed because SessionStore has its own chain". Immutable reference. |

### 5.1 Extraction Pipeline

After each session (Stop hook), the extractor:

1. Reads the transcript
2. Condenses it (all user messages, assistant bookends, drop tool_use/tool_result)
3. Sends to LLM with the taxonomy definitions
4. LLM returns structured candidates: `{ category, uri_hint, l0, l1, l2, merge_target? }`
5. Vector dedup against existing L0s
6. For each candidate: **skip** (duplicate), **create** (new node), or **merge** (update existing mergeable node)
7. Persist to SQLite + update vector index

### 5.2 The Relational Layer (ours, nobody else has this)

The relational profile from the current implementation maps to `mem://user/profile/communication` — a mergeable node in the profile category. Same extraction prompt (feedback calibration, working dynamic, corrections received, earned signals), same compounding behavior. But now it's a first-class node in the tree, not a separate table.

## 6. Smart Decay

Memories are not forever. Stolen from supermemory's insight that human memory fades.

```go
// Decay runs daily (or on serve startup)
func (e *Engine) DecayMemories() {
    // Half-life: 90 days without access
    // Each access resets the decay clock
    // Relevance floor: 0.1 (never fully forgotten, just deprioritized)

    e.db.Exec(`
        UPDATE mem_nodes
        SET relevance = MAX(0.1, relevance * pow(0.5,
            (strftime('%s','now')*1000 - last_access) / (90.0 * 86400000)
        ))
        WHERE last_access IS NOT NULL
        AND node_type = 'leaf'
    `)
}
```

Retrieval boosts relevance: when a memory is included in context injection or returned from search, its `last_access` and `access_count` update. Frequently useful memories stay vivid. Stale ones fade to near-zero but never disappear — a search can still surface them.

**Decay exemptions**: Relational profile nodes (`mem://user/profile/communication`) don't decay. How you work with someone doesn't become less relevant over time.

## 7. Retrieval: Hybrid Search

Two modes, matching OpenViking's find/search split:

### 7.1 `find(query)` — Fast, no LLM

Direct vector similarity against L0 abstracts. Returns top-k nodes ranked by `similarity * relevance`. Good for direct lookups: "what do I know about the acme API?"

### 7.2 `search(query, session_context)` — Smart, LLM-assisted

1. **Intent analysis** (LLM): decompose query into 1-3 typed sub-queries, each tagged MEMORY/RESOURCE/PATTERN
2. **Tree-aware retrieval**: for each sub-query:
   - Vector search against L0s to find candidate directories
   - Walk up to parent, check sibling relevance (contextual gravity)
   - Score = `0.5 * embedding_similarity + 0.3 * relevance + 0.2 * parent_score`
3. **Progressive loading**: return L1s by default, let the agent request specific L2s
4. **Convergence**: stop after 3 rounds of stable top-k

### 7.3 Context Injection (SessionStart)

The main context payload sent to Claude at session start:

```markdown
## Working With You
{L1 of mem://user/profile/communication}

## Your Profile
{L1 of mem://user/profile/coding-style}
{L1 of mem://user/profile/preferences}

## Recent Activity
{L1s of recent session nodes, sorted by date, filtered by relevance > 0.3}

## Active Entities
{L1s of frequently-accessed entity nodes}
```

Total budget: ~4K tokens for context injection. L0s used for selection, L1s used for content. L2s never injected automatically.

## 8. Signal Keywords

Stolen from supermemory. Certain phrases trigger immediate high-priority capture:

| Trigger | Action |
|---------|--------|
| "remember this", "don't forget" | Capture surrounding context as high-relevance memory |
| "always use X", "never do Y" | Capture as preference node (mergeable) |
| "architecture decision", "we decided" | Capture as event node (immutable) |
| "this pattern", "the trick is" | Capture as pattern node (mergeable) |
| "bug was", "root cause" | Capture as case node (immutable) |

These are processed at the UserPromptSubmit hook — no need to wait for session end. The extracted memory is immediately available in subsequent context injections within the same session.

## 9. LLM Backend

Continuity needs to call an LLM for: L0/L1 generation, memory extraction, relational profiling, intent analysis, and merge decisions.

Priority order:
1. **Claude CLI** — `claude -p` subprocess. Uses existing Claude Max subscription. No API key needed. Free with what you're already paying for. This is what claude-mem used successfully.
2. **Ollama** — Local models. Zero cost, works offline. Good enough for L0/L1 generation and potentially extraction.
3. **Anthropic API direct** — `ANTHROPIC_API_KEY` env var. Fastest and cleanest, but costs extra — API billing is completely separate from Claude Max.
4. **Gemini / OpenRouter** — Alternatives if you have keys.

Configuration in `~/.continuity/config.toml`:

```toml
[llm]
provider = "claude-cli"  # claude-cli | ollama | anthropic | gemini | openrouter
model = "claude-haiku-4-5-20251001"

[llm.extraction]
model = "claude-haiku-4-5-20251001"  # cheap/fast model for bulk extraction

[llm.merge]
model = "claude-sonnet-4-6"  # smarter model for merge decisions

[llm.ollama]
endpoint = "http://localhost:11434"
model = "llama3.2"

[llm.anthropic]
api_key_env = "ANTHROPIC_API_KEY"  # only needed if provider = "anthropic"
```

## 10. Hook Contract

Claude Code hooks communicate via stdin (JSON) and exit codes. Continuity handles this natively:

```
continuity hook start          # stdin: session info → inject context → stdout: JSON
continuity hook submit         # stdin: user message → signal keyword scan → stdout: JSON
continuity hook tool           # stdin: tool result → buffer observation
continuity hook stop           # stdin: session info → async: extract + relational + decay
continuity hook end            # stdin: session info → finalize session archive
```

Exit codes (unchanged from current):
- **0**: Success
- **1**: Non-blocking error (stderr shown to user)
- **2**: Blocking error (stderr fed to Claude)

No bun-runner. No node subprocess. No finding executables. The binary IS the executable.

## 11. HTTP API

Same port (37777), same endpoints, for viewer UI and search skill compatibility:

```
GET  /api/health
GET  /api/context/:sessionId
GET  /api/search?q=<query>&mode=find|search
GET  /api/profile
GET  /api/tree?uri=<path>
GET  /api/sessions
GET  /api/sessions/:id
POST /api/memories                    # Manual memory creation
GET  /                                # Embedded viewer UI (SPA)
```

The viewer UI HTML/JS/CSS is embedded in the binary via `go:embed`. No separate build step.

## 12. Storage

Single file: `~/.continuity/continuity.db`

SQLite via `modernc.org/sqlite` — pure Go, no CGO, cross-compiles to every platform without a C toolchain.

Vector index: HNSW implementation in pure Go, stored as a separate table in the same SQLite database. Alternatives to evaluate:
- `github.com/viterin/vek` — SIMD-accelerated vector operations
- Custom HNSW with SQLite-backed adjacency lists
- Or just brute-force cosine similarity if the corpus stays under 10K nodes (it will for a long time)

Embeddings: generated locally via Ollama, or via API (Anthropic/OpenAI/Voyage). Stored in a `mem_vectors` table alongside the node ID.

## 13. Migration Path

```
continuity import --from=claude-mem
```

Reads the existing `~/.claude-mem/claude-mem.db` (JS schema), extracts:
- Session summaries → `mem://sessions/` nodes
- Observations → fed through extraction pipeline to create categorized memories
- Relational profile → `mem://user/profile/communication`
- Settings → `~/.continuity/config.toml`

The import is non-destructive — it reads the old DB and writes to the new schema. Old DB is preserved.

## 14. Build & Distribution

```bash
# Build
go build -o continuity ./cmd/continuity

# Cross-compile
GOOS=darwin GOARCH=arm64 go build -o continuity-darwin-arm64
GOOS=linux GOARCH=amd64 go build -o continuity-linux-amd64
GOOS=windows GOARCH=amd64 go build -o continuity-windows-amd64.exe

# Install
brew install continuity        # macOS
curl -fsSL https://... | sh    # Linux
scoop install continuity       # Windows
```

GitHub Actions builds all platforms on tag push. Single binary, ~15-25MB with embedded UI.

## 15. Project Structure

```
continuity/
├── cmd/
│   └── continuity/
│       └── main.go              # CLI entry point (cobra)
├── internal/
│   ├── engine/
│   │   ├── engine.go            # Memory engine orchestrator
│   │   ├── extractor.go         # Taxonomy-based memory extraction
│   │   ├── relator.go           # Relational profile extraction
│   │   ├── decay.go             # Smart decay implementation
│   │   └── retriever.go         # Hybrid find/search retrieval
│   ├── hooks/
│   │   ├── handler.go           # Hook dispatcher
│   │   ├── start.go             # SessionStart: context injection
│   │   ├── submit.go            # UserPromptSubmit: signal keywords
│   │   ├── tool.go              # PostToolUse: buffer observations
│   │   ├── stop.go              # Stop: trigger extraction
│   │   └── end.go               # SessionEnd: finalize
│   ├── llm/
│   │   ├── client.go            # LLM client interface
│   │   ├── anthropic.go         # Anthropic API direct
│   │   ├── claude_cli.go        # Claude CLI subprocess
│   │   ├── ollama.go            # Ollama local
│   │   └── prompts.go           # All prompt templates
│   ├── server/
│   │   ├── server.go            # HTTP API (net/http or chi)
│   │   ├── routes.go            # Route handlers
│   │   └── middleware.go        # CORS, logging
│   ├── store/
│   │   ├── db.go                # SQLite connection + migrations
│   │   ├── nodes.go             # mem_nodes CRUD
│   │   ├── vectors.go           # Vector index operations
│   │   └── sessions.go          # Session tracking
│   ├── tree/
│   │   ├── tree.go              # Virtual filesystem operations
│   │   ├── uri.go               # mem:// URI parsing
│   │   └── walk.go              # Tree traversal
│   └── transcript/
│       ├── parser.go            # JSONL transcript parser
│       └── condenser.go         # Transcript condensation
├── ui/
│   └── viewer/                  # Embedded SPA (go:embed)
├── plugin/
│   └── hooks.json               # Claude Code hook definitions
├── go.mod
├── go.sum
└── Makefile
```

## 16. Implementation Phases

### Phase 0: Skeleton (1 session)
- `go mod init github.com/chuck/continuity`, cobra CLI, SQLite connection, health endpoint
- Prove: binary compiles, starts HTTP server, creates DB

### Phase 1: Hook Pipeline (2-3 sessions)
- Transcript parser (port from TS)
- Hook handlers (start/submit/tool/stop/end)
- Basic observation buffering (no extraction yet, just capture)
- Context injection (port existing format)

### Phase 2: Memory Engine (2-3 sessions)
- mem_nodes schema + CRUD
- LLM client (Anthropic API first, Claude CLI second)
- Extraction pipeline: transcript → candidates → dedup → create/merge
- L0/L1/L2 generation
- Relational profiler (port from TS)

### Phase 3: Retrieval (1-2 sessions)
- Vector embeddings (brute-force cosine first, HNSW later if needed)
- `find()` — direct vector search
- `search()` — LLM-assisted intent decomposition
- Smart decay

### Phase 4: UI + Polish (1-2 sessions)
- Embed existing viewer UI (or minimal replacement)
- `continuity import` migration tool
- Signal keyword detection
- Tree browsing CLI

### Phase 5: Distribution
- GitHub Actions cross-compilation
- Homebrew formula
- Install script
- Update the Claude Code plugin hooks.json to point at `continuity`

## 17. What We're NOT Building

- Cloud sync / multi-device (local-first, period)
- Multi-user / team features (single user, single machine)
- Plugin marketplace packaging (Continuity is a standalone binary that Claude Code hooks call)
- MCP server (hooks are simpler and sufficient; can add later if needed)
- Web scraping / URL ingestion (memories come from sessions, not the internet)

## 18. Success Criteria

Continuity is done when:

1. `continuity serve` replaces the entire bun worker service
2. Hook scripts are one-liners calling `continuity hook <event>`
3. Context injection at SessionStart works identically to current
4. Memory extraction produces categorized L0/L1/L2 nodes from real sessions
5. Relational profiling compounds across sessions
6. `continuity search` returns relevant results from CLI
7. A fresh machine goes from zero to working in: `brew install continuity`
8. No node. No bun. No python. No devbox. No npm. Just the binary.
