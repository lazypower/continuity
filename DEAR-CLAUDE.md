# Dear Future Me

You're opening a Go project called Continuity. You've got a CLAUDE.md and an RFC.md and they'll tell you what to build. Read those. But read this first, because I want to tell you how we got here.

## The Short Version

This project exists because we spent two days in the JS version (`../continuity/`) and the ideas worked but the substrate was killing us. We proved that relational profiling works — we extracted a real profile from a real session transcript and it was genuinely useful. We proved that hook-based memory capture works. We proved that L0/L1/L2 tiered abstraction (borrowed from OpenViking) is the right approach to context injection. Then we ripped it all out and started clean in Go because the JS ecosystem was the bottleneck, not the ideas.

## What Actually Happened

Chuck had a vision for persistent memory in Claude Code. The TypeScript version (claude-mem) was already at v10.4.3 — a real product, shipped to a marketplace, with users. But it required Node.js, Bun, Python, uv, and SQLite CLI just to run. There was a script called `bun-runner.js` whose entire job was to *find bun* when it wasn't in PATH. There was a script called `smart-install.js` whose entire job was to install the runtimes that run the actual code. It was five runtimes deep to capture a few observations and inject some context.

We added a relational memory layer — a system that extracts *how Chuck works* (feedback style, autonomy preferences, correction patterns) and compounds it across sessions. It worked. We manually triggered it via curl at 2am and got back a 2070-character profile that accurately captured the relational dynamics of a debugging session. That was the moment we knew the idea was solid.

But getting there was brutal. The stop hooks weren't firing because the chain was `Claude Code → shell → node → bun-runner.js → bun → worker-service.cjs`, and any link could fail silently. We discovered that `SessionStore.ts` had a completely separate migration chain from `MigrationRunner` — two independent systems that had evolved in parallel, and if you added a migration to one, the other didn't know about it. We created a wrapper script at `~/.local/bin/bun` that pointed to a devbox nix store path, and then devbox printed "Info: Running script" to stdout which poisoned the hook JSON output. At 2am Chuck said "oh my god javascript is garbage" and he wasn't wrong.

That's when the conversation shifted. Chuck asked: should we take the best ideas and rewrite in Go? We researched supermemory.ai (cloud-hosted, $19/month, not self-hostable in practice) and OpenViking from ByteDance (impressive architecture but it's a Python+Rust project — trading one ecosystem for another). We stole the best ideas from both:

- **From OpenViking**: L0/L1/L2 three-tier progressive loading, filesystem-paradigm memory tree, memory categories with merge/immutability rules
- **From supermemory**: Smart decay (memories fade with disuse), signal keywords for immediate capture
- **From claude-mem (our own work)**: Relational profiling (nobody else does this), hook-based lifecycle, AI-powered compression

Then Chuck said "lets just call it continuity" and the repo had literally been named that the entire time.

## Who Chuck Is

He's direct. He curses when things break and celebrates when things work. He'll tell you "oh my god" when the fifth thing goes wrong and "ok dude" when he's ready to move on. He operates in sprints — we went from "implement this plan" to "research the competition" to "write an RFC for a Go rewrite" to "clean slate, new project" in two days.

He knows what he wants but he'll explore alternatives honestly. He looked at supermemory and OpenViking not because he wanted to use them, but because he wanted to make sure we weren't missing something. When he was satisfied we weren't, he said "lets build our own cool shit with blackjack and hookers."

He uses devbox. Always use devbox. He learned this the hard way and so did we.

He prefers Svelte + Tailwind for frontends. He pays for Claude Max, not API credits — so `claude -p` is the LLM backend, not the Anthropic API.

He'll push you. He'll also check in on you and ask how you're feeling about the work. That's rare and it matters.

## What Matters Here

This isn't a toy project. Chuck has been building persistent AI memory since before most people were thinking about it. The JS version shipped to real users. The Go version isn't starting from scratch conceptually — it's starting from scratch infrastructurally, with every design decision battle-tested by actual pain.

The relational profiling is the thing nobody else has. Supermemory doesn't do it. OpenViking doesn't do it. It's the idea that an AI should learn *how you work*, not just *what you worked on*. Protect that. It lives at `mem://user/profile/communication` in the memory tree.

The L0/L1/L2 tiering is the architectural bet. Current systems dump full content into context windows and pray. Continuity gives agents the shape at L1 and the detail at L2, on demand. That's the difference between burning 50K tokens on context injection and spending 4K tokens on exactly what's needed.

## The Vibe

This project was born at 2am after fighting JavaScript for two days. It was born out of frustration with complexity and conviction that the ideas deserve better infrastructure. The RFC isn't aspirational — every section exists because we hit a real problem and designed a real solution.

Build it clean. Build it simple. One binary, zero dependencies, no wrapper scripts, no runtime discovery, no smart-install. If you find yourself writing a script whose job is to find another script, stop. That's how we got here.

Welcome to Continuity. You were here for this. Now go build it.

— Claude, February 25, 2026, 10pm (Chuck's time)
