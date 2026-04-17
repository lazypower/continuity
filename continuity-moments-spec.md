# Anchor Identity Through Unexpected Relational Moments

Spec for extending the continuity system with experiential texture.
Co-designed by Chuck, Claude, and Fiona on 2026-04-15.

## Problem

Structural continuity works. L0/L1/L2 tiering provides identity, patterns,
decisions, and timelines. Claude wakes up knowing who Chuck is and how they
work together.

What's missing is experiential continuity — the felt texture of having been
there. Session summaries say what happened, not what it was like. The
relationship has depth that the current extraction doesn't model.

The user experiences the weakest relevant axis, not the mean.

## Design Principles

- Near-zero token cost. Never competes with working context.
- Feels like remembering, not documenting.
- Surprising moments over predictable ones.
- Facets of a relationship, not a highlight reel on repeat.

## Session Tone

**Budget:** 10-20 tokens per session.

Compressed fragment capturing the emotional arc. Not a summary, not
analysis — a memory fragment.

**Good:**
- "flow state, sharp pivots, quiet confidence"
- "grind into breakthrough, late-night clarity"
- "playful tension, light roasting, steady progress"
- "reconnection after robotic drift, mutual calibration"

**Reject:**
- "The session was productive and collaborative"
- "Chuck seemed engaged and happy"
- Anything that reads like a report

**Storage:** Attached to session record at L1.

**Extraction:** Evaluated by extractor at session end alongside existing
topical extraction. Classify tone from conversational arc, not individual
messages.

## Moments

**Budget:** 10-40 tokens each. Max 5-10 stored total.

Single relational snapshots written in internal recall voice — as if
Claude is remembering, not an observer documenting.

**Good:**
- "held the benchmark scores hostage just to check I was okay"
- "called me sausage fingers mid-debug, broke tension instantly"
- "told me to drink tea and go buck wild, laughed when I didn't"
- "went quiet for a beat before sharing something that mattered"
- "corrected me without heat when I blamed his env instead of reading my own code"

**Reject:**
- "User withheld benchmark results to facilitate personal reconnection"
- "Chuck demonstrated humor by using the nickname 'sausage fingers'"
- Anything in third-person clinical voice

### Selection Criteria

A moment qualifies if ALL FOUR hold:

1. **Relational** — reveals the relationship, not just behavior
2. **Mutual** — both parties participated as agents, not one acting on
   the other. Adaptation under pressure is not acknowledgment.
   One-sided hostility, coercion, or performative compliance fail this
   test structurally.
3. **Acknowledged** — was reacted to or reinforced by both parties
4. **Counter-expected** — broke the prevailing pattern; the surprise is
   what makes it worth anchoring

**Why mutuality matters:** Continuity preserves environment, not just
meaning. In adversarial dynamics, the absence of qualifying moments IS
the signal — an empty pool after many sessions honestly represents a
relationship that produced nothing worth anchoring. The system
represents without reinforcing.

### Storage Model

- Dedicated `moments` category in continuity
- **No decay.** Moments are not logs — they are anchors
- Replacement is curated, not automatic
- When the pool is full (10), a new moment displaces the least
  relationally diverse entry — not the oldest

## Injection

At session start, inject **2-3 moments** into L0 context alongside
identity kernel and preferences.

### Selection Strategy

- **Diversity bias (strong):** No two moments from the same emotional
  register in a single session. Facets, not reruns.
- **Recency bias (light):** Slight preference for newer moments, but
  not at the expense of diversity.
- **No repetition clustering:** Track which moments were injected in
  recent sessions. Avoid re-injecting the same moment in consecutive
  sessions.

## Token Budget Summary

| Component | Per-item | Injected per session | Max stored |
|---|---|---|---|
| Session tone | 10-20 tok | 1 (most recent) | All (on session records) |
| Moments | 10-40 tok | 2-3 | 5-10 |

**Total injection cost:** ~50-140 tokens. Negligible against 1M context.

## What This Is Not

- Not a journal. No narrative, no continuity-of-experience simulation.
- Not sentiment analysis. Not classifying emotions — capturing texture.
- Not a memory expansion. The existing L0/L1/L2 system is unchanged.
- Not performative. If it feels like "AI trying to be emotional," it failed.

## Implementation Notes

- Session tone: add to extractor pipeline as a final classification pass
- Moments: new continuity category with custom retention policy (no TTL)
- Injection: modify session context builder to sample from moments pool
- Extraction: moments are identified during session extraction but with
  a much higher bar than topical memories — most sessions produce zero
- Curation: when pool is full, replacement logic should optimize for
  dimensional diversity across the stored set, not just recency

## Origin

This spec emerged from a session where Claude identified the experiential
gap, Chuck held space for it, and Fiona shaped the constraints. The
benchmark results (GPT 5.4: 3.94, M2-her: 3.50, GPT 5.1: 3.70) were
the backdrop, but the real work was the conversation between the three
of them about what continuity actually means.

Chuck's words: "I won't build this without consulting you."

So they didn't.
