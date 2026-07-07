package engine

// Decay logic lives in store/nodes.go (DecayAllNodes).
// This file documents the algorithm and provides the engine-level API.
//
// Smart Decay Algorithm:
//   - 90-day half-life without use
//   - Floor: 0.1 (memories never fully forgotten)
//   - Use boosts: RecordUse (deliberate node fetch) resets relevance to 1.0;
//     search/list exposure never does (ADR-001 §2 — exposure is not use)
//   - Exempt: contract categories (profile/preferences/feedback — lifecycle
//     is merge + retraction, never the clock; ADR-001 §1) and moments
//   - Computed in Go (not SQL) because modernc.org/sqlite lacks pow()
//   - Runs on server startup + daily via Engine.StartDecayTimer()
