package engine

// Decay logic lives in store/nodes.go (DecayAllNodes).
// This file documents the algorithm and provides the engine-level API.
//
// Smart Decay Algorithm:
//   - 90-day half-life without access
//   - Floor: 0.1 (memories never fully forgotten)
//   - Retrieval boosts: TouchNode resets relevance to 1.0
//   - Exempt: mem://user/profile/communication (relational profile)
//   - Computed in Go (not SQL) because modernc.org/sqlite lacks pow()
//   - Runs on server startup + daily via Engine.StartDecayTimer()
