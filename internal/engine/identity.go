package engine

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// canonicalIdentity maps a (model, dimensions) pair to a corpus-binding vector
// identity. Vectors are only comparable within the same identity.
//
// TF-IDF is special-cased to model-only: its output dimension is corpus-derived
// (the vocabulary size, which grows as memories are added), so binding the
// dimension would self-lock the fallback on normal corpus growth — tfidf:1 on a
// fresh DB, tfidf:N after a few writes. The dimension carries no identity signal
// for a corpus-derived embedder, so it is dropped. Stable embedders (real
// models) bind model:dims, which catches the cross-model and cross-dimension
// switches that are the actual failure mode.
func canonicalIdentity(model string, dims int) string {
	if model == "tfidf" {
		return "tfidf"
	}
	return fmt.Sprintf("%s:%d", model, dims)
}

// EmbedderIdentity is the corpus-binding identity of an embedder.
func EmbedderIdentity(emb Embedder) string {
	if emb == nil {
		return ""
	}
	return canonicalIdentity(emb.Model(), emb.Dimensions())
}

// embedderIfUnlocked returns the active embedder, or nil when the vector
// identity is locked. Embedding paths use this so that, while locked, new
// memories are still created but left Pending (no vector) rather than written
// into a vector space incompatible with the corpus.
func (e *Engine) embedderIfUnlocked() Embedder {
	if e.identityMismatch {
		return nil
	}
	return e.Embedder
}

// ActiveIdentity returns the identity of the engine's active embedder, or "" if
// none is configured. The health endpoint advertises this so doctor can compare
// against what the running server actually embeds with, rather than re-resolving
// a fresh embedder (which can differ from the live one).
func (e *Engine) ActiveIdentity() string {
	return EmbedderIdentity(e.Embedder)
}

// VectorIdentityStatus is the outcome of reconciling the active embedder against
// the corpus's declared vector identity at startup.
type VectorIdentityStatus struct {
	Active   string // identity of the active embedder ("" if none)
	Declared string // corpus's declared identity ("" if newly initialized)
	Match    bool   // active embedder is compatible with the corpus
	Action   string // human-readable summary of what reconciliation did
	Reason   string // when !Match, the lock reason (with repair guidance)
}

// ReconcileVectorIdentity binds the corpus to a vector identity and checks the
// active embedder against it, so the embedder can never be silently switched by
// environment — the root cause of the silent re-embed migration.
//
//   - No active embedder        -> no-op (search is already unavailable).
//   - Fresh corpus (no vectors,
//     no declared identity)      -> adopt the active embedder's identity.
//   - Declared absent, one stored
//     identity                   -> backfill the declaration from it (bind to the
//     corpus's truth, not whatever embedder is up).
//   - Declared absent, MULTIPLE
//     stored identities          -> LOCK: do not bless a majority; require repair.
//   - Active matches declared    -> Match; EmbedMissing may fill truly-missing vectors.
//   - Active differs             -> LOCK: do not re-embed, search fails closed, an
//     explicit snapshot-first repair is required.
//   - Reconciliation errors      -> LOCK (fail closed); never serve unproven.
//
// ReconcileVectorIdentity wraps the reconciliation logic with a fail-closed
// guarantee: if reconciliation ERRORS (e.g. the DB read/write fails), the engine
// is locked rather than left open, because an unproven embedder must never serve
// search against the stored corpus.
func (e *Engine) ReconcileVectorIdentity(ctx context.Context) (VectorIdentityStatus, error) {
	st, err := e.reconcileVectorIdentity(ctx)
	if err != nil {
		e.identityMismatch = true
		e.identityReason = "vector identity could not be reconciled (" + err.Error() +
			") — search disabled (fail closed). Run `continuity doctor`."
	}
	return st, err
}

func (e *Engine) reconcileVectorIdentity(ctx context.Context) (VectorIdentityStatus, error) {
	st := VectorIdentityStatus{Active: EmbedderIdentity(e.Embedder)}
	if e.Embedder == nil {
		st.Action = "no active embedder; vector identity not reconciled"
		return st, nil
	}

	declared, ok, err := e.DB.VectorIdentity()
	if err != nil {
		return st, err
	}

	if !ok {
		rows, err := e.DB.VectorModelCounts()
		if err != nil {
			return st, err
		}
		// Canonicalize stored vectors into identity buckets.
		buckets := map[string]int{}
		for _, r := range rows {
			buckets[canonicalIdentity(r.Model, r.Dimensions)] += r.Count
		}
		switch len(buckets) {
		case 0:
			// Fresh corpus: the active embedder defines the identity.
			if err := e.DB.SetVectorIdentity(st.Active); err != nil {
				return st, err
			}
			st.Declared, st.Match = st.Active, true
			st.Action = fmt.Sprintf("initialized corpus vector identity to %s", st.Active)
			return st, nil
		case 1:
			for id := range buckets {
				declared = id
			}
			if err := e.DB.SetVectorIdentity(declared); err != nil {
				return st, err
			}
			st.Action = fmt.Sprintf("backfilled corpus vector identity to %s (from stored vectors)", declared)
		default:
			// Mixed corpus: do NOT silently bless a majority — a prior interrupted
			// re-embed could have left a foreign identity in the lead. Fail closed
			// and require explicit repair.
			st.Match = false
			st.Reason = fmt.Sprintf(
				"corpus contains multiple vector identities (%s) — refusing to auto-pick one. "+
					"Run `continuity doctor` to inspect, then `continuity doctor --repair-vectors --apply` "+
					"(snapshot-first) to normalize the corpus to a single identity.",
				strings.Join(sortedKeys(buckets), ", "))
			st.Action = "LOCKED: mixed vector identities in corpus"
			e.identityMismatch, e.identityReason = true, st.Reason
			return st, nil
		}
	}
	st.Declared = declared

	if st.Active == declared {
		st.Match = true
		if st.Action == "" {
			st.Action = fmt.Sprintf("active embedder matches corpus vector identity (%s)", declared)
		}
		e.identityMismatch, e.identityReason = false, ""
		return st, nil
	}

	// Mismatch: lock. Never compare across vector spaces; never silently re-embed.
	st.Match = false
	st.Reason = fmt.Sprintf(
		"vector identity mismatch: the corpus was embedded with %s but the active embedder is %s. "+
			"Search is disabled to avoid comparing across vector spaces. Run `continuity doctor` to inspect, "+
			"then `continuity doctor --repair-vectors` (snapshot-first) to re-embed the corpus to %s.",
		declared, st.Active, st.Active)
	st.Action = "LOCKED: active embedder incompatible with corpus vector identity"
	e.identityMismatch, e.identityReason = true, st.Reason
	return st, nil
}

// sortedKeys returns a map's keys sorted, for deterministic messages.
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
