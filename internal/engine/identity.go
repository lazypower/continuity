package engine

import (
	"context"
	"fmt"
	"sort"
)

// EmbedderIdentity is the corpus-binding identity of an embedder: model + output
// dimension. Vectors are only comparable within the same identity. A finer
// fingerprint for corpus-derived embedders (e.g. a TF-IDF vocabulary hash) is a
// known refinement; model:dims already prevents the cross-model and
// cross-dimension silent switches that are the primary failure mode.
func EmbedderIdentity(emb Embedder) string {
	if emb == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", emb.Model(), emb.Dimensions())
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
//   - Declared identity absent
//     but vectors exist          -> backfill the declaration from the DOMINANT stored
//     identity (bind to the corpus's truth, not to
//     whatever embedder happens to be up), then compare.
//   - Active matches declared    -> Match; EmbedMissing may fill truly-missing vectors.
//   - Active differs             -> LOCK: do not re-embed, search fails closed, an
//     explicit snapshot-first repair is required.
func (e *Engine) ReconcileVectorIdentity(ctx context.Context) (VectorIdentityStatus, error) {
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
		counts, err := e.DB.VectorModelCounts()
		if err != nil {
			return st, err
		}
		if len(counts) == 0 {
			// Fresh corpus: the active embedder defines the identity.
			if err := e.DB.SetVectorIdentity(st.Active); err != nil {
				return st, err
			}
			st.Declared, st.Match = st.Active, true
			st.Action = fmt.Sprintf("initialized corpus vector identity to %s", st.Active)
			return st, nil
		}
		declared = dominantIdentity(counts)
		if err := e.DB.SetVectorIdentity(declared); err != nil {
			return st, err
		}
		st.Action = fmt.Sprintf("backfilled corpus vector identity to %s (from stored vectors)", declared)
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

// dominantIdentity returns the "model:dims" identity holding the most vectors,
// with a deterministic tiebreak so reconciliation is stable across runs.
func dominantIdentity(counts map[string]int) string {
	type kv struct {
		id string
		n  int
	}
	pairs := make([]kv, 0, len(counts))
	for id, n := range counts {
		pairs = append(pairs, kv{id, n})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].n != pairs[j].n {
			return pairs[i].n > pairs[j].n
		}
		return pairs[i].id < pairs[j].id
	})
	return pairs[0].id
}
