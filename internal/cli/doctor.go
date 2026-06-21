package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/lazypower/continuity/internal/config"
	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/hooks"
	"github.com/lazypower/continuity/internal/store"
	"github.com/spf13/cobra"
)

var (
	doctorJSON   bool
	doctorRepair bool
	doctorApply  bool
)

var doctorCmd = &cobra.Command{
	Use:     "doctor",
	Aliases: []string{"diagnose"},
	Short:   "Diagnose memory index health (embedder/vector coherence)",
	Long: `Diagnose checks whether the stored embedding vectors are coherent with the
embedder the server actually runs. It is strictly read-only — it never writes,
re-embeds, or touches access metrics. Repair is a separate, explicit step.

Checks:
  - active embedder + expected vector dimension
  - stored vector model/dimension distribution
  - missing vectors (leaves with no embedding)
  - mixed-dimension vectors
  - stale vectors from an older embedder
  - a read-only retrieval smoke test (do nodes retrieve themselves?)`,
	RunE: runDoctor,
}

func init() {
	doctorCmd.Flags().BoolVar(&doctorJSON, "json", false, "Emit the report as JSON")
	doctorCmd.Flags().BoolVar(&doctorRepair, "repair-vectors", false, "Re-embed stale/missing vectors to the active embedder (dry-run unless --apply)")
	doctorCmd.Flags().BoolVar(&doctorApply, "apply", false, "With --repair-vectors: snapshot first, then actually re-embed")
}

// vectorGroup is one (model, dimensions) bucket of the stored corpus.
type vectorGroup struct {
	Model      string `json:"model"`
	Dimensions int    `json:"dimensions"`
	Count      int    `json:"count"`
}

// smokeResult summarizes the read-only self-retrieval probe.
type smokeResult struct {
	Sampled    int    `json:"sampled"`
	SelfFound  int    `json:"self_found"`
	TopK       int    `json:"top_k"`
	MedianRank int    `json:"median_rank"` // -1 when not applicable
	Note       string `json:"note,omitempty"`
}

// doctorReport is the full diagnosis. It carries no remediation — repair is a
// separate, explicit command.
type doctorReport struct {
	ActiveEmbedder   string `json:"active_embedder"`   // what doctor resolves now (CLI-side)
	DeclaredIdentity string `json:"declared_identity"` // corpus's bound vector identity
	ExpectedDims     int    `json:"expected_dimensions"`

	// What the RUNNING server actually embeds with (vs. doctor's fresh resolve).
	// Closes the fresh-resolve blind spot: doctor reporting "healthy" while the
	// live server serves a different vector space.
	ServerReachable      bool   `json:"server_reachable"`
	ServerActiveEmbedder string `json:"server_active_embedder"`
	ServerIdentityLocked bool   `json:"server_identity_locked"`

	TotalLeaves    int           `json:"total_leaves"`
	TotalVectors   int           `json:"total_vectors"`
	MissingVectors int           `json:"missing_vectors"`
	Distribution   []vectorGroup `json:"vector_distribution"`
	MixedDims      bool          `json:"mixed_dimensions"`
	StaleVectors   int           `json:"stale_vectors"`
	DimMismatch    int           `json:"dim_mismatch_vectors"`
	Smoke          smokeResult   `json:"retrieval_smoke_test"`
	Findings       []string      `json:"findings"`
	Healthy        bool          `json:"healthy"`
}

// serverIdentity is what the running server reports about its live embedder.
type serverIdentity struct {
	Reachable      bool
	ActiveEmbedder string
	Locked         bool
}

// fetchServerIdentity asks the running server what it actually embeds with, via
// /api/health. Best-effort: an unreachable server yields a zero value, and
// doctor falls back to its own fresh resolve.
func fetchServerIdentity() serverIdentity {
	data, err := hooks.NewClient().Get("/api/health")
	if err != nil {
		return serverIdentity{}
	}
	var h struct {
		ActiveEmbedder string `json:"active_embedder"`
		Locked         bool   `json:"vector_identity_locked"`
	}
	if err := json.Unmarshal(data, &h); err != nil {
		return serverIdentity{}
	}
	return serverIdentity{Reachable: true, ActiveEmbedder: h.ActiveEmbedder, Locked: h.Locked}
}

func runDoctor(cmd *cobra.Command, args []string) error {
	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	emb, err := resolveActiveEmbedder(db, config.Default())
	if err != nil {
		return fmt.Errorf("resolve embedder: %w", err)
	}

	if doctorRepair {
		return runDoctorRepair(db, emb, doctorApply)
	}

	leaves, err := db.ListLeaves()
	if err != nil {
		return fmt.Errorf("list leaves: %w", err)
	}
	vectors, err := db.AllVectors()
	if err != nil {
		return fmt.Errorf("all vectors: %w", err)
	}
	declared, _, _ := db.VectorIdentity()

	rep := buildDoctorReport(emb, leaves, vectors, declared, fetchServerIdentity())

	if doctorJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	printDoctorReport(rep)
	return nil
}

// runDoctorRepair re-embeds stale and missing vectors to the active embedder and
// rebinds the corpus vector identity. It snapshots first and is dry-run unless
// --apply is passed. Repair rewrites only derived vectors (mem_vectors) and the
// identity marker — never memory content — but a restore point is taken anyway,
// per data-safety-is-paramount.
func runDoctorRepair(db *store.DB, emb engine.Embedder, apply bool) error {
	if emb == nil {
		return fmt.Errorf("no active embedder; cannot repair (start Ollama with nomic-embed-text, or allow the TF-IDF fallback)")
	}
	activeID := engine.EmbedderIdentity(emb)

	// Include RETRACTED leaves: their vectors are still used by the
	// dedup-against-retracted gate, so leaving them in an old vector space after
	// rebinding the corpus identity would blind that gate (different dimension)
	// or feed it cross-space noise (same dimension).
	leaves, err := db.ListLeavesIncludingRetracted()
	if err != nil {
		return fmt.Errorf("list leaves: %w", err)
	}

	// A leaf needs (re-)embedding if it has no vector, or a vector under a
	// different model/dimension than the active embedder.
	var todo []store.MemNode
	for _, n := range leaves {
		if n.L0Abstract == "" {
			continue
		}
		v, err := db.GetVector(n.ID)
		if err != nil {
			return fmt.Errorf("get vector %s: %w", n.URI, err)
		}
		if v == nil || v.Model != emb.Model() || v.Dimensions != emb.Dimensions() {
			todo = append(todo, n)
		}
	}

	fmt.Printf("Repair plan: re-embed %d of %d leaves to identity %s\n", len(todo), len(leaves), activeID)
	if !apply {
		fmt.Println("\n[dry-run] No changes made. Re-run with --repair-vectors --apply to snapshot and repair.")
		return nil
	}

	snap, err := db.SnapshotNow("pre-repair-vectors")
	if err != nil {
		return fmt.Errorf("snapshot before repair: %w", err)
	}
	if snap != "" {
		fmt.Printf("Snapshot: %s\n", snap)
	}

	// Phase 1: embed everything FIRST, writing nothing. An embedding failure
	// (e.g. Ollama drops) then leaves the corpus completely untouched rather than
	// half-migrated.
	ctx := context.Background()
	type pendingWrite struct {
		id  int64
		vec []float64
	}
	writes := make([]pendingWrite, 0, len(todo))
	for i := range todo {
		vec, err := emb.Embed(ctx, todo[i].L0Abstract)
		if err != nil {
			return fmt.Errorf("embed %s: %w (no vectors were written; snapshot at %s)", todo[i].URI, err, snap)
		}
		writes = append(writes, pendingWrite{todo[i].ID, vec})
	}

	// Phase 2: commit the new vectors, then rebind the identity last so a
	// mid-write failure never leaves the identity pointing at a space the
	// vectors don't fully occupy.
	done := 0
	for _, w := range writes {
		if err := db.SaveVector(w.id, w.vec, emb.Model()); err != nil {
			return fmt.Errorf("save vector (wrote %d/%d; snapshot at %s): %w", done, len(writes), snap, err)
		}
		done++
	}
	if err := db.SetVectorIdentity(activeID); err != nil {
		return fmt.Errorf("set vector identity: %w", err)
	}

	fmt.Printf("Re-embedded %d nodes; corpus vector identity is now %s\n", done, activeID)
	fmt.Println("Restart the server to clear the identity lock: continuity restart")
	return nil
}

// resolveActiveEmbedder builds the embedder the server would use, by the same
// env/probe logic as `serve` (resolveEmbedderChoice + ProbeOllama), so doctor
// reports reality rather than a guess. Returns (nil, nil) for the "none"
// choice. Read-only.
func resolveActiveEmbedder(db *store.DB, cfg config.Config) (engine.Embedder, error) {
	ollamaURL := cfg.LLM.OllamaURL
	if ollamaURL == "" {
		ollamaURL = "http://localhost:11434"
	}
	embeddingModel := cfg.LLM.EmbeddingModel
	if embeddingModel == "" {
		embeddingModel = "nomic-embed-text"
	}

	switch resolveEmbedderChoice(ollamaURL, embeddingModel) {
	case "none":
		return nil, nil
	case "ollama":
		return engine.NewOllamaEmbedder(ollamaURL, embeddingModel, 768), nil
	case "tfidf":
		return engine.NewTFIDFEmbedder(db, 512)
	default: // auto: probe Ollama, fall back to TF-IDF
		if engine.ProbeOllama(ollamaURL, embeddingModel) {
			return engine.NewOllamaEmbedder(ollamaURL, embeddingModel, 768), nil
		}
		return engine.NewTFIDFEmbedder(db, 512)
	}
}

func buildDoctorReport(emb engine.Embedder, leaves []store.MemNode, vectors []store.VectorRecord, declared string, srv serverIdentity) doctorReport {
	rep := doctorReport{
		TotalLeaves:          len(leaves),
		TotalVectors:         len(vectors),
		ActiveEmbedder:       "none",
		DeclaredIdentity:     declared,
		ServerReachable:      srv.Reachable,
		ServerActiveEmbedder: srv.ActiveEmbedder,
		ServerIdentityLocked: srv.Locked,
	}
	activeModel := ""
	expectedDims := 0
	if emb != nil {
		rep.ActiveEmbedder = engine.EmbedderIdentity(emb)
		activeModel = emb.Model()
		expectedDims = emb.Dimensions()
	}
	rep.ExpectedDims = expectedDims

	// Tally distribution + stale/mismatch counts, keyed by (model, dimensions).
	type key struct {
		model string
		dims  int
	}
	groups := map[key]int{}
	vecByNode := make(map[int64]store.VectorRecord, len(vectors))
	dimsSeen := map[int]bool{}
	for _, v := range vectors {
		groups[key{v.Model, v.Dimensions}]++
		vecByNode[v.NodeID] = v
		dimsSeen[v.Dimensions] = true
		if emb != nil {
			if v.Model != activeModel {
				rep.StaleVectors++
			}
			if v.Dimensions != expectedDims {
				rep.DimMismatch++
			}
		}
	}
	for k, c := range groups {
		rep.Distribution = append(rep.Distribution, vectorGroup{Model: k.model, Dimensions: k.dims, Count: c})
	}
	sort.Slice(rep.Distribution, func(i, j int) bool {
		return rep.Distribution[i].Count > rep.Distribution[j].Count
	})
	rep.MixedDims = len(dimsSeen) > 1

	// Missing vectors: leaves with no embedding row.
	for _, n := range leaves {
		if _, ok := vecByNode[n.ID]; !ok {
			rep.MissingVectors++
		}
	}

	rep.Smoke = smokeTest(emb, leaves, vectors)
	rep.Findings, rep.Healthy = diagnose(rep)
	return rep
}

// smokeTest samples up to 10 leaves and checks whether each retrieves ITSELF in
// the top-K by raw cosine similarity against the stored vectors. It is strictly
// read-only: it embeds query text and scores in-memory, never calling
// Find/TouchNode, so it cannot pollute the access metrics it diagnoses. A low
// self-retrieval rate means the active embedder is incoherent with the stored
// vectors (e.g. a dimension mismatch makes cosine 0, so nothing matches).
func smokeTest(emb engine.Embedder, leaves []store.MemNode, vectors []store.VectorRecord) smokeResult {
	const topK = 5
	res := smokeResult{TopK: topK, MedianRank: -1}
	switch {
	case emb == nil:
		res.Note = "no embedder configured"
		return res
	case len(vectors) == 0:
		res.Note = "no vectors stored"
		return res
	}

	ctx := context.Background()
	var ranks []int
	probed := 0
	for _, n := range sampleLeaves(leaves, 10) {
		if n.L0Abstract == "" {
			continue
		}
		probed++
		qv, err := emb.Embed(ctx, n.L0Abstract)
		if err != nil {
			res.Note = "embed failed: " + err.Error()
			res.Sampled = probed
			return res
		}
		if rank := selfRank(qv, n.ID, vectors, topK); rank > 0 {
			res.SelfFound++
			ranks = append(ranks, rank)
		}
	}
	res.Sampled = probed
	if len(ranks) > 0 {
		sort.Ints(ranks)
		res.MedianRank = ranks[len(ranks)/2]
	}
	return res
}

// selfRank returns the 1-based rank of nodeID's own vector among the top-K most
// similar vectors to qv, or 0 if it falls outside top-K (or scores 0).
func selfRank(qv []float64, nodeID int64, vectors []store.VectorRecord, topK int) int {
	type scored struct {
		id  int64
		sim float64
	}
	ranked := make([]scored, 0, len(vectors))
	for _, v := range vectors {
		ranked = append(ranked, scored{v.NodeID, engine.CosineSimilarity(qv, v.Embedding)})
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].sim > ranked[j].sim })
	for i, s := range ranked {
		if i >= topK {
			break
		}
		if s.id == nodeID && s.sim > 0 {
			return i + 1
		}
	}
	return 0
}

// sampleLeaves returns up to n leaves spread evenly across the slice —
// deterministic, no randomness, stable across runs.
func sampleLeaves(leaves []store.MemNode, n int) []store.MemNode {
	if len(leaves) <= n {
		return leaves
	}
	out := make([]store.MemNode, 0, n)
	step := float64(len(leaves)) / float64(n)
	for i := 0; i < n; i++ {
		out = append(out, leaves[int(float64(i)*step)])
	}
	return out
}

// diagnose turns the raw counts into human-readable findings and a verdict.
func diagnose(rep doctorReport) ([]string, bool) {
	var f []string
	healthy := true

	if rep.ActiveEmbedder == "none" {
		f = append(f, "No embedder configured — semantic search is disabled.")
		healthy = false
	}
	if rep.MissingVectors > 0 {
		f = append(f, fmt.Sprintf("%d leaf node(s) have no embedding vector.", rep.MissingVectors))
		healthy = false
	}
	if rep.MixedDims {
		f = append(f, "Stored vectors have mixed dimensions — cosine similarity is meaningless across them.")
		healthy = false
	}
	if rep.ActiveEmbedder != "none" && rep.DimMismatch > 0 {
		f = append(f, fmt.Sprintf("%d vector(s) don't match the active embedder's dimension (%d) — they score 0 in search and are effectively invisible.", rep.DimMismatch, rep.ExpectedDims))
		healthy = false
	}
	if rep.ActiveEmbedder != "none" && rep.StaleVectors > 0 {
		f = append(f, fmt.Sprintf("%d vector(s) were embedded by a different model than the active embedder (%s).", rep.StaleVectors, rep.ActiveEmbedder))
		healthy = false
	}

	// Live-server identity — the fresh-resolve blind spot. doctor resolves its
	// own embedder; the running server may differ. Compare against what the
	// server actually reports.
	if rep.ServerReachable {
		if rep.ServerIdentityLocked {
			f = append(f, "The running server reports its vector identity is LOCKED — search is failing closed. Run `continuity doctor --repair-vectors --apply`, then `continuity restart`.")
			healthy = false
		}
		if rep.DeclaredIdentity != "" && rep.ServerActiveEmbedder != "" && rep.ServerActiveEmbedder != rep.DeclaredIdentity {
			f = append(f, fmt.Sprintf("The running server embeds with %s but the corpus identity is %s — restart the server (or repair) so they match.", rep.ServerActiveEmbedder, rep.DeclaredIdentity))
			healthy = false
		}
	}
	if rep.ActiveEmbedder != "none" && rep.DeclaredIdentity != "" && rep.ActiveEmbedder != rep.DeclaredIdentity {
		f = append(f, fmt.Sprintf("The active embedder (%s) differs from the corpus's declared identity (%s) — re-embedding (repair) is required to switch vector spaces.", rep.ActiveEmbedder, rep.DeclaredIdentity))
		healthy = false
	}

	switch {
	case rep.Smoke.Sampled > 0 && rep.Smoke.SelfFound == 0:
		f = append(f, fmt.Sprintf("Retrieval smoke test: 0/%d sampled nodes retrieved themselves — search is effectively broken against the active embedder.", rep.Smoke.Sampled))
		healthy = false
	case rep.Smoke.Sampled > 0 && rep.Smoke.SelfFound < rep.Smoke.Sampled:
		f = append(f, fmt.Sprintf("Retrieval smoke test: only %d/%d nodes retrieved themselves in top-%d.", rep.Smoke.SelfFound, rep.Smoke.Sampled, rep.Smoke.TopK))
	}

	if healthy && len(f) == 0 {
		f = append(f, "All checks passed — embedder and stored vectors are coherent.")
	}
	return f, healthy
}

func printDoctorReport(rep doctorReport) {
	dash := func(s string) string {
		if s == "" {
			return "(none)"
		}
		return s
	}

	fmt.Println("continuity doctor — memory index health")
	fmt.Println()
	fmt.Printf("  active embedder:    %s\n", rep.ActiveEmbedder)
	fmt.Printf("  declared identity:  %s\n", dash(rep.DeclaredIdentity))
	if rep.ServerReachable {
		locked := ""
		if rep.ServerIdentityLocked {
			locked = "  [LOCKED]"
		}
		fmt.Printf("  server embedder:    %s%s\n", dash(rep.ServerActiveEmbedder), locked)
	} else {
		fmt.Println("  server embedder:    (server not reachable)")
	}
	fmt.Printf("  expected dimension: %d\n", rep.ExpectedDims)
	fmt.Printf("  leaf nodes:         %d\n", rep.TotalLeaves)
	fmt.Printf("  stored vectors:     %d\n", rep.TotalVectors)
	fmt.Println()

	fmt.Println("  vector distribution:")
	if len(rep.Distribution) == 0 {
		fmt.Println("    (none)")
	}
	for _, g := range rep.Distribution {
		marker := "ok"
		if fmt.Sprintf("%s:%d", g.Model, g.Dimensions) != rep.ActiveEmbedder {
			marker = "!!"
		}
		fmt.Printf("    [%s] %-30s dim=%-5d %d\n", marker, g.Model, g.Dimensions, g.Count)
	}
	fmt.Println()

	fmt.Printf("  missing vectors:    %d\n", rep.MissingVectors)
	fmt.Printf("  mixed dimensions:   %v\n", rep.MixedDims)
	fmt.Printf("  stale vectors:      %d\n", rep.StaleVectors)
	fmt.Printf("  dim mismatch:       %d\n", rep.DimMismatch)
	fmt.Println()

	s := rep.Smoke
	if s.Note != "" {
		fmt.Printf("  retrieval smoke:    %s\n", s.Note)
	} else {
		fmt.Printf("  retrieval smoke:    %d/%d nodes retrieved themselves (top-%d", s.SelfFound, s.Sampled, s.TopK)
		if s.MedianRank > 0 {
			fmt.Printf(", median rank %d", s.MedianRank)
		}
		fmt.Println(")")
	}
	fmt.Println()

	if rep.Healthy {
		fmt.Println("  VERDICT: healthy")
	} else {
		fmt.Println("  VERDICT: degraded")
	}
	for _, f := range rep.Findings {
		fmt.Printf("    - %s\n", f)
	}
	if !rep.Healthy {
		fmt.Println()
		fmt.Println("  Repair is a separate, explicit step (snapshot-first, coming next):")
		fmt.Println("    continuity doctor --repair-vectors")
	}
}
