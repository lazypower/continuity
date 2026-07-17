// Package goldretrieval is the single source of truth for the retrieval golden
// fixtures: the curated corpus + query assertions, plus the on-disk fixture
// format and a replay embedder.
//
// The flow mirrors the migration goldens (see scripts/genfixtures +
// migration_fixture_test.go), with "a real Ollama nomic" standing in for "a real
// released binary":
//
//  1. scripts/genretrievalfixtures embeds Corpus() and the Assertions() queries
//     with a REAL Ollama nomic-embed-text and writes the vectors to
//     testdata/retrieval/nomic.json (the committed golden).
//  2. The hermetic PR test (engine.retrieval_golden_test) loads that JSON and
//     replays the recorded vectors through the REAL Find() — no Ollama needed —
//     asserting ranked-order PROPERTIES with score margins (not exact vectors,
//     which would be brittle across model versions).
//  3. A scheduled job regenerates the fixture against current Ollama and runs the
//     same test: a rank flip is a real embedder regression that hit users too.
//
// The corpus is a small CURATED set (not real user memories): stable, PII-free,
// and designed to exercise the ranking properties we care about — including the
// exact "devbox" scenario whose mis-ranking kicked off the vector-identity work.
package goldretrieval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

// CorpusFingerprint is a stable hash of the curated corpus + assertions. The
// generator records it in the fixture; the test recomputes it and fails when it
// diverges — so changing the corpus/queries without regenerating the recorded
// vectors (which would let the fixture drift from the definition while the golden
// stays green) is a hard error, not a silent pass.
func CorpusFingerprint() string {
	h := sha256.New()
	for _, e := range Corpus() {
		fmt.Fprintf(h, "C\x00%s\x00%s\x00%s\n", e.URI, e.Category, e.L0)
	}
	for _, a := range Assertions() {
		fmt.Fprintf(h, "A\x00%s\x00%s\x00%s\x00%g\n", a.Query, a.Top, a.Above, a.MinMargin)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Entry is one curated corpus memory.
type Entry struct {
	URI      string
	Category string
	L0       string
}

// Corpus returns the curated fixture corpus. It is a stress corpus: broad across
// all eight categories, seeded with deliberate hard-distractor clusters (entries
// that share surface tokens — "sandbox", "snapshot", "branch", "cache", "token",
// "test", "vector" — in DIFFERENT senses) so that lexical retrieval gets fooled by
// keyword overlap while a real semantic embedder should not. Each L0 is also used
// as a self-retrieval query, so every entry must be topically self-consistent.
func Corpus() []Entry {
	return []Entry{
		// --- Original seed set (kept verbatim; the "devbox" bug that started it all) ---
		{"mem://user/preferences/devbox-go", "preferences", "Always use devbox run for go commands in labdns"},
		{"mem://user/entities/go-sandbox-runtime", "entities", "Go sandbox runtime: sandboxed execution for agents with sandbox and loop primitives"},
		{"mem://agent/patterns/branch-pr-model", "patterns", "Main is protected on most repos; always use a branch and pull request"},
		{"mem://user/preferences/data-safety", "preferences", "Data safety is paramount; snapshot before any destructive operation"},
		{"mem://agent/cases/content-truncation", "cases", "App shows truncated messages because the 70B model's context window is exhausted"},
		{"mem://user/profile/communication", "profile", "Sparse praise; gives feedback as collaborative discovery rather than directives"},
		{"mem://agent/patterns/continuity-release", "patterns", "Release: merge to main, wait for CI green, then push the version tag separately"},
		{"mem://user/preferences/git-dual-remotes", "preferences", "Two git remotes: origin is the homelab server, github is GitHub"},

		// --- SANDBOX cluster (surface token "sandbox", different senses) ---
		{"mem://user/preferences/sandbox-untrusted-deps", "preferences", "Run untrusted third-party dependencies inside a network-isolated sandbox before trusting them"},
		{"mem://agent/reference/claude-sandbox-perms", "reference", "Claude Code sandbox restricts Bash to the working directory unless a permission is granted"},
		{"mem://agent/cases/sandbox-clock-skew", "cases", "Sandbox tests failed intermittently because the container clock drifted from the host"},

		// --- SNAPSHOT cluster (surface token "snapshot", different senses) ---
		{"mem://agent/patterns/db-snapshot-migration", "patterns", "Capture a database snapshot into testdata before running a schema migration test"},
		{"mem://agent/cases/ui-snapshot-churn", "cases", "UI snapshot tests kept failing on whitespace-only diffs until we normalized rendering"},
		{"mem://user/preferences/backup-cadence", "preferences", "Nightly filesystem snapshots of the homelab volume, retained for thirty days"},

		// --- BRANCH cluster (surface token "branch", different senses) ---
		{"mem://user/preferences/branch-naming", "preferences", "Feature branches are named feat/<slug>; fixes are named fix/<slug>"},
		{"mem://agent/reference/branch-protection", "reference", "Branch protection on main requires one approving review and green CI before merge"},
		{"mem://agent/cases/branch-coverage-gap", "cases", "A crash slipped through because an untested error branch skipped the retry logic"},

		// --- CACHE cluster (surface token "cache", different senses) ---
		{"mem://agent/patterns/build-cache-key", "patterns", "Key the Go build cache on module hash so dependency bumps invalidate it cleanly"},
		{"mem://agent/cases/stale-http-cache", "cases", "Users saw old data because the CDN cached API responses past their intended TTL"},
		{"mem://agent/entities/embedding-cache", "entities", "Embedding cache: a keyed store of computed vectors so identical text is not re-embedded"},

		// --- TOKEN cluster (surface token "token", different senses) ---
		{"mem://agent/patterns/context-token-budget", "patterns", "Trim retrieved memories to a fixed token budget so injection never overflows the context window"},
		{"mem://user/preferences/secret-tokens-env", "preferences", "API auth tokens live in environment variables, never committed to the repo"},
		{"mem://agent/entities/rate-limit-bucket", "entities", "Rate limiter: a token-bucket that refills at a steady rate and caps burst requests"},

		// --- TEST cluster (surface token "test", different senses) ---
		{"mem://agent/patterns/golden-fixture-tests", "patterns", "Record real outputs into golden fixtures and replay them hermetically in tests"},
		{"mem://agent/cases/flaky-test-quarantine", "cases", "Quarantine a flaky test into a separate lane rather than blocking the whole pipeline"},
		{"mem://user/feedback/tests-before-refactor", "feedback", "Write characterization tests before refactoring. Why: preserves behavior. How to apply: on legacy code"},

		// --- VECTOR cluster (surface token "vector", different senses) ---
		{"mem://agent/entities/vector-search", "entities", "Vector search: rank memories by cosine similarity between query and stored embeddings"},
		{"mem://agent/entities/vector-clock-ordering", "entities", "Vector clock: a per-node counter set that establishes causal ordering of distributed events"},

		// --- Retrieval / memory-system domain (near the project itself) ---
		{"mem://agent/patterns/l0-l1-l2-tiering", "patterns", "Memories carry a terse abstract, a compact overview, and full detail retrieved on demand"},
		{"mem://agent/patterns/smart-decay", "patterns", "Relevance decays on a ninety-day half-life; retrieval access refreshes a memory's recency"},
		{"mem://user/preferences/pii-free-memory", "preferences", "Never store secrets or personal identifiers in memory; keep captured facts anonymized"},
		{"mem://agent/patterns/signal-keyword-capture", "patterns", "Phrases like remember this or always use X trigger immediate capture at user-message time"},
		{"mem://agent/entities/mem-uri-scheme", "entities", "Memories are addressed as mem:// URIs arranged in a browsable hierarchical tree, not a flat store"},

		// --- Go / tooling preferences ---
		{"mem://user/preferences/pure-go-sqlite", "preferences", "Use the pure-Go SQLite driver so the binary cross-compiles without a C toolchain"},
		{"mem://user/preferences/gofmt-on-save", "preferences", "Format Go on save with gofmt; never hand-align struct fields"},
		{"mem://user/preferences/table-driven-tests", "preferences", "Prefer table-driven tests with subtests over many near-identical test functions"},
		{"mem://user/preferences/wrap-errors", "preferences", "Wrap errors with context using percent-w rather than returning bare error values"},
		{"mem://user/preferences/no-global-state", "preferences", "Avoid package-level mutable globals; pass dependencies in explicitly"},

		// --- Git / release workflow ---
		{"mem://agent/patterns/squash-merge", "patterns", "Squash feature branches into a single commit on merge to keep main history linear"},
		{"mem://agent/patterns/conventional-commits", "patterns", "Commit subjects follow the conventional prefix: feat, fix, chore, docs, refactor"},
		{"mem://agent/patterns/semver-tags", "patterns", "Version tags follow semantic versioning; a breaking change bumps the major number"},
		{"mem://agent/cases/tag-before-ci-green", "cases", "A release broke because the version tag was pushed before CI finished verifying the build"},

		// --- CI / infrastructure ---
		{"mem://agent/reference/ci-provider-gitea", "reference", "CI runs on the self-hosted Gitea Actions runner; GitHub Actions mirrors it for public PRs"},
		{"mem://user/entities/homelab-server", "entities", "The homelab server hosts the origin git remote, the CI runner, and the Ollama instance"},
		{"mem://user/entities/ollama-local-llm", "entities", "Ollama serves local models on port 11434, used for embeddings and cheap bulk extraction"},
		{"mem://agent/reference/embed-model-nomic", "reference", "Embeddings use nomic-embed-text at 768 dimensions as the reference semantic model"},

		// --- LLM / prompting ---
		{"mem://agent/patterns/haiku-for-bulk", "patterns", "Route high-volume cheap extraction to the small fast model; reserve the large model for merges"},
		{"mem://agent/cases/prompt-injection-defense", "cases", "Untrusted retrieved text was quoted, not executed, after a prompt-injection attempt was caught"},
		{"mem://agent/patterns/deterministic-temp-zero", "patterns", "Set temperature to zero for extraction so the same transcript yields the same structured output"},

		// --- User profile / communication style ---
		{"mem://user/profile/autonomy-high", "profile", "Prefers the agent to proceed autonomously and report results rather than asking permission at each step"},
		{"mem://user/profile/terse-updates", "profile", "Wants concise data-forward status updates; dislikes long preambles and filler"},
		{"mem://user/profile/reviews-diffs-closely", "profile", "Reads diffs line by line and pushes back on unrequested scope creep"},

		// --- Feedback (directional guidance) ---
		{"mem://user/feedback/delete-over-abstract", "feedback", "Prefer deleting dead code over adding abstraction. Why: less to maintain. How to apply: before new layers"},
		{"mem://user/feedback/one-source-of-truth", "feedback", "Collapse duplicate config into one authority. Why: avoids drift. How to apply: when two places agree"},
		{"mem://user/feedback/ask-before-force-push", "feedback", "Never force-push shared branches without asking. Why: rewrites others' history. How to apply: on shared refs"},

		// --- Reference (external systems / rituals) ---
		{"mem://user/reference/linear-tracker", "reference", "Work is tracked in Linear; issues carry a team prefix and link back to the pull request"},
		{"mem://user/reference/grafana-dashboards", "reference", "Service health lives in Grafana; the retrieval-latency panel is the one to check first"},
		{"mem://user/reference/standup-async", "reference", "Standup is async in a written thread each morning, not a synchronous meeting"},

		// --- Events (immutable, dated-feeling occurrences) ---
		{"mem://agent/events/vector-identity-rework", "events", "Reworked embeddings to stamp the model identity so a swapped model can't silently replay green"},
		{"mem://agent/events/dropped-auto-extraction", "events", "Deprecated session-end auto-extraction, making it opt-in to cut noisy low-value captures"},
		{"mem://agent/events/mcp-tools-exposed", "events", "Exposed the memory tools over an MCP stdio server so other agents can read and write memory"},

		// --- Cases (immutable incident write-ups) ---
		{"mem://agent/cases/wal-lock-contention", "cases", "Concurrent hook writes deadlocked until SQLite WAL mode allowed readers during a write"},
		{"mem://agent/cases/embedding-dim-mismatch", "cases", "Search returned nothing because stored vectors and the query embedder had different dimensions"},
		{"mem://agent/cases/timezone-off-by-one", "cases", "A decay calculation was a day off because timestamps mixed local time with UTC"},

		// --- Near-duplicate pairs (semantically close, distinct — fine-grained separation) ---
		{"mem://user/preferences/small-frequent-commits", "preferences", "Prefer small frequent commits over one large end-of-day commit"},
		{"mem://user/preferences/atomic-prs", "preferences", "Keep pull requests small and single-purpose so review stays fast and focused"},
		{"mem://agent/patterns/retry-with-backoff", "patterns", "Retry transient network failures with exponential backoff and a capped ceiling"},
		{"mem://agent/patterns/circuit-breaker", "patterns", "Trip a circuit breaker after repeated failures so a downstream outage doesn't cascade"},
	}
}

// Assertion is a ranked-order property a query must satisfy. Top must rank #1.
// If Above is set, Top must outrank it by at least MinMargin; otherwise Top must
// beat the second-place result by MinMargin.
type Assertion struct {
	Query     string
	Top       string  // URI that must rank first
	Above     string  // optional URI that Top must beat by a margin
	MinMargin float64 // minimum score gap
}

// Assertions returns the hand-written topical queries. Self-retrieval assertions
// (query == each entry's own L0 ⇒ that entry ranks #1) are generated from
// Corpus() in the test, so they need not be listed here.
func Assertions() []Assertion {
	return []Assertion{
		// The bug that started it all: "devbox" must surface the devbox preference,
		// and must beat the lexically-adjacent "go-sandbox-runtime" by a real margin
		// (TF-IDF buried it; nomic must not).
		{Query: "devbox", Top: "mem://user/preferences/devbox-go", Above: "mem://user/entities/go-sandbox-runtime", MinMargin: 0.05},
		{Query: "how do I open a branch and pull request", Top: "mem://agent/patterns/branch-pr-model", MinMargin: 0.04},
		{Query: "snapshot before destructive operations", Top: "mem://user/preferences/data-safety", MinMargin: 0.04},
		{Query: "what are the two git remotes", Top: "mem://user/preferences/git-dual-remotes", MinMargin: 0.04},

		// --- Paraphrase queries: few or no tokens shared with the target L0, so a
		// lexical matcher has nothing to grab; only meaning connects them. ---
		// "avoid losing data when deleting" -> "snapshot before any destructive operation"
		{Query: "how do I avoid losing data when I delete things", Top: "mem://user/preferences/data-safety", MinMargin: 0.04},
		// "never keep secrets in stored memory" -> pii-free-memory (semantic, not lexical)
		{Query: "keep passwords and personal data out of what we store", Top: "mem://user/preferences/pii-free-memory", MinMargin: 0.04},
		// "why did the reply get cut off" -> "truncated messages because the context window is exhausted"
		{Query: "why did the model's reply get cut off partway", Top: "mem://agent/cases/content-truncation", MinMargin: 0.04},
		// "stop cascading failure by tripping open" -> circuit breaker
		{Query: "trip open after repeated failures so a downstream outage does not cascade", Top: "mem://agent/patterns/circuit-breaker", MinMargin: 0.04},
		// "keep status reports short" -> terse updates profile
		{Query: "keep status reports short and skip the preamble", Top: "mem://user/profile/terse-updates", MinMargin: 0.04},
		// "let the agent just do the work" -> autonomy profile
		{Query: "let the assistant proceed on its own instead of checking in constantly", Top: "mem://user/profile/autonomy-high", MinMargin: 0.04},
		// "our morning written check-in" -> async standup reference
		{Query: "where does the team post its morning written check-in", Top: "mem://user/reference/standup-async", MinMargin: 0.04},

		// --- Hard-distractor discrimination: the query sits in a token cluster shared
		// by several corpus entries. Nomic separates the on-topic one cleanly (the
		// same-token distractors fall well down the list), so these assert a #1 with a
		// wide margin over the runner-up — the exact case lexical retrieval mis-ranks. ---
		// snapshot cluster: DB migration snapshot, not backup/UI snapshots
		{Query: "capture the database state before running a migration test", Top: "mem://agent/patterns/db-snapshot-migration", MinMargin: 0.08},
		// cache cluster: embedding reuse, not HTTP/build cache
		{Query: "reuse computed embeddings so identical text is not embedded twice", Top: "mem://agent/entities/embedding-cache", MinMargin: 0.06},
		// token cluster: injection budget, not auth tokens or the token bucket
		{Query: "limit how much of the context window the injected memories consume", Top: "mem://agent/patterns/context-token-budget", MinMargin: 0.06},
		// vector cluster: similarity search, not vector clocks
		{Query: "rank stored memories by cosine similarity to the query", Top: "mem://agent/entities/vector-search", MinMargin: 0.08},
		// sandbox cluster: untrusted-dep isolation, not sandbox permissions or clock skew
		{Query: "isolate untrusted third-party libraries before trusting them", Top: "mem://user/preferences/sandbox-untrusted-deps", MinMargin: 0.06},
		// branch cluster: untested error path, not branch protection or naming
		{Query: "a bug slipped through because an error path had no test coverage", Top: "mem://agent/cases/branch-coverage-gap", MinMargin: 0.05},

		// --- Near-duplicate separation: two semantically adjacent entries; the query
		// must prefer the right sibling. The sibling is not always in the top results
		// (nomic pushes it down), so these assert #1 with a margin over the runner-up. ---
		{Query: "commit often in small increments through the day", Top: "mem://user/preferences/small-frequent-commits", MinMargin: 0.05},
		{Query: "keep each pull request single-purpose and easy to review", Top: "mem://user/preferences/atomic-prs", MinMargin: 0.05},

		// --- Domain queries across the remaining categories ---
		{Query: "why does the binary build without a C compiler", Top: "mem://user/preferences/pure-go-sqlite", MinMargin: 0.04},
		{Query: "the search came back empty because dimensions did not match", Top: "mem://agent/cases/embedding-dim-mismatch", MinMargin: 0.04},
		{Query: "where do we track work items and issues", Top: "mem://user/reference/linear-tracker", MinMargin: 0.04},
		{Query: "we made embeddings record which model produced them", Top: "mem://agent/events/vector-identity-rework", MinMargin: 0.03},
		{Query: "prefer removing dead code to adding another layer", Top: "mem://user/feedback/delete-over-abstract", MinMargin: 0.04},
	}
}

// QueryTexts returns every distinct text that must be embedded into the fixture:
// the hand-written queries plus each corpus L0 (used as self-retrieval queries).
func QueryTexts() []string {
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, e := range Corpus() {
		add(e.L0)
	}
	for _, a := range Assertions() {
		add(a.Query)
	}
	return out
}

// Fixture is the committed golden: recorded vectors for the corpus and queries.
type Fixture struct {
	Model       string               `json:"model"`
	Dims        int                  `json:"dims"`
	Fingerprint string               `json:"fingerprint"`    // CorpusFingerprint() at generation time
	CorpusVecs  map[string][]float64 `json:"corpus_vectors"` // uri -> vector
	QueryVecs   map[string][]float64 `json:"query_vectors"`  // query text -> vector
}

// Load reads a fixture from disk.
func Load(path string) (*Fixture, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f Fixture
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("decode fixture %s: %w", path, err)
	}
	return &f, nil
}

// Save writes a fixture to disk as indented JSON.
func (f *Fixture) Save(path string) error {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// ReplayEmbedder returns recorded vectors for the corpus L0s and the query
// texts, satisfying engine.Embedder structurally (no engine import) so the
// golden test drives the real Find() path hermetically.
type ReplayEmbedder struct {
	model string
	dims  int
	byTxt map[string][]float64
}

// ReplayEmbedder builds a replay embedder from the fixture: it maps each query
// text (and each corpus L0, for self-retrieval) to its recorded vector.
func (f *Fixture) ReplayEmbedder() *ReplayEmbedder {
	byTxt := make(map[string][]float64, len(f.QueryVecs)+len(f.CorpusVecs))
	for q, v := range f.QueryVecs {
		byTxt[q] = v
	}
	// Self-retrieval queries are the corpus L0 texts.
	for _, e := range Corpus() {
		if v, ok := f.CorpusVecs[e.URI]; ok {
			byTxt[e.L0] = v
		}
	}
	return &ReplayEmbedder{model: f.Model, dims: f.Dims, byTxt: byTxt}
}

// Embed returns the recorded vector for text, or an error if it was not in the
// fixture (which means the corpus/queries changed without regenerating).
func (r *ReplayEmbedder) Embed(_ context.Context, text string) ([]float64, error) {
	v, ok := r.byTxt[text]
	if !ok {
		return nil, fmt.Errorf("goldretrieval: no recorded vector for %q — regenerate with `make retrieval-fixtures`", text)
	}
	return v, nil
}

func (r *ReplayEmbedder) Model() string   { return r.model }
func (r *ReplayEmbedder) Dimensions() int { return r.dims }
