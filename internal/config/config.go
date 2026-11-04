package config

import "fmt"

// Config holds all continuity configuration.
// Phase 0: types and defaults only. Phase 1 adds Load() with TOML parsing.
// Hook behavior is deliberately absent here. Hooks are configured where Claude
// Code reads them — ~/.claude/settings.json, or plugin/hooks/hooks.json — and a
// second, inert copy of those knobs in config.toml only teaches operators to
// tune a file nothing reads.
type Config struct {
	Server     ServerConfig     `toml:"server"`
	Database   DatabaseConfig   `toml:"database"`
	LLM        LLMConfig        `toml:"llm"`
	Extraction ExtractionConfig `toml:"extraction"`
	Embedder   EmbedderConfig   `toml:"embedder"`
}

type ServerConfig struct {
	Bind string `toml:"bind"`
	Port int    `toml:"port"`
}

type DatabaseConfig struct {
	Path string `toml:"path"`
}

type LLMConfig struct {
	Provider       string `toml:"provider"` // "claude-cli", "anthropic", "ollama"
	Model          string `toml:"model"`    // e.g. "haiku", "sonnet"
	OllamaURL      string `toml:"ollama_url"`
	OllamaModel    string `toml:"ollama_model"`    // e.g. "llama3.2"
	EmbeddingModel string `toml:"embedding_model"` // e.g. "nomic-embed-text"
	AnthropicKey   string `toml:"anthropic_key"`
}

// ExtractionConfig governs automatic memory extraction.
type ExtractionConfig struct {
	// Auto enables automatic session-end extraction — the Stop/SessionEnd hooks
	// asking an LLM to infer memories from the whole transcript. It defaults to
	// OFF because its usefulness is unmeasured and its writes are not provenance-
	// distinguishable from authored ones (nodes carry no source_kind): an
	// always-on, untrusted write path is not a safe default. Retained for explicit
	// opt-in and compatibility, and may be removed once there is evidence either
	// way. Explicit `continuity remember`, the signal ("remember this") path, and
	// `continuity extract --force` (the manual override) are all unaffected.
	Auto bool `toml:"auto"`

	// RelationalAuto enables automatic relational profiling at session end. It
	// defaults to ON, deliberately decoupled from Auto (#78): unlike transcript
	// memory extraction, relational profiling merges into a single system-owned
	// node (mem://user/profile/communication), never creates arbitrary memories,
	// and its provenance is unambiguous — analysis of the session, not facts
	// transiting it. CONTINUITY_RELATIONAL_AUTO=false is the kill switch.
	RelationalAuto bool `toml:"relational_auto"`
}

// EmbedderConfig governs which embedding backend `serve` constructs.
type EmbedderConfig struct {
	// Backend is one of "auto" (default), "model2vec", "ollama", or "hashtf".
	// "auto" means identity-aware selection: match the corpus's already-declared
	// vector identity if one exists, otherwise default to model2vec for a fresh
	// corpus. Any other value is a deliberate, explicit override — it wins over
	// the corpus's declared identity (see engine.ReconcileVectorIdentity's lock
	// behavior for what happens on mismatch). This is the persistent form of the
	// CONTINUITY_EMBEDDER escape hatch; the env var takes precedence over this
	// field when both are set, so it still works for one-off testing.
	//
	// The ONLY thing that migrates an existing corpus to a new backend is the
	// explicit `continuity embedder use <backend>` command — it writes this
	// field AND re-embeds. Hand-editing this field in config.toml changes what
	// `serve` selects on next boot, but does NOT migrate existing vectors: a
	// mismatch against the corpus's declared identity locks (fails closed)
	// exactly like the CONTINUITY_EMBEDDER env override does today.
	Backend string `toml:"backend"`
}

// Default returns a Config with sensible defaults.
func Default() Config {
	return Config{
		Server: ServerConfig{
			Bind: "127.0.0.1",
			Port: 37777,
		},
		Database: DatabaseConfig{
			Path: "", // resolved at runtime via store.DefaultDBPath()
		},
		LLM: LLMConfig{
			Provider: "claude-cli",
			Model:    "haiku",
		},
		Extraction: ExtractionConfig{
			// Auto session extraction is off by default (deprecated, high-noise).
			Auto: false,
			// Relational profiling stays on by default (#78).
			RelationalAuto: true,
		},
		Embedder: EmbedderConfig{
			Backend: "auto",
		},
	}
}

// ListenAddr returns the bind:port address string.
func (c *Config) ListenAddr() string {
	return fmt.Sprintf("%s:%d", c.Server.Bind, c.Server.Port)
}
