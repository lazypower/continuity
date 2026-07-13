package config

import "fmt"

// Config holds all continuity configuration.
// Phase 0: types and defaults only. Phase 1 adds Load() with TOML parsing.
type Config struct {
	Server     ServerConfig     `toml:"server"`
	Database   DatabaseConfig   `toml:"database"`
	LLM        LLMConfig        `toml:"llm"`
	Hooks      HooksConfig      `toml:"hooks"`
	Extraction ExtractionConfig `toml:"extraction"`
}

type ServerConfig struct {
	Bind string `toml:"bind"`
	Port int    `toml:"port"`
}

type DatabaseConfig struct {
	Path string `toml:"path"`
}

type LLMConfig struct {
	Provider       string `toml:"provider"`    // "claude-cli", "anthropic", "ollama"
	Model          string `toml:"model"`       // e.g. "haiku", "sonnet"
	MergeModel     string `toml:"merge_model"` // model for merge decisions
	OllamaURL      string `toml:"ollama_url"`
	OllamaModel    string `toml:"ollama_model"`    // e.g. "llama3.2"
	EmbeddingModel string `toml:"embedding_model"` // e.g. "nomic-embed-text"
	AnthropicKey   string `toml:"anthropic_key"`
}

type HooksConfig struct {
	Enabled bool `toml:"enabled"`
	Timeout int  `toml:"timeout"` // seconds
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
			Provider:   "claude-cli",
			Model:      "haiku",
			MergeModel: "sonnet",
		},
		Hooks: HooksConfig{
			Enabled: true,
			Timeout: 120,
		},
		Extraction: ExtractionConfig{
			// Auto session extraction is off by default (deprecated, high-noise).
			Auto: false,
		},
	}
}

// ListenAddr returns the bind:port address string.
func (c *Config) ListenAddr() string {
	return fmt.Sprintf("%s:%d", c.Server.Bind, c.Server.Port)
}
