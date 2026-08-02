package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Path returns the default config file path: ~/.continuity/config.toml.
// Mirrors store.DefaultDBPath's shape for the sibling file in the same
// directory.
func Path() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".continuity", "config.toml"), nil
}

// Load reads config.toml from the default path and overlays it onto
// Default(). A missing file is not an error — it returns Default() unchanged,
// since config.toml is optional (every field has a working default).
//
// This is intentionally NOT a general TOML parser: continuity has no TOML
// dependency in go.mod and this project's guardrails forbid adding one for a
// single scalar field. LoadFile below reads exactly the subset of TOML this
// project actually writes (a flat `[section]` header followed by `key =
// "value"` or `key = bool` lines, one section deep, string/bool scalars
// only) — sufficient for every field Config declares today. If config.toml
// ever needs richer TOML (arrays, nested tables, multi-line strings), that is
// the trigger to reconsider a real TOML library rather than extending this
// further.
func Load() (Config, error) {
	path, err := Path()
	if err != nil {
		return Config{}, err
	}
	return LoadFile(path)
}

// LoadFile is Load with an explicit path, for tests.
func LoadFile(path string) (Config, error) {
	cfg := Default()
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	section := ""
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(line[1 : len(line)-1])
			continue
		}
		key, val, ok := splitKV(line)
		if !ok {
			continue // ignore anything we don't recognize rather than fail closed on it
		}
		applyKV(&cfg, section, key, val)
	}
	if err := sc.Err(); err != nil {
		return cfg, fmt.Errorf("read %s: %w", path, err)
	}
	return cfg, nil
}

// splitKV splits a "key = value" line, trims whitespace, and strips a single
// layer of surrounding double quotes from string values. ok is false if there
// is no '=' on the line.
func splitKV(line string) (key, val string, ok bool) {
	i := strings.Index(line, "=")
	if i < 0 {
		return "", "", false
	}
	key = strings.TrimSpace(line[:i])
	val = strings.TrimSpace(line[i+1:])
	if len(val) >= 2 && strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`) {
		val = val[1 : len(val)-1]
	}
	return key, val, true
}

// applyKV writes one recognized (section, key) into cfg. Unknown
// section/key pairs are silently ignored (forward-compatible with a
// hand-edited config.toml that has stray keys), matching Load's doc comment.
func applyKV(cfg *Config, section, key, val string) {
	switch section {
	case "server":
		switch key {
		case "bind":
			cfg.Server.Bind = val
		case "port":
			if n, err := parseIntStrict(val); err == nil {
				cfg.Server.Port = n
			}
		}
	case "database":
		if key == "path" {
			cfg.Database.Path = val
		}
	case "llm":
		switch key {
		case "provider":
			cfg.LLM.Provider = val
		case "model":
			cfg.LLM.Model = val
		case "ollama_url":
			cfg.LLM.OllamaURL = val
		case "ollama_model":
			cfg.LLM.OllamaModel = val
		case "embedding_model":
			cfg.LLM.EmbeddingModel = val
		case "anthropic_key":
			cfg.LLM.AnthropicKey = val
		}
	case "extraction":
		if key == "auto" {
			cfg.Extraction.Auto = parseBoolLoose(val)
		}
	case "embedder":
		if key == "backend" {
			cfg.Embedder.Backend = val
		}
	}
}

func parseBoolLoose(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == "true" || s == "1" || s == "yes"
}

func parseIntStrict(s string) (int, error) {
	n := 0
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty int")
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("not an int: %q", s)
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}

// SetEmbedderBackend writes (or updates in place) the [embedder].backend key
// in the config file at path, preserving every other line verbatim. It is the
// only writer of config.toml today — used exclusively by `continuity embedder
// use`, the sole migration verb (see EmbedderConfig.Backend's doc comment).
// Creates the file (and [embedder] section) if absent.
func SetEmbedderBackend(path, backend string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	existing, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read %s: %w", path, err)
	}

	lines := []string{}
	if len(existing) > 0 {
		lines = strings.Split(strings.TrimRight(string(existing), "\n"), "\n")
	}

	out := make([]string, 0, len(lines)+3)
	section := ""
	wroteKey := false
	sawEmbedderSection := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			// Leaving the embedder section without having written the key ->
			// insert it before this new section header.
			if section == "embedder" && !wroteKey {
				out = append(out, fmt.Sprintf("backend = %q", backend))
				wroteKey = true
			}
			section = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
			if section == "embedder" {
				sawEmbedderSection = true
			}
			out = append(out, line)
			continue
		}
		if section == "embedder" {
			if key, _, ok := splitKV(trimmed); ok && key == "backend" {
				out = append(out, fmt.Sprintf("backend = %q", backend))
				wroteKey = true
				continue
			}
		}
		out = append(out, line)
	}
	if section == "embedder" && !wroteKey {
		out = append(out, fmt.Sprintf("backend = %q", backend))
		wroteKey = true
	}
	if !sawEmbedderSection {
		if len(out) > 0 {
			out = append(out, "")
		}
		out = append(out, "[embedder]", fmt.Sprintf("backend = %q", backend))
	}

	content := strings.Join(out, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
