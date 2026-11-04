package cli

import (
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/config"
)

func clearServeEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{envServeDB, envServePort, envServeBind, envServeEmbedder, envServeExtractionAuto, envServeRelationalAuto} {
		t.Setenv(k, "")
	}
}

func TestApplyServeEnvOverrides_NoEnv(t *testing.T) {
	clearServeEnv(t)
	cfg := config.Default()
	want := cfg
	if err := applyServeEnvOverrides(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg != want {
		t.Errorf("expected cfg unchanged when no env set; got %+v", cfg)
	}
}

func TestApplyServeEnvOverrides_All(t *testing.T) {
	clearServeEnv(t)
	t.Setenv(envServeDB, "/tmp/test.db")
	t.Setenv(envServeBind, "0.0.0.0")
	t.Setenv(envServePort, "65432")

	cfg := config.Default()
	if err := applyServeEnvOverrides(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Database.Path != "/tmp/test.db" {
		t.Errorf("Database.Path = %q", cfg.Database.Path)
	}
	if cfg.Server.Bind != "0.0.0.0" {
		t.Errorf("Server.Bind = %q", cfg.Server.Bind)
	}
	if cfg.Server.Port != 65432 {
		t.Errorf("Server.Port = %d", cfg.Server.Port)
	}
}

func TestApplyServeEnvOverrides_PortInvalid(t *testing.T) {
	cases := []string{"abc", "-1", "70000", "  not_a_number  "}
	for _, in := range cases {
		clearServeEnv(t)
		t.Setenv(envServePort, in)
		cfg := config.Default()
		err := applyServeEnvOverrides(&cfg)
		if err == nil {
			t.Errorf("expected error for %s=%q; got nil", envServePort, in)
		}
	}
}

func TestApplyServeEnvOverrides_PortZeroAllowed(t *testing.T) {
	clearServeEnv(t)
	t.Setenv(envServePort, "0")
	cfg := config.Default()
	if err := applyServeEnvOverrides(&cfg); err != nil {
		t.Errorf("port 0 must be accepted so subprocess tests can request a kernel-assigned port: %v", err)
	}
	if cfg.Server.Port != 0 {
		t.Errorf("Server.Port = %d, want 0", cfg.Server.Port)
	}
}

func TestApplyServeEnvOverrides_WhitespaceIgnored(t *testing.T) {
	clearServeEnv(t)
	t.Setenv(envServeDB, "   ")
	t.Setenv(envServeBind, "   ")
	cfg := config.Default()
	want := cfg
	if err := applyServeEnvOverrides(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg != want {
		t.Errorf("whitespace-only env vars must be treated as unset; got %+v", cfg)
	}
}

// TestApplyServeEnvOverrides_RelationalAuto (#78): the kill switch flips the
// on-by-default relational profiling off; an invalid value fails fast.
func TestApplyServeEnvOverrides_RelationalAuto(t *testing.T) {
	clearServeEnv(t)
	cfg := config.Default()
	if !cfg.Extraction.RelationalAuto {
		t.Fatal("relational auto must default ON")
	}

	t.Setenv(envServeRelationalAuto, "false")
	if err := applyServeEnvOverrides(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Extraction.RelationalAuto {
		t.Error("CONTINUITY_RELATIONAL_AUTO=false must disable relational auto")
	}

	t.Setenv(envServeRelationalAuto, "not-a-bool")
	if err := applyServeEnvOverrides(&cfg); err == nil {
		t.Error("expected error for non-boolean CONTINUITY_RELATIONAL_AUTO")
	}
}

func TestNormalizeBackend(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"", "auto"},
		{"auto", "auto"},
		{"tfidf", "hashtf"},
		{"hashtf", "hashtf"},
		{"ollama", "ollama"},
		{"none", "none"},
		{"model2vec", "model2vec"},
	}
	for _, tc := range cases {
		if got := normalizeBackend(tc.in); got != tc.want {
			t.Errorf("normalizeBackend(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestNormalizeBackend_UnknownPassesThrough(t *testing.T) {
	// Unknown values pass through unchanged so selectEmbedder can detect and
	// warn on them rather than silently mis-selecting a known backend.
	if got := normalizeBackend("openai"); got != "openai" {
		t.Errorf("normalizeBackend(openai) = %q, want passthrough %q", got, "openai")
	}
}

// The env constants form a contract used by external automation; pin them.
func TestServeEnvConstants(t *testing.T) {
	cases := map[string]string{
		"CONTINUITY_DB":              envServeDB,
		"CONTINUITY_PORT":            envServePort,
		"CONTINUITY_BIND":            envServeBind,
		"CONTINUITY_EMBEDDER":        envServeEmbedder,
		"CONTINUITY_EXTRACTION_AUTO": envServeExtractionAuto,
		"CONTINUITY_RELATIONAL_AUTO": envServeRelationalAuto,
	}
	for want, got := range cases {
		if got != want {
			t.Errorf("env var name drifted: %q vs %q", got, want)
		}
	}
	// Defensive: ensure no overlap or typo collapses two distinct knobs into one.
	seen := map[string]bool{}
	for _, v := range []string{envServeDB, envServePort, envServeBind, envServeEmbedder} {
		if seen[v] {
			t.Errorf("duplicate env var name %q in serve env constants", v)
		}
		seen[v] = true
	}
	if !strings.HasPrefix(envServeDB, "CONTINUITY_") {
		t.Errorf("env vars must share the CONTINUITY_ prefix: %q", envServeDB)
	}
}
