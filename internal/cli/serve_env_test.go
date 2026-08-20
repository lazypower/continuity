package cli

import (
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/config"
)

func clearServeEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{envServeDB, envServePort, envServeBind, envServeEmbedder, envServeGate, envServeGateTau} {
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

// The gate env override must fail fast on garbage and NEVER normalize an
// unrecognized value into a mode — least of all "on" (#80).
func TestApplyServeEnvOverrides_GateMode(t *testing.T) {
	for _, in := range []string{"off", "shadow", "on", "ON", " Shadow "} {
		clearServeEnv(t)
		t.Setenv(envServeGate, in)
		cfg := config.Default()
		if err := applyServeEnvOverrides(&cfg); err != nil {
			t.Errorf("%s=%q: unexpected error %v", envServeGate, in, err)
			continue
		}
		want := strings.ToLower(strings.TrimSpace(in))
		if cfg.Gate.Mode != want {
			t.Errorf("%s=%q: Gate.Mode = %q, want %q", envServeGate, in, cfg.Gate.Mode, want)
		}
	}
	for _, in := range []string{"yes", "true", "1", "inject", "onn"} {
		clearServeEnv(t)
		t.Setenv(envServeGate, in)
		cfg := config.Default()
		if err := applyServeEnvOverrides(&cfg); err == nil {
			t.Errorf("%s=%q: expected refusal, got mode %q", envServeGate, in, cfg.Gate.Mode)
		}
	}
}

func TestApplyServeEnvOverrides_GateTau(t *testing.T) {
	clearServeEnv(t)
	t.Setenv(envServeGateTau, "0.55")
	cfg := config.Default()
	if err := applyServeEnvOverrides(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Gate.Tau != 0.55 {
		t.Errorf("Gate.Tau = %v, want 0.55", cfg.Gate.Tau)
	}
	for _, in := range []string{"0", "-0.2", "1.5", "abc"} {
		clearServeEnv(t)
		t.Setenv(envServeGateTau, in)
		cfg := config.Default()
		if err := applyServeEnvOverrides(&cfg); err == nil {
			t.Errorf("%s=%q: expected refusal, got tau %v", envServeGateTau, in, cfg.Gate.Tau)
		}
	}
}

// The env constants form a contract used by external automation; pin them.
func TestServeEnvConstants(t *testing.T) {
	cases := map[string]string{
		"CONTINUITY_DB":       envServeDB,
		"CONTINUITY_PORT":     envServePort,
		"CONTINUITY_BIND":     envServeBind,
		"CONTINUITY_EMBEDDER": envServeEmbedder,
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
