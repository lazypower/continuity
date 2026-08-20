package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFile_MissingFileReturnsDefault(t *testing.T) {
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "does-not-exist.toml"))
	if err != nil {
		t.Fatalf("LoadFile(missing): %v", err)
	}
	if cfg != Default() {
		t.Errorf("expected Default() when file is absent, got %+v", cfg)
	}
}

func TestLoadFile_EmbedderBackend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	content := "[embedder]\nbackend = \"model2vec\"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Embedder.Backend != "model2vec" {
		t.Errorf("Embedder.Backend = %q, want %q", cfg.Embedder.Backend, "model2vec")
	}
}

func TestLoadFile_DefaultBackendIsAuto(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	// A config file that exists but never mentions [embedder] at all.
	content := "[server]\nbind = \"0.0.0.0\"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Embedder.Backend != "auto" {
		t.Errorf("Embedder.Backend = %q, want default %q", cfg.Embedder.Backend, "auto")
	}
	if cfg.Server.Bind != "0.0.0.0" {
		t.Errorf("Server.Bind = %q, want %q (other sections must still parse)", cfg.Server.Bind, "0.0.0.0")
	}
}

// [gate] parsing (#80): only the three canonical modes are accepted, a typo
// resolves to the shadow default and can NEVER enable injection, and a
// garbage tau keeps the calibrated default.
func TestLoadFile_GateSection(t *testing.T) {
	cases := []struct {
		name, content, wantMode string
		wantTau                 float64
	}{
		{"explicit on", "[gate]\nmode = \"on\"\ntau = 0.55\n", GateOn, 0.55},
		{"explicit off", "[gate]\nmode = \"off\"\n", GateOff, DefaultGateTau},
		{"typo mode falls back to shadow", "[gate]\nmode = \"onn\"\n", GateShadow, DefaultGateTau},
		{"quoted tau parses", "[gate]\ntau = \"0.6\"\n", GateShadow, 0.6},
		{"tau out of range keeps default", "[gate]\ntau = 1.5\n", GateShadow, DefaultGateTau},
		{"tau garbage keeps default", "[gate]\ntau = \"lots\"\n", GateShadow, DefaultGateTau},
		{"absent section keeps defaults", "[server]\nbind = \"0.0.0.0\"\n", GateShadow, DefaultGateTau},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadFile(path)
			if err != nil {
				t.Fatalf("LoadFile: %v", err)
			}
			if cfg.Gate.Mode != tc.wantMode {
				t.Errorf("Gate.Mode = %q, want %q", cfg.Gate.Mode, tc.wantMode)
			}
			if cfg.Gate.Tau != tc.wantTau {
				t.Errorf("Gate.Tau = %v, want %v", cfg.Gate.Tau, tc.wantTau)
			}
		})
	}
}

func TestNormalizedGateMode(t *testing.T) {
	cases := map[string]string{
		"off": GateOff, "on": GateOn, "shadow": GateShadow,
		"": GateShadow, "ON": GateShadow, "inject": GateShadow, "true": GateShadow,
	}
	for in, want := range cases {
		if got := NormalizedGateMode(in); got != want {
			t.Errorf("NormalizedGateMode(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLoadFile_OtherFieldsRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	content := `[server]
bind = "127.0.0.1"
port = 9999

[database]
path = "/tmp/foo.db"

[llm]
provider = "ollama"
model = "sonnet"
ollama_url = "http://localhost:11434"
embedding_model = "nomic-embed-text"

[hooks]
enabled = false
timeout = 42

[extraction]
auto = true

[embedder]
backend = "ollama"
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Server.Bind != "127.0.0.1" || cfg.Server.Port != 9999 {
		t.Errorf("server section = %+v", cfg.Server)
	}
	if cfg.Database.Path != "/tmp/foo.db" {
		t.Errorf("database.path = %q", cfg.Database.Path)
	}
	if cfg.LLM.Provider != "ollama" || cfg.LLM.Model != "sonnet" {
		t.Errorf("llm section = %+v", cfg.LLM)
	}
	// The fixture still carries a [hooks] section. It is no longer a known
	// section — hooks are configured in ~/.claude/settings.json — and reaching
	// this line at all proves the retired keys are ignored rather than treated
	// as an error. An operator upgrading with the old block in config.toml must
	// not get a startup failure for it.
	if !cfg.Extraction.Auto {
		t.Error("extraction.auto = false, want true")
	}
	if cfg.Embedder.Backend != "ollama" {
		t.Errorf("embedder.backend = %q, want %q", cfg.Embedder.Backend, "ollama")
	}
}

func TestSetEmbedderBackend_CreatesFileWithSection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "config.toml")
	if err := SetEmbedderBackend(path, "model2vec"); err != nil {
		t.Fatalf("SetEmbedderBackend: %v", err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile after write: %v", err)
	}
	if cfg.Embedder.Backend != "model2vec" {
		t.Errorf("Embedder.Backend = %q, want %q", cfg.Embedder.Backend, "model2vec")
	}
}

func TestSetEmbedderBackend_UpdatesInPlacePreservingOtherSections(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	original := `[server]
bind = "0.0.0.0"
port = 1234

[embedder]
backend = "hashtf"

[llm]
provider = "ollama"
`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := SetEmbedderBackend(path, "ollama"); err != nil {
		t.Fatalf("SetEmbedderBackend: %v", err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Embedder.Backend != "ollama" {
		t.Errorf("Embedder.Backend = %q, want %q", cfg.Embedder.Backend, "ollama")
	}
	// Other sections must survive the rewrite untouched.
	if cfg.Server.Bind != "0.0.0.0" || cfg.Server.Port != 1234 {
		t.Errorf("server section clobbered: %+v", cfg.Server)
	}
	if cfg.LLM.Provider != "ollama" {
		t.Errorf("llm section clobbered: %+v", cfg.LLM)
	}
}

func TestSetEmbedderBackend_Idempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := SetEmbedderBackend(path, "model2vec"); err != nil {
		t.Fatal(err)
	}
	if err := SetEmbedderBackend(path, "model2vec"); err != nil {
		t.Fatal(err)
	}
	if err := SetEmbedderBackend(path, "ollama"); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Embedder.Backend != "ollama" {
		t.Errorf("Embedder.Backend = %q, want %q after repeated writes", cfg.Embedder.Backend, "ollama")
	}
	// No duplicate [embedder] sections or duplicate backend lines should have
	// accumulated.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, line := range splitLinesForTest(string(raw)) {
		if line == "[embedder]" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly one [embedder] section after repeated writes, found %d:\n%s", count, raw)
	}
}

func splitLinesForTest(s string) []string {
	var lines []string
	start := 0
	for i, r := range s {
		if r == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

func TestDefault_EmbedderBackendIsAuto(t *testing.T) {
	if Default().Embedder.Backend != "auto" {
		t.Errorf("Default().Embedder.Backend = %q, want %q", Default().Embedder.Backend, "auto")
	}
}
