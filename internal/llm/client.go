package llm

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/lazypower/continuity/internal/config"
)

// Client is the interface for LLM providers.
type Client interface {
	Complete(ctx context.Context, prompt string) (*Response, error)
}

// Response holds the result of an LLM completion.
type Response struct {
	Content    string
	Provider   string
	TokensUsed int
}

// NewClient creates an LLM client based on the config provider setting.
func NewClient(cfg config.LLMConfig) (Client, error) {
	switch cfg.Provider {
	case "claude-cli":
		model := cfg.Model
		if model == "" {
			model = "haiku"
		}
		return NewClaudeCLI(model), nil
	case "anthropic":
		if cfg.AnthropicKey == "" {
			return nil, fmt.Errorf("anthropic provider requires ANTHROPIC_API_KEY or config")
		}
		model := cfg.Model
		if model == "" {
			model = "claude-haiku-4-5-20251001"
		}
		return NewAnthropic(cfg.AnthropicKey, model), nil
	case "ollama":
		url := cfg.OllamaURL
		if url == "" {
			url = "http://localhost:11434"
		}
		model := cfg.OllamaModel
		if model == "" {
			model = "llama3.2"
		}
		return NewOllama(url, model), nil
	default:
		return nil, fmt.Errorf("unknown LLM provider: %q", cfg.Provider)
	}
}

// ProviderBinaryUnresolved reports the external CLI binary a provider needs when
// that binary is NOT resolvable on the current $PATH, or "" when the provider
// needs no external binary (or its binary is present). It lets serve print one
// clear startup warning instead of a per-extraction failure buried in the log —
// the common service-managed case where launchd/systemd lacks the login PATH
// (issue #41). Providers that don't shell out (anthropic, ollama-over-HTTP)
// return "".
func ProviderBinaryUnresolved(cfg config.LLMConfig) string {
	if cfg.Provider != "claude-cli" {
		return ""
	}
	if _, err := exec.LookPath("claude"); err != nil {
		return "claude"
	}
	return ""
}
