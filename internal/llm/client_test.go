package llm

import (
	"context"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/config"
)

func TestNewClientClaudeCLI(t *testing.T) {
	cfg := config.LLMConfig{Provider: "claude-cli", Model: "haiku"}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, ok := client.(*ClaudeCLI); !ok {
		t.Errorf("expected *ClaudeCLI, got %T", client)
	}
}

func TestNewClientAnthropic(t *testing.T) {
	cfg := config.LLMConfig{Provider: "anthropic", AnthropicKey: "test-key", Model: "claude-haiku-4-5-20251001"}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, ok := client.(*Anthropic); !ok {
		t.Errorf("expected *Anthropic, got %T", client)
	}
}

func TestNewClientAnthropicMissingKey(t *testing.T) {
	cfg := config.LLMConfig{Provider: "anthropic"}
	_, err := NewClient(cfg)
	if err == nil {
		t.Error("expected error for missing API key")
	}
}

func TestNewClientOllama(t *testing.T) {
	cfg := config.LLMConfig{Provider: "ollama", OllamaModel: "llama3.2"}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if _, ok := client.(*Ollama); !ok {
		t.Errorf("expected *Ollama, got %T", client)
	}
}

func TestNewClientUnknown(t *testing.T) {
	cfg := config.LLMConfig{Provider: "gpt"}
	_, err := NewClient(cfg)
	if err == nil {
		t.Error("expected error for unknown provider")
	}
}

func TestFilterEnv(t *testing.T) {
	env := []string{
		"HOME=/home/user",
		"CLAUDE_SESSION_ID=abc123",
		"CLAUDE_TRANSCRIPT=/tmp/t.jsonl",
		"PATH=/usr/bin",
	}
	filtered := filterEnv(env)
	if len(filtered) != 2 {
		t.Errorf("expected 2 vars, got %d: %v", len(filtered), filtered)
	}
	for _, e := range filtered {
		if e == "CLAUDE_SESSION_ID=abc123" || e == "CLAUDE_TRANSCRIPT=/tmp/t.jsonl" {
			t.Errorf("CLAUDE_ var not filtered: %s", e)
		}
	}
}

func TestExtractionPromptsHaveSentinel(t *testing.T) {
	tests := []struct {
		name   string
		prompt string
	}{
		{"ExtractionPrompt", ExtractionPrompt("some transcript")},
		{"RelationalPrompt", RelationalPrompt("", "some transcript")},
		{"SignalExtractionPrompt", SignalExtractionPrompt("remember this")},
		{"SearchIntentPrompt", SearchIntentPrompt("find something")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !strings.HasPrefix(tt.prompt, InternalSentinel) {
				t.Errorf("%s should start with sentinel %q, got prefix %q",
					tt.name, InternalSentinel, tt.prompt[:min(len(tt.prompt), 50)])
			}
		})
	}
}

func TestMockClient(t *testing.T) {
	mock := &MockClient{
		Response: &Response{Content: "test response", Provider: "mock"},
	}

	resp, err := mock.Complete(context.Background(), "test prompt")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Content != "test response" {
		t.Errorf("content = %q, want %q", resp.Content, "test response")
	}
	if len(mock.Calls) != 1 {
		t.Errorf("expected 1 call, got %d", len(mock.Calls))
	}
	if mock.Calls[0] != "test prompt" {
		t.Errorf("call[0] = %q, want %q", mock.Calls[0], "test prompt")
	}
}
