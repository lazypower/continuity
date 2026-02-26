package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const anthropicAPI = "https://api.anthropic.com/v1/messages"

// Anthropic calls the Anthropic Messages API directly.
type Anthropic struct {
	apiKey  string
	model   string
	client  *http.Client
}

// NewAnthropic creates a new Anthropic API client.
func NewAnthropic(apiKey, model string) *Anthropic {
	return &Anthropic{
		apiKey: apiKey,
		model:  model,
		client: &http.Client{Timeout: 120 * time.Second},
	}
}

// Complete sends a prompt to the Anthropic API.
func (a *Anthropic) Complete(ctx context.Context, prompt string) (*Response, error) {
	reqBody := map[string]any{
		"model":       a.model,
		"max_tokens":  2048,
		"temperature": 0.3,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", anthropicAPI, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", a.apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := a.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("anthropic api: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("anthropic api status %d: %s", resp.StatusCode, respBody)
	}

	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	text := ""
	if len(result.Content) > 0 {
		text = result.Content[0].Text
	}

	return &Response{
		Content:    text,
		Provider:   "anthropic",
		TokensUsed: result.Usage.InputTokens + result.Usage.OutputTokens,
	}, nil
}
