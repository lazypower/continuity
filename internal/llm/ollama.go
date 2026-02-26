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

// Ollama calls a local Ollama instance.
type Ollama struct {
	url    string
	model  string
	client *http.Client
}

// NewOllama creates a new Ollama client.
func NewOllama(url, model string) *Ollama {
	return &Ollama{
		url:    url,
		model:  model,
		client: &http.Client{Timeout: 120 * time.Second},
	}
}

// Complete sends a prompt to Ollama's generate endpoint.
func (o *Ollama) Complete(ctx context.Context, prompt string) (*Response, error) {
	reqBody := map[string]any{
		"model":  o.model,
		"prompt": prompt,
		"stream": false,
		"options": map[string]any{
			"temperature": 0.3,
			"num_predict": 2048,
		},
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", o.url+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama api: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ollama api status %d: %s", resp.StatusCode, respBody)
	}

	var result struct {
		Response string `json:"response"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &Response{
		Content:  result.Response,
		Provider: "ollama",
	}, nil
}
