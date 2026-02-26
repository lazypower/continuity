package llm

import "context"

// MockClient is a test double for the LLM Client interface.
// It can also be used for dry-run mode.
type MockClient struct {
	Response *Response
	Err      error
	Calls    []string // records prompts sent
}

// Complete records the call and returns the mock response.
func (m *MockClient) Complete(ctx context.Context, prompt string) (*Response, error) {
	m.Calls = append(m.Calls, prompt)
	return m.Response, m.Err
}
