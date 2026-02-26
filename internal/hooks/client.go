package hooks

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	defaultServerURL = "http://127.0.0.1:37777"
	httpTimeout      = 5 * time.Second
)

// Client talks to the continuity server.
type Client struct {
	http      *http.Client
	serverURL string
}

// NewClient creates a new hook HTTP client.
// Respects CONTINUITY_URL env var, falls back to http://127.0.0.1:37777.
func NewClient() *Client {
	url := os.Getenv("CONTINUITY_URL")
	if url == "" {
		url = defaultServerURL
	}
	return &Client{
		http:      &http.Client{Timeout: httpTimeout},
		serverURL: url,
	}
}

// Post sends a POST request with JSON body. Returns response body.
func (c *Client) Post(path string, body []byte) ([]byte, error) {
	resp, err := c.http.Post(c.serverURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", path, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response %s: %w", path, err)
	}
	if resp.StatusCode >= 400 {
		return data, fmt.Errorf("POST %s: status %d: %s", path, resp.StatusCode, data)
	}
	return data, nil
}

// Get sends a GET request. Returns response body.
func (c *Client) Get(path string) ([]byte, error) {
	resp, err := c.http.Get(c.serverURL + path)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response %s: %w", path, err)
	}
	if resp.StatusCode >= 400 {
		return data, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, data)
	}
	return data, nil
}

// Healthy checks if the server is reachable.
func (c *Client) Healthy() bool {
	resp, err := c.http.Get(c.serverURL +"/api/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
