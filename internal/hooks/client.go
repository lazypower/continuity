package hooks

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	serverURL   = "http://127.0.0.1:37777"
	httpTimeout = 5 * time.Second
)

// Client talks to the continuity server.
type Client struct {
	http *http.Client
}

// NewClient creates a new hook HTTP client.
func NewClient() *Client {
	return &Client{
		http: &http.Client{Timeout: httpTimeout},
	}
}

// Post sends a POST request with JSON body. Returns response body.
func (c *Client) Post(path string, body []byte) ([]byte, error) {
	resp, err := c.http.Post(serverURL+path, "application/json", bytes.NewReader(body))
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
	resp, err := c.http.Get(serverURL + path)
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
	resp, err := c.http.Get(serverURL + "/api/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
