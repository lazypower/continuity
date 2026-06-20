package hooks

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultBind = "127.0.0.1"
	defaultPort = "37777"
	httpTimeout = 5 * time.Second
)

// Client talks to the continuity server.
type Client struct {
	http      *http.Client
	serverURL string
}

// ResolveServerURL is the single source of truth for which server URL the CLI
// and hooks target. It MUST stay in lockstep with serve's address resolution
// (CONTINUITY_BIND / CONTINUITY_PORT) so restart/inspection never probe a
// different endpoint than the one serve binds. Precedence:
//
//	CONTINUITY_URL (explicit, wins outright)
//	else http://<CONTINUITY_BIND|127.0.0.1>:<CONTINUITY_PORT|37777>
//
// Defaults are identical to the historical hardcoded http://127.0.0.1:37777
// when nothing is set.
func ResolveServerURL() string {
	if url := strings.TrimSpace(os.Getenv("CONTINUITY_URL")); url != "" {
		return url
	}
	bind := strings.TrimSpace(os.Getenv("CONTINUITY_BIND"))
	if bind == "" {
		bind = defaultBind
	}
	port := strings.TrimSpace(os.Getenv("CONTINUITY_PORT"))
	if port == "" {
		port = defaultPort
	}
	return fmt.Sprintf("http://%s:%s", bind, port)
}

// NewClient creates a new hook HTTP client targeting ResolveServerURL().
func NewClient() *Client {
	return &Client{
		http:      &http.Client{Timeout: httpTimeout},
		serverURL: ResolveServerURL(),
	}
}

// ServerURL returns the resolved base URL this client targets.
func (c *Client) ServerURL() string { return c.serverURL }

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
	resp, err := c.http.Get(c.serverURL + "/api/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
