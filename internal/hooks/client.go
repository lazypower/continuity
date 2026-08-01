package hooks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	defaultBind = "127.0.0.1"
	defaultPort = "37777"

	// httpTimeout bounds hook calls. Hooks run inline in Claude Code's event
	// loop, so a slow server must not become a slow editor — 5s is a latency
	// budget, not a health signal.
	httpTimeout = 5 * time.Second

	// cliTimeout bounds interactive CLI calls. These block only a human at a
	// terminal, so they can afford to wait out a slow query rather than
	// reporting a perfectly healthy server as dead (issue #72).
	cliTimeout = 30 * time.Second
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

// NewCLIClient creates a client for interactive CLI use. Same target, longer
// patience — see cliTimeout.
func NewCLIClient() *Client {
	return &Client{
		http:      &http.Client{Timeout: cliTimeout},
		serverURL: ResolveServerURL(),
	}
}

// ServerURL returns the resolved base URL this client targets.
func (c *Client) ServerURL() string { return c.serverURL }

// timeout returns the client's configured request timeout.
func (c *Client) timeout() time.Duration { return c.http.Timeout }

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
	return c.CheckHealth() == nil
}

// CheckHealth probes the server and returns nil when it is reachable, or an
// error that says which failure actually occurred.
//
// The distinction is load-bearing. Collapsing every transport error into "not
// running" is what made issue #72 cost an afternoon: a healthy daemon answering
// correctly — but slower than the client timeout — reported as a dead one,
// sending the reporter after the port and the embedder while the real cause was
// an unbounded table. A timeout means the server IS there and IS answering; it
// just did not finish in time.
func (c *Client) CheckHealth() error {
	resp, err := c.http.Get(c.serverURL + "/api/health")
	if err != nil {
		if IsTimeout(err) {
			return fmt.Errorf("continuity server at %s is running but did not respond within %s — "+
				"this usually means the database has grown large; try: continuity prune",
				c.serverURL, c.timeout())
		}
		return fmt.Errorf("continuity server is not running — start it with: continuity serve")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("continuity server at %s is reachable but unhealthy (status %d)", c.serverURL, resp.StatusCode)
	}
	return nil
}

// DescribeError converts a request error from Post/Get into an operator-facing
// message, preserving the slow-vs-dead distinction that CheckHealth draws.
// Health can pass and a subsequent query still time out — /api/health answers
// instantly regardless of table size, which is precisely why it stayed green
// throughout issue #72.
func (c *Client) DescribeError(err error) error {
	if err == nil {
		return nil
	}
	if IsTimeout(err) {
		return fmt.Errorf("continuity server did not respond within %s — "+
			"the server is running, but the query is slow; try: continuity prune (original: %w)",
			c.timeout(), err)
	}
	return err
}

// IsTimeout reports whether err is a request timeout (client deadline or an
// underlying network timeout) rather than a connection failure.
func IsTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
