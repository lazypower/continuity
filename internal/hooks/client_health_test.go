package hooks

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestClient points a Client at url with the given timeout.
func newTestClient(url string, timeout time.Duration) *Client {
	return &Client{http: &http.Client{Timeout: timeout}, serverURL: url}
}

// TestCheckHealthDistinguishesSlowFromDead is the regression test for issue #72.
// A server that is up and answering — just slower than the client timeout — must
// NOT be reported as "not running". That collapse is what sent the reporter
// after the port and the embedder while the real cause was an unbounded table.
func TestCheckHealthSlowServerIsNotReportedAsDead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := newTestClient(srv.URL, 50*time.Millisecond).CheckHealth()
	if err == nil {
		t.Fatal("CheckHealth returned nil for a server slower than the timeout")
	}
	msg := err.Error()
	if strings.Contains(msg, "is not running") {
		t.Errorf("slow server reported as dead: %q", msg)
	}
	if !strings.Contains(msg, "running but did not respond") {
		t.Errorf("message does not identify the timeout: %q", msg)
	}
	if !strings.Contains(msg, "continuity prune") {
		t.Errorf("message does not point at the actual remedy: %q", msg)
	}
}

func TestCheckHealthDeadServerIsReportedAsDead(t *testing.T) {
	// Bind and immediately release a port so nothing is listening on it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	url := "http://" + ln.Addr().String()
	ln.Close()

	err = newTestClient(url, 2*time.Second).CheckHealth()
	if err == nil {
		t.Fatal("CheckHealth returned nil against a closed port")
	}
	if !strings.Contains(err.Error(), "is not running") {
		t.Errorf("dead server not reported as dead: %q", err.Error())
	}
}

func TestCheckHealthHealthyServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := newTestClient(srv.URL, 2*time.Second).CheckHealth(); err != nil {
		t.Errorf("CheckHealth on a healthy server: %v", err)
	}
	if !newTestClient(srv.URL, 2*time.Second).Healthy() {
		t.Error("Healthy() = false for a healthy server")
	}
}

func TestCheckHealthUnhealthyStatusIsNotDead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := newTestClient(srv.URL, 2*time.Second).CheckHealth()
	if err == nil {
		t.Fatal("CheckHealth returned nil for a 500")
	}
	if strings.Contains(err.Error(), "is not running") {
		t.Errorf("a reachable-but-unhealthy server reported as dead: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "unhealthy") {
		t.Errorf("status not surfaced: %q", err.Error())
	}
}

// TestDescribeErrorOnSlowQuery covers the case health cannot catch: /api/health
// answers instantly regardless of table size, so a query can still time out
// after health passes.
func TestDescribeErrorOnSlowQuery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, 50*time.Millisecond)
	_, err := c.Get("/api/search?q=x")
	if err == nil {
		t.Fatal("expected a timeout from Get")
	}

	described := c.DescribeError(err)
	if !strings.Contains(described.Error(), "the server is running") {
		t.Errorf("DescribeError lost the slow-vs-dead distinction: %q", described.Error())
	}
	if !errors.Is(described, err) {
		t.Error("DescribeError dropped the wrapped original error")
	}
}

func TestDescribeErrorPassesThroughNonTimeouts(t *testing.T) {
	c := newTestClient("http://127.0.0.1:1", time.Second)
	orig := errors.New("status 400: bad request")
	if got := c.DescribeError(orig); got != orig {
		t.Errorf("DescribeError rewrote a non-timeout error: %v", got)
	}
	if c.DescribeError(nil) != nil {
		t.Error("DescribeError(nil) should be nil")
	}
}

func TestIsTimeout(t *testing.T) {
	if IsTimeout(errors.New("connection refused")) {
		t.Error("plain error classified as timeout")
	}
	if IsTimeout(nil) {
		t.Error("nil classified as timeout")
	}
	var netErr net.Error = &net.DNSError{IsTimeout: true}
	if !IsTimeout(netErr) {
		t.Error("net.Error with Timeout()=true not classified as timeout")
	}
}

// TestCLIClientIsMorePatientThanHookClient pins the split: hooks run inline in
// Claude Code's event loop and must stay fast, while a human at a terminal can
// wait out a slow query rather than being told the server is down.
func TestCLIClientIsMorePatientThanHookClient(t *testing.T) {
	hook := NewClient().timeout()
	cli := NewCLIClient().timeout()
	if cli <= hook {
		t.Errorf("CLI timeout (%s) must exceed hook timeout (%s)", cli, hook)
	}
}
