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

// TestDescribeErrorPassesThroughNonTimeouts covers a real HTTP-level error from
// a server that IS up: the message is the server's own and must survive intact.
func TestDescribeErrorPassesThroughNonTimeouts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(srv.URL, time.Second)
	orig := errors.New("status 400: bad request")
	if got := c.DescribeError(orig); got != orig {
		t.Errorf("DescribeError rewrote a non-timeout error from a live server: %v", got)
	}
	if c.DescribeError(nil) != nil {
		t.Error("DescribeError(nil) should be nil")
	}
}

// TestDescribeErrorOnDeadDaemonIsActionable guards the regression Codex found
// when prune dropped its CheckHealth precondition: a connection-refused error is
// not a timeout, so without this the operator would see a raw
// "dial tcp ...: connection refused" instead of what to actually do about it.
func TestDescribeErrorOnDeadDaemonIsActionable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	url := "http://" + ln.Addr().String()
	ln.Close()

	c := newTestClient(url, 2*time.Second)
	_, postErr := c.Post("/api/prune", nil)
	if postErr == nil {
		t.Fatal("expected a dial error against a closed port")
	}
	described := c.DescribeError(postErr)
	if !strings.Contains(described.Error(), "continuity serve") {
		t.Errorf("dead-daemon error is not actionable: %q", described.Error())
	}
}

// TestHealthyDoesNotPayForTheDialProbe pins the hook-latency boundary. Hooks
// call Healthy() inline in Claude Code's event loop; a boolean gains nothing
// from slow-vs-dead, so it must not spend a second probe on top of an already
// elapsed timeout. Against an unroutable address the whole call must stay
// close to the client timeout, not timeout + dialProbeTimeout.
func TestHealthyDoesNotPayForTheDialProbe(t *testing.T) {
	c := newTestClient("http://203.0.113.1:37777", 200*time.Millisecond)

	start := time.Now()
	if c.Healthy() {
		t.Fatal("Healthy() = true against an unroutable address")
	}
	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond+dialProbeTimeout {
		t.Errorf("Healthy() took %s — it appears to be paying for the dial probe "+
			"on top of the request timeout, which slows every hook", elapsed)
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

// TestTimeoutAgainstNothingListeningIsStillDead guards the opposite-direction
// regression. IsTimeout alone does not prove a server exists: a black-holed
// CONTINUITY_URL, an unroutable host, or a DNS timeout all time out with no
// server involved. Claiming "running but slow" there is the same misdiagnosis
// as issue #72, just pointed the other way — so the claim requires a successful
// dial as positive evidence.
func TestTimeoutAgainstNothingListeningIsStillDead(t *testing.T) {
	// 203.0.113.0/24 is TEST-NET-3 (RFC 5737) — reserved for documentation and
	// guaranteed not to route, so connecting hangs until the deadline.
	c := newTestClient("http://203.0.113.1:37777", 100*time.Millisecond)

	err := c.CheckHealth()
	if err == nil {
		t.Fatal("CheckHealth returned nil against an unroutable address")
	}
	if !strings.Contains(err.Error(), "is not running") {
		t.Errorf("an unreachable server was reported as running-but-slow: %q", err.Error())
	}

	described := c.DescribeError(&timeoutError{})
	if strings.Contains(described.Error(), "the server is running") {
		t.Errorf("DescribeError asserted a server exists without evidence: %q", described.Error())
	}
}

// timeoutError is a minimal net.Error that reports as a timeout.
type timeoutError struct{}

func (*timeoutError) Error() string   { return "simulated i/o timeout" }
func (*timeoutError) Timeout() bool   { return true }
func (*timeoutError) Temporary() bool { return true }

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
