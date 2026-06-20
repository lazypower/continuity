package hooks

import (
	"errors"
	"testing"
)

// withInjectedKillPath swaps the injectable side-effect hooks for the duration
// of a test and restores them after. signals records every pid that pidSignaller
// was asked to terminate, so a test can assert "no signal was sent". NOTHING here
// touches a real process.
type killPathHarness struct {
	signals  []int
	respawns int
}

func withInjectedKillPath(t *testing.T, h *killPathHarness, fetch func(*Client) (*HealthStatus, error), exeMatch func(int, string) (bool, error)) {
	t.Helper()
	origFetch, origExe, origSig, origResp := healthFetcher, exeMatcher, pidSignaller, serverRespawner
	t.Cleanup(func() {
		healthFetcher, exeMatcher, pidSignaller, serverRespawner = origFetch, origExe, origSig, origResp
	})
	healthFetcher = fetch
	exeMatcher = exeMatch
	pidSignaller = func(pid int) error {
		h.signals = append(h.signals, pid)
		return nil
	}
	serverRespawner = func() error {
		h.respawns++
		return nil
	}
}

func strongHealth(pid int) *HealthStatus {
	return &HealthStatus{Status: "ok", PID: pid, APIVersion: 1, SchemaHead: 7, Exe: "/usr/local/bin/continuity"}
}

// indeterminateExe simulates the macOS / non-Linux case: can't tell, no error.
func indeterminateExe(pid int, want string) (bool, error) { return false, nil }

// matchingExe simulates a confirmed /proc/<pid>/exe == health.exe.
func matchingExe(pid int, want string) (bool, error) { return true, nil }

// mismatchExe simulates a DEFINITE exe mismatch (refuse).
func mismatchExe(pid int, want string) (bool, error) {
	return false, errors.New("running exe /usr/bin/python != health-reported exe /usr/local/bin/continuity")
}

func TestConfirmAndBounce_SpoofedHealthRefusedNoSignal(t *testing.T) {
	// A process answering {"status":"ok","pid":N} with NO api_version/schema_head
	// is a spoof/legacy/non-continuity server. It must be refused and NO signal
	// sent.
	h := &killPathHarness{}
	spoof := &HealthStatus{Status: "ok", PID: 4242} // missing api_version/schema_head
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return spoof, nil },
		indeterminateExe,
	)
	err := ConfirmAndBounce(&Client{}, 4242)
	if err == nil {
		t.Fatal("expected refusal for spoofed health, got nil")
	}
	if len(h.signals) != 0 {
		t.Errorf("spoofed health must NOT be signalled; got signals %v", h.signals)
	}
	if h.respawns != 0 {
		t.Errorf("no respawn should occur on refusal; got %d", h.respawns)
	}
}

func TestConfirmAndBounce_StrongIdentityIndeterminateExePasses(t *testing.T) {
	// Strong field identity + same pid + indeterminate exe (macOS case) => proceed.
	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return strongHealth(4242), nil },
		indeterminateExe,
	)
	if err := ConfirmAndBounce(&Client{}, 4242); err != nil {
		t.Fatalf("strong identity should pass, got %v", err)
	}
	if len(h.signals) != 1 || h.signals[0] != 4242 {
		t.Errorf("expected exactly one signal to pid 4242, got %v", h.signals)
	}
	if h.respawns != 1 {
		t.Errorf("expected one respawn, got %d", h.respawns)
	}
}

func TestConfirmAndBounce_ExeMatchPasses(t *testing.T) {
	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return strongHealth(99), nil },
		matchingExe,
	)
	if err := ConfirmAndBounce(&Client{}, 99); err != nil {
		t.Fatalf("confirmed exe match should pass, got %v", err)
	}
	if len(h.signals) != 1 || h.signals[0] != 99 {
		t.Errorf("expected one signal to pid 99, got %v", h.signals)
	}
}

func TestConfirmAndBounce_ExeMismatchRefusedNoSignal(t *testing.T) {
	// A DEFINITE OS-level exe mismatch must refuse without signalling — the pid
	// has been reused by an unrelated binary.
	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return strongHealth(4242), nil },
		mismatchExe,
	)
	err := ConfirmAndBounce(&Client{}, 4242)
	if err == nil {
		t.Fatal("expected refusal on exe mismatch, got nil")
	}
	if len(h.signals) != 0 {
		t.Errorf("exe mismatch must NOT be signalled; got %v", h.signals)
	}
}

func TestConfirmAndBounce_RevalidationPidMismatchAborts(t *testing.T) {
	// TOCTOU: between the initial sample and the pre-signal re-fetch, the live pid
	// changed (the old process died, pid reused). Abort, never signal.
	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return strongHealth(5555), nil }, // live pid != expected
		matchingExe,
	)
	err := ConfirmAndBounce(&Client{}, 4242) // we intended to kill 4242
	if err == nil {
		t.Fatal("expected abort on pid mismatch, got nil")
	}
	if len(h.signals) != 0 {
		t.Errorf("pid mismatch must NOT be signalled; got %v", h.signals)
	}
}

func TestConfirmAndBounce_RevalidationDisappearedAborts(t *testing.T) {
	// The endpoint became unreadable before signalling => abort (could be a
	// reused pid now).
	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return nil, errors.New("connection refused") },
		matchingExe,
	)
	err := ConfirmAndBounce(&Client{}, 4242)
	if err == nil {
		t.Fatal("expected abort when health disappears, got nil")
	}
	if len(h.signals) != 0 {
		t.Errorf("must NOT signal when revalidation fails; got %v", h.signals)
	}
}

func TestConfirmAndBounce_RevalidationLostIdentityAborts(t *testing.T) {
	// Revalidation now returns something that no longer strongly identifies as
	// continuity (e.g. a different service grabbed the port). Abort.
	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return &HealthStatus{Status: "nginx"}, nil },
		matchingExe,
	)
	err := ConfirmAndBounce(&Client{}, 4242)
	if err == nil {
		t.Fatal("expected abort when revalidated identity is lost, got nil")
	}
	if len(h.signals) != 0 {
		t.Errorf("must NOT signal when identity is lost; got %v", h.signals)
	}
}

func TestConfirmAndBounce_InvalidPidRefused(t *testing.T) {
	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return strongHealth(1), nil },
		matchingExe,
	)
	if err := ConfirmAndBounce(&Client{}, 0); err == nil {
		t.Fatal("expected refusal for pid 0")
	}
	if len(h.signals) != 0 {
		t.Errorf("must NOT signal for invalid pid; got %v", h.signals)
	}
}

func TestIsContinuityServer(t *testing.T) {
	tests := []struct {
		name string
		hs   *HealthStatus
		want bool
	}{
		{"nil", nil, false},
		{"empty", &HealthStatus{}, false},
		{"spoof ok+pid only", &HealthStatus{Status: "ok", PID: 10}, false},
		{"legacy ok no fields", &HealthStatus{Status: "ok"}, false},
		{"missing schema_head", &HealthStatus{Status: "ok", PID: 10, APIVersion: 1}, false},
		{"missing api_version", &HealthStatus{Status: "ok", PID: 10, SchemaHead: 7}, false},
		{"wrong status", &HealthStatus{Status: "up", PID: 10, APIVersion: 1, SchemaHead: 7}, false},
		{"no pid", &HealthStatus{Status: "ok", APIVersion: 1, SchemaHead: 7}, false},
		{"strong", &HealthStatus{Status: "ok", PID: 10, APIVersion: 1, SchemaHead: 7}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsContinuityServer(tt.hs); got != tt.want {
				t.Errorf("IsContinuityServer() = %v, want %v", got, tt.want)
			}
		})
	}
}
