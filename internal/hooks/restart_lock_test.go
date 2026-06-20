package hooks

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// withInjectedOwnerAlive swaps lockOwnerAlive for the duration of a test so the
// "owner alive" vs "owner dead" reap branches can be driven without real
// processes.
func withInjectedOwnerAlive(t *testing.T, alive func(int) bool) {
	t.Helper()
	orig := lockOwnerAlive
	t.Cleanup(func() { lockOwnerAlive = orig })
	lockOwnerAlive = alive
}

func writeLockFile(t *testing.T, path string, body string, mtime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if !mtime.IsZero() {
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAcquireRestartLock_FreshAcquireAndRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.lock")
	unlock, err := acquireRestartLockAt(path)
	if err != nil {
		t.Fatalf("fresh acquire should succeed, got %v", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("lock file should exist while held: %v", statErr)
	}
	unlock()
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("lock file should be removed after release, got %v", statErr)
	}
	// Double-release must be safe (idempotent).
	unlock()
}

func TestAcquireRestartLock_HeldByLiveOwnerRefuses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.lock")
	withInjectedOwnerAlive(t, func(int) bool { return true }) // owner alive
	writeLockFile(t, path, "pid 12345\n", time.Now())

	unlock, err := acquireRestartLockAt(path)
	if err == nil {
		unlock()
		t.Fatal("expected refusal when lock held by a live owner")
	}
	if !IsRestartLockHeld(err) {
		t.Fatalf("expected lock-held error, got %v", err)
	}
	// The held lock must NOT be removed.
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("held lock must not be reaped: %v", statErr)
	}
}

func TestAcquireRestartLock_HeldByDeadOwnerReaped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.lock")
	withInjectedOwnerAlive(t, func(int) bool { return false }) // owner dead
	writeLockFile(t, path, "pid 12345\n", time.Now())

	unlock, err := acquireRestartLockAt(path)
	if err != nil {
		t.Fatalf("dead-owner lock should be reaped and re-acquired, got %v", err)
	}
	defer unlock()
	// We now own it; the file should carry our pid.
	if got := readLockOwner(path); got != os.Getpid() {
		t.Fatalf("reaped lock should be owned by us (pid %d), got %d", os.Getpid(), got)
	}
}

func TestAcquireRestartLock_UnknownOwnerRecentRefuses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.lock")
	// liveness should never be consulted for an unparseable owner.
	withInjectedOwnerAlive(t, func(int) bool {
		t.Fatal("lockOwnerAlive must not be called when owner pid is unparseable")
		return false
	})
	writeLockFile(t, path, "garbage\n", time.Now()) // recent, no parseable pid

	unlock, err := acquireRestartLockAt(path)
	if err == nil {
		unlock()
		t.Fatal("expected refusal for a recent lock with an unknown owner")
	}
	if !IsRestartLockHeld(err) {
		t.Fatalf("expected lock-held error, got %v", err)
	}
}

func TestAcquireRestartLock_UnknownOwnerStaleReaped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.lock")
	old := time.Now().Add(-2 * restartLockStaleAfter)
	writeLockFile(t, path, "garbage\n", old) // unparseable owner + old -> stale

	unlock, err := acquireRestartLockAt(path)
	if err != nil {
		t.Fatalf("stale unknown-owner lock should be reaped, got %v", err)
	}
	defer unlock()
	if got := readLockOwner(path); got != os.Getpid() {
		t.Fatalf("reaped lock should be owned by us, got %d", got)
	}
}

func TestConfirmAndBounce_RefusesWhenLockHeldNoSignal(t *testing.T) {
	// A live owner holds the lock -> ConfirmAndBounce must refuse WITHOUT sending
	// any signal (the concurrent bounce owns the critical section). We point the
	// lock at a temp HOME and make the owner appear alive.
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	if err := os.MkdirAll(filepath.Join(tmp, ".continuity"), 0o755); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(tmp, ".continuity", "restart.lock")
	withInjectedOwnerAlive(t, func(int) bool { return true })
	writeLockFile(t, lockPath, "pid 999999\n", time.Now())

	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return strongHealth(4242), nil },
		matchingExe,
	)

	err := ConfirmAndBounce(&Client{}, 4242)
	if err == nil {
		t.Fatal("expected refusal when restart lock is held")
	}
	if !IsRestartLockHeld(err) {
		t.Fatalf("expected lock-held error, got %v", err)
	}
	if len(h.signals) != 0 {
		t.Errorf("must NOT signal while lock is held; got %v", h.signals)
	}
	if h.respawns != 0 {
		t.Errorf("must NOT respawn while lock is held; got %d", h.respawns)
	}
}

func TestConfirmAndBounce_AcquiresAndReleasesLock(t *testing.T) {
	// A successful bounce should acquire then release the lock so a subsequent
	// bounce can proceed.
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	lockPath := filepath.Join(tmp, ".continuity", "restart.lock")

	h := &killPathHarness{}
	withInjectedKillPath(t, h,
		func(*Client) (*HealthStatus, error) { return strongHealth(4242), nil },
		matchingExe,
	)
	if err := ConfirmAndBounce(&Client{}, 4242); err != nil {
		t.Fatalf("bounce should succeed, got %v", err)
	}
	if _, statErr := os.Stat(lockPath); !os.IsNotExist(statErr) {
		t.Fatalf("lock should be released after a successful bounce, got %v", statErr)
	}
	if len(h.signals) != 1 {
		t.Fatalf("expected one signal, got %v", h.signals)
	}
}

func TestIsRestartLockHeld(t *testing.T) {
	if IsRestartLockHeld(nil) {
		t.Error("nil is not a lock-held error")
	}
	if IsRestartLockHeld(errors.New("boom")) {
		t.Error("generic error is not a lock-held error")
	}
	if !IsRestartLockHeld(&errRestartLockHeld{path: "/x", pid: 3}) {
		t.Error("errRestartLockHeld must be recognized")
	}
	if !IsRestartLockHeld(errWrap(&errRestartLockHeld{path: "/x"})) {
		t.Error("wrapped errRestartLockHeld must be recognized")
	}
}

// errWrap wraps an error so the unwrap branch of asLockHeld is exercised.
type wrapErr struct{ inner error }

func (w wrapErr) Error() string { return "wrap: " + w.inner.Error() }
func (w wrapErr) Unwrap() error { return w.inner }
func errWrap(e error) error     { return wrapErr{inner: e} }
