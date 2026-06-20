package hooks

import (
	"fmt"
	"runtime"
)

// The kill path (re-validate identity, best-effort OS exe match, then signal +
// respawn) is the single most dangerous thing this tool does: it sends a
// terminating signal to a pid. Everything here is factored behind function vars
// so tests can inject side effects and assert "no signal was sent" without ever
// touching a real process. Production wiring lives in kill_path_unix.go /
// kill_path_windows.go.
//
// The overarching safety principle: this is a single-user localhost tool, so the
// threat is ACCIDENTAL collision (another local process squatting the port, pid
// reuse after the real server died), never a malicious forge. We therefore
// confirm identity hard, re-confirm immediately before signalling, and refuse on
// any definite mismatch — but we don't build a supervisor framework.

// healthFetcher re-fetches /api/health for TOCTOU revalidation. Injectable.
var healthFetcher = func(c *Client) (*HealthStatus, error) {
	return c.Status()
}

// exeMatcher performs a best-effort OS-level check that the live pid's executable
// matches the exe the health payload advertised. It returns:
//
//	match=true,  err=nil  -> confirmed same executable (strong signal to proceed)
//	match=false, err=nil  -> INDETERMINATE (OS can't tell; e.g. macOS) — caller
//	                         falls back to strong-field identity + revalidation
//	match=false, err!=nil -> DEFINITE MISMATCH — caller MUST refuse to signal
//
// Injectable so tests drive all three outcomes without a real /proc.
var exeMatcher = func(pid int, wantExe string) (bool, error) {
	return osExeMatch(pid, wantExe)
}

// pidSignaller stops a confirmed pid (SIGTERM, escalate to SIGKILL). Injectable.
var pidSignaller = func(pid int) error {
	return osStopPID(pid)
}

// serverRespawner relaunches a detached serve. Injectable.
var serverRespawner = func() error {
	_, err := SpawnDetachedServe()
	return err
}

// ConfirmKillTarget runs the full pre-signal safety gate against the server the
// client targets, for an intended-kill of expectPID. It performs NO signalling —
// it only decides whether signalling expectPID is safe. On success it returns the
// (revalidated) health. On any doubt it returns an error and the caller MUST NOT
// signal.
//
// Steps (in order, all must pass):
//  1. Re-fetch /api/health (TOCTOU): the process we sampled earlier may have died
//     and the pid been reused since.
//  2. The live health must still pass IsContinuityServer (strong field identity).
//  3. The live pid must be the SAME pid we intend to signal — if it changed, the
//     old process is gone; abort rather than chase a moving target.
//  4. Best-effort OS exe match: a DEFINITE mismatch refuses; an indeterminate
//     result is acceptable because (2)+(3) already establish strong identity.
func ConfirmKillTarget(c *Client, expectPID int) (*HealthStatus, error) {
	if expectPID <= 0 {
		return nil, fmt.Errorf("invalid target pid %d", expectPID)
	}

	live, err := healthFetcher(c)
	if err != nil {
		return nil, fmt.Errorf("re-validate before signal: health unreadable (%w); aborting to avoid signalling a possibly-reused pid", err)
	}
	if !IsContinuityServer(live) {
		return nil, fmt.Errorf("re-validate before signal: endpoint no longer identifies as continuity; aborting")
	}
	if live.PID != expectPID {
		return nil, fmt.Errorf("re-validate before signal: pid changed (%d -> %d); the process may have been replaced; aborting", expectPID, live.PID)
	}

	if match, err := exeMatcher(live.PID, live.Exe); err != nil {
		// Definite OS-level mismatch -> refuse outright.
		return nil, fmt.Errorf("executable mismatch for pid %d: %w; refusing to signal", live.PID, err)
	} else if !match {
		// Indeterminate (e.g. macOS, or empty exe in payload). Proceed: strong
		// field identity + same-pid revalidation already passed.
		if runtime.GOOS == "linux" && live.Exe != "" {
			// On Linux with a known exe, a non-match-non-error means we couldn't
			// read /proc at all; treat as indeterminate (process may be exiting),
			// not a green light beyond what identity already gives us.
		}
	}

	return live, nil
}

// ConfirmAndBounce is the full safe bounce: confirm the kill target, signal it,
// then respawn a detached serve. It is the shared implementation behind both
// `continuity restart`'s bare bounce and the hook auto-bounce, so the safety
// gate can never be bypassed by one caller. Returns an error (without having
// signalled) if the pre-signal gate fails.
//
// It serializes the entire critical section (revalidate -> signal -> respawn)
// under the per-user restart lock so two concurrent bounces can never race the
// same pid. The lock is acquired HERE, not in callers, so both the CLI bare path
// and the hook auto-bounce share one lock at one location. Any blocking work a
// caller wants to do (interactive confirmation, prompts) MUST happen before
// calling this, so the prompt is never held inside the lock. If the lock is held
// by another live bounce this returns *errRestartLockHeld (see
// IsRestartLockHeld) WITHOUT signalling.
func ConfirmAndBounce(c *Client, expectPID int) error {
	unlock, err := acquireRestartLock()
	if err != nil {
		return err
	}
	defer unlock()

	live, err := ConfirmKillTarget(c, expectPID)
	if err != nil {
		return err
	}
	if err := pidSignaller(live.PID); err != nil {
		return fmt.Errorf("stop server (pid %d): %w", live.PID, err)
	}
	if err := serverRespawner(); err != nil {
		return fmt.Errorf("respawn server: %w", err)
	}
	return nil
}
