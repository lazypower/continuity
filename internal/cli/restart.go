package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lazypower/continuity/internal/hooks"
	"github.com/spf13/cobra"
)

// isDecodeError reports whether a Client.Status() error came from failing to
// decode the /api/health body (i.e. something answered on the port but the
// response is not our health shape) rather than a transport failure (nothing
// listening / connection refused). Client.Status wraps decode failures with a
// stable "decode health payload" prefix.
func isDecodeError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "decode health payload")
}

// restartAction is the decision the restart command makes after combining the
// server's identity (its /api/health payload, or the lack of one) with the
// local service-management state. The actual side-effecting work (kickstart,
// systemctl, SIGTERM+respawn) is selected from this enum so the decision logic
// itself stays pure and unit-testable without shelling out.
type restartAction int

const (
	// actionRefuse: something is answering on the port but it is NOT a
	// continuity server (health didn't parse or is missing our identity
	// fields). We must never kill it — refuse loudly.
	actionRefuse restartAction = iota

	// actionStartService: nothing is serving, but a platform service is
	// installed. (Re)start it via the service manager.
	actionStartService

	// actionAdviseServe: nothing is serving and no service is installed.
	// Tell the user to run `continuity serve`.
	actionAdviseServe

	// actionRestartLaunchd: continuity confirmed running under a loaded
	// launchd LaunchAgent — kickstart it.
	actionRestartLaunchd

	// actionRestartSystemd: continuity confirmed running under an active
	// systemd user unit — `systemctl --user restart`.
	actionRestartSystemd

	// actionBounceBare: continuity confirmed running (STRONG identity) but NOT
	// managed by a loaded/active service — stop its PID gracefully and respawn
	// detached.
	actionBounceBare

	// actionRefuseLegacyBare: a server is reachable but does NOT pass the strong
	// continuity identity gate (e.g. a legacy pre-#36 server: status ok, pid 0,
	// no api_version/schema_head) AND there is no installed service to restart
	// through the manager. We cannot strongly confirm it is continuity, so we
	// refuse to bare-kill it and advise the user to stop it manually then run
	// `continuity serve` / upgrade.
	actionRefuseLegacyBare
)

func (a restartAction) String() string {
	switch a {
	case actionRefuse:
		return "refuse"
	case actionStartService:
		return "start-service"
	case actionAdviseServe:
		return "advise-serve"
	case actionRestartLaunchd:
		return "restart-launchd"
	case actionRestartSystemd:
		return "restart-systemd"
	case actionBounceBare:
		return "bounce-bare"
	case actionRefuseLegacyBare:
		return "refuse-legacy-bare"
	default:
		return "unknown"
	}
}

// managerStatus is the tri-state result of probing the service manager about an
// installed service definition. The distinction matters for safety: a FAILED
// probe must NOT be conflated with "inactive", because treating "I couldn't ask
// the manager" as "not manager-managed" is exactly what lets a bare SIGTERM slip
// past a service the manager actually owns.
type managerStatus int

const (
	// mgrUnknown: either no service is installed, or a service IS installed but
	// the manager probe failed/was untrustworthy (launchctl/systemctl errored,
	// not found, or returned something unparseable). When a service is installed,
	// this means "ask the manager to restart, never bare-kill".
	mgrUnknown managerStatus = iota

	// mgrActive: the manager reports the unit loaded (launchd) / active (systemd)
	// — it is supervising the running process; a manager-driven bounce is correct.
	mgrActive

	// mgrInactive: the manager was reachable and definitively reports the unit
	// NOT loaded/active. The installed definition is dormant; (re)start via the
	// manager rather than bare-spawning.
	mgrInactive
)

func (m managerStatus) String() string {
	switch m {
	case mgrActive:
		return "active"
	case mgrInactive:
		return "inactive"
	default:
		return "unknown"
	}
}

// serviceState is the platform-independent view of how (if at all) the local
// continuity service is managed. It is produced by platformServiceState() so
// the pure decision function never touches launchctl/systemctl directly.
type serviceState struct {
	// installed is true when a service definition exists on disk (plist /
	// unit), regardless of whether it is currently loaded/active.
	installed bool

	// status is the tri-state manager probe result (only meaningful when
	// installed is true): active / inactive(known) / unknown(probe-failed).
	status managerStatus

	// kind identifies the manager: "launchd", "systemd", or "" (none /
	// unsupported platform).
	kind string
}

// managerRestartAction maps a service kind to its manager-driven restart action.
// Manager restarts do NOT signal a pid — their trust comes from "we installed
// this service" (the plist/unit is ours) — so they are allowed even when the
// server's health is legacy/unidentified. Returns (action, true) for a known
// kind, or (0, false) for an unknown/unsupported kind.
func managerRestartAction(kind string) (restartAction, bool) {
	switch kind {
	case "launchd":
		return actionRestartLaunchd, true
	case "systemd":
		return actionRestartSystemd, true
	default:
		return 0, false
	}
}

// decideRestartAction is the pure core of the restart command. Given the
// server's health (nil + statusErr when unreachable, or a decoded payload) and
// the local service state, it returns the action to take plus a human-readable
// reason. It performs NO I/O so it can be exhaustively unit-tested.
//
// Identity requirements differ by ACTION, by design:
//
//   - MANAGER restart (launchd/systemd) does not signal a pid; its trust is "we
//     installed this service". So an INSTALLED service is restarted through the
//     manager even when health is legacy/unidentified — this is the real
//     first-upgrade case (a v0.5.0 service-managed server reports pid 0 / no new
//     fields, yet must still be restartable). A FAILED manager probe (unknown,
//     not inactive) is also routed to the manager, NEVER to a bare kill.
//   - BARE pid-kill requires the STRONG identity gate (hooks.IsContinuityServer:
//     status ok + pid>0 + api_version>0 + schema_head>0). A reachable but
//     not-strongly-identified server with no service is REFUSED, never killed.
//
// Matrix:
//
//	unreachable + installed                 -> start service
//	unreachable + no service                -> advise serve
//	reachable + installed + active          -> manager restart (kind)
//	reachable + installed + unknown probe   -> manager restart (kind)   [Fix C/D]
//	reachable + installed + inactive        -> start service
//	reachable + installed + unknown kind    -> refuse (won't bare-kill a managed one)
//	reachable + strong identity + no svc    -> bounce bare
//	reachable + ok-but-legacy + no svc      -> refuse-legacy-bare (stop manually/upgrade)
//	reachable + non-continuity + no svc     -> refuse
func decideRestartAction(hs *hooks.HealthStatus, statusErr error, svc serviceState) (restartAction, string) {
	// Unreachable: nothing answered (or the connection failed). There is no
	// continuity process to identify, so port-based killing never enters the
	// picture here.
	if statusErr != nil || hs == nil {
		if svc.installed {
			return actionStartService, "no server is responding; starting the installed service"
		}
		return actionAdviseServe, "no continuity server is running and no service is installed"
	}

	// Something answered AND a service is installed. The manager owns this
	// process (or should). Route through the manager — NEVER bare-kill — even if
	// the health payload is legacy/unidentified, because manager restart does not
	// rely on pid identity.
	if svc.installed {
		switch svc.status {
		case mgrActive, mgrUnknown:
			// active: manager is supervising. unknown: probe failed/untrusted —
			// we must not assume "not managed" and bare-kill; defer to the
			// manager. Both need a known manager kind.
			if act, ok := managerRestartAction(svc.kind); ok {
				if svc.status == mgrActive {
					return act, fmt.Sprintf("restarting %s-managed service", svc.kind)
				}
				return act, fmt.Sprintf("service is installed but %s status could not be confirmed; restarting via the service manager (never bare-killing a managed server)", svc.kind)
			}
			// Installed but we don't recognize the manager kind: refuse rather
			// than risk bare-killing a process some manager owns.
			return actionRefuse, "a service is installed but its manager could not be identified; refusing to bare-kill a possibly-managed server (restart it via your service manager)"
		case mgrInactive:
			// Definitively dormant unit: (re)start it through the manager.
			return actionStartService, "service is installed but not active; starting it via the service manager"
		}
	}

	// No service installed. The ONLY safe way to stop this is a bare pid-kill, so
	// it must clear the STRONG identity gate first.
	if hooks.IsContinuityServer(hs) {
		return actionBounceBare, "restarting bare continuity server (stop confirmed pid, respawn detached)"
	}

	// Reachable but not strongly identified, and no service to fall back on. If it
	// at least claims status "ok" it may be a legacy continuity server — advise a
	// manual stop + upgrade rather than blindly killing it. Otherwise it's some
	// unrelated process squatting the port.
	if hs.Status == "ok" {
		return actionRefuseLegacyBare, "a legacy or unidentified server is responding (no api_version/schema_head); refusing to bare-kill it"
	}
	return actionRefuse, "a non-continuity process is responding on the configured port; refusing to kill it"
}

var restartYes bool

var restartCmd = &cobra.Command{
	Use:   "restart",
	Short: "Restart the continuity server to pick up an upgraded binary",
	Long: `Bounces the running continuity server so it re-execs the current
binary — useful after 'brew upgrade', when a long-running 'serve' is still
holding the old code in memory.

restart never kills by port blindly: it first confirms via /api/health that the
process it is about to stop is genuinely continuity. If an unrelated process is
holding the port, restart refuses.

The new server applies any pending schema migration on boot (a safety snapshot
is taken automatically before migrating).`,
	RunE: runRestart,
}

func init() {
	restartCmd.Flags().BoolVarP(&restartYes, "yes", "y", false,
		"skip the confirmation prompt (non-interactive)")
	rootCmd.AddCommand(restartCmd)
}

func runRestart(cmd *cobra.Command, args []string) error {
	// Serialize concurrent `continuity restart` invocations so two of them can't
	// race the kill/respawn (e.g. both confirm the same pid, both SIGTERM, then
	// two respawns fight over the port). A simple per-user file lock is
	// proportionate for a single-user localhost tool.
	unlock, err := acquireRestartLock()
	if err != nil {
		return err
	}
	defer unlock()

	client := hooks.NewClient()

	hs, statusErr := client.Status()
	// A reachable-but-non-continuity endpoint surfaces either as a decode
	// error (non-JSON / wrong shape) or as a parsed payload that fails the
	// identity gate. Normalize a decode error into "something answered, but
	// not our shape": present a non-nil, non-identifying health so the
	// decision lands on REFUSE rather than the unreachable branch (which might
	// otherwise start/kick a service while an unrelated process squats the
	// port). A transport failure keeps its error and falls through to the
	// unreachable handling.
	if isDecodeError(statusErr) {
		hs = &hooks.HealthStatus{} // non-nil, status "" -> decideRestartAction refuses
		statusErr = nil
	}

	svc := platformServiceState()

	action, reason := decideRestartAction(hs, statusErr, svc)

	switch action {
	case actionRefuse:
		bind := configuredAddr()
		return fmt.Errorf(
			"refusing to restart: %s\n  %s is held by a process whose /api/health does not identify as continuity.\n  Stop that process yourself, or point continuity at a different port (CONTINUITY_PORT / CONTINUITY_URL)",
			reason, bind)

	case actionRefuseLegacyBare:
		bind := configuredAddr()
		return fmt.Errorf(
			"refusing to restart: %s\n  %s has a server that does not advertise the compatibility fields needed to safely confirm it is continuity (likely a pre-upgrade build running bare, with no service installed).\n  Stop it manually (find its pid and `kill` it), then run `continuity serve` (or reinstall the service) to come up on the new binary.",
			reason, bind)

	case actionAdviseServe:
		fmt.Printf("%s.\n  Run `continuity serve` to start it.\n", reason)
		return nil

	case actionStartService:
		fmt.Println(reason + "...")
		surfaceMigrationNote()
		if err := platformServiceStart(); err != nil {
			return fmt.Errorf("start service: %w", err)
		}
		return verifyBounce(client, 0)

	case actionRestartLaunchd, actionRestartSystemd:
		if !confirmRestart() {
			fmt.Println("Aborted.")
			return nil
		}
		fmt.Println(reason + "...")
		surfaceMigrationNote()
		oldPID := 0
		if hs != nil {
			oldPID = hs.PID
		}
		if err := platformServiceRestart(); err != nil {
			return fmt.Errorf("restart service: %w", err)
		}
		return verifyBounce(client, oldPID)

	case actionBounceBare:
		if !confirmRestart() {
			fmt.Println("Aborted.")
			return nil
		}
		fmt.Println(reason + "...")
		surfaceMigrationNote()
		// ConfirmAndBounce re-validates identity (TOCTOU) and best-effort
		// exe-matches immediately before signalling, then respawns. It refuses —
		// without signalling — if the live endpoint no longer strongly identifies
		// as this same continuity pid.
		oldPID := hs.PID
		if err := hooks.ConfirmAndBounce(client, oldPID); err != nil {
			return fmt.Errorf("bounce server: %w", err)
		}
		return verifyBounce(client, oldPID)
	}

	return fmt.Errorf("internal: unhandled restart action %q", action)
}

// confirmRestart returns true if the user approved (or --yes was passed). The
// migration side-effect is the reason for the prompt; --yes bypasses it for
// non-interactive use.
func confirmRestart() bool {
	if restartYes {
		return true
	}
	return promptYN("Restart now (applies any pending migration)? [y/N] ")
}

// surfaceMigrationNote prints the one-line migration side-effect notice.
func surfaceMigrationNote() {
	fmt.Println("  note: the restarted server picks up the new binary and applies any pending schema migration (a safety snapshot is taken automatically).")
}

// verifyBounce polls /api/health until the server comes back healthy, then
// prints the new version. oldPID, when > 0, lets us detect that the OLD process
// is truly gone vs. a stale healthy response from a process that never bounced.
func verifyBounce(client *hooks.Client, oldPID int) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		hs, err := client.Status()
		if err == nil && hooks.IsContinuityServer(hs) && (oldPID == 0 || hs.PID != oldPID) {
			fmt.Printf("Restarted. Server is healthy: %s (pid %d).\n", hs.Version, hs.PID)
			return nil
		}
		time.Sleep(250 * time.Millisecond)
	}

	home, _ := os.UserHomeDir()
	return fmt.Errorf(
		"server did not come back healthy within 10s — it may have failed to start or a migration may have errored.\n  Check the log: %s/.continuity/serve.log",
		home)
}

// restartLockStaleAfter bounds how long a lock file is honored. A restart
// (graceful stop + respawn + 10s health poll) finishes well within this; a lock
// older than this is assumed orphaned by a crashed invocation and is reclaimed.
const restartLockStaleAfter = 2 * time.Minute

// acquireRestartLock takes a per-user advisory file lock at
// ~/.continuity/restart.lock via O_CREATE|O_EXCL. It returns a release function
// (safe to defer) on success. A pre-existing lock newer than
// restartLockStaleAfter blocks (returns an error); an older one is treated as
// stale, removed, and re-acquired. This is best-effort mutual exclusion
// appropriate for a single-user tool — not a robust distributed lock.
func acquireRestartLock() (func(), error) {
	home, err := os.UserHomeDir()
	if err != nil {
		// Can't locate the lock dir; proceed without locking rather than block a
		// legitimate restart. Locking is a safety nicety, not a correctness gate.
		return func() {}, nil
	}
	dir := filepath.Join(home, ".continuity")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return func() {}, nil
	}
	path := filepath.Join(dir, "restart.lock")

	tryCreate := func() (*os.File, error) {
		return os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	}

	f, err := tryCreate()
	if err != nil {
		if !os.IsExist(err) {
			return func() {}, nil // unexpected FS error: don't block the restart
		}
		// Lock exists. Reclaim it only if it is clearly stale.
		info, statErr := os.Stat(path)
		if statErr != nil || time.Since(info.ModTime()) < restartLockStaleAfter {
			return nil, fmt.Errorf(
				"another `continuity restart` appears to be in progress (lock: %s).\n  If you're sure none is, remove that file and retry", path)
		}
		_ = os.Remove(path)
		f, err = tryCreate()
		if err != nil {
			return nil, fmt.Errorf(
				"another `continuity restart` raced the lock (%s); retry in a moment", path)
		}
	}

	_, _ = fmt.Fprintf(f, "pid %d\n", os.Getpid())
	_ = f.Close()
	return func() { _ = os.Remove(path) }, nil
}

// configuredAddr returns the human-facing address the client is targeting, for
// error messages. It delegates to hooks.ResolveServerURL so the CLI, hooks, and
// serve all agree on a single resolution of CONTINUITY_URL / CONTINUITY_BIND /
// CONTINUITY_PORT.
func configuredAddr() string {
	return hooks.ResolveServerURL()
}
