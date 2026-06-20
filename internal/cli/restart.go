package cli

import (
	"fmt"
	"os"
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

	// actionBounceBare: continuity confirmed running but NOT managed by a
	// loaded/active service — stop its PID gracefully and respawn detached.
	actionBounceBare
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

	// managerActive is true when the service manager reports the unit as
	// loaded (launchd) or active (systemd) — i.e. the running process is the
	// one the manager is supervising and a manager-driven bounce is correct.
	managerActive bool

	// kind identifies the manager: "launchd", "systemd", or "" (none /
	// unsupported platform).
	kind string
}

// isContinuityHealth reports whether a decoded /api/health payload actually
// looks like a continuity server, as opposed to some unrelated process that
// happened to answer on the port with parseable JSON. The server always emits
// status "ok" and a real pid; an unrelated service will not. This is the gate
// that prevents restart from ever killing a non-continuity process.
func isContinuityHealth(hs *hooks.HealthStatus) bool {
	return hs != nil && hs.Status == "ok" && hs.PID > 0
}

// decideRestartAction is the pure core of the restart command. Given the
// server's health (nil + statusErr when unreachable, or a decoded payload) and
// the local service state, it returns the action to take plus a human-readable
// reason. It performs NO I/O so it can be exhaustively unit-tested.
//
//   - Unreachable + service installed  -> start the service.
//   - Unreachable + no service         -> advise `continuity serve`.
//   - Reachable but not continuity     -> refuse (never kill it).
//   - Continuity + launchd active      -> kickstart.
//   - Continuity + systemd active      -> systemctl restart.
//   - Continuity + not manager-active  -> bounce the confirmed PID.
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

	// Something answered. Confirm it is actually continuity before we consider
	// stopping it — this is the critical safety gate.
	if !isContinuityHealth(hs) {
		return actionRefuse, "a non-continuity process is responding on the configured port; refusing to kill it"
	}

	// Confirmed continuity. Prefer a manager-driven bounce when the service
	// manager is actively supervising this process.
	if svc.managerActive {
		switch svc.kind {
		case "launchd":
			return actionRestartLaunchd, "restarting launchd-managed service"
		case "systemd":
			return actionRestartSystemd, "restarting systemd-managed service"
		}
	}

	// Confirmed continuity but not under an active manager (bare `serve`, or a
	// service installed-but-not-loaded): stop the confirmed PID and respawn.
	return actionBounceBare, "restarting bare continuity server (stop confirmed pid, respawn detached)"
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
		hs = &hooks.HealthStatus{} // non-nil but fails isContinuityHealth -> refuse
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
		if err := stopPID(hs.PID); err != nil {
			return fmt.Errorf("stop server (pid %d): %w", hs.PID, err)
		}
		if err := respawnServer(); err != nil {
			return fmt.Errorf("respawn server: %w", err)
		}
		return verifyBounce(client, hs.PID)
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
		if err == nil && isContinuityHealth(hs) && (oldPID == 0 || hs.PID != oldPID) {
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

// configuredAddr returns the human-facing address the client is targeting, for
// error messages. It honors CONTINUITY_URL / CONTINUITY_PORT / CONTINUITY_BIND
// the same way the client and serve paths do.
func configuredAddr() string {
	if url := os.Getenv("CONTINUITY_URL"); url != "" {
		return url
	}
	bind := os.Getenv("CONTINUITY_BIND")
	if bind == "" {
		bind = "127.0.0.1"
	}
	port := os.Getenv("CONTINUITY_PORT")
	if port == "" {
		port = "37777"
	}
	return fmt.Sprintf("%s:%s", bind, port)
}
