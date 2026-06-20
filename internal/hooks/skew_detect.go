package hooks

import (
	"errors"
	"fmt"
	"os"
)

// skewAction is the decision the session-start hook makes when the server is
// already running and turns out to be version/schema-skewed relative to this
// CLI binary. It is selected from inputs that are cheap to gather (the decoded
// health, the opt-in marker, the service-managed state) so the decision logic
// itself stays pure and unit-testable without signalling or spawning anything.
type skewAction int

const (
	// skewNothing: no skew (or no usable health) — the hook does nothing.
	skewNothing skewAction = iota

	// skewWarn: the server is stale; print a warning pointing the user at
	// `continuity restart`. This is the default whenever skew is detected.
	skewWarn

	// skewBounce: the user explicitly opted into hook-local bouncing AND the
	// server is bare (not service-managed) — stop the confirmed PID and respawn
	// detached. bounce==migrate, so this only ever happens with the opt-in
	// marker present.
	skewBounce
)

func (a skewAction) String() string {
	switch a {
	case skewNothing:
		return "nothing"
	case skewWarn:
		return "warn"
	case skewBounce:
		return "bounce"
	default:
		return "unknown"
	}
}

// decideSkewAction is the pure core of the session-start skew surfacing. Given
// the result of a CompatibilityCheck (skewErr is a *SkewError when skewed, nil
// otherwise), whether the opt-in bounce marker is present, and whether the
// running server is service-managed, it returns the action to take.
//
//   - no skew                                   -> nothing
//   - skew, no opt-in marker                    -> warn only
//   - skew, opt-in marker, service-managed      -> warn only (restart is `continuity restart`'s job)
//   - skew, opt-in marker, bare (not managed)   -> bounce
//
// It performs NO I/O so it can be exhaustively unit-tested.
func decideSkewAction(skewErr error, bounceMarker, serviceManaged bool) skewAction {
	var skew *SkewError
	if skewErr == nil || !errors.As(skewErr, &skew) {
		return skewNothing
	}
	if bounceMarker && !serviceManaged {
		return skewBounce
	}
	return skewWarn
}

// surfaceServerSkew runs the ambient skew check for the case where the server
// is ALREADY running and healthy at session start. It is cheap and strictly
// non-fatal: any error in the skew/bounce path is logged to stderr and
// swallowed so the hook never blocks a Claude session.
//
// By default it only warns. It auto-bounces ONLY when the opt-in marker
// (~/.continuity/autostart-bounce) is present AND the server is bare (not
// service-managed) — in which case it stops the confirmed PID and respawns a
// detached serve, hooks-locally. It never drives launchd/systemd from the hook;
// a service-managed server gets a warning even with the marker set.
func surfaceServerSkew(client *Client) {
	hs, err := client.Status()
	if err != nil || hs == nil {
		// Couldn't read health — nothing actionable here. The rest of the hook
		// proceeds; this is best-effort surfacing only.
		return
	}

	skewErr := CompatibilityCheck(hs)
	action := decideSkewAction(skewErr, bounceMarkerEnabled(), serviceManaged())

	switch action {
	case skewNothing:
		return

	case skewWarn:
		var skew *SkewError
		errors.As(skewErr, &skew)
		fmt.Fprintf(os.Stderr,
			"continuity: server is running stale code (server %s / CLI %s) — run `continuity restart` to pick up the new binary\n",
			skew.ServerVersion, skew.LocalVersion)
		return

	case skewBounce:
		if err := bounceBareServer(hs.PID); err != nil {
			// Non-fatal: fall back to the warning so the user still knows.
			fmt.Fprintf(os.Stderr,
				"continuity: auto-bounce of stale server failed (%v) — run `continuity restart`\n", err)
			return
		}
		fmt.Fprintln(os.Stderr, "continuity: bounced stale server to pick up the new binary")
	}
}

// bounceMarkerEnabled reports whether the user opted into hook-local auto-bounce
// by creating ~/.continuity/autostart-bounce, mirroring the autostart marker
// convention. bounce==migrate, so this is strictly opt-in.
func bounceMarkerEnabled() bool {
	path, err := bounceMarkerPath()
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}
