package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/lazypower/continuity/internal/hooks"
)

// skewWarning returns a human-facing warning string when the running server is
// version/schema-skewed relative to this CLI binary, or "" when there is
// nothing to warn about. It is the pure, testable core of warnIfSkewed: it
// takes the result of hooks.Client.Status() (the decoded health and any error)
// and decides whether to surface a skew warning.
//
// Decision:
//   - statusErr != nil (server down / unreachable / non-JSON): "" — this is a
//     different condition the command handles its own way. Never warn here.
//   - hs == nil: "" — nothing to compare.
//   - CompatibilityCheck returns a *SkewError: a warning that names both
//     versions and points the user at `continuity restart`.
//   - otherwise (interoperable): "".
func skewWarning(hs *hooks.HealthStatus, statusErr error) string {
	if statusErr != nil || hs == nil {
		return ""
	}
	err := hooks.CompatibilityCheck(hs)
	if err == nil {
		return ""
	}
	var skew *hooks.SkewError
	if !errors.As(err, &skew) {
		return ""
	}
	return fmt.Sprintf(
		"⚠ continuity server is running %s but this CLI is %s — schema/API mismatch; run `continuity restart` to pick up the new binary",
		skew.ServerVersion, skew.LocalVersion,
	)
}

// warnIfSkewed runs a cheap, non-blocking skew preflight against the running
// server and prints a warning to stderr when the server is stale relative to
// this CLI. It NEVER blocks or fails the command — it only surfaces the
// mismatch so a stale server (e.g. post `brew upgrade`) can't hide behind
// otherwise-cryptic server-side errors. Call it at the top of mutation
// commands' RunE.
//
// If the status check itself errors (server down / unreachable / non-continuity
// process), this is silent: that's a separate condition the command surfaces on
// its own when it makes its actual request.
func warnIfSkewed() {
	hs, err := hooks.NewClient().Status()
	if msg := skewWarning(hs, err); msg != "" {
		fmt.Fprintln(os.Stderr, msg)
	}
}
