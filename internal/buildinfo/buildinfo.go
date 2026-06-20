// Package buildinfo holds build-time metadata (version, commit, build date)
// stamped into the binary via -ldflags, plus the HTTP API contract version.
//
// It is a deliberately dependency-free leaf package so that BOTH internal/cli
// (the command layer) and internal/hooks (the HTTP client) can import it
// without creating an import cycle. internal/cli already imports
// internal/hooks; if build metadata lived in cli, then hooks importing it for
// the skew check would close a cycle (cli -> hooks -> cli). Keeping the
// metadata here breaks that knot: both layers depend on buildinfo, buildinfo
// depends on nothing.
package buildinfo

import "fmt"

// Set via -ldflags at build time. Defaults identify an un-stamped dev build.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// APIVersion is the version of the HTTP contract between the CLI and the
// running server. Bump it whenever the request/response shape of the API
// changes in a way that an older client and a newer server (or vice versa)
// can no longer safely interoperate. It is what skew detection compares on —
// a pure binary-version string difference that shares the same APIVersion and
// schema head is NOT considered skew.
const APIVersion = 1

// VersionString returns a formatted version string for use in health checks,
// the `version` command, and log lines.
func VersionString() string {
	return fmt.Sprintf("%s (%s)", Version, Commit)
}
