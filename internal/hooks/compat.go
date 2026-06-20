package hooks

import (
	"encoding/json"
	"fmt"

	"github.com/lazypower/continuity/internal/buildinfo"
	"github.com/lazypower/continuity/internal/store"
)

// HealthStatus is the decoded /api/health payload. It captures the
// machine-comparable compatibility fields the server now advertises (issue
// #36) alongside the human-facing liveness fields. Unknown/older servers that
// don't emit the newer fields decode them as zero values.
type HealthStatus struct {
	Status        string  `json:"status"`
	Version       string  `json:"version"`
	Uptime        float64 `json:"uptime"`
	DB            bool    `json:"db"`
	APIVersion    int     `json:"api_version"`
	SchemaHead    int     `json:"schema_head"`
	SchemaCurrent int     `json:"schema_current"`
	PID           int     `json:"pid"`
	StartedAt     int64   `json:"started_at"`
	DBPath        string  `json:"db_path"`
	Exe           string  `json:"exe"`
}

// IsContinuityServer reports whether a decoded /api/health payload strongly
// identifies as a continuity server — the single gate any pid-signalling path
// (continuity restart's bare bounce, the hook auto-bounce) MUST pass before it
// sends a signal. It is deliberately stricter than mere liveness:
//
//	status == "ok"      — the server's invariant status string
//	pid > 0             — a real process to (potentially) signal
//	api_version > 0     — a distinctive field an unrelated server won't emit
//	schema_head > 0     — likewise; together these two make a coincidental
//	                      match by some other localhost process implausible
//
// This is proportionate to the real threat (accidental local collision / pid
// reuse), not a malicious forge. A legacy pre-#36 continuity server decodes
// api_version==0 / schema_head==0 and so FAILS this gate by design: it must not
// be bare-killed (callers route it to a manager restart or refuse — see the
// restart decision matrix).
func IsContinuityServer(hs *HealthStatus) bool {
	return hs != nil &&
		hs.Status == "ok" &&
		hs.PID > 0 &&
		hs.APIVersion > 0 &&
		hs.SchemaHead > 0
}

// Status fetches and parses /api/health from the server. Unlike Healthy(),
// which discards the body and only reports reachability, this returns the full
// decoded compatibility payload so callers can run skew detection.
func (c *Client) Status() (*HealthStatus, error) {
	data, err := c.Get("/api/health")
	if err != nil {
		return nil, err
	}
	var hs HealthStatus
	if err := json.Unmarshal(data, &hs); err != nil {
		return nil, fmt.Errorf("decode health payload: %w", err)
	}
	return &hs, nil
}

// SkewError reports that the local CLI binary and the running server disagree
// on a dimension of the client/server contract — typically because a
// `brew upgrade` swapped the binary while a long-running `serve` kept the old
// code (and possibly old schema) in memory. It is a typed error so consumers
// can branch on it (e.g. to offer a restart) without parsing the message.
//
// Comparison is on api_version and schema head, NOT raw version strings: a
// dev/dirty/patch build that shares the same APIVersion and schema head as the
// server is fully interoperable and must not be flagged.
type SkewError struct {
	// LocalVersion / ServerVersion are the human-facing version strings, kept
	// for the actionable message only — they are not the comparison basis.
	LocalVersion  string
	ServerVersion string

	// The dimensions actually compared.
	LocalAPIVersion  int
	ServerAPIVersion int
	LocalSchemaHead  int
	ServerSchemaHead int

	// Which dimension(s) diverged. At least one is true when SkewError is
	// returned.
	APIVersionMismatch bool
	SchemaMismatch     bool
}

func (e *SkewError) Error() string {
	var dims string
	switch {
	case e.APIVersionMismatch && e.SchemaMismatch:
		dims = fmt.Sprintf("api_version (local %d / server %d) and schema (local head %d / server head %d)",
			e.LocalAPIVersion, e.ServerAPIVersion, e.LocalSchemaHead, e.ServerSchemaHead)
	case e.APIVersionMismatch:
		dims = fmt.Sprintf("api_version (local %d / server %d)",
			e.LocalAPIVersion, e.ServerAPIVersion)
	case e.SchemaMismatch:
		dims = fmt.Sprintf("schema (local head %d / server head %d)",
			e.LocalSchemaHead, e.ServerSchemaHead)
	}
	return fmt.Sprintf(
		"version skew between CLI (%s) and running server (%s): %s; "+
			"the server is running stale code — restart `continuity serve`",
		e.LocalVersion, e.ServerVersion, dims,
	)
}

// CompatibilityCheck compares the LOCAL binary's contract (buildinfo.APIVersion
// + store.HeadSchemaVersion + buildinfo version string) against the server's
// reported health fields. It returns a *SkewError when the api_version or
// schema head diverge, and nil when the two are interoperable.
//
// A pure binary-version string difference (e.g. v0.6.1 client vs v0.6.0
// server, or a -dirty dev build) that shares the same api_version and schema
// head is NOT skew and returns nil. This is the substrate for the consumer
// work (CLI write-path guard, restart prompt, hook surfacing); it does not
// itself act on the result.
func CompatibilityCheck(hs *HealthStatus) error {
	local := localContract()

	apiMismatch := hs.APIVersion != local.apiVersion
	schemaMismatch := hs.SchemaHead != local.schemaHead

	if !apiMismatch && !schemaMismatch {
		return nil
	}

	return &SkewError{
		LocalVersion:       local.version,
		ServerVersion:      hs.Version,
		LocalAPIVersion:    local.apiVersion,
		ServerAPIVersion:   hs.APIVersion,
		LocalSchemaHead:    local.schemaHead,
		ServerSchemaHead:   hs.SchemaHead,
		APIVersionMismatch: apiMismatch,
		SchemaMismatch:     schemaMismatch,
	}
}

// localContract captures the local binary's view of the client/server
// contract. Factored out so tests can reason about the comparison inputs.
type contract struct {
	version    string
	apiVersion int
	schemaHead int
}

func localContract() contract {
	return contract{
		version:    buildinfo.VersionString(),
		apiVersion: buildinfo.APIVersion,
		schemaHead: store.HeadSchemaVersion(),
	}
}
