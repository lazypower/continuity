package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/buildinfo"
	"github.com/lazypower/continuity/internal/hooks"
	"github.com/lazypower/continuity/internal/store"
)

func TestSkewWarning(t *testing.T) {
	localAPI := buildinfo.APIVersion
	localHead := store.HeadSchemaVersion()

	t.Run("skew -> warning contains remediation hint and both versions", func(t *testing.T) {
		hs := &hooks.HealthStatus{
			Version:    "v0.0.1 (oldoldo)",
			APIVersion: localAPI - 1,
			SchemaHead: localHead,
		}
		msg := skewWarning(hs, nil)
		if msg == "" {
			t.Fatal("expected a warning for a skewed server, got empty string")
		}
		if !strings.Contains(msg, "continuity restart") {
			t.Errorf("warning missing remediation hint `continuity restart`: %q", msg)
		}
		if !strings.Contains(msg, "v0.0.1 (oldoldo)") {
			t.Errorf("warning missing server version: %q", msg)
		}
		if !strings.Contains(msg, buildinfo.VersionString()) {
			t.Errorf("warning missing local version: %q", msg)
		}
	})

	t.Run("no skew -> silent", func(t *testing.T) {
		hs := &hooks.HealthStatus{
			Version:    "anything",
			APIVersion: localAPI,
			SchemaHead: localHead,
		}
		if msg := skewWarning(hs, nil); msg != "" {
			t.Errorf("expected no warning for interoperable server, got %q", msg)
		}
	})

	t.Run("status error -> silent (different condition)", func(t *testing.T) {
		// Even if hs would be skewed, a transport/status error must produce no
		// warning — the command surfaces unreachable on its own.
		hs := &hooks.HealthStatus{APIVersion: localAPI - 1, SchemaHead: localHead}
		if msg := skewWarning(hs, errors.New("connection refused")); msg != "" {
			t.Errorf("expected silence on status error, got %q", msg)
		}
	})

	t.Run("nil health -> silent", func(t *testing.T) {
		if msg := skewWarning(nil, nil); msg != "" {
			t.Errorf("expected silence on nil health, got %q", msg)
		}
	})
}
