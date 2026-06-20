package cli

import (
	"errors"
	"testing"

	"github.com/lazypower/continuity/internal/hooks"
)

// continuityHealth returns a STRONGLY-identified continuity health payload
// (status ok + pid + api_version + schema_head), the only shape allowed to drive
// a bare pid-kill.
func continuityHealth() *hooks.HealthStatus {
	return &hooks.HealthStatus{
		Status: "ok", PID: 4242, Version: "v0.6.0 (abc)",
		APIVersion: 1, SchemaHead: 7,
	}
}

// legacyHealth returns a pre-#36 server's health: reachable, status ok, but no
// api_version/schema_head and pid 0 — it must NOT pass the strong identity gate.
func legacyHealth() *hooks.HealthStatus {
	return &hooks.HealthStatus{Status: "ok", Version: "v0.5.0 (legacy)"}
}

func TestDecideRestartAction(t *testing.T) {
	tests := []struct {
		name      string
		hs        *hooks.HealthStatus
		statusErr error
		svc       serviceState
		want      restartAction
	}{
		{
			name:      "unreachable with no service advises serve",
			hs:        nil,
			statusErr: errors.New("connection refused"),
			svc:       serviceState{},
			want:      actionAdviseServe,
		},
		{
			name:      "unreachable with installed launchd service starts it",
			hs:        nil,
			statusErr: errors.New("connection refused"),
			svc:       serviceState{installed: true, kind: "launchd"},
			want:      actionStartService,
		},
		{
			name:      "unreachable with installed systemd service starts it",
			hs:        nil,
			statusErr: errors.New("connection refused"),
			svc:       serviceState{installed: true, kind: "systemd"},
			want:      actionStartService,
		},
		{
			name:      "non-continuity process on port is refused (empty payload)",
			hs:        &hooks.HealthStatus{},
			statusErr: nil,
			svc:       serviceState{},
			want:      actionRefuse,
		},
		{
			name:      "non-continuity status string is refused",
			hs:        &hooks.HealthStatus{Status: "healthy", PID: 0},
			statusErr: nil,
			svc:       serviceState{},
			want:      actionRefuse,
		},
		{
			// status ok but pid 0 and no compat fields: a continuity-ish but
			// not-strongly-identified server with no service -> refuse-legacy-bare
			// (never bare-kill an unconfirmed pid).
			name:      "ok status but no pid (no service) refuses legacy-bare",
			hs:        &hooks.HealthStatus{Status: "ok", PID: 0},
			statusErr: nil,
			svc:       serviceState{},
			want:      actionRefuseLegacyBare,
		},
		{
			// A service is installed, so we route through the manager regardless
			// of what the health says — never bare-kill a (possibly) managed one.
			name:      "non-continuity-looking health but installed launchd restarts via manager",
			hs:        &hooks.HealthStatus{Status: "nginx"},
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrActive, kind: "launchd"},
			want:      actionRestartLaunchd,
		},
		{
			name:      "continuity under active launchd kickstarts",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrActive, kind: "launchd"},
			want:      actionRestartLaunchd,
		},
		{
			name:      "continuity under active systemd restarts",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrActive, kind: "systemd"},
			want:      actionRestartSystemd,
		},
		{
			name:      "continuity running bare (strong identity, no service) bounces pid",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{},
			want:      actionBounceBare,
		},
		{
			// installed + definitively inactive -> (re)start via the manager, not
			// a bare spawn.
			name:      "continuity with installed-but-inactive launchd starts service",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrInactive, kind: "launchd"},
			want:      actionStartService,
		},
		{
			name:      "continuity with installed-but-inactive systemd starts service",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrInactive, kind: "systemd"},
			want:      actionStartService,
		},
		{
			// Fix C: probe failed (unknown) on an INSTALLED service must use the
			// manager restart path, NEVER a bare kill.
			name:      "installed launchd with unknown probe restarts via manager (never bare-kill)",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrUnknown, kind: "launchd"},
			want:      actionRestartLaunchd,
		},
		{
			name:      "installed systemd with unknown probe restarts via manager (never bare-kill)",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrUnknown, kind: "systemd"},
			want:      actionRestartSystemd,
		},
		{
			// Fix C: installed service but unrecognized manager kind -> refuse,
			// do NOT bare-kill a possibly-managed server.
			name:      "installed service with unknown manager kind refuses (no bare-kill)",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrActive, kind: "weird"},
			want:      actionRefuse,
		},
		{
			// Fix D: legacy server, service installed + active -> manager restart
			// even though health is unidentified (the real first-upgrade case).
			name:      "legacy health under active launchd restarts via manager",
			hs:        legacyHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrActive, kind: "launchd"},
			want:      actionRestartLaunchd,
		},
		{
			name:      "legacy health under unknown-probe systemd restarts via manager",
			hs:        legacyHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, status: mgrUnknown, kind: "systemd"},
			want:      actionRestartSystemd,
		},
		{
			// Fix D: legacy server running BARE (no service) -> refuse, advise
			// manual stop + upgrade. Never bare-kill an unidentified server.
			name:      "legacy health running bare refuses (advise manual stop)",
			hs:        legacyHealth(),
			statusErr: nil,
			svc:       serviceState{},
			want:      actionRefuseLegacyBare,
		},
		{
			// Strong-identity gate: ok status but pid 0 and no fields, no service
			// -> still legacy-bare refuse (it claims status ok).
			name:      "ok-status-only no fields no service refuses legacy-bare",
			hs:        &hooks.HealthStatus{Status: "ok"},
			statusErr: nil,
			svc:       serviceState{},
			want:      actionRefuseLegacyBare,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := decideRestartAction(tt.hs, tt.statusErr, tt.svc)
			if got != tt.want {
				t.Errorf("decideRestartAction() = %v (%s), want %v", got, reason, tt.want)
			}
			if reason == "" {
				t.Error("decideRestartAction() returned empty reason")
			}
		})
	}
}

func TestIsDecodeError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"transport", errors.New("GET /api/health: dial tcp: connection refused"), false},
		{"decode", errors.New("decode health payload: invalid character 'h'"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDecodeError(tt.err); got != tt.want {
				t.Errorf("isDecodeError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestartActionString(t *testing.T) {
	// Guard against an enum value losing its String() case.
	all := []restartAction{
		actionRefuse, actionStartService, actionAdviseServe,
		actionRestartLaunchd, actionRestartSystemd, actionBounceBare,
		actionRefuseLegacyBare,
	}
	seen := map[string]bool{}
	for _, a := range all {
		s := a.String()
		if s == "" || s == "unknown" {
			t.Errorf("action %d has no String()", int(a))
		}
		if seen[s] {
			t.Errorf("duplicate String() %q", s)
		}
		seen[s] = true
	}
}
