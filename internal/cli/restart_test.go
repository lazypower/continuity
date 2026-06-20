package cli

import (
	"errors"
	"testing"

	"github.com/lazypower/continuity/internal/hooks"
)

func continuityHealth() *hooks.HealthStatus {
	return &hooks.HealthStatus{Status: "ok", PID: 4242, Version: "v0.6.0 (abc)"}
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
			name:      "ok status but no pid is refused",
			hs:        &hooks.HealthStatus{Status: "ok", PID: 0},
			statusErr: nil,
			svc:       serviceState{},
			want:      actionRefuse,
		},
		{
			name:      "non-continuity refused even when a service is installed",
			hs:        &hooks.HealthStatus{Status: "nginx"},
			statusErr: nil,
			svc:       serviceState{installed: true, managerActive: true, kind: "launchd"},
			want:      actionRefuse,
		},
		{
			name:      "continuity under active launchd kickstarts",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, managerActive: true, kind: "launchd"},
			want:      actionRestartLaunchd,
		},
		{
			name:      "continuity under active systemd restarts",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, managerActive: true, kind: "systemd"},
			want:      actionRestartSystemd,
		},
		{
			name:      "continuity running bare bounces pid",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{},
			want:      actionBounceBare,
		},
		{
			name:      "continuity with installed-but-inactive launchd bounces bare",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, managerActive: false, kind: "launchd"},
			want:      actionBounceBare,
		},
		{
			name:      "continuity with installed-but-inactive systemd bounces bare",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, managerActive: false, kind: "systemd"},
			want:      actionBounceBare,
		},
		{
			name:      "active manager with unknown kind falls back to bare bounce",
			hs:        continuityHealth(),
			statusErr: nil,
			svc:       serviceState{installed: true, managerActive: true, kind: "weird"},
			want:      actionBounceBare,
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

func TestIsContinuityHealth(t *testing.T) {
	tests := []struct {
		name string
		hs   *hooks.HealthStatus
		want bool
	}{
		{"nil", nil, false},
		{"empty", &hooks.HealthStatus{}, false},
		{"ok no pid", &hooks.HealthStatus{Status: "ok"}, false},
		{"pid no ok status", &hooks.HealthStatus{PID: 10}, false},
		{"wrong status", &hooks.HealthStatus{Status: "up", PID: 10}, false},
		{"valid", &hooks.HealthStatus{Status: "ok", PID: 10}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isContinuityHealth(tt.hs); got != tt.want {
				t.Errorf("isContinuityHealth() = %v, want %v", got, tt.want)
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
