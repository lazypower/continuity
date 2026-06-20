package cli

import (
	"errors"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

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

func TestDecideVerify(t *testing.T) {
	now := time.Unix(1000, 0)
	before := now.Add(5 * time.Second)  // deadline in the future
	passed := now.Add(-1 * time.Second) // deadline already passed

	tests := []struct {
		name           string
		deadline       time.Time
		healthyBounced bool
		childPID       int
		childAlive     bool
		want           verifyState
	}{
		{
			name:           "healthy and bounced confirms (highest priority even past deadline)",
			deadline:       passed,
			healthyBounced: true,
			childPID:       4242,
			childAlive:     false,
			want:           verifyConfirmed,
		},
		{
			name:           "bare child dead before healthy is a hard failure",
			deadline:       before,
			healthyBounced: false,
			childPID:       4242,
			childAlive:     false,
			want:           verifyFailedDead,
		},
		{
			name:           "bare child still alive within deadline keeps waiting",
			deadline:       before,
			healthyBounced: false,
			childPID:       4242,
			childAlive:     true,
			want:           verifyKeepWaiting,
		},
		{
			name:           "bare child alive but deadline passed times out soft",
			deadline:       passed,
			healthyBounced: false,
			childPID:       4242,
			childAlive:     true,
			want:           verifyTimedOutSoft,
		},
		{
			name:           "managed (no child) within deadline keeps waiting",
			deadline:       before,
			healthyBounced: false,
			childPID:       0,
			childAlive:     false,
			want:           verifyKeepWaiting,
		},
		{
			name:           "managed (no child) past deadline times out soft, never hard-fails",
			deadline:       passed,
			healthyBounced: false,
			childPID:       0,
			childAlive:     false,
			want:           verifyTimedOutSoft,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decideVerify(now, tt.deadline, tt.healthyBounced, tt.childPID, tt.childAlive)
			if got != tt.want {
				t.Errorf("decideVerify() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestVerifyBounceBareChildCrashOnBootHardFails is the integration-ish guard for
// Fix 1: a bare restart whose respawned child EXITS before becoming healthy must
// surface as the hard verifyFailedDead failure, NOT the soft (nil-returning)
// timeout. It spawns a real short-lived process (exits immediately), reaps it the
// same way SpawnDetachedServe now does, then drives the real verifyBounce against
// an unreachable server with a deliberately generous timeout — so the only way it
// can return the hard failure is dead-child detection via hooks.ProcessAlive, not
// the deadline. Without the SpawnDetachedServe reap, the child would linger as a
// zombie that signal-0 reports ALIVE, verifyBounce would fall through to the soft
// timeout (nil error), and this test fails.
func TestVerifyBounceBareChildCrashOnBootHardFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bare bounce / detached spawn is unsupported on Windows")
	}

	// Point the client at a dead port so the server never reports healthy — the
	// verify outcome is then decided purely by the child's liveness.
	t.Setenv("CONTINUITY_URL", "http://127.0.0.1:1")

	// Spawn a process that exits immediately (crash-on-boot stand-in) and reap it
	// exactly like the fixed SpawnDetachedServe, so ProcessAlive(pid) can report
	// false once it's gone instead of seeing a zombie we still parent.
	cmd := exec.Command("sh", "-c", "exit 1")
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn short-lived child: %v", err)
	}
	childPID := cmd.Process.Pid
	reaped := make(chan struct{})
	go func() { _ = cmd.Wait(); close(reaped) }()

	// Wait for the child to actually be gone (exited AND reaped) before verifying,
	// so we exercise dead-child detection rather than racing a slow startup.
	<-reaped
	deadline := time.Now().Add(2 * time.Second)
	for hooks.ProcessAlive(childPID) {
		if time.Now().After(deadline) {
			t.Fatalf("child pid %d still reported alive after reap; cannot exercise crash-on-boot path", childPID)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Generous timeout so a soft deadline-timeout cannot fire first: a hard
	// failure here can ONLY come from the dead-child branch (verifyFailedDead).
	orig := restartTimeout
	t.Cleanup(func() { restartTimeout = orig })
	restartTimeout = 30 * time.Second

	client := hooks.NewClient()
	err := verifyBounce(client, 4242 /*oldPID*/, childPID)
	if err == nil {
		t.Fatal("verifyBounce returned nil (soft timeout) for a dead bare child; expected the hard crash-on-boot failure")
	}
	if !strings.Contains(err.Error(), "exited before coming up") {
		t.Errorf("expected hard crash-on-boot error, got: %v", err)
	}
}

func TestRestartCmdSilencesUsage(t *testing.T) {
	// A verify timeout / runtime failure must not make cobra dump the usage block,
	// so restartCmd must declare SilenceUsage.
	if !restartCmd.SilenceUsage {
		t.Error("restartCmd.SilenceUsage must be true so runtime errors don't print the flags/usage block")
	}
}

func TestRestartCmdHasTimeoutFlag(t *testing.T) {
	f := restartCmd.Flags().Lookup("timeout")
	if f == nil {
		t.Fatal("restart command must expose a --timeout flag")
	}
	if f.DefValue != defaultRestartTimeout.String() {
		t.Errorf("--timeout default = %q, want %q", f.DefValue, defaultRestartTimeout.String())
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
