//go:build linux

package cli

import (
	"errors"
	"testing"
)

func TestClassifyIsActive(t *testing.T) {
	// `systemctl is-active` exits non-zero for not-running units, so a non-nil err
	// is expected for the inactive/failed states and is NOT itself a probe failure.
	exitErr := errors.New("exit status 3")
	busErr := errors.New("exit status 1")

	tests := []struct {
		name  string
		state string
		err   error
		want  managerStatus
	}{
		{"active", "active", nil, mgrActive},
		{"inactive (err expected)", "inactive", exitErr, mgrInactive},
		{"failed (err expected)", "failed", exitErr, mgrInactive},
		{"activating", "activating", exitErr, mgrInactive},
		{"deactivating", "deactivating", exitErr, mgrInactive},
		{"reloading", "reloading", exitErr, mgrInactive},
		{"empty output -> unknown", "", busErr, mgrUnknown},
		{"empty output no err -> unknown", "", nil, mgrUnknown},
		{"bus error diagnostic -> unknown", "Failed to connect to bus: No such file or directory", busErr, mgrUnknown},
		{"unparseable text -> unknown", "wat", nil, mgrUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyIsActive(tt.state, tt.err); got != tt.want {
				t.Errorf("classifyIsActive(%q, %v) = %v, want %v", tt.state, tt.err, got, tt.want)
			}
		})
	}
}
