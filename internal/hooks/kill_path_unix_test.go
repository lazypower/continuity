//go:build !windows

package hooks

import (
	"runtime"
	"testing"
)

func TestStripDeletedSuffix(t *testing.T) {
	cases := map[string]string{
		"/usr/local/bin/continuity":           "/usr/local/bin/continuity",
		"/usr/local/bin/continuity (deleted)": "/usr/local/bin/continuity",
		"/x (deleted) (deleted)":              "/x (deleted)",
		"(deleted)":                           "(deleted)", // too short to be a real suffix
	}
	for in, want := range cases {
		if got := stripDeletedSuffix(in); got != want {
			t.Errorf("stripDeletedSuffix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestOsExeMatchIndeterminateOnNonLinuxOrEmpty(t *testing.T) {
	// Empty wantExe is always indeterminate (nothing to compare).
	if match, err := osExeMatch(1, ""); match || err != nil {
		t.Errorf("empty wantExe: got (match=%v, err=%v), want indeterminate (false,nil)", match, err)
	}
	if runtime.GOOS != "linux" {
		// On non-Linux unix there is no cheap per-pid exe lookup -> indeterminate.
		if match, err := osExeMatch(1, "/usr/local/bin/continuity"); match || err != nil {
			t.Errorf("non-linux: got (match=%v, err=%v), want indeterminate (false,nil)", match, err)
		}
	}
}
