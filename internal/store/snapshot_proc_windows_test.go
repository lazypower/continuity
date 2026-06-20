//go:build windows

package store

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

// TestFlockDowngrade_NoForeignExclusiveInGap is the Round 7 Finding 3 regression
// for the Windows EX→SH downgrade. Windows has no atomic EX→SH transition, so the
// downgrade releases the exclusive primary-range lock and re-takes it shared. A
// naive unlock-before-relock leaves a window in which a concurrent restore could
// grab EXCLUSIVE while the migrated SQLite conn is still live. The fix holds a
// SHARED lock on a bridge sub-range (byte 1) on a SECOND handle BEFORE releasing
// exclusive, so an inter-process lock is held CONTINUOUSLY.
//
// This test fires the in-gap seam (between the exclusive release and the shared
// re-lock) and, from a SEPARATE handle, attempts a foreign EXCLUSIVE acquire of
// the whole [0,2) range non-blocking. It MUST be refused — the bridge byte holds
// it out. With the bridge step removed (pure unlock-then-relock), the foreign
// exclusive would succeed in the gap and this assertion fails, exactly catching
// the regression.
func TestFlockDowngrade_NoForeignExclusiveInGap(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "continuity.db.lock")

	// Primary handle takes EXCLUSIVE on [0,2).
	primary, err := openLockFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	if ok, lerr := flockExclusiveNB(primary); lerr != nil || !ok {
		t.Fatalf("acquire exclusive: ok=%v err=%v", ok, lerr)
	}

	h := &dbLockHandle{exclusive: true, f: primary}

	var foreignRefusedInGap bool
	var seamFired bool
	hookWindowsDowngradeGap = func(lp string) {
		seamFired = true
		// A FOREIGN exclusive acquirer (separate handle) tries to lock the whole
		// [0,2) range non-blocking. It must be refused while the bridge holds byte 1.
		foreign, oerr := openLockFile(lp)
		if oerr != nil {
			t.Errorf("open foreign handle: %v", oerr)
			return
		}
		defer foreign.Close()
		ol := new(windows.Overlapped)
		gErr := windows.LockFileEx(
			windows.Handle(foreign.Fd()),
			windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
			0, lockRangeLen, 0, ol,
		)
		if gErr == nil {
			// Granted in the gap → the continuous-hold invariant is broken.
			ol2 := new(windows.Overlapped)
			_ = windows.UnlockFileEx(windows.Handle(foreign.Fd()), 0, lockRangeLen, 0, ol2)
			foreignRefusedInGap = false
			return
		}
		foreignRefusedInGap = true
	}
	defer func() { hookWindowsDowngradeGap = nil }()

	if err := flockDowngradeToShared(h); err != nil {
		t.Fatalf("downgrade: %v", err)
	}
	if !seamFired {
		t.Fatal("downgrade gap seam never fired")
	}
	if !foreignRefusedInGap {
		t.Fatal("a foreign EXCLUSIVE acquire succeeded during the EX→SH gap; the bridge lock did not keep it out")
	}
	if h.bridge == nil {
		t.Error("downgrade did not park a bridge handle (bridge step was skipped)")
	}
	// Cleanup: release the downgraded shared + bridge.
	if h.bridge != nil {
		_ = h.bridge.Close()
	}
	_ = os.Remove(lockPath)
}
