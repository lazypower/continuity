package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveBinaryPathFrom(t *testing.T) {
	tmp := t.TempDir()

	// Simulate Homebrew layout:
	//   <tmp>/Cellar/continuity/0.3.0/bin/continuity   (real binary)
	//   <tmp>/bin/continuity -> ../Cellar/.../continuity (stable symlink)
	cellarDir := filepath.Join(tmp, "Cellar", "continuity", "0.3.0", "bin")
	if err := os.MkdirAll(cellarDir, 0755); err != nil {
		t.Fatal(err)
	}
	binDir := filepath.Join(tmp, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatal(err)
	}

	cellarBin := filepath.Join(cellarDir, "continuity")
	if err := os.WriteFile(cellarBin, []byte("#!/bin/sh\n"), 0755); err != nil {
		t.Fatal(err)
	}
	stableLink := filepath.Join(binDir, "continuity")
	if err := os.Symlink(cellarBin, stableLink); err != nil {
		t.Fatal(err)
	}

	cellarReal, err := filepath.EvalSymlinks(cellarBin)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("prefers stable symlink when both resolve to same real binary", func(t *testing.T) {
		// os.Executable() returned the Cellar path; PATH has the stable symlink.
		got, err := resolveBinaryPathFrom(cellarBin, stableLink)
		if err != nil {
			t.Fatal(err)
		}
		wantAbs, _ := filepath.Abs(stableLink)
		if got != wantAbs {
			t.Errorf("expected stable symlink %q, got %q", wantAbs, got)
		}
	})

	t.Run("prefers stable symlink when self is the symlink itself", func(t *testing.T) {
		// os.Executable() returned the symlink (as it does on some systems).
		got, err := resolveBinaryPathFrom(stableLink, stableLink)
		if err != nil {
			t.Fatal(err)
		}
		wantAbs, _ := filepath.Abs(stableLink)
		if got != wantAbs {
			t.Errorf("expected stable symlink %q, got %q", wantAbs, got)
		}
	})

	t.Run("falls back to realpath when no PATH match", func(t *testing.T) {
		got, err := resolveBinaryPathFrom(cellarBin, "")
		if err != nil {
			t.Fatal(err)
		}
		if got != cellarReal {
			t.Errorf("expected realpath %q, got %q", cellarReal, got)
		}
	})

	t.Run("falls back to realpath when PATH points to a different binary", func(t *testing.T) {
		// User runs ./continuity from a dev tree but also has a different brew install on PATH.
		otherDir := filepath.Join(tmp, "other")
		if err := os.MkdirAll(otherDir, 0755); err != nil {
			t.Fatal(err)
		}
		otherBin := filepath.Join(otherDir, "continuity")
		if err := os.WriteFile(otherBin, []byte("#!/bin/sh\n# different\n"), 0755); err != nil {
			t.Fatal(err)
		}

		got, err := resolveBinaryPathFrom(cellarBin, otherBin)
		if err != nil {
			t.Fatal(err)
		}
		if got != cellarReal {
			t.Errorf("expected realpath %q (different binary on PATH should be ignored), got %q", cellarReal, got)
		}
	})
}
