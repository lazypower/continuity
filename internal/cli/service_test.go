package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildServicePATH(t *testing.T) {
	sep := string(os.PathListSeparator)
	home := "/home/tester"

	t.Run("captured install PATH comes first and is preserved", func(t *testing.T) {
		install := "/custom/tools" + sep + "/opt/homebrew/bin"
		got := buildServicePATH(install, home)
		parts := strings.Split(got, sep)
		if parts[0] != "/custom/tools" {
			t.Errorf("expected captured dir first, got %q (full: %q)", parts[0], got)
		}
		// The install-time PATH entries must all be present.
		if !strings.Contains(got, "/custom/tools") {
			t.Errorf("captured PATH dir missing: %q", got)
		}
	})

	t.Run("common provider locations are always included", func(t *testing.T) {
		got := buildServicePATH("/custom/tools", home)
		for _, want := range []string{
			"/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin",
			filepath.Join(home, ".claude", "local"),
			filepath.Join(home, ".local", "bin"),
		} {
			if !strings.Contains(got, want) {
				t.Errorf("expected default dir %q in PATH, got %q", want, got)
			}
		}
	})

	t.Run("empty install PATH still yields a usable default PATH", func(t *testing.T) {
		got := buildServicePATH("", home)
		for _, want := range []string{"/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin"} {
			if !strings.Contains(got, want) {
				t.Errorf("expected default dir %q with empty install PATH, got %q", want, got)
			}
		}
	})

	t.Run("no duplicate entries", func(t *testing.T) {
		// Install PATH already contains a default dir — it must not appear twice.
		got := buildServicePATH("/usr/local/bin"+sep+"/usr/bin", home)
		seen := map[string]int{}
		for _, p := range strings.Split(got, sep) {
			seen[p]++
		}
		if seen["/usr/local/bin"] != 1 {
			t.Errorf("/usr/local/bin appeared %d times, want 1: %q", seen["/usr/local/bin"], got)
		}
		if seen["/usr/bin"] != 1 {
			t.Errorf("/usr/bin appeared %d times, want 1: %q", seen["/usr/bin"], got)
		}
	})

	t.Run("entries with control chars are dropped (no line injection)", func(t *testing.T) {
		// A PATH entry containing a newline would inject extra lines into the
		// generated systemd unit / corrupt the plist. It must be dropped while the
		// well-formed entries survive. (Fix 2)
		install := "/good/tools" + sep + "/evil\nEnvironment=FOO=bar" + sep + "/also/good"
		got := buildServicePATH(install, home)
		if strings.ContainsAny(got, "\n\r\t") {
			t.Errorf("buildServicePATH leaked a control char: %q", got)
		}
		if strings.Contains(got, "Environment=FOO=bar") {
			t.Errorf("control-char entry was not dropped (line-injection risk): %q", got)
		}
		if !strings.Contains(got, "/good/tools") || !strings.Contains(got, "/also/good") {
			t.Errorf("well-formed entries should survive control-char neighbor: %q", got)
		}
		// Whole result must remain a single logical line.
		if len(strings.Split(got, "\n")) != 1 {
			t.Errorf("service PATH must stay one line, got multiple: %q", got)
		}
	})

	t.Run("space-containing dir is preserved", func(t *testing.T) {
		install := "/Applications/My Tools/bin"
		got := buildServicePATH(install, home)
		if !strings.Contains(got, "/Applications/My Tools/bin") {
			t.Errorf("space-containing dir should be preserved: %q", got)
		}
	})
}

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
