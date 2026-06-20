package hooks

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDecideSkewAction(t *testing.T) {
	skew := &SkewError{
		LocalVersion:       "v2 (new)",
		ServerVersion:      "v1 (old)",
		APIVersionMismatch: true,
	}

	tests := []struct {
		name           string
		skewErr        error
		bounceMarker   bool
		serviceManaged bool
		want           skewAction
	}{
		{
			name:    "no skew -> nothing",
			skewErr: nil,
			want:    skewNothing,
		},
		{
			name:    "skew, no marker -> warn",
			skewErr: skew,
			want:    skewWarn,
		},
		{
			name:         "skew, marker, bare -> bounce",
			skewErr:      skew,
			bounceMarker: true,
			want:         skewBounce,
		},
		{
			name:           "skew, marker, service-managed -> warn (never bounce managed)",
			skewErr:        skew,
			bounceMarker:   true,
			serviceManaged: true,
			want:           skewWarn,
		},
		{
			name:           "skew, no marker, service-managed -> warn",
			skewErr:        skew,
			serviceManaged: true,
			want:           skewWarn,
		},
		{
			name:    "non-skew error -> nothing",
			skewErr: errors.New("some other error"),
			want:    skewNothing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decideSkewAction(tt.skewErr, tt.bounceMarker, tt.serviceManaged)
			if got != tt.want {
				t.Errorf("decideSkewAction = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBounceMarkerEnabled(t *testing.T) {
	t.Run("absent by default", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("HOME", tmp)
		if bounceMarkerEnabled() {
			t.Error("bounceMarkerEnabled() should be false with no marker file")
		}
	})

	t.Run("present when marker exists", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("HOME", tmp)
		dir := filepath.Join(tmp, ".continuity")
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "autostart-bounce"), []byte("enabled\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if !bounceMarkerEnabled() {
			t.Error("bounceMarkerEnabled() should be true when marker file exists")
		}
	})
}
