package hooks

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lazypower/continuity/internal/buildinfo"
	"github.com/lazypower/continuity/internal/store"
)

func TestHealthStatusDecode(t *testing.T) {
	payload := `{
		"status": "ok",
		"version": "v1.2.3 (abcdef0)",
		"uptime": 12.5,
		"db": true,
		"api_version": 1,
		"schema_head": 9,
		"schema_current": 9,
		"pid": 4242,
		"started_at": 1700000000,
		"db_path": "/home/user/.continuity/continuity.db",
		"exe": "/usr/local/bin/continuity"
	}`

	var hs HealthStatus
	if err := json.Unmarshal([]byte(payload), &hs); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if hs.Status != "ok" {
		t.Errorf("Status = %q, want ok", hs.Status)
	}
	if hs.Version != "v1.2.3 (abcdef0)" {
		t.Errorf("Version = %q", hs.Version)
	}
	if hs.Uptime != 12.5 {
		t.Errorf("Uptime = %v, want 12.5", hs.Uptime)
	}
	if !hs.DB {
		t.Error("DB = false, want true")
	}
	if hs.APIVersion != 1 {
		t.Errorf("APIVersion = %d, want 1", hs.APIVersion)
	}
	if hs.SchemaHead != 9 {
		t.Errorf("SchemaHead = %d, want 9", hs.SchemaHead)
	}
	if hs.SchemaCurrent != 9 {
		t.Errorf("SchemaCurrent = %d, want 9", hs.SchemaCurrent)
	}
	if hs.PID != 4242 {
		t.Errorf("PID = %d, want 4242", hs.PID)
	}
	if hs.StartedAt != 1700000000 {
		t.Errorf("StartedAt = %d, want 1700000000", hs.StartedAt)
	}
	if hs.DBPath != "/home/user/.continuity/continuity.db" {
		t.Errorf("DBPath = %q", hs.DBPath)
	}
	if hs.Exe != "/usr/local/bin/continuity" {
		t.Errorf("Exe = %q", hs.Exe)
	}
}

func TestCompatibilityCheck(t *testing.T) {
	localAPI := buildinfo.APIVersion
	localHead := store.HeadSchemaVersion()
	localVer := buildinfo.VersionString()

	tests := []struct {
		name        string
		hs          HealthStatus
		wantSkew    bool
		wantAPIDiff bool
		wantSchDiff bool
	}{
		{
			name: "identical contract -> no skew",
			hs:   HealthStatus{Version: localVer, APIVersion: localAPI, SchemaHead: localHead},
		},
		{
			name: "same api+schema, different commit/version string -> no skew",
			hs:   HealthStatus{Version: "v0.0.1-dirty (deadbee)", APIVersion: localAPI, SchemaHead: localHead},
		},
		{
			name:        "server api_version lower -> skew on api",
			hs:          HealthStatus{Version: "old", APIVersion: localAPI - 1, SchemaHead: localHead},
			wantSkew:    true,
			wantAPIDiff: true,
		},
		{
			name:        "server schema_head lower -> skew on schema",
			hs:          HealthStatus{Version: "old", APIVersion: localAPI, SchemaHead: localHead - 1},
			wantSkew:    true,
			wantSchDiff: true,
		},
		{
			name:        "server ahead on both -> skew on both dimensions",
			hs:          HealthStatus{Version: "newer", APIVersion: localAPI + 1, SchemaHead: localHead + 1},
			wantSkew:    true,
			wantAPIDiff: true,
			wantSchDiff: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CompatibilityCheck(&tt.hs)
			if !tt.wantSkew {
				if err != nil {
					t.Fatalf("expected no skew, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected SkewError, got nil")
			}
			var skew *SkewError
			if !errors.As(err, &skew) {
				t.Fatalf("expected *SkewError, got %T: %v", err, err)
			}
			if skew.APIVersionMismatch != tt.wantAPIDiff {
				t.Errorf("APIVersionMismatch = %v, want %v", skew.APIVersionMismatch, tt.wantAPIDiff)
			}
			if skew.SchemaMismatch != tt.wantSchDiff {
				t.Errorf("SchemaMismatch = %v, want %v", skew.SchemaMismatch, tt.wantSchDiff)
			}
			if skew.ServerVersion != tt.hs.Version {
				t.Errorf("ServerVersion = %q, want %q", skew.ServerVersion, tt.hs.Version)
			}
			if skew.LocalVersion != localVer {
				t.Errorf("LocalVersion = %q, want %q", skew.LocalVersion, localVer)
			}
		})
	}
}

func TestClientStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/health" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":      "ok",
			"version":     "v9.9.9 (cafef00)",
			"api_version": 7,
			"schema_head": 42,
			"db":          true,
		})
	}))
	defer srv.Close()

	c := &Client{http: srv.Client(), serverURL: srv.URL}
	hs, err := c.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if hs.Version != "v9.9.9 (cafef00)" {
		t.Errorf("Version = %q", hs.Version)
	}
	if hs.APIVersion != 7 {
		t.Errorf("APIVersion = %d, want 7", hs.APIVersion)
	}
	if hs.SchemaHead != 42 {
		t.Errorf("SchemaHead = %d, want 42", hs.SchemaHead)
	}
}
