package hooks

import "testing"

func TestResolveServerURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		bind string
		port string
		want string
	}{
		{
			name: "defaults match historical hardcoded value",
			want: "http://127.0.0.1:37777",
		},
		{
			name: "CONTINUITY_URL wins outright",
			url:  "http://example.test:9999/base",
			bind: "10.0.0.1",
			port: "1234",
			want: "http://example.test:9999/base",
		},
		{
			name: "CONTINUITY_PORT honored (matches serve)",
			port: "40000",
			want: "http://127.0.0.1:40000",
		},
		{
			name: "CONTINUITY_BIND honored (matches serve)",
			bind: "0.0.0.0",
			want: "http://0.0.0.0:37777",
		},
		{
			name: "BIND and PORT together",
			bind: "192.168.1.5",
			port: "8080",
			want: "http://192.168.1.5:8080",
		},
		{
			name: "whitespace-only env treated as unset",
			bind: "   ",
			port: "  ",
			want: "http://127.0.0.1:37777",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CONTINUITY_URL", tt.url)
			t.Setenv("CONTINUITY_BIND", tt.bind)
			t.Setenv("CONTINUITY_PORT", tt.port)
			if got := ResolveServerURL(); got != tt.want {
				t.Errorf("ResolveServerURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewClientUsesResolver(t *testing.T) {
	t.Setenv("CONTINUITY_URL", "")
	t.Setenv("CONTINUITY_BIND", "127.0.0.1")
	t.Setenv("CONTINUITY_PORT", "45454")
	c := NewClient()
	if c.ServerURL() != "http://127.0.0.1:45454" {
		t.Errorf("NewClient targeted %q, want http://127.0.0.1:45454", c.ServerURL())
	}
}
