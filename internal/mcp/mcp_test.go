package mcp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lazypower/continuity/internal/hooks"
)

// mockDaemon stands up an httptest server that answers the /api/* endpoints the
// MCP tools call, and points hooks.NewClient() at it via CONTINUITY_URL.
func mockDaemon(t *testing.T) *hooks.Client {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/memories", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			json.NewEncoder(w).Encode(map[string]any{
				"status": "created", "uri": "mem://user/events/test-fact",
			})
			return
		}
		// GET = show
		json.NewEncoder(w).Encode(map[string]any{
			"uri": "mem://user/events/test-fact", "category": "events",
			"summary": "a test fact", "body": "the fact body", "detail": "",
		})
	})
	mux.HandleFunc("/api/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"query": r.URL.Query().Get("q"), "count": 1,
			"results": []map[string]any{{
				"uri": "mem://user/events/test-fact", "category": "events",
				"l0_abstract": "a test fact", "score": 0.9,
			}},
		})
	})
	mux.HandleFunc("/api/memories/retract", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"status": "retracted", "uri": "mem://user/events/test-fact"})
	})

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	t.Setenv("CONTINUITY_URL", ts.URL)
	return hooks.NewClient()
}

// drive runs the server over a set of request lines and returns the JSON-RPC
// responses keyed by their string id.
func drive(t *testing.T, srv *Server, lines ...string) map[string]parsedResp {
	t.Helper()
	in := strings.NewReader(strings.Join(lines, "\n") + "\n")
	var out bytes.Buffer
	if err := srv.Serve(in, &out); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	byID := map[string]parsedResp{}
	for _, raw := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		var pr parsedResp
		if err := json.Unmarshal([]byte(raw), &pr); err != nil {
			t.Fatalf("bad response line %q: %v", raw, err)
		}
		byID[string(pr.ID)] = pr
	}
	return byID
}

type parsedResp struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func req(id int, method string, params any) string {
	m := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		m["params"] = params
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func toolText(t *testing.T, pr parsedResp) (string, bool) {
	t.Helper()
	var res struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	if err := json.Unmarshal(pr.Result, &res); err != nil {
		t.Fatalf("decode tool result: %v (raw %s)", err, pr.Result)
	}
	if len(res.Content) == 0 {
		t.Fatalf("empty tool content: %s", pr.Result)
	}
	return res.Content[0].Text, res.IsError
}

func TestInitializeAndToolsList(t *testing.T) {
	srv := NewServer(mockDaemon(t), "test-version")
	resp := drive(t, srv,
		req(1, "initialize", map[string]any{"protocolVersion": "2025-06-18"}),
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`, // notification: no reply
		req(2, "tools/list", nil),
	)

	// The notification must not have produced a response line.
	if len(resp) != 2 {
		t.Fatalf("want 2 responses (notification suppressed), got %d: %v", len(resp), resp)
	}

	var initRes struct {
		ProtocolVersion string `json:"protocolVersion"`
		ServerInfo      struct {
			Name string `json:"name"`
		} `json:"serverInfo"`
	}
	if err := json.Unmarshal(resp["1"].Result, &initRes); err != nil {
		t.Fatalf("decode initialize: %v", err)
	}
	if initRes.ServerInfo.Name != "continuity" {
		t.Errorf("serverInfo.name = %q, want continuity", initRes.ServerInfo.Name)
	}
	if initRes.ProtocolVersion != "2025-06-18" {
		t.Errorf("protocolVersion = %q, want echo of client version", initRes.ProtocolVersion)
	}

	var listRes struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(resp["2"].Result, &listRes); err != nil {
		t.Fatalf("decode tools/list: %v", err)
	}
	names := map[string]bool{}
	for _, tl := range listRes.Tools {
		names[tl.Name] = true
	}
	for _, want := range []string{"remember", "search", "show", "tree", "profile", "retract"} {
		if !names[want] {
			t.Errorf("tools/list missing %q (got %v)", want, names)
		}
	}
}

func TestToolCallRememberAndSearch(t *testing.T) {
	srv := NewServer(mockDaemon(t), "test-version")
	resp := drive(t, srv,
		req(3, "tools/call", map[string]any{
			"name": "remember",
			"arguments": map[string]any{
				"category": "events", "name": "test-fact",
				"summary": "a test fact", "body": "the fact body, long enough to be real",
			},
		}),
		req(4, "tools/call", map[string]any{
			"name":      "search",
			"arguments": map[string]any{"query": "test fact"},
		}),
	)

	text, isErr := toolText(t, resp["3"])
	if isErr {
		t.Fatalf("remember reported error: %s", text)
	}
	if !strings.Contains(text, "created") || !strings.Contains(text, "mem://user/events/test-fact") {
		t.Errorf("remember text = %q", text)
	}

	text, isErr = toolText(t, resp["4"])
	if isErr {
		t.Fatalf("search reported error: %s", text)
	}
	if !strings.Contains(text, "mem://user/events/test-fact") {
		t.Errorf("search text = %q", text)
	}
}

func TestToolCallValidationIsError(t *testing.T) {
	srv := NewServer(mockDaemon(t), "test-version")
	resp := drive(t, srv,
		req(5, "tools/call", map[string]any{
			"name":      "remember",
			"arguments": map[string]any{"category": "events", "name": "x"}, // missing summary/body
		}),
	)
	text, isErr := toolText(t, resp["5"])
	if !isErr {
		t.Fatalf("expected isError for missing fields, got %q", text)
	}
	if !strings.Contains(text, "required") {
		t.Errorf("validation text = %q", text)
	}
}

func TestUnknownMethodIsProtocolError(t *testing.T) {
	srv := NewServer(mockDaemon(t), "test-version")
	resp := drive(t, srv, req(6, "does/not/exist", nil))
	if resp["6"].Error == nil {
		t.Fatalf("expected JSON-RPC error for unknown method")
	}
	if resp["6"].Error.Code != codeMethodNotFound {
		t.Errorf("error code = %d, want %d", resp["6"].Error.Code, codeMethodNotFound)
	}
}

func TestDaemonDownIsActionable(t *testing.T) {
	// Stand a server up then tear it down so the URL refuses connections.
	ts := httptest.NewServer(http.NewServeMux())
	url := ts.URL
	ts.Close()
	t.Setenv("CONTINUITY_URL", url)

	srv := NewServer(hooks.NewClient(), "test-version")
	resp := drive(t, srv,
		req(7, "tools/call", map[string]any{
			"name": "search", "arguments": map[string]any{"query": "anything"},
		}),
	)
	text, isErr := toolText(t, resp["7"])
	if !isErr {
		t.Fatalf("expected isError when daemon is down, got %q", text)
	}
	if !strings.Contains(text, "not running") {
		t.Errorf("daemon-down text = %q, want mention of 'not running'", text)
	}
}
