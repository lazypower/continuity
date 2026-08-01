package mcp

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/lazypower/continuity/internal/hooks"
)

// toolDef is one MCP tool: its advertised schema plus a handler that returns the
// human/agent-readable text for a tools/call result. A handler error becomes an
// isError result (see callTool).
type toolDef struct {
	name        string
	description string
	inputSchema map[string]any
	handler     func(args json.RawMessage) (string, error)
}

// validCategory mirrors the server's taxonomy so an obviously-wrong category is
// rejected before a round trip. The daemon remains the authority.
var validCategory = map[string]bool{
	"profile": true, "preferences": true, "feedback": true,
	"entities": true, "events": true, "patterns": true,
	"cases": true, "moments": true, "reference": true,
}

const categoryList = "profile, preferences, feedback, entities, events, patterns, cases, moments, reference"

func (s *Server) registerTools() {
	s.tools = []toolDef{
		{
			name:        "remember",
			description: "Store a memory directly in the continuity tree (no LLM). For durable facts, preferences, feedback, decisions. Tiers: summary=L0 (one sentence, <=200 chars, injected every session), body=L1 (<=2000 chars, compress aggressively), detail=L2 (<=40000 chars, optional, on-demand only).",
			inputSchema: object(props{
				"category":              enumProp("Memory category. One of: "+categoryList, keys(validCategory)),
				"name":                  strProp("Short kebab-case URI slug, e.g. 'devbox-tooling'"),
				"summary":               strProp("L0 abstract — one sentence, max 200 chars"),
				"body":                  strProp("L1 overview — max 2000 chars, compress detail aggressively"),
				"detail":                strProp("L2 full content — max 40000 chars (optional)"),
				"session_id":            strProp("Session id for provenance (optional)"),
				"acknowledge_retracted": boolProp("Proceed past a dedup match against a retracted memory (optional)"),
			}, "category", "name", "summary", "body"),
			handler: s.toolRemember,
		},
		{
			name:        "search",
			description: "Search the memory tree. Returns pointers (score, URI, L0 summary), not full bodies — deepen with the show tool.",
			inputSchema: object(props{
				"query":    strProp("What to search for"),
				"limit":    intProp("Max results (default 10)"),
				"category": strProp("Restrict to one category (optional): " + categoryList),
				"smart":    boolProp("Use LLM-assisted search mode (optional)"),
			}, "query"),
			handler: s.toolSearch,
		},
		{
			name:        "show",
			description: "Fetch one memory by URI and return its full summary/body/detail. Read this before updating a memory in place so appends don't clobber unseen content.",
			inputSchema: object(props{
				"uri":               strProp("Memory URI, e.g. mem://user/preferences/devbox (the mem:// prefix is optional)"),
				"include_retracted": boolProp("Reveal the reason and original content of a retracted memory (optional)"),
			}, "uri"),
			handler: s.toolShow,
		},
		{
			name:        "tree",
			description: "Browse the memory tree. Omit uri to list roots; pass a URI to list its children.",
			inputSchema: object(props{
				"uri":               strProp("Directory URI to list (optional; omit for roots)"),
				"include_retracted": boolProp("Include retracted nodes (optional)"),
			}),
			handler: s.toolTree,
		},
		{
			name:        "profile",
			description: "Show the relational profile — how the user works (feedback style, autonomy, corrections) — plus profile and preference nodes.",
			inputSchema: object(props{}),
			handler:     s.toolProfile,
		},
		{
			name:        "retract",
			description: "Retract a memory you wrote (tombstone or supersession). It stays as a marker but is excluded from default reads. A reason is required.",
			inputSchema: object(props{
				"uri":           strProp("URI of the memory to retract (must start with mem://)"),
				"reason":        strProp("Why it is being retracted (required, one sentence)"),
				"superseded_by": strProp("URI of the memory that replaces this one (optional; makes it a supersession)"),
			}, "uri", "reason"),
			handler: s.toolRetract,
		},
	}
	s.index = make(map[string]int, len(s.tools))
	for i, t := range s.tools {
		s.index[t.name] = i
	}
}

// --- handlers ---

func (s *Server) toolRemember(args json.RawMessage) (string, error) {
	var in struct {
		Category             string `json:"category"`
		Name                 string `json:"name"`
		Summary              string `json:"summary"`
		Body                 string `json:"body"`
		Detail               string `json:"detail"`
		SessionID            string `json:"session_id"`
		AcknowledgeRetracted bool   `json:"acknowledge_retracted"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return "", fmt.Errorf("invalid arguments: %v", err)
	}
	if in.Category == "" || in.Name == "" || in.Summary == "" || in.Body == "" {
		return "", fmt.Errorf("category, name, summary, and body are required")
	}
	if !validCategory[in.Category] {
		return "", fmt.Errorf("invalid category %q (valid: %s)", in.Category, categoryList)
	}

	payload := map[string]any{
		"category": in.Category,
		"name":     in.Name,
		"summary":  in.Summary,
		"body":     in.Body,
	}
	if in.Detail != "" {
		payload["detail"] = in.Detail
	}
	if in.SessionID != "" {
		payload["session_id"] = in.SessionID
	}
	if in.AcknowledgeRetracted {
		payload["acknowledge_retracted"] = true
	}

	data, postErr := s.post("/api/memories", payload)

	var resp struct {
		Status      string   `json:"status"`
		URI         string   `json:"uri"`
		MatchedURIs []string `json:"matched_uris"`
		Hint        string   `json:"hint"`
		Error       string   `json:"error"`
	}
	_ = json.Unmarshal(data, &resp) // best-effort; error paths carry structured bodies too

	// A dedup match against a retracted memory comes back structured on a non-2xx.
	// Surface the matched URIs and hint so the agent can inspect and decide.
	if resp.Status == "matches_retracted" {
		var b strings.Builder
		b.WriteString("write blocked — candidate matches retracted memory:")
		for _, u := range resp.MatchedURIs {
			fmt.Fprintf(&b, "\n  - %s", u)
		}
		if resp.Hint != "" {
			fmt.Fprintf(&b, "\n%s", resp.Hint)
		}
		return "", fmt.Errorf("%s", b.String())
	}
	if postErr != nil {
		if resp.Error != "" {
			return "", fmt.Errorf("%s", resp.Error)
		}
		return "", postErr
	}
	if resp.Error != "" {
		return "", fmt.Errorf("%s", resp.Error)
	}
	return fmt.Sprintf("%s: %s [%s]", resp.Status, resp.URI, in.Category), nil
}

func (s *Server) toolSearch(args json.RawMessage) (string, error) {
	var in struct {
		Query    string `json:"query"`
		Limit    int    `json:"limit"`
		Category string `json:"category"`
		Smart    bool   `json:"smart"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return "", fmt.Errorf("invalid arguments: %v", err)
	}
	if strings.TrimSpace(in.Query) == "" {
		return "", fmt.Errorf("query is required")
	}
	if in.Limit <= 0 {
		in.Limit = 10
	}

	params := url.Values{}
	params.Set("q", in.Query)
	params.Set("limit", strconv.Itoa(in.Limit))
	if in.Category != "" {
		params.Set("category", in.Category)
	}
	if in.Smart {
		params.Set("mode", "search")
	}

	data, err := s.get("/api/search?" + params.Encode())
	if err != nil {
		return "", err
	}
	var resp struct {
		Count   int `json:"count"`
		Results []struct {
			URI        string  `json:"uri"`
			Category   string  `json:"category"`
			L0Abstract string  `json:"l0_abstract"`
			Score      float64 `json:"score"`
		} `json:"results"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", fmt.Errorf("parse response: %v", err)
	}
	if resp.Count == 0 || len(resp.Results) == 0 {
		return "No results found.", nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%d result(s):", resp.Count)
	for i, r := range resp.Results {
		fmt.Fprintf(&b, "\n%d. [%.3f] %s\n   %s [%s]", i+1, r.Score, r.URI, r.L0Abstract, r.Category)
	}
	return b.String(), nil
}

func (s *Server) toolShow(args json.RawMessage) (string, error) {
	var in struct {
		URI              string `json:"uri"`
		IncludeRetracted bool   `json:"include_retracted"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return "", fmt.Errorf("invalid arguments: %v", err)
	}
	uri := normalizeURI(in.URI)
	if uri == "" {
		return "", fmt.Errorf("uri is required")
	}

	params := url.Values{}
	params.Set("uri", uri)
	if in.IncludeRetracted {
		params.Set("include_retracted", "true")
	}

	data, err := s.get("/api/memories?" + params.Encode())
	var resp struct {
		URI             string `json:"uri"`
		Category        string `json:"category"`
		Summary         string `json:"summary"`
		Body            string `json:"body"`
		Detail          string `json:"detail"`
		Retracted       bool   `json:"retracted"`
		TombstoneReason string `json:"tombstone_reason"`
		SupersededBy    string `json:"superseded_by"`
		Error           string `json:"error"`
	}
	_ = json.Unmarshal(data, &resp)
	if err != nil {
		if resp.Error != "" {
			return "", fmt.Errorf("%s", resp.Error)
		}
		return "", err
	}
	if resp.Error != "" {
		return "", fmt.Errorf("%s", resp.Error)
	}

	// Retracted-without-flag: metadata only, mirroring the CLI's absence contract.
	if resp.Retracted && !in.IncludeRetracted {
		out := fmt.Sprintf("%s [%s] [retracted]", resp.URI, resp.Category)
		if resp.SupersededBy != "" {
			out += "\n  superseded by: " + resp.SupersededBy
		}
		out += "\n(reason and original content hidden — pass include_retracted to reveal)"
		return out, nil
	}

	var b strings.Builder
	header := fmt.Sprintf("%s [%s]", resp.URI, resp.Category)
	if resp.Retracted {
		header += " [retracted]"
	}
	b.WriteString(header)
	if resp.Retracted {
		fmt.Fprintf(&b, "\n\n## Retraction\nReason: %s", resp.TombstoneReason)
		if resp.SupersededBy != "" {
			fmt.Fprintf(&b, "\nSuperseded by: %s", resp.SupersededBy)
		}
	}
	fmt.Fprintf(&b, "\n\n## Summary\n%s", resp.Summary)
	body := resp.Body
	if body == "" {
		body = "(empty)"
	}
	fmt.Fprintf(&b, "\n\n## Body\n%s", body)
	if resp.Detail != "" {
		fmt.Fprintf(&b, "\n\n## Detail\n%s", resp.Detail)
	}
	return b.String(), nil
}

func (s *Server) toolTree(args json.RawMessage) (string, error) {
	var in struct {
		URI              string `json:"uri"`
		IncludeRetracted bool   `json:"include_retracted"`
	}
	if len(args) > 0 {
		if err := json.Unmarshal(args, &in); err != nil {
			return "", fmt.Errorf("invalid arguments: %v", err)
		}
	}

	params := url.Values{}
	if in.URI != "" {
		params.Set("uri", normalizeURI(in.URI))
	}
	if in.IncludeRetracted {
		params.Set("include_retracted", "true")
	}
	path := "/api/tree"
	if enc := params.Encode(); enc != "" {
		path += "?" + enc
	}

	data, err := s.get(path)
	if err != nil {
		return "", err
	}
	var resp struct {
		URI   string `json:"uri"`
		Nodes []struct {
			URI        string `json:"uri"`
			NodeType   string `json:"node_type"`
			Category   string `json:"category"`
			L0Abstract string `json:"l0_abstract"`
			Children   int    `json:"children"`
			Retracted  bool   `json:"retracted"`
			Pinned     bool   `json:"pinned"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", fmt.Errorf("parse response: %v", err)
	}
	if len(resp.Nodes) == 0 {
		return "(empty)", nil
	}
	var b strings.Builder
	if resp.URI != "" {
		fmt.Fprintf(&b, "%s\n", resp.URI)
	}
	for _, n := range resp.Nodes {
		line := n.URI
		if n.NodeType == "dir" {
			fmt.Fprintf(&b, "%s/ (%d)\n", line, n.Children)
			continue
		}
		flags := ""
		if n.Pinned {
			flags += " [pinned]"
		}
		if n.Retracted {
			flags += " [retracted]"
		}
		fmt.Fprintf(&b, "%s%s\n", line, flags)
		if n.L0Abstract != "" {
			fmt.Fprintf(&b, "   %s\n", n.L0Abstract)
		}
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

func (s *Server) toolProfile(args json.RawMessage) (string, error) {
	data, err := s.get("/api/profile")
	if err != nil {
		return "", err
	}
	var resp struct {
		RelationalProfile string `json:"relational_profile"`
		Nodes             []struct {
			URI        string `json:"uri"`
			Category   string `json:"category"`
			L0Abstract string `json:"l0_abstract"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", fmt.Errorf("parse response: %v", err)
	}
	var b strings.Builder
	if strings.TrimSpace(resp.RelationalProfile) != "" {
		b.WriteString("## Relational Profile\n")
		b.WriteString(resp.RelationalProfile)
	} else {
		b.WriteString("No relational profile yet.")
	}
	if len(resp.Nodes) > 0 {
		b.WriteString("\n\n## Profile & Preference Nodes")
		for _, n := range resp.Nodes {
			fmt.Fprintf(&b, "\n- %s [%s]: %s", n.URI, n.Category, n.L0Abstract)
		}
	}
	return b.String(), nil
}

func (s *Server) toolRetract(args json.RawMessage) (string, error) {
	var in struct {
		URI          string `json:"uri"`
		Reason       string `json:"reason"`
		SupersededBy string `json:"superseded_by"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return "", fmt.Errorf("invalid arguments: %v", err)
	}
	in.URI = strings.TrimSpace(in.URI)
	if !strings.HasPrefix(in.URI, "mem://") {
		return "", fmt.Errorf("invalid uri %q: must start with mem://", in.URI)
	}
	if strings.TrimSpace(in.Reason) == "" {
		return "", fmt.Errorf("reason is required")
	}
	if in.SupersededBy != "" && !strings.HasPrefix(in.SupersededBy, "mem://") {
		return "", fmt.Errorf("invalid superseded_by %q: must start with mem://", in.SupersededBy)
	}

	payload := map[string]any{"uri": in.URI, "reason": in.Reason}
	if in.SupersededBy != "" {
		payload["superseded_by"] = in.SupersededBy
	}

	data, err := s.post("/api/memories/retract", payload)
	var resp struct {
		Status       string `json:"status"`
		URI          string `json:"uri"`
		SupersededBy string `json:"superseded_by"`
		Error        string `json:"error"`
	}
	_ = json.Unmarshal(data, &resp)
	if err != nil {
		if resp.Error != "" {
			return "", fmt.Errorf("%s", resp.Error)
		}
		return "", err
	}
	if resp.Error != "" {
		return "", fmt.Errorf("%s", resp.Error)
	}
	if resp.SupersededBy != "" {
		return fmt.Sprintf("%s: %s → %s", resp.Status, resp.URI, resp.SupersededBy), nil
	}
	return fmt.Sprintf("%s: %s", resp.Status, resp.URI), nil
}

// --- daemon plumbing ---

// post marshals payload and POSTs it, returning the response bytes on both the
// success and (structured) error paths. A dead daemon becomes an actionable
// message rather than a raw dial error.
func (s *Server) post(path string, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	data, err := s.client.Post(path, body)
	if err != nil {
		return data, s.daemonError(err)
	}
	return data, nil
}

func (s *Server) get(path string) ([]byte, error) {
	data, err := s.client.Get(path)
	if err != nil {
		return data, s.daemonError(err)
	}
	return data, nil
}

// daemonError translates a transport failure into the same guidance the CLI
// gives when the server is down, while leaving genuine HTTP-level errors intact.
func (s *Server) daemonError(err error) error {
	// A timeout means the daemon answered — just slowly. Probing health here
	// would report it as absent, since /api/health stays fast no matter how
	// large the tables have grown (issue #72).
	if hooks.IsTimeout(err) {
		return s.client.DescribeError(err)
	}
	if healthErr := s.client.CheckHealth(); healthErr != nil {
		return healthErr
	}
	return err
}

func normalizeURI(uri string) string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return ""
	}
	if !strings.HasPrefix(uri, "mem://") {
		uri = "mem://" + strings.TrimPrefix(uri, "/")
	}
	return uri
}

// --- tiny JSON-schema builders (keep the tool table readable) ---

type props = map[string]any

func object(p props, required ...string) map[string]any {
	schema := map[string]any{"type": "object", "properties": p}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func strProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}
func intProp(desc string) map[string]any {
	return map[string]any{"type": "integer", "description": desc}
}
func boolProp(desc string) map[string]any {
	return map[string]any{"type": "boolean", "description": desc}
}

func enumProp(desc string, values []string) map[string]any {
	return map[string]any{"type": "string", "description": desc, "enum": values}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
