// Package mcp exposes continuity's memory tools over the Model Context Protocol
// (MCP) so an agent can invoke them directly instead of shelling out to the CLI.
//
// The server speaks JSON-RPC 2.0 over stdio — newline-delimited messages, the
// MCP stdio transport — and is a thin client of the running `continuity serve`
// daemon: every tool maps to an existing /api/* endpoint, exactly as the CLI
// does. There is no engine or store access here, only protocol + HTTP. The
// binary IS the MCP server, the same way it IS the hook handler.
//
// The point is ergonomic: MCP tool arguments are structured JSON, so a
// multi-line memory body never passes through a shell — no quoting, no escaping,
// no heredocs. That is the friction the CLI path could not shed.
package mcp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"

	"github.com/lazypower/continuity/internal/hooks"
)

// protocolVersion is the MCP revision this server implements. On initialize we
// echo the client's requested version when it offers one (loose negotiation),
// falling back to this. The tool surface is version-agnostic.
const protocolVersion = "2025-06-18"

// maxMessageBytes bounds a single JSON-RPC line. A remember call can carry a 40k
// L2 detail plus JSON overhead, so the default 64k scanner token is too small;
// 16MB clears any real memory payload with room to spare.
const maxMessageBytes = 16 * 1024 * 1024

// Server is a stdio MCP server backed by the continuity daemon.
type Server struct {
	client  *hooks.Client
	version string
	tools   []toolDef
	index   map[string]int
}

// NewServer builds a server that reaches the daemon through client.
func NewServer(client *hooks.Client, version string) *Server {
	s := &Server{client: client, version: version, index: map[string]int{}}
	s.registerTools()
	return s
}

// --- JSON-RPC 2.0 wire types ---

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"` // absent ⇒ notification (no reply)
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// JSON-RPC error codes we use (subset of the spec).
const (
	codeParseError     = -32700
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
)

// Serve runs the read/dispatch/write loop until in is exhausted (client closed
// the pipe) or a write fails. Messages are newline-delimited JSON; each reply is
// flushed immediately so a client that waits for one response before sending the
// next never stalls.
func (s *Server) Serve(in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 0, 64*1024), maxMessageBytes)
	w := bufio.NewWriter(out)

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		resp := s.handleLine(line)
		if resp == nil {
			continue // notification: no reply
		}
		if err := writeMessage(w, resp); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func writeMessage(w *bufio.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	if err := w.WriteByte('\n'); err != nil {
		return err
	}
	return w.Flush() // flush per message — the client blocks on our reply
}

func (s *Server) handleLine(line []byte) *rpcResponse {
	var req rpcRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return s.errorResponse(json.RawMessage("null"), codeParseError, "parse error")
	}
	return s.dispatch(&req)
}

// dispatch routes a single message. A message without an id is a notification
// (e.g. notifications/initialized) and MUST NOT receive a response, so we return
// nil for those regardless of method.
func (s *Server) dispatch(req *rpcRequest) *rpcResponse {
	if len(req.ID) == 0 {
		return nil
	}
	switch req.Method {
	case "initialize":
		return s.result(req.ID, s.initializeResult(req.Params))
	case "ping":
		return s.result(req.ID, map[string]any{})
	case "tools/list":
		return s.result(req.ID, map[string]any{"tools": s.toolSchemas()})
	case "tools/call":
		return s.callTool(req.ID, req.Params)
	default:
		return s.errorResponse(req.ID, codeMethodNotFound, "method not found: "+req.Method)
	}
}

func (s *Server) initializeResult(params json.RawMessage) map[string]any {
	version := protocolVersion
	if len(params) > 0 {
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if json.Unmarshal(params, &p) == nil && p.ProtocolVersion != "" {
			version = p.ProtocolVersion // echo the client's version when supported
		}
	}
	return map[string]any{
		"protocolVersion": version,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo": map[string]any{
			"name":    "continuity",
			"version": s.version,
		},
	}
}

// callTool dispatches a tools/call. Tool *execution* failures come back as a
// successful JSON-RPC result with isError:true (the MCP contract) so the agent
// sees the message; only malformed calls become protocol-level errors.
func (s *Server) callTool(id, params json.RawMessage) *rpcResponse {
	var p struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return s.errorResponse(id, codeInvalidParams, "invalid tools/call params")
	}
	idx, ok := s.index[p.Name]
	if !ok {
		return s.errorResponse(id, codeInvalidParams, "unknown tool: "+p.Name)
	}
	text, err := s.tools[idx].handler(p.Arguments)
	if err != nil {
		return s.result(id, toolContent(err.Error(), true))
	}
	return s.result(id, toolContent(text, false))
}

func toolContent(text string, isErr bool) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": text}},
		"isError": isErr,
	}
}

func (s *Server) toolSchemas() []map[string]any {
	out := make([]map[string]any, len(s.tools))
	for i, t := range s.tools {
		out[i] = map[string]any{
			"name":        t.name,
			"description": t.description,
			"inputSchema": t.inputSchema,
		}
	}
	return out
}

func (s *Server) result(id json.RawMessage, res any) *rpcResponse {
	return &rpcResponse{JSONRPC: "2.0", ID: idOrNull(id), Result: res}
}

func (s *Server) errorResponse(id json.RawMessage, code int, msg string) *rpcResponse {
	return &rpcResponse{JSONRPC: "2.0", ID: idOrNull(id), Error: &rpcError{Code: code, Message: msg}}
}

func idOrNull(id json.RawMessage) json.RawMessage {
	if len(id) == 0 {
		return json.RawMessage("null")
	}
	return id
}
