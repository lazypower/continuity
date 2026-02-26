package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/lazypower/continuity/internal/engine"
)

func (s *Server) handleSessionInit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SessionID string `json:"session_id"`
		Project   string `json:"project"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if req.SessionID == "" {
		http.Error(w, `{"error":"session_id required"}`, http.StatusBadRequest)
		return
	}

	sess, err := s.db.InitSession(req.SessionID, req.Project)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"session_id": sess.SessionID,
		"status":     sess.Status,
		"tool_count": sess.ToolCount,
	})
}

func (s *Server) handleAddObservation(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "sessionID")

	var req struct {
		ToolName     string `json:"tool_name"`
		ToolInput    string `json:"tool_input"`
		ToolResponse string `json:"tool_response"`
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"read body failed"}`, http.StatusBadRequest)
		return
	}
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if err := s.db.AddObservation(sessionID, req.ToolName, req.ToolInput, req.ToolResponse); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	// Also increment tool count on the session
	s.db.IncrementToolCount(sessionID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleCompleteSession(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "sessionID")

	if err := s.db.CompleteSession(sessionID); err != nil {
		// Not finding an active session is not a server error — the session
		// may have already been completed or never existed. Log but return OK.
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "note": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "completed"})
}

func (s *Server) handleEndSession(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "sessionID")

	if err := s.db.EndSession(sessionID); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ended"})
}

func (s *Server) handleExtractSession(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "sessionID")

	var req struct {
		TranscriptPath string `json:"transcript_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}

	if s.engine == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "engine not configured"})
		return
	}

	// Async extraction — return 202 immediately
	go func() {
		if err := s.engine.ExtractSession(sessionID, req.TranscriptPath); err != nil {
			log.Printf("extraction failed for %s: %v", sessionID, err)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "extracting"})
}

func (s *Server) handleSignal(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "sessionID")

	var req struct {
		Prompt string `json:"prompt"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if req.Prompt == "" {
		http.Error(w, `{"error":"prompt required"}`, http.StatusBadRequest)
		return
	}

	if s.engine == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "engine not configured"})
		return
	}

	// Async extraction — return 202 immediately
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := s.engine.ExtractSignal(ctx, sessionID, req.Prompt); err != nil {
			log.Printf("signal extraction failed for %s: %v", sessionID, err)
		}
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "processing"})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, `{"error":"q parameter required"}`, http.StatusBadRequest)
		return
	}

	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "find"
	}

	limit := 10
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	category := r.URL.Query().Get("category")

	if s.engine == nil || s.engine.Embedder == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": "search not available — no embedder configured"})
		return
	}

	opts := engine.SearchOpts{
		Limit:    limit,
		Category: category,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	var results []engine.SearchResult
	var err error

	switch mode {
	case "search":
		results, err = engine.Search(ctx, s.db, s.engine.Embedder, s.engine.LLM, query, opts)
	default:
		results, err = engine.Find(ctx, s.db, s.engine.Embedder, query, opts)
	}

	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	type resultJSON struct {
		URI        string  `json:"uri"`
		Category   string  `json:"category"`
		L0Abstract string  `json:"l0_abstract"`
		L1Overview string  `json:"l1_overview,omitempty"`
		Score      float64 `json:"score"`
		Similarity float64 `json:"similarity"`
		Relevance  float64 `json:"relevance"`
	}

	out := make([]resultJSON, len(results))
	for i, r := range results {
		out[i] = resultJSON{
			URI:        r.Node.URI,
			Category:   r.Node.Category,
			L0Abstract: r.Node.L0Abstract,
			L1Overview: r.Node.L1Overview,
			Score:      r.Score,
			Similarity: r.Similarity,
			Relevance:  r.Node.Relevance,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"query":   query,
		"mode":    mode,
		"count":   len(out),
		"results": out,
	})
}

func (s *Server) handleProfile(w http.ResponseWriter, r *http.Request) {
	relProfile, err := s.db.GetNodeByURI("mem://user/profile/communication")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	profileText := ""
	if relProfile != nil {
		profileText = relProfile.L1Overview
	}

	// Collect user profile + preference nodes
	type nodeJSON struct {
		URI        string  `json:"uri"`
		Category   string  `json:"category"`
		L0Abstract string  `json:"l0_abstract"`
		L1Overview string  `json:"l1_overview,omitempty"`
		Relevance  float64 `json:"relevance"`
	}

	var profileNodes []nodeJSON
	profiles, _ := s.db.FindByCategory("profile")
	for _, n := range profiles {
		if n.URI == "mem://user/profile/communication" {
			continue
		}
		if n.L0Abstract != "" {
			profileNodes = append(profileNodes, nodeJSON{n.URI, n.Category, n.L0Abstract, n.L1Overview, n.Relevance})
		}
	}

	prefs, _ := s.db.FindByCategory("preferences")
	for _, n := range prefs {
		if n.L0Abstract != "" {
			profileNodes = append(profileNodes, nodeJSON{n.URI, n.Category, n.L0Abstract, n.L1Overview, n.Relevance})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"relational_profile": profileText,
		"nodes":              profileNodes,
	})
}

func (s *Server) handleTree(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("uri")

	type treeNodeJSON struct {
		URI        string `json:"uri"`
		NodeType   string `json:"node_type"`
		Category   string `json:"category"`
		L0Abstract string `json:"l0_abstract,omitempty"`
		L1Overview string `json:"l1_overview,omitempty"`
		Children   int    `json:"children,omitempty"`
	}

	var nodes []treeNodeJSON

	if uri == "" {
		// List roots
		roots, err := s.db.ListRoots()
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		for _, r := range roots {
			count, _ := s.db.CountChildren(r.URI)
			nodes = append(nodes, treeNodeJSON{
				URI:      r.URI,
				NodeType: r.NodeType,
				Category: r.Category,
				Children: count,
			})
		}
	} else {
		// List children
		children, err := s.db.GetChildren(uri)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		for _, c := range children {
			tn := treeNodeJSON{
				URI:        c.URI,
				NodeType:   c.NodeType,
				Category:   c.Category,
				L0Abstract: c.L0Abstract,
				L1Overview: c.L1Overview,
			}
			if c.NodeType == "dir" {
				count, _ := s.db.CountChildren(c.URI)
				tn.Children = count
			}
			nodes = append(nodes, tn)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"uri":   uri,
		"nodes": nodes,
	})
}
