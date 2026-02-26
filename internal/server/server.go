package server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/lazypower/continuity/internal/store"
)

// Server is the continuity HTTP API server.
type Server struct {
	db      *store.DB
	router  chi.Router
	version string
	started time.Time
}

// New creates a new Server with the given database and version string.
func New(db *store.DB, version string) *Server {
	s := &Server{
		db:      db,
		version: version,
		started: time.Now(),
	}
	s.routes()
	return s
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Server) routes() {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)

	r.Route("/api", func(r chi.Router) {
		r.Get("/health", s.handleHealth)

		// Phase 1: session + observation + context routes
		r.Post("/sessions/init", s.handleSessionInit)
		r.Post("/sessions/{sessionID}/observations", s.handleAddObservation)
		r.Post("/sessions/{sessionID}/complete", s.handleCompleteSession)
		r.Post("/sessions/{sessionID}/end", s.handleEndSession)
		r.Get("/context", s.handleGetContext)

		// Stub routes — return 501 until implemented
		r.Get("/search", stub("search"))
		r.Get("/profile", stub("profile"))
		r.Get("/tree", stub("tree"))
		r.Get("/sessions", stub("sessions"))
		r.Get("/sessions/{sessionID}", stub("session detail"))
		r.Post("/memories", stub("memories"))
	})

	s.router = r
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	dbOK := true
	if err := s.db.Ping(); err != nil {
		dbOK = false
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"version": s.version,
		"uptime":  time.Since(s.started).Seconds(),
		"db":      dbOK,
		"db_path": s.db.Path,
	})
}

func stub(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		json.NewEncoder(w).Encode(map[string]string{
			"error": name + " not yet implemented",
		})
	}
}
