package server

import (
	"encoding/json"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/lazypower/continuity/internal/buildinfo"
	"github.com/lazypower/continuity/internal/engine"
	"github.com/lazypower/continuity/internal/store"
)

// Server is the continuity HTTP API server.
type Server struct {
	db      *store.DB
	engine  *engine.Engine
	router  chi.Router
	version string
	started time.Time
	events  *eventRecorder

	// Durable extraction worker (H1): /extract and /signal enqueue into
	// store.extraction_queue instead of spawning a fire-and-forget goroutine; a
	// single serial worker drains the queue and deletes each row only on success,
	// so a crash or restart mid-extraction replays the work instead of losing it.
	extractWake     chan struct{} // buffered(1); pinged on enqueue to wake the worker
	extractStop     chan struct{} // closed by StopExtractionWorker to end the loop
	extractStopOnce sync.Once     // guards extractStop against a double close
	extractDone     chan struct{} // closed when the worker loop has exited
	// runJob executes one queued job. Defaults to runExtractionJob (needs a live
	// engine); overridable in tests to drive the drain loop without an LLM.
	runJob func(*store.ExtractionJob) error
}

// New creates a new Server with the given database, engine, and version string.
// Engine may be nil (e.g., in tests or when LLM is not configured).
func New(db *store.DB, eng *engine.Engine, version string) *Server {
	s := &Server{
		db:      db,
		engine:  eng,
		version: version,
		started: time.Now(),
		events:  newEventRecorder(db),

		extractWake: make(chan struct{}, 1),
		extractStop: make(chan struct{}),
		extractDone: make(chan struct{}),
	}
	if eng != nil {
		s.runJob = s.runExtractionJob
	}
	s.routes()
	return s
}

// Close releases the server's background resources (the telemetry recorder).
// Safe to call once; telemetry flush is bounded, never blocking shutdown.
func (s *Server) Close() {
	if s.events != nil {
		s.events.Close()
	}
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

func (s *Server) routes() {
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Use(securityHeaders)
	r.Use(localhostOnly)
	r.Use(limitRequestBody)

	r.Route("/api", func(r chi.Router) {
		r.Get("/health", s.handleHealth)

		// Session + observation + context routes
		r.Post("/sessions/init", s.handleSessionInit)
		r.Post("/sessions/{sessionID}/observations", s.handleAddObservation)
		r.Post("/sessions/{sessionID}/complete", s.handleCompleteSession)
		r.Post("/sessions/{sessionID}/end", s.handleEndSession)
		r.Get("/context", s.handleGetContext)

		// Phase 2: extraction
		r.Post("/sessions/{sessionID}/extract", s.handleExtractSession)
		r.Post("/sessions/unmark-empty-extractions", s.handleUnmarkEmptyExtractions)

		// Phase 4: signal keywords
		r.Post("/sessions/{sessionID}/signal", s.handleSignal)

		// Phase 3: retrieval routes
		r.Get("/search", s.handleSearch)
		r.Get("/profile", s.handleProfile)
		r.Get("/tree", s.handleTree)
		r.Get("/timeline", s.handleTimeline)
		r.Get("/metrics", s.handleMetrics)

		// Stub routes — return 501 until implemented
		r.Get("/sessions", stub("sessions"))
		r.Get("/sessions/{sessionID}", stub("session detail"))
		r.Post("/memories", s.handleRemember)
		r.Get("/memories", s.handleGetMemory)
		r.Post("/memories/retract", s.handleRetract)
		r.Post("/memories/pin", s.handlePin)
		r.Post("/memories/unpin", s.handleUnpin)
		r.Get("/memories/pinned", s.handleListPinned)
	})

	// Serve embedded UI at all non-API paths
	r.NotFound(spaHandler())

	s.router = r
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	dbOK := true
	if err := s.db.Ping(); err != nil {
		dbOK = false
	}

	// Current applied schema version of the open DB. Best-effort: a read error
	// surfaces as 0 rather than failing the health check, since the endpoint's
	// primary job is liveness. dbOK already reflects connectivity trouble.
	schemaCurrent, _ := s.db.SchemaVersion()

	// Surface the durable extraction backlog so a wedged or parked queue is
	// visible (accountability), not buried only in logs.
	pendingExtractions, _ := s.db.PendingExtractions()

	// os.Executable is best-effort; an empty string is acceptable for clients.
	exe, _ := os.Executable()

	// Advertise the embedder the running server ACTUALLY uses (and whether the
	// corpus vector identity is locked), so doctor compares against the live
	// embedder instead of re-resolving a fresh one — the fresh-resolve blind spot.
	activeEmbedder := ""
	identityLocked := false
	if s.engine != nil {
		activeEmbedder = s.engine.ActiveIdentity()
		identityLocked, _ = s.engine.VectorIdentityLocked()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		// Existing fields, preserved for backward-compat.
		"status":  "ok",
		"version": s.version,
		"uptime":  time.Since(s.started).Seconds(),
		"db":      dbOK,

		// Compatibility/skew-detection fields (issue #36).
		"api_version":         buildinfo.APIVersion,
		"schema_head":         store.HeadSchemaVersion(),
		"schema_current":      schemaCurrent,
		"pending_extractions": pendingExtractions,
		"pid":                 os.Getpid(),
		"started_at":          s.started.Unix(),
		"db_path":             s.db.Path,
		"exe":                 exe,

		// Vector-identity fields: what the live server embeds with, and whether
		// search is locked due to a corpus/embedder mismatch.
		"active_embedder":        activeEmbedder,
		"vector_identity_locked": identityLocked,
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
