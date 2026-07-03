// Package http exposes the worker's small HTTP surface: liveness, readiness,
// and (later) metrics. There are no public, authorization-guarded endpoints.
package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

// LiveResponse is the body returned by GET /live.
type LiveResponse struct {
	Status string `json:"status"`
}

// Render writes the response as JSON with HTTP 200.
func (r LiveResponse) Render(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(r)
}

// ServeLive is the K8s liveness handler: returns 200 unconditionally so the
// probe reflects process-alive only, independent of DB/target availability.
func ServeLive(w http.ResponseWriter, _ *http.Request) {
	LiveResponse{Status: "alive"}.Render(w)
}

// Deps contains the router's dependencies.
type Deps struct {
	// Health is the readiness handler (pings dependencies).
	Health *HealthHandler
	// Logger is the structured logger.
	Logger *zap.Logger
}

// NewRouter builds the chi router for /live, /health and /metrics.
func NewRouter(deps Deps) *chi.Mux {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)

	// Liveness (K8s livenessProbe): process-alive only, no dependency checks.
	r.Get("/live", ServeLive)
	// Readiness (K8s readinessProbe): checks the outbox + ledger DBs.
	r.Method(http.MethodGet, "/health", deps.Health)
	// Metrics. TODO(T031): serve Prometheus metrics via promhttp.
	r.Get("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return r
}
