// Package http exposes the worker's small HTTP surface: liveness, readiness,
// and metrics. There are no public, authorization-guarded endpoints.
package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

// writeJSON is the single JSON-response encoder for this package's handlers.
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// LiveResponse is the body returned by GET /live.
type LiveResponse struct {
	Status string `json:"status"`
}

// ServeLive is the K8s liveness handler: returns 200 unconditionally so the
// probe reflects process-alive only, independent of DB/target availability.
func ServeLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, LiveResponse{Status: "alive"})
}

// Deps contains the router's dependencies.
type Deps struct {
	// Health is the readiness handler (probes dependencies). Required.
	Health *HealthHandler
	// Metrics is the Prometheus handler (optional).
	Metrics http.Handler
	// Logger is the structured logger.
	Logger *zap.Logger
}

// NewRouter builds the chi router for /live, /health and /metrics.
func NewRouter(deps Deps) *chi.Mux {
	r := chi.NewRouter()
	r.Use(chimw.Recoverer)

	// Liveness (K8s livenessProbe): process-alive only, no dependency checks.
	r.Get("/live", ServeLive)
	// Readiness (K8s readinessProbe): probes the outbox + ledger DBs. Registered as a GET
	// func value (not r.Method with the bare handler) so the apispec generator traces it into
	// openapi.yaml alongside /live — else the readiness contract k8s depends on is undocumented.
	if deps.Health != nil {
		r.Get("/health", deps.Health.ServeHTTP)
	}
	// Metrics (Prometheus) — GET func value, same reason (keeps it in the generated spec).
	if deps.Metrics != nil {
		r.Get("/metrics", deps.Metrics.ServeHTTP)
	}
	return r
}
