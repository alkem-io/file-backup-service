package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Pinger performs an active round-trip to a dependency (pgxpool.Pool satisfies
// this directly). Implementations MUST honor ctx cancellation.
type Pinger interface {
	// Ping checks the dependency is reachable.
	Ping(ctx context.Context) error
}

// HealthResponse is the body of GET /health.
type HealthResponse struct {
	Status  string            `json:"status"`
	Details map[string]string `json:"details"`
}

// Render writes the response as JSON with the given status code.
func (r HealthResponse) Render(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(r)
}

// HealthHandler reports readiness by pinging the outbox and ledger databases.
// Nil pingers are skipped (unwired in the scaffold).
type HealthHandler struct {
	// Outbox pings the Alkemio DB (outbox).
	Outbox Pinger
	// Ledger pings this service's own ledger DB.
	Ledger Pinger
}

// ServeHTTP implements the readiness probe.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	details := map[string]string{}
	healthy := true
	check := func(name string, p Pinger) {
		if p == nil {
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		if err := p.Ping(ctx); err != nil {
			details[name] = "unreachable"
			healthy = false
			return
		}
		details[name] = "ok"
	}
	check("outbox", h.Outbox)
	check("ledger", h.Ledger)

	status, code := "healthy", http.StatusOK
	if !healthy {
		status, code = "unhealthy", http.StatusServiceUnavailable
	}
	HealthResponse{Status: status, Details: details}.Render(w, code)
}
