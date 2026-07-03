package http

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Prober verifies a dependency is USABLE — the schema is reachable via the scoped
// role, not merely that a connection opens. A bare Ping stays green after a runtime
// role revocation or schema drift while every claim fails; a Probe flips readiness.
type Prober interface {
	// Probe returns nil iff the dependency is currently usable.
	Probe(ctx context.Context) error
}

// HealthResponse is the body of GET /health.
type HealthResponse struct {
	Status  string            `json:"status"`
	Details map[string]string `json:"details"`
}

// HealthHandler reports readiness by probing the outbox and ledger. The verdict is
// cached for a short TTL so a K8s probe scraping every few seconds does not drive a
// DB round-trip per request, and the two probes run concurrently so a slow DB can't
// stack their timeouts past the probe budget.
type HealthHandler struct {
	Outbox Prober
	Ledger Prober
	TTL    time.Duration // cache window; <=0 uses defaultHealthTTL

	mu   sync.Mutex
	at   time.Time
	body HealthResponse
	code int
}

const (
	probeTimeout     = 2 * time.Second
	defaultHealthTTL = 5 * time.Second
)

// ServeHTTP implements the readiness probe.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	ttl := h.TTL
	if ttl <= 0 {
		ttl = defaultHealthTTL
	}
	h.mu.Lock()
	fresh := !h.at.IsZero() && time.Since(h.at) < ttl
	body, code := h.body, h.code
	h.mu.Unlock()

	if !fresh {
		body, code = h.probe(req.Context())
		h.mu.Lock()
		h.at, h.body, h.code = time.Now(), body, code
		h.mu.Unlock()
	}
	writeJSON(w, code, body)
}

func (h *HealthHandler) probe(parent context.Context) (HealthResponse, int) {
	checks := []struct {
		name string
		p    Prober
	}{{"outbox", h.Outbox}, {"ledger", h.Ledger}}
	details := make([]string, len(checks))

	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Add(1)
		go func(i int, p Prober) {
			defer wg.Done()
			if p == nil { // a missing dependency is UNHEALTHY, never silently skipped
				details[i] = "not configured"
				return
			}
			ctx, cancel := context.WithTimeout(parent, probeTimeout)
			defer cancel()
			if err := p.Probe(ctx); err != nil {
				details[i] = "unusable"
				return
			}
			details[i] = "ok"
		}(i, c.p)
	}
	wg.Wait()

	out := HealthResponse{Status: "healthy", Details: map[string]string{}}
	code := http.StatusOK
	for i, c := range checks {
		out.Details[c.name] = details[i]
		if details[i] != "ok" {
			out.Status, code = "unhealthy", http.StatusServiceUnavailable
		}
	}
	return out, code
}
