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

// HealthHandler reports readiness by probing the outbox and ledger concurrently on
// every request. There is no cache: a K8s probe scrapes every ~10s and each probe is
// a single LIMIT-1 round-trip, so the verdict is always current and a slow/cancelled
// scrape can't leave a stale unhealthy verdict behind.
type HealthHandler struct {
	Outbox Prober
	Ledger Prober
}

const probeTimeout = 2 * time.Second

// ServeHTTP implements the readiness probe.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
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
			// A driver panic in Probe must report unusable, not crash the serve process:
			// this goroutine is spawned per readiness scrape, and chi's Recoverer only
			// wraps the request goroutine, not its children.
			defer func() {
				if r := recover(); r != nil {
					details[i] = "unusable"
				}
			}()
			if p == nil { // a missing dependency is UNHEALTHY, never silently skipped
				details[i] = "not configured"
				return
			}
			// Detached from the request context: a kubelet timeoutSeconds shorter than
			// probeTimeout must not cancel the probe and report a healthy DB as unusable.
			ctx, cancel := context.WithTimeout(context.WithoutCancel(req.Context()), probeTimeout)
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
	writeJSON(w, code, out)
}
