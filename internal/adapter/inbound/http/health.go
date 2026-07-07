package http

import (
	"context"
	"net/http"
	"time"

	"github.com/alkem-io/file-backup-service/internal/domain"
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

	// domain.RunParallel owns the per-goroutine recover that every other concurrent sweep in
	// the service reuses — needed here because a driver panic in Probe must report unusable,
	// not crash serve: these goroutines are spawned per readiness scrape and chi's Recoverer
	// only wraps the request goroutine, not its children. A recovered panic leaves details[i]
	// unwritten (""), which the post-loop maps to "unusable".
	errs := domain.RunParallelIdx(len(checks),
		func(i int) string { return "probe " + checks[i].name },
		func(i int) error { details[i] = probeDetail(req, checks[i].p); return nil })
	for i := range details {
		if errs[i] != nil { // Probe panicked (RunParallel recovered it) → unusable
			details[i] = "unusable"
		}
	}

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

// probeDetail runs one dependency check and returns its readiness detail. A nil prober is
// "not configured" (a missing dependency is UNHEALTHY, never silently skipped). The probe ctx
// is DETACHED from the request (context.WithoutCancel) + bounded, so a kubelet timeoutSeconds
// shorter than probeTimeout can't cancel it and report a healthy DB as unusable.
func probeDetail(req *http.Request, p Prober) string {
	if p == nil {
		return "not configured"
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(req.Context()), probeTimeout)
	defer cancel()
	if err := p.Probe(ctx); err != nil {
		return "unusable"
	}
	return "ok"
}
