package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
)

// okProber is a dependency that is always usable; it counts probes to prove the handler
// actually calls Probe on every request (no cached verdict).
type okProber struct{ calls atomic.Int32 }

func (o *okProber) Probe(context.Context) error { o.calls.Add(1); return nil }

// errProber reports the dependency as unusable — the runtime-revocation / schema-drift case.
type errProber struct{}

func (errProber) Probe(context.Context) error { return errors.New("role revoked") }

// panicProber models a driver panic inside Probe; RunParallel must recover it into an error
// so a readiness scrape reports unusable instead of crashing serve.
type panicProber struct{}

func (panicProber) Probe(context.Context) error { panic("driver blew up") }

// TestServeLiveAlwaysAlive: /live is process-alive only — 200 with {"status":"alive"},
// independent of any dependency.
func TestServeLiveAlwaysAlive(t *testing.T) {
	rec := httptest.NewRecorder()
	ServeLive(rec, httptest.NewRequest(http.MethodGet, "/live", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}
	var body LiveResponse
	decode(t, rec, &body)
	if body.Status != "alive" {
		t.Errorf("status = %q, want alive", body.Status)
	}
}

// TestHealthAllUsable: both probes succeed → 200 healthy with per-dependency "ok" details,
// and each dependency is actually probed.
func TestHealthAllUsable(t *testing.T) {
	outbox, ledger := &okProber{}, &okProber{}
	h := &HealthHandler{Outbox: outbox, Ledger: ledger}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body HealthResponse
	decode(t, rec, &body)
	if body.Status != "healthy" {
		t.Errorf("status = %q, want healthy", body.Status)
	}
	if body.Details["outbox"] != "ok" || body.Details["ledger"] != "ok" {
		t.Errorf("details = %v, want both ok", body.Details)
	}
	if outbox.calls.Load() != 1 || ledger.calls.Load() != 1 {
		t.Errorf("probes not called once each: outbox=%d ledger=%d", outbox.calls.Load(), ledger.calls.Load())
	}
}

// TestHealthProbeErrorIsUnhealthy: an erroring probe flips readiness to 503 unhealthy and marks
// that dependency "unusable" while the healthy one stays "ok".
func TestHealthProbeErrorIsUnhealthy(t *testing.T) {
	h := &HealthHandler{Outbox: errProber{}, Ledger: &okProber{}}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body HealthResponse
	decode(t, rec, &body)
	if body.Status != "unhealthy" {
		t.Errorf("status = %q, want unhealthy", body.Status)
	}
	if body.Details["outbox"] != "unusable" {
		t.Errorf("outbox detail = %q, want unusable", body.Details["outbox"])
	}
	if body.Details["ledger"] != "ok" {
		t.Errorf("ledger detail = %q, want ok", body.Details["ledger"])
	}
}

// TestHealthNilProberNotConfigured: a missing dependency is UNHEALTHY ("not configured"),
// never silently skipped.
func TestHealthNilProberNotConfigured(t *testing.T) {
	h := &HealthHandler{Outbox: nil, Ledger: &okProber{}}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body HealthResponse
	decode(t, rec, &body)
	if body.Details["outbox"] != "not configured" {
		t.Errorf("outbox detail = %q, want 'not configured'", body.Details["outbox"])
	}
	if body.Status != "unhealthy" {
		t.Errorf("status = %q, want unhealthy", body.Status)
	}
}

// TestHealthProbePanicRecoveredAsUnusable: a panic inside Probe must be recovered by RunParallel
// and reported as unusable (503), not crash the request goroutine's children.
func TestHealthProbePanicRecoveredAsUnusable(t *testing.T) {
	h := &HealthHandler{Outbox: panicProber{}, Ledger: &okProber{}}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body HealthResponse
	decode(t, rec, &body)
	if body.Details["outbox"] != "unusable" {
		t.Errorf("outbox detail after panic = %q, want unusable", body.Details["outbox"])
	}
	if body.Status != "unhealthy" {
		t.Errorf("status = %q, want unhealthy", body.Status)
	}
}

// TestRouterWiresAllRoutes: /live, /health and /metrics are all reachable through the chi mux
// when their deps are supplied, and each returns its handler's output.
func TestRouterWiresAllRoutes(t *testing.T) {
	metricsHit := false
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		metricsHit = true
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "filebackup_up 1")
	})
	r := NewRouter(Deps{
		Health:  &HealthHandler{Outbox: &okProber{}, Ledger: &okProber{}},
		Metrics: metrics,
		Logger:  zap.NewNop(),
	})

	if code, body := do(t, r, "/live"); code != http.StatusOK || !strings.Contains(body, `"status":"alive"`) {
		t.Errorf("/live = %d %q", code, body)
	}
	if code, body := do(t, r, "/health"); code != http.StatusOK || !strings.Contains(body, `"status":"healthy"`) {
		t.Errorf("/health = %d %q", code, body)
	}
	if code, body := do(t, r, "/metrics"); code != http.StatusOK || !strings.Contains(body, "filebackup_up 1") {
		t.Errorf("/metrics = %d %q", code, body)
	}
	if !metricsHit {
		t.Error("metrics handler was not invoked by the router")
	}
}

// TestRouterHealthUnhealthyPropagates: the router surfaces the readiness verdict — a failing
// probe yields 503 on /health.
func TestRouterHealthUnhealthyPropagates(t *testing.T) {
	r := NewRouter(Deps{
		Health: &HealthHandler{Outbox: errProber{}, Ledger: &okProber{}},
		Logger: zap.NewNop(),
	})
	if code, _ := do(t, r, "/health"); code != http.StatusServiceUnavailable {
		t.Errorf("/health status = %d, want 503", code)
	}
}

// TestRouterOmitsUnsuppliedRoutes: with no Health/Metrics deps, those routes are not registered
// (404) — the conditional wiring, not a stub handler.
func TestRouterOmitsUnsuppliedRoutes(t *testing.T) {
	r := NewRouter(Deps{Logger: zap.NewNop()})

	if code, _ := do(t, r, "/live"); code != http.StatusOK {
		t.Errorf("/live should always exist, got %d", code)
	}
	if code, _ := do(t, r, "/health"); code != http.StatusNotFound {
		t.Errorf("/health without Health dep = %d, want 404", code)
	}
	if code, _ := do(t, r, "/metrics"); code != http.StatusNotFound {
		t.Errorf("/metrics without Metrics dep = %d, want 404", code)
	}
}

// do runs a GET against the handler through a recorder and returns the status and body.
func do(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

// decode unmarshals the recorder's JSON body into v.
func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
}
