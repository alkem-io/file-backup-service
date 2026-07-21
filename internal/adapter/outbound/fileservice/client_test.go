package fileservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// TestPreflightPassesWhenRouteAnswers: the invalid-key probe proves the /blob route exists when
// the server answers a non-success status that is NOT a route-miss — a 400 (the real route
// validated the bad key) or a transient 5xx/403 (reachable, must not crash-loop startup).
func TestPreflightPassesWhenRouteAnswers(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,          // 400 — the real /blob route rejected the invalid probe key: route PRESENT
		http.StatusServiceUnavailable,  // 503 — transient, reachable
		http.StatusInternalServerError, // 500
		http.StatusForbidden,           // 403
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
		c := New(srv.URL, 4, nil)
		err := c.Preflight(context.Background())
		srv.Close()
		if err != nil {
			t.Fatalf("Preflight on HTTP %d must pass (route answered, not a route-miss), got: %v", status, err)
		}
	}
}

// TestPreflightFailsWhenRouteMissing: for an INVALID key, a 404/410 can only mean the /blob route
// is absent (deploy skew / wrong endpoint) — the real route 400s an invalid key, never 404s it —
// and a 200 means a wrong endpoint served content it never should. All must FAIL the gate so the
// worker refuses to start rather than 404→skip the whole outbox.
func TestPreflightFailsWhenRouteMissing(t *testing.T) {
	for _, status := range []int{
		http.StatusNotFound, // 404 — chi default for a missing route / an endpoint without /blob
		http.StatusGone,     // 410 — also maps to source-gone; not the real route's invalid-key answer
		http.StatusOK,       // 200 — a wrong endpoint served content for an invalid key
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
		c := New(srv.URL, 4, nil)
		err := c.Preflight(context.Background())
		srv.Close()
		if err == nil {
			t.Fatalf("Preflight on HTTP %d must FAIL (route missing / wrong endpoint)", status)
		}
	}
}

// TestPreflightFailsOnTransportError: a connection/dial error (server down — did NOT answer)
// must fail Preflight, so a genuinely-unreachable file-service is caught at startup.
func TestPreflightFailsOnTransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // now nothing listens on url → connection refused
	c := New(url, 4, nil)
	if err := c.Preflight(context.Background()); err == nil {
		t.Fatal("Preflight must fail on a transport error (server down = unreachable)")
	}
}

// TestFetchContentClassifiesStatuses: the fetch path (not preflight) still treats 404/410 as
// ErrSourceGone and other non-2xx as a retryable failure — so the F3 change to Preflight
// didn't weaken the pipeline's error handling.
func TestFetchContentClassifiesStatuses(t *testing.T) {
	cases := []struct {
		status  int
		wantErr error // errors.Is target; nil = any non-nil error
	}{
		{http.StatusGone, domain.ErrSourceGone},
		{http.StatusNotFound, domain.ErrSourceGone},
		{http.StatusServiceUnavailable, errRemoteStatus},
		{http.StatusInternalServerError, errRemoteStatus},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(tc.status)
		}))
		c := New(srv.URL, 4, nil)
		_, err := c.FetchContent(context.Background(), domain.BackupItem{})
		srv.Close()
		if err == nil {
			t.Fatalf("FetchContent on HTTP %d must error", tc.status)
		}
		if !errors.Is(err, tc.wantErr) {
			t.Fatalf("FetchContent on HTTP %d: want errors.Is %v, got %v", tc.status, tc.wantErr, err)
		}
	}
}
