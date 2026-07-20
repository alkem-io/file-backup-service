package fileservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// TestPreflightPassesWhenEndpointPresent: a non-404 answer means the server ANSWERED and the
// by-hash endpoint EXISTS. 400 is the expected reply to the invalid-key probe (the handler ran
// and rejected it); a transient 5xx (file-service up but DB not ready during a coordinated
// deploy) or a 403 must also NOT crash-loop a required startup check.
func TestPreflightPassesWhenEndpointPresent(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,          // 400 — endpoint validated the invalid probe key (present)
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
			t.Fatalf("Preflight on HTTP %d must pass (endpoint present / reachable), got: %v", status, err)
		}
	}
}

// TestPreflightFailsWhenEndpointMissing: a 404/410 to the invalid-key probe means the route is
// NOT registered — this file-service predates GET /internal/blob/{hash}/content. Preflight MUST
// fail so an out-of-order deploy is a loud CrashLoopBackOff, not a silent whole-outbox skip.
func TestPreflightFailsWhenEndpointMissing(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
		c := New(srv.URL, 4, nil)
		err := c.Preflight(context.Background())
		srv.Close()
		if err == nil {
			t.Fatalf("Preflight on HTTP %d must FAIL (endpoint missing → out-of-order deploy)", status)
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
