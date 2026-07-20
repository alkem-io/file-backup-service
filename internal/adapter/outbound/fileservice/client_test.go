package fileservice

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// TestPreflightReachableOnServerResponse: ANY HTTP response means the server ANSWERED, so
// Preflight passes — the expected 404 (probe hash absent), a transient 5xx (file-service up but
// DB not ready during a coordinated deploy / a mid-rollout old pod), or a 403 must NOT turn a
// required startup check into a CrashLoopBackOff. A genuinely-missing endpoint surfaces at
// runtime (fetches 404 → objects Skipped → FileBackupSourceGoneSpike alert; backfill recovers), never here.
func TestPreflightReachableOnServerResponse(t *testing.T) {
	for _, status := range []int{
		http.StatusNotFound,            // 404 — probe hash absent (or a mid-rollout old pod / missing route)
		http.StatusGone,                // 410
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
			t.Fatalf("Preflight on HTTP %d must pass (server answered = reachable), got: %v", status, err)
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
