package fileservice

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// TestFetchContentSuccess: a 200 response streams the object body back to the caller. It also
// exercises New's maxIdleConns<1 default (the pool falls back to 16) — a nil http.Client with an
// unset concurrency must still build a working transport.
func TestFetchContentSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "hello-bytes")
	}))
	defer srv.Close()

	c := New(srv.URL, 0, nil) // maxIdleConns<1 → default idle-conn pool
	rc, err := c.FetchContent(context.Background(), domain.BackupItem{})
	if err != nil {
		t.Fatalf("FetchContent on 200 must succeed: %v", err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(b) != "hello-bytes" {
		t.Fatalf("body = %q, want %q", b, "hello-bytes")
	}
}

// TestPreflightPassesOn200: a 200 for the probe id means the server answered — Preflight's
// happy path (err==nil) closes the body and passes.
func TestPreflightPassesOn200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := New(srv.URL, 4, nil)
	if err := c.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight on a 200 must pass (server answered): %v", err)
	}
}

// TestFetchContentBuildRequestError: a base URL that can't form a valid request URL (a control
// byte in the host) must fail at request construction, not silently 404 every fetch.
func TestFetchContentBuildRequestError(t *testing.T) {
	c := New("http://\x7fbad-host", 4, nil)
	_, err := c.FetchContent(context.Background(), domain.BackupItem{})
	if err == nil || !strings.Contains(err.Error(), "build request") {
		t.Fatalf("a malformed base URL must fail request build, got: %v", err)
	}
}
