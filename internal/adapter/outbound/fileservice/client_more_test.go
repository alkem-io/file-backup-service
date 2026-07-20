package fileservice

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// TestFetchContentSuccess: a 200 response streams the object body back to the caller. It also
// exercises New's maxIdleConns<1 default (the pool falls back to 16) — a nil http.Client with an
// unset concurrency must still build a working transport. And it asserts the fetch is keyed by
// the object's ExternalID (content hash) on the /internal/blob/{hash}/content path — NOT by
// FileID — so replication reads the exact enqueued version, not the document's current content.
func TestFetchContentSuccess(t *testing.T) {
	const hash = "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"
	pathCh := make(chan string, 1) // channel receive gives the happens-before edge -race needs
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathCh <- r.URL.Path
		_, _ = io.WriteString(w, "hello-bytes")
	}))
	defer srv.Close()

	c := New(srv.URL, 0, nil) // maxIdleConns<1 → default idle-conn pool
	rc, err := c.FetchContent(context.Background(), domain.BackupItem{ExternalID: hash, FileID: uuid.New()})
	if err != nil {
		t.Fatalf("FetchContent on 200 must succeed: %v", err)
	}
	if want, gotPath := "/internal/blob/"+hash+"/content", <-pathCh; gotPath != want {
		t.Fatalf("fetch path = %q, want %q (must key on ExternalID, not FileID)", gotPath, want)
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

// TestPreflightPassesOn200: a 200 means the server answered — Preflight's
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
