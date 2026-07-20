// Package fileservice reads object bytes from file-service's internal
// content-addressed API (GET /internal/blob/{hash}/content) — a network read
// keyed by content hash that keeps the worker off the RWO volume and S3-ready.
package fileservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// preflightProbeKey is a deliberately INVALID content key. A file-service that exposes
// GET /internal/blob/{hash}/content validates the key and returns 400; one that predates the
// endpoint 404s (chi route-miss). Preflight uses that gap to tell "endpoint present" from
// "endpoint missing" — which a valid-but-absent hash could NOT (both 404). The hyphens make it
// fail the store's alphanumeric key rule.
const preflightProbeKey = "preflight-probe-not-a-content-hash"

// errRemoteStatus marks a fetch that got a NON-success HTTP status (not 200/404/410) — the
// server ANSWERED, it just returned an error (e.g. a transient 5xx during a coordinated
// deploy, or a 403). It is distinct from a transport error (dial/TLS/timeout — the server
// didn't answer). Preflight treats "the server answered" as reachable (a required-check pass)
// so a transient 5xx doesn't crash-loop the worker, while the fetch path still treats it as a
// retryable failure. Errors wrap it so callers test with errors.Is.
var errRemoteStatus = errors.New("file-service returned a non-success status")

// Client streams object content from file-service.
type Client struct {
	baseURL string
	http    *http.Client
}

// New constructs a Client for the given file-service base URL. maxIdleConns sizes the
// idle connection pool to the worker concurrency so concurrent fetches reuse
// keep-alive connections instead of churning a new TCP/TLS handshake per object.
func New(baseURL string, maxIdleConns int, hc *http.Client) *Client {
	if hc == nil {
		if maxIdleConns < 1 {
			maxIdleConns = 16
		}
		// No Client.Timeout — it caps the whole request including body read and
		// would abort large streamed objects. The per-object ctx bounds total
		// time; these transport timeouts catch a stalled peer (connect / TLS /
		// time-to-first-byte) so a half-open connection can't wedge a worker.
		hc = &http.Client{
			Transport: &http.Transport{
				DialContext:           (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second,
				IdleConnTimeout:       90 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
				MaxIdleConnsPerHost:   maxIdleConns,
			},
		}
	}
	// Trim trailing slashes so a base like "http://fs/" doesn't produce
	// "http://fs//internal/..." → 404 on every fetch → the whole outbox dead-letters.
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: hc}
}

// FetchContent streams GET {base}/internal/blob/{hash}/content — a CONTENT-ADDRESSED read
// keyed by the object's externalID (its SHA3-256 content hash), NOT by the document id.
// A blob under a hash is immutable, so this is version-exact: it returns the EXACT bytes the
// outbox enqueued, never whichever version the (mutable) document points at now. This is why
// a create-empty-then-fill doc, or any edited file, no longer integrity-fails: fetch-by-id
// would return the current content and mismatch the enqueued hash. A superseded (refcount-
// deleted) hash is a clean 404 → ErrSourceGone (FR-008; we replicate blobs by externalID).
// The caller closes the returned reader.
func (c *Client) FetchContent(ctx context.Context, e domain.BackupItem) (io.ReadCloser, error) {
	reqURL := fmt.Sprintf("%s/internal/blob/%s/content", c.baseURL, url.PathEscape(e.ExternalID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch content: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		// Drain (bounded) before Close so the keep-alive connection is reused instead
		// of torn down under a sustained error burst.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
		// 404 or 410: the object was deleted before backup ran — a benign terminal
		// (recorded skipped-source-absent), not a retryable failure that burns attempts.
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
			return nil, fmt.Errorf("file-service GET %s: %w", reqURL, domain.ErrSourceGone)
		}
		// The server ANSWERED with a non-success status (5xx/403/…). Wrap errRemoteStatus so
		// Preflight can tell it apart from a transport error (reachable vs unreachable); the
		// fetch path still sees a non-nil, non-gone error and retries as before.
		return nil, fmt.Errorf("file-service GET %s: status %d: %w", reqURL, resp.StatusCode, errRemoteStatus)
	}
	return resp.Body, nil
}

// Preflight checks, at startup, that file-service is reachable AND actually exposes the
// by-hash blob endpoint — by probing with an invalid key that a present endpoint rejects (400):
//
//   - 400 (errRemoteStatus — the handler ran and rejected the bad key), or any other
//     non-success like a transient 5xx (a coordinated deploy where file-service is up but its
//     DB isn't ready, or a 403): the server ANSWERED and the endpoint EXISTS → PASS. A transient
//     5xx must not crash-loop startup; runtime fetches retry with backoff.
//   - 404/410 (ErrSourceGone): the route is NOT registered — this file-service predates
//     GET /internal/blob/{hash}/content. FAIL. Running the worker against it would 404 every
//     fetch and silently drain the whole outbox to 'skipped' with green health; a loud
//     CrashLoopBackOff until file-service is upgraded is the correct, visible failure. (Deploy
//     order is file-service first; this catches an out-of-order deploy.)
//   - transport error (dial/TLS/timeout — the server did NOT answer): unreachable → FAIL.
func (c *Client) Preflight(ctx context.Context) error {
	rc, err := c.FetchContent(ctx, domain.BackupItem{ExternalID: preflightProbeKey})
	switch {
	case err == nil:
		_ = rc.Close()
		return nil // 200 (unexpected for an invalid key, but the server answered) — reachable
	case errors.Is(err, errRemoteStatus):
		return nil // 400 (endpoint validated the bad key) or 5xx/403 — reachable AND endpoint present
	case errors.Is(err, domain.ErrSourceGone):
		return fmt.Errorf("file-service is missing GET /internal/blob/{hash}/content "+
			"(404 on the endpoint probe) — upgrade file-service before the worker: %w", err)
	default:
		return fmt.Errorf("file-service unreachable: %w", err)
	}
}
