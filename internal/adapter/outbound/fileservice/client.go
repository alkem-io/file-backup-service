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

// preflightProbeHash is a syntactically valid but never-existing content hash used only by
// Preflight to confirm file-service answers: a real by-hash GET that returns a definite 404
// (reachable), exercising the same route the runtime fetch uses. 64 zeros is a valid SHA3-256
// key shape that no real content can produce.
const preflightProbeHash = "0000000000000000000000000000000000000000000000000000000000000000"

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

// Preflight checks file-service is reachable at startup (parity with the DB/sink
// checks): a GET for a nonexistent object. ANY HTTP response — the expected 404
// (ErrSourceGone) for the probe hash, OR a non-success status like a transient 5xx (a
// coordinated platform deploy where file-service is up but its DB isn't ready yet, or a 403)
// — means the server ANSWERED and is reachable, so the startup gate passes and the worker
// starts; runtime fetches retry with backoff until file-service is healthy. Only a
// connection/dial/timeout error (wrong host, down — the server did NOT answer) fails, so a
// transient 5xx can't turn this required check into a CrashLoopBackOff. It can't detect a
// wrong path PREFIX (that also 404s, indistinguishable from a missing object) — a mass
// filebackup_source_gone_total spike surfaces that at runtime.
func (c *Client) Preflight(ctx context.Context) error {
	rc, err := c.FetchContent(ctx, domain.BackupItem{ExternalID: preflightProbeHash})
	switch {
	case err == nil:
		_ = rc.Close()
		return nil
	case errors.Is(err, domain.ErrSourceGone):
		return nil // 404/410: reachable, the probe hash simply doesn't exist
	case errors.Is(err, errRemoteStatus):
		return nil // 5xx/403/…: the server ANSWERED (reachable); a transient error must not crash-loop startup
	default:
		return fmt.Errorf("file-service unreachable: %w", err)
	}
}
