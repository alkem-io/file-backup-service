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

// preflightProbeKey is a deliberately INVALID content-hash key (a hyphen is not a valid hash
// char, and it is far short of the 32-char minimum). A correctly-deployed file-service /blob
// route validates the key and answers 400 (ErrInvalidKey) BEFORE touching storage — so a 400
// PROVES the route exists. A 404 for an INVALID key can only mean the route is missing (an
// out-of-order/rolling deploy: an old pod behind the ClusterIP) or this isn't file-service at
// all — the real route 400s an invalid key, it never 404s it. That is what makes the startup
// gate able to distinguish a route-miss from an object-miss. See Preflight.
const preflightProbeKey = "preflight-probe-invalid-key"

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

// Preflight PROVES the by-hash route is actually present at startup (parity with the DB/sink
// checks), not merely that some HTTP server answers. It probes with an INVALID key
// (preflightProbeKey): the real /blob route validates the key and returns 400, so a 400 proves
// the route; a 404 can ONLY mean a route-miss (deploy skew / wrong endpoint), since the real
// route never 404s an invalid key. This is why the probe key is invalid, not a valid-but-absent
// hash — it collapses the route-miss-vs-object-miss ambiguity a valid-hash probe can't resolve.
//
// A 404 therefore FAILS the gate: serve and backfill refuse to start rather than 404→skip the
// whole outbox against a missing/wrong endpoint (which would silently protect nothing until an
// alert + manual backfill). This self-enforces the file-service-first deploy order — the worker
// crash-loops until file-service genuinely serves /blob, exactly as it already does when its DB
// or a sink is unreachable. Runtime 404→skip stays as the narrow claim-then-GC race backstop.
// A transient 5xx/403 still passes (the route answered; runtime fetches treat 5xx as retryable),
// so a coordinated-deploy blip can't turn a required check into a crash loop for the wrong reason.
//
// CROSS-REPO CONTRACT: this rests on file-service's /blob handler returning 400 (not 404) for a
// key its validator rejects — the invalid-key path is checked BEFORE any storage backend read, so
// it holds for the local FS today and an S3 backend later. That contract is enforced on the
// file-service side by its GetBlobContent invalid-key test; if it is ever changed there, this
// probe must change in lockstep (a broken contract fails CLOSED here — the worker refuses to start
// rather than silently mass-skipping, which is the intended direction of failure).
func (c *Client) Preflight(ctx context.Context) error {
	rc, err := c.FetchContent(ctx, domain.BackupItem{ExternalID: preflightProbeKey})
	switch {
	case err == nil:
		// 200 for an INVALID key: the endpoint served content it never should have — this is not
		// file-service's /blob route (which would 400 the key). Fail loudly (wrong endpoint).
		_ = rc.Close()
		return fmt.Errorf("file-service preflight: %s answered 200 to an invalid-key probe — not the /blob route", c.baseURL)
	case errors.Is(err, domain.ErrSourceGone):
		// 404/410 for an INVALID key ⇒ the /blob route is missing (deploy skew: an old pod behind
		// the ClusterIP) or this isn't file-service. Refuse to start; do NOT mass-skip the outbox.
		return fmt.Errorf("file-service preflight: %s has no /internal/blob route (404 to an invalid-key probe) — deploy skew or wrong fileServiceBase: %w", c.baseURL, err)
	case errors.Is(err, errRemoteStatus):
		// The route ANSWERED with a non-success status: a 400 (validated the bad key → route
		// present) or a transient 5xx/403 (reachable). Either way it's not a route-miss; pass, so
		// a transient server error can't crash-loop startup.
		return nil
	default:
		return fmt.Errorf("file-service unreachable: %w", err)
	}
}
