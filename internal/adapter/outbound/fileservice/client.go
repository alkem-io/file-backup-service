// Package fileservice reads object bytes from file-service's internal content
// API (GET /internal/file/{id}/content) — the network read path that keeps the
// worker off the RWO volume and S3-ready.
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

// FetchContent streams GET {base}/internal/file/{id}/content. The caller closes
// the returned reader.
func (c *Client) FetchContent(ctx context.Context, fileID string) (io.ReadCloser, error) {
	reqURL := fmt.Sprintf("%s/internal/file/%s/content", c.baseURL, url.PathEscape(fileID))
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
		return nil, fmt.Errorf("file-service GET %s: status %d", reqURL, resp.StatusCode)
	}
	return resp.Body, nil
}

// Preflight checks file-service is reachable at startup (parity with the DB/sink
// checks): a GET for a nonexistent object. Any HTTP response — including the expected
// 404 (ErrSourceGone) for the probe id — means the server answered; only a
// connection/dial/timeout error (wrong host, down) fails. It can't detect a wrong path
// PREFIX (that also 404s, indistinguishable from a missing object) — a mass
// filebackup_source_gone_total spike surfaces that at runtime.
func (c *Client) Preflight(ctx context.Context) error {
	rc, err := c.FetchContent(ctx, "00000000-0000-0000-0000-000000000000")
	switch {
	case err == nil:
		_ = rc.Close()
		return nil
	case errors.Is(err, domain.ErrSourceGone):
		return nil // 404/410: reachable, the probe id simply doesn't exist
	default:
		return fmt.Errorf("file-service unreachable: %w", err)
	}
}
