// Package fileservice reads object bytes from file-service's internal content
// API (GET /internal/file/{id}/content) — the network read path that keeps the
// worker off the RWO volume and S3-ready.
package fileservice

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// Client streams object content from file-service.
type Client struct {
	baseURL string
	http    *http.Client
}

// New constructs a Client for the given file-service base URL.
func New(baseURL string, hc *http.Client) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{baseURL: baseURL, http: hc}
}

// FetchContent streams GET {base}/internal/file/{id}/content. The caller closes
// the returned reader.
func (c *Client) FetchContent(ctx context.Context, fileID string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/internal/file/%s/content", c.baseURL, fileID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch content: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("file-service GET %s: status %d", url, resp.StatusCode)
	}
	return resp.Body, nil
}
