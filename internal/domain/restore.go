package domain

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// RestoreObject fetches hash from src, reverses the transform via the
// hash-arbiter, verifies it, and writes it to destDir/<hash> (idempotent: skips
// if already present). destDir is the primary store mount or a scratch dir.
func RestoreObject(ctx context.Context, src Sink, hash, destDir string) error {
	dest := filepath.Join(destDir, hash)
	if _, err := os.Stat(dest); err == nil {
		return nil // already present
	}
	out, err := fetchAndDecode(ctx, src, hash)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil { //nolint:gosec // primary store is world-readable
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp := dest + ".partial"
	// 0644 so file-service (a different uid, 65532) can read restored objects from
	// the primary store. Ownership is handled by the ops runbook / fsGroup.
	if err := os.WriteFile(tmp, out, 0o644); err != nil { //nolint:gosec // content-addressed blob, served by file-service
		return fmt.Errorf("write: %w", err)
	}
	return os.Rename(tmp, dest)
}

// VerifyObject fetches hash from src and confirms it decodes to hash.
func VerifyObject(ctx context.Context, src Sink, hash string) error {
	_, err := fetchAndDecode(ctx, src, hash)
	return err
}

func fetchAndDecode(ctx context.Context, src Sink, hash string) ([]byte, error) {
	rc, err := src.Fetch(ctx, hash)
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return DecodeArbiter(hash, raw)
}
