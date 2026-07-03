// Package domain holds the file-backup-service business core: the backup
// pipeline, the content-addressed Sink port, integrity hashing, and the
// per-target transform. It MUST NOT depend on infrastructure.
package domain

import (
	"context"
	"errors"
	"io"
)

// ErrSourceGone means the source object no longer exists (e.g. it was deleted
// before it could be backed up) — a benign TERMINAL condition, not a transient
// failure. The consumer records the outbox entry 'skipped' rather than retrying it
// toward dead-letter (which would burn ~10 attempts and page on a non-problem).
var ErrSourceGone = errors.New("source object no longer exists")

// Sink is a dumb, content-addressed byte store keyed by the content hash
// (externalID). No index, no packing: a stored object is restorable with only
// its bytes and a hash check. See specs/008 contracts/sink-interface.md.
type Sink interface {
	// Name returns the configured target name.
	Name() string
	// Store writes bytes for hash if absent (idempotent, atomic), applying the
	// target's configured transform. Returns bytes actually stored.
	Store(ctx context.Context, hash string, r io.Reader, size int64) (int64, error)
	// Exists reports whether the object is already present.
	Exists(ctx context.Context, hash string) (bool, error)
	// Fetch returns the ORIGINAL bytes (transform reversed via the hash-arbiter).
	Fetch(ctx context.Context, hash string) (io.ReadCloser, error)
	// PutManifest writes a periodic ledger snapshot object.
	PutManifest(ctx context.Context, name string, r io.Reader) error
}

// Source fetches an object's bytes by file id (the file-service content API).
type Source interface {
	// FetchContent streams the object identified by fileID.
	FetchContent(ctx context.Context, fileID string) (io.ReadCloser, error)
}
