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
	// Store ALWAYS writes r to EOF (idempotent by content hash; the codec is applied
	// upstream by fanOut, not here) and returns bytes stored. Dedup is the caller's job
	// via the ledger — a Store MUST NOT short-circuit on Exists, because reading r to EOF
	// is load-bearing: the commit is gated on the upstream VerifyReader hash check (no
	// length is passed), so a sink that skipped the read would stall the fan-out barrier
	// and break the verify-then-commit invariant.
	Store(ctx context.Context, hash string, r io.Reader) (int64, error)
	// Exists reports whether the object is already present.
	Exists(ctx context.Context, hash string) (bool, error)
	// Fetch returns the bytes AS STORED (still transformed). The caller reverses the
	// per-target codec via the hash-arbiter (raw-first, else bounded zstd) — see
	// restore.decodeStream; the sink does not decode.
	Fetch(ctx context.Context, hash string) (io.ReadCloser, error)
	// PutManifest writes a periodic ledger snapshot object.
	PutManifest(ctx context.Context, name string, r io.Reader) error
	// Preflight verifies the target is reachable + writable with the configured
	// credentials, so a misconfig fails loudly at startup instead of dead-lettering
	// every object.
	Preflight(ctx context.Context) error
}

// Source fetches an object's bytes. It takes the whole OutboxEntry because different
// sources key on different fields: the file-service source fetches by FileID (uuid),
// while reconcile's target-backed source fetches by ExternalID (content hash) — so
// neither has to fake the other's identifier.
type Source interface {
	// FetchContent streams the object's bytes for e.
	FetchContent(ctx context.Context, e BackupItem) (io.ReadCloser, error)
}
