package domain

import (
	"context"
	"time"
)

// OutboxEntry is a claimed backup-outbox row (Alkemio DB).
type OutboxEntry struct {
	ID          int64
	FileID      string
	ExternalID  string
	Priority    int16
	Size        int64     // object size (from the outbox) — recorded up front
	CreatedBy   string    // uuid text, "" if null — breadcrumb
	CreatedDate time.Time // when the source object was created — breadcrumb
}

// Outbox is the read/claim side of the backup outbox in the Alkemio DB, accessed
// via a scoped SELECT/UPDATE role.
type Outbox interface {
	// Claim atomically claims up to n pending rows (priority DESC, createdDate)
	// with FOR UPDATE SKIP LOCKED.
	Claim(ctx context.Context, n int) ([]OutboxEntry, error)
	// MarkDone marks an entry done once all required targets confirm.
	MarkDone(ctx context.Context, id int64) error
	// Fail records a failure — re-queues, or dead-letters past the attempt limit.
	// Returns true when the entry was moved to dead-letter.
	Fail(ctx context.Context, id int64, reason string) (bool, error)
	// ReapStale returns entries stuck in_progress past ttl to pending (crash safety).
	ReapStale(ctx context.Context, ttl time.Duration) error
	// Release returns a claim to pending WITHOUT counting an attempt — used on
	// graceful shutdown, where a cancelled in-flight object is not a failure.
	Release(ctx context.Context, id int64) error
}

// ObjectMeta are the ledger breadcrumbs for one backed-up object.
type ObjectMeta struct {
	ExternalID        string
	Size              int64
	CreatedBy         string    // uuid text, or "" for null
	SourceCreatedDate time.Time // outbox createdDate; zero => null
}

// Ledger records backup metadata in this service's own database.
type Ledger interface {
	// UpsertObject records an object (idempotent).
	UpsertObject(ctx context.Context, e ObjectMeta) error
	// UpsertTargetStatus records per-(object,target) completion.
	UpsertTargetStatus(ctx context.Context, externalID, target, state string, storedBytes int64) error
	// StoredTargets returns the set of target names already in state='stored' for
	// externalID, in one query — the dedup source of truth (never re-reads a
	// target, so it works with PutObject-only WORM credentials).
	StoredTargets(ctx context.Context, externalID string) (map[string]bool, error)
}
