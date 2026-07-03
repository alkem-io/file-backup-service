package domain

import (
	"context"
	"time"
)

// OutboxEntry is a claimed backup-outbox row (Alkemio DB).
type OutboxEntry struct {
	ID         int64
	FileID     string
	ExternalID string
	Priority   int16
}

// Outbox is the read/claim side of the backup outbox in the Alkemio DB, accessed
// via a scoped SELECT/UPDATE role.
type Outbox interface {
	// Claim atomically claims up to n pending rows (priority DESC, createdDate)
	// with FOR UPDATE SKIP LOCKED.
	Claim(ctx context.Context, n int) ([]OutboxEntry, error)
	// MarkDone marks an entry done once all required targets confirm.
	MarkDone(ctx context.Context, id int64) error
	// Fail records a failure — re-queues with backoff, or dead-letters past the
	// attempt limit.
	Fail(ctx context.Context, id int64, reason string) error
	// ReapStale returns entries stuck in_progress past ttl to pending (crash safety).
	ReapStale(ctx context.Context, ttl time.Duration) error
}

// ObjectMeta are the ledger breadcrumbs for one backed-up object.
type ObjectMeta struct {
	ExternalID string
	Size       int64
	CreatedBy  string // uuid text, or "" for null
	MimeType   string
}

// Ledger records backup metadata in this service's own database.
type Ledger interface {
	// UpsertObject records an object (idempotent).
	UpsertObject(ctx context.Context, e ObjectMeta) error
	// UpsertTargetStatus records per-(object,target) completion.
	UpsertTargetStatus(ctx context.Context, externalID, target, state string, storedBytes int64) error
}
