package domain

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// BackupItem is the content-identity of one object to back up: everything the pipeline and
// the Source need, and NOTHING outbox-specific. All three producers — a claimed outbox row, the
// corpus `file` reader (backfill), and reconcile — build a BackupItem, and BackupOne consumes
// it. Because it carries no outbox row ID, the pipeline CANNOT accidentally read an outbox-only
// field that backfill/reconcile don't populate (which would silently be the zero value); the
// "pipeline reads only content-identity" invariant is thus enforced by the type, not a comment.
type BackupItem struct {
	FileID     uuid.UUID     // file.id — the fileservice Source fetches bytes by it (uuid.Nil for reconcile, which keys on ExternalID)
	ExternalID string        // content hash (SHA3-256), NOT a uuid — the identity, key, and verifier
	Size       int64         // object size (breadcrumb; unverified outbox hearsay until the stream is hashed)
	CreatedBy  uuid.NullUUID // breadcrumb; Valid=false for a NULL createdBy
	// CreatedDate is a BEST-EFFORT source-age breadcrumb, not load-bearing: the backfill path
	// reads file.createdDate (true source creation), but the serve path carries the OUTBOX
	// createdDate, which is ENQUEUE time (≈ creation for a new file, but the replace/re-enqueue
	// time for an updated one). The outbox's enqueue-time semantics are what BacklogStats needs
	// for the RPO lag gauge; this breadcrumb just rides along.
	CreatedDate time.Time
}

// OutboxEntry is a claimed backup-outbox row (Alkemio DB): a BackupItem plus the outbox-only
// row ID the consumer needs for MarkDone/Fail/Defer/Skip. The pipeline takes the embedded
// BackupItem, never the whole OutboxEntry, so it can't reach ID. Priority is not carried into
// Go — ordering is done DB-side in the claim SQL.
type OutboxEntry struct {
	BackupItem
	ID int64
}

// Outbox is the read/claim side of the backup outbox in the Alkemio DB, accessed
// via a scoped SELECT/UPDATE role.
type Outbox interface {
	// Claim atomically claims up to n pending rows (priority DESC, createdDate)
	// with FOR UPDATE SKIP LOCKED.
	Claim(ctx context.Context, n int) ([]OutboxEntry, error)
	// MarkDone marks an entry done once every configured target confirms (symmetric).
	MarkDone(ctx context.Context, id int64) error
	// Defer re-queues an entry with a short backoff WITHOUT counting an attempt — used
	// when the object's only gap is a persistently-down (circuit-open) target (T017a),
	// so a single-target outage doesn't march it toward dead-letter.
	Defer(ctx context.Context, id int64) error
	// Fail records a failure — re-queues, or dead-letters past the attempt limit.
	// Returns true when the entry was moved to dead-letter.
	Fail(ctx context.Context, id int64, reason string) (bool, error)
	// ReapStale returns entries stuck in_progress past ttl to pending (crash safety)
	// and dead-letters crash-loopers; it returns the number dead-lettered so the
	// caller can fire the dead-letter observer.
	ReapStale(ctx context.Context, ttl time.Duration) (int, error)
	// Release returns a claim to pending WITHOUT counting an attempt — used on
	// graceful shutdown, where a cancelled in-flight object is not a failure.
	Release(ctx context.Context, id int64) error
	// Skip terminally records an entry as 'skipped' — the source object no longer
	// exists (ErrSourceGone), which is benign, not a failure to retry.
	Skip(ctx context.Context, id int64) error
	// Probe verifies the outbox is reachable AND has the columns the consumer
	// depends on (visibleAt/deliveries/attempts/claimedAt/size) so a scoped-role or
	// schema/contract drift fails loudly at startup, not as a green-health silent
	// stall where every Claim errors.
	Probe(ctx context.Context) error
}

// ObjectMeta are the ledger breadcrumbs for one backed-up object.
type ObjectMeta struct {
	ExternalID        string
	Size              int64
	SizeVerified      bool          // Size came from the hash-verified stream, not outbox hearsay
	CreatedBy         uuid.NullUUID // breadcrumb; Valid=false for a NULL createdBy
	SourceCreatedDate time.Time     // outbox createdDate; zero => null
}

// TargetStatus is one (object, target) completion record.
type TargetStatus struct {
	Target      string
	State       string // StateStored | StateFailed
	StoredBytes int64
}

// Ledger records backup metadata in this service's own database.
type Ledger interface {
	// RecordBackup writes the object row (FK parent) and every per-target status in
	// ONE atomic round-trip (idempotent; never downgrades a durable 'stored' to
	// 'failed') — so a backfill of millions of objects isn't N+ round-trips each.
	RecordBackup(ctx context.Context, obj ObjectMeta, statuses []TargetStatus) error
	// StoredTargets returns the set of target names already in state='stored' for
	// externalID, in one query — the dedup source of truth (never re-reads a
	// target, so it works with PutObject-only WORM credentials).
	StoredTargets(ctx context.Context, externalID string) (map[string]bool, error)
	// Probe verifies the ledger tables exist + are readable, so a skipped/misordered
	// migration fails loudly at startup and via readiness, not as a green-health
	// stall where every RecordBackup errors "relation does not exist".
	Probe(ctx context.Context) error
	// StoredObjectsPage returns up to limit objects currently stored ON target (what the
	// target actually holds — so a manifest/audit reads a true per-target inventory),
	// keyset-paginated by externalID: after is the last externalID of the previous page
	// ("" to start), results are ordered by externalID, and a short page (< limit) is the
	// last. Paging — not a held streaming cursor — releases the DB connection between
	// pages, so a slow consumer (audit's per-object Exists, a manifest's slow upload)
	// can't pin a pool connection for the whole operation.
	StoredObjectsPage(ctx context.Context, target, after string, limit int) ([]ObjectMeta, error)
	// TargetGaps streams objects that are NOT stored on every configured target,
	// with the set of target names that DO hold each — the reconcile work-list.
	TargetGaps(ctx context.Context, allTargets []string, fn func(externalID string, stored map[string]bool) error) error
}
