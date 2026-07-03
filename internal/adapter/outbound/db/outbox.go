package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// OutboxRepo reads/claims the backup outbox in the Alkemio DB (scoped role).
type OutboxRepo struct {
	p *Pool
}

// NewOutboxRepo binds an OutboxRepo to the alkemio pool.
func NewOutboxRepo(p *Pool) *OutboxRepo { return &OutboxRepo{p: p} }

const claimSQL = `UPDATE file_backup_outbox
SET status='in_progress', "claimedAt"=now()
WHERE id IN (
  SELECT id FROM file_backup_outbox
  WHERE status='pending' AND ("visibleAt" IS NULL OR "visibleAt" <= now())
  ORDER BY priority DESC, "createdDate"
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
RETURNING id, "fileId"::text, "externalID", priority, size, COALESCE("createdBy"::text,''), "createdDate"`

// Claim atomically claims up to n pending rows.
func (r *OutboxRepo) Claim(ctx context.Context, n int) ([]domain.OutboxEntry, error) {
	rows, err := r.p.Query(ctx, claimSQL, n)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	defer rows.Close()
	var out []domain.OutboxEntry
	for rows.Next() {
		var e domain.OutboxEntry
		if err := rows.Scan(&e.ID, &e.FileID, &e.ExternalID, &e.Priority, &e.Size, &e.CreatedBy, &e.CreatedDate); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return out, nil
}

// MarkDone marks an entry done, but only if this worker still owns the claim
// (status='in_progress'). A concurrently reaped/reclaimed row affects 0 rows and
// is a safe no-op — we must not clobber another worker's claim or a done row.
func (r *OutboxRepo) MarkDone(ctx context.Context, id int64) error {
	if _, err := r.p.Exec(ctx, `UPDATE file_backup_outbox SET status='done' WHERE id=$1 AND status='in_progress'`, id); err != nil {
		return fmt.Errorf("mark done: %w", err)
	}
	return nil
}

// Fail increments attempts and re-queues the entry, or dead-letters it once the
// attempt limit is reached. attempts counts genuine FAILURES (incremented here),
// never claims or reaps — so a slow object that is reaped/reclaimed is not
// dead-lettered. Guarded by status='in_progress' so a lost claim (reaped,
// reclaimed, or already done) is a no-op rather than a clobber. Returns true when
// the entry was moved to dead-letter.
func (r *OutboxRepo) Fail(ctx context.Context, id int64, reason string) (bool, error) {
	const q = `UPDATE file_backup_outbox
SET attempts = attempts + 1,
    status = CASE WHEN attempts + 1 >= $2 THEN 'dead_letter' ELSE 'pending' END,
    "lastError" = $3,
    "claimedAt" = NULL,
    "visibleAt" = now() + LEAST(make_interval(secs => 5 * (2 ^ attempts)), interval '10 minutes')
WHERE id = $1 AND status = 'in_progress'
RETURNING status = 'dead_letter'`
	var deadLettered bool
	if err := r.p.QueryRow(ctx, q, id, maxAttempts, reason).Scan(&deadLettered); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil // claim lost (reaped/reclaimed/done) — no-op
		}
		return false, fmt.Errorf("fail entry: %w", err)
	}
	return deadLettered, nil
}

// maxAttempts is the genuine-failure dead-letter threshold (configurable later — FR-006).
const maxAttempts = 10

// maxDeliveries dead-letters an object that repeatedly crashes/wedges the worker
// (counted by the reaper). Set well above any slow object's expected reap count.
const maxDeliveries = 50

// Probe verifies the outbox is reachable via the scoped role AND carries every
// column the consumer depends on. Selecting the actual columns (not SELECT 1)
// turns a stale/missing server migration into a loud startup failure instead of a
// green-health silent stall where every Claim errors on a missing column. An empty
// table (ErrNoRows) is success.
func (r *OutboxRepo) Probe(ctx context.Context) error {
	var (
		id   int64
		cols any
	)
	// Every column the consumer's SQL reads or writes (Claim RETURNING, Fail/
	// ReapStale SET, the claim WHERE) — so a stale/renamed column in the server
	// migration dies here at startup, not as a green-health silent stall.
	const q = `SELECT id, "fileId", "externalID", priority, status, attempts,
	  deliveries, "lastError", "createdBy", "createdDate", size, "claimedAt", "visibleAt"
	FROM file_backup_outbox LIMIT 1`
	err := r.p.QueryRow(ctx, q).Scan(&id, &cols, &cols, &cols, &cols, &cols,
		&cols, &cols, &cols, &cols, &cols, &cols, &cols)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("outbox probe (scoped role / schema drift?): %w", err)
	}
	return nil
}

// Release returns a claim to pending WITHOUT incrementing attempts (a graceful
// shutdown of an in-flight object is not a failure). No-op if the claim is lost.
func (r *OutboxRepo) Release(ctx context.Context, id int64) error {
	const q = `UPDATE file_backup_outbox SET status='pending', "claimedAt"=NULL, "visibleAt"=NULL
WHERE id=$1 AND status='in_progress'`
	if _, err := r.p.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("release: %w", err)
	}
	return nil
}

// Skip terminally marks an entry 'skipped' (source object gone). Guarded by
// status='in_progress' so a lost claim is a no-op.
func (r *OutboxRepo) Skip(ctx context.Context, id int64) error {
	const q = `UPDATE file_backup_outbox SET status='skipped', "claimedAt"=NULL
WHERE id=$1 AND status='in_progress'`
	if _, err := r.p.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("skip: %w", err)
	}
	return nil
}

// ReapStale requeues entries stuck in_progress past ttl (a crashed/wedged
// delivery — the per-object timeout would have Failed a merely-slow one). It
// counts them via `deliveries` and dead-letters a crash-looping object once it
// exceeds maxDeliveries, so a poison object that repeatedly kills the worker
// can't loop forever while a slow object (never reaped) is never penalised.
func (r *OutboxRepo) ReapStale(ctx context.Context, ttl time.Duration) (int, error) {
	// Count the rows this sweep pushed to dead_letter so the caller fires the
	// dead-letter observer — a crash-loop dead-letter happens here, not via Fail.
	const q = `WITH reaped AS (
    UPDATE file_backup_outbox
    SET deliveries = deliveries + 1,
        status = CASE WHEN deliveries + 1 >= $2 THEN 'dead_letter' ELSE 'pending' END,
        "lastError" = CASE WHEN deliveries + 1 >= $2 THEN 'crash-loop: exceeded max deliveries' ELSE "lastError" END,
        "claimedAt" = NULL
    WHERE status='in_progress' AND "claimedAt" < now() - make_interval(secs => $1)
    RETURNING status
)
SELECT count(*) FILTER (WHERE status = 'dead_letter') FROM reaped`
	var deadLettered int
	if err := r.p.QueryRow(ctx, q, ttl.Seconds(), maxDeliveries).Scan(&deadLettered); err != nil {
		return 0, fmt.Errorf("reap stale: %w", err)
	}
	return deadLettered, nil
}
