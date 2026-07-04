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
	p             *Pool
	maxAttempts   int // genuine-failure dead-letter threshold
	maxDeliveries int // crash-loop dead-letter threshold (counted by the reaper)
}

// NewOutboxRepo binds an OutboxRepo to the alkemio pool with the dead-letter limits.
func NewOutboxRepo(p *Pool, maxAttempts, maxDeliveries int) *OutboxRepo {
	return &OutboxRepo{p: p, maxAttempts: maxAttempts, maxDeliveries: maxDeliveries}
}

const claimSQL = `UPDATE file_backup_outbox
SET status='in_progress', "claimedAt"=now()
WHERE id IN (
  SELECT id FROM file_backup_outbox
  WHERE status='pending' AND ("visibleAt" IS NULL OR "visibleAt" <= now())
  ORDER BY priority DESC, "createdDate"
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
RETURNING id, "fileId"::text, "externalID", COALESCE(size,0),
  COALESCE("createdBy"::text,''), COALESCE("createdDate", now())`

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
		if err := rows.Scan(&e.ID, &e.FileID, &e.ExternalID, &e.Size, &e.CreatedBy, &e.CreatedDate); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return out, nil
}

// transition applies setClause to the claimed row, guarded by status='in_progress'
// so a concurrently reaped/reclaimed/done row is a safe no-op (never a clobber of
// another worker's claim). This owns the guard in one place for MarkDone/Release/Skip.
func (r *OutboxRepo) transition(ctx context.Context, id int64, verb, setClause string) error {
	if _, err := r.p.Exec(ctx,
		`UPDATE file_backup_outbox SET `+setClause+` WHERE id=$1 AND status='in_progress'`, id); err != nil {
		return fmt.Errorf("%s: %w", verb, err)
	}
	return nil
}

// MarkDone marks an entry done, but only if this worker still owns the claim.
func (r *OutboxRepo) MarkDone(ctx context.Context, id int64) error {
	return r.transition(ctx, id, "mark done", `status='done'`)
}

// Fail increments attempts and re-queues the entry, or dead-letters it once the
// attempt limit is reached. attempts counts genuine FAILURES (incremented here),
// never claims or reaps — so a slow object that is reaped/reclaimed is not
// dead-lettered. Guarded by status='in_progress' so a lost claim (reaped,
// reclaimed, or already done) is a no-op rather than a clobber. Returns true when
// the entry was moved to dead-letter.
func (r *OutboxRepo) Fail(ctx context.Context, id int64, reason string) (bool, error) {
	// COALESCE(attempts,0): defend against a server-owned column that is NULL (drift),
	// where NULL arithmetic would make the dead-letter CASE never fire (infinite retry).
	const q = `UPDATE file_backup_outbox
SET attempts = COALESCE(attempts,0) + 1,
    status = CASE WHEN COALESCE(attempts,0) + 1 >= $2 THEN 'dead_letter' ELSE 'pending' END,
    "lastError" = $3,
    "claimedAt" = NULL,
    -- LEAST(attempts,20) clamps the exponent so make_interval can't overflow the
    -- interval type before the outer LEAST caps the result at 10 minutes.
    "visibleAt" = now() + LEAST(make_interval(secs => 5 * (2 ^ LEAST(COALESCE(attempts,0), 20))), interval '10 minutes')
WHERE id = $1 AND status = 'in_progress'
RETURNING status = 'dead_letter'`
	var deadLettered bool
	if err := r.p.QueryRow(ctx, q, id, r.maxAttempts, reason).Scan(&deadLettered); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil // claim lost (reaped/reclaimed/done) — no-op
		}
		return false, fmt.Errorf("fail entry: %w", err)
	}
	return deadLettered, nil
}

// Probe verifies the outbox is reachable via the scoped role AND carries every
// column the consumer depends on. Selecting the actual columns (not SELECT 1)
// turns a stale/missing server migration into a loud failure instead of a
// green-health silent stall where every Claim errors on a missing column. It is
// READ-ONLY so it is safe to run on the readiness path every scrape (schema drift on
// the foreign, server-owned table is the real recurring risk). An empty table
// (ErrNoRows) is success.
func (r *OutboxRepo) Probe(ctx context.Context) error {
	var (
		id   int64
		cols any
	)
	// Every column the consumer's SQL reads or writes (Claim RETURNING, Fail/
	// ReapStale SET, the claim WHERE) — so a stale/renamed column in the server
	// migration dies here, not as a green-health silent stall.
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

// CheckWriteGrant verifies the UPDATE half of the SELECT/UPDATE grant — every
// Claim/Fail/Reap is an UPDATE, so a SELECT-only role would pass Probe then fail
// every claim at runtime. WHERE false touches no row but still checks the table-level
// UPDATE privilege at execution. Called ONCE at startup (not per readiness scrape),
// so the shared production table doesn't take a write transaction every ~10s.
func (r *OutboxRepo) CheckWriteGrant(ctx context.Context) error {
	if _, err := r.p.Exec(ctx, `UPDATE file_backup_outbox SET status = status WHERE false`); err != nil {
		return fmt.Errorf("outbox write-grant check (scoped role lacks UPDATE?): %w", err)
	}
	return nil
}

// Release returns a claim to pending WITHOUT incrementing attempts (a graceful
// shutdown of an in-flight object is not a failure). No-op if the claim is lost.
func (r *OutboxRepo) Release(ctx context.Context, id int64) error {
	return r.transition(ctx, id, "release", `status='pending', "claimedAt"=NULL, "visibleAt"=NULL`)
}

// Skip terminally marks an entry 'skipped' (source object gone). No-op if lost.
func (r *OutboxRepo) Skip(ctx context.Context, id int64) error {
	return r.transition(ctx, id, "skip", `status='skipped', "claimedAt"=NULL`)
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
    SET deliveries = COALESCE(deliveries,0) + 1,
        status = CASE WHEN COALESCE(deliveries,0) + 1 >= $2 THEN 'dead_letter' ELSE 'pending' END,
        "lastError" = CASE WHEN COALESCE(deliveries,0) + 1 >= $2 THEN 'crash-loop: exceeded max deliveries' ELSE "lastError" END,
        "claimedAt" = NULL
    -- claimedAt IS NULL too: on the shared/foreign outbox a row left in_progress with
    -- a NULL claimedAt (external writer / drift) would otherwise never match
    -- (NULL < x is NULL) and stall forever with green health.
    WHERE status='in_progress'
      AND ("claimedAt" IS NULL OR "claimedAt" < now() - make_interval(secs => $1))
    RETURNING status
)
SELECT count(*) FILTER (WHERE status = 'dead_letter') FROM reaped`
	var deadLettered int
	if err := r.p.QueryRow(ctx, q, ttl.Seconds(), r.maxDeliveries).Scan(&deadLettered); err != nil {
		return 0, fmt.Errorf("reap stale: %w", err)
	}
	return deadLettered, nil
}
