package db

import (
	"context"
	"fmt"
	"time"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// OutboxRepo reads/claims the backup outbox in the Alkemio DB (scoped role).
type OutboxRepo struct {
	p *Pool
}

// NewOutboxRepo binds an OutboxRepo to the alkemio pool.
func NewOutboxRepo(p *Pool) *OutboxRepo { return &OutboxRepo{p: p} }

const claimSQL = `UPDATE file_backup_outbox
SET status='in_progress', "claimedAt"=now(), attempts=attempts+1
WHERE id IN (
  SELECT id FROM file_backup_outbox
  WHERE status='pending'
  ORDER BY priority DESC, "createdDate"
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
RETURNING id, "fileId"::text, "externalID", priority`

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
		if err := rows.Scan(&e.ID, &e.FileID, &e.ExternalID, &e.Priority); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return out, nil
}

// MarkDone marks an entry done.
func (r *OutboxRepo) MarkDone(ctx context.Context, id int64) error {
	if _, err := r.p.Exec(ctx, `UPDATE file_backup_outbox SET status='done' WHERE id=$1`, id); err != nil {
		return fmt.Errorf("mark done: %w", err)
	}
	return nil
}

// Fail re-queues the entry (attempts already incremented on claim) or
// dead-letters it once the attempt limit is reached.
func (r *OutboxRepo) Fail(ctx context.Context, id int64, reason string) error {
	const q = `UPDATE file_backup_outbox
SET status = CASE WHEN attempts >= $2 THEN 'dead_letter' ELSE 'pending' END,
    "lastError" = $3,
    "claimedAt" = NULL
WHERE id = $1`
	if _, err := r.p.Exec(ctx, q, id, maxAttempts, reason); err != nil {
		return fmt.Errorf("fail entry: %w", err)
	}
	return nil
}

// maxAttempts is the dead-letter threshold (configurable later — FR-006).
const maxAttempts = 10

// ReapStale returns entries stuck in_progress past ttl to pending (crash safety).
func (r *OutboxRepo) ReapStale(ctx context.Context, ttl time.Duration) error {
	const q = `UPDATE file_backup_outbox
SET status='pending', "claimedAt"=NULL
WHERE status='in_progress' AND "claimedAt" < now() - make_interval(secs => $1)`
	if _, err := r.p.Exec(ctx, q, ttl.Seconds()); err != nil {
		return fmt.Errorf("reap stale: %w", err)
	}
	return nil
}
