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
//
// Constitution §IV waiver: these queries are hand-written pgx, not sqlc. The outbox is
// a FOREIGN, server-owned table in the Alkemio DB — it is NOT in this repo's ledger
// migrations that sqlc.yaml's `schema` points at, so sqlc has no schema to type-check it
// against. The startup Probe (which SELECTs every consumed column) is the compensating
// control: a server-side schema drift fails loudly at deploy instead of at runtime.
type OutboxRepo struct {
	p             PgxDB
	maxAttempts   int // genuine-failure dead-letter threshold
	maxDeliveries int // crash-loop dead-letter threshold (counted by the reaper)
	// readBounded self-bounds the outbox READS (Probe, BacklogStats) on the shared Alkemio pool at the
	// adapter (via boundRead) rather than relying on their callers, same as LedgerRepo/FileRepo.
	readBounded
}

// NewOutboxRepo binds an OutboxRepo to the alkemio pool with the dead-letter limits. It takes
// the PgxDB interface (satisfied by *Pool and by pgxmock) so it is unit-testable without a DB.
func NewOutboxRepo(p PgxDB, maxAttempts, maxDeliveries int) *OutboxRepo {
	return &OutboxRepo{p: p, maxAttempts: maxAttempts, maxDeliveries: maxDeliveries}
}

// WithReadTimeout sets the client-side per-read bound for the outbox READs (Probe, BacklogStats) to the
// pool's server-side statement_timeout (cfg.DBTimeout()). Returns the repo for chaining. Unset →
// defaultDBReadTimeout. See LedgerRepo.WithReadTimeout / the shared readBounded.
func (r *OutboxRepo) WithReadTimeout(d time.Duration) *OutboxRepo { r.setReadTimeout(d); return r }

const claimSQL = `UPDATE file_backup_outbox
SET status='in_progress', "claimedAt"=now()
WHERE id IN (
  SELECT id FROM file_backup_outbox
  WHERE status='pending' AND ("visibleAt" IS NULL OR "visibleAt" <= now())
  ORDER BY priority DESC, "createdDate"
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
RETURNING id, "fileId", "externalID", COALESCE(size,0),
  "createdBy", COALESCE("createdDate", now())`

// Claim atomically claims up to n pending rows.
func (r *OutboxRepo) Claim(ctx context.Context, n int) ([]domain.OutboxEntry, error) {
	rows, err := r.p.Query(ctx, claimSQL, n)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.OutboxEntry, error) {
		var e domain.OutboxEntry
		err := row.Scan(&e.ID, &e.FileID, &e.ExternalID, &e.Size, &e.CreatedBy, &e.CreatedDate)
		return e, err
	})
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

// Defer re-queues a claim WITHOUT counting an attempt when an object stored on every
// REACHABLE target and its only gap is a circuit-open (persistently-down) target (T017a).
// The re-check is a LAZY, JITTERED backstop (2–4 min), not the primary recovery: the
// object is already safely backed up on every reachable target, and RECONCILE is the real
// gap-fill when the target returns (the ledger records the stored targets, so TargetGaps
// sees the gap). The jitter desynchronizes what would otherwise be a synchronized
// re-claim/UPDATE herd on the SHARED production outbox every interval during a multi-hour
// outage. Guarded by status='in_progress' (a lost claim is a no-op).
func (r *OutboxRepo) Defer(ctx context.Context, id int64) error {
	// attempts=0: a deferred object stored on EVERY reachable target is NOT in a failure
	// state — its only gap is a down target — so any prior genuine-failure count is reset.
	// This also makes a deferred row uniquely (attempts=0 AND visibleAt IS NOT NULL), which
	// is what lets BacklogStats exclude ALL deferred rows (not just first-attempt ones) from
	// the RPO gauge — otherwise a failed-then-deferred object would re-spike oldest-age.
	return r.transition(ctx, id, "defer",
		`status='pending', "claimedAt"=NULL, attempts=0,
		 "visibleAt"=now() + interval '2 minutes' + random() * interval '2 minutes'`)
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
	ctx, cancel := boundRead(ctx, r.readTimeout)
	defer cancel()
	err := r.p.QueryRow(ctx, q).Scan(&id, &cols, &cols, &cols, &cols, &cols,
		&cols, &cols, &cols, &cols, &cols, &cols, &cols)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("outbox probe (scoped role / schema drift?): %w", err)
	}
	return nil
}

// BacklogStats returns the number of pending outbox entries and the age (seconds) of
// the oldest one — the backlog-depth + lag signal (FR-026). Age is 0 when empty.
func (r *OutboxRepo) BacklogStats(ctx context.Context) (pending int, oldestAgeSec float64, err error) {
	// Count only rows that represent REAL un-backed-up work, so the RPO gauge doesn't fire a
	// false backup-lag page during a single-target outage. Excludes:
	//   - not-yet-visible rows (visibleAt in the future) — matches Claim's visibility.
	//   - DEFERRED objects (T017a) — these ARE backed up on every reachable target; Defer
	//     resets attempts=0 (a deferred object is not in a failure state), so they're
	//     uniquely (attempts=0 AND visibleAt IS NOT NULL) — a fresh row has visibleAt NULL,
	//     a genuinely-retrying failure has attempts>0. This holds even for a failed-THEN-
	//     deferred object: the reset erases its earlier genuine-failure count. Deferred rows
	//     cycle through visibility every backoff, so a plain visibleAt filter isn't enough —
	//     their old createdDate would dominate min() in the visible window and spike oldest-
	//     age to hours.
	// A row counts iff: fresh (visibleAt NULL) OR a genuinely-retrying failure that is due
	// (visibleAt <= now() AND attempts > 0).
	//
	// CAVEAT (all-targets-down / single-target deployment): when EVERY pending target is
	// circuit-open, a claimed object stores nowhere and is deferred (attempts=0), so this
	// gauge excludes it and reads clean even though nothing is being backed up. That is by
	// design — the object is blocked on a down target, not lagging on throughput — and the
	// outage is surfaced by filebackup_targets_circuit_open>0 + last_success_age climbing, the
	// signals that DO fire when a target is down. See the oldest_pending_age metric help.
	const q = `SELECT count(*),
	  COALESCE(EXTRACT(EPOCH FROM now() - min("createdDate")), 0)
	FROM file_backup_outbox
	WHERE status='pending'
	  AND ("visibleAt" IS NULL OR ("visibleAt" <= now() AND COALESCE(attempts,0) > 0))`
	ctx, cancel := boundRead(ctx, r.readTimeout)
	defer cancel()
	if err := r.p.QueryRow(ctx, q).Scan(&pending, &oldestAgeSec); err != nil {
		return 0, 0, fmt.Errorf("backlog stats: %w", err)
	}
	return pending, oldestAgeSec, nil
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

// SourceStillReferenced reports whether any permanent file row still references this content
// hash — the authoritative "should this object exist" the worker consults on a source 404 to
// tell a genuinely-gone object from a transiently-unavailable source. Uses the SELECT-on-file
// grant (same grant backfill's corpus enumeration uses). A bounded, index-friendly EXISTS.
func (r *OutboxRepo) SourceStillReferenced(ctx context.Context, externalID string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM file WHERE "externalID"=$1 AND "temporaryLocation" IS NOT TRUE)`
	ctx, cancel := boundRead(ctx, r.readTimeout)
	defer cancel()
	var exists bool
	if err := r.p.QueryRow(ctx, q, externalID).Scan(&exists); err != nil {
		return false, fmt.Errorf("source-referenced check: %w", err)
	}
	return exists, nil
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
