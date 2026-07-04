package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db/queries"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// LedgerRepo implements domain.Ledger over the sqlc-generated queries (own DB),
// plus one raw pgx CTE (RecordBackup) that sqlc's analyzer can't type.
type LedgerRepo struct {
	p *Pool
	q *queries.Queries
}

// NewLedgerRepo binds a LedgerRepo to the ledger pool.
func NewLedgerRepo(p *Pool) *LedgerRepo { return &LedgerRepo{p: p, q: queries.New(p.Pool)} }

// recordBackupSQL writes the object row (FK parent) and every per-target status in
// ONE atomic statement. The data-modifying CTE inserts the object first; the FK
// check on the status rows sees it (same transaction), so ordering holds. Verified
// against Postgres 16 (FK, idempotency, and the no-downgrade CASE). Mirrors the
// ledger schema; there is no second copy (this replaces UpsertObject + the status
// batch).
const recordBackupSQL = `WITH obj AS (
  INSERT INTO file_backup_object ("externalID", size, "createdBy", "sourceCreatedDate")
  VALUES ($1, $2, $3, $4)
  -- Correct the size to a later VERIFIED value ($8), but never downgrade a verified
  -- size to unverified outbox hearsay (an all-fail retry): a first all-targets-fail
  -- attempt writes the unverified outbox size, a later success overwrites it.
  ON CONFLICT ("externalID") DO UPDATE
    SET size = CASE WHEN $8 THEN EXCLUDED.size ELSE file_backup_object.size END
)
INSERT INTO file_backup_target_status ("externalID", target, state, "storedBytes", "verifiedAt")
SELECT $1, t.target, t.state, t.bytes, CASE WHEN t.state = 'stored' THEN now() ELSE NULL END
FROM unnest($5::text[], $6::text[], $7::bigint[]) AS t(target, state, bytes)
ON CONFLICT ("externalID", target) DO UPDATE SET
  state = CASE WHEN file_backup_target_status.state = 'stored' AND EXCLUDED.state <> 'stored'
               THEN file_backup_target_status.state ELSE EXCLUDED.state END,
  "storedBytes" = CASE WHEN EXCLUDED.state = 'stored' THEN EXCLUDED."storedBytes"
                       ELSE file_backup_target_status."storedBytes" END,
  "verifiedAt" = CASE WHEN EXCLUDED.state = 'stored' THEN now()
                      ELSE file_backup_target_status."verifiedAt" END`

// RecordBackup writes the object row + all per-target statuses atomically (one RTT).
func (r *LedgerRepo) RecordBackup(ctx context.Context, obj domain.ObjectMeta, statuses []domain.TargetStatus) error {
	targets := make([]string, len(statuses))
	states := make([]string, len(statuses))
	storedBytes := make([]int64, len(statuses))
	for i, s := range statuses {
		targets[i], states[i], storedBytes[i] = s.Target, s.State, s.StoredBytes
	}
	if _, err := r.p.Exec(ctx, recordBackupSQL,
		obj.ExternalID, obj.Size, uuidOrNull(obj.CreatedBy), timestamptzOrNull(obj.SourceCreatedDate),
		targets, states, storedBytes, obj.SizeVerified); err != nil {
		return fmt.Errorf("record backup: %w", err)
	}
	return nil
}

// Probe verifies both ledger tables exist + are readable via the pool's role. A
// missing table (skipped migration) errors; an empty table is success.
func (r *LedgerRepo) Probe(ctx context.Context) error {
	var a, b any
	const q = `SELECT (SELECT 1 FROM file_backup_object LIMIT 1),
	                  (SELECT 1 FROM file_backup_target_status LIMIT 1)`
	if err := r.p.QueryRow(ctx, q).Scan(&a, &b); err != nil {
		return fmt.Errorf("ledger probe (schema/migrate?): %w", err)
	}
	return nil
}

// StoredTargets returns the set of target names already in state='stored' for
// externalID (one query — the dedup source of truth).
func (r *LedgerRepo) StoredTargets(ctx context.Context, externalID string) (map[string]bool, error) {
	rows, err := r.q.ListTargetStates(ctx, externalID)
	if err != nil {
		return nil, fmt.Errorf("list target states: %w", err)
	}
	out := make(map[string]bool, len(rows))
	for _, row := range rows {
		if row.State == domain.StateStored {
			out[row.Target] = true
		}
	}
	return out, nil
}

// uuidOrNull parses a DB-sourced UUID text (via COALESCE("createdBy"::text,”))
// into a pgtype.UUID; "" maps to SQL NULL. The source is a UUID column, so a real
// value cannot fail to parse — the only "" case is a genuine NULL breadcrumb.
func uuidOrNull(s string) pgtype.UUID {
	var u pgtype.UUID
	if s == "" {
		return u
	}
	_ = u.Scan(s)
	return u
}

func timestamptzOrNull(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}
