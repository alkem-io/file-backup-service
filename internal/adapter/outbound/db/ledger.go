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

// EachStoredObject streams the objects currently stored ON target for that target's
// manifest snapshot (FR-015), invoking fn per row. It joins target_status so a target's
// manifest lists ONLY what the target holds — not the whole ledger — so a standalone
// restore from an under-replicated target doesn't read a false inventory. Streaming
// (not ReadAll) keeps memory bounded across millions of rows.
func (r *LedgerRepo) EachStoredObject(ctx context.Context, target string, fn func(domain.ObjectMeta) error) error {
	const q = `SELECT o."externalID", o.size, COALESCE(o."createdBy"::text,''), o."sourceCreatedDate"
	FROM file_backup_object o
	JOIN file_backup_target_status ts ON ts."externalID" = o."externalID"
	WHERE ts.target = $1 AND ts.state = 'stored'
	ORDER BY o."externalID"`
	rows, err := r.p.Query(ctx, q, target)
	if err != nil {
		return fmt.Errorf("stream stored objects: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var m domain.ObjectMeta
		var created pgtype.Timestamptz
		if err := rows.Scan(&m.ExternalID, &m.Size, &m.CreatedBy, &created); err != nil {
			return fmt.Errorf("scan object: %w", err)
		}
		if created.Valid {
			m.SourceCreatedDate = created.Time
		}
		if err := fn(m); err != nil {
			return err
		}
	}
	return rows.Err()
}

// TargetGaps streams objects NOT stored on every configured target, with the set of
// CURRENT targets that DO hold each — the reconcile work-list. An object stored on all
// of allTargets is excluded; stale statuses for removed targets are ignored (the
// count + agg filter to allTargets).
func (r *LedgerRepo) TargetGaps(ctx context.Context, allTargets []string, fn func(string, map[string]bool) error) error {
	const q = `SELECT o."externalID",
	  COALESCE(array_agg(ts.target) FILTER (WHERE ts.state = 'stored' AND ts.target = ANY($2)), '{}')
	FROM file_backup_object o
	LEFT JOIN file_backup_target_status ts ON ts."externalID" = o."externalID"
	GROUP BY o."externalID"
	HAVING count(*) FILTER (WHERE ts.state = 'stored' AND ts.target = ANY($2)) < $1`
	rows, err := r.p.Query(ctx, q, len(allTargets), allTargets)
	if err != nil {
		return fmt.Errorf("target gaps: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var externalID string
		var storedList []string
		if err := rows.Scan(&externalID, &storedList); err != nil {
			return fmt.Errorf("scan gap: %w", err)
		}
		stored := make(map[string]bool, len(storedList))
		for _, t := range storedList {
			stored[t] = true
		}
		if err := fn(externalID, stored); err != nil {
			return err
		}
	}
	return rows.Err()
}

// CoverageGaps counts objects NOT stored on every configured target — the coverage
// backstop gauge (a dead-lettered object leaves the pending backlog but is still
// under-replicated, invisible to the backlog/lag gauges). Same filter as TargetGaps.
func (r *LedgerRepo) CoverageGaps(ctx context.Context, allTargets []string) (int, error) {
	const q = `SELECT count(*) FROM (
	  SELECT o."externalID"
	  FROM file_backup_object o
	  LEFT JOIN file_backup_target_status ts ON ts."externalID" = o."externalID"
	  GROUP BY o."externalID"
	  HAVING count(*) FILTER (WHERE ts.state = 'stored' AND ts.target = ANY($2)) < $1
	) g`
	var n int
	if err := r.p.QueryRow(ctx, q, len(allTargets), allTargets).Scan(&n); err != nil {
		return 0, fmt.Errorf("coverage gaps: %w", err)
	}
	return n, nil
}

// LastVerifiedAge returns the age (seconds) of the STALEST target's most recent verify
// — the max over targets of now()-max(verifiedAt) (FR-026's RPO signal). It is
// PESSIMISTIC on purpose: a global max(verifiedAt) reads healthy while a single target
// (e.g. the immutable off-site copy) silently receives nothing, because another target's
// traffic keeps the global max current. Aggregating per-target and taking the worst
// makes one lagging target drive the signal unhealthy. ok=false when nothing verified yet.
func (r *LedgerRepo) LastVerifiedAge(ctx context.Context) (ageSec float64, ok bool, err error) {
	const q = `SELECT EXTRACT(EPOCH FROM max(now() - mv)) FROM (
	  SELECT max("verifiedAt") AS mv FROM file_backup_target_status
	  WHERE state = 'stored' GROUP BY target
	) t`
	var age *float64
	if err := r.p.QueryRow(ctx, q).Scan(&age); err != nil {
		return 0, false, fmt.Errorf("last verified age: %w", err)
	}
	if age == nil { // no verified rows yet on any target
		return 0, false, nil
	}
	return *age, true, nil
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
