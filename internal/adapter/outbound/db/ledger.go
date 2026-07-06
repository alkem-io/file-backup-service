package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db/queries"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// LedgerRepo implements domain.Ledger over sqlc-generated queries plus a few hand-written
// pgx queries that sqlc's model can't express (constitution §IV exceptions):
//   - RecordBackup: a multi-arg unnest() CTE (sqlc's analyzer can't type it).
//   - StoredObjectsPage / TargetGaps: array_agg / keyset streaming shapes returning a
//     custom row set (TargetGaps also streams to bound memory).
//   - CoverageGaps / LastVerifiedAge: aggregate scalars over a text[] target filter.
//   - Probe: a schema-existence check, not a data query.
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
		obj.ExternalID, obj.Size, obj.CreatedBy, timestamptzOrNull(obj.SourceCreatedDate),
		targets, states, storedBytes, obj.SizeVerified); err != nil {
		return fmt.Errorf("record backup: %w", err)
	}
	return nil
}

// StoredObjectsPage returns up to limit objects stored ON target (FR-015 manifest /
// FR-014 audit), keyset-paginated by externalID. The join lists ONLY what the target
// holds (not the whole ledger), and the (target, state, "externalID") index makes the
// WHERE + ORDER an index-ordered range scan — no full sort, and the connection is
// released when the page returns (not held for the whole manifest upload / audit sweep).
func (r *LedgerRepo) StoredObjectsPage(ctx context.Context, target, after string, limit int) ([]domain.ObjectMeta, error) {
	const q = `SELECT o."externalID", o.size, o."createdBy", o."sourceCreatedDate"
	FROM file_backup_object o
	JOIN file_backup_target_status ts ON ts."externalID" = o."externalID"
	WHERE ts.target = $1 AND ts.state = 'stored' AND o."externalID" > $2
	ORDER BY o."externalID" LIMIT $3`
	rows, err := r.p.Query(ctx, q, target, after, limit)
	if err != nil {
		return nil, fmt.Errorf("stored objects page: %w", err)
	}
	defer rows.Close()
	out := make([]domain.ObjectMeta, 0, limit)
	for rows.Next() {
		var m domain.ObjectMeta
		var created pgtype.Timestamptz
		if err := rows.Scan(&m.ExternalID, &m.Size, &m.CreatedBy, &created); err != nil {
			return nil, fmt.Errorf("scan object: %w", err)
		}
		m.SourceCreatedDate = nullTime(created)
		out = append(out, m)
	}
	return out, rows.Err()
}

// TargetGaps invokes fn for every object NOT stored on every configured target, with the
// set of CURRENT targets that DO hold each — the reconcile work-list. It KEYSET-PAGES by
// externalID, releasing the ledger connection between pages: reconcile runs the full
// per-object repair (fetch + decode-to-tempfile + fan-out + nested ledger writes) inside
// fn, so a held cursor would pin a connection + an open snapshot for the whole multi-hour
// pass (blocking VACUUM on the ledger). An object stored on all of allTargets is excluded;
// stale statuses for removed targets are ignored (the count/agg filter to allTargets).
func (r *LedgerRepo) TargetGaps(ctx context.Context, allTargets []string, fn func(string, map[string]bool) error) error {
	after := ""
	for {
		page, err := r.targetGapsPage(ctx, allTargets, after, dbPageSize)
		if err != nil {
			return err
		}
		for i := range page {
			if err := fn(page[i].externalID, page[i].stored); err != nil {
				return err
			}
		}
		if len(page) < dbPageSize {
			return nil // a short page is the last
		}
		after = page[len(page)-1].externalID
	}
}

type targetGap struct {
	externalID string
	stored     map[string]bool
}

// targetGapsPage returns one keyset page (externalID order) of under-replicated objects.
func (r *LedgerRepo) targetGapsPage(ctx context.Context, allTargets []string, after string, limit int) ([]targetGap, error) {
	const q = `SELECT o."externalID",
	  COALESCE(array_agg(ts.target) FILTER (WHERE ts.state = 'stored' AND ts.target = ANY($2)), '{}')
	FROM file_backup_object o
	LEFT JOIN file_backup_target_status ts ON ts."externalID" = o."externalID"
	WHERE o."externalID" > $3
	GROUP BY o."externalID"
	HAVING count(*) FILTER (WHERE ts.state = 'stored' AND ts.target = ANY($2)) < $1
	ORDER BY o."externalID" LIMIT $4`
	rows, err := r.p.Query(ctx, q, len(allTargets), allTargets, after, limit)
	if err != nil {
		return nil, fmt.Errorf("target gaps page: %w", err)
	}
	defer rows.Close()
	out := make([]targetGap, 0, limit)
	for rows.Next() {
		var g targetGap
		var storedList []string
		if err := rows.Scan(&g.externalID, &storedList); err != nil {
			return nil, fmt.Errorf("scan gap: %w", err)
		}
		g.stored = make(map[string]bool, len(storedList))
		for _, t := range storedList {
			g.stored[t] = true
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// CoverageGaps counts objects NOT stored on every configured target — the coverage
// backstop gauge (a dead-lettered object leaves the pending backlog but is still
// under-replicated, invisible to the backlog/lag gauges). Computed as total objects
// MINUS fully-replicated objects: the fully-replicated term scans only
// file_backup_target_status (state='stored' AND target=ANY, GROUP BY externalID HAVING
// count>=N), served by the (target,state,externalID) index at scale — avoiding the
// LEFT JOIN + GROUP BY over the whole object heap this ran every 5 min. It MUST stay
// consistent with TargetGaps' "stored on
// target" predicate — TargetGaps streams the gap objects, CoverageGaps counts them; a
// change to one's stored-on-target rule must change the other (see coverage integration test).
// The total is an EXACT count(file_backup_object) (one row per object, index-only on the
// PK), not a pg_class.reltuples estimate: a coverage backstop must not UNDER-report (a
// stale-low estimate would clamp the gap to 0 and hide the very under-replication the
// gauge exists to catch), and the exact count is bounded by the coarse 5-min sample cadence.
func (r *LedgerRepo) CoverageGaps(ctx context.Context, allTargets []string) (int, error) {
	if len(allTargets) == 0 {
		return 0, nil // no targets configured → nothing can be under-replicated (matches TargetGaps)
	}
	const q = `SELECT
	  (SELECT count(*) FROM file_backup_object)
	  - (SELECT count(*) FROM (
	      SELECT "externalID" FROM file_backup_target_status
	      WHERE state = 'stored' AND target = ANY($2)
	      GROUP BY "externalID" HAVING count(*) >= $1
	    ) fully)`
	var n int
	if err := r.p.QueryRow(ctx, q, len(allTargets), allTargets).Scan(&n); err != nil {
		return 0, fmt.Errorf("coverage gaps: %w", err)
	}
	return n, nil
}

// LastVerifiedAge reports the RPO signal (FR-026) over the CONFIGURED targets: the age
// (seconds) of the STALEST target that has verified at least once, plus the count of
// configured targets that have NEVER verified anything. It takes allTargets and probes
// each per-target max(verifiedAt) so a target that has received NOTHING since inception
// (the immutable off-site copy misconfigured at deploy — the worst case) is COUNTED as
// never-verified, not silently absent from a GROUP BY. A global/among-stored-only max
// would read healthy while such a target has zero coverage. ok=false when nothing has
// verified on ANY configured target yet (bootstrap).
func (r *LedgerRepo) LastVerifiedAge(ctx context.Context, allTargets []string) (ageSec float64, neverVerified int, ok bool, err error) {
	const q = `SELECT
	  count(*) FILTER (WHERE mv IS NULL),
	  EXTRACT(EPOCH FROM max(now() - mv))
	FROM (
	  SELECT (SELECT max("verifiedAt") FROM file_backup_target_status
	          WHERE target = t AND state = 'stored') AS mv
	  FROM unnest($1::text[]) AS t
	) per_target`
	var age *float64
	if err := r.p.QueryRow(ctx, q, allTargets).Scan(&neverVerified, &age); err != nil {
		return 0, 0, false, fmt.Errorf("last verified age: %w", err)
	}
	if age == nil { // nothing verified on any configured target yet
		return 0, neverVerified, false, nil
	}
	return *age, neverVerified, true, nil
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

func timestamptzOrNull(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}
