package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db/queries"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// LedgerRepo implements domain.Ledger entirely over sqlc-generated queries (db/queries/
// ledger.sql). The ledger tables are THIS service's OWN (db/migrations), so per constitution
// §IV every query is compiled by sqlc — there is NO hand-written pgx here (unlike the foreign,
// server-owned outbox/file tables, which carry the documented §IV waiver because they are not
// in this repo's migrations for sqlc to type against).
type LedgerRepo struct {
	q *queries.Queries
}

// NewLedgerRepo binds a LedgerRepo to the ledger pool.
func NewLedgerRepo(p *Pool) *LedgerRepo { return &LedgerRepo{q: queries.New(p.Pool)} }

// statusRow is the per-target status shape marshaled into the jsonb array RecordBackup's
// jsonb_to_recordset decodes — the json keys MUST match the query's t(target, state, bytes)
// column definition list.
type statusRow struct {
	Target string `json:"target"`
	State  string `json:"state"`
	Bytes  int64  `json:"bytes"`
}

// RecordBackup writes the object row + all per-target statuses atomically (one RTT) via the
// sqlc RecordBackup CTE (object inserted first so the FK check on the status rows passes).
func (r *LedgerRepo) RecordBackup(ctx context.Context, obj domain.ObjectMeta, statuses []domain.TargetStatus) error {
	rows := make([]statusRow, len(statuses))
	for i, s := range statuses {
		rows[i] = statusRow{Target: s.Target, State: s.State, Bytes: s.StoredBytes}
	}
	blob, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("record backup: marshal statuses: %w", err)
	}
	if err := r.q.RecordBackup(ctx, queries.RecordBackupParams{
		ExternalID:        obj.ExternalID,
		Statuses:          blob,
		Size:              obj.Size,
		CreatedBy:         obj.CreatedBy,
		SourceCreatedDate: timestamptzOrNull(obj.SourceCreatedDate),
		SizeVerified:      obj.SizeVerified,
	}); err != nil {
		return fmt.Errorf("record backup: %w", err)
	}
	return nil
}

// StoredObjectsPage returns up to limit objects stored ON target (FR-015 manifest / FR-014
// audit), keyset-paginated by externalID (the connection is released when the page returns).
func (r *LedgerRepo) StoredObjectsPage(ctx context.Context, target, after string, limit int) ([]domain.ObjectMeta, error) {
	rows, err := r.q.StoredObjectsPage(ctx, queries.StoredObjectsPageParams{
		Target: target, After: after, PageLimit: int32(limit), //nolint:gosec // limit is dbPageSize (1000)
	})
	if err != nil {
		return nil, fmt.Errorf("stored objects page: %w", err)
	}
	out := make([]domain.ObjectMeta, len(rows))
	for i, row := range rows {
		out[i] = domain.ObjectMeta{
			ExternalID:        row.ExternalID,
			Size:              row.Size,
			CreatedBy:         row.CreatedBy,
			SourceCreatedDate: nullTime(row.SourceCreatedDate),
		}
	}
	return out, nil
}

// TargetGaps invokes fn for every object NOT stored on every configured target, with the set
// of CURRENT targets that DO hold each — the reconcile work-list. It KEYSET-PAGES by
// externalID (domain.KeysetLoop), releasing the ledger connection between pages so the
// per-object repair inside fn doesn't pin a connection + open snapshot for the whole pass.
func (r *LedgerRepo) TargetGaps(ctx context.Context, allTargets []string, fn func(string, map[string]bool) error) error {
	return domain.KeysetLoop("", dbPageSize,
		func(after string, limit int) ([]targetGap, error) {
			return r.targetGapsPage(ctx, allTargets, after, limit)
		},
		func(g targetGap) string { return g.externalID },
		func(g targetGap) error { return fn(g.externalID, g.stored) })
}

type targetGap struct {
	externalID string
	stored     map[string]bool
}

// targetGapsPage returns one keyset page (externalID order) of under-replicated objects.
func (r *LedgerRepo) targetGapsPage(ctx context.Context, allTargets []string, after string, limit int) ([]targetGap, error) {
	rows, err := r.q.TargetGapsPage(ctx, queries.TargetGapsPageParams{
		Targets:     allTargets,
		After:       after,
		TargetCount: int32(len(allTargets)), //nolint:gosec // configured target count, small
		PageLimit:   int32(limit),           //nolint:gosec // limit is dbPageSize (1000)
	})
	if err != nil {
		return nil, fmt.Errorf("target gaps page: %w", err)
	}
	out := make([]targetGap, len(rows))
	for i, row := range rows {
		stored := make(map[string]bool, len(row.Stored))
		for _, t := range row.Stored {
			stored[t] = true
		}
		out[i] = targetGap{externalID: row.ExternalID, stored: stored}
	}
	return out, nil
}

// CoverageGaps counts objects NOT stored on every configured target — the coverage backstop
// gauge (a dead-lettered object leaves the pending backlog but is still under-replicated).
// Computed as total objects MINUS fully-replicated; MUST stay consistent with TargetGaps'
// stored-on-target predicate (see the coverage integration test). The total is an EXACT
// count, not a reltuples estimate — a backstop must never under-report.
func (r *LedgerRepo) CoverageGaps(ctx context.Context, allTargets []string) (int, error) {
	if len(allTargets) == 0 {
		return 0, nil // no targets configured → nothing can be under-replicated (matches TargetGaps)
	}
	n, err := r.q.CoverageGaps(ctx, queries.CoverageGapsParams{
		Targets: allTargets, TargetCount: int32(len(allTargets)), //nolint:gosec // configured target count, small
	})
	if err != nil {
		return 0, fmt.Errorf("coverage gaps: %w", err)
	}
	return int(n), nil
}

// LastVerifiedAge reports the RPO signal (FR-026): the age (seconds) of the STALEST configured
// target that has verified at least once, plus the count of configured targets that have NEVER
// verified. ok=false only at bootstrap — nothing has verified on ANY configured target — which
// is exactly when every target is never-verified, so it's derived from the count rather than a
// nullable age (the query COALESCEs the age to 0 in that case).
func (r *LedgerRepo) LastVerifiedAge(ctx context.Context, allTargets []string) (ageSec float64, neverVerified int, ok bool, err error) {
	if len(allTargets) == 0 {
		return 0, 0, false, nil
	}
	row, qerr := r.q.LastVerifiedAge(ctx, allTargets)
	if qerr != nil {
		return 0, 0, false, fmt.Errorf("last verified age: %w", qerr)
	}
	ok = int(row.NeverVerified) < len(allTargets) // at least one target has verified
	return row.StalestAgeSec, int(row.NeverVerified), ok, nil
}

// Probe verifies both ledger tables exist + are readable via the pool's role. A missing
// table (skipped migration) errors; an empty table is success (EXISTS returns false, not NULL).
func (r *LedgerRepo) Probe(ctx context.Context) error {
	if _, err := r.q.Probe(ctx); err != nil {
		return fmt.Errorf("ledger probe (schema/migrate?): %w", err)
	}
	return nil
}

// StoredTargets returns the set of target names already in state='stored' for externalID
// (the dedup source of truth) — the 'stored' filter is in SQL (ListStoredTargets), not Go.
func (r *LedgerRepo) StoredTargets(ctx context.Context, externalID string) (map[string]bool, error) {
	targets, err := r.q.ListStoredTargets(ctx, externalID)
	if err != nil {
		return nil, fmt.Errorf("list stored targets: %w", err)
	}
	out := make(map[string]bool, len(targets))
	for _, t := range targets {
		out[t] = true
	}
	return out, nil
}

// timestamptzOrNull maps a domain time.Time breadcrumb (zero => SQL NULL) to pgtype.Timestamptz.
func timestamptzOrNull(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}
