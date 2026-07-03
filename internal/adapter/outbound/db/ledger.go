package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db/queries"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// LedgerRepo implements domain.Ledger over the sqlc-generated queries (own DB).
type LedgerRepo struct {
	q *queries.Queries
}

// NewLedgerRepo binds a LedgerRepo to the ledger pool.
func NewLedgerRepo(p *Pool) *LedgerRepo { return &LedgerRepo{q: queries.New(p.Pool)} }

// UpsertObject records an object (idempotent).
func (r *LedgerRepo) UpsertObject(ctx context.Context, e domain.ObjectMeta) error {
	return r.q.UpsertObject(ctx, queries.UpsertObjectParams{
		ExternalID:        e.ExternalID,
		Size:              e.Size,
		CreatedBy:         uuidOrNull(e.CreatedBy),
		SourceCreatedDate: timestamptzOrNull(e.SourceCreatedDate),
	})
}

// RecordTargetStatuses writes every per-target status in one batched round-trip.
func (r *LedgerRepo) RecordTargetStatuses(ctx context.Context, externalID string, statuses []domain.TargetStatus) error {
	if len(statuses) == 0 {
		return nil
	}
	params := make([]queries.BatchUpsertTargetStatusParams, len(statuses))
	for i, s := range statuses {
		params[i] = queries.BatchUpsertTargetStatusParams{
			ExternalID:  externalID,
			Target:      s.Target,
			State:       s.State,
			StoredBytes: pgtype.Int8{Int64: s.StoredBytes, Valid: true},
		}
	}
	var firstErr error
	// Exec closes the underlying batch results; capture the first per-query error.
	r.q.BatchUpsertTargetStatus(ctx, params).Exec(func(_ int, err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	})
	if firstErr != nil {
		return fmt.Errorf("batch target status: %w", firstErr)
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
