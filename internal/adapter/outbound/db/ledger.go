package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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

// UpsertTargetStatus records per-(object,target) completion.
func (r *LedgerRepo) UpsertTargetStatus(ctx context.Context, externalID, target, state string, storedBytes int64) error {
	return r.q.UpsertTargetStatus(ctx, queries.UpsertTargetStatusParams{
		ExternalID:  externalID,
		Target:      target,
		State:       state,
		StoredBytes: pgtype.Int8{Int64: storedBytes, Valid: true},
	})
}

// TargetState returns the recorded (state, storedBytes) for (externalID, target).
func (r *LedgerRepo) TargetState(ctx context.Context, externalID, target string) (string, int64, error) {
	row, err := r.q.GetTargetStatus(ctx, queries.GetTargetStatusParams{ExternalID: externalID, Target: target})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", 0, nil
		}
		return "", 0, fmt.Errorf("get target status: %w", err)
	}
	var stored int64
	if row.StoredBytes.Valid {
		stored = row.StoredBytes.Int64
	}
	return row.State, stored, nil
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
