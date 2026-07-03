package db

import (
	"context"

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
		ExternalID: e.ExternalID,
		Size:       e.Size,
		CreatedBy:  uuidOrNull(e.CreatedBy),
		MimeType:   textOrNull(e.MimeType),
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

func uuidOrNull(s string) pgtype.UUID {
	var u pgtype.UUID
	if s == "" {
		return u
	}
	_ = u.Scan(s) // invalid uuid -> null
	return u
}

func textOrNull(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}
