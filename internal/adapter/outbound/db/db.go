// Package db holds the Postgres adapters: the outbox reader (Alkemio DB, scoped
// role) and the ledger store (this service's own DB), plus the ledger migrator.
package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dbPageSize is the keyset page size for the connection-releasing corpus/gap sweeps
// (backfill EachFile, reconcile TargetGaps) so a slow per-object consumer never pins a
// pool connection for the whole pass.
const dbPageSize = 1000

// keysetLoop drives a keyset-paginated sweep: fetch pages via pageFn(after, dbPageSize)
// until a SHORT page (the last), invoking fn per item; cursorOf extracts the next `after`
// from the last item. The one owner of the after-cursor + short-page-stops loop, so a copy
// can't mis-treat a short page and silently stop a sweep early. Paging (not a held cursor)
// releases the DB connection between pages for a slow per-item consumer.
func keysetLoop[C any, T any](start C, pageFn func(after C, limit int) ([]T, error), cursorOf func(T) C, fn func(T) error) error {
	after := start
	for {
		page, err := pageFn(after, dbPageSize)
		if err != nil {
			return err
		}
		for i := range page {
			if err := fn(page[i]); err != nil {
				return err
			}
		}
		if len(page) < dbPageSize {
			return nil // a short page is the last
		}
		after = cursorOf(page[len(page)-1])
	}
}

// nullTime maps a scanned nullable TIMESTAMPTZ to a time.Time (zero when SQL NULL) — one
// owner for the pgtype→domain breadcrumb mapping shared by the ledger + corpus readers.
func nullTime(t pgtype.Timestamptz) time.Time {
	if t.Valid {
		return t.Time
	}
	return time.Time{}
}

// Pool is a thin pgxpool.Pool wrapper, so this package sets the pool's explicit
// MaxConns in one place.
type Pool struct {
	*pgxpool.Pool
}

// NewPool opens a pgx pool for dsn with an explicit MaxConns. Sizing matters:
// the permanent LISTEN connection plus concurrent Claim/MarkDone/Fail/health must
// all fit, or bookkeeping starves under a NOTIFY burst (default max is only
// max(4, NumCPU)).
func NewPool(ctx context.Context, dsn string, maxConns int32) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool open: %w", err)
	}
	return &Pool{Pool: p}, nil
}
