// Package db holds the Postgres adapters: the outbox reader (Alkemio DB, scoped
// role) and the ledger store (this service's own DB), plus the ledger migrator.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps a pgx connection pool; it satisfies the health Pinger (Ping).
type Pool struct {
	*pgxpool.Pool
}

// NewPool opens a pgx pool for dsn.
func NewPool(ctx context.Context, dsn string) (*Pool, error) {
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool open: %w", err)
	}
	return &Pool{Pool: p}, nil
}
