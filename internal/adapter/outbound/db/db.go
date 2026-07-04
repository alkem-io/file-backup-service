// Package db holds the Postgres adapters: the outbox reader (Alkemio DB, scoped
// role) and the ledger store (this service's own DB), plus the ledger migrator.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
