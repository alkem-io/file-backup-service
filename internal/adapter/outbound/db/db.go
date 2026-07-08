// Package db holds the Postgres adapters: the outbox reader (Alkemio DB, scoped
// role) and the ledger store (this service's own DB), plus the ledger migrator.
package db

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// nullTime maps a scanned nullable TIMESTAMPTZ to a time.Time (zero when SQL NULL) — one
// owner for the pgtype→domain breadcrumb mapping shared by the ledger + corpus readers.
func nullTime(t pgtype.Timestamptz) time.Time {
	if t.Valid {
		return t.Time
	}
	return time.Time{}
}

// sliceToSet builds a membership set from a slice of target names — the one owner of the
// stored-target set-build, so StoredTargets (the dedup source of truth) and targetGapsPage
// (the reconcile work-list) can't diverge on how a target-name list becomes a set.
func sliceToSet(xs []string) map[string]bool {
	set := make(map[string]bool, len(xs))
	for _, x := range xs {
		set[x] = true
	}
	return set
}

// Pool is a thin pgxpool.Pool wrapper, so this package sets the pool's explicit
// MaxConns in one place.
type Pool struct {
	*pgxpool.Pool
}

// NewPool opens a pgx pool for dsn with an explicit MaxConns and a server-side
// statement_timeout. Sizing matters: the permanent LISTEN connection plus concurrent
// Claim/MarkDone/Fail/health must all fit, or bookkeeping starves under a NOTIFY burst
// (default max is only max(4, NumCPU)). statementTimeout bounds EVERY query on the pool
// server-side, so a slow/overloaded DB can't hang a worker indefinitely in an otherwise
// unbounded Claim/reap — the query is aborted and retried instead. 0 leaves it unset.
// (The migrate step opens its own database/sql connection, NOT this pool, so long DDL
// like CREATE INDEX is unaffected by this bound.) statement_timeout does not affect a
// connection parked in WaitForNotification — LISTEN already returned; the wait is a
// client-side socket read, not a running statement.
func NewPool(ctx context.Context, dsn string, maxConns int32, statementTimeout time.Duration) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	if statementTimeout > 0 {
		cfg.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(statementTimeout.Milliseconds(), 10)
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgxpool open: %w", err)
	}
	return &Pool{Pool: p}, nil
}
