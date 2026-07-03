package db

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	dbmigrations "github.com/alkem-io/file-backup-service/db"
)

// Migrate runs the embedded ledger migrations up against the ledger DB dsn.
// Idempotent (ErrNoChange is not an error). Intended to run as a dedicated step
// (a `migrate` subcommand / k8s init-container), not per-replica on startup.
//
// It opens via the database/sql pgx driver, which accepts BOTH URL and libpq
// keyword DSNs — the same grammar pgxpool accepts for serve — so a keyword-form
// DSN that boots serve also migrates (no fragile scheme string-munging).
func Migrate(dsn string) error {
	src, err := iofs.New(dbmigrations.FS, "migrations")
	if err != nil {
		return fmt.Errorf("migrate source: %w", err)
	}
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open ledger db: %w", err)
	}
	defer func() { _ = sqldb.Close() }()
	driver, err := migratepgx.WithInstance(sqldb, &migratepgx.Config{})
	if err != nil {
		return fmt.Errorf("migrate driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("migrate init: %w", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}
