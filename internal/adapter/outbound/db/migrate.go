package db

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the "pgx5" scheme
	"github.com/golang-migrate/migrate/v4/source/iofs"

	dbmigrations "github.com/alkem-io/file-backup-service/db"
)

// Migrate runs the embedded ledger migrations up against the ledger DB dsn.
// Idempotent (ErrNoChange is not an error). Intended to run as a dedicated step
// (a `migrate` subcommand / k8s init-container), not per-replica on startup.
func Migrate(dsn string) error {
	src, err := iofs.New(dbmigrations.FS, "migrations")
	if err != nil {
		return fmt.Errorf("migrate source: %w", err)
	}
	// golang-migrate's pgx/v5 database driver registers the "pgx5" URL scheme.
	url := "pgx5://" + strings.TrimPrefix(strings.TrimPrefix(dsn, "postgresql://"), "postgres://")
	m, err := migrate.NewWithSourceInstance("iofs", src, url)
	if err != nil {
		return fmt.Errorf("migrate init: %w", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}
