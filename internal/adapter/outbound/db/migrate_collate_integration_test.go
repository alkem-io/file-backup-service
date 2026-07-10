//go:build integration

package db

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"

	dbmigrations "github.com/alkem-io/file-backup-service/db"
	"github.com/alkem-io/file-backup-service/internal/testsupport/pg"
)

// externalIDCollation reads the effective collation of a table's "externalID" column from the
// catalog (an empty collname means the database default).
func externalIDCollation(t *testing.T, p *Pool, table string) string {
	t.Helper()
	var coll string
	err := p.QueryRow(context.Background(),
		`SELECT COALESCE(c.collname, '') FROM pg_attribute a
		 LEFT JOIN pg_collation c ON c.oid = a.attcollation
		 WHERE a.attrelid = $1::regclass AND a.attname = 'externalID'`, table).Scan(&coll)
	if err != nil {
		t.Fatalf("collation query %s: %v", table, err)
	}
	return coll
}

// TestIntegrationExternalIDCollationIsC: the 000002 forward migration must have re-collated BOTH
// "externalID" columns to "C" (byte order), so the COLLATE "C" keyset/inventory queries stay
// index-backed and the DB order matches mergeInventory's byte-order merge. FAILS if 000002 is
// missing/reverted (the columns would keep the default collation).
func TestIntegrationExternalIDCollationIsC(t *testing.T) {
	p := ledgerPool(t)
	for _, tbl := range []string{"file_backup_object", "file_backup_target_status"} {
		if c := externalIDCollation(t, p, tbl); c != "C" {
			t.Fatalf("%s.externalID collation = %q, want C (000002 not applied?)", tbl, c)
		}
	}
}

// TestIntegrationMigrationCollateReversible: the 000002 up/down round-trips on a FRESH database — up
// sets "externalID" to C, down reverts it to the database default, up re-applies C — proving the
// migration is reversible + idempotent (as a forward migration must be, NOT an in-place edit to the
// already-applied 000001 that golang-migrate would no-op on an existing DB).
func TestIntegrationMigrationCollateReversible(t *testing.T) {
	ctx := context.Background()
	h, err := pg.Start(ctx)
	if err != nil {
		t.Fatalf("fresh harness: %v", err)
	}
	t.Cleanup(func() { h.Cleanup(ctx) })

	m := newMigratorForTest(t, h.LedgerDSN())
	t.Cleanup(func() { _, _ = m.Close() })
	p, err := NewPool(ctx, h.LedgerDSN(), 2, 30*time.Second)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(p.Close)

	if err := m.Up(); err != nil {
		t.Fatalf("up: %v", err)
	}
	if c := externalIDCollation(t, p, "file_backup_object"); c != "C" {
		t.Fatalf("after up: collation = %q, want C", c)
	}
	// Down ONE step (revert only 000002, keep the 000001 schema) → the default collation.
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 1: %v", err)
	}
	if c := externalIDCollation(t, p, "file_backup_object"); c == "C" {
		t.Fatal("after down: collation still C — the 000002 down migration did not revert it")
	}
	// Up again → C, proving the migration is re-appliable (idempotent + reversible).
	if err := m.Up(); err != nil {
		t.Fatalf("re-up: %v", err)
	}
	if c := externalIDCollation(t, p, "file_backup_object"); c != "C" {
		t.Fatalf("after re-up: collation = %q, want C", c)
	}
}

func newMigratorForTest(t *testing.T, dsn string) *migrate.Migrate {
	t.Helper()
	src, err := iofs.New(dbmigrations.FS, "migrations")
	if err != nil {
		t.Fatalf("migrate source: %v", err)
	}
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	driver, err := migratepgx.WithInstance(sqldb, &migratepgx.Config{})
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		t.Fatalf("migrate init: %v", err)
	}
	return m
}
