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

// TestIntegrationExternalIDCollationIsC: the ledger migration (000001) must create BOTH "externalID"
// columns BORN with collation "C" (byte order), so the COLLATE "C" keyset/inventory queries stay
// index-backed and the DB order matches mergeInventory's byte-order merge. FAILS if the CREATE TABLE
// loses the COLLATE "C" (the columns would take the database default collation).
func TestIntegrationExternalIDCollationIsC(t *testing.T) {
	p := ledgerPool(t)
	for _, tbl := range []string{"file_backup_object", "file_backup_target_status"} {
		if c := externalIDCollation(t, p, tbl); c != "C" {
			t.Fatalf("%s.externalID collation = %q, want C (CREATE TABLE lost COLLATE \"C\"?)", tbl, c)
		}
	}
}

// regClassText returns to_regclass(table) as text ("" when the relation does not exist).
func regClassText(t *testing.T, p *Pool, table string) string {
	t.Helper()
	var reg sql.NullString
	if err := p.QueryRow(context.Background(), `SELECT to_regclass($1)::text`, table).Scan(&reg); err != nil {
		t.Fatalf("to_regclass %s: %v", table, err)
	}
	return reg.String
}

// TestIntegrationMigration000001Reversible: the ledger migration round-trips on a FRESH database — up
// creates both tables with "externalID" BORN COLLATE "C", down drops them cleanly, up recreates them
// with C — proving 000001 is reversible + idempotent. The collation is defined at CREATE TABLE, NOT
// re-collated by a later migration: the ledger is a fresh, unreleased schema, so a brand-new (empty)
// table is born with the right collation and there is no blocking ALTER COLUMN TYPE table rewrite.
func TestIntegrationMigration000001Reversible(t *testing.T) {
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
	for _, tbl := range []string{"file_backup_object", "file_backup_target_status"} {
		if c := externalIDCollation(t, p, tbl); c != "C" {
			t.Fatalf("after up: %s.externalID collation = %q, want C", tbl, c)
		}
	}
	// Down → the whole ledger schema is dropped (000001 down is DROP TABLE); the tables must be gone.
	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}
	if reg := regClassText(t, p, "file_backup_object"); reg != "" {
		t.Fatalf("after down: file_backup_object still exists (%q) — 000001 down must drop it", reg)
	}
	// Up again → recreated with C, proving the migration is re-appliable (idempotent + reversible).
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
