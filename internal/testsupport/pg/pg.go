//go:build integration

// Package pg is a build-tagged (integration) test harness: it starts a throwaway Postgres
// container (testcontainers), creates the alkemio (outbox + file corpus) and filebackup
// (ledger) databases, and creates the FOREIGN, server-owned tables the adapters read
// (file_backup_outbox, file) with the schema the code expects. Shared by the db and cmd
// integration suites so the live-DB paths (Migrate, NewPool, the subcommand middles) are
// covered per constitution §VII — reserved for the integration suite, not the coverage bar's
// unit path. Only compiled under `-tags integration`.
package pg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Harness owns a running Postgres container and the connection parameters for both databases.
type Harness struct {
	Host           string
	Port           int
	User, Password string
	AlkemioDB      string // outbox + file corpus
	LedgerDB       string // this service's own ledger
	container      *postgres.PostgresContainer
}

// Start boots a Postgres container, creates the two databases, and provisions the foreign
// outbox + file tables in the alkemio DB. The caller MUST defer Cleanup.
func Start(ctx context.Context) (*Harness, error) {
	// Disable the Ryuk reaper sidecar: it can't mount the docker.sock under rootless/colima
	// Docker (mkdir ... operation not supported), and we terminate the container explicitly in
	// Cleanup anyway. Must be set before postgres.Run creates the reaper.
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	c, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("alkemio"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		return nil, err
	}
	h := &Harness{
		Host: host, Port: int(port.Num()), User: "test", Password: "test",
		AlkemioDB: "alkemio", LedgerDB: "filebackup", container: c,
	}
	if err := h.provision(ctx); err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	return h, nil
}

// dsn renders a libpq URL for one database in the container.
func (h *Harness) dsn(db string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", h.User, h.Password, h.Host, h.Port, db)
}

// AlkemioDSN is the outbox + file-corpus DB connection string.
func (h *Harness) AlkemioDSN() string { return h.dsn(h.AlkemioDB) }

// LedgerDSN is the ledger DB connection string.
func (h *Harness) LedgerDSN() string { return h.dsn(h.LedgerDB) }

// provision creates the ledger DB and the foreign outbox + file tables in alkemio.
func (h *Harness) provision(ctx context.Context) error {
	admin, err := pgx.Connect(ctx, h.AlkemioDSN())
	if err != nil {
		return fmt.Errorf("connect alkemio: %w", err)
	}
	defer func() { _ = admin.Close(ctx) }()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+h.LedgerDB); err != nil {
		return fmt.Errorf("create ledger db: %w", err)
	}
	// The FOREIGN, server-owned tables the outbox + corpus adapters read. These live in
	// file-service in production; here we create the shape the code's SQL expects (the Probe
	// column lists are the contract). A NOTIFY trigger isn't needed — the poll floor drives
	// progress in the test.
	ddl := []string{
		`CREATE TABLE file_backup_outbox (
			id           BIGSERIAL PRIMARY KEY,
			"fileId"     UUID NOT NULL,
			"externalID" VARCHAR(128) NOT NULL,
			priority     INT NOT NULL DEFAULT 0,
			status       VARCHAR(20) NOT NULL DEFAULT 'pending',
			attempts     INT,
			deliveries   INT,
			"lastError"  TEXT,
			"createdBy"  UUID,
			"createdDate" TIMESTAMPTZ DEFAULT now(),
			size         BIGINT,
			"claimedAt"  TIMESTAMPTZ,
			"visibleAt"  TIMESTAMPTZ
		)`,
		`CREATE TABLE file (
			id           UUID PRIMARY KEY,
			"externalID" VARCHAR(128),
			"createdBy"  UUID,
			"createdDate" TIMESTAMPTZ,
			"updatedDate" TIMESTAMPTZ,
			size         BIGINT,
			"temporaryLocation" BOOLEAN
		)`,
	}
	for _, stmt := range ddl {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("provision foreign table: %w", err)
		}
	}
	return nil
}

// Exec runs a statement against the given database (test setup: seed outbox/file rows).
func (h *Harness) Exec(ctx context.Context, db, sql string, args ...any) error {
	conn, err := pgx.Connect(ctx, h.dsn(db))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, sql, args...)
	return err
}

// ScalarRow scans a single-row, no-arg query against the given database (test assertions).
func (h *Harness) ScalarRow(ctx context.Context, db, sql string, dst ...any) error {
	conn, err := pgx.Connect(ctx, h.dsn(db))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	return conn.QueryRow(ctx, sql).Scan(dst...)
}

// ConfigYAML renders a config.yaml pointing at this container, the given fileServiceBase, and
// the given filesystem targets (name -> path). Secrets aren't needed (filesystem targets), and
// the container has no TLS, so sslMode=disable. Used by the cmd integration suite.
func (h *Harness) ConfigYAML(fileServiceBase string, filesystemTargets map[string]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "fileServiceBase: %s\n", fileServiceBase)
	writeDB := func(name, db string) {
		fmt.Fprintf(&b, "%s:\n  host: %s\n  port: %d\n  user: %s\n  password: %s\n  dbName: %s\n  sslMode: disable\n",
			name, h.Host, h.Port, h.User, h.Password, db)
	}
	writeDB("alkemioDB", h.AlkemioDB)
	writeDB("ledgerDB", h.LedgerDB)
	b.WriteString("targets:\n")
	for name, path := range filesystemTargets {
		fmt.Fprintf(&b, "  - name: %s\n    type: filesystem\n    path: %s\n", name, path)
	}
	return b.String()
}

// Cleanup terminates the container.
func (h *Harness) Cleanup(ctx context.Context) {
	if h.container != nil {
		_ = h.container.Terminate(ctx)
	}
}
