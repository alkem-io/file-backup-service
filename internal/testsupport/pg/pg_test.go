//go:build integration

package pg

import (
	"context"
	"strings"
	"testing"
)

// TestHarnessErrorPathsAndRendering exercises the harness's own reachable ERROR branches and its
// pure renderers, so the integration test harness is COVERED by the §VII bar rather than excluded from
// it. The container-startup failure paths in Start (postgres.Run / c.Host / c.MappedPort erroring) are
// the only residual uncovered lines — they need a broken Docker daemon and can't be exercised in a
// passing run.
func TestHarnessErrorPathsAndRendering(t *testing.T) {
	ctx := context.Background()
	h, err := Start(ctx)
	if err != nil {
		t.Fatalf("start harness: %v", err)
	}
	t.Cleanup(func() { h.Cleanup(ctx) })

	// Exec / ScalarRow against a NON-EXISTENT database → pgx.Connect fails (the DB is selected at
	// connection startup), exercising the connect-error return branch of each.
	if err := h.Exec(ctx, "no_such_db", "SELECT 1"); err == nil {
		t.Fatal("Exec against a nonexistent DB must return a connect error")
	}
	var x int
	if err := h.ScalarRow(ctx, "no_such_db", "SELECT 1", &x); err == nil {
		t.Fatal("ScalarRow against a nonexistent DB must return a connect error")
	}

	// Error propagation against a REAL database: a bad statement / a no-row scan surface the driver
	// error through Exec / ScalarRow.
	if err := h.Exec(ctx, h.AlkemioDB, "NOT VALID SQL"); err == nil {
		t.Fatal("Exec with invalid SQL must return an error")
	}
	if err := h.ScalarRow(ctx, h.AlkemioDB, "SELECT 1 WHERE false", &x); err == nil {
		t.Fatal("ScalarRow with no row must return an error")
	}
	// Happy-path Exec + ScalarRow round-trip (a real statement + a scanned value).
	if err := h.Exec(ctx, h.AlkemioDB, `INSERT INTO file (id) VALUES (gen_random_uuid())`); err != nil {
		t.Fatalf("Exec insert: %v", err)
	}
	var n int
	if err := h.ScalarRow(ctx, h.AlkemioDB, "SELECT count(*) FROM file", &n); err != nil || n != 1 {
		t.Fatalf("ScalarRow count: n=%d err=%v", n, err)
	}

	// A SECOND provision must fail at CREATE DATABASE (the ledger DB already exists), exercising
	// provision's Exec-error branch.
	if err := h.provision(ctx); err == nil {
		t.Fatal("a repeat provision must fail (ledger DB already exists)")
	}

	// Pure renderers: the DSN helpers and ConfigYAML.
	if got := h.AlkemioDSN(); !strings.Contains(got, "/"+h.AlkemioDB+"?") {
		t.Fatalf("AlkemioDSN missing db: %q", got)
	}
	if got := h.LedgerDSN(); !strings.Contains(got, "/"+h.LedgerDB+"?") {
		t.Fatalf("LedgerDSN missing db: %q", got)
	}
	cfg := h.ConfigYAML("http://fs", map[string]string{"local": "/tmp/x"})
	for _, want := range []string{"fileServiceBase: http://fs", "alkemioDB:", "ledgerDB:", "type: filesystem", "path: /tmp/x"} {
		if !strings.Contains(cfg, want) {
			t.Fatalf("ConfigYAML missing %q in:\n%s", want, cfg)
		}
	}
}
