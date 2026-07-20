//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db"
	"github.com/alkem-io/file-backup-service/internal/testsupport/pg"
)

// harness is one Postgres container for the whole cmd integration suite; the ledger is migrated
// once in TestMain so every subcommand test has its tables.
var harness *pg.Harness

func TestMain(m *testing.M) {
	ctx := context.Background()
	h, err := pg.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration harness:", err)
		os.Exit(1)
	}
	harness = h
	if err := db.Migrate(h.LedgerDSN()); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		h.Cleanup(ctx)
		os.Exit(1)
	}
	code := m.Run()
	h.Cleanup(ctx)
	os.Exit(code)
}

// (sha3hex is defined in main_test.go — reused here.)

// stubFileService serves object content at GET /internal/blob/{hash}/content — the
// content-addressed read the worker now uses: it keys on the object's SHA3-256 hash, not the
// document id. Callers still pass a fileId→bytes map (the fileId is the outbox breadcrumb); the
// stub re-keys it by sha3hex(bytes), which equals the outbox externalID the worker requests. A
// known hash returns its bytes; anything else 404s (which Preflight treats as reachable), so the
// preflightProbeHash probe passes.
func stubFileService(t *testing.T, content map[uuid.UUID][]byte) *httptest.Server {
	t.Helper()
	byHash := make(map[string][]byte, len(content))
	for _, b := range content {
		byHash[sha3hex(b)] = b
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/") // /internal/blob/{hash}/content
		if len(parts) < 4 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if b, ok := byHash[parts[3]]; ok {
			_, _ = w.Write(b)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// harnessConfig renders a config.yaml pointing at the harness + a single filesystem target in a
// temp dir + the given file-service base, plus any extra top-level lines, and returns its path
// and the target's root dir. (Uses writeConfig from main_test.go to write it.)
func harnessConfig(t *testing.T, fileServiceBase string, extra ...string) (cfgPath, targetDir string) {
	t.Helper()
	targetDir = t.TempDir()
	yaml := harness.ConfigYAML(fileServiceBase, map[string]string{"local": targetDir})
	if len(extra) > 0 {
		yaml += "\n" + strings.Join(extra, "\n") + "\n"
	}
	return writeConfig(t, t.TempDir(), yaml), targetDir
}

// storedPath is the sharded on-disk path a filesystem target uses for a hash.
func storedPath(root, hash string) string {
	return filepath.Join(root, hash[0:2], hash[2:4], hash)
}

func TestIntegrationRunMigrate(t *testing.T) {
	// TestMain already migrated; runMigrate must be idempotent through the CLI entrypoint.
	cfgPath, _ := harnessConfig(t, "http://unused")
	if err := runMigrate(cfgPath); err != nil {
		t.Fatalf("runMigrate: %v", err)
	}
}

func TestIntegrationRunReconcileEmpty(t *testing.T) {
	// An empty ledger has no gaps — reconcile exercises the full wiring (ledgerJob, openPool,
	// scratch + target preflight, NewReconciler, Run) and exits clean.
	cfgPath, _ := harnessConfig(t, "http://unused")
	if err := runReconcile([]string{"--config", cfgPath}); err != nil {
		t.Fatalf("runReconcile (empty ledger) must be clean, got %v", err)
	}
}

func TestIntegrationRunAuditEmpty(t *testing.T) {
	cfgPath, _ := harnessConfig(t, "http://unused")
	if err := runAudit([]string{"--config", cfgPath}); err != nil {
		t.Fatalf("runAudit (empty ledger) must be clean, got %v", err)
	}
}

func TestIntegrationRunBackfill(t *testing.T) {
	ctx := context.Background()
	content := []byte("backfill me — a pre-existing corpus object")
	h := sha3hex(content)
	fid := uuid.New()
	// Seed the corpus row file-service would have.
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file (id,"externalID",size,"temporaryLocation") VALUES ($1,$2,$3,false)`,
		fid, h, int64(len(content))); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	fs := stubFileService(t, map[uuid.UUID][]byte{fid: content})
	cfgPath, targetDir := harnessConfig(t, fs.URL)

	if err := runBackfill([]string{"--config", cfgPath}); err != nil {
		t.Fatalf("runBackfill: %v", err)
	}
	if _, err := os.Stat(storedPath(targetDir, h)); err != nil {
		t.Fatalf("backfilled object not stored at the target: %v", err)
	}
}

func TestIntegrationServeBacksUpOutboxRow(t *testing.T) {
	ctx := context.Background()
	content := []byte("serve me — an enqueued object")
	h := sha3hex(content)
	fid := uuid.New()
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file_backup_outbox ("fileId","externalID",size,status) VALUES ($1,$2,$3,'pending')`,
		fid, h, int64(len(content))); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
	fs := stubFileService(t, map[uuid.UUID][]byte{fid: content})
	// A distinct metrics port so serve's HTTP bind can't clash with a real 4004.
	cfgPath, targetDir := harnessConfig(t, fs.URL, "metricsPort: 14087", "pollEverySec: 1")

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveCtx(sctx, cfgPath) }()

	// Wait for the worker to back up the object, then shut it down.
	stored := storedPath(targetDir, h)
	deadline := time.After(30 * time.Second)
	for {
		if _, err := os.Stat(stored); err == nil {
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatal("serve did not back up the enqueued object within 30s")
		case err := <-done:
			t.Fatalf("serveCtx returned early: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !isCanceled(err) {
			t.Fatalf("serveCtx must return cleanly on shutdown, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveCtx did not return after cancel")
	}
	// The outbox row is marked done.
	var status string
	if err := harness.ScalarRow(ctx, harness.AlkemioDB,
		fmt.Sprintf(`SELECT status FROM file_backup_outbox WHERE "externalID"='%s'`, h), &status); err != nil {
		t.Fatalf("read outbox status: %v", err)
	}
	if status != "done" {
		t.Fatalf("outbox status = %q, want done", status)
	}
}

func isCanceled(err error) bool {
	return err != nil && strings.Contains(err.Error(), context.Canceled.Error())
}
