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

// stubFileService is a stand-in for file-service's content-addressed read
// GET /internal/blob/{hash}/content. It keys on the object's SHA3-256 hash (the content itself),
// so callers pass the bodies — there is no id: the stub keys each by sha3hex(body), which equals
// the outbox/corpus externalID the worker requests. A present hash → 200 + bytes; anything else
// (incl. the Preflight probe's valid-but-absent hash) → 404, which Preflight treats as reachable.
func stubFileService(t *testing.T, bodies ...[]byte) *httptest.Server {
	t.Helper()
	byHash := make(map[string][]byte, len(bodies))
	for _, b := range bodies {
		byHash[sha3hex(b)] = b
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/") // /internal/blob/{hash}/content
		if len(parts) >= 4 {
			if b, ok := byHash[parts[3]]; ok {
				_, _ = w.Write(b)
				return
			}
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
	// Unique content per run — see TestIntegrationServeBacksUpOutboxRow: a fixed hash already in
	// the shared ledger would be skipped on a re-run, so the physical write to this run's fresh
	// target never happens and the os.Stat below would falsely fail.
	content := []byte("backfill me — a pre-existing corpus object " + uuid.NewString())
	h := sha3hex(content)
	fid := uuid.New()
	// Seed the corpus row file-service would have.
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file (id,"externalID",size,"temporaryLocation") VALUES ($1,$2,$3,false)`,
		fid, h, int64(len(content))); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	fs := stubFileService(t, content)
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
	// Unique content per run: a fixed hash would already be in the shared ledger from a prior run
	// (or a prior -count iteration), so the pipeline would dedup and skip the physical write to
	// this run's fresh target — a false "done with no bytes". (Mirrors the fault suite's uuid body.)
	content := []byte("serve me — an enqueued object " + uuid.NewString())
	h := sha3hex(content)
	fid := uuid.New()
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file_backup_outbox ("fileId","externalID",size,status) VALUES ($1,$2,$3,'pending')`,
		fid, h, int64(len(content))); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
	fs := stubFileService(t, content)
	// A distinct metrics port so serve's HTTP bind can't clash with a real 4004.
	cfgPath, targetDir := harnessConfig(t, fs.URL, "metricsPort: 14087", "pollEverySec: 1")

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveCtx(sctx, cfgPath) }()

	// Wait for the row to be FINISHED (status → done), not merely for the target file to appear.
	// done is set last — after the target write and the ledger — so it is the authoritative
	// completion signal. Breaking on the file's appearance instead would race: the bytes land
	// before MarkDone commits, so cancelling then can abort the in-flight MarkDone (it runs on
	// the cancelled context) and leave the row pending. (Mirrors the fault suite's waitForServe.)
	waitForServe(t, done, 30*time.Second, func() bool {
		return outboxStatusOf(t, h) == "done"
	}, "serve to back up the enqueued object")

	cancel()
	select {
	case err := <-done:
		if err != nil && !isCanceled(err) {
			t.Fatalf("serveCtx must return cleanly on shutdown, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveCtx did not return after cancel")
	}
	// done implies the object was written to the target first — so the bytes must be there.
	if _, err := os.Stat(storedPath(targetDir, h)); err != nil {
		t.Fatalf("outbox row done but object not stored at the target: %v", err)
	}
}

func isCanceled(err error) bool {
	return err != nil && strings.Contains(err.Error(), context.Canceled.Error())
}
