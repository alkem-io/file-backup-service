//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// uniqueTarget returns a per-test target NAME so a test's ledger rows (keyed by target name) are
// isolated from the shared harness ledger — otherwise a DR read (restore all / drill) keyed on a
// shared "local" name would try to fetch another test's objects from THIS test's (different) dir.
func uniqueTarget() string { return "t" + strings.ReplaceAll(uuid.NewString(), "-", "")[:24] }

// cleanupLedger removes a test's ledger rows (by target name) + any now-orphaned object rows at
// test end, so the SHARED harness ledger returns to its prior state — otherwise objects stored on
// a unique target name look like under-replication gaps to a later "local"-target reconcile run.
func cleanupLedger(t *testing.T, names ...string) {
	t.Cleanup(func() {
		ctx := context.Background()
		for _, n := range names {
			_ = harness.Exec(ctx, harness.LedgerDB, `DELETE FROM file_backup_target_status WHERE target=$1`, n)
		}
		_ = harness.Exec(ctx, harness.LedgerDB,
			`DELETE FROM file_backup_object o WHERE NOT EXISTS (SELECT 1 FROM file_backup_target_status s WHERE s."externalID"=o."externalID")`)
	})
}

// drConfig renders a config with a single filesystem target under a unique NAME, returning the
// config path, the target's on-disk dir, and the name.
func drConfig(t *testing.T, fileServiceBase string, extra ...string) (cfgPath, targetDir, name string) {
	t.Helper()
	name = uniqueTarget()
	targetDir = t.TempDir()
	cleanupLedger(t, name) // return the shared ledger to its prior state at test end
	yaml := harness.ConfigYAML(fileServiceBase, map[string]string{name: targetDir})
	if len(extra) > 0 {
		yaml += "\n" + strings.Join(extra, "\n") + "\n"
	}
	return writeConfig(t, t.TempDir(), yaml), targetDir, name
}

// seedBackedUp seeds n file-corpus rows + a stub file-service and runs backfill, so the ledger
// records n objects stored on a unique target. Returns the config path, the target dir + name, and
// the objects' (hash, fileID, content). The corpus rows carry updatedDate=verTime for restore
// version resolution.
func seedBackedUp(t *testing.T, n int, verTime time.Time) (cfgPath, targetDir, name string, hashes []string, fids []uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	content := map[uuid.UUID][]byte{}
	for i := 0; i < n; i++ {
		body := []byte(fmt.Sprintf("backed-up object %d — %s", i, uuid.NewString()))
		h := sha3hex(body)
		fid := uuid.New()
		if err := harness.Exec(ctx, harness.AlkemioDB,
			`INSERT INTO file (id,"externalID",size,"temporaryLocation","createdDate","updatedDate") VALUES ($1,$2,$3,false,$4,$4)`,
			fid, h, int64(len(body)), verTime); err != nil {
			t.Fatalf("seed file %d: %v", i, err)
		}
		content[fid] = body
		hashes = append(hashes, h)
		fids = append(fids, fid)
	}
	fs := stubFileService(t, content)
	cfgPath, targetDir, name = drConfig(t, fs.URL)
	if err := runBackfill([]string{"--config", cfgPath}); err != nil {
		t.Fatalf("seed backfill: %v", err)
	}
	return cfgPath, targetDir, name, hashes, fids
}

// TestIntegrationRunDrill (T033): a restore drill over backed-up objects PASSES; corrupting a
// stored object makes the next drill FAIL (nonzero exit), and the drill textfile records the pass.
func TestIntegrationRunDrill(t *testing.T) {
	cfgPath, targetDir, name, hashes, _ := seedBackedUp(t, 3, time.Now().Add(-time.Hour))
	scratch := t.TempDir()
	metricsFile := filepath.Join(t.TempDir(), "drill.prom")

	if err := runDrill([]string{"--config", cfgPath, "--from", name, "--to", scratch, "--metrics-file", metricsFile}); err != nil {
		t.Fatalf("drill of intact objects must pass, got %v", err)
	}
	body, err := os.ReadFile(metricsFile) //nolint:gosec // test temp path
	if err != nil || !strings.Contains(string(body), "filebackup_restore_drill_pass 1") {
		t.Fatalf("drill textfile must record pass=1, got %q err=%v", body, err)
	}

	// Corrupt a stored object so it no longer hashes to its key — the next drill must FAIL.
	if err := os.WriteFile(storedPath(targetDir, hashes[0]), []byte("tampered bytes"), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatalf("corrupt stored object: %v", err)
	}
	if err := runDrill([]string{"--config", cfgPath, "--from", name, "--to", scratch}); err == nil {
		t.Fatal("a drill over a corrupted stored object must fail (nonzero exit)")
	}
}

// TestIntegrationRestoreAll (T023/T026): restore every ledger-recorded object from the target to a
// dir, verify the bytes, then re-run to prove idempotence (all skipped).
func TestIntegrationRestoreAll(t *testing.T) {
	cfgPath, _, name, hashes, _ := seedBackedUp(t, 4, time.Now().Add(-time.Hour))
	restoreDir := t.TempDir()
	if err := runRestoreAll([]string{"--config", cfgPath, "--from", name, "--to", restoreDir}); err != nil {
		t.Fatalf("restore all: %v", err)
	}
	for _, h := range hashes {
		if _, err := os.Stat(filepath.Join(restoreDir, h)); err != nil {
			t.Fatalf("object %s not restored: %v", h, err)
		}
	}
	// Re-run must be idempotent (every object already present + intact) — a clean exit.
	if err := runRestoreAll([]string{"--config", cfgPath, "--from", name, "--to", restoreDir}); err != nil {
		t.Fatalf("restore all re-run (idempotent) must be clean, got %v", err)
	}
}

// TestIntegrationRestoreVersion (T024/T026): with --at at/after the file's version time, restore
// version resolves the current hash from the `file` table and restores it; with --at BEFORE the
// version time it errors (the historical version needs PITR + --hash), and --hash always works.
func TestIntegrationRestoreVersion(t *testing.T) {
	verTime := time.Now().Add(-2 * time.Hour)
	cfgPath, _, name, hashes, fids := seedBackedUp(t, 1, verTime)
	restoreDir := t.TempDir()

	// --at AFTER the version time → the current hash IS the version as of --at → restores.
	after := verTime.Add(time.Hour).Format(time.RFC3339)
	if err := runRestoreVersion([]string{"--config", cfgPath, "--file-id", fids[0].String(), "--at", after, "--from", name, "--to", restoreDir}); err != nil {
		t.Fatalf("restore version (--at after version time) must resolve + restore, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, hashes[0])); err != nil {
		t.Fatalf("restore version did not write the object: %v", err)
	}

	// --at BEFORE the version time → the historical version isn't live → error directing to PITR.
	before := verTime.Add(-time.Hour).Format(time.RFC3339)
	err := runRestoreVersion([]string{"--config", cfgPath, "--file-id", fids[0].String(), "--at", before, "--from", name, "--to", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "PITR") {
		t.Fatalf("restore version (--at before a replaced version) must error toward PITR/--hash, got %v", err)
	}

	// An unknown file id → error (no current hash).
	if err := runRestoreVersion([]string{"--config", cfgPath, "--file-id", uuid.NewString(), "--at", after, "--from", name}); err == nil {
		t.Fatal("restore version of an unknown file id must error")
	}
}

// TestIntegrationAuditInventory (T032): after writing a manifest that matches the ledger, the
// target→ledger direction reports no drift (extra=0) and audit --inventory exits clean.
func TestIntegrationAuditInventory(t *testing.T) {
	ctx := context.Background()
	cfgPath, _, name, _, _ := seedBackedUp(t, 3, time.Now().Add(-time.Hour))

	// Write a manifest snapshot to the target (serve does this periodically; do it directly here).
	_, ledger, pool, targets, err := ledgerJob(ctx, cfgPath)
	if err != nil {
		t.Fatalf("ledgerJob: %v", err)
	}
	if err := domain.WriteManifests(ctx, ledger, targets, domain.ManifestName(time.Now())); err != nil {
		pool.Close()
		t.Fatalf("write manifest: %v", err)
	}
	pool.Close()

	// --inventory diffs the target's manifest against the ledger; a fresh manifest ⊆ ledger, so
	// extra=0 → clean exit.
	if err := runAudit([]string{"--config", cfgPath, "--inventory"}); err != nil {
		t.Fatalf("audit --inventory after a matching manifest must be clean, got %v", err)
	}
	_ = name
}
