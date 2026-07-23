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
// the objects' (hash, fileID, content). The corpus rows carry updatedDate=verTime — the SAFE
// restore-current guard (has the file been modified since --at?).
func seedBackedUp(t *testing.T, n int, verTime time.Time) (cfgPath, targetDir, name string, hashes []string, fids []uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	var bodies [][]byte
	for i := 0; i < n; i++ {
		body := []byte(fmt.Sprintf("backed-up object %d — %s", i, uuid.NewString()))
		h := sha3hex(body)
		fid := uuid.New()
		if err := harness.Exec(ctx, harness.AlkemioDB,
			`INSERT INTO file (id,"externalID",size,"temporaryLocation","createdDate","updatedDate") VALUES ($1,$2,$3,false,$4,$4)`,
			fid, h, int64(len(body)), verTime); err != nil {
			t.Fatalf("seed file %d: %v", i, err)
		}
		bodies = append(bodies, body)
		hashes = append(hashes, h)
		fids = append(fids, fid)
	}
	fs := stubFileService(t, bodies...)
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

// TestIntegrationRestoreCurrent (T024/T026, re-review item 2): the effective-time decision keys on
// the SAFE guard `file.updatedDate` (has the file been modified since --at?). With `updatedDate <=
// --at` the current version was in effect at --at → restores; with `updatedDate > --at` (a since-
// modification) it errors toward PITR + --hash — a deliberate safe over-refusal that NEVER returns a
// wrong version (content-version history / recyclable hashes are out of scope).
func TestIntegrationRestoreCurrent(t *testing.T) {
	verTime := time.Now().Add(-2 * time.Hour) // the file's updatedDate
	cfgPath, _, name, hashes, fids := seedBackedUp(t, 1, verTime)
	restoreDir := t.TempDir()

	// --at AFTER updatedDate → the file has NOT changed since --at → the current version was in effect.
	after := verTime.Add(time.Hour).Format(time.RFC3339)
	if err := runRestoreCurrent([]string{"--config", cfgPath, "--file-id", fids[0].String(), "--at", after, "--from", name, "--to", restoreDir}); err != nil {
		t.Fatalf("restore current (updatedDate <= --at) must resolve + restore, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, hashes[0])); err != nil {
		t.Fatalf("restore current did not write the object: %v", err)
	}

	// --at BEFORE updatedDate → the file was modified after --at → error directing to PITR.
	before := verTime.Add(-time.Hour).Format(time.RFC3339)
	err := runRestoreCurrent([]string{"--config", cfgPath, "--file-id", fids[0].String(), "--at", before, "--from", name, "--to", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "PITR") {
		t.Fatalf("restore current (updatedDate > --at) must error toward PITR/--hash, got %v", err)
	}

	// An unknown file id → error (no current hash).
	if err := runRestoreCurrent([]string{"--config", cfgPath, "--file-id", uuid.NewString(), "--at", after, "--from", name}); err == nil {
		t.Fatal("restore current of an unknown file id must error")
	}
}

// TestIntegrationRestoreCurrentMetadataEditFailsLoud (re-review item 2+5): a metadata-only edit bumps
// the file's `updatedDate` to now. With `--at` BEFORE the edit, the safe guard sees `updatedDate >
// --at` and FAILS LOUD (a deliberate conservative over-refusal — it CANNOT prove the current version
// was in effect at --at, and the content-version timeline is out of scope, so it never risks a wrong
// version). The operator restores the content directly via `--hash`. This DISCRIMINATES the guard:
// removing/inverting it would restore (test fails).
func TestIntegrationRestoreCurrentMetadataEditFailsLoud(t *testing.T) {
	cfgPath, _, name, hashes, fids := seedBackedUp(t, 1, time.Now().Add(-2*time.Hour))
	// A metadata-only edit AFTER --at: bump updatedDate to now (content/externalID unchanged).
	if err := harness.Exec(context.Background(), harness.AlkemioDB,
		`UPDATE file SET "updatedDate"=now() WHERE id=$1`, fids[0]); err != nil {
		t.Fatalf("metadata edit: %v", err)
	}
	// --at is BEFORE the metadata edit (1h ago) → updatedDate (now) > --at → fail loud.
	at := time.Now().Add(-time.Hour).Format(time.RFC3339)
	err := runRestoreCurrent([]string{"--config", cfgPath, "--file-id", fids[0].String(), "--at", at, "--from", name, "--to", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "PITR") {
		t.Fatalf("a file modified after --at (a metadata edit) must fail loud toward PITR (safe over-refusal), got %v", err)
	}
	// The operator's escape hatch: restore the current content DIRECTLY via --hash succeeds.
	restoreDir := t.TempDir()
	if err := runRestoreCurrent([]string{"--config", cfgPath, "--file-id", fids[0].String(), "--at", at, "--hash", hashes[0], "--from", name, "--to", restoreDir}); err != nil {
		t.Fatalf("the --hash escape hatch must restore regardless of updatedDate, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreDir, hashes[0])); err != nil {
		t.Fatalf("restore current did not write the object after a metadata edit: %v", err)
	}
}

// TestIntegrationAuditInventory (T032/review #9): the target→ledger direction must actually
// VERIFY (not no-op): after a matching manifest it reports the target verifiable + extra=0 (clean);
// after DELETING a ledger record the manifest still lists (an orphan), it detects extra>0 and
// fails.
func TestIntegrationAuditInventory(t *testing.T) {
	ctx := context.Background()
	cfgPath, _, name, hashes, _ := seedBackedUp(t, 3, time.Now().Add(-time.Hour))

	// Write a manifest snapshot to the target (serve does this periodically; do it directly here).
	cfg, ledger, pool, err := ledgerJob(ctx, cfgPath)
	if err != nil {
		t.Fatalf("ledgerJob: %v", err)
	}
	targets, err := buildTargets(cfg.Targets)
	if err != nil {
		pool.Close()
		t.Fatalf("buildTargets: %v", err)
	}
	if err := domain.WriteManifests(ctx, ledger, targets, domain.ManifestName(time.Now())); err != nil {
		pool.Close()
		t.Fatalf("write manifest: %v", err)
	}
	// Assert the target was actually VERIFIED (read its manifest + diffed) with no drift — not a
	// vacuous NoData/unverifiable no-op that a bare nil check would also accept.
	rep := domain.AuditInventory(ctx, ledger, targets)
	pool.Close()
	v := rep.Targets[0]
	if v.Status != domain.StatusVerified {
		t.Fatalf("the target must be VERIFIED (its manifest read + diffed), got %+v", v)
	}
	if v.Extra != 0 || !strings.Contains(v.Detail, "manifest=3") {
		t.Fatalf("a matching manifest must show extra=0 over 3 objects, got %+v", v)
	}
	if err := runAudit([]string{"--config", cfgPath, "--inventory"}); err != nil {
		t.Fatalf("audit --inventory after a matching manifest must be clean, got %v", err)
	}

	// Inject an ORPHAN: delete one ledger target_status row so the manifest lists an object the
	// ledger no longer records stored on the target → extra>0 → audit --inventory must FAIL.
	if err := harness.Exec(ctx, harness.LedgerDB,
		`DELETE FROM file_backup_target_status WHERE target=$1 AND "externalID"=$2`, name, hashes[0]); err != nil {
		t.Fatalf("inject orphan: %v", err)
	}
	if err := runAudit([]string{"--config", cfgPath, "--inventory"}); err == nil {
		t.Fatal("audit --inventory must FAIL when a target's manifest holds an object the ledger no longer records (orphan)")
	}
}

// TestIntegrationAuditJoinsVerdicts (review #7): a corrupt-manifest sweep error must NOT mask a
// genuine silent-loss (ledger→target) finding — both must surface.
func TestIntegrationAuditJoinsVerdicts(t *testing.T) {
	cfgPath, targetDir, _, hashes, _ := seedBackedUp(t, 1, time.Now().Add(-time.Hour))

	// Silent loss: the ledger records the object stored, but delete it from disk so Exists reports
	// absent (ledger→target missing).
	if err := os.Remove(storedPath(targetDir, hashes[0])); err != nil {
		t.Fatalf("delete stored object: %v", err)
	}
	// Corrupt the target's newest manifest so the inventory sweep errors.
	mdir := filepath.Join(targetDir, "_manifest")
	if err := os.MkdirAll(mdir, 0o750); err != nil {
		t.Fatalf("mkdir manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(mdir, "9999-01-01T000000.000000000Z.jsonl"), []byte("{corrupt\n"), 0o600); err != nil {
		t.Fatalf("write corrupt manifest: %v", err)
	}

	err := runAudit([]string{"--config", cfgPath, "--inventory"})
	if err == nil {
		t.Fatal("audit --inventory must fail (silent loss AND corrupt manifest)")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("the silent-loss (missing) finding must NOT be masked by the corrupt-manifest error: %v", err)
	}
}

// TestIntegrationRunDrillZeroSampled (review #4): a drill against a target the ledger has no rows
// for must FAIL (0 sampled proves nothing), not report a vacuous green pass.
func TestIntegrationRunDrillZeroSampled(t *testing.T) {
	cfgPath, _, name := drConfig(t, "http://unused") // a unique target with nothing backed up to it
	err := runDrill([]string{"--config", cfgPath, "--from", name, "--to", t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "0 objects") {
		t.Fatalf("a drill sampling 0 objects must fail with a 'nothing to drill' error, got %v", err)
	}
}

// TestIntegrationRestoreCurrentNullUpdatedDate (re-review item 2): a file with a NULL updatedDate has
// no became-current time to compare against --at — restore current (no --hash) must FAIL LOUD (direct
// to PITR/--hash), never silently return the current hash.
func TestIntegrationRestoreCurrentNullUpdatedDate(t *testing.T) {
	ctx := context.Background()
	content := []byte("null updatedDate object")
	h := sha3hex(content)
	fid := uuid.New()
	// createdDate set but updatedDate NULL (omitted).
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file (id,"externalID",size,"temporaryLocation","createdDate") VALUES ($1,$2,$3,false,now())`,
		fid, h, int64(len(content))); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	cfgPath, _ := harnessConfig(t, "http://unused")
	err := runRestoreCurrent([]string{
		"--config", cfgPath, "--file-id", fid.String(), "--at", time.Now().Format(time.RFC3339), "--from", "local",
	})
	if err == nil || !strings.Contains(err.Error(), "NULL updatedDate") {
		t.Fatalf("a NULL updatedDate must fail loud toward PITR/--hash, got %v", err)
	}
}
