package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---- restore sub-verb dispatch (no DB) ------------------------------------

// TestRunRestoreObjectSubverb: `restore object --hash …` routes to the single-object path (the
// explicit sub-verb form of the backward-compatible `restore --hash …`).
func TestRunRestoreObjectSubverb(t *testing.T) {
	base := t.TempDir()
	cfgPath := fsConfig(t, base)
	content := []byte("dispatch via restore object")
	h := storeObject(t, cfgPath, content)
	to := t.TempDir()
	if code := run([]string{"fbs", "restore", "object", "--config", cfgPath, "--hash", h, "--from", "local", "--to", to}); code != 0 {
		t.Fatalf("restore object of an intact object must exit 0, got %d", code)
	}
	got, err := os.ReadFile(filepath.Join(to, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("restore object did not write the bytes: %v", err)
	}
}

// ---- restore version --hash escape hatch (targets-only, no DB) -------------

// TestRunRestoreVersionHashOverride: with --hash (a PITR-recovered content hash), restore version
// restores it directly from the target — no file-table lookup, so no alkemio DB is needed.
func TestRunRestoreVersionHashOverride(t *testing.T) {
	base := t.TempDir()
	cfgPath := fsConfig(t, base)
	content := []byte("a past version recovered via PITR")
	h := storeObject(t, cfgPath, content)
	to := t.TempDir()
	err := runRestoreVersion([]string{
		"--config", cfgPath, "--file-id", "6f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
		"--at", "2026-07-01T00:00:00Z", "--hash", h, "--from", "local", "--to", to,
	})
	if err != nil {
		t.Fatalf("restore version --hash of a stored object must succeed, got %v", err)
	}
	got, rerr := os.ReadFile(filepath.Join(to, h)) //nolint:gosec // test temp path
	if rerr != nil || !bytes.Equal(got, content) {
		t.Fatalf("restore version --hash did not write the bytes: %v", rerr)
	}
}

func TestRunRestoreVersionMissingFlags(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	if err := runRestoreVersion([]string{"--config", cfgPath}); err == nil {
		t.Fatal("restore version without --file-id/--at must error")
	}
}

func TestRunRestoreVersionBadArgs(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	if err := runRestoreVersion([]string{"--config", cfgPath, "--file-id", "not-a-uuid", "--at", "2026-07-01T00:00:00Z"}); err == nil {
		t.Fatal("a non-uuid --file-id must error")
	}
	if err := runRestoreVersion([]string{"--config", cfgPath, "--file-id", "6f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b", "--at", "yesterday"}); err == nil {
		t.Fatal("a non-RFC3339 --at must error")
	}
}

// TestRunRestoreVersionUnknownTarget: --from names a target not in the config → pickTarget errors.
func TestRunRestoreVersionUnknownTarget(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	err := runRestoreVersion([]string{
		"--config", cfgPath, "--file-id", "6f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
		"--at", "2026-07-01T00:00:00Z", "--hash", sha3hex([]byte("x")), "--from", "nope",
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("an unknown --from must yield a not-found error, got %v", err)
	}
}

// TestRunRestoreVersionNoHashNeedsAlkemioDB: without --hash, resolving --file-id requires the
// alkemio DB; a targets-only config (no alkemioDB) fails validation before opening a pool.
func TestRunRestoreVersionNoHashNeedsAlkemioDB(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	err := runRestoreVersion([]string{
		"--config", cfgPath, "--file-id", "6f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
		"--at", "2026-07-01T00:00:00Z", "--from", "local",
	})
	if err == nil || !strings.Contains(err.Error(), "alkemioDB") {
		t.Fatalf("resolving --file-id without --hash must require alkemioDB, got %v", err)
	}
}

// TestRunRestoreVersionNoHashDBUnreachable: with a valid-but-unreachable alkemioDB, the DB-resolve
// path opens the pool then fails resolving the file id (connection refused) — the DR command
// surfaces the error rather than hanging or silently succeeding.
func TestRunRestoreVersionNoHashDBUnreachable(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, dir,
		"alkemioDB:\n  host: 127.0.0.1\n  port: 1\n  user: u\n  dbName: d\n  sslMode: disable\n"+
			"dbTimeoutSec: 15\ntargets:\n  - name: local\n    type: filesystem\n    path: "+filepath.Join(dir, "store")+"\n")
	err := runRestoreVersion([]string{
		"--config", cfgPath, "--file-id", "6f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
		"--at", "2026-07-01T00:00:00Z", "--from", "local",
	})
	if err == nil {
		t.Fatal("resolving --file-id against an unreachable alkemioDB must error")
	}
}

// ---- pickTarget default (first target) ------------------------------------

// TestRunRestoreVersionHashDefaultsToFirstTarget: --from omitted resolves to the first configured
// target (symmetric holders), so restore version --hash works without naming a target.
func TestRunRestoreVersionHashDefaultsToFirstTarget(t *testing.T) {
	base := t.TempDir()
	cfgPath := fsConfig(t, base)
	content := []byte("default-target restore")
	h := storeObject(t, cfgPath, content)
	to := t.TempDir()
	err := runRestoreVersion([]string{
		"--config", cfgPath, "--file-id", "6f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b",
		"--at", "2026-07-01T00:00:00Z", "--hash", h, "--to", to,
	})
	if err != nil {
		t.Fatalf("restore version --hash with default --from must succeed, got %v", err)
	}
	if _, rerr := os.ReadFile(filepath.Join(to, h)); rerr != nil { //nolint:gosec // test temp path
		t.Fatalf("restored file missing: %v", rerr)
	}
}

// ---- config-error paths of the ledger-backed restore/drill subcommands ----
// Each fails on ValidateDR (fsConfig has no ledgerDB) BEFORE opening a pool.

func TestRunRestoreAllInvalidConfig(t *testing.T) {
	// fsConfig has a target but no ledgerDB → ledgerJob's ValidateDR fails before any pool opens.
	if err := runRestoreAll([]string{"--config", fsConfig(t, t.TempDir())}); err == nil ||
		!strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("restore all without a ledgerDB must fail with an invalid-config error, got %v", err)
	}
}

func TestRunDrillInvalidConfig(t *testing.T) {
	if err := runDrill([]string{"--config", fsConfig(t, t.TempDir())}); err == nil ||
		!strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("drill without a ledgerDB must fail with an invalid-config error, got %v", err)
	}
}
