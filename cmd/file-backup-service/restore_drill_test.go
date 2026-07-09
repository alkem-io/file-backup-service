package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// ---- restore all SIGTERM verdict (review #3) ------------------------------

// TestRestoreAllVerdict (review Cluster 1): GENUINE failures (st.Failed — RestoreAll separates
// cancellations into st.Cancelled) exit NONZERO even when a SIGTERM coincides; a purely-cancelled
// run exits clean.
func TestRestoreAllVerdict(t *testing.T) {
	// genuine failures + a coincident SIGTERM enumeration-cancel → STILL nonzero (Cluster 1).
	if err := restoreAllVerdict(domain.RestoreAllStats{Failed: 1, Cancelled: 2}, context.Canceled); err == nil {
		t.Fatal("a genuine failure that coincides with a SIGTERM must still exit nonzero")
	}
	// purely cancelled (no genuine failures) → clean, resumable.
	if err := restoreAllVerdict(domain.RestoreAllStats{Restored: 2, Cancelled: 4}, context.Canceled); err != nil {
		t.Fatalf("a purely-cancelled run must exit cleanly (resumable), got %v", err)
	}
	// enumeration error (not a cancel) → surfaced.
	if err := restoreAllVerdict(domain.RestoreAllStats{}, errors.New("ledger down")); err == nil {
		t.Fatal("a genuine enumeration error must surface")
	}
	// genuine failures on an un-cancelled run → error (and NOT the old hardcoded hash-mismatch text).
	err := restoreAllVerdict(domain.RestoreAllStats{Restored: 5, Failed: 1}, nil)
	if err == nil || strings.Contains(err.Error(), "hash-mismatch / unreadable source") {
		t.Fatalf("un-cancelled failures must error with a generic message, got %v", err)
	}
	// a clean, complete run → nil.
	if err := restoreAllVerdict(domain.RestoreAllStats{Restored: 5}, nil); err != nil {
		t.Fatalf("a clean run must return nil, got %v", err)
	}
}

// TestPickReadableTarget (review Cluster 5): the default source SKIPS WORM (write-only) targets;
// an explicit WORM --from is rejected loud; an all-WORM config has no readable source.
func TestPickReadableTarget(t *testing.T) {
	worm := domain.Target{Sink: &fakeSink{name: "offsite"}, Worm: true}
	readable := domain.Target{Sink: &fakeSink{name: "local"}}
	// default (--from "") skips the WORM target, picks the readable one.
	if _, name, err := pickReadableTarget([]domain.Target{worm, readable}, ""); err != nil || name != "local" {
		t.Fatalf("default must pick the readable target, got %q err=%v", name, err)
	}
	// explicit WORM --from → rejected.
	if _, _, err := pickReadableTarget([]domain.Target{worm, readable}, "offsite"); err == nil ||
		!strings.Contains(err.Error(), "write-only") {
		t.Fatalf("an explicit WORM --from must be rejected, got %v", err)
	}
	// all-WORM config → no readable source.
	if _, _, err := pickReadableTarget([]domain.Target{worm}, ""); err == nil ||
		!strings.Contains(err.Error(), "no readable") {
		t.Fatalf("an all-WORM config must fail loud, got %v", err)
	}
	// unknown --from → not found.
	if _, _, err := pickReadableTarget([]domain.Target{readable}, "nope"); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Fatalf("an unknown --from must be not-found, got %v", err)
	}
}

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
	err := runRestoreCurrent([]string{
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
	if err := runRestoreCurrent([]string{"--config", cfgPath}); err == nil {
		t.Fatal("restore version without --file-id/--at must error")
	}
}

func TestRunRestoreVersionBadArgs(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	if err := runRestoreCurrent([]string{"--config", cfgPath, "--file-id", "not-a-uuid", "--at", "2026-07-01T00:00:00Z"}); err == nil {
		t.Fatal("a non-uuid --file-id must error")
	}
	if err := runRestoreCurrent([]string{"--config", cfgPath, "--file-id", "6f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b", "--at", "yesterday"}); err == nil {
		t.Fatal("a non-RFC3339 --at must error")
	}
}

// TestRunRestoreVersionUnknownTarget: --from names a target not in the config → pickTarget errors.
func TestRunRestoreVersionUnknownTarget(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	err := runRestoreCurrent([]string{
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
	err := runRestoreCurrent([]string{
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
	err := runRestoreCurrent([]string{
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
	err := runRestoreCurrent([]string{
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
