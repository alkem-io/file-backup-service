package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alkem-io/file-backup-service/internal/adapter/inbound/metrics"
	"github.com/alkem-io/file-backup-service/internal/config"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// ---- restore all SIGTERM verdict (review #3) ------------------------------

// TestRestoreAllVerdict (review Cluster 1): GENUINE failures (st.Failed — RestoreAll separates
// cancellations into st.Cancelled) exit NONZERO even when a SIGTERM coincides; a purely-cancelled
// run exits clean.
func TestRestoreAllVerdict(t *testing.T) {
	// genuine failures + a coincident SIGTERM enumeration-cancel → STILL nonzero (Cluster 1).
	if err := restoreAllVerdict(domain.RestoreAllStats{Failed: 1, Cancelled: 2}, context.Canceled, "t"); err == nil {
		t.Fatal("a genuine failure that coincides with a SIGTERM must still exit nonzero")
	}
	// purely cancelled (no genuine failures) → clean, resumable.
	if err := restoreAllVerdict(domain.RestoreAllStats{Restored: 2, Cancelled: 4}, context.Canceled, "t"); err != nil {
		t.Fatalf("a purely-cancelled run must exit cleanly (resumable), got %v", err)
	}
	// enumeration error (not a cancel) → surfaced.
	if err := restoreAllVerdict(domain.RestoreAllStats{}, errors.New("ledger down"), "t"); err == nil {
		t.Fatal("a genuine enumeration error must surface")
	}
	// genuine failures on an un-cancelled run → error (and NOT the old hardcoded hash-mismatch text).
	err := restoreAllVerdict(domain.RestoreAllStats{Restored: 5, Failed: 1}, nil, "t")
	if err == nil || strings.Contains(err.Error(), "hash-mismatch / unreadable source") {
		t.Fatalf("un-cancelled failures must error with a generic message, got %v", err)
	}
	// a clean, complete run → nil.
	if err := restoreAllVerdict(domain.RestoreAllStats{Restored: 5}, nil, "t"); err != nil {
		t.Fatalf("a clean run must return nil, got %v", err)
	}
	// 0 objects enumerated on a clean pass → fail with an UNAMBIGUOUS message that names the source and
	// says the ledger holds 0 for it (E4: not conflated with a data fault), like drill's 0-sampled.
	if err := restoreAllVerdict(domain.RestoreAllStats{}, nil, "offsite"); err == nil ||
		!strings.Contains(err.Error(), `0 objects stored on target "offsite"`) {
		t.Fatalf("a 0-enumerated restore-all must fail loud naming the target, got %v", err)
	}
	// 0 restored but N skipped (an idempotent re-run) stays success.
	if err := restoreAllVerdict(domain.RestoreAllStats{Skipped: 3}, nil, "t"); err != nil {
		t.Fatalf("a 0-restored-but-skipped run must stay success, got %v", err)
	}
}

// TestBackfillVerdict (Alt#2): a GENUINE failure that coincides with a mid-corpus SIGTERM must return a
// NON-Canceled error so the dispatch's onShutdownOK CANNOT rescue it to exit 0 — the false-NEGATIVE twin
// of the round-9 drain-window false-positive. A pure cancel (no failures) returns Canceled for
// onShutdownOK to map to exit 0 (resumable); a benign tail-drain cancel is in Cancelled, not Failed.
func TestBackfillVerdict(t *testing.T) {
	// genuine failure + a coincident mid-corpus SIGTERM → must NOT be Canceled (else onShutdownOK masks it).
	if err := backfillVerdict(domain.BackfillStats{Failed: 1}, context.Canceled); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("a genuine failure coinciding with a SIGTERM must return a non-Canceled error (onShutdownOK must not mask it), got %v", err)
	}
	// pure cancel (no failures, benign tail-drain in Cancelled) → Canceled passes through for onShutdownOK.
	if err := backfillVerdict(domain.BackfillStats{Backed: 5, Cancelled: 3}, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("a pure cancel must pass Canceled through for onShutdownOK → exit 0, got %v", err)
	}
	// a real sweep/DB error (not a cancel) → surfaced.
	if err := backfillVerdict(domain.BackfillStats{}, errors.New("corpus enum failed")); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("a real sweep error must surface, got %v", err)
	}
	// clean pass → nil.
	if err := backfillVerdict(domain.BackfillStats{Backed: 5}, nil); err != nil {
		t.Fatalf("a clean pass must be nil, got %v", err)
	}
	// clean sweep, backed=0, nothing deferred, objects skipped = the source 404'd every fetch
	// (outage / missing endpoint) → must NOT pass as a clean backfill.
	if err := backfillVerdict(domain.BackfillStats{Skipped: 100}, nil); err == nil {
		t.Fatal("clean sweep, backed=0, all-skipped (source 404'd everything) must be a nonzero-exit error, got nil")
	}
	// a SIGTERM mid-backfill (backed=0, a couple skipped, rest cancelled, sweepErr=Canceled) must
	// pass Canceled through — a resumable interruption is NOT a source outage.
	if err := backfillVerdict(domain.BackfillStats{Skipped: 2, Cancelled: 98}, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled backfill must pass Canceled through (resumable), got %v", err)
	}
	// backed=0 because a BACKUP TARGET is down (Deferred>0) is a target fault, not a source
	// outage — must NOT false-page as "the source 404'd" (FileBackupTargetCircuitOpen covers it).
	if err := backfillVerdict(domain.BackfillStats{Deferred: 50, Skipped: 3}, nil); err != nil {
		t.Fatalf("a down-target pass (Deferred>0) must not be misdiagnosed as a source outage, got %v", err)
	}
	// empty corpus (backed=0, skipped=0) is a clean no-op; a normal run with a few incidental
	// deletions (backed>0, some skipped) still passes.
	if err := backfillVerdict(domain.BackfillStats{}, nil); err != nil {
		t.Fatalf("an empty corpus must be a clean no-op, got %v", err)
	}
	if err := backfillVerdict(domain.BackfillStats{Backed: 90, Skipped: 3}, nil); err != nil {
		t.Fatalf("a normal run with a few incidental deletions must pass, got %v", err)
	}
}

// TestReconcileVerdict (Alt#2): same policy — a genuine failure OR a Skipped (on NO target) coinciding
// with a SIGTERM must not be masked to exit 0; a pure cancel passes Canceled through.
func TestReconcileVerdict(t *testing.T) {
	if err := reconcileVerdict(domain.ReconcileStats{Failed: 1}, context.Canceled); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("a genuine reconcile failure coinciding with a SIGTERM must not be masked, got %v", err)
	}
	if err := reconcileVerdict(domain.ReconcileStats{Skipped: 1}, context.Canceled); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("a Skipped (object on NO target) coinciding with a SIGTERM must not be masked, got %v", err)
	}
	if err := reconcileVerdict(domain.ReconcileStats{Repaired: 5, Cancelled: 2}, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("a pure cancel must pass Canceled through, got %v", err)
	}
	if err := reconcileVerdict(domain.ReconcileStats{Repaired: 5}, nil); err != nil {
		t.Fatalf("a clean pass must be nil, got %v", err)
	}
}

// TestSelectReadTarget (Pillar 4b): the default source SKIPS WORM (write-only) targets; an EXPLICIT
// WORM --from is now ALLOWED (restoring from the sole surviving immutable copy must not be refused);
// an all-WORM config has no default readable source; an unknown --from is not-found.
func TestSelectReadTarget(t *testing.T) {
	worm := config.Target{Name: "offsite", Worm: true}
	readable := config.Target{Name: "local"}
	// default (--from "") skips the WORM target, picks the readable one.
	if got, err := config.SelectReadTarget([]config.Target{worm, readable}, ""); err != nil || got.Name != "local" {
		t.Fatalf("default must pick the readable target, got %q err=%v", got.Name, err)
	}
	// explicit WORM --from → ALLOWED (honored as chosen).
	if got, err := config.SelectReadTarget([]config.Target{worm, readable}, "offsite"); err != nil || got.Name != "offsite" || !got.Worm {
		t.Fatalf("an explicit WORM --from must be allowed, got %+v err=%v", got, err)
	}
	// all-WORM config, default → no readable source.
	if _, err := config.SelectReadTarget([]config.Target{worm}, ""); err == nil ||
		!strings.Contains(err.Error(), "no readable") {
		t.Fatalf("an all-WORM config default must fail loud, got %v", err)
	}
	// unknown --from → not found.
	if _, err := config.SelectReadTarget([]config.Target{readable}, "nope"); err == nil ||
		!strings.Contains(err.Error(), "not found") {
		t.Fatalf("an unknown --from must be not-found, got %v", err)
	}
}

// TestBuildReadSourceWormAnnotatesFetch (Pillar 4b): an EXPLICIT WORM source is built + wrapped so a
// read failure carries the actionable WORM-recovery hint (rather than a raw error).
func TestBuildReadSourceWormAnnotatesFetch(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, dir,
		"targets:\n  - name: offsite\n    type: filesystem\n    worm: true\n    path: "+filepath.Join(dir, "store")+"\n")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	src, name, err := buildReadSource(cfg, "offsite")
	if err != nil || name != "offsite" {
		t.Fatalf("an explicit WORM --from must build, got %q err=%v", name, err)
	}
	// A Fetch of a never-stored object fails; the error must carry the WORM recovery hint.
	if _, ferr := src.Fetch(context.Background(), sha3hex([]byte("absent"))); ferr == nil ||
		!strings.Contains(ferr.Error(), "WORM/write-only") {
		t.Fatalf("a WORM source read failure must be annotated, got %v", ferr)
	}
	// A Fetch of a PRESENT object succeeds (the wrapper is transparent on the happy path): stage an
	// object via the wrapped sink's Store, then read it back byte-for-byte.
	content := []byte("recovered from the immutable copy via an admin credential")
	h := sha3hex(content)
	if _, serr := src.Store(context.Background(), h, bytes.NewReader(content)); serr != nil {
		t.Fatalf("store on the worm-wrapped sink: %v", serr)
	}
	rc, ferr := src.Fetch(context.Background(), h)
	if ferr != nil {
		t.Fatalf("a WORM source read of a present object must succeed, got %v", ferr)
	}
	defer func() { _ = rc.Close() }()
	if got, _ := io.ReadAll(rc); !bytes.Equal(got, content) {
		t.Fatalf("worm source read mismatch: %q", got)
	}
}

// TestBuildReadSourceWormWithAuditCredNotWrapped (vanilla8 E1): a worm source that HAS an audit
// credential is READ-CAPABLE (readClient uses the audit cred), so buildReadSource must NOT wrap it in
// the write-only credential-hint wrapper — a read failure there is transient/real, and the "supply a
// read-capable credential" hint would misdirect. Only a write-only (no-audit-cred) worm is wrapped.
func TestBuildReadSourceWormWithAuditCredNotWrapped(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeConfig(t, dir,
		"targets:\n  - name: offsite\n    type: s3\n    worm: true\n    insecure: true\n"+
			"    endpoint: 127.0.0.1:9000\n    bucket: bkt\n    region: us-east-1\n"+
			"    accessKey: AK\n    secretKey: SK\n    auditAccessKey: AAK\n    auditSecretKey: ASK\n")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	src, name, err := buildReadSource(cfg, "offsite")
	if err != nil || name != "offsite" {
		t.Fatalf("an s3 worm --from with an audit cred must build, got %q err=%v", name, err)
	}
	if _, wrapped := src.(wormReadSource); wrapped {
		t.Fatal("a read-capable (audit-cred) worm source must NOT be wrapped in the write-only credential-hint wrapper")
	}
}

// TestWormReadSourceAnnotatesLazyReadError (E1): the s3 sink's GetObject is LAZY — its 403 surfaces on
// the first Read, not from Fetch — so wormReadSource must annotate a READ-TIME failure too, not only an
// eager Fetch error. A filesystem sink can't exercise this (os.Open fails eagerly), so use a sink whose
// Fetch succeeds but whose reader errors on Read (the real off-site WORM/s3 shape).
func TestWormReadSourceAnnotatesLazyReadError(t *testing.T) {
	w := wormReadSource{Sink: lazyFetchSink{name: "offsite"}}
	rc, err := w.Fetch(context.Background(), sha3hex([]byte("x")))
	if err != nil {
		t.Fatalf("Fetch must not error for a lazy sink (the READ does), got %v", err)
	}
	defer func() { _ = rc.Close() }()
	if _, rerr := io.ReadAll(rc); rerr == nil || !strings.Contains(rerr.Error(), "WORM/write-only") {
		t.Fatalf("a lazy (read-time) WORM read failure must carry the recovery hint, got %v", rerr)
	}
}

// lazyFetchSink models the s3 sink's lazy GetObject: Fetch returns a reader with no error; the (403)
// failure surfaces on the first Read. Name() is provided (wormReadSource.annotate reads the promoted
// Sink.Name()).
type lazyFetchSink struct {
	domain.Sink
	name string
}

func (s lazyFetchSink) Name() string { return s.name }
func (lazyFetchSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(errReader{}), nil
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("AccessDenied") }

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
// Each fails on ValidateDRLimits (fsConfig has no ledgerDB) BEFORE opening a pool.

// TestDrillReportInterruptedPreservesTextfile (re-review item 4): an INTERRUPTED drill (derr is a
// ctx cancellation) must write NO gauges — it must not clobber the prior textfile's pass=1 with a red
// pass=0, nor reset last_success — and must still exit nonzero. This FAILS if the
// `errors.Is(derr, context.Canceled)` early-return is removed (drillReport would then write pass=0).
func TestDrillReportInterruptedPreservesTextfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drill.prom")
	// A prior FULL PASS writes the file (a separate short-lived drill process).
	pass := metrics.NewDrillMetrics()
	pass.SetPass(true, time.Unix(1_700_000_000, 0))
	if err := pass.WriteTextfile(path, true); err != nil {
		t.Fatalf("seed pass textfile: %v", err)
	}
	// An interrupted run (a partial outcome + a ctx-cancellation error) must leave the file untouched.
	err := drillReport(domain.DrillOutcome{Target: "t", Passed: 3}, context.Canceled, path, "t")
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("an interrupted drill must return the ctx error (nonzero exit), got %v", err)
	}
	b, rerr := os.ReadFile(path) //nolint:gosec // test temp path
	if rerr != nil {
		t.Fatalf("read textfile: %v", rerr)
	}
	body := string(b)
	if !strings.Contains(body, "filebackup_restore_drill_pass 1") {
		t.Fatalf("an interrupted drill must NOT clobber the prior pass=1 with a red pass=0:\n%s", body)
	}
	if !strings.Contains(body, "filebackup_drill_last_success_timestamp_seconds 1.7e+09") {
		t.Fatalf("an interrupted drill must NOT reset last_success:\n%s", body)
	}
}

// TestDrillReportPassWritesGauges: a clean full-pass drill writes pass=1 + the last-success timestamp
// (the non-interrupted path drillReport takes).
func TestDrillReportPassWritesGauges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drill.prom")
	if err := drillReport(domain.DrillOutcome{Target: "t", Passed: 5}, nil, path, "t"); err != nil {
		t.Fatalf("a clean full-pass drill must succeed, got %v", err)
	}
	b, _ := os.ReadFile(path) //nolint:gosec // test temp path
	if !strings.Contains(string(b), "filebackup_restore_drill_pass 1") {
		t.Fatalf("a passing drill must write pass=1:\n%s", b)
	}
}

// TestDrillReportZeroSampledWritesRedButKeepsLastSuccess pins the 0-sampled policy (CodeRabbit PR#4
// raised moving the 0-sampled guard BEFORE the metrics write — that would be a FAIL-OPEN regression):
// a drill that sampled 0 objects PROVED NOTHING (renamed/misconfigured target, or an empty/wrong
// ledger), so it MUST write pass=0 and exit nonzero — leaving the textfile untouched would keep the
// PRIOR pass=1 and show a STALE GREEN gauge while the drill is actually broken. It must NOT, however,
// clobber `last_success`: SetPass only stamps last_success on a PASS, and WriteTextfile carries the
// prior file's value forward on a non-pass — so the "when did a drill last actually succeed" signal
// survives a red run.
func TestDrillReportZeroSampledWritesRedButKeepsLastSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drill.prom")
	// A prior HEALTHY drill writes pass=1 + a real last_success.
	prior := metrics.NewDrillMetrics()
	prior.SetPass(true, time.Unix(1_700_000_000, 0))
	if err := prior.WriteTextfile(path, true); err != nil {
		t.Fatalf("seed the passing textfile: %v", err)
	}
	// Now a 0-sampled drill (nothing to drill) — a distinct FAILURE.
	err := drillReport(domain.DrillOutcome{Target: "t"}, nil, path, "t")
	if err == nil || !strings.Contains(err.Error(), "sampled 0 objects") {
		t.Fatalf("a 0-sampled drill must fail loud (it proved nothing), got %v", err)
	}
	b, _ := os.ReadFile(path) //nolint:gosec // test temp path
	if !strings.Contains(string(b), "filebackup_restore_drill_pass 0") {
		t.Fatalf("a 0-sampled drill must write pass=0 — NOT leave a stale green gauge:\n%s", b)
	}
	if !strings.Contains(string(b), "filebackup_drill_last_success_timestamp_seconds 1.7e+09") {
		t.Fatalf("a red run must CARRY FORWARD the prior last_success, never clobber it to 0:\n%s", b)
	}
}

// TestRunRestoreUnknownSubcommand: a non-flag first arg that isn't a known verb (e.g. the pre-release
// `version`, or a typo) errors loud as an unknown subcommand — rather than silently falling through to
// the bare-hash alias and doing a surprising object restore. A bare `--hash` (a flag) still falls
// through to the alias.
func TestRunRestoreUnknownSubcommand(t *testing.T) {
	err := runRestore([]string{"version", "--file-id", "x", "--at", "y"})
	if err == nil || !strings.Contains(err.Error(), "unknown restore subcommand") {
		t.Fatalf("`restore version` must error as an unknown subcommand, got %v", err)
	}
	if err := runRestore([]string{"bogus"}); err == nil || !strings.Contains(err.Error(), "unknown restore subcommand") {
		t.Fatalf("an unknown verb must error, got %v", err)
	}
}

func TestRunRestoreAllInvalidConfig(t *testing.T) {
	// fsConfig has a target but no ledgerDB → ledgerJob's ValidateDRLimits fails before any pool opens.
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
