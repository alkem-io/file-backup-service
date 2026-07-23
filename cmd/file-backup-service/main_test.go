package main

// These tests exercise the cmd sub-functions DIRECTLY (serve, runBackfill, runReconcile,
// runAudit, runMigrate, runRestore, runVerify, and the helpers). They deliberately never call
// main() or fatal() (which call os.Exit), and they never restructure the os.Args dispatch — the
// apispec/openapi generator statically traces main -> serve -> NewRouter, so main.go is left
// untouched. The DB-backed middles (the pool-opening bodies of serve/backfill/reconcile/audit and
// db.Migrate) genuinely need a live Postgres; here we assert the config-validation error paths
// that fire BEFORE any pool is opened, plus the DB-free helpers and the target-only DR paths
// (restore/verify against a filesystem sink).

import (
	"bytes"
	"context"
	"crypto/sha3"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"go.uber.org/zap"

	"github.com/alkem-io/file-backup-service/internal/adapter/inbound/metrics"
	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db"
	"github.com/alkem-io/file-backup-service/internal/config"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// fakeSink is a programmable domain.Sink: Name + Preflight are the only behaviors these cmd-level
// tests drive (startup gate, target preflight, manifest loop); the byte-store methods are no-ops
// because none of these paths store or fetch through a fakeSink.
type fakeSink struct {
	name         string
	preflightErr error
}

func (f *fakeSink) Name() string                      { return f.name }
func (f *fakeSink) Preflight(_ context.Context) error { return f.preflightErr }

func (*fakeSink) Store(_ context.Context, _ string, _ io.Reader) (int64, error) { return 0, nil }
func (*fakeSink) Exists(_ context.Context, _ string) (bool, error)              { return false, nil }
func (*fakeSink) Fetch(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, errors.New("fakeSink: Fetch not supported")
}
func (*fakeSink) PutManifest(_ context.Context, _ string, _ io.Reader) error { return nil }

// sha3hex returns the lowercase-hex SHA3-256 of b — the externalID a stored object is keyed and
// verified by.
func sha3hex(b []byte) string {
	sum := sha3.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// writeConfig writes YAML to <dir>/config.yaml and returns its path.
func writeConfig(t *testing.T, dir, yaml string) string {
	t.Helper()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return p
}

// fsConfig writes a config with a single filesystem target named "local" rooted at <dir>/store
// and returns the config path.
func fsConfig(t *testing.T, dir string) string {
	t.Helper()
	storeDir := filepath.Join(dir, "store")
	return writeConfig(t, dir, "targets:\n  - name: local\n    type: filesystem\n    path: "+storeDir+"\n")
}

// ---- onShutdownOK ---------------------------------------------------------

func TestOnShutdownOK(t *testing.T) {
	if err := onShutdownOK(context.Canceled); err != nil {
		t.Fatalf("context.Canceled must map to a clean exit (nil), got %v", err)
	}
	// A wrapped Canceled is still a clean drain.
	if err := onShutdownOK(errors.Join(context.Canceled, nil)); err != nil {
		t.Fatalf("wrapped context.Canceled must map to nil, got %v", err)
	}
	if err := onShutdownOK(nil); err != nil {
		t.Fatalf("nil must stay nil, got %v", err)
	}
	boom := errors.New("boom")
	if err := onShutdownOK(boom); !errors.Is(err, boom) {
		t.Fatalf("a non-Canceled error must pass through unchanged, got %v", err)
	}
	if err := onShutdownOK(context.DeadlineExceeded); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DeadlineExceeded is a real failure, not a clean drain; got %v", err)
	}
}

// TestResolveConcurrency: --concurrency 0 (or negative) falls back to the configured concurrency; a
// positive flag wins. The ONE default shared by restore-all and drill.
func TestResolveConcurrency(t *testing.T) {
	if got := resolveConcurrency(0, 8); got != 8 {
		t.Fatalf("--concurrency 0 must use the configured value (8), got %d", got)
	}
	if got := resolveConcurrency(-1, 8); got != 8 {
		t.Fatalf("a negative --concurrency must use the configured value (8), got %d", got)
	}
	if got := resolveConcurrency(3, 8); got != 3 {
		t.Fatalf("a positive --concurrency must win, got %d", got)
	}
}

// ---- buildTargets ---------------------------------------------------------

func TestBuildTargets(t *testing.T) {
	dir := t.TempDir()
	targets, err := buildTargets([]config.Target{
		{Name: "raw", Type: "filesystem", Path: dir},
		{Name: "z", Type: "filesystem", Path: dir, Compression: "zstd"},
	})
	if err != nil {
		t.Fatalf("valid filesystem targets: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("want 2 targets, got %d", len(targets))
	}
	if targets[0].Codec != domain.CodecNone {
		t.Fatalf("empty compression must map to CodecNone, got %q", targets[0].Codec)
	}
	if targets[0].Sink.Name() != "raw" {
		t.Fatalf("sink name not threaded: %q", targets[0].Sink.Name())
	}
	if targets[1].Codec != domain.CodecZstd {
		t.Fatalf("zstd compression must map to CodecZstd, got %q", targets[1].Codec)
	}
}

func TestBuildTargetsUnknownType(t *testing.T) {
	if _, err := buildTargets([]config.Target{{Name: "x", Type: "carrier-pigeon", Path: "/x"}}); err == nil {
		t.Fatal("an unknown target type must error")
	}
}

func TestBuildTargetsBadCompression(t *testing.T) {
	_, err := buildTargets([]config.Target{{Name: "x", Type: "filesystem", Path: "/x", Compression: "gzip"}})
	if err == nil {
		t.Fatal("an unknown compression must error")
	}
	if !strings.Contains(err.Error(), "x") {
		t.Fatalf("error should name the offending target, got %v", err)
	}
}

// ---- isolatedPipeline -----------------------------------------------------

func TestIsolatedPipeline(t *testing.T) {
	cfg, err := config.Load("") // env-only load → defaults (FanoutStallSec=60, circuit knobs)
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	targets := []domain.Target{{Sink: &fakeSink{name: "t"}}}
	pipeline, breaker := isolatedPipeline(cfg, nil, nil, targets)
	if pipeline == nil {
		t.Fatal("isolatedPipeline returned a nil pipeline")
	}
	if breaker == nil {
		t.Fatal("isolatedPipeline must return a non-nil breaker (serve threads it into the sampler)")
	}
	if pipeline.StallTimeout != cfg.FanoutStall() {
		t.Fatalf("StallTimeout not wired from config: got %v want %v", pipeline.StallTimeout, cfg.FanoutStall())
	}
	if pipeline.Circuit != breaker {
		t.Fatal("the returned breaker must be the same one wired into the pipeline")
	}
}

// ---- runChecks ------------------------------------------------------------

func TestRunChecksResults(t *testing.T) {
	errBoom := errors.New("driver exploded")
	// Live ctx: the ok/error/panic checks complete on their own, deterministically.
	errs := runChecks(context.Background(), []startCheck{
		{"ok-check", func(context.Context) error { return nil }},
		{"err-check", func(context.Context) error { return errBoom }},
		{"panic-check", func(context.Context) error { panic("kaboom") }},
	})
	if len(errs) != 3 {
		t.Fatalf("want 3 results, got %d", len(errs))
	}
	if errs[0] != nil {
		t.Fatalf("an ok check must yield nil, got %v", errs[0])
	}
	if !errors.Is(errs[1], errBoom) {
		t.Fatalf("a failing check must wrap its error, got %v", errs[1])
	}
	if !strings.Contains(errs[1].Error(), "err-check") {
		t.Fatalf("a failing check must be labelled by name, got %v", errs[1])
	}
	if errs[2] == nil || !strings.Contains(errs[2].Error(), "panicked") {
		t.Fatalf("a panicking check must be recovered into an error, got %v", errs[2])
	}
}

func TestRunChecksAbandonsHungCheck(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	// An already-deadline-exceeded ctx: a check that ignores its ctx (blocks) must be ABANDONED
	// rather than hang startup forever.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Hour))
	defer cancel()
	done := make(chan []error, 1)
	go func() {
		done <- runChecks(ctx, []startCheck{
			{"hung-target", func(context.Context) error { <-release; return nil }},
		})
	}()
	select {
	case errs := <-done:
		if len(errs) != 1 || errs[0] == nil {
			t.Fatalf("a hung check must produce an error, got %v", errs)
		}
		if !strings.Contains(errs[0].Error(), "startup deadline exceeded") {
			t.Fatalf("abandonment must report the startup-deadline error, got %v", errs[0])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runChecks HUNG on a ctx-ignoring check — abandonment failed")
	}
}

// ---- startupGate / preflightTargets ---------------------------------------

func TestStartupGateRequiredOK(t *testing.T) {
	targets := []domain.Target{{Sink: &fakeSink{name: "t1"}}}
	err := startupGate(context.Background(), zap.NewNop(),
		[]startCheck{{"req", func(context.Context) error { return nil }}}, targets)
	if err != nil {
		t.Fatalf("all-ok required + a usable target must pass, got %v", err)
	}
}

func TestStartupGateRequiredFails(t *testing.T) {
	targets := []domain.Target{{Sink: &fakeSink{name: "t1"}}}
	err := startupGate(context.Background(), zap.NewNop(),
		[]startCheck{{"req", func(context.Context) error { return errors.New("dep down") }}}, targets)
	if err == nil || !strings.Contains(err.Error(), "dep down") {
		t.Fatalf("a failing required check must be fatal, got %v", err)
	}
}

func TestStartupGateDegradedButUsable(t *testing.T) {
	targets := []domain.Target{
		{Sink: &fakeSink{name: "bad", preflightErr: errors.New("unreachable")}},
		{Sink: &fakeSink{name: "good"}},
	}
	// One target failing but another usable is DEGRADED, not fatal (FR-012).
	if err := startupGate(context.Background(), zap.NewNop(), nil, targets); err != nil {
		t.Fatalf("a degraded-but-usable target set must not be fatal, got %v", err)
	}
}

func TestStartupGateNoTargetUsable(t *testing.T) {
	targets := []domain.Target{
		{Sink: &fakeSink{name: "a", preflightErr: errors.New("down")}},
		{Sink: &fakeSink{name: "b", preflightErr: errors.New("down")}},
	}
	err := startupGate(context.Background(), zap.NewNop(), nil, targets)
	if err == nil || !strings.Contains(err.Error(), "no target is usable") {
		t.Fatalf("ALL targets failing must be fatal, got %v", err)
	}
}

// TestPreflightTargets exercises preflightTargets THROUGH startupGate with required=nil — the
// exact delegation reconcile uses (startupGate runs no required checks, then returns
// preflightTargets). It is driven via startupGate rather than calling preflightTargets directly
// because a direct test-root call makes gosec's G602 range analysis lose the len(errs)==len(targets)
// bound proof and false-positive on main.go's targets[i]; the production caller never does, and the
// covered lines are identical either way.
func TestPreflightTargets(t *testing.T) {
	nop := zap.NewNop()
	// all ok (multi-target)
	if err := startupGate(context.Background(), nop, nil,
		[]domain.Target{{Sink: &fakeSink{name: "a"}}, {Sink: &fakeSink{name: "b"}}}); err != nil {
		t.Fatalf("all-ok targets must pass, got %v", err)
	}
	// some fail, at least one ok → degraded, nil
	if err := startupGate(context.Background(), nop, nil,
		[]domain.Target{{Sink: &fakeSink{name: "a", preflightErr: errors.New("x")}}, {Sink: &fakeSink{name: "b"}}}); err != nil {
		t.Fatalf("degraded-but-usable must pass, got %v", err)
	}
	// none usable → error
	if err := startupGate(context.Background(), nop, nil,
		[]domain.Target{{Sink: &fakeSink{name: "a", preflightErr: errors.New("x")}}}); err == nil {
		t.Fatal("no usable target must error")
	}
}

// ---- preflightScratch -----------------------------------------------------

func TestPreflightScratch(t *testing.T) {
	nop := zap.NewNop()
	if err := preflightScratch(t.TempDir(), nop); err != nil {
		t.Fatalf("a writable scratch dir must pass, got %v", err)
	}
	if err := preflightScratch("", nop); err != nil {
		t.Fatalf("an empty scratch dir warns but falls back to OS temp (nil), got %v", err)
	}
	nonexistent := filepath.Join(t.TempDir(), "missing-parent", "sub")
	if err := preflightScratch(nonexistent, nop); err == nil {
		t.Fatal("a non-writable/nonexistent scratch dir must error")
	}
}

// ---- manifestLoop ---------------------------------------------------------

func TestManifestLoopReturnsOnCancel(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	t.Cleanup(mock.Close)
	ledger := db.NewLedgerRepo(mock)
	targets := []domain.Target{{Sink: &fakeSink{name: "t"}}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled: TickLoop runs one pass then exits promptly
	done := make(chan struct{})
	go func() {
		manifestLoop(ctx, ledger, targets, time.Hour, zap.NewNop(), metrics.New())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("manifestLoop HUNG on a cancelled ctx")
	}
}

// ---- config flags ---------------------------------------------------------

func TestConfigFlag(t *testing.T) {
	if got := configFlag("serve", nil); got != "config.yaml" {
		t.Fatalf("default config path must be config.yaml, got %q", got)
	}
	if got := configFlag("serve", []string{"--config", "/etc/fbs/x.yaml"}); got != "/etc/fbs/x.yaml" {
		t.Fatalf("--config override not honored, got %q", got)
	}
}

// ---- newLogger ------------------------------------------------------------

func TestNewLogger(t *testing.T) {
	logger, syncLog, err := newLogger()
	if err != nil {
		t.Fatalf("newLogger error: %v", err)
	}
	if logger == nil || syncLog == nil {
		t.Fatal("newLogger must return a non-nil logger and sync func")
	}
	syncLog() // must not panic
}

// ---- buildReadSource (the DR read-source resolver sourceOp inlines) --------

// loadSource is the config.Load + buildReadSource pair the DR read commands use — the shape sourceOp
// inlines. A test helper so the buildReadSource cases below read like the command call sites.
func loadSource(t *testing.T, cfgPath, from string) (domain.Sink, string, error) {
	t.Helper()
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, "", err
	}
	return buildReadSource(cfg, from)
}

func TestBuildReadSourceOK(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	sink, name, err := loadSource(t, cfgPath, "local")
	if err != nil {
		t.Fatalf("buildReadSource(local): %v", err)
	}
	if sink == nil {
		t.Fatal("buildReadSource must return a sink")
	}
	if sink.Name() != "local" || name != "local" {
		t.Fatalf("wrong sink resolved: %q / %q", sink.Name(), name)
	}
}

func TestBuildReadSourceMissingTarget(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	_, _, err := loadSource(t, cfgPath, "does-not-exist")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("an unknown target must yield a not-found error, got %v", err)
	}
}

func TestBuildReadSourceNoTargets(t *testing.T) {
	cfgPath := writeConfig(t, t.TempDir(), "fileServiceBase: http://x\n")
	if _, _, err := loadSource(t, cfgPath, "local"); err == nil {
		t.Fatal("a config with no targets must error")
	}
}

// ---- runVerify / runRestore (target-only, no DB) --------------------------

// storeObject puts content into the "local" filesystem target via its sink and returns the hash.
func storeObject(t *testing.T, cfgPath string, content []byte) string {
	t.Helper()
	sink, _, err := loadSource(t, cfgPath, "local")
	if err != nil {
		t.Fatalf("build read source: %v", err)
	}
	h := sha3hex(content)
	n, err := sink.Store(context.Background(), h, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("store object: %v", err)
	}
	if n != int64(len(content)) {
		t.Fatalf("stored byte count = %d, want %d", n, len(content))
	}
	return h
}

func TestRunVerifyOK(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	h := storeObject(t, cfgPath, []byte("verify me end to end"))
	if err := runVerify([]string{"--config", cfgPath, "--hash", h, "--from", "local"}); err != nil {
		t.Fatalf("runVerify of a stored object must succeed, got %v", err)
	}
}

func TestRunVerifyMissingFlags(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	if err := runVerify([]string{"--config", cfgPath}); err == nil {
		t.Fatal("runVerify without --hash/--from must error")
	}
}

func TestRunVerifyUnknownTarget(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	h := sha3hex([]byte("x"))
	err := runVerify([]string{"--config", cfgPath, "--hash", h, "--from", "nope"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("runVerify against an unknown target must error, got %v", err)
	}
}

func TestRunRestoreOK(t *testing.T) {
	base := t.TempDir()
	cfgPath := fsConfig(t, base)
	content := []byte("restore me end to end")
	h := storeObject(t, cfgPath, content)
	to := t.TempDir()
	if err := runRestore([]string{"--config", cfgPath, "--hash", h, "--from", "local", "--to", to}); err != nil {
		t.Fatalf("runRestore of a stored object must succeed, got %v", err)
	}
	got, err := os.ReadFile(filepath.Join(to, h)) //nolint:gosec // test temp path
	if err != nil {
		t.Fatalf("restored file not found at <to>/<hash>: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("restored bytes do not match the stored object")
	}
}

func TestRunRestoreMissingFlags(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	if err := runRestore([]string{"--config", cfgPath, "--to", t.TempDir()}); err == nil {
		t.Fatal("runRestore without --hash/--from must error")
	}
}

// ---- config-validation error paths of the DB subcommands ------------------
// Each of these fails on Validate/ValidateDRLimits BEFORE any pool is opened (verified by reading
// main.go's order-of-operations), so no DB is contacted.

// invalidCfg writes a valid-YAML but incomplete config (no fileServiceBase, no DB host).
func invalidCfg(t *testing.T) string {
	t.Helper()
	return writeConfig(t, t.TempDir(), "metricsPort: 4004\n")
}

func TestServeInvalidConfig(t *testing.T) {
	err := serve(invalidCfg(t))
	if err == nil || !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("serve with a config missing fileServiceBase must fail with an invalid-config error, got %v", err)
	}
}

func TestRunBackfillInvalidConfig(t *testing.T) {
	err := runBackfill([]string{"--config", invalidCfg(t)})
	if err == nil || !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("runBackfill with an invalid config must fail before opening a pool, got %v", err)
	}
}

func TestRunReconcileInvalidConfig(t *testing.T) {
	err := runReconcile([]string{"--config", invalidCfg(t)})
	if err == nil || !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("runReconcile with an invalid config must fail before opening a pool, got %v", err)
	}
}

func TestRunAuditInvalidConfig(t *testing.T) {
	err := runAudit([]string{"--config", invalidCfg(t)})
	if err == nil || !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("runAudit with an invalid config must fail before opening a pool, got %v", err)
	}
}

func TestRunMigrateInvalidConfig(t *testing.T) {
	err := runMigrate(invalidCfg(t))
	if err == nil || !strings.Contains(err.Error(), "invalid config") {
		t.Fatalf("runMigrate with a config missing ledgerDB must fail before touching the DB, got %v", err)
	}
}
