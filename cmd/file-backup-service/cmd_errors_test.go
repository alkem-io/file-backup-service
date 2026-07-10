package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	httpapi "github.com/alkem-io/file-backup-service/internal/adapter/inbound/http"
	"github.com/alkem-io/file-backup-service/internal/adapter/inbound/metrics"
	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db"
	"github.com/alkem-io/file-backup-service/internal/config"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// ---- openPool -------------------------------------------------------------

// TestOpenPoolBadDSN: a malformed DSN fails at parse time and openPool wraps it with the DB's
// role label, so a startup pool error names which DB (alkemio/ledger) was misconfigured.
func TestOpenPoolBadDSN(t *testing.T) {
	_, err := openPool(context.Background(), "postgres://u:p@h:5432/d?sslmode=bogus", 4, time.Second, "ledger")
	if err == nil || !strings.Contains(err.Error(), "ledger pool") {
		t.Fatalf("a bad DSN must fail openPool with a role-labelled error, got %v", err)
	}
}

// ---- run() dispatch: restore/verify arms + clean exit 0 -------------------

// storeRaw writes content under an ARBITRARY (valid-format) hash key on the "local" filesystem
// target, WITHOUT the pipeline's hash verification — so a test can stage bytes that do or do not
// match the key, exercising the DR verify/restore integrity check.
func storeRaw(t *testing.T, cfgPath, hash string, content []byte) {
	t.Helper()
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	sink, _, err := buildReadSource(cfg, "local")
	if err != nil {
		t.Fatalf("build read source: %v", err)
	}
	if _, err := sink.Store(context.Background(), hash, bytes.NewReader(content)); err != nil {
		t.Fatalf("store raw object: %v", err)
	}
}

// TestRunVerifyRestoreDispatchOK: run() routes the verify and restore subcommands and, on a
// stored+intact object, returns the clean exit code 0 — covering both dispatch arms and the
// success return that the invalid-config dispatch cases never reach.
func TestRunVerifyRestoreDispatchOK(t *testing.T) {
	dir := t.TempDir()
	cfgPath := fsConfig(t, dir)
	h := storeObject(t, cfgPath, []byte("dispatch through run"))
	if code := run([]string{"fbs", "verify", "--config", cfgPath, "--hash", h, "--from", "local"}); code != 0 {
		t.Fatalf("run verify of an intact object must exit 0, got %d", code)
	}
	if code := run([]string{"fbs", "restore", "--config", cfgPath, "--hash", h, "--from", "local", "--to", t.TempDir()}); code != 0 {
		t.Fatalf("run restore of an intact object must exit 0, got %d", code)
	}
}

// ---- runVerify / runRestore integrity failure -----------------------------

// TestRunVerifyTamperedFails: bytes stored under a hash they do NOT match must fail verify — the
// hash-arbiter re-derives the content hash and it won't equal the key, so a silently-corrupted
// target object is caught (a nonzero exit for cron/CI).
func TestRunVerifyTamperedFails(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	h := sha3hex([]byte("the real bytes"))
	storeRaw(t, cfgPath, h, []byte("TAMPERED bytes that do not hash to h"))
	err := runVerify([]string{"--config", cfgPath, "--hash", h, "--from", "local"})
	if err == nil {
		t.Fatal("verify of tampered bytes (hash mismatch) must fail")
	}
}

// TestRunRestoreTamperedFails: restore likewise refuses to write out bytes whose content hash
// doesn't match the requested hash — a DR restore never emits an unverified object.
func TestRunRestoreTamperedFails(t *testing.T) {
	cfgPath := fsConfig(t, t.TempDir())
	h := sha3hex([]byte("genuine content"))
	storeRaw(t, cfgPath, h, []byte("corrupt payload"))
	err := runRestore([]string{"--config", cfgPath, "--hash", h, "--from", "local", "--to", t.TempDir()})
	if err == nil {
		t.Fatal("restore of tampered bytes (hash mismatch) must fail")
	}
}

// ---- sourceOp: absurd perObjectTimeoutSec overflow fallback ---------------

// TestSourceOpTimeoutOverflowFallback: the single-source DR read path validates only the chosen
// target (not the numeric limits), so an absurd perObjectTimeoutSec that overflows time.Duration
// (PerObjectTimeout clamps it to 0) must fall back to the 30-minute default via
// domain.NormalizePerObjectTimeout rather than run the DR op with an instant deadline. A verify of an
// intact object still succeeds, proving the fallback timeout (not a 0/near-instant one) governed the op.
func TestSourceOpTimeoutOverflowFallback(t *testing.T) {
	dir := t.TempDir()
	storeDir := dir + "/store"
	// perObjectTimeoutSec * time.Second overflows int64 nanoseconds → negative Duration.
	cfgPath := writeConfig(t, dir,
		"perObjectTimeoutSec: 10000000000\ntargets:\n  - name: local\n    type: filesystem\n    path: "+storeDir+"\n")
	h := storeObject(t, cfgPath, []byte("survives the overflow fallback"))
	if err := runVerify([]string{"--config", cfgPath, "--hash", h, "--from", "local"}); err != nil {
		t.Fatalf("verify must succeed under the fallback timeout, got %v", err)
	}
}

// ---- manifestLoop: a failed snapshot is metered + logged, not fatal -------

// errManifestSink drains the encoder stream (so the ledger-reading producer completes) then
// fails the write — modelling a target whose PutManifest errors.
type errManifestSink struct{ *fakeSink }

func (errManifestSink) PutManifest(_ context.Context, _ string, r io.Reader) error {
	_, _ = io.Copy(io.Discard, r) // drain so the producer's ledger query completes before we return
	return errors.New("manifest write failed")
}

// TestManifestLoopErrorIsMeteredNotFatal: a failing manifest snapshot (a target write error) must
// route through TickLoop's onError — incrementing the ManifestError metric and logging a warning —
// WITHOUT crashing serve (a target manifest is a DR convenience, not the backup itself). The loop
// then exits promptly on ctx cancel.
func TestManifestLoopErrorIsMeteredNotFatal(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	t.Cleanup(mock.Close)
	// One empty ledger page so the manifest producer finishes cleanly (0 lines), then the sink
	// fails the write — the failure the onError branch must meter.
	mock.ExpectQuery("file_backup_object").
		WillReturnRows(pgxmock.NewRows([]string{"externalID", "size", "createdBy", "sourceCreatedDate"}))
	ledger := db.NewLedgerRepo(mock)
	targets := []domain.Target{{Sink: errManifestSink{&fakeSink{name: "t"}}}}

	core, logs := observer.New(zapcore.WarnLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		manifestLoop(ctx, ledger, targets, time.Hour, zap.New(core), metrics.New())
		close(done)
	}()

	deadline := time.After(3 * time.Second)
	for logs.FilterMessage("manifest snapshot").Len() == 0 {
		select {
		case <-deadline:
			t.Fatal("a failed manifest snapshot must be logged via onError")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("manifestLoop must exit promptly after ctx cancel")
	}
}

// ---- startHTTP / shutdown -------------------------------------------------

// TestStartHTTPOKThenShutdown: startHTTP binds a free port (":0") synchronously and returns a
// running server, which shutdown then closes cleanly.
func TestStartHTTPOKThenShutdown(t *testing.T) {
	srv, err := startHTTP(0, httpapi.Deps{Logger: zap.NewNop()}, zap.NewNop())
	if err != nil {
		t.Fatalf("startHTTP on port 0 must bind a free port, got %v", err)
	}
	if srv == nil {
		t.Fatal("startHTTP must return a non-nil server")
	}
	shutdown(srv) // must close cleanly without hanging
}

// TestStartHTTPPortInUse: startHTTP binds synchronously, so an already-occupied port fails serve
// LOUDLY (rather than a background listen error the worker ignores while running blind).
func TestStartHTTPPortInUse(t *testing.T) {
	// Occupy the same bind address startHTTP uses (":port" = all interfaces), so the second bind
	// genuinely collides regardless of the loopback-vs-wildcard distinction.
	ln, err := net.Listen("tcp", ":0") //nolint:gosec // deliberately occupies the wildcard addr startHTTP binds, to force the in-use collision
	if err != nil {
		t.Fatalf("occupy a port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	port := ln.Addr().(*net.TCPAddr).Port

	if _, err := startHTTP(port, httpapi.Deps{Logger: zap.NewNop()}, zap.NewNop()); err == nil {
		t.Fatalf("startHTTP on an occupied port %d must return an error", port)
	}
}
