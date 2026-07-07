// Command file-backup-service is the Alkemio continuous file-backup worker + CLI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	consumer "github.com/alkem-io/file-backup-service/internal/adapter/inbound/consumer"
	httpapi "github.com/alkem-io/file-backup-service/internal/adapter/inbound/http"
	"github.com/alkem-io/file-backup-service/internal/adapter/inbound/metrics"
	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db"
	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/fileservice"
	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/sink/filesystem"
	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/sink/s3"
	"github.com/alkem-io/file-backup-service/internal/config"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	args := os.Args[2:]
	// A DIRECT call per arm (not a func-value dispatch table) so the apispec generator can
	// statically trace main -> serve -> NewRouter and keep /live,/health,/metrics in
	// openapi.yaml. onShutdownOK wraps the long-running drain commands (serve/reconcile/
	// backfill) so a clean SIGTERM is exit 0; audit deliberately does NOT (see its arm).
	var err error
	switch os.Args[1] {
	case "serve":
		err = onShutdownOK(serve(configFlag("serve", args)))
	case "migrate":
		err = runMigrate(configFlag("migrate", args))
	case "restore":
		err = runRestore(args)
	case "verify":
		err = runVerify(args)
	case "reconcile":
		err = onShutdownOK(runReconcile(args))
	case "audit":
		// NO onShutdownOK — an interrupted audit is an INCOMPLETE integrity check and must
		// exit nonzero, so a cron doesn't read an aborted verification as passed.
		err = runAudit(args)
	case "backfill":
		err = onShutdownOK(runBackfill(args))
	case "drill":
		fmt.Fprintf(os.Stderr, "%q: not implemented yet (see specs/008 tasks)\n", os.Args[1])
		os.Exit(1)
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

// onShutdownOK maps a clean-shutdown context.Canceled to a success exit — a SIGTERM to a
// long-running subcommand is an orderly drain, not a crash, so k8s/systemd/cron don't read
// it as a failure. The one owner of this policy; audit deliberately opts out (see the switch).
func onShutdownOK(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func configFlag(name string, args []string) string {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	_ = fs.Parse(args)
	return *cfgPath
}

func runMigrate(cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.LedgerDB.Validate("ledgerDB"); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if err := db.Migrate(cfg.LedgerDB.DSN()); err != nil {
		return err
	}
	fmt.Println("ledger migrations applied")
	return nil
}

func serve(cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()
	ctx, stop := signalContext()
	defer stop()

	mx := metrics.New()

	// config.Load guarantees both DSNs and >=1 target — no silent no-op mode.
	// Size for: 1 permanent LISTEN + up to Concurrency in-flight bookkeeping +
	// health + margin, so a NOTIFY burst can't starve MarkDone/Fail.
	alkemioPool, err := openPool(ctx, cfg.AlkemioDB.DSN(), cfg.PoolSize(8), "alkemio")
	if err != nil {
		return err
	}
	defer alkemioPool.Close()
	ledgerPool, err := openPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4), "ledger")
	if err != nil {
		return err
	}
	defer ledgerPool.Close()

	outbox := db.NewOutboxRepo(alkemioPool, cfg.MaxAttempts, cfg.MaxDeliveries)
	ledger := db.NewLedgerRepo(ledgerPool)
	targets, err := buildTargets(cfg.Targets)
	if err != nil {
		return err
	}

	// Start the HTTP surface BEFORE the startup dependency checks, so /live is always
	// answerable and /health can report "unhealthy" while a dependency is unreachable —
	// rather than the process being invisible until every probe passes.
	srv, err := startHTTP(cfg.MetricsPort, httpapi.Deps{
		Health:  &httpapi.HealthHandler{Outbox: outbox, Ledger: ledger},
		Metrics: mx.Handler(),
		Logger:  logger,
	}, logger)
	if err != nil {
		return err
	}
	defer shutdown(srv)

	fsClient := fileservice.New(cfg.FileServiceBase, cfg.Concurrency, nil)
	// Fail loudly if any dependency is unusable at startup (bounded so a hung dial
	// fails fast instead of hanging), rather than a silent no-op with /health green.
	if err := startupChecks(ctx, logger, fsClient, outbox, ledger, targets); err != nil {
		return err
	}

	pipeline := domain.NewPipeline(fsClient, ledger, targets)
	pipeline.Metrics = mx
	// Per-target circuit breaker (T017a): a persistently-down/hung target trips out of the
	// fan-out so objects needing it are DEFERRED (re-claimable), not dead-lettered.
	breaker := domain.NewCircuitBreaker(cfg.CircuitThreshold, cfg.CircuitCooldown())
	pipeline.Circuit = breaker
	pipeline.StallTimeout = cfg.FanoutStall() // drop a hung target individually, don't stall the barrier
	cons := consumer.New(consumer.Deps{
		Outbox:           outbox,
		Pipeline:         pipeline,
		ListenPool:       alkemioPool.Pool,
		Concurrency:      cfg.Concurrency,
		PollEvery:        cfg.PollEvery(),
		StaleTTL:         cfg.StaleTTL(),
		PerObjectTimeout: cfg.PerObjectTimeout(),
		OnDeadLetter:     mx.DeadLetter,
		OnObjectTimeout:  mx.ObjectTimeout,
		OnSourceGone:     mx.SourceGone,
		Logger:           logger,
	})

	// Background loops, stopped before the pools close (defer runs LIFO, ahead of the
	// pool Close defers above): the RPO/lag gauges (FR-026) and the periodic ledger
	// snapshot to each target (FR-015, standalone-restorability).
	var bgWG sync.WaitGroup
	bgWG.Add(3)
	go func() { defer bgWG.Done(); sampleRPO(ctx, outbox, ledger, targets, breaker, mx) }()
	go func() { defer bgWG.Done(); sampleCoverage(ctx, ledger, targets, mx) }()
	go func() { defer bgWG.Done(); manifestLoop(ctx, ledger, targets, cfg.ManifestEvery(), logger, mx) }()
	defer bgWG.Wait()

	logger.Info("file-backup-service serving", zap.Int("metricsPort", cfg.MetricsPort), zap.Int("targets", len(targets)))
	return cons.Run(ctx)
}

// signalContext returns a context cancelled on SIGINT/SIGTERM, so a long-running
// serve loop or a stalled DR op honors the operator's stop signal.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}

// sourceOp is the shared scaffold for the DR subcommands: parse --config/--hash/--from
// (plus any extra flags via register), validate the resolved target's sink, and run op
// under a signal-cancellable context.
func sourceOp(name string, args []string, register func(*flag.FlagSet), op func(ctx context.Context, sink domain.Sink, hash string) error) error {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	hash := fs.String("hash", "", "content hash (externalID)")
	from := fs.String("from", "", "source target name")
	if register != nil {
		register(fs)
	}
	_ = fs.Parse(args)
	if *hash == "" || *from == "" {
		return fmt.Errorf("%s requires --hash and --from", name)
	}
	sink, err := sinkFor(*cfgPath, *from)
	if err != nil {
		return err
	}
	ctx, stop := signalContext()
	defer stop()
	return op(ctx, sink, *hash)
}

func runRestore(args []string) error {
	var to string
	return sourceOp("restore", args,
		func(fs *flag.FlagSet) { fs.StringVar(&to, "to", "/storage", "destination directory") },
		func(ctx context.Context, sink domain.Sink, hash string) error {
			if err := domain.RestoreObject(ctx, sink, hash, to); err != nil {
				return err
			}
			fmt.Printf("restored %s -> %s/%s\n", hash, to, hash)
			return nil
		})
}

func runVerify(args []string) error {
	return sourceOp("verify", args, nil,
		func(ctx context.Context, sink domain.Sink, hash string) error {
			if err := domain.VerifyObject(ctx, sink, hash); err != nil {
				return err
			}
			fmt.Printf("verified %s\n", hash)
			return nil
		})
}

// openPool opens a pgx pool, wrapping the error with a "<label> pool" prefix — the one
// owner of that error shape, shared by serve/backfill/ledgerJob (each of which still owns
// its own `defer pool.Close()` at the call site). label is the DB's role in the message.
func openPool(ctx context.Context, dsn string, size int32, label string) (*db.Pool, error) {
	p, err := db.NewPool(ctx, dsn, size)
	if err != nil {
		return nil, fmt.Errorf("%s pool: %w", label, err)
	}
	return p, nil
}

// registerConfigFlag registers the shared --config flag on fs with one canonical default +
// help, so the subcommands don't drift on the path default or the description. Used both by
// the parse-and-return configFlag wrapper and by the multi-flag subcommands (which own an fs).
func registerConfigFlag(fs *flag.FlagSet) *string {
	return fs.String("config", "config.yaml", "path to the config file")
}

// ledgerJob opens config for a ledger-DB-only subcommand (reconcile/audit): it
// validates the targets (not the full serve config — these run in the DR state without
// file-service / the outbox DB), opens + probes the ledger pool, and builds the sinks.
// The caller MUST close the returned pool.
func ledgerJob(ctx context.Context, cfgPath string) (*config.Config, *db.LedgerRepo, *db.Pool, []domain.Target, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("load config: %w", err)
	}
	if err := cfg.ValidateDR(); err != nil {
		return nil, nil, nil, nil, fmt.Errorf("invalid config: %w", err)
	}
	pool, err := openPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4), "ledger")
	if err != nil {
		return nil, nil, nil, nil, err
	}
	ledger := db.NewLedgerRepo(pool)
	if err := ledger.Probe(ctx); err != nil {
		pool.Close()
		return nil, nil, nil, nil, fmt.Errorf("ledger not accessible (schema / migrate?): %w", err)
	}
	targets, err := buildTargets(cfg.Targets)
	if err != nil {
		pool.Close()
		return nil, nil, nil, nil, err
	}
	return cfg, ledger, pool, targets, nil
}

// runReconcile repairs under-replicated objects target-to-target (FR-025/T029): for
// each object the ledger shows not stored on every target, fetch it from a target that
// has it and re-fan-out to the missing ones. Needs only the ledger DB + the targets.
func runReconcile(args []string) error {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	ratePerSec := fs.Int("rate", 0, "max repairs per second (0 = unlimited)")
	_ = fs.Parse(args)
	ctx, stop := signalContext()
	defer stop()
	cfg, ledger, pool, targets, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()
	// A decoded object is staged to scratchDir (up to the object-size cap) before re-fan-out;
	// a missing/read-only path must fail LOUD at startup, not per-object mid-repair.
	if err := preflightScratch(cfg.ScratchDir, logger); err != nil {
		return err
	}
	// Same hung-target isolation as serve: a black-holing destination target is stall-dropped
	// (not left to wedge every repair for the full perObjectTimeout) and circuit-tripped after
	// repeated failures so the pass stops hammering it.
	breaker := domain.NewCircuitBreaker(cfg.CircuitThreshold, cfg.CircuitCooldown())
	rec := domain.NewReconciler(ledger, targets, cfg.PerObjectTimeout(), cfg.ScratchDir, cfg.FanoutStall(), breaker).
		OnError(func(id string, e error) {
			logger.Warn("reconcile repair failed", zap.String("externalID", id), zap.Error(e))
		})
	st, err := rec.Run(ctx, *ratePerSec)
	fmt.Printf("reconcile: repaired=%d skipped=%d failed=%d\n", st.Repaired, st.Skipped, st.Failed)
	if err != nil {
		return err // sweep error, or ctx cancellation (onShutdownOK maps Canceled → clean exit 0)
	}
	// A COMPLETED pass that left objects unrepaired must exit nonzero so a cron/CI backstop
	// alerts — a silent exit 0 on an all-failed reconcile hides persistent under-replication.
	if st.Failed > 0 {
		return fmt.Errorf("reconcile left %d object(s) unrepaired (still under-replicated)", st.Failed)
	}
	return nil
}

// runBackfill backs up the pre-existing corpus (US2/T022): it enumerates the
// file-service `file` table and runs each object through the normal pipeline (which
// dedups against the ledger, so it's resumable + repeatable). Needs the full config —
// it fetches from file-service AND reads the alkemio `file` table AND writes the ledger.
func runBackfill(args []string) error {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	ratePerSec := fs.Int("rate", 0, "max backups per second (0 = unlimited)")
	_ = fs.Parse(args)
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	ctx, stop := signalContext()
	defer stop()

	alkemioPool, err := openPool(ctx, cfg.AlkemioDB.DSN(), cfg.PoolSize(4), "alkemio")
	if err != nil {
		return err
	}
	defer alkemioPool.Close()
	ledgerPool, err := openPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4), "ledger")
	if err != nil {
		return err
	}
	defer ledgerPool.Close()

	ledger := db.NewLedgerRepo(ledgerPool)
	files := db.NewFileRepo(alkemioPool)
	fsClient := fileservice.New(cfg.FileServiceBase, cfg.Concurrency, nil)
	targets, err := buildTargets(cfg.Targets)
	if err != nil {
		return err
	}

	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	// Required infrastructure for backfill: the source (file-service), the ledger, and the
	// `file` corpus. NOT the outbox (backfill reads `file`, not the queue). Same startup
	// gate + per-target isolation as serve.
	if err := startupGate(ctx, logger, []startCheck{
		{"file-service unreachable", fsClient.Preflight},
		{"ledger not accessible (schema / migrate?)", ledger.Probe},
		{"file corpus not readable", files.Probe},
	}, targets); err != nil {
		return err
	}

	// Same hung-target isolation as serve (stall-drop + circuit) — a bare pipeline would let a
	// black-holing target wedge every object for the full perObjectTimeout, invisibly.
	breaker := domain.NewCircuitBreaker(cfg.CircuitThreshold, cfg.CircuitCooldown())
	pipeline := domain.NewPipeline(fsClient, ledger, targets).WithIsolation(cfg.FanoutStall(), breaker)
	st, err := domain.NewBackfiller(files, pipeline, cfg.PerObjectTimeout()).Run(ctx, *ratePerSec)
	fmt.Printf("backfill: backed=%d failed=%d\n", st.Backed, st.Failed)
	if err != nil {
		return err // sweep/DB error, or ctx cancellation (onShutdownOK maps Canceled → clean exit 0)
	}
	// A COMPLETED pass that couldn't fully back up every object exits nonzero so an operator
	// scripting `backfill && next-step` doesn't proceed as if the corpus were fully protected.
	if st.Failed > 0 {
		return fmt.Errorf("backfill left %d object(s) not fully backed up", st.Failed)
	}
	return nil
}

// preflightScratch fails loud at reconcile startup if the scratch dir can't be written —
// reconcile stages each decoded object there (up to the object-size cap) before re-fan-out,
// so a missing/read-only mount would otherwise fail EVERY repair mid-pass with only a
// "failed=N" count. An empty dir uses the OS temp dir; warn, because a memory-backed /tmp
// (a tmpfs emptyDir) stages the full object into RAM and can OOM the pod on a large object —
// scratchDir should be a disk-backed volume sized for the largest object.
func preflightScratch(dir string, logger *zap.Logger) error {
	if dir == "" {
		logger.Warn("scratchDir unset: staging decoded objects under the OS temp dir — set scratchDir to a disk-backed volume sized for the largest object, else a tmpfs /tmp can OOM the pod",
			zap.String("osTempDir", os.TempDir()))
	}
	f, err := os.CreateTemp(dir, "reconcile-preflight-*")
	if err != nil {
		return fmt.Errorf("scratchDir not writable (set scratchDir to a writable, disk-backed path): %w", err)
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}

// runAudit verifies the ledger against reality (FR-014/T030): for a sample per target,
// confirm the target actually still holds what the ledger records as stored. A nonzero
// 'missing' count means silent loss on a target — a nonzero exit so cron/CI can alert.
func runAudit(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	sample := fs.Int("sample", 0, "objects to check per target (0 = all)")
	_ = fs.Parse(args)
	ctx, stop := signalContext()
	defer stop()
	_, ledger, pool, targets, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Audit derives its own random keyset band for a sampled run (*sample>0); the pass/fail
	// verdict lives with the report (rep.FailErr) — cmd only prints + propagates.
	rep, err := domain.Audit(ctx, ledger, targets, *sample)
	for _, t := range rep.Targets {
		status := ""
		switch {
		case t.UnexpectedlyUnverifiable():
			status = "  [UNVERIFIABLE — every Exists denied but target is NOT worm: read credential/endpoint broken?]"
		case t.Unverifiable():
			status = "  [unverifiable — worm target, read-denying by design; no coverage expected]"
		}
		fmt.Printf("audit %s: checked=%d missing=%d errors=%d%s\n", t.Target, t.Checked, t.Missing, t.Errors, status)
	}
	if err != nil { // print partial per-target results above, then surface the sweep error
		return err
	}
	return rep.FailErr()
}

// sinkFor loads config, validates the target set, and builds the named target's sink.
// It validates the whole target set (not the full serve config) so a DR restore fails
// with a clear config error — including an env-token collision that would silently
// build the sink with a sibling target's injected secret — instead of a raw minio/os
// error or a wrong-store restore.
func sinkFor(cfgPath, name string) (domain.Sink, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	if err := cfg.ValidateTargets(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	for _, t := range cfg.Targets {
		if t.Name == name {
			return buildSink(t)
		}
	}
	return nil, fmt.Errorf("target %q not found in config", name)
}

// everyTick runs fn immediately and then every interval until ctx is cancelled. Each
// fn call gets a timeout-bounded child ctx (derived from ctx, so shutdown aborts it)
// so a slow pass can't wedge shutdown. onPanic is invoked with the recovered value if fn
// panics, so the pass stays OBSERVABLE (sample-error counter / log) instead of the panic
// being swallowed silently. The shared skeleton for the background samplers.
func everyTick(ctx context.Context, interval, timeout time.Duration, fn func(context.Context), onPanic func(recovered any)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		runTick(ctx, timeout, fn, onPanic)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// runTick runs one bounded tick with a recover, so a pgx/driver panic in a sampler or the
// manifest loop degrades that one pass instead of crashing the serve process — these
// background goroutines are on the other side of a boundary no request/pipeline recover
// reaches. A panic unwinds fn BEFORE its own error branch runs (where SampleError lives),
// so the recover routes the panic to onPanic to still fire the sample-error / log — else a
// sampler that panics every tick would freeze its gauge stale-green with zero signal.
func runTick(ctx context.Context, timeout time.Duration, fn func(context.Context), onPanic func(recovered any)) {
	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil && onPanic != nil {
			onPanic(r)
		}
	}()
	fn(fctx)
}

// sampleRPO refreshes the backlog/lag/last-success gauges (FR-026). A failed pass
// increments the sample-error counter (alert on rate>0) so a frozen, stale-green gauge
// is itself detectable, rather than silently holding its last value.
func sampleRPO(ctx context.Context, outbox *db.OutboxRepo, ledger *db.LedgerRepo, targets []domain.Target, breaker *domain.CircuitBreaker, mx *metrics.Metrics) {
	names := domain.TargetNames(targets)
	everyTick(ctx, 15*time.Second, 5*time.Second, func(fctx context.Context) {
		mx.SetCircuitOpen(breaker.OpenCount()) // in-memory, always sampleable
		failed := false
		if pending, ageSec, err := outbox.BacklogStats(fctx); err == nil {
			mx.SetBacklog(pending, ageSec)
		} else {
			failed = true
		}
		if ageSec, never, ok, err := ledger.LastVerifiedAge(fctx, names); err != nil {
			failed = true
		} else {
			mx.SetNeverVerified(never) // a from-day-one dead target is counted, not invisible
			if ok {
				mx.SetLastSuccessAge(ageSec)
			}
		}
		if failed {
			mx.SampleError()
		}
	}, func(any) { mx.SampleError() }) // a panicking pass is a failed sample, not a silent freeze
}

// sampleCoverage refreshes the under-replication gauge on a coarse cadence (the count
// is a full-ledger scan, so not every 15s) — the coverage backstop a dead-lettered
// object can't hide from.
func sampleCoverage(ctx context.Context, ledger *db.LedgerRepo, targets []domain.Target, mx *metrics.Metrics) {
	names := domain.TargetNames(targets)
	everyTick(ctx, 5*time.Minute, 30*time.Second, func(fctx context.Context) {
		if n, err := ledger.CoverageGaps(fctx, names); err == nil {
			mx.SetUnderReplicated(n)
		} else {
			mx.SampleError()
		}
	}, func(any) { mx.SampleError() }) // a panicking pass is a failed sample, not a silent freeze
}

// manifestLoop writes a ledger snapshot (JSONL) to every target on a cadence (FR-015),
// starting immediately. Best-effort: a failed snapshot is logged, not fatal (a target's
// manifest is a DR convenience, not the backup itself).
func manifestLoop(ctx context.Context, ledger *db.LedgerRepo, targets []domain.Target, every time.Duration, logger *zap.Logger, mx *metrics.Metrics) {
	everyTick(ctx, every, 30*time.Minute, func(fctx context.Context) {
		if err := domain.WriteManifests(fctx, ledger, targets, domain.ManifestName(time.Now())); err != nil && ctx.Err() == nil {
			// Metric + log: a persistently failing manifest write silently defeats a target's
			// standalone restorability while every per-object gauge stays green (FR-015).
			mx.ManifestError()
			logger.Warn("manifest snapshot", zap.Error(err))
		}
	}, func(r any) { mx.ManifestError(); logger.Warn("manifest snapshot panicked", zap.Any("panic", r)) })
}

type startCheck struct {
	name string
	fn   func(context.Context) error
}

// runChecks runs every check CONCURRENTLY (independent pools/endpoints), each recovered so
// a driver panic becomes an error, not a crash. Returns per-check errors (nil = ok) in order.
func runChecks(ctx context.Context, checks []startCheck) []error {
	return domain.RunParallel(checks,
		func(c startCheck) string { return c.name },
		func(c startCheck) error {
			if err := c.fn(ctx); err != nil {
				return fmt.Errorf("%s: %w", c.name, err)
			}
			return nil
		})
}

// startupChecks validates dependencies at startup, bounded by a deadline so a hung dial
// fails fast. REQUIRED infrastructure (file-service, outbox, ledger) is fatal on any
// failure — the worker can do nothing without them. TARGETS follow the runtime's
// per-target isolation: a target that fails preflight is logged LOUD and left degraded
// (its objects stay not-done, retried, surfaced by the coverage gauge + failure
// counter), NOT fatal; serve fails only if NO target is usable (nothing could be backed
// up). This matches FR-012 rather than turning one target's blip into a fleet CrashLoop.
func startupChecks(ctx context.Context, logger *zap.Logger, fs *fileservice.Client, outbox *db.OutboxRepo, ledger *db.LedgerRepo, targets []domain.Target) error {
	return startupGate(ctx, logger, []startCheck{
		{"file-service unreachable", fs.Preflight},
		{"outbox not accessible (scoped role / schema?)", outbox.Probe},
		{"outbox not writable", outbox.CheckWriteGrant},
		{"ledger not accessible (schema / migrate?)", ledger.Probe},
	}, targets)
}

// startupGate runs the REQUIRED infra checks (fatal on any failure) then the target
// preflights (per-target isolation, fatal only if none usable), under a bounded deadline —
// the one owner of the startup policy, called by both serve and backfill with their own
// required-check list (so the two can't diverge on the deadline or the target policy).
func startupGate(ctx context.Context, logger *zap.Logger, required []startCheck, targets []domain.Target) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := errors.Join(runChecks(ctx, required)...); err != nil {
		return err
	}
	return preflightTargets(ctx, logger, targets)
}

// preflightTargets runs the target preflights with per-target isolation (FR-012),
// shared by serve and backfill: a target that fails is logged LOUD and left DEGRADED
// (retried at runtime), NOT fatal; only NO usable target is fatal (nothing could be
// backed up) — one target's transient blip must not CrashLoop the whole worker.
func preflightTargets(ctx context.Context, logger *zap.Logger, targets []domain.Target) error {
	tChecks := make([]startCheck, len(targets))
	for i, t := range targets {
		tChecks[i] = startCheck{"target " + t.Sink.Name(), t.Sink.Preflight}
	}
	errs := runChecks(ctx, tChecks)
	usable := 0
	for i, err := range errs {
		if err == nil {
			usable++
			continue
		}
		logger.Error("target preflight failed — starting DEGRADED; backups to it will retry until it recovers",
			zap.String("target", targets[i].Sink.Name()), zap.Error(err))
	}
	if usable == 0 {
		return fmt.Errorf("no target is usable at startup: %w", errors.Join(errs...))
	}
	return nil
}

func buildTargets(cfgs []config.Target) ([]domain.Target, error) {
	targets := make([]domain.Target, 0, len(cfgs))
	for _, t := range cfgs {
		sink, err := buildSink(t)
		if err != nil {
			return nil, err
		}
		codec, err := domain.ParseCodec(t.Compression) // same owner as config validation
		if err != nil {
			return nil, fmt.Errorf("target %q: %w", t.Name, err)
		}
		targets = append(targets, domain.Target{Sink: sink, Codec: codec, Worm: t.Worm})
	}
	return targets, nil
}

func buildSink(t config.Target) (domain.Sink, error) {
	switch t.Type {
	case config.TargetTypeFilesystem:
		return filesystem.New(t.Name, t.Path), nil
	case config.TargetTypeS3:
		return s3.New(s3.Config{
			Name: t.Name, Endpoint: t.Endpoint, Region: t.Region, Bucket: t.Bucket, Prefix: t.Prefix,
			AccessKey: t.AccessKey, SecretKey: t.SecretKey, UseSSL: t.UseSSL, SSE: t.SSE,
		})
	default:
		return nil, fmt.Errorf("target %q: unknown type %q", t.Name, t.Type)
	}
}

func startHTTP(port int, deps httpapi.Deps, logger *zap.Logger) (*http.Server, error) {
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           httpapi.NewRouter(deps),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	// Bind synchronously so a bad/taken port fails serve loudly, rather than being
	// logged-and-ignored while the worker runs on with no /health or /metrics.
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return nil, fmt.Errorf("http listen on %s: %w", srv.Addr, err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server", zap.Error(err))
		}
	}()
	return srv, nil
}

func shutdown(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: file-backup-service <serve|migrate|restore|verify|reconcile|audit|backfill|drill> [flags]")
}
