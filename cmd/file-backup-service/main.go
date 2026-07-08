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
	"github.com/alkem-io/file-backup-service/internal/config"
	"github.com/alkem-io/file-backup-service/internal/domain"
	"github.com/alkem-io/file-backup-service/internal/fsutil"
)

func main() { os.Exit(run(os.Args)) }

// run dispatches one subcommand and returns the process exit code — extracted from main so the
// dispatch + exit-code mapping is unit-testable (main is just os.Exit(run(os.Args))). A DIRECT
// call per arm (not a func-value dispatch table) keeps apispec's main -> run -> serve ->
// NewRouter static trace intact, so openapi.yaml still carries /live,/health,/metrics.
// onShutdownOK wraps the long-running drains (serve/reconcile/backfill) so a clean SIGTERM is
// exit 0; audit deliberately does NOT — an interrupted integrity check must exit nonzero.
func run(argv []string) int {
	if len(argv) < 2 {
		usage()
		return 2
	}
	args := argv[2:]
	var err error
	switch argv[1] {
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
		err = runAudit(args)
	case "backfill":
		err = onShutdownOK(runBackfill(args))
	case "drill":
		fmt.Fprintf(os.Stderr, "%q: not implemented yet (see specs/008 tasks)\n", argv[1])
		return 1
	default:
		usage()
		return 2
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
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
	ctx, stop := signalContext()
	defer stop()
	return serveCtx(ctx, cfgPath)
}

// serveCtx is serve parameterized on the context, so the worker loop can be driven to a clean
// shutdown from a test (a cancellable ctx) rather than only a real signal. main -> serve ->
// serveCtx -> startHTTP -> NewRouter keeps the apispec static trace intact.
func serveCtx(ctx context.Context, cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	logger, syncLog, err := newLogger()
	if err != nil {
		return err
	}
	defer syncLog()

	mx := metrics.New()

	// config.Load guarantees both DSNs and >=1 target — no silent no-op mode.
	// Size for: 1 permanent LISTEN + up to Concurrency in-flight bookkeeping +
	// health + margin, so a NOTIFY burst can't starve MarkDone/Fail.
	alkemioPool, err := openPool(ctx, cfg.AlkemioDB.DSN(), cfg.PoolSize(8), cfg.DBTimeout(), "alkemio")
	if err != nil {
		return err
	}
	defer alkemioPool.Close()
	ledgerPool, err := openPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4), cfg.DBTimeout(), "ledger")
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

	// Per-target circuit breaker (T017a): a persistently-down/hung target trips out of the
	// fan-out so objects needing it are DEFERRED (re-claimable), not dead-lettered. serve and
	// backfill both build their runtime pipeline via isolatedPipeline, so the stall-drop +
	// circuit wiring has ONE owner — a future isolation knob can't land in serve while backfill
	// silently keeps running a differently-isolated pipeline. serve also keeps the breaker for
	// the sampler's targets-down gauge.
	pipeline, breaker := isolatedPipeline(cfg, fsClient, ledger, targets)
	pipeline.Metrics = mx
	cons := consumer.New(consumer.Deps{
		Outbox:           outbox,
		Pipeline:         pipeline,
		ListenPool:       alkemioPool.Pool,
		Concurrency:      cfg.Concurrency,
		PollEvery:        cfg.PollEvery(),
		StaleTTL:         cfg.StaleTTL(),
		PerObjectTimeout: cfg.PerObjectTimeout(),
		DBTimeout:        cfg.DBTimeout(),
		OnDeadLetter:     mx.DeadLetter,
		OnObjectTimeout:  mx.ObjectTimeout,
		OnSourceGone:     mx.SourceGone,
		Logger:           logger,
	})

	// Background loops, stopped before the pools close (defer runs LIFO, ahead of the
	// pool Close defers above): the RPO/lag gauges (FR-026) and the periodic ledger
	// snapshot to each target (FR-015, standalone-restorability).
	sampler := domain.NewSampler(outbox, ledger, targets, breaker, mx)
	var bgWG sync.WaitGroup
	bgWG.Add(3)
	// SampleError once per failed pass (a returned read error OR a panic — TickLoop routes both
	// to onError), so a frozen stale-green gauge is itself detectable.
	go func() {
		defer bgWG.Done()
		domain.TickLoop(ctx, 15*time.Second, 5*time.Second, sampler.SampleRPO, func(any, bool) { mx.SampleError() })
	}()
	go func() { // coarse cadence: CoverageGaps is a full-ledger scan, not every 15s
		defer bgWG.Done()
		domain.TickLoop(ctx, 5*time.Minute, 30*time.Second, sampler.SampleCoverage, func(any, bool) { mx.SampleError() })
	}()
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

// newLogger builds the production zap logger + the sync closure the caller defers — the one
// owner of the logger-init block shared by serve/reconcile/backfill.
func newLogger() (*zap.Logger, func(), error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, nil, fmt.Errorf("init logger: %w", err)
	}
	return logger, func() { _ = logger.Sync() }, nil
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
	sink, cfg, err := sinkFor(*cfgPath, *from)
	if err != nil {
		return err
	}
	ctx, stop := signalContext()
	defer stop()
	// Bound the DR op like every other path (serve/backfill/reconcile all cap one object with
	// perObjectTimeout): a black-holing sink — one that accepts the connection but never
	// returns bytes — must fail the operator's command on the deadline, not hang it forever
	// (only SIGINT would otherwise stop it).
	timeout := cfg.PerObjectTimeout()
	if timeout <= 0 {
		// sinkFor validates only the targets, NOT the numeric limits (validateLimits' maxSec cap
		// is a serve/DR-pool concern), so an absurd perObjectTimeoutSec could overflow to a
		// non-positive Duration here. Fall back to the default rather than an instant-deadline op.
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
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
func openPool(ctx context.Context, dsn string, size int32, stmtTimeout time.Duration, label string) (*db.Pool, error) {
	p, err := db.NewPool(ctx, dsn, size, stmtTimeout)
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
	pool, err := openPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4), cfg.DBTimeout(), "ledger")
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
	logger, syncLog, err := newLogger()
	if err != nil {
		return err
	}
	defer syncLog()
	// A decoded object is staged to scratchDir (up to the object-size cap) before re-fan-out;
	// a missing/read-only path must fail LOUD at startup, not per-object mid-repair.
	if err := preflightScratch(cfg.ScratchDir, logger); err != nil {
		return err
	}
	// Preflight the targets too — the SAME gate serve/backfill run (ledgerJob already probed
	// the ledger, so no required checks here). A wholly-misconfigured/unreachable target must
	// fail LOUD at startup (degraded, or fatal if NONE usable), not be discovered per-object
	// mid-pass after burning source I/O and tripping its circuit.
	if err := startupGate(ctx, logger, nil, targets); err != nil {
		return err
	}
	// Same hung-target isolation as serve: a black-holing destination target is stall-dropped
	// (not left to wedge every repair for the full perObjectTimeout) and circuit-tripped after
	// repeated failures so the pass stops hammering it.
	breaker := cfg.NewCircuitBreaker()
	rec := domain.NewReconciler(ledger, targets, cfg.PerObjectTimeout(), cfg.ScratchDir, cfg.FanoutStall(), breaker, cfg.Concurrency).
		OnError(func(id string, e error) {
			logger.Warn("reconcile repair failed", zap.String("externalID", id), zap.Error(e))
		})
	st, err := rec.Run(ctx, *ratePerSec)
	fmt.Printf("reconcile: repaired=%d skipped=%d failed=%d\n", st.Repaired, st.Skipped, st.Failed)
	if err != nil {
		return err // sweep error, or ctx cancellation (onShutdownOK maps Canceled → clean exit 0)
	}
	// A COMPLETED pass that could NOT fully protect every object must exit nonzero so a cron/CI
	// backstop alerts. Both buckets count: Failed = a target was down (retryable next pass);
	// Skipped = the object lives on NO current target at all (near-total loss — only a
	// primary-store backfill can restore it), which is MORE severe, not less, so it must not
	// slip through as a clean exit 0.
	if st.Failed > 0 || st.Skipped > 0 {
		return fmt.Errorf("reconcile could not fully protect %d object(s): %d unrepaired (target down), %d on NO target (need a primary-store backfill)",
			st.Failed+st.Skipped, st.Failed, st.Skipped)
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

	alkemioPool, err := openPool(ctx, cfg.AlkemioDB.DSN(), cfg.PoolSize(4), cfg.DBTimeout(), "alkemio")
	if err != nil {
		return err
	}
	defer alkemioPool.Close()
	ledgerPool, err := openPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4), cfg.DBTimeout(), "ledger")
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

	logger, syncLog, err := newLogger()
	if err != nil {
		return err
	}
	defer syncLog()

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

	// Same hung-target isolation as serve (stall-drop + circuit) via the shared builder — a bare
	// pipeline would let a black-holing target wedge every object for the full perObjectTimeout,
	// invisibly. backfill doesn't need the breaker handle itself (no sampler).
	pipeline, _ := isolatedPipeline(cfg, fsClient, ledger, targets)
	st, err := domain.NewBackfiller(files, pipeline, cfg.PerObjectTimeout(), cfg.Concurrency).Run(ctx, *ratePerSec)
	fmt.Printf("backfill: backed=%d skipped=%d deferred=%d failed=%d\n", st.Backed, st.Skipped, st.Deferred, st.Failed)
	if err != nil {
		return err // sweep/DB error, or ctx cancellation (onShutdownOK maps Canceled → clean exit 0)
	}
	// A COMPLETED pass with GENUINE failures exits nonzero so an operator scripting
	// `backfill && next-step` doesn't proceed as if the corpus were fully protected. Skipped
	// (source deleted before backfill) and Deferred (stored on every reachable target; only a
	// circuit-open target's gap remains, which reconcile refills — T017a) are both benign and
	// do NOT fail the pass.
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
	if err := fsutil.ProbeWritable(dir); err != nil {
		return fmt.Errorf("scratchDir not writable (set scratchDir to a writable, disk-backed path): %w", err)
	}
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
		case t.Errors > 0 && !t.Worm:
			status = "  [PARTIAL — some Exists probes errored (throttled/intermittent); sample not fully verified]"
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
func sinkFor(cfgPath, name string) (domain.Sink, *config.Config, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, fmt.Errorf("load config: %w", err)
	}
	if err := cfg.ValidateTargets(); err != nil {
		return nil, nil, fmt.Errorf("invalid config: %w", err)
	}
	for _, t := range cfg.Targets {
		if t.Name == name {
			sink, berr := config.BuildSink(t)
			return sink, cfg, berr
		}
	}
	return nil, nil, fmt.Errorf("target %q not found in config", name)
}

// manifestLoop writes a ledger snapshot (JSONL) to every target on a cadence (FR-015),
// starting immediately. Best-effort: a failed snapshot is metered + logged, not fatal (a
// target's manifest is a DR convenience, not the backup itself). The failure side-effect
// (ManifestError + log) is stated ONCE in onError — TickLoop routes both a returned error and
// a panic there, distinguished by a type switch on cause.
func manifestLoop(ctx context.Context, ledger *db.LedgerRepo, targets []domain.Target, every time.Duration, logger *zap.Logger, mx *metrics.Metrics) {
	domain.TickLoop(ctx, every, 30*time.Minute,
		func(fctx context.Context) error {
			err := domain.WriteManifests(fctx, ledger, targets, domain.ManifestName(time.Now()))
			if err != nil && ctx.Err() == nil {
				return err
			}
			return nil // a shutdown-cancelled snapshot is not a failure
		},
		func(cause any, isPanic bool) {
			mx.ManifestError()
			if isPanic {
				logger.Warn("manifest snapshot panicked", zap.Any("panic", cause))
			} else if err, ok := cause.(error); ok {
				logger.Warn("manifest snapshot", zap.Error(err))
			}
		})
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
			// Run each check in its OWN goroutine and ABANDON it on ctx (the startup deadline):
			// a check that ignores its ctx — filesystem.Preflight's os.MkdirAll on a wedged
			// mount is an uninterruptible syscall — would otherwise hang serve FOREVER at
			// startup with green /health (which probes the DBs, not the targets). domain's
			// RunAbandonable is the single owner of that abandon + buffered-chan + recover
			// primitive (also used by the pipeline's storeWithCtx/callWithCtx), so this reuses it
			// rather than re-implementing the load-bearing cap-1 buffer that lets an abandoned
			// goroutine complete its send without leaking.
			return domain.RunAbandonable(ctx,
				func() error {
					if err := c.fn(ctx); err != nil {
						return fmt.Errorf("%s: %w", c.name, err)
					}
					return nil
				},
				func() error {
					return fmt.Errorf("%s: startup deadline exceeded (hung target/dependency?): %w", c.name, ctx.Err())
				},
				func(r any) error { return domain.PanicErr(c.name, r) })
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

// isolatedPipeline builds the runtime backup pipeline with the SAME per-target hung-target
// isolation (stall-drop + circuit breaker) for serve and backfill — the one owner of that
// wiring, so a future isolation knob can't be added to one command and silently missed by the
// other (which would leave a hung target un-dropped on that path). It returns the breaker too,
// which serve threads into the sampler's targets-down gauge; backfill discards it.
func isolatedPipeline(cfg *config.Config, src domain.Source, ledger domain.Ledger, targets []domain.Target) (*domain.Pipeline, *domain.CircuitBreaker) {
	breaker := cfg.NewCircuitBreaker()
	return domain.NewPipeline(src, ledger, targets).WithIsolation(cfg.FanoutStall(), breaker), breaker
}

func buildTargets(cfgs []config.Target) ([]domain.Target, error) {
	targets := make([]domain.Target, 0, len(cfgs))
	for _, t := range cfgs {
		sink, err := config.BuildSink(t)
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

func usage() {
	fmt.Fprintln(os.Stderr, "usage: file-backup-service <serve|migrate|restore|verify|reconcile|audit|backfill|drill> [flags]")
}
