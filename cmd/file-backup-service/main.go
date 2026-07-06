// Command file-backup-service is the Alkemio continuous file-backup worker + CLI.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(configFlag("serve", args))
		if errors.Is(err, context.Canceled) {
			err = nil
		}
	case "migrate":
		err = runMigrate(configFlag("migrate", args))
	case "restore":
		err = runRestore(args)
	case "verify":
		err = runVerify(args)
	case "reconcile":
		err = runReconcile(args)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
	case "audit":
		// NOT swallowing context.Canceled (unlike serve/reconcile/backfill): an
		// interrupted audit is an INCOMPLETE integrity check and must exit nonzero, so a
		// cron doesn't read an aborted verification as passed.
		err = runAudit(args)
	case "backfill":
		err = runBackfill(args)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
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

func configFlag(name string, args []string) string {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "path to the config file")
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
	alkemioPool, err := db.NewPool(ctx, cfg.AlkemioDB.DSN(), cfg.PoolSize(8))
	if err != nil {
		return fmt.Errorf("alkemio pool: %w", err)
	}
	defer alkemioPool.Close()
	ledgerPool, err := db.NewPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4))
	if err != nil {
		return fmt.Errorf("ledger pool: %w", err)
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
	go func() { defer bgWG.Done(); manifestLoop(ctx, ledger, targets, cfg.ManifestEvery(), logger) }()
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
	cfgPath := fs.String("config", "config.yaml", "config file")
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
	pool, err := db.NewPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4))
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("ledger pool: %w", err)
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
	cfgPath := fs.String("config", "config.yaml", "config file")
	ratePerSec := fs.Int("rate", 0, "max repairs per second (0 = unlimited)")
	_ = fs.Parse(args)
	ctx, stop := signalContext()
	defer stop()
	cfg, ledger, pool, targets, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	st, err := domain.NewReconciler(ledger, targets, cfg.PerObjectTimeout(), cfg.ScratchDir).Run(ctx, *ratePerSec)
	fmt.Printf("reconcile: repaired=%d skipped=%d failed=%d\n", st.Repaired, st.Skipped, st.Failed)
	return err
}

// runBackfill backs up the pre-existing corpus (US2/T022): it enumerates the
// file-service `file` table and runs each object through the normal pipeline (which
// dedups against the ledger, so it's resumable + repeatable). Needs the full config —
// it fetches from file-service AND reads the alkemio `file` table AND writes the ledger.
func runBackfill(args []string) error {
	fs := flag.NewFlagSet("backfill", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "config file")
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

	alkemioPool, err := db.NewPool(ctx, cfg.AlkemioDB.DSN(), cfg.PoolSize(4))
	if err != nil {
		return fmt.Errorf("alkemio pool: %w", err)
	}
	defer alkemioPool.Close()
	ledgerPool, err := db.NewPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4))
	if err != nil {
		return fmt.Errorf("ledger pool: %w", err)
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

	st, err := domain.NewBackfiller(files, domain.NewPipeline(fsClient, ledger, targets), cfg.PerObjectTimeout()).Run(ctx, *ratePerSec)
	fmt.Printf("backfill: backed=%d failed=%d\n", st.Backed, st.Failed)
	return err
}

// randHex returns a random externalID-shaped hex string — a rotating keyset start so a
// sampled audit checks a different band each run instead of the same fixed prefix.
func randHex() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "" // fall back to the start; a failed rand never blocks the audit
	}
	return hex.EncodeToString(b[:])
}

// runAudit verifies the ledger against reality (FR-014/T030): for a sample per target,
// confirm the target actually still holds what the ledger records as stored. A nonzero
// 'missing' count means silent loss on a target — a nonzero exit so cron/CI can alert.
func runAudit(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "config file")
	sample := fs.Int("sample", 0, "objects to check per target (0 = all)")
	_ = fs.Parse(args)
	ctx, stop := signalContext()
	defer stop()
	_, ledger, pool, targets, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	startAfter := "" // a full audit starts at the beginning
	if *sample > 0 {
		startAfter = randHex() // a sampled audit rotates its band each run (no fixed blind spot)
	}
	rep, err := domain.Audit(ctx, ledger, targets, *sample, startAfter)
	var unexpected []string
	for _, t := range rep.Targets {
		status := ""
		switch {
		case t.UnexpectedlyUnverifiable():
			status = "  [UNVERIFIABLE — every Exists denied but target is NOT worm: read credential/endpoint broken?]"
			unexpected = append(unexpected, t.Target)
		case t.Unverifiable():
			status = "  [unverifiable — worm target, read-denying by design; no coverage expected]"
		}
		fmt.Printf("audit %s: checked=%d missing=%d errors=%d%s\n", t.Target, t.Checked, t.Missing, t.Errors, status)
	}
	// Exit nonzero on silent loss OR a normally-readable target that couldn't be verified
	// (a broken read path) — an expected-worm Unverifiable target is fine (exit 0).
	switch {
	case err != nil:
		return err
	case rep.Missing() > 0:
		return fmt.Errorf("%d ledger-stored objects are missing from their target", rep.Missing())
	case len(unexpected) > 0:
		return fmt.Errorf("targets unverifiable (read path broken, not worm): %v", unexpected)
	}
	return nil
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
// so a slow pass can't wedge shutdown. The shared skeleton for the background samplers.
func everyTick(ctx context.Context, interval, timeout time.Duration, fn func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		fctx, cancel := context.WithTimeout(ctx, timeout)
		fn(fctx)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
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
	})
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
	})
}

// manifestLoop writes a ledger snapshot (JSONL) to every target on a cadence (FR-015),
// starting immediately. Best-effort: a failed snapshot is logged, not fatal (a target's
// manifest is a DR convenience, not the backup itself).
func manifestLoop(ctx context.Context, ledger *db.LedgerRepo, targets []domain.Target, every time.Duration, logger *zap.Logger) {
	everyTick(ctx, every, 30*time.Minute, func(fctx context.Context) {
		if err := domain.WriteManifests(fctx, ledger, targets, domain.ManifestName(time.Now())); err != nil && ctx.Err() == nil {
			logger.Warn("manifest snapshot", zap.Error(err))
		}
	})
}

type startCheck struct {
	name string
	fn   func(context.Context) error
}

// runChecks runs every check CONCURRENTLY (independent pools/endpoints), each with a
// recover so a driver panic becomes an error, not a crash. Returns per-check errors
// (nil = ok) in order.
func runChecks(ctx context.Context, checks []startCheck) []error {
	errs := make([]error, len(checks))
	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Add(1)
		go func(i int, c startCheck) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs[i] = domain.PanicErr(c.name, r)
				}
			}()
			if err := c.fn(ctx); err != nil {
				errs[i] = fmt.Errorf("%s: %w", c.name, err)
			}
		}(i, c)
	}
	wg.Wait()
	return errs
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
