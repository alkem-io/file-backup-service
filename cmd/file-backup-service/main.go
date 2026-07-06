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
		err = runAudit(args)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
	case "backfill", "drill":
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
	if cfg.LedgerDB.Host == "" || cfg.LedgerDB.User == "" || cfg.LedgerDB.DBName == "" {
		return errors.New("ledgerDB.host, ledgerDB.user and ledgerDB.dbName are required for migrate")
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
	alkemioPool, err := db.NewPool(ctx, cfg.AlkemioDB.DSN(), int32(cfg.Concurrency)+8) //nolint:gosec // Concurrency is a small operator-set value
	if err != nil {
		return fmt.Errorf("alkemio pool: %w", err)
	}
	defer alkemioPool.Close()
	ledgerPool, err := db.NewPool(ctx, cfg.LedgerDB.DSN(), int32(cfg.Concurrency)+4) //nolint:gosec // Concurrency is a small operator-set value
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
	if err := startupChecks(ctx, fsClient, outbox, ledger, targets); err != nil {
		return err
	}

	pipeline := domain.NewPipeline(fsClient, ledger, targets)
	pipeline.Metrics = mx
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
	go func() { defer bgWG.Done(); sampleRPO(ctx, outbox, ledger, mx) }()
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
func ledgerJob(ctx context.Context, cfgPath string) (*db.LedgerRepo, *db.Pool, []domain.Target, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load config: %w", err)
	}
	if err := cfg.ValidateTargets(); err != nil {
		return nil, nil, nil, fmt.Errorf("invalid config: %w", err)
	}
	pool, err := db.NewPool(ctx, cfg.LedgerDB.DSN(), int32(cfg.Concurrency)+4) //nolint:gosec // Concurrency is validated <=1024
	if err != nil {
		return nil, nil, nil, fmt.Errorf("ledger pool: %w", err)
	}
	ledger := db.NewLedgerRepo(pool)
	if err := ledger.Probe(ctx); err != nil {
		pool.Close()
		return nil, nil, nil, fmt.Errorf("ledger not accessible (schema / migrate?): %w", err)
	}
	targets, err := buildTargets(cfg.Targets)
	if err != nil {
		pool.Close()
		return nil, nil, nil, err
	}
	return ledger, pool, targets, nil
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
	ledger, pool, targets, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	st, err := domain.NewReconciler(ledger, targets).Run(ctx, *ratePerSec)
	fmt.Printf("reconcile: repaired=%d skipped=%d failed=%d\n", st.Repaired, st.Skipped, st.Failed)
	return err
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
	ledger, pool, targets, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	rep, err := domain.Audit(ctx, ledger, targets, *sample)
	fmt.Printf("audit: checked=%d missing=%d errors=%d\n", rep.Checked, rep.Missing, rep.Errors)
	if err == nil && rep.Missing > 0 {
		err = fmt.Errorf("%d ledger-stored objects are missing from their target", rep.Missing)
	}
	return err
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
func sampleRPO(ctx context.Context, outbox *db.OutboxRepo, ledger *db.LedgerRepo, mx *metrics.Metrics) {
	everyTick(ctx, 15*time.Second, 5*time.Second, func(fctx context.Context) {
		failed := false
		if pending, ageSec, err := outbox.BacklogStats(fctx); err == nil {
			mx.SetBacklog(pending, ageSec)
		} else {
			failed = true
		}
		if ageSec, ok, err := ledger.LastVerifiedAge(fctx); err != nil {
			failed = true
		} else if ok {
			mx.SetLastSuccessAge(ageSec)
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
	names := make([]string, len(targets))
	for i, t := range targets {
		names[i] = t.Sink.Name()
	}
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

// startupChecks fails serve loudly if any dependency is unusable: the outbox (scoped
// role reads the schema + holds the UPDATE grant), the ledger (schema/migrate), and
// every target sink (creds/bucket/path). Bounded by a deadline so a hung dial fails
// fast. All checks run CONCURRENTLY (they hit independent pools/endpoints), each with a
// recover so a driver panic in one is reported, not a crash.
func startupChecks(ctx context.Context, fs *fileservice.Client, outbox *db.OutboxRepo, ledger *db.LedgerRepo, targets []domain.Target) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	type check struct {
		name string
		fn   func(context.Context) error
	}
	checks := make([]check, 0, 4+len(targets))
	checks = append(checks,
		check{"file-service unreachable", fs.Preflight},
		check{"outbox not accessible (scoped role / schema?)", outbox.Probe},
		check{"outbox not writable", outbox.CheckWriteGrant},
		check{"ledger not accessible (schema / migrate?)", ledger.Probe},
	)
	for _, t := range targets {
		checks = append(checks, check{"target " + t.Sink.Name() + " preflight", t.Sink.Preflight})
	}

	errs := make([]error, len(checks))
	var wg sync.WaitGroup
	for i, c := range checks {
		wg.Add(1)
		go func(i int, c check) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs[i] = fmt.Errorf("%s panicked: %v", c.name, r)
				}
			}()
			if err := c.fn(ctx); err != nil {
				errs[i] = fmt.Errorf("%s: %w", c.name, err)
			}
		}(i, c)
	}
	wg.Wait()
	return errors.Join(errs...)
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
		targets = append(targets, domain.Target{Sink: sink, Codec: codec})
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
