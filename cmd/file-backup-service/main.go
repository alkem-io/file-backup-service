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

	// Fail loudly if any dependency is unusable at startup (bounded so a hung dial
	// fails fast instead of hanging), rather than a silent no-op with /health green.
	if err := startupChecks(ctx, outbox, ledger, targets); err != nil {
		return err
	}

	pipeline := domain.NewPipeline(fileservice.New(cfg.FileServiceBase, cfg.Concurrency, nil), ledger, targets)
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
		Logger:           logger,
	})

	// Background loops, stopped before the pools close (defer runs LIFO, ahead of the
	// pool Close defers above): the RPO/lag gauges (FR-026) and the periodic ledger
	// snapshot to each target (FR-015, standalone-restorability).
	var bgWG sync.WaitGroup
	bgWG.Add(2)
	go func() { defer bgWG.Done(); sampleRPO(ctx, outbox, ledger, mx) }()
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

// sampleRPO periodically refreshes the backlog/lag/last-success gauges until ctx is
// cancelled. Best-effort: a failed sample is skipped (the counters still flow), and a
// slow query is bounded so it can't wedge shutdown.
func sampleRPO(ctx context.Context, outbox *db.OutboxRepo, ledger *db.LedgerRepo, mx *metrics.Metrics) {
	const every = 15 * time.Second
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if pending, ageSec, err := outbox.BacklogStats(sctx); err == nil {
			mx.SetBacklog(pending, ageSec)
		}
		if ageSec, ok, err := ledger.LastVerifiedAge(sctx); err == nil && ok {
			mx.SetLastSuccessAge(ageSec)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// manifestLoop writes a ledger snapshot (JSONL) to every target on a cadence (FR-015),
// starting immediately, until ctx is cancelled. Best-effort: a failed snapshot is
// logged, not fatal (a target's manifest is a DR convenience, not the backup itself).
func manifestLoop(ctx context.Context, ledger *db.LedgerRepo, targets []domain.Target, every time.Duration, logger *zap.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		// Generous bound for a full ledger dump; derived from ctx so shutdown aborts it.
		mctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		if err := domain.WriteManifests(mctx, ledger, targets, domain.ManifestName(time.Now())); err != nil && ctx.Err() == nil {
			logger.Warn("manifest snapshot", zap.Error(err))
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// startupChecks fails serve loudly if any dependency is unusable: the outbox (scoped
// role reads the schema + holds the UPDATE grant), the ledger (schema/migrate), and
// every target sink (creds/bucket/path). Bounded by a deadline so a hung dial fails
// fast; the sink preflights (independent RTTs) run concurrently.
func startupChecks(ctx context.Context, outbox *db.OutboxRepo, ledger *db.LedgerRepo, targets []domain.Target) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := outbox.Probe(ctx); err != nil {
		return fmt.Errorf("outbox not accessible (scoped role / schema?): %w", err)
	}
	if err := outbox.CheckWriteGrant(ctx); err != nil {
		return fmt.Errorf("outbox not writable: %w", err)
	}
	if err := ledger.Probe(ctx); err != nil {
		return fmt.Errorf("ledger not accessible (schema / migrate?): %w", err)
	}
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(i int, t domain.Target) {
			defer wg.Done()
			if err := t.Sink.Preflight(ctx); err != nil {
				errs[i] = fmt.Errorf("target %s preflight: %w", t.Sink.Name(), err)
			}
		}(i, t)
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
	case "filesystem":
		return filesystem.New(t.Name, t.Path), nil
	case "s3":
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
