// Command file-backup-service is the Alkemio continuous file-backup worker + CLI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
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
	case "backfill", "reconcile", "drill":
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
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
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

	// Fail loudly if the scoped role can't reach the outbox, rather than a silent
	// no-op with /health green.
	outbox := db.NewOutboxRepo(alkemioPool)
	if err := outbox.Probe(ctx); err != nil {
		return fmt.Errorf("outbox not accessible (scoped role / schema?): %w", err)
	}

	targets, err := buildTargets(cfg.Targets)
	if err != nil {
		return err
	}
	pipeline := domain.NewPipeline(fileservice.New(cfg.FileServiceBase, nil), db.NewLedgerRepo(ledgerPool), targets)
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
		Logger:           logger,
	})

	srv := startHTTP(cfg.MetricsPort, httpapi.Deps{
		Health:  &httpapi.HealthHandler{Outbox: alkemioPool, Ledger: ledgerPool},
		Metrics: mx.Handler(),
		Logger:  logger,
	}, logger)
	defer shutdown(srv)

	logger.Info("file-backup-service serving", zap.Int("metricsPort", cfg.MetricsPort), zap.Int("targets", len(targets)))
	return cons.Run(ctx)
}

func runRestore(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "config file")
	hash := fs.String("hash", "", "content hash (externalID) to restore")
	from := fs.String("from", "", "source target name")
	to := fs.String("to", "/storage", "destination directory")
	_ = fs.Parse(args)
	if *hash == "" || *from == "" {
		return errors.New("restore requires --hash and --from")
	}
	sink, err := sinkFor(*cfgPath, *from)
	if err != nil {
		return err
	}
	if err := domain.RestoreObject(context.Background(), sink, *hash, *to); err != nil {
		return err
	}
	fmt.Printf("restored %s -> %s/%s\n", *hash, *to, *hash)
	return nil
}

func runVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	cfgPath := fs.String("config", "config.yaml", "config file")
	hash := fs.String("hash", "", "content hash to verify")
	from := fs.String("from", "", "source target name")
	scratch := fs.String("scratch", ".", "scratch dir for the streamed decode (must be real disk, not tmpfs)")
	_ = fs.Parse(args)
	if *hash == "" || *from == "" {
		return errors.New("verify requires --hash and --from")
	}
	sink, err := sinkFor(*cfgPath, *from)
	if err != nil {
		return err
	}
	if err := domain.VerifyObject(context.Background(), sink, *hash, *scratch); err != nil {
		return err
	}
	fmt.Printf("verified %s on %s\n", *hash, *from)
	return nil
}

// sinkFor loads config and builds the single named target's sink.
func sinkFor(cfgPath, name string) (domain.Sink, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	for _, t := range cfg.Targets {
		if t.Name == name {
			return buildSink(t)
		}
	}
	return nil, fmt.Errorf("target %q not found in config", name)
}

func buildTargets(cfgs []config.Target) ([]domain.Target, error) {
	targets := make([]domain.Target, 0, len(cfgs))
	for _, t := range cfgs {
		sink, err := buildSink(t)
		if err != nil {
			return nil, err
		}
		codec := domain.CodecNone
		if t.Compression == string(domain.CodecZstd) {
			codec = domain.CodecZstd
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

func startHTTP(port int, deps httpapi.Deps, logger *zap.Logger) *http.Server {
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           httpapi.NewRouter(deps),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server", zap.Error(err))
		}
	}()
	return srv
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
	fmt.Fprintln(os.Stderr, "usage: file-backup-service <serve|migrate|restore|verify|backfill|reconcile|drill> [flags]")
}
