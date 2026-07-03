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
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	cfgPath := fs.String("config", "config.json", "path to the config file")
	_ = fs.Parse(os.Args[2:])

	switch cmd {
	case "serve":
		if err := serve(*cfgPath); err != nil && !errors.Is(err, context.Canceled) {
			fatal(err)
		}
	case "migrate":
		if err := runMigrate(*cfgPath); err != nil {
			fatal(err)
		}
	case "backfill", "restore", "verify", "reconcile", "drill":
		fmt.Fprintf(os.Stderr, "%q: not implemented yet (see specs/008 tasks)\n", cmd)
		os.Exit(1)
	default:
		usage()
		os.Exit(2)
	}
}

func runMigrate(cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if cfg.LedgerDB == "" {
		return errors.New("ledgerDB not configured")
	}
	if err := db.Migrate(cfg.LedgerDB); err != nil {
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
	logger, _ := zap.NewProduction()
	defer func() { _ = logger.Sync() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Degraded (health-only) mode when the databases are not configured.
	if cfg.AlkemioDB == "" || cfg.LedgerDB == "" {
		logger.Warn("databases not configured — running health-only (no backup)")
		srv := startHTTP(cfg.MetricsPort, &httpapi.HealthHandler{}, logger)
		defer shutdown(srv)
		<-ctx.Done()
		return ctx.Err()
	}

	alkemioPool, err := db.NewPool(ctx, cfg.AlkemioDB)
	if err != nil {
		return fmt.Errorf("alkemio pool: %w", err)
	}
	defer alkemioPool.Close()
	ledgerPool, err := db.NewPool(ctx, cfg.LedgerDB)
	if err != nil {
		return fmt.Errorf("ledger pool: %w", err)
	}
	defer ledgerPool.Close()

	targets, err := buildTargets(cfg.Targets)
	if err != nil {
		return err
	}
	pipeline := domain.NewPipeline(
		fileservice.New(cfg.FileServiceBase, nil),
		db.NewLedgerRepo(ledgerPool),
		targets,
	)
	cons := consumer.New(consumer.Deps{
		Outbox:      db.NewOutboxRepo(alkemioPool),
		Pipeline:    pipeline,
		ListenPool:  alkemioPool.Pool,
		Concurrency: cfg.Concurrency,
		Logger:      logger,
	})

	srv := startHTTP(cfg.MetricsPort, &httpapi.HealthHandler{Outbox: alkemioPool, Ledger: ledgerPool}, logger)
	defer shutdown(srv)

	logger.Info("file-backup-service serving",
		zap.Int("metricsPort", cfg.MetricsPort), zap.Int("targets", len(targets)))
	return cons.Run(ctx)
}

func buildTargets(cfgs []config.Target) ([]domain.Target, error) {
	targets := make([]domain.Target, 0, len(cfgs))
	for _, t := range cfgs {
		var sink domain.Sink
		switch t.Type {
		case "filesystem":
			sink = filesystem.New(t.Name, t.Path)
		case "s3":
			sink = s3.New(t.Name, t.Endpoint, t.Bucket, t.Prefix)
		default:
			return nil, fmt.Errorf("target %q: unknown type %q", t.Name, t.Type)
		}
		codec := domain.CodecNone
		if t.Compression == string(domain.CodecZstd) {
			codec = domain.CodecZstd
		}
		targets = append(targets, domain.Target{Sink: sink, Required: t.Required, Codec: codec})
	}
	return targets, nil
}

func startHTTP(port int, health *httpapi.HealthHandler, logger *zap.Logger) *http.Server {
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           httpapi.NewRouter(httpapi.Deps{Health: health, Logger: logger}),
		ReadHeaderTimeout: 5 * time.Second,
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
	fmt.Fprintln(os.Stderr, "usage: file-backup-service <serve|migrate|backfill|restore|verify|reconcile|drill> [--config path]")
}
