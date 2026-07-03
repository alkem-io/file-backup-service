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
	"github.com/alkem-io/file-backup-service/internal/config"
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
	case "backfill", "restore", "verify", "reconcile", "drill", "migrate":
		fmt.Fprintf(os.Stderr, "%q: not implemented yet (see specs/008 tasks)\n", cmd)
		os.Exit(1)
	default:
		usage()
		os.Exit(2)
	}
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

	// Health/metrics surface.
	router := httpapi.NewRouter(httpapi.Deps{Health: &httpapi.HealthHandler{}, Logger: logger})
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server", zap.Error(err))
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("file-backup-service serving", zap.Int("metricsPort", cfg.MetricsPort), zap.Int("targets", len(cfg.Targets)))
	// TODO(T004/T006/T014): wire pgx pools, sinks, pipeline; run the real consumer.
	return consumer.New().Run(ctx)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: file-backup-service <serve|backfill|restore|verify|reconcile|drill|migrate> [--config path]")
}
