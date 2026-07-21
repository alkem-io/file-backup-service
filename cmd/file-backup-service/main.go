// Command file-backup-service is the Alkemio continuous file-backup worker + CLI.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
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
		// NOT onShutdownOK-wrapped (like audit): an interrupted restore drill must exit nonzero,
		// never read as a clean pass — an integrity check aborted midway proved nothing.
		err = runDrill(args)
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

	outbox := db.NewOutboxRepo(alkemioPool, cfg.MaxAttempts, cfg.MaxDeliveries).WithReadTimeout(cfg.DBTimeout())
	ledger := db.NewLedgerRepo(ledgerPool).WithReadTimeout(cfg.DBTimeout())
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
	immSampler := domain.NewImmutabilitySampler(targets, mx)
	var bgWG sync.WaitGroup
	bgWG.Add(4)
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
	// WORM immutability drift gauge (T032): coarse cadence — object-lock config rarely changes,
	// and each pass is an S3 GET per Worm target. It drives filebackup_immutability_ok ONLY for
	// targets it can actually read; a read-denying (PutObject-only) WORM copy is unverifiable and
	// emits no series (so the `== 0` alert can't false-fire). A panic is logged, never fatal.
	go func() {
		defer bgWG.Done()
		domain.TickLoop(ctx, 15*time.Minute, 2*time.Minute, immSampler.Sample, func(cause any, isPanic bool) {
			// SampleError on ANY failed pass (a returned Fault OR a panic in the sampler's own body),
			// same as the RPO/coverage samplers — so a frozen stale-green immutability_ok gauge is
			// detectable via filebackup_metrics_sample_errors_total, not only via a log line.
			mx.SampleError()
			if isPanic {
				logger.Warn("immutability sampler panicked", zap.Any("panic", cause))
			} else if err, ok := cause.(error); ok {
				// A Fault the sampler returns is a recovered driver panic in the drift-check — logged
				// distinctly (the gauge already dropped stale-green + raised the unverifiable signal).
				logger.Warn("immutability drift-check fault", zap.Error(err))
			}
		})
	}()
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

// sourceOp is the shared scaffold for the single-object DR subcommands (restore object / verify):
// parse --config/--hash/--from (plus any extra flags via register), build ONLY the resolved source
// sink, and run op under a signal-cancellable, per-object-bounded context.
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
	// Load config + build ONLY the resolved read source (the --from, or the first readable) — so an
	// unrelated misconfigured target can't block a restore/verify from a healthy one (Pillar 4c). Same
	// shape as runRestoreAll/runRestoreCurrent/runDrill.
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	sink, _, err := buildReadSource(cfg, *from)
	if err != nil {
		return err
	}
	ctx, stop := signalContext()
	defer stop()
	ctx, cancel := boundedRestoreCtx(ctx, cfg)
	defer cancel()
	return op(ctx, sink, *hash)
}

// buildReadSource validates + builds ONLY the chosen read target from an already-loaded config: the
// set-wide env-token collision guard (so a sibling can't have injected the chosen target's secret)
// plus the CHOSEN target's own field validation, then that single sink. An unrelated target's
// misconfig is never touched (Pillar 4c). A WORM source (allowed only as an explicit --from, Pillar
// 4b) is wrapped so a read-deny surfaces an actionable hint instead of a raw 403.
func buildReadSource(cfg *config.Config, from string) (domain.Sink, string, error) {
	if err := cfg.CheckTargetCollisions(); err != nil {
		return nil, "", fmt.Errorf("invalid config: %w", err)
	}
	ct, err := config.SelectReadTarget(cfg.Targets, from)
	if err != nil {
		return nil, "", err
	}
	if err := config.ValidateTargetFields(ct); err != nil {
		return nil, "", fmt.Errorf("invalid config: %w", err)
	}
	sink, err := config.BuildSink(ct)
	if err != nil {
		return nil, "", err
	}
	// Wrap ONLY a genuinely WRITE-ONLY worm source (a worm target with NO audit/read credential) in the
	// credential-hint wrapper: there a read failure IS the PutObject-only-credential case the hint
	// addresses. A worm target WITH an audit credential is read-capable (readClient uses the audit
	// cred), so a read failure is transient/real — the "supply a read-capable credential" hint would
	// MISDIRECT the operator — so it gets the raw sink, like any readable target.
	if ct.Worm && ct.AuditAccessKey == "" {
		return wormReadSource{Sink: sink}, ct.Name, nil
	}
	return sink, ct.Name, nil
}

// wormReadSource wraps a WRITE-ONLY worm target (no audit credential) chosen EXPLICITLY as a restore/
// verify/drill source (Pillar 4b): the read is ATTEMPTED (restoring from the sole surviving immutable
// copy must not be refused up front), but a read failure — typically a 403 on the PutObject-only worker
// credential — is annotated with an actionable hint, since reading an immutable copy needs a read-capable
// credential. It embeds domain.Sink, so the target name is the promoted Sink.Name() (no separate field).
type wormReadSource struct {
	domain.Sink
}

// Fetch attempts the read and, on failure, annotates it with the WORM-source recovery hint. The
// annotation must cover BOTH failure timings: a filesystem sink's os.Open fails EAGERLY here, but the
// s3 sink's GetObject is LAZY — its 403 surfaces on the first Read (inside the decode path), not from
// Fetch — so on the s3 sink (the real off-site WORM case) an un-wrapped rc would reach the decoder as a
// bare "peek magic: …AccessDenied", stripping the actionable hint at the exact moment DR needs it. So
// wrap the returned reader too (wormErrReader) to annotate a read-time failure identically.
func (w wormReadSource) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	rc, err := w.Sink.Fetch(ctx, hash)
	if err != nil {
		return nil, w.annotate(err)
	}
	return &wormErrReader{rc: rc, annotate: w.annotate}, nil
}

// annotate wraps a WORM-source read failure with the recovery hint (its worker credential is
// PutObject-only by design; restore from a readable target, or supply a read-capable admin credential).
func (w wormReadSource) annotate(err error) error {
	return fmt.Errorf("reading WORM/write-only target %q failed (its worker credential is PutObject-only by design; restore from a readable target, or supply the immutable copy's read-capable admin credential): %w", w.Name(), err)
}

// wormErrReader annotates a LATE (read-time) failure from a lazy WORM source with the same recovery hint
// wormReadSource.Fetch applies to an eager one — so a deferred s3 403 surfaces to the operator with
// guidance, not as a bare decode error. Bytes and io.EOF pass through untouched; only a non-EOF read
// error is wrapped.
type wormErrReader struct {
	rc       io.ReadCloser
	annotate func(error) error
}

func (r *wormErrReader) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, r.annotate(err)
	}
	return n, err
}

func (r *wormErrReader) Close() error { return r.rc.Close() }

// boundedRestoreCtx caps one DR object op with perObjectTimeout — like every other path
// (serve/backfill/reconcile all bound one object): a black-holing sink that accepts the
// connection but never returns bytes must fail the operator's command on the deadline, not hang
// forever (only SIGINT would otherwise stop it). It floors the bound through the SHARED
// domain.NormalizePerObjectTimeout (no private copy of the 30m default), so a non-positive OR
// overflow-degraded-to-0 perObjectTimeout — which the target-only DR validation doesn't range-check
// — falls back to the default rather than an instant-deadline op. One owner, shared by sourceOp
// (restore object/verify) and the restore-current DB path.
func boundedRestoreCtx(ctx context.Context, cfg *config.Config) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, domain.NormalizePerObjectTimeout(cfg.PerObjectTimeout()))
}

// runRestore dispatches the restore sub-verbs (contracts/restore-and-ops.md): `restore all`
// (whole store), `restore current` (a file's current backed-up version, guarded by --at), and
// `restore object` (a single hash). A bare `restore --hash …` (first arg is a FLAG, no sub-verb)
// DEFAULTS to `restore object` — the single-object case is by far the most common restore, and it is
// the form the quickstart documents. But a first arg that is a WORD (not a flag) and not a known verb
// is an unknown subcommand and FAILS LOUD — so `restore version …` (the pre-release name for
// `restore current`) or a typo can't SILENTLY fall through to a bare-hash object restore and do
// something surprising.
func runRestore(args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "all":
			return runRestoreAll(args[1:])
		case "current":
			return runRestoreCurrent(args[1:])
		case "object":
			return runRestoreObject(args[1:])
		}
		// A non-flag first arg that isn't a known verb is an unknown subcommand — fail loud rather than
		// silently treating the word as a bare-hash `restore object` (which would ignore it). A bare
		// `restore --hash …` has a FLAG first arg (leading '-'), so it still falls through to the alias.
		if len(args[0]) > 0 && args[0][0] != '-' {
			return fmt.Errorf("unknown restore subcommand %q (want: all | current | object | a bare --hash; `restore current` replaced the old `restore version`; see contracts/restore-and-ops.md)", args[0])
		}
	}
	return runRestoreObject(args)
}

// resolveConcurrency picks a DR sweep's parallelism: the operator's --concurrency flag when positive,
// else the configured Concurrency. The ONE owner of the "0 = the configured concurrency" default, shared
// by restore-all and drill so they can't diverge on how --concurrency 0 resolves.
func resolveConcurrency(flag, configured int) int {
	if flag <= 0 {
		return configured
	}
	return flag
}

// runRestoreObject restores a single object by hash from one target (`restore [object] --hash
// --from [--to]`) — the single-object DR path.
func runRestoreObject(args []string) error {
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

// runRestoreAll restores every object the ledger records stored on the source target to --to,
// resumable + idempotent (RestoreObject skips an already-present, intact object) and keyset-paged
// (no held snapshot), reusing the single-object restore path. Needs only the ledger DB + the source
// target (built alone — an unrelated misconfigured target can't block it, Pillar 4c). A clean SIGTERM
// mid-restore is exit 0 (the pass resumes on re-run); genuine per-object failures exit nonzero.
func runRestoreAll(args []string) error {
	fs := flag.NewFlagSet("restore all", flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	from := fs.String("from", "", "source target name (default: the first readable target)")
	to := fs.String("to", "/storage", "destination directory")
	concurrency := fs.Int("concurrency", 0, "parallel restores (0 = the configured concurrency)")
	_ = fs.Parse(args)
	ctx, stop := signalContext()
	defer stop()
	cfg, ledger, pool, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	src, name, err := buildReadSource(cfg, *from)
	if err != nil {
		return err
	}
	// Print the per-target stored-set sizes FIRST so an operator sees cross-target disparity before
	// trusting a single-`--from` restore (`restore all` restores only what the SOURCE holds; a smaller
	// source misses objects a fuller target has, which reconcile/backfill would refill). It is a single
	// boundRead-bounded count(*) over the LOCAL ledger — fast in practice, capped at cfg.DBTimeout(), and
	// best-effort (a slow/failed count warns, never blocks the restore) — so it runs synchronously here:
	// giving the operator the disparity (esp. a 0-object source → likely wrong --from) UPFRONT is worth a
	// bounded local query before a restore that runs for minutes/hours.
	printTargetStoredSizes(ctx, ledger, cfg.Targets, name)
	conc := resolveConcurrency(*concurrency, cfg.Concurrency)
	st, rerr := domain.RestoreAll(ctx, ledger, src, name, *to, conc, cfg.PerObjectTimeout())
	fmt.Printf("restore all (%s -> %s): restored=%d skipped=%d failed=%d cancelled=%d\n", name, *to, st.Restored, st.Skipped, st.Failed, st.Cancelled)
	return restoreAllVerdict(st, rerr, name)
}

// printTargetStoredSizes prints each configured target's stored-object count (marking the restore
// source), so an operator can spot a source that holds fewer objects than a sibling before trusting
// a single-source `restore all`. It reads the counts by the CONFIG target names (no sink built for a
// target the restore doesn't use). Best-effort: a count error is warned, not fatal.
func printTargetStoredSizes(ctx context.Context, ledger *db.LedgerRepo, targets []config.Target, source string) {
	names := make([]string, len(targets))
	for i, t := range targets {
		names[i] = t.Name
	}
	counts, err := ledger.StoredCountByTarget(ctx, names)
	if err != nil {
		fmt.Fprintln(os.Stderr, "warning: could not read per-target stored counts:", err)
		return
	}
	for _, n := range names {
		marker := ""
		if n == source {
			marker = "  <- restore source"
		}
		fmt.Printf("target %s: %d objects stored%s\n", n, counts[n], marker)
	}
}

// restoreAllVerdict maps a restore-all outcome to an exit error — extracted so the SIGTERM policy
// is unit-testable. GENUINE per-object failures (st.Failed — a source read/verify error, a
// per-object timeout, or a recovered panic; cancellations are separated into st.Cancelled by
// RestoreAll) exit NONZERO even when a SIGTERM coincides. A run that ENUMERATED zero objects on a
// clean pass also exits nonzero (a restore-all that restored nothing is worth flagging) — but with an
// UNAMBIGUOUS message that names the source target and says the LEDGER holds 0 objects for it (a
// new/empty target, or the wrong --from among the configured targets — the --from name was already
// validated, so this is NOT a data-fault/corruption exit and the per-target stored counts printed
// above disambiguate). A 0-restored run with objects merely SKIPPED (an idempotent re-run) stays
// success. Otherwise an enumeration error maps through onShutdownOK (a clean SIGTERM → nil) and a
// clean run exits 0.
func restoreAllVerdict(st domain.RestoreAllStats, rerr error, source string) error {
	if st.Failed > 0 {
		return fmt.Errorf("restore all left %d object(s) unrestored (a source read/verify failed, timed out, or panicked)", st.Failed)
	}
	if rerr == nil && st.Restored+st.Skipped+st.Cancelled == 0 { // st.Failed is 0 here (early return above)
		return fmt.Errorf("restore all: the ledger records 0 objects stored on target %q — nothing to restore (a new/empty target, or the wrong --from among the configured targets; see the per-target stored counts above)", source)
	}
	return onShutdownOK(rerr)
}

// backfillVerdict maps a backfill outcome to an exit error — extracted so the SIGTERM policy is
// unit-testable (like restoreAllVerdict). A GENUINE per-object failure (st.Failed) is checked FIRST, so
// a mid-corpus SIGTERM (which cancels EachFile → sweepErr=Canceled) can't mask a real failure as a clean
// exit 0: the returned failure error is NOT context.Canceled, so the dispatch's onShutdownOK wrapper
// won't rescue it to exit 0. A benign tail-drain cancel lands in st.Cancelled (NOT st.Failed), so it
// falls through; a pure cancellation returns sweepErr (Canceled) for dispatch's onShutdownOK to map to
// exit 0 (the pass is resumable); a real sweep/DB error returns nonzero. Skipped/Deferred are benign.
func backfillVerdict(st domain.BackfillStats, sweepErr error) error {
	if st.Failed > 0 {
		return fmt.Errorf("backfill left %d object(s) not fully backed up", st.Failed)
	}
	// Skipped/Deferred/Cancelled are benign for the exit code (routine source-gone deletions do NOT
	// fail a pass). "All-skipped" stays exit 0 because it is genuinely ambiguous — a tiny all-deleted
	// corpus and a drain-window SIGTERM look the same as a mass outage — and an exit-code guard for
	// it kept mis-firing on those benign edges. Preflight narrows but does NOT eliminate the bad
	// case: it proves the /blob ROUTE is present (an invalid-key probe is rejected 400 BEFORE
	// file-service touches storage), so it catches a route-miss / wrong endpoint AT STARTUP — but,
	// being one-shot, not a mid-run route-miss nor a route-healthy-but-storage-wiped file-service
	// that 404s every real hash. For backfill that
	// residual is NOT alerted: backfill is a one-shot Job with Nop metrics and no scrape endpoint,
	// and it routes ErrSourceGone to st.Skipped (never the source-gone counter — that path exists
	// only in the long-running serve consumer, where FileBackupSourceGoneSpike does fire). So a
	// backfill's ONLY signal for "protected nothing" is the printed backed/skipped counts — an
	// operator running a recovery MUST read them; a clean exit is not proof anything was backed up.
	return sweepErr
}

// reconcileVerdict maps a reconcile outcome to an exit error — same SIGTERM policy as backfillVerdict.
// st.Failed (a target was down, retryable) OR st.Skipped (the object is on NO current target — near-total
// loss, needs a primary-store backfill) is a genuine could-not-fully-protect outcome, checked BEFORE the
// cancellation escape so a mid-corpus SIGTERM can't mask it; a benign tail-drain cancel is in st.Cancelled.
func reconcileVerdict(st domain.ReconcileStats, sweepErr error) error {
	if st.Failed > 0 || st.Skipped > 0 {
		return fmt.Errorf("reconcile could not fully protect %d object(s): %d unrepaired (target down), %d on NO target (need a primary-store backfill)",
			st.Failed+st.Skipped, st.Failed, st.Skipped)
	}
	return sweepErr
}

// runRestoreCurrent restores a file's CURRENT backed-up version, GUARDED by --at (`restore current
// --file-id --at`). The live `file` table holds only the current version — there is NO version
// history — so this deliberately does NOT promise "restore the historical version at --at". Instead
// it FAILS LOUD unless the current version was already the one in effect at --at:
//   - --hash <externalID>: restore that hash directly — the reliable escape hatch when the operator
//     has recovered the historical file.externalID from a DB PITR/backup (targets-only, no DB needed).
//   - else: resolve the file's CURRENT hash from the alkemio `file` table by its last-modified time.
//     If the current version was live at/before --at it IS the version as of --at (restore it); if it
//     was replaced AFTER --at, or its version time is unknowable, error — directing the operator to
//     the DB-PITR + --hash procedure in contracts/restore-and-ops.md.
func runRestoreCurrent(args []string) error {
	fs := flag.NewFlagSet("restore current", flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	fileID := fs.String("file-id", "", "file uuid to restore the current version of")
	at := fs.String("at", "", "RFC3339 time the restored version must have been live at (fail loud if the current version is newer)")
	hashOverride := fs.String("hash", "", "explicit content hash (recovered via DB PITR) — restore it directly, skipping the file-table lookup")
	from := fs.String("from", "", "source target name (default: the first readable target)")
	to := fs.String("to", "/storage", "destination directory")
	_ = fs.Parse(args)
	if *fileID == "" || *at == "" {
		return errors.New("restore current requires --file-id and --at")
	}
	fid, ferr := uuid.Parse(*fileID)
	if ferr != nil {
		return fmt.Errorf("invalid --file-id (want a uuid): %w", ferr)
	}
	atTime, aerr := time.Parse(time.RFC3339, *at)
	if aerr != nil {
		return fmt.Errorf("invalid --at (want RFC3339, e.g. 2026-07-01T00:00:00Z): %w", aerr)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	src, srcName, err := buildReadSource(cfg, *from)
	if err != nil {
		return err
	}
	ctx, stop := signalContext()
	defer stop()

	hash := *hashOverride
	if hash == "" {
		hash, err = resolveCurrentHash(ctx, cfg, fid, atTime)
		if err != nil {
			return err
		}
	}
	rctx, cancel := boundedRestoreCtx(ctx, cfg)
	defer cancel()
	if err := domain.RestoreObject(rctx, src, hash, *to); err != nil {
		return err
	}
	// Only the resolved-from-DB path VERIFIED the current version was in effect at --at (updatedDate
	// <= --at). With an operator-supplied --hash we restored exactly that content and verified NOTHING
	// about its version/time, so make NO "current version in effect at --at" provenance claim.
	if *hashOverride != "" {
		fmt.Printf("restored operator-supplied hash %s for file %s from %s -> %s/%s (version provenance is the operator's PITR recovery, not verified here)\n", hash, fid, srcName, *to, hash)
	} else {
		fmt.Printf("restored current version of %s (verified in effect at %s) from %s -> %s/%s\n", fid, atTime.Format(time.RFC3339), srcName, *to, hash)
	}
	return nil
}

// resolveCurrentHash maps (file-id, at) to the content hash to restore, using the live `file` table
// (which holds only the current version). It needs the alkemio DB, so it validates + opens it here
// (and preflights the columns via FileRepo.Probe so a missing `updatedDate` fails loud up front, not
// mid-DR). It keys on the SAFE guard — `file.updatedDate` (when the current version became current):
// it returns the current hash ONLY when the file has NOT been modified since --at (updatedDate <=
// --at → the current version was in effect at --at). A modification since --at (updatedDate > --at) —
// which INCLUDES a metadata-only edit — is a deliberate SAFE OVER-REFUSAL (content-version history is
// out of scope; the ledger's first-seen time is unsafe because externalIDs are content hashes and a
// hash can RECYCLE A→B→A, so first-seen ≠ the current version's became-current time). It NEVER
// returns a wrong version; the operator recovers a historical version via a DB PITR + --hash.
func resolveCurrentHash(ctx context.Context, cfg *config.Config, fid uuid.UUID, at time.Time) (string, error) {
	if err := cfg.AlkemioDB.Validate("alkemioDB"); err != nil {
		return "", fmt.Errorf("invalid config (resolving --file-id needs alkemioDB; or pass --hash from a PITR query): %w", err)
	}
	pool, err := openPool(ctx, cfg.AlkemioDB.DSN(), cfg.PoolSize(1), cfg.DBTimeout(), "alkemio")
	if err != nil {
		return "", err
	}
	defer pool.Close()
	files := db.NewFileRepo(pool).WithReadTimeout(cfg.DBTimeout())
	// Preflight the columns FileByID reads (id/externalID/updatedDate) so a schema drift or a missing
	// updatedDate SELECT grant fails loud up front, not mid-DR on FileByID's Scan. ProbeCurrentVersion
	// (not the corpus Probe) so restore-current's updatedDate need doesn't gate backfill.
	if err := files.ProbeCurrentVersion(ctx); err != nil {
		return "", err
	}
	hash, versionTime, found, err := files.FileByID(ctx, fid)
	if err != nil {
		return "", fmt.Errorf("resolve file %s: %w", fid, err)
	}
	if !found {
		return "", fmt.Errorf("file %s has no current content hash in the live file table — recover its externalID as of %s from a DB PITR/backup and pass it via --hash (see contracts/restore-and-ops.md)", fid, at.Format(time.RFC3339))
	}
	// FAIL LOUD when the version time is unknowable: a NULL updatedDate means we cannot prove the
	// current version was the one in effect at --at, so we MUST NOT guess. Direct the operator to PITR.
	if versionTime.IsZero() {
		return "", fmt.Errorf("file %s has a NULL updatedDate — cannot determine whether it was modified after %s; recover file.externalID as of --at from a DB PITR/backup and pass it via --hash (see contracts/restore-and-ops.md)",
			fid, at.Format(time.RFC3339))
	}
	if versionTime.After(at) {
		return "", fmt.Errorf("file %s was modified at %s, AFTER --at %s — cannot prove the current version was in effect at --at (a since-replacement OR a metadata-only edit); recover file.externalID as of --at from a DB PITR/backup and pass it via --hash (see contracts/restore-and-ops.md)",
			fid, versionTime.Format(time.RFC3339), at.Format(time.RFC3339))
	}
	return hash, nil
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

// ledgerJob opens config for a ledger-DB-only subcommand (reconcile/audit/restore-all/drill): it
// validates the DR config (not the full serve config — these run in the DR state without
// file-service / the outbox DB) and opens + probes the ledger pool. It deliberately does NOT build
// any sinks — the caller builds only the targets IT needs (all of them for audit/reconcile, just the
// source for restore-all/drill — Pillar 4c). The caller MUST close the returned pool.
func ledgerJob(ctx context.Context, cfgPath string) (*config.Config, *db.LedgerRepo, *db.Pool, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load config: %w", err)
	}
	// Validate only the DR limits + the ledger DSN here — NOT the target SET: the single-source ops
	// (restore-all/drill) validate only their one source target (buildReadSource), so a sibling
	// misconfig can't block them (Pillar 4c). Audit/reconcile add cfg.ValidateTargets() themselves.
	if err := cfg.ValidateDRLimits(); err != nil {
		return nil, nil, nil, fmt.Errorf("invalid config: %w", err)
	}
	pool, err := openPool(ctx, cfg.LedgerDB.DSN(), cfg.PoolSize(4), cfg.DBTimeout(), "ledger")
	if err != nil {
		return nil, nil, nil, err
	}
	ledger := db.NewLedgerRepo(pool).WithReadTimeout(cfg.DBTimeout())
	if err := ledger.Probe(ctx); err != nil {
		pool.Close()
		return nil, nil, nil, fmt.Errorf("ledger not accessible (schema / migrate?): %w", err)
	}
	return cfg, ledger, pool, nil
}

// runReconcile repairs under-replicated objects target-to-target (FR-025/T029): for
// each object the ledger shows not stored on every target, fetch it from a target that
// has it and re-fan-out to the missing ones. Needs the ledger DB + EVERY target.
func runReconcile(args []string) error {
	fs := flag.NewFlagSet("reconcile", flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	ratePerSec := fs.Int("rate", 0, "max repairs per second (0 = unlimited)")
	_ = fs.Parse(args)
	ctx, stop := signalContext()
	defer stop()
	cfg, ledger, pool, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	// reconcile repairs across EVERY target, so validate + build the whole set (ledgerJob validated only
	// the limits + ledger DSN).
	targets, err := validateAndBuildTargets(cfg)
	if err != nil {
		return err
	}
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
	fmt.Printf("reconcile: repaired=%d skipped=%d failed=%d cancelled=%d\n", st.Repaired, st.Skipped, st.Failed, st.Cancelled)
	return reconcileVerdict(st, err)
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

	ledger := db.NewLedgerRepo(ledgerPool).WithReadTimeout(cfg.DBTimeout())
	files := db.NewFileRepo(alkemioPool).WithReadTimeout(cfg.DBTimeout())
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
	fmt.Printf("backfill: backed=%d skipped=%d deferred=%d failed=%d cancelled=%d\n", st.Backed, st.Skipped, st.Deferred, st.Failed, st.Cancelled)
	return backfillVerdict(st, err)
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
	inventory := fs.Bool("inventory", false, "also run the target→ledger direction: diff each target's own manifest against the ledger (detects orphans / lost ledger records)")
	_ = fs.Parse(args)
	ctx, stop := signalContext()
	defer stop()
	cfg, ledger, pool, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	// audit probes EVERY target, so validate + build the whole set (ledgerJob validated only the limits
	// + ledger DSN).
	targets, err := validateAndBuildTargets(cfg)
	if err != nil {
		return err
	}
	// The audit VERDICT combines every direction so a single nonzero exit alerts cron/CI, and every
	// direction runs on the SHARED verdict model + probe engine: ledger→target (silent loss), the
	// WORM immutability drift-check, and (opt-in) target→ledger inventory. Each direction is printed
	// + reduced to its ONE shared FailErr; they are errors.Join'd — NO early-return — so a
	// corrupt-manifest fault or drift on ONE target/direction can't MASK a finding on another.
	verdicts := make([]error, 0, 4) // 3 directions + the top-level ctx.Err() fold
	verdicts = append(verdicts,
		printAudit("audit", domain.Audit(ctx, ledger, targets, *sample)),
		printAudit("immutability", domain.CheckImmutability(ctx, targets)))
	if *inventory {
		verdicts = append(verdicts, printAudit("inventory", domain.AuditInventory(ctx, ledger, targets)))
	}
	// A SIGTERM mid-audit must exit NONZERO (audit is deliberately NOT shutdown-wrapped — an aborted
	// integrity check proved nothing): the per-direction probes map a parent cancellation to a benign
	// NoData verdict, so fold ctx.Err() in so a cancelled run can't read as a clean pass.
	verdicts = append(verdicts, ctx.Err())
	return errors.Join(verdicts...)
}

// printAudit prints one direction's per-target verdicts (label prefixes each line) and returns the
// direction's ONE shared FailErr for the caller to join. Every direction shares this — the printed
// shape and the pass/fail reduction can't diverge between them.
func printAudit(label string, rep domain.VerdictReport) error {
	for _, v := range rep.Targets {
		fmt.Printf("%s %s: %s — %s\n", label, v.Target, v.Status, v.Detail)
	}
	return rep.FailErr()
}

// runDrill runs a restore drill (T033/FR-024/SC-009): it samples random objects the ledger
// records stored on the source target, restores each to a scratch dir (proving the end-to-end
// restore PROCEDURE, not just byte existence), verifies the bytes hash to their key, and exits
// nonzero if any fails — so the (suspended) infra-ops drill CronJob's failing Job trips the
// kube_job_status_failed alert, exactly like the audit job. It also records the drill gauges and,
// when --metrics-file (or FBS_DRILL_METRICS_FILE) is set, exports them as a Prometheus textfile
// (a short-lived CronJob can't be scraped). Needs the ledger DB + only the SOURCE target (built
// alone — Pillar 4c). On a non-clean sweep — a SIGTERM interruption OR an enumeration/infra fault
// (e.g. a wedged-ledger per-page timeout) — it writes NO gauges at all (Pillar 6): the drill proved
// neither pass nor fail, so it must not clobber the textfile with a RED pass=0 or reset the prior
// last_success — either would otherwise page a week-long false failure.
func runDrill(args []string) error {
	fs := flag.NewFlagSet("drill", flag.ExitOnError)
	cfgPath := registerConfigFlag(fs)
	from := fs.String("from", "", "source target to drill (default: the first readable target)")
	sample := fs.Int("sample", 20, "objects to restore-verify (0 = all — a full-store drill)")
	concurrency := fs.Int("concurrency", 0, "parallel restore-verifies (0 = the configured concurrency); live scratch stays bounded to this many objects")
	to := fs.String("to", "", "scratch directory for the drill (default: scratchDir, else the OS temp dir)")
	metricsFile := fs.String("metrics-file", os.Getenv("FBS_DRILL_METRICS_FILE"), "write the drill gauges to this Prometheus textfile (node-exporter textfile-collector)")
	_ = fs.Parse(args)
	ctx, stop := signalContext()
	defer stop()
	cfg, ledger, pool, err := ledgerJob(ctx, *cfgPath)
	if err != nil {
		return err
	}
	defer pool.Close()
	src, name, err := buildReadSource(cfg, *from)
	if err != nil {
		return err
	}
	logger, syncLog, err := newLogger()
	if err != nil {
		return err
	}
	defer syncLog()
	// Default the scratch base to the configured scratchDir (as reconcile does), falling back to the
	// OS temp dir only when neither --to nor scratchDir is set. Preflight it the SAME way reconcile
	// does (writability + the tmpfs-OOM warning) so an unwritable/memory-backed scratch fails LOUD
	// up front instead of failing every drilled object mid-pass.
	base := *to
	if base == "" {
		base = cfg.ScratchDir
	}
	if err := preflightScratch(base, logger); err != nil {
		return err
	}
	// Stage restored objects in an isolated per-run subdir so the whole drill's scratch is removed
	// wholesale on exit (Drill also removes each object as it verifies, bounding live disk to one).
	dir, err := os.MkdirTemp(base, "drill-")
	if err != nil {
		return fmt.Errorf("create drill scratch dir (set --to / scratchDir to a writable, sized volume): %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	conc := resolveConcurrency(*concurrency, cfg.Concurrency)
	outcome, derr := domain.Drill(ctx, ledger, src, name, dir, *sample, conc, cfg.PerObjectTimeout())
	for _, f := range outcome.Failures {
		fmt.Printf("drill FAIL %s: %v\n", f.Hash, f.Err)
	}
	fmt.Printf("drill %s: checked=%d passed=%d failed=%d\n", name, outcome.Checked(), outcome.Passed, outcome.Failed)
	return drillReport(outcome, derr, *metricsFile, name)
}

// drillReport records + exports the drill gauges and maps the outcome to an exit error — extracted
// so the interruption invariant is unit-testable. On ANY non-nil derr — a SIGTERM cancellation OR an
// enumeration/infra fault (e.g. a wedged-ledger per-page timeout) — the drill did NOT complete a
// clean sweep, so it proved NEITHER pass nor fail and records NO gauges: writing a red pass=0 would
// page a false week-long failure and clobbering last_success would reset a genuinely-recent success,
// so it leaves the prior textfile untouched and just exits nonzero. Only a CLEAN completion writes the
// gauges (pass iff nothing failed AND >0 sampled; a textfile write failure is a warning, not a drill
// failure — the exit code is the primary signal) and returns: a distinct 0-sampled failure (proved
// nothing — a renamed target / empty ledger), a not-every-object-passed failure, or nil.
func drillReport(outcome domain.DrillOutcome, derr error, metricsFile, name string) error {
	if derr != nil {
		return derr // interrupted OR an enumeration/infra fault — nonzero exit, prior textfile untouched
	}
	dm := metrics.NewDrillMetrics()
	pass := outcome.Pass()
	dm.SetPass(pass, time.Now())
	if werr := dm.WriteTextfile(metricsFile, pass); werr != nil {
		fmt.Fprintln(os.Stderr, "warning: drill metrics textfile:", werr)
	}
	if outcome.Checked() == 0 {
		return fmt.Errorf("restore drill sampled 0 objects on %s — nothing to drill (renamed/misconfigured target, or an empty/wrong ledger?)", name)
	}
	if !outcome.Pass() {
		return fmt.Errorf("restore drill FAILED: %d of %d sampled objects did not restore+verify on %s", outcome.Failed, outcome.Checked(), name)
	}
	return nil
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

// validateAndBuildTargets validates the WHOLE target set then builds every sink — the shared step for
// the every-target DR commands (audit, reconcile), which run ledgerJob (limits + ledger DSN only) and
// must add the full-set validation + build themselves. One owner so the two can't diverge on the
// validation or the error wrapping (mirrors buildReadSource bundling validate+build for the
// single-source DR path).
func validateAndBuildTargets(cfg *config.Config) ([]domain.Target, error) {
	if err := cfg.ValidateTargets(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return buildTargets(cfg.Targets)
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
