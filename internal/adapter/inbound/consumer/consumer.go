// Package consumer drains the backup outbox: Postgres LISTEN/NOTIFY wakeups plus
// a polling floor plus a startup backlog drain, claiming rows with
// FOR UPDATE SKIP LOCKED. See specs/008 FR-005.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// Deps are the consumer's dependencies.
type Deps struct {
	// Outbox claims and completes outbox entries.
	Outbox domain.Outbox
	// Pipeline backs up one object.
	Pipeline *domain.Pipeline
	// ListenPool is used for LISTEN on the Alkemio DB (best-effort wakeups).
	ListenPool *pgxpool.Pool
	// Concurrency is the number of concurrent self-claiming worker goroutines.
	Concurrency int
	// PollEvery is the polling floor.
	PollEvery time.Duration
	// StaleTTL is how long a claimed entry may stay in_progress before the reaper
	// returns it to pending.
	StaleTTL time.Duration
	// PerObjectTimeout bounds a single object's backup so a hung fetch/sink can't
	// pin a worker slot forever.
	PerObjectTimeout time.Duration
	// DBTimeout bounds the otherwise-unbounded outbox Claim and stale-reap queries so a
	// slow/wedged Alkemio DB fails the op (retried after backoff) instead of parking a
	// worker forever with /live still green. 0 = unbounded (direct-construction tests).
	DBTimeout time.Duration
	// OnDeadLetter is called whenever an entry is moved to dead-letter (optional).
	OnDeadLetter func()
	// OnObjectTimeout is called when an object hits the per-object timeout (a
	// slow/wedged target, as distinct from a graceful shutdown) (optional).
	OnObjectTimeout func()
	// OnSourceGone is called when the source object is absent (file-service 404/410)
	// and the entry is skipped — so a mass skip is visible, not a silent drop (optional).
	OnSourceGone func()
	// Logger is the structured logger.
	Logger *zap.Logger
}

// Consumer drains the outbox until its context is cancelled.
type Consumer struct{ d Deps }

// New constructs a Consumer, applying defaults.
func New(d Deps) *Consumer {
	// The numeric knobs are already floored by config.applyDefaults (serve is the only
	// caller), so New does not re-default them — one source of truth. Only the Logger
	// gets a nil-guard, a genuine convenience for direct construction in tests.
	if d.Logger == nil {
		d.Logger = zap.NewNop()
	}
	return &Consumer{d: d}
}

// Run starts a fixed pool of Concurrency self-claiming workers plus the LISTEN and
// reaper goroutines, and blocks until every one returns on ctx cancellation — a
// clean graceful drain with no stranded rows or goroutines. Awaiting listen/reap
// too means the caller's pools aren't closed under a still-running goroutine.
func (c *Consumer) Run(ctx context.Context) error {
	wake := make(chan struct{}, 1)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); c.listen(ctx, wake) }()
	go func() { defer wg.Done(); c.reap(ctx) }()
	go func() { defer wg.Done(); c.poll(ctx, wake) }()
	for i := 0; i < c.d.Concurrency; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.worker(ctx, wake) }()
	}
	wg.Wait()
	return ctx.Err()
}

// poll signals wake every PollEvery so an idle worker re-checks even if a NOTIFY was
// missed — ONE shared ticker feeding the wake cascade, instead of one ticker per
// worker (which fired N empty claims against the shared DB every interval at idle).
// It uses domain.TickLoop (the one ticker-skeleton owner, like reap) rather than
// hand-rolling NewTicker + for/select; the extra immediate wake at startup is a harmless
// coalesced signal (workers already drain the backlog on entry), timeout 0 because
// signal() can't block or fail, and onError nil for the same reason.
func (c *Consumer) poll(ctx context.Context, wake chan<- struct{}) {
	interval := c.d.PollEvery
	if interval <= 0 { // floor it (as reap does) so a non-positive PollEvery can't panic NewTicker
		interval = time.Second
	}
	domain.TickLoop(ctx, interval, 0, func(context.Context) error { signal(wake); return nil }, nil)
}

// worker claims and processes ONE object at a time until ctx is cancelled.
// Claiming per-object (not a Concurrency-sized batch) keeps claimedAt≈process
// start, so the reaper never mistakes a still-queued row for a crashed one, and a
// graceful shutdown strands nothing — a worker only ever holds the single row it
// is actively processing, which process() releases on cancel. On a successful
// claim it wakes a sibling so a NOTIFY burst fans out across all workers instead
// of one worker draining it solo.
func (c *Consumer) worker(ctx context.Context, wake chan struct{}) {
	for ctx.Err() == nil {
		if !c.claimStep(ctx, wake) { // false → outbox empty, wait for a NOTIFY or the poll floor
			select {
			case <-ctx.Done():
				return
			case <-wake:
			}
		}
	}
}

// claimStep claims and processes one row, returning true if it did work (retry immediately)
// or false if the outbox was empty (wait). A panic (e.g. a pgx Scan of a drifted/renamed
// foreign-outbox column) is recovered into a logged error + backoff so it can't crash the
// worker goroutine — process() recovers the pipeline, but Claim itself is outside it.
func (c *Consumer) claimStep(ctx context.Context, wake chan struct{}) (worked bool) {
	defer func() {
		if r := recover(); r != nil {
			c.d.Logger.Error("panic in claim", zap.Any("panic", r), zap.Stack("stack"))
			backoff(ctx)
			worked = true // retry after backoff rather than block on wake
		}
	}()
	cctx, cancel := c.opCtx(ctx) // bound the claim so a wedged DB can't park this worker forever
	defer cancel()
	entries, err := c.d.Outbox.Claim(cctx, 1)
	if err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return true // graceful shutdown cancelled the claim — the ctx.Err() loop guard exits
		}
		// A DBTimeout (DeadlineExceeded) or a genuine DB error both log + back off + retry,
		// rather than the worker hanging in Claim: the pod stays live AND makes visible,
		// retrying progress instead of silently wedging with /live green.
		c.d.Logger.Error("claim outbox", zap.Error(err))
		backoff(ctx) // don't hot-loop a broken DB
		return true
	}
	if len(entries) == 1 {
		signal(wake) // cascade the drain: wake a sibling to grab the next row
		c.process(ctx, entries[0])
		return true
	}
	return false // outbox empty
}

// opCtx bounds a single DB operation with DBTimeout (derived from ctx, so shutdown still
// aborts it); DBTimeout<=0 leaves it unbounded (returns ctx + a no-op cancel). The one
// owner of the claim/reap query-deadline policy.
func (c *Consumer) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.d.DBTimeout > 0 {
		return context.WithTimeout(ctx, c.d.DBTimeout)
	}
	return ctx, func() {}
}

// signal does a non-blocking send — a coalescing wakeup.
func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// listen LISTENs for NOTIFY and signals wake. Best-effort: the polling floor and
// the durable outbox guarantee progress even if notifications are missed.
func (c *Consumer) listen(ctx context.Context, wake chan<- struct{}) {
	if c.d.ListenPool == nil {
		return
	}
	for ctx.Err() == nil {
		conn, err := c.d.ListenPool.Acquire(ctx)
		if err != nil {
			backoff(ctx)
			continue
		}
		if _, err := conn.Exec(ctx, "LISTEN file_backup_outbox"); err != nil {
			conn.Release()
			backoff(ctx)
			continue
		}
		for ctx.Err() == nil {
			if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
				backoff(ctx) // avoid busy re-acquire on a broken notify conn
				break
			}
			signal(wake)
		}
		conn.Release()
	}
}

func (c *Consumer) process(ctx context.Context, e domain.OutboxEntry) {
	// A panic in the pipeline/dispatcher must not crash the whole worker — count
	// it as a failure so a poison object dead-letters instead of crash-looping.
	defer func() {
		if r := recover(); r != nil {
			c.d.Logger.Error("panic backing up object", zap.Int64("id", e.ID),
				zap.String("hash", e.ExternalID), zap.Any("panic", r), zap.Stack("stack"))
			bctx, cancel := domain.DetachedBookkeepingCtx(ctx)
			defer cancel()
			c.fail(bctx, e.ID, fmt.Sprintf("panic: %v", r))
		}
	}()

	objCtx, cancel := context.WithTimeout(ctx, c.d.PerObjectTimeout)
	defer cancel()
	ok, deferred, err := c.d.Pipeline.BackupOne(objCtx, e.BackupItem) // pipeline gets the content-identity, not the outbox ID

	// Bookkeeping MUST survive per-object-timeout and shutdown cancellation, or the
	// row is stranded in_progress. Detach from the cancelled ctx with a fresh deadline.
	bctx, bcancel := domain.DetachedBookkeepingCtx(ctx)
	defer bcancel()
	c.settle(ctx, objCtx, bctx, e, ok, deferred, err)
}

// settle records the outbox outcome of one backup attempt. shutdown (parent ctx
// cancelled) Releases without an attempt; a vanished source Skips; a deferred object
// (only gap is a down/circuit-open target) is re-queued without an attempt (T017a); a
// genuine failure Fails (attempt).
func (c *Consumer) settle(ctx, objCtx, bctx context.Context, e domain.OutboxEntry, ok, deferred bool, err error) {
	switch {
	case ok && err == nil:
		if derr := c.d.Outbox.MarkDone(bctx, e.ID); derr != nil {
			c.d.Logger.Error("mark done", zap.Int64("id", e.ID), zap.Error(derr))
		}
	case ctx.Err() != nil:
		// Shutdown cancelled an incomplete backup — NOT a genuine failure. Release
		// the claim without counting an attempt, so deploy churn doesn't march
		// objects toward dead-letter.
		if rerr := c.d.Outbox.Release(bctx, e.ID); rerr != nil {
			c.d.Logger.Error("release claim on shutdown", zap.Int64("id", e.ID), zap.Error(rerr))
		}
	case errors.Is(err, domain.ErrSourceGone):
		// The source was deleted before we could back it up — benign and terminal.
		// Skip it so it doesn't burn ~10 retries and page on a non-problem. Emit a
		// metric so a MASS skip (e.g. a wrong fileServiceBase 404ing every path) is
		// visible/alertable rather than a silent, invisible drop to zero coverage.
		if c.d.OnSourceGone != nil {
			c.d.OnSourceGone()
		}
		c.d.Logger.Info("source gone, skipping", zap.Int64("id", e.ID), zap.String("hash", e.ExternalID))
		if serr := c.d.Outbox.Skip(bctx, e.ID); serr != nil {
			c.d.Logger.Error("skip vanished source", zap.Int64("id", e.ID), zap.Error(serr))
		}
	case deferred:
		// The object stored on every REACHABLE target; its only gap is a persistently-down
		// (circuit-open) target — NOT a failure of THIS object. Defer it (backoff,
		// re-claimable, NO attempt) so a single-target outage doesn't march the corpus to
		// dead-letter; reconcile refills the gap when the target returns (T017a).
		//
		// A per-object TIMEOUT during a target's finalization can trip that target's circuit
		// and surface HERE as a defer (not an error) — still fire the timeout metric so a
		// wedged target isn't a blind spot just because its circuit tripped on the same object
		// that timed out.
		c.fireObjectTimeout(ctx, objCtx)
		if derr := c.d.Outbox.Defer(bctx, e.ID); derr != nil {
			c.d.Logger.Error("defer down-target object", zap.Int64("id", e.ID), zap.Error(derr))
		}
	case err != nil:
		// A per-object timeout (objCtx deadline hit while the parent is still live)
		// is a slow/wedged target — surface it as its own metric, not just a failure.
		c.fireObjectTimeout(ctx, objCtx)
		c.d.Logger.Warn("backup failed", zap.Int64("id", e.ID), zap.String("hash", e.ExternalID), zap.Error(err))
		c.fail(bctx, e.ID, err.Error())
	default: // !ok
		c.fail(bctx, e.ID, "not all targets stored")
	}
}

// fireObjectTimeout fires the per-object-timeout observer iff THIS object hit its own
// deadline (objCtx DeadlineExceeded) rather than a graceful shutdown (parent ctx live).
// Shared by the failed branch and the deferred-with-tripped-circuit branch — both of which
// can be the surface of a per-object timeout — so the "was it a timeout" rule lives once.
func (c *Consumer) fireObjectTimeout(ctx, objCtx context.Context) {
	if c.d.OnObjectTimeout != nil && errors.Is(objCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		c.d.OnObjectTimeout()
	}
}

// fail marks an entry failed and fires the dead-letter observer when it crosses
// the attempt limit.
func (c *Consumer) fail(ctx context.Context, id int64, reason string) {
	deadLettered, err := c.d.Outbox.Fail(ctx, id, reason)
	if err != nil {
		c.d.Logger.Error("mark fail", zap.Int64("id", id), zap.Error(err))
		return
	}
	if deadLettered {
		c.d.Logger.Error("dead-lettered", zap.Int64("id", id), zap.String("reason", reason))
		if c.d.OnDeadLetter != nil {
			c.d.OnDeadLetter()
		}
	}
}

// reap sweeps stale in_progress entries back to pending: once immediately at
// startup (so a crashed worker's rows don't wait a full interval to recover),
// then on an interval SHORTER than StaleTTL so a stuck row is caught within
// ~StaleTTL rather than up to 2×.
func (c *Consumer) reap(ctx context.Context) {
	interval := c.d.StaleTTL / 4
	if interval < time.Minute {
		interval = time.Minute
	}
	// domain.TickLoop owns the ticker + startup sweep + panic recover; DBTimeout bounds each
	// sweep so a wedged Alkemio DB can't park the reaper indefinitely (shutdown still aborts it
	// via the parent ctx). A panic (a pgx Scan on a drifted foreign-outbox column), a returned
	// error, and a per-tick timeout all route to onError, so the reaper degrades to a logged
	// error, never a crashed worker (this goroutine is outside process()'s recover).
	domain.TickLoop(ctx, interval, c.d.DBTimeout,
		func(fctx context.Context) error {
			deadLettered, err := c.d.Outbox.ReapStale(fctx, c.d.StaleTTL)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil // shutdown, not a real error
				}
				return err
			}
			// A crash-loop dead-letter happens HERE (not via Fail), so it fires the same
			// observer/metric — else the exact case the delivery-count bound exists for is
			// invisible to alerting.
			if deadLettered > 0 {
				c.d.Logger.Error("crash-loop dead-lettered", zap.Int("count", deadLettered))
				if c.d.OnDeadLetter != nil {
					for i := 0; i < deadLettered; i++ {
						c.d.OnDeadLetter()
					}
				}
			}
			return nil
		},
		func(cause any, isPanic bool) {
			if isPanic {
				c.d.Logger.Error("panic in reaper", zap.Any("panic", cause), zap.Stack("stack"))
			} else if err, ok := cause.(error); ok {
				c.d.Logger.Error("reap stale", zap.Error(err))
			}
		})
}

// backoff waits ~1s or until ctx is cancelled — avoids hot-looping a transient
// DB / notify error.
func backoff(ctx context.Context) {
	t := time.NewTimer(time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
