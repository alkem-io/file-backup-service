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
func (c *Consumer) poll(ctx context.Context, wake chan<- struct{}) {
	interval := c.d.PollEvery
	if interval <= 0 { // floor it (as reap does) so a non-positive PollEvery can't panic NewTicker
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			signal(wake)
		}
	}
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
		entries, err := c.d.Outbox.Claim(ctx, 1)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return // graceful shutdown cancelled the in-flight claim — not an error
			}
			c.d.Logger.Error("claim outbox", zap.Error(err))
			backoff(ctx) // don't hot-loop a broken DB
			continue
		}
		if len(entries) == 1 {
			signal(wake) // cascade the drain: wake a sibling to grab the next row
			c.process(ctx, entries[0])
			continue // immediately try for the next
		}
		select { // outbox empty — wait for a NOTIFY or the shared poll floor
		case <-ctx.Done():
			return
		case <-wake:
		}
	}
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
	ok, err := c.d.Pipeline.BackupOne(objCtx, e)

	// Bookkeeping MUST survive per-object-timeout and shutdown cancellation, or the
	// row is stranded in_progress. Detach from the cancelled ctx with a fresh deadline.
	bctx, bcancel := domain.DetachedBookkeepingCtx(ctx)
	defer bcancel()

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
	case err != nil:
		// A per-object timeout (objCtx deadline hit while the parent is still live)
		// is a slow/wedged target — surface it as its own metric, not just a failure.
		if c.d.OnObjectTimeout != nil && errors.Is(objCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			c.d.OnObjectTimeout()
		}
		c.d.Logger.Warn("backup failed", zap.Int64("id", e.ID), zap.String("hash", e.ExternalID), zap.Error(err))
		c.fail(bctx, e.ID, err.Error())
	default: // !ok
		c.fail(bctx, e.ID, "not all targets stored")
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
	sweep := func() {
		deadLettered, err := c.d.Outbox.ReapStale(ctx, c.d.StaleTTL)
		if err != nil {
			if !errors.Is(err, context.Canceled) { // shutdown, not a real error
				c.d.Logger.Error("reap stale", zap.Error(err))
			}
			return
		}
		// A crash-loop dead-letter happens HERE (not via Fail), so it must fire the
		// same observer/metric — otherwise the exact case the delivery-count bound
		// exists for is invisible to alerting.
		if deadLettered > 0 {
			c.d.Logger.Error("crash-loop dead-lettered", zap.Int("count", deadLettered))
			if c.d.OnDeadLetter != nil {
				for i := 0; i < deadLettered; i++ {
					c.d.OnDeadLetter()
				}
			}
		}
	}
	interval := c.d.StaleTTL / 4
	if interval < time.Minute {
		interval = time.Minute
	}
	sweep() // startup sweep
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sweep()
		}
	}
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
