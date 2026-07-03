// Package consumer drains the backup outbox: Postgres LISTEN/NOTIFY wakeups plus
// a polling floor plus a startup backlog drain, claiming rows with
// FOR UPDATE SKIP LOCKED. See specs/008 FR-005.
package consumer

import (
	"context"
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
	// Concurrency is the number of in-flight objects per drain batch.
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
	// Logger is the structured logger.
	Logger *zap.Logger
}

// Consumer drains the outbox until its context is cancelled.
type Consumer struct{ d Deps }

// New constructs a Consumer, applying defaults.
func New(d Deps) *Consumer {
	if d.Concurrency <= 0 {
		d.Concurrency = 8
	}
	if d.PollEvery <= 0 {
		d.PollEvery = 10 * time.Second
	}
	if d.StaleTTL <= 0 {
		d.StaleTTL = time.Hour
	}
	if d.PerObjectTimeout <= 0 {
		d.PerObjectTimeout = 30 * time.Minute
	}
	if d.Logger == nil {
		d.Logger = zap.NewNop()
	}
	return &Consumer{d: d}
}

// Run drains at startup, then on NOTIFY wakeups and a polling floor.
func (c *Consumer) Run(ctx context.Context) error {
	wake := make(chan struct{}, 1)
	go c.listen(ctx, wake)
	go c.reap(ctx)

	c.drain(ctx) // startup backlog drain
	ticker := time.NewTicker(c.d.PollEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.drain(ctx)
		case <-wake:
			c.drain(ctx)
		}
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
			sleep(ctx, time.Second)
			continue
		}
		if _, err := conn.Exec(ctx, "LISTEN file_backup_outbox"); err != nil {
			conn.Release()
			sleep(ctx, time.Second)
			continue
		}
		for ctx.Err() == nil {
			if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
				sleep(ctx, time.Second) // avoid busy re-acquire on a broken notify conn
				break
			}
			select {
			case wake <- struct{}{}:
			default:
			}
		}
		conn.Release()
	}
}

// drain claims and processes batches until the outbox is empty.
func (c *Consumer) drain(ctx context.Context) {
	for ctx.Err() == nil {
		entries, err := c.d.Outbox.Claim(ctx, c.d.Concurrency)
		if err != nil {
			c.d.Logger.Error("claim outbox", zap.Error(err))
			return
		}
		if len(entries) == 0 {
			return
		}
		var wg sync.WaitGroup
		for _, e := range entries {
			wg.Add(1)
			go func(e domain.OutboxEntry) {
				defer wg.Done()
				c.process(ctx, e)
			}(e)
		}
		wg.Wait()
	}
}

func (c *Consumer) process(ctx context.Context, e domain.OutboxEntry) {
	objCtx, cancel := context.WithTimeout(ctx, c.d.PerObjectTimeout)
	defer cancel()
	ok, err := c.d.Pipeline.BackupOne(objCtx, e)

	// Bookkeeping (fail / mark-done) MUST survive per-object-timeout and
	// shutdown cancellation, or the row is stranded in_progress. Detach from the
	// cancelled ctx with a fresh short deadline.
	bctx, bcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer bcancel()
	switch {
	case err != nil:
		c.d.Logger.Warn("backup failed", zap.Int64("id", e.ID), zap.String("hash", e.ExternalID), zap.Error(err))
		c.fail(bctx, e.ID, err.Error())
	case !ok:
		c.fail(bctx, e.ID, "not all targets stored")
	default:
		if derr := c.d.Outbox.MarkDone(bctx, e.ID); derr != nil {
			c.d.Logger.Error("mark done", zap.Int64("id", e.ID), zap.Error(derr))
		}
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

// reap periodically returns stale in_progress entries to pending.
func (c *Consumer) reap(ctx context.Context) {
	t := time.NewTicker(c.d.StaleTTL)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.d.Outbox.ReapStale(ctx, c.d.StaleTTL); err != nil {
				c.d.Logger.Error("reap stale", zap.Error(err))
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
