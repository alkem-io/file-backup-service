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
	if d.Logger == nil {
		d.Logger = zap.NewNop()
	}
	return &Consumer{d: d}
}

// Run drains at startup, then on NOTIFY wakeups and a polling floor.
func (c *Consumer) Run(ctx context.Context) error {
	wake := make(chan struct{}, 1)
	go c.listen(ctx, wake)

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
	ok, err := c.d.Pipeline.BackupOne(ctx, e)
	switch {
	case err != nil:
		c.d.Logger.Warn("backup failed", zap.Int64("id", e.ID), zap.String("hash", e.ExternalID), zap.Error(err))
		if ferr := c.d.Outbox.Fail(ctx, e.ID, err.Error()); ferr != nil {
			c.d.Logger.Error("mark fail", zap.Int64("id", e.ID), zap.Error(ferr))
		}
	case !ok:
		if ferr := c.d.Outbox.Fail(ctx, e.ID, "not all required targets stored"); ferr != nil {
			c.d.Logger.Error("mark fail", zap.Int64("id", e.ID), zap.Error(ferr))
		}
	default:
		if derr := c.d.Outbox.MarkDone(ctx, e.ID); derr != nil {
			c.d.Logger.Error("mark done", zap.Int64("id", e.ID), zap.Error(derr))
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
