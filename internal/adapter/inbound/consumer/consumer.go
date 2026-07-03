// Package consumer drains the backup outbox: Postgres LISTEN/NOTIFY wakeups plus
// a polling floor plus a startup backlog drain, claiming rows with
// FOR UPDATE SKIP LOCKED. See specs/008 FR-005.
package consumer

import "context"

// Consumer runs the outbox drain loop.
type Consumer struct{}

// New constructs a Consumer.
func New() *Consumer { return &Consumer{} }

// Run blocks, draining the outbox until ctx is cancelled.
//
// TODO(T014/T015): LISTEN file_backup_outbox + polling floor + startup drain;
// claim (priority DESC, "createdDate") FOR UPDATE SKIP LOCKED; run the pipeline;
// retry/backoff + dead-letter; stale-claim reaper.
func (*Consumer) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}
