package domain

import (
	"context"
	"time"
)

// newPacer returns a wait func that blocks until the next tick (rate limiting) and a
// stop func. ratePerSec<=0 is unlimited — wait then only observes ctx cancellation.
// Shared by reconcile and backfill so the rate/ticker policy lives in one place.
func newPacer(ratePerSec int) (wait func(context.Context) error, stop func()) {
	if ratePerSec <= 0 {
		return func(ctx context.Context) error { return ctx.Err() }, func() {}
	}
	interval := time.Second / time.Duration(ratePerSec)
	if interval <= 0 {
		interval = time.Nanosecond // an absurd rate: effectively unlimited, never NewTicker(0)
	}
	t := time.NewTicker(interval)
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	}, t.Stop
}

// recoverFailed converts a panic in a per-item sweep step (reconcile repair / backfill
// backup) into a counted failure, so one poison object can't crash the whole pass.
func recoverFailed(failed *int) {
	if r := recover(); r != nil { //nolint:revive // recover works here — recoverFailed is invoked directly via `defer recoverFailed(...)`
		*failed++
	}
}

// CorpusEnumerator streams the authoritative file corpus — the source of truth for what
// SHOULD be backed up (the file-service `file` table). Backfill uses it to find + fill
// the pre-existing objects the outbox never carried (those created before the service).
type CorpusEnumerator interface {
	// EachFile invokes fn once per corpus file (as an OutboxEntry); if fn returns an
	// error, iteration stops and returns it.
	EachFile(ctx context.Context, fn func(OutboxEntry) error) error
}

// BackfillStats summarizes a backfill pass.
type BackfillStats struct {
	Backed int // fully stored on every target after this pass (incl. already-present)
	Failed int // a target failed, the source was gone, or the pass was cancelled
}

// Backfiller backs up the pre-existing corpus (US2/T022): it enumerates every file and
// runs the normal backup pipeline for each. BackupOne dedups against the ledger, so an
// already-backed-up object costs one ledger query and NO fetch — which makes the pass
// both resumable (re-run skips completed objects cheaply) and repeatable as a
// completeness sweep. Rate-limited + ctx-cancellable; a poison object can't crash it.
type Backfiller struct {
	corpus     CorpusEnumerator
	p          *Pipeline
	perObjectT time.Duration // bounds one object's backup (a hung fetch/sink), like serve
}

// NewBackfiller binds a Backfiller to the corpus source and a source-backed pipeline;
// perObjectTimeout bounds one object so a hung fetch/sink can't stall the whole pass.
func NewBackfiller(corpus CorpusEnumerator, p *Pipeline, perObjectTimeout time.Duration) *Backfiller {
	return &Backfiller{corpus: corpus, p: p, perObjectT: perObjectTimeout}
}

// Run enumerates the corpus and backs up each object at up to ratePerSec (0 = unlimited),
// stopping on ctx cancellation.
func (b *Backfiller) Run(ctx context.Context, ratePerSec int) (BackfillStats, error) {
	var st BackfillStats
	wait, stop := newPacer(ratePerSec)
	defer stop()
	err := b.corpus.EachFile(ctx, func(e OutboxEntry) error {
		if err := wait(ctx); err != nil {
			return err
		}
		b.backupOne(ctx, e, &st)
		return ctx.Err()
	})
	return st, err
}

// backupOne backs up one corpus entry, contained by a recover so a single poison object
// (e.g. a nil-slice panic on a shutdown-interrupted fetch) can't crash the whole pass.
func (b *Backfiller) backupOne(ctx context.Context, e OutboxEntry, st *BackfillStats) {
	defer recoverFailed(&st.Failed)
	ctx, cancel := context.WithTimeout(ctx, b.perObjectT) // a hung fetch/sink fails this object, not the pass
	defer cancel()
	if done, err := b.p.BackupOne(ctx, e); err == nil && done {
		st.Backed++
	} else {
		st.Failed++
	}
}
