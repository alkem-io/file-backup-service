package domain

import (
	"context"
	"errors"
	"sync"
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
// backup) into a counted failure, so one poison object can't crash the whole pass. mu guards
// the counter because the sweeps run items concurrently (runBoundedPaced).
func recoverFailed(mu *sync.Mutex, failed *int) {
	if r := recover(); r != nil { //nolint:revive // recover works here — recoverFailed is invoked directly via `defer recoverFailed(...)`
		bump(mu, failed)
	}
}

// bump increments a shared stats counter under mu — the sweeps mutate their stats from many
// worker goroutines, so every counter touch is locked.
func bump(mu *sync.Mutex, counter *int) {
	mu.Lock()
	*counter++
	mu.Unlock()
}

// runBoundedPaced dispatches each item `enumerate` yields to `work`, at up to `concurrency`
// in flight and paced at ratePerSec DISPATCHES/sec (0 = unlimited). The pacer AND a semaphore
// gate dispatch; each item runs in a worker goroutine; ALL in-flight workers are drained before
// returning. This overlaps the per-object I/O latency of the batch/DR sweeps (backfill,
// reconcile) — which the Concurrency knob sizes just like serve's worker pool — and, for
// reconcile, removes the head-of-line stall where one slow object blocked the whole pass.
// enumerate is the sweep source (backfill's EachFile / reconcile's TargetGaps); a yield/enumerate
// error stops the sweep. The one owner of the bounded-paced worker-pool scaffold.
func runBoundedPaced[T any](ctx context.Context, concurrency, ratePerSec int, enumerate func(yield func(T) error) error, work func(T)) error {
	if concurrency < 1 {
		concurrency = 1
	}
	wait, stop := newPacer(ratePerSec)
	defer stop()
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	err := enumerate(func(item T) error {
		if err := wait(ctx); err != nil { // pacer gates DISPATCH (nil = go, else stop the sweep)
			return err
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		wg.Add(1)
		go func(item T) {
			defer wg.Done()
			defer func() { <-sem }()
			work(item)
		}(item)
		return ctx.Err()
	})
	wg.Wait() // drain in-flight workers before the caller reads the stats
	return err
}

// CorpusEnumerator streams the authoritative file corpus — the source of truth for what
// SHOULD be backed up (the file-service `file` table). Backfill uses it to find + fill
// the pre-existing objects the outbox never carried (those created before the service).
type CorpusEnumerator interface {
	// EachFile invokes fn once per corpus file (as a BackupItem); if fn returns an
	// error, iteration stops and returns it.
	EachFile(ctx context.Context, fn func(BackupItem) error) error
}

// BackfillStats summarizes a backfill pass.
type BackfillStats struct {
	Backed  int // fully stored on every target after this pass (incl. already-present)
	Skipped int // source object gone (deleted before backfill) — benign terminal, not a failure
	Failed  int // a target genuinely failed, or the pass was cancelled
}

// Backfiller backs up the pre-existing corpus (US2/T022): it enumerates every file and
// runs the normal backup pipeline for each. BackupOne dedups against the ledger, so an
// already-backed-up object costs one ledger query and NO fetch — which makes the pass
// both resumable (re-run skips completed objects cheaply) and repeatable as a
// completeness sweep. Rate-limited + ctx-cancellable; a poison object can't crash it.
type Backfiller struct {
	corpus      CorpusEnumerator
	p           *Pipeline
	perObjectT  time.Duration // bounds one object's backup (a hung fetch/sink), like serve
	concurrency int           // in-flight objects (the Concurrency knob), like serve's worker pool
}

// NewBackfiller binds a Backfiller to the corpus source and a source-backed pipeline;
// perObjectTimeout bounds one object so a hung fetch/sink can't stall the whole pass, and
// concurrency (the Concurrency knob) is the in-flight object count so the sweep overlaps
// per-object fetch/store latency instead of running 1-at-a-time.
func NewBackfiller(corpus CorpusEnumerator, p *Pipeline, perObjectTimeout time.Duration, concurrency int) *Backfiller {
	return &Backfiller{corpus: corpus, p: p, perObjectT: perObjectTimeout, concurrency: concurrency}
}

// Run enumerates the corpus and backs up each object with up to `concurrency` in flight, paced
// at up to ratePerSec dispatches/sec (0 = unlimited), stopping on ctx cancellation.
func (b *Backfiller) Run(ctx context.Context, ratePerSec int) (BackfillStats, error) {
	var st BackfillStats
	var mu sync.Mutex // st is mutated from concurrent workers
	err := runBoundedPaced(ctx, b.concurrency, ratePerSec,
		func(yield func(BackupItem) error) error { return b.corpus.EachFile(ctx, yield) },
		func(e BackupItem) { b.backupOne(ctx, e, &st, &mu) })
	return st, err
}

// backupOne backs up one corpus entry, contained by a recover so a single poison object
// (e.g. a nil-slice panic on a shutdown-interrupted fetch) can't crash the whole pass. st is
// mutated under mu (workers run concurrently); the lock is taken AFTER the object's I/O, so it
// doesn't serialize the fetch/store.
func (b *Backfiller) backupOne(ctx context.Context, e BackupItem, st *BackfillStats, mu *sync.Mutex) {
	defer recoverFailed(mu, &st.Failed)
	ctx, cancel := context.WithTimeout(ctx, b.perObjectT) // a hung fetch/sink fails this object, not the pass
	defer cancel()
	done, _, err := b.p.BackupOne(ctx, e)
	mu.Lock()
	defer mu.Unlock()
	switch {
	case err == nil && done:
		st.Backed++
	case errors.Is(err, ErrSourceGone):
		// Source deleted before backfill ran (file-service 404/410) — a benign terminal
		// condition, NOT a failure: the object needn't be backed up. Mirror the serve
		// consumer's Skip so a corpus with routine deletions doesn't fail the pass (and
		// doesn't bury genuine target failures in an undifferentiated Failed count).
		st.Skipped++
	default:
		st.Failed++
	}
}
