package domain

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DrillFailure records one object that failed its restore drill.
type DrillFailure struct {
	Hash string
	Err  error
}

// DrillOutcome is a restore-drill result: it proves the end-to-end RESTORE PROCEDURE (fetch →
// decode → hash-verify → write to disk), not just byte existence, for a random sample of the
// objects the ledger records stored on the drilled target (FR-024/SC-009/T033). Only the two
// INDEPENDENTLY-observed outcomes are stored (Passed, Failed); Checked is DERIVED, so no stored
// field can drift out of lock-step with a counter that is actually recorded. A cancellation is
// counted in NEITHER — the sweep surfaces an interruption as its returned error.
type DrillOutcome struct {
	Target   string
	Passed   int // objects that restored + hash-matched
	Failed   int // GENUINE restore/verify failures (a real DR problem)
	Failures []DrillFailure
}

// Checked is the number of objects actually drilled — DERIVED (Passed + Failed), not stored.
func (o DrillOutcome) Checked() int { return o.Passed + o.Failed }

// Pass reports whether the drill PROVED the restore procedure: at least one object was drilled AND
// none failed. A 0-checked drill is NOT a pass — it proved nothing, and a 0-count is itself a
// SIGNAL (a renamed/misconfigured target, or an empty/wrong ledger, yields no sampled rows), so it
// must not read green and mask that; the caller surfaces it as a distinct failure.
func (o DrillOutcome) Pass() bool { return o.Checked() > 0 && o.Failed == 0 }

// Drill samples up to `sample` objects the ledger records stored on src's target (a random band,
// so successive weekly drills cover different objects — sample<=0 drills every stored object) and,
// for each, restores it to scratchDir (reusing RestoreObject, so the drill exercises the exact
// operator restore path: hash-arbiter decode → SHA3-256 verify → durable write) and then removes
// the restored file to bound scratch disk. It runs through the shared runBoundedPaced scaffold
// (concurrency 1) so it inherits the sweep's cancellation propagation + the STREAMING id source
// (sample=0 doesn't buffer the whole corpus). A per-object panic is contained as one failure; a
// per-object failure is recorded and the drill continues, surfacing every other problem in the same
// run. Each object is bounded by perObjectTimeout. Returns the outcome; a non-nil error is an
// enumeration failure OR a cancellation (an interrupted drill — the caller must not read it green).
func Drill(ctx context.Context, led Ledger, src Sink, targetName, scratchDir string, sample int, perObjectTimeout time.Duration) (DrillOutcome, error) {
	perObjectTimeout = NormalizePerObjectTimeout(perObjectTimeout)
	out := DrillOutcome{Target: targetName}
	var mu sync.Mutex
	dispatched := 0 // objects actually handed to a worker (single-threaded enumeration, read after drain)
	err := runBoundedPaced(ctx, 1, 0,
		func(yield func(string) error) error {
			return streamSampledStored(ctx, led, targetName, sample, func(h string) error {
				if yerr := yield(h); yerr != nil {
					return yerr
				}
				dispatched++
				return nil
			})
		},
		func(h string) { drillOne(ctx, src, h, scratchDir, perObjectTimeout, &out, &mu) })
	return out, drillInterruptErr(ctx, err, out.Checked(), dispatched)
}

// drillInterruptErr decides a drill's terminal error from PROGRESS, not from ctx.Err() sampled after
// completion: a sweep error (a mid-sweep cancel / enumeration failure) propagates; otherwise the
// drill is "interrupted" ONLY when the sweep was TRUNCATED — a dispatched object went uncounted
// because its work goroutine was cancelled after enumeration finished (checked < dispatched). A
// SIGTERM that lands AFTER every sampled object already passed leaves checked == dispatched, so the
// COMPLETED pass's success is NOT discarded (the old `if err==nil { err=ctx.Err() }` wrongly paged a
// false stale-drill on a post-completion SIGTERM).
func drillInterruptErr(ctx context.Context, sweepErr error, checked, dispatched int) error {
	if sweepErr != nil {
		return sweepErr
	}
	if checked < dispatched {
		return ctx.Err()
	}
	return nil
}

// drillOne restores one object to scratchDir under a per-object deadline, removes the restored file
// (bounding scratch disk to one object at a time), and folds the outcome into out under mu. A sink
// panic is already contained as an error by the restore path's callWithCtx, so it lands here as a
// GENUINE failure. Its own recover (recoverDrillFailure) counts a stray panic AS a failure AND records
// the DrillFailure with this object's hash — so the operator's per-object FAIL list is never missing a
// failed object (unlike the counter-only recoverFailed the other sweeps use, which have no per-object
// detail to record). A cancellation aborting this object in flight (a SIGTERM) is NOT counted — the
// sweep surfaces it as its returned error, not a failed object.
func drillOne(ctx context.Context, src Sink, hash, scratchDir string, perObjectTimeout time.Duration, out *DrillOutcome, mu *sync.Mutex) {
	defer recoverDrillFailure(mu, out, hash)
	octx, cancel := context.WithTimeout(ctx, perObjectTimeout)
	defer cancel()
	err := RestoreObject(octx, src, hash, scratchDir)
	_ = os.Remove(filepath.Join(scratchDir, hash)) // best-effort; the caller RemoveAll's the whole dir
	mu.Lock()
	defer mu.Unlock()
	if cancelledInFlight(ctx, err) {
		return // interrupted mid-object — the sweep's cancellation error surfaces it, not a Failed count
	}
	if err != nil {
		recordDrillFailure(out, hash, err)
		return
	}
	out.Passed++
}

// recordDrillFailure appends a failure (hash + cause) and bumps the count — the ONE place both stay in
// lock-step, so a panic-recover and the normal error path can't record one without the other. The
// caller holds mu.
func recordDrillFailure(out *DrillOutcome, hash string, err error) {
	out.Failed++
	out.Failures = append(out.Failures, DrillFailure{Hash: hash, Err: err})
}

// recoverDrillFailure is drillOne's deferred guard: a stray panic (one NOT already turned into an error
// by the restore path's callWithCtx) is recorded as a failure WITH this object's hash, so the drill's
// FAIL list is complete. It takes mu itself because the panic unwinds before drillOne's own mu.Lock().
func recoverDrillFailure(mu *sync.Mutex, out *DrillOutcome, hash string) {
	if r := recover(); r != nil { //nolint:revive // recover works here — invoked directly via defer
		mu.Lock()
		defer mu.Unlock()
		recordDrillFailure(out, hash, PanicErr("drill "+hash, r))
	}
}

// streamSampledStored yields up to `sample` externalIDs the ledger records stored on target, drawn
// from a RANDOM keyset band (sample<=0 = all) via the shared keysetSample driver — so a weekly
// drill checks a different slice of the corpus each run, and a full drill (sample=0) STREAMS the
// ids to the sweep rather than buffering the whole corpus in memory. A high random start wraps once
// to the beginning, so the sample is never silently under-filled.
func streamSampledStored(ctx context.Context, led Ledger, target string, sample int, yield func(string) error) error {
	return keysetSample(ctx, sample, sampledStart(sample),
		func(after string, limit int) ([]string, error) {
			// Bounded per-page (like audit/inventory): a wedged ledger during a scheduled drill must
			// self-abort, not hang the CronJob indefinitely.
			page, err := storedPageBounded(ctx, led, target, after, limit)
			if err != nil {
				return nil, fmt.Errorf("drill sample %s: %w", target, err)
			}
			return page, nil
		},
		func(page []string) error {
			for _, h := range page {
				if err := yield(h); err != nil {
					return err
				}
			}
			return nil
		})
}
