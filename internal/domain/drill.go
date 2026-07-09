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
	err := runBoundedPaced(ctx, 1, 0,
		func(yield func(string) error) error { return streamSampledStored(ctx, led, targetName, sample, yield) },
		func(h string) { drillOne(ctx, src, h, scratchDir, perObjectTimeout, &out, &mu) })
	// A cancellation that landed DURING a work goroutine (after enumeration already dispatched the
	// last object) leaves runBoundedPaced's return nil — but the drill was still INTERRUPTED and
	// proved less than it sampled. Surface ctx.Err() so an aborted integrity check can't read green
	// (unlike restore-all, an interrupted drill must exit nonzero — the drilled object went uncounted).
	if err == nil {
		err = ctx.Err()
	}
	return out, err
}

// drillOne restores one object to scratchDir under a per-object deadline, removes the restored file
// (bounding scratch disk to one object at a time), and folds the outcome into out under mu. A poison
// object whose sink panics is contained by the restore path's own callWithCtx (RestoreObject →
// decodeStream), which turns the panic into an error — so it lands here as a GENUINE failure, not a
// crash; drillOne has NO panic path of its own (RestoreObject's only panic sources are the sink,
// behind callWithCtx, and the os-spine + mutex ops don't panic — a drill's fresh scratch never hits
// the pre-existing-file read). A cancellation aborting this object in flight (parent ctx cancelled —
// a SIGTERM) is NOT counted: the sweep surfaces it as its returned error, not a failed object.
func drillOne(ctx context.Context, src Sink, hash, scratchDir string, perObjectTimeout time.Duration, out *DrillOutcome, mu *sync.Mutex) {
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
		out.Failed++
		out.Failures = append(out.Failures, DrillFailure{Hash: hash, Err: err})
		return
	}
	out.Passed++
}

// streamSampledStored yields up to `sample` externalIDs the ledger records stored on target, drawn
// from a RANDOM keyset band (sample<=0 = all) via the shared keysetSample driver — so a weekly
// drill checks a different slice of the corpus each run, and a full drill (sample=0) STREAMS the
// ids to the sweep rather than buffering the whole corpus in memory. A high random start wraps once
// to the beginning, so the sample is never silently under-filled.
func streamSampledStored(ctx context.Context, led Ledger, target string, sample int, yield func(string) error) error {
	startAfter := ""
	if sample > 0 {
		startAfter = randKeysetStart()
	}
	return keysetSample(ctx, sample, startAfter,
		func(after string, limit int) ([]string, error) {
			page, err := led.StoredExternalIDsPage(ctx, target, after, limit)
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
