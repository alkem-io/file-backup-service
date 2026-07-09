package domain

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DrillFailure records one object that failed its restore drill.
type DrillFailure struct {
	Hash string
	Err  error
}

// DrillOutcome is a restore-drill result: it proves the end-to-end RESTORE PROCEDURE (fetch →
// decode → hash-verify → write to disk), not just byte existence, for a random sample of the
// objects the ledger records stored on the drilled target (FR-024/SC-009/T033).
type DrillOutcome struct {
	Target   string
	Checked  int // objects drilled (up to the sample)
	Passed   int // restored to scratch AND hashed to their key
	Failed   int // restore/verify failed (a real DR problem)
	Failures []DrillFailure
}

// Pass reports whether every drilled object restored + verified. An EMPTY drill (Checked==0,
// nothing stored on the target yet) is a vacuous pass — there is nothing to prove — which is
// correct: a fresh target with no objects has no restore to fail.
func (o DrillOutcome) Pass() bool { return o.Failed == 0 }

// Drill samples up to `sample` objects the ledger records stored on src's target (a random band,
// so successive weekly drills cover different objects — sample<=0 drills every stored object) and,
// for each, restores it to scratchDir (reusing RestoreObject, so the drill exercises the exact
// operator restore path: hash-arbiter decode → SHA3-256 verify → durable write) and then removes
// the restored file to bound scratch disk. A per-object failure is recorded and the drill
// continues, so one bad object surfaces every other problem in the same run. Each object is
// bounded by perObjectTimeout (a hung source/sink fails that object, not the drill). Returns the
// outcome; a ctx cancellation stops the drill and is returned so the caller can distinguish an
// interrupted drill from a clean pass.
func Drill(ctx context.Context, led Ledger, src Sink, targetName, scratchDir string, sample int, perObjectTimeout time.Duration) (DrillOutcome, error) {
	perObjectTimeout = normalizePerObjectTimeout(perObjectTimeout)
	out := DrillOutcome{Target: targetName}
	hashes, err := sampleStored(ctx, led, targetName, sample)
	if err != nil {
		return out, err
	}
	for _, h := range hashes {
		if err := ctx.Err(); err != nil {
			return out, err // interrupted — the partial outcome is returned alongside the error
		}
		out.Checked++
		if derr := drillOne(ctx, src, h, scratchDir, perObjectTimeout); derr != nil {
			out.Failed++
			out.Failures = append(out.Failures, DrillFailure{Hash: h, Err: derr})
			continue
		}
		out.Passed++
	}
	return out, nil
}

// drillOne restores one object to scratchDir under a per-object deadline, then removes the
// restored file so the drill's scratch footprint stays bounded to one object at a time. A
// restore error (a source read fault, a decode/hash mismatch) is the failure the drill exists
// to catch. The cleanup is best-effort — a failed remove is not itself a drill failure (the
// caller RemoveAll's the whole scratch dir at the end).
func drillOne(ctx context.Context, src Sink, hash, scratchDir string, perObjectTimeout time.Duration) error {
	octx, cancel := context.WithTimeout(ctx, perObjectTimeout)
	defer cancel()
	if err := RestoreObject(octx, src, hash, scratchDir); err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(scratchDir, hash))
	return nil
}

// sampleStored returns up to `sample` externalIDs the ledger records stored on target, drawn from
// a RANDOM keyset band (sample<=0 = all) via the shared keysetSample driver — so a weekly drill
// checks a different slice of the corpus each run instead of always the lowest-prefix band. A
// high random start wraps once to the beginning, so the sample is never silently under-filled.
func sampleStored(ctx context.Context, led Ledger, target string, sample int) ([]string, error) {
	startAfter := ""
	if sample > 0 {
		startAfter = randKeysetStart()
	}
	var out []string
	err := keysetSample(ctx, sample, startAfter,
		func(after string, limit int) ([]string, error) {
			page, err := led.StoredExternalIDsPage(ctx, target, after, limit)
			if err != nil {
				return nil, fmt.Errorf("drill sample %s: %w", target, err)
			}
			return page, nil
		},
		func(page []string) error { out = append(out, page...); return nil })
	if err != nil {
		return nil, err
	}
	return out, nil
}
