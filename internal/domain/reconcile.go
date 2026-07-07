package domain

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

// Reconciler repairs under-replicated objects (FR-025/T029): for each object not
// stored on every configured target (the ledger's TargetGaps), it fetches the
// plaintext from a target that HAS it and re-fans it out to the targets missing it via
// the backup pipeline — which dedups the source, hash-verifies, and records the
// ledger. Target-to-target repair; no re-fetch from the primary store. This is the
// recovery path that makes object-level dead-letter safe: a target that was down comes
// back and reconcile fills its gaps.
type Reconciler struct {
	ledger     Ledger
	targets    []Target
	byName     map[string]Target
	perObjectT time.Duration // bounds one object's repair (a hung source/sink), like serve
	scratchDir string        // where decodingSource stages a decoded object ("" = OS temp dir)
	stall      time.Duration // fan-out stall-drop (hung DESTINATION target), like serve
	circuit    *CircuitBreaker
	onError    func(externalID string, err error) // per-object failure sink (logging); nil = silent
}

// OnError registers a callback invoked with the cause each time an object can't be repaired,
// so a DR run surfaces a SYSTEMIC failure (an unwritable scratchDir, a decode bug) that the
// failed COUNT alone hides — the consumer logs every backup failure; the repair path must too.
func (rc *Reconciler) OnError(fn func(externalID string, err error)) *Reconciler {
	rc.onError = fn
	return rc
}

// reportErr invokes the failure callback if set.
func (rc *Reconciler) reportErr(externalID string, err error) {
	if rc.onError != nil && err != nil {
		rc.onError(externalID, err)
	}
}

// NewReconciler binds a Reconciler to the ledger and target set; perObjectTimeout bounds
// one object's repair so a hung fetch/sink can't stall the whole single-threaded pass,
// and scratchDir ("" = OS temp) is where a decoded object is staged before re-fan-out.
// stall + circuit give the repair fan-out the SAME hung-target isolation as serve (a black-
// holing destination target is dropped at stall / skipped once its circuit trips, instead
// of wedging every repair for the full perObjectTimeout); pass 0/nil to disable (tests).
func NewReconciler(led Ledger, targets []Target, perObjectTimeout time.Duration, scratchDir string, stall time.Duration, circuit *CircuitBreaker) *Reconciler {
	byName := make(map[string]Target, len(targets))
	for _, t := range targets {
		byName[t.Sink.Name()] = t
	}
	return &Reconciler{ledger: led, targets: targets, byName: byName, perObjectT: perObjectTimeout, scratchDir: scratchDir, stall: stall, circuit: circuit}
}

// ReconcileStats reports one reconcile pass.
type ReconcileStats struct {
	Repaired int // objects brought to full replication this pass
	Skipped  int // no target holds the object — needs a backfill from the primary store
	Failed   int // a repair errored or left the object still under-replicated
}

// Run repairs every under-replicated object at most ratePerSec repairs/sec (0 =
// unlimited). It continues past a single-object failure (counted), stopping only on
// ctx cancellation.
func (rc *Reconciler) Run(ctx context.Context, ratePerSec int) (ReconcileStats, error) {
	var st ReconcileStats
	wait, stop := newPacer(ratePerSec)
	defer stop()
	names := TargetNames(rc.targets)
	p := NewPipeline(nil, rc.ledger, rc.targets).WithIsolation(rc.stall, rc.circuit)
	err := rc.ledger.TargetGaps(ctx, names, func(externalID string, stored map[string]bool) error {
		if err := wait(ctx); err != nil {
			return err
		}
		rc.repair(ctx, p, externalID, stored, &st)
		return ctx.Err() // stop the pass on shutdown
	})
	return st, err
}

// repair brings one under-replicated object to full replication. It tries each target
// the ledger says holds it AS THE SOURCE, falling through to the next if a source is
// gone/corrupt (a Fetch error, or the pipeline's VerifyReader rejecting bad bytes) —
// no upfront Exists probe, and no reliance on a possibly-stale ledger. backupFrom
// dedups the source + re-fans-out to the missing targets + records the ledger. A panic
// in one repair is contained (counted failed) so a poison object can't crash the pass.
func (rc *Reconciler) repair(ctx context.Context, p *Pipeline, externalID string, stored map[string]bool, st *ReconcileStats) {
	defer recoverFailed(&st.Failed)
	ctx, cancel := context.WithTimeout(ctx, rc.perObjectT) // a hung source/sink fails this object, not the pass
	defer cancel()
	entry := BackupItem{ExternalID: externalID} // FileID unused: decodingSource keys on ExternalID
	tried := false
	var lastErr error // the last source's fetch/decode cause, surfaced if every source fails
	for name := range stored {
		src, ok := rc.byName[name]
		if !ok {
			continue // stale status for a removed target
		}
		tried = true
		done, _, err := p.backupFrom(ctx, decodingSource{src: src.Sink, scratchDir: rc.scratchDir}, entry)
		if err == nil {
			// The source fetched + verified cleanly. Either fully repaired (done), or a
			// DESTINATION target failed (!done) — and rotating to another source can't fix
			// a down destination (it fails identically for any source), so STOP re-fetching
			// rather than re-reading the whole object from every holder to retry a dead
			// destination. Only a SOURCE failure (err != nil) is worth trying the next source.
			if done {
				st.Repaired++
			} else {
				st.Failed++
				rc.reportErr(externalID, fmt.Errorf("repaired source but a destination target is still unwritable"))
			}
			return
		}
		lastErr = err
		if ctx.Err() != nil {
			st.Failed++ // shutdown mid-repair
			return
		}
		// this source was gone/corrupt — try the next source
	}
	if !tried {
		st.Skipped++ // no configured target holds it — needs a backfill from the primary store
		rc.reportErr(externalID, fmt.Errorf("on NO current target — needs a primary-store backfill (not reconcilable)"))
		return
	}
	st.Failed++ // every source failed to fetch
	rc.reportErr(externalID, fmt.Errorf("every source failed to fetch/decode: %w", lastErr))
}

// decodingSource adapts a backup target into a pipeline Source: it decodes the stored
// object to a temp file using the SAME codec-agnostic decode as restore (decodeStream:
// zstd-magic arbiter + RAW FALLBACK + maxRestoreBytes cap + ctx-cancellable), then serves
// that as the plaintext for the pipeline to re-verify + re-fan-out. Reusing decodeStream
// (rather than a streaming magic-peek) is what makes reconcile recover EXACTLY what
// restore can: it survives a target's compression config being flipped (decode by bytes,
// not config) AND a raw-stored object that merely begins with the zstd magic (a .zst
// upload on a CodecNone target) — the latter needs the re-read-as-raw fallback, which a
// one-pass stream can't do. The temp file is bounded + verified against externalID and
// removed on Close.
type decodingSource struct {
	src        Sink
	scratchDir string // "" = OS temp dir
}

// FetchContent implements Source: decode the stored object to a temp file and serve it.
// It keys on e.ExternalID (the content hash — the target is content-addressed), NOT
// e.FileID, so reconcile no longer fakes a FileID.
func (d decodingSource) FetchContent(ctx context.Context, e BackupItem) (io.ReadCloser, error) {
	tmp, err := os.CreateTemp(d.scratchDir, "reconcile-*.plain") // "" = OS temp dir
	if err != nil {
		return nil, fmt.Errorf("reconcile temp: %w", err)
	}
	// One cleanup site (a committed-flag defer, like fsutil.CommitWrite) instead of a
	// close+remove copy on every early-return path: the temp is dropped unless it's handed to
	// tempReadCloser, which then owns removing it on Close.
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	if err := decodeStream(ctx, d.src, e.ExternalID, tmp, func() error { return rewindTruncate(tmp) }); err != nil {
		return nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("reconcile temp rewind: %w", err)
	}
	committed = true
	return &tempReadCloser{f: tmp}, nil
}

// tempReadCloser serves the decoded plaintext temp file and removes it on Close.
type tempReadCloser struct{ f *os.File }

// Read yields the decoded plaintext.
func (t *tempReadCloser) Read(p []byte) (int, error) { return t.f.Read(p) }

// Close closes and removes the temp file.
func (t *tempReadCloser) Close() error {
	err := t.f.Close()
	_ = os.Remove(t.f.Name())
	return err
}
