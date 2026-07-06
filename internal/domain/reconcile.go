package domain

import (
	"context"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Reconciler repairs under-replicated objects (FR-025/T029): for each object not
// stored on every configured target (the ledger's TargetGaps), it fetches the
// plaintext from a target that HAS it and re-fans it out to the targets missing it via
// the backup pipeline — which dedups the source, hash-verifies, and records the
// ledger. Target-to-target repair; no re-fetch from the primary store. This is the
// recovery path that makes object-level dead-letter safe: a target that was down comes
// back and reconcile fills its gaps.
type Reconciler struct {
	ledger  Ledger
	targets []Target
	byName  map[string]Target
}

// NewReconciler binds a Reconciler to the ledger and target set.
func NewReconciler(led Ledger, targets []Target) *Reconciler {
	byName := make(map[string]Target, len(targets))
	for _, t := range targets {
		byName[t.Sink.Name()] = t
	}
	return &Reconciler{ledger: led, targets: targets, byName: byName}
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
	names := make([]string, len(rc.targets))
	for i, t := range rc.targets {
		names[i] = t.Sink.Name()
	}
	p := NewPipeline(nil, rc.ledger, rc.targets)
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
	defer func() {
		if r := recover(); r != nil {
			st.Failed++
		}
	}()
	entry := OutboxEntry{FileID: externalID, ExternalID: externalID}
	tried := false
	for name := range stored {
		src, ok := rc.byName[name]
		if !ok {
			continue // stale status for a removed target
		}
		tried = true
		done, err := p.backupFrom(ctx, decodingSource{src: src.Sink, codec: src.Codec}, entry)
		if err == nil && done {
			st.Repaired++
			return
		}
		if ctx.Err() != nil {
			st.Failed++ // shutdown mid-repair
			return
		}
		// this source was gone/corrupt (or a missing target failed) — try the next source
	}
	if !tried {
		st.Skipped++ // no configured target holds it — needs a backfill from the primary store
		return
	}
	st.Failed++ // every source failed
}

// decodingSource adapts a backup target into a pipeline Source: it Fetches the stored
// bytes and, per the source target's codec, yields the PLAINTEXT for the pipeline to
// re-verify against externalID and re-fan-out. A corrupt source is caught by the
// pipeline's VerifyReader (nothing is committed).
type decodingSource struct {
	src   Sink
	codec Codec
}

// FetchContent implements Source: fetch the stored bytes and yield the plaintext.
func (d decodingSource) FetchContent(ctx context.Context, externalID string) (io.ReadCloser, error) {
	rc, err := d.src.Fetch(ctx, externalID)
	if err != nil {
		return nil, err
	}
	if d.codec != CodecZstd {
		return rc, nil // stored raw == plaintext
	}
	zr, err := newZstdDecoder(rc)
	if err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("zstd decode: %w", err)
	}
	// Bound the DECODED output (not just the compressed input): a compromised target
	// could hold a tiny frame that expands to PB. The pipeline's VerifyReader then
	// rejects a bomb (the capped stream won't hash to externalID) with nothing committed.
	return &zstdReadCloser{r: io.LimitReader(zr, maxRestoreBytes+1), zr: zr, under: rc}, nil
}

type zstdReadCloser struct {
	r     io.Reader // decoded stream, bomb-capped
	zr    *zstd.Decoder
	under io.Closer
}

func (z *zstdReadCloser) Read(p []byte) (int, error) { return z.r.Read(p) }

// Close releases the decoder and the underlying stored-object reader.
func (z *zstdReadCloser) Close() error {
	z.zr.Close()
	return z.under.Close()
}
