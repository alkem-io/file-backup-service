package domain

import (
	"context"
	"fmt"
	"io"
	"time"

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
	var pace <-chan time.Time
	if ratePerSec > 0 {
		t := time.NewTicker(time.Second / time.Duration(ratePerSec))
		defer t.Stop()
		pace = t.C
	}
	names := make([]string, len(rc.targets))
	for i, t := range rc.targets {
		names[i] = t.Sink.Name()
	}
	p := NewPipeline(nil, rc.ledger, rc.targets)
	err := rc.ledger.TargetGaps(ctx, names, func(externalID string, stored map[string]bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if pace != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-pace:
			}
		}
		src, ok := rc.pickSource(ctx, externalID, stored)
		if !ok {
			st.Skipped++ // nowhere holds it — reconcile can't repair; a backfill must re-fetch
			return nil
		}
		p.Source = decodingSource{src: src.Sink, codec: src.Codec}
		done, rerr := p.BackupOne(ctx, OutboxEntry{FileID: externalID, ExternalID: externalID})
		switch {
		case rerr != nil && ctx.Err() != nil:
			return rerr // shutdown — stop the pass
		case rerr != nil || !done:
			st.Failed++
		default:
			st.Repaired++
		}
		return nil
	})
	return st, err
}

// pickSource returns a configured target that holds externalID, preferring one whose
// presence can be Exists-verified (so a stale ledger row isn't chosen as the source);
// it falls back to the ledger's word when no candidate is introspectable (all WORM).
func (rc *Reconciler) pickSource(ctx context.Context, externalID string, stored map[string]bool) (Target, bool) {
	var fallback Target
	var have bool
	for name := range stored {
		t, ok := rc.byName[name]
		if !ok {
			continue // stale status for a removed target
		}
		if !have {
			fallback, have = t, true
		}
		if present, err := t.Sink.Exists(ctx, externalID); err == nil && present {
			return t, true
		}
	}
	return fallback, have
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
	zr, err := zstd.NewReader(io.LimitReader(rc, maxRestoreBytes+1), zstd.WithDecoderConcurrency(1))
	if err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("zstd decode: %w", err)
	}
	return &zstdReadCloser{zr: zr, under: rc}, nil
}

type zstdReadCloser struct {
	zr    *zstd.Decoder
	under io.Closer
}

func (z *zstdReadCloser) Read(p []byte) (int, error) { return z.zr.Read(p) }

// Close releases the decoder and the underlying stored-object reader.
func (z *zstdReadCloser) Close() error {
	z.zr.Close()
	return z.under.Close()
}
