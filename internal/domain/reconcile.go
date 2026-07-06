package domain

import (
	"bufio"
	"bytes"
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
	defer recoverFailed(&st.Failed)
	entry := OutboxEntry{FileID: externalID, ExternalID: externalID}
	tried := false
	for name := range stored {
		src, ok := rc.byName[name]
		if !ok {
			continue // stale status for a removed target
		}
		tried = true
		done, err := p.backupFrom(ctx, decodingSource{src: src.Sink}, entry)
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
			}
			return
		}
		if ctx.Err() != nil {
			st.Failed++ // shutdown mid-repair
			return
		}
		// this source was gone/corrupt — try the next source
	}
	if !tried {
		st.Skipped++ // no configured target holds it — needs a backfill from the primary store
		return
	}
	st.Failed++ // every source failed to fetch
}

// decodingSource adapts a backup target into a pipeline Source: it Fetches the stored
// bytes and yields the PLAINTEXT for the pipeline to re-verify against externalID and
// re-fan-out. It arbitrates the codec from the STORED BYTES (zstd-magic peek), NOT the
// target's configured codec — so flipping a target's compression config doesn't make
// its already-stored objects unreconcilable (the recovery path must survive a config
// change the same way restore does). BOTH branches bound the yielded plaintext at
// maxRestoreBytes+1 (a compromised/oversized source can't fill the recovery host disk),
// and a wrong decode (a rare raw-object-that-looks-like-zstd) is caught by the
// pipeline's VerifyReader (hash mismatch → nothing committed → repair rotates source).
type decodingSource struct {
	src Sink
}

// FetchContent implements Source: fetch the stored bytes and yield the (bounded) plaintext.
func (d decodingSource) FetchContent(ctx context.Context, externalID string) (io.ReadCloser, error) {
	rc, err := d.src.Fetch(ctx, externalID)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReaderSize(rc, 8<<10)
	magic, _ := br.Peek(4) // short object → short magic, simply won't match
	if !bytes.Equal(magic, zstdMagic) {
		return &boundedReadCloser{r: io.LimitReader(br, maxRestoreBytes+1), under: rc}, nil // stored raw == plaintext
	}
	zr, err := newZstdDecoder(br)
	if err != nil {
		_ = rc.Close()
		return nil, fmt.Errorf("zstd decode: %w", err)
	}
	return &zstdReadCloser{r: io.LimitReader(zr, maxRestoreBytes+1), zr: zr, under: rc}, nil
}

// boundedReadCloser yields a bounded reader and closes the underlying stored-object reader.
type boundedReadCloser struct {
	r     io.Reader
	under io.Closer
}

// Read yields the bounded stream.
func (b *boundedReadCloser) Read(p []byte) (int, error) { return b.r.Read(p) }

// Close releases the underlying stored-object reader.
func (b *boundedReadCloser) Close() error { return b.under.Close() }

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
