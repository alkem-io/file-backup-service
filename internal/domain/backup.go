package domain

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// errAbortedBeforeEOF marks a target whose pipe went dead before consuming the
// full verified stream — it must never be recorded as "stored".
var errAbortedBeforeEOF = errors.New("sink closed before consuming the full stream")

// Target is one backup destination. All targets are symmetric — there is no
// primary/required/optional; every object goes to every target and "done"
// requires all of them (FR-012). A flaky target is isolated, never a main one.
type Target struct {
	// Sink is the content-addressed store.
	Sink Sink
	// Codec is the per-target transform (none | zstd).
	Codec Codec
}

// Pipeline backs up one object to all configured targets and updates the ledger.
type Pipeline struct {
	// Source fetches object bytes by file id.
	Source Source
	// Ledger records object + per-target status and answers dedup.
	Ledger Ledger
	// Targets are the configured sinks.
	Targets []Target
	// Metrics receives observations (Nop if unset).
	Metrics Metrics
}

// NewPipeline constructs a Pipeline.
func NewPipeline(src Source, ledger Ledger, targets []Target) *Pipeline {
	return &Pipeline{Source: src, Ledger: ledger, Targets: targets, Metrics: Nop{}}
}

// BackupOne stores the object on every target that still needs it. The source is
// fetched ONCE and fanned out to all targets concurrently (streamed, bounded
// memory), so adding a second target does not multiply read load on the primary
// store — FR: N symmetric configurable targets. Dedup is answered by our own
// ledger (never by re-reading a target), so it works with PutObject-only WORM
// credentials. Returns true only when EVERY target is stored (symmetric
// done-gate); a flaky target leaves the object not-done for retry while never
// blocking the healthy targets.
func (p *Pipeline) BackupOne(ctx context.Context, e OutboxEntry) (bool, error) {
	pending := make([]Target, 0, len(p.Targets))
	anyStored := false
	for _, t := range p.Targets {
		if state, _, err := p.Ledger.TargetState(ctx, e.ExternalID, t.Sink.Name()); err == nil && state == "stored" {
			p.Metrics.ObjectDedup(t.Sink.Name())
			anyStored = true // the object row already exists (FK parent present)
			continue
		}
		pending = append(pending, t)
	}
	if len(pending) == 0 {
		return true, nil // every target already has it
	}

	results, size, err := p.fanOut(ctx, e, pending)
	if err != nil {
		return false, err // source fetch failed — outbox retries the whole object
	}

	allStored := true
	// objectRecorded gates the FK-safe "failed" write; if a prior round already
	// stored the object on some target, its row exists, so failures this round
	// (even before any success) can be recorded.
	objectRecorded := anyStored
	for i, t := range pending {
		name := t.Sink.Name()
		if results[i].err != nil {
			p.Metrics.ObjectFailed(name)
			if objectRecorded {
				_ = p.Ledger.UpsertTargetStatus(ctx, e.ExternalID, name, "failed", 0)
			}
			allStored = false
			continue
		}
		// Object row must exist before its target_status (FK); idempotent.
		if err := p.Ledger.UpsertObject(ctx, ObjectMeta{
			ExternalID:        e.ExternalID,
			Size:              size,
			CreatedBy:         e.CreatedBy,
			SourceCreatedDate: e.CreatedDate,
		}); err != nil {
			return false, fmt.Errorf("ledger object: %w", err)
		}
		objectRecorded = true
		p.Metrics.ObjectStored(name, results[i].stored)
		if err := p.Ledger.UpsertTargetStatus(ctx, e.ExternalID, name, "stored", results[i].stored); err != nil {
			return false, fmt.Errorf("ledger target status: %w", err)
		}
	}
	return allStored, nil
}

type targetResult struct {
	stored int64
	err    error
}

// fanOut fetches the source once and copies the hash-verified plaintext to every
// target concurrently (each applying its own codec). A per-target failure is
// isolated — its pipe is dropped and the remaining targets keep receiving bytes.
// A corrupt source (VerifyReader fails at EOF) aborts every target's write, so
// nothing is committed anywhere. Returns per-target results and the plaintext size.
func (p *Pipeline) fanOut(ctx context.Context, e OutboxEntry, targets []Target) ([]targetResult, int64, error) {
	rc, err := p.Source.FetchContent(ctx, e.FileID)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rc.Close() }()
	vr := NewVerifyReader(rc, e.ExternalID)

	n := len(targets)
	results := make([]targetResult, n)
	writers := make([]*io.PipeWriter, n)
	var wg sync.WaitGroup
	for i, t := range targets {
		pr, pw := io.Pipe()
		writers[i] = pw
		wg.Add(1)
		go func(i int, t Target, pr *io.PipeReader) {
			defer wg.Done()
			// A panicking sink must fail its target, not crash the worker.
			defer func() {
				if r := recover(); r != nil {
					results[i] = targetResult{err: fmt.Errorf("sink %s panicked: %v", t.Sink.Name(), r)}
					_ = pr.CloseWithError(errAbortedBeforeEOF)
				}
			}()
			var reader io.Reader = pr
			if t.Codec == CodecZstd {
				zr := ZstdReader(pr)
				defer func() { _ = zr.Close() }()
				reader = zr
			}
			// size = -1 is load-bearing, not merely "unknown". Commit is gated on
			// the dispatcher closing the pipes AFTER VerifyReader checks the hash;
			// a known byte-count would let a sink finalize (e.g. minio single-PUT)
			// on length alone, before verification, committing unverified bytes.
			stored, serr := t.Sink.Store(ctx, e.ExternalID, reader, -1)
			results[i] = targetResult{stored: stored, err: serr}
			// Unblock the dispatcher if Store bailed before draining the pipe.
			_ = pr.CloseWithError(serr)
		}(i, t, pr)
	}

	fw := &fanoutWriter{writers: make([]io.Writer, n), dead: make([]bool, n)}
	for i := range writers {
		fw.writers[i] = writers[i]
	}
	// copyErr is set on a SOURCE failure: a VerifyReader hash mismatch (corrupt
	// source) or ctx cancellation/timeout during the read.
	_, copyErr := io.Copy(fw, vr)
	for _, pw := range writers {
		_ = pw.CloseWithError(copyErr)
	}
	wg.Wait()
	// A target whose pipe went dead (stopped reading) but reported no error did
	// NOT receive the full verified stream — force it to failed so a non-consuming
	// sink can never be recorded as "stored".
	for i := range results {
		if fw.dead[i] && results[i].err == nil {
			results[i].err = errAbortedBeforeEOF
		}
	}
	if copyErr != nil {
		// Surface the real source error (integrity / cancellation) instead of
		// letting it masquerade as N target-store failures.
		return results, vr.Total, fmt.Errorf("source read: %w", copyErr)
	}
	return results, vr.Total, nil
}

// fanoutWriter writes each chunk to every still-live target pipe. A pipe write
// error marks that target dead (its Store exited) and the rest continue; it
// stops the source copy only once every target is gone.
type fanoutWriter struct {
	writers []io.Writer
	dead    []bool
}

func (f *fanoutWriter) Write(p []byte) (int, error) {
	live := 0
	for i, w := range f.writers {
		if f.dead[i] {
			continue
		}
		if _, err := w.Write(p); err != nil {
			f.dead[i] = true
			continue
		}
		live++
	}
	if live == 0 {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
