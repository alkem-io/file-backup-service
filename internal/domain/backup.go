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
	stored, err := p.Ledger.StoredTargets(ctx, e.ExternalID) // one query, not N
	if err != nil {
		return false, fmt.Errorf("ledger read: %w", err)
	}
	pending := make([]Target, 0, len(p.Targets))
	for _, t := range p.Targets {
		if stored[t.Sink.Name()] {
			p.Metrics.ObjectDedup(t.Sink.Name())
			continue
		}
		pending = append(pending, t)
	}
	if len(pending) == 0 {
		return true, nil // every target already has it
	}

	// Record the object row up front (idempotent) from the outbox size, so a
	// target_status write never FK-violates AND an object that fails on every
	// target still leaves a ledger trace — failure telemetry no longer requires a
	// prior success.
	if err := p.Ledger.UpsertObject(ctx, ObjectMeta{
		ExternalID:        e.ExternalID,
		Size:              e.Size,
		CreatedBy:         e.CreatedBy,
		SourceCreatedDate: e.CreatedDate,
	}); err != nil {
		return false, fmt.Errorf("ledger object: %w", err)
	}

	results, ferr := p.fanOut(ctx, e, pending)
	if ferr != nil {
		return false, ferr // source integrity / cancellation — outbox retries
	}

	allStored := true
	for i, t := range pending {
		name := t.Sink.Name()
		if results[i].err != nil {
			p.Metrics.ObjectFailed(name)
			_ = p.Ledger.UpsertTargetStatus(ctx, e.ExternalID, name, "failed", 0)
			allStored = false
			continue
		}
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
// nothing is committed anywhere. Returns per-target results; a non-nil error is a
// SOURCE failure (integrity mismatch or ctx cancellation).
func (p *Pipeline) fanOut(ctx context.Context, e OutboxEntry, targets []Target) ([]targetResult, error) {
	rc, err := p.Source.FetchContent(ctx, e.FileID)
	if err != nil {
		return nil, err
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
			stored, serr := storeWithCtx(ctx, t.Sink, e.ExternalID, reader)
			results[i] = targetResult{stored: stored, err: serr}
			// Unblock the dispatcher if Store bailed before draining the pipe.
			_ = pr.CloseWithError(serr)
		}(i, t, pr)
	}

	fw := &fanoutWriter{writers: writers, dead: make([]bool, n)}
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
		// A cancelled/timed-out ctx is a real abort — surface it so the consumer
		// releases (shutdown) or fails+backs-off (per-object timeout) rather than
		// mislabeling it. With a live ctx, io.ErrClosedPipe means every target pipe
		// died (all sinks failed), NOT a source problem: the per-target results
		// already record each failure, so fall through and let BackupOne write the
		// per-target failed status + metrics. Only a genuine source error (integrity
		// mismatch, fetch read error) short-circuits as "source read".
		switch {
		case ctx.Err() != nil:
			return results, fmt.Errorf("backup aborted: %w", ctx.Err())
		case errors.Is(copyErr, io.ErrClosedPipe):
			// all targets dead — results carry the per-target errors; not a source fault
		default:
			return results, fmt.Errorf("source read: %w", copyErr)
		}
	}
	return results, nil
}

// storeWithCtx runs Sink.Store but honors ctx even when the sink cannot (a
// filesystem sink blocked in a hung fsync/write on a wedged mount — a regular-file
// syscall Go cannot interrupt by closing the fd): on ctx cancellation it returns
// the ctx error and abandons the inner Store goroutine, so wg.Wait / the dispatcher
// unblock and the worker slot is freed rather than pinned forever. The abandoned
// goroutine is bounded (one per wedged object) and its eventual write is an
// idempotent overwrite of identical content-addressed bytes — never corruption.
//
// The Store call is in its OWN goroutine, so a panicking sink is recovered HERE
// (a recover() only catches its own goroutine) and converted to an error — the
// per-target recover() in fanOut cannot reach across this goroutine boundary.
//
// size = -1 is load-bearing: commit is gated on the dispatcher closing the pipes
// AFTER VerifyReader checks the hash; a known byte-count would let a sink finalize
// (e.g. minio single-PUT) on length alone, before verification.
func storeWithCtx(ctx context.Context, sink Sink, hash string, r io.Reader) (n int64, err error) {
	type result struct {
		n   int64
		err error
	}
	ch := make(chan result, 1) // buffered so an abandoned goroutine never blocks
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				ch <- result{0, fmt.Errorf("sink %s panicked: %v", sink.Name(), rec)}
			}
		}()
		sn, serr := sink.Store(ctx, hash, r, -1)
		ch <- result{sn, serr}
	}()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case res := <-ch:
		return res.n, res.err
	}
}

// fanoutWriter writes each chunk to every still-live target pipe. A pipe write
// error marks that target dead (its Store exited) and the rest continue; it stops
// the source copy only once every target is gone. Write is called serially by the
// single io.Copy driver, so f.dead is single-reader; the per-chunk goroutines each
// touch only their own (disjoint) f.dead[i], published to the next Write by
// wg.Wait — no data race.
type fanoutWriter struct {
	writers []*io.PipeWriter
	dead    []bool
}

func (f *fanoutWriter) Write(p []byte) (int, error) {
	// Single-target fast path (the launch config): write inline, no goroutine.
	if last := f.soleLive(); last >= 0 {
		if _, err := f.writers[last].Write(p); err != nil {
			f.dead[last] = true
			return 0, io.ErrClosedPipe
		}
		return len(p), nil
	}
	// ≥2 live targets: write to all concurrently so per-chunk latency is
	// max(targets), not sum — a slow target no longer throttles the fast ones.
	var wg sync.WaitGroup
	for i := range f.writers {
		if f.dead[i] {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := f.writers[i].Write(p); err != nil {
				f.dead[i] = true // own index only
			}
		}(i)
	}
	wg.Wait()
	for i := range f.writers {
		if !f.dead[i] {
			return len(p), nil
		}
	}
	return 0, io.ErrClosedPipe
}

// soleLive returns the index of the only live writer, or -1 if zero or ≥2 are
// live (so the caller either short-circuits closed or takes the concurrent path).
func (f *fanoutWriter) soleLive() int {
	last := -1
	for i := range f.writers {
		if f.dead[i] {
			continue
		}
		if last >= 0 {
			return -1 // ≥2 live
		}
		last = i
	}
	return last
}
