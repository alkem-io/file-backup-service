package domain

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// errAbortedBeforeEOF marks a target whose pipe went dead before consuming the
// full verified stream — it must never be recorded as "stored".
var errAbortedBeforeEOF = errors.New("sink closed before consuming the full stream")

// panicErr renders a recovered panic as an error for the pipeline's per-target
// recover guards (each lives on a different goroutine, so they can't share a defer).
func panicErr(what string, r any) error {
	return fmt.Errorf("%s panicked: %v", what, r)
}

// Per-target ledger states. The dedup reader, the writers, and the metrics labels
// must all agree on these exact strings (StoredTargets treats only StateStored as
// "already has it"), so they live in one place.
const (
	StateStored = "stored"
	StateFailed = "failed"
)

// BookkeepingTimeout bounds a post-cancellation bookkeeping write (the ledger record
// here, the outbox mark-done/fail in the consumer) — long enough for a slow DB, short
// enough not to hang shutdown.
const BookkeepingTimeout = 15 * time.Second

// DetachedBookkeepingCtx derives a bounded context that SURVIVES ctx's cancellation,
// so a per-object timeout or shutdown that fires just as the last store finishes can't
// abort the write that records what already happened (leaving a stored target
// unrecorded and needlessly re-stored, or the done-gate never tripping).
func DetachedBookkeepingCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), BookkeepingTimeout)
}

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
	// Dedup is per content-hash, NOT per outbox row: a fresh row (attempts=0) can
	// reference an already-stored externalID (duplicate content, or a backfill/
	// reconcile re-enqueue), so the StoredTargets read must run unconditionally —
	// skipping it on a "fresh" row would re-PUT duplicates to every target incl. WORM.
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

	results, verified, ferr := p.fanOut(ctx, e, pending)
	if ferr != nil {
		return false, ferr // source integrity / cancellation — outbox retries
	}

	// Ledger bookkeeping MUST survive a per-object timeout that can fire just as the
	// last store finishes — otherwise a target that IS stored goes unrecorded and is
	// needlessly re-stored (or the done-gate never trips).
	bctx, cancel := DetachedBookkeepingCtx(ctx)
	defer cancel()

	// Use the VERIFIED plaintext size when we have it — the outbox size is the
	// producer's unverified hearsay; the hash guarantees these bytes. On an
	// all-targets-fail (verified<0) fall back to the outbox size so the object + its
	// failed statuses still leave a trace.
	objSize := e.Size
	if verified >= 0 {
		objSize = verified
	}

	statuses := make([]TargetStatus, len(pending))
	allStored := true
	for i, t := range pending {
		name := t.Sink.Name()
		if results[i].err != nil {
			p.Metrics.ObjectFailed(name)
			statuses[i] = TargetStatus{Target: name, State: StateFailed}
			allStored = false
			continue
		}
		p.Metrics.ObjectStored(name, results[i].stored)
		statuses[i] = TargetStatus{Target: name, State: StateStored, StoredBytes: results[i].stored}
	}
	// Object row + all statuses in one atomic write (FK parent first, inside the CTE).
	// SizeVerified gates the size UPDATE so an all-fail retry (unverified e.Size, which
	// is 0 when the outbox breadcrumb is unpopulated) can't downgrade an earlier
	// verified size.
	if err := p.Ledger.RecordBackup(bctx, ObjectMeta{
		ExternalID: e.ExternalID, Size: objSize, SizeVerified: verified >= 0,
		CreatedBy: e.CreatedBy, SourceCreatedDate: e.CreatedDate,
	}, statuses); err != nil {
		return false, fmt.Errorf("ledger record: %w", err)
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
// nothing is committed anywhere. Returns per-target results and the VERIFIED byte
// count (>=0 only when the full stream was read and hash-verified, else -1); a
// non-nil error is a SOURCE failure (integrity mismatch or ctx cancellation).
func (p *Pipeline) fanOut(ctx context.Context, e OutboxEntry, targets []Target) ([]targetResult, int64, error) {
	rc, err := p.Source.FetchContent(ctx, e.FileID)
	if err != nil {
		return nil, -1, err
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
			// A panic in this goroutine's own setup/teardown must fail its target, not
			// crash the worker (the sink's Store panic is recovered inside storeWithCtx,
			// on the other side of a goroutine boundary this recover can't reach).
			defer func() {
				if r := recover(); r != nil {
					results[i] = targetResult{err: panicErr("sink "+t.Sink.Name(), r)}
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

	fw := newFanoutWriter(writers)
	// copyErr is a SOURCE failure: a VerifyReader hash mismatch (corrupt source) or
	// ctx cancellation/timeout during the read.
	copyErr := fanoutCopy(fw, vr)
	fw.close() // stop the pump goroutines before closing the pipes
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
			return results, -1, fmt.Errorf("backup aborted: %w", ctx.Err())
		case errors.Is(copyErr, io.ErrClosedPipe):
			// All targets dead — results carry the per-target errors; not a source
			// fault. The stream was NOT read to EOF/hash-verified, so the size is
			// unknown: return -1 so BackupOne falls back to the outbox size rather
			// than freezing a partial vr.Total into the ledger (ON CONFLICT DO NOTHING).
			return results, -1, nil
		default:
			return results, -1, fmt.Errorf("source read: %w", copyErr)
		}
	}
	// copyErr == nil here means the full stream was read and VerifyReader confirmed
	// the hash, so vr.Total is the true verified plaintext size.
	return results, vr.Total, nil
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
// Reading r to EOF is load-bearing: commit is gated on the dispatcher closing the
// pipes AFTER VerifyReader checks the hash; a sink that finalized on a known
// byte-count (e.g. minio single-PUT) would commit before verification, which is why
// Store takes no length.
func storeWithCtx(ctx context.Context, sink Sink, hash string, r io.Reader) (int64, error) {
	ch := make(chan targetResult, 1) // buffered so an abandoned goroutine never blocks
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				ch <- targetResult{err: panicErr("sink "+sink.Name(), rec)}
			}
		}()
		sn, serr := sink.Store(ctx, hash, r)
		ch <- targetResult{stored: sn, err: serr}
	}()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case res := <-ch:
		return res.stored, res.err
	}
}

// fanoutCopy streams vr into fw in ~1 MiB writes. An HTTP-body Read returns only
// ~16–32 KiB, and every fanout Write costs one per-target barrier, so filling a
// larger buffer via io.ReadFull first amortizes the barrier over a big chunk and
// cuts rendezvous/scheduling ~32× — for one 1 MiB buffer of extra memory per
// in-flight object. The hash verification still happens inside vr at true EOF.
// fanoutBufPool reuses the 1 MiB aggregation buffer across objects (one live buffer
// per in-flight object) instead of allocating + GCing it per BackupOne.
var fanoutBufPool = sync.Pool{New: func() any { b := make([]byte, 1<<20); return &b }}

func fanoutCopy(fw *fanoutWriter, vr io.Reader) error {
	bufp := fanoutBufPool.Get().(*[]byte)
	defer fanoutBufPool.Put(bufp)
	buf := *bufp
	for {
		n, rerr := io.ReadFull(vr, buf)
		if n > 0 {
			if _, werr := fw.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		switch {
		case rerr == nil:
			continue // buffer filled — read the next chunk
		case errors.Is(rerr, io.EOF), errors.Is(rerr, io.ErrUnexpectedEOF):
			return nil // end of stream — vr verified the hash (at EOF or a truncated ErrUnexpectedEOF)
		default:
			return rerr // integrity mismatch / ctx cancel / read error
		}
	}
}

// fanoutWriter writes each source chunk to every still-live target pipe using a
// fixed set of per-target PUMP goroutines spawned ONCE (not one per chunk) — a
// large object fanned to N targets would otherwise spawn millions of goroutines.
// Each Write hands the chunk to every live pump and waits (a reused per-chunk
// WaitGroup barrier) so the shared buffer isn't reused before every pump has
// consumed it. Within a chunk the writes run concurrently; across chunks they are
// paced to the slowest live target (the bounded-memory tradeoff — one shared
// buffer). A pipe write error marks that target dead and the rest continue; Write
// fails only once every target is gone. Write is called serially by the single
// fanoutCopy driver, and each pump touches only its own dead[i] (published by the
// barrier), so there is no data race.
type fanoutWriter struct {
	pumps  []chan []byte
	dead   []bool
	chunk  sync.WaitGroup // per-chunk barrier, reused across chunks (Write is serial)
	pumpWg sync.WaitGroup
}

func newFanoutWriter(writers []*io.PipeWriter) *fanoutWriter {
	n := len(writers)
	f := &fanoutWriter{pumps: make([]chan []byte, n), dead: make([]bool, n)}
	for i := range writers {
		f.pumps[i] = make(chan []byte)
		f.pumpWg.Add(1)
		go func(i int, w *io.PipeWriter) {
			defer f.pumpWg.Done()
			for chunk := range f.pumps[i] {
				if _, err := w.Write(chunk); err != nil {
					f.dead[i] = true // own index; published to Write by chunk.Wait()
				}
				f.chunk.Done()
			}
		}(i, writers[i])
	}
	return f
}

func (f *fanoutWriter) Write(p []byte) (int, error) {
	for i := range f.pumps {
		if !f.dead[i] {
			f.chunk.Add(1)
			f.pumps[i] <- p
		}
	}
	f.chunk.Wait()
	for i := range f.dead {
		if !f.dead[i] {
			return len(p), nil
		}
	}
	return 0, io.ErrClosedPipe
}

// close stops the pumps. Safe once fanoutCopy has returned: every pump finished its
// last chunk (the final Write's barrier joined it) and is parked on its channel, so
// closing the channels lets each range-loop exit cleanly.
func (f *fanoutWriter) close() {
	for i := range f.pumps {
		close(f.pumps[i])
	}
	f.pumpWg.Wait()
}
