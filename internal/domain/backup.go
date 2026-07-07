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

// PanicErr renders a recovered panic as an error — the one owner of the "<what>
// panicked: <v>" convention, shared by the pipeline's per-target recover guards (each on
// a different goroutine, so they can't share a defer) and the CLI's startup-check guard.
func PanicErr(what string, r any) error {
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
	// Worm marks a write-once, read-denying target (PutObject-only creds): audit expects
	// its Exists to always error, so an all-errored Worm target is not an alert.
	Worm bool
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
	// Circuit trips a persistently-down target out of the fan-out so an object whose only
	// gap is that target is deferred, not dead-lettered (T017a). Nil (batch paths) disables it.
	Circuit *CircuitBreaker
	// StallTimeout is the per-chunk drain deadline: a target that doesn't consume a
	// fan-out chunk within it is DROPPED individually (its pipe closed) so one hung sink
	// can't stall the healthy targets on the shared barrier. 0 (batch paths) = unbounded.
	StallTimeout time.Duration
}

// NewPipeline constructs a Pipeline.
func NewPipeline(src Source, ledger Ledger, targets []Target) *Pipeline {
	return &Pipeline{Source: src, Ledger: ledger, Targets: targets, Metrics: Nop{}}
}

// WithIsolation gives a pipeline the same per-target hung-target isolation serve uses: a
// stall-drop (drop a target not draining a fan-out chunk within stall, BEFORE the object
// timeout) and a circuit breaker (skip a persistently-down target). The batch/DR paths
// (backfill, reconcile) MUST set these too — a bare pipeline has StallTimeout=0 (no drop)
// and Circuit=nil, so a black-holing target wedges every object for the full per-object
// timeout with no isolation. stall<=0 / circuit==nil leave the respective mechanism off
// (the in-memory-sink tests). Returns p for fluent construction.
func (p *Pipeline) WithIsolation(stall time.Duration, circuit *CircuitBreaker) *Pipeline {
	p.StallTimeout = stall
	p.Circuit = circuit
	return p
}

// RunParallel runs run(item) for every item CONCURRENTLY, each guarded by a recover that
// converts a panic into an error labelled by label(item) — so one item's driver panic
// fails just that item, never the process (a recover only catches its own goroutine). The
// one owner of the concurrent-with-per-item-recover scaffold. Returns per-item errors in
// order (nil = ok).
func RunParallel[T any](items []T, label func(T) string, run func(T) error) []error {
	errs := make([]error, len(items))
	var wg sync.WaitGroup
	for i := range items {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs[i] = PanicErr(label(items[i]), r)
				}
			}()
			errs[i] = run(items[i])
		}(i)
	}
	wg.Wait()
	return errs
}

// TargetNames returns the sink names of targets — the allTargets argument the ledger's
// gap/coverage/RPO queries take. One owner so the reconcile work-list and the gauges
// compute over the same name set.
func TargetNames(targets []Target) []string {
	names := make([]string, len(targets))
	for i, t := range targets {
		names[i] = t.Sink.Name()
	}
	return names
}

// BackupOne stores the object on every target that still needs it. The source is fetched
// ONCE and fanned out to all targets concurrently (streamed, bounded memory), so adding a
// target doesn't multiply read load on the primary store. Dedup is answered by our own
// ledger (never by re-reading a target), so it works with PutObject-only WORM credentials.
//
// done is true only when EVERY target is stored (the symmetric done-gate). deferred is
// true when the object couldn't complete ONLY because a target's circuit is open (a
// persistently-down target): the caller re-queues it WITHOUT an attempt (T017a), so a
// single-target outage doesn't march the corpus to dead-letter — reconcile refills the
// gap when the target returns. A flaky target leaves the object not-done for retry while
// never blocking the healthy ones.
func (p *Pipeline) BackupOne(ctx context.Context, e BackupItem) (done, deferred bool, err error) {
	return p.backupFrom(ctx, p.Source, e)
}

// backupFrom is BackupOne parameterized on the source, so reconcile can supply a
// target-backed source per call without mutating shared Pipeline state.
func (p *Pipeline) backupFrom(ctx context.Context, src Source, e BackupItem) (done, deferred bool, err error) {
	// Dedup is per content-hash, NOT per outbox row: a fresh row (attempts=0) can
	// reference an already-stored externalID (duplicate content, or a backfill/
	// reconcile re-enqueue), so the StoredTargets read must run unconditionally —
	// skipping it on a "fresh" row would re-PUT duplicates to every target incl. WORM.
	stored, err := p.Ledger.StoredTargets(ctx, e.ExternalID) // one query, not N
	if err != nil {
		return false, false, fmt.Errorf("ledger read: %w", err)
	}
	pending, skippedOpen := p.pendingTargets(stored)
	if len(pending) == 0 {
		// Nothing to fan out: fully done unless a down target was skipped, in which case
		// the object is DEFERRED (re-claimable, NO attempt) until that target recovers.
		return !skippedOpen, skippedOpen, nil
	}

	results, verified, ferr := p.fanOut(ctx, src, e, pending)
	// A SOURCE error records nothing — the object is bad or was never fetched. That is
	// either (a) nil results: the fetch itself failed BEFORE any per-target result
	// (including a ctx-cancelled fetch, where ctx.Err()!=nil — so this must NOT rely on
	// ctx.Err() alone, or the status loop below indexes a nil slice and panics), or (b)
	// a source integrity error with ctx still live. A ctx-ABORT (per-object timeout /
	// shutdown) that produced partial per-target results falls through to record the
	// targets that DURABLY stored (err==nil = passed their own ctx gate), so a retry
	// doesn't re-store them.
	aborted := ferr != nil
	if aborted && (results == nil || ctx.Err() == nil) {
		return false, false, ferr
	}

	if shouldRecordCircuit(ctx, aborted, verified >= 0) {
		p.recordCircuit(pending, results)
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

	statuses, healthyFailed, allStored := p.classifyFanout(pending, results, aborted)
	// Object row + all statuses in one atomic write (FK parent first, inside the CTE).
	// SizeVerified gates the size UPDATE so an all-fail retry (unverified e.Size, which
	// is 0 when the outbox breadcrumb is unpopulated) can't downgrade an earlier
	// verified size.
	if len(statuses) > 0 {
		if err := p.Ledger.RecordBackup(bctx, ObjectMeta{
			ExternalID: e.ExternalID, Size: objSize, SizeVerified: verified >= 0,
			CreatedBy: e.CreatedBy, SourceCreatedDate: e.CreatedDate,
		}, statuses); err != nil {
			return false, false, fmt.Errorf("ledger record: %w", err)
		}
	}
	// A reachable target genuinely failed → Fail (attempt). On a per-object TIMEOUT that
	// target is wedging → surface ferr so the consumer's timeout metric fires.
	if healthyFailed {
		if aborted {
			return false, false, ferr
		}
		return false, false, nil // consumer's default (!ok) Fails it
	}
	// No reachable target failed. done iff every target is stored; otherwise the ONLY gaps
	// are down/timed-out targets → DEFERRED (retry later, NO attempt; reconcile refills).
	done = allStored && !skippedOpen && !aborted
	return done, !done, nil
}

// pendingTargets returns the targets that still need the object — not already stored
// (dedup) and not circuit-OPEN — plus whether a circuit-open (down) target was skipped
// (a down target isn't fanned to: it would stall; reconcile refills its gap).
func (p *Pipeline) pendingTargets(stored map[string]bool) (pending []Target, skippedOpen bool) {
	pending = make([]Target, 0, len(p.Targets))
	for _, t := range p.Targets {
		name := t.Sink.Name()
		if stored[name] {
			p.Metrics.ObjectDedup(name)
			continue
		}
		if p.Circuit != nil && p.Circuit.Open(name) {
			skippedOpen = true
			continue
		}
		pending = append(pending, t)
	}
	return pending, skippedOpen
}

// shouldRecordCircuit decides whether to fold per-target results into the circuit breaker.
// Fold on the clean path (each target's outcome is its own), OR on a per-object TIMEOUT
// where the source stream was fully read + hash-VERIFIED (streamVerified). The stall-drop
// handles a mid-STREAM hang; a FINALIZATION hang (a target that drained + verified the
// stream, then hangs on S3 CompleteMultipartUpload / fsync+rename) reaches the aborted path
// with the copy already complete. streamVerified (fanOut returns verified size >=0 only when
// copyErr==nil, i.e. the whole stream was delivered to every pipe and VerifyReader confirmed
// the hash) proves it's a target-SPECIFIC finalize hang, NOT a shared-barrier stall — in a
// barrier stall the copy blocks on the hung target so copyErr is the ctx deadline (verified
// = -1), and folding would trip healthy targets in lockstep. Unlike the old anyStored signal
// this also holds for a SINGLE-target deployment (or an all-targets-finalize-hang), where no
// sibling stored: streamVerified is true regardless of how many targets stored, so a
// finalize-hung sole target now trips (bounding storeWithCtx's abandoned-goroutine leak to
// ~threshold objects, after which the object DEFERS) instead of never tripping and leaking
// an fd + .partial per object over the whole outage.
func shouldRecordCircuit(ctx context.Context, aborted, streamVerified bool) bool {
	if !aborted {
		return true
	}
	return errors.Is(ctx.Err(), context.DeadlineExceeded) && streamVerified
}

// recordCircuit folds each fanned target's Store result into the circuit breaker.
func (p *Pipeline) recordCircuit(pending []Target, results []targetResult) {
	if p.Circuit == nil {
		return
	}
	for i, t := range pending {
		p.Circuit.Record(t.Sink.Name(), results[i].err == nil)
	}
}

// classifyFanout turns per-target results into ledger statuses + outcome flags. A DOWN
// target's failure (circuit-open, incl. one just tripped) is a TEMPORARY gap — not
// recorded failed and not counted against the object (it defers; reconcile refills). A
// REACHABLE target's failure is genuine (recorded + Failed). On abort a failed target's
// status is omitted (its state is unknown), but healthyFailed still reflects it.
func (p *Pipeline) classifyFanout(pending []Target, results []targetResult, aborted bool) (statuses []TargetStatus, healthyFailed, allStored bool) {
	statuses = make([]TargetStatus, 0, len(pending))
	allStored = true
	for i, t := range pending {
		name := t.Sink.Name()
		if results[i].err != nil {
			allStored = false
			// Down (a PURE read), not Open: classifying results must not mutate the breaker
			// — on the aborted path recordCircuit is skipped precisely to leave circuit
			// state untouched, and Open() would steal the target's half-open probe here.
			if p.Circuit != nil && p.Circuit.Down(name) {
				continue
			}
			healthyFailed = true
			if !aborted {
				p.Metrics.ObjectFailed(name)
				statuses = append(statuses, TargetStatus{Target: name, State: StateFailed})
			}
			continue
		}
		p.Metrics.ObjectStored(name, results[i].stored)
		statuses = append(statuses, TargetStatus{Target: name, State: StateStored, StoredBytes: results[i].stored})
	}
	return statuses, healthyFailed, allStored
}

type targetResult struct {
	stored int64
	err    error
	sawEOF bool // the sink read its stream to EOF — proof it consumed the full object
}

// eofReader records whether the consumer read the wrapped stream to EOF. It is the
// robust "did the sink actually consume the verified stream" signal: unlike the
// pipe-liveness heuristic, it works through a codec wrapper (a CodecZstd sink reads
// the encoder's output, not the source pipe, so pipe-liveness can't see a
// non-consuming zstd sink), so a sink that returns success without reading to EOF is
// caught for every codec.
type eofReader struct {
	r      io.Reader
	sawEOF bool
}

func (e *eofReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if errors.Is(err, io.EOF) {
		e.sawEOF = true
	}
	return n, err
}

// fanOut fetches the source once and copies the hash-verified plaintext to every
// target concurrently (each applying its own codec). A per-target failure is
// isolated — its pipe is dropped and the remaining targets keep receiving bytes.
// A corrupt source (VerifyReader fails at EOF) aborts every target's write, so
// nothing is committed anywhere. Returns per-target results and the VERIFIED byte
// count (>=0 only when the full stream was read and hash-verified, else -1); a
// non-nil error is a SOURCE failure (integrity mismatch or ctx cancellation).
func (p *Pipeline) fanOut(ctx context.Context, src Source, e BackupItem, targets []Target) ([]targetResult, int64, error) {
	rc, err := src.FetchContent(ctx, e)
	if err != nil {
		return nil, -1, err
	}
	defer func() { _ = rc.Close() }()
	vr := NewVerifyReader(rc, e.ExternalID, maxObjectBytes) // cap so a stored object is always restorable

	n := len(targets)
	results := make([]targetResult, n)
	writers := make([]*io.PipeWriter, n)
	readers := make([]*io.PipeReader, n)
	cancels := make([]context.CancelFunc, n)
	var wg sync.WaitGroup
	for i, t := range targets {
		pr, pw := io.Pipe()
		writers[i] = pw
		readers[i] = pr
		// Per-target ctx so the stall-drop can abandon ONE hung target's Store without
		// touching the healthy targets: closing pr unblocks a Store blocked READING the
		// pipe; cancelling tctx abandons a Store blocked WRITING to its own wedged backend
		// (having already drained the pipe) — either way fanOut's wg.Wait() can't hang.
		tctx, tcancel := context.WithCancel(ctx)
		cancels[i] = tcancel
		wg.Add(1)
		go func(i int, t Target, pr *io.PipeReader, tctx context.Context) {
			defer wg.Done()
			defer tcancel()
			// A panic in this goroutine's own setup/teardown must fail its target, not
			// crash the worker (the sink's Store panic is recovered inside storeWithCtx,
			// on the other side of a goroutine boundary this recover can't reach).
			defer func() {
				if r := recover(); r != nil {
					results[i] = targetResult{err: PanicErr("sink "+t.Sink.Name(), r)}
					_ = pr.CloseWithError(errAbortedBeforeEOF)
				}
			}()
			var reader io.Reader = pr
			if t.Codec == CodecZstd {
				zr := ZstdReader(pr)
				defer func() { _ = zr.Close() }()
				reader = zr
			}
			er := &eofReader{r: reader}
			res := storeWithCtx(tctx, t.Sink, e.ExternalID, er)
			results[i] = res
			// Unblock the dispatcher if Store bailed before draining the pipe.
			_ = pr.CloseWithError(res.err)
		}(i, t, pr, tctx)
	}

	// drop isolates a hung target i: close its READER (unblocks the pump's blocked write
	// AND a Store blocked reading the pipe) AND cancel its store ctx (abandons a Store
	// blocked writing to its own wedged backend, having already drained the pipe).
	drop := func(i int) {
		_ = readers[i].CloseWithError(errStalled)
		cancels[i]()
	}
	fw := newFanoutWriter(writers, drop, p.StallTimeout)
	// copyErr is a SOURCE failure: a VerifyReader hash mismatch (corrupt source) or
	// ctx cancellation/timeout during the read. Recover a PANIC in the source read (vr.Read
	// on a wrapped/custom reader) here — like every other concurrent driver — and convert it
	// to copyErr, so the teardown below (fw.close + pw.CloseWithError + wg.Wait) always runs.
	// Without this, a panic would unwind past the teardown and permanently leak the pump +
	// store goroutines (parked on an unclosed pipe/channel), masked by the caller's recover.
	var copyErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				copyErr = PanicErr("fanout source", r)
			}
		}()
		copyErr = fanoutCopy(fw, vr)
	}()
	fw.close() // stop the pump goroutines before closing the pipes
	for _, pw := range writers {
		_ = pw.CloseWithError(copyErr)
	}
	wg.Wait()
	// A sink that reported success but did NOT read its stream to EOF did not receive
	// the full verified object — force it to failed so a non-consuming sink can never
	// be recorded as "stored". sawEOF (not the pipe-liveness fw.dead[i]) is used
	// because a CodecZstd sink reads the encoder's output, not the source pipe, so
	// pipe-liveness can't detect its non-consumption.
	for i := range results {
		if results[i].err == nil && !results[i].sawEOF {
			results[i].err = errAbortedBeforeEOF
		}
	}
	// A cancelled/timed-out ctx is a real abort — surface it so the consumer releases
	// (shutdown) or fails+backs-off (per-object timeout). This is checked BEFORE copyErr:
	// a timeout during a sink's FINALIZATION phase (S3 CompleteMultipartUpload / a slow
	// fsync+rename, AFTER the stream already verified so copyErr==nil) leaves the abort
	// only in a per-target results[i].err, and this is the sole place that surfaces it as
	// the abort/timeout the metric + retry path key on. If the stream verified before the
	// abort (copyErr==nil), vr.Total is the true size so committed targets record it;
	// otherwise the size is unknown (-1).
	if ctx.Err() != nil {
		size := int64(-1)
		if copyErr == nil {
			size = vr.Total
		}
		return results, size, fmt.Errorf("backup aborted: %w", ctx.Err())
	}
	if copyErr != nil {
		// With a live ctx, io.ErrClosedPipe means every target pipe died (all sinks
		// failed), NOT a source problem: the per-target results already record each
		// failure, so fall through and let BackupOne write the per-target failed status +
		// metrics. The stream was NOT hash-verified, so size is unknown (-1) → BackupOne
		// falls back to the outbox size (SizeVerified=false, can't downgrade a later
		// verified size). Only a genuine source error short-circuits as "source read".
		if errors.Is(copyErr, io.ErrClosedPipe) {
			return results, -1, nil
		}
		return results, -1, fmt.Errorf("source read: %w", copyErr)
	}
	// copyErr == nil and ctx live: the full stream was read and VerifyReader confirmed
	// the hash, so vr.Total is the true verified plaintext size.
	return results, vr.Total, nil
}

// runAbandonable runs fn in its OWN goroutine and returns its result, but honors ctx even when
// fn cannot (a filesystem call blocked in a hung fsync/write on a wedged mount — a regular-file
// syscall Go can't interrupt): on ctx cancellation it returns onCancel() and ABANDONS the
// goroutine (bounded, one per wedged op), so the caller unblocks rather than pinning forever.
// The result channel is BUFFERED (cap 1) so the abandoned goroutine never blocks on its send. A
// panic in fn is recovered HERE (a recover only catches its own goroutine — the caller's recover
// can't reach across this boundary) and mapped via onPanic. The single owner of this
// abandon + buffered-chan + recover primitive, shared by storeWithCtx and callWithCtx.
func runAbandonable[T any](ctx context.Context, fn func() T, onCancel func() T, onPanic func(recovered any) T) T {
	ch := make(chan T, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- onPanic(r)
			}
		}()
		ch <- fn()
	}()
	select {
	case <-ctx.Done():
		return onCancel()
	case res := <-ch:
		return res
	}
}

// callWithCtx runs fn honoring ctx even when fn cannot (a hung fsync/write on a wedged mount Go
// can't interrupt): on cancellation it returns ctx.Err() and abandons the goroutine; a panic in
// fn becomes an error so a panicking sink op fails its target instead of crashing the process.
// The manifest writer's abandonment path. See runAbandonable.
func callWithCtx(ctx context.Context, fn func() error) error {
	return runAbandonable(ctx, fn,
		func() error { return ctx.Err() },
		func(r any) error { return PanicErr("sink op", r) })
}

// storeWithCtx runs Sink.Store honoring ctx even when the sink cannot (a filesystem sink blocked
// in a hung fsync/write on a wedged mount Go cannot interrupt by closing the fd): on ctx
// cancellation it returns the ctx error and abandons the inner Store goroutine so wg.Wait / the
// dispatcher unblock and the worker slot is freed. The abandoned goroutine is bounded (one per
// wedged object); if it ever completes, the filesystem sink's ctx-gate refuses to commit a
// cancelled write (temp removed) and an S3 sink writes identical content-addressed bytes — never
// corruption. er.sawEOF is read INSIDE fn (the goroutine that owns er) and sent via the channel,
// giving the fanOut reader a happens-before; the onCancel path never touches er, so the abandoned
// goroutine's sawEOF write never races. Reading r to EOF is load-bearing: commit is gated on the
// dispatcher closing the pipes AFTER VerifyReader checks the hash, so Store takes no length (a
// known-length finalize, e.g. minio single-PUT, would commit before verification).
func storeWithCtx(ctx context.Context, sink Sink, hash string, er *eofReader) targetResult {
	return runAbandonable(ctx,
		func() targetResult {
			sn, serr := sink.Store(ctx, hash, er)
			return targetResult{stored: sn, err: serr, sawEOF: er.sawEOF}
		},
		func() targetResult { return targetResult{err: ctx.Err()} },
		func(r any) targetResult { return targetResult{err: PanicErr("sink "+sink.Name(), r)} })
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

// errStalled marks a target dropped from the fan-out because it did not consume a chunk
// within the stall timeout — a HUNG sink (a black-holed endpoint / wedged mount that
// accepts the write but never drains). It surfaces as that target's own failure so its
// circuit records it and later objects skip it (T017a) without the hung target ever
// stalling the healthy targets.
var errStalled = errors.New("sink stalled: did not drain a chunk within the stall timeout")

// fanoutWriter writes each source chunk to every still-live target pipe using a fixed set
// of per-target PUMP goroutines spawned ONCE (not one per chunk) — a large object fanned
// to N targets would otherwise spawn millions of goroutines. Each Write hands the chunk
// to every live pump and waits for all to consume it (so the shared buffer isn't reused
// before every pump has copied it — the bounded-memory tradeoff). Within a chunk the
// writes run concurrently; across chunks they are paced to the slowest LIVE target — but
// a pump that doesn't drain a chunk within stall is DROPPED (its reader closed so its
// write unblocks with an error), so ONE hung target can't stall the healthy targets
// (runtime FR-012 isolation). A pipe write error marks that target dead and the rest
// continue; Write fails only once every target is gone. Write is called serially by the
// single fanoutCopy driver; each pump publishes its own dead[i] to Write via the done
// channel (happens-before), so there is no data race.
type fanoutWriter struct {
	pumps   []chan []byte
	drop    func(int) // isolate a stalled target i (close reader + cancel its store)
	dead    []bool
	pending []bool        // Write-only: which dispatched pumps haven't finished this chunk
	done    chan int      // a pump signals its index after consuming (or erroring on) a chunk
	stall   time.Duration // per-chunk drain deadline; 0 = unbounded (batch paths)
	timer   *time.Timer   // ONE reused stall timer (Reset per chunk), not a fresh alloc per 1 MiB
	pumpWg  sync.WaitGroup
}

func newFanoutWriter(writers []*io.PipeWriter, drop func(int), stall time.Duration) *fanoutWriter {
	n := len(writers)
	f := &fanoutWriter{
		pumps:   make([]chan []byte, n),
		drop:    drop,
		dead:    make([]bool, n),
		pending: make([]bool, n),
		done:    make(chan int, n), // buffered so a pump's signal never blocks (Write may be timing out)
		stall:   stall,
	}
	if stall > 0 {
		f.timer = time.NewTimer(stall)
		f.timer.Stop() // created stopped; collect Resets it per chunk (Go 1.23+ Reset semantics)
	}
	for i := range writers {
		f.pumps[i] = make(chan []byte)
		f.pumpWg.Add(1)
		go func(i int, w *io.PipeWriter) {
			defer f.pumpWg.Done()
			for chunk := range f.pumps[i] {
				if _, err := w.Write(chunk); err != nil {
					f.dead[i] = true // set BEFORE the done send, which publishes it to Write
				}
				f.done <- i
			}
		}(i, writers[i])
	}
	return f
}

func (f *fanoutWriter) Write(p []byte) (int, error) {
	dispatched := 0
	for i := range f.pumps {
		if !f.dead[i] {
			f.pending[i] = true
			f.pumps[i] <- p // a live pump finished its last chunk, so it's ready to receive
			dispatched++
		}
	}
	f.collect(dispatched)
	for i := range f.dead {
		if !f.dead[i] {
			return len(p), nil
		}
	}
	return 0, io.ErrClosedPipe
}

// collect waits for all dispatched pumps to finish the chunk, dropping any that stall
// past f.stall (their reader is closed so their write unblocks with errStalled → dead).
func (f *fanoutWriter) collect(dispatched int) {
	var tick <-chan time.Time
	if f.timer != nil {
		f.timer.Reset(f.stall) // reuse the one timer instead of allocating per chunk
		defer f.timer.Stop()
		tick = f.timer.C
	}
	for got := 0; got < dispatched; {
		select {
		case i := <-f.done:
			f.pending[i] = false
			got++
		case <-tick:
			tick = nil // fire once, then block-collect the forced completions below
			// Drain already-finished pumps first, so a pump whose done-signal is already
			// buffered isn't dropped. A residual ~ns window remains: a pump whose w.Write
			// just returned but hasn't yet run its (buffered) `done <- i` send is still seen
			// as pending and dropped — a spurious drop of a healthy target right at the
			// stall boundary. Accepted: bounded (object retried, no commit/corruption —
			// nothing commits without EOF), per-index (siblings untouched), self-healing, and
			// an ~ns window against a 60s stall; a grace-period second timer would add real
			// complexity for a negligible gain.
			for drained := false; !drained; {
				select {
				case i := <-f.done:
					f.pending[i] = false
					got++
				default:
					drained = true
				}
			}
			for i := range f.pending {
				if f.pending[i] { // still outstanding at the deadline: isolate it (its pump write unblocks with errStalled)
					f.drop(i)
				}
			}
		}
	}
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
