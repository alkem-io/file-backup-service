package domain

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

type fakeCorpus struct{ entries []BackupItem }

// hangingSource blocks until its (per-object) ctx fires — models a fetch stalled after
// headers / a wedged sink.
type hangingSource struct{}

func (hangingSource) FetchContent(ctx context.Context, _ BackupItem) (io.ReadCloser, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestBackfillPerObjectTimeout: a hung object must fail via the per-object timeout and
// the pass must continue — not stall the whole single-threaded backfill indefinitely.
func TestBackfillPerObjectTimeout(t *testing.T) {
	corpus := fakeCorpus{entries: []BackupItem{{ExternalID: hashOf("h1")}, {ExternalID: hashOf("h2")}}}
	p := NewPipeline(hangingSource{}, newFakeLedger(), []Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	// Run ctx is Background (un-cancelled) so ONLY the per-object timeout can end a hang.
	st, err := NewBackfiller(corpus, p, 50*time.Millisecond, 4).Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if st.Failed != 2 || st.Backed != 0 {
		t.Fatalf("stats: %+v (want both objects failed via per-object timeout)", st)
	}
}

// TestBackfillCancelledInFlightIsNotFailed (Alt#1): a SIGTERM that cancels an in-flight object during
// the final drain window (enumerate already finished, so runBoundedPaced returns nil) must count
// Cancelled, NOT Failed — otherwise runBackfill exits nonzero on a fully-successful, resumable pass. A
// per-object DEADLINE (a wedged source, TestBackfillPerObjectTimeout) still counts Failed, since
// cancelledInFlight requires the error itself to be context.Canceled.
func TestBackfillCancelledInFlightIsNotFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the parent (a SIGTERM); hangingSource then returns the per-object ctx's Canceled
	p := NewPipeline(hangingSource{}, newFakeLedger(), []Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	b := NewBackfiller(fakeCorpus{}, p, time.Minute, 1)
	var st BackfillStats
	var mu sync.Mutex
	b.backupOne(ctx, BackupItem{ExternalID: hashOf("x")}, &st, &mu)
	if st.Cancelled != 1 || st.Failed != 0 {
		t.Fatalf("a SIGTERM-cancelled in-flight object must count Cancelled, not Failed: %+v", st)
	}
}

// TestBackfillDefersCircuitOpenTarget: an object whose ONLY gap is a circuit-open
// (persistently-down) target is a DEFER (stored on every reachable target, ledger-recorded,
// reconcile refills the gap) — NOT a Failed. Folding it into Failed made a single-target
// outage exit the whole backfill nonzero for a fully-recoverable state. (V6)
func TestBackfillDefersCircuitOpenTarget(t *testing.T) {
	data := []byte("object whose only target is down")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	breaker := NewCircuitBreaker(1, time.Minute)
	breaker.Record("t", false) // trip the sole target's circuit (threshold 1)
	sink := newMemSink("t")
	p := NewPipeline(fakeSource{data}, newFakeLedger(),
		[]Target{{Sink: sink, Codec: CodecNone}}).WithIsolation(0, breaker)
	corpus := fakeCorpus{entries: []BackupItem{{ExternalID: h}}}

	st, err := NewBackfiller(corpus, p, time.Minute, 4).Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if st.Deferred != 1 || st.Failed != 0 || st.Backed != 0 {
		t.Fatalf("stats: %+v (want deferred=1, not failed — a circuit-open target is a defer)", st)
	}
	if len(sink.store) != 0 {
		t.Fatal("nothing should be fanned out to a circuit-open target")
	}
}

func (c fakeCorpus) EachFile(_ context.Context, fn func(BackupItem) error) error {
	for _, e := range c.entries {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// TestBackfillBacksUpCorpus: backfill stores a pre-existing object and is resumable —
// a second pass dedups against the ledger (no re-store) yet still counts it backed.
func TestBackfillBacksUpCorpus(t *testing.T) {
	data := []byte("legacy object never enqueued")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("t1")
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: sink, Codec: CodecNone}})
	corpus := fakeCorpus{entries: []BackupItem{{ExternalID: h}}}

	st, err := NewBackfiller(corpus, p, time.Minute, 4).Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if st.Backed != 1 || st.Failed != 0 {
		t.Fatalf("stats: %+v (want backed=1 failed=0)", st)
	}
	if !bytes.Equal(sink.store[h], data) {
		t.Fatal("object not stored")
	}

	// Resumable: a second pass finds it already stored (dedup, no re-store) and still
	// reports it backed.
	st2, err := NewBackfiller(corpus, p, time.Minute, 4).Run(context.Background(), 0)
	if err != nil || st2.Backed != 1 || st2.Failed != 0 {
		t.Fatalf("resume: %+v err=%v (want backed=1 failed=0)", st2, err)
	}
}

// TestBackfillSkipsSourceGone: a deleted-before-backfill object is a benign Skip, NOT a
// Failure — so a corpus with routine deletions doesn't fail the pass (mirrors the consumer).
// goneSource (backup_test.go) returns a WRAPPED ErrSourceGone, so this also confirms the
// backfill switch matches via errors.Is, not ==.
func TestBackfillSkipsSourceGone(t *testing.T) {
	corpus := fakeCorpus{entries: []BackupItem{{ExternalID: hashOf("gone1")}, {ExternalID: hashOf("gone2")}}}
	p := NewPipeline(goneSource{}, newFakeLedger(), []Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	st, err := NewBackfiller(corpus, p, time.Minute, 4).Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if st.Skipped != 2 || st.Failed != 0 || st.Backed != 0 {
		t.Fatalf("stats: %+v (want both objects skipped as source-gone, zero failed)", st)
	}
}

// TestNormalizePerObjectTimeoutFloor: a non-positive perObjectTimeout must be floored to the
// default in NewBackfiller/NewReconciler so a direct caller can't produce an already-expired
// per-object deadline that fails every object (CodeRabbit #5). Asserts the shared normalizer
// and that the constructors apply it.
func TestNormalizePerObjectTimeoutFloor(t *testing.T) {
	if got := NormalizePerObjectTimeout(0); got != DefaultPerObjectTimeout {
		t.Fatalf("0 must floor to the default, got %v", got)
	}
	if got := NormalizePerObjectTimeout(-time.Second); got != DefaultPerObjectTimeout {
		t.Fatalf("negative must floor to the default, got %v", got)
	}
	if got := NormalizePerObjectTimeout(5 * time.Second); got != 5*time.Second {
		t.Fatalf("a positive value must pass through, got %v", got)
	}
	if b := NewBackfiller(nil, nil, 0, 1); b.perObjectT != DefaultPerObjectTimeout {
		t.Fatalf("NewBackfiller must floor perObjectT, got %v", b.perObjectT)
	}
	if rc := NewReconciler(nil, nil, 0, "", 0, nil, 1); rc.perObjectT != DefaultPerObjectTimeout {
		t.Fatalf("NewReconciler must floor perObjectT, got %v", rc.perObjectT)
	}
}
