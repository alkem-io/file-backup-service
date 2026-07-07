package domain

import (
	"bytes"
	"context"
	"io"
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
