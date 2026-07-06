package domain

import (
	"bytes"
	"context"
	"testing"
)

type fakeCorpus struct{ entries []OutboxEntry }

func (c fakeCorpus) EachFile(_ context.Context, fn func(OutboxEntry) error) error {
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
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("t1")
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: sink, Codec: CodecNone}})
	corpus := fakeCorpus{entries: []OutboxEntry{{ExternalID: h}}}

	st, err := NewBackfiller(corpus, p).Run(context.Background(), 0)
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
	st2, err := NewBackfiller(corpus, p).Run(context.Background(), 0)
	if err != nil || st2.Backed != 1 || st2.Failed != 0 {
		t.Fatalf("resume: %+v err=%v (want backed=1 failed=0)", st2, err)
	}
}
