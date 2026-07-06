package domain

import (
	"bytes"
	"context"
	"io"
	"testing"
)

// TestReconcileRepairsGap: an object stored on A but missing on B is repaired by
// fetching from A and storing to B, and the source A is NOT re-stored (dedup).
func TestReconcileRepairsGap(t *testing.T) {
	ctx := context.Background()
	data := []byte("reconcile me please")
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	a.store[h] = data // A holds it (raw)
	b := newMemSink("b")

	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(data))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	rec := NewReconciler(led, []Target{{Sink: a, Codec: CodecNone}, {Sink: b, Codec: CodecNone}})
	st, err := rec.Run(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st.Repaired != 1 || st.Failed != 0 || st.Skipped != 0 {
		t.Fatalf("stats: %+v", st)
	}
	if !bytes.Equal(b.store[h], data) {
		t.Fatal("B should now hold the object")
	}
	if led.states[h+"/b"] != StateStored {
		t.Fatalf("ledger should record B stored, got %q", led.states[h+"/b"])
	}
}

// TestReconcileSkipsWhenNoSource: an object stored on NO target can't be repaired by
// reconcile (it needs a backfill from the primary store) — counted as skipped.
func TestReconcileSkipsWhenNoSource(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "orphan", Size: 1},
		[]TargetStatus{{Target: "a", State: StateFailed}, {Target: "b", State: StateFailed}})

	rec := NewReconciler(led, []Target{{Sink: newMemSink("a")}, {Sink: newMemSink("b")}})
	st, err := rec.Run(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st.Skipped != 1 || st.Repaired != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

// TestReconcileZstdSource: repair works when the source target stored the object zstd —
// it's decoded to plaintext, re-verified, and re-fanned out.
func TestReconcileZstdSource(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("zstd reconcile "), 20)
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	// A holds the object zstd-compressed (as the zstd codec would have stored it).
	a := newMemSink("a")
	zr := ZstdReader(bytes.NewReader(data))
	compressed, err := io.ReadAll(zr)
	_ = zr.Close()
	if err != nil {
		t.Fatal(err)
	}
	a.store[h] = compressed
	b := newMemSink("b")

	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(data))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	rec := NewReconciler(led, []Target{{Sink: a, Codec: CodecZstd}, {Sink: b, Codec: CodecNone}})
	st, err := rec.Run(ctx, 0)
	if err != nil || st.Repaired != 1 {
		t.Fatalf("reconcile zstd: stats=%+v err=%v", st, err)
	}
	if !bytes.Equal(b.store[h], data) {
		t.Fatal("B should hold the decoded plaintext")
	}
}
