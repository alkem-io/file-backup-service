package domain

import (
	"bytes"
	"context"
	"io"
	"slices"
	"testing"
	"time"
)

// TestReconcileOrdersHealthySourcesFirst: repair must try circuit-HEALTHY holders as the
// source before circuit-down ones, so a black-holing holder (whose decode read has no
// stall-drop) isn't picked first and made to burn the whole perObjectTimeout, failing a
// repairable object instead of rotating to a healthy holder. Each bucket is sorted so the
// order is deterministic, not Go map-random. (V10)
func TestReconcileOrdersHealthySourcesFirst(t *testing.T) {
	breaker := NewCircuitBreaker(1, time.Minute)
	breaker.Record("down1", false) // trip two holders' circuits
	breaker.Record("down2", false)
	rc := NewReconciler(newFakeLedger(), nil, time.Minute, "", 0, breaker, 1)
	stored := map[string]bool{"healthyB": true, "down1": true, "healthyA": true, "down2": true}
	got := rc.orderSources(stored)
	want := []string{"healthyA", "healthyB", "down1", "down2"}
	if !slices.Equal(got, want) {
		t.Fatalf("orderSources = %v, want %v (healthy-first, then down; each sorted)", got, want)
	}
}

// TestReconcileRepairsGap: an object stored on A but missing on B is repaired by
// fetching from A and storing to B, and the source A is NOT re-stored (dedup).
func TestReconcileRepairsGap(t *testing.T) {
	ctx := context.Background()
	data := []byte("reconcile me please")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	a.store[h] = data // A holds it (raw)
	b := newMemSink("b")

	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(data))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	rec := NewReconciler(led, []Target{{Sink: a, Codec: CodecNone}, {Sink: b, Codec: CodecNone}}, time.Minute, "", 0, nil, 4)
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

	rec := NewReconciler(led, []Target{{Sink: newMemSink("a")}, {Sink: newMemSink("b")}}, time.Minute, "", 0, nil, 4)
	st, err := rec.Run(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st.Skipped != 1 || st.Repaired != 0 {
		t.Fatalf("stats: %+v", st)
	}
}

// TestReconcileFailsOnCorruptSource (T034 mismatch): the only holder's stored bytes no longer
// hash to the key (silent corruption). Reconcile fetches+decodes from it, the hash-verify fails,
// no other source has it → the object is counted FAILED (not falsely repaired), and the
// destination is never given the bad bytes.
func TestReconcileFailsOnCorruptSource(t *testing.T) {
	ctx := context.Background()
	data := []byte("the genuine content")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	a.store[h] = []byte("CORRUPT bytes that do not hash to h") // A's copy is silently corrupt
	b := newMemSink("b")

	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(data))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	rec := NewReconciler(led, []Target{{Sink: a, Codec: CodecNone}, {Sink: b, Codec: CodecNone}}, time.Minute, "", 0, nil, 4)
	st, err := rec.Run(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st.Failed != 1 || st.Repaired != 0 {
		t.Fatalf("a corrupt-only source must fail the repair, got %+v", st)
	}
	if _, present := b.store[h]; present {
		t.Fatal("the destination must NOT receive the corrupt bytes")
	}
}

// TestReconcileRotatesPastCorruptSource (T034 mismatch): with TWO holders where the first
// (alphabetically) is corrupt and the second is intact, reconcile rotates past the bad source to
// the good one and repairs the missing target — a mismatched source doesn't defeat the repair.
func TestReconcileRotatesPastCorruptSource(t *testing.T) {
	ctx := context.Background()
	data := []byte("rotate past the corrupt holder")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	a.store[h] = []byte("garbage") // A: corrupt
	b := newMemSink("b")
	b.store[h] = data // B: intact
	c := newMemSink("c")

	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(data))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateStored}, {Target: "c", State: StateFailed}})

	rec := NewReconciler(led,
		[]Target{{Sink: a, Codec: CodecNone}, {Sink: b, Codec: CodecNone}, {Sink: c, Codec: CodecNone}},
		time.Minute, "", 0, nil, 4)
	st, err := rec.Run(ctx, 0)
	if err != nil || st.Repaired != 1 {
		t.Fatalf("reconcile must rotate past the corrupt source and repair, got stats=%+v err=%v", st, err)
	}
	if !bytes.Equal(c.store[h], data) {
		t.Fatal("C should hold the intact plaintext, sourced from B")
	}
}

// TestReconcileSurvivesCodecFlip: an object stored zstd on A while A's CONFIGURED codec
// is now CodecNone (operator flipped compression after storage). decodingSource
// arbitrates from the stored bytes (zstd magic), not the stale config, so reconcile
// still repairs — the recovery path survives a config change (the old config-codec
// path would mis-read the zstd bytes as raw and fail on the hash).
func TestReconcileSurvivesCodecFlip(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("flip me "), 30)
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	zr := ZstdReader(bytes.NewReader(data))
	compressed, err := io.ReadAll(zr)
	_ = zr.Close()
	if err != nil {
		t.Fatal(err)
	}
	a.store[h] = compressed // stored zstd
	b := newMemSink("b")

	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(data))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	// A's configured codec is now None (flipped) — the stored bytes are still zstd.
	rec := NewReconciler(led, []Target{{Sink: a, Codec: CodecNone}, {Sink: b, Codec: CodecNone}}, time.Minute, "", 0, nil, 4)
	st, err := rec.Run(ctx, 0)
	if err != nil || st.Repaired != 1 {
		t.Fatalf("reconcile after codec flip: stats=%+v err=%v", st, err)
	}
	if !bytes.Equal(b.store[h], data) {
		t.Fatal("B should hold the decoded plaintext despite the config flip")
	}
}

// TestReconcileRawZstdLookalike: a raw-stored object whose bytes are a VALID zstd frame
// (a .zst upload on a CodecNone target) must still reconcile — the decode falls back to
// raw (like restore) instead of force-decoding as zstd and failing on every source
// forever. Guards the magic-arbiter regression.
func TestReconcileRawZstdLookalike(t *testing.T) {
	ctx := context.Background()
	plaintext := bytes.Repeat([]byte("z"), 200)
	frame, err := io.ReadAll(ZstdReader(bytes.NewReader(plaintext))) // a real zstd frame
	if err != nil {
		t.Fatal(err)
	}
	h, err := sum(bytes.NewReader(frame)) // stored RAW: the FRAME bytes are the object
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	a.store[h] = frame
	b := newMemSink("b")

	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(frame))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	rec := NewReconciler(led, []Target{{Sink: a, Codec: CodecNone}, {Sink: b, Codec: CodecNone}}, time.Minute, "", 0, nil, 4)
	st, err := rec.Run(ctx, 0)
	if err != nil || st.Repaired != 1 {
		t.Fatalf("reconcile raw-zstd-lookalike: stats=%+v err=%v", st, err)
	}
	if !bytes.Equal(b.store[h], frame) {
		t.Fatal("B should hold the raw frame bytes (the object)")
	}
}

// TestReconcileZstdSource: repair works when the source target stored the object zstd —
// it's decoded to plaintext, re-verified, and re-fanned out.
func TestReconcileZstdSource(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("zstd reconcile "), 20)
	h, err := sum(bytes.NewReader(data))
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

	rec := NewReconciler(led, []Target{{Sink: a, Codec: CodecZstd}, {Sink: b, Codec: CodecNone}}, time.Minute, "", 0, nil, 4)
	st, err := rec.Run(ctx, 0)
	if err != nil || st.Repaired != 1 {
		t.Fatalf("reconcile zstd: stats=%+v err=%v", st, err)
	}
	if !bytes.Equal(b.store[h], data) {
		t.Fatal("B should hold the decoded plaintext")
	}
}
