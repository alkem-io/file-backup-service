package domain

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

// seedDrillCorpus stores n objects raw on a memSink and records each stored on the sink's target
// in the ledger, returning their hashes. Drill samples the ledger + restores from the sink.
func seedDrillCorpus(t *testing.T, led *fakeLedger, sink *memSink, n int) []string {
	t.Helper()
	ctx := context.Background()
	var hashes []string
	for i := 0; i < n; i++ {
		content := []byte(fmt.Sprintf("drill object %d", i))
		h, err := sum(bytes.NewReader(content))
		if err != nil {
			t.Fatal(err)
		}
		sink.store[h] = content
		if err := led.RecordBackup(ctx, ObjectMeta{ExternalID: h}, []TargetStatus{{Target: sink.Name(), State: StateStored}}); err != nil {
			t.Fatalf("seed ledger: %v", err)
		}
		hashes = append(hashes, h)
	}
	return hashes
}

func TestDrillAllPass(t *testing.T) {
	led := newFakeLedger()
	sink := newMemSink("t")
	seedDrillCorpus(t, led, sink, 5)
	scratch := t.TempDir()
	out, err := Drill(context.Background(), led, sink, "t", scratch, 0, time.Minute)
	if err != nil {
		t.Fatalf("Drill: %v", err)
	}
	if out.Checked != 5 || out.Passed != 5 || out.Failed != 0 || !out.Pass() {
		t.Fatalf("want checked=5 passed=5 failed=0 pass, got %+v", out)
	}
	// The drill removes each restored file — scratch must be empty after (bounded disk).
	entries, _ := os.ReadDir(scratch)
	if len(entries) != 0 {
		t.Fatalf("drill must clean up restored files, %d left", len(entries))
	}
}

func TestDrillDetectsCorruptObject(t *testing.T) {
	led := newFakeLedger()
	sink := newMemSink("t")
	hashes := seedDrillCorpus(t, led, sink, 3)
	// Corrupt one object's stored bytes so it no longer hashes to its key — the exact silent-loss
	// case a restore drill exists to catch (byte existence alone would pass it).
	sink.store[hashes[1]] = []byte("tampered — does not hash to the key")
	out, err := Drill(context.Background(), led, sink, "t", t.TempDir(), 0, time.Minute)
	if err != nil {
		t.Fatalf("Drill: %v", err)
	}
	if out.Failed != 1 || out.Passed != 2 || out.Pass() {
		t.Fatalf("want failed=1 passed=2 !pass, got %+v", out)
	}
	if len(out.Failures) != 1 || out.Failures[0].Hash != hashes[1] {
		t.Fatalf("the corrupt object must be reported as the failure, got %+v", out.Failures)
	}
}

// TestDrillZeroCheckedIsNotPass: a drill that sampled 0 objects proved NOTHING (a renamed target
// or an empty/wrong ledger yields no rows), so it must NOT read as a pass — else a green gauge
// masks a misconfiguration.
func TestDrillZeroCheckedIsNotPass(t *testing.T) {
	out, err := Drill(context.Background(), newFakeLedger(), newMemSink("t"), "t", t.TempDir(), 0, time.Minute)
	if err != nil {
		t.Fatalf("Drill: %v", err)
	}
	if out.Checked != 0 {
		t.Fatalf("an empty target must sample 0, got %+v", out)
	}
	if out.Pass() {
		t.Fatal("a 0-checked drill must NOT be a pass (it proved nothing)")
	}
}

func TestDrillHonoursSample(t *testing.T) {
	led := newFakeLedger()
	sink := newMemSink("t")
	seedDrillCorpus(t, led, sink, 20)
	out, err := Drill(context.Background(), led, sink, "t", t.TempDir(), 5, time.Minute)
	if err != nil {
		t.Fatalf("Drill: %v", err)
	}
	if out.Checked != 5 {
		t.Fatalf("a --sample of 5 must drill exactly 5 objects, got %d", out.Checked)
	}
}

func TestDrillCancelledStops(t *testing.T) {
	led := newFakeLedger()
	sink := newMemSink("t")
	seedDrillCorpus(t, led, sink, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Drill(ctx, led, sink, "t", t.TempDir(), 0, time.Minute)
	if err == nil {
		t.Fatal("a cancelled drill must return the ctx error, not a clean pass")
	}
}

// cancelOnFetchSink cancels the parent ctx the FIRST time Fetch is called, modelling a SIGTERM
// arriving mid-object — so the drill's per-object result must be classified as an INTERRUPTION,
// not a verify failure.
type cancelOnFetchSink struct {
	memSink
	cancel context.CancelFunc
}

func (s *cancelOnFetchSink) Fetch(ctx context.Context, h string) (io.ReadCloser, error) {
	s.cancel()
	return s.memSink.Fetch(ctx, h)
}

// TestDrillCancelDuringObjectIsInterruptNotFailure (review #6): a cancellation that lands WHILE an
// object is being drilled must abort the drill as interrupted (return the ctx error, Failed==0) —
// NOT count a spurious Failed object + report RED.
func TestDrillCancelDuringObjectIsInterruptNotFailure(t *testing.T) {
	led := newFakeLedger()
	base := newMemSink("t")
	hashes := seedDrillCorpus(t, led, base, 1)
	ctx, cancel := context.WithCancel(context.Background())
	// Copy the seeded object into the cancel-on-fetch sink so restore reads valid bytes but the
	// ctx is cancelled the moment the fetch begins.
	sink := &cancelOnFetchSink{memSink: memSink{stubSink: stubSink{name: "t"}, store: map[string][]byte{hashes[0]: base.store[hashes[0]]}}, cancel: cancel}
	out, err := Drill(ctx, led, sink, "t", t.TempDir(), 0, time.Minute)
	if err == nil {
		t.Fatal("a cancel during the drilled object must abort with the ctx error")
	}
	if out.Failed != 0 {
		t.Fatalf("a cancellation must NOT be counted as a failed object, got Failed=%d", out.Failed)
	}
}
