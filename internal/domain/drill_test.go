package domain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
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
	if out.Checked() != 5 || out.Passed != 5 || out.Failed != 0 || !out.Pass() {
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
	if out.Checked() != 0 {
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
	if out.Checked() != 5 {
		t.Fatalf("a --sample of 5 must drill exactly 5 objects, got %d", out.Checked())
	}
}

// TestDrillSampleLedgerError: a ledger error while sampling the objects to drill aborts the drill
// with that error (it can't prove anything without a work-list).
func TestDrillSampleLedgerError(t *testing.T) {
	led := errStoredLedger{newFakeLedger()}
	if _, err := Drill(context.Background(), led, newMemSink("t"), "t", t.TempDir(), 3, time.Minute); err == nil {
		t.Fatal("a ledger sampling error must abort the drill")
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

// TestDrillInterruptErr (re-review item 4): a COMPLETED sweep (every dispatched object counted) is
// NOT interrupted even if a SIGTERM lands after it — so a completed drill keeps its pass=1 /
// last_success; only a TRUNCATED sweep (a dispatched object went uncounted) is interrupted. This
// FAILS if the guard is reverted to the unconditional `if err==nil { err=ctx.Err() }`.
func TestDrillInterruptErr(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	// Completed (checked==dispatched) + a post-completion SIGTERM → NOT interrupted (nil).
	if err := drillInterruptErr(cancelled, nil, 5, 5); err != nil {
		t.Fatalf("a completed drill (checked==dispatched) must not be interrupted by a post-completion SIGTERM, got %v", err)
	}
	// Truncated (a dispatched object went uncounted) → interrupted.
	if err := drillInterruptErr(cancelled, nil, 4, 5); err == nil {
		t.Fatal("a truncated drill (checked<dispatched) must be interrupted")
	}
	// A sweep error always propagates.
	if err := drillInterruptErr(context.Background(), errors.New("ledger down"), 0, 0); err == nil {
		t.Fatal("a sweep error must propagate")
	}
}

// TestDrillOneGenuineFailureAtSIGTERMCountsFailed (re-review item 3): the drill's per-object
// classifier shares cancelledInFlight with restore-all — a hash-mismatch whose classification
// coincides with a parent SIGTERM must count as a Failed drill object, NOT be swallowed as an
// interruption (which would read the drill green on real corruption). Same sequencing as the
// restore-all counterpart; FAILS if cancelledInFlight is reverted to the imprecise predicate.
func TestDrillOneGenuineFailureAtSIGTERMCountsFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := newMemSink("t")
	h := hashOf("wanted")
	sink.store[h] = []byte("bytes that do NOT hash to the key") // decode → hash-mismatch (non-Canceled)
	var out DrillOutcome
	var mu sync.Mutex
	mu.Lock()
	done := make(chan struct{})
	go func() { drillOne(ctx, sink, h, t.TempDir(), time.Minute, &out, &mu); close(done) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	mu.Unlock()
	<-done
	if out.Failed != 1 || out.Passed != 0 {
		t.Fatalf("a hash-mismatch coinciding with SIGTERM must be a Failed drill object, not swallowed: %+v", out)
	}
}

// panicFetchSink panics on Fetch — a poison object. The restore path's callWithCtx (RestoreObject →
// decodeStream) turns that panic into an error, so the drill records it as a failure instead of
// crashing.
type panicFetchSink struct{ memSink }

func (*panicFetchSink) Fetch(context.Context, string) (io.ReadCloser, error) { panic("boom") }

// TestDrillContainsPoisonObject: a poison object whose sink Fetch PANICS is contained by the restore
// path (callWithCtx) as one GENUINE failure — the drill keeps going and reports it, never crashes.
// (drillOne itself has no panic path of its own — the sink is behind callWithCtx and the os-spine
// doesn't panic — so this defends the reachable containment, not a dead recover.)
func TestDrillContainsPoisonObject(t *testing.T) {
	led := newFakeLedger()
	hashes := seedDrillCorpus(t, led, newMemSink("t"), 1)
	sink := &panicFetchSink{memSink: memSink{stubSink: stubSink{name: "t"}, store: map[string][]byte{hashes[0]: []byte("x")}}}
	out, err := Drill(context.Background(), led, sink, "t", t.TempDir(), 0, time.Minute)
	if err != nil {
		t.Fatalf("a contained poison object must not abort the drill, got %v", err)
	}
	if out.Checked() != 1 || out.Failed != 1 || out.Pass() {
		t.Fatalf("a panicking-Fetch object must be one contained failure, got %+v", out)
	}
}
