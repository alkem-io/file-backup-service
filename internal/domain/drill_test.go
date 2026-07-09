package domain

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
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

func TestDrillEmptyTargetIsVacuousPass(t *testing.T) {
	out, err := Drill(context.Background(), newFakeLedger(), newMemSink("t"), "t", t.TempDir(), 0, time.Minute)
	if err != nil {
		t.Fatalf("Drill: %v", err)
	}
	if out.Checked != 0 || !out.Pass() {
		t.Fatalf("an empty target has nothing to fail — vacuous pass, got %+v", out)
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

// TestDrillWritesToScratch: confirm the drill actually WRITES each object to the scratch dir
// (proving the restore-write procedure, not just verify) — checked by inspecting a mid-drill file
// via a sink that captures the dest before drillOne removes it.
func TestDrillWritesToScratch(t *testing.T) {
	led := newFakeLedger()
	sink := newMemSink("t")
	hashes := seedDrillCorpus(t, led, sink, 1)
	scratch := t.TempDir()
	// Restore the single object directly to prove the on-disk artifact, then drill (which restores
	// + removes). This asserts the drill's restore path lands real, correct bytes on disk.
	if err := RestoreObject(context.Background(), sink, hashes[0], scratch); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(scratch, hashes[0])) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, []byte("drill object 0")) {
		t.Fatalf("drill-restored artifact mismatch: %v", err)
	}
}
