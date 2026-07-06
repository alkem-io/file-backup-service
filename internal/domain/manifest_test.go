package domain

import (
	"context"
	"strings"
	"testing"
)

// TestWriteManifests: the snapshot to each target is a JSONL line per ledger object.
func TestWriteManifests(t *testing.T) {
	led := newFakeLedger()
	ctx := context.Background()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "hashA", Size: 10}, []TargetStatus{{Target: "t", State: StateStored}})
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "hashB", Size: 20}, []TargetStatus{{Target: "t", State: StateStored}})

	sink := newMemSink("t")
	if err := WriteManifests(ctx, led, []Target{{Sink: sink}}, "snap.jsonl"); err != nil {
		t.Fatalf("WriteManifests: %v", err)
	}
	got := string(sink.store["_manifest/snap.jsonl"])
	lines := strings.Count(strings.TrimSpace(got), "\n") + 1
	if lines != 2 {
		t.Fatalf("expected 2 JSONL lines, got %d: %q", lines, got)
	}
	for _, want := range []string{`"externalID":"hashA"`, `"externalID":"hashB"`, `"size":20`} {
		if !strings.Contains(got, want) {
			t.Errorf("manifest missing %q; got %q", want, got)
		}
	}
}

// TestWriteManifestsEmptyLedger: an empty ledger writes an empty (valid) manifest, not
// an error — the sink's own empty-object handling covers the 0-byte case.
func TestWriteManifestsEmptyLedger(t *testing.T) {
	sink := newMemSink("t")
	if err := WriteManifests(context.Background(), newFakeLedger(), []Target{{Sink: sink}}, "snap.jsonl"); err != nil {
		t.Fatalf("empty-ledger manifest should not error: %v", err)
	}
	if _, ok := sink.store["_manifest/snap.jsonl"]; !ok {
		t.Fatal("expected an (empty) manifest object to be written")
	}
}
