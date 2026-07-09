package domain

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// manifestSink is a stubSink whose LatestManifest returns a fixed JSONL manifest (or an error),
// satisfying the inventoryReader capability.
type manifestSink struct {
	stubSink
	manifest []byte
	err      error
}

func (s manifestSink) LatestManifest(context.Context) (io.ReadCloser, error) {
	if s.err != nil {
		return nil, s.err
	}
	return io.NopCloser(bytes.NewReader(s.manifest)), nil
}

// manifestOf renders a JSONL manifest listing the given externalIDs (the fields the diff reads).
func manifestOf(ids ...string) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for _, id := range ids {
		_ = enc.Encode(manifestLine{ExternalID: id, Size: 1})
	}
	return b.Bytes()
}

func storeOnTarget(t *testing.T, led *fakeLedger, target string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := led.RecordBackup(context.Background(), ObjectMeta{ExternalID: id},
			[]TargetStatus{{Target: target, State: StateStored}}); err != nil {
			t.Fatalf("seed ledger: %v", err)
		}
	}
}

func TestAuditInventoryExtraAndMissing(t *testing.T) {
	led := newFakeLedger()
	inLedger, inBoth, onlyManifest := hashOf("a"), hashOf("b"), hashOf("c")
	// Ledger records inLedger + inBoth stored on "t"; the manifest lists inBoth + onlyManifest.
	storeOnTarget(t, led, "t", inLedger, inBoth)
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(inBoth, onlyManifest)}

	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err != nil {
		t.Fatalf("AuditInventory: %v", err)
	}
	a := rep.Targets[0]
	if a.Extra != 1 { // onlyManifest is in the manifest but not ledger-stored
		t.Fatalf("want Extra=1 (orphan/lost ledger record), got %d", a.Extra)
	}
	if a.Missing != 1 { // inLedger is ledger-stored but not in the manifest (newer than the snapshot)
		t.Fatalf("want Missing=1, got %d", a.Missing)
	}
	if a.ManifestSize != 2 {
		t.Fatalf("want ManifestSize=2, got %d", a.ManifestSize)
	}
	if !a.Failed() {
		t.Fatal("a target with an extra (orphan) manifest object must fail the target→ledger direction")
	}
	if err := rep.FailErr(); err == nil {
		t.Fatal("FailErr must be non-nil when a target has extras")
	}
}

func TestAuditInventoryCleanAndMissingOnlyPasses(t *testing.T) {
	led := newFakeLedger()
	a1, a2 := hashOf("x"), hashOf("y")
	storeOnTarget(t, led, "t", a1, a2)
	// The manifest is a SUBSET of the ledger (a1 only) — a2 was stored after the last snapshot.
	// Missing>0 (a2), Extra=0 → informational, NOT a failure.
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(a1)}
	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err != nil {
		t.Fatalf("AuditInventory: %v", err)
	}
	a := rep.Targets[0]
	if a.Extra != 0 || a.Missing != 1 {
		t.Fatalf("want Extra=0 Missing=1, got Extra=%d Missing=%d", a.Extra, a.Missing)
	}
	if a.Failed() {
		t.Fatal("Missing-only (snapshot staleness) must NOT fail the audit")
	}
	if err := rep.FailErr(); err != nil {
		t.Fatalf("a missing-only report must pass, got %v", err)
	}
}

func TestAuditInventoryUnverifiable(t *testing.T) {
	led := newFakeLedger()
	// (a) no capability — a sink that can't enumerate its manifest.
	// (b) no manifest yet — os.ErrNotExist.
	// (c) read-denying — a plain error.
	rep, err := AuditInventory(context.Background(), led, []Target{
		{Sink: stubSink{name: "nocap"}},
		{Sink: manifestSink{stubSink: stubSink{name: "empty"}, err: os.ErrNotExist}},
		{Sink: manifestSink{stubSink: stubSink{name: "denied"}, err: errors.New("AccessDenied")}},
	})
	if err != nil {
		t.Fatalf("AuditInventory: %v", err)
	}
	for _, a := range rep.Targets {
		if !a.Unverifiable || a.Failed() {
			t.Fatalf("target %s must be unverifiable, not failed: %+v", a.Target, a)
		}
	}
	if err := rep.FailErr(); err != nil {
		t.Fatalf("an all-unverifiable report must pass, got %v", err)
	}
}

// errStoredLedger is a fakeLedger whose per-target stored enumeration errors — used to exercise
// the ledger-failure paths of AuditInventory and RestoreAll.
type errStoredLedger struct{ *fakeLedger }

func (errStoredLedger) StoredExternalIDsPage(context.Context, string, string, int) ([]string, error) {
	return nil, errors.New("ledger down")
}

func TestAuditInventoryLedgerError(t *testing.T) {
	led := errStoredLedger{newFakeLedger()}
	// The manifest reads fine, but building the ledger's stored set errors → the target carries an
	// Err and AuditInventory returns a non-nil sweep error.
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(hashOf("a"))}
	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err == nil {
		t.Fatal("a ledger error must surface as a sweep error")
	}
	if rep.Targets[0].Err == nil || rep.Targets[0].Failed() {
		t.Fatalf("the target must carry the ledger error (and not double-count as drift): %+v", rep.Targets[0])
	}
}

// trackedCloser records whether Close was called (a leak detector for the abandon path).
type trackedCloser struct{ closed chan struct{} }

func (trackedCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (t *trackedCloser) Close() error          { close(t.closed); return nil }

// blockingManifestSink blocks in LatestManifest until released (ignoring ctx, like a wedged
// mount), then hands back a trackedCloser — so a test can prove the reader is CLOSED even when the
// fetch completes AFTER the ctx-cancel abandon.
type blockingManifestSink struct {
	stubSink
	release chan struct{}
	closer  *trackedCloser
}

func (s *blockingManifestSink) LatestManifest(context.Context) (io.ReadCloser, error) {
	<-s.release
	return s.closer, nil
}

// TestReadLatestManifestClosesReaderOnAbandon (review #8): if LatestManifest completes AFTER a
// ctx-cancel abandon, the ReadCloser it produced must be Closed by the drain path — otherwise it
// leaks an fd no one ever closes.
func TestReadLatestManifestClosesReaderOnAbandon(t *testing.T) {
	closer := &trackedCloser{closed: make(chan struct{})}
	sink := &blockingManifestSink{stubSink: stubSink{name: "t"}, release: make(chan struct{}), closer: closer}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := readLatestManifest(ctx, sink); done <- err }()
	cancel() // abandon while LatestManifest is still blocked
	if err := <-done; err == nil {
		t.Fatal("a cancelled manifest read must return the ctx error")
	}
	close(sink.release) // let the abandoned LatestManifest complete + produce its reader
	select {
	case <-closer.closed:
		// good — the abandoned reader was drained + closed, no fd leak.
	case <-time.After(3 * time.Second):
		t.Fatal("the late-produced manifest reader was LEAKED (never Closed) after abandon")
	}
}

func TestAuditInventoryMalformedManifestErrors(t *testing.T) {
	led := newFakeLedger()
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: []byte("{not json\n")}
	// A corrupt/truncated manifest WAS fetched but is malformed — a REAL DR fault (the target's
	// standalone inventory is broken), surfaced as a sweep error (nonzero exit), NOT silently
	// under-counted as unverifiable or a clean pass.
	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err == nil {
		t.Fatal("a malformed manifest must surface as a sweep error")
	}
	if !errors.Is(err, errCorruptManifest) {
		t.Fatalf("want a corrupt-manifest error, got %v", err)
	}
	if rep.Targets[0].Failed() {
		t.Fatal("a corrupt-manifest target's Err path must not double-count as drift")
	}
}
