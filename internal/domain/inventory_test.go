package domain

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
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

// manifestOf renders a JSONL manifest listing the given externalIDs in ASCENDING order (a real
// manifest is written from StoredObjectsPage ORDER BY externalID, which the streaming merge relies
// on).
func manifestOf(ids ...string) []byte {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for _, id := range sorted {
		_ = enc.Encode(manifestLine{ExternalID: id, Size: 1})
	}
	return b.Bytes()
}

func storeOnTarget(t *testing.T, led *fakeLedger, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := led.RecordBackup(context.Background(), ObjectMeta{ExternalID: id},
			[]TargetStatus{{Target: "t", State: StateStored}}); err != nil {
			t.Fatalf("seed ledger: %v", err)
		}
	}
}

func TestAuditInventoryExtraAndMissing(t *testing.T) {
	led := newFakeLedger()
	inLedger, inBoth, onlyManifest := hashOf("a"), hashOf("b"), hashOf("c")
	// Ledger records inLedger + inBoth stored on "t"; the manifest lists inBoth + onlyManifest.
	storeOnTarget(t, led, inLedger, inBoth)
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
	storeOnTarget(t, led, a1, a2)
	// The manifest is a SUBSET of the ledger — the missing one was stored after the last snapshot.
	// Missing>0, Extra=0 → informational, NOT a failure. (Use the smaller of the two ids so the
	// manifest is a genuine prefix of the ledger set.)
	smaller := a1
	if a2 < a1 {
		smaller = a2
	}
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(smaller)}
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

// TestAuditInventoryUnverifiableBenign: a target with NO DATA (no capability, or no manifest yet) is
// unverifiable but benign — never a failure, regardless of worm.
func TestAuditInventoryUnverifiableBenign(t *testing.T) {
	led := newFakeLedger()
	rep, err := AuditInventory(context.Background(), led, []Target{
		{Sink: stubSink{name: "nocap"}},                                                                  // no capability → NoData
		{Sink: manifestSink{stubSink: stubSink{name: "empty"}, err: os.ErrNotExist}},                     // no manifest yet → NoData
		{Sink: manifestSink{stubSink: stubSink{name: "wormdenied"}, err: errors.New("403")}, Worm: true}, // worm read-deny → benign
	})
	if err != nil {
		t.Fatalf("AuditInventory: %v", err)
	}
	for _, a := range rep.Targets {
		if !a.Unverifiable || a.Failed() {
			t.Fatalf("target %s must be unverifiable + benign, got %+v", a.Target, a)
		}
	}
	if err := rep.FailErr(); err != nil {
		t.Fatalf("an all-benign-unverifiable report must pass, got %v", err)
	}
}

// TestAuditInventoryNonWormUnreadableFails (review Cluster 4): a NON-worm target whose manifest
// can't be READ (a broken read path — not "no manifest yet") FAILS, consistent with ledger→target's
// UnexpectedlyUnverifiable. A worm target with the same error does NOT (read-deny by design).
func TestAuditInventoryNonWormUnreadableFails(t *testing.T) {
	led := newFakeLedger()
	rep, err := AuditInventory(context.Background(), led, []Target{
		{Sink: manifestSink{stubSink: stubSink{name: "broken"}, err: errors.New("connection refused")}},                 // non-worm unreadable → FAIL
		{Sink: manifestSink{stubSink: stubSink{name: "wormbroken"}, err: errors.New("connection refused")}, Worm: true}, // worm unreadable → benign
	})
	if err != nil {
		t.Fatalf("AuditInventory: %v", err)
	}
	byName := map[string]InventoryAudit{}
	for _, a := range rep.Targets {
		byName[a.Target] = a
	}
	if !byName["broken"].Failed() {
		t.Fatalf("a non-worm target with an unreadable manifest must FAIL, got %+v", byName["broken"])
	}
	if byName["wormbroken"].Failed() {
		t.Fatalf("a worm target's unreadable manifest is by design, must NOT fail, got %+v", byName["wormbroken"])
	}
	if err := rep.FailErr(); err == nil {
		t.Fatal("FailErr must flag the non-worm broken read path")
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
	// The manifest reads fine, but paging the ledger's stored set errors → the target carries an Err
	// (a fault) and AuditInventory returns a non-nil sweep error.
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(hashOf("a"))}
	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err == nil {
		t.Fatal("a ledger error must surface as a sweep error")
	}
	if rep.Targets[0].Err == nil || rep.Targets[0].Failed() {
		t.Fatalf("the target must carry the ledger error (and not double-count as drift): %+v", rep.Targets[0])
	}
}

func TestAuditInventoryMalformedManifestErrors(t *testing.T) {
	led := newFakeLedger()
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: []byte("{not json\n")}
	// A corrupt/truncated manifest WAS fetched but is malformed — a REAL DR fault, surfaced as a
	// sweep error (nonzero exit), NOT silently under-counted as unverifiable or a clean pass.
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

// TestAuditInventoryMergeBoundaries (Cluster 6 stream-merge): an EMPTY manifest against a
// non-empty ledger is all-Missing; a non-empty manifest against an EMPTY ledger is all-Extra — the
// two one-sided merge branches.
func TestAuditInventoryMergeBoundaries(t *testing.T) {
	led := newFakeLedger()
	x, y := hashOf("x"), hashOf("y")
	storeOnTarget(t, led, x, y)

	// Empty manifest vs a 2-object ledger → Missing=2, Extra=0.
	empty := manifestSink{stubSink: stubSink{name: "t"}, manifest: nil}
	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: empty}})
	if err != nil {
		t.Fatalf("AuditInventory: %v", err)
	}
	if a := rep.Targets[0]; a.Extra != 0 || a.Missing != 2 {
		t.Fatalf("empty manifest: want Extra=0 Missing=2, got %+v", a)
	}

	// A 2-object manifest vs an EMPTY ledger target ("none") → Extra=2, Missing=0.
	full := manifestSink{stubSink: stubSink{name: "none"}, manifest: manifestOf(x, y)}
	rep2, err := AuditInventory(context.Background(), led, []Target{{Sink: full}})
	if err != nil {
		t.Fatalf("AuditInventory: %v", err)
	}
	if a := rep2.Targets[0]; a.Extra != 2 || a.Missing != 0 {
		t.Fatalf("empty ledger: want Extra=2 Missing=0, got %+v", a)
	}
}

// TestAuditInventoryNonAscendingIsCorrupt (review Cluster 3): a manifest whose keys aren't strictly
// ascending is CORRUPT (the streaming merge relies on order) — a fault, not silently mis-counted.
func TestAuditInventoryNonAscendingIsCorrupt(t *testing.T) {
	led := newFakeLedger()
	// Hand-write two lines out of order (bb then aa).
	body := []byte(`{"externalID":"bb"}` + "\n" + `{"externalID":"aa"}` + "\n")
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: body}
	_, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err == nil || !errors.Is(err, errCorruptManifest) {
		t.Fatalf("a non-ascending manifest must be corrupt, got %v", err)
	}
}

// TestManifestIteratorClassifiesReadErrors (delta re-review #1): the scanner-error classification —
// a ctx cancel/deadline propagates as cancellation (NOT corrupt); a transient network reset is a
// plain read error (NOT corrupt); a JSON parse error, bufio.ErrTooLong, and a non-ascending key are
// all CORRUPT.
func TestManifestIteratorClassifiesReadErrors(t *testing.T) {
	line := func(id string) []byte { return []byte(`{"externalID":"` + id + `"}` + "\n") }

	// ctx-cancel mid-read → the ctx error, NOT corrupt.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := manifestIterator(ctx, bytes.NewReader(line("aa")))(); !errors.Is(err, context.Canceled) || errors.Is(err, errCorruptManifest) {
		t.Fatalf("a ctx-cancel mid-read must be a cancellation, not corrupt: %v", err)
	}

	// transient network reset mid-stream → a plain read error, NOT corrupt.
	boom := errors.New("connection reset by peer")
	it := manifestIterator(context.Background(), io.MultiReader(bytes.NewReader(line("aa")), errReader{boom}))
	if _, ok, _ := it(); !ok {
		t.Fatal("first manifest line should yield")
	}
	_, _, err := it()
	if err == nil || errors.Is(err, errCorruptManifest) || errors.Is(err, context.Canceled) {
		t.Fatalf("a transient mid-read reset must be a plain read error (not corrupt/cancel): %v", err)
	}

	// a malformed JSON line → corrupt.
	if _, _, err := manifestIterator(context.Background(), bytes.NewReader([]byte("{bad json\n")))(); !errors.Is(err, errCorruptManifest) {
		t.Fatalf("a JSON parse error must be corrupt: %v", err)
	}

	// a line longer than the 1 MiB buffer → bufio.ErrTooLong → corrupt.
	huge := append(bytes.Repeat([]byte("a"), 2<<20), '\n')
	if _, _, err := manifestIterator(context.Background(), bytes.NewReader(huge))(); !errors.Is(err, errCorruptManifest) || !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("an over-long line must be corrupt (ErrTooLong): %v", err)
	}

	// non-ascending order → corrupt (surfaced on the second pull).
	it2 := manifestIterator(context.Background(), io.MultiReader(bytes.NewReader(line("bb")), bytes.NewReader(line("aa"))))
	_, _, _ = it2() // consume "bb"
	if _, _, err := it2(); !errors.Is(err, errCorruptManifest) {
		t.Fatalf("a non-ascending key must be corrupt: %v", err)
	}
}

// ioErrManifestSink serves prefix bytes then a fixed I/O error mid-stream — a transient reset.
type ioErrManifestSink struct {
	stubSink
	prefix []byte
	err    error
}

func (s ioErrManifestSink) LatestManifest(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(io.MultiReader(bytes.NewReader(s.prefix), errReader{s.err})), nil
}

// TestAuditInventoryTransientReadIsUnverifiable (delta re-review #1): a mid-stream transient read
// reset is UNVERIFIABLE (retry next pass) — NOT a data-corruption fault, symmetric with the fetch
// path.
func TestAuditInventoryTransientReadIsUnverifiable(t *testing.T) {
	led := newFakeLedger()
	storeOnTarget(t, led, hashOf("a"))
	sink := ioErrManifestSink{stubSink: stubSink{name: "t"}, prefix: []byte(`{"externalID":"` + hashOf("a") + `"}` + "\n"), err: errors.New("connection reset by peer")}
	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err != nil {
		t.Fatalf("a transient read reset must NOT be a sweep fault, got %v", err)
	}
	if a := rep.Targets[0]; !a.Unverifiable || a.Err != nil {
		t.Fatalf("a transient mid-read reset must be unverifiable (not corrupt/fault): %+v", a)
	}
}

// TestAuditInventoryCancelledIsBenign (delta re-review #1): a ctx cancellation during the read is
// benign at the target level (NoData) — the audit's top-level ctx.Err() fold surfaces the abort, not
// a spurious per-target corruption fault.
func TestAuditInventoryCancelledIsBenign(t *testing.T) {
	led := newFakeLedger()
	storeOnTarget(t, led, hashOf("a"))
	sink := ioErrManifestSink{stubSink: stubSink{name: "t"}, prefix: []byte(`{"externalID":"` + hashOf("a") + `"}` + "\n"), err: context.Canceled}
	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err != nil {
		t.Fatalf("a cancellation must not be a per-target sweep fault, got %v", err)
	}
	if a := rep.Targets[0]; !a.Unverifiable || !a.NoData || a.Err != nil || a.Failed() {
		t.Fatalf("a cancelled read must be benign (unverifiable+NoData, no fault), got %+v", a)
	}
}

// panicManifestSink panics in LatestManifest — a driver bug that must be contained as an error,
// not crash the audit sweep.
type panicManifestSink struct{ stubSink }

func (panicManifestSink) LatestManifest(context.Context) (io.ReadCloser, error) { panic("driver boom") }

// TestAuditInventoryRecoversManifestPanic: a panic in LatestManifest is CONTAINED (via
// fetchLatestManifest's RunAbandonableCleanup recover) — the sweep doesn't crash. A non-worm
// target whose manifest couldn't be read (here because it panicked) is reported unverifiable and
// FAILS the audit (a broken read path).
func TestAuditInventoryRecoversManifestPanic(t *testing.T) {
	rep, err := AuditInventory(context.Background(), newFakeLedger(), []Target{{Sink: panicManifestSink{stubSink: stubSink{name: "boom"}}}})
	if err != nil {
		t.Fatalf("a contained panic must not crash the sweep, got %v", err)
	}
	if !rep.Targets[0].Unverifiable || !rep.Targets[0].Failed() {
		t.Fatalf("a panicked non-worm manifest read must be unverifiable + failing, got %+v", rep.Targets[0])
	}
}

// TestAuditInventorySkipsBlankAndEmptyLines: a manifest with a blank line and an empty-externalID
// line ignores both (they carry no object), counting only the real entries.
func TestAuditInventorySkipsBlankAndEmptyLines(t *testing.T) {
	led := newFakeLedger()
	realID := hashOf("real")
	storeOnTarget(t, led, realID)
	body := []byte("\n" + `{"externalID":""}` + "\n" + `{"externalID":"` + realID + `"}` + "\n")
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: body}
	rep, err := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if err != nil {
		t.Fatalf("AuditInventory: %v", err)
	}
	if a := rep.Targets[0]; a.ManifestSize != 1 || a.Extra != 0 || a.Missing != 0 {
		t.Fatalf("blank/empty lines must be skipped, want manifest=1 extra=0 missing=0, got %+v", a)
	}
}

// trackedCloser records whether Close was called (a leak detector for the abandon path).
type trackedCloser struct{ closed chan struct{} }

func (trackedCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (t *trackedCloser) Close() error          { close(t.closed); return nil }

// blockingManifestSink blocks in LatestManifest until released (ignoring ctx, like a wedged mount),
// then hands back a trackedCloser — so a test can prove the reader is CLOSED even when the fetch
// completes AFTER the ctx-cancel abandon.
type blockingManifestSink struct {
	stubSink
	release chan struct{}
	closer  *trackedCloser
}

func (s *blockingManifestSink) LatestManifest(context.Context) (io.ReadCloser, error) {
	<-s.release
	return s.closer, nil
}

// TestFetchLatestManifestClosesReaderOnAbandon (review #8): if LatestManifest completes AFTER a
// ctx-cancel abandon, the ReadCloser it produced must be Closed by the cleanup path — otherwise it
// leaks an fd no one ever closes.
func TestFetchLatestManifestClosesReaderOnAbandon(t *testing.T) {
	closer := &trackedCloser{closed: make(chan struct{})}
	sink := &blockingManifestSink{stubSink: stubSink{name: "t"}, release: make(chan struct{}), closer: closer}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := fetchLatestManifest(ctx, sink); done <- err }()
	cancel() // abandon while LatestManifest is still blocked
	if err := <-done; err == nil {
		t.Fatal("a cancelled manifest fetch must return the ctx error")
	}
	close(sink.release) // let the abandoned LatestManifest complete + produce its reader
	select {
	case <-closer.closed:
		// good — the abandoned reader was drained + closed, no fd leak.
	case <-time.After(3 * time.Second):
		t.Fatal("the late-produced manifest reader was LEAKED (never Closed) after abandon")
	}
}

// TestAuditInventoryBoundsWedgedTarget (review Cluster 3 probe deadline): a target whose
// LatestManifest hangs must not hang the audit — the per-target deadline (bounded here by a short
// parent ctx) aborts it, reporting the target unverifiable.
func TestAuditInventoryBoundsWedgedTarget(t *testing.T) {
	sink := &blockingManifestSink{stubSink: stubSink{name: "wedged"}, release: make(chan struct{}), closer: &trackedCloser{closed: make(chan struct{})}}
	t.Cleanup(func() { close(sink.release) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan InventoryReport, 1)
	go func() {
		rep, _ := AuditInventory(ctx, newFakeLedger(), []Target{{Sink: sink}})
		done <- rep
	}()
	select {
	case rep := <-done:
		if !rep.Targets[0].Unverifiable {
			t.Fatalf("a wedged target must be reported unverifiable, got %+v", rep.Targets[0])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AuditInventory HUNG on a wedged target — per-target deadline not enforced")
	}
}
