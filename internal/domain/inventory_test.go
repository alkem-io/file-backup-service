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

// manifestOf renders a JSONL manifest listing the given externalIDs in ASCENDING byte order (a real
// manifest is written from StoredObjectsPage ORDER BY "externalID" COLLATE "C", which the streaming
// merge relies on).
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
	storeOnTarget(t, led, inLedger, inBoth)
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(inBoth, onlyManifest)}

	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	v := rep.Targets[0]
	if v.Extra != 1 { // onlyManifest is in the manifest but not ledger-stored
		t.Fatalf("want Extra=1 (orphan/lost ledger record), got %d", v.Extra)
	}
	if v.Missing != 1 { // inLedger is ledger-stored but not in the manifest (newer than the snapshot)
		t.Fatalf("want Missing=1, got %d", v.Missing)
	}
	if v.Status != StatusDrift || !v.Failed() {
		t.Fatalf("a target with an extra (orphan) manifest object must be DRIFT+fail: %+v", v)
	}
	if rep.FailErr() == nil {
		t.Fatal("FailErr must be non-nil when a target has extras")
	}
}

func TestAuditInventoryCleanAndMissingOnlyPasses(t *testing.T) {
	led := newFakeLedger()
	a1, a2 := hashOf("x"), hashOf("y")
	storeOnTarget(t, led, a1, a2)
	smaller := a1
	if a2 < a1 {
		smaller = a2
	}
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(smaller)}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	v := rep.Targets[0]
	if v.Extra != 0 || v.Missing != 1 {
		t.Fatalf("want Extra=0 Missing=1, got Extra=%d Missing=%d", v.Extra, v.Missing)
	}
	if v.Status != StatusVerified || v.Failed() {
		t.Fatalf("Missing-only (snapshot staleness) must be Verified+pass, got %+v", v)
	}
	if err := rep.FailErr(); err != nil {
		t.Fatalf("a missing-only report must pass, got %v", err)
	}
}

// TestAuditInventoryUnverifiableBenign: a NoData target (no capability, or no manifest yet) and a
// read-denied WORM target are benign — never a failure.
func TestAuditInventoryUnverifiableBenign(t *testing.T) {
	led := newFakeLedger()
	rep := AuditInventory(context.Background(), led, []Target{
		{Sink: stubSink{name: "nocap"}},                                                                  // no capability → NoData
		{Sink: manifestSink{stubSink: stubSink{name: "empty"}, err: os.ErrNotExist}},                     // no manifest yet → NoData
		{Sink: manifestSink{stubSink: stubSink{name: "wormdenied"}, err: errors.New("403")}, Worm: true}, // worm read-deny → Unverifiable but benign
	})
	by := byTarget(rep)
	if v := by["nocap"]; v.Status != StatusNoData || v.Failed() {
		t.Fatalf("nocap must be NoData+benign, got %+v", v)
	}
	if v := by["empty"]; v.Status != StatusNoData || v.Failed() {
		t.Fatalf("empty must be NoData+benign, got %+v", v)
	}
	if v := by["wormdenied"]; v.Status != StatusUnverifiable || v.Failed() {
		t.Fatalf("wormdenied must be Unverifiable+benign, got %+v", v)
	}
	if err := rep.FailErr(); err != nil {
		t.Fatalf("an all-benign report must pass, got %v", err)
	}
}

// TestAuditInventoryNonWormUnreadableFails: a NON-worm target whose manifest can't be READ (a broken
// read path — not "no manifest yet") FAILS, consistent with ledger→target's Unverifiable; a worm
// target with the same error does NOT (read-deny by design).
func TestAuditInventoryNonWormUnreadableFails(t *testing.T) {
	led := newFakeLedger()
	rep := AuditInventory(context.Background(), led, []Target{
		{Sink: manifestSink{stubSink: stubSink{name: "broken"}, err: errors.New("connection refused")}},                 // non-worm unreadable → FAIL
		{Sink: manifestSink{stubSink: stubSink{name: "wormbroken"}, err: errors.New("connection refused")}, Worm: true}, // worm unreadable → benign
	})
	by := byTarget(rep)
	if v := by["broken"]; v.Status != StatusUnverifiable || !v.Failed() {
		t.Fatalf("a non-worm unreadable manifest must be Unverifiable+fail, got %+v", v)
	}
	if v := by["wormbroken"]; v.Status != StatusUnverifiable || v.Failed() {
		t.Fatalf("a worm target's unreadable manifest is by design, must not fail, got %+v", v)
	}
	if rep.FailErr() == nil {
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
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(hashOf("a"))}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	v := rep.Targets[0]
	if v.Status != StatusFault || v.Err == nil || !v.Failed() {
		t.Fatalf("a ledger read error must be a Fault carrying the cause: %+v", v)
	}
	if !errors.Is(rep.FailErr(), errLedgerRead) {
		t.Fatalf("FailErr must surface the ledger-read fault, got %v", rep.FailErr())
	}
}

func TestAuditInventoryMalformedManifestErrors(t *testing.T) {
	led := newFakeLedger()
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: []byte("{not json\n")}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	v := rep.Targets[0]
	if v.Status != StatusCorrupt || v.Err == nil || !v.Failed() {
		t.Fatalf("a malformed manifest must be a Corrupt fault: %+v", v)
	}
	if !errors.Is(rep.FailErr(), errCorruptManifest) {
		t.Fatalf("want a corrupt-manifest error in the verdict, got %v", rep.FailErr())
	}
}

// TestAuditInventoryMergeBoundaries: an EMPTY manifest against a non-empty ledger is all-Missing
// (Verified); a non-empty manifest against an EMPTY ledger is all-Extra (Drift).
func TestAuditInventoryMergeBoundaries(t *testing.T) {
	led := newFakeLedger()
	x, y := hashOf("x"), hashOf("y")
	storeOnTarget(t, led, x, y)

	empty := manifestSink{stubSink: stubSink{name: "t"}, manifest: nil}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: empty}})
	if v := rep.Targets[0]; v.Extra != 0 || v.Missing != 2 || v.Status != StatusVerified {
		t.Fatalf("empty manifest: want Extra=0 Missing=2 Verified, got %+v", v)
	}

	full := manifestSink{stubSink: stubSink{name: "none"}, manifest: manifestOf(x, y)}
	rep2 := AuditInventory(context.Background(), led, []Target{{Sink: full}})
	if v := rep2.Targets[0]; v.Extra != 2 || v.Missing != 0 || v.Status != StatusDrift {
		t.Fatalf("empty ledger: want Extra=2 Missing=0 Drift, got %+v", v)
	}
}

// TestAuditInventoryNonAscendingFallsBackToSortedDiff (re-review C3): a non-byte-ascending manifest
// (an old-format / locale-collated manifest, legitimately written) must NOT hard-fail as corrupt —
// the order-independent (sorted) fallback re-fetches, sorts, and diffs correctly.
func TestAuditInventoryNonAscendingFallsBackToSortedDiff(t *testing.T) {
	led := newFakeLedger()
	storeOnTarget(t, led, "aa", "cc") // ledger stores aa, cc on "t"
	// The manifest lists cc, bb, aa OUT of byte order. After the sorted fallback: bb is an orphan
	// (extra=1) → Drift; aa/cc are in both. It must NOT be a corruption fault.
	body := []byte(`{"externalID":"cc"}` + "\n" + `{"externalID":"bb"}` + "\n" + `{"externalID":"aa"}` + "\n")
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: body}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if v := rep.Targets[0]; v.Status != StatusDrift || v.Extra != 1 {
		t.Fatalf("a non-ascending manifest must fall back to a sorted diff (bb orphan → Drift extra=1), got %+v", v)
	}
	if errors.Is(rep.FailErr(), errCorruptManifest) {
		t.Fatalf("a non-ascending (old-format) manifest must NOT be a corruption fault: %v", rep.FailErr())
	}
}

// flakyManifestSink returns a fixed manifest on the FIRST LatestManifest call and an error on the
// SECOND — so a test can exercise the order-independent fallback's RE-FETCH failing.
type flakyManifestSink struct {
	stubSink
	first []byte
	err   error
	calls int
}

func (s *flakyManifestSink) LatestManifest(context.Context) (io.ReadCloser, error) {
	s.calls++
	if s.calls == 1 {
		return io.NopCloser(bytes.NewReader(s.first)), nil
	}
	return nil, s.err
}

// TestAuditInventorySortedDiffRefetchError (re-review C3): if the non-ascending fallback's RE-FETCH
// of the manifest fails, it classifies that error (here a read-deny → Unverifiable), not a crash.
func TestAuditInventorySortedDiffRefetchError(t *testing.T) {
	led := newFakeLedger()
	storeOnTarget(t, led, "aa", "bb")
	sink := &flakyManifestSink{stubSink: stubSink{name: "t"}, first: []byte(`{"externalID":"bb"}` + "\n" + `{"externalID":"aa"}` + "\n"), err: errors.New("connection refused")}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if v := rep.Targets[0]; v.Status != StatusUnverifiable {
		t.Fatalf("a failed fallback re-fetch must be Unverifiable, got %+v", v)
	}
}

// TestAuditInventorySortedDiffTooLarge (re-review C3): a non-ascending manifest too LARGE to buffer
// order-independently (over maxSortedManifestIDs) must be Unverifiable — the fallback refuses to
// buffer rather than risk an OOM.
func TestAuditInventorySortedDiffTooLarge(t *testing.T) {
	old := maxSortedManifestIDs
	maxSortedManifestIDs = 2
	defer func() { maxSortedManifestIDs = old }()
	led := newFakeLedger()
	storeOnTarget(t, led, "aa", "bb", "cc") // all in the ledger → extra=0 when the non-ascending fires
	body := []byte(`{"externalID":"cc"}` + "\n" + `{"externalID":"bb"}` + "\n" + `{"externalID":"aa"}` + "\n")
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: body}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if v := rep.Targets[0]; v.Status != StatusUnverifiable {
		t.Fatalf("a non-ascending manifest over the sorted-diff bound must be Unverifiable, got %+v", v)
	}
}

// TestManifestIteratorClassifiesReadErrors: a ctx cancel/deadline propagates as cancellation (NOT
// corrupt); a transient network reset is a plain read error (NOT corrupt); a JSON parse error,
// bufio.ErrTooLong, and a non-ascending key are all CORRUPT. manifestIterator takes the reader
// directly (it is already ctx-bounded upstream by the stallReader), so a ctx error is injected via a
// reader that returns it.
func TestManifestIteratorClassifiesReadErrors(t *testing.T) {
	line := func(id string) []byte { return []byte(`{"externalID":"` + id + `"}` + "\n") }

	// a reader that returns context.Canceled → the ctx error, NOT corrupt.
	if _, _, err := manifestIterator(errReader{context.Canceled})(); !errors.Is(err, context.Canceled) || errors.Is(err, errCorruptManifest) {
		t.Fatalf("a ctx-cancel read must be a cancellation, not corrupt: %v", err)
	}

	// transient network reset mid-stream → a plain read error, NOT corrupt.
	boom := errors.New("connection reset by peer")
	it := manifestIterator(io.MultiReader(bytes.NewReader(line("aa")), errReader{boom}))
	if _, ok, _ := it(); !ok {
		t.Fatal("first manifest line should yield")
	}
	_, _, err := it()
	if err == nil || errors.Is(err, errCorruptManifest) || errors.Is(err, context.Canceled) {
		t.Fatalf("a transient mid-read reset must be a plain read error (not corrupt/cancel): %v", err)
	}

	// a malformed JSON line → corrupt.
	if _, _, err := manifestIterator(bytes.NewReader([]byte("{bad json\n")))(); !errors.Is(err, errCorruptManifest) {
		t.Fatalf("a JSON parse error must be corrupt: %v", err)
	}

	// a line longer than the 1 MiB buffer → bufio.ErrTooLong → corrupt.
	huge := append(bytes.Repeat([]byte("a"), 2<<20), '\n')
	if _, _, err := manifestIterator(bytes.NewReader(huge))(); !errors.Is(err, errCorruptManifest) || !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("an over-long line must be corrupt (ErrTooLong): %v", err)
	}

	// non-ascending order → errNonAscendingManifest (NOT corrupt — the fallback signal), surfaced on
	// the second pull.
	it2 := manifestIterator(io.MultiReader(bytes.NewReader(line("bb")), bytes.NewReader(line("aa"))))
	_, _, _ = it2() // consume "bb"
	if _, _, err := it2(); !errors.Is(err, errNonAscendingManifest) || errors.Is(err, errCorruptManifest) {
		t.Fatalf("a non-ascending key must signal errNonAscendingManifest (not corrupt): %v", err)
	}
}

// TestManifestScannerNonStrict: the non-strict scanner (the order-independent fallback collector)
// yields non-ascending ids WITHOUT erroring — it does NOT enforce monotonicity (the caller sorts).
func TestManifestScannerNonStrict(t *testing.T) {
	line := func(id string) []byte { return []byte(`{"externalID":"` + id + `"}` + "\n") }
	itNS := manifestScanner(io.MultiReader(bytes.NewReader(line("bb")), bytes.NewReader(line("aa"))), false)
	if id, ok, err := itNS(); !ok || err != nil || id != "bb" {
		t.Fatalf("non-strict scan first: id=%q ok=%v err=%v", id, ok, err)
	}
	if id, ok, err := itNS(); !ok || err != nil || id != "aa" {
		t.Fatalf("non-strict scan must yield a non-ascending id without error: id=%q ok=%v err=%v", id, ok, err)
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

// TestAuditInventoryPartialDriftPreserved (re-review C2): a definitive orphan (extra>0) observed
// BEFORE a transient mid-stream read fault must be reported as Drift, NOT discarded and relabeled
// Unverifiable — mirrors the immutability 404-drift preservation. (An empty ledger makes the
// manifest's first id an orphan.)
func TestAuditInventoryPartialDriftPreserved(t *testing.T) {
	led := newFakeLedger()
	orphan := hashOf("orphan")
	sink := ioErrManifestSink{stubSink: stubSink{name: "t"}, prefix: []byte(`{"externalID":"` + orphan + `"}` + "\n"), err: errors.New("connection reset by peer")}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if v := rep.Targets[0]; v.Status != StatusDrift || v.Extra != 1 {
		t.Fatalf("an orphan seen before a transient read fault must be Drift extra=1 (not discarded), got %+v", v)
	}
}

// TestAuditInventoryTransientReadIsUnverifiable: a mid-stream transient read reset is Unverifiable
// (retry next pass) — NOT a corruption fault — and a non-worm target then FAILS.
func TestAuditInventoryTransientReadIsUnverifiable(t *testing.T) {
	led := newFakeLedger()
	storeOnTarget(t, led, hashOf("a"))
	sink := ioErrManifestSink{stubSink: stubSink{name: "t"}, prefix: []byte(`{"externalID":"` + hashOf("a") + `"}` + "\n"), err: errors.New("connection reset by peer")}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	v := rep.Targets[0]
	if v.Status != StatusUnverifiable || v.Err != nil {
		t.Fatalf("a transient mid-read reset must be Unverifiable (not corrupt/fault): %+v", v)
	}
	if rep.FailErr() == nil {
		t.Fatal("a non-worm target with an unverifiable transient read must FAIL the audit")
	}
}

// TestAuditInventoryProbeTimeoutFailsNonWorm: a per-operation DEADLINE (a wedged/black-holing target
// — child DeadlineExceeded while the PARENT is still live) is Unverifiable with a non-worm FAIL.
func TestAuditInventoryProbeTimeoutFailsNonWorm(t *testing.T) {
	old := auditProbeTimeout
	auditProbeTimeout = 50 * time.Millisecond
	defer func() { auditProbeTimeout = old }()

	sink := &blockingManifestSink{stubSink: stubSink{name: "t"}, release: make(chan struct{}), closer: &trackedCloser{closed: make(chan struct{})}}
	t.Cleanup(func() { close(sink.release) })
	done := make(chan VerdictReport, 1)
	go func() { done <- AuditInventory(context.Background(), newFakeLedger(), []Target{{Sink: sink}}) }()
	select {
	case rep := <-done:
		v := rep.Targets[0]
		if v.Status != StatusUnverifiable {
			t.Fatalf("a wedged non-worm target (probe timeout, parent live) must be Unverifiable, got %+v", v)
		}
		if rep.FailErr() == nil {
			t.Fatal("a wedged non-worm target must FAIL the audit — an incomplete integrity check must not read green")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AuditInventory HUNG on a wedged target — per-operation deadline not enforced")
	}
}

// TestClassifyInventoryErr pins the classifier: a per-operation DeadlineExceeded (parent live) →
// Unverifiable (a non-worm target then FAILS); a PARENT-cancel → NoData (benign).
func TestClassifyInventoryErr(t *testing.T) {
	timeout := classifyInventoryErr(context.Background(), "t", context.DeadlineExceeded)
	if timeout.Status != StatusUnverifiable || timeout.Err != nil || !timeout.Failed() {
		t.Fatalf("a per-operation timeout must be Unverifiable + failing (non-worm), got %+v", timeout)
	}
	pctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdown := classifyInventoryErr(pctx, "t", context.Canceled)
	if shutdown.Status != StatusNoData || shutdown.Err != nil || shutdown.Failed() {
		t.Fatalf("a parent SIGTERM must be benign NoData, got %+v", shutdown)
	}
}

// TestAuditInventoryCancelledIsBenign: a genuine parent-ctx cancellation (SIGTERM) is benign at the
// target level (NoData) — the audit's top-level ctx.Err() fold surfaces the abort, not a per-target
// fault or a non-worm failure.
func TestAuditInventoryCancelledIsBenign(t *testing.T) {
	led := newFakeLedger()
	storeOnTarget(t, led, hashOf("a"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: manifestOf(hashOf("a"))}
	rep := AuditInventory(ctx, led, []Target{{Sink: sink}})
	if v := rep.Targets[0]; v.Status != StatusNoData || v.Err != nil || v.Failed() {
		t.Fatalf("a shutdown must be benign NoData (no fault, not failing), got %+v", v)
	}
	if rep.FailErr() != nil {
		t.Fatalf("a shutdown-cancelled audit must not fail via the inventory verdict, got %v", rep.FailErr())
	}
}

// panicManifestSink panics in LatestManifest — a driver bug that must be contained as an error.
type panicManifestSink struct{ stubSink }

func (panicManifestSink) LatestManifest(context.Context) (io.ReadCloser, error) { panic("driver boom") }

// TestAuditInventoryManifestPanicIsFault (re-review B3): a panic in LatestManifest is CONTAINED (via
// abandonableFetch's recover) — the sweep doesn't crash — and routed to Fault (a driver panic is a
// code bug, never benign), so it FAILS the audit even on a worm target.
func TestAuditInventoryManifestPanicIsFault(t *testing.T) {
	rep := AuditInventory(context.Background(), newFakeLedger(), []Target{{Sink: panicManifestSink{stubSink: stubSink{name: "boom"}}}})
	if v := rep.Targets[0]; v.Status != StatusFault || !v.Failed() {
		t.Fatalf("a panicked manifest read must be a failing Fault (fail-loud on a code bug), got %+v", v)
	}
}

// TestAuditInventorySkipsBlankAndEmptyLines: a manifest with a blank line and an empty-externalID
// line ignores both, counting only the real entries.
func TestAuditInventorySkipsBlankAndEmptyLines(t *testing.T) {
	led := newFakeLedger()
	realID := hashOf("real")
	storeOnTarget(t, led, realID)
	body := []byte("\n" + `{"externalID":""}` + "\n" + `{"externalID":"` + realID + `"}` + "\n")
	sink := manifestSink{stubSink: stubSink{name: "t"}, manifest: body}
	rep := AuditInventory(context.Background(), led, []Target{{Sink: sink}})
	if v := rep.Targets[0]; v.Extra != 0 || v.Missing != 0 || v.Status != StatusVerified {
		t.Fatalf("blank/empty lines must be skipped, want extra=0 missing=0 Verified, got %+v", v)
	}
}

// blockingReader blocks in Read IGNORING ctx (a wedged mount / black-holing S3 body), so the
// stallReader's per-read bound is the only thing that can stop it. Its Close records that it was
// closed (so a test can prove the abandon path — not a concurrent deferred Close — owns closing it).
type blockingReader struct {
	release chan struct{}
	closed  chan struct{}
}

func (b blockingReader) Read([]byte) (int, error) { <-b.release; return 0, io.EOF }
func (b blockingReader) Close() error {
	if b.closed != nil {
		close(b.closed)
	}
	return nil
}

// TestStallReaderBoundsWedgedRead: a mid-stream read that stalls IGNORING ctx must be abandoned at
// the stallReader's per-read deadline (so a wedged manifest body can't hang the inventory diff), the
// abandon path must OWN closing rc (only AFTER the wedged read finishes — never concurrently), and an
// already-cancelled ctx short-circuits before even starting a read.
func TestStallReaderBoundsWedgedRead(t *testing.T) {
	br := blockingReader{release: make(chan struct{}), closed: make(chan struct{})}
	sr := &stallReader{ctx: context.Background(), rc: br, timeout: 50 * time.Millisecond}
	done := make(chan error, 1)
	go func() { _, err := sr.Read(make([]byte, 16)); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("a wedged mid-stream read must be bounded with a deadline error, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stallReader HUNG on a wedged reader — per-read bound not enforced")
	}
	// Close() must be a no-op while the read is still wedged (the abandon path owns the close);
	// rc is closed only after the read goroutine finishes.
	if err := sr.Close(); err != nil {
		t.Fatalf("Close after abandon must be a no-op, got %v", err)
	}
	select {
	case <-br.closed:
		t.Fatal("rc was closed while the wedged read was still in flight (fd race)")
	default:
	}
	close(br.release) // let the wedged read finish; the abandon path then closes rc
	select {
	case <-br.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("the abandon path did not close rc after the wedged read finished (fd leak)")
	}

	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&stallReader{ctx: cctx, rc: blockingReader{release: make(chan struct{})}, timeout: time.Minute}).Read(make([]byte, 16)); !errors.Is(err, context.Canceled) {
		t.Fatalf("an already-cancelled ctx must short-circuit the read, got %v", err)
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

// TestAbandonableFetchClosesReaderOnAbandon: if the fetch completes AFTER a ctx-cancel abandon, the
// ReadCloser it produced must be Closed by the cleanup path — otherwise it leaks an fd.
func TestAbandonableFetchClosesReaderOnAbandon(t *testing.T) {
	closer := &trackedCloser{closed: make(chan struct{})}
	sink := &blockingManifestSink{stubSink: stubSink{name: "t"}, release: make(chan struct{}), closer: closer}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := abandonableFetch(ctx, auditProbeTimeout, func() (io.ReadCloser, error) { return sink.LatestManifest(ctx) })
		done <- err
	}()
	cancel() // abandon while LatestManifest is still blocked
	if err := <-done; err == nil {
		t.Fatal("a cancelled manifest fetch must return the ctx error")
	}
	close(sink.release) // let the abandoned LatestManifest complete + produce its reader
	select {
	case <-closer.closed:
		// good — the abandoned reader was closed, no fd leak.
	case <-time.After(3 * time.Second):
		t.Fatal("the late-produced manifest reader was LEAKED (never Closed) after abandon")
	}
}
