package domain

import (
	"context"
	"errors"
	"testing"
)

// TestAuditDetectsMissing: an object the ledger records stored on a target that no
// longer holds it is reported as missing (the silent-loss case).
func TestAuditDetectsMissing(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "hashA"},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateStored}})

	a := newMemSink("a")
	a.store["hashA"] = []byte("x") // A really has it
	b := newMemSink("b")           // B: ledger says stored, but the sink is empty → missing

	rep, err := Audit(ctx, led, []Target{{Sink: a}, {Sink: b}}, 0)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if rep.Missing() != 1 || len(rep.Targets) != 2 {
		t.Fatalf("report: %+v (want 1 missing across 2 targets)", rep)
	}
	// a: checked=1 missing=0; b: checked=1 missing=1
	for _, ta := range rep.Targets {
		if ta.Target == "a" && (ta.Checked != 1 || ta.Missing != 0) {
			t.Fatalf("target a: %+v", ta)
		}
		if ta.Target == "b" && (ta.Checked != 1 || ta.Missing != 1) {
			t.Fatalf("target b: %+v", ta)
		}
	}
}

// TestAuditWORMTargetUnverifiable: a target whose Exists always errors (WORM) is
// reported Unverifiable, not clean — so missing=0 there isn't mistaken for coverage.
func TestAuditWORMTargetUnverifiable(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "hashA"},
		[]TargetStatus{{Target: "t", State: StateStored}, {Target: "worm", State: StateStored}})

	// A target NOT marked Worm whose Exists always errors is UNEXPECTEDLY unverifiable
	// (a broken read path — an alert). The SAME sink marked Worm is expected (not an alert).
	repBad, err := Audit(ctx, led, []Target{{Sink: existsErrSink{stubSink{name: "t"}}}}, 0)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if !repBad.Targets[0].Unverifiable() || !repBad.Targets[0].UnexpectedlyUnverifiable() {
		t.Fatalf("non-worm all-errored target must be UnexpectedlyUnverifiable: %+v", repBad.Targets[0])
	}

	repWorm, err := Audit(ctx, led, []Target{{Sink: existsErrSink{stubSink{name: "worm"}}, Worm: true}}, 0)
	if err != nil {
		t.Fatalf("audit worm: %v", err)
	}
	if !repWorm.Targets[0].Unverifiable() || repWorm.Targets[0].UnexpectedlyUnverifiable() {
		t.Fatalf("worm target must be Unverifiable but EXPECTED: %+v", repWorm.Targets[0])
	}
}

// TestAuditCancelledPropagates: a cancelled audit must return an error, not a partial
// report as a clean pass (an incomplete integrity check that exits 0 reads as verified).
func TestAuditCancelledPropagates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	led := newFakeLedger()
	_ = led.RecordBackup(context.Background(), ObjectMeta{ExternalID: "hashA"},
		[]TargetStatus{{Target: "a", State: StateStored}})
	a := newMemSink("a")
	a.store["hashA"] = []byte("x")
	if _, err := Audit(ctx, led, []Target{{Sink: a}}, 0); err == nil {
		t.Fatal("a cancelled audit must return an error, not a clean (partial) report")
	}
}

// TestAuditSampleWrapsFromHighStart: a sampled audit starting near the END of the keyspace
// must WRAP to the start and still check the full sample — not under-check (a high random
// start reporting Checked<<sample, exit 0, would be a false clean pass).
func TestAuditSampleWrapsFromHighStart(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	a := newMemSink("a")
	// 10 objects with externalIDs "00".."09"; store all on target "a".
	var ids []string
	for i := 0; i < 10; i++ {
		id := string(rune('0'+i/10)) + string(rune('0'+i%10))
		ids = append(ids, id)
		_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: id}, []TargetStatus{{Target: "a", State: StateStored}})
		a.store[id] = []byte("x")
	}
	// Start ABOVE the highest id ("09") so pass 1 finds nothing → must wrap to "" and check 5.
	rep, err := auditWithStart(ctx, led, []Target{{Sink: a}}, 5, "zz")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if rep.Targets[0].Checked != 5 {
		t.Fatalf("a high-start sampled audit must wrap and check the full sample, got Checked=%d", rep.Targets[0].Checked)
	}
	_ = ids
}

// TestAuditSampleNoBoundaryDoubleCount: a mid-keyspace start that wraps must NOT re-check
// (double-count) the objects it already checked in pass 1 — Checked must not exceed the
// distinct object count.
func TestAuditSampleNoBoundaryDoubleCount(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	a := newMemSink("a")
	for i := 0; i < 10; i++ {
		id := "0" + string(rune('0'+i)) // "00".."09"
		_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: id}, []TargetStatus{{Target: "a", State: StateStored}})
		a.store[id] = []byte("x")
	}
	// Start at "05" with sample 20 (> the 10 objects): pass 1 checks 06-09 (4), wraps,
	// pass 2 checks 00-04 (5). Must be 9 distinct (05 itself is skipped by keyset >), never 14.
	rep, err := auditWithStart(ctx, led, []Target{{Sink: a}}, 20, "05")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if c := rep.Targets[0].Checked; c != 9 {
		t.Fatalf("wrapped sample must not double-count the boundary: Checked=%d (want 9 distinct)", c)
	}
}

// existsErrSink models a PutObject-only WORM credential: Exists always errors (403).
type existsErrSink struct{ stubSink }

func (existsErrSink) Exists(context.Context, string) (bool, error) {
	return false, errors.New("AccessDenied")
}

// TestFailErrFlagsPartialUnverifiable: a non-WORM target with SOME (not all) errored probes
// must fail the audit — half the sample silently unverified is not a clean pass (FR-014).
func TestFailErrFlagsPartialUnverifiable(t *testing.T) {
	// Partial errors on a non-worm target → fail.
	rep := AuditReport{Targets: []TargetAudit{{Target: "t", Checked: 10, Missing: 0, Errors: 4, Worm: false}}}
	if err := rep.FailErr(); err == nil {
		t.Fatal("a non-worm target with partial probe errors must fail the audit")
	}
	// The same error profile on a WORM target is expected → pass.
	worm := AuditReport{Targets: []TargetAudit{{Target: "w", Checked: 10, Missing: 0, Errors: 10, Worm: true}}}
	if err := worm.FailErr(); err != nil {
		t.Fatalf("a worm target's read-denied probes must not fail the audit: %v", err)
	}
	// Clean pass (no errors, no missing) → nil.
	clean := AuditReport{Targets: []TargetAudit{{Target: "t", Checked: 10, Missing: 0, Errors: 0}}}
	if err := clean.FailErr(); err != nil {
		t.Fatalf("a clean audit must pass: %v", err)
	}
}
