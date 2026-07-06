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

	rep, err := Audit(ctx, led, []Target{{Sink: a}, {Sink: b}}, 0, "")
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
	repBad, err := Audit(ctx, led, []Target{{Sink: existsErrSink{stubSink{name: "t"}}}}, 0, "")
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if !repBad.Targets[0].Unverifiable() || !repBad.Targets[0].UnexpectedlyUnverifiable() {
		t.Fatalf("non-worm all-errored target must be UnexpectedlyUnverifiable: %+v", repBad.Targets[0])
	}

	repWorm, err := Audit(ctx, led, []Target{{Sink: existsErrSink{stubSink{name: "worm"}}, Worm: true}}, 0, "")
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
	if _, err := Audit(ctx, led, []Target{{Sink: a}}, 0, ""); err == nil {
		t.Fatal("a cancelled audit must return an error, not a clean (partial) report")
	}
}

// existsErrSink models a PutObject-only WORM credential: Exists always errors (403).
type existsErrSink struct{ stubSink }

func (existsErrSink) Exists(context.Context, string) (bool, error) {
	return false, errors.New("AccessDenied")
}
