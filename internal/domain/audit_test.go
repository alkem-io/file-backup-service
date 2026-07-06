package domain

import (
	"context"
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
	if rep.Checked != 2 || rep.Missing != 1 || rep.Errors != 0 {
		t.Fatalf("report: %+v (want checked=2 missing=1 errors=0)", rep)
	}
}
