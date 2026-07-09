package domain

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestExistsWithCtxAbandonsWedgedProbe: an Exists probe that blocks uninterruptibly (a
// filesystem os.Stat on a wedged mount) must return a ctx-error result at the deadline, not
// hang — so a scheduled audit self-bounds on an unhealthy filesystem target instead of running
// forever and never alerting. (F2)
func TestExistsWithCtxAbandonsWedgedProbe(t *testing.T) {
	h := newHangingSink("wedged")
	t.Cleanup(func() { close(h.release) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan existsResult, 1)
	go func() { done <- existsWithCtx(ctx, h, "hash") }()
	select {
	case res := <-done:
		if res.err == nil {
			t.Fatal("existsWithCtx on a wedged probe must return a ctx error, not a clean result")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("existsWithCtx HUNG on a wedged probe — abandonment failed (F2 regression)")
	}
}

// byTarget indexes a report's verdicts by target name for per-target assertions.
func byTarget(rep VerdictReport) map[string]TargetVerdict {
	m := make(map[string]TargetVerdict, len(rep.Targets))
	for _, v := range rep.Targets {
		m[v.Target] = v
	}
	return m
}

// TestAuditDetectsMissing: an object the ledger records stored on a target that no longer holds it
// is DRIFT (the silent-loss case); a target that still holds it is Verified.
func TestAuditDetectsMissing(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "hashA"},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateStored}})

	a := newMemSink("a")
	a.store["hashA"] = []byte("x") // A really has it
	b := newMemSink("b")           // B: ledger says stored, but the sink is empty → missing (drift)

	rep := Audit(ctx, led, []Target{{Sink: a}, {Sink: b}}, 0)
	if len(rep.Targets) != 2 {
		t.Fatalf("report: %+v (want 2 targets)", rep)
	}
	by := byTarget(rep)
	if v := by["a"]; v.Status != StatusVerified || v.Checked != 1 || v.Missing != 0 {
		t.Fatalf("target a must be verified with no missing: %+v", v)
	}
	if v := by["b"]; v.Status != StatusDrift || v.Checked != 1 || v.Missing != 1 || !v.Failed() {
		t.Fatalf("target b's lost object must be DRIFT (missing=1, failing): %+v", v)
	}
	if rep.FailErr() == nil {
		t.Fatal("a silent-loss (drift) target must fail the audit verdict")
	}
}

// existsErrSink models a PutObject-only WORM credential: Exists always errors (403).
type existsErrSink struct{ stubSink }

func (existsErrSink) Exists(context.Context, string) (bool, error) {
	return false, errors.New("AccessDenied")
}

// TestAuditWORMTargetUnverifiable: a target whose Exists always errors is Unverifiable, and that
// FAILS the audit for a NON-worm target (a broken read path) but NOT for a worm target (read-denying
// by design).
func TestAuditWORMTargetUnverifiable(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "hashA"},
		[]TargetStatus{{Target: "t", State: StateStored}, {Target: "worm", State: StateStored}})

	repBad := Audit(ctx, led, []Target{{Sink: existsErrSink{stubSink{name: "t"}}}}, 0)
	if v := repBad.Targets[0]; v.Status != StatusUnverifiable || !v.Failed() {
		t.Fatalf("a non-worm all-errored target must be Unverifiable and FAIL: %+v", v)
	}

	repWorm := Audit(ctx, led, []Target{{Sink: existsErrSink{stubSink{name: "worm"}}, Worm: true}}, 0)
	if v := repWorm.Targets[0]; v.Status != StatusUnverifiable || v.Failed() {
		t.Fatalf("a worm all-errored target must be Unverifiable but NOT fail: %+v", v)
	}
}

// TestAuditCancelledIsBenignAtDomain: a cancelled parent yields a benign NoData verdict at the domain
// level (FailErr nil) — the top-level ctx.Err() fold in runAudit surfaces the abort, so the domain
// must NOT manufacture a spurious per-target failure from the shutdown.
func TestAuditCancelledIsBenignAtDomain(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	led := newFakeLedger()
	_ = led.RecordBackup(context.Background(), ObjectMeta{ExternalID: "hashA"},
		[]TargetStatus{{Target: "a", State: StateStored}})
	a := newMemSink("a")
	a.store["hashA"] = []byte("x")
	rep := Audit(ctx, led, []Target{{Sink: a}}, 0)
	if v := rep.Targets[0]; v.Status != StatusNoData || v.Failed() {
		t.Fatalf("a cancelled audit must be benign NoData at the domain level, got %+v", v)
	}
	if rep.FailErr() != nil {
		t.Fatalf("a cancelled audit must not fail via the domain verdict (runAudit folds ctx.Err()), got %v", rep.FailErr())
	}
}

// TestAuditSampleWrapsFromHighStart: a sampled audit starting near the END of the keyspace must WRAP
// to the start and still check the full sample — not under-check (a false clean pass).
func TestAuditSampleWrapsFromHighStart(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	a := newMemSink("a")
	for i := 0; i < 10; i++ {
		id := string(rune('0'+i/10)) + string(rune('0'+i%10))
		_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: id}, []TargetStatus{{Target: "a", State: StateStored}})
		a.store[id] = []byte("x")
	}
	rep := auditWithStart(ctx, led, []Target{{Sink: a}}, 5, "zz")
	if rep.Targets[0].Checked != 5 {
		t.Fatalf("a high-start sampled audit must wrap and check the full sample, got Checked=%d", rep.Targets[0].Checked)
	}
}

// TestAuditSampleNoBoundaryDoubleCount: a mid-keyspace start that wraps must NOT re-check the
// objects it already checked in pass 1 — Checked must not exceed the distinct object count.
func TestAuditSampleNoBoundaryDoubleCount(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	a := newMemSink("a")
	for i := 0; i < 10; i++ {
		id := "0" + string(rune('0'+i))
		_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: id}, []TargetStatus{{Target: "a", State: StateStored}})
		a.store[id] = []byte("x")
	}
	rep := auditWithStart(ctx, led, []Target{{Sink: a}}, 20, "05")
	if c := rep.Targets[0].Checked; c != 9 {
		t.Fatalf("wrapped sample must not double-count the boundary: Checked=%d (want 9 distinct)", c)
	}
}

// TestVerdictStatusString pins the printed status labels (the audit report shows them per target):
// a drift/corrupt/fault must render LOUD (uppercase) so an operator scanning output sees it.
func TestVerdictStatusString(t *testing.T) {
	for status, want := range map[VerdictStatus]string{
		StatusVerified:     "verified",
		StatusDrift:        "DRIFT",
		StatusNoData:       "no-data",
		StatusUnverifiable: "unverifiable",
		StatusCorrupt:      "CORRUPT",
		StatusFault:        "FAULT",
		VerdictStatus(99):  "unknown",
	} {
		if got := status.String(); got != want {
			t.Fatalf("status %d String()=%q, want %q", status, got, want)
		}
	}
}

// TestClassifyAuditErr pins the ledger→target error classifier: a parent SIGTERM → benign NoData; a
// ledger read fault → a failing Fault; a per-page deadline (parent live) → Unverifiable (a non-worm
// target then fails).
func TestClassifyAuditErr(t *testing.T) {
	timeout := classifyAuditErr(context.Background(), "t", context.DeadlineExceeded)
	if timeout.Status != StatusUnverifiable || !timeout.Failed() {
		t.Fatalf("a per-page deadline must be Unverifiable + failing (non-worm), got %+v", timeout)
	}
	fault := classifyAuditErr(context.Background(), "t", fmt.Errorf("%w: boom", errLedgerRead))
	if fault.Status != StatusFault || fault.Err == nil || !fault.Failed() {
		t.Fatalf("a ledger read error must be a failing Fault carrying the cause, got %+v", fault)
	}
	pctx, cancel := context.WithCancel(context.Background())
	cancel()
	shutdown := classifyAuditErr(pctx, "t", context.Canceled)
	if shutdown.Status != StatusNoData || shutdown.Failed() {
		t.Fatalf("a parent SIGTERM must be benign NoData, got %+v", shutdown)
	}
}

// TestAuditVerdictPolicy pins the shared pass/fail policy for the ledger→target direction on the
// TargetVerdict: a non-worm partial-unverifiable fails; the same on a worm target passes; a clean
// (Verified) sweep passes.
func TestAuditVerdictPolicy(t *testing.T) {
	partial := VerdictReport{Targets: []TargetVerdict{{Target: "t", Status: StatusUnverifiable, Checked: 10}}}
	if err := partial.FailErr(); err == nil {
		t.Fatal("a non-worm unverifiable target must fail the audit")
	}
	worm := VerdictReport{Targets: []TargetVerdict{{Target: "w", Worm: true, Status: StatusUnverifiable, Checked: 10}}}
	if err := worm.FailErr(); err != nil {
		t.Fatalf("a worm target's read-denied probes must not fail the audit: %v", err)
	}
	clean := VerdictReport{Targets: []TargetVerdict{{Target: "t", Status: StatusVerified, Checked: 10}}}
	if err := clean.FailErr(); err != nil {
		t.Fatalf("a clean audit must pass: %v", err)
	}
}
