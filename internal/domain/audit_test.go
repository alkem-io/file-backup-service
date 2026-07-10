package domain

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
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

// ImmutabilityReadable()==false models a by-design write-only WORM copy (a PutObject-only credential
// that 403s Exists), so its Unverifiable is EXEMPT — the exemption axis is read-capability, not the
// worm flag. A non-worm existsErrSink is not exempt regardless (targetUnverifiableExempt short-circuits
// on !Worm), so the non-worm case in TestAuditWORMTargetUnverifiable still FAILS.
func (existsErrSink) ImmutabilityReadable() bool { return false }

// readableWormSink models a WORM (object-lock) target whose WORKER credential IS read-capable but which
// has NO separate audit credential (ImmutabilityReadable()==false): object-lock restricts delete/
// overwrite, not GET, so Exists reads fine. This is the config B (removed-behavior) flagged as silently
// skipped by the old short-circuit — it must now be PROBED and get a real Verified/Drift verdict.
type readableWormSink struct {
	stubSink
	present bool // Exists result (models present vs a silently-lost object)
}

func (s readableWormSink) Exists(context.Context, string) (bool, error) { return s.present, nil }
func (readableWormSink) ImmutabilityReadable() bool                     { return false }

// TestAuditWORMTargetUnverifiable: a NON-worm target whose Exists always errors is Unverifiable and
// FAILS (a broken read path); a by-design WRITE-ONLY WORM target (worker cred 403s Exists, no audit
// cred) is PROBED and reaches Unverifiable, which targetUnverifiableExempt makes benign (never a
// short-circuit-to-NoData — a read-capable WORM must not be skipped).
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
		t.Fatalf("a write-only WORM target is probed → Unverifiable but EXEMPT (benign), got %+v", v)
	}
}

// TestAuditReadableWORMIsProbed (regression for B): a WORM target whose worker credential can READ (no
// separate audit cred) must be existence-probed, NOT skipped. A present object → Verified; a silently
// lost object → Drift (missing>0, failing). The old short-circuit reported NoData (exit 0), silently
// hiding silent loss on an 'immutable' target.
func TestAuditReadableWORMIsProbed(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "hashA"}, []TargetStatus{{Target: "w", State: StateStored}})

	present := Audit(ctx, led, []Target{{Sink: readableWormSink{stubSink{name: "w"}, true}, Worm: true}}, 0)
	if v := present.Targets[0]; v.Status != StatusVerified || v.Failed() {
		t.Fatalf("a read-capable WORM with a present object must be Verified (probed, not skipped): %+v", v)
	}
	lost := Audit(ctx, led, []Target{{Sink: readableWormSink{stubSink{name: "w"}, false}, Worm: true}}, 0)
	if v := lost.Targets[0]; v.Status != StatusDrift || v.Missing != 1 || !v.Failed() {
		t.Fatalf("a read-capable WORM with a LOST object must be Drift+fail (silent loss detected): %+v", v)
	}
}

// readDeniedCountingSink models a strictly write-only WORM worker credential: every Exists returns a
// domain.ErrReadDenied-tagged 403, and it counts the probes so a test can assert the audit STOPPED early
// instead of doing a doomed HEAD per object across the whole (multi-page) ledger.
type readDeniedCountingSink struct {
	stubSink
	mu    sync.Mutex
	calls int
}

func (s *readDeniedCountingSink) Exists(context.Context, string) (bool, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return false, fmt.Errorf("stat: %w (AccessDenied)", ErrReadDenied)
}
func (*readDeniedCountingSink) ImmutabilityReadable() bool { return false }

// seedStored records n stored objects on target, returning their ids in the ledger's (byte-sorted)
// page order so a test can pin which id lands on the LAST page.
func seedStored(led *fakeLedger, target string, n int, label string) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = hashOf(fmt.Sprintf("%s-%06d", label, i))
		_ = led.RecordBackup(context.Background(), ObjectMeta{ExternalID: ids[i]},
			[]TargetStatus{{Target: target, State: StateStored}})
	}
	sort.Strings(ids)
	return ids
}

// TestAuditWriteOnlyWORMEarlyStops (Eff1): a by-design write-only WORM target whose worker credential
// uniformly read-denies (403) is Unverifiable-exempt (benign), and the audit STOPS after the first
// all-read-denied page — NOT one doomed HEAD per object across the whole multi-page ledger.
func TestAuditWriteOnlyWORMEarlyStops(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	seedStored(led, "w", KeysetPageSize+500, "wo") // > one page → a full sweep would probe every object
	sink := &readDeniedCountingSink{stubSink: stubSink{name: "w"}}

	v := Audit(ctx, led, []Target{{Sink: sink, Worm: true}}, 0).Targets[0]
	if v.Status != StatusUnverifiable || v.Failed() {
		t.Fatalf("a write-only WORM target must be Unverifiable but EXEMPT (benign): %+v", v)
	}
	if sink.calls == 0 {
		t.Fatal("audit must probe at least one page before concluding write-only")
	}
	if sink.calls > KeysetPageSize {
		t.Fatalf("audit must STOP after the first all-read-denied page (~%d probes), did %d (no early-stop)", KeysetPageSize, sink.calls)
	}
}

// TestAuditReadCapableWORMNotEarlyStopped (Eff1 must not regress B): the read-deny early-stop must fire
// ONLY on a uniform read-denial — a READ-CAPABLE WORM (worker cred reads) with a silently-lost object on
// the LAST page is still fully swept and caught as Drift, because a successful read makes readDenied !=
// checked and disables the early-stop.
func TestAuditReadCapableWORMNotEarlyStopped(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	ids := seedStored(led, "w", KeysetPageSize+5, "rc")
	lost := ids[len(ids)-1] // sorts LAST → only a complete multi-page sweep reaches it

	v := Audit(ctx, led, []Target{{Sink: presentExceptSink{stubSink{name: "w"}, lost}, Worm: true}}, 0).Targets[0]
	if v.Status != StatusDrift || v.Missing != 1 || !v.Failed() {
		t.Fatalf("a read-capable WORM's silent loss on the LAST page must be Drift+fail (no early-stop): %+v", v)
	}
}

// presentExceptSink reports every object present except `lost` (which reads as a clean absence). Models
// a read-capable WORM worker credential with one silently-lost object.
type presentExceptSink struct {
	stubSink
	lost string
}

func (s presentExceptSink) Exists(_ context.Context, h string) (bool, error) { return h != s.lost, nil }
func (presentExceptSink) ImmutabilityReadable() bool                         { return false }

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
		StatusUnknown:      "UNKNOWN",
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

// TestAuditVerdictPolicy pins the shared pass/fail policy on the TargetVerdict: the exemption axis is
// READ-CAPABILITY, not worm. A read-capable partial-unverifiable fails; a NOT-read-capable (by-design
// write-only WORM copy) unverifiable passes; a clean (Verified) sweep passes. A read-capable WORM
// target (one WITH an audit credential) therefore FAILS an Unverifiable too — which is what aligns the
// `audit` CLI with the serve immutability sampler (see targetReadCapable).
func TestAuditVerdictPolicy(t *testing.T) {
	// Not exempt (a read-capable target, the fail-closed default) → an Unverifiable fails.
	partial := VerdictReport{Targets: []TargetVerdict{{Target: "t", Status: StatusUnverifiable, Checked: 10}}}
	if err := partial.FailErr(); err == nil {
		t.Fatal("a read-capable unverifiable target must fail the audit")
	}
	// Exempt (a by-design write-only WORM copy) → an Unverifiable passes.
	writeOnly := VerdictReport{Targets: []TargetVerdict{{Target: "w", ExemptUnverifiable: true, Status: StatusUnverifiable, Checked: 10}}}
	if err := writeOnly.FailErr(); err != nil {
		t.Fatalf("a by-design write-only WORM target's read-denied probes must not fail the audit: %v", err)
	}
	clean := VerdictReport{Targets: []TargetVerdict{{Target: "t", Status: StatusVerified, Checked: 10}}}
	if err := clean.FailErr(); err != nil {
		t.Fatalf("a clean audit must pass: %v", err)
	}
}
