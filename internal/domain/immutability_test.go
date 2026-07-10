package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// immSink is a stubSink that reports a fixed object-lock/versioning verdict (or an error, e.g. a
// read-denying WORM credential 403). A POINTER receiver so a test can flip its verdict between
// sampler passes without a second type.
type immSink struct {
	stubSink
	lock, versioning bool
	err              error
}

func (s *immSink) CheckImmutability(context.Context) (bool, bool, error) {
	return s.lock, s.versioning, s.err
}

// ImmutabilityReadable: immSink models a READ-CAPABLE target (it HAS an audit/read credential and can
// report object-lock/versioning), so an Unverifiable on it is a genuine anomaly that FAILS — NOT the
// exempt by-design write-only case (that is noAuditCredSink, ImmutabilityReadable()==false).
func (s *immSink) ImmutabilityReadable() bool { return true }

// noAuditCredSink is a WORM s3-like target WITHOUT an audit/read credential (ImmutabilityReadable() ==
// false — the standard immutable prod config, a PutObject-only worker credential). Its drift-check is
// N/A → NoData (silent), so it never false-passes AND never false-alerts.
type noAuditCredSink struct{ stubSink }

func (noAuditCredSink) CheckImmutability(context.Context) (bool, bool, error) {
	return false, false, errors.New("AccessDenied")
}
func (noAuditCredSink) ImmutabilityReadable() bool { return false }

func wormTarget(sink Sink) Target { return Target{Sink: sink, Worm: true} }

func TestCheckImmutabilitySkipsNonWorm(t *testing.T) {
	rep := CheckImmutability(context.Background(), []Target{
		{Sink: &immSink{stubSink: stubSink{name: "plain"}, lock: true, versioning: true}}, // not Worm
	})
	if len(rep.Targets) != 0 {
		t.Fatalf("a non-Worm target must not be immutability-checked, got %+v", rep.Targets)
	}
}

func TestCheckImmutabilityOKAndDrift(t *testing.T) {
	rep := CheckImmutability(context.Background(), []Target{
		wormTarget(&immSink{stubSink: stubSink{name: "good"}, lock: true, versioning: true}),
		wormTarget(&immSink{stubSink: stubSink{name: "nolock"}, lock: false, versioning: true}),
		wormTarget(&immSink{stubSink: stubSink{name: "nover"}, lock: true, versioning: false}),
	})
	by := byTarget(rep)
	if v := by["good"]; v.Status != StatusVerified || v.Failed() {
		t.Fatalf("good: want Verified+pass, got %+v", v)
	}
	if v := by["nolock"]; v.Status != StatusDrift || !v.Failed() {
		t.Fatalf("nolock: object-lock off must be DRIFT+fail, got %+v", v)
	}
	if v := by["nover"]; v.Status != StatusDrift || !v.Failed() {
		t.Fatalf("nover: versioning off must be DRIFT+fail, got %+v", v)
	}
	if rep.FailErr() == nil {
		t.Fatal("a set with drift must produce a non-nil verdict")
	}
}

func TestCheckImmutabilityReadCapableErrorFails(t *testing.T) {
	// A READ-CAPABLE WORM target (it HAS an audit/read credential — ImmutabilityReadable()==true) whose
	// config GET errors is Unverifiable AND FAILS: it should have been readable, so a read failure is a
	// genuine anomaly (a credential rotated to write-only, a wedged endpoint). This is what aligns the
	// audit CLI with the serve immutability sampler's alert. (A by-design write-only WORM copy is NoData
	// instead — TestCheckImmutabilityNoAuditCredIsNoData.)
	rep := CheckImmutability(context.Background(), []Target{
		wormTarget(&immSink{stubSink: stubSink{name: "worm403"}, err: errors.New("AccessDenied")}),
	})
	if v := rep.Targets[0]; v.Status != StatusUnverifiable || !v.Failed() {
		t.Fatalf("a read-capable WORM target that errors must be Unverifiable and FAIL: %+v", v)
	}
	if err := rep.FailErr(); err == nil {
		t.Fatal("a read-capable unverifiable worm target must fail the verdict")
	}
}

func TestCheckImmutabilityNoCapabilityIsNoData(t *testing.T) {
	// A Worm target whose sink can't report object-lock AT ALL (a filesystem target — stubSink
	// implements no immutabilityChecker) is NoData (structural: it can never carry a signal).
	rep := CheckImmutability(context.Background(), []Target{wormTarget(stubSink{name: "fsworm"})})
	if v := rep.Targets[0]; v.Status != StatusNoData || v.Failed() {
		t.Fatalf("a non-s3 WORM target must be NoData (benign): %+v", v)
	}
}

// TestCheckImmutabilityNoAuditCredIsNoData (re-review B1): a WORM s3 target WITHOUT a read/audit
// credential (ImmutabilityReadable()==false) is NoData — the drift-check is N/A, so it never
// false-passes and never false-alerts. CheckImmutability is NOT called on it (the gate short-circuits).
func TestCheckImmutabilityNoAuditCredIsNoData(t *testing.T) {
	rep := CheckImmutability(context.Background(), []Target{wormTarget(noAuditCredSink{stubSink{name: "offsite"}})})
	if v := rep.Targets[0]; v.Status != StatusNoData || v.Failed() {
		t.Fatalf("a WORM target with no audit credential must be NoData (silent, N/A): %+v", v)
	}
}

// fakeImmGauge records the three gauge calls so the sampler's derive-from-Status policy is testable
// without a live registry.
type fakeImmGauge struct {
	set     map[string]bool
	cleared map[string]bool
	unverif map[string]bool
}

func newFakeImmGauge() *fakeImmGauge {
	return &fakeImmGauge{set: map[string]bool{}, cleared: map[string]bool{}, unverif: map[string]bool{}}
}
func (g *fakeImmGauge) SetImmutabilityOK(target string, ok bool) {
	g.set[target] = ok
	delete(g.cleared, target)
}
func (g *fakeImmGauge) ClearImmutabilityOK(target string) {
	g.cleared[target] = true
	delete(g.set, target)
}
func (g *fakeImmGauge) SetImmutabilityUnverifiable(target string, unverifiable bool) {
	g.unverif[target] = unverifiable
}

func TestImmutabilitySamplerPolicy(t *testing.T) {
	g := newFakeImmGauge()
	s := NewImmutabilitySampler([]Target{
		wormTarget(&immSink{stubSink: stubSink{name: "ok"}, lock: true, versioning: true}),
		wormTarget(&immSink{stubSink: stubSink{name: "drift"}, lock: false, versioning: true}),
		wormTarget(&immSink{stubSink: stubSink{name: "readfail"}, err: errors.New("timeout")}), // READABLE (has audit cred), read failed → Unverifiable → alert
		wormTarget(noAuditCredSink{stubSink{name: "offsite"}}),                                 // NO audit cred → NoData → silent
		wormTarget(stubSink{name: "nocap"}),                                                    // non-s3 → NoData → silent
		{Sink: &immSink{stubSink: stubSink{name: "plain"}, lock: true, versioning: true}},      // not Worm
	}, g)
	if err := s.Sample(context.Background()); err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if v, ok := g.set["ok"]; !ok || !v || g.unverif["ok"] {
		t.Fatalf("verified must set gauge true + clear the unverifiable signal, got set=%v/%v unverif=%v", v, ok, g.unverif["ok"])
	}
	if v, ok := g.set["drift"]; !ok || v {
		t.Fatalf("drift must set gauge false, got %v/%v", v, ok)
	}
	// readfail = a READ-CAPABLE target (has an audit cred) whose read failed → DROP the _ok series AND
	// raise the distinct signal (a genuinely unexpected fault worth alerting).
	if _, ok := g.set["readfail"]; ok {
		t.Fatal("a readable target that failed its read must NOT keep an _ok series")
	}
	if !g.cleared["readfail"] || !g.unverif["readfail"] {
		t.Fatalf("a readable target that failed its read must clear _ok AND raise the unverifiable signal, cleared=%v unverif=%v", g.cleared["readfail"], g.unverif["readfail"])
	}
	// offsite = NO audit credential (by-design write-only) → NoData: drop _ok, stay SILENT.
	if !g.cleared["offsite"] || g.unverif["offsite"] {
		t.Fatalf("a no-audit-cred WORM target must clear _ok but stay silent, cleared=%v unverif=%v", g.cleared["offsite"], g.unverif["offsite"])
	}
	// nocap = non-s3 NoData: drop _ok, no alert.
	if !g.cleared["nocap"] || g.unverif["nocap"] {
		t.Fatalf("a non-s3 NoData target must clear _ok but stay silent, cleared=%v unverif=%v", g.cleared["nocap"], g.unverif["nocap"])
	}
	if _, ok := g.set["plain"]; ok {
		t.Fatal("a non-Worm target must not be sampled")
	}
}

// TestImmutabilitySamplerByDesignWormStaysSilent (re-review item 2): a WORM target with NO audit
// credential (the standard immutable prod config — a PutObject-only worker credential) is N/A →
// NoData: across passes the sampler must never keep an _ok series NOR raise the unverifiable signal.
// Paging continuously on a healthy, correctly-immutable target would be a false alarm; its
// immutability is asserted by object-lock + the audit (with a read cred) + never_verified.
func TestImmutabilitySamplerByDesignWormStaysSilent(t *testing.T) {
	g := newFakeImmGauge()
	s := NewImmutabilitySampler([]Target{wormTarget(noAuditCredSink{stubSink{name: "offsite"}})}, g)
	for pass := 1; pass <= 2; pass++ {
		if err := s.Sample(context.Background()); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if _, ok := g.set["offsite"]; ok {
			t.Fatalf("pass %d: a no-audit-cred WORM target must have no _ok series", pass)
		}
		if g.unverif["offsite"] {
			t.Fatalf("pass %d: a by-design write-only WORM target must NOT raise the unverifiable signal (false alarm)", pass)
		}
	}
}

func TestImmutabilitySamplerCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := newFakeImmGauge()
	s := NewImmutabilitySampler([]Target{wormTarget(&immSink{stubSink: stubSink{name: "ok"}, lock: true, versioning: true})}, g)
	if err := s.Sample(ctx); err == nil {
		t.Fatal("a cancelled sample must return the ctx error")
	}
}

// TestCheckImmutabilityBoundsHangingProbe: a probe that ignores ctx (a black-holing backend) must be
// abandoned at the per-target auditProbeTimeout and reported Unverifiable, not hang the sampler.
func TestCheckImmutabilityBoundsHangingProbe(t *testing.T) {
	old := auditProbeTimeout
	auditProbeTimeout = 50 * time.Millisecond
	defer func() { auditProbeTimeout = old }()

	h := &hangingImmSink{stubSink: stubSink{name: "hang"}, release: make(chan struct{})}
	t.Cleanup(func() { close(h.release) })
	done := make(chan VerdictReport, 1)
	go func() { done <- CheckImmutability(context.Background(), []Target{wormTarget(h)}) }()
	select {
	case rep := <-done:
		if v := rep.Targets[0]; v.Status != StatusUnverifiable {
			t.Fatalf("a hung immutability probe must be abandoned as Unverifiable, got %+v", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CheckImmutability HUNG on a ctx-ignoring probe")
	}
}

type hangingImmSink struct {
	stubSink
	release chan struct{}
}

func (h *hangingImmSink) CheckImmutability(context.Context) (bool, bool, error) {
	<-h.release
	return false, false, errors.New("released")
}

// panicImmSink panics inside CheckImmutability — a driver bug that probeTargets recovers into a Fault.
type panicImmSink struct{ stubSink }

func (panicImmSink) CheckImmutability(context.Context) (bool, bool, error) {
	panic("boom in drift-check")
}
func (panicImmSink) ImmutabilityReadable() bool { return true }

// TestImmutabilitySamplerFaultReturnsError: a Fault (a recovered driver panic in the drift-check) is
// surfaced as a RETURNED error so the serve loop logs it distinctly — while the gauge still drops
// stale-green AND raises the unverifiable signal (it pages either way). A plain Unverifiable returns no
// error (see TestImmutabilitySamplerPolicy's readfail case).
func TestImmutabilitySamplerFaultReturnsError(t *testing.T) {
	g := newFakeImmGauge()
	s := NewImmutabilitySampler([]Target{wormTarget(panicImmSink{stubSink{name: "boom"}})}, g)
	if err := s.Sample(context.Background()); err == nil {
		t.Fatal("a Fault (recovered panic) must be returned so it is logged distinctly")
	}
	if !g.cleared["boom"] || !g.unverif["boom"] {
		t.Fatalf("a Fault must still drop _ok AND raise the unverifiable signal, cleared=%v unverif=%v", g.cleared["boom"], g.unverif["boom"])
	}
}
