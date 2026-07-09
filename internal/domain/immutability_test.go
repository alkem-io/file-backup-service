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

func TestCheckImmutabilityReadDenyingIsUnverifiable(t *testing.T) {
	// A read-denying (PutObject-only) WORM credential 403s the config GET — Unverifiable (NOT drift,
	// NOT structural NoData), and being worm it does NOT fail the audit.
	rep := CheckImmutability(context.Background(), []Target{
		wormTarget(&immSink{stubSink: stubSink{name: "worm403"}, err: errors.New("AccessDenied")}),
	})
	if v := rep.Targets[0]; v.Status != StatusUnverifiable || v.Failed() {
		t.Fatalf("a read-denying WORM target must be Unverifiable and not fail: %+v", v)
	}
	if err := rep.FailErr(); err != nil {
		t.Fatalf("an unverifiable worm target must not fail the verdict, got %v", err)
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
		wormTarget(&immSink{stubSink: stubSink{name: "denied"}, err: errors.New("AccessDenied")}), // Unverifiable
		wormTarget(stubSink{name: "nocap"}),                                                       // NoData
		{Sink: &immSink{stubSink: stubSink{name: "plain"}, lock: true, versioning: true}},         // not Worm
	}, g)
	if err := s.Sample(context.Background()); err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if v, ok := g.set["ok"]; !ok || !v {
		t.Fatalf("verified must set gauge true, got %v/%v", v, ok)
	}
	if g.unverif["ok"] {
		t.Fatal("a verified target must clear its unverifiable signal")
	}
	if v, ok := g.set["drift"]; !ok || v {
		t.Fatalf("drift must set gauge false, got %v/%v", v, ok)
	}
	// denied = Unverifiable, never verified this pass (a by-design write-only WORM): DROP the _ok
	// series (no stale-green) but stay SILENT — do NOT raise the distinct signal (its 403 is expected;
	// paging on the standard immutable prod config would be a continuous false alarm).
	if _, ok := g.set["denied"]; ok {
		t.Fatal("an unverifiable target must NOT keep an _ok series")
	}
	if !g.cleared["denied"] {
		t.Fatal("an unverifiable target must CLEAR its _ok series (no stale-green)")
	}
	if g.unverif["denied"] {
		t.Fatal("a by-design (never-verified) unverifiable WORM target must NOT raise the unverifiable signal")
	}
	// nocap = structural NoData: drop the _ok series but do NOT alert (it can never carry a signal).
	if !g.cleared["nocap"] {
		t.Fatal("a structural-NoData target must clear its _ok series")
	}
	if g.unverif["nocap"] {
		t.Fatal("a structural-NoData target must NOT raise the unverifiable signal (it's expected)")
	}
	if _, ok := g.set["plain"]; ok {
		t.Fatal("a non-Worm target must not be sampled")
	}
}

// TestImmutabilitySamplerAlertsOnLostReadAccess (Pillar 1): a target that was verifiable-green then
// turns unverifiable (an UNEXPECTED loss of read access — e.g. a credential rotated to write-only)
// must NOT stay frozen stale-green — the _ok series is DROPPED and, BECAUSE it was readable before,
// the distinct unverifiable signal IS raised, so a later real drift can't be masked.
func TestImmutabilitySamplerAlertsOnLostReadAccess(t *testing.T) {
	g := newFakeImmGauge()
	sink := &immSink{stubSink: stubSink{name: "flip"}, lock: true, versioning: true}
	s := NewImmutabilitySampler([]Target{wormTarget(sink)}, g)
	if err := s.Sample(context.Background()); err != nil {
		t.Fatalf("pass 1: %v", err)
	}
	if v, present := g.set["flip"]; !present || !v {
		t.Fatalf("pass 1 must set the series green, got %v/%v", v, present)
	}
	sink.err = errors.New("timeout") // pass 2: the formerly-readable target now can't be read
	if err := s.Sample(context.Background()); err != nil {
		t.Fatalf("pass 2: %v", err)
	}
	if !g.cleared["flip"] {
		t.Fatal("a now-unverifiable pass must DROP the formerly-green series (no stale-green)")
	}
	if !g.unverif["flip"] {
		t.Fatal("a target that LOST its read access must raise the distinct unverifiable signal (not go silent)")
	}
}

// TestImmutabilitySamplerByDesignWormStaysSilent (re-review item 2): a WORM target that is
// unverifiable FROM THE START (a PutObject-only worker credential — the standard immutable prod
// config) is NEVER readable, so its 403 is EXPECTED: across passes the sampler clears any _ok series
// but must NEVER raise the unverifiable signal — paging continuously on a healthy, correctly-immutable
// target would be a false alarm. Its immutability is covered by object-lock + audit + never_verified.
func TestImmutabilitySamplerByDesignWormStaysSilent(t *testing.T) {
	g := newFakeImmGauge()
	sink := &immSink{stubSink: stubSink{name: "offsite"}, err: errors.New("AccessDenied")} // 403 from inception
	s := NewImmutabilitySampler([]Target{wormTarget(sink)}, g)
	for pass := 1; pass <= 2; pass++ {
		if err := s.Sample(context.Background()); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if _, ok := g.set["offsite"]; ok {
			t.Fatalf("pass %d: a never-readable WORM target must have no _ok series", pass)
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
