package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// immSink is a stubSink that reports a fixed object-lock/versioning verdict (or an error, e.g. a
// read-denying WORM credential 403), satisfying the immutabilityChecker capability.
type immSink struct {
	stubSink
	lock, versioning bool
	err              error
}

func (s immSink) CheckImmutability(context.Context) (bool, bool, error) {
	return s.lock, s.versioning, s.err
}

func wormTarget(sink Sink) Target { return Target{Sink: sink, Worm: true} }

func TestCheckImmutabilitySkipsNonWorm(t *testing.T) {
	// A non-Worm target carries NO result even if its sink could report lock config.
	res := CheckImmutability(context.Background(), []Target{
		{Sink: immSink{stubSink: stubSink{name: "plain"}, lock: true, versioning: true}}, // not Worm
	})
	if len(res) != 0 {
		t.Fatalf("a non-Worm target must not be immutability-checked, got %+v", res)
	}
}

func TestCheckImmutabilityOKAndDrift(t *testing.T) {
	res := CheckImmutability(context.Background(), []Target{
		wormTarget(immSink{stubSink: stubSink{name: "good"}, lock: true, versioning: true}),
		wormTarget(immSink{stubSink: stubSink{name: "nolock"}, lock: false, versioning: true}),
		wormTarget(immSink{stubSink: stubSink{name: "nover"}, lock: true, versioning: false}),
	})
	by := map[string]ImmutabilityResult{}
	for _, r := range res {
		by[r.Target] = r
	}
	if r := by["good"]; !r.Checked || !r.OK || r.Failed() || r.Unverifiable {
		t.Fatalf("good: want checked+ok, got %+v", r)
	}
	if r := by["nolock"]; !r.Checked || r.OK || !r.Failed() {
		t.Fatalf("nolock: object-lock off must be drift, got %+v", r)
	}
	if r := by["nover"]; !r.Checked || r.OK || !r.Failed() {
		t.Fatalf("nover: versioning off must be drift, got %+v", r)
	}
	if err := ImmutabilityFailErr(res); err == nil {
		t.Fatal("a set with drift must produce a non-nil verdict")
	}
}

func TestCheckImmutabilityReadDenyingIsUnverifiable(t *testing.T) {
	// A read-denying (PutObject-only) WORM credential 403s the config GET — that is UNVERIFIABLE,
	// NOT drift: it must never fail the audit / drive the gauge to 0.
	res := CheckImmutability(context.Background(), []Target{
		wormTarget(immSink{stubSink: stubSink{name: "worm403"}, err: errors.New("AccessDenied")}),
	})
	if len(res) != 1 || !res[0].Unverifiable || res[0].Failed() {
		t.Fatalf("a read-denying WORM target must be unverifiable, not failed: %+v", res)
	}
	if err := ImmutabilityFailErr(res); err != nil {
		t.Fatalf("an unverifiable target must not fail the verdict, got %v", err)
	}
}

func TestCheckImmutabilityNoCapabilityIsUnverifiable(t *testing.T) {
	// A Worm target whose sink can't report object-lock at all (a filesystem target — stubSink
	// implements no immutabilityChecker) is unverifiable, not drift.
	res := CheckImmutability(context.Background(), []Target{wormTarget(stubSink{name: "fsworm"})})
	if len(res) != 1 || !res[0].Unverifiable || res[0].Failed() {
		t.Fatalf("a non-s3 WORM target must be unverifiable: %+v", res)
	}
}

// fakeImmGauge records SetImmutabilityOK calls so the sampler's skip-unverifiable POLICY is
// testable without a live registry.
type fakeImmGauge struct{ set map[string]bool }

func (g *fakeImmGauge) SetImmutabilityOK(target string, ok bool) { g.set[target] = ok }

func TestImmutabilitySamplerEmitsOnlyVerifiable(t *testing.T) {
	g := &fakeImmGauge{set: map[string]bool{}}
	s := NewImmutabilitySampler([]Target{
		wormTarget(immSink{stubSink: stubSink{name: "ok"}, lock: true, versioning: true}),
		wormTarget(immSink{stubSink: stubSink{name: "drift"}, lock: false, versioning: true}),
		wormTarget(immSink{stubSink: stubSink{name: "denied"}, err: errors.New("AccessDenied")}),
		{Sink: immSink{stubSink: stubSink{name: "plain"}, lock: true, versioning: true}}, // not Worm
	}, g)
	if err := s.Sample(context.Background()); err != nil {
		t.Fatalf("Sample: %v", err)
	}
	if v, ok := g.set["ok"]; !ok || !v {
		t.Fatalf("verifiable-ok target must set gauge true, got %v/%v", v, ok)
	}
	if v, ok := g.set["drift"]; !ok || v {
		t.Fatalf("verifiable-drift target must set gauge false, got %v/%v", v, ok)
	}
	if _, ok := g.set["denied"]; ok {
		t.Fatal("a read-denying (unverifiable) target must NOT emit a gauge series (else `== 0` false-fires)")
	}
	if _, ok := g.set["plain"]; ok {
		t.Fatal("a non-Worm target must not be sampled")
	}
}

func TestImmutabilitySamplerCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := &fakeImmGauge{set: map[string]bool{}}
	s := NewImmutabilitySampler([]Target{wormTarget(immSink{stubSink: stubSink{name: "ok"}, lock: true, versioning: true})}, g)
	if err := s.Sample(ctx); err == nil {
		t.Fatal("a cancelled sample must return the ctx error")
	}
}

// TestCheckImmutabilityBoundsHangingProbe: a probe that ignores ctx (a black-holing backend) must
// be abandoned at the per-probe deadline and reported unverifiable, not hang the sampler.
func TestCheckImmutabilityBoundsHangingProbe(t *testing.T) {
	h := &hangingImmSink{stubSink: stubSink{name: "hang"}, release: make(chan struct{})}
	t.Cleanup(func() { close(h.release) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan []ImmutabilityResult, 1)
	go func() { done <- CheckImmutability(ctx, []Target{wormTarget(h)}) }()
	select {
	case res := <-done:
		if len(res) != 1 || !res[0].Unverifiable {
			t.Fatalf("a hung immutability probe must be abandoned as unverifiable, got %+v", res)
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
