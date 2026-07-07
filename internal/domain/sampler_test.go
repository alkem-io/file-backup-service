package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeBacklog / fakeCoverage / fakeGauges let the sampling POLICY be exercised with zero DBs —
// the exact testability the domain Sampler exists to provide.
type fakeBacklog struct {
	pending int
	age     float64
	err     error
}

func (f fakeBacklog) BacklogStats(context.Context) (int, float64, error) {
	return f.pending, f.age, f.err
}

type fakeCoverage struct {
	lvAge   float64
	lvNever int
	lvOK    bool
	lvErr   error
	cov     int
	covErr  error
}

func (f fakeCoverage) LastVerifiedAge(context.Context, []string) (float64, int, bool, error) {
	return f.lvAge, f.lvNever, f.lvOK, f.lvErr
}
func (f fakeCoverage) CoverageGaps(context.Context, []string) (int, error) { return f.cov, f.covErr }

type fakeGauges struct {
	backlogSet, lastSuccessSet, neverSet, circuitSet, underRepSet bool
	sampleErrors                                                  int
	never, circuit, underRep                                      int
}

func (g *fakeGauges) SetBacklog(int, float64)   { g.backlogSet = true }
func (g *fakeGauges) SetLastSuccessAge(float64) { g.lastSuccessSet = true }
func (g *fakeGauges) SetNeverVerified(n int)    { g.neverSet = true; g.never = n }
func (g *fakeGauges) SetCircuitOpen(n int)      { g.circuitSet = true; g.circuit = n }
func (g *fakeGauges) SetUnderReplicated(n int)  { g.underRepSet = true; g.underRep = n }
func (g *fakeGauges) SampleError()              { g.sampleErrors++ }

func newSampler(b fakeBacklog, c fakeCoverage, circuit *CircuitBreaker, g *fakeGauges) *Sampler {
	return NewSampler(b, c, []Target{{Sink: newMemSink("t1")}, {Sink: newMemSink("t2")}}, circuit, g)
}

func TestSampleRPOAllSucceed(t *testing.T) {
	g := &fakeGauges{}
	s := newSampler(
		fakeBacklog{pending: 3, age: 42},
		fakeCoverage{lvAge: 10, lvNever: 1, lvOK: true},
		nil, g)
	if err := s.SampleRPO(context.Background()); err != nil {
		t.Fatalf("clean pass must not error: %v", err)
	}
	if !g.backlogSet || !g.neverSet || !g.lastSuccessSet || !g.circuitSet {
		t.Fatalf("all gauges must be set on a clean pass: %+v", g)
	}
	if g.circuit != 0 { // nil breaker → 0
		t.Fatalf("nil circuit must read 0, got %d", g.circuit)
	}
}

func TestSampleRPOReportsCircuitOpenCount(t *testing.T) {
	// A real breaker with one tripped target must be reflected in the circuit-open gauge.
	cb := NewCircuitBreaker(1, time.Minute)
	cb.Record("down", false) // threshold 1 → trips open
	g := &fakeGauges{}
	s := newSampler(fakeBacklog{}, fakeCoverage{lvOK: true}, cb, g)
	if err := s.SampleRPO(context.Background()); err != nil {
		t.Fatalf("clean pass: %v", err)
	}
	if !g.circuitSet || g.circuit != 1 {
		t.Fatalf("circuit-open gauge must reflect the tripped target: got set=%v n=%d", g.circuitSet, g.circuit)
	}
}

func TestSampleRPOPartialFailureStillSetsSiblingAndFlags(t *testing.T) {
	// BacklogStats fails but LastVerifiedAge succeeds: the never/last-success gauges must STILL
	// be set (a failed read doesn't suppress a healthy sibling), and the pass returns an error
	// so the caller fires exactly one SampleError.
	g := &fakeGauges{}
	s := newSampler(
		fakeBacklog{err: errors.New("backlog boom")},
		fakeCoverage{lvAge: 10, lvNever: 2, lvOK: true},
		nil, g)
	err := s.SampleRPO(context.Background())
	if err == nil {
		t.Fatal("a failed read must return an error so the caller flags a SampleError")
	}
	if g.backlogSet {
		t.Fatal("backlog gauge must NOT be set when BacklogStats failed")
	}
	if !g.neverSet || !g.lastSuccessSet {
		t.Fatal("the healthy LastVerifiedAge sibling gauges must still be set")
	}
}

func TestSampleRPOBootstrapNoLastSuccess(t *testing.T) {
	// ok=false (nothing verified yet): SetNeverVerified fires, SetLastSuccessAge does NOT, no error.
	g := &fakeGauges{}
	s := newSampler(fakeBacklog{pending: 0}, fakeCoverage{lvNever: 2, lvOK: false}, nil, g)
	if err := s.SampleRPO(context.Background()); err != nil {
		t.Fatalf("bootstrap is not a failure: %v", err)
	}
	if !g.neverSet || g.lastSuccessSet {
		t.Fatalf("bootstrap: never set, last-success NOT set — got %+v", g)
	}
}

func TestSampleCoverage(t *testing.T) {
	g := &fakeGauges{}
	s := newSampler(fakeBacklog{}, fakeCoverage{cov: 7}, nil, g)
	if err := s.SampleCoverage(context.Background()); err != nil {
		t.Fatalf("clean coverage: %v", err)
	}
	if !g.underRepSet || g.underRep != 7 {
		t.Fatalf("under-replicated gauge must be set to 7: %+v", g)
	}
	// A failed read leaves the gauge untouched and returns the error.
	g2 := &fakeGauges{}
	s2 := newSampler(fakeBacklog{}, fakeCoverage{covErr: errors.New("scan boom")}, nil, g2)
	if err := s2.SampleCoverage(context.Background()); err == nil {
		t.Fatal("a failed coverage read must return an error")
	}
	if g2.underRepSet {
		t.Fatal("the gauge must not be set when the read failed")
	}
}
