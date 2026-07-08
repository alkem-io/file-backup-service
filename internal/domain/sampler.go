package domain

import (
	"context"
	"errors"
)

// BacklogReader is the outbox read the RPO sampler needs — backlog depth + oldest-pending age.
// A narrow interface (not the whole Outbox port) so a sampler fake needn't implement Claim/Fail/…
type BacklogReader interface {
	// BacklogStats returns the pending-work count and the age (seconds) of the oldest pending entry.
	BacklogStats(ctx context.Context) (pending int, oldestAgeSec float64, err error)
}

// CoverageReader is the ledger reads the RPO/coverage sampler needs. Narrow, for the same reason.
type CoverageReader interface {
	// LastVerifiedAge returns the stalest configured target's last-verified age (seconds), the
	// count of never-verified targets, and ok=false at bootstrap (nothing verified anywhere yet).
	LastVerifiedAge(ctx context.Context, allTargets []string) (ageSec float64, neverVerified int, ok bool, err error)
	// CoverageGaps returns the count of objects not yet stored on every configured target.
	CoverageGaps(ctx context.Context, allTargets []string) (int, error)
}

// RPOGauges receives the sampled RPO/lag/coverage observations — the alerting spine (SC-001).
// The Prometheus adapter implements it; a fake records the calls so the sampling POLICY below is
// testable without a live serve + two Postgres DBs.
type RPOGauges interface {
	// SetBacklog sets the pending-work depth + oldest-pending age gauges.
	SetBacklog(pending int, oldestAgeSec float64)
	// SetLastSuccessAge sets the stalest-target last-verified age gauge.
	SetLastSuccessAge(ageSec float64)
	// SetNeverVerified sets the count of configured targets that have never verified anything.
	SetNeverVerified(n int)
	// SetCircuitOpen sets the count of targets whose circuit is currently open.
	SetCircuitOpen(n int)
	// SetUnderReplicated sets the count of objects not stored on every configured target.
	SetUnderReplicated(n int)
	// SampleError increments the failed-sample counter (alert on rate>0 → a frozen gauge).
	SampleError()
}

// Sampler owns the RPO/coverage sampling POLICY (FR-026): which gauges to refresh from the
// outbox + ledger, and the rule that ANY read failure flags one SampleError so a frozen
// stale-green gauge is itself detectable. It lives in domain (not cmd's inline closures over
// concrete *db types) so that policy — the alerting spine — is fake-testable.
type Sampler struct {
	backlog  BacklogReader
	coverage CoverageReader
	targets  []string
	circuit  *CircuitBreaker
	gauges   RPOGauges
}

// NewSampler binds a Sampler to its reads, the configured target names, the circuit breaker
// (for the targets-down gauge; may be nil), and the gauge sink.
func NewSampler(backlog BacklogReader, coverage CoverageReader, targets []Target, circuit *CircuitBreaker, gauges RPOGauges) *Sampler {
	return &Sampler{backlog: backlog, coverage: coverage, targets: TargetNames(targets), circuit: circuit, gauges: gauges}
}

// SampleRPO does one RPO pass: the circuit-open gauge (in-memory, always sampleable), the
// backlog depth+age, and the per-target last-verified age + never-verified count. Each gauge it
// CAN compute is set independently (a failed read doesn't suppress a successful sibling); it
// returns a non-nil error iff a DB read failed, so the caller fires SampleError exactly once —
// stating the failure side-effect in ONE place (TickLoop routes both this error and a panic to
// the same onError).
func (s *Sampler) SampleRPO(ctx context.Context) error {
	s.gauges.SetCircuitOpen(s.circuitOpen())
	var errs []error
	if pending, ageSec, err := s.backlog.BacklogStats(ctx); err == nil {
		s.gauges.SetBacklog(pending, ageSec)
	} else {
		errs = append(errs, err)
	}
	if ageSec, never, ok, err := s.coverage.LastVerifiedAge(ctx, s.targets); err == nil {
		s.gauges.SetNeverVerified(never) // a from-day-one dead target is counted, not invisible
		if ok {
			s.gauges.SetLastSuccessAge(ageSec)
		}
	} else {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// SampleCoverage does one coverage pass — the full-ledger under-replication count (a coarse
// backstop a dead-lettered object can't hide from). A read failure returns the error (→ one
// SampleError) and leaves the gauge at its last value.
func (s *Sampler) SampleCoverage(ctx context.Context) error {
	n, err := s.coverage.CoverageGaps(ctx, s.targets)
	if err != nil {
		return err
	}
	s.gauges.SetUnderReplicated(n)
	return nil
}

func (s *Sampler) circuitOpen() int {
	if s.circuit == nil {
		return 0
	}
	return s.circuit.OpenCount()
}
