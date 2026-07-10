package domain

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// immutabilityChecker is an OPTIONAL sink capability (only the s3 sink implements it) for the
// WORM drift-check (T032/FR-014): report whether the target bucket still enforces object-lock
// and versioning. It is defined structurally over plain types so the s3 adapter satisfies it
// WITHOUT importing domain (the sinks stay infrastructure-pure). A read-denying (PutObject-only)
// credential errors here — that is EXPECTED and surfaces as Unverifiable, never as drift.
type immutabilityChecker interface {
	CheckImmutability(ctx context.Context) (objectLock, versioning bool, err error)
}

// immutabilityReadable is an OPTIONAL sink capability: report whether the target has a credential
// that CAN read its object-lock config (an audit/read credential). A sink NOT implementing it is
// assumed readable (the check proceeds); a sink implementing it and returning false → the WORM
// drift-check is legitimately N/A → NoData (silent), so a by-design write-only WORM copy never
// false-passes AND never false-alerts, and the worker's write-only credential no longer makes EVERY
// WORM target unverifiable-but-passing.
type immutabilityReadable interface {
	ImmutabilityReadable() bool
}

// inventoryReader is an OPTIONAL sink capability (s3 + filesystem) for the audit target→ledger
// direction (T032/FR-025): return the target's most-recent manifest snapshot — its OWN declared
// inventory — so audit can diff it against the ledger without a full physical object List (which
// a WORM/PutObject-only target can't do). Structural (plain types) so the adapters don't import
// domain. A read-denying target's read errors → the target is reported Unverifiable, not failed.
type inventoryReader interface {
	LatestManifest(ctx context.Context) (io.ReadCloser, error)
}

// CheckImmutability runs the WORM drift-check on every target that is EXPECTED to be immutable
// (Target.Worm), returning one TargetVerdict each. A non-Worm target is skipped entirely. The
// per-target concurrency, the auditProbeTimeout bound, and the wedged-vs-shutdown classification
// are owned by the shared probeTargets engine; this direction supplies only the per-target closure:
//   - a sink that can't report object-lock at all (a filesystem target) → NoData (structural: it can
//     NEVER carry a signal; the gauge drops any series and never alerts);
//   - a read-denying credential / transient error / timeout → Unverifiable (the target SHOULD be
//     readable but wasn't this pass — the gauge drops stale-green and raises a distinct signal);
//   - object-lock AND versioning both enabled → Verified; either disabled → Drift.
func CheckImmutability(ctx context.Context, targets []Target) VerdictReport {
	worm := make([]Target, 0, len(targets))
	for _, t := range targets {
		if t.Worm {
			worm = append(worm, t)
		}
	}
	return VerdictReport{Targets: probeTargets(ctx, worm, auditProbeTimeout, immutabilityProbe)}
}

// immutabilityProbe is one Worm target's drift-check closure. A driver panic is caught by
// probeTargets (→ Fault); a wedged probe that ignores ctx is abandoned at auditProbeTimeout (→
// Unverifiable) by probeTargets — so this closure handles only the clean returns.
func immutabilityProbe(pctx context.Context, t Target) TargetVerdict {
	ic, ok := t.Sink.(immutabilityChecker)
	if !ok {
		return TargetVerdict{Status: StatusNoData, Detail: "target type cannot report object-lock (not an S3 target)"}
	}
	// A WORM target WITHOUT a read/audit credential can't be checked by the worker's write-only
	// credential — the drift-check is legitimately N/A → NoData (silent), never a false pass and never
	// a false alert. Its immutability is asserted by object-lock + the audit + never_verified.
	if r, ok := t.Sink.(immutabilityReadable); ok && !r.ImmutabilityReadable() {
		return TargetVerdict{Status: StatusNoData, Detail: "no audit/read credential — immutability asserted by object-lock + never_verified"}
	}
	lock, versioning, err := ic.CheckImmutability(pctx)
	if err != nil {
		// A read-denying WORM credential (403), or any inability to READ the config, is Unverifiable
		// by design — NOT drift: the immutable off-site copy's write-only credential legitimately
		// can't read its own config, and a normally-readable target that suddenly can't be read is a
		// broken read path, not a lock that was removed.
		return TargetVerdict{Status: StatusUnverifiable, Detail: fmt.Sprintf("unverifiable (read-denying credential or transient error): %v", err)}
	}
	if lock && versioning {
		return TargetVerdict{Status: StatusVerified, Detail: fmt.Sprintf("object-lock=%v versioning=%v", lock, versioning)}
	}
	return TargetVerdict{Status: StatusDrift, Detail: fmt.Sprintf("object-lock=%v versioning=%v", lock, versioning)}
}

// ImmutabilityGauge receives the per-target WORM drift verdict — the Prometheus adapter implements
// it; a fake records the calls so the serve sampler POLICY is testable without a live registry.
type ImmutabilityGauge interface {
	// SetImmutabilityOK sets target's drift gauge (true = ok, false = drift). Called ONLY for a
	// VERIFIED or DRIFTED target — never for an unverifiable one (whose series is dropped).
	SetImmutabilityOK(target string, ok bool)
	// ClearImmutabilityOK drops target's drift-gauge series — used when the target becomes
	// unverifiable, so a formerly-green target that turns unreadable can't FREEZE STALE-GREEN at 1
	// and mask a later real drift (the absent series reads as "no signal", not "ok").
	ClearImmutabilityOK(target string)
	// SetImmutabilityUnverifiable raises (true) or drops (false) target's distinct
	// filebackup_immutability_unverifiable signal — the alertable marker that a target the worker
	// SHOULD be able to read has been unverifiable (a credential rotated to write-only, a persistent
	// read-deny, a wedged endpoint). It is what lets us drop the stale-green _ok series WITHOUT going
	// silent: the operator alerts on this signal instead of on a frozen gauge.
	SetImmutabilityUnverifiable(target string, unverifiable bool)
}

// ImmutabilitySampler drives the filebackup_immutability_ok gauge from serve: it periodically
// re-checks every Worm target and maps each verdict's Status to a gauge action (below). It lives in
// domain (not a cmd closure) so the derive-gauge-from-Status policy is fake-testable. The read-cred
// model makes it STATELESS: a WORM target without an audit/read credential is NoData (silent) at the
// probe, so an Unverifiable verdict here only ever means a READ-CAPABLE target (it HAS a read cred)
// that couldn't be read this pass — a genuinely unexpected fault worth alerting.
type ImmutabilitySampler struct {
	targets []Target
	gauge   ImmutabilityGauge
}

// NewImmutabilitySampler binds a sampler to the target set + the gauge sink.
func NewImmutabilitySampler(targets []Target, gauge ImmutabilityGauge) *ImmutabilitySampler {
	return &ImmutabilitySampler{targets: targets, gauge: gauge}
}

// Sample runs one drift-check pass and derives the gauges from each verdict's Status:
//   - Verified → SetImmutabilityOK(1); clear the unverifiable signal.
//   - Drift → SetImmutabilityOK(0); clear the unverifiable signal.
//   - NoData (a non-s3 target, OR a WORM target with NO audit/read credential — the by-design
//     write-only prod config) → drop the _ok series; NO unverifiable signal (it is expected, not an
//     anomaly — the immutability is asserted by object-lock + the audit + never_verified). This is
//     what stops the continuous false page on a healthy, correctly-immutable target.
//   - Unverifiable / Fault → drop stale-green AND raise the distinct unverifiable signal. Under the
//     read-cred model this only fires for a READ-CAPABLE target (it HAS an audit credential) whose
//     read failed this pass (a transient error, a wedged endpoint, a driver panic) — a genuinely
//     unexpected fault that should alert, so the _ok series is dropped without going silent.
//
// A transient/read-deny Unverifiable returns NO error (the gauge carries it); a Fault (a recovered
// driver panic — a code bug on OUR side) is ALSO returned so the serve loop LOGS it distinctly rather
// than flattening it into the same silent gauge flip as a benign read blip. A genuine drift is
// surfaced via the gauge (0), never a returned error.
func (s *ImmutabilitySampler) Sample(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var faults []error
	for _, v := range CheckImmutability(ctx, s.targets).Targets {
		switch v.Status {
		case StatusVerified:
			s.gauge.SetImmutabilityOK(v.Target, true)
			s.gauge.SetImmutabilityUnverifiable(v.Target, false)
		case StatusDrift:
			s.gauge.SetImmutabilityOK(v.Target, false)
			s.gauge.SetImmutabilityUnverifiable(v.Target, false)
		case StatusNoData:
			s.gauge.ClearImmutabilityOK(v.Target)
			s.gauge.SetImmutabilityUnverifiable(v.Target, false)
		default: // Unverifiable / Fault on a READ-CAPABLE target — drop stale-green AND alert
			s.gauge.ClearImmutabilityOK(v.Target)
			s.gauge.SetImmutabilityUnverifiable(v.Target, true)
			// A Fault carries a recovered driver panic in Err — a code bug, not a benign read blip:
			// surface it so it is LOGGED distinctly, while the gauge above still pages either way.
			if v.Status == StatusFault && v.Err != nil {
				faults = append(faults, v.Err)
			}
		}
	}
	return errors.Join(faults...)
}
