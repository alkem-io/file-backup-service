package domain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// VerdictStatus is the single classification an audit direction assigns to one target — the ONE
// enum shared by all three DR-observability directions (ledger→target existence, WORM immutability
// drift, target→ledger inventory). It exists so the pass/fail POLICY (Failed) and the gauge
// derivation are written ONCE, not re-derived per direction from a soup of parallel booleans.
type VerdictStatus int

const (
	// StatusUnknown is the ZERO VALUE — an unpopulated verdict. It MUST never read as a pass: if a
	// probe goroutine panics in its own body (outside the inner abandon-recover) before writing its
	// slot, that slot stays zero-value, and a passing zero value would let a target SILENTLY PASS.
	// So the zero value fails closed (Failed()=true), and probeTargets additionally folds any
	// recovered body-panic into an explicit StatusFault.
	StatusUnknown VerdictStatus = iota
	// StatusVerified means the target was checked and is correct.
	StatusVerified
	// StatusDrift means the target was DEFINITIVELY checked and is WRONG — silent loss (ledger→target),
	// an orphan / lost ledger record (inventory), or object-lock/versioning removed (immutability).
	// Always fails.
	StatusDrift
	// StatusNoData means there is nothing to verify — no manifest yet, a sink with no such capability,
	// or a read-denied-by-design (PutObject-only) WORM target. Benign; drives no gauge series.
	StatusNoData
	// StatusUnverifiable means the target SHOULD be verifiable but this pass could not read it (a
	// transient error, a wedged per-target probe, a read-denying credential). Fails iff NOT worm.
	StatusUnverifiable
	// StatusCorrupt means content WAS fetched but is structurally broken (a malformed / non-ascending
	// manifest). A real fault — always fails; the underlying cause is carried in Err.
	StatusCorrupt
	// StatusFault means an infrastructure fault on OUR side (a ledger read error, a driver panic).
	// Always fails; the cause is carried in Err.
	StatusFault
)

func (s VerdictStatus) String() string {
	switch s {
	case StatusUnknown:
		return "UNKNOWN"
	case StatusVerified:
		return "verified"
	case StatusDrift:
		return "DRIFT"
	case StatusNoData:
		return "no-data"
	case StatusUnverifiable:
		return "unverifiable"
	case StatusCorrupt:
		return "CORRUPT"
	case StatusFault:
		return "FAULT"
	default:
		return "unknown"
	}
}

// Failed is the ONE audit pass/fail policy, a pure function of (status, exemptUnverifiable), shared by
// every direction so the rule can't drift between them:
//   - Verified / NoData → benign (not a failure).
//   - Drift / Corrupt / Fault → always a failure.
//   - Unverifiable → a failure UNLESS exemptUnverifiable. The axis is read-capability, NOT worm: ONLY
//     a by-design write-only WORM copy (a WORM target whose sink declares !ImmutabilityReadable — no
//     audit/read credential) is exempt, because it legitimately can't be read; every other target —
//     a normal target, a WORM target WITH an audit credential, a filesystem target — FAILS an
//     Unverifiable (a read path that should work but didn't; an incomplete integrity check must not
//     read green). This is why `audit` and the serve immutability sampler agree: a read-capable WORM
//     target that turns unreadable fails BOTH. exemptUnverifiable defaults to FALSE, so an unstamped
//     verdict fails CLOSED (like StatusUnknown) — only an explicit write-only-WORM stamp exempts it.
func (s VerdictStatus) Failed(exemptUnverifiable bool) bool {
	switch s {
	case StatusDrift, StatusCorrupt, StatusFault, StatusUnknown:
		return true // StatusUnknown (the zero value) fails closed — an unpopulated verdict is never a pass
	case StatusUnverifiable:
		return !exemptUnverifiable
	default: // StatusVerified, StatusNoData
		return false
	}
}

// TargetVerdict is one target's outcome for ONE audit direction. Status is the sole source of truth
// for the pass/fail verdict and the gauge; the count fields are direction-specific breadcrumbs the
// printed report shows. Illegal combinations are unrepresentable: a target has exactly one Status,
// and Failed() derives from it — there is no way to record "drift" and "benign" at once.
type TargetVerdict struct {
	Target string
	// ExemptUnverifiable is the axis the Unverifiable pass/fail policy turns on (see
	// VerdictStatus.Failed): true ONLY for a by-design write-only WORM copy (a WORM target whose sink
	// declares !ImmutabilityReadable) that legitimately can't be read; false for every read-capable
	// target. It defaults to false so an unstamped verdict fails CLOSED; probeOne stamps it once.
	ExemptUnverifiable bool
	Status             VerdictStatus
	Detail             string
	Checked            int   // ledger→target: objects probed on the sink
	Missing            int   // ledger→target: ledger-stored but absent; inventory: ledger-stored, not in manifest
	Extra              int   // inventory: in the manifest but NOT ledger-stored (orphan / lost ledger record)
	Err                error // Corrupt/Fault: the underlying cause, surfaced by FailErr so a caller can errors.Is it
}

// Failed reports whether this verdict fails the audit, per the shared VerdictStatus policy.
func (v TargetVerdict) Failed() bool { return v.Status.Failed(v.ExemptUnverifiable) }

// VerdictReport is one direction's per-target verdicts plus the ONE shared pass/fail verdict.
type VerdictReport struct{ Targets []TargetVerdict }

// FailErr is the ONE audit verdict, shared by all three directions: non-nil (a nonzero exit for
// cron/CI) when any target Failed. A Corrupt/Fault target surfaces its underlying Err (so a caller
// can errors.Is a corrupt-manifest / ledger fault); a Drift/Unverifiable target is named with its
// status + detail. nil = every target passed (or was benign).
func (r VerdictReport) FailErr() error {
	var errs []error
	var failed []string
	for _, v := range r.Targets {
		if !v.Failed() {
			continue
		}
		if v.Err != nil { // Corrupt/Fault carry the real cause — surface it, don't flatten to a name
			errs = append(errs, v.Err)
			continue
		}
		failed = append(failed, fmt.Sprintf("%s (%s: %s)", v.Target, v.Status, v.Detail))
	}
	if len(failed) > 0 {
		errs = append(errs, fmt.Errorf("targets failed audit: %v", failed))
	}
	return errors.Join(errs...)
}

// probeTargets is the ONE per-target concurrent-probe engine shared by all three audit directions
// (ledger→target existence, WORM immutability, target→ledger inventory). It owns the concurrency
// (RunParallelIdx, recover-guarded so a driver panic fails one target instead of crashing the
// sweep), the optional per-target child-ctx timeout, and — crucially — the ONE place the
// parent-cancel-vs-per-target-probe-timeout distinction lives: a parent SIGTERM is benign
// (NoData, propagated), a per-target DeadlineExceeded while the parent is still live is a wedged
// target (Unverifiable, which fails a non-worm target). Each direction supplies ONLY its per-target
// closure, which returns a partial verdict (Status/Detail/counts/Err); probeTargets stamps
// Target + ExemptUnverifiable and guarantees an always-populated slice.
//
// perTargetTimeout<=0 runs the closure on the parent ctx (no whole-probe deadline) — for the sweep
// directions that must scale with the corpus and bound their own per-page/per-read operations
// instead; >0 bounds the whole closure (a single-RTT probe like the immutability config read).
func probeTargets(ctx context.Context, targets []Target, perTargetTimeout time.Duration,
	probe func(pctx context.Context, t Target) TargetVerdict) []TargetVerdict {
	out := make([]TargetVerdict, len(targets))
	errs := RunParallelIdx(len(targets),
		func(i int) string { return "probe " + safeSinkName(targets[i]) },
		func(i int) error {
			out[i] = probeOne(ctx, targets[i], perTargetTimeout, probe)
			return nil
		})
	// A panic in probeOne's OWN body — OUTSIDE the inner abandon-recover, e.g. a nil t.Sink deref
	// while stamping the verdict — is recovered by RunParallelIdx and returned here; out[i] would then
	// stay the zero-value TargetVerdict, which MUST NOT read as a pass. Fold any such recovered error
	// into an explicit failing Fault so no target can silently pass on a body panic. Never discard.
	for i, err := range errs {
		if err != nil {
			// StatusFault always fails regardless of ExemptUnverifiable, so no need to compute it here (and
			// the panic being folded may have come from probing the sink — don't re-touch it).
			out[i] = TargetVerdict{Target: safeSinkName(targets[i]), Status: StatusFault, Err: err}
		}
	}
	return out
}

// safeSinkName returns the target's sink name, guarding a nil Sink so building the panic label /
// fault verdict can't itself panic (which would crash the recover handler).
func safeSinkName(t Target) string {
	if t.Sink == nil {
		return "<nil-sink>"
	}
	return t.Sink.Name()
}

// probeOne runs one target's probe in an abandonable goroutine (so a probe that blocks IGNORING ctx
// — a filesystem os.Stat/os.ReadDir on a wedged mount — is abandoned at the deadline rather than
// hanging the sweep), bounded by an optional per-target timeout, and classifies an abandonment /
// panic into the shared verdict vocabulary. The probe's own returned verdict wins on the happy path.
func probeOne(ctx context.Context, t Target, timeout time.Duration,
	probe func(pctx context.Context, t Target) TargetVerdict) TargetVerdict {
	pctx := ctx
	cancel := func() {}
	if timeout > 0 {
		var c context.CancelFunc
		pctx, c = context.WithTimeout(ctx, timeout)
		cancel = c
	}
	defer cancel()
	v := RunAbandonable(pctx,
		func() TargetVerdict { return probe(pctx, t) },
		func() TargetVerdict { return abandonVerdict(ctx, pctx) },
		func(r any) TargetVerdict {
			return TargetVerdict{Status: StatusFault, Err: PanicErr("probe "+safeSinkName(t), r)}
		})
	v.Target = safeSinkName(t)
	v.ExemptUnverifiable = targetUnverifiableExempt(t)
	return v
}

// targetUnverifiableExempt reports whether an Unverifiable verdict for this target is EXPECTED (and so
// exempt from failing — VerdictStatus.Failed). The axis is read-capability, not worm: the ONLY exempt
// case is a WORM target whose sink declares !ImmutabilityReadable() — a by-design write-only copy (no
// audit/read credential) that legitimately can't be read. A non-worm target, a WORM target WITH an
// audit credential (ImmutabilityReadable()==true), and a sink that can't declare readability (the
// filesystem sink — POSIX reads always work on a mounted root; a gone mount is a genuine fault) are
// all NOT exempt, so a read failure on any of them fails the audit. Defaults to false → fail-closed.
func targetUnverifiableExempt(t Target) bool {
	if !t.Worm {
		return false
	}
	if r, ok := t.Sink.(immutabilityReadable); ok {
		return !r.ImmutabilityReadable()
	}
	return false
}

// abandonVerdict classifies a probe abandoned at its ctx boundary: a PARENT cancel (SIGTERM) is a
// benign shutdown (NoData — the top-level ctx.Err() fold surfaces the abort, not a spurious
// per-target failure); a per-target DeadlineExceeded while the parent is still live is a WEDGED
// target (Unverifiable → fails a non-worm target: an incomplete integrity check must not read green).
func abandonVerdict(parentCtx, probeCtx context.Context) TargetVerdict {
	if parentCtx.Err() != nil {
		return shutdownVerdict(probeCtx.Err())
	}
	return TargetVerdict{Status: StatusUnverifiable, Detail: fmt.Sprintf("per-target probe deadline exceeded (wedged target): %v", probeCtx.Err())}
}

// shutdownVerdict is the benign NoData verdict for a probe aborted by a PARENT cancel (a SIGTERM):
// the audit's top-level ctx.Err() fold surfaces the abort, so a per-target shutdown must NOT read as a
// failure. The ONE owner of this construction, shared by abandonVerdict + both direction classifiers.
func shutdownVerdict(cause error) TargetVerdict {
	return TargetVerdict{Status: StatusNoData, Detail: fmt.Sprintf("aborted by shutdown: %v", cause)}
}
