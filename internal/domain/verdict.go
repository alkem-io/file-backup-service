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
	// StatusVerified means the target was checked and is correct.
	StatusVerified VerdictStatus = iota
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

// Failed is the ONE audit pass/fail policy, a pure function of (status, worm), shared by every
// direction so the rule can't drift between them:
//   - Verified / NoData → benign (not a failure).
//   - Drift / Corrupt / Fault → always a failure.
//   - Unverifiable → a failure iff the target is NOT worm (a read-denying WORM copy is expected to
//     be unverifiable by design; a normally-readable target that suddenly can't be read is a broken
//     read path, and an incomplete integrity check must not read green).
func (s VerdictStatus) Failed(worm bool) bool {
	switch s {
	case StatusDrift, StatusCorrupt, StatusFault:
		return true
	case StatusUnverifiable:
		return !worm
	default: // StatusVerified, StatusNoData
		return false
	}
}

// TargetVerdict is one target's outcome for ONE audit direction. Status is the sole source of truth
// for the pass/fail verdict and the gauge; the count fields are direction-specific breadcrumbs the
// printed report shows. Illegal combinations are unrepresentable: a target has exactly one Status,
// and Failed() derives from it — there is no way to record "drift" and "benign" at once.
type TargetVerdict struct {
	Target  string
	Worm    bool
	Status  VerdictStatus
	Detail  string
	Checked int   // ledger→target: objects probed on the sink
	Missing int   // ledger→target: ledger-stored but absent; inventory: ledger-stored, not in manifest
	Extra   int   // inventory: in the manifest but NOT ledger-stored (orphan / lost ledger record)
	Err     error // Corrupt/Fault: the underlying cause, surfaced by FailErr so a caller can errors.Is it
}

// Failed reports whether this verdict fails the audit, per the shared VerdictStatus policy.
func (v TargetVerdict) Failed() bool { return v.Status.Failed(v.Worm) }

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
// Target/Worm and guarantees an always-populated slice.
//
// perTargetTimeout<=0 runs the closure on the parent ctx (no whole-probe deadline) — for the sweep
// directions that must scale with the corpus and bound their own per-page/per-read operations
// instead; >0 bounds the whole closure (a single-RTT probe like the immutability config read).
func probeTargets(ctx context.Context, targets []Target, perTargetTimeout time.Duration,
	probe func(pctx context.Context, t Target) TargetVerdict) []TargetVerdict {
	out := make([]TargetVerdict, len(targets))
	_ = RunParallelIdx(len(targets),
		func(i int) string { return "probe " + targets[i].Sink.Name() },
		func(i int) error {
			out[i] = probeOne(ctx, targets[i], perTargetTimeout, probe)
			return nil
		})
	return out
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
			return TargetVerdict{Status: StatusFault, Err: PanicErr("probe "+t.Sink.Name(), r)}
		})
	v.Target = t.Sink.Name()
	v.Worm = t.Worm
	return v
}

// abandonVerdict classifies a probe abandoned at its ctx boundary: a PARENT cancel (SIGTERM) is a
// benign shutdown (NoData — the top-level ctx.Err() fold surfaces the abort, not a spurious
// per-target failure); a per-target DeadlineExceeded while the parent is still live is a WEDGED
// target (Unverifiable → fails a non-worm target: an incomplete integrity check must not read green).
func abandonVerdict(parentCtx, probeCtx context.Context) TargetVerdict {
	if parentCtx.Err() != nil {
		return TargetVerdict{Status: StatusNoData, Detail: fmt.Sprintf("aborted by shutdown: %v", probeCtx.Err())}
	}
	return TargetVerdict{Status: StatusUnverifiable, Detail: fmt.Sprintf("per-target probe deadline exceeded (wedged target): %v", probeCtx.Err())}
}
