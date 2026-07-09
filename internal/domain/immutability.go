package domain

import (
	"context"
	"fmt"
	"io"
)

// immutabilityChecker is an OPTIONAL sink capability (only the s3 sink implements it) for the
// WORM drift-check (T032/FR-014): report whether the target bucket still enforces object-lock
// and versioning. It is defined structurally over plain types so the s3 adapter satisfies it
// WITHOUT importing domain (the sinks stay infrastructure-pure). A read-denying (PutObject-only)
// credential errors here — that is EXPECTED and surfaces as "unverifiable", never as drift.
type immutabilityChecker interface {
	CheckImmutability(ctx context.Context) (objectLock, versioning bool, err error)
}

// inventoryReader is an OPTIONAL sink capability (s3 + filesystem) for the audit target→ledger
// direction (T032/FR-025): return the target's most-recent manifest snapshot — its OWN declared
// inventory — so audit can diff it against the ledger without a full physical object List (which
// a WORM/PutObject-only target can't do). Structural (plain types) so the adapters don't import
// domain. A read-denying target's read errors → the target is reported unverifiable, not failed.
type inventoryReader interface {
	LatestManifest(ctx context.Context) (io.ReadCloser, error)
}

// ImmutabilityResult is one target's WORM drift-check outcome.
type ImmutabilityResult struct {
	Target string
	OK     bool // VERIFIED: object-lock AND versioning are both still enabled (meaningful only when !Unverifiable)
	// Unverifiable: no definitive verdict. Structural=true means the sink CANNOT report object-lock
	// AT ALL (a non-s3 target) — it will never have a signal, so the gauge drops any series;
	// Structural=false is a TRANSIENT / read-denying error (a 403 PutObject-only WORM credential, a
	// timeout) — the gauge KEEPS the prior value (a slow-but-healthy target must not go no-signal).
	Unverifiable bool
	Structural   bool
	Detail       string // human-readable state or the unverifiable reason
}

// Failed reports a GENUINE drift: the probe read the config and object-lock or versioning is no
// longer enabled. An unverifiable target (read-denying WORM credential, or a non-s3 target) is
// NOT a failure — it is reported unverifiable, per the WORM contract. Checked (== !Unverifiable) is
// derived, not stored, so the drift verdict can't drift out of lock-step with the state.
func (r ImmutabilityResult) Failed() bool { return !r.Unverifiable && !r.OK }

// ImmutabilityFailErr is the WORM drift pass/fail VERDICT over a target set — non-nil (a nonzero
// exit for cron/CI) when any Worm target's object-lock/versioning is verifiably no longer enabled.
// An unverifiable target (read-denying credential) never fails, matching the gauge's "skip
// unverifiable" policy so audit and the serve gauge agree on what counts as drift.
func ImmutabilityFailErr(results []ImmutabilityResult) error {
	var drifted []string
	for _, r := range results {
		if r.Failed() {
			drifted = append(drifted, r.Target)
		}
	}
	if len(drifted) > 0 {
		return fmt.Errorf("targets with WORM immutability drift (object-lock/versioning no longer enabled): %v", drifted)
	}
	return nil
}

// CheckImmutability runs the WORM drift-check on every target that is EXPECTED to be immutable
// (Target.Worm). A non-Worm target is skipped entirely (it carries no result). A Worm target
// whose sink can't report object-lock (a filesystem target, or an s3 target whose read-denying
// credential 403s the GetObjectLockConfiguration/GetBucketVersioning calls) is Unverifiable —
// NOT drifted — so a PutObject-only immutable target never false-alerts. Each probe is bounded
// and recover-guarded (a driver panic becomes that target's error, not a crash), and runs
// concurrently since each is an independent backend RTT.
func CheckImmutability(ctx context.Context, targets []Target) []ImmutabilityResult {
	worm := make([]Target, 0, len(targets))
	for _, t := range targets {
		if t.Worm {
			worm = append(worm, t)
		}
	}
	results := make([]ImmutabilityResult, len(worm))
	// RunParallelIdx's per-goroutine recover already folds a driver panic into that index's
	// error; checkOne ALSO recovers (RunAbandonable) and writes an unverifiable result, so the
	// slice is always fully populated regardless. Each probe is an independent backend RTT.
	_ = RunParallelIdx(len(worm),
		func(i int) string { return "immutability " + worm[i].Sink.Name() },
		func(i int) error { results[i] = checkOne(ctx, worm[i]); return nil })
	return results
}

// checkOne probes a single Worm target's immutability, bounded by auditProbeTimeout (the shared DR
// probe deadline) and recover-guarded so a driver panic (a broken minio response) becomes a
// TRANSIENT-unverifiable result rather than crashing the sampler/audit goroutine. A sink that can't
// report object-lock at all (not s3) is STRUCTURAL-unverifiable; a read-deny / timeout / panic is
// TRANSIENT-unverifiable (the gauge keeps the prior value).
func checkOne(ctx context.Context, t Target) ImmutabilityResult {
	name := t.Sink.Name()
	ic, ok := t.Sink.(immutabilityChecker)
	if !ok {
		return ImmutabilityResult{Target: name, Unverifiable: true, Structural: true, Detail: "target type cannot report object-lock (not an S3 target)"}
	}
	pctx, cancel := context.WithTimeout(ctx, auditProbeTimeout)
	defer cancel()
	return RunAbandonable(pctx,
		func() ImmutabilityResult {
			lock, versioning, err := ic.CheckImmutability(pctx)
			if err != nil {
				// A read-denying WORM credential (403) — or any inability to READ the config — is
				// unverifiable by design, NOT drift: report it as such so the `== 0` alert never
				// fires on a target we simply couldn't inspect. TRANSIENT (Structural=false): keep prior.
				return ImmutabilityResult{Target: name, Unverifiable: true, Detail: fmt.Sprintf("unverifiable (read-denying credential or transient error): %v", err)}
			}
			return ImmutabilityResult{
				Target: name,
				OK:     lock && versioning,
				Detail: fmt.Sprintf("object-lock=%v versioning=%v", lock, versioning),
			}
		},
		func() ImmutabilityResult {
			return ImmutabilityResult{Target: name, Unverifiable: true, Detail: fmt.Sprintf("unverifiable (probe deadline/cancel: %v)", pctx.Err())}
		},
		func(r any) ImmutabilityResult {
			return ImmutabilityResult{Target: name, Unverifiable: true, Detail: fmt.Sprintf("unverifiable (probe panicked: %v)", r)}
		})
}

// ImmutabilityGauge receives the per-target WORM drift verdict — the Prometheus adapter
// implements it; a fake records the calls so the serve sampler POLICY is testable without a
// live registry.
type ImmutabilityGauge interface {
	// SetImmutabilityOK sets target's drift gauge (true = ok, false = drift). It is called
	// ONLY for a verifiable target — the sampler never emits a series for an unverifiable one.
	SetImmutabilityOK(target string, ok bool)
	// ClearImmutabilityOK drops target's gauge series (used when the target becomes
	// UNVERIFIABLE) so a previously-green target that turns unreadable can't freeze STALE-GREEN
	// at 1 and mask a real drift — the absent series reads as "no signal", not "ok".
	ClearImmutabilityOK(target string)
}

// ImmutabilitySampler drives the filebackup_immutability_ok gauge from serve: it periodically
// re-checks every Worm target and emits the gauge ONLY for the ones it could verify, so a
// read-denying immutable target (unverifiable) never produces a false `== 0` alert. It lives in
// domain (not a cmd closure) so the "skip unverifiable" policy is fake-testable.
type ImmutabilitySampler struct {
	targets []Target
	gauge   ImmutabilityGauge
}

// NewImmutabilitySampler binds a sampler to the target set + the gauge sink.
func NewImmutabilitySampler(targets []Target, gauge ImmutabilityGauge) *ImmutabilitySampler {
	return &ImmutabilitySampler{targets: targets, gauge: gauge}
}

// Sample runs one drift-check pass and updates the gauge per the WORM signal model:
//   - VERIFIED (ok or drift) → SetImmutabilityOK(1/0).
//   - TRANSIENT-unverifiable (a read-denying credential, a timeout) → do NOTHING — KEEP the prior
//     value. This is the fix for the over-clear: a slow-but-healthy WORM target that merely times
//     out must not go no-signal and hide a real drift; and a read-denying (by-design) target simply
//     never gets a series, so the `== 0` alert can't false-fire.
//   - STRUCTURAL-unverifiable (the sink can NEVER report object-lock — a non-s3 target) → CLEAR any
//     series: it will never carry a valid signal, so it must not linger stale.
//
// It never returns an error for an unverifiable target (that is the expected state of a
// PutObject-only WORM copy); a genuine drift is surfaced through the gauge (0), not this return.
func (s *ImmutabilitySampler) Sample(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, r := range CheckImmutability(ctx, s.targets) {
		switch {
		case !r.Unverifiable:
			s.gauge.SetImmutabilityOK(r.Target, r.OK)
		case r.Structural:
			s.gauge.ClearImmutabilityOK(r.Target)
		default:
			// transient / read-denying → keep the prior value (don't clobber a healthy-but-slow target)
		}
	}
	return nil
}
