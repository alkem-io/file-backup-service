package domain

import (
	"context"
	"errors"
	"fmt"
)

// errStopSample stops EachStoredObject early once a target's audit sample is reached.
var errStopSample = errors.New("sample complete")

// TargetAudit is one target's audit outcome.
type TargetAudit struct {
	Target  string
	Checked int // objects checked (up to the sample)
	Missing int // ledger records stored, but Exists says absent — silent loss
	Errors  int // Exists could not determine presence (e.g. a PutObject-only WORM credential)
}

// Unverifiable reports whether the audit gave NO real coverage for this target: every
// check errored (and at least one ran) — the definitional WORM case, where Exists always
// 403s. Missing==0 then means "couldn't look", NOT "clean", so it must not read as coverage.
func (t TargetAudit) Unverifiable() bool { return t.Checked > 0 && t.Errors == t.Checked }

// AuditReport is the per-target audit result.
type AuditReport struct {
	Targets []TargetAudit
}

// Missing is the total silent-loss count across all targets.
func (r AuditReport) Missing() int {
	n := 0
	for _, t := range r.Targets {
		n += t.Missing
	}
	return n
}

// Audit verifies the ledger against reality (FR-014 drift check / T030): for up to
// samplePerTarget objects the ledger records as stored on each target, it checks the
// target ACTUALLY still holds them (Sink.Exists). A "missing" object is one the ledger
// believes is backed up but the target lost — the silent-loss case reconcile (which
// trusts the ledger) can't detect. samplePerTarget<=0 checks every stored object.
//
// A target whose Exists always errors (a PutObject-only WORM credential) is reported
// Unverifiable rather than clean — Exists is definitionally blind under that credential,
// so audit gives no coverage there and the caller must not mistake it for a pass.
func Audit(ctx context.Context, led Ledger, targets []Target, samplePerTarget int) (AuditReport, error) {
	rep := AuditReport{Targets: make([]TargetAudit, 0, len(targets))}
	for _, t := range targets {
		ta := TargetAudit{Target: t.Sink.Name()}
		err := led.EachStoredObject(ctx, ta.Target, func(m ObjectMeta) error {
			if samplePerTarget > 0 && ta.Checked >= samplePerTarget {
				return errStopSample
			}
			ta.Checked++
			switch present, err := t.Sink.Exists(ctx, m.ExternalID); {
			case err != nil:
				ta.Errors++
			case !present:
				ta.Missing++
			}
			return ctx.Err()
		})
		if err != nil && !errors.Is(err, errStopSample) {
			return rep, fmt.Errorf("audit target %s: %w", ta.Target, err)
		}
		rep.Targets = append(rep.Targets, ta)
	}
	return rep, nil
}
