package domain

import (
	"context"
	"errors"
	"fmt"
)

// errStopSample stops EachStoredObject early once a target's audit sample is reached.
var errStopSample = errors.New("sample complete")

// AuditReport summarizes one audit pass.
type AuditReport struct {
	Checked int // (object,target) pairs checked
	Missing int // the ledger records it stored, but the target's Exists says absent — silent loss
	Errors  int // Exists could not determine presence (e.g. a PutObject-only WORM credential)
}

// Audit verifies the ledger against reality (FR-014 drift check / T030): for up to
// samplePerTarget objects the ledger records as stored on each target, it checks the
// target ACTUALLY still holds them (Sink.Exists). A "missing" object is one the ledger
// believes is backed up but the target lost — the silent-loss case reconcile (which
// trusts the ledger) can't detect. samplePerTarget<=0 checks every stored object.
func Audit(ctx context.Context, led Ledger, targets []Target, samplePerTarget int) (AuditReport, error) {
	var rep AuditReport
	for _, t := range targets {
		n := 0
		err := led.EachStoredObject(ctx, t.Sink.Name(), func(m ObjectMeta) error {
			if samplePerTarget > 0 && n >= samplePerTarget {
				return errStopSample
			}
			n++
			rep.Checked++
			switch present, err := t.Sink.Exists(ctx, m.ExternalID); {
			case err != nil:
				rep.Errors++ // e.g. a PutObject-only WORM credential can't introspect
			case !present:
				rep.Missing++ // ledger=stored but the target doesn't have it — drift/loss
			}
			return ctx.Err()
		})
		if err != nil && !errors.Is(err, errStopSample) {
			return rep, fmt.Errorf("audit target %s: %w", t.Sink.Name(), err)
		}
	}
	return rep, nil
}
