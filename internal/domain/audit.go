package domain

import (
	"context"
	"fmt"
)

// auditConcurrency bounds the parallel Exists probes per page (each is an independent
// network RTT — e.g. an S3 StatObject HEAD — so a serial sweep is RTT-bound).
const auditConcurrency = 16

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
		after := ""
		for {
			// A cancelled audit (SIGINT / a cron timeout) MUST surface the error, not
			// return a partial report as a clean pass — an incomplete integrity check that
			// exits 0 is read as "verified" by a monitoring cron.
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			limit := storedPageSize
			if samplePerTarget > 0 { // push the sample bound into SQL — don't scan+fetch more than needed
				remaining := samplePerTarget - ta.Checked
				if remaining <= 0 {
					break
				}
				if remaining < limit {
					limit = remaining
				}
			}
			page, err := led.StoredObjectsPage(ctx, ta.Target, after, limit)
			if err != nil {
				return rep, fmt.Errorf("audit target %s: %w", ta.Target, err)
			}
			if len(page) == 0 {
				break
			}
			results := existsPage(ctx, t.Sink, page)
			// If cancellation landed mid-page, the in-flight Exists calls returned
			// ctx.Canceled — don't count those tainted errors (they'd falsely trip
			// Unverifiable); surface the cancellation instead.
			if err := ctx.Err(); err != nil {
				return rep, err
			}
			for _, e := range results {
				ta.Checked++
				switch {
				case e.err != nil:
					ta.Errors++
				case !e.present:
					ta.Missing++
				}
			}
			after = page[len(page)-1].ExternalID
			if len(page) < limit {
				break // a short page is the last
			}
		}
		rep.Targets = append(rep.Targets, ta)
	}
	return rep, nil
}

type existsResult struct {
	present bool
	err     error
}

// existsPage probes Sink.Exists for every object in the page concurrently (bounded by
// auditConcurrency), so a page of independent HEAD RTTs collapses to ~page/concurrency
// wall-clock instead of a serial sum. Both the concurrency-acquire AND the result
// collection observe ctx, so a cancelled audit RETURNS promptly even when in-flight
// probes are wedged on an uninterruptible os.Stat (a hung filesystem mount) — the stuck
// goroutines are abandoned (their buffered send never blocks), not waited on, so the
// audit can't hang. The caller re-checks ctx.Err() and surfaces the cancellation.
func existsPage(ctx context.Context, sink Sink, page []ObjectMeta) []existsResult {
	results := make([]existsResult, len(page))
	for i := range results {
		results[i].err = context.Canceled // default: unchecked (a cancelled/abandoned probe)
	}
	type done struct {
		i int
		r existsResult
	}
	ch := make(chan done, len(page)) // buffered: an abandoned probe's send never blocks
	sem := make(chan struct{}, auditConcurrency)
	dispatched := 0
dispatch:
	for i := range page {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break dispatch // stop dispatching; don't block the acquire when probes are wedged
		}
		dispatched++
		go func(i int) {
			defer func() { <-sem }()
			present, err := sink.Exists(ctx, page[i].ExternalID)
			ch <- done{i, existsResult{present: present, err: err}}
		}(i)
	}
	for n := 0; n < dispatched; n++ {
		select {
		case d := <-ch:
			results[d.i] = d.r
		case <-ctx.Done():
			return results // abandon wedged probes; caller sees ctx.Err() and returns
		}
	}
	return results
}
