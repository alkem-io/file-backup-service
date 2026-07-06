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
	Worm    bool // unverifiability is EXPECTED (a read-denying PutObject-only credential)
	Checked int  // objects checked (up to the sample)
	Missing int  // ledger records stored, but Exists says absent — silent loss
	Errors  int  // Exists could not determine presence (e.g. a PutObject-only WORM credential)
}

// Unverifiable reports whether the audit gave NO real coverage for this target: every
// check errored (and at least one ran) — the definitional WORM case, where Exists always
// 403s. Missing==0 then means "couldn't look", NOT "clean", so it must not read as coverage.
func (t TargetAudit) Unverifiable() bool { return t.Checked > 0 && t.Errors == t.Checked }

// UnexpectedlyUnverifiable is an Unverifiable target that was NOT declared Worm — a
// normally-readable target whose read path broke (expired credential, moved endpoint).
// This must fail the audit; an expected-Worm Unverifiable target must not.
func (t TargetAudit) UnexpectedlyUnverifiable() bool { return t.Unverifiable() && !t.Worm }

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
//
// startAfter seeds the keyset cursor. For a SAMPLED audit the caller passes a RANDOM
// externalID so repeated runs sample a different band each time — otherwise a fixed ""
// start would re-check the same lowest-externalID prefix every run, a permanent blind
// spot for every object past the first N. When a sampled sweep reaches the end of the
// keyspace with budget remaining it WRAPS ONCE to "" (bounded by the start), so it still
// checks min(sample, total) objects — a high random start doesn't under-check and read as
// a false clean pass. A full audit (samplePerTarget<=0) passes "" and never wraps.
func Audit(ctx context.Context, led Ledger, targets []Target, samplePerTarget int, startAfter string) (AuditReport, error) {
	rep := AuditReport{Targets: make([]TargetAudit, 0, len(targets))}
	for _, t := range targets {
		ta, err := auditTarget(ctx, led, t, samplePerTarget, startAfter)
		if err != nil {
			return rep, err
		}
		rep.Targets = append(rep.Targets, ta)
	}
	return rep, nil
}

// auditTarget sweeps one target: for up to samplePerTarget objects the ledger records
// stored on it, confirm the target still holds them (Sink.Exists), keyset-paged from
// startAfter with a single wrap (see Audit's doc). A cancelled sweep returns the error.
func auditTarget(ctx context.Context, led Ledger, t Target, samplePerTarget int, startAfter string) (TargetAudit, error) {
	ta := TargetAudit{Target: t.Sink.Name(), Worm: t.Worm}
	after := startAfter
	wrapped := false
	for {
		if err := ctx.Err(); err != nil { // a cancelled audit must error, not read as a clean pass
			return ta, err
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
			return ta, fmt.Errorf("audit target %s: %w", ta.Target, err)
		}
		if len(page) == 0 {
			if wrapEnd(samplePerTarget, startAfter, &wrapped) {
				after = ""
				continue // end of keyspace with budget left → wrap once to the start
			}
			break
		}
		results := existsPage(ctx, t.Sink, page)
		// If cancellation landed mid-page, the in-flight Exists calls returned ctx.Canceled
		// — don't count those tainted errors (they'd falsely trip Unverifiable); surface it.
		if err := ctx.Err(); err != nil {
			return ta, err
		}
		ta.tally(results)
		after = page[len(page)-1].ExternalID
		if wrapped && after >= startAfter {
			break // wrapped back to the start — full circle
		}
		if len(page) < limit {
			if wrapEnd(samplePerTarget, startAfter, &wrapped) {
				after = ""
				continue
			}
			break // a short page is the last
		}
	}
	return ta, nil
}

// tally folds one page's Exists results into the target's counters.
func (t *TargetAudit) tally(results []existsResult) {
	for _, e := range results {
		t.Checked++
		switch {
		case e.err != nil:
			t.Errors++
		case !e.present:
			t.Missing++
		}
	}
}

// wrapEnd reports whether a sampled sweep that reached the end of the keyspace with budget
// remaining should WRAP ONCE to the start (it started mid-keyspace and hasn't wrapped yet);
// it flips *wrapped so the wrap happens at most once.
func wrapEnd(samplePerTarget int, startAfter string, wrapped *bool) bool {
	if samplePerTarget > 0 && !*wrapped && startAfter != "" {
		*wrapped = true
		return true
	}
	return false
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
