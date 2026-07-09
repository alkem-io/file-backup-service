package domain

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"
)

// auditConcurrency bounds the parallel Exists probes per page (each is an independent
// network RTT — e.g. an S3 StatObject HEAD — so a serial sweep is RTT-bound).
const auditConcurrency = 16

// auditProbeTimeout bounds one probe (an Exists probe, or a per-target inventory fetch/read) so a
// black-holing backend can't stall the whole integrity check (audit runs under signalContext, which
// has no deadline of its own). A var (not const) only so tests can lower it to exercise the
// per-target-timeout path without a 30s wait.
var auditProbeTimeout = 30 * time.Second

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

// FailErr is the audit pass/fail VERDICT — the rule lives WITH the report, not re-derived by
// each caller. Non-nil (a nonzero exit for cron/CI) when a ledger-stored object is MISSING
// from its target (silent loss) OR a normally-readable target couldn't be verified at all
// (a broken read path); an expected-WORM read-denying target is fine. nil = pass.
func (r AuditReport) FailErr() error {
	if m := r.Missing(); m > 0 {
		return fmt.Errorf("%d ledger-stored objects are missing from their target", m)
	}
	var unverified []string
	for _, t := range r.Targets {
		// A non-WORM target with ANY errored probe could not fully verify its sample — a
		// broken/throttled read path. This catches the PARTIAL case (0 < Errors < Checked, e.g.
		// intermittent 503s) too, not just the all-errored UnexpectedlyUnverifiable case: half
		// the sample silently unverified must NOT read as a clean pass (FR-014). A WORM target's
		// errors are expected (read-denying by design) and never fail the audit.
		//
		// FAIL-CLOSED by design: an integrity check that couldn't verify part of its sample is
		// not a clean pass — an errored probe is "presence UNKNOWN", which for a loss detector
		// is a signal, not a non-event. The cost is that a single transient backend error fails
		// the run (re-run clears it); that recall-over-precision trade is deliberate for a
		// data-loss check. errors=N is printed per target so the operator sees the magnitude.
		if t.Errors > 0 && !t.Worm {
			unverified = append(unverified, t.Target)
		}
	}
	if len(unverified) > 0 {
		return fmt.Errorf("targets with unverifiable objects (read path broken/throttled, not worm): %v", unverified)
	}
	return nil
}

// randKeysetStart returns a random externalID-shaped hex string — a rotating keyset start so a
// SAMPLED audit checks a different band each run instead of the same fixed lowest-prefix band
// (a permanent blind spot). A failed rand falls back to "" (the beginning) so a sampled audit
// never blocks on entropy.
func randKeysetStart() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
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
// For a SAMPLED audit (samplePerTarget>0) Audit derives a RANDOM keyset start so repeated
// runs sample a different band each time — otherwise a fixed "" start would re-check the same
// lowest-externalID prefix every run, a permanent blind spot for every object past the first
// N. Deriving the start HERE keeps the sample<->random-start pairing an INVARIANT of Audit,
// not a contract each caller must remember. When a sampled sweep reaches the end of the
// keyspace with budget remaining it WRAPS ONCE to "" (bounded by the start), so it still
// checks min(sample, total) objects — a high random start doesn't under-check and read as a
// false clean pass. A full audit (samplePerTarget<=0) starts at "" and never wraps.
func Audit(ctx context.Context, led Ledger, targets []Target, samplePerTarget int) (AuditReport, error) {
	startAfter := ""
	if samplePerTarget > 0 {
		startAfter = randKeysetStart()
	}
	return auditWithStart(ctx, led, targets, samplePerTarget, startAfter)
}

// auditWithStart is the deterministic core: it sweeps from an EXPLICIT startAfter (Audit
// derives a random one for a sampled run; tests inject a fixed one to exercise the wrap /
// boundary cases). Kept unexported so the sample<->random-start pairing stays Audit's invariant.
func auditWithStart(ctx context.Context, led Ledger, targets []Target, samplePerTarget int, startAfter string) (AuditReport, error) {
	// Sweep targets CONCURRENTLY — each is an independent backend + keyset with its own
	// TargetAudit, so wall-clock is the slowest target, not the sum. Results are written to
	// distinct indices (config order preserved); a cancelled sweep on any target surfaces
	// its error via errors.Join.
	rep := AuditReport{Targets: make([]TargetAudit, len(targets))}
	// RunParallel (not a bare WaitGroup) so a panic in one target's auditTarget — e.g. a pgx
	// scan on a drifted ledger column — becomes that target's error instead of crashing the
	// audit process; every other concurrent sweep here is recover-guarded, this must be too.
	errs := RunParallelIdx(len(targets),
		func(i int) string { return "audit " + targets[i].Sink.Name() },
		func(i int) error {
			var err error
			rep.Targets[i], err = auditTarget(ctx, led, targets[i], samplePerTarget, startAfter)
			return err
		})
	return rep, errors.Join(errs...)
}

// auditTarget sweeps one target: for up to samplePerTarget objects the ledger records
// stored on it, confirm the target still holds them (Sink.Exists), keyset-paged from
// startAfter with a single wrap (see Audit's doc). A cancelled sweep returns the error.
func auditTarget(ctx context.Context, led Ledger, t Target, samplePerTarget int, startAfter string) (TargetAudit, error) {
	ta := TargetAudit{Target: t.Sink.Name(), Worm: t.Worm}
	err := keysetSample(ctx, samplePerTarget, startAfter,
		func(after string, limit int) ([]string, error) {
			page, err := led.StoredExternalIDsPage(ctx, ta.Target, after, limit)
			if err != nil {
				return nil, fmt.Errorf("audit target %s: %w", ta.Target, err)
			}
			return page, nil
		},
		func(page []string) error {
			results := existsPage(ctx, t.Sink, page)
			// If cancellation landed mid-page, the in-flight Exists calls returned ctx.Canceled
			// — don't count those tainted errors (they'd falsely trip Unverifiable); surface it.
			if err := ctx.Err(); err != nil {
				return err
			}
			ta.tally(results)
			return nil
		})
	return ta, err
}

// keysetSample drives a random-band, single-wrap keyset sweep of up to `sample` ids (sample<=0 =
// every id from startAfter, no wrap), calling emit once per non-empty page and counting len(page)
// against the sample budget. It is the ONE owner of the sample→wrap-once logic, shared by the
// audit sweep (emit probes each id on the sink) and the drill sampler (emit collects the ids), so
// a hand-rolled copy can't drift on the boundary trim or under-check on a high random start. The
// start is a PARAMETER (Audit/drill pass randKeysetStart(); tests inject a fixed one) so the
// deterministic wrap/boundary behaviour stays exercisable. A pageFn error or an emit error stops
// the sweep and propagates. ctx is checked at the top of every page so a cancelled sweep errors
// (never reads as a clean pass).
func keysetSample(ctx context.Context, sample int, startAfter string,
	pageFn func(after string, limit int) ([]string, error), emit func(page []string) error) error {
	after := startAfter
	wrapped := false
	consumed := 0
	for {
		if err := ctx.Err(); err != nil { // a cancelled sweep must error, not read as a clean pass
			return err
		}
		limit := KeysetPageSize
		if sample > 0 { // push the sample bound into SQL — don't scan+fetch more than needed
			remaining := sample - consumed
			if remaining <= 0 {
				return nil
			}
			limit = min(limit, remaining)
		}
		page, err := pageFn(after, limit)
		if err != nil {
			return err
		}
		// A short page (incl. empty) is the end of this segment. On the WRAPPED pass, also stop at
		// startAfter — trim ids >= it (already covered in pass 1) BEFORE emitting, so the boundary
		// page isn't double-counted.
		segmentEnd := len(page) < limit
		if wrapped {
			var atBoundary bool
			if page, atBoundary = trimAtBoundary(page, startAfter); atBoundary {
				segmentEnd = true
			}
		}
		if len(page) > 0 {
			if err := emit(page); err != nil {
				return err
			}
			consumed += len(page)
			after = page[len(page)-1]
		}
		if segmentEnd {
			if wrapEnd(sample, startAfter, &wrapped) {
				after = "" // end of [startAfter,end) with budget left → wrap once to the start
				continue
			}
			return nil
		}
	}
}

// trimAtBoundary returns page truncated at the first externalID >= boundary (the wrapped pass
// must not re-check objects already covered in pass 1), and whether it trimmed (reached the
// boundary).
func trimAtBoundary(page []string, boundary string) ([]string, bool) {
	if i := slices.IndexFunc(page, func(id string) bool { return id >= boundary }); i >= 0 {
		return page[:i], true
	}
	return page, false
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

// existsWithCtx runs Sink.Exists honoring ctx even when the sink CANNOT — a filesystem
// os.Stat on a wedged mount ignores ctx and blocks uninterruptibly, exactly like the write
// path's os.Open/fsync. It runs the probe in its own goroutine and, on ctx cancellation
// (the per-probe auditProbeTimeout), returns a ctx-error result and ABANDONS the goroutine
// (bounded, one per wedged probe), so the per-probe timeout is actually ENFORCED for os.Stat
// and the audit can't hang. A panic in the driver becomes an error result. Symmetric to
// storeWithCtx on the write side.
func existsWithCtx(ctx context.Context, sink Sink, hash string) existsResult {
	return RunAbandonable(ctx,
		func() existsResult {
			present, err := sink.Exists(ctx, hash)
			return existsResult{present: present, err: err}
		},
		func() existsResult { return existsResult{err: ctx.Err()} },
		func(r any) existsResult { return existsResult{err: PanicErr("sink Exists "+sink.Name(), r)} })
}

// existsPage probes Sink.Exists for every object in the page concurrently (bounded by
// auditConcurrency), so a page of independent HEAD RTTs collapses to ~page/concurrency
// wall-clock instead of a serial sum. Each probe goes through existsWithCtx, so a
// per-probe timeout is enforced even against a filesystem os.Stat that ignores ctx (a hung
// mount) — the probe returns a timeout-error result within auditProbeTimeout rather than
// blocking forever, so the collect loop always completes and a scheduled audit self-bounds
// (it does NOT depend on the deadline-less audit ctx being cancelled). The collect loop also
// still escapes on ctx.Done so a SIGINT returns promptly.
func existsPage(ctx context.Context, sink Sink, page []string) []existsResult {
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
			// Bound each probe with auditProbeTimeout, enforced via existsWithCtx even when the
			// sink op ignores ctx (a filesystem os.Stat on a wedged mount): the probe returns a
			// timeout-error result at the deadline and the stuck os.Stat goroutine is abandoned
			// (its buffered ch send never blocks). A black-holing S3 StatObject is bounded the
			// same way. existsWithCtx recovers a driver panic into an error result too.
			pctx, cancel := context.WithTimeout(ctx, auditProbeTimeout)
			defer cancel()
			ch <- done{i, existsWithCtx(pctx, sink, page[i])}
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
