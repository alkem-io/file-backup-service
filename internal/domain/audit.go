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

// auditProbeTimeout bounds one per-target operation — a single Exists probe, one immutability config
// read, a manifest fetch, or a single manifest-read / ledger-page in the inventory diff — so a
// black-holing backend can't stall the integrity check (the DR ops run under a deadline-less signal
// ctx). It is a PER-OPERATION bound, never a whole-sweep one, so a large HEALTHY corpus (many fast
// operations) never false-fails. A var (not const) only so tests can lower it.
var auditProbeTimeout = 30 * time.Second

// storedPageBounded fetches ONE keyset page of the externalIDs the ledger records stored on target,
// bounded by a per-PAGE deadline (auditProbeTimeout) — the domain-layer bound for the ONE ledger read
// the DR audit sweeps drive from the domain (StoredExternalIDsPage, used by audit existence, inventory
// diff, restore-all, drill), so the ledger page shares the sweep's per-operation timeout (the same
// auditProbeTimeout that bounds a sink Exists/manifest read). EVERY OTHER adapter DB read self-bounds
// at the DB ADAPTER with db.boundRead (StoredObjectsPage, StoredCountByTarget, TargetGapsPage,
// filesPage, FileByID, the Probes, …). Between the two, every client-side DB read is bounded and none
// can drift into an UNBOUNDED raw-ctx read a black-holed connection would hang (the pool's server-side
// statement_timeout can't fire when no bytes ever return). It is a per-page bound, never a whole-sweep
// one, so a large HEALTHY corpus (many fast pages) never false-fails. Callers wrap the error with their
// own tag (errLedgerRead for audit/inventory, a command-specific message for drill/restore-all).
func storedPageBounded(ctx context.Context, led Ledger, target, after string, limit int) ([]string, error) {
	pctx, cancel := context.WithTimeout(ctx, auditProbeTimeout)
	defer cancel()
	return led.StoredExternalIDsPage(pctx, target, after, limit)
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

// sampledStart returns the keyset start for a sweep of `sample` ids: a rotating randKeysetStart() for a
// SAMPLED sweep (sample>0), so successive runs cover a different band; "" for a FULL sweep (sample<=0),
// which starts at the beginning and never wraps. The ONE owner of the sample→start pairing, shared by
// Audit and the drill sampler so they can't drift on the band policy (keysetSample owns the wrap logic).
func sampledStart(sample int) string {
	if sample > 0 {
		return randKeysetStart()
	}
	return ""
}

// Audit verifies the ledger against reality (FR-014 drift check / T030), returning one TargetVerdict
// per target: for up to samplePerTarget objects the ledger records stored on each target, it checks
// the target ACTUALLY still holds them (Sink.Exists). A "missing" object is one the ledger believes
// is backed up but the target lost — the silent-loss case reconcile (which trusts the ledger) can't
// detect. samplePerTarget<=0 checks every stored object.
//
// Verdict per target:
//   - a ledger-stored object absent on the sink (Missing>0) → Drift (silent loss);
//   - probes that could not determine presence (Errors>0, Missing==0) → Unverifiable (a WORM
//     target's read-denied probes are expected; a non-worm target's broken/throttled read path
//     FAILS — FR-014 fail-closed: an integrity check that couldn't verify part of its sample is not
//     a clean pass);
//   - nothing recorded stored / a benign shutdown → NoData; a ledger read error → Fault;
//   - all checked and present → Verified.
//
// For a SAMPLED audit (samplePerTarget>0) Audit derives a RANDOM keyset start so repeated runs
// sample a different band each time (a fixed "" start would re-check the same lowest-externalID
// prefix every run, a permanent blind spot). A sampled sweep that reaches the end of the keyspace
// with budget remaining WRAPS ONCE to "" so it still checks min(sample, total) objects. A full audit
// (samplePerTarget<=0) starts at "" and never wraps.
func Audit(ctx context.Context, led Ledger, targets []Target, samplePerTarget int) VerdictReport {
	return auditWithStart(ctx, led, targets, samplePerTarget, sampledStart(samplePerTarget))
}

// auditWithStart is the deterministic core: it sweeps from an EXPLICIT startAfter (Audit derives a
// random one for a sampled run; tests inject a fixed one to exercise the wrap / boundary cases).
// Kept unexported so the sample<->random-start pairing stays Audit's invariant. The per-target
// concurrency + panic-recover + wedged-vs-shutdown classification is owned by probeTargets; each
// target supplies its own keyset sweep, with perTargetTimeout=0 so a full audit of a large corpus is
// bounded per-Exists-probe (existsPage), never by a single whole-sweep deadline.
func auditWithStart(ctx context.Context, led Ledger, targets []Target, samplePerTarget int, startAfter string) VerdictReport {
	return VerdictReport{Targets: probeTargets(ctx, targets, 0, func(pctx context.Context, t Target) TargetVerdict {
		return auditTarget(pctx, led, t, samplePerTarget, startAfter)
	})}
}

// auditTarget sweeps one target: for up to samplePerTarget objects the ledger records stored on it,
// confirm the target still holds them (Sink.Exists), keyset-paged from startAfter with a single
// wrap. It tallies checked/missing/errored probes and classifies the result into a verdict.
func auditTarget(ctx context.Context, led Ledger, t Target, samplePerTarget int, startAfter string) TargetVerdict {
	// This direction ALWAYS probes via Sink.Exists (readClient) — it does NOT short-circuit a WORM target
	// on the a-priori "has a separate audit credential" predicate. That predicate mis-classifies a
	// READ-CAPABLE worker credential on a WORM (object-lock) bucket — object-lock restricts delete/
	// overwrite, NOT GET — so such a target IS existence-verifiable and MUST be probed for silent loss;
	// short-circuiting it to NoData silently disabled its DR verification. So: probe every target. A
	// genuinely write-only WORM copy (PutObject-only creds) instead read-denies on each StatObject →
	// errored++ → Unverifiable, which targetUnverifiableExempt (worm && no audit cred) then makes benign.
	// The cost is a doomed HEAD per object on a full audit of a strictly-write-only target — the honest
	// price of not silently skipping a verifiable one (correctness over the doomed-read optimization).
	var checked, missing, errored int
	var panicErr error
	name := t.Sink.Name()
	err := keysetSample(ctx, samplePerTarget, startAfter,
		func(after string, limit int) ([]string, error) {
			page, perr := storedPageBounded(ctx, led, name, after, limit)
			if perr != nil {
				return nil, fmt.Errorf("%w: audit target %s: %w", errLedgerRead, name, perr)
			}
			return page, nil
		},
		func(page []string) error {
			results := existsPage(ctx, t.Sink, page)
			// If cancellation landed mid-page the in-flight Exists calls returned ctx.Canceled —
			// don't count those tainted errors (they'd falsely inflate Errors); surface it.
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			for _, e := range results {
				checked++
				switch {
				case e.err != nil:
					errored++
					if errors.Is(e.err, ErrProbePanic) { // a driver panic is a code bug, not a benign read error
						panicErr = e.err
					}
				case !e.present:
					missing++
				}
			}
			return nil
		})
	if err != nil {
		return classifyAuditErr(ctx, name, err)
	}
	if panicErr != nil { // a recovered probe panic → Fault (fail-loud), not swallowed to Unverifiable
		return TargetVerdict{Status: StatusFault, Checked: checked, Err: fmt.Errorf("audit probe %s: %w", name, panicErr), Detail: "probe panicked"}
	}
	detail := fmt.Sprintf("checked=%d missing=%d errors=%d", checked, missing, errored)
	switch {
	case missing > 0: // ledger-stored but absent on the sink — silent loss
		return TargetVerdict{Status: StatusDrift, Checked: checked, Missing: missing, Detail: detail}
	case errored > 0: // couldn't determine presence for part/all of the sample — a WORM read-deny, or a broken read path
		return TargetVerdict{Status: StatusUnverifiable, Checked: checked, Detail: detail}
	default: // every probed object present (or nothing recorded stored) — clean
		return TargetVerdict{Status: StatusVerified, Checked: checked, Detail: detail}
	}
}

// classifyAuditErr folds a ledger→target sweep error into a verdict: a PARENT cancel (SIGTERM) →
// NoData (benign; the top-level ctx.Err() fold surfaces the abort); a ledger read error → Fault —
// this INCLUDES a per-page auditProbeTimeout DeadlineExceeded, which auditTarget wraps as
// errLedgerRead, because a stalled/unreadable OWN ledger is our-side infra: page loudly, never
// worm-exempt. The `default` is a defensive catch-all for a sweep error that is NOT a ledger read (a
// wedged existence probe surfacing ctx.Err() other than a parent cancel) → Unverifiable.
func classifyAuditErr(parentCtx context.Context, name string, err error) TargetVerdict {
	switch {
	case cancelledInFlight(parentCtx, err):
		return shutdownVerdict(err)
	case errors.Is(err, errLedgerRead):
		return TargetVerdict{Status: StatusFault, Err: fmt.Errorf("audit target %s: %w", name, err), Detail: "ledger read error"}
	default:
		return TargetVerdict{Status: StatusUnverifiable, Detail: fmt.Sprintf("sweep unverifiable (wedged/deadline): %v", err)}
	}
}

// keysetSample drives a random-band, single-wrap keyset sweep of up to `sample` ids (sample<=0 =
// every id from startAfter, no wrap), calling emit once per non-empty page and counting len(page)
// against the sample budget. It is the ONE owner of the sample→wrap-once logic, shared by the audit
// sweep (emit probes each id on the sink) and the drill sampler (emit collects the ids), so a
// hand-rolled copy can't drift on the boundary trim or under-check on a high random start. A pageFn
// error or an emit error stops the sweep and propagates. ctx is checked at the top of every page so
// a cancelled sweep errors (never reads as a clean pass).
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

// existsWithCtx runs Sink.Exists honoring ctx even when the sink CANNOT — a filesystem os.Stat on a
// wedged mount ignores ctx and blocks uninterruptibly, exactly like the write path's os.Open/fsync.
// It runs the probe in its own goroutine and, on ctx cancellation (the per-probe auditProbeTimeout),
// returns a ctx-error result and ABANDONS the goroutine (bounded, one per wedged probe), so the
// per-probe timeout is actually ENFORCED for os.Stat and the audit can't hang. A panic in the driver
// becomes an error result. Symmetric to storeWithCtx on the write side.
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
// auditConcurrency), so a page of independent HEAD RTTs collapses to ~page/concurrency wall-clock.
// Each probe goes through existsWithCtx under a per-probe auditProbeTimeout, so a per-probe timeout
// is enforced even against a filesystem os.Stat that ignores ctx (a hung mount) — the collect loop
// always completes and a scheduled audit self-bounds. The collect loop also escapes on ctx.Done so a
// SIGINT returns promptly.
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
