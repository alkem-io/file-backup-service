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

// errAuditReadDenied is auditTarget's INTERNAL early-stop signal (never surfaced): a by-design
// write-only WORM copy whose whole probed set uniformly read-denied (ErrReadDenied) — the sweep stops
// and classifies the tally normally (→ Unverifiable, exempt→benign). Not a fault.
var errAuditReadDenied = errors.New("audit: target uniformly read-denied (write-only WORM early-stop)")

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
//
// Why auditProbeTimeout and NOT the operator's cfg.DBTimeout() (which db.boundRead uses): a DR sweep
// bounds EVERY per-operation step at ONE uniform budget — a sink Exists/manifest read AND this ledger
// page — so the sweep can't stall on any single op. A single index-only keyset page (KeysetPageSize
// rows on the COLLATE "C" covering index) completing in <30s is a healthy ledger; a page exceeding it
// is a sick one, which a DR integrity check SHOULD fault on rather than wait out. So this is
// deliberately the sweep's per-operation bound, not the operator's whole-DB-op budget — raising
// DBTimeout for a slow-but-alive ledger does not (and should not) loosen the audit's per-page bound.
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
//
// CANCELLATION CONTRACT (shared by CheckImmutability + AuditInventory): a parent-ctx cancellation
// (SIGTERM) yields a benign NoData verdict for every target NOT YET completed (a target that finished a
// real Drift/Verified before the cancel keeps it), NOT an error — an aborted sweep proved nothing about
// the remaining targets, but their verdicts must not be spurious FAILURES. So VerdictReport.FailErr()
// alone CANNOT tell a cancelled (INCOMPLETE) audit from a clean pass — the incomplete targets read
// benign. The caller MUST fold ctx.Err() into its exit verdict to fail an interrupted run — see
// runAudit's `verdicts = append(verdicts, ctx.Err())`. A future caller reducing via FailErr() alone
// would read a cancelled, unverified audit as green.
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
	// A doomed HEAD per object on a full audit of a strictly-write-only target is avoided by the
	// read-deny early-stop (auditTally.allReadDenied — not by skipping the target a-priori, the old bug).
	var tally auditTally
	name := t.Sink.Name()
	exempt := targetUnverifiableExempt(t)
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
			tally.add(results)
			// Early-stop the doomed full sweep of a by-design write-only WORM copy: once EVERY probe so
			// far uniformly read-denied, the credential demonstrably can't read ANY object, so the
			// remaining objects are guaranteed to 403 to the SAME Unverifiable-exempt (benign) verdict.
			// allReadDenied turns permanently false the instant any probe returns present / a clean
			// 404-missing / a transient (non-403) error / a panic, so a READ-CAPABLE WORM (the silent-loss
			// case the round-6 fix restored) is NEVER short-circuited — it full-sweeps and still catches
			// missing objects. Gated on `exempt` so a non-worm target (whose Unverifiable FAILS) is fully swept.
			if exempt && tally.allReadDenied() {
				return errAuditReadDenied
			}
			return nil
		})
	stoppedEarly := errors.Is(err, errAuditReadDenied)
	if err != nil && !stoppedEarly {
		return classifyAuditErr(ctx, name, err)
	}
	return tally.verdict(name, stoppedEarly)
}

// auditTally accumulates one target's existence-probe outcomes across the swept pages, then classifies
// them into a verdict — extracted from auditTarget so the loop + verdict switch don't inflate its
// cyclomatic complexity, and so the early-stop signal (allReadDenied) has one clear owner.
type auditTally struct {
	checked, missing, errored, readDenied int
	panicErr                              error
}

func (a *auditTally) add(results []existsResult) {
	for _, e := range results {
		a.checked++
		switch {
		case e.err != nil:
			a.errored++
			if errors.Is(e.err, ErrReadDenied) { // a definitive 403 (write-only credential)
				a.readDenied++
			}
			if errors.Is(e.err, ErrProbePanic) { // a driver panic is a code bug, not a benign read error
				a.panicErr = e.err
			}
		case !e.present:
			a.missing++
		}
	}
}

// allReadDenied reports whether EVERY probe so far uniformly read-denied (a definitively write-only
// credential) — the write-only-WORM early-stop signal.
func (a *auditTally) allReadDenied() bool { return a.checked > 0 && a.readDenied == a.checked }

// verdict classifies the accumulated tally: a recovered panic → Fault (fail-loud on a code bug); a
// missing object → Drift (silent loss); any other errored probe → Unverifiable (a WORM read-deny or a
// broken read path); else Verified. stoppedEarly annotates the detail (a partial sweep of a write-only WORM).
func (a *auditTally) verdict(name string, stoppedEarly bool) TargetVerdict {
	if a.panicErr != nil {
		return TargetVerdict{Status: StatusFault, Checked: a.checked, Err: fmt.Errorf("audit probe %s: %w", name, a.panicErr), Detail: "probe panicked"}
	}
	detail := fmt.Sprintf("checked=%d missing=%d errors=%d", a.checked, a.missing, a.errored)
	if stoppedEarly {
		detail += " (write-only WORM: stopped after a uniform read-deny page)"
	}
	switch {
	case a.missing > 0: // ledger-stored but absent on the sink — silent loss
		return TargetVerdict{Status: StatusDrift, Checked: a.checked, Missing: a.missing, Detail: detail}
	case a.errored > 0: // couldn't determine presence for part/all of the sample — a WORM read-deny, or a broken read path
		return TargetVerdict{Status: StatusUnverifiable, Checked: a.checked, Detail: detail}
	default: // every probed object present (or nothing recorded stored) — clean
		return TargetVerdict{Status: StatusVerified, Checked: a.checked, Detail: detail}
	}
}

// classifyAuditErr folds a ledger→target sweep error into a verdict: a PARENT cancel (SIGTERM) →
// NoData (benign; the top-level ctx.Err() fold surfaces the abort); ANY other error → Fault — this
// INCLUDES a per-page auditProbeTimeout DeadlineExceeded, which auditTarget wraps as errLedgerRead,
// because a stalled/unreadable OWN ledger is our-side infra: page loudly, never worm-exempt. The
// existence direction runs with NO per-target deadline (probeTargets timeout=0), so err here can ONLY be
// a parent Canceled or an errLedgerRead-wrapped page error — the final Fault is a fail-LOUD catch-all for
// any error class a future change could introduce: an unexpected classification here must PAGE, never
// silently become an exempt-benign Unverifiable on a WORM target (the fail-open direction).
func classifyAuditErr(parentCtx context.Context, name string, err error) TargetVerdict {
	if cancelledInFlight(parentCtx, err) {
		return shutdownVerdict(err)
	}
	// errLedgerRead (the only other reachable class today) AND any unexpected future class → Fault.
	return TargetVerdict{Status: StatusFault, Err: fmt.Errorf("audit target %s: %w", name, err), Detail: "ledger/sweep read error"}
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
