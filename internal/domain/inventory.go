package domain

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)

// errCorruptManifest marks a manifest that WAS fetched but has a STRUCTURAL content fault — a
// malformed JSONL line or a bufio.ErrTooLong line — a real fault (the target's DR inventory is
// broken). It deliberately does NOT cover a non-ascending order (that is errNonAscendingManifest — an
// old-format / locale manifest, handled by an order-independent fallback), a transient mid-stream read
// reset (Unverifiable, retry), or a ctx cancellation (propagated), so a blip/SIGTERM/old-format
// manifest can't fire a false data-corruption fault.
var errCorruptManifest = errors.New("corrupt manifest")

// errLedgerRead marks an error from the ledger side of the merge, so a genuine DB fault (Fault) is
// classified apart from a transient MANIFEST read (Unverifiable) — both surface through
// mergeInventory as one error otherwise.
var errLedgerRead = errors.New("ledger read")

// errNonAscendingManifest marks a manifest whose keys are NOT strictly byte-ascending. This is NOT a
// data-corruption fault: a manifest written before the COLLATE "C" migration (or on a locale-collated
// DB) can legitimately be non-ascending in byte order, so it must not hard-fail the audit. The
// streaming merge assumes byte order, so on this signal the probe falls back to an order-INDEPENDENT
// (sorted) diff. Kept distinct from errCorruptManifest (which IS a fault: malformed JSON / too-long).
var errNonAscendingManifest = errors.New("manifest not byte-ascending (old-format / locale)")

// maxSortedManifestIDs bounds the order-independent fallback's in-memory manifest buffer so a huge
// non-ascending manifest can't OOM the pod: past it the target is reported Unverifiable rather than
// buffered. A var (not const) so tests can lower it.
var maxSortedManifestIDs = 5_000_000

// AuditInventory runs the target→ledger direction for every target, returning one TargetVerdict
// each: it stream-merges each target's most recent manifest (its self-declared inventory) against
// the ledger's current stored set for that target — O(page) memory, no full-corpus maps.
//   - a manifest object the ledger no longer records stored on this target (Extra>0) → Drift (an
//     orphan, or a lost ledger record the manifest could rebuild);
//   - ledger-stored objects the manifest omits (Missing>0, Extra==0) → informational (usually stored
//     after the last snapshot), NOT a failure → Verified;
//   - no manifest yet / no capability / a benign shutdown → NoData;
//   - a manifest that can't be read (a WORM read-deny, a wedged/black-holing target, a transient
//     reset) → Unverifiable (benign for a worm target, a broken read path for a non-worm one);
//   - a corrupt manifest → Corrupt; a ledger read error / driver panic → Fault.
//
// The per-target concurrency + panic-recover is owned by probeTargets; per-target progress is
// bounded per-OPERATION (the manifest fetch, each manifest read, each ledger page) rather than by a
// single whole-diff deadline, so a healthy LARGE corpus never false-fails while a wedged target is
// still bounded.
func AuditInventory(ctx context.Context, led Ledger, targets []Target) VerdictReport {
	return VerdictReport{Targets: probeTargets(ctx, targets, 0, func(pctx context.Context, t Target) TargetVerdict {
		return inventoryProbe(pctx, led, t)
	})}
}

// inventoryProbe is one target's target→ledger closure. It runs on the parent probe ctx (probeTargets
// imposes no whole-probe deadline for this direction) and bounds each blocking sub-operation itself.
func inventoryProbe(ctx context.Context, led Ledger, t Target) TargetVerdict {
	ir, ok := t.Sink.(inventoryReader)
	if !ok {
		return TargetVerdict{Status: StatusNoData, Detail: "target type cannot enumerate its manifest"}
	}
	// A by-design write-only WORM copy (no audit/read credential) can't read its OWN manifest either,
	// so the inventory diff is legitimately N/A → NoData — matching the immutability direction (the same
	// targetUnverifiableExempt predicate) instead of burning a doomed 403 read to reach Unverifiable.
	if targetUnverifiableExempt(t) {
		return TargetVerdict{Status: StatusNoData, Detail: "no audit/read credential — cannot enumerate manifest"}
	}
	name := t.Sink.Name()
	rc, err := abandonableFetch(ctx, auditProbeTimeout, func() (io.ReadCloser, error) { return ir.LatestManifest(ctx) })
	if err != nil {
		return classifyInventoryErr(ctx, name, err)
	}
	// The stallReader OWNS closing rc: on an abandoned (wedged) read it closes rc only AFTER the read
	// goroutine finishes, so a deferred Close here can't race an in-flight Read (an fd race).
	sr := &stallReader{ctx: ctx, rc: rc, timeout: auditProbeTimeout}
	defer func() { _ = sr.Close() }()

	extra, missing, msize, merr := mergeInventory(manifestIterator(sr), ledgerStoredPull(ctx, led, name))
	// A non-monotonic (old-format / locale) manifest with NO definitive orphan yet is not a fault: retry
	// with an order-independent diff so a legitimately-written older manifest doesn't hard-fail. (An
	// orphan already seen — extra>0 — is a REAL drift mergeToVerdict preserves, so don't discard it.)
	if merr != nil && extra == 0 && errors.Is(merr, errNonAscendingManifest) {
		return inventorySortedDiff(ctx, ir, led, name)
	}
	return mergeToVerdict(ctx, name, extra, missing, msize, merr)
}

// mergeToVerdict maps a completed-or-faulted inventory merge to a verdict — the ONE owner of the
// counts→verdict + fault rules, shared by the streaming and the order-independent (sorted) paths so
// they can't diverge. A definitive orphan (Extra>0) observed BEFORE a transient read fault is a REAL
// drift (report it, don't relabel the target Unverifiable — mirrors the immutability 404-drift
// preservation); any other merge fault classifies via classifyInventoryErr; a clean merge → the
// counts verdict.
func mergeToVerdict(ctx context.Context, name string, extra, missing, msize int, merr error) TargetVerdict {
	if merr == nil {
		return inventoryVerdict(extra, missing, msize)
	}
	if extra > 0 {
		return TargetVerdict{Status: StatusDrift, Extra: extra, Missing: missing, Detail: fmt.Sprintf("extra=%d (orphan) seen before read fault: %v", extra, merr)}
	}
	return classifyInventoryErr(ctx, name, merr)
}

// inventoryVerdict maps a completed diff's counts to a verdict: an orphan (Extra>0) is Drift; a
// clean or merely snapshot-stale (Missing-only) diff is Verified — shared by the streaming and the
// order-independent (sorted) paths so they can't diverge on the counts→verdict rule.
func inventoryVerdict(extra, missing, manifestSize int) TargetVerdict {
	detail := fmt.Sprintf("manifest=%d extra=%d missing=%d", manifestSize, extra, missing)
	if extra > 0 { // an orphan / lost ledger record — the genuine target→ledger drift
		return TargetVerdict{Status: StatusDrift, Extra: extra, Missing: missing, Detail: detail}
	}
	return TargetVerdict{Status: StatusVerified, Extra: extra, Missing: missing, Detail: detail}
}

// inventorySortedDiff is the order-INDEPENDENT fallback for a non-byte-ascending (old-format /
// locale-collated) manifest: re-fetch it, buffer its ids (bounded by maxSortedManifestIDs), sort them
// byte-order, and re-run the SAME lock-step merge against the ledger. This keeps drift detection for a
// legitimately-written older manifest instead of hard-failing it as corrupt, at O(manifest) memory —
// only on the rare fallback path (new manifests are ascending and never reach here). A manifest too
// large to buffer safely → Unverifiable (can't diff it order-independently without risking an OOM).
func inventorySortedDiff(ctx context.Context, ir inventoryReader, led Ledger, name string) TargetVerdict {
	rc, err := abandonableFetch(ctx, auditProbeTimeout, func() (io.ReadCloser, error) { return ir.LatestManifest(ctx) })
	if err != nil {
		return classifyInventoryErr(ctx, name, err)
	}
	sr := &stallReader{ctx: ctx, rc: rc, timeout: auditProbeTimeout}
	defer func() { _ = sr.Close() }()

	ids, cerr := collectManifestIDs(sr, maxSortedManifestIDs)
	if cerr != nil {
		return classifyInventoryErr(ctx, name, cerr)
	}
	sort.Strings(ids)
	extra, missing, msize, merr := mergeInventory(sliceIterator(ids), ledgerStoredPull(ctx, led, name))
	// Same counts→verdict + orphan-preservation rule as the streaming path (one owner: mergeToVerdict).
	// The non-ascending signal can't recur here (ids are sorted), so there's no further fallback.
	return mergeToVerdict(ctx, name, extra, missing, msize, merr)
}

// collectManifestIDs reads every externalID from a (possibly non-ascending) manifest into a slice,
// bounded by maxIDs — it does NOT enforce monotonicity (the caller sorts). A structural fault (bad
// JSON / too-long line) still surfaces as errCorruptManifest; exceeding maxIDs surfaces as a distinct
// "too large" error the caller maps to Unverifiable.
func collectManifestIDs(r io.Reader, maxIDs int) ([]string, error) {
	next := manifestScanner(r, false) // non-strict: don't reject non-ascending
	var ids []string
	for {
		id, ok, err := next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return ids, nil
		}
		if len(ids) >= maxIDs {
			return nil, fmt.Errorf("manifest exceeds %d ids — too large to diff order-independently", maxIDs)
		}
		ids = append(ids, id)
	}
}

// sliceIterator returns a pull iterator over a slice of ids (for the sorted-fallback merge).
func sliceIterator(ids []string) func() (string, bool, error) {
	i := 0
	return func() (string, bool, error) {
		if i >= len(ids) {
			return "", false, nil
		}
		id := ids[i]
		i++
		return id, true, nil
	}
}

// classifyInventoryErr folds a manifest fetch/read/diff error into a verdict — the ONE classifier
// for both the fetch and the merge paths, so they can't diverge on what "corrupt" vs "unverifiable"
// vs "shutdown" means. parentCtx is the AUDIT's ctx, so a real shutdown is told apart from a
// per-operation deadline:
//   - os.ErrNotExist → NoData (no manifest yet) — benign.
//   - a PARENT cancel (a real SIGTERM — cancelledInFlight) → NoData; the audit's top-level ctx.Err()
//     fold surfaces the abort, not a spurious per-target fault.
//   - errCorruptManifest (a JSON parse error, bufio.ErrTooLong, or a non-ascending key) → Corrupt.
//   - errLedgerRead (a genuine DB error) → Fault.
//   - anything else — a per-operation DEADLINE (a wedged/black-holing target while the parent is
//     live), a read-denying credential, or a transient read reset → Unverifiable (a non-worm target
//     then FAILS: an incomplete integrity check must not read green).
func classifyInventoryErr(parentCtx context.Context, name string, err error) TargetVerdict {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return TargetVerdict{Status: StatusNoData, Detail: "no manifest written yet (nothing to diff)"}
	case cancelledInFlight(parentCtx, err):
		return shutdownVerdict(err)
	case errors.Is(err, ErrProbePanic):
		// A recovered driver panic is a code bug, never benign — fail loud (Fault), not Unverifiable.
		return TargetVerdict{Status: StatusFault, Err: fmt.Errorf("inventory probe %s: %w", name, err), Detail: "probe panicked"}
	case errors.Is(err, errCorruptManifest):
		return TargetVerdict{Status: StatusCorrupt, Err: fmt.Errorf("manifest for %s: %w", name, err), Detail: "corrupt manifest"}
	case errors.Is(err, errLedgerRead):
		return TargetVerdict{Status: StatusFault, Err: fmt.Errorf("inventory diff for %s: %w", name, err), Detail: "ledger read error"}
	default:
		return TargetVerdict{Status: StatusUnverifiable, Detail: fmt.Sprintf("manifest unverifiable (wedged/read-denying/transient): %v", err)}
	}
}

// mergeInventory lock-steps two STRICTLY-ASCENDING externalID streams — the target's manifest and
// the ledger's stored set — counting Extra (in manifest, not ledger), Missing (in ledger, not
// manifest) and the manifest size, in O(1) extra memory (no full-corpus maps). Both sources are
// strictly ascending in BYTE order: the manifest is written from StoredObjectsPage ORDER BY
// "externalID" COLLATE "C" and the ledger page query is ORDER BY "externalID" COLLATE "C", so the DB
// order matches the byte-order `<` comparison here; manifestIterator enforces monotonicity and
// reports a non-ascending manifest as corrupt.
func mergeInventory(nextManifest, nextLedger func() (string, bool, error)) (extra, missing, manifestSize int, err error) {
	m, mok, err := nextManifest()
	if err != nil {
		return extra, missing, manifestSize, err
	}
	l, lok, err := nextLedger()
	if err != nil {
		return extra, missing, manifestSize, err
	}
	for mok || lok {
		switch {
		case mok && (!lok || m < l): // in manifest, not (yet) in ledger → extra
			extra++
			manifestSize++
			if m, mok, err = nextManifest(); err != nil {
				return extra, missing, manifestSize, err
			}
		case lok && (!mok || l < m): // in ledger, not in manifest → missing
			missing++
			if l, lok, err = nextLedger(); err != nil {
				return extra, missing, manifestSize, err
			}
		default: // m == l — in both
			manifestSize++
			if m, mok, err = nextManifest(); err != nil {
				return extra, missing, manifestSize, err
			}
			if l, lok, err = nextLedger(); err != nil {
				return extra, missing, manifestSize, err
			}
		}
	}
	return extra, missing, manifestSize, nil
}

// manifestIterator returns the STRICT (byte-ascending-enforcing) pull iterator over the manifest's
// externalIDs — the streaming fast path. A non-ascending key signals errNonAscendingManifest (NOT a
// corruption fault) so the probe can fall back to an order-independent (sorted) diff, since an
// old-format / locale-collated manifest is legitimately written but not byte-ascending.
func manifestIterator(r io.Reader) func() (string, bool, error) { return manifestScanner(r, true) }

// manifestScanner is the shared JSONL manifest reader. The manifest is parsed line-by-line so a large
// one isn't buffered whole. strict=true enforces byte-ascending order (→ errNonAscendingManifest, the
// fallback signal); strict=false yields every id in file order (the order-independent fallback sorts
// afterward). A malformed line or a bufio.ErrTooLong IS a corruption fault either way. The reader is
// already ctx-bounded (a stallReader), so this does not re-wrap it.
func manifestScanner(r io.Reader, strict bool) func() (string, bool, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	prev := ""
	first := true
	return func() (string, bool, error) {
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var ml manifestLine
			if uerr := json.Unmarshal(line, &ml); uerr != nil {
				return "", false, fmt.Errorf("%w: parse line: %w", errCorruptManifest, uerr)
			}
			if ml.ExternalID == "" {
				continue
			}
			if strict && !first && ml.ExternalID <= prev {
				// NOT corruption: an old-format / locale-collated manifest can be non-ascending in byte
				// order → signal the caller to retry with an order-independent (sorted) diff.
				return "", false, fmt.Errorf("%w (%q after %q)", errNonAscendingManifest, ml.ExternalID, prev)
			}
			prev, first = ml.ExternalID, false
			return ml.ExternalID, true, nil
		}
		if serr := sc.Err(); serr != nil {
			switch {
			case errors.Is(serr, context.Canceled) || errors.Is(serr, context.DeadlineExceeded):
				// A cancellation / per-operation deadline (surfaced through the stallReader) — propagate
				// as-is, NOT a corruption fault: a clean SIGTERM / stall timeout is not "the manifest is
				// broken".
				return "", false, serr
			case errors.Is(serr, bufio.ErrTooLong):
				// A line larger than the buffer — a genuine structural content fault.
				return "", false, fmt.Errorf("%w: line too long: %w", errCorruptManifest, serr)
			default:
				// A transient mid-stream read fault (a network reset) — NOT structural corruption;
				// benign, retry next pass (classified Unverifiable by classifyInventoryErr).
				return "", false, fmt.Errorf("manifest read: %w", serr)
			}
		}
		return "", false, nil // clean EOF
	}
}

// ledgerStoredPull returns a pull iterator over the externalIDs the ledger records stored on target,
// keyset-paged by externalID via the shared keysetPull driver (so it can't diverge from KeysetLoop's
// after-cursor + short-page-stops contract), holding at most one page in memory and releasing the DB
// connection between pages. Each page fetch is bounded by a per-page deadline (auditProbeTimeout) —
// a per-PAGE bound, not a whole-diff one, so a large HEALTHY corpus (many fast pages) never
// false-fails while a wedged ledger page is still caught. A page error is tagged errLedgerRead so it
// classifies as a Fault, not a manifest corruption.
func ledgerStoredPull(ctx context.Context, led Ledger, target string) func() (string, bool, error) {
	return keysetPull("", KeysetPageSize,
		func(after string, limit int) ([]string, error) {
			page, err := storedPageBounded(ctx, led, target, after, limit)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", errLedgerRead, err)
			}
			return page, nil
		},
		func(id string) string { return id })
}

// manifestFetch carries a LatestManifest result across the RunAbandonableClose boundary.
type manifestFetch struct {
	rc  io.ReadCloser
	err error
}

// abandonableFetch runs fetch honoring ctx even when it can't (a filesystem os.ReadDir/os.Open on a
// WEDGED MOUNT is uninterruptible — the write path guards it with callWithCtx too), and bounds its
// DURATION by `timeout` WITHOUT cancelling the parent ctx the returned reader is tied to (so an s3
// object reader stays valid for the subsequent streaming read): it abandons on a CHILD deadline ctx
// while fetch itself runs on the PARENT ctx (the caller's closure captures it). On abandon (the
// timeout OR a parent cancel) it returns the ctx error and, via RunAbandonableClose's onLateResult
// hook, CLOSES any ReadCloser the abandoned fetch produces LATE (no fd leak). A driver panic becomes
// an error. Folded onto the shared RunAbandonableClose primitive rather than a hand-rolled copy.
func abandonableFetch(ctx context.Context, timeout time.Duration, fetch func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	fctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res := RunAbandonableClose(fctx,
		func() manifestFetch { rc, err := fetch(); return manifestFetch{rc: rc, err: err} },
		func() manifestFetch {
			return manifestFetch{err: fmt.Errorf("manifest fetch deadline/cancel (wedged target): %w", fctx.Err())}
		},
		func(r any) manifestFetch { return manifestFetch{err: PanicErr("latest manifest", r)} },
		func(f manifestFetch) {
			if f.rc != nil {
				_ = f.rc.Close() // free the abandoned fetch's late-produced reader
			}
		})
	return res.rc, res.err
}

// stallReader bounds each Read by `timeout`, abandoning a wedged read (a filesystem os.Read on a hung
// mount, or a black-holing S3 body) — so a mid-stream STALL can't hang the diff, while a HEALTHY
// stream (whose reads return promptly) never hits the deadline, so a large corpus does NOT false-fail
// (the per-read bound replaces the old whole-diff deadline). It reads into its OWN buffer and copies
// on success, so an abandoned late read never races the caller's p. It also OWNS closing the
// underlying ReadCloser: an abandoned read's goroutine keeps running against rc, so Close() must NOT
// close rc concurrently — the abandon path closes rc only AFTER that goroutine finishes (no fd race).
type stallReader struct {
	ctx       context.Context
	rc        io.ReadCloser
	timeout   time.Duration
	own       []byte
	abandoned bool // an abandon happened → the abandon path owns rc.Close(); Read/Close must not touch rc
}

// readResult carries one stallReader read across the RunAbandonableClose boundary.
type readResult struct {
	n   int
	err error
}

func (s *stallReader) Read(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	if s.abandoned { // never re-read after an abandon — the read goroutine still owns rc
		return 0, context.DeadlineExceeded
	}
	if len(s.own) < len(p) {
		s.own = make([]byte, len(p))
	}
	buf := s.own[:len(p)]
	rctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	// Delegate the abandon + cap-1-buffer + recover + late-close to the shared RunAbandonableClose
	// primitive rather than a hand-rolled copy: on the per-read deadline it marks the reader abandoned
	// (so later Reads short-circuit) and closes rc only AFTER the wedged read goroutine finishes via
	// onLateResult (no fd race with a caller's deferred Close). The read fills s.own; on success we copy
	// into the caller's p, so an abandoned late read never races p.
	res := RunAbandonableClose(rctx,
		func() readResult { n, err := s.rc.Read(buf); return readResult{n, err} },
		func() readResult { s.abandoned = true; return readResult{err: rctx.Err()} },
		func(r any) readResult { return readResult{err: PanicErr("manifest read", r)} },
		func(readResult) { _ = s.rc.Close() }) // free rc once the abandoned read finally completes
	n := copy(p, buf[:res.n])
	return n, res.err
}

// Close closes the underlying reader UNLESS a read was abandoned — in which case the abandon path
// owns closing it (after the wedged read goroutine finishes), so Close must not race an in-flight
// Read. Called by inventoryProbe via defer.
func (s *stallReader) Close() error {
	if s.abandoned {
		return nil
	}
	return s.rc.Close()
}
