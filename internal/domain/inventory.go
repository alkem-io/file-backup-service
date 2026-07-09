package domain

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// InventoryAudit is one target's target→ledger drift (T032b/FR-025). It compares the target's
// OWN latest manifest snapshot (its self-declared inventory) against the ledger's current
// per-target stored set:
//   - Extra:   externalIDs the manifest lists but the ledger no longer records stored on this
//     target — an orphan on the target, or (since the ledger never drops a 'stored' status) a
//     LOST ledger record the manifest could rebuild. This is the genuine drift the audit fails on.
//   - Missing: externalIDs the ledger records stored on this target but the manifest omits —
//     normally just objects stored AFTER the last manifest snapshot (the manifest is periodic),
//     so it is informational, not a failure.
type InventoryAudit struct {
	Target       string
	ManifestSize int  // externalIDs in the target's latest manifest
	Extra        int  // in the manifest, NOT ledger-stored on this target (orphan / lost ledger record)
	Missing      int  // ledger-stored on this target, NOT in the manifest (usually newer than the snapshot)
	Unverifiable bool // no definitive diff (see NoData/Worm for whether that's benign)
	NoData       bool // Unverifiable because there is nothing to diff yet (no manifest / no capability) — benign
	Worm         bool // read-denying by design; an unreadable (not no-data) NON-worm target is a broken read path → FAIL
	Detail       string
	Err          error // a fault (corrupt manifest, ledger read error, panic) — surfaced via the joined sweep error
}

// Failed reports GENUINE target→ledger drift for this target: the target's own manifest lists
// objects the ledger has no stored record of (an orphan / lost ledger record), OR — the unified
// audit policy — a NON-worm target whose manifest could not be READ (a broken read path, not just
// "no manifest yet"), consistent with ledger→target's UnexpectedlyUnverifiable. A fault (Err) is
// surfaced separately (the sweep error), a worm read-deny / no-data is benign, and Missing
// (snapshot staleness) never fails.
func (a InventoryAudit) Failed() bool {
	if a.Err != nil {
		return false
	}
	if a.Unverifiable {
		return !a.NoData && !a.Worm
	}
	return a.Extra > 0
}

// errCorruptManifest marks a manifest that WAS fetched but is malformed (a truncated/corrupt JSONL,
// a bufio.ErrTooLong line, a non-ascending key, or a mid-stream read fault) — a real fault (the
// target's DR inventory is broken), distinct from a read-DENIED or missing manifest, which are
// merely unverifiable.
var errCorruptManifest = errors.New("corrupt manifest")

// InventoryReport is the per-target target→ledger audit result.
type InventoryReport struct {
	Targets []InventoryAudit
}

// FailErr is the target→ledger pass/fail verdict: non-nil (nonzero exit for cron/CI) when any
// target's manifest references objects the ledger doesn't record stored (Extra>0), OR a non-worm
// target's manifest couldn't be read (a broken read path). Faults (per-target Err) are surfaced
// via AuditInventory's joined return, not here.
func (r InventoryReport) FailErr() error {
	var extra, unreadable []string
	for _, a := range r.Targets {
		switch {
		case a.Err != nil:
			// a fault — surfaced separately
		case !a.Unverifiable && a.Extra > 0:
			extra = append(extra, fmt.Sprintf("%s (%d extra)", a.Target, a.Extra))
		case a.Unverifiable && !a.NoData && !a.Worm:
			unreadable = append(unreadable, a.Target)
		}
	}
	var errs []error
	if len(extra) > 0 {
		errs = append(errs, fmt.Errorf("targets whose manifest holds objects the ledger doesn't record stored (orphan / lost ledger record): %v", extra))
	}
	if len(unreadable) > 0 {
		errs = append(errs, fmt.Errorf("non-worm targets whose manifest could not be read (broken read path): %v", unreadable))
	}
	return errors.Join(errs...)
}

// AuditInventory runs the target→ledger direction for every target: it stream-merges each target's
// most recent manifest (its self-declared inventory) against the ledger's current stored set for
// that target — O(page) memory, no full-corpus maps. A target whose manifest can't be read (a WORM
// read-denying credential, or no manifest written yet) is Unverifiable — benign for a worm/no-data
// target, a failure for a non-worm one (broken read path). Targets are swept concurrently; a fault
// (corrupt manifest / ledger error / panic) is captured in the target's Err and surfaced via the
// joined return.
func AuditInventory(ctx context.Context, led Ledger, targets []Target) (InventoryReport, error) {
	rep := InventoryReport{Targets: make([]InventoryAudit, len(targets))}
	errs := RunParallelIdx(len(targets),
		func(i int) string { return "inventory " + targets[i].Sink.Name() },
		func(i int) error {
			a := auditInventoryTarget(ctx, led, targets[i])
			rep.Targets[i] = a
			return a.Err
		})
	return rep, errors.Join(errs...)
}

// auditInventoryTarget stream-merges one target's latest manifest against the ledger's stored set
// for it, bounded by a per-target deadline (so a wedged NFS / black-holing S3 can't hang the Job)
// and panic-recovered (so a driver/scan panic becomes this target's Err, not a crashed sweep).
func auditInventoryTarget(ctx context.Context, led Ledger, t Target) (a InventoryAudit) {
	name := t.Sink.Name()
	a.Target, a.Worm = name, t.Worm
	// A driver panic (a pgx scan on a drifted column, a broken reader) becomes this target's Err —
	// a populated result is ALWAYS written, like checkOne.
	defer func() {
		if r := recover(); r != nil {
			a = InventoryAudit{Target: name, Worm: t.Worm, Err: PanicErr("inventory "+name, r)}
		}
	}()
	// Bound the whole per-target probe (fetch + read + ledger paging) — it runs under a deadline-less
	// signal ctx, so a wedged target would otherwise hang the audit Job forever.
	tctx, cancel := context.WithTimeout(ctx, auditProbeTimeout)
	defer cancel()

	ir, ok := t.Sink.(inventoryReader)
	if !ok {
		a.Unverifiable, a.NoData, a.Detail = true, true, "target type cannot enumerate its manifest"
		return a
	}
	rc, err := fetchLatestManifest(tctx, ir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			a.Unverifiable, a.NoData, a.Detail = true, true, "no manifest written yet (nothing to diff)"
			return a
		}
		// A read-denying credential / transient fetch error → unverifiable (fails a non-worm target).
		a.Unverifiable, a.Detail = true, fmt.Sprintf("manifest unreadable (read-denying credential or transient error): %v", err)
		return a
	}
	defer func() { _ = rc.Close() }()

	extra, missing, msize, merr := mergeInventory(manifestIterator(tctx, rc), ledgerStoredIterator(tctx, led, name))
	if merr != nil {
		if errors.Is(merr, errCorruptManifest) {
			a.Err = fmt.Errorf("manifest for %s: %w", name, merr)
		} else {
			a.Err = fmt.Errorf("inventory diff for %s (ledger read): %w", name, merr)
		}
		return a
	}
	a.Extra, a.Missing, a.ManifestSize = extra, missing, msize
	a.Detail = fmt.Sprintf("manifest=%d extra=%d missing=%d", msize, extra, missing)
	return a
}

// mergeInventory lock-steps two STRICTLY-ASCENDING externalID streams — the target's manifest and
// the ledger's stored set — counting Extra (in manifest, not ledger), Missing (in ledger, not
// manifest) and the manifest size, in O(1) extra memory (no full-corpus maps). Both sources are
// strictly ascending (the manifest is written from StoredObjectsPage ORDER BY externalID; the
// ledger page query is ORDER BY externalID); manifestIterator enforces monotonicity and reports a
// non-ascending manifest as corrupt.
func mergeInventory(nextManifest, nextLedger func() (string, bool, error)) (extra, missing, manifestSize int, err error) {
	m, mok, err := nextManifest()
	if err != nil {
		return 0, 0, 0, err
	}
	l, lok, err := nextLedger()
	if err != nil {
		return 0, 0, 0, err
	}
	for mok || lok {
		switch {
		case mok && (!lok || m < l): // in manifest, not (yet) in ledger → extra
			extra++
			manifestSize++
			if m, mok, err = nextManifest(); err != nil {
				return 0, 0, 0, err
			}
		case lok && (!mok || l < m): // in ledger, not in manifest → missing
			missing++
			if l, lok, err = nextLedger(); err != nil {
				return 0, 0, 0, err
			}
		default: // m == l — in both
			manifestSize++
			if m, mok, err = nextManifest(); err != nil {
				return 0, 0, 0, err
			}
			if l, lok, err = nextLedger(); err != nil {
				return 0, 0, 0, err
			}
		}
	}
	return extra, missing, manifestSize, nil
}

// manifestIterator returns a pull iterator over the manifest's externalIDs. The manifest is JSONL
// (one manifestLine per row) written RAW (no codec), parsed line-by-line so a large manifest isn't
// buffered whole. A malformed line, a bufio.ErrTooLong / mid-stream read fault, OR a non-ascending
// key is a CORRUPT manifest (errCorruptManifest — a real fault), NOT unverifiable: the object was
// fetched but its content is broken.
func manifestIterator(ctx context.Context, rc io.Reader) func() (string, bool, error) {
	sc := bufio.NewScanner(ctxReader{ctx, rc})
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
			if !first && ml.ExternalID <= prev {
				return "", false, fmt.Errorf("%w: not strictly ascending (%q after %q)", errCorruptManifest, ml.ExternalID, prev)
			}
			prev, first = ml.ExternalID, false
			return ml.ExternalID, true, nil
		}
		if serr := sc.Err(); serr != nil {
			return "", false, fmt.Errorf("%w: read: %w", errCorruptManifest, serr)
		}
		return "", false, nil // clean EOF
	}
}

// ledgerStoredIterator returns a pull iterator over the externalIDs the ledger records stored on
// target, keyset-paged (ORDER BY externalID) so it holds at most one page in memory and releases
// the DB connection between pages.
func ledgerStoredIterator(ctx context.Context, led Ledger, target string) func() (string, bool, error) {
	var page []string
	i := 0
	after := ""
	done := false
	return func() (string, bool, error) {
		for i >= len(page) {
			if done {
				return "", false, nil
			}
			p, err := led.StoredExternalIDsPage(ctx, target, after, KeysetPageSize)
			if err != nil {
				return "", false, err
			}
			if len(p) < KeysetPageSize {
				done = true
			}
			if len(p) == 0 {
				return "", false, nil
			}
			after, page, i = p[len(p)-1], p, 0
		}
		id := page[i]
		i++
		return id, true, nil
	}
}

// manifestFetch carries a LatestManifest result across the abandonment boundary.
type manifestFetch struct {
	rc  io.ReadCloser
	err error
}

// fetchLatestManifest runs LatestManifest honoring ctx even when the sink can't (a filesystem
// os.ReadDir/os.Open on a WEDGED MOUNT is uninterruptible — the write path guards it with
// callWithCtx too), via the shared RunAbandonableCleanup primitive: on ctx cancellation it returns
// ctx.Err() and abandons the goroutine, and the cleanup CLOSES any ReadCloser the abandoned call
// produces LATE so it can't leak an fd. A driver panic becomes an error.
func fetchLatestManifest(ctx context.Context, ir inventoryReader) (io.ReadCloser, error) {
	res := RunAbandonableCleanup(ctx,
		func() manifestFetch { rc, err := ir.LatestManifest(ctx); return manifestFetch{rc: rc, err: err} },
		func() manifestFetch { return manifestFetch{err: ctx.Err()} },
		func(r any) manifestFetch { return manifestFetch{err: PanicErr("latest manifest", r)} },
		func(f manifestFetch) {
			if f.rc != nil {
				_ = f.rc.Close()
			}
		})
	return res.rc, res.err
}
