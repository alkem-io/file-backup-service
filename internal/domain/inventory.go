package domain

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// errCorruptManifest marks a manifest that WAS fetched but has a STRUCTURAL content fault — a
// malformed JSONL line, a bufio.ErrTooLong line, or a non-ascending key — a real fault (the
// target's DR inventory is broken). It deliberately does NOT cover a transient mid-stream read
// reset (Unverifiable, retry) or a ctx cancellation (propagated), so a blip/SIGTERM can't fire a
// false data-corruption fault.
var errCorruptManifest = errors.New("corrupt manifest")

// errLedgerRead marks an error from the ledger side of the merge, so a genuine DB fault (Fault) is
// classified apart from a transient MANIFEST read (Unverifiable) — both surface through
// mergeInventory as one error otherwise.
var errLedgerRead = errors.New("ledger read")

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
	rc, err := abandonableFetch(ctx, auditProbeTimeout, func() (io.ReadCloser, error) { return ir.LatestManifest(ctx) })
	if err != nil {
		return classifyInventoryErr(ctx, t.Sink.Name(), err)
	}
	defer func() { _ = rc.Close() }()

	manifestNext := manifestIterator(&stallReader{ctx: ctx, r: rc, timeout: auditProbeTimeout})
	ledgerNext := ledgerStoredPull(ctx, led, t.Sink.Name())
	extra, missing, msize, merr := mergeInventory(manifestNext, ledgerNext)
	if merr != nil {
		return classifyInventoryErr(ctx, t.Sink.Name(), merr)
	}
	detail := fmt.Sprintf("manifest=%d extra=%d missing=%d", msize, extra, missing)
	if extra > 0 { // an orphan / lost ledger record — the genuine target→ledger drift
		return TargetVerdict{Status: StatusDrift, Extra: extra, Missing: missing, Detail: detail}
	}
	return TargetVerdict{Status: StatusVerified, Extra: extra, Missing: missing, Detail: detail}
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
		return TargetVerdict{Status: StatusNoData, Detail: fmt.Sprintf("aborted by shutdown: %v", err)}
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
// fetched but its content is broken. The reader is already ctx-bounded (a stallReader), so this
// does not re-wrap it.
func manifestIterator(r io.Reader) func() (string, bool, error) {
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
			if !first && ml.ExternalID <= prev {
				return "", false, fmt.Errorf("%w: not strictly ascending (%q after %q)", errCorruptManifest, ml.ExternalID, prev)
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
			pctx, cancel := context.WithTimeout(ctx, auditProbeTimeout)
			defer cancel()
			page, err := led.StoredExternalIDsPage(pctx, target, after, limit)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", errLedgerRead, err)
			}
			return page, nil
		},
		func(id string) string { return id })
}

// abandonableFetch runs fetch honoring ctx even when it can't (a filesystem os.ReadDir/os.Open on a
// WEDGED MOUNT is uninterruptible — the write path guards it with callWithCtx too), and bounds its
// DURATION by timeout WITHOUT cancelling the parent ctx the returned reader is tied to (so an s3
// object reader stays valid for the subsequent streaming read). On ctx cancel OR the timeout it
// returns the error and, in a detached goroutine, CLOSES any ReadCloser the abandoned fetch produces
// LATE (no fd leak). A driver panic becomes an error. The non-generic successor to the old
// RunAbandonableCleanup + manifestFetch carrier, built on the same buffered-chan abandon primitive.
func abandonableFetch(ctx context.Context, timeout time.Duration, fetch func() (io.ReadCloser, error)) (io.ReadCloser, error) {
	type result struct {
		rc  io.ReadCloser
		err error
	}
	ch := make(chan result, 1) // buffered so an abandoned fetch never blocks on its send
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{err: PanicErr("latest manifest", r)}
			}
		}()
		rc, err := fetch()
		ch <- result{rc: rc, err: err}
	}()
	closeLate := func() {
		go func() {
			if r := <-ch; r.rc != nil {
				_ = r.rc.Close() // free the abandoned fetch's late-produced reader
			}
		}()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		closeLate()
		return nil, ctx.Err()
	case <-timer.C:
		closeLate()
		return nil, fmt.Errorf("manifest fetch deadline exceeded (wedged target): %w", context.DeadlineExceeded)
	case r := <-ch:
		return r.rc, r.err
	}
}

// stallReader bounds each Read by `timeout`, abandoning a wedged read (a filesystem os.Read on a hung
// mount, or a black-holing S3 body) — so a mid-stream STALL can't hang the diff, while a HEALTHY
// stream (whose reads return promptly) never hits the deadline, so a large corpus does NOT false-fail
// (the per-read bound replaces the old whole-diff deadline). It reads into its OWN buffer and copies
// on success, so an abandoned late read never races the caller's p; on a timeout the caller (the
// manifest scanner) stops reading, so the abandoned buffer is never re-read.
type stallReader struct {
	ctx     context.Context
	r       io.Reader
	timeout time.Duration
	own     []byte
}

func (s *stallReader) Read(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	if len(s.own) < len(p) {
		s.own = make([]byte, len(p))
	}
	buf := s.own[:len(p)]
	rctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	type res struct {
		n   int
		err error
	}
	out := RunAbandonable(rctx,
		func() res { n, err := s.r.Read(buf); return res{n, err} },
		func() res { return res{err: rctx.Err()} },
		func(r any) res { return res{err: PanicErr("manifest read", r)} })
	n := copy(p, buf[:out.n])
	return n, out.err
}
