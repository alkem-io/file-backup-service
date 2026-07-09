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
	Unverifiable bool // no manifest capability, or the target's read is denied / no manifest yet
	Detail       string
	Err          error // a non-nil ledger/parse error for this target (surfaced via errors.Join)
}

// Failed reports GENUINE target→ledger drift for this target: the target's own manifest lists
// objects the ledger has no stored record of (an orphan, or a ledger record lost since the
// snapshot). An unverifiable target never fails; Missing (snapshot staleness) never fails.
func (a InventoryAudit) Failed() bool { return !a.Unverifiable && a.Err == nil && a.Extra > 0 }

// errCorruptManifest marks a manifest that WAS fetched but is malformed (a truncated/corrupt
// JSONL) — a real fault (the target's DR inventory is broken), distinct from a read-DENIED or
// missing manifest, which are merely unverifiable.
var errCorruptManifest = errors.New("corrupt manifest")

// InventoryReport is the per-target target→ledger audit result.
type InventoryReport struct {
	Targets []InventoryAudit
}

// FailErr is the target→ledger pass/fail verdict: non-nil (nonzero exit for cron/CI) when any
// target's own manifest references objects the ledger doesn't record as stored there (Extra>0).
func (r InventoryReport) FailErr() error {
	var drifted []string
	for _, a := range r.Targets {
		if a.Failed() {
			drifted = append(drifted, fmt.Sprintf("%s (%d extra)", a.Target, a.Extra))
		}
	}
	if len(drifted) > 0 {
		return fmt.Errorf("targets whose manifest holds objects the ledger doesn't record stored (orphan / lost ledger record): %v", drifted)
	}
	return nil
}

// AuditInventory runs the target→ledger direction for every target: it reads each target's most
// recent manifest (its self-declared inventory) and diffs it against the ledger's current stored
// set for that target. A target whose sink can't return a manifest (a WORM read-denying
// credential, or a target with no manifest written yet) is reported Unverifiable — NOT failed —
// per the WORM contract. Targets are swept concurrently; a ledger error for one target is
// captured in its Err and surfaced via the joined return.
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

// auditInventoryTarget diffs one target's latest manifest against the ledger's stored set for it.
func auditInventoryTarget(ctx context.Context, led Ledger, t Target) InventoryAudit {
	name := t.Sink.Name()
	ir, ok := t.Sink.(inventoryReader)
	if !ok {
		return InventoryAudit{Target: name, Unverifiable: true, Detail: "target type cannot enumerate its manifest"}
	}
	manifest, err := readLatestManifest(ctx, ir)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return InventoryAudit{Target: name, Unverifiable: true, Detail: "no manifest written yet (nothing to diff)"}
		case errors.Is(err, errCorruptManifest):
			// The manifest WAS fetched but is malformed — a real DR fault (its inventory is broken),
			// surfaced as a sweep error (nonzero exit), not silently under-counted as unverifiable.
			return InventoryAudit{Target: name, Err: fmt.Errorf("manifest for %s: %w", name, err)}
		default:
			// A read-denying WORM credential (403) or a transient read fault → unverifiable, not drift.
			return InventoryAudit{Target: name, Unverifiable: true, Detail: fmt.Sprintf("manifest unreadable (read-denying credential or transient error): %v", err)}
		}
	}
	stored, err := storedSet(ctx, led, name)
	if err != nil {
		return InventoryAudit{Target: name, Err: fmt.Errorf("ledger stored set for %s: %w", name, err)}
	}
	a := InventoryAudit{Target: name, ManifestSize: len(manifest)}
	for id := range manifest {
		if !stored[id] {
			a.Extra++
		}
	}
	for id := range stored {
		if !manifest[id] {
			a.Missing++
		}
	}
	a.Detail = fmt.Sprintf("manifest=%d ledger-stored=%d", len(manifest), len(stored))
	return a
}

// manifestFetch carries a LatestManifest result across the abandonment boundary.
type manifestFetch struct {
	rc  io.ReadCloser
	err error
}

// readLatestManifest reads a target's newest manifest snapshot into a set of externalIDs. The
// manifest is JSONL (one manifestLine per row) written RAW (no codec), so it is parsed directly.
// A malformed line stops the parse (a truncated/corrupt manifest is a real fault, not silently
// under-counted). The reader is always closed.
//
// The LatestManifest CALL is run through RunAbandonable so a filesystem sink's os.ReadDir/os.Open
// on a WEDGED MOUNT (uninterruptible, ctx-ignoring — the same hazard the write path guards with
// callWithCtx) can't hang the audit past ctx; a driver panic becomes an error too.
func readLatestManifest(ctx context.Context, ir inventoryReader) (map[string]bool, error) {
	res := RunAbandonable(ctx,
		func() manifestFetch { rc, err := ir.LatestManifest(ctx); return manifestFetch{rc: rc, err: err} },
		func() manifestFetch { return manifestFetch{err: ctx.Err()} },
		func(r any) manifestFetch { return manifestFetch{err: PanicErr("latest manifest", r)} })
	if res.err != nil {
		return nil, res.err
	}
	rc := res.rc
	defer func() { _ = rc.Close() }()
	set := map[string]bool{}
	// A manifest can be large (one line per object); scan line-by-line with a generous max token
	// so a long JSONL row (all fields present) isn't rejected, without buffering the whole file.
	sc := bufio.NewScanner(ctxReader{ctx, rc})
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var ml manifestLine
		if err := json.Unmarshal(line, &ml); err != nil {
			return nil, fmt.Errorf("%w: parse line: %w", errCorruptManifest, err)
		}
		if ml.ExternalID != "" {
			set[ml.ExternalID] = true
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	return set, nil
}

// storedSet loads the ledger's current stored externalID set for target, keyset-paged (releasing
// the connection between pages) so the diff doesn't pin a connection for the whole enumeration.
func storedSet(ctx context.Context, led Ledger, target string) (map[string]bool, error) {
	set := map[string]bool{}
	err := KeysetLoop("", KeysetPageSize,
		func(after string, limit int) ([]string, error) {
			return led.StoredExternalIDsPage(ctx, target, after, limit)
		},
		func(id string) string { return id },
		func(id string) error { set[id] = true; return nil })
	if err != nil {
		return nil, err
	}
	return set, nil
}
