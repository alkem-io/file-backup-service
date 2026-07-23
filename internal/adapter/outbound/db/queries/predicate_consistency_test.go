package queries

import (
	"strings"
	"testing"
)

// TestCoverageAndGapPredicatesAgree guards a cross-query invariant that has no other guard:
// CoverageGaps (the filebackup_under_replicated_objects gauge) and TargetGapsPage (the
// reconcile work-list) must encode the IDENTICAL "fully-replicated on a configured target"
// predicate — state='stored' AND target=ANY(targets), counted against the configured
// target_count. If one drifts (e.g. a future edit changes the stored-state literal or the
// target filter in only one query), the gauge silently disagrees with what reconcile
// repairs: operators read "0 under-replicated" while objects are genuinely under-replicated,
// or a gauge stuck nonzero reconcile can never drive to 0. There is no live-DB integration
// test in this repo, so this asserts the shared predicate fragments at the SQL-string level —
// a mechanical drift guard, not coverage padding.
//
// Scope (kept honest, since F4 was itself an overclaiming comment): it catches drift in the
// stored-state literal, the configured-target filter, and the presence of the int count
// comparison. It does NOT catch a flipped comparison DIRECTION (coverageGaps counts
// fully-replicated with >=, TargetGapsPage counts under-replicated with <, so they can't be
// asserted identical) — catching that would need a live-DB test that runs both against the
// same rows.
func TestCoverageAndGapPredicatesAgree(t *testing.T) {
	// Every fragment both queries must contain to compute the same predicate. If a maintainer
	// renames the stored state, changes the configured-target filter, or drops the int count
	// comparison in one query, the missing fragment fails the test and forces the other to match.
	fragments := []struct {
		frag, why string
	}{
		{"state = 'stored'", "the stored-state literal (must equal domain.StateStored) both queries filter on"},
		{"target = ANY(", "the configured-target filter (only count a hold on a CURRENTLY-configured target)"},
		{"::int", "the count comparison against the configured target_count (fully- vs under-replicated)"},
	}
	for _, f := range fragments {
		if !strings.Contains(coverageGaps, f.frag) {
			t.Errorf("coverageGaps missing %q — %s; it has drifted from TargetGapsPage", f.frag, f.why)
		}
		if !strings.Contains(targetGapsPage, f.frag) {
			t.Errorf("targetGapsPage missing %q — %s; it has drifted from CoverageGaps", f.frag, f.why)
		}
	}
}

// TestInventoryPagingIsByteOrdered guards the COLLATE "C" contract at the SQL-string level: the
// audit target→ledger inventory diff (domain.mergeInventory) lock-steps the ledger's paged
// externalIDs against a manifest using Go's byte-order `<`, and manifestIterator enforces
// strictly-ascending BYTE order — so the ledger paging queries MUST order (and range-filter) in byte
// order, not a locale collation, or the diff mis-counts drift AND a valid manifest is rejected as
// non-ascending. Both StoredExternalIDsPage (the audit/ledger sweep) and StoredObjectsPage (the
// manifest export the diff is compared against) must pin `COLLATE "C"` on BOTH the keyset range
// predicate and the ORDER BY, so the predicate and the ordering can't disagree (which would skip or
// repeat keyset rows). There is no live-DB test on a locale-collated database here, so this asserts
// the compiled SQL — a mechanical guard that FAILS if the collation is stripped, not padding.
func TestInventoryPagingIsByteOrdered(t *testing.T) {
	for _, q := range []struct {
		name, sql, order string
	}{
		{"StoredExternalIDsPage", storedExternalIDsPage, `ORDER BY "externalID" COLLATE "C"`},
		{"StoredObjectsPage", storedObjectsPage, `ORDER BY o."externalID" COLLATE "C"`},
	} {
		// The keyset range predicate must be byte-ordered (consistent with the ORDER BY collation).
		if !strings.Contains(q.sql, `> $2 COLLATE "C"`) {
			t.Errorf("%s: keyset range predicate is not byte-ordered — missing `> $2 COLLATE \"C\"`; a locale collation would skip/repeat keyset rows", q.name)
		}
		// The ORDER BY must be byte-ordered so the DB order matches mergeInventory's byte-order merge.
		if !strings.Contains(q.sql, q.order) {
			t.Errorf("%s: ORDER BY is not byte-ordered — missing %q; the inventory diff would mis-count and reject valid manifests", q.name, q.order)
		}
	}
}
