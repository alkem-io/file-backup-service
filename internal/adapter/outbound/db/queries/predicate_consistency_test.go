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
