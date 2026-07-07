package domain

import (
	"sync"
	"time"
)

// CircuitBreaker tracks per-target health so a persistently-down (or hung, or FLAKY)
// target trips OUT of the fan-out instead of burning every object's shared attempt count
// toward object-level dead-letter (T017a/FR-012). It trips on a failure RATE over a
// sliding window (threshold failures within the last 2*threshold observed outcomes), NOT
// a strict consecutive count — so a flaky-but-mostly-down target (fail 4 / succeed 1 /
// repeat) still trips instead of evading isolation and dead-lettering the corpus. Once
// tripped, the circuit OPENS for cooldown: the pipeline stops fanning to it, and an
// object whose only gap is an open-circuit target is DEFERRED (re-claimable, no attempt),
// so a single-target outage under-replicates TEMPORARILY (reconcile refills the gaps when
// the target returns) instead of dead-lettering. Shared across the worker pool, so
// mutex-guarded.
//
// It is PER-PROCESS (in-memory), not fleet-wide: with >1 worker replica, a down target
// must be observed independently by each replica's breaker, so during the initial trip
// window some objects can still take a real attempt (and, unluckily, dead-letter). That is
// acceptable because RECONCILE is the cross-worker backstop: an object that stored on its
// reachable targets has a ledger row with those stored statuses, so TargetGaps sees it as
// under-replicated and refills the down target when it returns — even if its OUTBOX row
// dead-lettered in the window. Fleet-wide circuit state (persisted in the ledger) would
// tighten the window but is not worth the coordination/staleness cost for an edge reconcile
// already recovers.
type CircuitBreaker struct {
	mu        sync.Mutex
	threshold int
	window    int // rolling outcomes kept per target (2*threshold); trip at threshold fails within it
	cooldown  time.Duration
	recent    map[string][]bool    // target -> last `window` outcomes (true=ok), oldest first
	openUntil map[string]time.Time // target -> when its open circuit next half-opens
}

// NewCircuitBreaker returns a breaker that opens a target after `threshold` failures
// within its last 2*threshold observed outcomes, held open for cooldown (after which one
// probe half-opens it).
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	threshold = max(threshold, 1)
	return &CircuitBreaker{
		threshold: threshold,
		window:    2 * threshold,
		cooldown:  cooldown,
		recent:    map[string][]bool{},
		openUntil: map[string]time.Time{},
	}
}

// Open reports whether target's circuit is currently open (the caller should SKIP the
// target). While open it returns true. Once the cooldown elapses it HALF-OPENS: the first
// caller gets false (it probes the target) and, atomically, the slot is re-reserved for
// another cooldown so EVERY other caller still gets true — so a down target is re-probed
// by ONE object at a time, never the whole worker pool (which would re-stall on it every
// cooldown). Record then closes (probe ok) or keeps it open (probe failed); if the probe
// never Records the reservation self-heals after the cooldown.
func (cb *CircuitBreaker) Open(target string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	until, open := cb.openUntil[target]
	if !open {
		return false // closed
	}
	if time.Now().Before(until) {
		return true // open, cooling down
	}
	cb.openUntil[target] = time.Now().Add(cb.cooldown) // reserve the probe slot; THIS caller probes
	return false
}

// Record folds a target's Store result into its rolling window. A successful HALF-OPEN
// probe (success while open) closes the circuit — confirmed recovered. Otherwise the
// outcome is appended to the window and the circuit opens (or a failed probe re-extends
// it) when failures within the window reach the threshold. A normal success does NOT wipe
// the window (only a probe success does), so a flaky target's occasional success can't
// reset it back to consecutive-counting.
func (cb *CircuitBreaker) Record(target string, ok bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if _, open := cb.openUntil[target]; ok && open {
		delete(cb.recent, target)
		delete(cb.openUntil, target)
		return
	}
	r := cb.recent[target]
	r = append(r, ok)
	if len(r) > cb.window {
		r = r[len(r)-cb.window:]
	}
	cb.recent[target] = r
	fails := 0
	for _, o := range r {
		if !o {
			fails++
		}
	}
	if fails >= cb.threshold {
		cb.openUntil[target] = time.Now().Add(cb.cooldown)
	}
}

// Down is a PURE read: does target have an open circuit right now (including one whose
// cooldown has elapsed but hasn't been confirmed recovered)? Unlike Open it has NO side
// effect — it never reserves a half-open probe slot — so a result-CLASSIFICATION caller
// (which must not mutate breaker state, e.g. on the aborted fan-out path) can ask "is
// this failed target a down-target gap?" without stealing the target's recovery probe.
func (cb *CircuitBreaker) Down(target string) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	_, open := cb.openUntil[target]
	return open
}

// OpenCount returns how many targets are currently tripped — the "targets down" gauge.
// It counts every target with an open-circuit entry, INCLUDING one whose cooldown has
// elapsed but hasn't been confirmed recovered by a successful probe yet: an entry is
// cleared ONLY by Record(ok=true), so a cooldown-elapsed-but-unprobed target still reads
// down (else the gauge would flap to 0 during a traffic lull while the target is still down).
func (cb *CircuitBreaker) OpenCount() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return len(cb.openUntil)
}
