package domain

import (
	"sync"
	"time"
)

// CircuitBreaker tracks per-target health so a persistently-down (or hung) target trips
// OUT of the fan-out instead of burning every object's shared attempt count toward
// object-level dead-letter (T017a/FR-012). After threshold consecutive failures a
// target's circuit OPENS for cooldown: the pipeline stops fanning to it, and an object
// whose only gap is an open-circuit target is DEFERRED (re-claimable, no attempt) rather
// than Failed — so a multi-hour single-target outage under-replicates the corpus
// TEMPORARILY (reconcile refills the gaps when the target returns) instead of marching
// the whole corpus to dead-letter. Shared across the worker pool, so it is mutex-guarded.
type CircuitBreaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	fails     map[string]int       // consecutive failures per target
	openUntil map[string]time.Time // target -> when its open circuit next half-opens
}

// NewCircuitBreaker returns a breaker that opens a target after threshold consecutive
// failures and keeps it open for cooldown (after which one probe half-opens it).
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold < 1 {
		threshold = 1
	}
	return &CircuitBreaker{
		threshold: threshold,
		cooldown:  cooldown,
		fails:     map[string]int{},
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

// Record folds a target's Store result into its circuit: a success closes it (and clears
// the failure count); a failure increments it, opening (or, on a failed half-open probe,
// re-extending) the circuit at the threshold.
func (cb *CircuitBreaker) Record(target string, ok bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if ok {
		delete(cb.fails, target)
		delete(cb.openUntil, target)
		return
	}
	cb.fails[target]++
	if cb.fails[target] >= cb.threshold {
		cb.openUntil[target] = time.Now().Add(cb.cooldown)
	}
}

// OpenCount returns how many targets currently have an open circuit — the "targets down"
// gauge (a deferred object shows as backlog, not dead-letter, so this is the direct signal).
func (cb *CircuitBreaker) OpenCount() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	now := time.Now()
	n := 0
	for _, until := range cb.openUntil {
		if now.Before(until) {
			n++
		}
	}
	return n
}
