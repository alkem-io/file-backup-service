package domain

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Millisecond)
	if cb.Open("x") {
		t.Fatal("a fresh circuit must be closed")
	}
	cb.Record("x", false)
	cb.Record("x", false)
	if cb.Open("x") {
		t.Fatal("under threshold must stay closed")
	}
	cb.Record("x", false) // 3rd consecutive failure trips it
	if !cb.Open("x") || cb.OpenCount() != 1 {
		t.Fatalf("at threshold must open (OpenCount=%d)", cb.OpenCount())
	}
	if !cb.Open("x") {
		t.Fatal("still cooling down must be open")
	}
	// After the cooldown it half-opens: exactly ONE caller probes (false), others reserved.
	time.Sleep(50 * time.Millisecond)
	first, second := cb.Open("x"), cb.Open("x")
	if first || !second {
		t.Fatalf("half-open must let ONE probe through (first=%v second=%v)", first, second)
	}
	cb.Record("x", true) // a successful probe closes the circuit
	if cb.Open("x") || cb.OpenCount() != 0 {
		t.Fatalf("a successful probe must close the circuit (OpenCount=%d)", cb.OpenCount())
	}
}

// TestBackupOneDefersOnDownTarget: when a target's circuit is open (down), an object that
// stored on every reachable target is DEFERRED (no genuine failure), not Failed — so a
// single-target outage doesn't march the corpus to dead-letter (T017a).
func TestBackupOneDefersOnDownTarget(t *testing.T) {
	data := []byte("defer me while a target is down")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	up := newMemSink("up")
	down := &failSink{stubSink{name: "down"}}
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: up, Codec: CodecNone}, {Sink: down, Codec: CodecNone}})
	p.Circuit = NewCircuitBreaker(1, time.Minute) // threshold 1: "down" trips open on its first failure

	done, deferred, err := p.BackupOne(context.Background(), OutboxEntry{ExternalID: h})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	if done || !deferred {
		t.Fatalf("want deferred (not done): done=%v deferred=%v", done, deferred)
	}
	if !bytes.Equal(up.store[h], data) {
		t.Fatal("the reachable target must still be stored")
	}
	// Second attempt: the object dedups on "up" and SKIPS the now-open "down" circuit → deferred again.
	done, deferred, err = p.BackupOne(context.Background(), OutboxEntry{ExternalID: h})
	if err != nil || done || !deferred {
		t.Fatalf("retry must defer while the target stays down: done=%v deferred=%v err=%v", done, deferred, err)
	}
}
