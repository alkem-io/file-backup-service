package domain

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

// hungSink accepts the Store call and reads NOTHING until its release channel closes —
// a black-holed endpoint / wedged mount that stalls the fan-out barrier.
type hungSink struct {
	stubSink
	release chan struct{}
}

func (h *hungSink) Store(ctx context.Context, _ string, _ io.Reader) (int64, error) {
	select {
	case <-h.release:
		return 0, io.EOF // never used; keeps the type honest
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// TestFanoutDropsHungTarget: with a stall timeout, a HUNG target is dropped individually
// and the HEALTHY co-fanned target still stores — one hung sink must NOT stall the
// barrier and starve a healthy target (the Alt-1 / T017a runtime-isolation guarantee).
// Without the stall-drop this test would hang until the outer timeout.
func TestFanoutDropsHungTarget(t *testing.T) {
	data := bytes.Repeat([]byte("fan me out past one chunk "), 100000) // multi-chunk (>1 MiB)
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	up := newMemSink("up")
	hung := &hungSink{stubSink: stubSink{name: "hung"}, release: make(chan struct{})}
	defer close(hung.release)
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: up, Codec: CodecNone}, {Sink: hung, Codec: CodecNone}})
	p.StallTimeout = 100 * time.Millisecond // drop a non-draining target fast (test)

	done := make(chan struct{})
	var ok, deferred bool
	var berr error
	go func() {
		ok, deferred, berr = p.BackupOne(context.Background(), OutboxEntry{ExternalID: h})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("BackupOne hung — the stall-drop did not isolate the hung target")
	}
	if berr != nil {
		t.Fatalf("backup: %v", berr)
	}
	if ok || deferred {
		t.Fatalf("want not-done, not-deferred (hung target failed on its own, not circuit-open yet): ok=%v deferred=%v", ok, deferred)
	}
	if !bytes.Equal(up.store[h], data) {
		t.Fatal("the healthy target must have stored the full object despite the hung sibling")
	}
	if led.states[h+"/hung"] == StateStored {
		t.Fatal("the hung target must NOT be recorded stored")
	}
}

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

// TestCircuitBreakerFlakyTarget: a flaky-but-mostly-down target (fail 4 / succeed 1)
// never has `threshold` CONSECUTIVE failures, yet must still trip on the failure RATE —
// the old consecutive-count breaker let it evade isolation and dead-letter the corpus.
func TestCircuitBreakerFlakyTarget(t *testing.T) {
	cb := NewCircuitBreaker(5, time.Minute) // window = 10
	// Feed the flaky pattern one outcome at a time, stopping once it trips — modelling the
	// pipeline skipping a down target (pendingTargets). The success every 5th outcome resets
	// a CONSECUTIVE counter, so the old breaker would never trip; the windowed one does.
	flaky := []bool{false, false, false, false, true}
	for i := 0; i < 30 && !cb.Down("t"); i++ {
		cb.Record("t", flaky[i%len(flaky)])
	}
	if !cb.Down("t") {
		t.Fatal("a flaky-mostly-down target (80% failure) must trip on the failure rate, not evade isolation")
	}
	// A genuinely healthy target (occasional single blip) must NOT trip.
	cb2 := NewCircuitBreaker(5, time.Minute)
	for i := 0; i < 40; i++ {
		ok := i%20 != 0 // one blip every 20
		cb2.Record("h", ok)
	}
	if cb2.Down("h") {
		t.Fatal("a healthy target with rare blips must not trip")
	}
}

// finalizeHangSink drains its stream to EOF (so the fanout barrier completes and a sibling
// stores) but then HANGS before returning — modelling an S3 CompleteMultipartUpload /
// fsync-on-a-wedged-mount hang in the FINALIZATION phase (after the stream verified).
type finalizeHangSink struct {
	stubSink
	release chan struct{}
}

func (h *finalizeHangSink) Store(ctx context.Context, _ string, r io.Reader) (int64, error) {
	_, _ = io.Copy(io.Discard, r) // drain to EOF — the barrier completes, the sibling stores
	select {
	case <-h.release:
		return 0, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// TestFinalizeHangTripsCircuit: a target that hangs on FINALIZE (post-stream) is caught by
// the per-object timeout on the aborted path; because a sibling stored (anyStored), its
// failure folds into the circuit and it trips — instead of never tripping and dead-lettering.
func TestFinalizeHangTripsCircuit(t *testing.T) {
	up := newMemSink("up")
	hung := &finalizeHangSink{stubSink: stubSink{name: "hung"}, release: make(chan struct{})}
	defer close(hung.release)
	led := newFakeLedger()
	p := NewPipeline(nil, led, []Target{{Sink: up, Codec: CodecNone}, {Sink: hung, Codec: CodecNone}})
	p.Circuit = NewCircuitBreaker(3, time.Minute) // trips at 3 failures in the window

	for i := 0; i < 6 && !p.Circuit.Down("hung"); i++ {
		data := bytes.Repeat([]byte("x"), 20+i) // distinct content per object
		h, _ := sum(bytes.NewReader(data))
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, _, _ = p.backupFrom(ctx, fakeSource{data}, OutboxEntry{ExternalID: h})
		cancel()
	}
	if !p.Circuit.Down("hung") {
		t.Fatal("a finalize-hung target must trip its circuit (via the timeout+anyStored fold), not dead-letter forever")
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
