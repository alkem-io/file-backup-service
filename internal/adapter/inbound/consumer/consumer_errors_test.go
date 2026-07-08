package consumer

import (
	"context"
	"testing"
	"time"
)

// TestSignalCoalesces: signal is a non-blocking send, so a second signal while one wakeup is
// already queued is DROPPED (the default branch) rather than blocking the caller — the wake
// channel is a coalescing edge, not a counter.
func TestSignalCoalesces(t *testing.T) {
	ch := make(chan struct{}, 1)
	signal(ch) // first: queues a wakeup
	signal(ch) // second: channel full → non-blocking default, dropped (must not block)

	select {
	case <-ch:
	default:
		t.Fatal("the first signal must have queued a wakeup")
	}
	select {
	case <-ch:
		t.Fatal("signal must coalesce: a second wakeup must not queue while one is pending")
	default:
	}
}

// TestBackoffWaitsWhenLive: with a live ctx, backoff blocks on its ~1s timer (the timer-fired
// branch) rather than returning instantly — this is what stops a broken-DB/notify retry loop from
// hot-spinning. Complements TestBackoffReturnsFastOnCancel (the cancel branch).
func TestBackoffWaitsWhenLive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	backoff(ctx)
	if d := time.Since(start); d < 700*time.Millisecond {
		t.Fatalf("backoff with a live ctx returned in %v, want it to wait ~1s on the timer", d)
	}
}
