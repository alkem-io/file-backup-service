package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// runOneTick is the heart of TickLoop; testing it directly avoids racing a live ticker.
func TestRunOneTickRoutesErrorAndPanicToOnError(t *testing.T) {
	// A returned error → onError(cause) with the error.
	boom := errors.New("boom")
	var got any
	runOneTick(context.Background(), 0, func(context.Context) error { return boom }, func(c any) { got = c })
	if got != any(boom) {
		t.Fatalf("a returned error must route to onError: got %v", got)
	}

	// A panic → onError(cause) with the recovered value, NOT a crash.
	got = nil
	runOneTick(context.Background(), 0, func(context.Context) error { panic("kaboom") }, func(c any) { got = c })
	if got != "kaboom" {
		t.Fatalf("a panic must be recovered and routed to onError: got %v", got)
	}

	// Success → onError is NOT called.
	called := false
	runOneTick(context.Background(), 0, func(context.Context) error { return nil }, func(any) { called = true })
	if called {
		t.Fatal("onError must not fire on a successful pass")
	}
}

func TestRunOneTickTimeoutBoundsFn(t *testing.T) {
	// timeout>0 gives fn a ctx with a deadline; timeout<=0 runs on the parent ctx (no deadline).
	runOneTick(context.Background(), 50*time.Millisecond, func(fctx context.Context) error {
		if _, ok := fctx.Deadline(); !ok {
			t.Error("timeout>0 must give fn a bounded ctx")
		}
		return nil
	}, nil)
	runOneTick(context.Background(), 0, func(fctx context.Context) error {
		if _, ok := fctx.Deadline(); ok {
			t.Error("timeout<=0 must run fn on the parent ctx (no deadline)")
		}
		return nil
	}, nil)
}
