package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// runOneTick is the heart of TickLoop; testing it directly avoids racing a live ticker.
func TestRunOneTickRoutesErrorAndPanicToOnError(t *testing.T) {
	// A returned error → onError(cause, isPanic=false).
	boom := errors.New("boom")
	var got any
	var gotPanic bool
	runOneTick(context.Background(), 0, func(context.Context) error { return boom }, func(c any, p bool) { got, gotPanic = c, p })
	if got != any(boom) || gotPanic {
		t.Fatalf("a returned error must route with isPanic=false: got=%v isPanic=%v", got, gotPanic)
	}

	// A string panic → recovered (not a crash), isPanic=true.
	got, gotPanic = nil, false
	runOneTick(context.Background(), 0, func(context.Context) error { panic("kaboom") }, func(c any, p bool) { got, gotPanic = c, p })
	if got != "kaboom" || !gotPanic {
		t.Fatalf("a panic must route with isPanic=true: got=%v isPanic=%v", got, gotPanic)
	}

	// A RUNTIME panic (nil-map write) — its recovered value ITSELF implements error, so a
	// cause.(error) type switch would misroute it as a normal failure; the isPanic FLAG must
	// still be true. This is exactly the bug the flag exists to prevent.
	got, gotPanic = nil, false
	runOneTick(context.Background(), 0, func(context.Context) error {
		var m map[string]int
		m["x"] = 1 //nolint:staticcheck // intentional nil-map write to raise a runtime.Error (implements error)
		return nil
	}, func(c any, p bool) { got, gotPanic = c, p })
	if !gotPanic {
		t.Fatalf("a runtime panic must be flagged isPanic=true even though it implements error")
	}
	if _, ok := got.(error); !ok {
		t.Fatal("sanity: a runtime panic value implements error — which is WHY the explicit flag is needed")
	}

	// Success → onError is NOT called.
	called := false
	runOneTick(context.Background(), 0, func(context.Context) error { return nil }, func(any, bool) { called = true })
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
