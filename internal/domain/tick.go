package domain

import (
	"context"
	"time"
)

// TickLoop runs fn immediately and then every interval until ctx is cancelled — the one owner
// of the background ticker skeleton (the serve RPO/coverage samplers, the manifest loop, and
// the consumer's stale-claim reaper all use it instead of hand-rolling NewTicker + for/select).
//
// fn's returned error AND a recovered panic are BOTH routed to onError — so a caller declares
// its failure side-effect ONCE, not duplicated across an error branch and a separate panic
// handler. isPanic tells the two apart EXPLICITLY: cause is the returned `error` when
// isPanic=false and the recovered panic value when isPanic=true. This is a passed flag, NOT a
// `cause.(error)` type switch, because a recovered RUNTIME panic (nil deref, bounds, bad type
// assertion) itself implements error — a type switch would misroute it as a normal failure and
// drop its panic-only handling (e.g. a stack trace). A panic is recovered because these are
// background goroutines on the far side of a boundary no request/pipeline recover reaches — an
// unrecovered one would crash the whole process. onError may be nil (no-op).
//
// timeout>0 bounds each pass with a child ctx (derived from ctx, so shutdown still aborts a
// slow pass); timeout<=0 runs fn on ctx directly (the reaper, whose single UPDATE needs no
// per-tick bound).
func TickLoop(ctx context.Context, interval, timeout time.Duration, fn func(context.Context) error, onError func(cause any, isPanic bool)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		runOneTick(ctx, timeout, fn, onError)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func runOneTick(ctx context.Context, timeout time.Duration, fn func(context.Context) error, onError func(cause any, isPanic bool)) {
	fctx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		fctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// A panic unwinds fn BEFORE it can return an error, so recover routes it to the SAME
	// onError sink (isPanic=true) — else a pass that panics every tick would take down the
	// process (or, for a sampler, freeze its gauge stale-green with zero signal).
	defer func() {
		if r := recover(); r != nil && onError != nil {
			onError(r, true)
		}
	}()
	if err := fn(fctx); err != nil && onError != nil {
		onError(err, false)
	}
}
