package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// recordingOutbox records which settle action fired, so a settle test can assert the state
// machine routed each (ok, deferred, err, ctx) combination to the right outbox call. The
// adapter layer had no tests; settle is its most load-bearing logic (a mis-ordered case
// turns a down-target defer into a Fail that marches the corpus to dead-letter — the T017a
// failure V6 was a variant of).
type recordingOutbox struct {
	action     string
	failReason string
	referenced bool // SourceStillReferenced's return (default false → genuinely gone → Skip)
}

func (o *recordingOutbox) Claim(context.Context, int) ([]domain.OutboxEntry, error) { return nil, nil }
func (o *recordingOutbox) MarkDone(context.Context, int64) error                    { o.action = "done"; return nil }
func (o *recordingOutbox) Defer(context.Context, int64) error                       { o.action = "defer"; return nil }
func (o *recordingOutbox) Fail(_ context.Context, _ int64, reason string) (bool, error) {
	o.action, o.failReason = "fail", reason
	return false, nil
}
func (o *recordingOutbox) ReapStale(context.Context, time.Duration) (int, error) { return 0, nil }
func (o *recordingOutbox) Release(context.Context, int64) error                  { o.action = "release"; return nil }
func (o *recordingOutbox) Skip(context.Context, int64) error                     { o.action = "skip"; return nil }
func (o *recordingOutbox) SourceStillReferenced(context.Context, string) (bool, error) {
	return o.referenced, nil
}
func (o *recordingOutbox) Probe(context.Context) error { return nil }

// expiredDeadline returns a context already past its deadline (Err()==DeadlineExceeded) —
// the per-object-timeout signal, without sleeping.
func expiredDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	t.Cleanup(cancel)
	return ctx
}

func cancelled(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	t.Cleanup(func() {})
	return ctx
}

func TestSettleRoutesEachOutcome(t *testing.T) {
	bg := context.Background()
	errFail := errors.New("a target failed")

	cases := []struct {
		name         string
		ctx, objCtx  context.Context
		ok, deferred bool
		referenced   bool // corpus still references the hash → a 404 means source-unavailable, not gone
		err          error
		wantAction   string
		wantTimeout  bool // OnObjectTimeout fired
		wantGone     bool // OnSourceGone fired
	}{
		{name: "done", ctx: bg, objCtx: bg, ok: true, wantAction: "done"},
		// Shutdown (parent ctx cancelled) takes precedence over a failure err: Release, no attempt.
		{name: "shutdown-release", ctx: cancelled(t), objCtx: cancelled(t), err: errFail, wantAction: "release"},
		// 404 AND the corpus no longer references the hash → genuinely gone → Skip + metric.
		{name: "source-gone-skip", ctx: bg, objCtx: bg, err: domain.ErrSourceGone, wantAction: "skip", wantGone: true},
		// 404 BUT the corpus still references the hash → the source is unavailable, not gone →
		// Defer (no attempt burn), and the source-gone metric must NOT fire (it isn't gone).
		{name: "source-unavailable-defer", ctx: bg, objCtx: bg, err: domain.ErrSourceGone, referenced: true, wantAction: "defer"},
		// A plain defer (circuit-open target, no timeout): Defer, and the timeout metric must NOT fire.
		{name: "defer-no-timeout", ctx: bg, objCtx: bg, deferred: true, wantAction: "defer"},
		// A per-object timeout that tripped the circuit surfaces as a defer — still fire the metric (V5).
		{name: "defer-with-timeout", ctx: bg, objCtx: expiredDeadline(t), deferred: true, wantAction: "defer", wantTimeout: true},
		// A per-object timeout that failed a reachable target: Fail + timeout metric.
		{name: "fail-with-timeout", ctx: bg, objCtx: expiredDeadline(t), err: errFail, wantAction: "fail", wantTimeout: true},
		// A genuine non-timeout failure: Fail, no timeout metric.
		{name: "fail-plain", ctx: bg, objCtx: bg, err: errFail, wantAction: "fail"},
		// Not-ok with no error (a reachable target didn't store): Fail with the default reason.
		{name: "fail-not-ok", ctx: bg, objCtx: bg, wantAction: "fail"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ob := &recordingOutbox{referenced: tc.referenced}
			var timeoutFired, goneFired bool
			c := New(Deps{
				Outbox:          ob,
				OnObjectTimeout: func() { timeoutFired = true },
				OnSourceGone:    func() { goneFired = true },
			})
			c.settle(tc.ctx, tc.objCtx, bg, domain.OutboxEntry{ID: 1}, tc.ok, tc.deferred, tc.err)

			if ob.action != tc.wantAction {
				t.Fatalf("action = %q, want %q", ob.action, tc.wantAction)
			}
			if timeoutFired != tc.wantTimeout {
				t.Fatalf("OnObjectTimeout fired = %v, want %v", timeoutFired, tc.wantTimeout)
			}
			if goneFired != tc.wantGone {
				t.Fatalf("OnSourceGone fired = %v, want %v", goneFired, tc.wantGone)
			}
		})
	}
}
