package consumer

import (
	"bytes"
	"context"
	"crypto/sha3"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// --- test doubles ------------------------------------------------------------

// fakeOutbox is a configurable domain.Outbox that records the terminal action taken on each
// entry and lets a test program Claim / ReapStale behaviour (return values, errors, panics).
// Every method locks: Run drives Concurrency workers + the reaper concurrently, so the
// recorder must be race-clean. It is DISTINCT from consumer_test.go's recordingOutbox (which
// only serves the settle() table) — reused would have meant tangling two unrelated fakes.
type fakeOutbox struct {
	mu            sync.Mutex
	claimFn       func(n int) ([]domain.OutboxEntry, error)
	reapFn        func(ttl time.Duration) (int, error)
	failRet       func() (bool, error) // Fail's return (deadLettered, err); nil => (false, nil)
	errOn         map[string]error     // per-method error injection (markdone/defer/release/skip)
	markDone      int
	failed        int
	skipped       int
	deferred      int
	referencedRet func() (bool, error) // SourceStillReferenced's return; nil => (false, nil)
	failReasons   []string
}

func (o *fakeOutbox) Claim(_ context.Context, n int) ([]domain.OutboxEntry, error) {
	o.mu.Lock()
	fn := o.claimFn
	o.mu.Unlock()
	if fn != nil {
		return fn(n)
	}
	return nil, nil
}

func (o *fakeOutbox) MarkDone(context.Context, int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.markDone++
	return o.errOn["markdone"] // nil map read yields nil — no error unless injected
}

func (o *fakeOutbox) Defer(context.Context, int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deferred++
	return o.errOn["defer"]
}

func (o *fakeOutbox) SourceStillReferenced(context.Context, string) (bool, error) {
	o.mu.Lock()
	fn := o.referencedRet
	o.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return false, nil // default: not referenced → genuinely gone → Skip (prior behaviour)
}

func (o *fakeOutbox) Release(context.Context, int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.errOn["release"]
}

func (o *fakeOutbox) Probe(context.Context) error { return nil }

func (o *fakeOutbox) Fail(_ context.Context, _ int64, reason string) (bool, error) {
	o.mu.Lock()
	o.failed++
	o.failReasons = append(o.failReasons, reason)
	fn := o.failRet
	o.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return false, nil
}

func (o *fakeOutbox) Skip(context.Context, int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.skipped++
	return o.errOn["skip"]
}

func (o *fakeOutbox) ReapStale(_ context.Context, ttl time.Duration) (int, error) {
	o.mu.Lock()
	fn := o.reapFn
	o.mu.Unlock()
	if fn != nil {
		return fn(ttl)
	}
	return 0, nil
}

func (o *fakeOutbox) markDoneN() int { o.mu.Lock(); defer o.mu.Unlock(); return o.markDone }
func (o *fakeOutbox) failedN() int   { o.mu.Lock(); defer o.mu.Unlock(); return o.failed }
func (o *fakeOutbox) skippedN() int  { o.mu.Lock(); defer o.mu.Unlock(); return o.skipped }

func (o *fakeOutbox) reasons() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.failReasons...)
}

// fakeSource serves data (or fails with err) for the pipeline's FetchContent.
type fakeSource struct {
	data []byte
	err  error
}

func (s fakeSource) FetchContent(context.Context, domain.BackupItem) (io.ReadCloser, error) {
	if s.err != nil {
		return nil, s.err
	}
	return io.NopCloser(bytes.NewReader(s.data)), nil
}

// fakeLedger answers the pipeline's dedup query; storedFn can inject a panic or a custom set.
type fakeLedger struct {
	storedFn func(externalID string) (map[string]bool, error)
}

func (fakeLedger) RecordBackup(context.Context, domain.ObjectMeta, []domain.TargetStatus) error {
	return nil
}

func (f fakeLedger) StoredTargets(_ context.Context, id string) (map[string]bool, error) {
	if f.storedFn != nil {
		return f.storedFn(id)
	}
	return map[string]bool{}, nil
}

func (fakeLedger) Probe(context.Context) error { return nil }
func (fakeLedger) StoredObjectsPage(context.Context, string, string, int) ([]domain.ObjectMeta, error) {
	return nil, nil
}
func (fakeLedger) StoredExternalIDsPage(context.Context, string, string, int) ([]string, error) {
	return nil, nil
}
func (fakeLedger) TargetGaps(context.Context, []string, func(string, map[string]bool) error) error {
	return nil
}

// memSink is a minimal in-memory content-addressed Sink for a real pipeline.
type memSink struct {
	name  string
	mu    sync.Mutex
	store map[string][]byte
}

func newMemSink(name string) *memSink { return &memSink{name: name, store: map[string][]byte{}} }

func (m *memSink) Name() string { return m.name }

func (m *memSink) Store(_ context.Context, h string, r io.Reader) (int64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	m.store[h] = b
	m.mu.Unlock()
	return int64(len(b)), nil
}

func (m *memSink) get(h string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store[h]
}

func (*memSink) Exists(context.Context, string) (bool, error) { return false, nil }
func (*memSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	return nil, errors.New("memsink: fetch unused")
}
func (*memSink) PutManifest(context.Context, string, io.Reader) error { return nil }
func (*memSink) Preflight(context.Context) error                      { return nil }

// --- helpers -----------------------------------------------------------------

// sha3hex is the SHA3-256 externalID of b — the identity the VerifyReader checks against.
func sha3hex(b []byte) string {
	h := sha3.New256()
	_, _ = h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

// validHash is a format-valid (64 lowercase-hex) externalID for a label, for cases where the
// bytes are never streamed/verified (they only need to pass fsutil.ValidateContentHash).
func validHash(label string) string { return sha3hex([]byte(label)) }

// mustNotHang runs fn in a goroutine and fails the test if it doesn't return within d, so a
// regression that wedges a loop fails loudly instead of blocking the suite forever. fn MUST NOT
// call t.Fatal (wrong goroutine) — assert after mustNotHang returns (the done channel
// establishes happens-before for any value fn wrote).
func mustNotHang(t *testing.T, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("timed out after %v (hang / goroutine leak)", d)
	}
}

// waitFor polls cond until true or the deadline, failing the test on timeout.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", d)
		}
		time.Sleep(time.Millisecond)
	}
}

func observedLogger() (*zap.Logger, *observer.ObservedLogs) {
	core, logs := observer.New(zap.ErrorLevel)
	return zap.New(core), logs
}

// gonePipeline is a real pipeline whose source has vanished: BackupOne returns ErrSourceGone,
// so process()/settle() route the entry to Skip — a clean, panic-free way to exercise the loop
// machinery end to end without hand-computing a matching content hash.
func gonePipeline() *domain.Pipeline {
	return domain.NewPipeline(
		fakeSource{err: domain.ErrSourceGone},
		fakeLedger{},
		[]domain.Target{{Sink: newMemSink("t1"), Codec: domain.CodecNone}},
	)
}

// --- Run ---------------------------------------------------------------------

// TestRunDrainsOnCancel: a running consumer processes a claimed entry, then returns
// context.Canceled promptly on cancel with no stranded goroutine (guarded by a timeout so a
// hang fails rather than blocks). The single entry is processed exactly once (skip==1) — the
// per-object claim + cascade never double-processes a row.
func TestRunDrainsOnCancel(t *testing.T) {
	var served atomic.Bool
	entryHash := validHash("run-entry")
	ob := &fakeOutbox{
		claimFn: func(int) ([]domain.OutboxEntry, error) {
			if served.CompareAndSwap(false, true) {
				return []domain.OutboxEntry{{
					ID:         1,
					BackupItem: domain.BackupItem{ExternalID: entryHash},
				}}, nil
			}
			return nil, nil
		},
	}
	c := New(Deps{
		Outbox:           ob,
		Pipeline:         gonePipeline(),
		ListenPool:       nil, // listen() returns immediately
		Concurrency:      2,
		PollEvery:        5 * time.Millisecond,
		StaleTTL:         time.Millisecond,
		PerObjectTimeout: time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()

	waitFor(t, 2*time.Second, func() bool { return ob.skippedN() >= 1 })
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel (goroutine leak / hang)")
	}
	if got := ob.skippedN(); got != 1 {
		t.Fatalf("entry processed %d times (skip count), want exactly 1", got)
	}
}

// --- claimStep ---------------------------------------------------------------

// TestClaimStepEntryProcessed: a claim that returns a row processes it, signals a sibling
// worker (the drain cascade), and reports work done (true).
func TestClaimStepEntryProcessed(t *testing.T) {
	ob := &fakeOutbox{
		claimFn: func(int) ([]domain.OutboxEntry, error) {
			return []domain.OutboxEntry{{
				ID:         5,
				BackupItem: domain.BackupItem{ExternalID: validHash("claim-entry")},
			}}, nil
		},
	}
	c := New(Deps{Outbox: ob, Pipeline: gonePipeline(), PerObjectTimeout: time.Second})
	wake := make(chan struct{}, 1)

	var worked bool
	mustNotHang(t, 5*time.Second, func() { worked = c.claimStep(context.Background(), wake) })

	if !worked {
		t.Fatal("claimStep with a claimed row must return true")
	}
	if got := ob.skippedN(); got != 1 {
		t.Fatalf("claimed row processed %d times, want 1", got)
	}
	select {
	case <-wake:
	default:
		t.Fatal("a successful claim must cascade a wake to a sibling worker")
	}
}

// TestClaimStepEmptyReturnsFalse: an empty claim returns false (worker should wait) and does
// NOT spend a wake signal.
func TestClaimStepEmptyReturnsFalse(t *testing.T) {
	ob := &fakeOutbox{} // claimFn nil -> empty claim
	c := New(Deps{Outbox: ob})
	wake := make(chan struct{}, 1)

	var worked bool
	mustNotHang(t, 5*time.Second, func() { worked = c.claimStep(context.Background(), wake) })

	if worked {
		t.Fatal("an empty claim must return false")
	}
	select {
	case <-wake:
		t.Fatal("an empty claim must not signal wake")
	default:
	}
}

// TestClaimStepErrorBacksOffReturnsTrue: a genuine Claim error is logged, backs off, and
// returns true (retry). The cancelled ctx makes backoff return fast — asserted so a broken
// backoff (ignoring ctx) fails instead of adding ~1s.
func TestClaimStepErrorBacksOffReturnsTrue(t *testing.T) {
	ob := &fakeOutbox{
		claimFn: func(int) ([]domain.OutboxEntry, error) { return nil, errors.New("db down") },
	}
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, Logger: logger})
	ctx := cancelled(t) // backoff returns immediately
	wake := make(chan struct{}, 1)

	var worked bool
	start := time.Now()
	mustNotHang(t, 5*time.Second, func() { worked = c.claimStep(ctx, wake) })

	if !worked {
		t.Fatal("a Claim error must return true (retry after backoff)")
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("backoff ignored the cancelled ctx (took %v, want <500ms)", d)
	}
	if logs.FilterMessage("claim outbox").Len() == 0 {
		t.Fatal("a Claim error must be logged")
	}
}

// TestClaimStepShutdownCancelReturnsTrue: a Claim aborted by shutdown (context.Canceled, parent
// ctx cancelled) returns true WITHOUT logging an error — a graceful drain is not a fault.
func TestClaimStepShutdownCancelReturnsTrue(t *testing.T) {
	ob := &fakeOutbox{
		claimFn: func(int) ([]domain.OutboxEntry, error) { return nil, context.Canceled },
	}
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, Logger: logger})
	ctx := cancelled(t)
	wake := make(chan struct{}, 1)

	var worked bool
	mustNotHang(t, 5*time.Second, func() { worked = c.claimStep(ctx, wake) })

	if !worked {
		t.Fatal("a shutdown-cancelled Claim must return true")
	}
	if logs.FilterMessage("claim outbox").Len() != 0 {
		t.Fatal("a graceful shutdown cancel must not be logged as a Claim error")
	}
}

// TestClaimStepPanicRecovered: a panic inside Claim is recovered (never crashes the worker),
// logged, and reported as work done (true) so the loop retries after backoff.
func TestClaimStepPanicRecovered(t *testing.T) {
	ob := &fakeOutbox{
		claimFn: func(int) ([]domain.OutboxEntry, error) { panic("pgx scan drift") },
	}
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, Logger: logger})
	ctx := cancelled(t) // backoff returns fast
	wake := make(chan struct{}, 1)

	var worked bool
	mustNotHang(t, 5*time.Second, func() { worked = c.claimStep(ctx, wake) })

	if !worked {
		t.Fatal("a recovered Claim panic must return true")
	}
	if logs.FilterMessage("panic in claim").Len() == 0 {
		t.Fatal("a recovered Claim panic must be logged")
	}
}

// --- poll --------------------------------------------------------------------

// TestPollSignalsThenExits: poll wakes at least once (the immediate + tiny-interval tick) and
// exits on ctx cancel.
func TestPollSignalsThenExits(t *testing.T) {
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	c := New(Deps{PollEvery: time.Millisecond})

	done := make(chan struct{})
	go func() { defer close(done); c.poll(ctx, wake) }()

	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("poll never signalled wake")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll did not exit on ctx cancel")
	}
}

// --- reap --------------------------------------------------------------------

// TestReapFiresDeadLetterPerObject: a reap sweep that dead-letters N crash-loopers fires
// OnDeadLetter exactly N times, forwards StaleTTL to ReapStale, and exits on cancel.
func TestReapFiresDeadLetterPerObject(t *testing.T) {
	const dead = 3
	var gotTTL atomic.Int64
	ob := &fakeOutbox{
		reapFn: func(ttl time.Duration) (int, error) {
			gotTTL.Store(int64(ttl))
			return dead, nil
		},
	}
	fired := make(chan struct{}, 16)
	c := New(Deps{
		Outbox:       ob,
		StaleTTL:     time.Millisecond, // exercise the max(StaleTTL/4, 1m) interval floor
		OnDeadLetter: func() { fired <- struct{}{} },
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.reap(ctx) }()

	for i := 0; i < dead; i++ {
		select {
		case <-fired:
		case <-time.After(2 * time.Second):
			t.Fatalf("OnDeadLetter fired %d times, want %d", i, dead)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reap did not exit on ctx cancel")
	}

	if got := time.Duration(gotTTL.Load()); got != time.Millisecond {
		t.Fatalf("ReapStale ttl = %v, want %v", got, time.Millisecond)
	}
	if len(fired) != 0 {
		t.Fatalf("OnDeadLetter fired more than the %d dead-lettered", dead)
	}
}

// TestReapErrorLoggedNotFatal: a ReapStale error routes to onError (logged), the reaper keeps
// running, and it exits on ctx cancel.
func TestReapErrorLoggedNotFatal(t *testing.T) {
	ob := &fakeOutbox{
		reapFn: func(time.Duration) (int, error) { return 0, errors.New("reap query failed") },
	}
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, StaleTTL: time.Millisecond, Logger: logger})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.reap(ctx) }()

	waitFor(t, 2*time.Second, func() bool { return logs.FilterMessage("reap stale").Len() > 0 })

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reap did not exit on ctx cancel")
	}
}

// --- process -----------------------------------------------------------------

// TestProcessSuccessMarksDone: a pipeline that stores the object on every target marks the
// outbox row done and actually persists the verified bytes.
func TestProcessSuccessMarksDone(t *testing.T) {
	data := []byte("back this object up")
	h := sha3hex(data)
	sink := newMemSink("t1")
	p := domain.NewPipeline(fakeSource{data: data}, fakeLedger{},
		[]domain.Target{{Sink: sink, Codec: domain.CodecNone}})
	ob := &fakeOutbox{}
	c := New(Deps{Outbox: ob, Pipeline: p, PerObjectTimeout: 30 * time.Second})
	entry := domain.OutboxEntry{ID: 7, BackupItem: domain.BackupItem{ExternalID: h}}

	mustNotHang(t, 5*time.Second, func() { c.process(context.Background(), entry) })

	if got := ob.markDoneN(); got != 1 {
		t.Fatalf("MarkDone called %d times, want 1", got)
	}
	if got := ob.failedN(); got != 0 {
		t.Fatalf("Fail called %d times, want 0", got)
	}
	if !bytes.Equal(sink.get(h), data) {
		t.Fatal("the verified object bytes were not stored on the target")
	}
}

// TestProcessPanicFails: a panic in the pipeline is recovered (worker survives), the entry is
// failed with a panic-tagged reason, and the panic is logged.
func TestProcessPanicFails(t *testing.T) {
	led := fakeLedger{
		storedFn: func(string) (map[string]bool, error) { panic("scan drift in ledger") },
	}
	p := domain.NewPipeline(fakeSource{data: []byte("x")}, led,
		[]domain.Target{{Sink: newMemSink("t1"), Codec: domain.CodecNone}})
	ob := &fakeOutbox{}
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, Pipeline: p, PerObjectTimeout: 30 * time.Second, Logger: logger})
	entry := domain.OutboxEntry{ID: 8, BackupItem: domain.BackupItem{ExternalID: validHash("poison")}}

	mustNotHang(t, 5*time.Second, func() { c.process(context.Background(), entry) })

	if got := ob.failedN(); got != 1 {
		t.Fatalf("Fail called %d times, want 1", got)
	}
	reasons := ob.reasons()
	if len(reasons) != 1 || !strings.HasPrefix(reasons[0], "panic:") {
		t.Fatalf("fail reason = %v, want a single panic-tagged reason", reasons)
	}
	if logs.FilterMessage("panic backing up object").Len() == 0 {
		t.Fatal("a recovered pipeline panic must be logged")
	}
}

// --- opCtx -------------------------------------------------------------------

// TestOpCtxBoundsWithDBTimeout: DBTimeout>0 bounds a claim/reap op with a derived deadline, so
// a wedged Alkemio DB can't park a worker forever.
func TestOpCtxBoundsWithDBTimeout(t *testing.T) {
	c := New(Deps{DBTimeout: 50 * time.Millisecond})
	ctx, cancel := c.opCtx(context.Background())
	defer cancel()

	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("DBTimeout>0 must bound the op with a deadline")
	}
	if until := time.Until(dl); until <= 0 || until > 100*time.Millisecond {
		t.Fatalf("deadline in %v, want ~50ms", until)
	}
}

// TestOpCtxUnboundedWhenZero: DBTimeout<=0 leaves the op unbounded (direct-construction path).
func TestOpCtxUnboundedWhenZero(t *testing.T) {
	c := New(Deps{}) // DBTimeout 0
	ctx, cancel := c.opCtx(context.Background())
	defer cancel()

	if _, ok := ctx.Deadline(); ok {
		t.Fatal("DBTimeout<=0 must leave the op ctx unbounded")
	}
}

// --- fail --------------------------------------------------------------------

// TestFailDeadLetterFiresObserver: crossing the attempt limit (Fail reports deadLettered=true)
// logs and fires the dead-letter observer once.
func TestFailDeadLetterFiresObserver(t *testing.T) {
	ob := &fakeOutbox{failRet: func() (bool, error) { return true, nil }}
	var dl atomic.Int32
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, Logger: logger, OnDeadLetter: func() { dl.Add(1) }})

	mustNotHang(t, 2*time.Second, func() { c.fail(context.Background(), 42, "too many attempts") })

	if got := dl.Load(); got != 1 {
		t.Fatalf("OnDeadLetter fired %d times, want 1", got)
	}
	if logs.FilterMessage("dead-lettered").Len() == 0 {
		t.Fatal("a dead-letter must be logged")
	}
}

// TestFailErrorLoggedNoObserver: a Fail write error is logged and short-circuits — the
// dead-letter observer must NOT fire on an unknown outcome.
func TestFailErrorLoggedNoObserver(t *testing.T) {
	ob := &fakeOutbox{failRet: func() (bool, error) { return false, errors.New("update failed") }}
	var dl atomic.Int32
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, Logger: logger, OnDeadLetter: func() { dl.Add(1) }})

	mustNotHang(t, 2*time.Second, func() { c.fail(context.Background(), 42, "boom") })

	if got := dl.Load(); got != 0 {
		t.Fatalf("OnDeadLetter fired %d times on a Fail error, want 0", got)
	}
	if logs.FilterMessage("mark fail").Len() == 0 {
		t.Fatal("a Fail write error must be logged")
	}
}

// --- reap panic --------------------------------------------------------------

// TestReapPanicRecovered: a panic inside a reap sweep is recovered and logged (never crashes
// the background reaper goroutine), and the reaper still exits on ctx cancel.
func TestReapPanicRecovered(t *testing.T) {
	ob := &fakeOutbox{reapFn: func(time.Duration) (int, error) { panic("scan drift in reaper") }}
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, StaleTTL: time.Millisecond, Logger: logger})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.reap(ctx) }()

	waitFor(t, 2*time.Second, func() bool { return logs.FilterMessage("panic in reaper").Len() > 0 })

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reap did not exit on ctx cancel after a recovered panic")
	}
}

// TestReapSuppressesShutdownCancel: a ReapStale that returns context.Canceled (the sweep was
// aborted by graceful shutdown) is NOT a fault — it must not be logged as a reap error.
func TestReapSuppressesShutdownCancel(t *testing.T) {
	called := make(chan struct{}, 4)
	ob := &fakeOutbox{
		reapFn: func(time.Duration) (int, error) { called <- struct{}{}; return 0, context.Canceled },
	}
	logger, logs := observedLogger()
	c := New(Deps{Outbox: ob, StaleTTL: time.Millisecond, Logger: logger})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.reap(ctx) }()

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("reap never ran a sweep")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reap did not exit on ctx cancel")
	}
	if logs.FilterMessage("reap stale").Len() != 0 {
		t.Fatal("a context.Canceled from ReapStale (graceful shutdown) must not be logged as an error")
	}
}

// --- poll floor --------------------------------------------------------------

// TestPollFloorsNonPositiveInterval: a non-positive PollEvery is floored (can't panic
// NewTicker); the immediate startup tick still wakes.
func TestPollFloorsNonPositiveInterval(t *testing.T) {
	wake := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	c := New(Deps{PollEvery: 0}) // floored to 1s; the immediate tick wakes before the ticker

	done := make(chan struct{})
	go func() { defer close(done); c.poll(ctx, wake) }()

	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("poll must signal an immediate wake even with a non-positive interval")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll did not exit on ctx cancel")
	}
}

// --- settle bookkeeping errors -----------------------------------------------

// TestSettleLogsBookkeepingErrors: each settle branch logs (never silently swallows) a failed
// outbox write, so a stranded row is visible in the logs rather than an invisible drop.
func TestSettleLogsBookkeepingErrors(t *testing.T) {
	bg := context.Background()
	boom := errors.New("db write failed")
	cases := []struct {
		name         string
		errKey       string
		ctx, objCtx  context.Context
		ok, deferred bool
		err          error
		wantLog      string
	}{
		{"mark-done", "markdone", bg, bg, true, false, nil, "mark done"},
		{"release", "release", cancelled(t), cancelled(t), false, false, boom, "release claim on shutdown"},
		{"skip", "skip", bg, bg, false, false, domain.ErrSourceGone, "skip vanished source"},
		{"defer", "defer", bg, bg, false, true, nil, "defer down-target object"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ob := &fakeOutbox{errOn: map[string]error{tc.errKey: boom}}
			logger, logs := observedLogger()
			c := New(Deps{Outbox: ob, Logger: logger})

			c.settle(tc.ctx, tc.objCtx, bg, domain.OutboxEntry{ID: 1}, tc.ok, tc.deferred, tc.err)

			if logs.FilterMessage(tc.wantLog).Len() == 0 {
				t.Fatalf("a bookkeeping error on the %q path must be logged (%q)", tc.name, tc.wantLog)
			}
		})
	}
}

// --- backoff -----------------------------------------------------------------

// TestBackoffReturnsFastOnCancel: backoff returns immediately when ctx is already cancelled,
// rather than sleeping ~1s.
func TestBackoffReturnsFastOnCancel(t *testing.T) {
	ctx := cancelled(t)
	start := time.Now()
	mustNotHang(t, 2*time.Second, func() { backoff(ctx) })
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("backoff on a cancelled ctx returned in %v, want <500ms", d)
	}
}
