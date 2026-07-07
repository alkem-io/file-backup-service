package domain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// sum is a test-only convenience: the lowercase-hex SHA3-256 of r (an externalID). The
// production paths hash through VerifyReader/copyVerify/newHash, so this lives with the tests.
func sum(r io.Reader) (string, error) {
	h := newHash()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hexSum(h), nil
}

// hashOf returns a deterministic, format-valid 64-hex externalID for a label — the source/
// sink fakes ignore the hash's CONTENT (they key on it as an opaque id), they just need it
// to pass the content-address format gate (fsutil.ValidateContentHash) that the production
// ingress now enforces. Use it anywhere a test needs a stand-in externalID that is NOT the
// hash of specific bytes.
func hashOf(label string) string {
	h := newHash()
	_, _ = h.Write([]byte(label))
	return hexSum(h)
}

type fakeSource struct{ data []byte }

func (f fakeSource) FetchContent(context.Context, BackupItem) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

// fakeLedger enforces the real FK invariant: a target status can only be written
// after the object row exists (file_backup_target_status REFERENCES file_backup_object).
type fakeLedger struct {
	mu       sync.Mutex // concurrent backfill/reconcile workers share one ledger, like the real pgx pool
	objects  map[string]bool
	sizes    map[string]int64  // externalID -> recorded object size
	states   map[string]string // externalID+"/"+target -> last state
	statuses int
}

func newFakeLedger() *fakeLedger {
	return &fakeLedger{objects: map[string]bool{}, sizes: map[string]int64{}, states: map[string]string{}}
}

func (f *fakeLedger) RecordBackup(_ context.Context, m ObjectMeta, statuses []TargetStatus) error {
	// The real CTE writes the object (FK parent) before the statuses atomically;
	// the fake records both together. Mirror the size no-downgrade rule: a first
	// insert sets it, a later write only overwrites when the size is verified.
	f.mu.Lock()
	defer f.mu.Unlock()
	_, existed := f.objects[m.ExternalID]
	f.objects[m.ExternalID] = true
	if !existed || m.SizeVerified {
		f.sizes[m.ExternalID] = m.Size
	}
	for _, s := range statuses {
		// Mirror the production CTE's no-downgrade: a durable 'stored' is never
		// overwritten by a later 'failed'.
		key := m.ExternalID + "/" + s.Target
		if f.states[key] == StateStored && s.State != StateStored {
			f.statuses++
			continue
		}
		f.states[key] = s.State
		f.statuses++
	}
	return nil
}
func (f *fakeLedger) StoredTargets(_ context.Context, externalID string) (map[string]bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]bool{}
	for k, v := range f.states {
		if v == StateStored && strings.HasPrefix(k, externalID+"/") {
			out[strings.TrimPrefix(k, externalID+"/")] = true
		}
	}
	return out, nil
}
func (f *fakeLedger) Probe(context.Context) error { return nil }
func (f *fakeLedger) StoredObjectsPage(_ context.Context, target, after string, limit int) ([]ObjectMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.objects))
	for id := range f.objects {
		if f.states[id+"/"+target] == StateStored && id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := make([]ObjectMeta, 0, limit)
	for _, id := range ids {
		if len(out) >= limit {
			break
		}
		out = append(out, ObjectMeta{ExternalID: id, Size: f.sizes[id]})
	}
	return out, nil
}
func (f *fakeLedger) StoredExternalIDsPage(_ context.Context, target, after string, limit int) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.objects))
	for id := range f.objects {
		if f.states[id+"/"+target] == StateStored && id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	return ids, nil
}
func (f *fakeLedger) TargetGaps(_ context.Context, allTargets []string, fn func(string, map[string]bool) error) error {
	// Snapshot the gaps under the lock, then invoke fn WITHOUT holding it: fn dispatches
	// concurrent repair workers that call RecordBackup (which locks), and when the worker
	// semaphore is full, yield blocks until a worker finishes — a worker that would itself
	// deadlock waiting on a lock still held here. Mirrors the real driver: the gap cursor is
	// a separate connection from the workers' writes.
	type gap struct {
		id     string
		stored map[string]bool
	}
	f.mu.Lock()
	var gaps []gap
	for id := range f.objects {
		stored := map[string]bool{}
		for _, t := range allTargets {
			if f.states[id+"/"+t] == StateStored {
				stored[t] = true
			}
		}
		if len(stored) < len(allTargets) {
			gaps = append(gaps, gap{id, stored})
		}
	}
	f.mu.Unlock()
	for _, g := range gaps {
		if err := fn(g.id, g.stored); err != nil {
			return err
		}
	}
	return nil
}

// stubSink is a no-op Sink base: embed it and override only the method a fake needs,
// so a new port method is one edit here, not one per fake.
type stubSink struct{ name string }

func (s stubSink) Name() string                                          { return s.name }
func (stubSink) Exists(context.Context, string) (bool, error)            { return false, nil }
func (stubSink) Store(context.Context, string, io.Reader) (int64, error) { return 0, nil }
func (stubSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("stub: no fetch")
}
func (stubSink) PutManifest(context.Context, string, io.Reader) error { return nil }
func (stubSink) Preflight(context.Context) error                      { return nil }

type memSink struct {
	stubSink
	mu    sync.Mutex // concurrent backfill/reconcile workers fan out to one target, like the real stateless sink
	store map[string][]byte
}

func newMemSink(name string) *memSink {
	return &memSink{stubSink: stubSink{name: name}, store: map[string][]byte{}}
}
func (m *memSink) Exists(_ context.Context, h string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.store[h]
	return ok, nil
}
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
func (m *memSink) Fetch(_ context.Context, h string) (io.ReadCloser, error) {
	m.mu.Lock()
	b := m.store[h]
	m.mu.Unlock()
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (m *memSink) PutManifest(_ context.Context, name string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.store["_manifest/"+name] = b
	m.mu.Unlock()
	return nil
}

// hangingSink blocks in Fetch/Exists IGNORING ctx — models a filesystem sink on a wedged
// mount (os.Open/os.Stat stuck in uninterruptible kernel sleep). It proves the read/probe
// abandonment wrappers (callWithCtx in decodeStream, existsWithCtx in audit) actually return
// at the ctx deadline rather than hanging. release is closed at test end to free the goroutine.
type hangingSink struct {
	stubSink
	release chan struct{}
}

func newHangingSink(name string) *hangingSink {
	return &hangingSink{stubSink: stubSink{name: name}, release: make(chan struct{})}
}
func (h *hangingSink) Fetch(_ context.Context, _ string) (io.ReadCloser, error) {
	<-h.release // block ignoring ctx, like a wedged os.Open
	return nil, errors.New("released")
}
func (h *hangingSink) Exists(_ context.Context, _ string) (bool, error) {
	<-h.release // block ignoring ctx, like a wedged os.Stat
	return false, errors.New("released")
}

func TestPipelineBackupOne(t *testing.T) {
	data := []byte("back me up")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("t1")
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: sink, Codec: CodecNone}})

	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err != nil || !ok {
		t.Fatalf("backup: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(sink.store[h], data) {
		t.Fatal("stored bytes mismatch")
	}
	if !led.objects[h] || led.statuses != 1 {
		t.Fatalf("ledger not updated: %+v", led)
	}
}

// truncatedReader yields all of data, then io.ErrUnexpectedEOF instead of io.EOF —
// exactly what net/http returns when a response body is shorter than its
// Content-Length (a mid-stream connection drop).
type truncatedReader struct {
	data []byte
	pos  int
}

func (r *truncatedReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
func (r *truncatedReader) Close() error { return nil }

type truncatedSource struct{ served []byte }

func (s truncatedSource) FetchContent(context.Context, BackupItem) (io.ReadCloser, error) {
	return &truncatedReader{data: s.served}, nil
}

// TestPipelineTruncatedSourceNotCommitted: a source that ends in io.ErrUnexpectedEOF
// (short read) must fail the hash gate, not be silently committed as a verified backup.
func TestPipelineTruncatedSourceNotCommitted(t *testing.T) {
	full := bytes.Repeat([]byte("truncate me "), 500)
	h, err := sum(bytes.NewReader(full))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("t")
	p := NewPipeline(truncatedSource{full[:3000]}, newFakeLedger(), []Target{{Sink: sink, Codec: CodecNone}})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err == nil || ok {
		t.Fatal("a truncated source (io.ErrUnexpectedEOF) must fail, not be recorded as a verified backup")
	}
	if len(sink.store) != 0 {
		t.Fatal("truncated bytes must not be committed to the sink")
	}
}

// ctxErrSource models a fetch aborted by ctx cancellation (SIGTERM/timeout in the
// dial/headers window) — FetchContent returns before any bytes.
type ctxErrSource struct{}

func (ctxErrSource) FetchContent(context.Context, BackupItem) (io.ReadCloser, error) {
	return nil, context.Canceled
}

// TestPipelineFetchCancelledNoPanic: a fetch that fails because ctx was cancelled
// (nil results, ctx.Err()!=nil) must return the error, not panic on a nil results slice.
func TestPipelineFetchCancelledNoPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sink := newMemSink("t")
	p := NewPipeline(ctxErrSource{}, newFakeLedger(), []Target{{Sink: sink, Codec: CodecNone}})
	ok, _, err := p.BackupOne(ctx, BackupItem{ExternalID: hashOf("abc")})
	if err == nil || ok {
		t.Fatal("a ctx-cancelled fetch must return an error, not succeed (and must not panic)")
	}
}

// TestBackupRejectsOverCapObject: an object larger than the end-to-end cap must FAIL the
// backup (nothing committed), not be stored on every target and then be unrestorable.
func TestBackupRejectsOverCapObject(t *testing.T) {
	old := maxObjectBytes
	maxObjectBytes = 100
	defer func() { maxObjectBytes = old }()

	data := bytes.Repeat([]byte("x"), 500) // > the 100-byte cap
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("t")
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{{Sink: sink, Codec: CodecNone}})
	ok, deferred, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err == nil || ok || deferred {
		t.Fatalf("an over-cap object must fail: ok=%v deferred=%v err=%v", ok, deferred, err)
	}
	if len(sink.store) != 0 {
		t.Fatal("an over-cap object must NOT be committed to any target")
	}
}

func TestPipelineSourceCorrupt(t *testing.T) {
	sink := newMemSink("t1")
	p := NewPipeline(fakeSource{[]byte("wrong")}, newFakeLedger(), []Target{{Sink: sink}})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: hashOf("deadbeef")})
	if err == nil {
		t.Fatal("expected the source integrity error to be surfaced, not hidden as a target failure")
	}
	if ok {
		t.Fatal("corrupt source must not report success")
	}
	if len(sink.store) != 0 {
		t.Fatal("corrupt object must not be committed to the sink")
	}
}

type countingSource struct {
	data  []byte
	fetch int
}

func (c *countingSource) FetchContent(context.Context, BackupItem) (io.ReadCloser, error) {
	c.fetch++
	return io.NopCloser(bytes.NewReader(c.data)), nil
}

// TestPipelineSingleFetchFanOut is the point of the fan-out design: N targets
// must not multiply reads on the source (the primary store).
func TestPipelineSingleFetchFanOut(t *testing.T) {
	data := bytes.Repeat([]byte("payload "), 100)
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	src := &countingSource{data: data}
	a := newMemSink("a")
	b := newMemSink("b")
	p := NewPipeline(src, newFakeLedger(), []Target{
		{Sink: a, Codec: CodecNone},
		{Sink: b, Codec: CodecNone},
	})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err != nil || !ok {
		t.Fatalf("backup: ok=%v err=%v", ok, err)
	}
	if src.fetch != 1 {
		t.Fatalf("expected exactly 1 source fetch for 2 targets, got %d", src.fetch)
	}
	if !bytes.Equal(a.store[h], data) || !bytes.Equal(b.store[h], data) {
		t.Fatal("both targets must receive the full object from one fetch")
	}
}

// TestPipelineZstdTarget exercises the zstd fan-out branch: the target receives
// compressed bytes that decompress back to the plaintext.
func TestPipelineZstdTarget(t *testing.T) {
	data := bytes.Repeat([]byte("zstd fan-out payload "), 100)
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("z")
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{{Sink: sink, Codec: CodecZstd}})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err != nil || !ok {
		t.Fatalf("backup: ok=%v err=%v", ok, err)
	}
	if bytes.Equal(sink.store[h], data) {
		t.Fatal("expected compressed bytes, got plaintext")
	}
	dec, err := zstd.NewReader(bytes.NewReader(sink.store[h]))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	out, err := io.ReadAll(dec)
	if err != nil || !bytes.Equal(out, data) {
		t.Fatalf("zstd target round-trip mismatch: %v", err)
	}
}

// TestPipelineLargeObjectMultiChunk exercises fanoutCopy's multi-chunk aggregation
// (>1 MiB → several io.ReadFull passes) fanned concurrently to two targets.
func TestPipelineLargeObjectMultiChunk(t *testing.T) {
	data := bytes.Repeat([]byte("large fan-out payload "), 130*1024) // ~2.7 MiB
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	b := newMemSink("b")
	p := NewPipeline(fakeSource{data}, newFakeLedger(),
		[]Target{{Sink: a, Codec: CodecNone}, {Sink: b, Codec: CodecZstd}})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h, Size: int64(len(data))})
	if err != nil || !ok {
		t.Fatalf("large multi-chunk fan-out: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(a.store[h], data) {
		t.Fatal("raw target mismatch")
	}
	dec, _ := zstd.NewReader(bytes.NewReader(b.store[h]))
	defer dec.Close()
	out, _ := io.ReadAll(dec)
	if !bytes.Equal(out, data) {
		t.Fatal("zstd target mismatch")
	}
}

// nonConsumingSink reports success without ever reading its stream — models a
// misbehaving sink that must NOT be recorded as stored. The stub's Store is exactly
// that (returns 0,nil consuming nothing), so no override is needed.
type nonConsumingSink struct{ stubSink }

// TestPipelineNonConsumingSinkFails: a sink that returns success without reading
// the verified stream must be forced to failed (dead-pipe cross-check), so the
// object is not marked done, while the healthy target still stores.
func TestPipelineNonConsumingSinkFails(t *testing.T) {
	data := []byte("hello dead pipe")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	good := newMemSink("good")
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{
		{Sink: &nonConsumingSink{stubSink{name: "bad"}}, Codec: CodecNone},
		{Sink: good, Codec: CodecNone},
	})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatal("a sink that reported success without consuming the stream must not make the object done")
	}
	if !bytes.Equal(good.store[h], data) {
		t.Fatal("healthy target should still store the object")
	}
}

// TestPipelineNonConsumingZstdSinkFails: the same guard must hold through the zstd
// codec, where the encoder drains the source pipe regardless of whether the sink
// reads its output — so pipe-liveness alone can't catch it; the EOF-consumption
// check must.
func TestPipelineNonConsumingZstdSinkFails(t *testing.T) {
	data := bytes.Repeat([]byte("compress me "), 8) // small: fits the encoder's buffer
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{
		{Sink: &nonConsumingSink{stubSink{name: "bad-zstd"}}, Codec: CodecZstd},
	})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatal("a non-consuming zstd sink must not make the object done")
	}
	if led.states[h+"/bad-zstd"] == StateStored {
		t.Fatal("a non-consuming zstd sink must never be recorded as stored")
	}
}

// TestPipelineAllTargetsFailRecorded: when the ONLY target fails (the single-target
// launch config), the object is not-done but the failure is a per-target failure —
// it must be recorded as a 'failed' target_status, NOT masquerade as a source error.
func TestPipelineAllTargetsFailRecorded(t *testing.T) {
	data := []byte("all targets down")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: &failSink{stubSink{name: "down"}}, Codec: CodecNone}})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err != nil {
		t.Fatalf("an all-targets-down failure must NOT be reported as a source error: %v", err)
	}
	if ok {
		t.Fatal("object must not be done when its only target failed")
	}
	if led.states[h+"/down"] != "failed" {
		t.Fatalf("expected a 'failed' target_status breadcrumb, got %q", led.states[h+"/down"])
	}
}

// TestPipelineAllTargetsFailRecordsOutboxSize guards the regression where an
// all-targets-dead backup recorded a PARTIAL vr.Total (bytes read before the pipes
// died) as the object size; instead it falls back to the outbox size with
// SizeVerified=false. The object must exceed one io.Copy buffer so a partial read is
// distinguishable.
func TestPipelineAllTargetsFailRecordsOutboxSize(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 200*1024) // > 32 KiB io.Copy buffer
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: &failSink{stubSink{name: "down"}}, Codec: CodecNone}})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h, Size: int64(len(data))})
	if err != nil || ok {
		t.Fatalf("all-targets-fail: ok=%v err=%v", ok, err)
	}
	if led.sizes[h] != int64(len(data)) {
		t.Fatalf("all-fail must record the outbox size %d, not a partial %d", len(data), led.sizes[h])
	}
}

// TestBackupRejectsMalformedExternalID: a hostile/drifted externalID (traversal payload,
// wrong length, non-hex) must be rejected at the pipeline ingress — never fetched, never
// handed to a sink as a path component — so it can't drive a directory-traversal write or
// an over-length ledger INSERT. (V1)
func TestBackupRejectsMalformedExternalID(t *testing.T) {
	sink := newMemSink("t")
	p := NewPipeline(fakeSource{[]byte("x")}, newFakeLedger(), []Target{{Sink: sink, Codec: CodecNone}})
	for _, bad := range []string{"", "abc", "../../../../etc/passwd",
		strings.Repeat("a", 63), strings.Repeat("A", 64), strings.Repeat("a", 65), strings.Repeat("g", 64)} {
		if _, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: bad}); err == nil {
			t.Fatalf("malformed externalID %q must be rejected", bad)
		}
	}
	if len(sink.store) != 0 {
		t.Fatal("a malformed externalID must never reach a sink")
	}
}

// TestBackupRejectsZeroTargets: a pipeline with no targets must fail loudly, never report an
// object done while storing nothing (the silent-total-loss guard the config check backstops). (V2)
func TestBackupRejectsZeroTargets(t *testing.T) {
	p := NewPipeline(fakeSource{[]byte("x")}, newFakeLedger(), nil)
	done, deferred, err := p.BackupOne(context.Background(), BackupItem{ExternalID: hashOf("x")})
	if err == nil || done || deferred {
		t.Fatalf("zero-target pipeline must error, got done=%v deferred=%v err=%v", done, deferred, err)
	}
}

// goneSource models a source object deleted before backup (404 → ErrSourceGone).
type goneSource struct{}

func (goneSource) FetchContent(context.Context, BackupItem) (io.ReadCloser, error) {
	return nil, fmt.Errorf("file-service GET: %w", ErrSourceGone)
}

// TestPipelineSourceGonePropagates: a vanished source must surface ErrSourceGone
// so the consumer can mark the entry 'skipped' instead of retrying it.
func TestPipelineSourceGonePropagates(t *testing.T) {
	p := NewPipeline(goneSource{}, newFakeLedger(),
		[]Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	_, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: hashOf("abc")})
	if !errors.Is(err, ErrSourceGone) {
		t.Fatalf("expected ErrSourceGone to propagate, got %v", err)
	}
}

type failSink struct{ stubSink }

func (f *failSink) Store(context.Context, string, io.Reader) (int64, error) {
	return 0, fmt.Errorf("sink down")
}

// TestPipelineTargetIsolation: targets are symmetric — a flaky target must not
// abort the others (they still receive the object), and the object must NOT be
// "done" until every target has it.
func TestPipelineTargetIsolation(t *testing.T) {
	data := []byte("hello isolation")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	good := newMemSink("good")
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{
		{Sink: &failSink{stubSink{name: "bad"}}, Codec: CodecNone},
		{Sink: good, Codec: CodecNone},
	})
	ok, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatal("one target failed — object must be not-done (retried), not marked backed-up")
	}
	if !bytes.Equal(good.store[h], data) {
		t.Fatal("healthy target must still receive the full object despite the flaky one")
	}
}
