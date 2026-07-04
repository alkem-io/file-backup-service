package domain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

type fakeSource struct{ data []byte }

func (f fakeSource) FetchContent(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

// fakeLedger enforces the real FK invariant: a target status can only be written
// after the object row exists (file_backup_target_status REFERENCES file_backup_object).
type fakeLedger struct {
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
	_, existed := f.objects[m.ExternalID]
	f.objects[m.ExternalID] = true
	if !existed || m.SizeVerified {
		f.sizes[m.ExternalID] = m.Size
	}
	for _, s := range statuses {
		f.states[m.ExternalID+"/"+s.Target] = s.State
		f.statuses++
	}
	return nil
}
func (f *fakeLedger) StoredTargets(context.Context, string) (map[string]bool, error) {
	return map[string]bool{}, nil
}
func (f *fakeLedger) Probe(context.Context) error { return nil }

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
	store map[string][]byte
}

func newMemSink(name string) *memSink {
	return &memSink{stubSink: stubSink{name: name}, store: map[string][]byte{}}
}
func (m *memSink) Exists(_ context.Context, h string) (bool, error) {
	_, ok := m.store[h]
	return ok, nil
}
func (m *memSink) Store(_ context.Context, h string, r io.Reader) (int64, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	m.store[h] = b
	return int64(len(b)), nil
}
func (m *memSink) Fetch(_ context.Context, h string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.store[h])), nil
}

func TestPipelineBackupOne(t *testing.T) {
	data := []byte("back me up")
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("t1")
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: sink, Codec: CodecNone}})

	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f1", ExternalID: h})
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

func (s truncatedSource) FetchContent(context.Context, string) (io.ReadCloser, error) {
	return &truncatedReader{data: s.served}, nil
}

// TestPipelineTruncatedSourceNotCommitted: a source that ends in io.ErrUnexpectedEOF
// (short read) must fail the hash gate, not be silently committed as a verified backup.
func TestPipelineTruncatedSourceNotCommitted(t *testing.T) {
	full := bytes.Repeat([]byte("truncate me "), 500)
	h, err := Sum(bytes.NewReader(full))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("t")
	p := NewPipeline(truncatedSource{full[:3000]}, newFakeLedger(), []Target{{Sink: sink, Codec: CodecNone}})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: h})
	if err == nil || ok {
		t.Fatal("a truncated source (io.ErrUnexpectedEOF) must fail, not be recorded as a verified backup")
	}
	if len(sink.store) != 0 {
		t.Fatal("truncated bytes must not be committed to the sink")
	}
}

func TestPipelineSourceCorrupt(t *testing.T) {
	sink := newMemSink("t1")
	p := NewPipeline(fakeSource{[]byte("wrong")}, newFakeLedger(), []Target{{Sink: sink}})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: "deadbeef"})
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

func (c *countingSource) FetchContent(context.Context, string) (io.ReadCloser, error) {
	c.fetch++
	return io.NopCloser(bytes.NewReader(c.data)), nil
}

// TestPipelineSingleFetchFanOut is the point of the fan-out design: N targets
// must not multiply reads on the source (the primary store).
func TestPipelineSingleFetchFanOut(t *testing.T) {
	data := bytes.Repeat([]byte("payload "), 100)
	h, err := Sum(bytes.NewReader(data))
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
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: h})
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
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := newMemSink("z")
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{{Sink: sink, Codec: CodecZstd}})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: h})
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
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	b := newMemSink("b")
	p := NewPipeline(fakeSource{data}, newFakeLedger(),
		[]Target{{Sink: a, Codec: CodecNone}, {Sink: b, Codec: CodecZstd}})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: h, Size: int64(len(data))})
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
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	good := newMemSink("good")
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{
		{Sink: &nonConsumingSink{stubSink{name: "bad"}}, Codec: CodecNone},
		{Sink: good, Codec: CodecNone},
	})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: h})
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

// TestPipelineAllTargetsFailRecorded: when the ONLY target fails (the single-target
// launch config), the object is not-done but the failure is a per-target failure —
// it must be recorded as a 'failed' target_status, NOT masquerade as a source error.
func TestPipelineAllTargetsFailRecorded(t *testing.T) {
	data := []byte("all targets down")
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: &failSink{stubSink{name: "down"}}, Codec: CodecNone}})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: h})
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
// all-targets-dead backup recorded a PARTIAL vr.Total (bytes read before the
// pipes died) as the object size, frozen forever by ON CONFLICT DO NOTHING. The
// object must exceed one io.Copy buffer so a partial read is distinguishable.
func TestPipelineAllTargetsFailRecordsOutboxSize(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 200*1024) // > 32 KiB io.Copy buffer
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: &failSink{stubSink{name: "down"}}, Codec: CodecNone}})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: h, Size: int64(len(data))})
	if err != nil || ok {
		t.Fatalf("all-targets-fail: ok=%v err=%v", ok, err)
	}
	if led.sizes[h] != int64(len(data)) {
		t.Fatalf("all-fail must record the outbox size %d, not a partial %d", len(data), led.sizes[h])
	}
}

// goneSource models a source object deleted before backup (404 → ErrSourceGone).
type goneSource struct{}

func (goneSource) FetchContent(context.Context, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("file-service GET: %w", ErrSourceGone)
}

// TestPipelineSourceGonePropagates: a vanished source must surface ErrSourceGone
// so the consumer can mark the entry 'skipped' instead of retrying it.
func TestPipelineSourceGonePropagates(t *testing.T) {
	p := NewPipeline(goneSource{}, newFakeLedger(),
		[]Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	_, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: "abc"})
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
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	good := newMemSink("good")
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{
		{Sink: &failSink{stubSink{name: "bad"}}, Codec: CodecNone},
		{Sink: good, Codec: CodecNone},
	})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: h})
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
