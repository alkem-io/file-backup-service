package domain

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
)

type fakeSource struct{ data []byte }

func (f fakeSource) FetchContent(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

// fakeLedger enforces the real FK invariant: a target status can only be written
// after the object row exists (file_backup_target_status REFERENCES file_backup_object).
type fakeLedger struct {
	objects  map[string]bool
	statuses int
	fkError  bool
}

func newFakeLedger() *fakeLedger { return &fakeLedger{objects: map[string]bool{}} }

func (f *fakeLedger) UpsertObject(_ context.Context, m ObjectMeta) error {
	f.objects[m.ExternalID] = true
	return nil
}
func (f *fakeLedger) UpsertTargetStatus(_ context.Context, externalID, _, _ string, _ int64) error {
	if !f.objects[externalID] {
		f.fkError = true
		return fmt.Errorf("fk violation: object %s absent", externalID)
	}
	f.statuses++
	return nil
}
func (f *fakeLedger) TargetState(context.Context, string, string) (string, int64, error) {
	return "", 0, nil
}

type memSink struct {
	name  string
	store map[string][]byte
}

func (m *memSink) Name() string { return m.name }
func (m *memSink) Exists(_ context.Context, h string) (bool, error) {
	_, ok := m.store[h]
	return ok, nil
}
func (m *memSink) Store(_ context.Context, h string, r io.Reader, _ int64) (int64, error) {
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
func (m *memSink) PutManifest(context.Context, string, io.Reader) error { return nil }

func TestPipelineBackupOne(t *testing.T) {
	data := []byte("back me up")
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{name: "t1", store: map[string][]byte{}}
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: sink, Codec: CodecNone}})

	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f1", ExternalID: h})
	if err != nil || !ok {
		t.Fatalf("backup: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(sink.store[h], data) {
		t.Fatal("stored bytes mismatch")
	}
	if led.fkError {
		t.Fatal("target status written before object row (FK order)")
	}
	if !led.objects[h] || led.statuses != 1 {
		t.Fatalf("ledger not updated: %+v", led)
	}
}

func TestPipelineSourceCorrupt(t *testing.T) {
	sink := &memSink{name: "t1", store: map[string][]byte{}}
	p := NewPipeline(fakeSource{[]byte("wrong")}, newFakeLedger(), []Target{{Sink: sink}})
	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f", ExternalID: "deadbeef"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected required target to fail the integrity check")
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
	a := &memSink{name: "a", store: map[string][]byte{}}
	b := &memSink{name: "b", store: map[string][]byte{}}
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

type failSink struct{ name string }

func (f *failSink) Name() string                                 { return f.name }
func (f *failSink) Exists(context.Context, string) (bool, error) { return false, nil }
func (f *failSink) Store(context.Context, string, io.Reader, int64) (int64, error) {
	return 0, fmt.Errorf("sink down")
}
func (f *failSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("unavailable")
}
func (f *failSink) PutManifest(context.Context, string, io.Reader) error { return nil }

// TestPipelineTargetIsolation: targets are symmetric — a flaky target must not
// abort the others (they still receive the object), and the object must NOT be
// "done" until every target has it.
func TestPipelineTargetIsolation(t *testing.T) {
	data := []byte("hello isolation")
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	good := &memSink{name: "good", store: map[string][]byte{}}
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{
		{Sink: &failSink{name: "bad"}, Codec: CodecNone},
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
