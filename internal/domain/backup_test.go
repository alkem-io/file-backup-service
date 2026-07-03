package domain

import (
	"bytes"
	"context"
	"io"
	"testing"
)

type fakeSource struct{ data []byte }

func (f fakeSource) FetchContent(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

type fakeLedger struct{ objects, statuses int }

func (f *fakeLedger) UpsertObject(context.Context, ObjectMeta) error { f.objects++; return nil }
func (f *fakeLedger) UpsertTargetStatus(context.Context, string, string, string, int64) error {
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
	led := &fakeLedger{}
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: sink, Required: true, Codec: CodecNone}})

	ok, err := p.BackupOne(context.Background(), OutboxEntry{FileID: "f1", ExternalID: h})
	if err != nil || !ok {
		t.Fatalf("backup: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(sink.store[h], data) {
		t.Fatal("stored bytes mismatch")
	}
	if led.objects != 1 || led.statuses != 1 {
		t.Fatalf("ledger not updated: %+v", led)
	}
}

func TestPipelineSourceCorrupt(t *testing.T) {
	sink := &memSink{name: "t1", store: map[string][]byte{}}
	p := NewPipeline(fakeSource{[]byte("wrong")}, &fakeLedger{}, []Target{{Sink: sink, Required: true}})
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
