package domain

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreRoundTripRaw(t *testing.T) {
	data := []byte("restore me raw")
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{name: "s", store: map[string][]byte{h: data}} // stored raw
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore raw: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored bytes mismatch: %v", err)
	}
	// skip-if-present: a second restore is a no-op and leaves the file intact.
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore idempotent: %v", err)
	}
}

func TestRestoreRoundTripZstd(t *testing.T) {
	data := bytes.Repeat([]byte("restore me zstd "), 100)
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := io.ReadAll(ZstdReader(bytes.NewReader(data))) // as a zstd target stores it
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{name: "s", store: map[string][]byte{h: compressed}}
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore zstd: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored zstd bytes mismatch: %v", err)
	}
	if err := VerifyObject(context.Background(), sink, h, t.TempDir()); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestRestoreCorruptFails(t *testing.T) {
	sink := &memSink{name: "s", store: map[string][]byte{"deadbeef": []byte("garbage not zstd")}}
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, "deadbeef", dir); err == nil {
		t.Fatal("expected integrity error on a corrupt object")
	}
	if _, err := os.Stat(filepath.Join(dir, "deadbeef")); err == nil {
		t.Fatal("corrupt object must not be written to dest")
	}
}
