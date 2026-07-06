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
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: data}} // stored raw
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
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := io.ReadAll(ZstdReader(bytes.NewReader(data))) // as a zstd target stores it
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: compressed}}
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore zstd: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored zstd bytes mismatch: %v", err)
	}
	if err := VerifyObject(context.Background(), sink, h); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// TestRestoreRawStartingWithZstdMagic exercises the magic-collision fallback: a
// raw-stored object whose plaintext begins with the zstd magic (e.g. a .zst upload
// stored with CodecNone) must still restore, by re-reading as raw.
func TestRestoreRawStartingWithZstdMagic(t *testing.T) {
	data := append([]byte{0x28, 0xB5, 0x2F, 0xFD}, []byte(" not a real zstd frame payload")...)
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: data}} // stored raw
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore raw-with-zstd-magic: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored bytes mismatch: %v", err)
	}
}

// TestRestoreRawOversizedZstdFrame guards the errBomb regression: a raw-stored object
// whose bytes are a VALID zstd frame that decodes PAST the cap must still restore by
// falling back to the (bounded, bomb-safe) raw read — not be rejected as a bomb. The
// cap is set between the frame's raw size and its decoded size so decodeZstd over-caps
// while decodeRaw fits.
func TestRestoreRawOversizedZstdFrame(t *testing.T) {
	plaintext := bytes.Repeat([]byte("x"), 500)
	frame, err := io.ReadAll(ZstdReader(bytes.NewReader(plaintext))) // small frame, decodes to 500
	if err != nil {
		t.Fatal(err)
	}
	h, err := sum(bytes.NewReader(frame)) // stored RAW: the FRAME bytes hash to h
	if err != nil {
		t.Fatal(err)
	}
	old := maxRestoreBytes
	maxRestoreBytes = int64(len(frame)) + 4 // >= frame (raw fallback fits), < 500 (decode over-caps)
	defer func() { maxRestoreBytes = old }()

	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: frame}}
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore raw oversized-zstd-frame: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, frame) {
		t.Fatalf("restored bytes mismatch: %v", err)
	}
}

func TestRestoreCorruptFails(t *testing.T) {
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{"deadbeef": []byte("garbage not zstd")}}
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, "deadbeef", dir); err == nil {
		t.Fatal("expected integrity error on a corrupt object")
	}
	if _, err := os.Stat(filepath.Join(dir, "deadbeef")); err == nil {
		t.Fatal("corrupt object must not be written to dest")
	}
}
