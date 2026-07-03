package domain

import (
	"bytes"
	"io"
	"testing"
)

func TestDecodeArbiter_Plain(t *testing.T) {
	data := []byte("plain content, stored raw")
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeArbiter(h, data)
	if err != nil {
		t.Fatalf("decode plain: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("plain round-trip mismatch")
	}
}

func TestDecodeArbiter_Zstd(t *testing.T) {
	data := bytes.Repeat([]byte("compressible payload "), 200)
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Encode(&buf, bytes.NewReader(data), CodecZstd); err != nil {
		t.Fatal(err)
	}
	if buf.Len() >= len(data) {
		t.Skip("payload did not compress")
	}
	out, err := DecodeArbiter(h, buf.Bytes())
	if err != nil {
		t.Fatalf("decode zstd: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("zstd round-trip mismatch")
	}
}

func TestDecodeArbiter_Corrupt(t *testing.T) {
	// Bytes that neither hash to want nor decode to it must error.
	if _, err := DecodeArbiter("00ff", []byte("not the content and not zstd")); err == nil {
		t.Fatal("expected integrity error")
	}
}

func TestZstdReaderRoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("compressible payload "), 200)
	h, err := Sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := io.ReadAll(ZstdReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) >= len(data) {
		t.Skip("payload did not compress")
	}
	out, err := DecodeArbiter(h, compressed)
	if err != nil {
		t.Fatalf("decode streamed zstd: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("streamed zstd round-trip mismatch")
	}
}
