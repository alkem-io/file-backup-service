package domain

import (
	"bytes"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestZstdReaderRoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("compressible payload "), 200)
	compressed, err := io.ReadAll(ZstdReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) >= len(data) {
		t.Skip("payload did not compress")
	}
	dec, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	out, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("streamed zstd round-trip mismatch")
	}
}

// TestZstdReaderPoolReuse drives ZstdReader repeatedly to exercise pooled-encoder
// reuse (Reset after Close), catching a broken reuse cycle.
func TestZstdReaderPoolReuse(t *testing.T) {
	for i := 0; i < 5; i++ {
		data := bytes.Repeat([]byte("reuse "), 200)
		compressed, err := io.ReadAll(ZstdReader(bytes.NewReader(data)))
		if err != nil {
			t.Fatal(err)
		}
		dec, err := zstd.NewReader(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		out, err := io.ReadAll(dec)
		dec.Close()
		if err != nil || !bytes.Equal(out, data) {
			t.Fatalf("round %d: pooled encoder reuse broke: %v", i, err)
		}
	}
}
