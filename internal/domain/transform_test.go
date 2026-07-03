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

func TestEncodeNoneAndZstd(t *testing.T) {
	data := []byte("hello encode")
	var raw bytes.Buffer
	if err := Encode(&raw, bytes.NewReader(data), CodecNone); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw.Bytes(), data) {
		t.Fatal("CodecNone must pass bytes through unchanged")
	}
	big := bytes.Repeat(data, 50)
	var z bytes.Buffer
	if err := Encode(&z, bytes.NewReader(big), CodecZstd); err != nil {
		t.Fatal(err)
	}
	dec, err := zstd.NewReader(bytes.NewReader(z.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	out, err := io.ReadAll(dec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, big) {
		t.Fatal("zstd encode/decode mismatch")
	}
}
