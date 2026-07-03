package domain

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Codec is a per-target content transform. The target key is always the hash of
// the PLAINTEXT, so restore derives the codec by hashing (hash-arbiter) — no
// per-object codec metadata is stored. See specs/008 FR-016.
type Codec string

const (
	// CodecNone stores raw bytes.
	CodecNone Codec = "none"
	// CodecZstd stores zstd-compressed bytes.
	CodecZstd Codec = "zstd"
)

// Encode streams r into w applying codec.
func Encode(w io.Writer, r io.Reader, codec Codec) error {
	switch codec {
	case CodecNone, "":
		if _, err := io.Copy(w, r); err != nil {
			return fmt.Errorf("encode none: %w", err)
		}
		return nil
	case CodecZstd:
		enc, err := zstd.NewWriter(w)
		if err != nil {
			return fmt.Errorf("zstd writer: %w", err)
		}
		if _, err := io.Copy(enc, r); err != nil {
			_ = enc.Close()
			return fmt.Errorf("zstd copy: %w", err)
		}
		if err := enc.Close(); err != nil {
			return fmt.Errorf("zstd close: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unknown codec %q", codec)
	}
}

// ZstdReader returns a reader that zstd-compresses src on the fly (streamed via a
// pipe, no full-object buffer). Close it after the consuming call to release the
// worker goroutine — errors from src (e.g. a VerifyReader integrity failure)
// propagate to the reader so a downstream Sink never commits bad data.
func ZstdReader(src io.Reader) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(Encode(pw, src, CodecZstd))
	}()
	return pr
}

// DecodeArbiter reverses the transform using the content hash as the sole
// arbiter: if the raw bytes already hash to want they are plaintext (handles the
// edge case of a plaintext that is itself a zstd stream); otherwise they are
// zstd-decompressed and re-verified. Returns the original bytes.
func DecodeArbiter(want string, raw []byte) ([]byte, error) {
	if ok, _ := Verify(want, bytes.NewReader(raw)); ok {
		return raw, nil // stored plain
	}
	dec, err := zstd.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("integrity: raw does not match %s and is not zstd: %w", want, err)
	}
	defer dec.Close()
	out, err := io.ReadAll(dec)
	if err != nil {
		return nil, fmt.Errorf("zstd decode: %w", err)
	}
	if ok, _ := Verify(want, bytes.NewReader(out)); !ok {
		return nil, fmt.Errorf("integrity: decoded bytes do not match %s", want)
	}
	return out, nil
}
