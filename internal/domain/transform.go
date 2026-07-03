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

// DefaultMinGain is the minimum fractional size reduction for CompressAdaptive to
// keep the compressed form (3%).
const DefaultMinGain = 0.03

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

// CompressAdaptive returns zstd(data) when it is at least minGain smaller than
// data (the bool reports that compression was kept); otherwise it returns data
// unchanged. This is the "keep only if meaningfully smaller" rule — the stored
// form is always self-describing via the hash-arbiter on restore.
func CompressAdaptive(data []byte, minGain float64) ([]byte, bool, error) {
	var buf bytes.Buffer
	if err := Encode(&buf, bytes.NewReader(data), CodecZstd); err != nil {
		return nil, false, err
	}
	if float64(buf.Len()) <= float64(len(data))*(1-minGain) {
		return buf.Bytes(), true, nil
	}
	return data, false, nil
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
