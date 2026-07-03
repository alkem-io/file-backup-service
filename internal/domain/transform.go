package domain

import (
	"fmt"
	"io"
)

// Codec is a per-target content transform. The target key is always the hash of
// the PLAINTEXT, so restore derives the codec by hashing (hash-arbiter) — no
// per-object codec metadata is stored. See specs/008 FR-016.
type Codec string

const (
	// CodecNone stores raw bytes.
	CodecNone Codec = "none"
	// CodecZstd stores adaptive zstd (compress, keep only if smaller).
	CodecZstd Codec = "zstd"
)

// Encode copies r into w applying the codec.
//
// TODO(T008): implement adaptive zstd (klauspost/compress). The scaffold
// implements "none" only; zstd returns ErrNotImplemented so it is never
// silently stored raw under a compressed contract.
func Encode(w io.Writer, r io.Reader, codec Codec) error {
	switch codec {
	case CodecNone, "":
		if _, err := io.Copy(w, r); err != nil {
			return fmt.Errorf("encode none: %w", err)
		}
		return nil
	case CodecZstd:
		return ErrNotImplemented
	default:
		return fmt.Errorf("unknown codec %q", codec)
	}
}
