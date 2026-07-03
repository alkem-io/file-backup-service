package domain

import (
	"fmt"
	"io"
	"sync"

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

// zstdEncoderPool reuses single-goroutine zstd encoders across objects — each
// NewWriter eagerly allocates GOMAXPROCS block encoders (~1.25 MiB of hash tables
// each), so a fresh one per object per zstd target churns tens of MiB and GC.
// We already parallelize across targets/objects, so encoder concurrency is 1.
var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		return enc
	},
}

// ZstdReader returns a reader that zstd-compresses src on the fly (streamed via a
// pipe, no full-object buffer), using a pooled encoder. Close it after the
// consuming call to release the worker goroutine — errors from src (e.g. a
// VerifyReader integrity failure) propagate to the reader so a downstream Sink
// never commits bad data.
func ZstdReader(src io.Reader) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		enc := zstdEncoderPool.Get().(*zstd.Encoder)
		// A panic in the encoder must fail THIS target, not crash the worker — a
		// recover only catches its own goroutine, and every other sink-facing
		// goroutine is guarded the same way. A panicked encoder is in an unknown
		// state, so it is dropped (GC'd), never returned to the pool.
		defer func() {
			if r := recover(); r != nil {
				pw.CloseWithError(fmt.Errorf("zstd encode panicked: %v", r))
			}
		}()
		enc.Reset(pw)
		_, err := io.Copy(enc, src)
		if cerr := enc.Close(); err == nil {
			err = cerr
		}
		enc.Reset(nil) // drop the pipe reference before returning to the pool
		zstdEncoderPool.Put(enc)
		pw.CloseWithError(err)
	}()
	return pr
}

// The restore-side hash-arbiter (magic-peek: bounded zstd if the frame magic is
// present, else raw) is a streamed single-pass decode in restore.go's decodeStream.
