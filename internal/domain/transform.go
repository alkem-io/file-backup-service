package domain

import (
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

// The restore-side hash-arbiter (raw-first, then bounded zstd) is implemented
// as a streamed decode in restore.go's decodeToTemp — no whole-object buffering.
