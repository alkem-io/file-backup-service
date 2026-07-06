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

// ParseCodec is the single owner of the compression vocabulary: config validation
// and the sink builder both route through it, so a codec name can never validate in
// one place and silently map to CodecNone in another.
func ParseCodec(s string) (Codec, error) {
	switch s {
	case "", string(CodecNone):
		return CodecNone, nil
	case string(CodecZstd):
		return CodecZstd, nil
	default:
		return "", fmt.Errorf("unknown compression %q (want none|zstd)", s)
	}
}

// newZstdDecoder builds the single-goroutine zstd decoder used by BOTH decode paths
// (restore + reconcile) — one owner for the concurrency cap (WithDecoderConcurrency(1)
// avoids the per-core block-decoder allocation). The DECODED-output bomb bound is
// applied by each caller (copyVerify's limit in restore, a LimitReader in reconcile).
func newZstdDecoder(r io.Reader) (*zstd.Decoder, error) {
	return zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
}

// zstdEncoderPool reuses single-goroutine zstd encoders across objects — each
// NewWriter eagerly allocates GOMAXPROCS block encoders (~1.25 MiB of hash tables
// each), so a fresh one per object per zstd target churns tens of MiB and GC.
// We already parallelize across targets/objects, so encoder concurrency is 1.
var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if err != nil { // a fixed valid option set — an error here is a build-time bug
			panic(fmt.Sprintf("zstd encoder: %v", err))
		}
		return enc
	},
}

// ZstdReader returns a reader that zstd-compresses src on the fly (streamed via a
// pipe, no full-object buffer), using a pooled encoder. Close it after the
// consuming call to release the worker goroutine — errors from src (e.g. a
// VerifyReader integrity failure) propagate to the reader so a downstream Sink
// never commits bad data.
func ZstdReader(src io.Reader) io.ReadCloser {
	// pipeThrough's recover wraps the whole closure — incl. the pool Get — so even a panic
	// in the pool constructor fails THIS target, not the worker. A panicked encoder never
	// reaches the Put below, so it's dropped (GC'd), and one that errored on Close ended in
	// an unknown state, so it's dropped too; only a cleanly-closed encoder is returned.
	return pipeThrough("zstd encode", func(w io.Writer) error {
		enc := zstdEncoderPool.Get().(*zstd.Encoder)
		enc.Reset(w)
		_, err := io.Copy(enc, src)
		closeErr := enc.Close()
		if err == nil {
			err = closeErr
		}
		if closeErr == nil {
			enc.Reset(nil) // drop the pipe reference before returning to the pool
			zstdEncoderPool.Put(enc)
		}
		return err
	})
}

// pipeThrough runs produce on a background goroutine writing into the returned reader's
// pipe, then closes the pipe with produce's error (or a recovered panic named by who), so
// a reader always sees the terminal error — the one owner of the "producer goroutine →
// io.Pipe → CloseWithError" scaffold (a missed CloseWithError leaks the goroutine forever
// on a parked pw.Write). Shared by the zstd encoder and the manifest writer.
func pipeThrough(who string, produce func(w io.Writer) error) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				pw.CloseWithError(PanicErr(who, r))
			}
		}()
		pw.CloseWithError(produce(pw))
	}()
	return pr
}

// The restore-side hash-arbiter (magic-peek: bounded zstd if the frame magic is
// present, else raw) is a streamed single-pass decode in restore.go's decodeStream.
