package domain

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"

	"github.com/alkem-io/file-backup-service/internal/fsutil"
)

// maxRestoreBytes caps a decoded zstd object so a corrupt/tampered frame (a
// decompression bomb from a compromised target) cannot OOM the restore/verify
// tooling. It MUST exceed the S3 backup ceiling (5 MiB PartSize x 10000 parts ≈
// 48 GiB) so a large-but-legit object stored zstd stays restorable — otherwise a
// 16–48 GiB object would back up but fail restore (overCap -> raw fallback hashes
// the compressed frame -> mismatch). 64 GiB clears that ceiling and still bounds a bomb.
const maxRestoreBytes int64 = 64 << 30 // 64 GiB

// zstdMagic is the zstd frame magic (RFC 8878). A stored object is zstd iff it
// begins with these bytes — so a 4-byte peek picks the codec without a probe write.
var zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

// errTryRaw signals that a zstd-magic object did not decode+hash as zstd, so the
// stored bytes must be the plaintext (which happens to start with the zstd magic).
var errTryRaw = errors.New("not zstd after all")

// errHashMismatch means the decoded content did not hash to the requested key.
var errHashMismatch = errors.New("hash mismatch")

// RestoreObject streams hash from src, reverses the transform via the hash-arbiter,
// and writes the verified plaintext to destDir/<hash> (0644, durable) in a SINGLE
// pass — magic-peek picks raw vs zstd, so a zstd object decodes straight to the
// destination temp instead of being staged through a compressed temp (was 3× disk
// I/O; a restore is a DR event and must be fast). If the object is already present
// it is hash-verified first — a corrupt file in the primary store does not mask the
// good backup copy.
func RestoreObject(ctx context.Context, src Sink, hash, destDir string) error {
	dest := filepath.Join(destDir, hash)
	if _, err := os.Stat(dest); err == nil {
		if f, oerr := os.Open(dest); oerr == nil { //nolint:gosec // primary-store path
			ok, _ := Verify(hash, f)
			_ = f.Close()
			if ok {
				return nil // already present and intact
			}
		}
		// present but corrupt/mismatched — overwrite with the good backup copy.
	}
	// 0644 so file-service (uid 65532) can read restored objects. The temp/fsync/
	// ctx-gate/commit ceremony is the same durable spine the sink write uses.
	return fsutil.CommitWrite(ctx, destDir, hash, 0o644, func(tmp *os.File) error {
		// reset rewinds the temp if the rare magic-collision fallback re-decodes it.
		reset := func() error {
			if _, e := tmp.Seek(0, io.SeekStart); e != nil {
				return e
			}
			return tmp.Truncate(0)
		}
		return decodeStream(ctx, src, hash, tmp, reset)
	})
}

// VerifyObject streams hash from src and confirms it decodes+hashes to hash,
// writing the plaintext to io.Discard — no temp file, no scratch disk, bounded
// memory, bomb-safe (the decode is length-capped). One pass.
func VerifyObject(ctx context.Context, src Sink, hash string) error {
	return decodeStream(ctx, src, hash, io.Discard, nil)
}

// decodeStream streams the stored object from src through the hash-arbiter
// (magic-peek: bounded zstd, else raw) into dst, verifying the plaintext hash, in
// one pass. dst may be io.Discard (verify) or a temp file (restore). reset, if
// non-nil, rewinds dst for the rare zstd-magic-but-actually-raw fallback.
func decodeStream(ctx context.Context, src Sink, hash string, dst io.Writer, reset func() error) error {
	rc, err := src.Fetch(ctx, hash)
	if err != nil {
		return err
	}
	br := bufio.NewReaderSize(rc, 8<<10)
	magic, _ := br.Peek(4) // short object → short magic, simply won't match
	if bytes.Equal(magic, zstdMagic) {
		derr := decodeZstd(br, hash, dst)
		_ = rc.Close()
		if derr == nil {
			return nil
		}
		if !errors.Is(derr, errTryRaw) {
			return derr // decompression bomb / hard error — do not fall back
		}
		// zstd magic but it did not decode+hash as zstd: the plaintext itself starts
		// with the zstd magic (an incompressible upload stored raw). Re-read as raw.
		if reset != nil {
			if e := reset(); e != nil {
				return e
			}
		}
		rc2, ferr := src.Fetch(ctx, hash)
		if ferr != nil {
			return ferr
		}
		defer func() { _ = rc2.Close() }()
		return decodeRaw(rc2, hash, dst)
	}
	// No zstd magic — zstd always begins with it, so the bytes are the plaintext.
	defer func() { _ = rc.Close() }()
	return decodeRaw(br, hash, dst)
}

// copyVerify streams src into dst, hashing via the shared object-hash primitive so
// restore verifies by the exact same digest rule as backup (hash.go). If limit > 0
// it caps the copy and returns overCap=true past it (decompression-bomb protection
// on the zstd path); limit <= 0 is uncapped (the RAW path — the stored bytes ARE the
// object, no amplification, so a legitimately-large object must not be rejected).
func copyVerify(dst io.Writer, src io.Reader, want string, limit int64) (overCap bool, err error) {
	h := newHash()
	rdr := src
	if limit > 0 {
		rdr = io.LimitReader(src, limit+1)
	}
	n, err := io.Copy(io.MultiWriter(dst, h), rdr)
	if err != nil {
		return false, err
	}
	if limit > 0 && n > limit {
		return true, nil
	}
	if hexSum(h) != want {
		return false, errHashMismatch
	}
	return false, nil
}

// decodeRaw copies r straight to dst and verifies it equals hash. Uncapped: the raw
// stored bytes are the object itself, so backup (which streams any size) and restore
// agree — a 17 GiB object backs up and restores, not rejected as "corrupt".
func decodeRaw(r io.Reader, hash string, dst io.Writer) error {
	_, err := copyVerify(dst, r, hash, 0)
	switch {
	case err != nil && !errors.Is(err, errHashMismatch):
		return fmt.Errorf("read: %w", err)
	case errors.Is(err, errHashMismatch):
		return fmt.Errorf("integrity: stored bytes for %s are neither valid zstd nor the plaintext", hash)
	}
	return nil
}

// decodeZstd streams r through a bounded zstd decoder into dst and verifies it
// equals hash. A non-zstd stream, a decode error, a decoded-size overrun, OR a hash
// mismatch all return errTryRaw so the caller re-reads as raw (size-bounded) — that
// is what keeps a raw-stored object that is ITSELF a valid zstd bomb restorable from
// its small raw bytes. Only when BOTH interpretations fail is it genuinely corrupt.
func decodeZstd(r io.Reader, hash string, dst io.Writer) error {
	// Serial single-stream decode — cap concurrency so NewReader doesn't eagerly
	// allocate a block decoder per core (mirrors the encoder's WithEncoderConcurrency).
	zr, err := zstd.NewReader(io.LimitReader(r, maxRestoreBytes+1), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return errTryRaw
	}
	defer zr.Close()
	// Cap the DECODED output (bomb protection); a tiny zstd frame can expand to PB.
	if overCap, verr := copyVerify(dst, zr, hash, maxRestoreBytes); verr != nil || overCap {
		return errTryRaw
	}
	return nil
}
