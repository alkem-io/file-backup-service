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

	"github.com/alkem-io/file-backup-service/internal/fsutil"
)

// MaxObjectBytes bounds the decoded PLAINTEXT the restore/verify path accepts — a
// decompression-bomb / oversized-blob guard. It is sized to REALITY: file-service's
// file.size is INTEGER (int32, ~2 GiB max), so no object larger than ~2 GiB can exist;
// 4 GiB is 2x that with headroom. Keeping the cap near the real max is what makes the
// bomb bound tight (a bomb can't inflate 32x past any legit object) AND keeps the S3
// sink's flat 5 MiB part size within the 10,000-part ceiling (4 GiB / 5 MiB ≈ 820 parts,
// far under 10,000 — so no per-object part-size arithmetic is needed). If the file
// schema ever widens past int32, raise this AND revisit the S3 part size together.
const MaxObjectBytes int64 = 4 << 30 // 4 GiB

// maxRestoreBytes bounds the decoded PLAINTEXT on BOTH the raw and zstd restore paths
// (bomb / oversized-blob protection). It is MaxObjectBytes as a var only so tests can
// lower it to exercise the over-cap paths.
var maxRestoreBytes = MaxObjectBytes

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
			// Bounded + ctx-cancellable: a large stale/corrupt file must not read
			// unboundedly nor swallow the operator's SIGINT/SIGTERM.
			overCap, verr := copyVerify(io.Discard, ctxReader{ctx, f}, hash, maxRestoreBytes)
			_ = f.Close()
			if verr == nil && !overCap {
				return nil // already present and intact
			}
			if err := ctx.Err(); err != nil {
				return err
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
	// ctxReader so a large decode from a filesystem source (os.File.Read ignores ctx)
	// honors SIGINT/SIGTERM mid-read, not only at the CommitWrite gate afterward.
	br := bufio.NewReaderSize(ctxReader{ctx, rc}, 8<<10)
	magic, _ := br.Peek(4) // short object → short magic, simply won't match
	if bytes.Equal(magic, zstdMagic) {
		derr := decodeZstd(br, hash, dst)
		_ = rc.Close()
		if derr == nil {
			return nil
		}
		// decodeZstd returns only nil or errTryRaw, so any non-nil error means fall back
		// to the (bounded, bomb-safe) raw read: the plaintext started with the zstd magic
		// but did not decode+hash as zstd — an incompressible upload stored raw, or a
		// raw object whose bytes happen to be an oversized zstd frame. Re-read as raw.
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
		return decodeRaw(ctxReader{ctx, rc2}, hash, dst)
	}
	// No zstd magic — zstd always begins with it, so the bytes are the plaintext.
	defer func() { _ = rc.Close() }()
	return decodeRaw(br, hash, dst)
}

// ctxReader makes a Read loop cancellable: it returns ctx.Err() before each Read so a
// long source read (a filesystem restore, the pre-existing-intact check) honors
// SIGINT/SIGTERM rather than running to completion (os.File.Read ignores ctx).
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// copyVerify streams src into dst, hashing via the shared object-hash primitive so
// restore verifies by the exact same digest rule as backup (hash.go). If limit > 0 it
// caps the copy and returns overCap=true past it (decompression-bomb protection on the
// zstd path); limit <= 0 is uncapped (the RAW path — the stored bytes ARE the object, no
// amplification, so a legitimately-large object must not be rejected).
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

// decodeRaw copies r straight to dst and verifies it equals hash, bounded by
// maxRestoreBytes — so a corrupt/tampered target that returns an oversized stream for
// a CodecNone object can't fill the restore disk before the hash check fails (the
// plaintext cap applies to the raw path too, not only zstd).
func decodeRaw(r io.Reader, hash string, dst io.Writer) error {
	overCap, err := copyVerify(dst, r, hash, maxRestoreBytes)
	switch {
	case err != nil && !errors.Is(err, errHashMismatch):
		return fmt.Errorf("read: %w", err)
	case overCap:
		return fmt.Errorf("integrity: stored bytes for %s exceed the %d-byte restore cap", hash, maxRestoreBytes)
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
	// Bomb protection is the DECODED-output cap (copyVerify below); do NOT also cap the
	// COMPRESSED input — an incompressible near-cap object stores as raw zstd blocks
	// slightly LARGER than its plaintext, and an input cap would truncate its frame and
	// make it (falsely) unrestorable.
	zr, err := newZstdDecoder(r)
	if err != nil {
		return errTryRaw
	}
	defer zr.Close()
	// Cap the DECODED output (bomb protection); a tiny zstd frame can expand to PB.
	// An over-cap decode returns errTryRaw so decodeStream falls back to decodeRaw —
	// which is ITSELF bounded at maxRestoreBytes, so the fallback is inherently bomb-safe:
	// a raw-stored object whose plaintext is a valid oversized zstd frame (e.g. a large
	// .zst uploaded as-is) restores from its small raw bytes, while a genuine tampered
	// zstd bomb fails both paths (the raw bytes don't hash). Never dropping the fallback
	// costs at most one extra bounded read on hostile input, in exchange for no data loss.
	if overCap, verr := copyVerify(dst, zr, hash, maxRestoreBytes); overCap || verr != nil {
		return errTryRaw
	}
	return nil
}
