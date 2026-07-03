package domain

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha3"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"

	"github.com/alkem-io/file-backup-service/internal/fsutil"
)

// maxRestoreBytes caps a decoded object so a corrupt/hostile zstd frame (a
// decompression bomb) cannot OOM the restore/verify tooling. Larger than any
// plausible object in this corpus; raise it deliberately if ever needed.
const maxRestoreBytes int64 = 16 << 30 // 16 GiB

// zstdMagic is the zstd frame magic (RFC 8878). A stored object is zstd iff it
// begins with these bytes — so a 4-byte peek picks the codec without a probe write.
var zstdMagic = []byte{0x28, 0xB5, 0x2F, 0xFD}

// errTryRaw signals that a zstd-magic object did not decode+hash as zstd, so the
// stored bytes must be the plaintext (which happens to start with the zstd magic).
var errTryRaw = errors.New("not zstd after all")

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
	if err := os.MkdirAll(destDir, 0o755); err != nil { //nolint:gosec // primary store is world-readable
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp, tmpName, err := createTemp(destDir, hash)
	if err != nil {
		return err
	}
	closed := false
	closeTmp := func() {
		if !closed {
			_ = tmp.Close()
			closed = true
		}
	}
	keep := false
	defer func() {
		closeTmp()
		if !keep {
			_ = os.Remove(tmpName)
		}
	}()
	// reset rewinds the temp if the rare magic-collision fallback re-decodes it.
	reset := func() error {
		if _, e := tmp.Seek(0, io.SeekStart); e != nil {
			return e
		}
		return tmp.Truncate(0)
	}
	if err := decodeStream(ctx, src, hash, tmp, reset); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	closeTmp()
	// 0644 so file-service (uid 65532) can read restored objects.
	if err := os.Chmod(tmpName, 0o644); err != nil { //nolint:gosec // content-addressed blob, served by file-service
		return fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	keep = true
	return fsutil.SyncDir(filepath.Dir(dest))
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

// decodeRaw copies r straight to dst, hashing, and verifies it equals hash.
func decodeRaw(r io.Reader, hash string, dst io.Writer) error {
	h := sha3.New256()
	n, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(r, maxRestoreBytes+1))
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if n > maxRestoreBytes {
		return fmt.Errorf("stored object exceeds %d bytes (possible corruption)", maxRestoreBytes)
	}
	if hex.EncodeToString(h.Sum(nil)) != hash {
		return fmt.Errorf("integrity: stored bytes for %s are neither valid zstd nor the plaintext", hash)
	}
	return nil
}

// decodeZstd streams r through a bounded zstd decoder into dst, hashing, and
// verifies it equals hash. A non-zstd stream, a decode error, or a hash mismatch
// returns errTryRaw (the caller re-reads as raw); a decompression bomb is a hard
// error (no fallback).
func decodeZstd(r io.Reader, hash string, dst io.Writer) error {
	zr, err := zstd.NewReader(io.LimitReader(r, maxRestoreBytes+1))
	if err != nil {
		return errTryRaw
	}
	defer zr.Close()
	h := sha3.New256()
	n, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(zr, maxRestoreBytes+1))
	if err != nil {
		return errTryRaw
	}
	if n > maxRestoreBytes {
		return fmt.Errorf("integrity: decoded output exceeds %d bytes (possible corruption/bomb)", maxRestoreBytes)
	}
	if hex.EncodeToString(h.Sum(nil)) != hash {
		return errTryRaw
	}
	return nil
}

// createTemp opens a unique "<prefix>.*.partial" temp under dir.
func createTemp(dir, prefix string) (*os.File, string, error) {
	f, err := os.CreateTemp(dir, prefix+".*.partial")
	if err != nil {
		return nil, "", fmt.Errorf("create temp: %w", err)
	}
	return f, f.Name(), nil
}
