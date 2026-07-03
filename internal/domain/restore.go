package domain

import (
	"context"
	"crypto/sha3"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
)

// maxRestoreBytes caps a decoded object so a corrupt/hostile zstd frame (a
// decompression bomb) cannot OOM the restore/verify tooling. Larger than any
// plausible object in this corpus; raise it deliberately if ever needed.
const maxRestoreBytes int64 = 16 << 30 // 16 GiB

// RestoreObject streams hash from src, reverses the transform via the
// hash-arbiter, verifies it, and writes it to destDir/<hash> (0644, durable).
// If the object is already present it is hash-verified first — a corrupt file in
// the primary store (the reason to restore) does not mask the good backup copy.
// Fully streamed via temp files: no whole-object buffering.
func RestoreObject(ctx context.Context, src Sink, hash, destDir string) error {
	dest := filepath.Join(destDir, hash)
	if _, err := os.Stat(dest); err == nil {
		if ok, _ := verifyFile(dest, hash); ok {
			return nil // already present and intact
		}
		// present but corrupt/mismatched — overwrite with the good backup copy.
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil { //nolint:gosec // primary store is world-readable
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp, err := decodeToTemp(ctx, src, hash, destDir)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp) }() // no-op after a successful rename
	// 0644 so file-service (uid 65532) can read restored objects.
	if err := os.Chmod(tmp, 0o644); err != nil { //nolint:gosec // content-addressed blob, served by file-service
		return fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return fsyncDir(filepath.Dir(dest))
}

// VerifyObject streams hash from src and confirms it decodes+hashes to hash,
// without holding the object in memory and without OOMing on a bomb. scratchDir
// MUST be real disk (not tmpfs) — a multi-GB verify on tmpfs would exhaust RAM.
func VerifyObject(ctx context.Context, src Sink, hash, scratchDir string) error {
	tmp, err := decodeToTemp(ctx, src, hash, scratchDir)
	if err != nil {
		return err
	}
	_ = os.Remove(tmp) // best-effort cleanup — not a verification result
	return nil
}

// decodeToTemp streams the stored bytes from src, applies the hash-arbiter (raw
// first, else bounded zstd), verifies the result against hash, and returns the
// path of a temp file (under workDir) holding the verified plaintext. The caller
// renames or removes it. Streamed throughout — memory is bounded to io.Copy
// buffers regardless of object size.
func decodeToTemp(ctx context.Context, src Sink, hash, workDir string) (string, error) {
	rc, err := src.Fetch(ctx, hash)
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()

	// Stage 1: stream stored bytes to a temp, hashing to detect a plaintext store.
	raw, err := os.CreateTemp(workDir, hash+".raw.*.partial")
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	rawName := raw.Name()
	h := sha3.New256()
	// Cap the raw download too — a hostile/corrupt stored object must not fill the
	// primary store during a restore (the decoded cap alone left Stage 1 unbounded).
	rawN, err := io.Copy(io.MultiWriter(raw, h), io.LimitReader(rc, maxRestoreBytes+1))
	if err != nil {
		_ = raw.Close()
		_ = os.Remove(rawName)
		return "", fmt.Errorf("read: %w", err)
	}
	if rawN > maxRestoreBytes {
		_ = raw.Close()
		_ = os.Remove(rawName)
		return "", fmt.Errorf("stored object exceeds %d bytes (possible corruption)", maxRestoreBytes)
	}
	_ = raw.Sync()
	_ = raw.Close()
	if hex.EncodeToString(h.Sum(nil)) == hash {
		return rawName, nil // stored plain — the raw temp IS the plaintext
	}

	// Stage 2: stored bytes are zstd — decode (bounded) into a second temp, verify.
	defer func() { _ = os.Remove(rawName) }()
	in, err := os.Open(rawName) //nolint:gosec // temp under workDir
	if err != nil {
		return "", fmt.Errorf("reopen temp: %w", err)
	}
	defer func() { _ = in.Close() }()
	dec, err := zstd.NewReader(in)
	if err != nil {
		return "", fmt.Errorf("integrity: %s is neither plaintext nor zstd: %w", hash, err)
	}
	defer dec.Close()

	out, err := os.CreateTemp(workDir, hash+".out.*.partial")
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	outName := out.Name()
	h2 := sha3.New256()
	n, err := io.Copy(io.MultiWriter(out, h2), io.LimitReader(dec, maxRestoreBytes+1))
	if err != nil {
		_ = out.Close()
		_ = os.Remove(outName)
		return "", fmt.Errorf("zstd decode: %w", err)
	}
	if n > maxRestoreBytes {
		_ = out.Close()
		_ = os.Remove(outName)
		return "", fmt.Errorf("integrity: decoded output exceeds %d bytes (possible corruption/bomb)", maxRestoreBytes)
	}
	_ = out.Sync()
	_ = out.Close()
	if hex.EncodeToString(h2.Sum(nil)) != hash {
		_ = os.Remove(outName)
		return "", fmt.Errorf("integrity: decoded bytes do not match %s", hash)
	}
	return outName, nil
}

// verifyFile reports whether the plaintext file at path hashes to hash.
func verifyFile(path, hash string) (bool, error) {
	f, err := os.Open(path) //nolint:gosec // caller-provided primary-store path
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	h := sha3.New256()
	if _, err := io.Copy(h, f); err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == hash, nil
}

// fsyncDir makes a rename/create within dir durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // destination dir under the primary store
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
