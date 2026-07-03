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

	"github.com/alkem-io/file-backup-service/internal/fsutil"
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
	return fsutil.SyncDir(filepath.Dir(dest))
}

// VerifyObject streams hash from src and confirms it decodes+hashes to hash,
// without holding the object in memory and without OOMing on a bomb. scratchDir
// MUST be real disk (not tmpfs) — a multi-GB verify on tmpfs would exhaust RAM.
func VerifyObject(ctx context.Context, src Sink, hash, scratchDir string) error {
	if err := os.MkdirAll(scratchDir, 0o755); err != nil { //nolint:gosec // operator scratch dir
		return fmt.Errorf("mkdir scratch: %w", err)
	}
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
	// A single guarded defer removes the temp unless we hand it back — no error
	// branch can leak a *.partial into the scratch dir / primary store.
	raw, rawName, err := createTemp(workDir, hash+".raw")
	if err != nil {
		return "", err
	}
	keepRaw := false
	defer func() {
		if !keepRaw {
			_ = os.Remove(rawName)
		}
	}()
	h := sha3.New256()
	// Cap the raw download too — a hostile/corrupt stored object must not fill the
	// primary store during a restore (the decoded cap alone left Stage 1 unbounded).
	rawN, err := io.Copy(io.MultiWriter(raw, h), io.LimitReader(rc, maxRestoreBytes+1))
	_ = raw.Sync()
	_ = raw.Close()
	if err != nil {
		return "", fmt.Errorf("read: %w", err)
	}
	if rawN > maxRestoreBytes {
		return "", fmt.Errorf("stored object exceeds %d bytes (possible corruption)", maxRestoreBytes)
	}
	if hex.EncodeToString(h.Sum(nil)) == hash {
		keepRaw = true
		return rawName, nil // stored plain — the raw temp IS the plaintext
	}

	// Stage 2: stored bytes are zstd — decode (bounded) into a second temp, verify.
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

	out, outName, err := createTemp(workDir, hash+".out")
	if err != nil {
		return "", err
	}
	keepOut := false
	defer func() {
		if !keepOut {
			_ = os.Remove(outName)
		}
	}()
	h2 := sha3.New256()
	n, err := io.Copy(io.MultiWriter(out, h2), io.LimitReader(dec, maxRestoreBytes+1))
	_ = out.Sync()
	_ = out.Close()
	if err != nil {
		return "", fmt.Errorf("zstd decode: %w", err)
	}
	if n > maxRestoreBytes {
		return "", fmt.Errorf("integrity: decoded output exceeds %d bytes (possible corruption/bomb)", maxRestoreBytes)
	}
	if hex.EncodeToString(h2.Sum(nil)) != hash {
		return "", fmt.Errorf("integrity: decoded bytes do not match %s", hash)
	}
	keepOut = true
	return outName, nil
}

// createTemp opens a unique "<prefix>.*.partial" temp under dir.
func createTemp(dir, prefix string) (*os.File, string, error) {
	f, err := os.CreateTemp(dir, prefix+".*.partial")
	if err != nil {
		return nil, "", fmt.Errorf("create temp: %w", err)
	}
	return f, f.Name(), nil
}
