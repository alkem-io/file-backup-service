// Package fsutil holds small, stdlib-only filesystem + content-addressing
// helpers shared by the sink adapters and the domain restore path. It is a leaf
// package with zero project dependencies, so the domain importing it does not
// violate the hexagonal rule (it is not "infrastructure").
package fsutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"
)

const (
	manifestPrefix  = "_manifest"
	preflightPrefix = "_preflight"
	// contentHashLen is the hex length of a SHA3-256 externalID (32 bytes -> 64 hex chars).
	contentHashLen = 64
)

// ValidateContentHash rejects an externalID that is not exactly 64 lowercase-hex
// characters (a SHA3-256 digest). It is the ONE gate that stops an untrusted or
// schema-drifted externalID — from the file-service outbox, or an operator's --hash
// argument — from becoming a directory-traversing filesystem path (a "../" resolves
// through filepath.Join and escapes the sink root / restore destDir) or an over-length
// ledger key (the file_backup_object / _target_status columns are VARCHAR(128)). Enforced
// at every ingress: the backup pipeline, restore/verify, and the filesystem sink.
func ValidateContentHash(s string) error {
	if len(s) != contentHashLen {
		return fmt.Errorf("invalid content hash: %d chars, want %d lowercase-hex", len(s), contentHashLen)
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("invalid content hash: byte %d is not lowercase hex", i)
		}
	}
	return nil
}

// PreflightKey returns a unique reserved key for a startup write-probe object. It is
// unique per run so an object-lock/WORM bucket can't reject an overwrite on restart.
func PreflightKey() string {
	var b [6]byte
	_, _ = rand.Read(b[:]) //nolint:errcheck // rand.Read never fails; the value only needs to be unique-ish
	return path.Join(preflightPrefix, fmt.Sprintf("%d-%s", time.Now().UTC().UnixNano(), hex.EncodeToString(b[:])))
}

// ShardKey returns the two-level hex-sharded key for a content hash —
// <hash[0:2]>/<hash[2:4]>/<hash> — joined with "/" (S3/URL style; filesystem
// callers convert with filepath.FromSlash). Owning this layout in ONE place
// keeps the sinks and the restore path from diverging, which would silently
// break restore (the key is re-derived from the hash).
func ShardKey(hash string) string {
	if len(hash) >= 4 {
		return path.Join(hash[0:2], hash[2:4], hash)
	}
	return hash
}

// CreateTemp opens a unique "<prefix>.*.partial" temp under dir. The .partial
// suffix is the marker any orphan sweep / reconcile keys on.
func CreateTemp(dir, prefix string) (*os.File, string, error) {
	f, err := os.CreateTemp(dir, prefix+".*.partial")
	if err != nil {
		return nil, "", fmt.Errorf("create temp: %w", err)
	}
	return f, f.Name(), nil
}

// ProbeWritable verifies dir is writable by creating then removing a throwaway ".partial"
// temp — the write-probe shared by the filesystem sink's Preflight and the reconcile
// scratch-dir check, so both use ONE policy and the same sweepable temp naming (a probe that
// leaks after a crash is caught by the same orphan sweep as any other .partial). dir="" uses
// the OS temp dir.
func ProbeWritable(dir string) error {
	f, name, err := CreateTemp(dir, preflightPrefix)
	if err != nil {
		return err
	}
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}

// CommitWrite is the durable-write spine shared by the filesystem sink and the DR
// restore path, so the temp/fsync/ctx-gate/commit policy can't drift between backup
// and restore. It: MkdirAll(dir,0755) -> temp -> fill(temp) -> fsync -> close ->
// ctx-cancel gate -> chmod(mode)+rename+dir-fsync, removing the temp on any failure
// or cancellation. fill writes the object body into the temp (an io.Copy, a decode).
func CommitWrite(ctx context.Context, dir, base string, fill func(*os.File) error) error {
	// 0755 (not 0750): blobs are chmod'd 0644 so a different-uid restore/reconcile/
	// file-service can read them, which needs world-execute to traverse the shard dirs.
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // content-addressed store, readable by the restore uid
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	f, tmp, err := CreateTemp(dir, base)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = f.Close() // idempotent after an explicit Close below
			_ = os.Remove(tmp)
		}
	}()
	if err := fill(f); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err // cancelled mid-write — don't commit a late/orphaned object
	}
	committed = true // commitFile owns the temp from here (removes it on its own error)
	return commitFile(tmp, filepath.Join(dir, base))
}

// commitFile durably publishes an already-written+closed temp to dest: chmod 0644,
// atomic rename, and a parent-dir fsync (atomic != durable). The temp is removed on
// any error so a failure never leaks it. Callers gate on ctx BEFORE calling this so
// a cancelled write is never committed. 0644 (so a different-uid restore/reconcile/
// file-service can read the blob) is fixed by design at both call sites — a constant, not
// a threaded parameter.
func commitFile(tmpName, dest string) error {
	if err := os.Chmod(tmpName, 0o644); err != nil { //nolint:gosec // content-addressed blob, must be readable by the restore uid
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename: %w", err)
	}
	return syncDir(filepath.Dir(dest))
}

// ManifestKey returns the slash-style key for a ledger-snapshot manifest object
// (`_manifest/<base>`). Owning this layout in ONE place keeps the sinks and the
// DR-restore tooling from diverging on where manifests live.
func ManifestKey(name string) string {
	return path.Join(manifestPrefix, path.Base(name))
}

// syncDir fsyncs a directory so a create/rename within it is durable
// (atomic != durable). Used after every content write.
func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // caller-provided path under a configured root
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
