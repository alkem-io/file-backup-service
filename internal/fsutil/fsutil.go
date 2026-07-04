// Package fsutil holds small, stdlib-only filesystem + content-addressing
// helpers shared by the sink adapters and the domain restore path. It is a leaf
// package with zero project dependencies, so the domain importing it does not
// violate the hexagonal rule (it is not "infrastructure").
package fsutil

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
)

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

// CommitWrite is the durable-write spine shared by the filesystem sink and the DR
// restore path, so the temp/fsync/ctx-gate/commit policy can't drift between backup
// and restore. It: MkdirAll(dir,0755) -> temp -> fill(temp) -> fsync -> close ->
// ctx-cancel gate -> chmod(mode)+rename+dir-fsync, removing the temp on any failure
// or cancellation. fill writes the object body into the temp (an io.Copy, a decode).
func CommitWrite(ctx context.Context, dir, base string, mode os.FileMode, fill func(*os.File) error) error {
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
	committed = true // CommitFile owns the temp from here (removes it on its own error)
	return CommitFile(tmp, filepath.Join(dir, base), mode)
}

// CommitFile durably publishes an already-written+closed temp to dest: chmod mode,
// atomic rename, and a parent-dir fsync (atomic != durable). The temp is removed on
// any error so a failure never leaks it. Callers gate on ctx BEFORE calling this so
// a cancelled write is never committed.
func CommitFile(tmpName, dest string, mode os.FileMode) error {
	if err := os.Chmod(tmpName, mode); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename: %w", err)
	}
	return SyncDir(filepath.Dir(dest))
}

// ManifestKey returns the slash-style key for a ledger-snapshot manifest object
// (`_manifest/<base>`). Owning this layout in ONE place keeps the sinks and the
// DR-restore tooling from diverging on where manifests live.
func ManifestKey(name string) string {
	return path.Join("_manifest", path.Base(name))
}

// SyncDir fsyncs a directory so a create/rename within it is durable
// (atomic != durable). Used after every content write.
func SyncDir(dir string) error {
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
