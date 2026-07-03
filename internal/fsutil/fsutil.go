// Package fsutil holds small, stdlib-only filesystem + content-addressing
// helpers shared by the sink adapters and the domain restore path. It is a leaf
// package with zero project dependencies, so the domain importing it does not
// violate the hexagonal rule (it is not "infrastructure").
package fsutil

import (
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
