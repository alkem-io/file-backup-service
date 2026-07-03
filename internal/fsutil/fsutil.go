// Package fsutil holds small, stdlib-only filesystem + content-addressing
// helpers shared by the sink adapters and the domain restore path. It is a leaf
// package with zero project dependencies, so the domain importing it does not
// violate the hexagonal rule (it is not "infrastructure").
package fsutil

import (
	"os"
	"path"
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
