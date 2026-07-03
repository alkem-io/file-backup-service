// Package filesystem implements the Sink port over a POSIX path, sharded by a
// two-level hex prefix of the content hash.
package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Sink stores objects on a filesystem path.
type Sink struct {
	name string
	root string
}

// New constructs a filesystem Sink rooted at root.
func New(name, root string) *Sink { return &Sink{name: name, root: root} }

// Name returns the target name.
func (s *Sink) Name() string { return s.name }

func (s *Sink) pathFor(hash string) string {
	if len(hash) >= 4 {
		return filepath.Join(s.root, hash[0:2], hash[2:4], hash)
	}
	return filepath.Join(s.root, hash)
}

// Exists reports whether the object is present.
func (s *Sink) Exists(_ context.Context, hash string) (bool, error) {
	_, err := os.Stat(s.pathFor(hash))
	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, fmt.Errorf("stat: %w", err)
	}
}

// Store writes bytes for hash (atomic temp+rename). Dedup is the pipeline's job
// (via the ledger); Store always writes so the stream is consumed and the real
// byte count is returned — an identical existing object is just overwritten
// atomically.
//
// TODO(T012): chmod to the configured mode (664) after rename.
func (s *Sink) Store(_ context.Context, hash string, r io.Reader, _ int64) (int64, error) {
	dest := s.pathFor(hash)
	dir := filepath.Dir(dest)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return 0, fmt.Errorf("mkdir: %w", err)
	}
	// Unique temp per writer (not <hash>.partial): two concurrent Stores of the
	// same hash must not truncate/interleave a shared temp file.
	f, err := os.CreateTemp(dir, hash+".*.partial")
	if err != nil {
		return 0, fmt.Errorf("create temp: %w", err)
	}
	tmp := f.Name()
	n, copyErr := io.Copy(f, r)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := firstErr(copyErr, syncErr, closeErr); err != nil {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("write temp: %w", err)
	}
	// 0644 so a verify/reconcile job or file-service (different uid) can read the
	// backup blob, not just the worker (CreateTemp defaults to 0600).
	if err := os.Chmod(tmp, 0o644); err != nil { //nolint:gosec // content-addressed backup blob
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("rename: %w", err)
	}
	// fsync the directory so the rename survives power-loss — atomic != durable.
	if err := syncDir(dir); err != nil {
		return 0, fmt.Errorf("sync dir: %w", err)
	}
	return n, nil
}

// Fetch opens the stored object.
func (s *Sink) Fetch(_ context.Context, hash string) (io.ReadCloser, error) {
	f, err := os.Open(s.pathFor(hash)) //nolint:gosec // path derived from a validated hash
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return f, nil
}

// PutManifest writes a ledger snapshot object under _manifest/ atomically
// (temp + fsync + rename + dir fsync) so a crash mid-write can't leave a
// truncated manifest a DR restore would trust.
func (s *Sink) PutManifest(_ context.Context, name string, r io.Reader) error {
	dir := filepath.Join(s.root, "_manifest")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("mkdir manifest: %w", err)
	}
	dest := filepath.Join(dir, filepath.Base(name))
	f, err := os.CreateTemp(dir, "manifest.*.partial")
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	tmp := f.Name()
	_, copyErr := io.Copy(f, r)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := firstErr(copyErr, syncErr, closeErr); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename manifest: %w", err)
	}
	return syncDir(dir)
}

// syncDir fsyncs a directory so a rename/create within it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // dir is under the configured root
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
