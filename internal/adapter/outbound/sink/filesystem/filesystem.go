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

// Store writes bytes for hash if absent (atomic temp+rename).
//
// TODO(T012): chmod to the configured mode (664) after rename.
func (s *Sink) Store(ctx context.Context, hash string, r io.Reader, _ int64) (int64, error) {
	if ok, err := s.Exists(ctx, hash); err != nil {
		return 0, err
	} else if ok {
		return 0, nil // dedup
	}
	dest := s.pathFor(hash)
	if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
		return 0, fmt.Errorf("mkdir: %w", err)
	}
	tmp := dest + ".partial"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // path derived from a validated content hash under the configured root
	if err != nil {
		return 0, fmt.Errorf("create temp: %w", err)
	}
	n, copyErr := io.Copy(f, r)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := firstErr(copyErr, syncErr, closeErr); err != nil {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("write temp: %w", err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("rename: %w", err)
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

// PutManifest writes a ledger snapshot object under _manifest/.
func (s *Sink) PutManifest(_ context.Context, name string, r io.Reader) error {
	dest := filepath.Join(s.root, "_manifest", filepath.Base(name))
	if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
		return fmt.Errorf("mkdir manifest: %w", err)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600) //nolint:gosec // dest is <root>/_manifest/<base>, not user-controlled
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	_, copyErr := io.Copy(f, r)
	closeErr := f.Close()
	if err := firstErr(copyErr, closeErr); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
