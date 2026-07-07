// Package filesystem implements the Sink port over a POSIX path, sharded by a
// two-level hex prefix of the content hash.
package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/alkem-io/file-backup-service/internal/fsutil"
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

// osPath maps a slash-style fsutil key (ShardKey/ManifestKey) to an OS path under the
// root — the one place root-joining + slash conversion lives, so objects and
// manifests can't diverge on how the root is applied.
func (s *Sink) osPath(key string) string {
	return filepath.Join(s.root, filepath.FromSlash(key))
}

func (s *Sink) pathFor(hash string) string { return s.osPath(fsutil.ShardKey(hash)) }

// Preflight verifies the root exists and is writable, so a missing mount / wrong
// path / read-only volume fails loudly at startup instead of dead-lettering every
// object.
func (s *Sink) Preflight(ctx context.Context) error {
	// Honor ctx like the s3/fileservice preflights (an os.MkdirAll on a wedged mount can't be
	// interrupted mid-syscall, but the startup runner abandons a hung Preflight on the deadline;
	// checking ctx first avoids even starting one after a cancel).
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.root, 0o755); err != nil { //nolint:gosec // backup store readable by the restore uid
		return fmt.Errorf("filesystem preflight %q: mkdir %s: %w", s.name, s.root, err)
	}
	f, name, err := fsutil.CreateTemp(s.root, ".preflight")
	if err != nil {
		return fmt.Errorf("filesystem preflight %q: %s not writable: %w", s.name, s.root, err)
	}
	_ = f.Close()
	_ = os.Remove(name)
	return nil
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
func (s *Sink) Store(ctx context.Context, hash string, r io.Reader) (int64, error) {
	dest := s.pathFor(hash)
	return writeAtomic(ctx, filepath.Dir(dest), filepath.Base(dest), r)
}

// Fetch opens the stored object.
func (s *Sink) Fetch(_ context.Context, hash string) (io.ReadCloser, error) {
	f, err := os.Open(s.pathFor(hash)) //nolint:gosec // path derived from a validated hash
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return f, nil
}

// PutManifest writes a ledger snapshot object under _manifest/ atomically so a
// crash mid-write can't leave a truncated manifest a DR restore would trust. It
// uses the same 0644 durable spine as Store — a DR restore on a different uid
// must be able to read it.
func (s *Sink) PutManifest(ctx context.Context, name string, r io.Reader) error {
	dest := s.osPath(fsutil.ManifestKey(name))
	_, err := writeAtomic(ctx, filepath.Dir(dest), filepath.Base(dest), r)
	return err
}

// writeAtomic streams r into dir/base (0644) via the shared fsutil.CommitWrite durable
// spine (mkdir → unique temp → fsync → chmod → rename → dir-fsync, ctx-gated so a
// cancelled write is removed rather than committed). Returns the bytes written.
func writeAtomic(ctx context.Context, dir, base string, r io.Reader) (int64, error) {
	var n int64
	err := fsutil.CommitWrite(ctx, dir, base, func(f *os.File) error {
		var cerr error
		n, cerr = io.Copy(f, r)
		return cerr
	})
	return n, err
}
