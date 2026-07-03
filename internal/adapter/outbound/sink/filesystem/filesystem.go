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

func (s *Sink) pathFor(hash string) string {
	return filepath.Join(s.root, filepath.FromSlash(fsutil.ShardKey(hash)))
}

// Preflight verifies the root exists and is writable, so a missing mount / wrong
// path / read-only volume fails loudly at startup instead of dead-lettering every
// object.
func (s *Sink) Preflight(_ context.Context) error {
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
func (s *Sink) Store(ctx context.Context, hash string, r io.Reader, _ int64) (int64, error) {
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
	_, err := writeAtomic(ctx, filepath.Join(s.root, "_manifest"), filepath.Base(name), r)
	return err
}

// writeAtomic streams r into dir/base durably: MkdirAll, a unique temp per writer
// (two concurrent Stores of the same hash must not share a temp), fsync, chmod
// 0644 (so a verify/reconcile job or file-service on a different uid can read the
// blob — CreateTemp defaults to 0600), atomic rename, and a parent-dir fsync
// (atomic != durable). Returns the bytes written.
//
// The commit is gated on ctx: if the caller cancelled while this was blocked in a
// hung write/fsync (the storeWithCtx abandon path), we DELETE the temp and refuse
// to rename rather than committing an orphaned write. The bytes are
// content-addressed so a late commit would be harmless, but not committing keeps
// the store honest and reclaims the temp instead of leaking it. The still-blocked
// syscall itself can't be interrupted from Go (a regular-file fd close waits on the
// in-flight op) — that residual is an OS limit, not a policy choice.
func writeAtomic(ctx context.Context, dir, base string, r io.Reader) (int64, error) {
	// 0755 (not 0750): the blobs are chmod'd 0644 precisely so a different-uid DR
	// restore / reconcile / file-service can read them — which requires world-execute
	// on the shard dirs to traverse. Matches the restore path (RestoreObject 0755).
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // content-addressed backup store, readable by the restore uid
		return 0, fmt.Errorf("mkdir: %w", err)
	}
	f, tmp, err := fsutil.CreateTemp(dir, base)
	if err != nil {
		return 0, err
	}
	n, copyErr := io.Copy(f, r)
	syncErr := f.Sync()
	closeErr := f.Close()
	if err := firstErr(copyErr, syncErr, closeErr); err != nil {
		_ = os.Remove(tmp)
		return 0, fmt.Errorf("write temp: %w", err)
	}
	if err := ctx.Err(); err != nil {
		_ = os.Remove(tmp) // cancelled mid-store — don't commit a late/orphaned write
		return 0, err
	}
	if err := fsutil.CommitFile(tmp, filepath.Join(dir, base), 0o644); err != nil {
		return 0, err
	}
	return n, nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}
