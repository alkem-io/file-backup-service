// Package filesystem implements the Sink port over a POSIX path, sharded by a
// two-level hex prefix of the content hash.
package filesystem

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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

// pathFor resolves a content hash to its on-disk path, rejecting any hash that is not a
// well-formed content address BEFORE it is joined to the root — the actual chokepoint for
// the path-traversal class (a "../" in an unvalidated hash resolves through filepath.Join
// and escapes s.root). The domain validates at its ingress too; this is the last line at
// the filesystem boundary, so the traversal cannot happen regardless of caller.
func (s *Sink) pathFor(hash string) (string, error) {
	if err := fsutil.ValidateContentHash(hash); err != nil {
		return "", fmt.Errorf("filesystem %q: %w", s.name, err)
	}
	return s.osPath(fsutil.ShardKey(hash)), nil
}

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
	if err := fsutil.ProbeWritable(s.root); err != nil {
		return fmt.Errorf("filesystem preflight %q: %s not writable: %w", s.name, s.root, err)
	}
	return nil
}

// Exists reports whether the object is present. A missing object is reported absent ONLY once the
// root is confirmed present: a detached/gone root (a mount that vanished) makes EVERY object's
// os.Stat return IsNotExist, so without this guard the ledger→target audit would read the whole
// target as silent loss (missing>0 → Drift). A gone root instead surfaces as an ERROR, which the
// audit classifies as Unverifiable — the filesystem analogue of the s3 sink's confirmBucket-on-404.
func (s *Sink) Exists(_ context.Context, hash string) (bool, error) {
	path, err := s.pathFor(hash)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		if rerr := s.confirmRoot(); rerr != nil {
			return false, rerr // the "object" absence is really a gone/unreachable root
		}
		return false, nil // root present → the object is genuinely absent
	default:
		return false, fmt.Errorf("stat: %w", err)
	}
}

// Store writes bytes for hash (atomic temp+rename). Dedup is the pipeline's job
// (via the ledger); Store always writes so the stream is consumed and the real
// byte count is returned — an identical existing object is just overwritten
// atomically.
func (s *Sink) Store(ctx context.Context, hash string, r io.Reader) (int64, error) {
	dest, err := s.pathFor(hash)
	if err != nil {
		return 0, err
	}
	return writeAtomic(ctx, filepath.Dir(dest), filepath.Base(dest), r)
}

// Fetch opens the stored object.
func (s *Sink) Fetch(_ context.Context, hash string) (io.ReadCloser, error) {
	path, err := s.pathFor(hash) // validates hash → path can't traverse out of s.root
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path) //nolint:gosec // pathFor validated the hash is 64-hex, so no traversal
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return f, nil
}

// PutManifest writes a ledger snapshot object under _manifest/ atomically so a crash mid-write
// can't leave a truncated manifest a DR restore would trust, then BEST-EFFORT overwrites the
// `_manifest/LATEST` pointer with the snapshot's name so a reader single-opens the pointer instead of
// scanning the dir. It uses the same 0644 durable spine as Store — a DR restore on a different uid
// must be able to read it. The pointer is only a read-time OPTIMIZATION (OpenLatestManifest falls
// back to a dir scan when it is absent/stale, so it is self-healing) — therefore a pointer-only
// write failure must NOT fail the whole PutManifest: the manifest object itself is durably written.
func (s *Sink) PutManifest(ctx context.Context, name string, r io.Reader) error {
	dest := s.osPath(fsutil.ManifestKey(name))
	if _, err := writeAtomic(ctx, filepath.Dir(dest), filepath.Base(dest), r); err != nil {
		return err
	}
	ptr := s.osPath(fsutil.ManifestLatestKey())
	// Best-effort pointer update: swallow a failure (the scan fallback covers correctness).
	_, _ = writeAtomic(ctx, filepath.Dir(ptr), filepath.Base(ptr), strings.NewReader(name))
	return nil
}

// LatestManifest opens the newest ledger-snapshot manifest under _manifest/ — the filesystem
// sink's half of domain's optional inventoryReader capability (audit target→ledger). The newest name
// is resolved by the shared fsutil.OpenLatestManifest (pointer fast-path, else a dir scan filtered
// to VALID timestamped manifests), so s3 and filesystem can't diverge on selection. Filesystem
// targets have no object-lock, so this sink deliberately does NOT implement CheckImmutability — a
// filesystem WORM target is reported unverifiable for the drift check, which is correct (POSIX has no
// bucket object-lock to read).
//
// Selection + the stale/DELETED-pointer fallback (if the selected tip is gone, scan for the newest
// SURVIVING manifest) are owned by the shared fsutil.OpenLatestManifest, so s3 and filesystem can't
// diverge. Parity with the s3 sink: a present root with no manifest yet returns a wrapped
// os.ErrNotExist (the domain maps that to NoData — benign), while a GONE root (a detached mount) is
// caught by the confirmRoot above and returns a NON-ErrNotExist error so the audit reports the target
// Unverifiable rather than benignly "no manifest" — a disappeared target has NOT lost nothing.
func (s *Sink) LatestManifest(_ context.Context) (io.ReadCloser, error) {
	if err := s.confirmRoot(); err != nil {
		return nil, err
	}
	return fsutil.OpenLatestManifest(s.readManifestPointer, s.listManifestNamesAfter,
		func(name string) (io.ReadCloser, error) {
			f, err := os.Open(s.osPath(fsutil.ManifestKey(name))) //nolint:gosec // name is a manifest base name under the configured root, not caller-supplied
			if err != nil {
				if os.IsNotExist(err) { // named by the pointer/scan but vanished — the resolver tries an older one
					return nil, fmt.Errorf("manifest %s vanished: %w", name, os.ErrNotExist)
				}
				return nil, fmt.Errorf("open manifest %s: %w", name, err)
			}
			return f, nil
		})
}

// confirmRoot verifies the sink root still exists and is a directory — the filesystem analogue of
// the s3 sink's confirmBucket, so a detached mount surfaces as an ERROR (Unverifiable) instead of
// being read as an empty/no-manifest target (NoData). A missing root deliberately does NOT wrap
// os.ErrNotExist: a gone mount is a target-level fault, not the benign "no manifest yet".
func (s *Sink) confirmRoot() error {
	info, err := os.Stat(s.root)
	switch {
	case err == nil && info.IsDir():
		return nil
	case err == nil:
		return fmt.Errorf("filesystem root %s is not a directory (misconfigured mount?)", s.root)
	case os.IsNotExist(err):
		return fmt.Errorf("filesystem root %s is gone (detached mount?)", s.root)
	default:
		return fmt.Errorf("stat filesystem root %s: %w", s.root, err)
	}
}

// readManifestPointer reads the `_manifest/LATEST` pointer's raw contents (a manifest base name),
// returning ok=false when the pointer is absent/unreadable/empty. The name is VALIDATED by
// OpenLatestManifest, not here.
func (s *Sink) readManifestPointer() (string, bool) {
	b, err := os.ReadFile(s.osPath(fsutil.ManifestLatestKey())) //nolint:gosec // fixed pointer key under the configured root
	if err != nil {
		return "", false
	}
	name := strings.TrimSpace(string(b))
	return name, name != ""
}

// listManifestNamesAfter returns every non-dir base name under _manifest/ STRICTLY AFTER `after` (so
// OpenLatestManifest can bound its stale-pointer check to newer manifests; `after==""` = all). A
// missing manifest dir is not necessarily benign: the ROOT could have vanished between confirmRoot and
// this ReadDir (a TOCTOU on a detached mount), so an ENOENT re-checks the root — a gone root → an
// ERROR (Unverifiable), a present root with no manifest dir yet → benign NoData (no names).
func (s *Sink) listManifestNamesAfter(after string) ([]string, error) {
	dir := s.osPath(fsutil.ManifestDir())
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			if rerr := s.confirmRoot(); rerr != nil {
				return nil, rerr // the root vanished after confirmRoot (TOCTOU) → not benign
			}
			return nil, nil // present root, no manifest dir yet → benign NoData
		}
		return nil, fmt.Errorf("read manifest dir %s: %w", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if base := e.Name(); !e.IsDir() && base > after {
			names = append(names, base)
		}
	}
	return names, nil
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
