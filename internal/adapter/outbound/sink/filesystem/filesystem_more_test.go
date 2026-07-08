package filesystem

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// errBoom is the mid-copy failure injected by errAfterReader.
var errBoom = errors.New("boom mid-copy")

// errAfterReader yields one short chunk then a hard error, so an io.Copy inside
// Store fails partway through — exercising the atomic-write abort path.
type errAfterReader struct{ done bool }

func (e *errAfterReader) Read(p []byte) (int, error) {
	if e.done {
		return 0, errBoom
	}
	e.done = true
	return copy(p, []byte("partial")), nil
}

// TestExistsFalseBeforeStoreAndFetchMissing: a never-stored object reads as absent
// (false, nil), a Fetch of it errors (os.Open miss — the sink's not-found path), and
// after a Store it flips to present. Complements the round-trip test in
// filesystem_test.go, which only checks the present-after-Store side. (S1)
func TestExistsFalseBeforeStoreAndFetchMissing(t *testing.T) {
	root := t.TempDir()
	s := New("fs", root)
	ctx := context.Background()
	hash := strings.Repeat("b", 64)

	ok, err := s.Exists(ctx, hash)
	if err != nil {
		t.Fatalf("Exists before store: unexpected err %v", err)
	}
	if ok {
		t.Fatal("Exists must report false before the object is stored")
	}
	if _, err := s.Fetch(ctx, hash); err == nil {
		t.Fatal("Fetch of a never-stored object must return an error (open: no such file)")
	}

	if _, err := s.Store(ctx, hash, bytes.NewReader([]byte("x"))); err != nil {
		t.Fatalf("store: %v", err)
	}
	ok, err = s.Exists(ctx, hash)
	if err != nil || !ok {
		t.Fatalf("Exists after store must be (true, nil), got ok=%v err=%v", ok, err)
	}
}

// TestPutManifestWritesUnderManifestDir: PutManifest lands under _manifest/<name>,
// is byte-for-byte readable, and a 0-byte ledger snapshot is a legitimate manifest
// (empty-safe). (S1)
func TestPutManifestWritesUnderManifestDir(t *testing.T) {
	root := t.TempDir()
	s := New("fs", root)
	ctx := context.Background()

	data := []byte(`{"objects":1}`)
	if err := s.PutManifest(ctx, "snapshot-42", bytes.NewReader(data)); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "_manifest", "snapshot-42")) //nolint:gosec // test reads a file it just wrote under t.TempDir
	if err != nil {
		t.Fatalf("manifest not readable under _manifest/: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("manifest content mismatch: %q != %q", got, data)
	}

	if err := s.PutManifest(ctx, "empty-snapshot", bytes.NewReader(nil)); err != nil {
		t.Fatalf("PutManifest(empty): %v", err)
	}
	info, err := os.Stat(filepath.Join(root, "_manifest", "empty-snapshot"))
	if err != nil {
		t.Fatalf("empty manifest not written: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("empty manifest size = %d, want 0", info.Size())
	}
}

// TestPreflightCreatesWritableRoot: Preflight on a not-yet-existing, creatable path
// succeeds and creates the (nested) root — the startup writability probe. (S1)
func TestPreflightCreatesWritableRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "backup-root")
	s := New("fs", root)
	if err := s.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight on a creatable, writable root must succeed: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		t.Fatalf("Preflight must MkdirAll the root: info=%v err=%v", info, err)
	}
}

// TestPreflightUncreatableRoot: a root path UNDER a regular file can never be
// mkdir'd (ENOTDIR), so Preflight fails loudly at startup rather than dead-lettering
// every object. (S1)
func TestPreflightUncreatableRoot(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	s := New("fs", filepath.Join(file, "sub", "root"))
	if err := s.Preflight(context.Background()); err == nil {
		t.Fatal("Preflight on a root under a regular file must error (mkdir ENOTDIR)")
	}
}

// TestPreflightUnwritableRoot: a root that exists but is not writable fails the
// write-probe (ProbeWritable) branch of Preflight. Skipped as root, which bypasses
// directory write permissions. (S1)
func TestPreflightUnwritableRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: directory write permissions are not enforced")
	}
	root := filepath.Join(t.TempDir(), "ro-root")
	if err := os.Mkdir(root, 0o500); err != nil { // r-x, no write bit
		t.Fatalf("setup: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o700) }) //nolint:gosec // restore dir perms so t.TempDir cleanup can remove it
	s := New("fs", root)
	if err := s.Preflight(context.Background()); err == nil {
		t.Fatal("Preflight on a read-only root must error (not writable)")
	}
}

// TestPreflightCancelledContext: Preflight honors a cancelled ctx and returns the
// context error without starting an uninterruptible MkdirAll on a wedged mount. (S1)
func TestPreflightCancelledContext(t *testing.T) {
	root := t.TempDir()
	s := New("fs", root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Preflight(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Preflight with a cancelled ctx must return the ctx error, got %v", err)
	}
}

// TestStoreReaderErrorLeavesNoPartialFile: when the source reader errors mid-copy,
// Store fails AND the atomic-write contract holds — the destination is never
// committed and no orphan ".partial" temp is left behind. (S1)
func TestStoreReaderErrorLeavesNoPartialFile(t *testing.T) {
	root := t.TempDir()
	s := New("fs", root)
	hash := strings.Repeat("c", 64)

	if _, err := s.Store(context.Background(), hash, &errAfterReader{}); err == nil {
		t.Fatal("Store must fail when the source reader errors mid-copy")
	}

	dest := filepath.Join(root, hash[0:2], hash[2:4], hash)
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("a mid-copy failure must not commit the dest file; stat err = %v", statErr)
	}

	shardDir := filepath.Join(root, hash[0:2], hash[2:4])
	entries, err := os.ReadDir(shardDir)
	if err != nil {
		t.Fatalf("read shard dir: %v", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".partial") {
			t.Fatalf("a failed Store left an orphan temp: %s", e.Name())
		}
	}
}
