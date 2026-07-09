package filesystem

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSinkRejectsTraversalHash: a hash that is not a 64-hex content address must be rejected
// by every path-deriving sink method BEFORE it becomes an OS path, so a "../" can never
// escape the sink root. This is the last line of the path-traversal defense at the
// filesystem boundary (the domain validates at its ingress too). (V1)
func TestSinkRejectsTraversalHash(t *testing.T) {
	root := t.TempDir()
	s := New("fs", root)
	ctx := context.Background()
	for _, bad := range []string{"../../../../etc/passwd", "", "abc", strings.Repeat("A", 64)} {
		if _, err := s.Fetch(ctx, bad); err == nil {
			t.Fatalf("Fetch(%q) must reject a non-content-address hash", bad)
		}
		if _, err := s.Exists(ctx, bad); err == nil {
			t.Fatalf("Exists(%q) must reject a non-content-address hash", bad)
		}
		if _, err := s.Store(ctx, bad, bytes.NewReader([]byte("x"))); err == nil {
			t.Fatalf("Store(%q) must reject a non-content-address hash", bad)
		}
	}
	// Confirm nothing was written outside the root by the rejected Stores.
	if _, err := os.Stat("/etc/passwd_fbs_probe"); err == nil {
		t.Fatal("a rejected Store must not have written outside the root")
	}
}

// TestLatestManifestPicksNewestViaPointer: PutManifest writes the _manifest/LATEST pointer, so
// LatestManifest resolves the newest snapshot via the pointer (fast path) — even when an OLDER
// manifest sorts higher by name, the pointer (not a name-sort) decides.
func TestLatestManifestPicksNewestViaPointer(t *testing.T) {
	root := t.TempDir()
	s := New("fs", root)
	ctx := context.Background()
	newest := []byte(`{"externalID":"newest"}` + "\n")
	if err := s.PutManifest(ctx, "2026-01-01T000000.000000000Z.jsonl", bytes.NewReader([]byte("old\n"))); err != nil {
		t.Fatalf("put old manifest: %v", err)
	}
	if err := s.PutManifest(ctx, "2026-06-01T000000.000000000Z.jsonl", bytes.NewReader(newest)); err != nil {
		t.Fatalf("put new manifest: %v", err)
	}
	// The pointer now names the second (newest) manifest. Corrupt it to prove the pointer is USED
	// (a name-sort fallback would still find "2026-06-...", so re-point at the OLD one and expect it).
	if err := os.WriteFile(filepath.Join(root, "_manifest", "LATEST"), []byte("2026-01-01T000000.000000000Z.jsonl"), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatalf("repoint: %v", err)
	}
	rc, err := s.LatestManifest(ctx)
	if err != nil {
		t.Fatalf("LatestManifest: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil || !bytes.Equal(got, []byte("old\n")) {
		t.Fatalf("LatestManifest must honor the pointer (the OLD manifest), got %q err=%v", got, err)
	}
}

// TestLatestManifestFallbackScanFiltersStray: with NO pointer (old data written before the
// pointer), LatestManifest falls back to the highest `.jsonl` and IGNORES a stray non-manifest file
// so it can't pick it as "latest".
func TestLatestManifestFallbackScanFiltersStray(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "_manifest")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	newest := []byte(`{"externalID":"newest"}` + "\n")
	// Two manifests written DIRECTLY (no pointer), plus a stray file that sorts ABOVE any .jsonl.
	for name, body := range map[string][]byte{
		"2026-01-01T000000.000000000Z.jsonl": []byte("old\n"),
		"2026-06-01T000000.000000000Z.jsonl": newest,
		"zzz-stray.txt":                      []byte("not a manifest"),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o644); err != nil { //nolint:gosec // test fixture
			t.Fatalf("write %s: %v", name, err)
		}
	}
	rc, err := New("fs", root).LatestManifest(context.Background())
	if err != nil {
		t.Fatalf("LatestManifest (fallback): %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, newest) {
		t.Fatalf("fallback scan must pick the newest .jsonl (ignoring the stray), got %q", got)
	}
}

// TestLatestManifestNoneIsNotExist: no manifest dir (or an empty one) is a wrapped os.ErrNotExist,
// which the audit maps to "unverifiable — nothing to diff", not a failure.
func TestLatestManifestNoneIsNotExist(t *testing.T) {
	s := New("fs", t.TempDir())
	if _, err := s.LatestManifest(context.Background()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a missing manifest dir must wrap os.ErrNotExist, got %v", err)
	}
	// A manifest dir with only non-.jsonl entries also yields ErrNotExist (nothing to diff).
	root := t.TempDir()
	s2 := New("fs", root)
	dir := filepath.Join(root, "_manifest")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatalf("write: %v", err)
	}
	if _, err := s2.LatestManifest(context.Background()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a manifest dir with no .jsonl must be os.ErrNotExist, got %v", err)
	}
}

// TestLatestManifestGoneRootIsError: a sink rooted at a path that does NOT exist (a detached mount)
// must surface a NON-os.ErrNotExist error — a disappeared target is Unverifiable (it has lost
// EVERYTHING), NOT the benign "no manifest yet" (os.ErrNotExist → NoData). confirmRoot draws that
// distinction; without it a gone mount would read as an empty, healthy target.
func TestLatestManifestGoneRootIsError(t *testing.T) {
	goneRoot := filepath.Join(t.TempDir(), "never-created") // a subdir that is never mkdir'd
	_, err := New("fs", goneRoot).LatestManifest(context.Background())
	if err == nil {
		t.Fatal("LatestManifest on a gone root must error, not succeed")
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a gone root must NOT be a benign os.ErrNotExist (that is NoData); got %v", err)
	}
}

// TestLatestManifestReadDirError: a _manifest path that is a FILE (not a dir) makes ReadDir fail
// with a non-ErrNotExist error, which must propagate (not be masked as "no manifest").
func TestLatestManifestReadDirError(t *testing.T) {
	root := t.TempDir()
	// Put a regular file where the _manifest dir would be → ReadDir returns ENOTDIR.
	if err := os.WriteFile(filepath.Join(root, "_manifest"), []byte("x"), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatalf("write blocker: %v", err)
	}
	_, err := New("fs", root).LatestManifest(context.Background())
	if err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a non-ErrNotExist ReadDir error must propagate, got %v", err)
	}
}

// TestSinkRoundTripValidHash: a well-formed content address stores, reports present, and
// reads back the exact bytes — the basic filesystem-sink contract (the adapter layer was
// previously untested; S1).
func TestSinkRoundTripValidHash(t *testing.T) {
	root := t.TempDir()
	s := New("fs", root)
	ctx := context.Background()
	hash := strings.Repeat("a", 64) // valid 64-lowercase-hex format
	data := []byte("backup me")

	if _, err := s.Store(ctx, hash, bytes.NewReader(data)); err != nil {
		t.Fatalf("store: %v", err)
	}
	ok, err := s.Exists(ctx, hash)
	if err != nil || !ok {
		t.Fatalf("exists after store: ok=%v err=%v", ok, err)
	}
	// The object lands under the two-level shard, inside the root.
	if _, err := os.Stat(filepath.Join(root, hash[0:2], hash[2:4], hash)); err != nil {
		t.Fatalf("stored object not at the sharded path: %v", err)
	}
	rc, err := s.Fetch(ctx, hash)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: %q != %q", got, data)
	}
}
