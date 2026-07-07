package filesystem

import (
	"bytes"
	"context"
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
