package fsutil

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fillBytes returns a fill func that writes b into the temp — the realistic
// "copy the object body" shape CommitWrite expects.
func fillBytes(b []byte) func(*os.File) error {
	return func(f *os.File) error {
		_, err := f.Write(b)
		return err
	}
}

// noPartialLeftover asserts dir holds no ".partial" temp — the invariant that
// every CommitWrite path (commit, failure, cancellation) removes its scratch
// file so the orphan sweep has nothing to reap.
func noPartialLeftover(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".partial") {
			t.Fatalf("leftover temp %q in %s", e.Name(), dir)
		}
	}
}

func TestValidateContentHashInvariants(t *testing.T) {
	valid := strings.Repeat("0123456789abcdef", 4) // 64 lowercase-hex chars
	if len(valid) != contentHashLen {
		t.Fatalf("test fixture wrong length: %d", len(valid))
	}
	if err := ValidateContentHash(valid); err != nil {
		t.Fatalf("valid 64-hex hash rejected: %v", err)
	}

	// Wrong length is rejected regardless of charset.
	for _, s := range []string{"", "abc", strings.Repeat("a", 63), strings.Repeat("a", 65)} {
		if err := ValidateContentHash(s); err == nil {
			t.Fatalf("hash of length %d accepted, want rejected", len(s))
		}
	}

	// Correct length but a non-lowercase-hex byte is rejected. This is the gate
	// that stops a traversal ('/') or an over-length/uppercase externalID from
	// becoming a filesystem path — assert each dangerous class at 64 chars.
	for name, bad := range map[string]string{
		"uppercase":     strings.Repeat("a", 63) + "A",
		"letter-g":      strings.Repeat("a", 63) + "g",
		"slash":         strings.Repeat("a", 63) + "/", // '/' is 0x2f, just below '0'
		"dot":           strings.Repeat("a", 63) + ".",
		"between-9-a":   strings.Repeat("a", 63) + ":", // ':' is 0x3a, between '9' and 'a'
		"leading-slash": "/" + strings.Repeat("a", 63),
	} {
		if len(bad) != contentHashLen {
			t.Fatalf("%s fixture wrong length: %d", name, len(bad))
		}
		if err := ValidateContentHash(bad); err == nil {
			t.Fatalf("%s hash %q accepted, want rejected", name, bad)
		}
	}
}

func TestShardKeyFallbackAndFull(t *testing.T) {
	full := strings.Repeat("0123456789abcdef", 4) // 64 chars
	if got, want := ShardKey(full), full[0:2]+"/"+full[2:4]+"/"+full; got != want {
		t.Fatalf("ShardKey(64-char) = %q, want %q", got, want)
	}

	// Exactly 4 chars still shards (>= 4 boundary).
	if got := ShardKey("abcd"); got != "ab/cd/abcd" {
		t.Fatalf("ShardKey(\"abcd\") = %q, want ab/cd/abcd", got)
	}

	// Short inputs (< 4) fall through unchanged — never a "/"-joined key.
	for _, s := range []string{"", "a", "ab", "abc"} {
		if got := ShardKey(s); got != s {
			t.Fatalf("ShardKey(%q) = %q, want the input unchanged", s, got)
		}
	}
}

func TestManifestKeyStripsPathDir(t *testing.T) {
	if got := ManifestKey("a/b/c.json"); got != manifestPrefix+"/c.json" {
		t.Fatalf("ManifestKey(\"a/b/c.json\") = %q, want %s/c.json", got, manifestPrefix)
	}
}

func TestCreateTempHappy(t *testing.T) {
	dir := t.TempDir()
	f, name, err := CreateTemp(dir, "obj")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer func() { _ = os.Remove(name) }()

	if !strings.HasSuffix(name, ".partial") {
		t.Fatalf("temp name %q must end in .partial", name)
	}
	if got := filepath.Dir(name); got != dir {
		t.Fatalf("temp is under %q, want under %q", got, dir)
	}
	if base := filepath.Base(name); !strings.HasPrefix(base, "obj") {
		t.Fatalf("temp base %q must start with the prefix", base)
	}
	// The returned handle is open and writable.
	if _, err := f.Write([]byte("x")); err != nil {
		t.Fatalf("write to returned temp: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close returned temp: %v", err)
	}
}

func TestCreateTempError(t *testing.T) {
	f, name, err := CreateTemp(filepath.Join(t.TempDir(), "does-not-exist"), "obj")
	if err == nil {
		t.Fatalf("CreateTemp in a non-existent dir must error")
	}
	if f != nil || name != "" {
		t.Fatalf("on error, want (nil, \"\"), got (%v, %q)", f, name)
	}
}

func TestProbeWritableHappy(t *testing.T) {
	dir := t.TempDir()
	if err := ProbeWritable(dir); err != nil {
		t.Fatalf("ProbeWritable(writable dir): %v", err)
	}
	// The probe must clean up after itself.
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(ents) != 0 {
		t.Fatalf("ProbeWritable left %d file(s) behind: %v", len(ents), ents)
	}
}

func TestProbeWritableError(t *testing.T) {
	if err := ProbeWritable(filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatalf("ProbeWritable on a non-existent dir must error")
	}
}

func TestCommitWriteHappy(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "shard")
	payload := []byte("hello content \x00\x01 world")

	if err := CommitWrite(context.Background(), dir, "blob", fillBytes(payload)); err != nil {
		t.Fatalf("CommitWrite: %v", err)
	}

	dest := filepath.Join(dir, "blob")
	info, err := os.Stat(dest)
	if err != nil {
		t.Fatalf("committed file missing: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o644 {
		t.Fatalf("committed file mode = %o, want 0644", perm)
	}
	got, err := os.ReadFile(dest) //nolint:gosec // G304: path built under t.TempDir()
	if err != nil {
		t.Fatalf("read committed file: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("committed bytes = %q, want %q", got, payload)
	}
	noPartialLeftover(t, dir)
}

func TestCommitWriteFillError(t *testing.T) {
	dir := t.TempDir()
	errFill := errors.New("fill boom")

	err := CommitWrite(context.Background(), dir, "blob", func(_ *os.File) error {
		return errFill
	})
	if !errors.Is(err, errFill) {
		t.Fatalf("CommitWrite error = %v, want to wrap %v", err, errFill)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "blob")); !os.IsNotExist(statErr) {
		t.Fatalf("blob must not be committed when fill fails (stat err = %v)", statErr)
	}
	noPartialLeftover(t, dir)
}

func TestCommitWriteContextCancelled(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled BEFORE the ctx gate, AFTER fill runs

	// fill still writes a full temp; the ctx gate must refuse to publish it.
	err := CommitWrite(ctx, dir, "blob", fillBytes([]byte("late bytes")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CommitWrite error = %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "blob")); !os.IsNotExist(statErr) {
		t.Fatalf("blob must not be committed after cancellation (stat err = %v)", statErr)
	}
	noPartialLeftover(t, dir)
}

func TestCommitWriteMkdirError(t *testing.T) {
	root := t.TempDir()
	notDir := filepath.Join(root, "iamafile")
	if err := os.WriteFile(notDir, []byte("x"), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	// A file sits where MkdirAll needs a directory component.
	dir := filepath.Join(notDir, "sub")

	err := CommitWrite(context.Background(), dir, "blob", fillBytes([]byte("x")))
	if err == nil || !strings.Contains(err.Error(), "mkdir") {
		t.Fatalf("CommitWrite error = %v, want a mkdir failure", err)
	}
}

func TestCommitWriteBadBaseSeparator(t *testing.T) {
	dir := t.TempDir()
	// A base with a path separator can't become a temp pattern; CommitWrite
	// surfaces the CreateTemp failure rather than writing outside dir.
	err := CommitWrite(context.Background(), dir, "bad/base", fillBytes([]byte("x")))
	if err == nil {
		t.Fatalf("CommitWrite with a separator in base must error")
	}
	noPartialLeftover(t, dir)
}

func TestCommitWriteSyncErrorWhenFillClosesHandle(t *testing.T) {
	dir := t.TempDir()

	// A misbehaving fill that closes the handle makes the subsequent Sync fail;
	// nothing is committed and the temp is swept.
	err := CommitWrite(context.Background(), dir, "blob", func(f *os.File) error {
		return f.Close()
	})
	if err == nil {
		t.Fatalf("CommitWrite must error when the handle is closed before sync")
	}
	if _, statErr := os.Stat(filepath.Join(dir, "blob")); !os.IsNotExist(statErr) {
		t.Fatalf("blob must not be committed on sync failure (stat err = %v)", statErr)
	}
	noPartialLeftover(t, dir)
}

func TestCommitWriteRenameError(t *testing.T) {
	dir := t.TempDir()
	// A directory already occupies the destination path, so the atomic rename of
	// the temp over it fails.
	dest := filepath.Join(dir, "blob")
	if err := os.Mkdir(dest, 0o750); err != nil {
		t.Fatalf("seed dest dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dest, "child"), []byte("x"), 0o600); err != nil {
		t.Fatalf("seed dest child: %v", err)
	}

	err := CommitWrite(context.Background(), dir, "blob", fillBytes([]byte("x")))
	if err == nil || !strings.Contains(err.Error(), "rename") {
		t.Fatalf("CommitWrite error = %v, want a rename failure", err)
	}
	// The rename failure must not leak the scratch temp.
	noPartialLeftover(t, dir)
}

// The next three defend commitFile/syncDir failure modes that CommitWrite can't
// reach through its public happy/error paths on a normal filesystem. They are
// unexported but in-package, so exercise them directly to lock in that a
// chmod/open/sync failure is surfaced (never swallowed).

func TestCommitFileChmodErrorOnMissingTemp(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone.partial")
	err := commitFile(missing, filepath.Join(t.TempDir(), "dest"))
	if err == nil || !strings.Contains(err.Error(), "chmod") {
		t.Fatalf("commitFile on a missing temp = %v, want a chmod failure", err)
	}
}

func TestSyncDirOpenError(t *testing.T) {
	if err := syncDir(filepath.Join(t.TempDir(), "no-such-dir")); err == nil {
		t.Fatalf("syncDir on a non-existent dir must error")
	}
}

func TestSyncDirSyncError(t *testing.T) {
	const devNull = "/dev/null"
	if _, err := os.Stat(devNull); err != nil {
		t.Skipf("%s unavailable on this platform: %v", devNull, err)
	}
	// /dev/null opens fine but does not support fsync, so syncDir must surface
	// the sync error rather than report a false-durable success.
	if err := syncDir(devNull); err == nil {
		t.Fatalf("syncDir(%s) must surface the fsync failure", devNull)
	}
}
