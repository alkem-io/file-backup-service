package fsutil

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// TestIsTimestampedManifest locks in that a manifest name is accepted ONLY when it both carries
// the `.jsonl` suffix AND parses as the fixed-width ManifestName layout — so a stray `.jsonl`
// (which sorts ABOVE every real 2026-…Z name) can never be picked as "newest". This is the ONE
// naming rule shared by the s3 + filesystem sinks and OpenLatestManifest.
func TestIsTimestampedManifest(t *testing.T) {
	// A hand-written genuine name, and a name freshly stamped from the canonical layout (the exact
	// shape domain.ManifestName produces) must both be accepted.
	if !IsTimestampedManifest("2026-06-01T000000.000000000Z.jsonl") {
		t.Fatal("a genuine timestamped manifest name must be accepted")
	}
	live := time.Now().UTC().Format(manifestTimeLayout) + manifestSuffix
	if !IsTimestampedManifest(live) {
		t.Fatalf("a name from the canonical layout %q must be accepted", live)
	}

	for _, bad := range []string{
		"backup.jsonl",                       // .jsonl suffix but NOT a timestamp — must not sort above real names
		"LATEST",                             // the pointer object, not a manifest
		"2026-06-01T000000.000000000Z.txt",   // right timestamp prefix, wrong suffix
		"2026-06-01T000000.000000000Z",       // a timestamp with no suffix
		"2026-13-01T000000.000000000Z.jsonl", // .jsonl but an impossible month → parse fails
		"not-a-date.jsonl",
		"",
	} {
		if IsTimestampedManifest(bad) {
			t.Fatalf("%q must be rejected as a timestamped manifest", bad)
		}
	}
}

// The selection tests below exercise OpenLatestManifest's fast-path/scan selection through the public
// entry point (OpenLatestManifest was inlined into it). openManifestStub/readAllClose are defined
// alongside the other OpenLatestManifest tests.

// TestOpenLatestPointerCurrentBoundedList: a VALID pointer with NOTHING newer (the bounded
// listFrom(pointer) returns empty) is opened directly, and the staleness check lists AFTER the pointer
// (a bounded StartAfter, not a full scan).
func TestOpenLatestPointerCurrentBoundedList(t *testing.T) {
	const valid = "2026-06-01T000000.000000000Z.jsonl"
	var listedAfter string
	rc, err := OpenLatestManifest(
		func() (string, bool) { return valid, true },
		func(after string) ([]string, error) { listedAfter = after; return nil, nil },
		openManifestStub(map[string]bool{valid: true}, nil),
	)
	if err != nil {
		t.Fatalf("current pointer must not error: %v", err)
	}
	if got := readAllClose(t, rc); got != valid {
		t.Fatalf("a current pointer must be opened, got %q want %q", got, valid)
	}
	if listedAfter != valid {
		t.Fatalf("the staleness check must list AFTER the pointer (%q), listed after %q", valid, listedAfter)
	}
}

// TestOpenLatestStalePointerOverridden: the pointer write is best-effort, so it can be STALE. If a
// NEWER timestamped manifest exists (the bounded listFrom returns it), the newer one is opened instead
// of the stale pointer — else the inventory diff would miss an orphan added after the stale pointer.
func TestOpenLatestStalePointerOverridden(t *testing.T) {
	const stale = "2026-03-01T000000.000000000Z.jsonl"
	const newer = "2026-06-01T000000.000000000Z.jsonl"
	rc, err := OpenLatestManifest(
		func() (string, bool) { return stale, true },
		func(after string) ([]string, error) {
			if after != stale {
				t.Fatalf("must list after the stale pointer %q, got %q", stale, after)
			}
			return []string{newer, "backup.jsonl"}, nil // a newer valid manifest exists; ignore the stray
		},
		openManifestStub(map[string]bool{newer: true, stale: true}, nil),
	)
	if err != nil {
		t.Fatalf("stale-pointer override must not error: %v", err)
	}
	if got := readAllClose(t, rc); got != newer {
		t.Fatalf("a stale pointer must be overridden by the newer manifest, got %q want %q", got, newer)
	}
}

// TestOpenLatestScanIgnoresStrayAndPointer: an INVALID pointer forces a full scan (from ""), which
// picks the highest VALID timestamped name while ignoring a stray `backup.jsonl` (sorts above every
// real name) and the `LATEST` pointer object.
func TestOpenLatestScanIgnoresStrayAndPointer(t *testing.T) {
	const newest = "2026-06-01T000000.000000000Z.jsonl"
	rc, err := OpenLatestManifest(
		func() (string, bool) { return "backup.jsonl", true }, // present but not timestamped → full scan
		func(after string) ([]string, error) {
			if after != "" {
				t.Fatalf("an invalid pointer must trigger a full scan (after==\"\"), got %q", after)
			}
			return []string{
				"2026-01-01T000000.000000000Z.jsonl",
				newest, // newest VALID
				"2026-03-01T000000.000000000Z.jsonl",
				"backup.jsonl", // stray — lexically ABOVE any 2026-… name; must be ignored
				"LATEST",       // the pointer object itself
			}, nil
		},
		openManifestStub(map[string]bool{newest: true, "2026-01-01T000000.000000000Z.jsonl": true, "2026-03-01T000000.000000000Z.jsonl": true}, nil),
	)
	if err != nil {
		t.Fatalf("scan fallback must not error: %v", err)
	}
	if got := readAllClose(t, rc); got != newest {
		t.Fatalf("scan opened %q, want the highest VALID timestamped name %q", got, newest)
	}
}

// TestOpenLatestListErrorPropagates: a listFrom error (a read-deny / gone container) propagates
// unchanged — the caller reports Unverifiable, not "no manifest".
func TestOpenLatestListErrorPropagates(t *testing.T) {
	boom := errors.New("list read denied")
	_, err := OpenLatestManifest(
		func() (string, bool) { return "", false },
		func(string) ([]string, error) { return nil, boom },
		openManifestStub(nil, nil),
	)
	if !errors.Is(err, boom) {
		t.Fatalf("listFrom error must propagate, got %v", err)
	}
}

// TestOpenLatestEmptyOrAllInvalid: with no names — or only non-timestamped ones — the result is a
// wrapped os.ErrNotExist, which the caller maps to NoData (benign).
func TestOpenLatestEmptyOrAllInvalid(t *testing.T) {
	_, err := OpenLatestManifest(
		func() (string, bool) { return "", false },
		func(string) ([]string, error) { return nil, nil },
		openManifestStub(nil, nil),
	)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("an empty scan must be os.ErrNotExist, got %v", err)
	}
	_, err = OpenLatestManifest(
		func() (string, bool) { return "", false },
		func(string) ([]string, error) { return []string{"backup.jsonl", "LATEST", "notes.txt"}, nil },
		openManifestStub(nil, nil),
	)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("only-invalid names must be os.ErrNotExist, got %v", err)
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

// openManifestStub models a sink's open(name) for OpenLatestManifest: a reader for a name in
// `present`, the configured error for a name in `denied`, else os.ErrNotExist (this manifest is gone).
func openManifestStub(present map[string]bool, denied map[string]error) func(string) (io.ReadCloser, error) {
	return func(name string) (io.ReadCloser, error) {
		if err, ok := denied[name]; ok {
			return nil, err
		}
		if present[name] {
			return io.NopCloser(strings.NewReader(name)), nil
		}
		return nil, os.ErrNotExist // this specific manifest has been deleted
	}
}

func readAllClose(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	_ = rc.Close()
	return string(b)
}

// TestOpenLatestManifestPointerTipExists: the fast path — a valid pointer whose tip still exists is
// opened directly (no fallback scan).
func TestOpenLatestManifestPointerTipExists(t *testing.T) {
	const tip = "2026-06-01T000000.000000000Z.jsonl"
	rc, err := OpenLatestManifest(
		func() (string, bool) { return tip, true },
		func(string) ([]string, error) { return nil, nil }, // nothing newer than the pointer
		openManifestStub(map[string]bool{tip: true}, nil),
	)
	if err != nil {
		t.Fatalf("open tip must succeed: %v", err)
	}
	if got := readAllClose(t, rc); got != tip {
		t.Fatalf("opened %q, want the tip %q", got, tip)
	}
}

// TestOpenLatestManifestNoneIsNotExist: no manifest at all → os.ErrNotExist (the caller maps to NoData).
func TestOpenLatestManifestNoneIsNotExist(t *testing.T) {
	_, err := OpenLatestManifest(
		func() (string, bool) { return "", false },
		func(string) ([]string, error) { return nil, nil },
		openManifestStub(nil, nil),
	)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no manifest must be os.ErrNotExist (→ NoData), got %v", err)
	}
}

// TestOpenLatestManifestDeletedTipFallsBackToOlder: the pointer names a since-DELETED tip while an
// OLDER manifest survives — OpenLatestManifest must full-scan and return the newest SURVIVING manifest,
// not read the vanished tip as NoData (which would miss an orphan the older manifest still reveals).
func TestOpenLatestManifestDeletedTipFallsBackToOlder(t *testing.T) {
	const tip = "2026-06-01T000000.000000000Z.jsonl"   // pointed-at, DELETED
	const older = "2026-03-01T000000.000000000Z.jsonl" // survives
	rc, err := OpenLatestManifest(
		func() (string, bool) { return tip, true },
		func(after string) ([]string, error) {
			if after == "" { // the fallback full scan
				return []string{older, "backup.jsonl"}, nil // stray must be ignored by latestFirst
			}
			return nil, nil // the bounded staleness check finds nothing newer than the tip
		},
		openManifestStub(map[string]bool{older: true}, nil), // tip gone, older survives
	)
	if err != nil {
		t.Fatalf("deleted-tip fallback must succeed: %v", err)
	}
	if got := readAllClose(t, rc); got != older {
		t.Fatalf("fallback opened %q, want the newest surviving %q", got, older)
	}
}

// TestOpenLatestManifestAllGoneIsNotExist: the tip AND every fallback candidate are gone → os.ErrNotExist.
func TestOpenLatestManifestAllGoneIsNotExist(t *testing.T) {
	const tip = "2026-06-01T000000.000000000Z.jsonl"
	const older = "2026-03-01T000000.000000000Z.jsonl"
	_, err := OpenLatestManifest(
		func() (string, bool) { return tip, true },
		func(after string) ([]string, error) {
			if after == "" {
				return []string{older}, nil
			}
			return nil, nil
		},
		openManifestStub(nil, nil), // everything gone
	)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("all manifests gone must be os.ErrNotExist (→ NoData), got %v", err)
	}
}

// TestOpenLatestManifestReadDenySurfaces: a non-ErrNotExist open error on the selected tip (a read-deny
// / gone container) surfaces unchanged (Unverifiable), NOT swallowed into the NoData fallback.
func TestOpenLatestManifestReadDenySurfaces(t *testing.T) {
	const tip = "2026-06-01T000000.000000000Z.jsonl"
	denied := errors.New("403 read denied")
	_, err := OpenLatestManifest(
		func() (string, bool) { return tip, true },
		func(string) ([]string, error) { return nil, nil },
		openManifestStub(nil, map[string]error{tip: denied}),
	)
	if !errors.Is(err, denied) {
		t.Fatalf("a read-deny must surface (Unverifiable), got %v", err)
	}
}

// TestOpenLatestManifestFallbackListErrorSurfaces: a listFrom error DURING the deleted-tip fallback
// scan is surfaced (Unverifiable), not swallowed.
func TestOpenLatestManifestFallbackListErrorSurfaces(t *testing.T) {
	const tip = "2026-06-01T000000.000000000Z.jsonl"
	boom := errors.New("scan denied")
	_, err := OpenLatestManifest(
		func() (string, bool) { return tip, true },
		func(after string) ([]string, error) {
			if after == "" {
				return nil, boom // the fallback scan itself fails
			}
			return nil, nil
		},
		openManifestStub(nil, nil), // tip gone → triggers the fallback scan
	)
	if !errors.Is(err, boom) {
		t.Fatalf("fallback scan error must surface, got %v", err)
	}
}

// TestOpenLatestManifestFallbackReadDenySurfaces: during the fallback scan, a candidate whose open
// returns a non-ErrNotExist error surfaces it (Unverifiable) rather than skipping past it.
func TestOpenLatestManifestFallbackReadDenySurfaces(t *testing.T) {
	const tip = "2026-06-01T000000.000000000Z.jsonl"
	const older = "2026-03-01T000000.000000000Z.jsonl"
	denied := errors.New("403 on older")
	_, err := OpenLatestManifest(
		func() (string, bool) { return tip, true },
		func(after string) ([]string, error) {
			if after == "" {
				return []string{older}, nil
			}
			return nil, nil
		},
		openManifestStub(nil, map[string]error{older: denied}), // tip gone (ErrNotExist), older read-denied
	)
	if !errors.Is(err, denied) {
		t.Fatalf("a read-deny on a fallback candidate must surface, got %v", err)
	}
}

// TestLatestFirst: newest-first order, dropping non-timestamped strays (never picked as newest).
func TestLatestFirst(t *testing.T) {
	got := latestFirst([]string{
		"2026-01-01T000000.000000000Z.jsonl",
		"backup.jsonl", // stray — dropped
		"2026-06-01T000000.000000000Z.jsonl",
		"LATEST", // dropped
		"2026-03-01T000000.000000000Z.jsonl",
	})
	want := []string{
		"2026-06-01T000000.000000000Z.jsonl",
		"2026-03-01T000000.000000000Z.jsonl",
		"2026-01-01T000000.000000000Z.jsonl",
	}
	if len(got) != len(want) {
		t.Fatalf("latestFirst got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("latestFirst[%d]=%q want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}
