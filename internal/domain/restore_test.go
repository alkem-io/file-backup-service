package domain

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestRestoreAllOneGenuineFailureAtSIGTERMCountsFailed (re-review item 3): a hash-mismatch (a REAL
// corruption) whose per-object CLASSIFICATION coincides with a parent SIGTERM must be counted
// Failed, NOT Cancelled — else `restore all` exits 0 on real corruption. The object is decoded with
// the parent LIVE (so the mismatch, not a ctx error, is the result), then the parent is cancelled
// while the worker is blocked entering its stats switch (holding the stats mutex sequences it). This
// FAILS if cancelledInFlight is reverted to the imprecise `err!=nil && parent.Err()!=nil`, which
// would bucket the mismatch as Cancelled.
func TestRestoreAllOneGenuineFailureAtSIGTERMCountsFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := newMemSink("t")
	h := hashOf("wanted")
	sink.store[h] = []byte("bytes that do NOT hash to the key") // decode → hash-mismatch (non-Canceled)
	var st RestoreAllStats
	var mu sync.Mutex
	mu.Lock() // hold the stats mutex so the worker blocks BEFORE classifying (after the decode)
	done := make(chan struct{})
	go func() { restoreAllOne(ctx, sink, h, t.TempDir(), time.Minute, &st, &mu); close(done) }()
	time.Sleep(100 * time.Millisecond) // the decode (parent live → mismatch) finishes; worker parks on mu.Lock()
	cancel()                           // a SIGTERM coinciding with the classification
	mu.Unlock()
	<-done
	if st.Failed != 1 || st.Cancelled != 0 {
		t.Fatalf("a hash-mismatch coinciding with SIGTERM must be Failed, not Cancelled: %+v", st)
	}
}

// TestDecodeStreamAbandonsWedgedSource: a filesystem source whose Fetch blocks uninterruptibly
// (a wedged mount) must NOT hang decodeStream past its ctx deadline — the abandonment wrapper
// (callWithCtx) returns ctx.Err() and abandons the stuck goroutine, so reconcile/restore/verify
// stay bounded instead of needing SIGKILL. (F1)
func TestDecodeStreamAbandonsWedgedSource(t *testing.T) {
	h := newHangingSink("wedged")
	t.Cleanup(func() { close(h.release) })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- decodeStream(ctx, h, strings.Repeat("a", 64), io.Discard, nil) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("decodeStream on a wedged source must return a ctx error, not succeed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("decodeStream HUNG on a wedged source — abandonment failed (F1 regression)")
	}
}

func TestRestoreRoundTripRaw(t *testing.T) {
	data := []byte("restore me raw")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: data}} // stored raw
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore raw: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored bytes mismatch: %v", err)
	}
	// skip-if-present: a second restore is a no-op and leaves the file intact.
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore idempotent: %v", err)
	}
}

func TestRestoreRoundTripZstd(t *testing.T) {
	data := bytes.Repeat([]byte("restore me zstd "), 100)
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := io.ReadAll(ZstdReader(bytes.NewReader(data))) // as a zstd target stores it
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: compressed}}
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore zstd: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored zstd bytes mismatch: %v", err)
	}
	if err := VerifyObject(context.Background(), sink, h); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// TestRestoreRawStartingWithZstdMagic exercises the magic-collision fallback: a
// raw-stored object whose plaintext begins with the zstd magic (e.g. a .zst upload
// stored with CodecNone) must still restore, by re-reading as raw.
func TestRestoreRawStartingWithZstdMagic(t *testing.T) {
	data := append([]byte{0x28, 0xB5, 0x2F, 0xFD}, []byte(" not a real zstd frame payload")...)
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: data}} // stored raw
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore raw-with-zstd-magic: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("restored bytes mismatch: %v", err)
	}
}

// TestRestoreRawOversizedZstdFrame guards the errBomb regression: a raw-stored object
// whose bytes are a VALID zstd frame that decodes PAST the cap must still restore by
// falling back to the (bounded, bomb-safe) raw read — not be rejected as a bomb. The
// cap is set between the frame's raw size and its decoded size so decodeZstd over-caps
// while decodeRaw fits.
func TestRestoreRawOversizedZstdFrame(t *testing.T) {
	plaintext := bytes.Repeat([]byte("x"), 500)
	frame, err := io.ReadAll(ZstdReader(bytes.NewReader(plaintext))) // small frame, decodes to 500
	if err != nil {
		t.Fatal(err)
	}
	h, err := sum(bytes.NewReader(frame)) // stored RAW: the FRAME bytes hash to h
	if err != nil {
		t.Fatal(err)
	}
	old := maxObjectBytes
	maxObjectBytes = int64(len(frame)) + 4 // >= frame (raw fallback fits), < 500 (decode over-caps)
	defer func() { maxObjectBytes = old }()

	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: frame}}
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err != nil {
		t.Fatalf("restore raw oversized-zstd-frame: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
	if err != nil || !bytes.Equal(got, frame) {
		t.Fatalf("restored bytes mismatch: %v", err)
	}
}

func TestRestoreCorruptFails(t *testing.T) {
	h := hashOf("corrupt") // a valid content-address whose stored bytes are garbage
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: []byte("garbage not zstd")}}
	dir := t.TempDir()
	if err := RestoreObject(context.Background(), sink, h, dir); err == nil {
		t.Fatal("expected integrity error on a corrupt object")
	}
	if _, err := os.Stat(filepath.Join(dir, h)); err == nil {
		t.Fatal("corrupt object must not be written to dest")
	}
}

// TestRestoreAllRoundTripAndResume: restore all objects the ledger records on a target, verify
// the on-disk bytes, then re-run to prove idempotence (every object skipped-as-present, resumable).
func TestRestoreAllRoundTripAndResume(t *testing.T) {
	led := newFakeLedger()
	sink := newMemSink("t")
	want := map[string][]byte{}
	for i := 0; i < 6; i++ {
		content := []byte("restore-all object " + string(rune('A'+i)))
		h, err := sum(bytes.NewReader(content))
		if err != nil {
			t.Fatal(err)
		}
		sink.store[h] = content
		want[h] = content
		_ = led.RecordBackup(context.Background(), ObjectMeta{ExternalID: h}, []TargetStatus{{Target: "t", State: StateStored}})
	}
	dir := t.TempDir()
	st, err := RestoreAll(context.Background(), led, sink, "t", dir, 3, time.Minute)
	if err != nil {
		t.Fatalf("RestoreAll: %v", err)
	}
	if st.Restored != 6 || st.Skipped != 0 || st.Failed != 0 {
		t.Fatalf("first pass: want restored=6, got %+v", st)
	}
	for h, content := range want {
		got, rerr := os.ReadFile(filepath.Join(dir, h)) //nolint:gosec // test temp path
		if rerr != nil || !bytes.Equal(got, content) {
			t.Fatalf("restored bytes mismatch for %s: %v", h, rerr)
		}
	}
	// Re-run: every object is already present + intact → skipped (idempotent, resumable).
	st2, err := RestoreAll(context.Background(), led, sink, "t", dir, 3, time.Minute)
	if err != nil {
		t.Fatalf("RestoreAll re-run: %v", err)
	}
	if st2.Restored != 0 || st2.Skipped != 6 {
		t.Fatalf("second pass must skip all (idempotent), got %+v", st2)
	}
}

// TestRestoreAllCountsFailure: a corrupt source object (bytes don't hash to the key) is counted
// as failed and does not abort the whole pass — the other objects still restore.
func TestRestoreAllCountsFailure(t *testing.T) {
	led := newFakeLedger()
	sink := newMemSink("t")
	var hashes []string
	for i := 0; i < 3; i++ {
		content := []byte("obj " + string(rune('0'+i)))
		h, _ := sum(bytes.NewReader(content))
		sink.store[h] = content
		hashes = append(hashes, h)
		_ = led.RecordBackup(context.Background(), ObjectMeta{ExternalID: h}, []TargetStatus{{Target: "t", State: StateStored}})
	}
	sink.store[hashes[0]] = []byte("garbage that does not hash to the key")
	st, err := RestoreAll(context.Background(), led, sink, "t", t.TempDir(), 2, time.Minute)
	if err != nil {
		t.Fatalf("RestoreAll: %v", err)
	}
	if st.Failed != 1 || st.Restored != 2 {
		t.Fatalf("want failed=1 restored=2, got %+v", st)
	}
}

// TestRestoreAllEnumerationError: a ledger enumeration failure aborts the whole-store restore with
// the error, rather than silently reporting a clean (empty) pass.
func TestRestoreAllEnumerationError(t *testing.T) {
	led := errStoredLedger{newFakeLedger()}
	if _, err := RestoreAll(context.Background(), led, newMemSink("t"), "t", t.TempDir(), 2, time.Minute); err == nil {
		t.Fatal("a ledger enumeration error must propagate from RestoreAll")
	}
}

// TestRestoreAllCancelledReturnsCtxError (review #3): a cancelled whole-store restore returns the
// ctx error (which the CLI maps to a clean exit — a resumable interruption, not a failure), rather
// than swallowing the cancellation and reporting the in-flight cancels as genuine failures.
func TestRestoreAllCancelledReturnsCtxError(t *testing.T) {
	led := newFakeLedger()
	sink := newMemSink("t")
	for i := 0; i < 5; i++ {
		content := []byte("cancel me " + string(rune('a'+i)))
		h, _ := sum(bytes.NewReader(content))
		sink.store[h] = content
		_ = led.RecordBackup(context.Background(), ObjectMeta{ExternalID: h}, []TargetStatus{{Target: "t", State: StateStored}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RestoreAll(ctx, led, sink, "t", t.TempDir(), 1, time.Minute); err == nil {
		t.Fatal("a cancelled restore-all must return the ctx error so the CLI can map it to a clean, resumable exit")
	}
}

// TestRestoreAllCountsCancelNotFailure (review Cluster 1): a cancellation that lands WHILE an object
// restores is counted as Cancelled, NOT Failed — so a clean SIGTERM doesn't manufacture a false
// "restore failed" verdict.
func TestRestoreAllCountsCancelNotFailure(t *testing.T) {
	led := newFakeLedger()
	content := []byte("restore me")
	h, _ := sum(bytes.NewReader(content))
	base := newMemSink("t")
	base.store[h] = content
	_ = led.RecordBackup(context.Background(), ObjectMeta{ExternalID: h}, []TargetStatus{{Target: "t", State: StateStored}})

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel the parent ctx the moment the object's Fetch begins → the in-flight restore is aborted.
	sink := &cancelOnFetchSink{memSink: memSink{stubSink: stubSink{name: "t"}, store: map[string][]byte{h: content}}, cancel: cancel}
	st, _ := RestoreAll(ctx, led, sink, "t", t.TempDir(), 1, time.Minute)
	if st.Failed != 0 {
		t.Fatalf("an in-flight cancellation must NOT be counted as a genuine failure, got Failed=%d", st.Failed)
	}
	if st.Cancelled == 0 && st.Restored == 0 {
		t.Fatalf("the object should be counted cancelled (or restored if it beat the cancel), got %+v", st)
	}
}

// ioErrSink serves prefix bytes then a transient I/O error — modelling an S3 reset/timeout
// mid-stream. prefix must begin with the zstd magic so decodeStream takes the zstd path.
type ioErrSink struct {
	stubSink
	prefix []byte
	err    error
}

func (s *ioErrSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(io.MultiReader(bytes.NewReader(s.prefix), errReader{s.err})), nil
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// TestRestorePropagatesSourceIOError: a genuinely zstd-stored object whose SOURCE read fails
// mid-stream (S3 reset/timeout) must surface a RETRYABLE I/O error, NOT the false 'neither
// valid zstd nor the plaintext' corruption verdict the raw fallback would otherwise produce.
func TestRestorePropagatesSourceIOError(t *testing.T) {
	data := bytes.Repeat([]byte("restore me zstd "), 200)
	h, _ := sum(bytes.NewReader(data))
	compressed, err := io.ReadAll(ZstdReader(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	if len(compressed) < 16 {
		t.Skip("compressed object too small to fail mid-stream")
	}
	boom := errors.New("connection reset by peer")
	// Serve the first half (incl. the zstd magic) then fail — a real source I/O fault.
	sink := &ioErrSink{stubSink: stubSink{name: "s"}, prefix: compressed[:len(compressed)/2], err: boom}
	rerr := RestoreObject(context.Background(), sink, h, t.TempDir())
	if rerr == nil {
		t.Fatal("expected the mid-stream source I/O error to surface")
	}
	if !errors.Is(rerr, boom) {
		t.Fatalf("must propagate the retryable source I/O error: %v", rerr)
	}
	if strings.Contains(rerr.Error(), "neither valid zstd nor the plaintext") {
		t.Fatalf("a transient I/O error must NOT be reported as permanent corruption: %v", rerr)
	}
}
