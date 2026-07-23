package domain

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

// ---- restore.go: rewindTruncate + the zstd->raw reset failure -------------

// TestRewindTruncateSeekError: rewindTruncate on a CLOSED file surfaces the Seek error rather
// than silently proceeding to a Truncate that would operate on a bad descriptor — the raw
// fallback must not re-decode into a non-rewound temp.
func TestRewindTruncateSeekError(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "rt")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	_ = f.Close() // a closed file makes Seek fail
	if err := rewindTruncate(f); err == nil {
		t.Fatal("rewindTruncate on a closed file must surface the Seek error")
	}
}

// magicGarbageSink serves bytes that BEGIN with the zstd frame magic but are not a decodable
// zstd frame, so decodeZstd fails to decode+hash and signals errTryRaw (the raw fallback).
type magicGarbageSink struct{ stubSink }

func (magicGarbageSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	b := append(append([]byte{}, zstdMagic...), []byte{0xff, 0xff, 0xff, 0xff, 0x00, 0x01}...)
	return io.NopCloser(bytes.NewReader(b)), nil
}

// TestDecodeStreamResetError: on the zstd->raw fallback (the bytes carried the magic but didn't
// decode as zstd), a failure in the reset callback (the caller's temp rewind) must propagate —
// re-decoding into a non-rewound buffer would corrupt the restored object, so the reset error
// aborts instead.
func TestDecodeStreamResetError(t *testing.T) {
	resetErr := errors.New("rewind failed")
	err := decodeStreamInner(context.Background(), magicGarbageSink{}, strings.Repeat("a", 64),
		io.Discard, func() error { return resetErr })
	if !errors.Is(err, resetErr) {
		t.Fatalf("a reset failure in the zstd->raw fallback must propagate, got %v", err)
	}
}

// ---- reconcile.go: tempReadCloser.Close surfaces a remove failure ---------

// TestTempReadCloserRemoveErrorJoined: a non-IsNotExist remove failure on Close (here: the temp
// path is a NON-EMPTY directory, so os.Remove returns ENOTEMPTY) must be JOINED into Close's
// error, not swallowed — a persistent scratch-dir delete failure on a long DR pass has to be
// observable, not a silent temp-file leak.
func TestTempReadCloserRemoveErrorJoined(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "nonempty")
	if err := os.Mkdir(sub, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sub, "child"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write child: %v", err)
	}
	f, err := os.Open(sub) //nolint:gosec // opens a directory the test just created under t.TempDir()
	if err != nil {
		t.Fatalf("open dir: %v", err)
	}
	trc := &tempReadCloser{f: f}
	if err := trc.Close(); err == nil {
		t.Fatal("Close must surface a non-IsNotExist remove failure, not swallow it")
	}
}

// ---- audit.go: a cancellation landing mid-page aborts the target ----------

// onePageLedger yields a single stored id so auditTarget reaches the existsPage probe.
type onePageLedger struct{ stubLedger }

func (onePageLedger) StoredExternalIDsPage(context.Context, string, string, int) ([]string, error) {
	return []string{"h1"}, nil
}

// cancelOnExistsSink cancels the audit context from inside an Exists probe, simulating a
// SIGINT/timeout that lands WHILE a page is being probed.
type cancelOnExistsSink struct {
	stubSink
	cancel context.CancelFunc
}

func (s cancelOnExistsSink) Exists(context.Context, string) (bool, error) {
	s.cancel()
	return true, nil
}

// TestAuditTargetCancelledMidPage: if cancellation lands after StoredExternalIDsPage but during
// the concurrent Exists probes, auditTarget must NOT tally the in-flight tainted probes as a clean
// (Verified) pass — the cancelled sweep classifies as benign NoData (the cmd's top-level ctx.Err()
// fold surfaces the abort), never a false green that would hide silent loss.
func TestAuditTargetCancelledMidPage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := cancelOnExistsSink{stubSink: stubSink{name: "t"}, cancel: cancel}
	v := auditTarget(ctx, onePageLedger{}, Target{Sink: sink}, 0, "")
	if v.Status == StatusVerified {
		t.Fatalf("a cancellation mid-page must NOT read as a Verified pass, got %+v", v)
	}
	if v.Status != StatusNoData {
		t.Fatalf("a cancelled sweep must classify benign NoData at the domain level, got %+v", v)
	}
}
