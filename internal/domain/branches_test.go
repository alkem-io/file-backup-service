package domain

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// stubLedger is a no-op Ledger base: embed it and override only the method a fake needs, so a
// new port method is one edit here, not one per fake (mirrors stubSink). Distinct from
// backup_test.go's fakeLedger, which enforces FK/dedup invariants; this is for error injection.
type stubLedger struct{}

func (stubLedger) RecordBackup(context.Context, ObjectMeta, []TargetStatus) error { return nil }
func (stubLedger) StoredTargets(context.Context, string) (map[string]bool, error) {
	return map[string]bool{}, nil
}
func (stubLedger) Probe(context.Context) error { return nil }
func (stubLedger) StoredObjectsPage(context.Context, string, string, int) ([]ObjectMeta, error) {
	return nil, nil
}
func (stubLedger) StoredExternalIDsPage(context.Context, string, string, int) ([]string, error) {
	return nil, nil
}
func (stubLedger) TargetGaps(context.Context, []string, func(string, map[string]bool) error) error {
	return nil
}

// ---------- transform.go: ParseCodec ----------

// TestParseCodec is the single owner of the compression vocabulary: "" and "none" map to
// CodecNone, "zstd" to CodecZstd, and anything else errors (so a typo can't silently disable
// compression).
func TestParseCodec(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Codec
	}{
		{"", CodecNone},
		{"none", CodecNone},
		{"zstd", CodecZstd},
	} {
		got, err := ParseCodec(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("ParseCodec(%q) = %q,%v; want %q,nil", tc.in, got, err, tc.want)
		}
	}
	if _, err := ParseCodec("gzip"); err == nil {
		t.Fatal("ParseCodec must reject an unknown codec")
	}
}

// ---------- tick.go: TickLoop ----------

// TestTickLoopRunsUntilCancel: TickLoop runs fn immediately and then every interval, exiting
// only when ctx is cancelled — the background-ticker skeleton the samplers/reaper depend on.
func TestTickLoopRunsUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var n int32
	done := make(chan struct{})
	go func() {
		TickLoop(ctx, time.Millisecond, 0, func(context.Context) error {
			if atomic.AddInt32(&n, 1) >= 3 {
				cancel() // stop after a few ticks
			}
			return nil
		}, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("TickLoop did not exit on ctx cancel")
	}
	if atomic.LoadInt32(&n) < 3 {
		t.Fatalf("TickLoop must run fn repeatedly, got %d ticks", atomic.LoadInt32(&n))
	}
}

// ---------- keyset.go: KeysetLoop ----------

// TestKeysetLoopPagesAndStops drives the after-cursor + short-page-stops loop directly: it must
// advance the cursor across full pages, invoke fn per item, and stop on a short page.
func TestKeysetLoopPagesAndStops(t *testing.T) {
	all := []int{1, 2, 3, 4, 5} // pageSize 2 → pages [1,2] [3,4] [5(short)]
	var seen []int
	err := KeysetLoop(0, 2,
		func(after, limit int) ([]int, error) {
			out := []int{}
			for _, v := range all {
				if v > after {
					out = append(out, v)
					if len(out) >= limit {
						break
					}
				}
			}
			return out, nil
		},
		func(v int) int { return v }, // cursorOf
		func(v int) error { seen = append(seen, v); return nil })
	if err != nil {
		t.Fatalf("KeysetLoop: %v", err)
	}
	if len(seen) != 5 || seen[0] != 1 || seen[4] != 5 {
		t.Fatalf("KeysetLoop must visit every item in order across pages, got %v", seen)
	}
}

// TestKeysetLoopPropagatesErrors: a pageFn error and an fn error must both stop the sweep and
// surface — a hand-rolled copy that swallowed either would silently truncate the pass.
func TestKeysetLoopPropagatesErrors(t *testing.T) {
	boom := errors.New("page boom")
	if err := KeysetLoop(0, 2,
		func(int, int) ([]int, error) { return nil, boom },
		func(int) int { return 0 },
		func(int) error { return nil }); !errors.Is(err, boom) {
		t.Fatalf("a pageFn error must propagate, got %v", err)
	}
	fnBoom := errors.New("fn boom")
	if err := KeysetLoop(0, 2,
		func(after, _ int) ([]int, error) { return []int{after + 1, after + 2}, nil },
		func(v int) int { return v },
		func(int) error { return fnBoom }); !errors.Is(err, fnBoom) {
		t.Fatalf("an fn error must propagate, got %v", err)
	}
}

// ---------- sampler.go: SampleRPO coverage-read failure ----------

// TestSampleRPOCoverageReadFails: a LastVerifiedAge failure must be recorded as a sample error
// (returned) while the successful backlog sibling gauge is still set.
func TestSampleRPOCoverageReadFails(t *testing.T) {
	g := &fakeGauges{}
	s := newSampler(
		fakeBacklog{pending: 5, age: 1},
		fakeCoverage{lvErr: errors.New("coverage boom")},
		nil, g)
	if err := s.SampleRPO(context.Background()); err == nil {
		t.Fatal("a failed LastVerifiedAge read must return an error")
	}
	if !g.backlogSet {
		t.Fatal("the healthy backlog gauge must still be set")
	}
	if g.neverSet || g.lastSuccessSet {
		t.Fatal("a failed coverage read must not set its gauges")
	}
}

// ---------- audit.go ----------

// TestAuditFailErrFlagsMissing: a nonzero silent-loss count fails the audit verdict.
func TestAuditFailErrFlagsMissing(t *testing.T) {
	rep := AuditReport{Targets: []TargetAudit{{Target: "t", Checked: 3, Missing: 2}}}
	if err := rep.FailErr(); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("a missing object must fail the audit verdict, got %v", err)
	}
}

// TestRandKeysetStart: the rotating sampled-audit start is a content-hash-shaped 64-hex string.
func TestRandKeysetStart(t *testing.T) {
	s := randKeysetStart()
	if len(s) != 64 {
		t.Fatalf("randKeysetStart must be 64 hex chars, got %d (%q)", len(s), s)
	}
	for _, r := range s {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Fatalf("randKeysetStart must be lowercase hex, got %q", s)
		}
	}
}

// TestAuditSampledDerivesRandomStart: a sampled audit (samplePerTarget>0) exercises Audit's
// random-keyset-start branch and still checks the sample bound.
func TestAuditSampledDerivesRandomStart(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	a := newMemSink("a")
	for i := 0; i < 5; i++ {
		id := hashOf(fmt.Sprintf("obj-%d", i))
		_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: id}, []TargetStatus{{Target: "a", State: StateStored}})
		a.store[id] = []byte("x")
	}
	rep, err := Audit(ctx, led, []Target{{Sink: a}}, 3)
	if err != nil {
		t.Fatalf("sampled audit: %v", err)
	}
	if rep.Targets[0].Checked == 0 {
		t.Fatal("a sampled audit must still check objects")
	}
}

// auditPageLedger injects a ledger read fault (or panic) into the audit sweep's page source.
type auditPageLedger struct {
	stubLedger
	panicPage bool
	pageErr   error
}

func (l auditPageLedger) StoredExternalIDsPage(context.Context, string, string, int) ([]string, error) {
	if l.panicPage {
		panic("ledger scan boom")
	}
	return nil, l.pageErr
}

// TestAuditLedgerReadErrorPropagates: a ledger page-read failure must surface as the audit's
// error (an incomplete integrity check must not read as a clean pass).
func TestAuditLedgerReadErrorPropagates(t *testing.T) {
	_, err := Audit(context.Background(),
		auditPageLedger{pageErr: errors.New("scan boom")},
		[]Target{{Sink: newMemSink("t")}}, 0)
	if err == nil {
		t.Fatal("a ledger read error during audit must propagate")
	}
}

// TestAuditTargetPanicIsolated: a panic in one target's sweep (e.g. a pgx scan on a drifted
// column) becomes that target's error via RunParallel's recover, not a process crash.
func TestAuditTargetPanicIsolated(t *testing.T) {
	_, err := Audit(context.Background(),
		auditPageLedger{panicPage: true},
		[]Target{{Sink: newMemSink("t")}}, 0)
	if err == nil {
		t.Fatal("a panic in a target's audit must be recovered into an error, not crash")
	}
}

// existsPanicSink models a probe path that panics (a driver bug on a drifted backend).
type existsPanicSink struct{ stubSink }

func (existsPanicSink) Exists(context.Context, string) (bool, error) { panic("exists boom") }

// TestAuditExistsPanicContained: a panic in an Exists probe is recovered by existsWithCtx into
// an errored probe result (counted, non-worm → unexpectedly unverifiable), never a crash.
func TestAuditExistsPanicContained(t *testing.T) {
	ctx := context.Background()
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: "hashA"}, []TargetStatus{{Target: "t", State: StateStored}})
	rep, err := Audit(ctx, led, []Target{{Sink: existsPanicSink{stubSink{name: "t"}}}}, 0)
	if err != nil {
		t.Fatalf("a panicking probe must be contained per-probe, not returned: %v", err)
	}
	if rep.Targets[0].Errors == 0 || !rep.Targets[0].UnexpectedlyUnverifiable() {
		t.Fatalf("a panicking Exists must count as an errored probe: %+v", rep.Targets[0])
	}
}

// ---------- manifest.go ----------

// TestManifestName: the snapshot name is a UTC nanosecond-precision timestamp, so two runs in
// the same wall-clock second get distinct keys (a WORM target rejects an overwrite).
func TestManifestName(t *testing.T) {
	name := ManifestName(time.Date(2026, 7, 8, 12, 34, 56, 123456789, time.UTC))
	if name != "2026-07-08T123456.123456789Z.jsonl" {
		t.Fatalf("ManifestName = %q", name)
	}
}

// pagingLedger paginates a rich object set (with breadcrumbs) so the manifest export's
// keyset sweep runs across >1 page and serializes createdBy + sourceCreatedDate.
type pagingLedger struct {
	stubLedger
	objs []ObjectMeta // sorted by ExternalID
}

func (l pagingLedger) StoredObjectsPage(_ context.Context, _, after string, limit int) ([]ObjectMeta, error) {
	out := make([]ObjectMeta, 0, limit)
	for _, m := range l.objs {
		if m.ExternalID > after {
			out = append(out, m)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// TestWriteManifestMultiPageWithBreadcrumbs: a manifest over more than one page must emit a
// JSONL line per object AND serialize the createdBy / sourceCreatedDate breadcrumbs when set.
func TestWriteManifestMultiPageWithBreadcrumbs(t *testing.T) {
	const n = KeysetPageSize + 1 // forces a full page then a short page (multi-page cursor)
	created := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	uid := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	objs := make([]ObjectMeta, n)
	for i := range objs {
		objs[i] = ObjectMeta{ExternalID: fmt.Sprintf("%064d", i), Size: int64(i)}
	}
	objs[0].SourceCreatedDate = created
	objs[0].CreatedBy = uuid.NullUUID{UUID: uid, Valid: true}

	sink := newMemSink("t")
	if err := WriteManifests(context.Background(), pagingLedger{objs: objs}, []Target{{Sink: sink}}, "snap.jsonl"); err != nil {
		t.Fatalf("WriteManifests: %v", err)
	}
	got := string(sink.store["_manifest/snap.jsonl"])
	if lines := strings.Count(strings.TrimSpace(got), "\n") + 1; lines != n {
		t.Fatalf("manifest lines = %d, want %d", lines, n)
	}
	if !strings.Contains(got, uid.String()) {
		t.Fatalf("manifest must serialize a valid createdBy uuid; got first line %q", firstLine(got))
	}
	if !strings.Contains(got, `"sourceCreatedDate":"2025-01-02T03:04:05Z"`) {
		t.Fatalf("manifest must serialize a set sourceCreatedDate; got first line %q", firstLine(got))
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// panicObjPageLedger panics inside the manifest sweep's page source.
type panicObjPageLedger struct{ stubLedger }

func (panicObjPageLedger) StoredObjectsPage(context.Context, string, string, int) ([]ObjectMeta, error) {
	panic("manifest page boom")
}

// TestWriteManifestProducerPanicContained: a panic in the manifest encoder's producer goroutine
// must be recovered by pipeThrough (surfaced as the reader's terminal error), not crash serve.
func TestWriteManifestProducerPanicContained(t *testing.T) {
	err := WriteManifests(context.Background(), panicObjPageLedger{}, []Target{{Sink: newMemSink("t")}}, "snap.jsonl")
	if err == nil {
		t.Fatal("a panic in the manifest producer must surface as an error, not crash")
	}
}

// panicPutSink panics in PutManifest (a wedged/buggy sink write).
type panicPutSink struct{ stubSink }

func (panicPutSink) PutManifest(context.Context, string, io.Reader) error { panic("putmanifest boom") }

// TestWriteManifestPutPanicContained: a panic in a sink's PutManifest is recovered by
// callWithCtx into that target's error, isolating a best-effort DR snapshot from serve.
func TestWriteManifestPutPanicContained(t *testing.T) {
	err := WriteManifests(context.Background(), newFakeLedger(), []Target{{Sink: panicPutSink{stubSink{name: "t"}}}}, "snap.jsonl")
	if err == nil {
		t.Fatal("a panic in PutManifest must be contained as an error, not crash")
	}
}

// ---------- backup.go: ledger error paths + panic isolation ----------

// errLedger injects StoredTargets / RecordBackup failures.
type errLedger struct {
	stubLedger
	storedErr error
	recordErr error
}

func (l errLedger) StoredTargets(context.Context, string) (map[string]bool, error) {
	if l.storedErr != nil {
		return nil, l.storedErr
	}
	return map[string]bool{}, nil
}
func (l errLedger) RecordBackup(context.Context, ObjectMeta, []TargetStatus) error {
	return l.recordErr
}

// TestBackupOneLedgerReadError: a dedup-read failure aborts the backup with the wrapped error,
// never a silent skip.
func TestBackupOneLedgerReadError(t *testing.T) {
	p := NewPipeline(fakeSource{[]byte("x")}, errLedger{storedErr: errors.New("read boom")},
		[]Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	_, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: hashOf("x")})
	if err == nil || !strings.Contains(err.Error(), "ledger read") {
		t.Fatalf("a dedup-read failure must surface as a ledger read error, got %v", err)
	}
}

// TestBackupOneLedgerRecordError: a store that succeeds but whose ledger write fails must
// surface the record error (else a stored target goes unrecorded and is needlessly re-stored).
func TestBackupOneLedgerRecordError(t *testing.T) {
	data := []byte("record me")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	p := NewPipeline(fakeSource{data}, errLedger{recordErr: errors.New("write boom")},
		[]Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	_, _, err = p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err == nil || !strings.Contains(err.Error(), "ledger record") {
		t.Fatalf("a ledger write failure must surface as a ledger record error, got %v", err)
	}
}

// panicStoreSink panics inside Store (a sink driver bug).
type panicStoreSink struct{ stubSink }

func (panicStoreSink) Store(context.Context, string, io.Reader) (int64, error) { panic("store boom") }

// TestBackupOneStorePanicFailsTarget: a panicking sink Store is recovered by storeWithCtx into
// that target's failure — the object is not done and the worker survives.
func TestBackupOneStorePanicFailsTarget(t *testing.T) {
	data := []byte("panic store")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	led := newFakeLedger()
	p := NewPipeline(fakeSource{data}, led, []Target{{Sink: panicStoreSink{stubSink{name: "boom"}}, Codec: CodecNone}})
	done, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: h})
	if err != nil {
		t.Fatalf("a panicking sink must fail its target, not the whole backup: %v", err)
	}
	if done {
		t.Fatal("a panicking sink must leave the object not-done")
	}
	if led.states[h+"/boom"] == StateStored {
		t.Fatal("a panicking sink must never be recorded stored")
	}
}

// panicReadSource returns a reader that panics on Read (a corrupt/custom source reader).
type panicReadSource struct{}

func (panicReadSource) FetchContent(context.Context, BackupItem) (io.ReadCloser, error) {
	return io.NopCloser(panicReader{}), nil
}

type panicReader struct{}

func (panicReader) Read([]byte) (int, error) { panic("read boom") }

// TestBackupOneSourceReadPanicContained: a panic in the source read is recovered inside fanOut
// and returned as the object's error — the pump/store goroutines are not leaked and no crash.
func TestBackupOneSourceReadPanicContained(t *testing.T) {
	p := NewPipeline(panicReadSource{}, newFakeLedger(), []Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	done, _, err := p.BackupOne(context.Background(), BackupItem{ExternalID: hashOf("x")})
	if err == nil || done {
		t.Fatalf("a panic in the source read must fail the object, got done=%v err=%v", done, err)
	}
}

// ---------- backfill.go: pacer + recover ----------

// TestBackfillRateLimited: a rate-limited pass still backs up every object (exercises the
// ticker-paced dispatch path).
func TestBackfillRateLimited(t *testing.T) {
	data := []byte("paced object")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	corpus := fakeCorpus{entries: []BackupItem{{ExternalID: h}, {ExternalID: h}}}
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	st, err := NewBackfiller(corpus, p, time.Minute, 2).Run(context.Background(), 100000)
	if err != nil {
		t.Fatalf("rate-limited backfill: %v", err)
	}
	if st.Backed != 2 {
		t.Fatalf("stats: %+v (want backed=2 under a rate limit)", st)
	}
}

// TestBackfillAbsurdRate: an absurdly high rate floors the ticker interval to a nanosecond
// (never NewTicker(0)) and still completes the pass.
func TestBackfillAbsurdRate(t *testing.T) {
	data := []byte("absurd rate")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	corpus := fakeCorpus{entries: []BackupItem{{ExternalID: h}}}
	p := NewPipeline(fakeSource{data}, newFakeLedger(), []Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	st, err := NewBackfiller(corpus, p, time.Minute, 1).Run(context.Background(), 2_000_000_000)
	if err != nil {
		t.Fatalf("absurd-rate backfill: %v", err)
	}
	if st.Backed != 1 {
		t.Fatalf("stats: %+v (want backed=1)", st)
	}
}

// TestBackfillPacerCancelled: a cancelled ctx stops the paced dispatch loop and returns the
// ctx error (the pacer gates dispatch, so a drain aborts promptly).
func TestBackfillPacerCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	corpus := fakeCorpus{entries: []BackupItem{{ExternalID: hashOf("h1")}}}
	p := NewPipeline(fakeSource{[]byte("x")}, newFakeLedger(), []Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	if _, err := NewBackfiller(corpus, p, time.Minute, 1).Run(ctx, 1); err == nil {
		t.Fatal("a cancelled ctx must stop the paced backfill with an error")
	}
}

// panicSource panics in FetchContent (a poison object).
type panicSource struct{}

func (panicSource) FetchContent(context.Context, BackupItem) (io.ReadCloser, error) {
	panic("fetch boom")
}

// TestBackfillRecoversPoisonObject: a panic while backing up one object is recovered into a
// counted failure — the pass continues rather than crashing.
func TestBackfillRecoversPoisonObject(t *testing.T) {
	corpus := fakeCorpus{entries: []BackupItem{{ExternalID: hashOf("h1")}}}
	p := NewPipeline(panicSource{}, newFakeLedger(), []Target{{Sink: newMemSink("t"), Codec: CodecNone}})
	st, err := NewBackfiller(corpus, p, time.Minute, 1).Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("a poison object must not crash the pass: %v", err)
	}
	if st.Failed != 1 {
		t.Fatalf("stats: %+v (want the poison object counted failed)", st)
	}
}

// ---------- restore.go ----------

// TestRestoreRejectsMalformedHash: an invalid content hash must be rejected before it can be
// used as a path component (traversal guard) — both RestoreObject and VerifyObject.
func TestRestoreRejectsMalformedHash(t *testing.T) {
	sink := newMemSink("s")
	if err := RestoreObject(context.Background(), sink, "../../etc/passwd", t.TempDir()); err == nil {
		t.Fatal("RestoreObject must reject a malformed hash")
	}
	if err := VerifyObject(context.Background(), sink, "not-a-hash"); err == nil {
		t.Fatal("VerifyObject must reject a malformed hash")
	}
}

// TestRestoreCancelledDuringExistingCheck: a cancelled ctx while verifying an already-present
// (possibly stale) file must abort with the ctx error, not run the read to completion.
func TestRestoreCancelledDuringExistingCheck(t *testing.T) {
	h := hashOf("present")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, h), bytes.Repeat([]byte("stale"), 100), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sink := newMemSink("s")
	err := RestoreObject(ctx, sink, h, dir)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled restore during the pre-existing check must return the ctx error, got %v", err)
	}
}

// peekErrSink serves NO bytes then a source I/O error, so the magic peek itself faults.
type peekErrSink struct {
	stubSink
	err error
}

func (s peekErrSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(errReader{s.err}), nil
}

// TestVerifyPeekMagicIOError: a source read fault AT the magic peek (before any bytes) must be a
// retryable error, NOT silently classified as "short object → not zstd" (which would re-hash
// compressed bytes into a false corruption verdict).
func TestVerifyPeekMagicIOError(t *testing.T) {
	boom := errors.New("reset by peer at first byte")
	err := VerifyObject(context.Background(), peekErrSink{stubSink{name: "s"}, boom}, hashOf("x"))
	if err == nil || !strings.Contains(err.Error(), "peek magic") {
		t.Fatalf("a peek-time source fault must surface as a retryable peek error, got %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("the peek error must wrap the underlying source I/O error, got %v", err)
	}
}

// rawIOErrSink serves a NON-zstd-magic prefix then an I/O error — the raw decode path faults.
type rawIOErrSink struct {
	stubSink
	prefix []byte
	err    error
}

func (s rawIOErrSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(io.MultiReader(bytes.NewReader(s.prefix), errReader{s.err})), nil
}

// TestVerifyRawSourceIOError: a mid-stream source fault on a RAW (non-zstd) object must surface
// as a retryable read error, not a false corruption verdict.
func TestVerifyRawSourceIOError(t *testing.T) {
	boom := errors.New("connection reset mid raw read")
	sink := rawIOErrSink{stubSink: stubSink{name: "s"}, prefix: []byte("plain-not-zstd-magic"), err: boom}
	err := VerifyObject(context.Background(), sink, hashOf("x"))
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("a raw source I/O error must be retryable and wrap the cause, got %v", err)
	}
}

// TestVerifyRawOverCap: a raw stored object larger than the restore cap must be rejected as
// over-cap (the decompression-bomb / oversized-blob guard applies to the raw path too).
func TestVerifyRawOverCap(t *testing.T) {
	old := maxObjectBytes
	maxObjectBytes = 100
	defer func() { maxObjectBytes = old }()

	data := bytes.Repeat([]byte("x"), 500) // > cap, not zstd magic
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{stubSink: stubSink{name: "s"}, store: map[string][]byte{h: data}}
	err = VerifyObject(context.Background(), sink, h)
	if err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("an over-cap raw object must be rejected as over-cap, got %v", err)
	}
}

// flipFetchSink serves zstd-magic-but-not-zstd bytes on the first Fetch, then an error — so the
// raw-fallback re-fetch fails.
type flipFetchSink struct {
	stubSink
	first []byte
	err   error
	mu    sync.Mutex
	calls int
}

func (s *flipFetchSink) Fetch(context.Context, string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if n == 1 {
		return io.NopCloser(bytes.NewReader(s.first)), nil
	}
	return nil, s.err
}

// TestVerifyRawFallbackRefetchError: when a zstd-magic object fails to decode as zstd and the
// raw-fallback re-fetch errors, that fetch error must surface (not a corruption verdict).
func TestVerifyRawFallbackRefetchError(t *testing.T) {
	boom := errors.New("refetch failed")
	magicGarbage := append([]byte{0x28, 0xB5, 0x2F, 0xFD}, []byte(" not a real zstd frame payload")...)
	sink := &flipFetchSink{stubSink: stubSink{name: "s"}, first: magicGarbage, err: boom}
	err := VerifyObject(context.Background(), sink, hashOf("x"))
	if !errors.Is(err, boom) {
		t.Fatalf("a failed raw-fallback re-fetch must surface the fetch error, got %v", err)
	}
	if sink.calls < 2 {
		t.Fatalf("the raw fallback must re-fetch (2 calls), got %d", sink.calls)
	}
}

// ---------- reconcile.go ----------

// TestReconcileStaleTargetSkipped: a `stored` set that references a target no longer configured
// must be skipped (not repaired), leaving the object needing a primary-store backfill.
func TestReconcileStaleTargetSkipped(t *testing.T) {
	led := newFakeLedger()
	rc := NewReconciler(led, []Target{{Sink: newMemSink("a"), Codec: CodecNone}}, time.Minute, "", 0, nil, 1)
	var called atomic.Bool
	rc.OnError(func(string, error) { called.Store(true) })
	p := NewPipeline(nil, led, []Target{{Sink: newMemSink("a"), Codec: CodecNone}})

	var st ReconcileStats
	var mu sync.Mutex
	rc.repair(context.Background(), p, hashOf("x"), map[string]bool{"ghost": true}, &st, &mu)
	if st.Skipped != 1 {
		t.Fatalf("a stale-target-only object must be skipped: %+v", st)
	}
	if !called.Load() {
		t.Fatal("the OnError callback must report the unrepairable object")
	}
}

// TestReconcileDestinationUnwritable: when the source verifies cleanly but a destination target
// stays unwritable, the object is counted failed and reported (rotating sources can't fix a dead
// destination).
func TestReconcileDestinationUnwritable(t *testing.T) {
	ctx := context.Background()
	data := []byte("dest is down")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	a.store[h] = data
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(data))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	var reported string
	rc := NewReconciler(led,
		[]Target{{Sink: a, Codec: CodecNone}, {Sink: &failSink{stubSink{name: "b"}}, Codec: CodecNone}},
		time.Minute, "", 0, nil, 1).OnError(func(_ string, e error) { reported = e.Error() })
	st, err := rc.Run(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st.Failed != 1 || st.Repaired != 0 {
		t.Fatalf("stats: %+v (want failed=1: source ok but destination unwritable)", st)
	}
	if !strings.Contains(reported, "destination target is still unwritable") {
		t.Fatalf("the destination-unwritable failure must be reported, got %q", reported)
	}
}

// TestReconcileEverySourceFails: when every holder's source fetch/decode fails (corrupt bytes),
// the object is counted failed and the last cause is reported (the cleanup path runs too).
func TestReconcileEverySourceFails(t *testing.T) {
	ctx := context.Background()
	h := hashOf("corrupt-holder")
	a := newMemSink("a")
	a.store[h] = []byte("garbage that does not hash to h")
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	var reported string
	rc := NewReconciler(led,
		[]Target{{Sink: a, Codec: CodecNone}, {Sink: newMemSink("b"), Codec: CodecNone}},
		time.Minute, "", 0, nil, 1).OnError(func(_ string, e error) { reported = e.Error() })
	st, err := rc.Run(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st.Failed != 1 {
		t.Fatalf("stats: %+v (want failed=1: every source failed)", st)
	}
	if !strings.Contains(reported, "every source failed") {
		t.Fatalf("the all-sources-failed cause must be reported, got %q", reported)
	}
}

// TestReconcileScratchDirUnwritable: an unusable scratchDir fails the repair with a clear
// staging error (a systemic fault the failed COUNT alone would hide).
func TestReconcileScratchDirUnwritable(t *testing.T) {
	ctx := context.Background()
	data := []byte("needs staging")
	h, err := sum(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	a := newMemSink("a")
	a.store[h] = data
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: int64(len(data))},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	badScratch := filepath.Join(t.TempDir(), "does-not-exist-subdir")
	var reported string
	rc := NewReconciler(led,
		[]Target{{Sink: a, Codec: CodecNone}, {Sink: newMemSink("b"), Codec: CodecNone}},
		time.Minute, badScratch, 0, nil, 1).OnError(func(_ string, e error) { reported = e.Error() })
	st, err := rc.Run(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st.Failed != 1 {
		t.Fatalf("stats: %+v (want failed=1 on an unwritable scratchDir)", st)
	}
	if !strings.Contains(reported, "reconcile temp") {
		t.Fatalf("an unwritable scratchDir must be reported as a staging error, got %q", reported)
	}
}

// TestReconcileSourceTimesOut: a holder whose fetch wedges (ignores ctx) must fail the object at
// the per-object timeout — the abandonment wrapper unblocks the repair.
func TestReconcileSourceTimesOut(t *testing.T) {
	ctx := context.Background()
	h := hashOf("wedged-source")
	a := newHangingSink("a")
	t.Cleanup(func() { close(a.release) })
	led := newFakeLedger()
	_ = led.RecordBackup(ctx, ObjectMeta{ExternalID: h},
		[]TargetStatus{{Target: "a", State: StateStored}, {Target: "b", State: StateFailed}})

	rc := NewReconciler(led,
		[]Target{{Sink: a, Codec: CodecNone}, {Sink: newMemSink("b"), Codec: CodecNone}},
		50*time.Millisecond, "", 0, nil, 1)
	st, err := rc.Run(ctx, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if st.Failed != 1 {
		t.Fatalf("stats: %+v (want failed=1: source timed out)", st)
	}
}
