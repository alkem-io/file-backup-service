package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// TestNewRegistersRuntimeCollectors: New() must wire the Go/process collectors so a
// goroutine wedge is visible on the private registry, not just the domain metrics.
func TestNewRegistersRuntimeCollectors(t *testing.T) {
	m := New()
	if m == nil || m.reg == nil {
		t.Fatal("New() returned a Metrics without a registry")
	}
	body := scrape(t, m)
	for _, name := range []string{"go_goroutines", "go_memstats_alloc_bytes"} {
		if !strings.Contains(body, name) {
			t.Errorf("scrape missing %q; runtime collectors not registered", name)
		}
	}
}

// TestHandlerServesIncrementedCounter: Handler() serves the private registry over HTTP and a
// counter incremented via an observation method is exposed with its result/target labels.
func TestHandlerServesIncrementedCounter(t *testing.T) {
	m := New()
	m.ObjectStored("t1", 42)

	body := scrape(t, m)
	for _, want := range []string{
		"filebackup_objects_total",
		`result="stored"`,
		`target="t1"`,
		"filebackup_bytes_stored_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape body missing %q\n---\n%s", want, body)
		}
	}
}

// TestObjectStoredMovesStoredAndBytes: each ObjectStored bumps the stored counter by one and
// adds the payload size to the per-target bytes counter, and repeats accumulate (no reset).
func TestObjectStoredMovesStoredAndBytes(t *testing.T) {
	m := New()
	stored := m.objects.WithLabelValues(domain.StateStored, "t1")
	bytes := m.bytes.WithLabelValues("t1")

	m.ObjectStored("t1", 100)
	if got := testutil.ToFloat64(stored); got != 1 {
		t.Errorf("stored counter = %v, want 1", got)
	}
	if got := testutil.ToFloat64(bytes); got != 100 {
		t.Errorf("bytes counter = %v, want 100", got)
	}

	m.ObjectStored("t1", 5)
	if got := testutil.ToFloat64(stored); got != 2 {
		t.Errorf("stored counter after 2nd = %v, want 2", got)
	}
	if got := testutil.ToFloat64(bytes); got != 105 {
		t.Errorf("bytes counter after 2nd = %v, want 105", got)
	}
}

// TestObjectFailedAndDedupUseTheirResultLabel: failed/dedup increment their own result-labelled
// series and don't leak into the stored bucket.
func TestObjectFailedAndDedupUseTheirResultLabel(t *testing.T) {
	m := New()
	m.ObjectFailed("t1")
	m.ObjectFailed("t1")
	m.ObjectDedup("t1")

	if got := testutil.ToFloat64(m.objects.WithLabelValues(domain.StateFailed, "t1")); got != 2 {
		t.Errorf("failed counter = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.objects.WithLabelValues("dedup", "t1")); got != 1 {
		t.Errorf("dedup counter = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.objects.WithLabelValues(domain.StateStored, "t1")); got != 0 {
		t.Errorf("stored counter should be untouched, got %v", got)
	}
}

// TestScalarCountersEachInc: every plain counter observation moves exactly its own metric.
func TestScalarCountersEachInc(t *testing.T) {
	m := New()

	m.DeadLetter()
	if got := testutil.ToFloat64(m.deadletter); got != 1 {
		t.Errorf("deadletter = %v, want 1", got)
	}
	m.ObjectTimeout()
	if got := testutil.ToFloat64(m.timeout); got != 1 {
		t.Errorf("timeout = %v, want 1", got)
	}
	m.SourceGone()
	if got := testutil.ToFloat64(m.sourceGone); got != 1 {
		t.Errorf("sourceGone = %v, want 1", got)
	}
	m.ManifestError()
	if got := testutil.ToFloat64(m.manifestErr); got != 1 {
		t.Errorf("manifestErr = %v, want 1", got)
	}
	m.SampleError()
	if got := testutil.ToFloat64(m.sampleErrors); got != 1 {
		t.Errorf("sampleErrors = %v, want 1", got)
	}
}

// TestGaugesSet: each RPO/coverage gauge setter writes the value the sampler passes it.
func TestGaugesSet(t *testing.T) {
	m := New()

	m.SetBacklog(7, 12.5)
	if got := testutil.ToFloat64(m.backlogPending); got != 7 {
		t.Errorf("backlogPending = %v, want 7", got)
	}
	if got := testutil.ToFloat64(m.oldestPendingAge); got != 12.5 {
		t.Errorf("oldestPendingAge = %v, want 12.5", got)
	}

	m.SetLastSuccessAge(3.5)
	if got := testutil.ToFloat64(m.lastSuccessAge); got != 3.5 {
		t.Errorf("lastSuccessAge = %v, want 3.5", got)
	}
	m.SetNeverVerified(2)
	if got := testutil.ToFloat64(m.neverVerified); got != 2 {
		t.Errorf("neverVerified = %v, want 2", got)
	}
	m.SetCircuitOpen(1)
	if got := testutil.ToFloat64(m.circuitOpen); got != 1 {
		t.Errorf("circuitOpen = %v, want 1", got)
	}
	m.SetUnderReplicated(4)
	if got := testutil.ToFloat64(m.underReplicated); got != 4 {
		t.Errorf("underReplicated = %v, want 4", got)
	}
}

// TestCountersCachedPerTarget: the per-target handle set is resolved once and reused, so the
// hot path re-uses the same Counter pointers instead of re-hashing the label tuple.
func TestCountersCachedPerTarget(t *testing.T) {
	m := New()
	first := m.counters("t1")
	second := m.counters("t1")
	if first != second {
		t.Fatal("counters(\"t1\") returned distinct handle sets; sync.Map cache not reused")
	}

	// A second observation to the same target must not create new series — the CounterVec
	// still holds exactly the 3 result children (stored/failed/dedup) it created on first use.
	m.ObjectStored("t1", 1)
	m.ObjectStored("t1", 1)
	if got := testutil.CollectAndCount(m.objects); got != 3 {
		t.Errorf("objects series after repeated same-target stores = %d, want 3", got)
	}

	// A different target is independent and adds its own 3 series.
	if other := m.counters("t2"); other == first {
		t.Fatal("counters(\"t2\") aliased t1's handle set")
	}
	m.ObjectStored("t2", 1)
	if got := testutil.CollectAndCount(m.objects); got != 6 {
		t.Errorf("objects series across two targets = %d, want 6", got)
	}
}

// TestSetAndClearImmutabilityOK: the WORM drift gauge is a per-target series set to 1 (ok) / 0
// (drift); ClearImmutabilityOK (the real adapter method — Cluster 4's structural-clear) DROPS a
// series, so CollectAndCount reflects it.
func TestSetAndClearImmutabilityOK(t *testing.T) {
	m := New()
	m.SetImmutabilityOK("good", true)
	m.SetImmutabilityOK("drift", false)
	if got := testutil.ToFloat64(m.immutabilityOK.WithLabelValues("good")); got != 1 {
		t.Errorf("good immutability gauge = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.immutabilityOK.WithLabelValues("drift")); got != 0 {
		t.Errorf("drift immutability gauge = %v, want 0", got)
	}
	if got := testutil.CollectAndCount(m.immutabilityOK); got != 2 {
		t.Errorf("immutability series = %d, want exactly 2 (no series for unverifiable targets)", got)
	}
	// ClearImmutabilityOK drops the series (used for a structurally-unverifiable target).
	m.ClearImmutabilityOK("good")
	if got := testutil.CollectAndCount(m.immutabilityOK); got != 1 {
		t.Errorf("after clear, immutability series = %d, want 1 (the cleared target's series is gone)", got)
	}
	m.ClearImmutabilityOK("never-set") // clearing an absent series is a harmless no-op
	if got := testutil.CollectAndCount(m.immutabilityOK); got != 1 {
		t.Errorf("clearing an absent series must be a no-op, got %d series", got)
	}
}

// TestSetImmutabilityUnverifiable: the DISTINCT unverifiable signal is a per-target gauge raised to
// 1 when a readable WORM target turns unreadable (so the dropped stale-green _ok series is not
// silent), and DELETED again once it verifies. It is a separate GaugeVec from immutabilityOK — set
// true → the series is present with value 1; set false → the series is dropped (CollectAndCount falls).
func TestSetImmutabilityUnverifiable(t *testing.T) {
	m := New()
	if got := testutil.CollectAndCount(m.immutabilityUnverifiable); got != 0 {
		t.Fatalf("no target should be unverifiable initially, got %d series", got)
	}
	m.SetImmutabilityUnverifiable("t", true)
	if got := testutil.ToFloat64(m.immutabilityUnverifiable.WithLabelValues("t")); got != 1 {
		t.Errorf("unverifiable gauge for t = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.immutabilityUnverifiable); got != 1 {
		t.Errorf("unverifiable series = %d, want exactly 1", got)
	}
	// Setting false drops the series (not a 0 value) so a recovered target goes silent, not stale-red.
	m.SetImmutabilityUnverifiable("t", false)
	if got := testutil.CollectAndCount(m.immutabilityUnverifiable); got != 0 {
		t.Errorf("after clearing, unverifiable series = %d, want 0 (the series is dropped)", got)
	}
	// Dropping an absent series is a harmless no-op.
	m.SetImmutabilityUnverifiable("never-set", false)
	if got := testutil.CollectAndCount(m.immutabilityUnverifiable); got != 0 {
		t.Errorf("dropping an absent series must be a no-op, got %d series", got)
	}
}

// TestDrillMetricsSetAndTextfile: a full-pass drill sets pass=1 and a last-success timestamp; the
// textfile export writes valid exposition with both drill gauges (and NO go_*/process_* series,
// so it can't collide with the worker's own /metrics when the node exporter merges them).
func TestDrillMetricsSetAndTextfile(t *testing.T) {
	d := NewDrillMetrics()
	now := time.Unix(1_700_000_000, 0)
	d.SetPass(true, now)
	if got := testutil.ToFloat64(d.pass); got != 1 {
		t.Errorf("drill pass = %v, want 1", got)
	}
	if got := testutil.ToFloat64(d.lastSuccess); got != 1_700_000_000 {
		t.Errorf("drill last-success = %v, want the unix timestamp", got)
	}

	path := filepath.Join(t.TempDir(), "drill.prom")
	if err := d.WriteTextfile(path); err != nil {
		t.Fatalf("WriteTextfile: %v", err)
	}
	b, err := os.ReadFile(path) //nolint:gosec // test temp path
	if err != nil {
		t.Fatalf("read textfile: %v", err)
	}
	body := string(b)
	for _, want := range []string{"filebackup_restore_drill_pass 1", "filebackup_drill_last_success_timestamp_seconds 1.7e+09"} {
		if !strings.Contains(body, want) {
			t.Errorf("textfile missing %q\n---\n%s", want, body)
		}
	}
	if strings.Contains(body, "go_goroutines") || strings.Contains(body, "process_") {
		t.Errorf("drill textfile must NOT carry runtime collectors (would collide with /metrics):\n%s", body)
	}
	// "" path is a no-op (the exit code carries the signal when no textfile is wired).
	if err := d.WriteTextfile(""); err != nil {
		t.Fatalf("empty path must be a no-op, got %v", err)
	}
}

// TestDrillWriteTextfileBadPath: a textfile whose parent dir doesn't exist fails the write. (The
// durable CommitWrite MkdirAll's the parent, so a NONEXISTENT parent chain with a file in the way
// is used to force the failure.)
func TestDrillWriteTextfileBadPath(t *testing.T) {
	d := NewDrillMetrics()
	d.SetPass(true, time.Unix(1, 0))
	// A regular file where a parent DIRECTORY is expected → MkdirAll fails (ENOTDIR).
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	if err := d.WriteTextfile(filepath.Join(blocker, "sub", "x.prom")); err == nil {
		t.Fatal("WriteTextfile under a file-as-dir must error")
	}
}

// TestDrillMetricsCarriesForwardLastSuccess (review Cluster 2): each drill is a SEPARATE process
// overwriting the same textfile. A FAILING run (a fresh DrillMetrics, last-success 0 in memory) must
// NOT clobber the file's true last-success to 0 — it reads the prior textfile and re-emits it.
func TestDrillMetricsCarriesForwardLastSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "drill.prom")
	// Process 1: a PASS at t=1700000000 writes the file.
	pass := NewDrillMetrics()
	pass.SetPass(true, time.Unix(1_700_000_000, 0))
	if err := pass.WriteTextfile(path); err != nil {
		t.Fatalf("pass write: %v", err)
	}
	// Process 2 (fresh metrics): a FAIL overwrites the file — last-success must be CARRIED FORWARD.
	fail := NewDrillMetrics()
	fail.SetPass(false, time.Unix(1_800_000_000, 0))
	if err := fail.WriteTextfile(path); err != nil {
		t.Fatalf("fail write: %v", err)
	}
	b, err := os.ReadFile(path) //nolint:gosec // test temp path
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	body := string(b)
	if !strings.Contains(body, "filebackup_restore_drill_pass 0") {
		t.Errorf("failing run must record pass=0:\n%s", body)
	}
	if !strings.Contains(body, "filebackup_drill_last_success_timestamp_seconds 1.7e+09") {
		t.Errorf("failing run must CARRY FORWARD the prior last-success (1.7e+09), not clobber to 0:\n%s", body)
	}
}

// scrape does a GET against the Metrics HTTP handler and returns the 200 exposition body.
func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL) //nolint:noctx // test-local request against an in-process server
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}
