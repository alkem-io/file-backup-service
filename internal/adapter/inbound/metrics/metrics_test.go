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

// TestSetImmutabilityOK: the WORM drift gauge is a per-target series set to 1 (ok) / 0 (drift),
// and a target we never set has NO series (so `== 0` can't false-fire on an unverifiable target).
func TestSetImmutabilityOK(t *testing.T) {
	m := New()
	m.SetImmutabilityOK("good", true)
	m.SetImmutabilityOK("drift", false)
	if got := testutil.ToFloat64(m.immutabilityOK.WithLabelValues("good")); got != 1 {
		t.Errorf("good immutability gauge = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.immutabilityOK.WithLabelValues("drift")); got != 0 {
		t.Errorf("drift immutability gauge = %v, want 0", got)
	}
	// Only the two series we set exist — an unverifiable target contributes none.
	if got := testutil.CollectAndCount(m.immutabilityOK); got != 2 {
		t.Errorf("immutability series = %d, want exactly 2 (no series for unverifiable targets)", got)
	}
}

// TestDrillMetricsSetAndTextfile: a full-pass drill sets pass=1 and a last-success timestamp; the
// textfile export writes valid exposition with both drill gauges (and NO go_*/process_* series,
// so it can't collide with the worker's own /metrics when the node exporter merges them).
func TestDrillMetricsSetAndTextfile(t *testing.T) {
	d := NewDrillMetrics()
	now := time.Unix(1_700_000_000, 0)
	d.Set(true, now)
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

// TestDrillWriteTextfileBadPath: a textfile whose parent dir doesn't exist fails the write (the
// temp can't be created); a path that is an existing DIRECTORY fails the final rename. Both must
// surface as errors, not be silently dropped.
func TestDrillWriteTextfileBadPath(t *testing.T) {
	d := NewDrillMetrics()
	d.Set(true, time.Unix(1, 0))
	if err := d.WriteTextfile(filepath.Join(t.TempDir(), "no-such-dir", "x.prom")); err == nil {
		t.Fatal("WriteTextfile to a nonexistent parent dir must error")
	}
	// path is an existing directory → the temp is created in its parent, but the final rename of a
	// file onto a directory fails.
	if err := d.WriteTextfile(t.TempDir()); err == nil {
		t.Fatal("WriteTextfile onto an existing directory must fail the rename")
	}
}

// TestDrillMetricsFailKeepsLastSuccess: a FAILING drill sets pass=0 but leaves the last-success
// timestamp untouched, so an operator sees when the drill last actually succeeded.
func TestDrillMetricsFailKeepsLastSuccess(t *testing.T) {
	d := NewDrillMetrics()
	d.Set(true, time.Unix(1_000, 0)) // an earlier success
	d.Set(false, time.Unix(2_000, 0))
	if got := testutil.ToFloat64(d.pass); got != 0 {
		t.Errorf("drill pass after failure = %v, want 0", got)
	}
	if got := testutil.ToFloat64(d.lastSuccess); got != 1_000 {
		t.Errorf("last-success must stay at the last PASS (1000), got %v", got)
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
