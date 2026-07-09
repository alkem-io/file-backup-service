// Package metrics is the Prometheus adapter — it implements domain.Metrics and
// serves /metrics. See specs/008 contracts/restore-and-ops.md.
package metrics

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/expfmt"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// Metrics holds the Prometheus collectors.
type Metrics struct {
	reg         *prometheus.Registry
	objects     *prometheus.CounterVec
	bytes       *prometheus.CounterVec
	deadletter  prometheus.Counter
	timeout     prometheus.Counter
	sourceGone  prometheus.Counter
	manifestErr prometheus.Counter
	// RPO/lag gauges — the alerting spine (FR-026/SC-001). Set periodically by the
	// serve sampler, not on the per-object path.
	backlogPending   prometheus.Gauge
	oldestPendingAge prometheus.Gauge
	lastSuccessAge   prometheus.Gauge
	underReplicated  prometheus.Gauge // objects not yet stored on every target (coverage)
	neverVerified    prometheus.Gauge // configured targets that have never verified anything
	circuitOpen      prometheus.Gauge // targets with an open circuit (tripped out, being deferred)
	sampleErrors     prometheus.Counter
	// immutabilityOK is the WORM drift gauge (T032): per Worm target, 1 = object-lock +
	// versioning still enabled, 0 = drift detected. A read-denying/unverifiable target's
	// series is NEVER emitted (so the `== 0` alert can't false-fire on an inherently
	// unverifiable PutObject-only credential — the never-verified/audit signals cover it).
	immutabilityOK *prometheus.GaugeVec
	byTarget       sync.Map // target name -> *targetCounters (resolved once, per target)
}

// New builds a Metrics with its own registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	// Runtime/process collectors so /metrics exposes go_goroutines, heap, fds —
	// without these a goroutine wedge is invisible on the private registry.
	reg.MustRegister(collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := promauto.With(reg)
	return &Metrics{
		reg: reg,
		objects: f.NewCounterVec(prometheus.CounterOpts{
			Name: "filebackup_objects_total", Help: "Objects processed per target, by result.",
		}, []string{"result", "target"}),
		bytes: f.NewCounterVec(prometheus.CounterOpts{
			Name: "filebackup_bytes_stored_total", Help: "Bytes stored per target.",
		}, []string{"target"}),
		deadletter: f.NewCounter(prometheus.CounterOpts{
			Name: "filebackup_deadletter_total", Help: "Outbox entries moved to dead-letter.",
		}),
		timeout: f.NewCounter(prometheus.CounterOpts{
			Name: "filebackup_object_timeout_total",
			Help: "Objects that hit the per-object timeout (a slow or wedged target).",
		}),
		sourceGone: f.NewCounter(prometheus.CounterOpts{
			Name: "filebackup_source_gone_total",
			Help: "Entries skipped because the source object was absent (404/410) — a mass spike means a wrong fileServiceBase, not benign deletions.",
		}),
		manifestErr: f.NewCounter(prometheus.CounterOpts{
			Name: "filebackup_manifest_write_errors_total",
			Help: "Failed periodic manifest-snapshot writes (per pass, any target) — alert on rate>0: a persistently failing manifest defeats a target's STANDALONE restorability (FR-015) while every per-object gauge stays green.",
		}),
		backlogPending: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_outbox_pending", Help: "Outbox entries awaiting backup (backlog depth).",
		}),
		oldestPendingAge: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_oldest_pending_age_seconds",
			Help: "Age of the oldest pending outbox entry — the backup lag / RPO signal. EXCLUDES T017a-deferred objects (blocked only on a circuit-open target — they can't be backed up faster), so a TOTAL outage where the ONLY target(s) are down does NOT show here: that case surfaces in filebackup_targets_circuit_open>0 (and filebackup_last_success_age climbing). Alert on this gauge for lag AND on targets_circuit_open for a target-down outage.",
		}),
		lastSuccessAge: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_last_success_age_seconds",
			Help: "Age of the STALEST target's most recent verified backup (max over targets; one lagging target drives it). It is time-since-last-NEW-store, so it climbs during a quiet or all-duplicate period even when fully replicated — ALERT ON IT TOGETHER WITH filebackup_outbox_pending>0 (real lag = work exists AND isn't getting done). 0 until the first.",
		}),
		underReplicated: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_under_replicated_objects",
			Help: "Objects with a ledger row not yet stored on every configured target — the coverage backstop for a partially-stored object that dead-lettered (drained from the backlog). A source-poison object that NEVER stored has no ledger row and shows only in filebackup_deadletter_total, so alert on both.",
		}),
		neverVerified: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_targets_never_verified",
			Help: "Configured targets that have never verified a single object — catches a target (e.g. a misconfigured immutable off-site copy) that received nothing since inception, which last_success_age can't see.",
		}),
		circuitOpen: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_targets_circuit_open",
			Help: "Targets whose circuit is currently OPEN (tripped out of the fan-out after repeated failures); objects needing them are deferred (re-claimable), not dead-lettered (T017a).",
		}),
		sampleErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "filebackup_metrics_sample_errors_total",
			Help: "Failed RPO/coverage sampling passes — alert on rate>0 so a frozen (stale-green) gauge is itself detectable.",
		}),
		immutabilityOK: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "filebackup_immutability_ok",
			Help: "Per WORM/immutable target: 1 = object-lock + versioning still configured, 0 = drift detected. Emitted ONLY for targets whose immutability could actually be read (an S3 target whose credential can query the lock/versioning config); a read-denying (PutObject-only) WORM target is unverifiable so its series is absent — alert on == 0.",
		}, []string{"target"}),
	}
}

// SetImmutabilityOK records a WORM target's drift-check verdict (1 ok / 0 drift). Called ONLY
// for a target whose immutability was actually verifiable; an unverifiable (read-denying)
// target's series is deliberately never set, so `filebackup_immutability_ok == 0` can't fire
// on a target we simply couldn't read.
func (m *Metrics) SetImmutabilityOK(target string, ok bool) {
	m.immutabilityOK.WithLabelValues(target).Set(b2f(ok))
}

// ClearImmutabilityOK removes target's drift series (used when it becomes unverifiable), so a
// formerly-green target that turns unreadable drops to NO series rather than freezing stale at 1.
// Deleting an absent series is a no-op, so an always-unverifiable target simply never appears.
func (m *Metrics) ClearImmutabilityOK(target string) {
	m.immutabilityOK.DeleteLabelValues(target)
}

// b2f maps a bool to the Prometheus 1/0 gauge convention.
func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// DrillMetrics is the restore-drill's OWN metric set (T033), separate from the worker Metrics:
// the `drill` subcommand is a short-lived CronJob, so its gauges are exported via a
// textfile-collector file, NOT a scraped /metrics — and its registry holds ONLY the drill gauges
// (no go_*/process_* collectors), so the textfile can't collide with the worker's own /metrics
// series when the node exporter merges them.
type DrillMetrics struct {
	reg         *prometheus.Registry
	pass        prometheus.Gauge
	lastSuccess prometheus.Gauge
}

// NewDrillMetrics builds the drill metric set on a private, collector-free registry.
func NewDrillMetrics() *DrillMetrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	return &DrillMetrics{
		reg: reg,
		pass: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_restore_drill_pass",
			Help: "Last restore-drill result: 1 = every sampled object restored + hash-matched, 0 = at least one failed. Set by the `drill` subcommand; exported via a textfile (--metrics-file) since the drill process is short-lived. Alert on == 0.",
		}),
		lastSuccess: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_drill_last_success_timestamp_seconds",
			Help: "Unix timestamp (seconds) of the last FULLY-PASSING restore drill; 0 until the first. Alert on time() - this > a week to catch a drill that stopped succeeding.",
		}),
	}
}

// Set records a drill outcome: the pass gauge (1/0) and, on a full pass, the last-success
// timestamp. A failing drill leaves the last-success timestamp untouched, so an operator sees when
// the drill last actually SUCCEEDED, not merely when it last ran.
func (d *DrillMetrics) Set(pass bool, at time.Time) {
	d.pass.Set(b2f(pass))
	if pass {
		d.lastSuccess.Set(float64(at.Unix()))
	}
}

// WriteTextfile atomically writes the drill registry to path in Prometheus text-exposition format
// (temp + rename, so a scrape never reads a half-written file) — the node-exporter
// textfile-collector convention for a short-lived batch job's metrics (a scraped /metrics can't
// carry them because the process exits). A "" path is a no-op (the exit code carries the signal).
func (d *DrillMetrics) WriteTextfile(path string) error {
	if path == "" {
		return nil
	}
	mfs, gerr := d.reg.Gather()
	if gerr != nil {
		return fmt.Errorf("gather metrics: %w", gerr)
	}
	tmp, cerr := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if cerr != nil {
		return fmt.Errorf("create metrics temp: %w", cerr)
	}
	// Remove the temp on any failure path; on success it has been renamed away (the committed
	// flag skips the Remove of the now-absent temp).
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	enc := expfmt.NewEncoder(tmp, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		if eerr := enc.Encode(mf); eerr != nil {
			return fmt.Errorf("encode metrics: %w", eerr)
		}
	}
	if serr := tmp.Sync(); serr != nil {
		return fmt.Errorf("sync metrics temp: %w", serr)
	}
	if clerr := tmp.Close(); clerr != nil {
		return fmt.Errorf("close metrics temp: %w", clerr)
	}
	if rerr := os.Rename(tmp.Name(), path); rerr != nil {
		return fmt.Errorf("commit metrics textfile: %w", rerr)
	}
	committed = true
	return nil
}

// SetBacklog updates the outbox backlog gauges (pending count + oldest-pending age).
func (m *Metrics) SetBacklog(pending int, oldestAgeSec float64) {
	m.backlogPending.Set(float64(pending))
	m.oldestPendingAge.Set(oldestAgeSec)
}

// SetLastSuccessAge records seconds since the stalest verified target's last backup.
func (m *Metrics) SetLastSuccessAge(ageSec float64) { m.lastSuccessAge.Set(ageSec) }

// SetNeverVerified records how many configured targets have never verified anything.
func (m *Metrics) SetNeverVerified(n int) { m.neverVerified.Set(float64(n)) }

// SetCircuitOpen records how many targets currently have an open circuit (tripped out).
func (m *Metrics) SetCircuitOpen(n int) { m.circuitOpen.Set(float64(n)) }

// SetUnderReplicated records the count of objects not stored on every target.
func (m *Metrics) SetUnderReplicated(n int) { m.underReplicated.Set(float64(n)) }

// SampleError records a failed sampling pass so a frozen gauge is alertable.
func (m *Metrics) SampleError() { m.sampleErrors.Inc() }

// targetCounters holds a target's resolved Counter handles, so the per-object hot path
// does a direct .Inc()/.Add() instead of re-hashing the label tuple + a CounterVec map
// lookup on every observation (matters on a backlog/backfill drain).
type targetCounters struct {
	stored, failed, dedup, bytes prometheus.Counter
}

// counters returns target's cached handle set, resolving it once on first use.
func (m *Metrics) counters(target string) *targetCounters {
	if v, ok := m.byTarget.Load(target); ok {
		return v.(*targetCounters)
	}
	tc := &targetCounters{
		stored: m.objects.WithLabelValues(domain.StateStored, target),
		failed: m.objects.WithLabelValues(domain.StateFailed, target),
		dedup:  m.objects.WithLabelValues("dedup", target),
		bytes:  m.bytes.WithLabelValues(target),
	}
	actual, _ := m.byTarget.LoadOrStore(target, tc)
	return actual.(*targetCounters)
}

// ObjectStored implements domain.Metrics.
func (m *Metrics) ObjectStored(target string, storedBytes int64) {
	tc := m.counters(target)
	tc.stored.Inc()
	tc.bytes.Add(float64(storedBytes))
}

// ObjectFailed implements domain.Metrics.
func (m *Metrics) ObjectFailed(target string) { m.counters(target).failed.Inc() }

// ObjectDedup implements domain.Metrics.
func (m *Metrics) ObjectDedup(target string) { m.counters(target).dedup.Inc() }

// DeadLetter records an entry moved to dead-letter.
func (m *Metrics) DeadLetter() { m.deadletter.Inc() }

// ObjectTimeout records an object that hit the per-object timeout — the direct,
// alertable signal of a slow/wedged target (a wedge otherwise only shows as a
// slowly-climbing go_goroutines from abandoned stores).
func (m *Metrics) ObjectTimeout() { m.timeout.Inc() }

// SourceGone records an entry skipped because its source object was absent.
func (m *Metrics) SourceGone() { m.sourceGone.Inc() }

// ManifestError records a failed manifest-snapshot pass so a target silently losing its
// standalone-restore inventory is alertable, not just a log line.
func (m *Metrics) ManifestError() { m.manifestErr.Inc() }

// Handler returns the Prometheus HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
