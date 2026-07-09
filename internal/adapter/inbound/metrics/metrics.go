// Package metrics is the Prometheus adapter — it implements domain.Metrics and
// serves /metrics. See specs/008 contracts/restore-and-ops.md.
package metrics

import (
	"context"
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
	"github.com/prometheus/common/model"

	"github.com/alkem-io/file-backup-service/internal/domain"
	"github.com/alkem-io/file-backup-service/internal/fsutil"
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
	// immutabilityUnverifiable is the DISTINCT unverifiable signal (Pillar 1): per Worm target the
	// worker SHOULD be able to read, 1 = it has been unverifiable this pass (a credential rotated to
	// write-only, a persistent read-deny, a wedged endpoint). It exists so the _ok series can be
	// DROPPED when a target turns unreadable — avoiding a frozen stale-green that masks a later real
	// drift — WITHOUT going silent: alert on `filebackup_immutability_unverifiable == 1` sustained.
	immutabilityUnverifiable *prometheus.GaugeVec
	byTarget                 sync.Map // target name -> *targetCounters (resolved once, per target)
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
			Help: "Per WORM/immutable target: 1 = object-lock + versioning still configured, 0 = drift detected. Emitted ONLY for a target whose immutability could actually be READ this pass; a target that turns unverifiable has its series DROPPED (not frozen stale-green) and raises filebackup_immutability_unverifiable instead — alert on == 0.",
		}, []string{"target"}),
		immutabilityUnverifiable: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "filebackup_immutability_unverifiable",
			Help: "Per WORM target the worker SHOULD be able to read: 1 = its object-lock/versioning could NOT be read this pass (a credential rotated to write-only, a persistent read-deny, a wedged endpoint). Set when the _ok series is dropped so the drop is alertable, not silent — alert on == 1 sustained. A structurally-unreadable target (a filesystem WORM with no bucket object-lock) does NOT set this (it is expected, not an anomaly).",
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

// ClearImmutabilityOK removes target's drift series (used when it becomes unverifiable or is
// structurally unreadable), so a formerly-green target that turns unreadable drops to NO series
// rather than freezing stale at 1. Deleting an absent series is a no-op, so an always-unverifiable
// target simply never appears.
func (m *Metrics) ClearImmutabilityOK(target string) {
	m.immutabilityOK.DeleteLabelValues(target)
}

// SetImmutabilityUnverifiable raises (true → 1) or drops (false → delete) target's distinct
// unverifiable signal. It is raised when a target the worker SHOULD be able to read turns
// unreadable — so dropping the stale-green _ok series does not go silent — and dropped again once the
// target verifies or is structurally-unreadable-by-design. Deleting an absent series is a no-op.
func (m *Metrics) SetImmutabilityUnverifiable(target string, unverifiable bool) {
	if unverifiable {
		m.immutabilityUnverifiable.WithLabelValues(target).Set(1)
		return
	}
	m.immutabilityUnverifiable.DeleteLabelValues(target)
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
	passed      bool // whether the recorded run passed — drives the last-success carry-forward
}

// drillLastSuccessMetric is the last-success gauge name — shared by NewDrillMetrics and the
// carry-forward parse (readPriorLastSuccess), so they can't disagree on the metric name.
const drillLastSuccessMetric = "filebackup_drill_last_success_timestamp_seconds"

// NewDrillMetrics builds the drill metric set on a private, collector-free registry.
func NewDrillMetrics() *DrillMetrics {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	return &DrillMetrics{
		reg: reg,
		pass: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_restore_drill_pass",
			Help: "Last restore-drill result: 1 = every sampled object restored + hash-matched, 0 = at least one failed (or 0 sampled). Set by the `drill` subcommand; exported via a textfile (--metrics-file) since the drill process is short-lived. Alert on == 0.",
		}),
		lastSuccess: f.NewGauge(prometheus.GaugeOpts{
			Name: drillLastSuccessMetric,
			Help: "Unix timestamp (seconds) of the last FULLY-PASSING restore drill; 0 until the first. Preserved across FAILING runs. Alert on time() - this > a week to catch a drill that stopped succeeding.",
		}),
	}
}

// SetPass records a drill outcome: the pass gauge (1/0) and, ON A PASS, the last-success timestamp.
// A FAILING/0-checked run leaves last-success at 0 HERE — WriteTextfile then carries forward the
// prior textfile's last-success so a failing run never CLOBBERS the true last-success to 0 (each
// drill is a separate short-lived process overwriting the same file).
func (d *DrillMetrics) SetPass(pass bool, at time.Time) {
	d.passed = pass
	d.pass.Set(b2f(pass))
	if pass {
		d.lastSuccess.Set(float64(at.Unix()))
	}
}

// WriteTextfile durably writes the drill registry to path in Prometheus text-exposition format via
// fsutil.CommitWrite (temp → fsync → rename → parent-dir fsync, so a crash can't leave a torn or
// non-durable file). On a NON-pass run it first carries forward the PRIOR textfile's last-success
// timestamp, so a failing/0-checked drill overwriting the file never resets last-success to 0 — the
// file always carries the true last-success. A "" path is a no-op (the exit code carries the signal).
func (d *DrillMetrics) WriteTextfile(path string) error {
	if path == "" {
		return nil
	}
	if !d.passed {
		if prior := readPriorLastSuccess(path); prior > 0 {
			d.lastSuccess.Set(prior)
		}
	}
	mfs, gerr := d.reg.Gather()
	if gerr != nil {
		return fmt.Errorf("gather metrics: %w", gerr)
	}
	return fsutil.CommitWrite(context.Background(), filepath.Dir(path), filepath.Base(path), func(f *os.File) error {
		enc := expfmt.NewEncoder(f, expfmt.NewFormat(expfmt.TypeTextPlain))
		for _, mf := range mfs {
			if eerr := enc.Encode(mf); eerr != nil {
				return fmt.Errorf("encode metrics: %w", eerr)
			}
		}
		return nil
	})
}

// readPriorLastSuccess parses the existing textfile for the last-success gauge's value, so a
// failing run can re-emit it unchanged. It uses expfmt.TextParser — the SAME exposition-format
// parser Prometheus itself uses — rather than a fragile hand-rolled 2-field split that a HELP/TYPE
// comment, a label set, or a scientific-notation value could defeat. A missing/unparseable file, or
// an absent metric, yields 0 (nothing to carry).
func readPriorLastSuccess(path string) float64 {
	f, err := os.Open(path) //nolint:gosec // operator-configured metrics textfile path
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	// NewTextParser (not a zero-value TextParser): a zero-value parser has an UnsetValidation name
	// scheme, whose IsValidMetricName PANICS in prometheus/common v0.66.1 — which would crash any
	// failing drill run that reads a prior textfile. Use the library's default name-validation scheme.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	mfs, err := parser.TextToMetricFamilies(f)
	if err != nil {
		return 0
	}
	mf, ok := mfs[drillLastSuccessMetric]
	if !ok {
		return 0
	}
	for _, m := range mf.GetMetric() {
		if g := m.GetGauge(); g != nil {
			return g.GetValue()
		}
	}
	return 0
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
