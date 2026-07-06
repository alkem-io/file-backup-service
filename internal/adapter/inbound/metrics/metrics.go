// Package metrics is the Prometheus adapter — it implements domain.Metrics and
// serves /metrics. See specs/008 contracts/restore-and-ops.md.
package metrics

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// Metrics holds the Prometheus collectors.
type Metrics struct {
	reg        *prometheus.Registry
	objects    *prometheus.CounterVec
	bytes      *prometheus.CounterVec
	deadletter prometheus.Counter
	timeout    prometheus.Counter
	sourceGone prometheus.Counter
	// RPO/lag gauges — the alerting spine (FR-026/SC-001). Set periodically by the
	// serve sampler, not on the per-object path.
	backlogPending   prometheus.Gauge
	oldestPendingAge prometheus.Gauge
	lastSuccessAge   prometheus.Gauge
	underReplicated  prometheus.Gauge // objects not yet stored on every target (coverage)
	sampleErrors     prometheus.Counter
	byTarget         sync.Map // target name -> *targetCounters (resolved once, per target)
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
		backlogPending: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_outbox_pending", Help: "Outbox entries awaiting backup (backlog depth).",
		}),
		oldestPendingAge: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_oldest_pending_age_seconds",
			Help: "Age of the oldest pending outbox entry — the backup lag / RPO signal.",
		}),
		lastSuccessAge: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_last_success_age_seconds",
			Help: "Seconds since the most recent verified backup (0 until the first).",
		}),
		underReplicated: f.NewGauge(prometheus.GaugeOpts{
			Name: "filebackup_under_replicated_objects",
			Help: "Objects not yet stored on every configured target — coverage backstop that a dead-lettered object (drained from the backlog) can't hide from.",
		}),
		sampleErrors: f.NewCounter(prometheus.CounterOpts{
			Name: "filebackup_metrics_sample_errors_total",
			Help: "Failed RPO/coverage sampling passes — alert on rate>0 so a frozen (stale-green) gauge is itself detectable.",
		}),
	}
}

// SetBacklog updates the outbox backlog gauges (pending count + oldest-pending age).
func (m *Metrics) SetBacklog(pending int, oldestAgeSec float64) {
	m.backlogPending.Set(float64(pending))
	m.oldestPendingAge.Set(oldestAgeSec)
}

// SetLastSuccessAge records seconds since the most recent verified backup.
func (m *Metrics) SetLastSuccessAge(ageSec float64) { m.lastSuccessAge.Set(ageSec) }

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

// Handler returns the Prometheus HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
