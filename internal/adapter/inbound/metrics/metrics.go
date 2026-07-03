// Package metrics is the Prometheus adapter — it implements domain.Metrics and
// serves /metrics. See specs/008 contracts/restore-and-ops.md.
package metrics

import (
	"net/http"

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
	}
}

// ObjectStored implements domain.Metrics.
func (m *Metrics) ObjectStored(target string, storedBytes int64) {
	m.objects.WithLabelValues(domain.StateStored, target).Inc()
	m.bytes.WithLabelValues(target).Add(float64(storedBytes))
}

// ObjectFailed implements domain.Metrics.
func (m *Metrics) ObjectFailed(target string) {
	m.objects.WithLabelValues(domain.StateFailed, target).Inc()
}

// ObjectDedup implements domain.Metrics.
func (m *Metrics) ObjectDedup(target string) { m.objects.WithLabelValues("dedup", target).Inc() }

// DeadLetter records an entry moved to dead-letter.
func (m *Metrics) DeadLetter() { m.deadletter.Inc() }

// Handler returns the Prometheus HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
