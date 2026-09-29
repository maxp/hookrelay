package valkey

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/valkey-io/valkey-go"
)

// scriptOperations maps each registered script to its bounded operation
// label; non-script reads use endpoint_read.
var scriptOperations = map[string]string{
	"endpoint_create_v1":     "endpoint_create",
	"accept_v2":              "accept",
	"claim_v3":               "claim",
	"ack_v3":                 "acknowledgement",
	"nack_v2":                "negative_acknowledgement",
	"activate_retry_v1":      "retry_activation",
	"extend_v1":              "extension",
	"expire_lease_v2":        "lease_expiry",
	"reconcile_recipient_v2": "reconciliation",
	"reconcile_dlq_v1":       "reconciliation",
	"reconcile_dedup_v1":     "reconciliation",
	"reconcile_counter_v1":   "reconciliation",
}

// errScriptResult marks a script result the typed parser rejected.
var errScriptResult = errors.New("valkey: invalid script result")

// adapterMetrics are the required Valkey metrics. A nil *adapterMetrics
// records nothing, so an uninstrumented adapter (tests) works unchanged.
type adapterMetrics struct {
	operations   *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	scriptErrors *prometheus.CounterVec
	connected    prometheus.Gauge
}

// Instrument registers the Valkey metrics and starts recording them.
func (a *Adapter) Instrument(reg prometheus.Registerer) error {
	m := &adapterMetrics{
		operations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_valkey_operations_total",
			Help: "Valkey operations by bounded operation name and outcome (success, error).",
		}, []string{"operation", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hookrelay_valkey_operation_duration_seconds",
			Help:    "Valkey operation duration, including one NOSCRIPT reload when needed.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"operation"}),
		scriptErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_valkey_script_errors_total",
			Help: "Script executions that failed server-side or returned a result the typed parser rejected.",
		}, []string{"script"}),
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookrelay_valkey_connected",
			Help: "1 while the readiness gate's PING succeeds, 0 otherwise.",
		}),
	}
	for _, c := range []prometheus.Collector{m.operations, m.duration, m.scriptErrors, m.connected} {
		if err := reg.Register(c); err != nil {
			return fmt.Errorf("valkey: register metrics: %w", err)
		}
	}
	for script := range registry {
		m.scriptErrors.WithLabelValues(script)
	}
	a.metrics = m
	return nil
}

// observe records one operation's outcome and duration.
func (m *adapterMetrics) observe(operation string, start time.Time, err error) {
	if m == nil {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	m.operations.WithLabelValues(operation, outcome).Inc()
	m.duration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
}

// observeScript records a script call; server-side script errors and
// rejected results also count as script errors, transport failures do not.
func (m *adapterMetrics) observeScript(script string, start time.Time, err error) {
	if m == nil {
		return
	}
	operation, ok := scriptOperations[script]
	if !ok {
		operation = "other"
	}
	m.observe(operation, start, err)
	if err == nil || errors.Is(err, ErrNotDispatched) {
		return
	}
	var serverErr *valkey.ValkeyError
	if errors.As(err, &serverErr) || errors.Is(err, errScriptResult) {
		m.scriptErrors.WithLabelValues(script).Inc()
	}
}

func (m *adapterMetrics) setConnected(ok bool) {
	if m == nil {
		return
	}
	if ok {
		m.connected.Set(1)
	} else {
		m.connected.Set(0)
	}
}
