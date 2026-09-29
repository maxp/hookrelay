package delivery

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	claims            *prometheus.CounterVec
	attempts          *prometheus.CounterVec
	attemptDuration   *prometheus.HistogramVec
	retriesWaiting    prometheus.Gauge
	activeLeases      prometheus.Gauge
	readyRecipients   prometheus.Gauge
	blockedRecipients prometheus.Gauge
	queueMessages     prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer, waiting func() float64) (*metrics, error) {
	m := &metrics{
		claims: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_delivery_claims_total",
			Help: "Consumer claim requests by bounded outcome.",
		}, []string{"outcome"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_delivery_attempts_total",
			Help: "Completed Delivery Attempts by Recipient scope and outcome.",
		}, []string{"recipient_scope", "outcome"}),
		attemptDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hookrelay_delivery_attempt_duration_seconds",
			Help:    "Completed Delivery Attempt duration from claim to completion (Valkey time), by Recipient scope and outcome.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}, []string{"recipient_scope", "outcome"}),
		retriesWaiting: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookrelay_retries_waiting",
			Help: "Recipients whose head message waits for a retry.",
		}),
		activeLeases: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookrelay_active_leases",
			Help: "Unexpired Message Leases, work-pool-wide.",
		}),
		readyRecipients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookrelay_ready_recipients",
			Help: "Recipients in the ready index.",
		}),
		blockedRecipients: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookrelay_blocked_recipients",
			Help: "Recipients held by a block marker.",
		}),
		queueMessages: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookrelay_queue_messages",
			Help: "Messages queued across all Recipients.",
		}),
	}
	collectors := []prometheus.Collector{m.claims, m.attempts, m.attemptDuration, m.retriesWaiting, m.activeLeases, m.readyRecipients, m.blockedRecipients, m.queueMessages,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hookrelay_waiting_claims",
			Help: "Claim requests waiting for work in this process.",
		}, waiting),
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("delivery: register metrics: %w", err)
		}
	}
	return m, nil
}
