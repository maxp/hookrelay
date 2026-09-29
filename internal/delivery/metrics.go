package delivery

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	claims            *prometheus.CounterVec
	attempts          *prometheus.CounterVec
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
	collectors := []prometheus.Collector{m.claims, m.attempts, m.activeLeases, m.readyRecipients, m.blockedRecipients, m.queueMessages,
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
