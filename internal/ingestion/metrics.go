package ingestion

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// unknownLabel aggregates unregistered Webhook Types and unknown event types
// so attacker-controlled path segments never become label values.
const unknownLabel = "unknown"

type metrics struct {
	requests            *prometheus.CounterVec
	duration            *prometheus.HistogramVec
	bodyBytes           *prometheus.HistogramVec
	accepted            *prometheus.CounterVec
	duplicates          *prometheus.CounterVec
	dedupConflicts      *prometheus.CounterVec
	dedupCapacity       prometheus.Counter
	routingIssues       *prometheus.CounterVec
	dedupRecords        prometheus.Gauge
	eventTimeIssues     *prometheus.CounterVec
	dedupRecordCapacity prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	m := &metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_webhook_requests_total",
			Help: "Webhook requests by Webhook Type and bounded outcome.",
		}, []string{"webhook_type", "outcome"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hookrelay_webhook_request_duration_seconds",
			Help:    "Webhook request handling duration.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"webhook_type"}),
		bodyBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hookrelay_webhook_request_body_bytes",
			Help:    "Size of webhook request bodies read.",
			Buckets: prometheus.ExponentialBuckets(256, 4, 6), // 256 B … 256 KiB
		}, []string{"webhook_type"}),
		accepted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_messages_accepted_total",
			Help: "Canonical Messages accepted, by Bot Platform and Recipient scope.",
		}, []string{"bot_platform", "recipient_scope"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_messages_duplicate_total",
			Help: "Webhook requests proven duplicate.",
		}, []string{"webhook_type"}),
		dedupConflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_dedup_conflicts_total",
			Help: "Duplicates whose request body differs from the original.",
		}, []string{"webhook_type"}),
		dedupCapacity: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "hookrelay_dedup_capacity_rejections_total",
			Help: "Webhook requests rejected because the deduplication record cap was reached.",
		}),
		routingIssues: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_routing_issues_total",
			Help: "Accepted relay-scoped messages by bounded routing issue.",
		}, []string{"bot_platform", "reason"}),
		eventTimeIssues: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_event_time_issues_total",
			Help: "Platform event timestamps that were missing or unusable, so occurred_ms was omitted.",
		}, []string{"bot_platform", "reason"}),
		dedupRecords: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookrelay_dedup_records",
			Help: "Live deduplication records, refreshed by the acceptance probe.",
		}),
		dedupRecordCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "hookrelay_dedup_record_capacity",
			Help: "Configured maximum number of deduplication records.",
		}),
	}
	for _, c := range []prometheus.Collector{m.requests, m.duration, m.bodyBytes, m.accepted, m.duplicates, m.dedupConflicts, m.dedupCapacity, m.routingIssues, m.eventTimeIssues, m.dedupRecords, m.dedupRecordCapacity} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("ingestion: register metrics: %w", err)
		}
	}
	return m, nil
}

// registerInflight exports the in-flight semaphore occupancy.
func (m *metrics) registerInflight(reg prometheus.Registerer, occupancy func() float64) error {
	if err := reg.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "hookrelay_webhook_inflight",
		Help: "Webhook requests currently holding an in-flight slot.",
	}, occupancy)); err != nil {
		return fmt.Errorf("ingestion: register metrics: %w", err)
	}
	return nil
}
