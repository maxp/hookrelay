package administration

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Bounded audit operation and outcome label values.
const (
	opWebhookEndpointCreated = "webhook_endpoint_created"
	opAdminAuthRejected      = "admin_auth_rejected"
	opRecipientBlockCleared  = "recipient_block_cleared"
	opDeadLetterReplayed     = "dead_letter_replayed"

	outcomeSuccess = "success"
	outcomeFailure = "failure"

	// replayFailed is the replay outcome for a definite storage refusal
	// or an unavailable dependency; the other outcomes are ReplayResult
	// values.
	replayFailed = "failed"
)

// metrics holds the required administrative audit metrics. Labels use only
// the bounded operation and outcome allowlists above.
type metrics struct {
	auditEvents        *prometheus.CounterVec
	auditWriteFailures *prometheus.CounterVec
	replays            *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) (*metrics, error) {
	m := &metrics{
		auditEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_audit_events_total",
			Help: "Administrative audit events appended to Valkey, by operation and outcome.",
		}, []string{"operation", "outcome"}),
		auditWriteFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_audit_write_failures_total",
			Help: "Best-effort administrative audit appends that failed, by operation.",
		}, []string{"operation"}),
		replays: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_dead_letter_replays_total",
			Help: "Administrative dead-letter replays, by bounded outcome.",
		}, []string{"outcome"}),
	}
	for _, c := range []prometheus.Collector{m.auditEvents, m.auditWriteFailures, m.replays} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("administration: register metrics: %w", err)
		}
	}
	// Pre-create the bounded series so they are exported at zero.
	m.auditEvents.WithLabelValues(opWebhookEndpointCreated, outcomeSuccess)
	m.auditEvents.WithLabelValues(opAdminAuthRejected, outcomeFailure)
	m.auditEvents.WithLabelValues(opRecipientBlockCleared, outcomeSuccess)
	m.auditEvents.WithLabelValues(opDeadLetterReplayed, outcomeSuccess)
	for _, o := range []string{string(ReplayReplayed), string(ReplayNotFound), string(ReplayMessageMissing), string(ReplayRecipientBlocked),
		string(ReplayDeduplicationConflict), string(ReplayUncertain), replayFailed} {
		m.replays.WithLabelValues(o)
	}
	m.auditWriteFailures.WithLabelValues(opAdminAuthRejected)
	return m, nil
}
