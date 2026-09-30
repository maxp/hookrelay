package administration

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

// Bounded audit operation and outcome label values.
const (
	opWebhookEndpointCreated  = "webhook_endpoint_created"
	opAdminAuthRejected       = "admin_auth_rejected"
	opRecipientBlockCleared   = "recipient_block_cleared"
	opDeadLetterReplayed      = "dead_letter_replayed"
	opWebhookEndpointEnabled  = "webhook_endpoint_enabled"
	opWebhookEndpointDisabled = "webhook_endpoint_disabled"
	opWebhookEndpointDeleted  = "webhook_endpoint_deleted"
	opDeadLetterPayloadViewed = "dead_letter_payload_viewed"
	opDeadLetterDeleted       = "dead_letter_deleted"

	outcomeSuccess = "success"
	outcomeFailure = "failure"

	// replayFailed is the replay outcome for a definite storage refusal
	// or an unavailable dependency; the other outcomes are ReplayResult
	// values.
	replayFailed = "failed"

	// Payload inspection outcomes.
	payloadViewDisclosed   = "disclosed"
	payloadViewNotFound    = "not_found"
	payloadViewUnavailable = "unavailable"

	// Permanent deletion outcomes.
	dlqDeletionDeleted     = "deleted"
	dlqDeletionAbsent      = "absent"
	dlqDeletionRefused     = "refused"
	dlqDeletionUnavailable = "unavailable"
)

// metrics holds the required administrative audit metrics. Labels use only
// the bounded operation and outcome allowlists above.
type metrics struct {
	auditEvents        *prometheus.CounterVec
	auditWriteFailures *prometheus.CounterVec
	replays            *prometheus.CounterVec
	payloadViews       *prometheus.CounterVec
	dlqDeletions       *prometheus.CounterVec
	loginAttempts      *prometheus.CounterVec
	csrfRejections     *prometheus.CounterVec
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
		payloadViews: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_dead_letter_payload_views_total",
			Help: "Privileged dead-letter payload inspections, by bounded outcome.",
		}, []string{"outcome"}),
		dlqDeletions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_dead_letter_deletions_total",
			Help: "Administrative permanent dead-letter deletions, by bounded outcome.",
		}, []string{"outcome"}),
		loginAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_admin_login_attempts_total",
			Help: "Administrative browser login attempts, by bounded outcome.",
		}, []string{"outcome"}),
		csrfRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_admin_csrf_rejections_total",
			Help: "Cookie-authenticated requests refused by the Origin or CSRF check, by reason.",
		}, []string{"reason"}),
	}
	for _, c := range []prometheus.Collector{m.auditEvents, m.auditWriteFailures, m.replays, m.payloadViews, m.dlqDeletions,
		m.loginAttempts, m.csrfRejections} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("administration: register metrics: %w", err)
		}
	}
	// Pre-create the bounded series so they are exported at zero.
	m.auditEvents.WithLabelValues(opWebhookEndpointCreated, outcomeSuccess)
	m.auditEvents.WithLabelValues(opAdminAuthRejected, outcomeFailure)
	m.auditEvents.WithLabelValues(opRecipientBlockCleared, outcomeSuccess)
	m.auditEvents.WithLabelValues(opDeadLetterReplayed, outcomeSuccess)
	m.auditEvents.WithLabelValues(opDeadLetterPayloadViewed, outcomeSuccess)
	for _, o := range []string{payloadViewDisclosed, payloadViewNotFound, payloadViewUnavailable} {
		m.payloadViews.WithLabelValues(o)
	}
	m.auditEvents.WithLabelValues(opDeadLetterDeleted, outcomeSuccess)
	for _, o := range []string{loginSuccess, loginFailure, loginRateLimited, loginCapacityExceeded, loginUnavailable} {
		m.loginAttempts.WithLabelValues(o)
	}
	for _, r := range []string{csrfReasonOrigin, csrfReasonToken} {
		m.csrfRejections.WithLabelValues(r)
	}
	for _, o := range []string{dlqDeletionDeleted, dlqDeletionAbsent, dlqDeletionRefused, dlqDeletionUnavailable} {
		m.dlqDeletions.WithLabelValues(o)
	}
	for _, o := range []string{string(ReplayReplayed), string(ReplayNotFound), string(ReplayMessageMissing), string(ReplayRecipientBlocked),
		string(ReplayDeduplicationConflict), string(ReplayUncertain), replayFailed} {
		m.replays.WithLabelValues(o)
	}
	m.auditWriteFailures.WithLabelValues(opAdminAuthRejected)
	return m, nil
}
