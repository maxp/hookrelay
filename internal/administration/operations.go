package administration

import (
	"context"
	"errors"
	"net/http"
)

// OperationsSnapshot is one consistent read of the operational counters
// plus Valkey memory. A zero time means "none" (no lease, no DLQ entry).
type OperationsSnapshot struct {
	QueuedMessages         int64
	ReadyRecipients        int64
	LeasedRecipients       int64
	RetryWaitRecipients    int64
	BlockedRecipients      int64
	EarliestLeaseExpiresMs int64
	EarliestRetryAtMs      int64
	DeadLetters            int64
	OldestDeadLetteredMs   int64
	NewestDeadLetteredMs   int64
	WebhookEndpoints       int64
	AdminSessions          int64
	AuditLength            int64
	UsedMemoryBytes        int64
	MaxMemoryBytes         int64
}

// OperationsReader reads the operations snapshot; a key of another type is
// ErrStoredWrongType.
type OperationsReader interface {
	OperationsSnapshot(ctx context.Context) (OperationsSnapshot, error)
}

// ReadinessView is the process readiness the summary reports.
type ReadinessView interface {
	Ready() bool
	AcceptingWebhooks() bool
	ReconciliationState() string
}

// OperationsSummary is the operations summary response.
type OperationsSummary struct {
	GeneratedMs      int64              `json:"generated_ms"`
	Readiness        *summaryReadiness  `json:"readiness,omitempty"`
	Valkey           summaryValkey      `json:"valkey"`
	Queues           summaryQueues      `json:"queues"`
	DeadLetters      summaryDeadLetters `json:"dead_letters"`
	WebhookEndpoints summaryCount       `json:"webhook_endpoints"`
	AdminSessions    summaryIndexed     `json:"admin_sessions"`
	Audit            summaryLength      `json:"audit"`
	Links            summaryLinks       `json:"links"`
}

type summaryReadiness struct {
	Ready                 bool   `json:"ready"`
	AcceptingWebhooks     bool   `json:"accepting_webhooks"`
	StartupReconciliation string `json:"startup_reconciliation"`
}

type summaryValkey struct {
	UsedMemoryBytes int64 `json:"used_memory_bytes"`
	MaxMemoryBytes  int64 `json:"maxmemory_bytes"`
}

type summaryQueues struct {
	QueuedMessages         int64 `json:"queued_messages"`
	ReadyRecipients        int64 `json:"ready_recipients"`
	LeasedRecipients       int64 `json:"leased_recipients"`
	RetryWaitRecipients    int64 `json:"retry_wait_recipients"`
	BlockedRecipients      int64 `json:"blocked_recipients"`
	EarliestLeaseExpiresMs int64 `json:"earliest_lease_expires_ms,omitempty"`
	EarliestRetryAtMs      int64 `json:"earliest_retry_at_ms,omitempty"`
}

type summaryDeadLetters struct {
	Count                int64 `json:"count"`
	OldestDeadLetteredMs int64 `json:"oldest_dead_lettered_ms,omitempty"`
	NewestDeadLetteredMs int64 `json:"newest_dead_lettered_ms,omitempty"`
}

type summaryCount struct {
	Count int64 `json:"count"`
}

type summaryIndexed struct {
	Indexed int64 `json:"indexed"`
}

type summaryLength struct {
	Length int64 `json:"length"`
}

type summaryLinks struct {
	GrafanaURL string `json:"grafana_url,omitempty"`
}

// OperationsSummary reports readiness, Valkey memory, queue depths, the
// DLQ, and the configured dashboard link. It is served while the process
// is not ready (it then shows why); only a Valkey failure refuses it.
func (s *Service) OperationsSummary(ctx context.Context) (OperationsSummary, error) {
	snap, err := s.operations.OperationsSnapshot(ctx)
	if err != nil {
		if errors.Is(err, ErrStoredWrongType) {
			return OperationsSummary{}, DependencyError{detail: ErrStoredWrongType.Error()}
		}
		return OperationsSummary{}, DependencyError{}
	}
	out := OperationsSummary{
		GeneratedMs: s.now().UnixMilli(),
		Valkey:      summaryValkey{UsedMemoryBytes: snap.UsedMemoryBytes, MaxMemoryBytes: snap.MaxMemoryBytes},
		Queues: summaryQueues{QueuedMessages: snap.QueuedMessages, ReadyRecipients: snap.ReadyRecipients,
			LeasedRecipients: snap.LeasedRecipients, RetryWaitRecipients: snap.RetryWaitRecipients, BlockedRecipients: snap.BlockedRecipients,
			EarliestLeaseExpiresMs: snap.EarliestLeaseExpiresMs, EarliestRetryAtMs: snap.EarliestRetryAtMs},
		DeadLetters: summaryDeadLetters{Count: snap.DeadLetters, OldestDeadLetteredMs: snap.OldestDeadLetteredMs,
			NewestDeadLetteredMs: snap.NewestDeadLetteredMs},
		WebhookEndpoints: summaryCount{Count: snap.WebhookEndpoints},
		AdminSessions:    summaryIndexed{Indexed: snap.AdminSessions},
		Audit:            summaryLength{Length: snap.AuditLength},
		Links:            summaryLinks{GrafanaURL: s.grafanaURL},
	}
	if s.readiness != nil {
		out.Readiness = &summaryReadiness{Ready: s.readiness.Ready(), AcceptingWebhooks: s.readiness.AcceptingWebhooks(),
			StartupReconciliation: s.readiness.ReconciliationState()}
	}
	return out, nil
}

func (s *Service) handleOperationsSummary(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	v, err := s.OperationsSummary(r.Context())
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, v)
}
