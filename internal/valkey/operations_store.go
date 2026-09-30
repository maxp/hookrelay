package valkey

import (
	"context"
	"fmt"

	"github.com/maxp/hookrelay/internal/administration"
)

type operationsStore struct{ a *Adapter }

// NewOperationsStore returns the operations summary reader.
func NewOperationsStore(a *Adapter) administration.OperationsReader { return &operationsStore{a: a} }

// OperationsSnapshot runs the read-only operations_summary_v1 snapshot and
// adds Valkey memory from INFO (a separate, non-atomic read).
func (s *operationsStore) OperationsSnapshot(ctx context.Context) (administration.OperationsSnapshot, error) {
	res, err := s.a.RunScript(ctx, "operations_summary_v1",
		[]string{"hr1:stats:queued_messages", "hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked", "hr1:dlq", "hr1:webhooks",
			"hr1:admin_sessions", auditKey},
		[]string{"hr1"})
	if err != nil {
		return administration.OperationsSnapshot{}, err
	}
	if res.Status == "wrong_type" {
		return administration.OperationsSnapshot{}, administration.ErrStoredWrongType
	}
	v := make([]int64, len(res.Fields))
	for i, f := range res.Fields {
		if v[i], err = f.AsInt64(); err != nil {
			return administration.OperationsSnapshot{}, fmt.Errorf("valkey: operations_summary_v1: field %d: %w", i, err)
		}
	}
	mem, err := s.a.memory(ctx)
	if err != nil {
		return administration.OperationsSnapshot{}, err
	}
	return administration.OperationsSnapshot{
		QueuedMessages: v[0], ReadyRecipients: v[1], LeasedRecipients: v[2], RetryWaitRecipients: v[3], BlockedRecipients: v[4],
		EarliestLeaseExpiresMs: v[5], EarliestRetryAtMs: v[6], DeadLetters: v[7], OldestDeadLetteredMs: v[8], NewestDeadLetteredMs: v[9],
		WebhookEndpoints: v[10], AdminSessions: v[11], AuditLength: v[12], UsedMemoryBytes: mem.used, MaxMemoryBytes: mem.max,
	}, nil
}
