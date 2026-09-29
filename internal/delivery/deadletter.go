package delivery

import (
	"log/slog"

	"github.com/maxp/hookrelay/internal/observability"
)

// Dead-letter reasons (the bounded reason label).
const (
	reasonNackExhausted   = "nack_exhausted"
	reasonExpiryExhausted = "expiry_exhausted"
)

// deadLetter is one final attempt that moved its message to the DLQ.
type deadLetter struct {
	RecipientIdentity string
	MessageID         string
	Reason            string
	DeliveryCycle     int64
	Attempt           int64
	ClaimedMs         int64
	DeadLetteredMs    int64
}

// recordDeadLetter counts the final attempt as outcome=dead_lettered (only)
// and the dead-letter by reason, and emits delivery_dead_lettered with the
// safe Recipient fields plus extra (request_id, reason_code,
// consumer_instance_id where known).
func recordDeadLetter(log *slog.Logger, attempts *AttemptMetrics, d deadLetter, extra ...any) {
	fields := []any{
		"message_id", d.MessageID, "delivery_cycle", d.DeliveryCycle, "attempt", d.Attempt,
		"dead_lettered_ms", d.DeadLetteredMs, "dead_letter_reason", d.Reason,
		"duration_ms", d.DeadLetteredMs - d.ClaimedMs,
	}
	fields = append(fields, extra...)
	fields, scope := recipientEventFields(d.RecipientIdentity, fields)
	attempts.Observe(scope, "dead_lettered", d.ClaimedMs, d.DeadLetteredMs)
	attempts.countDeadLetter(scope, d.Reason)
	observability.LogEvent(log, slog.LevelWarn, "delivery_dead_lettered", "message moved to the dead-letter queue", fields...)
}
