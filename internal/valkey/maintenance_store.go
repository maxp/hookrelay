package valkey

import (
	"context"

	"github.com/maxp/hookrelay/internal/delivery"
)

// DueRetries reads at most limit due hr1:retries members, oldest first,
// against Valkey time.
func (s *DeliveryStore) DueRetries(ctx context.Context, limit int) (delivery.DueBatch, error) {
	return s.dueEntries(ctx, "hr1:retries", limit)
}

func (s *DeliveryStore) dueEntries(ctx context.Context, key string, limit int) (delivery.DueBatch, error) {
	now, err := s.a.serverTimeMs(ctx)
	if err != nil {
		return delivery.DueBatch{}, err
	}
	c := s.a.client
	scored, err := c.Do(ctx, c.B().Zrange().Key(key).Min("-inf").Max(itoa64(now)).Byscore().Limit(0, int64(limit)).Withscores().Build()).AsZScores()
	if err != nil {
		return delivery.DueBatch{}, err
	}
	batch := delivery.DueBatch{NowMs: now, Entries: make([]delivery.DueEntry, 0, len(scored))}
	for _, z := range scored {
		batch.Entries = append(batch.Entries, delivery.DueEntry{RecipientIdentity: z.Member, DueMs: int64(z.Score)})
	}
	return batch, nil
}

// ActivateRetry runs activate_retry_v1 for one Recipient.
func (s *DeliveryStore) ActivateRetry(ctx context.Context, recipientIdentity string) delivery.ActivationResult {
	res, err := s.a.RunScript(ctx, "activate_retry_v1",
		[]string{"hr1:ready", "hr1:ready_seq", "hr1:retries", "hr1:blocked"},
		[]string{recipientIdentity, "hr1"})
	if err != nil {
		// A lost response is safe: a repeat re-validates the head state.
		return delivery.ActivationResult{Outcome: delivery.ActivationDependencyUnavailable}
	}
	switch res.Status {
	case "activated":
		id, err1 := res.Fields[0].ToString()
		attempt, err2 := res.Fields[1].AsInt64()
		if err1 != nil || err2 != nil {
			return delivery.ActivationResult{Outcome: delivery.ActivationInternalFailure}
		}
		return delivery.ActivationResult{Outcome: delivery.ActivationActivated, MessageID: id, Attempt: attempt}
	case "wrong_type":
		return delivery.ActivationResult{Outcome: delivery.ActivationInternalFailure}
	default:
		return delivery.ActivationResult{Outcome: delivery.ActivationOutcome(res.Status)}
	}
}
