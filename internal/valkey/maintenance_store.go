package valkey

import (
	"context"
	"errors"
	"strings"

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

// DueLeases reads at most limit due hr1:leases members, oldest first,
// against Valkey time.
func (s *DeliveryStore) DueLeases(ctx context.Context, limit int) (delivery.DueBatch, error) {
	return s.dueEntries(ctx, "hr1:leases", limit)
}

// ExpireLease runs expire_lease_v2 for one Recipient.
func (s *DeliveryStore) ExpireLease(ctx context.Context, recipientIdentity string, retryDelaysMs []int64, maxAttempts int) delivery.ExpiryResult {
	delays := make([]string, len(retryDelaysMs))
	for i, d := range retryDelaysMs {
		delays[i] = itoa64(d)
	}
	res, err := s.a.RunScript(ctx, "expire_lease_v2",
		[]string{"hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:retries", "hr1:blocked", "hr1:dlq", "hr1:stats:queued_messages"},
		[]string{recipientIdentity, strings.Join(delays, ","), itoa64(int64(maxAttempts)), itoa64(TombstoneTTL.Milliseconds()), "hr1"})
	if err != nil {
		// A lost response is safe: a repeat re-validates the head state.
		return delivery.ExpiryResult{Outcome: delivery.ExpiryDependencyUnavailable}
	}
	switch res.Status {
	case "retry_scheduled":
		var perr error
		num := func(i int) int64 {
			v, err := res.Fields[i].AsInt64()
			if err != nil {
				perr = err
			}
			return v
		}
		id, err1 := res.Fields[0].ToString()
		instance, err2 := res.Fields[6].ToString()
		out := delivery.ExpiryResult{
			Outcome: delivery.ExpiryRetryScheduled, MessageID: id, Attempt: num(1), RetryAtMs: num(2),
			DeliveryCycle: num(3), ClaimedMs: num(4), ExpiredMs: num(5), ConsumerInstanceID: instance,
		}
		if err1 != nil || err2 != nil || perr != nil {
			return delivery.ExpiryResult{Outcome: delivery.ExpiryInternalFailure}
		}
		return out
	case "dead_lettered":
		id, err1 := res.Fields[0].ToString()
		instance, err2 := res.Fields[5].ToString()
		cycle, err3 := res.Fields[1].AsInt64()
		at, err4 := res.Fields[2].AsInt64()
		attempt, err5 := res.Fields[3].AsInt64()
		claimed, err6 := res.Fields[4].AsInt64()
		if err := errors.Join(err1, err2, err3, err4, err5, err6); err != nil {
			return delivery.ExpiryResult{Outcome: delivery.ExpiryInternalFailure}
		}
		// The dead-letter time is also when the lease expired.
		return delivery.ExpiryResult{
			Outcome: delivery.ExpiryDeadLettered, MessageID: id, DeliveryCycle: cycle, DeadLetteredMs: at,
			ExpiredMs: at, Attempt: attempt, ClaimedMs: claimed, ConsumerInstanceID: instance,
		}
	case "wrong_type":
		return delivery.ExpiryResult{Outcome: delivery.ExpiryInternalFailure}
	default:
		return delivery.ExpiryResult{Outcome: delivery.ExpiryOutcome(res.Status)}
	}
}
