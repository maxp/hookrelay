package valkey

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/maxp/hookrelay/internal/delivery"
)

// ClaimOpTTL is the fixed claim operation record retention.
const ClaimOpTTL = 10 * time.Minute

// ClaimLimits are the lease bounds passed to claim_v3.
type ClaimLimits struct {
	MaxActiveLeases      int
	InitialLeaseDuration time.Duration
}

// DeliveryStore implements delivery.Claimer with claim_v3 and
// delivery.StatsReader for the delivery gauges.
type DeliveryStore struct {
	a      *Adapter
	limits ClaimLimits
}

// NewDeliveryStore returns the delivery storage implementation.
func NewDeliveryStore(a *Adapter, limits ClaimLimits) *DeliveryStore {
	return &DeliveryStore{a: a, limits: limits}
}

func (s *DeliveryStore) Claim(ctx context.Context, req delivery.ClaimRequest) delivery.ClaimResult {
	recordEmpty := "0"
	if req.RecordEmpty {
		recordEmpty = "1"
	}
	res, err := s.a.RunScript(ctx, "claim_v3",
		[]string{"hr1:ready", "hr1:leases", "hr1:blocked"},
		[]string{
			req.OperationID,
			req.ArgsDigest,
			itoa64(int64(s.limits.MaxActiveLeases)),
			itoa64(ClaimOpTTL.Milliseconds()),
			itoa64(s.limits.InitialLeaseDuration.Milliseconds()),
			req.Token,
			req.TokenDigest,
			req.ConsumerInstanceID,
			recordEmpty,
			"hr1",
		})
	if err != nil {
		// Whether or not the script ran, repeating the same operation_id
		// recovers a lease it may have created.
		return delivery.ClaimResult{Outcome: delivery.ClaimDependencyUnavailable}
	}
	switch res.Status {
	case "claimed", "replay_active":
		out := delivery.ClaimResult{Outcome: delivery.ClaimOutcome(res.Status)}
		var perr error
		str := func(i int) string {
			v, err := res.Fields[i].ToString()
			if err != nil {
				perr = err
			}
			return v
		}
		num := func(i int) int64 {
			v, err := res.Fields[i].AsInt64()
			if err != nil {
				perr = err
			}
			return v
		}
		out.Delivery = delivery.Delivery{
			Token:          str(0),
			MessageID:      str(1),
			DeliveryCycle:  num(2),
			Attempt:        num(3),
			ClaimedMs:      num(4),
			LeaseExpiresMs: num(5),
		}
		out.MessageJSON = []byte(str(6))
		out.BlockedDetected = num(7)
		if perr != nil {
			return delivery.ClaimResult{Outcome: delivery.ClaimInternalFailure}
		}
		return out
	case "empty":
		n, err := res.Fields[0].AsInt64()
		if err != nil {
			return delivery.ClaimResult{Outcome: delivery.ClaimInternalFailure}
		}
		return delivery.ClaimResult{Outcome: delivery.ClaimEmpty, BlockedDetected: n}
	case "wrong_type":
		return delivery.ClaimResult{Outcome: delivery.ClaimInternalFailure}
	default:
		return delivery.ClaimResult{Outcome: delivery.ClaimOutcome(res.Status)}
	}
}

// Stats reads the delivery gauge sources.
func (s *DeliveryStore) Stats(ctx context.Context) (delivery.Stats, error) {
	var st delivery.Stats
	now, err := s.a.serverTimeMs(ctx)
	if err != nil {
		return st, err
	}
	c := s.a.client
	cmds := c.DoMulti(ctx,
		c.B().Zcount().Key("hr1:leases").Min("("+itoa64(now)).Max("+inf").Build(),
		c.B().Zcard().Key("hr1:ready").Build(),
		c.B().Zcard().Key("hr1:blocked").Build(),
		c.B().Get().Key("hr1:stats:queued_messages").Build(),
		c.B().Zcard().Key("hr1:retries").Build(),
	)
	targets := []*int64{&st.ActiveLeases, &st.ReadyRecipients, &st.BlockedRecipients, &st.QueuedMessages, &st.RetriesWaiting}
	for i, r := range cmds {
		v, err := r.AsInt64()
		if err != nil && !isNil(err) {
			return st, err
		}
		*targets[i] = v
	}
	return st, nil
}

func itoa64(v int64) string { return strconv.FormatInt(v, 10) }

func isNil(err error) bool { return valkey.IsValkeyNil(err) }

// Fixed M1 retentions for acknowledgement records.
const (
	SuccessTTL   = 24 * time.Hour
	TombstoneTTL = time.Hour
)

// Ack runs ack_v3.
func (s *DeliveryStore) Ack(ctx context.Context, req delivery.AckRequest) delivery.AckResult {
	res, err := s.a.RunScript(ctx, "ack_v3",
		[]string{"hr1:t:" + req.TokenDigest, "hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:blocked", "hr1:stats:queued_messages"},
		[]string{req.Token, req.TokenDigest, itoa64(SuccessTTL.Milliseconds()), itoa64(TombstoneTTL.Milliseconds()), "hr1"})
	if err != nil {
		// Whether or not the script ran, repeating the acknowledgement
		// returns the recorded result or the current outcome.
		return delivery.AckResult{Outcome: delivery.AckDependencyUnavailable}
	}
	switch res.Status {
	case "acknowledged", "already_acknowledged":
		out := delivery.AckResult{Outcome: delivery.AckOutcome(res.Status)}
		var perr error
		if out.MessageID, perr = res.Fields[0].ToString(); perr == nil {
			if out.AcknowledgedMs, perr = res.Fields[1].AsInt64(); perr == nil {
				if out.RecipientIdentity, perr = res.Fields[2].ToString(); perr == nil {
					if out.DeliveryCycle, perr = res.Fields[3].AsInt64(); perr == nil {
						if out.Attempt, perr = res.Fields[4].AsInt64(); perr == nil {
							out.ClaimedMs, perr = res.Fields[5].AsInt64()
						}
					}
				}
			}
		}
		if perr != nil {
			return delivery.AckResult{Outcome: delivery.AckInternalFailure}
		}
		return out
	case "wrong_type":
		return delivery.AckResult{Outcome: delivery.AckInternalFailure}
	default:
		return delivery.AckResult{Outcome: delivery.AckOutcome(res.Status)}
	}
}

// Nack runs nack_v1.
func (s *DeliveryStore) Nack(ctx context.Context, req delivery.NackRequest) delivery.NackResult {
	delays := make([]string, len(req.RetryDelaysMs))
	for i, d := range req.RetryDelaysMs {
		delays[i] = itoa64(d)
	}
	res, err := s.a.RunScript(ctx, "nack_v1",
		[]string{"hr1:t:" + req.TokenDigest, "hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:retries", "hr1:blocked", "hr1:dlq", "hr1:stats:queued_messages"},
		[]string{req.Token, req.TokenDigest, req.ReasonCode, strings.Join(delays, ","), itoa64(int64(req.MaxAttempts)), itoa64(TombstoneTTL.Milliseconds()), "hr1"})
	if err != nil {
		// Whether or not the script ran, repeating the negative
		// acknowledgement returns the recorded result or the current outcome.
		return delivery.NackResult{Outcome: delivery.NackDependencyUnavailable}
	}
	var perr error
	str := func(i int) string {
		v, err := res.Fields[i].ToString()
		if err != nil {
			perr = err
		}
		return v
	}
	num := func(i int) int64 {
		v, err := res.Fields[i].AsInt64()
		if err != nil {
			perr = err
		}
		return v
	}
	var out delivery.NackResult
	switch res.Status {
	case "retry_scheduled":
		out = delivery.NackResult{
			Outcome: delivery.NackRetryScheduled, Result: "retry_scheduled",
			MessageID: str(0), Attempt: num(1), RetryAtMs: num(2), RecipientIdentity: str(3),
			DeliveryCycle: num(4), ClaimedMs: num(5), CompletedMs: num(6),
		}
	case "already_nacked":
		out = delivery.NackResult{Outcome: delivery.NackAlreadyNacked, Result: str(0), MessageID: str(1), Attempt: num(2), RetryAtMs: num(3)}
	case "wrong_type":
		return delivery.NackResult{Outcome: delivery.NackInternalFailure}
	default:
		return delivery.NackResult{Outcome: delivery.NackOutcome(res.Status)}
	}
	if perr != nil {
		return delivery.NackResult{Outcome: delivery.NackInternalFailure}
	}
	return out
}
