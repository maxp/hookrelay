package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/maxp/hookrelay/internal/delivery"
)

// ClaimOpTTL is the fixed claim operation record retention.
const ClaimOpTTL = 10 * time.Minute

// ClaimLimits are the lease bounds passed to claim_v3 and extend_v1.
type ClaimLimits struct {
	MaxActiveLeases      int
	InitialLeaseDuration time.Duration
	// LeaseExtension and MaxLeaseLifetime come from validated configuration.
	LeaseExtension   time.Duration
	MaxLeaseLifetime time.Duration
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
		c.B().Zcard().Key("hr1:dlq").Build(),
	)
	targets := []*int64{&st.ActiveLeases, &st.ReadyRecipients, &st.BlockedRecipients, &st.QueuedMessages, &st.RetriesWaiting, &st.DeadLetterMessages}
	for i, r := range cmds {
		v, err := r.AsInt64()
		if err != nil && !isNil(err) {
			return st, err
		}
		*targets[i] = v
	}
	st.OldestReadyAgeMs, err = s.oldestReadyAge(ctx, now)
	return st, err
}

// oldestReadyAge reads the head message of the lowest-sequence ready
// Recipient and returns its age from received_ms (0 when nothing is
// ready or the blob carries no received_ms).
func (s *DeliveryStore) oldestReadyAge(ctx context.Context, now int64) (int64, error) {
	c := s.a.client
	first, err := c.Do(ctx, c.B().Zrange().Key("hr1:ready").Min("0").Max("0").Build()).AsStrSlice()
	if err != nil || len(first) == 0 {
		return 0, err
	}
	head, err := c.Do(ctx, c.B().Hget().Key("hr1:r:"+first[0]+":s").Field("head_message_id").Build()).ToString()
	if err != nil {
		if isNil(err) || isWrongType(err) {
			return 0, nil
		}
		return 0, err
	}
	blob, err := c.Do(ctx, c.B().Get().Key("hr1:m:"+head).Build()).ToString()
	if err != nil {
		if isNil(err) || isWrongType(err) {
			return 0, nil
		}
		return 0, err
	}
	var m struct {
		ReceivedMs int64 `json:"received_ms"`
	}
	if json.Unmarshal([]byte(blob), &m) != nil || m.ReceivedMs <= 0 || m.ReceivedMs > now {
		return 0, nil
	}
	return now - m.ReceivedMs, nil
}

func itoa64(v int64) string { return strconv.FormatInt(v, 10) }

func isNil(err error) bool { return valkey.IsValkeyNil(err) }

// Fixed M1 retentions for acknowledgement records.
const (
	SuccessTTL   = 24 * time.Hour
	TombstoneTTL = time.Hour
)

// Ack runs ack_v4.
func (s *DeliveryStore) Ack(ctx context.Context, req delivery.AckRequest) delivery.AckResult {
	res, err := s.a.RunScript(ctx, "ack_v4",
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

// Nack runs nack_v3.
func (s *DeliveryStore) Nack(ctx context.Context, req delivery.NackRequest) delivery.NackResult {
	delays := make([]string, len(req.RetryDelaysMs))
	for i, d := range req.RetryDelaysMs {
		delays[i] = itoa64(d)
	}
	res, err := s.a.RunScript(ctx, "nack_v3",
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
			Outcome: delivery.NackRetryScheduled, Result: delivery.NackResultRetryScheduled,
			MessageID: str(0), Attempt: num(1), RetryAtMs: num(2), RecipientIdentity: str(3),
			DeliveryCycle: num(4), ClaimedMs: num(5), CompletedMs: num(6),
		}
	case "dead_lettered":
		out = delivery.NackResult{
			Outcome: delivery.NackDeadLettered, Result: delivery.NackResultDeadLettered,
			// The dead-letter time is also when the attempt completed.
			MessageID: str(0), DeliveryCycle: num(1), DeadLetteredMs: num(2), RecipientIdentity: str(3),
			Attempt: num(4), ClaimedMs: num(5), CompletedMs: num(2),
		}
	case "already_nacked":
		out = delivery.NackResult{Outcome: delivery.NackAlreadyNacked, Result: str(0), MessageID: str(1)}
		if out.Result == delivery.NackResultDeadLettered {
			out.DeliveryCycle, out.DeadLetteredMs = num(2), num(3)
		} else {
			out.Attempt, out.RetryAtMs = num(2), num(3)
		}
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

// Extend runs extend_v1.
func (s *DeliveryStore) Extend(ctx context.Context, req delivery.ExtendRequest) delivery.ExtendResult {
	res, err := s.a.RunScript(ctx, "extend_v1",
		[]string{"hr1:t:" + req.TokenDigest, "hr1:op:" + req.OperationID, "hr1:leases"},
		[]string{req.Token, req.TokenDigest, req.OperationID, req.ArgsDigest,
			itoa64(s.limits.LeaseExtension.Milliseconds()), itoa64(s.limits.MaxLeaseLifetime.Milliseconds()),
			itoa64(ClaimOpTTL.Milliseconds()), "hr1"})
	if err != nil {
		// Whether or not the script ran, repeating the same operation_id
		// returns the recorded deadline.
		return delivery.ExtendResult{Outcome: delivery.ExtendDependencyUnavailable}
	}
	switch res.Status {
	case "extended", "replay":
		id, err1 := res.Fields[0].ToString()
		lease, err2 := res.Fields[1].AsInt64()
		maxLease, err3 := res.Fields[2].AsInt64()
		out := delivery.ExtendResult{Outcome: delivery.ExtendOutcome(res.Status), MessageID: id, LeaseExpiresMs: lease, MaxLeaseExpiresMs: maxLease}
		var err4, err5, err6 error
		if res.Status == "extended" {
			out.RecipientIdentity, err4 = res.Fields[3].ToString()
			out.DeliveryCycle, err5 = res.Fields[4].AsInt64()
			out.Attempt, err6 = res.Fields[5].AsInt64()
		}
		if errors.Join(err1, err2, err3, err4, err5, err6) != nil {
			return delivery.ExtendResult{Outcome: delivery.ExtendInternalFailure}
		}
		return out
	case "wrong_type":
		return delivery.ExtendResult{Outcome: delivery.ExtendInternalFailure}
	default:
		return delivery.ExtendResult{Outcome: delivery.ExtendOutcome(res.Status)}
	}
}
