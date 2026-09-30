package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/maxp/hookrelay/internal/ingestion"
)

// AcceptLimits are the capacity and retention bounds passed to accept_v3
// and evict_dedup_v1.
type AcceptLimits struct {
	MaxQueuedMessages             int
	MaxQueuedMessagesPerRecipient int
	MaxDedupRecords               int64
	DedupRetention                time.Duration
	// DedupMinRetention: no record younger than this is evicted early.
	// Zero means DedupRetention: nothing is evicted early.
	DedupMinRetention time.Duration
	// EvictionBatch bounds one proactive eviction call (100 when zero).
	EvictionBatch int
}

// MessageAcceptor implements ingestion.MessageAcceptor with accept_v3,
// ingestion.CapacityReader for the global acceptance stop conditions, and
// ingestion.DedupEvictor with evict_dedup_v1.
type MessageAcceptor struct {
	a      *Adapter
	limits AcceptLimits
}

// NewMessageAcceptor returns the ingestion acceptance implementation.
func NewMessageAcceptor(a *Adapter, limits AcceptLimits) *MessageAcceptor {
	return &MessageAcceptor{a: a, limits: limits}
}

// Capacity reads the global queued-message counter, the live dedup index
// members (counted exactly as accept_v3 does) with the oldest one's
// acceptance time, and Valkey memory use.
func (m *MessageAcceptor) Capacity(ctx context.Context) (ingestion.Capacity, error) {
	c := ingestion.Capacity{
		MaxQueuedMessages: int64(m.limits.MaxQueuedMessages),
		MaxDedupRecords:   m.limits.MaxDedupRecords,
	}
	now, err := m.a.serverTimeMs(ctx)
	if err != nil {
		return c, err
	}
	c.NowMs = now
	mem, err := m.a.memory(ctx)
	if err != nil {
		return c, err
	}
	c.UsedMemoryBytes, c.MaxMemoryBytes = mem.used, mem.max
	liveMin := "(" + strconv.FormatInt(now-m.limits.DedupRetention.Milliseconds(), 10)
	oldest, err := m.a.client.Do(ctx, m.a.client.B().Zrange().Key("hr1:dedup_age").Min(liveMin).Max("+inf").Byscore().
		Limit(0, 1).Withscores().Build()).AsZScores()
	if err != nil {
		return c, fmt.Errorf("valkey: dedup index: %w", err)
	}
	if len(oldest) == 1 {
		c.OldestDedupAcceptedMs = int64(oldest[0].Score)
	}
	queued, err := m.a.client.Do(ctx, m.a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64()
	if err != nil && !valkey.IsValkeyNil(err) {
		return c, fmt.Errorf("valkey: queued counter: %w", err)
	}
	live, err := m.a.client.Do(ctx, m.a.client.B().Zcount().Key("hr1:dedup_age").Min(liveMin).Max("+inf").Build()).AsInt64()
	if err != nil {
		return c, fmt.Errorf("valkey: dedup index: %w", err)
	}
	c.QueuedMessages, c.DedupRecords = queued, live
	return c, nil
}

func (m *MessageAcceptor) Accept(ctx context.Context, req ingestion.AcceptRequest) ingestion.AcceptResult {
	rid := req.RecipientIdentity
	keys := []string{
		"hr1:d:" + req.DedupIdentityDigest,
		"hr1:dedup_age",
		"hr1:m:" + req.MessageID,
		"hr1:r:" + rid + ":q",
		"hr1:r:" + rid + ":s",
		"hr1:ready",
		"hr1:ready_seq",
		"hr1:leases",
		"hr1:blocked",
		"hr1:q:" + rid,
		"hr1:stats:queued_messages",
		"hr1:mi:" + req.MessageID,
	}
	occurred := ""
	if req.OccurredMs != nil {
		occurred = strconv.FormatInt(*req.OccurredMs, 10)
	}
	args := []string{
		req.MessageID,
		req.DedupIdentityDigest,
		req.BodyDigest,
		strconv.FormatInt(req.ReceivedMs, 10),
		occurred,
		string(req.MessageJSON),
		rid,
		strconv.Itoa(m.limits.MaxQueuedMessages),
		strconv.Itoa(m.limits.MaxQueuedMessagesPerRecipient),
		strconv.FormatInt(m.limits.MaxDedupRecords, 10),
		strconv.FormatInt(m.limits.DedupRetention.Milliseconds(), 10),
		strconv.FormatInt(m.minRetention().Milliseconds(), 10),
	}

	unavailable := ingestion.AcceptResult{Outcome: ingestion.AcceptDependencyUnavailable}
	res, err := m.a.RunScript(ctx, "accept_v3", keys, args)
	if err != nil {
		// Whether or not the script ran, the platform retries and a retry of
		// an accepted message is proven duplicate, so an uncertain outcome
		// is safely reported as unavailable.
		return unavailable
	}
	switch res.Status {
	case "accepted":
		ms, err1 := res.Fields[0].AsInt64()
		evicted, err2 := res.Fields[1].AsInt64()
		if err1 != nil || err2 != nil {
			return unavailable
		}
		return ingestion.AcceptResult{Outcome: ingestion.AcceptAccepted, MessageID: req.MessageID, AcceptedMs: ms, EarlyEvicted: int(evicted)}
	case "duplicate", "duplicate_conflict":
		original, err := res.Fields[0].ToString()
		if err != nil {
			return unavailable
		}
		return ingestion.AcceptResult{Outcome: ingestion.AcceptOutcome(res.Status), MessageID: original}
	case "recipient_blocked", "recipient_capacity", "global_capacity", "dedup_capacity":
		return ingestion.AcceptResult{Outcome: ingestion.AcceptOutcome(res.Status)}
	default: // wrong_type, state_inconsistent: corrupt state, not transient
		return ingestion.AcceptResult{Outcome: ingestion.AcceptInternalFailure}
	}
}

// minRetention is the early-eviction floor (DedupRetention when unset, so
// nothing live is ever evicted early).
func (m *MessageAcceptor) minRetention() time.Duration {
	if m.limits.DedupMinRetention > 0 {
		return m.limits.DedupMinRetention
	}
	return m.limits.DedupRetention
}

// EvictDedup runs one evict_dedup_v1 batch. An inconsistent candidate
// stops the batch and is reported as an error after its evictions count.
func (m *MessageAcceptor) EvictDedup(ctx context.Context) (int, error) {
	batch := m.limits.EvictionBatch
	if batch <= 0 {
		batch = 100
	}
	res, err := m.a.RunScript(ctx, "evict_dedup_v1", []string{"hr1:dedup_age"}, []string{
		strconv.FormatInt(m.limits.MaxDedupRecords, 10),
		strconv.FormatInt(m.minRetention().Milliseconds(), 10),
		strconv.FormatInt(m.limits.DedupRetention.Milliseconds(), 10),
		strconv.Itoa(batch),
		"hr1",
	})
	if err != nil {
		return 0, err
	}
	if res.Status != "evicted" {
		return 0, fmt.Errorf("valkey: evict_dedup_v1: %s", res.Status)
	}
	count, err1 := res.Fields[0].AsInt64()
	stop, err2 := res.Fields[3].ToString()
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("valkey: evict_dedup_v1: result shape")
	}
	if stop == "candidate_invalid" {
		return int(count), fmt.Errorf("valkey: evict_dedup_v1: the oldest deduplication record is inconsistent")
	}
	return int(count), nil
}

// endpointLookup implements ingestion.EndpointLookup over the endpoint Hash.
type endpointLookup struct{ a *Adapter }

// NewEndpointLookup returns the ingestion endpoint reader.
func NewEndpointLookup(a *Adapter) ingestion.EndpointLookup { return &endpointLookup{a: a} }

func (l *endpointLookup) LookupEndpoint(ctx context.Context, webhookType, identifier string) (*ingestion.Endpoint, error) {
	e, err := l.a.GetEndpoint(ctx, webhookType, identifier)
	if err != nil {
		if errors.Is(err, ErrWrongType) {
			return nil, ingestion.ErrStoredWrongType
		}
		return nil, err
	}
	if e == nil {
		return nil, nil
	}
	return &ingestion.Endpoint{
		BotID:           e.BotID,
		Enabled:         e.Enabled,
		CredentialKind:  e.CredentialKind,
		CredentialValue: e.CredentialValue,
	}, nil
}
