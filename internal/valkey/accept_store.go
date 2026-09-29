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

// AcceptLimits are the capacity and retention bounds passed to accept_v1.
type AcceptLimits struct {
	MaxQueuedMessages             int
	MaxQueuedMessagesPerRecipient int
	MaxDedupRecords               int64
	DedupRetention                time.Duration
}

// MessageAcceptor implements ingestion.MessageAcceptor with accept_v1 and
// ingestion.CapacityReader for the global acceptance stop conditions.
type MessageAcceptor struct {
	a      *Adapter
	limits AcceptLimits
}

// NewMessageAcceptor returns the ingestion acceptance implementation.
func NewMessageAcceptor(a *Adapter, limits AcceptLimits) *MessageAcceptor {
	return &MessageAcceptor{a: a, limits: limits}
}

// Capacity reads the global queued-message counter and the live dedup index
// members, counted exactly as accept_v1 does.
func (m *MessageAcceptor) Capacity(ctx context.Context) (ingestion.Capacity, error) {
	c := ingestion.Capacity{
		MaxQueuedMessages: int64(m.limits.MaxQueuedMessages),
		MaxDedupRecords:   m.limits.MaxDedupRecords,
	}
	now, err := m.a.serverTimeMs(ctx)
	if err != nil {
		return c, err
	}
	queued, err := m.a.client.Do(ctx, m.a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64()
	if err != nil && !valkey.IsValkeyNil(err) {
		return c, fmt.Errorf("valkey: queued counter: %w", err)
	}
	live, err := m.a.client.Do(ctx, m.a.client.B().Zcount().Key("hr1:dedup_age").
		Min("("+strconv.FormatInt(now-m.limits.DedupRetention.Milliseconds(), 10)).Max("+inf").Build()).AsInt64()
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
	}

	unavailable := ingestion.AcceptResult{Outcome: ingestion.AcceptDependencyUnavailable}
	res, err := m.a.RunScript(ctx, "accept_v1", keys, args)
	if err != nil {
		// Whether or not the script ran, the platform retries and a retry of
		// an accepted message is proven duplicate, so an uncertain outcome
		// is safely reported as unavailable.
		return unavailable
	}
	switch res.Status {
	case "accepted":
		ms, err := res.Fields[0].AsInt64()
		if err != nil {
			return unavailable
		}
		return ingestion.AcceptResult{Outcome: ingestion.AcceptAccepted, MessageID: req.MessageID, AcceptedMs: ms}
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
