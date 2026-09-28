package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/maxp/hookrelay/internal/administration"
)

// Endpoint is the full stored Webhook Endpoint record. The credential value
// is secret-bearing: only the Valkey adapter and verification ever see it;
// API responses map to safe metadata.
type Endpoint struct {
	Type            string
	Identifier      string
	BotPlatform     string // derived from the type mapping by the caller
	BotID           string
	Enabled         bool
	CredentialKind  string
	CredentialValue string
	GenerationID    string
	CreatedMs       int64
	UpdatedMs       int64
	ConfigVersion   int64
}

// ErrWrongType reports an unexpected stored key type: an inconsistency the
// current slice does not repair.
var ErrWrongType = errors.New("valkey: stored structure has an unexpected type")

// CreateEndpointResult is the bounded outcome of the create transition.
type CreateEndpointResult string

const (
	CreateOK          CreateEndpointResult = "created"
	CreateConflict    CreateEndpointResult = "conflict"
	CreateBotLimit    CreateEndpointResult = "bot_endpoint_limit"
	CreateWrongType   CreateEndpointResult = "wrong_type"
	CreateUnavailable CreateEndpointResult = "dependency_unavailable"
)

// CreateEndpoint runs the endpoint_create_v1 transition: endpoint Hash, Bot
// Identity SET, global listing ZSET, and the mandatory audit append in one
// atomic operation.
func (a *Adapter) CreateEndpoint(ctx context.Context, e Endpoint, eventID, operation, requestID string) (createdMs, updatedMs int64, result CreateEndpointResult) {
	enabled := "0"
	if e.Enabled {
		enabled = "1"
	}
	keys := []string{
		"hr1:wh:" + e.Type + ":" + e.Identifier,
		"hr1:bot:" + e.BotPlatform + ":" + e.BotID + ":webhooks",
		"hr1:webhooks",
		"hr1:audit",
	}
	args := []string{
		e.Type,
		e.Identifier,
		e.BotPlatform,
		e.BotID,
		enabled,
		e.CredentialKind,
		e.CredentialValue,
		e.GenerationID,
		eventID,
		operation,
		requestID,
	}

	res, err := a.RunScript(ctx, "endpoint_create_v1", keys, args)
	if err != nil {
		return 0, 0, CreateUnavailable
	}
	switch CreateEndpointResult(res.Status) {
	case CreateOK:
		created, err1 := res.Field(0)
		updated, err2 := res.Field(1)
		if err1 != nil || err2 != nil {
			return 0, 0, CreateUnavailable
		}
		c, err1 := created.AsInt64()
		u, err2 := updated.AsInt64()
		if err1 != nil || err2 != nil {
			return 0, 0, CreateUnavailable
		}
		return c, u, CreateOK
	case CreateConflict, CreateBotLimit, CreateWrongType:
		return 0, 0, CreateEndpointResult(res.Status)
	default:
		return 0, 0, CreateUnavailable
	}
}

// GetEndpoint reads one endpoint. It returns (nil, nil) when absent.
func (a *Adapter) GetEndpoint(ctx context.Context, webhookType, identifier string) (*Endpoint, error) {
	key := "hr1:wh:" + webhookType + ":" + identifier
	t, err := a.keyType(ctx, key)
	if err != nil {
		return nil, err
	}
	switch t {
	case "none":
		return nil, nil
	case "hash":
	default:
		return nil, ErrWrongType
	}

	msg, err := a.client.Do(ctx, a.client.B().Hgetall().Key(key).Build()).ToMessage()
	if err != nil {
		return nil, fmt.Errorf("valkey: hgetall %s: %w", key, err)
	}
	fields, err := msg.AsStrMap()
	if err != nil {
		return nil, fmt.Errorf("valkey: hgetall %s shape: %w", key, err)
	}
	e := &Endpoint{
		Type:            webhookType,
		Identifier:      identifier,
		BotID:           fields["bot_id"],
		Enabled:         fields["enabled"] == "1",
		CredentialKind:  fields["credential_kind"],
		CredentialValue: fields["credential_value"],
		GenerationID:    fields["generation_id"],
	}
	if e.ConfigVersion, err = strconv.ParseInt(fields["config_version"], 10, 64); err != nil {
		return nil, fmt.Errorf("valkey: %s config_version: %w", key, err)
	}
	if e.CreatedMs, err = strconv.ParseInt(fields["created_ms"], 10, 64); err != nil {
		return nil, fmt.Errorf("valkey: %s created_ms: %w", key, err)
	}
	if e.UpdatedMs, err = strconv.ParseInt(fields["updated_ms"], 10, 64); err != nil {
		return nil, fmt.Errorf("valkey: %s updated_ms: %w", key, err)
	}
	return e, nil
}

// AppendRejectedAuth records a rejected administrative authentication attempt
// in the audit stream, best effort: failures are swallowed because rejected
// authentication stays rejected regardless of audit availability.
func (a *Adapter) AppendRejectedAuth(ctx context.Context, eventID, requestID, target string) {
	auditCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	a.client.Do(auditCtx, a.client.B().Arbitrary(
		"XADD", "hr1:audit", "MAXLEN", "~", "1000000", "*",
		"event_id", eventID,
		"actor", "admin_bearer",
		"operation", "admin_auth_rejected",
		"target", target,
		"request_id", requestID,
		"outcome", "failure",
	).Build())
}

// --- administration glue -----------------------------------------------------

// endpointStore adapts the Adapter to the administration.EndpointRepository
// interface without leaking hr1: keys or adapter types upward.
type endpointStore struct{ a *Adapter }

// NewEndpointStore returns the administration repository implementation.
func NewEndpointStore(a *Adapter) administration.EndpointRepository {
	return &endpointStore{a: a}
}

func (s *endpointStore) CreateEndpoint(ctx context.Context, e administration.Endpoint, eventID, operation, requestID string) (int64, int64, administration.CreateEndpointResult) {
	created, updated, result := s.a.CreateEndpoint(ctx, Endpoint{
		Type:            e.Type,
		Identifier:      e.Identifier,
		BotPlatform:     e.BotPlatform,
		BotID:           e.BotID,
		Enabled:         e.Enabled,
		CredentialKind:  e.CredentialKind,
		CredentialValue: e.CredentialValue,
		GenerationID:    e.GenerationID,
	}, eventID, operation, requestID)
	switch result {
	case CreateOK:
		return created, updated, administration.CreateOK
	case CreateConflict:
		return 0, 0, administration.CreateConflict
	case CreateBotLimit:
		return 0, 0, administration.CreateBotLimit
	case CreateWrongType:
		return 0, 0, administration.CreateWrongType
	default:
		return 0, 0, administration.CreateUnavailable
	}
}

func (s *endpointStore) GetEndpoint(ctx context.Context, webhookType, identifier string) (*administration.Endpoint, error) {
	e, err := s.a.GetEndpoint(ctx, webhookType, identifier)
	if err != nil {
		if errors.Is(err, ErrWrongType) {
			return nil, administration.ErrStoredWrongType
		}
		return nil, err
	}
	if e == nil {
		return nil, nil
	}
	return &administration.Endpoint{
		Type:            e.Type,
		Identifier:      e.Identifier,
		BotPlatform:     e.BotPlatform,
		BotID:           e.BotID,
		Enabled:         e.Enabled,
		CredentialKind:  e.CredentialKind,
		CredentialValue: e.CredentialValue,
		GenerationID:    e.GenerationID,
		CreatedMs:       e.CreatedMs,
		UpdatedMs:       e.UpdatedMs,
		ConfigVersion:   e.ConfigVersion,
	}, nil
}

type auditSink struct{ a *Adapter }

// NewAuditSink returns the administration best-effort audit implementation.
func NewAuditSink(a *Adapter) administration.AuditSink { return &auditSink{a: a} }

func (s *auditSink) AppendRejectedAuth(ctx context.Context, eventID, requestID, target string) {
	s.a.AppendRejectedAuth(ctx, eventID, requestID, target)
}

// AuditEntries reads the bounded recent administrative audit stream. It backs
// the later audit listing surfaces and gives integration tests one seam for
// asserting exactly-once audit behavior.
func (a *Adapter) AuditEntries(ctx context.Context, count int64) ([]map[string]string, error) {
	msg, err := a.client.Do(ctx, a.client.B().Xrange().Key("hr1:audit").Start("-").End("+").Count(count).Build()).ToMessage()
	if err != nil {
		return nil, fmt.Errorf("valkey: audit xrange: %w", err)
	}
	items, err := msg.ToArray()
	if err != nil {
		return nil, fmt.Errorf("valkey: audit shape: %w", err)
	}
	out := make([]map[string]string, 0, len(items))
	for _, item := range items {
		parts, err := item.ToArray()
		if err != nil || len(parts) != 2 {
			return nil, fmt.Errorf("valkey: audit entry shape")
		}
		pairs, err := parts[1].ToArray()
		if err != nil {
			return nil, fmt.Errorf("valkey: audit fields shape: %w", err)
		}
		entry := map[string]string{}
		for i := 0; i+1 < len(pairs); i += 2 {
			k, _ := pairs[i].ToString()
			v, _ := pairs[i+1].ToString()
			entry[k] = v
		}
		out = append(out, entry)
	}
	return out, nil
}
