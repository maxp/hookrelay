package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"

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

// auditKey is the administrative audit Stream.
const auditKey = "hr1:audit"

// CreateEndpointResult is the bounded outcome of the create transition.
type CreateEndpointResult string

const (
	CreateOK          CreateEndpointResult = "created"
	CreateConflict    CreateEndpointResult = "conflict"
	CreateBotLimit    CreateEndpointResult = "bot_endpoint_limit"
	CreateWrongType   CreateEndpointResult = "wrong_type"
	CreateUnavailable CreateEndpointResult = "dependency_unavailable"
	// CreateUncertain: the script may have run, fully or partially. Neither
	// success nor failure may be reported; the caller reconciles by reading
	// persisted state and audit.
	CreateUncertain CreateEndpointResult = "uncertain"
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
		auditKey,
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
		if errors.Is(err, ErrNotDispatched) {
			return 0, 0, CreateUnavailable
		}
		return 0, 0, CreateUncertain
	}
	switch CreateEndpointResult(res.Status) {
	case CreateOK:
		// The parser guarantees both positional fields; a non-integer value
		// still means the script ran, so the outcome is uncertain.
		c, err1 := res.Fields[0].AsInt64()
		u, err2 := res.Fields[1].AsInt64()
		if err1 != nil || err2 != nil {
			return 0, 0, CreateUncertain
		}
		return c, u, CreateOK
	case CreateConflict, CreateBotLimit, CreateWrongType:
		return 0, 0, CreateEndpointResult(res.Status)
	default:
		return 0, 0, CreateUncertain
	}
}

// GetEndpoint reads one endpoint in a single HGETALL. It returns (nil, nil)
// when absent. BotPlatform is not stored; the caller derives it from the
// Webhook Type mapping.
func (a *Adapter) GetEndpoint(ctx context.Context, webhookType, identifier string) (_ *Endpoint, err error) {
	start := time.Now()
	defer func() { a.metrics.observe("endpoint_read", start, err) }()
	key := "hr1:wh:" + webhookType + ":" + identifier
	msg, err := a.client.Do(ctx, a.client.B().Hgetall().Key(key).Build()).ToMessage()
	if err != nil {
		if isWrongType(err) {
			return nil, ErrWrongType
		}
		return nil, fmt.Errorf("valkey: hgetall %s: %w", key, err)
	}
	fields, err := msg.AsStrMap()
	if err != nil {
		return nil, fmt.Errorf("valkey: hgetall %s shape: %w", key, err)
	}
	if len(fields) == 0 {
		return nil, nil
	}
	return parseEndpoint(webhookType, identifier, fields)
}

// errMalformedEndpoint reports an endpoint Hash with a missing or
// unparsable field.
var errMalformedEndpoint = errors.New("valkey: malformed endpoint record")

// parseEndpoint maps the fields of a non-empty endpoint Hash.
func parseEndpoint(webhookType, identifier string, fields map[string]string) (_ *Endpoint, err error) {
	key := "hr1:wh:" + webhookType + ":" + identifier
	for _, required := range []string{"bot_id", "enabled", "credential_kind", "credential_value", "generation_id"} {
		if fields[required] == "" {
			return nil, fmt.Errorf("%w: %s: missing field %s", errMalformedEndpoint, key, required)
		}
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
		return nil, fmt.Errorf("%w: %s config_version: %w", errMalformedEndpoint, key, err)
	}
	if e.CreatedMs, err = strconv.ParseInt(fields["created_ms"], 10, 64); err != nil {
		return nil, fmt.Errorf("%w: %s created_ms: %w", errMalformedEndpoint, key, err)
	}
	if e.UpdatedMs, err = strconv.ParseInt(fields["updated_ms"], 10, 64); err != nil {
		return nil, fmt.Errorf("%w: %s updated_ms: %w", errMalformedEndpoint, key, err)
	}
	return e, nil
}

// isWrongType reports a Valkey WRONGTYPE server error.
func isWrongType(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "WRONGTYPE")
}

// AppendRejectedAuth records a rejected administrative authentication attempt
// in the audit stream, best effort: the caller counts the returned error but
// never changes the refusal. The timestamp uses authoritative Valkey TIME,
// matching the Lua-path entries; without it nothing is appended rather than
// an entry with a fabricated timestamp.
func (a *Adapter) AppendRejectedAuth(ctx context.Context, eventID, requestID, target string) error {
	auditCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	timestampMs, err := a.serverTimeMs(auditCtx)
	if err != nil {
		return err
	}
	_, err = a.client.Do(auditCtx, a.client.B().Arbitrary(
		"XADD", auditKey, "MAXLEN", "~", "1000000", "*",
		"event_id", eventID,
		"timestamp_ms", strconv.FormatInt(timestampMs, 10),
		"actor", "admin_bearer",
		"operation", "admin_auth_rejected",
		"target", target,
		"request_id", requestID,
		"outcome", "failure",
	).Build()).ToMessage()
	if err != nil {
		return fmt.Errorf("valkey: audit xadd: %w", err)
	}
	return nil
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
	case CreateUnavailable:
		return 0, 0, administration.CreateUnavailable
	default:
		return 0, 0, administration.CreateUncertain
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
	return adminEndpoint(e), nil
}

// ListEndpoints pages hr1:webhooks in descending (score, member) order and
// reads every member's Hash in one pipelined round trip.
func (s *endpointStore) ListEndpoints(ctx context.Context, limit int, after *administration.EndpointCursor) (_ []administration.EndpointListing, err error) {
	start := time.Now()
	defer func() { s.a.metrics.observe("endpoint_read", start, err) }()
	c := s.a.client
	from := "+inf"
	if after != nil {
		from = strconv.FormatInt(after.CreatedMs, 10)
	}
	var out []administration.EndpointListing
	// Members sharing the cursor score are skipped down to the cursor
	// member; read further pages until the limit is filled or the index ends.
	for offset := int64(0); len(out) < limit; {
		page, err := c.Do(ctx, c.B().Zrange().Key("hr1:webhooks").Min(from).Max("-inf").Byscore().Rev().Limit(offset, int64(limit)).Withscores().Build()).AsZScores()
		if err != nil {
			return nil, err
		}
		for _, z := range page {
			sc := int64(z.Score)
			if after != nil && sc == after.CreatedMs && z.Member >= after.ID {
				continue
			}
			if len(out) < limit {
				out = append(out, administration.EndpointListing{Member: z.Member, CreatedMs: sc})
			}
		}
		if len(page) < limit {
			break
		}
		offset += int64(len(page))
	}
	return out, s.readListings(ctx, out)
}

// readListings fills each listing with its endpoint record or an orphan
// reason. Only a transport failure is an error.
func (s *endpointStore) readListings(ctx context.Context, items []administration.EndpointListing) error {
	if len(items) == 0 {
		return nil
	}
	c := s.a.client
	cmds := make(valkey.Commands, len(items))
	for i, it := range items {
		cmds[i] = c.B().Hgetall().Key("hr1:wh:" + it.Member).Build()
	}
	for i, r := range c.DoMulti(ctx, cmds...) {
		fields, err := r.AsStrMap()
		switch {
		case isWrongType(err):
			items[i].Orphan = administration.OrphanWrongType
			continue
		case err != nil:
			return err
		case len(fields) == 0:
			items[i].Orphan = administration.OrphanMissing
			continue
		}
		webhookType, identifier, ok := strings.Cut(items[i].Member, ":")
		if !ok {
			items[i].Orphan = administration.OrphanMalformed
			continue
		}
		e, err := parseEndpoint(webhookType, identifier, fields)
		if err != nil {
			items[i].Orphan = administration.OrphanMalformed
			continue
		}
		items[i].Endpoint = adminEndpoint(e)
	}
	return nil
}

// adminEndpoint maps a stored record to the administration model.
func adminEndpoint(e *Endpoint) *administration.Endpoint {
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
	}
}

type auditSink struct{ a *Adapter }

// NewAuditSink returns the administration best-effort audit implementation.
func NewAuditSink(a *Adapter) administration.AuditSink { return &auditSink{a: a} }

func (s *auditSink) AppendRejectedAuth(ctx context.Context, eventID, requestID, target string) error {
	return s.a.AppendRejectedAuth(ctx, eventID, requestID, target)
}

// AuditEntries reads the bounded recent administrative audit stream. It backs
// the later audit listing surfaces and gives integration tests one seam for
// asserting exactly-once audit behavior.
func (a *Adapter) AuditEntries(ctx context.Context, count int64) ([]map[string]string, error) {
	msg, err := a.client.Do(ctx, a.client.B().Xrange().Key(auditKey).Start("-").End("+").Count(count).Build()).ToMessage()
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

// serverTimeMs reads authoritative Valkey TIME in milliseconds.
func (a *Adapter) serverTimeMs(ctx context.Context) (int64, error) {
	msg, err := a.client.Do(ctx, a.client.B().Time().Build()).ToMessage()
	if err != nil {
		return 0, fmt.Errorf("valkey: time: %w", err)
	}
	parts, err := msg.ToArray()
	if err != nil || len(parts) != 2 {
		return 0, fmt.Errorf("valkey: time shape")
	}
	sec, e1 := parts[0].AsInt64()
	usec, e2 := parts[1].AsInt64()
	if e1 != nil || e2 != nil {
		return 0, fmt.Errorf("valkey: time shape")
	}
	return sec*1000 + usec/1000, nil
}
