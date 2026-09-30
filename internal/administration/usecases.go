package administration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"

	"github.com/maxp/hookrelay/internal/observability"
)

// EndpointView is the safe read model: credential kind and configured state,
// never the credential value.
type EndpointView struct {
	Type           string
	Identifier     string
	BotPlatform    string
	BotID          string
	Enabled        bool
	CredentialKind string
	CredentialSet  bool
	GenerationID   string
	ConfigVersion  int64
	CreatedMs      int64
	UpdatedMs      int64
}

// Bounded identifier validation from the message contract.
var (
	webhookTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	identifierPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	botIDPattern       = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
)

// Telegram credential rules from the adapter design.
const telegramPlatform = "telegram"

// maxEndpointsPerBot is the Bot Identity endpoint cap enforced by the
// endpoint_create_v1 transition.
const maxEndpointsPerBot = 100

// CreateWebhook is the validated create use case. It returns the safe view
// of the created endpoint or an apiError.
func (s *Service) CreateWebhook(ctx context.Context, req CreateRequest, requestID string) (EndpointView, error) {
	if err := s.validateCreate(req); err != nil {
		return EndpointView{}, BadRequestError{msg: err.Error()}
	}

	platform, kinds, ok := s.catalog.Lookup(req.WebhookType)
	if !ok {
		return EndpointView{}, BadRequestError{msg: "unsupported webhook type", code: "unsupported_webhook_type"}
	}
	if !slices.Contains(kinds, req.Credential.Kind) {
		return EndpointView{}, BadRequestError{msg: "unsupported credential kind", code: "unsupported_credential_kind"}
	}
	if err := validateCredentialValue(platform, req.Credential.Kind, req.Credential.Value); err != nil {
		return EndpointView{}, BadRequestError{msg: err.Error()}
	}

	identifier := req.WebhookIdentifier
	if identifier == "" {
		v, err := s.gen.Base64URL(16)
		if err != nil {
			return EndpointView{}, DependencyError{}
		}
		identifier = "wh_" + v
	}

	e := Endpoint{
		Type:            req.WebhookType,
		Identifier:      identifier,
		BotPlatform:     platform,
		BotID:           req.BotID,
		Enabled:         *req.Enabled,
		CredentialKind:  req.Credential.Kind,
		CredentialValue: req.Credential.Value,
		GenerationID:    s.gen.UUIDv7(),
	}
	eventID := s.gen.UUIDv7()
	createdMs, updatedMs, result := s.repo.CreateEndpoint(ctx, e, eventID, opWebhookEndpointCreated, requestID)
	switch result {
	case CreateOK:
		e.CreatedMs = createdMs
		e.UpdatedMs = updatedMs
		e.ConfigVersion = 1 // fixed by the endpoint_create_v1 contract
		s.metrics.auditEvents.WithLabelValues(opWebhookEndpointCreated, outcomeSuccess).Inc()
		s.logAudit(eventID, opWebhookEndpointCreated, e.Type+":"+e.Identifier, requestID, outcomeSuccess)
		observability.LogEvent(s.log, slog.LevelInfo, opWebhookEndpointCreated, "webhook endpoint created",
			"request_id", requestID,
			"webhook_type", e.Type,
			"webhook_identifier", e.Identifier,
			"bot_platform", e.BotPlatform,
			"bot_id", e.BotID,
			"credential_kind", e.CredentialKind,
			"enabled", e.Enabled,
		)
		return viewOf(&e), nil
	case CreateConflict:
		return EndpointView{}, ConflictError{msg: "webhook identifier already exists", code: "webhook_identifier_conflict"}
	case CreateBotLimit:
		return EndpointView{}, ConflictError{msg: fmt.Sprintf("this bot already has %d webhook endpoints", maxEndpointsPerBot), code: "bot_endpoint_limit_exceeded"}
	case CreateWrongType:
		s.logCreateFailure(requestID, e, "wrong_type")
		return EndpointView{}, DependencyError{detail: "stored structure has an unexpected type"}
	case CreateUncertain:
		s.logCreateFailure(requestID, e, "outcome_uncertain")
		return EndpointView{}, DependencyError{detail: "create outcome is uncertain: read the endpoint and audit before retrying"}
	default:
		s.logCreateFailure(requestID, e, "dependency_unavailable")
		return EndpointView{}, DependencyError{}
	}
}

// logCreateFailure records a bounded error event for a create that could not
// be confirmed. It never carries the credential value.
func (s *Service) logCreateFailure(requestID string, e Endpoint, reason string) {
	observability.LogEvent(s.log, slog.LevelError, "webhook_endpoint_create_failed", "webhook endpoint create not confirmed",
		"request_id", requestID,
		"webhook_type", e.Type,
		"webhook_identifier", e.Identifier,
		"error_code", "dependency_unavailable",
		"reason_code", reason,
	)
}

// recordRejectedAuth appends the best-effort rejected-authentication audit
// event and counts its outcome. It never changes the refusal.
func (s *Service) recordRejectedAuth(ctx context.Context, requestID string) {
	eventID := s.gen.UUIDv7()
	s.logAudit(eventID, opAdminAuthRejected, "admin_api", requestID, outcomeFailure)
	if s.audit == nil {
		return
	}
	if err := s.audit.AppendRejectedAuth(ctx, eventID, requestID, "admin_api"); err != nil {
		s.metrics.auditWriteFailures.WithLabelValues(opAdminAuthRejected).Inc()
		return
	}
	s.metrics.auditEvents.WithLabelValues(opAdminAuthRejected, outcomeFailure).Inc()
}

// logAudit is the best-effort stdout copy of an administrative audit event.
// It shares its identity with the Valkey Stream entry but carries only bounded
// metadata. The log handler provides the millisecond timestamp envelope.
func (s *Service) logAudit(eventID, operation, target, requestID, outcome string) {
	observability.LogEvent(s.log, slog.LevelInfo, "administrative_audit", "administrative audit event",
		"event_id", eventID,
		"actor", "admin_bearer",
		"operation", operation,
		"target", target,
		"request_id", requestID,
		"outcome", outcome,
	)
}

// GetWebhook returns the safe read model for one endpoint. The Bot Platform
// is not stored; it is derived from the one-to-one Webhook Type mapping.
func (s *Service) GetWebhook(ctx context.Context, webhookType, identifier string) (EndpointView, error) {
	platform, _, ok := s.catalog.Lookup(webhookType)
	if !ok {
		return EndpointView{}, NotFoundError{}
	}
	e, err := s.repo.GetEndpoint(ctx, webhookType, identifier)
	if err != nil {
		if errors.Is(err, ErrStoredWrongType) {
			return EndpointView{}, DependencyError{detail: ErrStoredWrongType.Error()}
		}
		return EndpointView{}, DependencyError{}
	}
	if e == nil {
		return EndpointView{}, NotFoundError{}
	}
	e.BotPlatform = platform
	return viewOf(e), nil
}

// viewOf maps a stored record to the safe read model.
func viewOf(e *Endpoint) EndpointView {
	return EndpointView{
		Type:           e.Type,
		Identifier:     e.Identifier,
		BotPlatform:    e.BotPlatform,
		BotID:          e.BotID,
		Enabled:        e.Enabled,
		CredentialKind: e.CredentialKind,
		// Every endpoint has exactly one credential, fixed at creation;
		// transition results carry only its kind.
		CredentialSet: e.CredentialKind != "",
		GenerationID:  e.GenerationID,
		ConfigVersion: e.ConfigVersion,
		CreatedMs:     e.CreatedMs,
		UpdatedMs:     e.UpdatedMs,
	}
}

// validateCreate enforces the bounded request validation.
func (s *Service) validateCreate(req CreateRequest) error {
	var missing []string
	if req.WebhookType == "" {
		missing = append(missing, "webhook_type")
	}
	if req.BotID == "" {
		missing = append(missing, "bot_id")
	}
	if req.Credential.Value == "" {
		missing = append(missing, "credential.value")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required fields: %s", strings.Join(missing, ", "))
	}
	if !webhookTypePattern.MatchString(req.WebhookType) {
		return fmt.Errorf("webhook_type must match [a-z][a-z0-9_-]{0,31}")
	}
	if req.WebhookIdentifier != "" && !identifierPattern.MatchString(req.WebhookIdentifier) {
		return fmt.Errorf("webhook_identifier must match [A-Za-z0-9_-]{1,128}")
	}
	if !botIDPattern.MatchString(req.BotID) {
		return fmt.Errorf("bot_id must be a canonical positive decimal with at most 20 digits")
	}
	return nil
}

func validateCredentialValue(platform, kind, value string) error {
	if len(value) < 1 || len(value) > 8192 {
		return fmt.Errorf("credential.value must be 1–8192 bytes")
	}
	if platform == telegramPlatform && kind == "secret_token" {
		// 1–256 characters from A-Z, a-z, 0-9, _, and -.
		if len(value) > 256 {
			return fmt.Errorf("telegram secret_token must be 1–256 characters")
		}
		for _, c := range value {
			switch {
			case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '-':
			default:
				return fmt.Errorf("telegram secret_token allows only A-Z, a-z, 0-9, _, and -")
			}
		}
	}
	return nil
}
