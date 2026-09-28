package administration

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
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
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	botIDPattern      = regexp.MustCompile(`^[1-9][0-9]{0,19}$`)
)

// Telegram credential rules from the adapter design.
const (
	telegramSecretTokenPattern = `^[A-Za-z0-9_-]{1,256}$`
	telegramPlatform           = "telegram"
)

// CreateWebhook is the validated create use case. It returns the created
// record and the HTTP-mappable outcome string.
func (s *Service) CreateWebhook(ctx context.Context, req CreateRequest, requestID string) (Endpoint, error) {
	if err := s.validateCreate(req); err != nil {
		return Endpoint{}, BadRequestError{msg: err.Error()}
	}

	platform, kinds, ok := s.catalog.Lookup(req.WebhookType)
	if !ok {
		return Endpoint{}, BadRequestError{msg: "unsupported webhook type", code: "unsupported_webhook_type"}
	}
	kindAllowed := false
	for _, k := range kinds {
		if k == req.Credential.Kind {
			kindAllowed = true
			break
		}
	}
	if !kindAllowed {
		return Endpoint{}, BadRequestError{msg: "unsupported credential kind", code: "unsupported_credential_kind"}
	}
	if err := validateCredentialValue(req.WebhookType, req.Credential.Kind, req.Credential.Value); err != nil {
		return Endpoint{}, BadRequestError{msg: err.Error()}
	}

	identifier := req.WebhookIdentifier
	if identifier == "" {
		v, err := s.gen.Base64URL(16)
		if err != nil {
			return Endpoint{}, DependencyError{}
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
	createdMs, updatedMs, result := s.repo.CreateEndpoint(ctx, e, s.gen.UUIDv7(), "webhook_endpoint_created", requestID)
	switch result {
	case CreateOK:
		e.CreatedMs = createdMs
		e.UpdatedMs = updatedMs
		e.ConfigVersion = 1
		return e, nil
	case CreateConflict:
		return Endpoint{}, ConflictError{msg: "webhook identifier already exists", code: "webhook_identifier_conflict"}
	case CreateBotLimit:
		return Endpoint{}, ConflictError{msg: "this bot already has 100 webhook endpoints", code: "bot_endpoint_limit_exceeded"}
	case CreateWrongType:
		return Endpoint{}, DependencyError{detail: "stored structure has an unexpected type"}
	default:
		return Endpoint{}, DependencyError{}
	}
}

// GetWebhook returns the safe read model for one endpoint.
func (s *Service) GetWebhook(ctx context.Context, webhookType, identifier string) (EndpointView, error) {
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
	return EndpointView{
		Type:           e.Type,
		Identifier:     e.Identifier,
		BotPlatform:    e.BotPlatform,
		BotID:          e.BotID,
		Enabled:        e.Enabled,
		CredentialKind: e.CredentialKind,
		CredentialSet:  e.CredentialValue != "",
		GenerationID:   e.GenerationID,
		ConfigVersion:  e.ConfigVersion,
		CreatedMs:      e.CreatedMs,
		UpdatedMs:      e.UpdatedMs,
	}, nil
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
	if !isValidWebhookType(req.WebhookType) {
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

func isValidWebhookType(t string) bool {
	if len(t) == 0 || len(t) > 32 {
		return false
	}
	if t[0] < 'a' || t[0] > 'z' {
		return false
	}
	for i := 1; i < len(t); i++ {
		c := t[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func validateCredentialValue(webhookType, kind, value string) error {
	if len(value) < 1 || len(value) > 8192 {
		return fmt.Errorf("credential.value must be 1–8192 bytes")
	}
	if webhookType == telegramPlatform && kind == "secret_token" {
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
