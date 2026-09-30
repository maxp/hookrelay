package administration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/maxp/hookrelay/internal/observability"
)

// WebhookPage is one endpoint list page with the opaque continuation cursor.
type WebhookPage struct {
	Items      []endpointResponse `json:"items"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

// ListWebhooks pages all Webhook Endpoints newest first by safe metadata.
func (s *Service) ListWebhooks(ctx context.Context, limit, cursor, requestID string) (WebhookPage, error) {
	n := defaultListLimit
	if limit != "" {
		v, err := strconv.Atoi(limit)
		if err != nil || v < 1 || v > maxListLimit {
			return WebhookPage{}, BadRequestError{msg: "limit must be between 1 and 200"}
		}
		n = v
	}
	var after *EndpointCursor
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		var c EndpointCursor
		if err != nil || json.Unmarshal(raw, &c) != nil || c.ID == "" {
			return WebhookPage{}, BadRequestError{msg: "cursor is malformed", code: "invalid_cursor"}
		}
		after = &c
	}
	items, err := s.repo.ListEndpoints(ctx, n+1, after)
	if err != nil {
		return WebhookPage{}, DependencyError{}
	}
	page := WebhookPage{Items: []endpointResponse{}}
	if len(items) > n {
		last := items[n-1]
		raw, _ := json.Marshal(EndpointCursor{CreatedMs: last.CreatedMs, ID: last.Member})
		page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
		items = items[:n]
	}
	page.Items = s.endpointViews(items, "webhook_listing", requestID)
	return page, nil
}

// endpointViews maps listings to safe representations. Orphan members (and
// records of a Webhook Type no longer registered) are skipped and reported
// in one webhook_index_orphan event; the listing indexes are reconciled by
// later hardening, not here.
func (s *Service) endpointViews(items []EndpointListing, index, requestID string) []endpointResponse {
	out := []endpointResponse{}
	orphans, first, reason := 0, "", ""
	for _, it := range items {
		why := it.Orphan
		var platform string
		if why == "" {
			var ok bool
			if platform, _, ok = s.catalog.Lookup(it.Endpoint.Type); !ok {
				why = "unknown_webhook_type"
			}
		}
		if why != "" {
			if orphans == 0 {
				first, reason = it.Member, why
			}
			orphans++
			continue
		}
		e := *it.Endpoint
		e.BotPlatform = platform
		out = append(out, endpointBody(viewOf(&e)))
	}
	if orphans > 0 {
		observability.LogEvent(s.log, slog.LevelWarn, "webhook_index_orphan", "endpoint index members without a usable record were skipped",
			"request_id", requestID, "index", index, "orphan_count", orphans, "member", first, "reason_code", reason)
	}
	return out
}

func (s *Service) handleListWebhooks(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	q := r.URL.Query()
	page, err := s.ListWebhooks(r.Context(), q.Get("limit"), q.Get("cursor"), requestID)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

// entityTagPattern is one strong entity tag "<generation_id>:<config_version>".
// Weak tags, "*", and lists are unsupported.
var entityTagPattern = regexp.MustCompile(`^"([A-Za-z0-9-]{1,64}):([1-9][0-9]{0,18})"$`)

// parseIfMatch reads the optional If-Match precondition. Absent → nil;
// present but not exactly one strong tag → 400.
func parseIfMatch(r *http.Request) (*EntityVersion, error) {
	values := r.Header.Values("If-Match")
	if len(values) == 0 {
		return nil, nil
	}
	m := entityTagPattern.FindStringSubmatch(strings.TrimSpace(values[0]))
	if len(values) != 1 || m == nil {
		return nil, BadRequestError{msg: `If-Match must be one strong entity tag "<generation_id>:<config_version>"`}
	}
	version, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return nil, BadRequestError{msg: "If-Match config_version is out of range"}
	}
	return &EntityVersion{GenerationID: m[1], ConfigVersion: version}, nil
}

func preconditionRequired() error {
	return StatusError{Status: http.StatusPreconditionRequired, Code: "precondition_required",
		Msg: "If-Match with the current ETag is required: read the endpoint first"}
}

func preconditionFailed() error {
	return StatusError{Status: http.StatusPreconditionFailed, Code: "precondition_failed",
		Msg: "the endpoint changed: read it again for the current ETag"}
}

// PatchRequest is the strict PATCH body; only the enabled flag may change.
type PatchRequest struct {
	Enabled *bool `json:"enabled"`
}

// SetWebhookEnabled runs the audited enable/disable transition. Setting the
// current value is a no-op that returns the unchanged representation.
func (s *Service) SetWebhookEnabled(ctx context.Context, webhookType, identifier string, enabled bool, expected *EntityVersion, requestID string) (EndpointView, error) {
	platform, _, ok := s.catalog.Lookup(webhookType)
	if !ok {
		return EndpointView{}, NotFoundError{}
	}
	eventID := s.gen.UUIDv7()
	e, result := s.repo.SetEndpointEnabled(ctx, webhookType, identifier, enabled, expected, eventID, requestID)
	switch result {
	case SetEnabledUpdated, SetEnabledUnchanged:
		e.BotPlatform = platform
		view := viewOf(e)
		if result == SetEnabledUnchanged {
			return view, nil
		}
		op := opWebhookEndpointDisabled
		if enabled {
			op = opWebhookEndpointEnabled
		}
		target := webhookType + ":" + identifier
		s.metrics.auditEvents.WithLabelValues(op, outcomeSuccess).Inc()
		s.logAudit(eventID, op, target, requestID, outcomeSuccess)
		observability.LogEvent(s.log, slog.LevelInfo, op, "webhook endpoint "+map[bool]string{true: "enabled", false: "disabled"}[enabled],
			"request_id", requestID, "webhook_type", webhookType, "webhook_identifier", identifier, "bot_platform", platform,
			"bot_id", e.BotID, "credential_kind", e.CredentialKind, "generation_id", e.GenerationID, "config_version", e.ConfigVersion)
		return view, nil
	case SetEnabledNotFound:
		return EndpointView{}, NotFoundError{}
	case SetEnabledPreconditionRequired:
		return EndpointView{}, preconditionRequired()
	case SetEnabledPreconditionFailed:
		return EndpointView{}, preconditionFailed()
	case SetEnabledWrongType:
		s.logMutationFailure("webhook_endpoint_update_failed", requestID, webhookType, identifier, "wrong_type")
		return EndpointView{}, DependencyError{detail: "stored structure has an unexpected type"}
	case SetEnabledUncertain:
		s.logMutationFailure("webhook_endpoint_update_failed", requestID, webhookType, identifier, "outcome_uncertain")
		return EndpointView{}, DependencyError{detail: "update outcome is uncertain: read the endpoint and audit before retrying"}
	default:
		s.logMutationFailure("webhook_endpoint_update_failed", requestID, webhookType, identifier, "dependency_unavailable")
		return EndpointView{}, DependencyError{}
	}
}

// logMutationFailure records a bounded error event for an endpoint
// mutation that could not be confirmed.
func (s *Service) logMutationFailure(event, requestID, webhookType, identifier, reason string) {
	observability.LogEvent(s.log, slog.LevelError, event, "webhook endpoint mutation not confirmed",
		"request_id", requestID, "webhook_type", webhookType, "webhook_identifier", identifier,
		"error_code", "dependency_unavailable", "reason_code", reason)
}

// endpointPath validates the endpoint route values; unknown shapes are
// indistinguishable from unknown endpoints.
func endpointPath(r *http.Request) (webhookType, identifier string, ok bool) {
	webhookType, identifier = r.PathValue("webhook_type"), r.PathValue("webhook_identifier")
	return webhookType, identifier, webhookTypePattern.MatchString(webhookType) && identifierPattern.MatchString(identifier)
}

func (s *Service) handlePatch(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	webhookType, identifier, ok := endpointPath(r)
	if !ok {
		writeAPIError(w, NotFoundError{}, requestID)
		return
	}
	var req PatchRequest
	if err := decodeBody(r, &req); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	if req.Enabled == nil {
		writeAPIError(w, BadRequestError{msg: "enabled is required"}, requestID)
		return
	}
	expected, err := parseIfMatch(r)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	view, err := s.SetWebhookEnabled(r.Context(), webhookType, identifier, *req.Enabled, expected, requestID)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeEndpoint(w, http.StatusOK, view)
}

// DeleteWebhook permanently deletes a disabled endpoint. Deleting an absent
// endpoint succeeds without another audit event; that proves no earlier
// uncertain deletion wrote its audit.
func (s *Service) DeleteWebhook(ctx context.Context, webhookType, identifier string, expected *EntityVersion, requestID string) error {
	platform, _, ok := s.catalog.Lookup(webhookType)
	if !ok {
		// No endpoint of an unregistered type can exist.
		return nil
	}
	eventID := s.gen.UUIDv7()
	e, result := s.repo.DeleteEndpoint(ctx, webhookType, identifier, platform, expected, eventID, requestID)
	switch result {
	case DeleteDeleted:
		target := webhookType + ":" + identifier
		s.metrics.auditEvents.WithLabelValues(opWebhookEndpointDeleted, outcomeSuccess).Inc()
		s.logAudit(eventID, opWebhookEndpointDeleted, target, requestID, outcomeSuccess)
		observability.LogEvent(s.log, slog.LevelInfo, opWebhookEndpointDeleted, "webhook endpoint deleted",
			"request_id", requestID, "webhook_type", webhookType, "webhook_identifier", identifier, "bot_platform", platform,
			"bot_id", e.BotID, "credential_kind", e.CredentialKind, "generation_id", e.GenerationID, "config_version", e.ConfigVersion)
		return nil
	case DeleteAbsent:
		return nil
	case DeletePreconditionRequired:
		return preconditionRequired()
	case DeletePreconditionFailed:
		return preconditionFailed()
	case DeleteMustBeDisabled:
		return ConflictError{msg: "Disable the webhook endpoint before deleting it.", code: "endpoint_must_be_disabled"}
	case DeleteWrongType:
		s.logMutationFailure("webhook_endpoint_delete_failed", requestID, webhookType, identifier, "wrong_type")
		return DependencyError{detail: "stored structure has an unexpected type"}
	case DeleteUncertain:
		s.logMutationFailure("webhook_endpoint_delete_failed", requestID, webhookType, identifier, "outcome_uncertain")
		return DependencyError{detail: "delete outcome is uncertain: read the endpoint and audit before retrying"}
	default:
		s.logMutationFailure("webhook_endpoint_delete_failed", requestID, webhookType, identifier, "dependency_unavailable")
		return DependencyError{}
	}
}

func (s *Service) handleDelete(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	webhookType, identifier, ok := endpointPath(r)
	if !ok {
		// A shape that cannot name an endpoint names an absent one.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	expected, err := parseIfMatch(r)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	if err := s.DeleteWebhook(r.Context(), webhookType, identifier, expected, requestID); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// BotWebhooks is the unpaginated endpoint collection of one Bot Identity.
type BotWebhooks struct {
	Items []endpointResponse `json:"items"`
}

// ListBotWebhooks returns every endpoint of one Bot Identity, newest first,
// for the credential-replacement flow.
func (s *Service) ListBotWebhooks(ctx context.Context, botPlatform, botID, requestID string) (BotWebhooks, error) {
	if !s.catalog.KnownPlatform(botPlatform) {
		return BotWebhooks{}, BadRequestError{msg: "unknown bot platform"}
	}
	if !botIDPattern.MatchString(botID) {
		return BotWebhooks{}, BadRequestError{msg: "bot_id must be a canonical decimal identifier"}
	}
	items, err := s.repo.ListBotEndpoints(ctx, botPlatform, botID)
	if err != nil {
		if errors.Is(err, ErrStoredWrongType) {
			return BotWebhooks{}, DependencyError{detail: ErrStoredWrongType.Error()}
		}
		return BotWebhooks{}, DependencyError{}
	}
	return BotWebhooks{Items: s.endpointViews(items, "bot_webhooks", requestID)}, nil
}

func (s *Service) handleBotWebhooks(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	list, err := s.ListBotWebhooks(r.Context(), r.PathValue("bot_platform"), r.PathValue("bot_id"), requestID)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, list)
}
