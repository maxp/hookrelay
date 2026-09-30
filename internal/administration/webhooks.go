package administration

import (
	"context"
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
	n, err := parseListLimit(limit)
	if err != nil {
		return WebhookPage{}, err
	}
	after, err := decodeCursor(cursor, func(c EndpointCursor) bool { return c.ID != "" })
	if err != nil {
		return WebhookPage{}, err
	}
	items, err := s.repo.ListEndpoints(ctx, n+1, after)
	if err != nil {
		return WebhookPage{}, DependencyError{}
	}
	page := WebhookPage{Items: []endpointResponse{}}
	if len(items) > n {
		last := items[n-1]
		page.NextCursor = encodeCursor(EndpointCursor{CreatedMs: last.CreatedMs, ID: last.Member})
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
				why = OrphanUnknownType
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
func (s *Service) SetWebhookEnabled(ctx context.Context, ref EndpointRef, enabled bool, expected *EntityVersion, requestID string) (EndpointView, error) {
	platform, _, ok := s.catalog.Lookup(ref.Type)
	if !ok {
		return EndpointView{}, NotFoundError{}
	}
	eventID := s.gen.UUIDv7()
	e, result := s.repo.SetEndpointEnabled(ctx, ref, enabled, expected, eventID, requestID)
	switch result {
	case SetEnabledUpdated, SetEnabledUnchanged:
		e.BotPlatform = platform
		if result == SetEnabledUpdated {
			op := opWebhookEndpointDisabled
			if enabled {
				op = opWebhookEndpointEnabled
			}
			s.endpointMutated(op, ref, platform, eventID, requestID,
				"bot_id", e.BotID, "credential_kind", e.CredentialKind, "generation_id", e.GenerationID, "config_version", e.ConfigVersion)
		}
		return viewOf(e), nil
	case SetEnabledNotFound:
		return EndpointView{}, NotFoundError{}
	case SetEnabledPreconditionRequired:
		return EndpointView{}, preconditionRequired()
	case SetEnabledPreconditionFailed:
		return EndpointView{}, preconditionFailed()
	case SetEnabledWrongType:
		return EndpointView{}, s.mutationNotConfirmed("webhook_endpoint_update_failed", ref, requestID, reasonWrongType)
	case SetEnabledUncertain:
		return EndpointView{}, s.mutationNotConfirmed("webhook_endpoint_update_failed", ref, requestID, reasonUncertain)
	default:
		return EndpointView{}, s.mutationNotConfirmed("webhook_endpoint_update_failed", ref, requestID, reasonUnavailable)
	}
}

// DeleteWebhook permanently deletes a disabled endpoint. Deleting an absent
// endpoint succeeds without another audit event; that proves no earlier
// uncertain deletion wrote its audit.
func (s *Service) DeleteWebhook(ctx context.Context, ref EndpointRef, expected *EntityVersion, requestID string) error {
	platform, _, ok := s.catalog.Lookup(ref.Type)
	if !ok {
		// No endpoint of an unregistered type can exist.
		return nil
	}
	eventID := s.gen.UUIDv7()
	d, result := s.repo.DeleteEndpoint(ctx, ref, platform, expected, eventID, requestID)
	switch result {
	case DeleteDeleted:
		s.endpointMutated(opWebhookEndpointDeleted, ref, platform, eventID, requestID,
			"bot_id", d.BotID, "credential_kind", d.CredentialKind, "generation_id", d.GenerationID, "config_version", d.ConfigVersion)
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
		return s.mutationNotConfirmed("webhook_endpoint_delete_failed", ref, requestID, reasonWrongType)
	case DeleteUncertain:
		return s.mutationNotConfirmed("webhook_endpoint_delete_failed", ref, requestID, reasonUncertain)
	default:
		return s.mutationNotConfirmed("webhook_endpoint_delete_failed", ref, requestID, reasonUnavailable)
	}
}

// endpointMutated records a confirmed endpoint mutation whose audit entry
// the transition already appended: the audit metric, the best-effort audit
// copy, and the feature event named after the operation, with safe fields.
func (s *Service) endpointMutated(op string, ref EndpointRef, platform, eventID, requestID string, fields ...any) {
	s.metrics.auditEvents.WithLabelValues(op, outcomeSuccess).Inc()
	s.logAudit(eventID, op, ref.String(), requestID, outcomeSuccess)
	fields = append([]any{"request_id", requestID, "webhook_type", ref.Type, "webhook_identifier", ref.Identifier, "bot_platform", platform}, fields...)
	observability.LogEvent(s.log, slog.LevelInfo, op, strings.ReplaceAll(op, "_", " "), fields...)
}

// Reasons an endpoint mutation could not be confirmed.
const (
	reasonWrongType   = "wrong_type"
	reasonUncertain   = "outcome_uncertain"
	reasonUnavailable = "dependency_unavailable"
)

var mutationFailureDetail = map[string]string{
	reasonWrongType: "stored structure has an unexpected type",
	reasonUncertain: "outcome is uncertain: read the endpoint and audit before retrying",
}

// mutationNotConfirmed logs a bounded error event for an endpoint mutation
// that could not be confirmed and returns its 503.
func (s *Service) mutationNotConfirmed(event string, ref EndpointRef, requestID, reason string) error {
	observability.LogEvent(s.log, slog.LevelError, event, "webhook endpoint mutation not confirmed",
		"request_id", requestID, "webhook_type", ref.Type, "webhook_identifier", ref.Identifier,
		"error_code", "dependency_unavailable", "reason_code", reason)
	return DependencyError{detail: mutationFailureDetail[reason]}
}

// endpointPath validates the endpoint route values; unknown shapes are
// indistinguishable from unknown endpoints.
func endpointPath(r *http.Request) (EndpointRef, bool) {
	ref := EndpointRef{Type: r.PathValue("webhook_type"), Identifier: r.PathValue("webhook_identifier")}
	return ref, webhookTypePattern.MatchString(ref.Type) && identifierPattern.MatchString(ref.Identifier)
}

// ifMatchFor parses If-Match for a mutation of ref. A malformed tag is 400
// only for an existing endpoint: the contract answers a missing endpoint
// the same way with or without If-Match, so absent reports that instead.
func (s *Service) ifMatchFor(r *http.Request, ref EndpointRef) (expected *EntityVersion, absent bool, err error) {
	expected, err = parseIfMatch(r)
	if err == nil {
		return expected, false, nil
	}
	if _, getErr := s.GetWebhook(r.Context(), ref.Type, ref.Identifier); errors.As(getErr, new(NotFoundError)) {
		return nil, true, nil
	}
	return nil, false, err
}

func (s *Service) handlePatch(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	ref, ok := endpointPath(r)
	if !ok {
		writeAPIError(w, NotFoundError{}, requestID)
		return
	}
	// The body is validated first: a malformed request is refused before
	// any storage access.
	var req PatchRequest
	if err := decodeBody(r, &req); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	if req.Enabled == nil {
		writeAPIError(w, BadRequestError{msg: "enabled is required"}, requestID)
		return
	}
	expected, absent, err := s.ifMatchFor(r, ref)
	switch {
	case absent:
		err = NotFoundError{}
	case err == nil:
		var view EndpointView
		if view, err = s.SetWebhookEnabled(r.Context(), ref, *req.Enabled, expected, requestID); err == nil {
			writeEndpoint(w, http.StatusOK, view)
			return
		}
	}
	writeAPIError(w, err, requestID)
}

func (s *Service) handleDelete(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	ref, ok := endpointPath(r)
	if !ok {
		// A shape that cannot name an endpoint names an absent one.
		w.WriteHeader(http.StatusNoContent)
		return
	}
	expected, absent, err := s.ifMatchFor(r, ref)
	if err == nil && !absent {
		err = s.DeleteWebhook(r.Context(), ref, expected, requestID)
	}
	if err != nil {
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
	if len(items) > maxEndpointsPerBot {
		// Creation enforces the cap; more members mean an inconsistent
		// index, so the response keeps the contract's bound.
		observability.LogEvent(s.log, slog.LevelWarn, "webhook_bot_endpoint_limit_exceeded", "bot endpoint index exceeds its cap; the list is truncated",
			"request_id", requestID, "bot_platform", botPlatform, "bot_id", botID, "member_count", len(items))
		items = items[:maxEndpointsPerBot]
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
