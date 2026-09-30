package administration

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
