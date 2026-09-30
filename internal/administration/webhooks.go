package administration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

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
