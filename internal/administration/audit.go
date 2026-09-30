package administration

import (
	"context"
	"errors"
	"net/http"
	"regexp"
)

// AuditEntry is one administrative audit event: the stream entry ID plus
// the allowlisted event fields that are present. Any other stored field is
// dropped by the reader.
type AuditEntry struct {
	StreamID    string `json:"stream_id"`
	EventID     string `json:"event_id,omitempty"`
	TimestampMs int64  `json:"timestamp_ms,omitempty"`
	Actor       string `json:"actor,omitempty"`
	Operation   string `json:"operation,omitempty"`
	Target      string `json:"target,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

// AuditLog reads the bounded administrative audit Stream.
type AuditLog interface {
	// ListAudit returns at most limit entries newest first, strictly older
	// than the before stream ID when it is set. A missing Stream is an
	// empty list; a key of another type is ErrStoredWrongType.
	ListAudit(ctx context.Context, limit int, before string) ([]AuditEntry, error)
}

// AuditCursor is the position after the last listed entry.
type AuditCursor struct {
	ID string `json:"id"`
}

// streamIDPattern is a complete Valkey stream entry ID.
var streamIDPattern = regexp.MustCompile(`^[0-9]{1,20}-[0-9]{1,20}$`)

// AuditPage is one audit list page with the opaque continuation cursor.
type AuditPage struct {
	Items      []AuditEntry `json:"items"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// ListAudit pages the administrative audit newest first. Reading the
// audit is not itself audited.
func (s *Service) ListAudit(ctx context.Context, limit, cursor string) (AuditPage, error) {
	n, err := parseListLimit(limit)
	if err != nil {
		return AuditPage{}, err
	}
	after, err := decodeCursor(cursor, func(c AuditCursor) bool { return streamIDPattern.MatchString(c.ID) })
	if err != nil {
		return AuditPage{}, err
	}
	before := ""
	if after != nil {
		before = after.ID
	}
	items, err := s.auditLog.ListAudit(ctx, n+1, before)
	if err != nil {
		if errors.Is(err, ErrStoredWrongType) {
			return AuditPage{}, DependencyError{detail: ErrStoredWrongType.Error()}
		}
		return AuditPage{}, DependencyError{}
	}
	page := AuditPage{Items: items}
	if len(items) > n {
		page.Items = items[:n]
		page.NextCursor = encodeCursor(AuditCursor{ID: items[n-1].StreamID})
	}
	if page.Items == nil {
		page.Items = []AuditEntry{}
	}
	return page, nil
}

func (s *Service) handleListAudit(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	q := r.URL.Query()
	page, err := s.ListAudit(r.Context(), q.Get("limit"), q.Get("cursor"))
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, page)
}
