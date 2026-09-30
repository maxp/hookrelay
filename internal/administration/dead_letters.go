package administration

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/maxp/hookrelay/internal/observability"
)

// messageIDPattern is the canonical lowercase UUID form of a Message
// Identifier; other shapes cannot name a dead letter.
var messageIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// DeadLetterView is one dead letter's safe metadata.
type DeadLetterView struct {
	MessageID        string          `json:"message_id"`
	Recipient        RecipientInput  `json:"recipient"`
	DeadLetteredMs   int64           `json:"dead_lettered_ms"`
	DeadLetterReason string          `json:"dead_letter_reason"`
	DeliveryCycle    int64           `json:"delivery_cycle"`
	Attempts         []AttemptEntry  `json:"attempts,omitempty"`
	ArchivedCycles   *ArchivedCycles `json:"archived_cycles_summary,omitempty"`
}

// DeadLetterPage is one list page with the opaque continuation cursor.
type DeadLetterPage struct {
	Items      []DeadLetterView `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

func deadLetterView(d DeadLetter) (DeadLetterView, bool) {
	rcpt, ok := recipientOf(d.RecipientIdentity)
	if !ok {
		return DeadLetterView{}, false
	}
	return DeadLetterView{MessageID: d.MessageID, Recipient: rcpt, DeadLetteredMs: d.DeadLetteredMs,
		DeadLetterReason: d.Reason, DeliveryCycle: d.DeliveryCycle, Attempts: d.Attempts, ArchivedCycles: d.Archived}, true
}

// ListDeadLetters pages the global DLQ newest first by safe metadata.
func (s *Service) ListDeadLetters(ctx context.Context, limit, cursor string) (DeadLetterPage, error) {
	n, err := parseListLimit(limit)
	if err != nil {
		return DeadLetterPage{}, err
	}
	after, err := decodeCursor(cursor, func(c DeadLetterCursor) bool { return c.Member != "" })
	if err != nil {
		return DeadLetterPage{}, err
	}
	items, err := s.deadLetters.ListDeadLetters(ctx, n+1, after)
	if err != nil {
		return DeadLetterPage{}, DependencyError{}
	}
	page := DeadLetterPage{Items: []DeadLetterView{}}
	for i, it := range items {
		if i == n {
			last := items[n-1]
			page.NextCursor = encodeCursor(DeadLetterCursor{Score: last.DeadLetteredMs, Member: last.MessageID})
			break
		}
		if it.RecordMissing {
			// A member without its record is reconciliation's concern; the
			// cursor still advances past it.
			continue
		}
		if v, ok := deadLetterView(it); ok {
			page.Items = append(page.Items, v)
		}
	}
	return page, nil
}

// GetDeadLetter reads one dead letter with its retained attempt history.
func (s *Service) GetDeadLetter(ctx context.Context, messageID string) (DeadLetterView, error) {
	if !messageIDPattern.MatchString(messageID) {
		return DeadLetterView{}, deadLetterNotFound()
	}
	d, err := s.deadLetters.GetDeadLetter(ctx, messageID)
	if err != nil {
		return DeadLetterView{}, DependencyError{}
	}
	if d == nil {
		return DeadLetterView{}, deadLetterNotFound()
	}
	v, ok := deadLetterView(*d)
	if !ok {
		return DeadLetterView{}, DependencyError{detail: "stored dead-letter record is malformed"}
	}
	return v, nil
}

func deadLetterNotFound() error {
	return StatusError{Status: http.StatusNotFound, Code: "dead_letter_not_found", Msg: "dead-letter message not found"}
}

// ReplayRequest is the strict replay body; an empty body replays with the
// default reject resolution.
type ReplayRequest struct {
	DeduplicationConflictResolution *string `json:"deduplication_conflict_resolution"`
}

// ReplayView is the replayed response.
type ReplayView struct {
	Status                  string `json:"status"`
	MessageID               string `json:"message_id"`
	DeliveryCycle           int64  `json:"delivery_cycle"`
	QueuePosition           string `json:"queue_position"`
	ReplayedMs              int64  `json:"replayed_ms"`
	DeduplicationResolution string `json:"deduplication_resolution"`
}

// ReplayDeadLetter runs the audited replay. An uncertain outcome is
// reported as such: the caller reconciles instead of retrying.
func (s *Service) ReplayDeadLetter(ctx context.Context, messageID string, req ReplayRequest, requestID string) (ReplayView, error) {
	resolution := ResolutionReject
	if req.DeduplicationConflictResolution != nil {
		resolution = *req.DeduplicationConflictResolution
	}
	if resolution != ResolutionReject && resolution != ResolutionKeepCurrent {
		return ReplayView{}, BadRequestError{msg: "deduplication_conflict_resolution must be reject or keep_current"}
	}
	if !messageIDPattern.MatchString(messageID) {
		s.metrics.replays.WithLabelValues(string(ReplayNotFound)).Inc()
		return ReplayView{}, deadLetterNotFound()
	}
	eventID := s.gen.UUIDv7()
	r := s.deadLetters.ReplayDeadLetter(ctx, messageID, resolution, eventID, requestID)
	switch r.Result {
	case ReplayReplayed, ReplayNotFound, ReplayMessageMissing, ReplayRecipientBlocked, ReplayDeduplicationConflict, ReplayUncertain:
		s.metrics.replays.WithLabelValues(string(r.Result)).Inc()
	default:
		s.metrics.replays.WithLabelValues(replayFailed).Inc()
	}
	switch r.Result {
	case ReplayReplayed:
		s.metrics.auditEvents.WithLabelValues(opDeadLetterReplayed, outcomeSuccess).Inc()
		s.logAudit(eventID, opDeadLetterReplayed, messageID, requestID, outcomeSuccess)
		fields := []any{"request_id", requestID, "message_id", messageID, "delivery_cycle", r.DeliveryCycle,
			"queue_position", r.QueuePosition, "deduplication_resolution", r.DeduplicationResolution}
		if rcpt, ok := recipientOf(r.RecipientIdentity); ok {
			fields = append(fields, "recipient_scope", rcpt.Scope, "bot_platform", rcpt.BotPlatform, "bot_id", rcpt.BotID)
			if rcpt.ChatID != "" {
				fields = append(fields, "chat_id", rcpt.ChatID)
			}
			if rcpt.UserID != "" {
				fields = append(fields, "user_id", rcpt.UserID)
			}
		}
		observability.LogEvent(s.log, slog.LevelInfo, "delivery_replayed", "dead-letter message replayed", fields...)
		if r.QueuePosition == "head" {
			s.signalReady(readySourceReplay)
		}
		return ReplayView{Status: "replayed", MessageID: messageID, DeliveryCycle: r.DeliveryCycle, QueuePosition: r.QueuePosition,
			ReplayedMs: r.ReplayedMs, DeduplicationResolution: r.DeduplicationResolution}, nil
	case ReplayNotFound:
		return ReplayView{}, deadLetterNotFound()
	case ReplayRecipientBlocked:
		return ReplayView{}, StatusError{Status: http.StatusConflict, Code: "recipient_blocked",
			Msg: "the recipient is blocked: recover the block before replay"}
	case ReplayDeduplicationConflict:
		return ReplayView{}, StatusError{Status: http.StatusConflict, Code: "deduplication_conflict",
			Msg: "the deduplication identity now maps to another message: replay with keep_current to keep that mapping"}
	case ReplayMessageMissing:
		s.logReplayFailure(requestID, messageID, "message_missing")
		return ReplayView{}, DependencyError{detail: "the dead-letter message is missing: reconciliation holds readiness until it is corrected"}
	case ReplayUncertain:
		s.logReplayFailure(requestID, messageID, "outcome_uncertain")
		return ReplayView{}, DependencyError{detail: "replay outcome is uncertain: read the dead letter and the audit before any retry"}
	case ReplayWrongType:
		s.logReplayFailure(requestID, messageID, "wrong_type")
		return ReplayView{}, DependencyError{detail: "stored structure has an unexpected type"}
	default:
		s.logReplayFailure(requestID, messageID, "dependency_unavailable")
		return ReplayView{}, DependencyError{}
	}
}

func (s *Service) logReplayFailure(requestID, messageID, reason string) {
	observability.LogEvent(s.log, slog.LevelError, "dead_letter_replay_failed", "dead-letter replay not confirmed",
		"request_id", requestID, "message_id", messageID, "error_code", "dependency_unavailable", "reason_code", reason)
}

func (s *Service) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	q := r.URL.Query()
	page, err := s.ListDeadLetters(r.Context(), q.Get("limit"), q.Get("cursor"))
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Service) handleGetDeadLetter(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	v, err := s.GetDeadLetter(r.Context(), r.PathValue("message_id"))
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Service) handleReplayDeadLetter(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	var req ReplayRequest
	if r.ContentLength != 0 {
		if err := decodeBody(r, &req); err != nil {
			writeAPIError(w, err, requestID)
			return
		}
	}
	v, err := s.ReplayDeadLetter(r.Context(), r.PathValue("message_id"), req, requestID)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, v)
}
